// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/CPUAffinityManager.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadMetricUtil.h>
#include <Common/TiFlashMetrics.h>
#include <Common/setThreadName.h>
#include <Core/Types.h>
#include <Flash/BatchCoprocessorHandler.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/FlashService.h>
#include <Flash/Management/ManualCompact.h>
#include <Flash/Mpp/MPPHandler.h>
#include <Flash/Mpp/MPPTaskManager.h>
#include <Flash/Mpp/Utils.h>
#include <Flash/ServiceUtils.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Server/IServer.h>
#include <Storages/IManageableStorage.h>
#include <Storages/Transaction/TMTContext.h>
#include <grpcpp/server_builder.h>
 #if USE_JEMALLOC
#include <jemalloc/jemalloc.h>
 #endif

#include <ext/scope_guard.h>

#include "Common/ThreadFactory.h"

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

constexpr char tls_err_msg[] = "common name check is failed";

FlashService::FlashService(IServer & server_)
    : server(server_)
    , security_config(server_.securityConfig())
    , log(&Poco::Logger::get("FlashService"))
    , manual_compact_manager(std::make_unique<Management::ManualCompactManager>(
          server_.context().getGlobalContext(),
          server_.context().getGlobalContext().getSettingsRef()))
{
    auto settings = server_.context().getSettingsRef();
    enable_local_tunnel = settings.enable_local_tunnel;
    enable_async_grpc_client = settings.enable_async_grpc_client;
    const size_t default_size = 2 * getNumberOfPhysicalCPUCores();

    auto cop_pool_size = static_cast<size_t>(settings.cop_pool_size);
    cop_pool_size = cop_pool_size ? cop_pool_size : default_size;
    LOG_FMT_INFO(log, "Use a thread pool with {} threads to handle cop requests.", cop_pool_size);
    cop_pool = std::make_unique<ThreadPool>(cop_pool_size, [] { setThreadName("cop-pool"); });

    auto batch_cop_pool_size = static_cast<size_t>(settings.batch_cop_pool_size);
    batch_cop_pool_size = batch_cop_pool_size ? batch_cop_pool_size : default_size;
    LOG_FMT_INFO(log, "Use a thread pool with {} threads to handle batch cop requests.", batch_cop_pool_size);
    batch_cop_pool = std::make_unique<ThreadPool>(batch_cop_pool_size, [] { setThreadName("batch-cop-pool"); });
    std::thread t = ThreadFactory::newThread(false, "MemTrackThread", &FlashService::memCheckJob, this);
    t.detach();
}

void jestats() 
{
#if USE_JEMALLOC
    size_t sz;
    // Update the statistics cached by mallctl.
    uint64_t epoch = 1;
    sz = sizeof(epoch);
    mallctl("epoch", &epoch, &sz, &epoch, sz);

    // Get basic allocation statistics.  Take care to check for
    // errors, since --enable-stats must have been specified at
    // build time for these statistics to be available.
    size_t allocated, active, metadata, resident, mapped, retained;
    sz = sizeof(size_t);
    if (mallctl("stats.allocated", &allocated, &sz, NULL, 0) == 0
        && mallctl("stats.active", &active, &sz, NULL, 0) == 0
        && mallctl("stats.metadata", &metadata, &sz, NULL, 0) == 0
        && mallctl("stats.resident", &resident, &sz, NULL, 0) == 0
        && mallctl("stats.mapped", &mapped, &sz, NULL, 0) == 0 
        && mallctl("stats.retained", &retained, &sz, NULL, 0) == 0) {
            fprintf(stderr,
                "Current allocated/active/metadata/resident/mapped/retained: %zu/%zu/%zu/%zu/%zu/%zu\n",
                allocated, active, metadata, resident, mapped, retained);
    }
#endif
}

void process_mem_usage(double& vm_usage, double& resident_set)
{
   using std::ios_base;
   using std::ifstream;
   using std::string;

   vm_usage     = 0.0;
   resident_set = 0.0;

   // 'file' stat seems to give the most reliable results
   //
   ifstream stat_stream("/proc/self/stat",ios_base::in);

   // dummy vars for leading entries in stat that we don't care about
   //
   string pid, comm, state, ppid, pgrp, session, tty_nr;
   string tpgid, flags, minflt, cminflt, majflt, cmajflt;
   string utime, stime, cutime, cstime, priority, nice;
   string O, itrealvalue, starttime;

   // the two fields we want
   //
   unsigned long vsize;
   long rss;

   stat_stream >> pid >> comm >> state >> ppid >> pgrp >> session >> tty_nr
               >> tpgid >> flags >> minflt >> cminflt >> majflt >> cmajflt
               >> utime >> stime >> cutime >> cstime >> priority >> nice
               >> O >> itrealvalue >> starttime >> vsize >> rss; // don't care about the rest

   stat_stream.close();

   long page_size_kb = sysconf(_SC_PAGE_SIZE) / 1024; // in case x86-64 is configured to use 2MB pages
   vm_usage     = vsize / 1024.0;
   resident_set = rss * page_size_kb;
}

void FlashService::memCheckJob()
{
    double vm_usage;
    double resident_set;
    size_t max_mem = 0;
    while (!end_syn)
    {
        DB::ProcessList & proc_list = server.context().getGlobalContext().getProcessList();
        long long tracked_used = static_cast<long long>(proc_list.getMemAlloacted());
        long long limit = static_cast<long long>(proc_list.getMemLimit());
        long long cur_tracked_proto = tracked_proto.load();
 #if USE_JEMALLOC
        size_t value{0};
        size_t size = sizeof(value);
        mallctl("stats.resident", &value, &size, nullptr, 0);
 #endif
        long long cur_tracked_peak = tracked_peak;
        tracked_peak = tracked_mem.load();
        process_mem_usage(vm_usage, resident_set);
        resident_set*=1024;
        real_rss = (long long)resident_set;
        LOG_FMT_INFO(log, "mem_check: tracked: {}, limit: {}, proto: {}, glb_mem_track: {},glb_peak:{}, proc_mem:{}, alloc_cnt:{}, reloc_cnt:{}, free_cnt:{}, alloc_rec_cnt:{}, reloc_rec_cnt:{}, free_rec_cnt:{}", 
        tracked_used, limit, cur_tracked_proto, tracked_mem.load(), cur_tracked_peak, resident_set,
        tracked_alloc.load(), tracked_reloc.load(), tracked_free.load(),
        tracked_rec_alloc.load(), tracked_rec_reloc.load(), tracked_rec_free.load());
        max_mem = std::max(max_mem, resident_set);
        LOG_FMT_INFO(log, "mem_checkV2: tracked: {} GB, allocator: {} GB, dirty_alloc: {} , avg_alloc: {}, max_alloc:{}, proto: {} GB, untracked_proto: {} GB, glb_mem_track: {} GB, glb_peak:{} GB, proc_mem:{} GB, diff:{} GB, max_mem:{} GB, alloc: {} GB, delloc: {} GB", 
        static_cast<long long>(tracked_used/1024/1024)/1000.0, 
        static_cast<long long>(tracked_alct.load()/1024/1024)/1000.0, 
        static_cast<long long>(dirty_alloc.load()),
        static_cast<long long>(alct_cnt.load()? alct_sum.load()/alct_cnt.load(): 0),
        static_cast<long long>(max_alct.load()),
        static_cast<long long>(cur_tracked_proto/1024/1024)/1000.0,
        static_cast<long long>(untracked_proto.load()/1024/1024)/1000.0,
        static_cast<long long>(tracked_mem.load()/1024/1024)/1000.0,
        static_cast<long long>(cur_tracked_peak/1024/1024)/1000.0,
        static_cast<long long>(resident_set/1024/1024)/1000.0,
        static_cast<long long>((resident_set-tracked_used)/1024/1024)/1000.0,
        static_cast<long long>(max_mem/1024/1024)/1000.0,
        static_cast<long long>(alloc_mem/1024/1024)/1000.0,
        static_cast<long long>(dealloc_mem/1024/1024)/1000.0
         );
        jestats();
#if USE_JEMALLOC
        mallctl("prof.dump", NULL, NULL, NULL, 0);
#endif
        // std::cerr<<"*******************"<<std::endl;
        // malloc_stats_print(NULL, NULL, NULL);
        // std::cerr<<"*******************"<<std::endl;
        usleep(100000);
    }
    end_fin = true;
}

FlashService::~FlashService()
{
    end_syn = true;
    while (!end_fin)
    {
        usleep(10000);
    }
}

// Use executeInThreadPool to submit job to thread pool which return grpc::Status.
grpc::Status executeInThreadPool(ThreadPool & pool, std::function<grpc::Status()> job)
{
    std::packaged_task<grpc::Status()> task(job);
    std::future<grpc::Status> future = task.get_future();
    pool.schedule([&task] { task(); });
    return future.get();
}

grpc::Status FlashService::Coprocessor(
    grpc::ServerContext * grpc_context,
    const coprocessor::Request * request,
    coprocessor::Response * response)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    LOG_FMT_DEBUG(log, "Handling coprocessor request: {}", request->DebugString());

    if (!security_config.checkGrpcContext(grpc_context))
    {
        return grpc::Status(grpc::PERMISSION_DENIED, tls_err_msg);
    }

    GET_METRIC(tiflash_coprocessor_request_count, type_cop).Increment();
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_cop).Increment();
    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_cop).Decrement();
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_cop).Observe(watch.elapsedSeconds());
        GET_METRIC(tiflash_coprocessor_response_bytes).Increment(response->ByteSizeLong());
    });

    grpc::Status ret = executeInThreadPool(*cop_pool, [&] {
        auto [context, status] = createDBContext(grpc_context);
        if (!status.ok())
        {
            return status;
        }
        CoprocessorContext cop_context(*context, request->context(), *grpc_context);
        CoprocessorHandler cop_handler(cop_context, request, response);
        return cop_handler.execute();
    });

    LOG_FMT_DEBUG(log, "Handle coprocessor request done: {}, {}", ret.error_code(), ret.error_message());
    return ret;
}

::grpc::Status FlashService::BatchCoprocessor(::grpc::ServerContext * grpc_context, const ::coprocessor::BatchRequest * request, ::grpc::ServerWriter<::coprocessor::BatchResponse> * writer)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    LOG_FMT_DEBUG(log, "Handling coprocessor request: {}", request->DebugString());

    if (!security_config.checkGrpcContext(grpc_context))
    {
        return grpc::Status(grpc::PERMISSION_DENIED, tls_err_msg);
    }

    GET_METRIC(tiflash_coprocessor_request_count, type_super_batch).Increment();
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_super_batch).Increment();
    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_super_batch).Decrement();
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_super_batch).Observe(watch.elapsedSeconds());
        // TODO: update the value of metric tiflash_coprocessor_response_bytes.
    });

    grpc::Status ret = executeInThreadPool(*batch_cop_pool, [&] {
        auto [context, status] = createDBContext(grpc_context);
        if (!status.ok())
        {
            return status;
        }
        CoprocessorContext cop_context(*context, request->context(), *grpc_context);
        BatchCoprocessorHandler cop_handler(cop_context, request, writer);
        return cop_handler.execute();
    });

    LOG_FMT_DEBUG(log, "Handle coprocessor request done: {}, {}", ret.error_code(), ret.error_message());
    return ret;
}

::grpc::Status FlashService::DispatchMPPTask(
    ::grpc::ServerContext * grpc_context,
    const ::mpp::DispatchTaskRequest * request,
    ::mpp::DispatchTaskResponse * response)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    LOG_FMT_DEBUG(log, "Handling mpp dispatch request: {}", request->DebugString());
    if (!security_config.checkGrpcContext(grpc_context))
    {
        return grpc::Status(grpc::PERMISSION_DENIED, tls_err_msg);
    }
    GET_METRIC(tiflash_coprocessor_request_count, type_dispatch_mpp_task).Increment();
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_dispatch_mpp_task).Increment();
    GET_METRIC(tiflash_thread_count, type_active_threads_of_dispatch_mpp).Increment();
    GET_METRIC(tiflash_thread_count, type_total_threads_of_raw).Increment();
    if (!tryToResetMaxThreadsMetrics())
    {
        GET_METRIC(tiflash_thread_count, type_max_threads_of_dispatch_mpp).Set(std::max(GET_METRIC(tiflash_thread_count, type_max_threads_of_dispatch_mpp).Value(), GET_METRIC(tiflash_thread_count, type_active_threads_of_dispatch_mpp).Value()));
        GET_METRIC(tiflash_thread_count, type_max_threads_of_raw).Set(std::max(GET_METRIC(tiflash_thread_count, type_max_threads_of_raw).Value(), GET_METRIC(tiflash_thread_count, type_total_threads_of_raw).Value()));
    }

    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_thread_count, type_total_threads_of_raw).Decrement();
        GET_METRIC(tiflash_thread_count, type_active_threads_of_dispatch_mpp).Decrement();
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_dispatch_mpp_task).Decrement();
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_dispatch_mpp_task).Observe(watch.elapsedSeconds());
        GET_METRIC(tiflash_coprocessor_response_bytes).Increment(response->ByteSizeLong());
    });

    auto [context, status] = createDBContext(grpc_context);
    if (!status.ok())
    {
        return status;
    }

    MPPHandler mpp_handler(*request);
    return mpp_handler.execute(context, response);
}

::grpc::Status FlashService::IsAlive(::grpc::ServerContext * grpc_context [[maybe_unused]],
                                     const ::mpp::IsAliveRequest * request [[maybe_unused]],
                                     ::mpp::IsAliveResponse * response [[maybe_unused]])
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    if (!security_config.checkGrpcContext(grpc_context))
    {
        return grpc::Status(grpc::PERMISSION_DENIED, tls_err_msg);
    }

    auto [context, status] = createDBContext(grpc_context);
    if (!status.ok())
    {
        return status;
    }

    auto & tmt_context = context->getTMTContext();
    response->set_available(tmt_context.checkRunning());
    return ::grpc::Status::OK;
}

::grpc::Status returnStatus(EstablishCallData * calldata, const grpc::Status & status)
{
    if (calldata)
    {
        calldata->writeDone(status);
    }
    return status;
}

::grpc::Status FlashService::establishMPPConnectionSyncOrAsync(::grpc::ServerContext * grpc_context,
                                                               const ::mpp::EstablishMPPConnectionRequest * request,
                                                               ::grpc::ServerWriter<::mpp::MPPDataPacket> * sync_writer,
                                                               EstablishCallData * calldata)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    // Establish a pipe for data transferring. The pipes has registered by the task in advance.
    // We need to find it out and bind the grpc stream with it.
    LOG_FMT_DEBUG(log, "Handling establish mpp connection request: {}", request->DebugString());

    if (!security_config.checkGrpcContext(grpc_context))
    {
        return returnStatus(calldata, grpc::Status(grpc::PERMISSION_DENIED, tls_err_msg));
    }
    GET_METRIC(tiflash_coprocessor_request_count, type_mpp_establish_conn).Increment();
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_mpp_establish_conn).Increment();
    GET_METRIC(tiflash_thread_count, type_active_threads_of_establish_mpp).Increment();
    GET_METRIC(tiflash_thread_count, type_total_threads_of_raw).Increment();
    if (!tryToResetMaxThreadsMetrics())
    {
        GET_METRIC(tiflash_thread_count, type_max_threads_of_establish_mpp).Set(std::max(GET_METRIC(tiflash_thread_count, type_max_threads_of_establish_mpp).Value(), GET_METRIC(tiflash_thread_count, type_active_threads_of_establish_mpp).Value()));
        GET_METRIC(tiflash_thread_count, type_max_threads_of_raw).Set(std::max(GET_METRIC(tiflash_thread_count, type_max_threads_of_raw).Value(), GET_METRIC(tiflash_thread_count, type_total_threads_of_raw).Value()));
    }
    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_thread_count, type_total_threads_of_raw).Decrement();
        GET_METRIC(tiflash_thread_count, type_active_threads_of_establish_mpp).Decrement();
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_mpp_establish_conn).Decrement();
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_mpp_establish_conn).Observe(watch.elapsedSeconds());
        // TODO: update the value of metric tiflash_coprocessor_response_bytes.
    });

    auto [context, status] = createDBContext(grpc_context);
    if (!status.ok())
    {
        return returnStatus(calldata, status);
    }

    auto & tmt_context = context->getTMTContext();
    auto task_manager = tmt_context.getMPPTaskManager();
    std::chrono::seconds timeout(10);
    std::string err_msg;
    MPPTunnelPtr tunnel = nullptr;
    {
        MPPTaskPtr sender_task = task_manager->findTaskWithTimeout(request->sender_meta(), timeout, err_msg);
        if (sender_task != nullptr)
        {
            std::tie(tunnel, err_msg) = sender_task->getTunnel(request);
        }
        if (tunnel == nullptr)
        {
            if (calldata)
            {
                LOG_ERROR(log, err_msg);
                // In Async version, writer::Write() return void.
                // So the way to track Write fail and return grpc::StatusCode::UNKNOWN is to catch the exeception.
                calldata->writeErr(getPacketWithError(err_msg));
                return grpc::Status::OK;
            }
            else
            {
                LOG_ERROR(log, err_msg);
                if (sync_writer->Write(getPacketWithError(err_msg)))
                {
                    return grpc::Status::OK;
                }
                else
                {
                    LOG_FMT_DEBUG(log, "Write error message failed for unknown reason.");
                    return grpc::Status(grpc::StatusCode::UNKNOWN, "Write error message failed for unknown reason.");
                }
            }
        }
    }
    Stopwatch stopwatch;
    if (calldata)
    {
        calldata->attachTunnel(tunnel);
        // In async mode, this function won't wait for the request done and the finish event is handled in EstablishCallData.
        tunnel->connect(calldata);
        LOG_FMT_DEBUG(tunnel->getLogger(), "connect tunnel successfully in async way");
    }
    else
    {
        SyncPacketWriter writer(sync_writer);
        tunnel->connect(&writer);
        LOG_FMT_DEBUG(tunnel->getLogger(), "connect tunnel successfully and begin to wait");
        tunnel->waitForFinish();
        LOG_FMT_INFO(tunnel->getLogger(), "connection for {} cost {} ms.", tunnel->id(), stopwatch.elapsedMilliseconds());
    }

    // TODO: Check if there are errors in task.

    return grpc::Status::OK;
}

::grpc::Status FlashService::CancelMPPTask(
    ::grpc::ServerContext * grpc_context,
    const ::mpp::CancelTaskRequest * request,
    ::mpp::CancelTaskResponse * response)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    // CancelMPPTask cancels the query of the task.
    LOG_FMT_DEBUG(log, "cancel mpp task request: {}", request->DebugString());

    if (!security_config.checkGrpcContext(grpc_context))
    {
        return grpc::Status(grpc::PERMISSION_DENIED, tls_err_msg);
    }
    GET_METRIC(tiflash_coprocessor_request_count, type_cancel_mpp_task).Increment();
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_cancel_mpp_task).Increment();
    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_cancel_mpp_task).Decrement();
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_cancel_mpp_task).Observe(watch.elapsedSeconds());
        GET_METRIC(tiflash_coprocessor_response_bytes).Increment(response->ByteSizeLong());
    });

    auto [context, status] = createDBContext(grpc_context);
    if (!status.ok())
    {
        auto err = std::make_unique<mpp::Error>();
        err->set_msg("error status");
        response->set_allocated_error(err.release());
        return status;
    }
    auto & tmt_context = context->getTMTContext();
    auto task_manager = tmt_context.getMPPTaskManager();
    task_manager->cancelMPPQuery(request->meta().start_ts(), "Receive cancel request from TiDB");
    return grpc::Status::OK;
}

String getClientMetaVarWithDefault(const grpc::ServerContext * grpc_context, const String & name, const String & default_val)
{
    if (auto it = grpc_context->client_metadata().find(name); it != grpc_context->client_metadata().end())
        return String(it->second.data(), it->second.size());

    return default_val;
}

std::tuple<ContextPtr, grpc::Status> FlashService::createDBContext(const grpc::ServerContext * grpc_context) const
{
    try
    {
        /// Create DB context.
        auto context = std::make_shared<Context>(server.context());
        context->setGlobalContext(server.context());

        /// Set a bunch of client information.
        std::string user = getClientMetaVarWithDefault(grpc_context, "user", "default");
        std::string password = getClientMetaVarWithDefault(grpc_context, "password", "");
        std::string quota_key = getClientMetaVarWithDefault(grpc_context, "quota_key", "");
        std::string peer = grpc_context->peer();
        Int64 pos = peer.find(':');
        if (pos == -1)
        {
            return std::make_tuple(context, ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Invalid peer address: " + peer));
        }
        std::string client_ip = peer.substr(pos + 1);
        Poco::Net::SocketAddress client_address(client_ip);

        context->setUser(user, password, client_address, quota_key);

        String query_id = getClientMetaVarWithDefault(grpc_context, "query_id", "");
        context->setCurrentQueryId(query_id);

        ClientInfo & client_info = context->getClientInfo();
        client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
        client_info.interface = ClientInfo::Interface::GRPC;

        /// Set DAG parameters.
        std::string dag_records_per_chunk_str = getClientMetaVarWithDefault(grpc_context, "dag_records_per_chunk", "");
        if (!dag_records_per_chunk_str.empty())
        {
            context->setSetting("dag_records_per_chunk", dag_records_per_chunk_str);
        }

        String max_threads = getClientMetaVarWithDefault(grpc_context, "tidb_max_tiflash_threads", "");
        if (!max_threads.empty())
        {
            context->setSetting("max_threads", max_threads);
            LOG_FMT_INFO(log, "set context setting max_threads to {}", max_threads);
        }

        context->setSetting("enable_async_server", is_async ? "true" : "false");
        context->setSetting("enable_local_tunnel", enable_local_tunnel ? "true" : "false");
        context->setSetting("enable_async_grpc_client", enable_async_grpc_client ? "true" : "false");
        return std::make_tuple(context, grpc::Status::OK);
    }
    catch (Exception & e)
    {
        LOG_FMT_ERROR(log, "DB Exception: {}", e.message());
        return std::make_tuple(std::make_shared<Context>(server.context()), grpc::Status(tiflashErrorCodeToGrpcStatusCode(e.code()), e.message()));
    }
    catch (const std::exception & e)
    {
        LOG_FMT_ERROR(log, "std exception: {}", e.what());
        return std::make_tuple(std::make_shared<Context>(server.context()), grpc::Status(grpc::StatusCode::INTERNAL, e.what()));
    }
    catch (...)
    {
        LOG_FMT_ERROR(log, "other exception");
        return std::make_tuple(std::make_shared<Context>(server.context()), grpc::Status(grpc::StatusCode::INTERNAL, "other exception"));
    }
}

::grpc::Status FlashService::Compact(::grpc::ServerContext * grpc_context, const ::kvrpcpb::CompactRequest * request, ::kvrpcpb::CompactResponse * response)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    if (!security_config.checkGrpcContext(grpc_context))
    {
        return grpc::Status(grpc::PERMISSION_DENIED, tls_err_msg);
    }

    return manual_compact_manager->handleRequest(request, response);
}

} // namespace DB
