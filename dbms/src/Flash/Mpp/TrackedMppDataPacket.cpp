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

#include "TrackedMppDataPacket.h"


namespace DB
{
void TrackedMppDataPacket::alloc()
{
    if (size)
    {
        try
        {
            // TODO: troubleshoot what cause CurrentMemoryTracker incorrect
            // server.context().getGlobalContext().getProcessList().total_memory_tracker.alloc(size);
            if (proc_memory_tracker)
                proc_memory_tracker->alloc(size);
        }
        catch (...)
        {
            has_err = true;
            std::rethrow_exception(std::current_exception());
        }
    }
}

void TrackedMppDataPacket::trackFree() const
{
    if (size && !has_err)
    {
        // TODO: troubleshoot what cause CurrentMemoryTracker incorrect
        if (proc_memory_tracker)
            proc_memory_tracker->free(size);
    }
}

MemoryTracker * TrackedMppDataPacket::proc_memory_tracker = nullptr;

} // namespace DB
