#include <common/crc64.h>
#include <common/crc64_fast.h>
#include <common/crc64_table.h>
#include <common/detect_features.h>
#include <common/simd.h>
namespace crc64
{
using namespace common;
Digest::Digest(Mode mode)
{
    // clang-format off
#ifdef TIFLASH_CRC64_HAS_SIMD_SUPPORT
    using namespace simd_option;
#if TIFLASH_COMPILER_VPCLMULQDQ_SUPPORT
#ifdef TIFLASH_ENABLE_AVX512_SUPPORT
    if ((mode == Mode::Auto || mode >= Mode::SIMD_512) && ENABLE_AVX512
        && cpu_feature_flags.vpclmulqdq && cpu_feature_flags.avx512dq)
    {
        update_fn = [](uint64_t _state, const void * _src, size_t _length) {
            return crc64::_detail::update_fast<512>(crc64::_detail::update_vpclmulqdq_avx512, _state, _src, _length);
        };
    }
    else
#endif // TIFLASH_ENABLE_AVX512_SUPPORT
#ifdef TIFLASH_ENABLE_AVX_SUPPORT
    if ((mode == Mode::Auto || mode >= Mode::SIMD_256) && ENABLE_AVX
        && cpu_feature_flags.vpclmulqdq && cpu_feature_flags.avx2)
    {
        update_fn = [](uint64_t _state, const void * _src, size_t _length) {
            return crc64::_detail::update_fast<256>(crc64::_detail::update_vpclmulqdq_avx2, _state, _src, _length);
        };
    }
    else
#endif // TIFLASH_ENABLE_AVX_SUPPORT
#endif // TIFLASH_COMPILER_VPCLMULQDQ_SUPPORT
    if (mode == Mode::Auto || mode >= Mode::SIMD_128)
    {
        update_fn = [](uint64_t _state, const void * _src, size_t _length) {
            return crc64::_detail::update_fast(crc64::_detail::update_simd, _state, _src, _length);
        };
#ifdef TIFLASH_ENABLE_ASIMD_SUPPORT
        if (!ENABLE_ASIMD || !cpu_feature_flags.pmull)
        {
            update_fn = _detail::update_table;
        }
#else // must be SSE case then
        if (!cpu_feature_flags.pclmulqdq)
        {
            update_fn = _detail::update_table;
        }
#endif // TIFLASH_ENABLE_ASIMD_SUPPORT
    }
    else
#endif // TIFLASH_CRC64_HAS_SIMD_SUPPORT
    {
        update_fn = _detail::update_table;
    }
    // clang-format on
}
} // namespace crc64