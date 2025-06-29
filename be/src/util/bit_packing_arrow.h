// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <arrow/util/bpacking.h>
#ifdef __ARM_NEON
#include <arrow/util/bpacking_neon.h>
#endif
#ifdef __AVX2__
#include <arrow/util/bpacking_avx2.h>
#endif

#include "common/logging.h"
#include "util/bit_packing_default.h"

namespace starrocks::util::bitpacking_arrow {

template <typename OutType, int BIT_WIDTH>
static const uint8_t* UnpackValues(const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
                                   OutType* __restrict__ out) {
    int batch_size = num_values / 32 * 32;
    const int byte_width = 8;
    if constexpr (sizeof(OutType) == 4) {
#if defined(__AVX2__)
        int num_unpacked = arrow::internal::unpack32_avx2(reinterpret_cast<const uint32_t*>(in),
                                                          reinterpret_cast<uint32_t*>(out), batch_size, BIT_WIDTH);
#elif defined(__ARM_NEON)
        int num_unpacked = arrow::internal::unpack32_neon(reinterpret_cast<const uint32_t*>(in),
                                                          reinterpret_cast<uint32_t*>(out), batch_size, BIT_WIDTH);
#else
        int num_unpacked = arrow::internal::unpack32(reinterpret_cast<const uint32_t*>(in),
                                                     reinterpret_cast<uint32_t*>(out), batch_size, BIT_WIDTH);
#endif

        DCHECK(num_unpacked == batch_size);
        in += num_unpacked * BIT_WIDTH / byte_width;
    } else if constexpr (sizeof(OutType) == 8 && BIT_WIDTH > 32) {
        // Use unpack64 only if BIT_WIDTH is larger than 32
        // TODO (ARROW-13677): improve the performance of internal::unpack64
        // and remove the restriction of BIT_WIDTH
        int num_unpacked = arrow::internal::unpack64(in, reinterpret_cast<uint64_t*>(out), batch_size, BIT_WIDTH);
        DCHECK(num_unpacked == batch_size);
        in += num_unpacked * BIT_WIDTH / byte_width;
    } else {
        // TODO: revisit this limit if necessary
        DCHECK_LE(BIT_WIDTH, 32);
        const int buffer_size = 1024;
        uint32_t unpack_buffer[buffer_size];

        int64_t decoded = 0;
        while (decoded < batch_size) {
            auto size = batch_size - decoded > buffer_size ? buffer_size : batch_size - decoded;
#if defined(__AVX2__)
            int num_unpacked = arrow::internal::unpack32_avx2(reinterpret_cast<const uint32_t*>(in), unpack_buffer,
                                                              size, BIT_WIDTH);
#elif defined(__ARM_NEON)
            int num_unpacked = arrow::internal::unpack32_neon(reinterpret_cast<const uint32_t*>(in), unpack_buffer,
                                                              size, BIT_WIDTH);
#else
            int num_unpacked =
                    arrow::internal::unpack32(reinterpret_cast<const uint32_t*>(in), unpack_buffer, size, BIT_WIDTH);
#endif
            DCHECK(num_unpacked == size);
            for (int k = 0; k < size; ++k) {
                out[decoded + k] = static_cast<OutType>(unpack_buffer[k]);
            }
            in += num_unpacked * BIT_WIDTH / byte_width;
            decoded += size;
        }
    }

    auto remainder_values = num_values % 32;
    if (remainder_values > 0) {
        out += batch_size;
        in_bytes -= batch_size * BIT_WIDTH / 8;
        in = starrocks::util::bitpacking_default::UnpackValues(BIT_WIDTH, in, in_bytes, remainder_values, out).first;
    }
    return in;
}

// the in_bytes is enough for num_values, that should be checked by caller.
template <typename OutType>
static const uint8_t* UnpackValues(int bit_width, const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
                                   OutType* __restrict__ out) {
#pragma push_macro("UNPACK_ARROW_VALUES_CASE")
#define UNPACK_ARROW_VALUES_CASE(ignore1, i, ignore2) \
    case i:                                           \
        return UnpackValues<OutType, i>(in, in_bytes, num_values, out);

    switch (bit_width) {
        // Expand cases from 0 to 64.
        BOOST_PP_REPEAT_FROM_TO(0, 65, UNPACK_ARROW_VALUES_CASE, ignore);
    default:
        DCHECK(false);
        return nullptr;
    }
#pragma pop_macro("UNPACK_ARROW_VALUES_CASE")
}

} // namespace starrocks::util::bitpacking_arrow
