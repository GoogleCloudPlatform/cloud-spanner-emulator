//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "common/bit_reverse.h"

#include <cstdint>

#include "zetasql/base/bits.h"

int64_t BitReverse(int64_t input, bool preserve_sign) {
  if (input == 0) {
    return 0;
  }
  // Explicitly cast to uint64_t here, as zetasql_base::Bits::ReverseBits64() receives
  // an uint64_t input, and for easier bit manipulation.
  uint64_t value = zetasql_base::Bits::ReverseBits64(static_cast<uint64_t>(input));
  if (preserve_sign) {
    // The sign bit is now the least significant bit, take it and shift
    // to the furthest left.
    uint64_t sign_bit = (value & 1) << 63;
    //  Shift the rest one bit to the right
    value = value >> 1;

    // Put the sign bit back in:
    return static_cast<int64_t>(sign_bit | value);
  }

  return static_cast<int64_t>(value);
}
