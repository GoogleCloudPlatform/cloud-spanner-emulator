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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_COMMON_BIT_REVERSE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_COMMON_BIT_REVERSE_H_

#include <cstdint>

// This function takes in an INT64 value and returns its bit-reversed
// version. The returned value is also INT64.
//
// If `preserve_sign` is true, the function preserves the sign bit (MSB), so
// that the bit-reversed output has the same sign as `input`. I.e. a positive
// `input` produces positive a output, while a negative `input` produces a
// negative output.
//
// If `preserve_sign` is false, all bits of `input` are reversed.
int64_t BitReverse(int64_t input, bool preserve_sign);

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_COMMON_BIT_REVERSE_H_
