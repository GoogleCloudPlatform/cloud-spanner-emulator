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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_COMMON_UTILS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_COMMON_UTILS_H_

#include <cstdint>

#include "absl/strings/string_view.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Parses the retention period provided in the DDL statement and the returned
// time is in seconds. Return -1 if the retention period is invalid.
int64_t ParseSchemaTimeSpec(absl::string_view spec);
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_COMMON_UTILS_H_
