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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_PARTITION_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_PARTITION_H_

#include "zetasql/base/statusor.h"
#include "frontend/proto/partition_token.pb.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

// Converts a partition token into a byte string.
zetasql_base::StatusOr<std::string> PartitionTokenToString(
    const PartitionToken& partition_token);

// Converts a byte string into a partition token.
zetasql_base::StatusOr<PartitionToken> PartitionTokenFromString(
    const std::string& token);

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_PARTITION_H_
