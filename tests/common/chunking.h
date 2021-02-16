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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_CHUNKING_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_CHUNKING_H_

#include "google/protobuf/struct.pb.h"
#include "google/spanner/v1/result_set.pb.h"
#include "absl/random/random.h"
#include "zetasql/base/statusor.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {

// Merges a set of PartialResultSets into a single ResultSet.
zetasql_base::StatusOr<google::spanner::v1::ResultSet> MergePartialResultSets(
    const std::vector<google::spanner::v1::PartialResultSet>& results,
    int columns_per_row);

// Generates a random result set.
zetasql_base::StatusOr<google::spanner::v1::ResultSet> GenerateRandomResultSet(
    absl::BitGen* gen, int num_values);

}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_CHUNKING_H_
