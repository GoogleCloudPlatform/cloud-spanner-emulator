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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_CHUNKING_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_CHUNKING_H_

#include <vector>

#include "google/protobuf/struct.pb.h"
#include "google/spanner/v1/result_set.pb.h"
#include "zetasql/base/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

// Takes a ResultSet and chunks it into smaller pieces as necessary. Each
// resulting piece will have a size <= max_chunk_size. Returns an ordered list
// of PartialResultSets or an error.
zetasql_base::StatusOr<std::vector<google::spanner::v1::PartialResultSet>>
ChunkResultSet(const google::spanner::v1::ResultSet& set, int64_t max_chunk_size);

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_CHUNKING_H_
