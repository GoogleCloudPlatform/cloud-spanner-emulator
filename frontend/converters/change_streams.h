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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_CHANGE_STREAMS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_CHANGE_STREAMS_H_

#include <optional>
#include <vector>

#include "google/spanner/v1/result_set.pb.h"
#include "backend/access/read.h"
namespace google {
namespace spanner {
namespace emulator {
namespace frontend {
namespace spanner_api = ::google::spanner::v1;

// Takes a row cursor for a change stream partition table, and convert the row
// cursor into a child partition record partial result set. If
// initial_start_time is not empty, current row cursor is yielded from initial
// change stream query. Start time of all change stream partition tokens will be
// set to the user passed start time in the metadata instead of the actual
// partition token start time in partition table.
absl::StatusOr<std::vector<spanner_api::PartialResultSet>>
ConvertChildPartitionRecordsToPartialResultSetProto(
    backend::RowCursor* row_cursor,
    std::optional<absl::Time> initial_start_time, bool expect_metadata = false);

// Takes a timestamp and convert the timestamp
// into a heartbeat record partial result set.
absl::StatusOr<std::vector<spanner_api::PartialResultSet>>
ConvertHeartbeatTimestampToPartialResultSetProto(absl::Time timestamp,
                                                 bool expect_metadata = false);

// Takes a row cursor from data table and convert all rows into a vector of
// partial result set.
absl::StatusOr<std::vector<spanner_api::PartialResultSet>>
ConvertDataTableRowCursorToPartialResultSetProto(backend::RowCursor* row_cursor,
                                                 bool expect_metadata = false);

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_CHANGE_STREAMS_H_
