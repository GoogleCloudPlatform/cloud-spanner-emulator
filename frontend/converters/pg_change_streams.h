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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_PG_CHANGE_STREAMS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_PG_CHANGE_STREAMS_H_

#include <optional>
#include <string>
#include <vector>

#include "google/spanner/v1/result_set.pb.h"
#include "absl/status/statusor.h"
#include "absl/time/time.h"
#include "backend/access/read.h"
#include "nlohmann/json.hpp"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {
namespace spanner_api = ::google::spanner::v1;

using JSON = ::nlohmann::json;
// Field names of the change stream TVF output. See
// https://cloud.google.com/spanner/docs/change-streams/details#change_streams_record_format
// for the output format of the change stream TVF.
// Fields for data change record json object:
static constexpr char kDataChangeRecord[] = "data_change_record";
static constexpr char kCommitTimestamp[] = "commit_timestamp";
static constexpr char kRecordSequence[] = "record_sequence";
static constexpr char kServerTransactionId[] = "server_transaction_id";
static constexpr char kIsLastRecordInTransactionInPartition[] =
    "is_last_record_in_transaction_in_partition";
static constexpr char kTableName[] = "table_name";
static constexpr char kColumnTypes[] = "column_types";
static constexpr char kName[] = "name";
static constexpr char kType[] = "type";
static constexpr char kIsPrimaryKey[] = "is_primary_key";
static constexpr char kOrdinalPosition[] = "ordinal_position";
static constexpr char kMods[] = "mods";
static constexpr char kKeys[] = "keys";
static constexpr char kNewValues[] = "new_values";
static constexpr char kOldValues[] = "old_values";
static constexpr char kModType[] = "mod_type";
static constexpr char kValueCaptureType[] = "value_capture_type";
static constexpr char kNumberOfRecordsInTransaction[] =
    "number_of_records_in_transaction";
static constexpr char kNumberOfPartitionsInTransaction[] =
    "number_of_partitions_in_transaction";
static constexpr char kTransactionTag[] = "transaction_tag";
static constexpr char kIsSystemTransaction[] = "is_system_transaction";
// Fields for heartbeat record json object:
static constexpr char kHeartbeatRecord[] = "heartbeat_record";
static constexpr char kTimestamp[] = "timestamp";
// Fields for child partition record json object:
static constexpr char kChildPartitionsRecord[] = "child_partitions_record";
static constexpr char kStartTimestamp[] = "start_timestamp";
static constexpr char kChildPartitions[] = "child_partitions";
static constexpr char kToken[] = "token";
static constexpr char kParentPartitionTokens[] = "parent_partition_tokens";

// Takes a timestamp and convert the timestamp into a heartbeat record partial
// result set as JSON.
absl::StatusOr<std::vector<spanner_api::PartialResultSet>>
ConvertHeartbeatTimestampToJson(absl::Time timestamp,
                                const std::string& tvf_name,
                                bool expect_metadata = false);

// Takes a row cursor for a change stream partition table, and convert the row
// cursor into a child partition record partial result set as JSON. If
// initial_start_time is not empty, current row cursor is yielded from initial
// change stream query. Start time of all change stream partition tokens will be
// set to the user passed start time in the metadata instead of the actual
// partition token start time in partition table.
absl::StatusOr<std::vector<spanner_api::PartialResultSet>>
ConvertPartitionTableRowCursorToJson(
    backend::RowCursor* row_cursor,
    std::optional<absl::Time> initial_start_time, const std::string& tvf_name,
    bool expect_metadata = false);

// Takes a row cursor from data table and convert all rows into partial result
// set as JSON.
absl::StatusOr<std::vector<spanner_api::PartialResultSet>>
ConvertDataTableRowCursorToJson(backend::RowCursor* row_cursor,
                                const std::string& tvf_name,
                                bool expect_metadata = false);

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_PG_CHANGE_STREAMS_H_
