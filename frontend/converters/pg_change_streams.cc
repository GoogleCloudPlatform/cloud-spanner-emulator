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

#include "frontend/converters/pg_change_streams.h"

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "google/spanner/v1/result_set.pb.h"
#include "google/spanner/v1/type.pb.h"
#include "zetasql/public/json_value.h"
#include "zetasql/public/value.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/time/time.h"
#include "backend/access/read.h"
#include "common/limits.h"
#include "frontend/converters/change_streams.h"
#include "frontend/converters/chunking.h"
#include "frontend/converters/values.h"
#include "nlohmann/json.hpp"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {
using JSON = ::nlohmann::json;
namespace {

std::string ToFragmentIdString(int64_t record_sequence) {
  return absl::StrFormat("%08d", record_sequence);
}

JSON CreateHeartbeatRecord(absl::Time timestamp) {
  JSON heartbeat_record;
  heartbeat_record[kTimestamp] = absl::FormatTime(timestamp);
  return heartbeat_record;
}

// The passed in cursor is guaranteed to have the shape of
// {start_time(TIMESTAMP), partition_token(STRING), parents(ARRAY<STRING>)}
// due to the fixed shape of change stream internal partition table.
JSON CreateChildPartitionRecord(
    backend::RowCursor* cursor, int64_t record_sequence,
    std::optional<absl::Time> initial_start_timestamp) {
  JSON child_partition_record;
  child_partition_record[kStartTimestamp] =
      initial_start_timestamp.has_value()
          ? absl::FormatTime(initial_start_timestamp.value())
          : absl::FormatTime(cursor->ColumnValue(0).ToTime());
  child_partition_record[kRecordSequence] = ToFragmentIdString(record_sequence);
  // If a split event happens during partition query, one child partition record
  // can contain up to two partition tokens after split. If initial_start_time
  // doesn't have value and there are more than one row contain in the partition
  // table row cursor, we know a split event happened and keep inserting the
  // remaining splitted partition tokens in current row cursor.
  JSON child_partitions = JSON::array();
  do {
    JSON parent_partition_tokens = JSON::array();
    zetasql::Value partition_token_val = cursor->ColumnValue(1);
    zetasql::Value parent_tokens_arr = cursor->ColumnValue(2);
    for (int i = 0; i < parent_tokens_arr.num_elements(); i++) {
      if (initial_start_timestamp.has_value()) break;
      parent_partition_tokens.push_back(
          parent_tokens_arr.element(i).string_value());
    }
    JSON child_partition;
    child_partition[kToken] = partition_token_val.string_value();
    child_partition[kParentPartitionTokens] = parent_partition_tokens;
    child_partitions.push_back(child_partition);
  } while (!initial_start_timestamp.has_value() && cursor->Next());
  child_partition_record[kChildPartitions] = child_partitions;
  return child_partition_record;
}

JSON CreateDataChangeRecord(backend::RowCursor* cursor) {
  // The passed in cursor is guaranteed to have the shape of
  // {"partition_token(STRING)",
  //  "commit_timestamp(TIMESTAMP)",
  //  "server_transaction_id(STRING)",
  //  "record_sequence(STRING)",
  //  "is_last_record_in_transaction_in_partition(BOOL)",
  //  "table_name(STRING)",
  //  "column_types_name(ARRAY<STRING>)",
  //  "column_types_type(ARRAY<STRING>)",
  //  "column_types_is_primary_key(ARRAY<BOOL>)"
  //  "column_types_ordinal_position(ARRAY<INT64>)"
  //  "mods_keys(ARRAY<STRING>)",
  //  "mods_new_values(ARRAY<STRING>)",
  //  "mods_old_values(ARRAY<STRING>)",
  //  "mod_type(STRING)",
  //  "value_capture_type(STRING)",
  //  "number_of_records_in_transaction(INT64)",
  //  "number_of_partitions_in_transaction(INT64)",
  //  "transaction_tag(STRING)",
  //  "is_system_transaction(BOOL)"},
  // due to the fixed shape of change stream internal data table.
  JSON data_change_record;
  data_change_record[kCommitTimestamp] =
      absl::FormatTime(cursor->ColumnValue(1).ToTime());
  // Swap the order of server transaction id and record sequence due to the
  // different table schema.
  data_change_record[kRecordSequence] = cursor->ColumnValue(3).string_value();
  data_change_record[kServerTransactionId] =
      cursor->ColumnValue(2).string_value();
  data_change_record[kIsLastRecordInTransactionInPartition] =
      cursor->ColumnValue(4).bool_value();
  data_change_record[kTableName] = cursor->ColumnValue(5).string_value();

  // Construct column types JSON as above:
  //  "column_types": [
  //   {
  //     "name": "BoolKeyCol",
  //     "type": {"code": "BOOL"},
  //     "is_primary_key": true,
  //     "ordinal_position": 1
  //   },...]
  JSON column_types = JSON::array();
  zetasql::Value column_types_name_arr = cursor->ColumnValue(6);
  zetasql::Value column_types_type_arr = cursor->ColumnValue(7);
  zetasql::Value column_types_is_primary_key = cursor->ColumnValue(8);
  zetasql::Value column_types_ordinal_position = cursor->ColumnValue(9);
  for (int i = 0; i < column_types_name_arr.num_elements(); i++) {
    JSON column_type;
    column_type[kName] = column_types_name_arr.element(i).string_value();
    column_type[kType] =
        JSON::parse(column_types_type_arr.element(i).string_value());
    column_type[kIsPrimaryKey] =
        column_types_is_primary_key.element(i).bool_value();
    column_type[kOrdinalPosition] =
        column_types_ordinal_position.element(i).int64_value();
    column_types.push_back(column_type);
  }
  data_change_record[kColumnTypes] = column_types;

  // Construct mods JSON as above:
  // "mods" : [ {
  //   "keys" : {"BoolKeyCol" : true},
  //   "new_values" : {
  //     "ArrayCol" : [ "ArrayCol1", "ArrayCol2" ],
  //     "FloatValue" : 2.434899388972327E307
  //   },
  //   "old_values" : {}
  // }... ]
  JSON mods = JSON::array();
  zetasql::Value mods_keys_arr = cursor->ColumnValue(10);
  zetasql::Value mods_new_values_arr = cursor->ColumnValue(11);
  zetasql::Value mods_old_values_arr = cursor->ColumnValue(12);
  for (int i = 0; i < mods_keys_arr.num_elements(); i++) {
    JSON mod;
    mod[kKeys] = JSON::parse(mods_keys_arr.element(i).string_value());
    mod[kNewValues] =
        JSON::parse(mods_new_values_arr.element(i).string_value());
    mod[kOldValues] =
        JSON::parse(mods_old_values_arr.element(i).string_value());
    mods.push_back(mod);
  }
  data_change_record[kMods] = mods;

  data_change_record[kModType] = cursor->ColumnValue(13).string_value();
  data_change_record[kValueCaptureType] =
      cursor->ColumnValue(14).string_value();
  data_change_record[kNumberOfRecordsInTransaction] =
      cursor->ColumnValue(15).int64_value();
  data_change_record[kNumberOfPartitionsInTransaction] =
      cursor->ColumnValue(16).int64_value();
  data_change_record[kTransactionTag] = cursor->ColumnValue(17).string_value();
  data_change_record[kIsSystemTransaction] =
      cursor->ColumnValue(18).bool_value();

  return data_change_record;
}
absl::Status PopulateMetadata(
    std::vector<spanner_api::PartialResultSet>* responses,
    const std::string& tvf_name) {
  auto* result_metadata_pb = responses->at(0).mutable_metadata();
  auto* field_pb = result_metadata_pb->mutable_row_type()->add_fields();
  field_pb->set_name(tvf_name);
  field_pb->mutable_type()->set_code(google::spanner::v1::TypeCode::JSON);
  field_pb->mutable_type()->set_type_annotation(
      google::spanner::v1::TypeAnnotationCode::PG_JSONB);
  return absl::OkStatus();
}

void PopulateFakeResumeTokens(
    std::vector<spanner_api::PartialResultSet>* responses) {
  for (auto& response : *responses) {
    *response.mutable_resume_token() = kChangeStreamDummyResumeToken;
  }
}

}  // namespace

absl::StatusOr<std::vector<spanner_api::PartialResultSet>>
ConvertHeartbeatTimestampToJson(absl::Time timestamp,
                                const std::string& tvf_name,
                                bool expect_metadata) {
  spanner_api::ResultSet result_pb;
  auto* row_pb = result_pb.add_rows();
  JSON change_record;
  change_record[kHeartbeatRecord] = CreateHeartbeatRecord(timestamp);
  ZETASQL_ASSIGN_OR_RETURN(auto heartbeat_record_json_value,
                   zetasql::JSONValue::ParseJSONString(change_record.dump()));
  ZETASQL_ASSIGN_OR_RETURN(*row_pb->add_values(),
                   ValueToProto(zetasql::Value::Json(
                       std::move(heartbeat_record_json_value))));
  ZETASQL_ASSIGN_OR_RETURN(auto responses,
                   ChunkResultSet(result_pb, limits::kMaxStreamingChunkSize));
  if (expect_metadata) {
    ZETASQL_RETURN_IF_ERROR(PopulateMetadata(&responses, tvf_name));
  } else {
    responses.at(0).clear_metadata();
  }
  PopulateFakeResumeTokens(&responses);
  return responses;
}

absl::StatusOr<std::vector<spanner_api::PartialResultSet>>
ConvertPartitionTableRowCursorToJson(
    backend::RowCursor* row_cursor,
    std::optional<absl::Time> initial_start_time, const std::string& tvf_name,
    bool expect_metadata) {
  spanner_api::ResultSet result_pb;
  int64_t record_sequence = 0;
  while (row_cursor->Next()) {
    auto* row_pb = result_pb.add_rows();
    JSON change_record;
    change_record[kChildPartitionsRecord] = CreateChildPartitionRecord(
        row_cursor, record_sequence, initial_start_time);
    ZETASQL_ASSIGN_OR_RETURN(
        auto child_partitions_record_json_value,
        zetasql::JSONValue::ParseJSONString(change_record.dump()));
    ZETASQL_ASSIGN_OR_RETURN(*row_pb->add_values(),
                     ValueToProto(zetasql::Value::Json(
                         std::move(child_partitions_record_json_value))));
    record_sequence++;
  }
  ZETASQL_ASSIGN_OR_RETURN(auto responses,
                   ChunkResultSet(result_pb, limits::kMaxStreamingChunkSize));
  if (expect_metadata) {
    ZETASQL_RETURN_IF_ERROR(PopulateMetadata(&responses, tvf_name));
  } else {
    responses.at(0).clear_metadata();
  }
  PopulateFakeResumeTokens(&responses);
  return responses;
}

absl::StatusOr<std::vector<spanner_api::PartialResultSet>>
ConvertDataTableRowCursorToJson(backend::RowCursor* row_cursor,
                                const std::string& tvf_name,
                                bool expect_metadata) {
  spanner_api::ResultSet result_pb;
  while (row_cursor->Next()) {
    auto* row_pb = result_pb.add_rows();
    JSON change_record;
    change_record[kDataChangeRecord] = CreateDataChangeRecord(row_cursor);
    ZETASQL_ASSIGN_OR_RETURN(
        auto data_change_record_json_value,
        zetasql::JSONValue::ParseJSONString(change_record.dump()));
    ZETASQL_ASSIGN_OR_RETURN(*row_pb->add_values(),
                     ValueToProto(zetasql::Value::Json(
                         std::move(data_change_record_json_value))));
  }
  ZETASQL_ASSIGN_OR_RETURN(auto responses,
                   ChunkResultSet(result_pb, limits::kMaxStreamingChunkSize));
  if (expect_metadata) {
    ZETASQL_RETURN_IF_ERROR(PopulateMetadata(&responses, tvf_name));
  } else {
    responses.at(0).clear_metadata();
  }
  PopulateFakeResumeTokens(&responses);
  return responses;
}

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
