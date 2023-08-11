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

#include "tests/common/change_streams.h"

#include "google/protobuf/struct.pb.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"
namespace google {
namespace spanner {
namespace emulator {
namespace test {
namespace {
// Indexes of nested structs inside the ChangeRecord that is returned from
// change stream ARRAY<STRUCT> TVF.
static constexpr int32_t kDataChangeRecordIndex = 0;
static constexpr int32_t kHeartbeatRecordIndex = 1;
static constexpr int32_t kChildPartitionsRecordIndex = 2;

void AppendToList(google::protobuf::ListValue* from, int start_index,
                  google::protobuf::ListValue* to) {
  for (int i = start_index; i < from->values_size(); i++) {
    to->add_values()->Swap(from->mutable_values(i));
  }
}

void MergeChunk(google::protobuf::ListValue* chunked_value,
                google::protobuf::ListValue* part_value) {
  if (part_value->values_size() == 0) return;

  if (chunked_value->values_size() == 0) {
    chunked_value->mutable_values()->Swap(part_value->mutable_values());
    return;
  }

  auto* value = chunked_value->mutable_values(chunked_value->values_size() - 1);
  if (value->kind_case() != part_value->values(0).kind_case()) {
    AppendToList(part_value, 0, chunked_value);
  } else if (part_value->values(0).kind_case() ==
             google::protobuf::Value::kListValue) {
    MergeChunk(value->mutable_list_value(),
               part_value->mutable_values(0)->mutable_list_value());
    AppendToList(part_value, 1, chunked_value);

  } else if (part_value->values(0).kind_case() ==
             google::protobuf::Value::kStringValue) {
    value->mutable_string_value()->append(part_value->values(0).string_value());
    AppendToList(part_value, 1, chunked_value);
  } else {
    AppendToList(part_value, 0, chunked_value);
  }
}

// Functions to extract ChangeRecord information from the row returned by an
// ARRAY<STRUCT> change stream TVF.
bool IsEmptyDataChangeRecord(const google::protobuf::ListValue& change_record) {
  return change_record.values(kDataChangeRecordIndex)
             .list_value()
             .values_size() == 0;
}

bool IsEmptyHeartbeatRecord(const google::protobuf::ListValue& change_record) {
  return change_record.values(kHeartbeatRecordIndex)
             .list_value()
             .values_size() == 0;
}

bool IsEmptyChildPartitionsRecord(
    const google::protobuf::ListValue& change_record) {
  return change_record.values(kChildPartitionsRecordIndex)
             .list_value()
             .values_size() == 0;
}

absl::Status GetChangeStreamRecordsFromArrayHelper(
    const google::spanner::v1::ResultSet& result_set,
    ChangeStreamRecords* change_stream_records) {
  for (const auto& row : result_set.rows()) {
    const auto& change_record =
        row.values(0).list_value().values(0).list_value();
    // The format of change_record is:
    // STRUCT<data_change_record STRUCT,
    //        heartbeat_record STRUCT,
    //        child_partitions_record STRUCT>
    bool data_change_record_is_present =
        !IsEmptyDataChangeRecord(change_record);
    bool heartbeat_record_is_present = !IsEmptyHeartbeatRecord(change_record);
    bool child_partitions_record_is_present =
        !IsEmptyChildPartitionsRecord(change_record);

    // Validate that only one type of record is present in change_stream_record.
    if ((data_change_record_is_present && heartbeat_record_is_present) ||
        (data_change_record_is_present && child_partitions_record_is_present) ||
        (heartbeat_record_is_present && child_partitions_record_is_present)) {
      ZETASQL_RET_CHECK_FAIL()
          << "ChangeRecord can have exactly one of the three STRUCTs: "
             "DataChangeRecord, HeartbeatRecord and ChildPartitionsRecord. "
          << change_record.DebugString();
    }
    if (data_change_record_is_present) {
      change_stream_records->data_change_records.push_back(
          DataChangeRecord{change_record.values(kDataChangeRecordIndex)
                               .list_value()
                               .values(0)
                               .list_value()});
    }
    if (heartbeat_record_is_present) {
      change_stream_records->heartbeat_records.push_back(
          HeartbeatRecord{change_record.values(kHeartbeatRecordIndex)
                              .list_value()
                              .values(0)
                              .list_value()});
    }
    if (child_partitions_record_is_present) {
      change_stream_records->child_partition_records.push_back(
          ChildPartitionRecord{change_record.values(kChildPartitionsRecordIndex)
                                   .list_value()
                                   .values(0)
                                   .list_value()});
    }
  }
  return absl::OkStatus();
}
}  // namespace

absl::StatusOr<ChangeStreamRecords> GetChangeStreamRecordsFromResultSet(
    const google::spanner::v1::ResultSet& result_set) {
  ChangeStreamRecords change_stream_records;
  if (result_set.rows().empty()) {
    return change_stream_records;
  }
  const auto& row_value = result_set.rows(0).values(0);
  ZETASQL_RET_CHECK(row_value.has_list_value());
  ZETASQL_RET_CHECK(
      GetChangeStreamRecordsFromArrayHelper(result_set, &change_stream_records)
          .ok());
  return change_stream_records;
}

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
