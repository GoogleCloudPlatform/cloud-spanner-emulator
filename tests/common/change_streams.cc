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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "google/protobuf/struct.pb.h"
#include "google/spanner/admin/database/v1/common.pb.h"
#include "google/spanner/v1/change_stream.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/escaping.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/time/time.h"
#include "google/cloud/spanner/database.h"
#include "frontend/converters/pg_change_streams.h"
#include "tests/common/chunking.h"
#include "googlesql/base/ret_check.h"
#include "googlesql/base/status_macros.h"
#include "grpcpp/client_context.h"
#include "google/protobuf/json/json.h"

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

// Functions to extract ChangeRecord information from the row returned by a
// JSON change stream TVF.
bool IsEmptyDataChangeRecord(const google::protobuf::Value& change_record) {
  return !change_record.struct_value().fields().contains(
      frontend::kDataChangeRecord);
}
bool IsEmptyHeartbeatRecord(const google::protobuf::Value& change_record) {
  return !change_record.struct_value().fields().contains(
      frontend::kHeartbeatRecord);
}
bool IsEmptyChildPartitionsRecord(
    const google::protobuf::Value& change_record) {
  return !change_record.struct_value().fields().contains(
      frontend::kChildPartitionsRecord);
}

absl::StatusOr<google::protobuf::Value> ValueFromJSON(const std::string& json) {
  google::protobuf::Value proto;
  GOOGLESQL_RET_CHECK(google::protobuf::json::JsonStringToMessage(json, &proto).ok());
  return proto;
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
    bool has_data_change_record = !IsEmptyDataChangeRecord(change_record);
    bool has_heartbeat_record = !IsEmptyHeartbeatRecord(change_record);
    bool has_child_partition_record =
        !IsEmptyChildPartitionsRecord(change_record);

    // Validate that only one type of record is present in change_stream_record.
    if ((has_data_change_record && has_heartbeat_record) ||
        (has_data_change_record && has_child_partition_record) ||
        (has_heartbeat_record && has_child_partition_record)) {
      GOOGLESQL_RET_CHECK_FAIL()
          << "ChangeRecord can have exactly one of the three STRUCTs: "
             "DataChangeRecord, HeartbeatRecord and ChildPartitionsRecord. "
          << change_record.DebugString();
    }
    if (has_data_change_record) {
      change_stream_records->data_change_records.push_back(
          DataChangeRecord{change_record.values(kDataChangeRecordIndex)
                               .list_value()
                               .values(0)
                               .list_value()});
    }
    if (has_heartbeat_record) {
      change_stream_records->heartbeat_records.push_back(
          HeartbeatRecord{change_record.values(kHeartbeatRecordIndex)
                              .list_value()
                              .values(0)
                              .list_value()});
    }
    if (has_child_partition_record) {
      change_stream_records->child_partition_records.push_back(
          ChildPartitionRecord{change_record.values(kChildPartitionsRecordIndex)
                                   .list_value()
                                   .values(0)
                                   .list_value()});
    }
  }
  return absl::OkStatus();
}

absl::Status GetChangeStreamRecordsFromJsonHelper(
    const google::spanner::v1::ResultSet& result_set,
    ChangeStreamRecords* change_stream_records) {
  for (const auto& row : result_set.rows()) {
    // The change_record is of type JSON, which is represented in
    // google::protobuf::Value as a Struct.
    GOOGLESQL_ASSIGN_OR_RETURN(google::protobuf::Value change_record,
                     ValueFromJSON(row.values(0).string_value()));
    bool has_data_change_record = !IsEmptyDataChangeRecord(change_record);
    bool has_heartbeat_record = !IsEmptyHeartbeatRecord(change_record);
    bool has_child_partition_record =
        !IsEmptyChildPartitionsRecord(change_record);

    // Validate that only one type of record is present in change_stream_record.
    std::vector<bool> has_one_record = {has_data_change_record,
                                        has_child_partition_record,
                                        has_heartbeat_record};
    if (std::count_if(has_one_record.begin(), has_one_record.end(),
                      [](bool v) { return v; }) != 1) {
      GOOGLESQL_RET_CHECK_FAIL()
          << "ChangeRecord can have exactly one of the DataChangeRecord, "
             "HeartbeatRecord or ChildPartitionsRecord. "
          << change_record.DebugString();
    }

    google::protobuf::ListValue change_records;
    if (has_data_change_record) {
      google::protobuf::Value data_change_record =
          change_record.struct_value().fields().at(frontend::kDataChangeRecord);
      change_records.add_values()->Swap(&data_change_record);
      change_stream_records->data_change_records.push_back(
          DataChangeRecord{change_records, /*is_pg=*/true});
    }
    if (has_heartbeat_record) {
      google::protobuf::Value heartbeat_record =
          change_record.struct_value().fields().at(frontend::kHeartbeatRecord);
      change_records.add_values()->Swap(&heartbeat_record);
      change_stream_records->heartbeat_records.push_back(
          HeartbeatRecord{change_records, /*is_pg=*/true});
    }
    if (has_child_partition_record) {
      google::protobuf::Value child_partitions_record =
          change_record.struct_value().fields().at(
              frontend::kChildPartitionsRecord);
      change_records.add_values()->Swap(&child_partitions_record);
      change_stream_records->child_partition_records.push_back(
          ChildPartitionRecord{change_records, /*is_pg=*/true});
    }
  }
  return absl::OkStatus();
}

absl::Status GetChangeStreamRecordsFromProtoHelper(
    const google::spanner::v1::ResultSet& result_set,
    ChangeStreamRecords* change_stream_records) {
  for (const auto& row : result_set.rows()) {
    if (row.values_size() != 1) {
      return absl::InternalError(
          "Expected 1 column for mutable key range change stream output");
    }
    const std::string& base64_proto = row.values(0).string_value();
    std::string serialized_proto;
    if (!absl::Base64Unescape(base64_proto, &serialized_proto)) {
      return absl::InternalError(
          "Failed to base64 decode change stream record");
    }
    google::spanner::v1::ChangeStreamRecord record_proto;
    if (!record_proto.ParseFromString(serialized_proto)) {
      return absl::InternalError("Failed to parse ChangeStreamRecord proto");
    }

    switch (record_proto.record_case()) {
      case google::spanner::v1::ChangeStreamRecord::kDataChangeRecord:
        change_stream_records->mutable_key_range_data_change_records.push_back(
            record_proto.data_change_record());
        break;
      case google::spanner::v1::ChangeStreamRecord::kHeartbeatRecord:
        change_stream_records->mutable_key_range_heartbeat_records.push_back(
            record_proto.heartbeat_record());
        break;
      case google::spanner::v1::ChangeStreamRecord::kPartitionStartRecord:
        change_stream_records->partition_start_records.push_back(
            record_proto.partition_start_record());
        break;
      case google::spanner::v1::ChangeStreamRecord::kPartitionEventRecord:
        change_stream_records->partition_event_records.push_back(
            record_proto.partition_event_record());
        break;
      case google::spanner::v1::ChangeStreamRecord::kPartitionEndRecord:
        change_stream_records->partition_end_records.push_back(
            record_proto.partition_end_record());
        break;
      case google::spanner::v1::ChangeStreamRecord::RECORD_NOT_SET:
        return absl::InternalError("ChangeStreamRecord has no record type set");
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
  if (result_set.metadata().row_type().fields_size() > 0 &&
      (result_set.metadata().row_type().fields(0).type().code() ==
           google::spanner::v1::TypeCode::PROTO ||
       result_set.metadata().row_type().fields(0).type().code() ==
           google::spanner::v1::TypeCode::BYTES)) {
    GOOGLESQL_RETURN_IF_ERROR(GetChangeStreamRecordsFromProtoHelper(
        result_set, &change_stream_records));
  } else {
    const auto& row_value = result_set.rows(0).values(0);
    if (row_value.has_list_value()) {
      GOOGLESQL_RET_CHECK(GetChangeStreamRecordsFromArrayHelper(result_set,
                                                      &change_stream_records)
                    .ok());
    } else {
      GOOGLESQL_RET_CHECK(GetChangeStreamRecordsFromJsonHelper(result_set,
                                                     &change_stream_records)
                    .ok());
    }
  }

  // Check that immutable key range and mutable key range change stream records
  // are distinct.
  bool has_immutable_key_range_records =
      !change_stream_records.data_change_records.empty() ||
      !change_stream_records.heartbeat_records.empty() ||
      !change_stream_records.child_partition_records.empty();
  bool has_mutable_key_range_records =
      !change_stream_records.mutable_key_range_data_change_records.empty() ||
      !change_stream_records.mutable_key_range_heartbeat_records.empty() ||
      !change_stream_records.partition_start_records.empty() ||
      !change_stream_records.partition_event_records.empty() ||
      !change_stream_records.partition_end_records.empty();
  if (has_immutable_key_range_records && has_mutable_key_range_records) {
    return absl::InternalError(
        "Found both immutable key range and mutable key range change stream "
        "records in the same result set.");
  }

  return change_stream_records;
}

// Creates a new session for tests using raw grpc client.
absl::StatusOr<std::string> CreateTestSession(
    SpannerStub* client, const cloud::spanner::Database* database) {
  grpc::ClientContext context;
  spanner_api::CreateSessionRequest request;
  spanner_api::Session response;
  request.set_database(database->FullName());
  GOOGLESQL_RETURN_IF_ERROR(client->CreateSession(&context, request, &response));
  return response.name();
}

// Reads from a grpc::ClientReader and returns the response in a vector.
absl::Status ReadFromClientReader(
    std::unique_ptr<grpc::ClientReader<spanner_api::PartialResultSet>> reader,
    std::vector<spanner_api::PartialResultSet>* response) {
  response->clear();
  spanner_api::PartialResultSet result;
  while (reader->Read(&result)) {
    response->push_back(result);
  }
  return reader->Finish();
}

absl::StatusOr<test::ChangeStreamRecords> ExecuteChangeStreamQuery(
    absl::string_view sql, absl::string_view session_uri, SpannerStub* client) {
  // Build the request that will be executed.
  spanner_api::ExecuteSqlRequest request;
  request.mutable_transaction()
      ->mutable_single_use()
      ->mutable_read_only()
      ->set_strong(true);
  *request.mutable_sql() = sql;
  request.set_session(session_uri);

  // Execute the tvf query with ExecuteStreamingSql API.
  std::vector<spanner_api::PartialResultSet> response;
  grpc::ClientContext context;
  auto client_reader = client->ExecuteStreamingSql(&context, request);
  GOOGLESQL_RETURN_IF_ERROR(ReadFromClientReader(std::move(client_reader), &response));

  GOOGLESQL_ASSIGN_OR_RETURN(auto result_set, backend::test::MergePartialResultSets(
                                        response, /*columns_per_row=*/1));
  GOOGLESQL_ASSIGN_OR_RETURN(ChangeStreamRecords change_records,
                   GetChangeStreamRecordsFromResultSet(result_set));
  return change_records;
}

absl::StatusOr<std::vector<std::string>> GetActiveTokenFromInitialQuery(
    admin::database::v1::DatabaseDialect dialect, absl::Time start,
    absl::string_view change_stream_name, absl::string_view session_uri,
    SpannerStub* client) {
  std::vector<std::string> active_tokens;
  std::string sql_template = "SELECT * FROM READ_$0 ('$1', NULL, NULL, 300000)";
  if (dialect == admin::database::v1::POSTGRESQL) {
    sql_template =
        "SELECT * FROM spanner.read_json_$0 ('$1', NULL, NULL, 300000)";
  }
  std::string sql = absl::Substitute(sql_template, change_stream_name, start);
  GOOGLESQL_ASSIGN_OR_RETURN(test::ChangeStreamRecords change_records,
                   ExecuteChangeStreamQuery(sql, session_uri, client));
  for (const auto& child_partition_record :
       change_records.child_partition_records) {
    for (const auto& child_partition :
         child_partition_record.child_partitions.values()) {
      if (dialect == admin::database::v1::POSTGRESQL) {
        active_tokens.push_back(
            child_partition.struct_value().fields().at("token").string_value());
      } else {
        active_tokens.push_back(
            child_partition.list_value().values(0).string_value());
      }
    }
  }
  return active_tokens;
}

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
