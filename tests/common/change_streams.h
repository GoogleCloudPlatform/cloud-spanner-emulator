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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_CHANGE_STREAMS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_CHANGE_STREAMS_H_
#include <memory>
#include <string>
#include <vector>

#include "google/protobuf/struct.pb.h"
#include "google/spanner/v1/result_set.pb.h"
#include "google/spanner/v1/spanner.grpc.pb.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "google/cloud/spanner/database.h"
#include "frontend/converters/pg_change_streams.h"
#include "grpcpp/support/sync_stream.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {
namespace spanner_api = ::google::spanner::v1;
using SpannerStub = v1::Spanner::Stub;

struct DataChangeRecord {
  google::protobuf::Value commit_timestamp;
  google::protobuf::Value record_sequence;
  google::protobuf::Value server_transaction_id;
  google::protobuf::Value is_last_record_in_transaction_in_partition;
  google::protobuf::Value table_name;
  google::protobuf::ListValue column_types;
  google::protobuf::ListValue mods;
  google::protobuf::Value mod_type;
  google::protobuf::Value value_capture_type;
  google::protobuf::Value number_of_records_in_transaction;
  google::protobuf::Value number_of_partitions_in_transaction;
  google::protobuf::Value transaction_tag;
  google::protobuf::Value is_system_transaction;
  explicit DataChangeRecord(const google::protobuf::ListValue& list_values,
                            bool is_pg = false) {
    if (is_pg) {
      commit_timestamp = list_values.values(0).struct_value().fields().at(
          frontend::kCommitTimestamp);
      record_sequence = list_values.values(0).struct_value().fields().at(
          frontend::kRecordSequence);
      server_transaction_id = list_values.values(0).struct_value().fields().at(
          frontend::kServerTransactionId);
      is_last_record_in_transaction_in_partition =
          list_values.values(0).struct_value().fields().at(
              frontend::kIsLastRecordInTransactionInPartition);
      table_name = list_values.values(0).struct_value().fields().at(
          frontend::kTableName);
      column_types = list_values.values(0)
                         .struct_value()
                         .fields()
                         .at(frontend::kColumnTypes)
                         .list_value();
      mods = list_values.values(0)
                 .struct_value()
                 .fields()
                 .at(frontend::kMods)
                 .list_value();
      mod_type =
          list_values.values(0).struct_value().fields().at(frontend::kModType);
      value_capture_type = list_values.values(0).struct_value().fields().at(
          frontend::kValueCaptureType);
      number_of_records_in_transaction =
          list_values.values(0).struct_value().fields().at(
              frontend::kNumberOfRecordsInTransaction);
      number_of_partitions_in_transaction =
          list_values.values(0).struct_value().fields().at(
              frontend::kNumberOfPartitionsInTransaction);
      transaction_tag = list_values.values(0).struct_value().fields().at(
          frontend::kTransactionTag);
      is_system_transaction = list_values.values(0).struct_value().fields().at(
          frontend::kIsSystemTransaction);
    } else {
      commit_timestamp = list_values.values(0);
      record_sequence = list_values.values(1);
      server_transaction_id = list_values.values(2);
      is_last_record_in_transaction_in_partition = list_values.values(3);
      table_name = list_values.values(4);
      column_types = list_values.values(5).list_value();
      mods = list_values.values(6).list_value();
      mod_type = list_values.values(7);
      value_capture_type = list_values.values(8);
      number_of_records_in_transaction = list_values.values(9);
      number_of_partitions_in_transaction = list_values.values(10);
      transaction_tag = list_values.values(11);
      is_system_transaction = list_values.values(12);
    }
  }
};

struct HeartbeatRecord {
  google::protobuf::Value timestamp;
  explicit HeartbeatRecord(const google::protobuf::ListValue& list_values,
                           bool is_pg = false) {
    if (is_pg) {
      timestamp = list_values.values(0).struct_value().fields().at(
          frontend::kTimestamp);
    } else {
      timestamp = list_values.values(0);
    }
  }
};

struct ChildPartitionRecord {
  google::protobuf::Value start_time;
  google::protobuf::Value record_sequence;
  google::protobuf::ListValue child_partitions;
  explicit ChildPartitionRecord(const google::protobuf::ListValue& list_values,
                                bool is_pg = false) {
    if (is_pg) {
      start_time = list_values.values(0).struct_value().fields().at(
          frontend::kStartTimestamp);
      record_sequence = list_values.values(0).struct_value().fields().at(
          frontend::kRecordSequence);
      child_partitions = list_values.values(0)
                             .struct_value()
                             .fields()
                             .at(frontend::kChildPartitions)
                             .list_value();
    } else {
      start_time = list_values.values(0);
      record_sequence = list_values.values(1);
      child_partitions = list_values.values(2).list_value();
    }
  }
};

struct ChangeStreamRecords {
  std::vector<DataChangeRecord> data_change_records;
  std::vector<HeartbeatRecord> heartbeat_records;
  std::vector<ChildPartitionRecord> child_partition_records;
};

absl::StatusOr<ChangeStreamRecords> GetChangeStreamRecordsFromResultSet(
    const google::spanner::v1::ResultSet& result_set);

inline std::string EncodeTimestampString(absl::Time timestamp,
                                         bool is_pg = false) {
  if (is_pg) {
    return absl::StrCat(
        absl::FormatTime(absl::RFC3339_full, timestamp, absl::LocalTimeZone()));
  }
  constexpr char kRFC3339TimeFormatNoOffset[] = "%E4Y-%m-%dT%H:%M:%E*S";
  return absl::StrCat(absl::FormatTime(kRFC3339TimeFormatNoOffset, timestamp,
                                       absl::UTCTimeZone()),
                      "Z");
}
absl::StatusOr<std::string> CreateTestSession(
    SpannerStub* client, const cloud::spanner::Database* database);

absl::Status ReadFromClientReader(
    std::unique_ptr<grpc::ClientReader<spanner_api::PartialResultSet>> reader,
    std::vector<spanner_api::PartialResultSet>* response);

absl::StatusOr<test::ChangeStreamRecords> ExecuteChangeStreamQuery(
    absl::string_view sql, absl::string_view session_uri, SpannerStub* client);

absl::StatusOr<std::vector<std::string>> GetActiveTokenFromInitialQuery(
    absl::Time start, absl::string_view change_stream_name,
    absl::string_view session_uri, SpannerStub* client);

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_CHANGE_STREAMS_H_
