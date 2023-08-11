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
#include <string>
#include <vector>

#include "google/protobuf/struct.pb.h"
#include "google/spanner/v1/result_set.pb.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

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
  explicit DataChangeRecord(const google::protobuf::ListValue& list_values) {
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
};

struct HeartbeatRecord {
  google::protobuf::Value timestamp;
  explicit HeartbeatRecord(const google::protobuf::ListValue& list_values) {
    timestamp = list_values.values(0);
  }
};

struct ChildPartitionRecord {
  google::protobuf::Value start_time;
  google::protobuf::Value record_sequence;
  google::protobuf::ListValue child_partitions;
  explicit ChildPartitionRecord(
      const google::protobuf::ListValue& list_values) {
    start_time = list_values.values(0);
    record_sequence = list_values.values(1);
    child_partitions = list_values.values(2).list_value();
  }
};

struct ChangeStreamRecords {
  std::vector<DataChangeRecord> data_change_records;
  std::vector<HeartbeatRecord> heartbeat_records;
  std::vector<ChildPartitionRecord> child_partition_records;
};

absl::StatusOr<ChangeStreamRecords> GetChangeStreamRecordsFromResultSet(
    const google::spanner::v1::ResultSet& result_set);

inline std::string EncodeTimestampString(absl::Time timestamp) {
  constexpr char kRFC3339TimeFormatNoOffset[] = "%E4Y-%m-%dT%H:%M:%E*S";
  return absl::StrCat(absl::FormatTime(kRFC3339TimeFormatNoOffset, timestamp,
                                       absl::UTCTimeZone()),
                      "Z");
}

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_CHANGE_STREAMS_H_
