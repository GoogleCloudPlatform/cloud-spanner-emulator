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

#include <algorithm>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "google/spanner/v1/result_set.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/substitute.h"
#include "absl/time/time.h"
#include "google/cloud/spanner/bytes.h"
#include "google/cloud/spanner/keys.h"
#include "google/cloud/spanner/mutations.h"
#include "google/cloud/spanner/numeric.h"
#include "google/cloud/spanner/timestamp.h"
#include "common/clock.h"
#include "tests/common/change_streams.h"
#include "tests/conformance/common/database_test_base.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {
using InsertMutationBuilder = cloud::spanner::InsertMutationBuilder;
using UpdateMutationBuilder = cloud::spanner::UpdateMutationBuilder;
using DeleteMutationBuilder = cloud::spanner::DeleteMutationBuilder;
using ReplaceMutationBuilder = cloud::spanner::ReplaceMutationBuilder;
class ChangeStreamTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    ZETASQL_RETURN_IF_ERROR(SetSchema({
        R"(
          CREATE TABLE Accounts(
            AccountId     INT64 NOT NULL,
            Country       STRING(MAX),
            Year        INT64,
          ) PRIMARY KEY (AccountId)
        )",
        R"(
          CREATE TABLE Users(
            UserId     INT64 NOT NULL,
            Name       STRING(MAX),
            Age        INT64,
          ) PRIMARY KEY (UserId)
        )",
        R"(
          CREATE TABLE Users2(
            UserId     INT64 NOT NULL,
            Gender     STRING(MAX),
            Name       STRING(MAX),
            Age        INT64,
          ) PRIMARY KEY (UserId)
        )",
        R"(
          CREATE TABLE Relation(
            User1Id     INT64 NOT NULL,
            User2Id     INT64 NOT NULL,
            Description       STRING(MAX),
          ) PRIMARY KEY (User1Id, User2Id)
        )",
        R"(
          CREATE TABLE ScalarTypesTable (
            intVal INT64 NOT NULL,
            boolVal BOOL,
            bytesVal BYTES(MAX),
            dateVal DATE,
            float64Val FLOAT64,
            stringVal STRING(MAX),
            numericVal NUMERIC,
            timestampVal TIMESTAMP,
            jsonVal JSON,
            arrayInt ARRAY<INT64>,
            arrayStr ARRAY<STRING(MAX)>,
            arrayJson ARRAY<JSON>,
            float32Val FLOAT32,
          ) PRIMARY KEY(intVal)
        )",
        R"(
          CREATE TABLE ScalarTypesAllKeysTable (
            intVal INT64 NOT NULL,
            boolVal BOOL,
            bytesVal BYTES(MAX),
            dateVal DATE,
            float64Val FLOAT64,
            stringVal STRING(MAX),
            numericVal NUMERIC,
            timestampVal TIMESTAMP,
          ) PRIMARY KEY(intVal,boolVal,bytesVal,dateVal,float64Val,stringVal,numericVal,timestampVal)
        )",
        R"(
           CREATE TABLE ASTable (
                UserId INT64 NOT NULL,
                OtherUserId STRING(MAX) AS (JSON_VALUE(Data, "$.a.b")) STORED,
                Data JSON,
              ) PRIMARY KEY(UserId)
        )",
        R"(
          CREATE CHANGE STREAM StreamAll FOR ALL
        )",
        R"(
          CREATE CHANGE STREAM StreamUsers FOR Users OPTIONS (
          value_capture_type = 'NEW_VALUES' )
        )",
        R"(
          CREATE CHANGE STREAM StreamScalarTypes FOR ScalarTypesTable, ScalarTypesAllKeysTable
        )",
        R"(
          CREATE CHANGE STREAM StreamRelation FOR Relation
        )",
        R"(
          CREATE CHANGE STREAM StreamSpecifiedColumns FOR Users2(Name,Age)
        )",
    }));
    ZETASQL_ASSIGN_OR_RETURN(test_session_uri_,
                     CreateTestSession(raw_client(), database()));
    return absl::OkStatus();
  }

 protected:
  std::string test_session_uri_;

  absl::StatusOr<std::vector<DataChangeRecord>> GetDataRecordsFromStartToNow(
      absl::Time start, std::string change_stream_name) {
    ZETASQL_ASSIGN_OR_RETURN(std::vector<std::string> active_tokens,
                     GetActiveTokenFromInitialQuery(
                         database_api::GOOGLE_STANDARD_SQL, start,
                         change_stream_name, test_session_uri_, raw_client()));
    std::vector<DataChangeRecord> merged_data_change_records;
    // Ensure that end time is no less than start time.
    absl::Time end = std::max(Clock().Now(), start);
    for (const auto& partition_token : active_tokens) {
      std::string sql = absl::Substitute(
          "SELECT * FROM "
          "READ_$0 ('$1','$2', '$3', 300000 )",
          change_stream_name, start, end, partition_token);
      ZETASQL_ASSIGN_OR_RETURN(
          test::ChangeStreamRecords change_records,
          ExecuteChangeStreamQuery(sql, test_session_uri_, raw_client()));
      merged_data_change_records.insert(
          merged_data_change_records.end(),
          change_records.data_change_records.begin(),
          change_records.data_change_records.end());
    }
    return merged_data_change_records;
  }
};

TEST_F(ChangeStreamTest, SingleInsertVerifyDataChangeRecordContentNewValues) {
  auto mutation_builder_insert =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "name1"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_insert.Build()}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.commit_timestamp.string_value(),
            cloud::spanner_internal::TimestampToRFC3339(commit_timestamp));
  EXPECT_EQ(record.record_sequence.string_value(), "00000000");
  EXPECT_EQ(record.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  EXPECT_EQ(record.table_name.string_value(), "Users");
  EXPECT_EQ(record.mod_type.string_value(), "INSERT");
  EXPECT_EQ(record.value_capture_type.string_value(), "NEW_VALUES");
  EXPECT_EQ(record.number_of_records_in_transaction.string_value(), "1");
  EXPECT_EQ(record.number_of_partitions_in_transaction.string_value(), "1");
  EXPECT_EQ(record.transaction_tag.string_value(), "");
  EXPECT_EQ(record.is_system_transaction.bool_value(), false);
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Name" }
                           values { string_value: "{\"code\":\"STRING\"}" }
                           values { bool_value: false }
                           values { string_value: "2" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Age" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: false }
                           values { string_value: "3" }
                         }
                       })pb"));
  EXPECT_THAT(
      record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"1\"}" }
                   values { string_value: "{\"Age\":null,\"Name\":\"name1\"}" }
                   values { string_value: "{}" }
                 }
               })pb"));
}

TEST_F(ChangeStreamTest,
       SingleInsertVerifyDataChangeRecordContentOldAndNewValues) {
  auto mutation_builder_insert =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "name1"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_insert.Build()}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamAll"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.commit_timestamp.string_value(),
            cloud::spanner_internal::TimestampToRFC3339(commit_timestamp));
  EXPECT_EQ(record.record_sequence.string_value(), "00000000");
  EXPECT_EQ(record.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  EXPECT_EQ(record.table_name.string_value(), "Users");
  EXPECT_EQ(record.mod_type.string_value(), "INSERT");
  EXPECT_EQ(record.value_capture_type.string_value(), "OLD_AND_NEW_VALUES");
  EXPECT_EQ(record.number_of_records_in_transaction.string_value(), "1");
  EXPECT_EQ(record.number_of_partitions_in_transaction.string_value(), "1");
  EXPECT_EQ(record.transaction_tag.string_value(), "");
  EXPECT_EQ(record.is_system_transaction.bool_value(), false);
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Name" }
                           values { string_value: "{\"code\":\"STRING\"}" }
                           values { bool_value: false }
                           values { string_value: "2" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Age" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: false }
                           values { string_value: "3" }
                         }
                       })pb"));
  EXPECT_THAT(
      record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"1\"}" }
                   values { string_value: "{\"Age\":null,\"Name\":\"name1\"}" }
                   values { string_value: "{}" }
                 }
               })pb"));
}

TEST_F(ChangeStreamTest, SingleInsertVerifyDataChangeRecordContentNewRow) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"(
          ALTER CHANGE STREAM StreamUsers SET OPTIONS ( value_capture_type = 'NEW_ROW' )
        )",
  }));
  auto mutation_builder_insert =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "name1"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_insert.Build()}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.commit_timestamp.string_value(),
            cloud::spanner_internal::TimestampToRFC3339(commit_timestamp));
  EXPECT_EQ(record.record_sequence.string_value(), "00000000");
  EXPECT_EQ(record.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  EXPECT_EQ(record.table_name.string_value(), "Users");
  EXPECT_EQ(record.mod_type.string_value(), "INSERT");
  EXPECT_EQ(record.value_capture_type.string_value(), "NEW_ROW");
  EXPECT_EQ(record.number_of_records_in_transaction.string_value(), "1");
  EXPECT_EQ(record.number_of_partitions_in_transaction.string_value(), "1");
  EXPECT_EQ(record.transaction_tag.string_value(), "");
  EXPECT_EQ(record.is_system_transaction.bool_value(), false);
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Name" }
                           values { string_value: "{\"code\":\"STRING\"}" }
                           values { bool_value: false }
                           values { string_value: "2" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Age" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: false }
                           values { string_value: "3" }
                         }
                       })pb"));
  EXPECT_THAT(
      record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"1\"}" }
                   values { string_value: "{\"Age\":null,\"Name\":\"name1\"}" }
                   values { string_value: "{}" }
                 }
               })pb"));
}

TEST_F(ChangeStreamTest, SingleUpdateVerifyDataChangeRecordContentNewValues) {
  auto mutation_builder_insert =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "name1"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  ZETASQL_EXPECT_OK(Commit({mutation_builder_insert.Build()}));
  auto mutation_builder_update =
      UpdateMutationBuilder("Users", {"UserId", "Name"});
  rows = {{1, "name1Update"}};
  for (const auto& row : rows) {
    mutation_builder_update.AddRow(row);
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_update.Build()}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.commit_timestamp.string_value(),
            cloud::spanner_internal::TimestampToRFC3339(commit_timestamp));
  EXPECT_EQ(record.record_sequence.string_value(), "00000000");
  EXPECT_EQ(record.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  EXPECT_EQ(record.table_name.string_value(), "Users");
  EXPECT_EQ(record.mod_type.string_value(), "UPDATE");
  EXPECT_EQ(record.value_capture_type.string_value(), "NEW_VALUES");
  EXPECT_EQ(record.number_of_records_in_transaction.string_value(), "1");
  EXPECT_EQ(record.number_of_partitions_in_transaction.string_value(), "1");
  EXPECT_EQ(record.transaction_tag.string_value(), "");
  EXPECT_EQ(record.is_system_transaction.bool_value(), false);
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Name" }
                           values { string_value: "{\"code\":\"STRING\"}" }
                           values { bool_value: false }
                           values { string_value: "2" }
                         }
                       })pb"));
  EXPECT_THAT(record.mods,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "{\"UserId\":\"1\"}" }
                           values { string_value: "{\"Name\":\"name1Update\"}" }
                           values { string_value: "{}" }
                         }
                       })pb"));
}

TEST_F(ChangeStreamTest,
       SingleUpdateVerifyDataChangeRecordContentOldAndNewValues) {
  auto mutation_builder_insert =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "name1"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  ZETASQL_EXPECT_OK(Commit({mutation_builder_insert.Build()}));
  auto mutation_builder_update =
      UpdateMutationBuilder("Users", {"UserId", "Name"});
  rows = {{1, "name1Update"}};
  for (const auto& row : rows) {
    mutation_builder_update.AddRow(row);
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_update.Build()}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamAll"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.commit_timestamp.string_value(),
            cloud::spanner_internal::TimestampToRFC3339(commit_timestamp));
  EXPECT_EQ(record.record_sequence.string_value(), "00000000");
  EXPECT_EQ(record.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  EXPECT_EQ(record.table_name.string_value(), "Users");
  EXPECT_EQ(record.mod_type.string_value(), "UPDATE");
  EXPECT_EQ(record.value_capture_type.string_value(), "OLD_AND_NEW_VALUES");
  EXPECT_EQ(record.number_of_records_in_transaction.string_value(), "1");
  EXPECT_EQ(record.number_of_partitions_in_transaction.string_value(), "1");
  EXPECT_EQ(record.transaction_tag.string_value(), "");
  EXPECT_EQ(record.is_system_transaction.bool_value(), false);
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Name" }
                           values { string_value: "{\"code\":\"STRING\"}" }
                           values { bool_value: false }
                           values { string_value: "2" }
                         }
                       })pb"));
  EXPECT_THAT(record.mods,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "{\"UserId\":\"1\"}" }
                           values { string_value: "{\"Name\":\"name1Update\"}" }
                           values { string_value: "{\"Name\":\"name1\"}" }
                         }
                       })pb"));
}

TEST_F(ChangeStreamTest, SingleUpdateVerifyDataChangeRecordContentNewRow) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"(
          ALTER CHANGE STREAM StreamAll SET OPTIONS ( value_capture_type = 'NEW_ROW' )
        )",
  }));
  auto mutation_builder_insert =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "name1"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  ZETASQL_EXPECT_OK(Commit({mutation_builder_insert.Build()}));
  auto mutation_builder_update =
      UpdateMutationBuilder("Users", {"UserId", "Name"});
  rows = {{1, "name1Update"}};
  for (const auto& row : rows) {
    mutation_builder_update.AddRow(row);
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_update.Build()}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamAll"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.commit_timestamp.string_value(),
            cloud::spanner_internal::TimestampToRFC3339(commit_timestamp));
  EXPECT_EQ(record.record_sequence.string_value(), "00000000");
  EXPECT_EQ(record.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  EXPECT_EQ(record.table_name.string_value(), "Users");
  EXPECT_EQ(record.mod_type.string_value(), "UPDATE");
  EXPECT_EQ(record.value_capture_type.string_value(), "NEW_ROW");
  EXPECT_EQ(record.number_of_records_in_transaction.string_value(), "1");
  EXPECT_EQ(record.number_of_partitions_in_transaction.string_value(), "1");
  EXPECT_EQ(record.transaction_tag.string_value(), "");
  EXPECT_EQ(record.is_system_transaction.bool_value(), false);
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Name" }
                           values { string_value: "{\"code\":\"STRING\"}" }
                           values { bool_value: false }
                           values { string_value: "2" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Age" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: false }
                           values { string_value: "3" }
                         }
                       })pb"));
  EXPECT_THAT(
      record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"1\"}" }
                   values {
                     string_value: "{\"Age\":null,\"Name\":\"name1Update\"}"
                   }
                   values { string_value: "{}" }
                 }
               })pb"));
}

TEST_F(ChangeStreamTest,
       SingleUpdateVerifyDataChangeRecordContentNewRowAllColsPopulatedEarlier) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"(
          ALTER CHANGE STREAM StreamAll SET OPTIONS ( value_capture_type = 'NEW_ROW' )
        )",
  }));
  auto mutation_builder_insert =
      InsertMutationBuilder("Users", {"UserId", "Name", "Age"});
  std::vector<ValueRow> rows = {{1, "name1", "20"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  ZETASQL_EXPECT_OK(Commit({mutation_builder_insert.Build()}));
  auto mutation_builder_update =
      UpdateMutationBuilder("Users", {"UserId", "Name"});
  rows = {{1, "name1Update"}};
  for (const auto& row : rows) {
    mutation_builder_update.AddRow(row);
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_update.Build()}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamAll"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.commit_timestamp.string_value(),
            cloud::spanner_internal::TimestampToRFC3339(commit_timestamp));
  EXPECT_EQ(record.record_sequence.string_value(), "00000000");
  EXPECT_EQ(record.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  EXPECT_EQ(record.table_name.string_value(), "Users");
  EXPECT_EQ(record.mod_type.string_value(), "UPDATE");
  EXPECT_EQ(record.value_capture_type.string_value(), "NEW_ROW");
  EXPECT_EQ(record.number_of_records_in_transaction.string_value(), "1");
  EXPECT_EQ(record.number_of_partitions_in_transaction.string_value(), "1");
  EXPECT_EQ(record.transaction_tag.string_value(), "");
  EXPECT_EQ(record.is_system_transaction.bool_value(), false);
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Name" }
                           values { string_value: "{\"code\":\"STRING\"}" }
                           values { bool_value: false }
                           values { string_value: "2" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Age" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: false }
                           values { string_value: "3" }
                         }
                       })pb"));
  EXPECT_THAT(
      record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"1\"}" }
                   values {
                     string_value: "{\"Age\":\"20\",\"Name\":\"name1Update\"}"
                   }
                   values { string_value: "{}" }
                 }
               })pb"));
}

TEST_F(ChangeStreamTest, SingleDeleteVerifyDataChangeRecordContentNewValues) {
  auto mutation_builder_insert =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "name1"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  ZETASQL_EXPECT_OK(Commit({mutation_builder_insert.Build()}));
  // Delete an existing row generates a DELETE record
  auto mutation_builder_delete = DeleteMutationBuilder(
      "Users", KeySet().AddKey(cloud::spanner::MakeKey(1)));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_delete.Build()}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.commit_timestamp.string_value(),
            cloud::spanner_internal::TimestampToRFC3339(commit_timestamp));
  EXPECT_EQ(record.record_sequence.string_value(), "00000000");
  EXPECT_EQ(record.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  EXPECT_EQ(record.table_name.string_value(), "Users");
  EXPECT_EQ(record.mod_type.string_value(), "DELETE");
  EXPECT_EQ(record.value_capture_type.string_value(), "NEW_VALUES");
  EXPECT_EQ(record.number_of_records_in_transaction.string_value(), "1");
  EXPECT_EQ(record.number_of_partitions_in_transaction.string_value(), "1");
  EXPECT_EQ(record.transaction_tag.string_value(), "");
  EXPECT_EQ(record.is_system_transaction.bool_value(), false);
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Name" }
                           values { string_value: "{\"code\":\"STRING\"}" }
                           values { bool_value: false }
                           values { string_value: "2" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Age" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: false }
                           values { string_value: "3" }
                         }
                       })pb"));
  EXPECT_THAT(record.mods,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "{\"UserId\":\"1\"}" }
                           values { string_value: "{}" }
                           values { string_value: "{}" }
                         }
                       })pb"));
}

TEST_F(ChangeStreamTest,
       SingleDeleteVerifyDataChangeRecordContentOldAndNewValues) {
  auto mutation_builder_insert =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "name1"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  ZETASQL_EXPECT_OK(Commit({mutation_builder_insert.Build()}));
  // Delete an existing row generates a DELETE record
  auto mutation_builder_delete = DeleteMutationBuilder(
      "Users", KeySet().AddKey(cloud::spanner::MakeKey(1)));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_delete.Build()}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamAll"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.commit_timestamp.string_value(),
            cloud::spanner_internal::TimestampToRFC3339(commit_timestamp));
  EXPECT_EQ(record.record_sequence.string_value(), "00000000");
  EXPECT_EQ(record.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  EXPECT_EQ(record.table_name.string_value(), "Users");
  EXPECT_EQ(record.mod_type.string_value(), "DELETE");
  EXPECT_EQ(record.value_capture_type.string_value(), "OLD_AND_NEW_VALUES");
  EXPECT_EQ(record.number_of_records_in_transaction.string_value(), "1");
  EXPECT_EQ(record.number_of_partitions_in_transaction.string_value(), "1");
  EXPECT_EQ(record.transaction_tag.string_value(), "");
  EXPECT_EQ(record.is_system_transaction.bool_value(), false);
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Name" }
                           values { string_value: "{\"code\":\"STRING\"}" }
                           values { bool_value: false }
                           values { string_value: "2" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Age" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: false }
                           values { string_value: "3" }
                         }
                       })pb"));
  EXPECT_THAT(
      record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"1\"}" }
                   values { string_value: "{}" }
                   values { string_value: "{\"Age\":null,\"Name\":\"name1\"}" }
                 }
               })pb"));
}

TEST_F(ChangeStreamTest, SingleDeleteVerifyDataChangeRecordContentNewRow) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"(
          ALTER CHANGE STREAM StreamUsers SET OPTIONS ( value_capture_type = 'NEW_ROW' )
        )",
  }));
  auto mutation_builder_insert =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "name1"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  ZETASQL_EXPECT_OK(Commit({mutation_builder_insert.Build()}));
  // Delete an existing row generates a DELETE record
  auto mutation_builder_delete = DeleteMutationBuilder(
      "Users", KeySet().AddKey(cloud::spanner::MakeKey(1)));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_delete.Build()}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.commit_timestamp.string_value(),
            cloud::spanner_internal::TimestampToRFC3339(commit_timestamp));
  EXPECT_EQ(record.record_sequence.string_value(), "00000000");
  EXPECT_EQ(record.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  EXPECT_EQ(record.table_name.string_value(), "Users");
  EXPECT_EQ(record.mod_type.string_value(), "DELETE");
  EXPECT_EQ(record.value_capture_type.string_value(), "NEW_ROW");
  EXPECT_EQ(record.number_of_records_in_transaction.string_value(), "1");
  EXPECT_EQ(record.number_of_partitions_in_transaction.string_value(), "1");
  EXPECT_EQ(record.transaction_tag.string_value(), "");
  EXPECT_EQ(record.is_system_transaction.bool_value(), false);
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Name" }
                           values { string_value: "{\"code\":\"STRING\"}" }
                           values { bool_value: false }
                           values { string_value: "2" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Age" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: false }
                           values { string_value: "3" }
                         }
                       })pb"));
  EXPECT_THAT(record.mods,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "{\"UserId\":\"1\"}" }
                           values { string_value: "{}" }
                           values { string_value: "{}" }
                         }
                       })pb"));
}

TEST_F(ChangeStreamTest, InsertRowAlreadyExist) {
  auto mutation_builder_insert =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "Foo Bar"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  // 2nd transaction inserting to the same row
  commit_result = Commit({mutation_builder_insert.Build()});
  EXPECT_THAT(commit_result.status(),
              zetasql_base::testing::StatusIs(absl::StatusCode::kAlreadyExists));
}

TEST_F(ChangeStreamTest, DeleteRowNotExisting) {
  auto mutation_builder_delete = DeleteMutationBuilder(
      "Users", KeySet().AddKey(cloud::spanner::MakeKey(1)));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_delete.Build()}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 0);
}

// Check column_types and mods for different data types are correct.
TEST_F(ChangeStreamTest, DiffDataTypesInKey) {
  auto mutation_builder_insert_friends = InsertMutationBuilder(
      "ScalarTypesAllKeysTable",
      {"intVal", "boolVal", "bytesVal", "dateVal", "float64Val", "stringVal",
       "numericVal", "timestampVal"});
  std::vector<ValueRow> rows = {
      {1, true,
       cloud::spanner::Bytes(
           cloud::spanner_internal::BytesFromBase64("blue").value()),
       "2014-09-27", 1.1, "stringVal", cloud::spanner::MakeNumeric("1").value(),
       cloud::spanner::MakeTimestamp(absl::UnixEpoch()).value()}};
  for (const auto& row : rows) {
    mutation_builder_insert_friends.AddRow(row);
  }
  // 1 transaction
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert_friends.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  Timestamp commit_timestamp = commit_result->commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamScalarTypes"));
  EXPECT_EQ(data_change_records[0].table_name.string_value(),
            "ScalarTypesAllKeysTable");
  ASSERT_EQ(data_change_records.size(), 1);
  // column_types
  EXPECT_THAT(data_change_records[0].column_types, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "intVal" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: true }
                    values { string_value: "1" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "boolVal" }
                    values { string_value: "{\"code\":\"BOOL\"}" }
                    values { bool_value: true }
                    values { string_value: "2" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "bytesVal" }
                    values { string_value: "{\"code\":\"BYTES\"}" }
                    values { bool_value: true }
                    values { string_value: "3" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "dateVal" }
                    values { string_value: "{\"code\":\"DATE\"}" }
                    values { bool_value: true }
                    values { string_value: "4" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "float64Val" }
                    values { string_value: "{\"code\":\"FLOAT64\"}" }
                    values { bool_value: true }
                    values { string_value: "5" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "stringVal" }
                    values { string_value: "{\"code\":\"STRING\"}" }
                    values { bool_value: true }
                    values { string_value: "6" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "numericVal" }
                    values { string_value: "{\"code\":\"NUMERIC\"}" }
                    values { bool_value: true }
                    values { string_value: "7" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "timestampVal" }
                    values { string_value: "{\"code\":\"TIMESTAMP\"}" }
                    values { bool_value: true }
                    values { string_value: "8" }
                  }
                }
              )pb"));
  // mods
  EXPECT_THAT(
      data_change_records[0].mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values {
                     string_value: "{\"boolVal\":true,\"bytesVal\":\"blue\",\"dateVal\":\"2014-09-27\",\"float64Val\":1.1,\"intVal\":\"1\",\"numericVal\":\"1\",\"stringVal\":\"stringVal\",\"timestampVal\":\"1970-01-01T00:00:00Z\"}"
                   }
                   values { string_value: "{}" }
                   values { string_value: "{}" }
                 }
               })pb"));
}

// DML inserts all columns including those not explicitly mentioned in the
// statement. Null values for those columns will be populated into the
// DataChangeRecord.
TEST_F(ChangeStreamTest, SingleInsertDMLVerifyDataChangeRecordContent) {
  // Test ddl statement1: Even though only UserId is explicitly populated with a
  // value in the dml statement, all other tracked columns will be automatically
  // populated with null values.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto commit_result,
      CommitDml({SqlStatement("INSERT INTO Users (UserId) VALUES (1)")}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.commit_timestamp.string_value(),
            cloud::spanner_internal::TimestampToRFC3339(commit_timestamp));
  EXPECT_EQ(record.record_sequence.string_value(), "00000000");
  EXPECT_EQ(record.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  EXPECT_EQ(record.table_name.string_value(), "Users");
  EXPECT_EQ(record.mod_type.string_value(), "INSERT");
  EXPECT_EQ(record.value_capture_type.string_value(), "NEW_VALUES");
  EXPECT_EQ(record.number_of_records_in_transaction.string_value(), "1");
  EXPECT_EQ(record.number_of_partitions_in_transaction.string_value(), "1");
  EXPECT_EQ(record.transaction_tag.string_value(), "");
  EXPECT_EQ(record.is_system_transaction.bool_value(), false);
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Name" }
                           values { string_value: "{\"code\":\"STRING\"}" }
                           values { bool_value: false }
                           values { string_value: "2" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Age" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: false }
                           values { string_value: "3" }
                         }
                       })pb"));
  EXPECT_THAT(
      record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"1\"}" }
                   values { string_value: "{\"Age\":null,\"Name\":null}" }
                   values { string_value: "{}" }
                 }
               })pb"));
  // Test ddl statement2: Check the inserted string value doesn't have extra
  // quotes
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      commit_result,
      CommitDml({SqlStatement(
          "INSERT INTO Users (UserId, Name) VALUES (2, 'name2')")}));
  commit_timestamp = commit_result.commit_timestamp;
  query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 1);
  record = data_change_records[0];
  EXPECT_THAT(
      record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"2\"}" }
                   values { string_value: "{\"Age\":null,\"Name\":\"name2\"}" }
                   values { string_value: "{}" }
                 }
               })pb"));
  // Test ddl statement3: Check the inserted string key value doesn't have extra
  // quotes
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"(
          CREATE TABLE Singer(
            SingerId   INT64 NOT NULL,
            Name       STRING(MAX) NOT NULL,
            Age        INT64,
          ) PRIMARY KEY (SingerId, Name)
        )",
      R"(
          CREATE CHANGE STREAM StreamSinger FOR Singer OPTIONS (
          value_capture_type = 'NEW_VALUES' )
        )",
  }));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      commit_result,
      CommitDml({SqlStatement(
          "INSERT INTO Singer (SingerId, Name) VALUES (1, 'name1')")}));
  commit_timestamp = commit_result.commit_timestamp;
  query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamSinger"));
  ASSERT_EQ(data_change_records.size(), 1);
  record = data_change_records[0];
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "SingerId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Name" }
                           values { string_value: "{\"code\":\"STRING\"}" }
                           values { bool_value: true }
                           values { string_value: "2" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Age" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: false }
                           values { string_value: "3" }
                         }
                       })pb"));
  EXPECT_THAT(
      record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values {
                     string_value: "{\"Name\":\"name1\",\"SingerId\":\"1\"}"
                   }
                   values { string_value: "{\"Age\":null}" }
                   values { string_value: "{}" }
                 }
               })pb"));
}

// Columns not populated in DML are inserted with null values on emulator while
// not in production, so this test is disabled.
TEST_F(ChangeStreamTest, DISABLED_MultipleDMLVerifyDataChangeRecordContent) {
  // Insert a row with UserId only and then update the row with a age value.
  ZETASQL_EXPECT_OK(CommitDml({SqlStatement("INSERT INTO Users (UserId) VALUES (1)")}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto commit_result,
      CommitDml({SqlStatement("UPDATE Users SET Age = 20 WHERE UserId = 1")}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.commit_timestamp.string_value(),
            cloud::spanner_internal::TimestampToRFC3339(commit_timestamp));
  EXPECT_EQ(record.record_sequence.string_value(), "00000000");
  EXPECT_EQ(record.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  EXPECT_EQ(record.table_name.string_value(), "Users");
  EXPECT_EQ(record.mod_type.string_value(), "UPDATE");
  EXPECT_EQ(record.value_capture_type.string_value(), "NEW_VALUES");
  EXPECT_EQ(record.number_of_records_in_transaction.string_value(), "1");
  EXPECT_EQ(record.number_of_partitions_in_transaction.string_value(), "1");
  EXPECT_EQ(record.transaction_tag.string_value(), "");
  EXPECT_EQ(record.is_system_transaction.bool_value(), false);
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Age" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: false }
                           values { string_value: "3" }
                         }
                       })pb"));
  EXPECT_THAT(record.mods,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "{\"UserId\":\"1\"}" }
                           values { string_value: "{\"Age\":\"20\"}" }
                           values { string_value: "{}" }
                         }
                       })pb"));
}

// Generates 1 DataChangeRecord
TEST_F(ChangeStreamTest, MultiUpdateSameExistingRowSameTransactionNewValues) {
  // 1st transaction
  auto mutation_builder_insert =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "Foo Bar"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  // 2nd transaction
  auto mutation_builder_update =
      UpdateMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> update_rows = {{1, "Foo Bar Update1"},
                                       {1, "Foo Bar Update2"}};
  for (const auto& row : update_rows) {
    mutation_builder_update.AddRow(row);
  }
  absl::StatusOr<CommitResult> commit_result2 =
      Commit({mutation_builder_update.Build()});
  ZETASQL_EXPECT_OK(commit_result2.status());
  Timestamp commit_timestamp = commit_result2->commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.mod_type.string_value(), "UPDATE");
  // commit_timestamp
  EXPECT_EQ(record.commit_timestamp.string_value(),
            cloud::spanner_internal::TimestampToRFC3339(commit_timestamp));
  // record_sequence
  EXPECT_EQ(record.record_sequence.string_value(), "00000000");
  // is_last_record_in_transaction_in_partition
  EXPECT_EQ(record.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  // table_name
  EXPECT_EQ(record.table_name.string_value(), "Users");
  // mod_type
  EXPECT_EQ(record.mod_type.string_value(), "UPDATE");
  // value_capture_type
  EXPECT_EQ(record.value_capture_type.string_value(), "NEW_VALUES");
  // number_of_records_in_transaction
  EXPECT_EQ(record.number_of_records_in_transaction.string_value(), "1");
  // number_of_partitions_in_transaction
  EXPECT_EQ(record.number_of_partitions_in_transaction.string_value(), "1");
  // transaction_tag
  EXPECT_EQ(record.transaction_tag.string_value(), "");
  // is_system_transaction
  EXPECT_EQ(record.is_system_transaction.bool_value(), false);
  // column_types
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Name" }
                           values { string_value: "{\"code\":\"STRING\"}" }
                           values { bool_value: false }
                           values { string_value: "2" }
                         }
                       })pb"));
  // mods
  EXPECT_THAT(
      record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"1\"}" }
                   values { string_value: "{\"Name\":\"Foo Bar Update2\"}" }
                   values { string_value: "{}" }
                 }
               })pb"));
}

// Generates 1 DataChangeRecord
TEST_F(ChangeStreamTest,
       MultiUpdateSameExistingRowSameTransactionOldAndNewValues) {
  // 1st transaction
  auto mutation_builder_insert =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "Foo Bar"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  // 2nd transaction
  auto mutation_builder_update =
      UpdateMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> update_rows = {{1, "Foo Bar Update1"},
                                       {1, "Foo Bar Update2"}};
  for (const auto& row : update_rows) {
    mutation_builder_update.AddRow(row);
  }
  absl::StatusOr<CommitResult> commit_result2 =
      Commit({mutation_builder_update.Build()});
  ZETASQL_EXPECT_OK(commit_result2.status());
  Timestamp commit_timestamp = commit_result2->commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamAll"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.mod_type.string_value(), "UPDATE");
  // commit_timestamp
  EXPECT_EQ(record.commit_timestamp.string_value(),
            cloud::spanner_internal::TimestampToRFC3339(commit_timestamp));
  // record_sequence
  EXPECT_EQ(record.record_sequence.string_value(), "00000000");
  // is_last_record_in_transaction_in_partition
  EXPECT_EQ(record.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  // table_name
  EXPECT_EQ(record.table_name.string_value(), "Users");
  // mod_type
  EXPECT_EQ(record.mod_type.string_value(), "UPDATE");
  // value_capture_type
  EXPECT_EQ(record.value_capture_type.string_value(), "OLD_AND_NEW_VALUES");
  // number_of_records_in_transaction
  EXPECT_EQ(record.number_of_records_in_transaction.string_value(), "1");
  // number_of_partitions_in_transaction
  EXPECT_EQ(record.number_of_partitions_in_transaction.string_value(), "1");
  // transaction_tag
  EXPECT_EQ(record.transaction_tag.string_value(), "");
  // is_system_transaction
  EXPECT_EQ(record.is_system_transaction.bool_value(), false);
  // column_types
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Name" }
                           values { string_value: "{\"code\":\"STRING\"}" }
                           values { bool_value: false }
                           values { string_value: "2" }
                         }
                       })pb"));
  // mods
  EXPECT_THAT(
      record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"1\"}" }
                   values { string_value: "{\"Name\":\"Foo Bar Update2\"}" }
                   values { string_value: "{\"Name\":\"Foo Bar\"}" }
                 }
               })pb"));
}

// Generates 1 DataChangeRecord
TEST_F(ChangeStreamTest, MultiUpdateSameExistingRowSameTransactionNewRow) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"(
          ALTER CHANGE STREAM StreamUsers SET OPTIONS ( value_capture_type = 'NEW_ROW' )
        )",
  }));
  // 1st transaction
  auto mutation_builder_insert =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "Foo Bar"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  // 2nd transaction
  auto mutation_builder_update =
      UpdateMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> update_rows = {{1, "Foo Bar Update1"},
                                       {1, "Foo Bar Update2"}};
  for (const auto& row : update_rows) {
    mutation_builder_update.AddRow(row);
  }
  absl::StatusOr<CommitResult> commit_result2 =
      Commit({mutation_builder_update.Build()});
  ZETASQL_EXPECT_OK(commit_result2.status());
  Timestamp commit_timestamp = commit_result2->commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.mod_type.string_value(), "UPDATE");
  // commit_timestamp
  EXPECT_EQ(record.commit_timestamp.string_value(),
            cloud::spanner_internal::TimestampToRFC3339(commit_timestamp));
  // record_sequence
  EXPECT_EQ(record.record_sequence.string_value(), "00000000");
  // is_last_record_in_transaction_in_partition
  EXPECT_EQ(record.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  // table_name
  EXPECT_EQ(record.table_name.string_value(), "Users");
  // mod_type
  EXPECT_EQ(record.mod_type.string_value(), "UPDATE");
  // value_capture_type
  EXPECT_EQ(record.value_capture_type.string_value(), "NEW_ROW");
  // number_of_records_in_transaction
  EXPECT_EQ(record.number_of_records_in_transaction.string_value(), "1");
  // number_of_partitions_in_transaction
  EXPECT_EQ(record.number_of_partitions_in_transaction.string_value(), "1");
  // transaction_tag
  EXPECT_EQ(record.transaction_tag.string_value(), "");
  // is_system_transaction
  EXPECT_EQ(record.is_system_transaction.bool_value(), false);
  // column_types
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Name" }
                           values { string_value: "{\"code\":\"STRING\"}" }
                           values { bool_value: false }
                           values { string_value: "2" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Age" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: false }
                           values { string_value: "3" }
                         }
                       })pb"));
  // mods
  EXPECT_THAT(
      record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"1\"}" }
                   values {
                     string_value: "{\"Age\":null,\"Name\":\"Foo Bar Update2\"}"
                   }
                   values { string_value: "{}" }
                 }
               })pb"));
}

// Generate 1 INSERT DataChangeRecord
TEST_F(ChangeStreamTest, InsertUpdateSameRowSameTransaction) {
  auto mutation_builder_insert =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "Foo Bar Update"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }

  auto mutation_builder_update =
      UpdateMutationBuilder("Users", {"UserId", "Name"});
  rows = {{1, "Foo Bar Update1"}, {1, "Foo Bar Update2"}};
  for (const auto& row : rows) {
    mutation_builder_update.AddRow(row);
  }
  // Same transaction
  absl::StatusOr<CommitResult> commit_result = Commit(
      {mutation_builder_insert.Build(), mutation_builder_update.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  Timestamp commit_timestamp = commit_result->commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.table_name.string_value(), "Users");
  EXPECT_EQ(record.mod_type.string_value(), "INSERT");
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Name" }
                           values { string_value: "{\"code\":\"STRING\"}" }
                           values { bool_value: false }
                           values { string_value: "2" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Age" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: false }
                           values { string_value: "3" }
                         }
                       })pb"));
  EXPECT_THAT(
      record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"1\"}" }
                   values {
                     string_value: "{\"Age\":null,\"Name\":\"Foo Bar Update2\"}"
                   }
                   values { string_value: "{}" }
                 }
               })pb"));
}

// One insert mutation and one update mutation in the same transaction generate
// 2 DataChangeRecords.
TEST_F(ChangeStreamTest, InsertUpdateDiffRowSameTransaction) {
  // Insert one row as the existing row for the second transaction
  auto mutation_builder_insert_users =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "name1"}};
  for (const auto& row : rows) {
    mutation_builder_insert_users.AddRow(row);
  }
  // Commit the 1st transaction
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert_users.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  // Build the 2nd transaction
  auto mutation_builder_insert_users2 =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows2 = {{2, "name2"}};
  for (const auto& row : rows2) {
    mutation_builder_insert_users2.AddRow(row);
  }
  auto mutation_builder_update_users =
      UpdateMutationBuilder("Users", {"UserId", "Name"});
  rows = {{1, "name1update"}};
  for (const auto& row : rows) {
    mutation_builder_update_users.AddRow(row);
  }
  // Commit the 2nd transaction
  absl::StatusOr<CommitResult> commit_result2 =
      Commit({mutation_builder_insert_users2.Build(),
              mutation_builder_update_users.Build()});
  ZETASQL_EXPECT_OK(commit_result2.status());
  Timestamp commit_timestamp2 = commit_result2->commit_timestamp;
  absl::Time query_start_time2 = commit_timestamp2.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time2, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 2);
  absl::flat_hash_set<std::string> mod_types;
  for (const auto& record : data_change_records) {
    mod_types.insert(record.mod_type.string_value());
  }
  ASSERT_TRUE(mod_types.contains("INSERT"));
  ASSERT_TRUE(mod_types.contains("UPDATE"));
  DataChangeRecord insert_record =
      data_change_records[0].mod_type.string_value() == "INSERT"
          ? data_change_records[0]
          : data_change_records[1];
  DataChangeRecord update_record =
      data_change_records[1].mod_type.string_value() == "UPDATE"
          ? data_change_records[1]
          : data_change_records[0];
  EXPECT_EQ(insert_record.table_name.string_value(), "Users");
  EXPECT_EQ(update_record.table_name.string_value(), "Users");
  EXPECT_THAT(update_record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Name" }
                           values { string_value: "{\"code\":\"STRING\"}" }
                           values { bool_value: false }
                           values { string_value: "2" }
                         }
                       })pb"));
  EXPECT_THAT(update_record.mods,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "{\"UserId\":\"1\"}" }
                           values { string_value: "{\"Name\":\"name1update\"}" }
                           values { string_value: "{}" }
                         }
                       })pb"));
  EXPECT_THAT(insert_record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Name" }
                           values { string_value: "{\"code\":\"STRING\"}" }
                           values { bool_value: false }
                           values { string_value: "2" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Age" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: false }
                           values { string_value: "3" }
                         }
                       })pb"));
  EXPECT_THAT(
      insert_record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"2\"}" }
                   values { string_value: "{\"Age\":null,\"Name\":\"name2\"}" }
                   values { string_value: "{}" }
                 }
               })pb"));
}

// No DataChangeRecord is generated.
TEST_F(ChangeStreamTest, InsertDeleteSameRowSameTransaction) {
  auto mutation_builder_insert =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "Foo Bar Insert"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }

  auto mutation_builder_delete = DeleteMutationBuilder(
      "Users", KeySet().AddKey(cloud::spanner::MakeKey(1)));
  // 1 transaction
  absl::StatusOr<CommitResult> commit_result = Commit(
      {mutation_builder_insert.Build(), mutation_builder_delete.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  Timestamp commit_timestamp = commit_result->commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 0);
}

// No DataChangeRecord is generated.
TEST_F(ChangeStreamTest, InsertDeleteInsertDeleteSameRowSameTransaction) {
  auto mutation_builder_insert =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "name1"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }

  auto mutation_builder_delete = DeleteMutationBuilder(
      "Users", KeySet().AddKey(cloud::spanner::MakeKey(1)));
  auto mutation_builder_insert_2 =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  for (const auto& row : rows) {
    mutation_builder_insert_2.AddRow(row);
  }
  auto mutation_builder_delete_2 = DeleteMutationBuilder(
      "Users", KeySet().AddKey(cloud::spanner::MakeKey(1)));
  // 1 transaction
  absl::StatusOr<CommitResult> commit_result = Commit(
      {mutation_builder_insert.Build(), mutation_builder_delete.Build(),
       mutation_builder_insert_2.Build(), mutation_builder_delete_2.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  Timestamp commit_timestamp = commit_result->commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 0);
}

// No DataChangeRecord is generated.
TEST_F(ChangeStreamTest, InsertUpdateDeleteSameRowSameTransaction) {
  auto mutation_builder_insert =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "Foo Bar Insert"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }

  auto mutation_builder_update =
      UpdateMutationBuilder("Users", {"UserId", "Name"});
  rows = {{1, "Foo Bar Update1"}, {1, "Foo Bar Update2"}};
  for (const auto& row : rows) {
    mutation_builder_update.AddRow(row);
  }

  auto mutation_builder_delete = DeleteMutationBuilder(
      "Users", KeySet().AddKey(cloud::spanner::MakeKey(1)));
  // 1 transaction
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert.Build(), mutation_builder_update.Build(),
              mutation_builder_delete.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  Timestamp commit_timestamp = commit_result->commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 0);
}

// One INSERT DataChangeRecord is generated.
TEST_F(ChangeStreamTest, InsertDeleteInsertSameRowSameTransaction) {
  auto mutation_builder_insert =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "name1"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }

  auto mutation_builder_delete = DeleteMutationBuilder(
      "Users", KeySet().AddKey(cloud::spanner::MakeKey(1)));
  auto mutation_builder_insert_2 =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  for (const auto& row : rows) {
    mutation_builder_insert_2.AddRow(row);
  }
  // 1 transaction
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert.Build(), mutation_builder_delete.Build(),
              mutation_builder_insert_2.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  Timestamp commit_timestamp = commit_result->commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.mod_type.string_value(), "INSERT");
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Name" }
                           values { string_value: "{\"code\":\"STRING\"}" }
                           values { bool_value: false }
                           values { string_value: "2" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Age" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: false }
                           values { string_value: "3" }
                         }
                       })pb"));
  EXPECT_THAT(
      record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"1\"}" }
                   values { string_value: "{\"Age\":null,\"Name\":\"name1\"}" }
                   values { string_value: "{}" }
                 }
               })pb"));
}

// Only a DELETE DataChangeRecord is generated.
TEST_F(ChangeStreamTest, UpdateDeleteSameExistingRowSameTransaction) {
  // 1st transaction
  auto mutation_builder_insert =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{4, "Foo Bar Update0"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  Timestamp commit_timestamp = commit_result->commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();

  // 2nd transaction
  auto mutation_builder = UpdateMutationBuilder("Users", {"UserId", "Name"});
  rows = {{4, "Foo Bar Update1"}, {4, "Foo Bar Update2"}};
  for (const auto& row : rows) {
    mutation_builder.AddRow(row);
  }
  auto mutation_builder_delete = DeleteMutationBuilder(
      "Users", KeySet().AddKey(cloud::spanner::MakeKey(4)));
  ZETASQL_EXPECT_OK(
      Commit({mutation_builder.Build(), mutation_builder_delete.Build()}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 2);
  EXPECT_EQ(data_change_records[0].table_name.string_value(), "Users");
  EXPECT_EQ(data_change_records[0].mod_type.string_value(), "INSERT");
  EXPECT_EQ(data_change_records[0]
                .is_last_record_in_transaction_in_partition.bool_value(),
            true);
  EXPECT_EQ(data_change_records[1].mod_type.string_value(), "DELETE");

  DataChangeRecord insert_record = data_change_records[0];
  DataChangeRecord delete_record = data_change_records[1];
  EXPECT_EQ(insert_record.column_types.values_size(), 3);
  EXPECT_THAT(insert_record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Name" }
                           values { string_value: "{\"code\":\"STRING\"}" }
                           values { bool_value: false }
                           values { string_value: "2" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Age" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: false }
                           values { string_value: "3" }
                         }
                       })pb"));
  EXPECT_EQ(insert_record.mods.values_size(), 1);
  EXPECT_THAT(
      insert_record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"4\"}" }
                   values {
                     string_value: "{\"Age\":null,\"Name\":\"Foo Bar Update0\"}"
                   }
                   values { string_value: "{}" }
                 }
               })pb"));
  EXPECT_THAT(delete_record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Name" }
                           values { string_value: "{\"code\":\"STRING\"}" }
                           values { bool_value: false }
                           values { string_value: "2" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Age" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: false }
                           values { string_value: "3" }
                         }
                       })pb"));

  EXPECT_THAT(delete_record.mods,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "{\"UserId\":\"4\"}" }
                           values { string_value: "{}" }
                           values { string_value: "{}" }
                         }
                       })pb"));
}

TEST_F(ChangeStreamTest,
       InsertToTwoTablesTrackedBySameChangeStreamInSameTransaction) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"(
          CREATE CHANGE STREAM StreamAccountsUsers FOR Accounts,Users OPTIONS ( value_capture_type = 'NEW_VALUES' )
        )",
  }));
  auto mutation_builder_insert_users =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "name1"}};
  for (const auto& row : rows) {
    mutation_builder_insert_users.AddRow(row);
  }

  auto mutation_builder_insert_accounts =
      InsertMutationBuilder("Accounts", {"AccountId", "Country"});
  rows = {{1, "country1"}, {2, "country2"}};
  for (const auto& row : rows) {
    mutation_builder_insert_accounts.AddRow(row);
  }
  // Same transaction
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert_users.Build(),
              mutation_builder_insert_accounts.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  Timestamp commit_timestamp = commit_result->commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamAccountsUsers"));
  ASSERT_EQ(data_change_records.size(), 2);
  absl::flat_hash_set<std::string> tables_inserted;
  for (const auto& record : data_change_records) {
    tables_inserted.insert(record.table_name.string_value());
  }
  EXPECT_TRUE(tables_inserted.contains("Accounts"));
  EXPECT_TRUE(tables_inserted.contains("Users"));
  DataChangeRecord accounts_record =
      data_change_records[0].table_name.string_value() == "Accounts"
          ? data_change_records[0]
          : data_change_records[1];
  DataChangeRecord users_record =
      data_change_records[0].table_name.string_value() == "Users"
          ? data_change_records[0]
          : data_change_records[1];
  DataChangeRecord record1 = data_change_records[0];
  DataChangeRecord record2 = data_change_records[1];
  EXPECT_EQ(record1.record_sequence.string_value(), "00000000");
  EXPECT_EQ(record2.record_sequence.string_value(), "00000001");
  EXPECT_EQ(record1.is_last_record_in_transaction_in_partition.bool_value(),
            false);
  EXPECT_EQ(record2.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  EXPECT_THAT(accounts_record.column_types, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "AccountId" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: true }
                    values { string_value: "1" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Country" }
                    values { string_value: "{\"code\":\"STRING\"}" }
                    values { bool_value: false }
                    values { string_value: "2" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Year" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: false }
                    values { string_value: "3" }
                  }
                })pb"));
  EXPECT_THAT(
      accounts_record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"AccountId\":\"1\"}" }
                   values {
                     string_value: "{\"Country\":\"country1\",\"Year\":null}"
                   }
                   values { string_value: "{}" }
                 }
               }
               values {
                 list_value {
                   values { string_value: "{\"AccountId\":\"2\"}" }
                   values {
                     string_value: "{\"Country\":\"country2\",\"Year\":null}"
                   }
                   values { string_value: "{}" }
                 }
               })pb"));
  EXPECT_THAT(users_record.column_types, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "UserId" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: true }
                    values { string_value: "1" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Name" }
                    values { string_value: "{\"code\":\"STRING\"}" }
                    values { bool_value: false }
                    values { string_value: "2" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Age" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: false }
                    values { string_value: "3" }
                  }
                }
              )pb"));
  EXPECT_THAT(
      users_record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"1\"}" }
                   values { string_value: "{\"Age\":null,\"Name\":\"name1\"}" }
                   values { string_value: "{}" }
                 }
               })pb"));
}

TEST_F(ChangeStreamTest, InsertToDiffSetsTrackedColsDiffRowsSameTransaction) {
  auto mutation_builder_insert_users_name =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "name1"}};
  for (const auto& row : rows) {
    mutation_builder_insert_users_name.AddRow(row);
  }

  auto mutation_builder_insert_users_age =
      InsertMutationBuilder("Users", {"UserId", "Age"});
  rows = {{2, 20}};
  for (const auto& row : rows) {
    mutation_builder_insert_users_age.AddRow(row);
  }
  // Same transaction
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert_users_name.Build(),
              mutation_builder_insert_users_age.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  Timestamp commit_timestamp = commit_result->commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_THAT(record.column_types, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "UserId" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: true }
                    values { string_value: "1" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Name" }
                    values { string_value: "{\"code\":\"STRING\"}" }
                    values { bool_value: false }
                    values { string_value: "2" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Age" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: false }
                    values { string_value: "3" }
                  }
                }
              )pb"));
  EXPECT_THAT(
      record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"1\"}" }
                   values { string_value: "{\"Age\":null,\"Name\":\"name1\"}" }
                   values { string_value: "{}" }
                 }
               }
               values {
                 list_value {
                   values { string_value: "{\"UserId\":\"2\"}" }
                   values { string_value: "{\"Age\":\"20\",\"Name\":null}" }
                   values { string_value: "{}" }
                 }
               })pb"));
}

TEST_F(ChangeStreamTest, UpdateToDiffSetsTrackedColsSameRowsSameTransaction) {
  auto mutation_builder_insert_users =
      InsertMutationBuilder("Users", {"UserId", "Name", "Age"});
  std::vector<ValueRow> rows = {{1, "name1", 20}, {2, "name2", 20}};
  for (const auto& row : rows) {
    mutation_builder_insert_users.AddRow(row);
  }
  // First transaction
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert_users.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  // Build the 2nd transaction
  auto mutation_builder_update_users_age =
      UpdateMutationBuilder("Users", {"UserId", "Age"});
  rows = {{1, 22}};
  for (const auto& row : rows) {
    mutation_builder_update_users_age.AddRow(row);
  }
  auto mutation_builder_update_users_name =
      UpdateMutationBuilder("Users", {"UserId", "Name"});
  rows = {{1, "name1update"}};
  for (const auto& row : rows) {
    mutation_builder_update_users_name.AddRow(row);
  }
  // Same transaction
  commit_result = Commit({mutation_builder_update_users_age.Build(),
                          mutation_builder_update_users_name.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  Timestamp commit_timestamp = commit_result->commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsers"));
  EXPECT_EQ(data_change_records[0].table_name.string_value(), "Users");
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_THAT(record.column_types, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "UserId" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: true }
                    values { string_value: "1" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Name" }
                    values { string_value: "{\"code\":\"STRING\"}" }
                    values { bool_value: false }
                    values { string_value: "2" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Age" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: false }
                    values { string_value: "3" }
                  }
                }
              )pb"));
  EXPECT_THAT(
      record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"1\"}" }
                   values {
                     string_value: "{\"Age\":\"22\",\"Name\":\"name1update\"}"
                   }
                   values { string_value: "{}" }
                 }
               })pb"));
}

TEST_F(ChangeStreamTest, UpdateToDiffSetsTrackedColsDiffRowsSameTransaction) {
  auto mutation_builder_insert_users =
      InsertMutationBuilder("Users", {"UserId", "Name", "Age"});
  std::vector<ValueRow> rows = {{1, "name1", 20}, {2, "name2", 20}};
  for (const auto& row : rows) {
    mutation_builder_insert_users.AddRow(row);
  }
  // First transaction
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert_users.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  // Build the 2nd transaction
  auto mutation_builder_update_users_age =
      UpdateMutationBuilder("Users", {"UserId", "Age"});
  rows = {{1, 22}};
  for (const auto& row : rows) {
    mutation_builder_update_users_age.AddRow(row);
  }
  auto mutation_builder_update_users_name =
      UpdateMutationBuilder("Users", {"UserId", "Name"});
  rows = {{2, "name2update"}};
  for (const auto& row : rows) {
    mutation_builder_update_users_name.AddRow(row);
  }
  // Same transaction
  commit_result = Commit({mutation_builder_update_users_age.Build(),
                          mutation_builder_update_users_name.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  Timestamp commit_timestamp = commit_result->commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsers"));
  // EXPECT_EQ(data_change_records[0].table_name.string_value(), "Accounts");
  ASSERT_EQ(data_change_records.size(), 2);
  DataChangeRecord col_age_record =
      data_change_records[0]
                  .mods.values(0)
                  .list_value()
                  .values(1)
                  .string_value() == "{\"Age\":\"22\"}"
          ? data_change_records[0]
          : data_change_records[1];
  DataChangeRecord col_name_record =
      data_change_records[0]
                  .mods.values(0)
                  .list_value()
                  .values(1)
                  .string_value() == "{\"Name\":\"name1update\"}"
          ? data_change_records[0]
          : data_change_records[1];
  EXPECT_THAT(col_age_record.column_types, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "UserId" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: true }
                    values { string_value: "1" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Age" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: false }
                    values { string_value: "3" }
                  }
                }
              )pb"));
  EXPECT_THAT(col_name_record.column_types, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "UserId" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: true }
                    values { string_value: "1" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Name" }
                    values { string_value: "{\"code\":\"STRING\"}" }
                    values { bool_value: false }
                    values { string_value: "2" }
                  }
                }
              )pb"));
  EXPECT_THAT(col_name_record.mods,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "{\"UserId\":\"2\"}" }
                           values { string_value: "{\"Name\":\"name2update\"}" }
                           values { string_value: "{}" }
                         }
                       })pb"));
  EXPECT_THAT(col_age_record.mods,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "{\"UserId\":\"1\"}" }
                           values { string_value: "{\"Age\":\"22\"}" }
                           values { string_value: "{}" }
                         }
                       })pb"));
}

// If a column is tracked by 2 change streams, inserting into the column will
// generate 2 DataChangeRecords.
TEST_F(ChangeStreamTest, InsertSameColsTrackedByDiffChangeStreams) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"(
          CREATE CHANGE STREAM StreamUserAge FOR Users(Age) OPTIONS (
          value_capture_type = 'NEW_VALUES' )
        )",
  }));
  auto mutation_builder_insert_users =
      InsertMutationBuilder("Users", {"UserId", "Age"});
  std::vector<ValueRow> rows = {{1, 20}};
  for (const auto& row : rows) {
    mutation_builder_insert_users.AddRow(row);
  }
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert_users.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  Timestamp commit_timestamp = commit_result->commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  // 1 transaction
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records_user_age,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUserAge"));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records_users,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsers"));
  ASSERT_EQ(data_change_records_user_age.size(), 1);
  ASSERT_EQ(data_change_records_users.size(), 1);
  DataChangeRecord user_age_record = data_change_records_user_age[0];
  DataChangeRecord users_record = data_change_records_users[0];
  ASSERT_EQ(user_age_record.table_name.string_value(), "Users");
  ASSERT_EQ(users_record.table_name.string_value(), "Users");
  ASSERT_EQ(user_age_record.mod_type.string_value(), "INSERT");
  ASSERT_EQ(users_record.mod_type.string_value(), "INSERT");
  EXPECT_THAT(user_age_record.column_types, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "UserId" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: true }
                    values { string_value: "1" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Age" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: false }
                    values { string_value: "3" }
                  }
                })pb"));
  EXPECT_THAT(user_age_record.mods,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "{\"UserId\":\"1\"}" }
                           values { string_value: "{\"Age\":\"20\"}" }
                           values { string_value: "{}" }
                         }
                       })pb"));
  EXPECT_THAT(users_record.column_types, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "UserId" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: true }
                    values { string_value: "1" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Name" }
                    values { string_value: "{\"code\":\"STRING\"}" }
                    values { bool_value: false }
                    values { string_value: "2" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Age" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: false }
                    values { string_value: "3" }
                  }
                })pb"));
  EXPECT_THAT(
      users_record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"1\"}" }
                   values { string_value: "{\"Age\":\"20\",\"Name\":null}" }
                   values { string_value: "{}" }
                 }
               })pb"));
}

// Generate 1 DataChangeRecord with column_types containing all tracked columns
// and mods only containing values of the tracked columns.
TEST_F(ChangeStreamTest,
       InsertToTrackedAndUntrackedColsSameRowSameTransaction) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"(
          CREATE CHANGE STREAM StreamUserName FOR Users(Name) OPTIONS ( value_capture_type = 'NEW_VALUES' )
        )",
  }));
  auto mutation_builder_insert_users_name =
      InsertMutationBuilder("Users", {"UserId", "Name", "Age"});
  std::vector<ValueRow> rows = {{1, "name1", 20}};
  for (const auto& row : rows) {
    mutation_builder_insert_users_name.AddRow(row);
  }
  // 1 transaction
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert_users_name.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  Timestamp commit_timestamp = commit_result->commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUserName"));
  // EXPECT_EQ(data_change_records[0].table_name.string_value(), "Accounts");
  ASSERT_EQ(data_change_records.size(), 1);
  EXPECT_EQ(data_change_records[0].table_name.string_value(), "Users");
  // column_types
  EXPECT_THAT(data_change_records[0].column_types, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "UserId" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: true }
                    values { string_value: "1" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Name" }
                    values { string_value: "{\"code\":\"STRING\"}" }
                    values { bool_value: false }
                    values { string_value: "2" }
                  }
                })pb"));
  // mods
  EXPECT_THAT(data_change_records[0].mods,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "{\"UserId\":\"1\"}" }
                           values { string_value: "{\"Name\":\"name1\"}" }
                           values { string_value: "{}" }
                         }
                       })pb"));
}

// Generate 1 DataChangeRecord.
TEST_F(ChangeStreamTest, AddTableToChangeStreamTrackAll) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"(
          CREATE TABLE Friends(
            FriendId     INT64 NOT NULL,
            Name       STRING(MAX),
            Age        INT64,
          ) PRIMARY KEY (FriendId)
        )",
  }));
  // 1 transaction
  auto mutation_builder_insert_friends =
      InsertMutationBuilder("Friends", {"FriendId", "Name"});
  std::vector<ValueRow> rows = {{1, "name1"}};
  for (const auto& row : rows) {
    mutation_builder_insert_friends.AddRow(row);
  }
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert_friends.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  Timestamp commit_timestamp = commit_result->commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamAll"));
  ASSERT_EQ(data_change_records.size(), 1);
  EXPECT_EQ(data_change_records[0].table_name.string_value(), "Friends");
  DataChangeRecord record = data_change_records[0];
  // column_types
  EXPECT_THAT(data_change_records[0].column_types, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "FriendId" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: true }
                    values { string_value: "1" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Name" }
                    values { string_value: "{\"code\":\"STRING\"}" }
                    values { bool_value: false }
                    values { string_value: "2" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Age" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: false }
                    values { string_value: "3" }
                  }
                })pb"));
  // mods
  EXPECT_THAT(
      data_change_records[0].mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"FriendId\":\"1\"}" }
                   values { string_value: "{\"Age\":null,\"Name\":\"name1\"}" }
                   values { string_value: "{}" }
                 }
               })pb"));
}

// Check the column_types and mods with multiple pk columns
TEST_F(ChangeStreamTest, ChangeStreamTrackingTableWithCompositeKey) {
  auto mutation_builder_insert_friends =
      InsertMutationBuilder("Relation", {"User1Id", "User2Id", "Description"});
  std::vector<ValueRow> rows = {{1, 1, "Friends"}};
  for (const auto& row : rows) {
    mutation_builder_insert_friends.AddRow(row);
  }
  // 1 transaction
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert_friends.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  Timestamp commit_timestamp = commit_result->commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamRelation"));
  EXPECT_EQ(data_change_records[0].record_sequence.string_value(), "00000000");
  EXPECT_EQ(data_change_records[0]
                .is_last_record_in_transaction_in_partition.bool_value(),
            true);
  EXPECT_EQ(data_change_records[0].table_name.string_value(), "Relation");
  ASSERT_EQ(data_change_records.size(), 1);
  EXPECT_EQ(data_change_records[0].mod_type.string_value(), "INSERT");
  EXPECT_EQ(data_change_records[0].value_capture_type.string_value(),
            "OLD_AND_NEW_VALUES");
  EXPECT_EQ(
      data_change_records[0].number_of_records_in_transaction.string_value(),
      "1");
  EXPECT_EQ(
      data_change_records[0].number_of_partitions_in_transaction.string_value(),
      "1");
  EXPECT_EQ(data_change_records[0].transaction_tag.string_value(), "");
  EXPECT_EQ(data_change_records[0].is_system_transaction.bool_value(), false);
  // column_types
  EXPECT_THAT(data_change_records[0].column_types, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "User1Id" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: true }
                    values { string_value: "1" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "User2Id" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: true }
                    values { string_value: "2" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Description" }
                    values { string_value: "{\"code\":\"STRING\"}" }
                    values { bool_value: false }
                    values { string_value: "3" }
                  }
                })pb"));
  // mods
  EXPECT_THAT(
      data_change_records[0].mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values {
                     string_value: "{\"User1Id\":\"1\",\"User2Id\":\"1\"}"
                   }
                   values { string_value: "{\"Description\":\"Friends\"}" }
                   values { string_value: "{}" }
                 }
               })pb"));
}

TEST_F(ChangeStreamTest, SingleInsertWithPKOnly) {
  auto mutation_builder_insert_users =
      InsertMutationBuilder("Users", {"UserId"});
  std::vector<ValueRow> rows = {{1}};
  for (const auto& row : rows) {
    mutation_builder_insert_users.AddRow(row);
  }
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert_users.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  Timestamp commit_timestamp = commit_result->commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.table_name.string_value(), "Users");
  EXPECT_EQ(record.mod_type.string_value(), "INSERT");
  // column_types
  EXPECT_THAT(data_change_records[0].column_types, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "UserId" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: true }
                    values { string_value: "1" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Name" }
                    values { string_value: "{\"code\":\"STRING\"}" }
                    values { bool_value: false }
                    values { string_value: "2" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Age" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: false }
                    values { string_value: "3" }
                  }
                })pb"));
  // mods
  EXPECT_THAT(
      data_change_records[0].mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"1\"}" }
                   values { string_value: "{\"Age\":null,\"Name\":null}" }
                   values { string_value: "{}" }
                 }
               })pb"));
}

// Still generate 1 DataChangeRecord.
TEST_F(ChangeStreamTest, UpdateWithTheSameValuesAsExistingRow) {
  // Insert one row as the existing row for the second transaction
  auto mutation_builder_insert_users =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "name1"}};
  for (const auto& row : rows) {
    mutation_builder_insert_users.AddRow(row);
  }
  // Commit the 1st transaction
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert_users.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  // Update the same row with the same value
  auto mutation_builder_update_users =
      UpdateMutationBuilder("Users", {"UserId", "Name"});
  rows = {{1, "name1"}};
  for (const auto& row : rows) {
    mutation_builder_update_users.AddRow(row);
  }
  // Commit the 2nd transaction
  absl::StatusOr<CommitResult> commit_result_update =
      Commit({mutation_builder_update_users.Build()});
  ZETASQL_EXPECT_OK(commit_result_update.status());
  Timestamp commit_timestamp_update = commit_result_update->commit_timestamp;
  absl::Time query_start_time_update =
      commit_timestamp_update.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time_update, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.table_name.string_value(), "Users");
  EXPECT_EQ(record.mod_type.string_value(), "UPDATE");
  // column_types
  EXPECT_THAT(data_change_records[0].column_types, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "UserId" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: true }
                    values { string_value: "1" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Name" }
                    values { string_value: "{\"code\":\"STRING\"}" }
                    values { bool_value: false }
                    values { string_value: "2" }
                  }
                })pb"));
  // mods
  EXPECT_THAT(data_change_records[0].mods,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "{\"UserId\":\"1\"}" }
                           values { string_value: "{\"Name\":\"name1\"}" }
                           values { string_value: "{}" }
                         }
                       })pb"));
}

TEST_F(ChangeStreamTest, UpdateAColumnNotTracked) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"(
          CREATE CHANGE STREAM StreamUserAge FOR Users(Age) OPTIONS (
          value_capture_type = 'NEW_VALUES' )
        )",
  }));
  // Insert one row as the existing row for the second transaction
  auto mutation_builder_insert_users =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "name1"}};
  for (const auto& row : rows) {
    mutation_builder_insert_users.AddRow(row);
  }
  // Commit the 1st transaction
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert_users.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  auto mutation_builder_update_users =
      UpdateMutationBuilder("Users", {"UserId", "Name"});
  rows = {{1, "name2"}};
  for (const auto& row : rows) {
    mutation_builder_update_users.AddRow(row);
  }
  // Commit the 2nd transaction
  absl::StatusOr<CommitResult> commit_result_update =
      Commit({mutation_builder_update_users.Build()});
  ZETASQL_EXPECT_OK(commit_result_update.status());
  Timestamp commit_timestamp_update = commit_result_update->commit_timestamp;
  absl::Time query_start_time_update =
      commit_timestamp_update.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time_update, "StreamUserAge"));
  ASSERT_EQ(data_change_records.size(), 0);
}

// Alter StreamUsers to track Accounts and insert into Users and Accounts.
// Change record is generated only for Accounts
TEST_F(ChangeStreamTest, AlterChangeStreamTrackingTable) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"(
          ALTER CHANGE STREAM StreamUsers SET FOR Accounts
        )",
  }));
  auto mutation_builder_insert_users =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "name1"}};
  for (const auto& row : rows) {
    mutation_builder_insert_users.AddRow(row);
  }

  auto mutation_builder_insert_accounts =
      InsertMutationBuilder("Accounts", {"AccountId", "Country"});
  rows = {{1, "country1"}};
  for (const auto& row : rows) {
    mutation_builder_insert_accounts.AddRow(row);
  }
  // 1 transaction
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert_users.Build(),
              mutation_builder_insert_accounts.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  Timestamp commit_timestamp = commit_result->commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 1);
  EXPECT_EQ(data_change_records[0].table_name.string_value(), "Accounts");
  EXPECT_THAT(data_change_records[0].column_types, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "AccountId" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: true }
                    values { string_value: "1" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Country" }
                    values { string_value: "{\"code\":\"STRING\"}" }
                    values { bool_value: false }
                    values { string_value: "2" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Year" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: false }
                    values { string_value: "3" }
                  }
                })pb"));
  EXPECT_THAT(
      data_change_records[0].mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"AccountId\":\"1\"}" }
                   values {
                     string_value: "{\"Country\":\"country1\",\"Year\":null}"
                   }
                   values { string_value: "{}" }
                 }
               })pb"));
}

// Alter the StreamUsers to track nothing and insert into Users to verify no
// change record is generated.
TEST_F(ChangeStreamTest, AlterChangeStreamDropForAll) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"(
          ALTER CHANGE STREAM StreamUsers DROP FOR ALL
        )",
  }));
  auto mutation_builder_insert_users =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "name1"}};
  for (const auto& row : rows) {
    mutation_builder_insert_users.AddRow(row);
  }

  // 1 transaction
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert_users.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  Timestamp commit_timestamp = commit_result->commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 0);
}

// Create a table, create a change stream, then create an index. Verify that no
// change records are generated from the index backfill.
TEST_F(ChangeStreamTest, IndexBackfillDoesNotGenerateRecords) {
  // Table Users and change stream StreamUsers already exist in the schema. Now
  // create a index for Users.
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"(
          CREATE INDEX UsersByIdName ON Users(UserId, Name)
        )",
  }));
  auto mutation_builder_insert_users =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "name1"}};
  for (const auto& row : rows) {
    mutation_builder_insert_users.AddRow(row);
  }

  // 1 transaction
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert_users.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  Timestamp commit_timestamp = commit_result->commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 1);
  EXPECT_THAT(data_change_records[0].column_types, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "UserId" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: true }
                    values { string_value: "1" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Name" }
                    values { string_value: "{\"code\":\"STRING\"}" }
                    values { bool_value: false }
                    values { string_value: "2" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Age" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: false }
                    values { string_value: "3" }
                  }
                })pb"));
  // mods
  EXPECT_THAT(
      data_change_records[0].mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"1\"}" }
                   values { string_value: "{\"Age\":null,\"Name\":\"name1\"}" }
                   values { string_value: "{}" }
                 }
               })pb"));
}

// CREATE CHANGE STREAM change_stream FOR Table();
// First, insert into the table with non-key columns inserted as well. The mods
// of DataChangeRecord only has keys populated. Second, update the table with
// non-key columns updated as well. No change record is generated. Third, delete
// the row. One change record is generated.
TEST_F(ChangeStreamTest,
       InsertUpdateDeleteToTableTrackedByChangeStreamTrackingPKOnly) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"(
          CREATE CHANGE STREAM StreamUsersId FOR Users() OPTIONS ( value_capture_type = 'NEW_VALUES' )
        )",
  }));
  auto mutation_builder_insert =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "name1"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  // 1st transaction
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  Timestamp commit_timestamp = commit_result->commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsersId"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  // commit_timestamp
  EXPECT_EQ(record.commit_timestamp.string_value(),
            cloud::spanner_internal::TimestampToRFC3339(commit_timestamp));
  // record_sequence
  EXPECT_EQ(record.record_sequence.string_value(), "00000000");
  // is_last_record_in_transaction_in_partition
  EXPECT_EQ(record.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  // table_name
  EXPECT_EQ(record.table_name.string_value(), "Users");
  // mod_type
  EXPECT_EQ(record.mod_type.string_value(), "INSERT");
  // value_capture_type
  EXPECT_EQ(record.value_capture_type.string_value(), "NEW_VALUES");
  // number_of_records_in_transaction
  EXPECT_EQ(record.number_of_records_in_transaction.string_value(), "1");
  // number_of_partitions_in_transaction
  EXPECT_EQ(record.number_of_partitions_in_transaction.string_value(), "1");
  // transaction_tag
  EXPECT_EQ(record.transaction_tag.string_value(), "");
  // is_system_transaction
  EXPECT_EQ(record.is_system_transaction.bool_value(), false);
  EXPECT_THAT(record.column_types, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "UserId" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: true }
                    values { string_value: "1" }
                  }
                })pb"));
  // mods
  EXPECT_THAT(data_change_records[0].mods,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "{\"UserId\":\"1\"}" }
                           values { string_value: "{}" }
                           values { string_value: "{}" }
                         }
                       })pb"));
  // 2nd transaction
  auto mutation_builder_update =
      UpdateMutationBuilder("Users", {"UserId", "Name"});
  rows = {{1, "name1update"}};
  for (const auto& row : rows) {
    mutation_builder_update.AddRow(row);
  }
  commit_result = Commit({mutation_builder_update.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  commit_timestamp = commit_result->commit_timestamp;
  query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsersId"));
  ASSERT_EQ(data_change_records.size(), 0);
  // 3rd transaction
  auto mutation_builder_delete = DeleteMutationBuilder(
      "Users", KeySet().AddKey(cloud::spanner::MakeKey(1)));
  commit_result = Commit({mutation_builder_delete.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  commit_timestamp = commit_result->commit_timestamp;
  query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsersId"));
  ASSERT_EQ(data_change_records.size(), 1);
  record = data_change_records[0];
  EXPECT_THAT(record.column_types, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "UserId" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: true }
                    values { string_value: "1" }
                  }
                })pb"));
  // mods
  EXPECT_THAT(data_change_records[0].mods,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "{\"UserId\":\"1\"}" }
                           values { string_value: "{}" }
                           values { string_value: "{}" }
                         }
                       })pb"));
  // commit_timestamp
  EXPECT_EQ(record.commit_timestamp.string_value(),
            cloud::spanner_internal::TimestampToRFC3339(commit_timestamp));
  // record_sequence
  EXPECT_EQ(record.record_sequence.string_value(), "00000000");
  // is_last_record_in_transaction_in_partition
  EXPECT_EQ(record.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  // table_name
  EXPECT_EQ(record.table_name.string_value(), "Users");
  // mod_type
  EXPECT_EQ(record.mod_type.string_value(), "DELETE");
  // value_capture_type
  EXPECT_EQ(record.value_capture_type.string_value(), "NEW_VALUES");
  // number_of_records_in_transaction
  EXPECT_EQ(record.number_of_records_in_transaction.string_value(), "1");
  // number_of_partitions_in_transaction
  EXPECT_EQ(record.number_of_partitions_in_transaction.string_value(), "1");
  // transaction_tag
  EXPECT_EQ(record.transaction_tag.string_value(), "");
  // is_system_transaction
  EXPECT_EQ(record.is_system_transaction.bool_value(), false);
}

// Insert a new row including the newly added column.
// One change record is generated.
TEST_F(ChangeStreamTest, AddColumnToChangeStreamTrackingAll) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"(
          ALTER TABLE Users ADD COLUMN Description STRING(MAX)
        )"}));
  // 1st transaction
  auto mutation_builder_insert =
      InsertMutationBuilder("Users", {"UserId", "Name", "Age", "Description"});
  std::vector<ValueRow> rows = {{1, "name1", 20, "Foo Bar"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  Timestamp commit_timestamp = commit_result->commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 1);
  // 1st DataChangeRecord
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.mod_type.string_value(), "INSERT");
  EXPECT_THAT(record.column_types, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "UserId" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: true }
                    values { string_value: "1" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Name" }
                    values { string_value: "{\"code\":\"STRING\"}" }
                    values { bool_value: false }
                    values { string_value: "2" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Age" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: false }
                    values { string_value: "3" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Description" }
                    values { string_value: "{\"code\":\"STRING\"}" }
                    values { bool_value: false }
                    values { string_value: "4" }
                  }
                })pb"));
  // mods
  EXPECT_THAT(
      data_change_records[0].mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"1\"}" }
                   values {
                     string_value: "{\"Age\":\"20\",\"Description\":\"Foo Bar\",\"Name\":\"name1\"}"
                   }
                   values { string_value: "{}" }
                 }
               })pb"));
}

TEST_F(ChangeStreamTest, RangeDeleteExistingRows) {
  // Insert one row as the existing row for the second transaction
  auto mutation_builder_insert_users =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "name1"}, {2, "name2"}, {3, "name3"}};
  for (const auto& row : rows) {
    mutation_builder_insert_users.AddRow(row);
  }
  // Commit the 1st transaction
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert_users.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  auto mutation_builder_range_delete =
      DeleteMutationBuilder("Users", KeySet()
                                         .AddKey(cloud::spanner::MakeKey(1))
                                         .AddKey(cloud::spanner::MakeKey(2)));
  absl::StatusOr<CommitResult> commit_result_range_delete =
      Commit({mutation_builder_range_delete.Build()});
  ZETASQL_EXPECT_OK(commit_result_range_delete.status());
  Timestamp commit_timestamp_range_delete =
      commit_result_range_delete->commit_timestamp;
  absl::Time query_start_time_range_delete =
      commit_timestamp_range_delete.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto data_change_records,
                       GetDataRecordsFromStartToNow(
                           query_start_time_range_delete, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.table_name.string_value(), "Users");
  EXPECT_EQ(record.mod_type.string_value(), "DELETE");
  EXPECT_THAT(record.column_types, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "UserId" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: true }
                    values { string_value: "1" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Name" }
                    values { string_value: "{\"code\":\"STRING\"}" }
                    values { bool_value: false }
                    values { string_value: "2" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Age" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: false }
                    values { string_value: "3" }
                  }
                })pb"));
  EXPECT_THAT(record.mods, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "{\"UserId\":\"1\"}" }
                    values { string_value: "{}" }
                    values { string_value: "{}" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "{\"UserId\":\"2\"}" }
                    values { string_value: "{}" }
                    values { string_value: "{}" }
                  }
                })pb"));
}

// Check column_types and mods for different data types are correct.
TEST_F(ChangeStreamTest, DiffDataTypes) {
  auto mutation_builder_insert_friends = InsertMutationBuilder(
      "ScalarTypesTable",
      {"intVal", "boolVal", "bytesVal", "dateVal", "float64Val", "stringVal",
       "numericVal", "timestampVal", "jsonVal", "arrayInt", "arrayStr",
       "arrayJson", "float32Val"});
  Array<Numeric> numeric_arr{cloud::spanner::MakeNumeric("1").value(),
                             cloud::spanner::MakeNumeric("2").value(),
                             cloud::spanner::MakeNumeric("3").value()};
  Array<std::int64_t> num_arr{1, 2, 3};
  Array<std::string> str_arr{"Hello", "Hi"};
  Array<Json> json_arr{Json(R"("Hello")"), Json(R"("Hi")")};
  std::vector<ValueRow> rows = {
      {1, true,
       cloud::spanner::Bytes(
           cloud::spanner_internal::BytesFromBase64("blue").value()),
       "2014-09-27", 1.1, "stringVal", cloud::spanner::MakeNumeric("1").value(),
       cloud::spanner::MakeTimestamp(absl::UnixEpoch()).value(),
       Json(R"("Hello world!")"), num_arr, str_arr, json_arr, 3.14f}};
  for (const auto& row : rows) {
    mutation_builder_insert_friends.AddRow(row);
  }
  // 1 transaction
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert_friends.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  Timestamp commit_timestamp = commit_result->commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamScalarTypes"));
  EXPECT_EQ(data_change_records[0].table_name.string_value(),
            "ScalarTypesTable");
  ASSERT_EQ(data_change_records.size(), 1);
  // column_types
  EXPECT_THAT(
      data_change_records[0].column_types, test::EqualsProto(R"pb(
        values {
          list_value {
            values { string_value: "intVal" }
            values { string_value: "{\"code\":\"INT64\"}" }
            values { bool_value: true }
            values { string_value: "1" }
          }
        }
        values {
          list_value {
            values { string_value: "boolVal" }
            values { string_value: "{\"code\":\"BOOL\"}" }
            values { bool_value: false }
            values { string_value: "2" }
          }
        }
        values {
          list_value {
            values { string_value: "bytesVal" }
            values { string_value: "{\"code\":\"BYTES\"}" }
            values { bool_value: false }
            values { string_value: "3" }
          }
        }
        values {
          list_value {
            values { string_value: "dateVal" }
            values { string_value: "{\"code\":\"DATE\"}" }
            values { bool_value: false }
            values { string_value: "4" }
          }
        }
        values {
          list_value {
            values { string_value: "float64Val" }
            values { string_value: "{\"code\":\"FLOAT64\"}" }
            values { bool_value: false }
            values { string_value: "5" }
          }
        }
        values {
          list_value {
            values { string_value: "stringVal" }
            values { string_value: "{\"code\":\"STRING\"}" }
            values { bool_value: false }
            values { string_value: "6" }
          }
        }
        values {
          list_value {
            values { string_value: "numericVal" }
            values { string_value: "{\"code\":\"NUMERIC\"}" }
            values { bool_value: false }
            values { string_value: "7" }
          }
        }
        values {
          list_value {
            values { string_value: "timestampVal" }
            values { string_value: "{\"code\":\"TIMESTAMP\"}" }
            values { bool_value: false }
            values { string_value: "8" }
          }
        }
        values {
          list_value {
            values { string_value: "jsonVal" }
            values { string_value: "{\"code\":\"JSON\"}" }
            values { bool_value: false }
            values { string_value: "9" }
          }
        }
        values {
          list_value {
            values { string_value: "arrayInt" }
            values {
              string_value: "{\"array_element_type\":{\"code\":\"INT64\"},\"code\":\"ARRAY\"}"
            }
            values { bool_value: false }
            values { string_value: "10" }
          }
        }
        values {
          list_value {
            values { string_value: "arrayStr" }
            values {
              string_value: "{\"array_element_type\":{\"code\":\"STRING\"},\"code\":\"ARRAY\"}"
            }
            values { bool_value: false }
            values { string_value: "11" }
          }
        }
        values {
          list_value {
            values { string_value: "arrayJson" }
            values {
              string_value: "{\"array_element_type\":{\"code\":\"JSON\"},\"code\":\"ARRAY\"}"
            }
            values { bool_value: false }
            values { string_value: "12" }
          }
        }
        values {
          list_value {
            values { string_value: "float32Val" }
            values { string_value: "{\"code\":\"FLOAT32\"}" }
            values { bool_value: false }
            values { string_value: "13" }
          }
        }
      )pb"));
  // mods
  EXPECT_THAT(
      data_change_records[0].mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"intVal\":\"1\"}" }
                   values {
                     string_value: "{\"arrayInt\":[\"1\",\"2\",\"3\"],\"arrayJson\":[\"\\\"Hello\\\"\",\"\\\"Hi\\\"\"],\"arrayStr\":[\"Hello\",\"Hi\"],\"boolVal\":true,\"bytesVal\":\"blue\",\"dateVal\":\"2014-09-27\",\"float32Val\":3.140000104904175,\"float64Val\":1.1,\"jsonVal\":\"\\\"Hello world!\\\"\",\"numericVal\":\"1\",\"stringVal\":\"stringVal\",\"timestampVal\":\"1970-01-01T00:00:00Z\"}"
                   }
                   values { string_value: "{}" }
                 }
               })pb"));
}
// TODO: This test should be re-enabled after the feature gap is
// covered. Currently TransactionStore::BufferInsert always substitute the prior
// DELETE row with the current inserted value, which leads to only 1
// DataChangeRecord to be generated while in production 2 DataChangeRecords are
// generated.
// Single REPLACE is the same as DELETE + INSERT. Single replace to an existing
// row generates 2 records (1 DELETE record and then 1 INSERT record)
TEST_F(ChangeStreamTest, DISABLED_SingleReplaceExistingRow) {
  // Build the mutation for the 1st transaction
  auto mutation_builder_insert_users =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "name1"}};
  for (const auto& row : rows) {
    mutation_builder_insert_users.AddRow(row);
  }
  // Commit the 1st transaction
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert_users.Build()});
  // Build the mutation for the 2nd transaction
  auto replace_mutation_builder =
      ReplaceMutationBuilder("Users", {"UserId", "Name"});
  rows = {{1, "name2"}};
  for (const auto& row : rows) {
    replace_mutation_builder.AddRow(row);
  }
  // Commit the 2nd transaction
  absl::StatusOr<CommitResult> commit_result2 =
      Commit({replace_mutation_builder.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  Timestamp commit_timestamp_replace = commit_result2->commit_timestamp;
  absl::Time query_start_time_replace =
      commit_timestamp_replace.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time_replace, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 2);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.table_name.string_value(), "Users");
  EXPECT_EQ(record.mod_type.string_value(), "DELETE");
  EXPECT_THAT(record.column_types, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "UserId" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: true }
                    values { string_value: "1" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Name" }
                    values { string_value: "{\"code\":\"STRING\"}" }
                    values { bool_value: false }
                    values { string_value: "2" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Age" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: false }
                    values { string_value: "3" }
                  }
                })pb"));
  EXPECT_THAT(record.mods, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "{\"UserId\":\"1\"}" }
                    values { string_value: "{}" }
                    values { string_value: "{}" }
                  }
                })pb"));
  record = data_change_records[1];
  EXPECT_EQ(record.table_name.string_value(), "Users");
  EXPECT_EQ(record.mod_type.string_value(), "INSERT");
  EXPECT_THAT(record.column_types, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "UserId" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: true }
                    values { string_value: "1" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Name" }
                    values { string_value: "{\"code\":\"STRING\"}" }
                    values { bool_value: false }
                    values { string_value: "2" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Age" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: false }
                    values { string_value: "3" }
                  }
                })pb"));
  EXPECT_THAT(record.mods, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "{\"UserId\":\"1\"}" }
                    values { string_value: "{\"Age\":null,\"Name\":\"name2\"}" }
                    values { string_value: "{}" }
                  }
                })pb"));
}

// DELETE -> INSERT -> DELETE an existing row should generate 1 DELETE record.
TEST_F(ChangeStreamTest, DeleteInsertDeleteExistingRow) {
  // Create an existing row
  auto mutation_builder_insert_users =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "name1"}};
  for (const auto& row : rows) {
    mutation_builder_insert_users.AddRow(row);
  }
  // Commit the 1st transaction
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert_users.Build()});
  // Build the mutation for the 2nd transaction
  auto mutation_builder_delete = DeleteMutationBuilder(
      "Users", KeySet().AddKey(cloud::spanner::MakeKey(1)));
  mutation_builder_insert_users =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  rows = {{1, "name2"}};
  for (const auto& row : rows) {
    mutation_builder_insert_users.AddRow(row);
  }
  auto mutation_builder_delete_2 = DeleteMutationBuilder(
      "Users", KeySet().AddKey(cloud::spanner::MakeKey(1)));

  // Commit the 2nd transaction
  absl::StatusOr<CommitResult> commit_result2 = Commit(
      {mutation_builder_delete.Build(), mutation_builder_insert_users.Build(),
       mutation_builder_delete_2.Build()});
  ZETASQL_EXPECT_OK(commit_result2.status());
  Timestamp commit_timestamp = commit_result2->commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.table_name.string_value(), "Users");
  EXPECT_EQ(record.mod_type.string_value(), "DELETE");
  EXPECT_THAT(record.column_types, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "UserId" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: true }
                    values { string_value: "1" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Name" }
                    values { string_value: "{\"code\":\"STRING\"}" }
                    values { bool_value: false }
                    values { string_value: "2" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Age" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: false }
                    values { string_value: "3" }
                  }
                })pb"));
  EXPECT_THAT(record.mods, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "{\"UserId\":\"1\"}" }
                    values { string_value: "{}" }
                    values { string_value: "{}" }
                  }
                })pb"));
}

// TODO: This test should be re-enabled after the feature gap is
// covered. Currently TransactionStore::BufferInsert always substitute the prior
// DELETE row with the current inserted value, which leads to only 1
// DataChangeRecord to be generated while in production 2 DataChangeRecords are
// generated.
TEST_F(ChangeStreamTest, DISABLED_ConsecutiveReplace) {
  // Build the mutation for the 1st transaction
  auto mutation_builder_insert_users =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "name1"}};
  for (const auto& row : rows) {
    mutation_builder_insert_users.AddRow(row);
  }
  // Commit the 1st transaction
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert_users.Build()});
  // Build the mutation for the 2nd transaction
  auto replace_mutation_builder =
      ReplaceMutationBuilder("Users", {"UserId", "Name"});
  rows = {{1, "name2"}, {1, "name3"}};
  for (const auto& row : rows) {
    replace_mutation_builder.AddRow(row);
  }
  // Commit the 2nd transaction
  absl::StatusOr<CommitResult> commit_result2 =
      Commit({replace_mutation_builder.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  Timestamp commit_timestamp_replace = commit_result2->commit_timestamp;
  absl::Time query_start_time_replace =
      commit_timestamp_replace.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time_replace, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 2);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.table_name.string_value(), "Users");
  EXPECT_EQ(record.mod_type.string_value(), "DELETE");
  EXPECT_THAT(record.column_types, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "UserId" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: true }
                    values { string_value: "1" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Name" }
                    values { string_value: "{\"code\":\"STRING\"}" }
                    values { bool_value: false }
                    values { string_value: "2" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Age" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: false }
                    values { string_value: "3" }
                  }
                })pb"));
  EXPECT_THAT(record.mods, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "{\"UserId\":\"1\"}" }
                    values { string_value: "{}" }
                    values { string_value: "{}" }
                  }
                })pb"));
  record = data_change_records[1];
  EXPECT_EQ(record.table_name.string_value(), "Users");
  EXPECT_EQ(record.mod_type.string_value(), "INSERT");
  EXPECT_THAT(record.column_types, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "UserId" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: true }
                    values { string_value: "1" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Name" }
                    values { string_value: "{\"code\":\"STRING\"}" }
                    values { bool_value: false }
                    values { string_value: "2" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Age" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: false }
                    values { string_value: "3" }
                  }
                })pb"));
  EXPECT_THAT(record.mods, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "{\"UserId\":\"1\"}" }
                    values { string_value: "{\"Age\":null,\"Name\":\"name3\"}" }
                    values { string_value: "{}" }
                  }
                })pb"));
}

TEST_F(ChangeStreamTest, RangeDeleteNewRowInsertedInTheSameTransaction) {
  // Insert one row as the existing row for the second transaction
  auto mutation_builder_insert_users =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "name1"}, {2, "name2"}};
  for (const auto& row : rows) {
    mutation_builder_insert_users.AddRow(row);
  }
  // Commit the 1st transaction
  absl::StatusOr<CommitResult> commit_result =
      Commit({mutation_builder_insert_users.Build()});
  ZETASQL_EXPECT_OK(commit_result.status());
  // Insert mutation in the second transaction
  mutation_builder_insert_users =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  rows = {{3, "name3"}};
  for (const auto& row : rows) {
    mutation_builder_insert_users.AddRow(row);
  }
  // Delete mutation in the second transaction
  auto mutation_builder_range_delete =
      DeleteMutationBuilder("Users", KeySet()
                                         .AddKey(cloud::spanner::MakeKey(1))
                                         .AddKey(cloud::spanner::MakeKey(3)));
  absl::StatusOr<CommitResult> commit_result_range_delete =
      Commit({mutation_builder_insert_users.Build(),
              mutation_builder_range_delete.Build()});
  ZETASQL_EXPECT_OK(commit_result_range_delete.status());
  Timestamp commit_timestamp_range_delete =
      commit_result_range_delete->commit_timestamp;
  absl::Time query_start_time_range_delete =
      commit_timestamp_range_delete.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto data_change_records,
                       GetDataRecordsFromStartToNow(
                           query_start_time_range_delete, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.table_name.string_value(), "Users");
  EXPECT_EQ(record.mod_type.string_value(), "DELETE");
  EXPECT_THAT(record.column_types, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "UserId" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: true }
                    values { string_value: "1" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Name" }
                    values { string_value: "{\"code\":\"STRING\"}" }
                    values { bool_value: false }
                    values { string_value: "2" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Age" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: false }
                    values { string_value: "3" }
                  }
                })pb"));
  EXPECT_THAT(record.mods, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "{\"UserId\":\"1\"}" }
                    values { string_value: "{}" }
                    values { string_value: "{}" }
                  }
                })pb"));
}

TEST_F(ChangeStreamTest, ModValuesOrderByAlphabeticalOrder) {
  // INSERT transaction
  auto mutation_builder_insert_users =
      InsertMutationBuilder("Users", {"UserId", "Name", "Age"});
  std::vector<ValueRow> rows = {{1, "name1", 20}, {2, "name2", 20}};
  for (const auto& row : rows) {
    mutation_builder_insert_users.AddRow(row);
  }
  // Commit the 1st transaction
  absl::StatusOr<CommitResult> commit_result_insert_users =
      Commit({mutation_builder_insert_users.Build()});
  ZETASQL_EXPECT_OK(commit_result_insert_users.status());
  Timestamp commit_timestamp_insert_users =
      commit_result_insert_users->commit_timestamp;
  absl::Time query_start_time_insert_users =
      commit_timestamp_insert_users.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto data_change_records,
                       GetDataRecordsFromStartToNow(
                           query_start_time_insert_users, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.table_name.string_value(), "Users");
  EXPECT_EQ(record.mod_type.string_value(), "INSERT");
  EXPECT_THAT(record.column_types, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "UserId" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: true }
                    values { string_value: "1" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Name" }
                    values { string_value: "{\"code\":\"STRING\"}" }
                    values { bool_value: false }
                    values { string_value: "2" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Age" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: false }
                    values { string_value: "3" }
                  }
                })pb"));
  EXPECT_THAT(data_change_records[0].mods,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "{\"UserId\":\"1\"}" }
                           values {
                             string_value: "{\"Age\":\"20\",\"Name\":\"name1\"}"
                           }
                           values { string_value: "{}" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "{\"UserId\":\"2\"}" }
                           values {
                             string_value: "{\"Age\":\"20\",\"Name\":\"name2\"}"
                           }
                           values { string_value: "{}" }
                         }
                       })pb"));
  // UPDATE transaction
  auto mutation_builder_update_users =
      UpdateMutationBuilder("Users", {"UserId", "Name", "Age"});
  rows = {{1, "name1", 30}, {2, "name2", 30}};
  for (const auto& row : rows) {
    mutation_builder_update_users.AddRow(row);
  }
  // Commit the 2st transaction
  absl::StatusOr<CommitResult> commit_result_update_users =
      Commit({mutation_builder_update_users.Build()});
  ZETASQL_EXPECT_OK(commit_result_update_users.status());
  Timestamp commit_timestamp_update_users =
      commit_result_update_users->commit_timestamp;
  absl::Time query_start_time_update_users =
      commit_timestamp_update_users.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(data_change_records,
                       GetDataRecordsFromStartToNow(
                           query_start_time_update_users, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 1);
  record = data_change_records[0];
  EXPECT_EQ(record.table_name.string_value(), "Users");
  EXPECT_EQ(record.mod_type.string_value(), "UPDATE");
  EXPECT_THAT(record.column_types, test::EqualsProto(R"pb(
                values {
                  list_value {
                    values { string_value: "UserId" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: true }
                    values { string_value: "1" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Name" }
                    values { string_value: "{\"code\":\"STRING\"}" }
                    values { bool_value: false }
                    values { string_value: "2" }
                  }
                }
                values {
                  list_value {
                    values { string_value: "Age" }
                    values { string_value: "{\"code\":\"INT64\"}" }
                    values { bool_value: false }
                    values { string_value: "3" }
                  }
                })pb"));
  EXPECT_THAT(record.mods,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "{\"UserId\":\"1\"}" }
                           values {
                             string_value: "{\"Age\":\"30\",\"Name\":\"name1\"}"
                           }
                           values { string_value: "{}" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "{\"UserId\":\"2\"}" }
                           values {
                             string_value: "{\"Age\":\"30\",\"Name\":\"name2\"}"
                           }
                           values { string_value: "{}" }
                         }
                       })pb"));
}

// TODO: This test should be re-enabled after the feature gap is
// covered. The returned DataChangeRecords should be in the same order as
// they're committed in the transaction. However, the ordered is messed up by
// BufferWriteOp in transaction_store.
TEST_F(ChangeStreamTest,
       DISABLED_DataChangeRecordOrderForMultiTablesSameTransaction) {
  // INSERT transaction
  auto mutation_builder_insert_users =
      InsertMutationBuilder("Users", {"UserId", "Name", "Age"});
  std::vector<ValueRow> rows = {{1, "name1", 20}};
  for (const auto& row : rows) {
    mutation_builder_insert_users.AddRow(row);
  }
  auto mutation_builder_insert_accounts =
      InsertMutationBuilder("Accounts", {"AccountId", "Country", "Year"});
  rows = {{1, "US", 2023}};
  for (const auto& row : rows) {
    mutation_builder_insert_accounts.AddRow(row);
  }
  // Commit the 1st transaction
  absl::StatusOr<CommitResult> commit_result_insert =
      Commit({mutation_builder_insert_users.Build(),
              mutation_builder_insert_accounts.Build()});
  ZETASQL_EXPECT_OK(commit_result_insert.status());
  Timestamp commit_timestamp_insert = commit_result_insert->commit_timestamp;
  absl::Time query_start_time_insert =
      commit_timestamp_insert.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time_insert, "StreamAll"));
  ASSERT_EQ(data_change_records.size(), 2);
  DataChangeRecord users_record = data_change_records[0];
  DataChangeRecord accounts_record = data_change_records[1];
  EXPECT_EQ(users_record.table_name.string_value(), "Users");
  EXPECT_EQ(accounts_record.table_name.string_value(), "Accounts");
}

TEST_F(ChangeStreamTest, GeneratedColumnsAreNotPopulatedInInsert) {
  auto mutation_builder_insert =
      InsertMutationBuilder("ASTable", {"UserId", "Data"});
  std::vector<ValueRow> rows = {{1, Json(R"({"a":{"b":456}})")}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_insert.Build()}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamAll"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = std::move(data_change_records[0]);
  EXPECT_EQ(record.commit_timestamp.string_value(),
            cloud::spanner_internal::TimestampToRFC3339(commit_timestamp));
  EXPECT_EQ(record.record_sequence.string_value(), "00000000");
  EXPECT_EQ(record.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  EXPECT_EQ(record.table_name.string_value(), "ASTable");
  EXPECT_EQ(record.mod_type.string_value(), "INSERT");
  EXPECT_EQ(record.value_capture_type.string_value(), "OLD_AND_NEW_VALUES");
  EXPECT_EQ(record.number_of_records_in_transaction.string_value(), "1");
  EXPECT_EQ(record.number_of_partitions_in_transaction.string_value(), "1");
  EXPECT_EQ(record.transaction_tag.string_value(), "");
  EXPECT_EQ(record.is_system_transaction.bool_value(), false);
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Data" }
                           values { string_value: "{\"code\":\"JSON\"}" }
                           values { bool_value: false }
                           values { string_value: "3" }
                         }
                       })pb"));
  EXPECT_THAT(
      record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"1\"}" }
                   values {
                     string_value: "{\"Data\":\"{\\\"a\\\":{\\\"b\\\":456}}\"}"
                   }
                   values { string_value: "{}" }
                 }
               })pb"));
}

TEST_F(ChangeStreamTest, GeneratedColumnsAreNotPopulatedInUpdate) {
  auto mutation_builder_insert =
      InsertMutationBuilder("ASTable", {"UserId", "Data"});
  std::vector<ValueRow> rows = {{1, Json(R"({"a":{"b":456}})")}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  ZETASQL_EXPECT_OK(Commit({mutation_builder_insert.Build()}));
  auto mutation_builder_update =
      UpdateMutationBuilder("ASTable", {"UserId", "Data"});
  rows = {{1, Json(R"({"a":{"b":999}})")}};
  for (const auto& row : rows) {
    mutation_builder_update.AddRow(row);
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_update.Build()}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamAll"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = std::move(data_change_records[0]);
  EXPECT_EQ(record.commit_timestamp.string_value(),
            cloud::spanner_internal::TimestampToRFC3339(commit_timestamp));
  EXPECT_EQ(record.record_sequence.string_value(), "00000000");
  EXPECT_EQ(record.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  EXPECT_EQ(record.table_name.string_value(), "ASTable");
  EXPECT_EQ(record.mod_type.string_value(), "UPDATE");
  EXPECT_EQ(record.value_capture_type.string_value(), "OLD_AND_NEW_VALUES");
  EXPECT_EQ(record.number_of_records_in_transaction.string_value(), "1");
  EXPECT_EQ(record.number_of_partitions_in_transaction.string_value(), "1");
  EXPECT_EQ(record.transaction_tag.string_value(), "");
  EXPECT_EQ(record.is_system_transaction.bool_value(), false);
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Data" }
                           values { string_value: "{\"code\":\"JSON\"}" }
                           values { bool_value: false }
                           values { string_value: "3" }
                         }
                       })pb"));
  EXPECT_THAT(
      record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"1\"}" }
                   values {
                     string_value: "{\"Data\":\"{\\\"a\\\":{\\\"b\\\":999}}\"}"
                   }
                   values {
                     string_value: "{\"Data\":\"{\\\"a\\\":{\\\"b\\\":456}}\"}"
                   }
                 }
               })pb"));
}

TEST_F(ChangeStreamTest, GeneratedColumnsAreNotPopulatedInDelete) {
  auto mutation_builder_insert =
      InsertMutationBuilder("ASTable", {"UserId", "Data"});
  std::vector<ValueRow> rows = {{1, Json(R"({"a":{"b":456}})")}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  ZETASQL_EXPECT_OK(Commit({mutation_builder_insert.Build()}));
  auto mutation_builder_delete = DeleteMutationBuilder(
      "ASTable", KeySet().AddKey(cloud::spanner::MakeKey(1)));
  ;
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_delete.Build()}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamAll"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = std::move(data_change_records[0]);
  EXPECT_EQ(record.commit_timestamp.string_value(),
            cloud::spanner_internal::TimestampToRFC3339(commit_timestamp));
  EXPECT_EQ(record.record_sequence.string_value(), "00000000");
  EXPECT_EQ(record.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  EXPECT_EQ(record.table_name.string_value(), "ASTable");
  EXPECT_EQ(record.mod_type.string_value(), "DELETE");
  EXPECT_EQ(record.value_capture_type.string_value(), "OLD_AND_NEW_VALUES");
  EXPECT_EQ(record.number_of_records_in_transaction.string_value(), "1");
  EXPECT_EQ(record.number_of_partitions_in_transaction.string_value(), "1");
  EXPECT_EQ(record.transaction_tag.string_value(), "");
  EXPECT_EQ(record.is_system_transaction.bool_value(), false);
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Data" }
                           values { string_value: "{\"code\":\"JSON\"}" }
                           values { bool_value: false }
                           values { string_value: "3" }
                         }
                       })pb"));
  EXPECT_THAT(
      record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"1\"}" }
                   values { string_value: "{}" }
                   values {
                     string_value: "{\"Data\":\"{\\\"a\\\":{\\\"b\\\":456}}\"}"
                   }
                 }
               })pb"));
}

TEST_F(ChangeStreamTest, NewRow_GeneratedColumnsAreNotPopulatedInInsert) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"(
          ALTER CHANGE STREAM StreamAll SET OPTIONS ( value_capture_type = 'NEW_ROW' )
        )",
  }));
  auto mutation_builder_insert =
      InsertMutationBuilder("ASTable", {"UserId", "Data"});
  std::vector<ValueRow> rows = {{1, Json(R"({"a":{"b":456}})")}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_insert.Build()}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamAll"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = std::move(data_change_records[0]);
  EXPECT_EQ(record.commit_timestamp.string_value(),
            cloud::spanner_internal::TimestampToRFC3339(commit_timestamp));
  EXPECT_EQ(record.record_sequence.string_value(), "00000000");
  EXPECT_EQ(record.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  EXPECT_EQ(record.table_name.string_value(), "ASTable");
  EXPECT_EQ(record.mod_type.string_value(), "INSERT");
  EXPECT_EQ(record.value_capture_type.string_value(), "NEW_ROW");
  EXPECT_EQ(record.number_of_records_in_transaction.string_value(), "1");
  EXPECT_EQ(record.number_of_partitions_in_transaction.string_value(), "1");
  EXPECT_EQ(record.transaction_tag.string_value(), "");
  EXPECT_EQ(record.is_system_transaction.bool_value(), false);
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Data" }
                           values { string_value: "{\"code\":\"JSON\"}" }
                           values { bool_value: false }
                           values { string_value: "3" }
                         }
                       })pb"));
  EXPECT_THAT(
      record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"1\"}" }
                   values {
                     string_value: "{\"Data\":\"{\\\"a\\\":{\\\"b\\\":456}}\"}"
                   }
                   values { string_value: "{}" }
                 }
               })pb"));
}

TEST_F(ChangeStreamTest, NewRow_GeneratedColumnsAreNotPopulatedInUpdate) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"(
          ALTER CHANGE STREAM StreamAll SET OPTIONS ( value_capture_type = 'NEW_ROW' )
        )",
  }));
  auto mutation_builder_insert =
      InsertMutationBuilder("ASTable", {"UserId", "Data"});
  std::vector<ValueRow> rows = {{1, Json(R"({"a":{"b":456}})")}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  ZETASQL_EXPECT_OK(Commit({mutation_builder_insert.Build()}));
  auto mutation_builder_update =
      UpdateMutationBuilder("ASTable", {"UserId", "Data"});
  rows = {{1, Json(R"({"a":{"b":999}})")}};
  for (const auto& row : rows) {
    mutation_builder_update.AddRow(row);
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_update.Build()}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamAll"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = std::move(data_change_records[0]);
  EXPECT_EQ(record.commit_timestamp.string_value(),
            cloud::spanner_internal::TimestampToRFC3339(commit_timestamp));
  EXPECT_EQ(record.record_sequence.string_value(), "00000000");
  EXPECT_EQ(record.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  EXPECT_EQ(record.table_name.string_value(), "ASTable");
  EXPECT_EQ(record.mod_type.string_value(), "UPDATE");
  EXPECT_EQ(record.value_capture_type.string_value(), "NEW_ROW");
  EXPECT_EQ(record.number_of_records_in_transaction.string_value(), "1");
  EXPECT_EQ(record.number_of_partitions_in_transaction.string_value(), "1");
  EXPECT_EQ(record.transaction_tag.string_value(), "");
  EXPECT_EQ(record.is_system_transaction.bool_value(), false);
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Data" }
                           values { string_value: "{\"code\":\"JSON\"}" }
                           values { bool_value: false }
                           values { string_value: "3" }
                         }
                       })pb"));
  EXPECT_THAT(
      record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"1\"}" }
                   values {
                     string_value: "{\"Data\":\"{\\\"a\\\":{\\\"b\\\":999}}\"}"
                   }
                   values { string_value: "{}" }
                 }
               })pb"));
}

TEST_F(ChangeStreamTest, NewRow_GeneratedColumnsAreNotPopulatedInDelete) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"(
          ALTER CHANGE STREAM StreamAll SET OPTIONS ( value_capture_type = 'NEW_ROW' )
        )",
  }));
  auto mutation_builder_insert =
      InsertMutationBuilder("ASTable", {"UserId", "Data"});
  std::vector<ValueRow> rows = {{1, Json(R"({"a":{"b":456}})")}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  ZETASQL_EXPECT_OK(Commit({mutation_builder_insert.Build()}));
  auto mutation_builder_delete = DeleteMutationBuilder(
      "ASTable", KeySet().AddKey(cloud::spanner::MakeKey(1)));
  ;
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_delete.Build()}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamAll"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = std::move(data_change_records[0]);
  EXPECT_EQ(record.commit_timestamp.string_value(),
            cloud::spanner_internal::TimestampToRFC3339(commit_timestamp));
  EXPECT_EQ(record.record_sequence.string_value(), "00000000");
  EXPECT_EQ(record.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  EXPECT_EQ(record.table_name.string_value(), "ASTable");
  EXPECT_EQ(record.mod_type.string_value(), "DELETE");
  EXPECT_EQ(record.value_capture_type.string_value(), "NEW_ROW");
  EXPECT_EQ(record.number_of_records_in_transaction.string_value(), "1");
  EXPECT_EQ(record.number_of_partitions_in_transaction.string_value(), "1");
  EXPECT_EQ(record.transaction_tag.string_value(), "");
  EXPECT_EQ(record.is_system_transaction.bool_value(), false);
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Data" }
                           values { string_value: "{\"code\":\"JSON\"}" }
                           values { bool_value: false }
                           values { string_value: "3" }
                         }
                       })pb"));
  EXPECT_THAT(record.mods,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "{\"UserId\":\"1\"}" }
                           values { string_value: "{}" }
                           values { string_value: "{}" }
                         }
                       })pb"));
}

TEST_F(ChangeStreamTest,
       OldAndNewValues_UntrackedDeletedColumnsAreNotPopulatedInOldValues) {
  auto mutation_builder_insert =
      InsertMutationBuilder("Users2", {"UserId", "Name", "Gender"});
  std::vector<ValueRow> rows = {{1, "name1", "F"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  ZETASQL_EXPECT_OK(Commit({mutation_builder_insert.Build()}));
  // Delete an existing row generates a DELETE record
  auto mutation_builder_delete = DeleteMutationBuilder(
      "Users2", KeySet().AddKey(cloud::spanner::MakeKey(1)));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_delete.Build()}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamSpecifiedColumns"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.commit_timestamp.string_value(),
            cloud::spanner_internal::TimestampToRFC3339(commit_timestamp));
  EXPECT_EQ(record.record_sequence.string_value(), "00000000");
  EXPECT_EQ(record.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  EXPECT_EQ(record.table_name.string_value(), "Users2");
  EXPECT_EQ(record.mod_type.string_value(), "DELETE");
  EXPECT_EQ(record.value_capture_type.string_value(), "OLD_AND_NEW_VALUES");
  EXPECT_EQ(record.number_of_records_in_transaction.string_value(), "1");
  EXPECT_EQ(record.number_of_partitions_in_transaction.string_value(), "1");
  EXPECT_EQ(record.transaction_tag.string_value(), "");
  EXPECT_EQ(record.is_system_transaction.bool_value(), false);
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Name" }
                           values { string_value: "{\"code\":\"STRING\"}" }
                           values { bool_value: false }
                           values { string_value: "3" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Age" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: false }
                           values { string_value: "4" }
                         }
                       })pb"));
  EXPECT_THAT(
      record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"1\"}" }
                   values { string_value: "{}" }
                   values { string_value: "{\"Age\":null,\"Name\":\"name1\"}" }
                 }
               })pb"));
}

TEST_F(ChangeStreamTest,
       OldAndNewValues_UntrackedUpdatedColumnsAreNotPopulatedInOldValues) {
  auto mutation_builder_insert =
      InsertMutationBuilder("Users2", {"UserId", "Name", "Gender"});
  std::vector<ValueRow> rows = {{1, "name1", "F"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  ZETASQL_EXPECT_OK(Commit({mutation_builder_insert.Build()}));
  auto mutation_builder_update =
      UpdateMutationBuilder("Users2", {"UserId", "Name", "Gender"});
  rows = {{1, "name1Update", "M"}};
  for (const auto& row : rows) {
    mutation_builder_update.AddRow(row);
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_update.Build()}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamSpecifiedColumns"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.commit_timestamp.string_value(),
            cloud::spanner_internal::TimestampToRFC3339(commit_timestamp));
  EXPECT_EQ(record.record_sequence.string_value(), "00000000");
  EXPECT_EQ(record.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  EXPECT_EQ(record.table_name.string_value(), "Users2");
  EXPECT_EQ(record.mod_type.string_value(), "UPDATE");
  EXPECT_EQ(record.value_capture_type.string_value(), "OLD_AND_NEW_VALUES");
  EXPECT_EQ(record.number_of_records_in_transaction.string_value(), "1");
  EXPECT_EQ(record.number_of_partitions_in_transaction.string_value(), "1");
  EXPECT_EQ(record.transaction_tag.string_value(), "");
  EXPECT_EQ(record.is_system_transaction.bool_value(), false);
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Name" }
                           values { string_value: "{\"code\":\"STRING\"}" }
                           values { bool_value: false }
                           values { string_value: "3" }
                         }
                       })pb"));
  EXPECT_THAT(record.mods,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "{\"UserId\":\"1\"}" }
                           values { string_value: "{\"Name\":\"name1Update\"}" }
                           values { string_value: "{\"Name\":\"name1\"}" }
                         }
                       })pb"));
}

TEST_F(ChangeStreamTest,
       SingleInsertVerifyDataChangeRecordContentNewRowAndOldValues) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"(
          ALTER CHANGE STREAM StreamAll SET OPTIONS
          ( value_capture_type = 'NEW_ROW_AND_OLD_VALUES' )
        )",
  }));
  auto mutation_builder_insert =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "name1"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_insert.Build()}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamAll"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.commit_timestamp.string_value(),
            cloud::spanner_internal::TimestampToRFC3339(commit_timestamp));
  EXPECT_EQ(record.record_sequence.string_value(), "00000000");
  EXPECT_EQ(record.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  EXPECT_EQ(record.table_name.string_value(), "Users");
  EXPECT_EQ(record.mod_type.string_value(), "INSERT");
  EXPECT_EQ(record.value_capture_type.string_value(), "NEW_ROW_AND_OLD_VALUES");
  EXPECT_EQ(record.number_of_records_in_transaction.string_value(), "1");
  EXPECT_EQ(record.number_of_partitions_in_transaction.string_value(), "1");
  EXPECT_EQ(record.transaction_tag.string_value(), "");
  EXPECT_EQ(record.is_system_transaction.bool_value(), false);
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Name" }
                           values { string_value: "{\"code\":\"STRING\"}" }
                           values { bool_value: false }
                           values { string_value: "2" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Age" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: false }
                           values { string_value: "3" }
                         }
                       })pb"));
  EXPECT_THAT(
      record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"1\"}" }
                   values { string_value: "{\"Age\":null,\"Name\":\"name1\"}" }
                   values { string_value: "{}" }
                 }
               })pb"));
}

TEST_F(ChangeStreamTest,
       SingleDeleteVerifyDataChangeRecordContentNewRowAndOldValues) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"(
          ALTER CHANGE STREAM StreamAll SET OPTIONS
          ( value_capture_type = 'NEW_ROW_AND_OLD_VALUES' )
        )",
  }));
  auto mutation_builder_insert =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "name1"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  ZETASQL_EXPECT_OK(Commit({mutation_builder_insert.Build()}));
  // Delete an existing row generates a DELETE record
  auto mutation_builder_delete = DeleteMutationBuilder(
      "Users", KeySet().AddKey(cloud::spanner::MakeKey(1)));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_delete.Build()}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamAll"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.commit_timestamp.string_value(),
            cloud::spanner_internal::TimestampToRFC3339(commit_timestamp));
  EXPECT_EQ(record.record_sequence.string_value(), "00000000");
  EXPECT_EQ(record.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  EXPECT_EQ(record.table_name.string_value(), "Users");
  EXPECT_EQ(record.mod_type.string_value(), "DELETE");
  EXPECT_EQ(record.value_capture_type.string_value(), "NEW_ROW_AND_OLD_VALUES");
  EXPECT_EQ(record.number_of_records_in_transaction.string_value(), "1");
  EXPECT_EQ(record.number_of_partitions_in_transaction.string_value(), "1");
  EXPECT_EQ(record.transaction_tag.string_value(), "");
  EXPECT_EQ(record.is_system_transaction.bool_value(), false);
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Name" }
                           values { string_value: "{\"code\":\"STRING\"}" }
                           values { bool_value: false }
                           values { string_value: "2" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Age" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: false }
                           values { string_value: "3" }
                         }
                       })pb"));
  EXPECT_THAT(
      record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"1\"}" }
                   values { string_value: "{}" }
                   values { string_value: "{\"Age\":null,\"Name\":\"name1\"}" }
                 }
               })pb"));
}

TEST_F(ChangeStreamTest,
       SingleUpdateVerifyDataChangeRecordContentNewRowAndOldValues) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"(
          ALTER CHANGE STREAM StreamAll SET OPTIONS
          ( value_capture_type = 'NEW_ROW_AND_OLD_VALUES' )
        )",
  }));
  auto mutation_builder_insert =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{1, "name1"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  ZETASQL_EXPECT_OK(Commit({mutation_builder_insert.Build()}));
  auto mutation_builder_update =
      UpdateMutationBuilder("Users", {"UserId", "Name"});
  rows = {{1, "name1Update"}};
  for (const auto& row : rows) {
    mutation_builder_update.AddRow(row);
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_update.Build()}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamAll"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.commit_timestamp.string_value(),
            cloud::spanner_internal::TimestampToRFC3339(commit_timestamp));
  EXPECT_EQ(record.record_sequence.string_value(), "00000000");
  EXPECT_EQ(record.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  EXPECT_EQ(record.table_name.string_value(), "Users");
  EXPECT_EQ(record.mod_type.string_value(), "UPDATE");
  EXPECT_EQ(record.value_capture_type.string_value(), "NEW_ROW_AND_OLD_VALUES");
  EXPECT_EQ(record.number_of_records_in_transaction.string_value(), "1");
  EXPECT_EQ(record.number_of_partitions_in_transaction.string_value(), "1");
  EXPECT_EQ(record.transaction_tag.string_value(), "");
  EXPECT_EQ(record.is_system_transaction.bool_value(), false);
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Name" }
                           values { string_value: "{\"code\":\"STRING\"}" }
                           values { bool_value: false }
                           values { string_value: "2" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Age" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: false }
                           values { string_value: "3" }
                         }
                       })pb"));
  EXPECT_THAT(
      record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"1\"}" }
                   values {
                     string_value: "{\"Age\":null,\"Name\":\"name1Update\"}"
                   }
                   values { string_value: "{\"Name\":\"name1\"}" }
                 }
               })pb"));
}

TEST_F(ChangeStreamTest,
       NewRowAndOldValues_UntrackedUpdatedColumnsAreNotPopulatedInOldValues) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"(
          ALTER CHANGE STREAM StreamSpecifiedColumns SET OPTIONS ( value_capture_type = 'NEW_ROW_AND_OLD_VALUES' )
        )",
  }));
  auto mutation_builder_insert =
      InsertMutationBuilder("Users2", {"UserId", "Name", "Gender"});
  std::vector<ValueRow> rows = {{1, "name1", "F"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  ZETASQL_EXPECT_OK(Commit({mutation_builder_insert.Build()}));
  auto mutation_builder_update =
      UpdateMutationBuilder("Users2", {"UserId", "Name", "Gender"});
  rows = {{1, "name1Update", "M"}};
  for (const auto& row : rows) {
    mutation_builder_update.AddRow(row);
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_update.Build()}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamSpecifiedColumns"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.commit_timestamp.string_value(),
            cloud::spanner_internal::TimestampToRFC3339(commit_timestamp));
  EXPECT_EQ(record.record_sequence.string_value(), "00000000");
  EXPECT_EQ(record.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  EXPECT_EQ(record.table_name.string_value(), "Users2");
  EXPECT_EQ(record.mod_type.string_value(), "UPDATE");
  EXPECT_EQ(record.value_capture_type.string_value(), "NEW_ROW_AND_OLD_VALUES");
  EXPECT_EQ(record.number_of_records_in_transaction.string_value(), "1");
  EXPECT_EQ(record.number_of_partitions_in_transaction.string_value(), "1");
  EXPECT_EQ(record.transaction_tag.string_value(), "");
  EXPECT_EQ(record.is_system_transaction.bool_value(), false);
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Name" }
                           values { string_value: "{\"code\":\"STRING\"}" }
                           values { bool_value: false }
                           values { string_value: "3" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Age" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: false }
                           values { string_value: "4" }
                         }
                       })pb"));
  EXPECT_THAT(
      record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"1\"}" }
                   values {
                     string_value: "{\"Age\":null,\"Name\":\"name1Update\"}"
                   }
                   values { string_value: "{\"Name\":\"name1\"}" }
                 }
               })pb"));
}

TEST_F(ChangeStreamTest,
       NewRowAndOldValues_UntrackedDeletedColumnsAreNotPopulatedInOldValues) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"(
          ALTER CHANGE STREAM StreamSpecifiedColumns SET OPTIONS
          ( value_capture_type = 'NEW_ROW_AND_OLD_VALUES' )
        )",
  }));
  auto mutation_builder_insert =
      InsertMutationBuilder("Users2", {"UserId", "Name", "Gender"});
  std::vector<ValueRow> rows = {{1, "name1", "F"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  ZETASQL_EXPECT_OK(Commit({mutation_builder_insert.Build()}));
  // Delete an existing row generates a DELETE record
  auto mutation_builder_delete = DeleteMutationBuilder(
      "Users2", KeySet().AddKey(cloud::spanner::MakeKey(1)));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_delete.Build()}));
  Timestamp commit_timestamp = commit_result.commit_timestamp;
  absl::Time query_start_time = commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamSpecifiedColumns"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.commit_timestamp.string_value(),
            cloud::spanner_internal::TimestampToRFC3339(commit_timestamp));
  EXPECT_EQ(record.record_sequence.string_value(), "00000000");
  EXPECT_EQ(record.is_last_record_in_transaction_in_partition.bool_value(),
            true);
  EXPECT_EQ(record.table_name.string_value(), "Users2");
  EXPECT_EQ(record.mod_type.string_value(), "DELETE");
  EXPECT_EQ(record.value_capture_type.string_value(), "NEW_ROW_AND_OLD_VALUES");
  EXPECT_EQ(record.number_of_records_in_transaction.string_value(), "1");
  EXPECT_EQ(record.number_of_partitions_in_transaction.string_value(), "1");
  EXPECT_EQ(record.transaction_tag.string_value(), "");
  EXPECT_EQ(record.is_system_transaction.bool_value(), false);
  EXPECT_THAT(record.column_types,
              test::EqualsProto(
                  R"pb(values {
                         list_value {
                           values { string_value: "UserId" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: true }
                           values { string_value: "1" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Name" }
                           values { string_value: "{\"code\":\"STRING\"}" }
                           values { bool_value: false }
                           values { string_value: "3" }
                         }
                       }
                       values {
                         list_value {
                           values { string_value: "Age" }
                           values { string_value: "{\"code\":\"INT64\"}" }
                           values { bool_value: false }
                           values { string_value: "4" }
                         }
                       })pb"));
  EXPECT_THAT(
      record.mods,
      test::EqualsProto(
          R"pb(values {
                 list_value {
                   values { string_value: "{\"UserId\":\"1\"}" }
                   values { string_value: "{}" }
                   values { string_value: "{\"Age\":null,\"Name\":\"name1\"}" }
                 }
               })pb"));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
