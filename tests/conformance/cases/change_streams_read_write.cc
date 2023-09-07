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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "google/spanner/v1/result_set.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/substitute.h"
#include "absl/time/time.h"
#include "google/cloud/spanner/mutations.h"
#include "google/cloud/spanner/timestamp.h"
#include "common/clock.h"
#include "tests/common/change_streams.h"
#include "tests/common/chunking.h"
#include "tests/conformance/common/database_test_base.h"
#include "grpcpp/client_context.h"
#include "grpcpp/support/sync_stream.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {
using InsertMutationBuilder = cloud::spanner::InsertMutationBuilder;
class ChangeStreamTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    ZETASQL_RETURN_IF_ERROR(SetSchema({
        R"(
          CREATE TABLE Users(
            UserId     INT64 NOT NULL,
            Name       STRING(MAX),
            Age        INT64,
          ) PRIMARY KEY (UserId)
        )",
        R"(
          CREATE TABLE ScalarTypesTable (
            intVal INT64 NOT NULL,
            boolVal BOOL,
            bytesVal BYTES(MAX),
            dateVal DATE,
            floatVal FLOAT64,
            stringVal STRING(MAX),
            numericVal NUMERIC,
            timestampVal TIMESTAMP,
            jsonVal JSON,
          ) PRIMARY KEY(intVal)
        )",
        R"(
          CREATE CHANGE STREAM StreamAll FOR ALL
        )",
        R"(
          CREATE CHANGE STREAM StreamUsers FOR Users OPTIONS ( value_capture_type = 'NEW_VALUES' )
        )",
        R"(
          CREATE CHANGE STREAM StreamScalarTypes FOR ScalarTypesTable
        )",
    }));
    ZETASQL_ASSIGN_OR_RETURN(test_session_uri_, CreateTestSession());
    return absl::OkStatus();
  }

 protected:
  std::string test_session_uri_;
  // Creates a new session for tests using raw grpc client.
  absl::StatusOr<std::string> CreateTestSession() {
    grpc::ClientContext context;
    spanner_api::CreateSessionRequest request;
    spanner_api::Session response;
    request.set_database(database()->FullName());
    ZETASQL_RETURN_IF_ERROR(raw_client()->CreateSession(&context, request, &response));
    return response.name();
  }

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
      std::string sql) {
    // Build the request that will be executed.
    spanner_api::ExecuteSqlRequest request;
    request.mutable_transaction()
        ->mutable_single_use()
        ->mutable_read_only()
        ->set_strong(true);
    *request.mutable_sql() = sql;
    request.set_session(test_session_uri_);

    // Execute the tvf query with ExecuteStreamingSql API.
    std::vector<spanner_api::PartialResultSet> response;
    grpc::ClientContext context;
    auto client_reader = raw_client()->ExecuteStreamingSql(&context, request);
    ZETASQL_EXPECT_OK(ReadFromClientReader(std::move(client_reader), &response));

    ZETASQL_ASSIGN_OR_RETURN(auto result_set, backend::test::MergePartialResultSets(
                                          response, /*columns_per_row=*/1));
    ZETASQL_ASSIGN_OR_RETURN(ChangeStreamRecords change_records,
                     GetChangeStreamRecordsFromResultSet(result_set));
    return change_records;
  }

  absl::StatusOr<std::vector<std::string>> GetActiveTokenFromInitialQuery(
      absl::Time start, std::string change_stream_name) {
    std::vector<std::string> active_tokens;
    std::string sql = absl::Substitute(
        "SELECT * FROM "
        "READ_$0 ('$1',NULL, NULL, 300000 )",
        change_stream_name, start);
    ZETASQL_ASSIGN_OR_RETURN(test::ChangeStreamRecords change_records,
                     ExecuteChangeStreamQuery(sql));
    for (const auto& child_partition_record :
         change_records.child_partition_records) {
      for (const auto& child_partition :
           child_partition_record.child_partitions.values()) {
        active_tokens.push_back(
            child_partition.list_value().values(0).string_value());
      }
    }
    return active_tokens;
  }

  absl::StatusOr<std::vector<DataChangeRecord>> GetDataRecordsFromStartToNow(
      absl::Time start, std::string change_stream_name) {
    ZETASQL_ASSIGN_OR_RETURN(std::vector<std::string> active_tokens,
                     GetActiveTokenFromInitialQuery(start, change_stream_name));
    std::vector<DataChangeRecord> merged_data_change_records;
    for (const auto& partition_token : active_tokens) {
      std::string sql = absl::Substitute(
          "SELECT * FROM "
          "READ_$0 ('$1','$2', '$3', 300000 )",
          change_stream_name, start, Clock().Now(), partition_token);
      ZETASQL_ASSIGN_OR_RETURN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql));
      merged_data_change_records.insert(
          merged_data_change_records.end(),
          change_records.data_change_records.begin(),
          change_records.data_change_records.end());
    }
    return merged_data_change_records;
  }
};

TEST_F(ChangeStreamTest, SingleInsertYieldsOneRecord) {
  auto mutation_builder_insert =
      InsertMutationBuilder("Users", {"UserId", "Name"});
  std::vector<ValueRow> rows = {{4, "Foo Bar"}};
  for (const auto& row : rows) {
    mutation_builder_insert.AddRow(row);
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_result,
                       Commit({mutation_builder_insert.Build()}));
  absl::Time query_start_time =
      commit_result.commit_timestamp.get<absl::Time>().value();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto data_change_records,
      GetDataRecordsFromStartToNow(query_start_time, "StreamUsers"));
  ASSERT_EQ(data_change_records.size(), 1);
  DataChangeRecord record = data_change_records[0];
  EXPECT_EQ(record.table_name.string_value(), "Users");
  EXPECT_EQ(record.mod_type.string_value(), "INSERT");
  EXPECT_EQ(record.mods.values_size(), 1);
}

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
          CREATE CHANGE STREAM StreamSinger FOR Singer OPTIONS ( value_capture_type = 'NEW_VALUES' )
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

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
