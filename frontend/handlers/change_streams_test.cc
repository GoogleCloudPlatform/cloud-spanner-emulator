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

#include <string>
#include <thread>  // NOLINT
#include <vector>

#include "google/protobuf/struct.pb.h"
#include "google/spanner/v1/commit_response.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/time/time.h"
#include "backend/schema/updater/schema_updater.h"
#include "common/clock.h"
#include "frontend/handlers/change_streams.h"
#include "tests/common/chunking.h"
#include "tests/common/proto_matchers.h"
#include "tests/common/test_env.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {
namespace database_api = ::google::spanner::admin::database::v1;
namespace operations_api = ::google::longrunning;
namespace spanner_api = ::google::spanner::v1;
using testing::AnyOf;
using zetasql_base::testing::StatusIs;

class ChangeStreamQueryAPITest : public test::ServerTest {
 public:
  void SetUp() override {
    absl::SetFlag(
        &FLAGS_cloud_spanner_emulator_read_mock_change_streams_internal_tables,
        true);
    ZETASQL_ASSERT_OK(CreateTestInstance());
    ZETASQL_ASSERT_OK(CreateTestDatabaseWithChangeStream());
    ZETASQL_ASSERT_OK_AND_ASSIGN(test_session_uri_, CreateTestSession());
    now_ = Clock().Now();
    now_str_ = test::EncodeTimestampString(now_);
    ZETASQL_ASSERT_OK(PopulatePartitionTable());
    ZETASQL_ASSERT_OK(PopulateDataTable());
    null_end_initial_query_ = absl::Substitute(
        "SELECT * FROM "
        "READ_change_stream_test_table ('$0', NULL, NULL, 10000 )",
        now_);
    transaction_selector_err_msg_ =
        "Change stream queries must be strong reads "
        "executed via single use transactions using the ExecuteStreamingSql "
        "API.";
  }

  std::string test_session_uri_;
  std::string null_end_initial_query_;
  std::string transaction_selector_err_msg_;
  absl::Time now_;
  std::string now_str_;
  absl::StatusOr<test::ChangeStreamRecords> ExecuteChangeStreamQuery(
      const std::string& sql) {
    spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(absl::Substitute(
        R"pb(
          transaction {}
          sql: "$0"
        )pb",
        sql));
    request.set_session(test_session_uri_);

    std::vector<spanner_api::PartialResultSet> response;
    ZETASQL_RETURN_IF_ERROR(ExecuteStreamingSql(request, &response));
    for (int i = 0; i < response.size(); ++i) {
      ZETASQL_RET_CHECK(i == 0 ? response[i].has_metadata()
                       : !response[i].has_metadata());
    }
    ZETASQL_ASSIGN_OR_RETURN(auto result_set, backend::test::MergePartialResultSets(
                                          response, /*columns_per_row=*/1));
    ZETASQL_ASSIGN_OR_RETURN(test::ChangeStreamRecords change_records,
                     test::GetChangeStreamRecordsFromResultSet(result_set));
    return change_records;
  }
  absl::Status UpdateDatabaseDdl(
      const std::string& database_uri,
      const std::vector<std::string>& update_statements,
      database_api::UpdateDatabaseDdlMetadata* metadata = nullptr) {
    grpc::ClientContext context;
    database_api::UpdateDatabaseDdlRequest request;
    request.set_database(database_uri);
    for (auto statement : update_statements) {
      request.add_statements(statement);
    }
    operations_api::Operation operation;
    ZETASQL_RETURN_IF_ERROR(test_env()->database_admin_client()->UpdateDatabaseDdl(
        &context, request, &operation));
    ZETASQL_RETURN_IF_ERROR(WaitForOperation(operation.name(), &operation));
    if (metadata) {
      ZETASQL_RET_CHECK(operation.metadata().UnpackTo(metadata));
    }
    google::rpc::Status status = operation.error();
    return absl::Status(static_cast<absl::StatusCode>(status.code()),
                        status.message());
  }

  absl::Status DropChangeStream() {
    database_api::UpdateDatabaseDdlMetadata metadata;
    std::vector<std::string> statements = {R"(
     DROP CHANGE STREAM change_stream_test_table
  )"};
    return UpdateDatabaseDdl(test_database_uri_, statements, &metadata);
  }

  absl::Status ChangeRetentionToZeroSec() {
    absl::SetFlag(&FLAGS_cloud_spanner_emulator_disable_cs_retention_check,
                  true);
    database_api::UpdateDatabaseDdlMetadata metadata;
    std::vector<std::string> statements = {R"(
     ALTER CHANGE STREAM change_stream_test_table SET OPTIONS ( retention_period='0s' )
  )"};
    return UpdateDatabaseDdl(test_database_uri_, statements, &metadata);
  }

  // Populate a fake partition table for testing purpose before partition token
  // churning is done. In this mocked table the parent-children relationship is
  // : historical_token1 -> initial_token1 -> future_token1 .
  absl::Status PopulatePartitionTable() {
    spanner_api::CommitRequest commit_request =
        PARSE_TEXT_PROTO(absl::Substitute(
            R"pb(
              single_use_transaction { read_write {} }
              mutations {
                insert {
                  table: "partition_table"
                  columns: "partition_token"
                  columns: "start_time"
                  columns: "end_time"
                  columns: "parents"
                  columns: "children"
                  values {
                    values { string_value: "initial_token1" }
                    values { string_value: "$0" }
                    values { string_value: "$1" }
                    values {
                      list_value: {
                        values { string_value: "historical_token1" }
                      }
                    }
                    values {
                      list_value: { values { string_value: "future_token1" } }
                    }
                  }
                  values {
                    values { string_value: "initial_token2" }
                    values { string_value: "$0" }
                    values { string_value: "$1" }
                    values {
                      list_value: { values
                                    [] }
                    }
                    values {
                      list_value: { values
                                    [] }
                    }
                  }
                  values {
                    values { string_value: "historical_token1" }
                    values { string_value: "$2" }
                    values { string_value: "$3" }
                    values {
                      list_value: { values
                                    [] }
                    }
                    values {
                      list_value: { values { string_value: "initial_token1" } }
                    }
                  }
                  values {
                    values { string_value: "null_end_token" }
                    values { string_value: "$2" }
                    values { null_value: NULL_VALUE }
                    values {
                      list_value: { values
                                    [] }
                    }
                    values {
                      list_value: { values
                                    [] }
                    }
                  }
                  values {
                    values { string_value: "future_token1" }
                    values { string_value: "$4" }
                    values { string_value: "$5" }
                    values {
                      list_value: { values { string_value: "initial_token1" } }
                    }
                    values {
                      list_value: { values
                                    [] }
                    }
                  }
                }
              }
            )pb",
            test::EncodeTimestampString(now_),
            test::EncodeTimestampString(now_ + absl::Seconds(1)),
            test::EncodeTimestampString(now_ - absl::Seconds(1)),
            test::EncodeTimestampString(now_),
            test::EncodeTimestampString(now_ + absl::Seconds(1)),
            test::EncodeTimestampString(now_ + absl::Seconds(2))));
    *commit_request.mutable_session() = test_session_uri_;
    spanner_api::CommitResponse commit_response;
    return Commit(commit_request, &commit_response);
  }

  std::string ConstructFakeDataChangeRecord(
      absl::string_view partition_token, absl::string_view record_sequence,
      absl::Duration commit_duration_from_now) {
    return absl::Substitute(
        R"pb(
          values { string_value: "$0" }
          values { string_value: "$1" }
          values { string_value: "$2" }
          values { string_value: "test_id" }
          values { bool_value: false }
          values { string_value: "test_table" }
          values {
            list_value {
                values
                [ {
                  string_value: "{ \"name\": \"IsPrimaryUser\", \"type\": \"{ \\\"code\\\": \\\"BOOL\\\" }\",\"is_primary_key\": false, \"ordinal_position\": 1 }"
                }
                  , {
                    string_value: "{ \"name\": \"UserId\", \"type\": \"{ \\\"code\\\": \\\"STRING\\\" }\",\"is_primary_key\": true, \"ordinal_position\": 2 }"
                  }] }
          }
          values {
            list_value {
                values
                [ {
                  string_value: "{ \"keys\" : \"{\\\"UserId\\\": \\\"User2\\\"}\", \"new_values\": \"{ \\\"IsPrimaryUser\\\": true, \\\"UserId\\\": \\\"User2\\\" }\", \"old_values\": \"{}\" }"
                }
                  , {
                    string_value: "{ \"keys\" : \"{\\\"UserId\\\": \\\"User2\\\"}\", \"new_values\": \"{ \\\"IsPrimaryUser\\\": true }\", \"old_values\": \"{}\" }"
                  }] }
          }
          values { string_value: "UPDATE" }
          values { string_value: "NEW_VALUES" }
          values { string_value: "3" }
          values { string_value: "2" }
          values { string_value: "test_tag" }
          values { bool_value: false })pb",
        partition_token,
        test::EncodeTimestampString(now_ + commit_duration_from_now),
        record_sequence);
  }

  // Populate a fake data table for testing purpose before write effect is done.
  absl::Status InsertDataChangeRecord() {
    spanner_api::CommitRequest commit_request =
        PARSE_TEXT_PROTO(absl::Substitute(
            R"pb(
              single_use_transaction { read_write {} }
              mutations {
                insert {
                  table: "data_table"
                  columns: "partition_token"
                  columns: "commit_timestamp"
                  columns: "record_sequence"
                  columns: "server_transaction_id"
                  columns: "is_last_record_in_transaction_in_partition"
                  columns: "table_name"
                  columns: "column_types"
                  columns: "mods"
                  columns: "mod_type"
                  columns: "value_capture_type"
                  columns: "number_of_records_in_transaction"
                  columns: "number_of_partitions_in_transaction"
                  columns: "transaction_tag"
                  columns: "is_system_transaction"
                  values { $0 }
                }
              }
            )pb",
            ConstructFakeDataChangeRecord("initial_token1", "00000001",
                                          absl::ZeroDuration())));
    *commit_request.mutable_session() = test_session_uri_;
    spanner_api::CommitResponse commit_response;
    return Commit(commit_request, &commit_response);
  }

  // Populate a fake data table for testing purpose before write effect is done.
  absl::Status PopulateDataTable() {
    spanner_api::CommitRequest commit_request =
        PARSE_TEXT_PROTO(absl::Substitute(
            R"pb(
              single_use_transaction { read_write {} }
              mutations {
                insert {
                  table: "data_table"
                  columns: "partition_token"
                  columns: "commit_timestamp"
                  columns: "record_sequence"
                  columns: "server_transaction_id"
                  columns: "is_last_record_in_transaction_in_partition"
                  columns: "table_name"
                  columns: "column_types"
                  columns: "mods"
                  columns: "mod_type"
                  columns: "value_capture_type"
                  columns: "number_of_records_in_transaction"
                  columns: "number_of_partitions_in_transaction"
                  columns: "transaction_tag"
                  columns: "is_system_transaction"
                  values { $0 }
                  values { $1 }
                  values { $2 }
                  values { $3 }
                  values { $4 }
                }
              }
            )pb",
            ConstructFakeDataChangeRecord("historical_token1", "00000001",
                                          absl::Microseconds(-2)),
            ConstructFakeDataChangeRecord("historical_token1", "00000002",
                                          absl::Microseconds(-1)),
            ConstructFakeDataChangeRecord("initial_token1", "00000001",
                                          absl::Microseconds(2)),
            ConstructFakeDataChangeRecord("initial_token1", "00000002",
                                          absl::Microseconds(1)),
            ConstructFakeDataChangeRecord("null_end_token", "00000002",
                                          absl::Microseconds(-2))));
    *commit_request.mutable_session() = test_session_uri_;
    spanner_api::CommitResponse commit_response;
    return Commit(commit_request, &commit_response);
  }
};

TEST_F(ChangeStreamQueryAPITest,
       CanReadWithSingleUseReadOnlyStrongTransaction) {
  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(absl::Substitute(
      R"pb(
        transaction { single_use { read_only { strong: true } } }
        sql: "$0"
      )pb",
      null_end_initial_query_));
  request.set_session(test_session_uri_);

  std::vector<spanner_api::PartialResultSet> response;
  ZETASQL_EXPECT_OK(ExecuteStreamingSql(request, &response));
}

TEST_F(ChangeStreamQueryAPITest, CanReadWithEmptyDefaultTransactionSelector) {
  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(absl::Substitute(
      R"pb(
        transaction {}
        sql: "$0"
      )pb",
      null_end_initial_query_));
  request.set_session(test_session_uri_);

  std::vector<spanner_api::PartialResultSet> response;
  ZETASQL_EXPECT_OK(ExecuteStreamingSql(request, &response));
}

TEST_F(ChangeStreamQueryAPITest, CannotReadWithReadWriteTransaction) {
  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(absl::Substitute(
      R"pb(
        transaction { begin { read_write {} } }
        sql: "$0"
      )pb",
      null_end_initial_query_));
  request.set_session(test_session_uri_);

  std::vector<spanner_api::PartialResultSet> response;
  EXPECT_THAT(ExecuteStreamingSql(request, &response),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr(transaction_selector_err_msg_)));
}

TEST_F(ChangeStreamQueryAPITest, CannotReadWithPartitionedDmlTransaction) {
  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(absl::Substitute(
      R"pb(
        transaction { begin { partitioned_dml {} } }
        sql: "$0"
      )pb",
      null_end_initial_query_));
  request.set_session(test_session_uri_);

  std::vector<spanner_api::PartialResultSet> response;
  EXPECT_THAT(ExecuteStreamingSql(request, &response),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr(transaction_selector_err_msg_)));
}

TEST_F(ChangeStreamQueryAPITest, CannotReadUsingReadOnlyStrongTransaction) {
  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(absl::Substitute(
      R"pb(
        transaction { begin { read_only { strong: true } } }
        sql: "$0"
      )pb",
      null_end_initial_query_));
  request.set_session(test_session_uri_);

  std::vector<spanner_api::PartialResultSet> response;
  EXPECT_THAT(ExecuteStreamingSql(request, &response),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr(transaction_selector_err_msg_)));
}

TEST_F(ChangeStreamQueryAPITest,
       CannotReadUsingSingleUseReadOnlyNonStrongTransaction) {
  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(absl::Substitute(
      R"pb(
        transaction { begin { read_only { exact_staleness { seconds: 1 } } } }
        sql: "$0"
      )pb",
      null_end_initial_query_));
  request.set_session(test_session_uri_);

  std::vector<spanner_api::PartialResultSet> response;
  EXPECT_THAT(ExecuteStreamingSql(request, &response),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr(transaction_selector_err_msg_)));
}
TEST_F(ChangeStreamQueryAPITest, RejectsPlanMode) {
  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(absl::Substitute(
      R"pb(
        transaction { single_use { read_only { strong: true } } }
        query_mode: PLAN
        sql: "$0"
      )pb",
      null_end_initial_query_));
  request.set_session(test_session_uri_);
  std::vector<spanner_api::PartialResultSet> response;
  EXPECT_THAT(ExecuteStreamingSql(request, &response),
              StatusIs(absl::StatusCode::kUnimplemented));
}

TEST_F(ChangeStreamQueryAPITest, RejectsRequestWithValidQueryIfNonStreaming) {
  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(absl::Substitute(
      R"pb(
        transaction { single_use { read_only { strong: true } } }
        query_mode: PLAN
        sql: "$0"
      )pb",
      null_end_initial_query_));
  request.set_session(test_session_uri_);

  spanner_api::ResultSet response;
  EXPECT_THAT(ExecuteSql(request, &response),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr("Change stream queries are not "
                                          "supported for the ExecuteSql API")));
}

TEST_F(ChangeStreamQueryAPITest, RejectsRequestWithInvalidQueryIfNonStreaming) {
  std::string invalid_query =
      "SELECT * FROM "
      "READ_change_stream_test_table ( NULL, "
      "NULL, NULL, NULL )";
  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(absl::Substitute(
      R"pb(
        transaction { single_use { read_only { strong: true } } }
        query_mode: PLAN
        sql: "$0"
      )pb",
      null_end_initial_query_));
  request.set_session(test_session_uri_);

  spanner_api::ResultSet response;
  EXPECT_THAT(ExecuteSql(request, &response),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr("Change stream queries are not "
                                          "supported for the ExecuteSql API")));
}

TEST_F(ChangeStreamQueryAPITest, ExecuteQueryOnJustDroppedChangeStream) {
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', NULL, NULL, 10000 )",
      now_ + absl::Seconds(1));
  std::thread drop_change_stream(&ChangeStreamQueryAPITest::DropChangeStream,
                                 this);
  // Occasionally due to latency in unit test it is possible that drop thread
  // finishes before ExecuteStreamingSql handler finishes validating query. To
  // avoid waiting too long in unit test, we just test the error status is
  // kInvalidArgument(tvf not found) if drop thread finishes too quickly, or
  // kNotFound(change stream not found) if drop thread finishes after initial
  // tvf query validation).
  EXPECT_THAT(ExecuteChangeStreamQuery(sql),
              StatusIs(AnyOf(absl::StatusCode::kNotFound,
                             absl::StatusCode::kInvalidArgument)));
  drop_change_stream.join();
}

TEST_F(ChangeStreamQueryAPITest,
       ExecuteQueryStartTimeTooFarInTheFutureInvalid) {
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', NULL, NULL, 10000 )",
      now_ + absl::Minutes(11));
  EXPECT_THAT(
      ExecuteChangeStreamQuery(sql),
      StatusIs(absl::StatusCode::kOutOfRange,
               testing::HasSubstr(
                   "Specified start_timestamp is too far in the future.")));
}

TEST_F(ChangeStreamQueryAPITest, ExecuteQueryStartTimeTooOldInThePastInvalid) {
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', NULL, NULL, 10000 )",
      now_ - absl::Minutes(10));
  EXPECT_THAT(
      ExecuteChangeStreamQuery(sql),
      StatusIs(absl::StatusCode::kOutOfRange,
               testing::HasSubstr(
                   "Specified start_timestamp is too far in the past.")));
}

TEST_F(ChangeStreamQueryAPITest, ExecuteQueryStartTimeNotProvidedInvalid) {
  std::string sql =
      "SELECT * FROM "
      "READ_change_stream_test_table (NULL, NULL, NULL, 10000 )";
  EXPECT_THAT(
      ExecuteChangeStreamQuery(sql),
      StatusIs(absl::StatusCode::kInvalidArgument,
               testing::HasSubstr("start_timestamp must not be null.")));
}

TEST_F(ChangeStreamQueryAPITest,
       ExecutePartitionQueryWithNonExistedPartitionToken) {
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', NULL , 'null_token', 10000 )",
      now_);
  EXPECT_THAT(
      ExecuteChangeStreamQuery(sql),
      StatusIs(absl::StatusCode::kInvalidArgument,
               testing::HasSubstr("Specified partition_token is invalid")));
}

TEST_F(ChangeStreamQueryAPITest,
       ExecutePartitionQueryWithInvalidPartitionStartTime) {
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', NULL , 'initial_token1', 10000 )",
      now_ + absl::Seconds(10));
  EXPECT_THAT(
      ExecuteChangeStreamQuery(sql),
      StatusIs(absl::StatusCode::kOutOfRange,
               testing::HasSubstr(
                   "Specified start_timestamp is invalid for the partition.")));
}

TEST_F(ChangeStreamQueryAPITest,
       ExecuteInitialQueryOnInitialBackfilledPartitions) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(null_end_initial_query_));
  EXPECT_THAT(change_records.child_partition_records.at(0)
                  .child_partitions.values_size(),
              1);
  EXPECT_THAT(change_records.child_partition_records.at(1)
                  .child_partitions.values_size(),
              1);
  EXPECT_EQ(change_records.child_partition_records.at(0)
                .record_sequence.string_value(),
            "00000000");
  EXPECT_EQ(change_records.child_partition_records.at(1)
                .record_sequence.string_value(),
            "00000001");
  // Verify that the partition token start time is the same as the user passed
  // @start_timestamp for initial query.
  EXPECT_EQ(
      change_records.child_partition_records.at(0).start_time.string_value(),
      now_str_);
  EXPECT_THAT(
      change_records.child_partition_records.at(1).start_time.string_value(),
      now_str_);
}

TEST_F(ChangeStreamQueryAPITest,
       ExecuteHistoricalPartitionQueryStartAtTokenEndTime) {
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', NULL, 'historical_token1', 10000 )",
      now_);
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql));
  ASSERT_EQ(change_records.child_partition_records.size(), 1);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 0);
}

TEST_F(ChangeStreamQueryAPITest,
       ExecuteHistoricalPartitionQueryEndAtTokenEndTime) {
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', '$1', 'historical_token1', 10000 )",
      now_ - absl::Microseconds(2), now_);
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql));
  ASSERT_EQ(change_records.child_partition_records.size(), 1);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 2);
}

TEST_F(ChangeStreamQueryAPITest,
       ExecuteHistoricalPartitionQueryEndAfterTokenEndTime) {
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', '$1', 'historical_token1', 10000 )",
      now_ - absl::Microseconds(3), now_ + absl::Microseconds(1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql));
  ASSERT_EQ(change_records.child_partition_records.size(), 1);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 2);
}

TEST_F(ChangeStreamQueryAPITest,
       ExecuteHistoricalPartitionQueryEndBeforeTokenEndTime) {
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', '$1', 'historical_token1', 10000 )",
      now_ - absl::Microseconds(3), now_ - absl::Microseconds(1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql));
  ASSERT_EQ(change_records.child_partition_records.size(), 0);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 2);
}

TEST_F(ChangeStreamQueryAPITest,
       ExecuteHistoricalPartitionQuerySameStartAndEndTime) {
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', '$1', 'historical_token1', 10000 )",
      now_ - absl::Microseconds(1), now_ - absl::Microseconds(1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql));
  ASSERT_EQ(change_records.child_partition_records.size(), 0);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 1);
}

TEST_F(ChangeStreamQueryAPITest,
       ExecuteHistoricalPartitionQueryOnNullEndPartitionToken) {
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', '$1', 'null_end_token', 10000 )",
      now_ - absl::Microseconds(2), now_ - absl::Microseconds(1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql));
  ASSERT_EQ(change_records.child_partition_records.size(), 0);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 1);
}

TEST_F(ChangeStreamQueryAPITest, ExecuteRealTimePartitionQueryWithStaleToken) {
  ZETASQL_ASSERT_OK(ChangeRetentionToZeroSec());
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', NULL , 'initial_token1', 10000 )",
      now_ + absl::Seconds(1));
  EXPECT_THAT(
      ExecuteChangeStreamQuery(sql),
      StatusIs(absl::StatusCode::kOutOfRange,
               testing::HasSubstr("This partition is no longer valid.")));
}

TEST_F(ChangeStreamQueryAPITest,
       ExecuteRealTimePartitionQueryStartAtTokenEndTime) {
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', NULL , 'initial_token1', 10000 )",
      now_ + absl::Seconds(1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql));
  ASSERT_EQ(change_records.child_partition_records.size(), 1);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 0);
}

TEST_F(ChangeStreamQueryAPITest,
       ExecuteRealTimePartitionQueryEndAtTokenEndTime) {
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', '$1' , 'initial_token1', 10000 )",
      now_, now_ + absl::Seconds(1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql));
  ASSERT_EQ(change_records.child_partition_records.size(), 1);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 2);
}

TEST_F(ChangeStreamQueryAPITest,
       ExecuteRealTimePartitionQueryEndBeforeTokenEndTime) {
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', '$1' , 'initial_token1', 10000 )",
      now_, now_ + absl::Microseconds(50));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql));
  ASSERT_EQ(change_records.child_partition_records.size(), 0);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 2);
}

TEST_F(ChangeStreamQueryAPITest,
       ExecuteRealTimePartitionQueryEndAfterTokenEndTime) {
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', '$1' , 'initial_token1', 10000 )",
      now_, now_ + absl::Milliseconds(1100));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql));
  ASSERT_EQ(change_records.child_partition_records.size(), 1);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 2);
}

TEST_F(ChangeStreamQueryAPITest,
       ExecuteRealTimePartitionQuerySameStartAndEndTime) {
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', '$1' , 'initial_token1', 10000 )",
      now_ + absl::Microseconds(1), now_ + absl::Microseconds(1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql));
  ASSERT_EQ(change_records.child_partition_records.size(), 0);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 1);
}

TEST_F(ChangeStreamQueryAPITest,
       ExecuteRealTimePartitionQueryOnNullEndPartitionToken) {
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', '$1', 'null_end_token', 10000 )",
      now_ - absl::Microseconds(2), now_ + absl::Microseconds(500));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql));
  ASSERT_EQ(change_records.child_partition_records.size(), 0);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 1);
}

TEST_F(ChangeStreamQueryAPITest,
       ExecuteRealTimePartitionQueryWithMultipleHeartbeatRecords) {
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', '$1' , 'initial_token1', 100 )",
      now_, now_ + absl::Seconds(1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql));
  ASSERT_EQ(change_records.child_partition_records.size(), 1);
  ASSERT_EQ(change_records.heartbeat_records.size(), 8);
  ASSERT_EQ(change_records.data_change_records.size(), 2);
}

TEST_F(ChangeStreamQueryAPITest, ExecuteRealTimePartitionQueryThreaded) {
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0',NULL,'initial_token1', 10000 )",
      now_);
  std::thread insert_data_record(
      &ChangeStreamQueryAPITest::InsertDataChangeRecord, this);
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql));
  insert_data_record.join();
  ASSERT_EQ(change_records.child_partition_records.size(), 1);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 3);
}

TEST_F(ChangeStreamQueryAPITest, ExecuteRealTimePartitionQueryWithParameter) {
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0',NULL, @token , 10000 )",
      now_);
  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(absl::Substitute(
      R"pb(
        transaction { single_use { read_only { strong: true } } }
        sql: "$0"
        params {
          fields {
            key: "token"
            value { string_value: "$1" }
          }
        }
        param_types {
          key: "token"
          value { code: STRING }
        }
      )pb",
      sql, "initial_token1"));
  request.set_session(test_session_uri_);
  std::vector<spanner_api::PartialResultSet> response;
  ZETASQL_ASSERT_OK(ExecuteStreamingSql(request, &response));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto result_set, backend::test::MergePartialResultSets(
                                            response, /*columns_per_row=*/1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       test::GetChangeStreamRecordsFromResultSet(result_set));
  ASSERT_EQ(change_records.child_partition_records.size(), 1);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 2);
}

}  // namespace

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
