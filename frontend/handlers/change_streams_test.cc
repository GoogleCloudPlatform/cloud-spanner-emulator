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
#include <string>
#include <thread>  // NOLINT
#include <vector>

#include "google/protobuf/struct.pb.h"
#include "google/spanner/admin/database/v1/spanner_database_admin.pb.h"
#include "google/spanner/v1/commit_response.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/flags/flag.h"
#include "absl/flags/reflection.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/time/time.h"
#include "backend/database/change_stream/change_stream_partition_churner.h"
#include "backend/schema/updater/schema_updater.h"
#include "common/clock.h"
#include "frontend/handlers/change_streams.h"
#include "tests/common/chunking.h"
#include "tests/common/proto_matchers.h"
#include "tests/common/test_env.h"
#include "grpcpp/client_context.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

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
    // Set churning interval and churner thread sleep interval to 1 second to
    // prevent long running unit tests.
    absl::SetFlag(&FLAGS_change_stream_churning_interval,
                  absl::Milliseconds(500));
    absl::SetFlag(&FLAGS_change_stream_churn_thread_sleep_interval,
                  absl::Milliseconds(500));
    ZETASQL_ASSERT_OK(CreateTestInstance());
    ZETASQL_ASSERT_OK(CreateTestDatabaseWithChangeStream());
    ZETASQL_ASSERT_OK_AND_ASSIGN(test_session_uri_, CreateTestSession());
    now_ = Clock().Now();
    now_str_ = test::EncodeTimestampString(now_);
    ZETASQL_ASSERT_OK(PopulateTestDatabase());
    valid_initial_query_ = absl::Substitute(
        "SELECT * FROM "
        "READ_change_stream_test_table ('$0', NULL, NULL, 10000 )",
        now_);
    transaction_selector_err_msg_ =
        "Change stream queries must be strong reads "
        "executed via single use transactions using the ExecuteStreamingSql "
        "API.";
  }
  absl::FlagSaver flag_saver_;
  std::string test_session_uri_;
  std::string valid_initial_query_;
  std::string transaction_selector_err_msg_;
  absl::Time now_;
  std::string now_str_;
  absl::StatusOr<test::ChangeStreamRecords> ExecuteChangeStreamQuery(
      const std::string& sql, const std::string& transaction_selector = "{}") {
    spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(absl::Substitute(
        R"pb(
          transaction $0 sql: "$1"
        )pb",
        transaction_selector, sql));
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

  absl::Status UpdateSchema(std::vector<std::string> statements) {
    database_api::UpdateDatabaseDdlMetadata metadata;
    return UpdateDatabaseDdl(test_database_uri_, statements, &metadata);
  }

  absl::StatusOr<std::string> GetActiveTokenFromInitialQuery(absl::Time start) {
    std::string sql = absl::Substitute(
        "SELECT * FROM "
        "READ_change_stream_test_table ('$0',NULL, NULL, 10000 )",
        start);
    ZETASQL_ASSIGN_OR_RETURN(test::ChangeStreamRecords change_records,
                     ExecuteChangeStreamQuery(sql));
    std::string active_partition_token = "|";
    for (const auto& child_partition_record :
         change_records.child_partition_records) {
      for (const auto& child_partition :
           child_partition_record.child_partitions.values()) {
        active_partition_token =
            std::min(active_partition_token,
                     child_partition.list_value().values(0).string_value());
      }
    }
    return active_partition_token;
  }

  absl::Status InsertOneRow(absl::string_view table_name) {
    spanner_api::CommitRequest commit_request = PARSE_TEXT_PROTO(
        absl::Substitute(R"pb(
                           single_use_transaction { read_write {} }
                           mutations {
                             insert {
                               table: "$0"
                               columns: "int64_col"
                               columns: "string_col"
                               values {
                                 values { string_value: "2" }
                                 values { string_value: "row_2" }
                               }
                             }
                           }
                         )pb",
                         table_name));
    *commit_request.mutable_session() = test_session_uri_;

    spanner_api::CommitResponse commit_response;
    return Commit(commit_request, &commit_response);
  }

  absl::Status PopulateTestDatabase() {
    spanner_api::CommitRequest commit_request = PARSE_TEXT_PROTO(R"pb(
      single_use_transaction { read_write {} }
      mutations {
        insert {
          table: "test_table"
          columns: "int64_col"
          columns: "string_col"
          values {
            values { string_value: "1" }
            values { string_value: "row_1" }
          }
        }
      }
    )pb");
    *commit_request.mutable_session() = test_session_uri_;

    spanner_api::CommitResponse commit_response;
    return Commit(commit_request, &commit_response);
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
                    values { string_value: "$0" }
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
                    values { string_value: "$1" }
                    values { string_value: "$3" }
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
            test::EncodeTimestampString(now_ + absl::Seconds(2))));
    *commit_request.mutable_session() = test_session_uri_;
    spanner_api::CommitResponse commit_response;
    return Commit(commit_request, &commit_response);
  }
};

TEST_F(ChangeStreamQueryAPITest,
       CanReadWithSingleUseReadOnlyStrongTransaction) {
  ZETASQL_EXPECT_OK(ExecuteChangeStreamQuery(
      valid_initial_query_, "{ single_use { read_only { strong: true } } }"));
}

TEST_F(ChangeStreamQueryAPITest, CanReadWithEmptyDefaultTransactionSelector) {
  ZETASQL_EXPECT_OK(ExecuteChangeStreamQuery(valid_initial_query_, "{}"));
}

TEST_F(ChangeStreamQueryAPITest, CannotReadWithReadWriteTransaction) {
  EXPECT_THAT(ExecuteChangeStreamQuery(valid_initial_query_,
                                       "{ begin { read_write {} } }"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr(transaction_selector_err_msg_)));
}

TEST_F(ChangeStreamQueryAPITest, CannotReadWithPartitionedDmlTransaction) {
  EXPECT_THAT(ExecuteChangeStreamQuery(valid_initial_query_,
                                       "{ begin { partitioned_dml {} } }"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr(transaction_selector_err_msg_)));
}

TEST_F(ChangeStreamQueryAPITest, CannotReadUsingReadOnlyStrongTransaction) {
  EXPECT_THAT(
      ExecuteChangeStreamQuery(valid_initial_query_,
                               "{ begin { read_only { strong: true } } }"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               testing::HasSubstr(transaction_selector_err_msg_)));
}

TEST_F(ChangeStreamQueryAPITest,
       CannotReadUsingSingleUseReadOnlyNonStrongTransaction) {
  EXPECT_THAT(ExecuteChangeStreamQuery(
                  valid_initial_query_,
                  "{ begin { read_only { exact_staleness { seconds: 1 } } } }"),
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
      valid_initial_query_));
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
      valid_initial_query_));
  request.set_session(test_session_uri_);

  spanner_api::ResultSet response;
  EXPECT_THAT(ExecuteSql(request, &response),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr("Change stream queries are not "
                                          "supported for the ExecuteSql API")));
}

TEST_F(ChangeStreamQueryAPITest, RejectsRequestWithInvalidQueryIfNonStreaming) {
  std::string sql =
      "SELECT * FROM "
      "READ_change_stream_test_table ( NULL, "
      "NULL, NULL, NULL )";
  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(absl::Substitute(
      R"pb(
        transaction { single_use { read_only { strong: true } } }
        query_mode: PLAN
        sql: "$0"
      )pb",
      sql));
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
  std::thread drop_change_stream(&ChangeStreamQueryAPITest::UpdateSchema, this,
                                 std::vector<std::string>{R"(
     DROP CHANGE STREAM change_stream_test_table
  )"});
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
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::string initial_active_token,
                       GetActiveTokenFromInitialQuery(now_));
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', NULL , '$1', 10000 )",
      now_ + absl::Seconds(10), initial_active_token);
  EXPECT_THAT(
      ExecuteChangeStreamQuery(sql),
      StatusIs(absl::StatusCode::kOutOfRange,
               testing::HasSubstr(
                   "Specified start_timestamp is invalid for the partition.")));
}

TEST_F(ChangeStreamQueryAPITest,
       ExecuteInitialQueryOnInitialBackfilledPartitions) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(valid_initial_query_));
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

TEST_F(ChangeStreamQueryAPITest, ExecuteInitialQueryAfterChurned) {
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', NULL, NULL, 10000 )",
      now_ + absl::Milliseconds(1500));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql));
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
  // Verify that no parents partition records are populated for initial query.
  EXPECT_EQ(change_records.child_partition_records.at(0)
                .child_partitions.values(0)
                .list_value()
                .values(1)
                .list_value()
                .values_size(),
            0);
  EXPECT_EQ(change_records.child_partition_records.at(1)
                .child_partitions.values(0)
                .list_value()
                .values(1)
                .list_value()
                .values_size(),
            0);
}

TEST_F(ChangeStreamQueryAPITest,
       ExecuteHistoricalPartitionQueryStartAtTokenEndTime) {
  ZETASQL_ASSERT_OK(PopulatePartitionTable());
  absl::SetFlag(&FLAGS_cloud_spanner_emulator_test_with_fake_partition_table,
                true);
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

TEST_F(ChangeStreamQueryAPITest, VerifyChildPartitionsRecordContent) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto initial_active_token,
                       GetActiveTokenFromInitialQuery(now_));
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', '$1', '$2', 10000 )",
      now_, now_ + absl::Milliseconds(1100), initial_active_token);
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql));
  ASSERT_EQ(change_records.child_partition_records.size(), 1);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 1);
  auto child_partition_record = change_records.child_partition_records[0];
  // Start time of child token should be greater than start time of current
  // token.
  ASSERT_TRUE(child_partition_record.start_time.string_value() > now_str_);
  ASSERT_EQ(child_partition_record.record_sequence.string_value(), "00000000");
  for (const auto& child_partition :
       child_partition_record.child_partitions.values()) {
    // Returned child tokens should be different from the current token.
    ASSERT_NE(child_partition.list_value().values(0).string_value(),
              initial_active_token);
    for (const auto& parent_token :
         child_partition.list_value().values(1).list_value().values()) {
      // Parent token of the returned child partition token should be exactly
      // the same with current input token.
      ASSERT_EQ(parent_token.string_value(), initial_active_token);
    }
  }
}

TEST_F(ChangeStreamQueryAPITest, VerifyHeartbeatRecordContent) {
  absl::Time query_start = Clock().Now();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto initial_active_token,
                       GetActiveTokenFromInitialQuery(query_start));
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', NULL, '$1', 100 )",
      query_start, initial_active_token);
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql));
  ASSERT_EQ(change_records.child_partition_records.size(), 1);
  // actual number of heartbeat records may vary due to latencies and lagging
  // churning thread, so for a test token with lifetime in [500ms, 1000ms], we
  // check there are at least 1 heartbeat record returned when heartbeat
  // milliseconds is 100ms.
  ASSERT_GE(change_records.heartbeat_records.size(), 1);
  ASSERT_EQ(change_records.data_change_records.size(), 0);
  std::string last_record_time = test::EncodeTimestampString(query_start);
  for (const auto& heartbeat_record : change_records.heartbeat_records) {
    // Timestamp for heartbeat record should be increasing.
    ASSERT_TRUE(heartbeat_record.timestamp.string_value() > last_record_time);
    last_record_time = heartbeat_record.timestamp.string_value();
  }
}

TEST_F(ChangeStreamQueryAPITest,
       ExecuteHistoricalPartitionQueryEndAtTokenEndTime) {
  ZETASQL_ASSERT_OK(PopulatePartitionTable());
  absl::SetFlag(&FLAGS_cloud_spanner_emulator_test_with_fake_partition_table,
                true);
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', '$1', 'historical_token1', 10000 "
      ")",
      now_ - absl::Microseconds(2), now_);
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql));
  ASSERT_EQ(change_records.child_partition_records.size(), 1);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 0);
}

TEST_F(ChangeStreamQueryAPITest,
       ExecuteHistoricalPartitionQueryEndAfterTokenEndTime) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto initial_active_token,
                       GetActiveTokenFromInitialQuery(now_));
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', '$1', '$2', 10000 )",
      now_, now_ + absl::Seconds(1), initial_active_token);
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql));
  ASSERT_EQ(change_records.child_partition_records.size(), 1);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 1);
}

TEST_F(ChangeStreamQueryAPITest,
       ExecuteHistoricalPartitionQueryEndBeforeTokenEndTime) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto initial_active_token,
                       GetActiveTokenFromInitialQuery(now_));
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', '$1', '$2', 10000 )",
      now_ - absl::Microseconds(1), now_, initial_active_token);
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql));
  ASSERT_EQ(change_records.child_partition_records.size(), 0);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 0);
}

TEST_F(ChangeStreamQueryAPITest,
       ExecuteHistoricalPartitionQuerySameStartAndEndTime) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto initial_active_token,
                       GetActiveTokenFromInitialQuery(now_));
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', '$1', '$2', 10000 )",
      now_ - absl::Milliseconds(1), now_ - absl::Milliseconds(1),
      initial_active_token);
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql));
  ASSERT_EQ(change_records.child_partition_records.size(), 0);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 0);
}

TEST_F(ChangeStreamQueryAPITest,
       ExecuteHistoricalPartitionQueryOnNullEndPartitionToken) {
  ZETASQL_ASSERT_OK(PopulatePartitionTable());
  absl::SetFlag(&FLAGS_cloud_spanner_emulator_test_with_fake_partition_table,
                true);
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', '$1', 'null_end_token', 10000 )",
      now_ - absl::Microseconds(2), now_ - absl::Microseconds(1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql));
  ASSERT_EQ(change_records.child_partition_records.size(), 0);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 0);
}

TEST_F(ChangeStreamQueryAPITest, ExecuteRealTimePartitionQueryWithStaleToken) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto initial_active_token,
                       GetActiveTokenFromInitialQuery(now_));
  absl::SetFlag(&FLAGS_cloud_spanner_emulator_disable_cs_retention_check, true);
  ZETASQL_ASSERT_OK(UpdateSchema(std::vector<std::string>{R"(
     ALTER CHANGE STREAM change_stream_test_table SET OPTIONS ( retention_period='0s' )
  )"}));
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', NULL , '$1', 10000 )",
      now_ + absl::Milliseconds(100), initial_active_token);
  EXPECT_THAT(
      ExecuteChangeStreamQuery(sql),
      StatusIs(absl::StatusCode::kOutOfRange,
               testing::HasSubstr("start_timestamp is too far in the past.")));
}

TEST_F(ChangeStreamQueryAPITest,
       ExecuteRealTimePartitionQueryStartAtTokenEndTime) {
  ZETASQL_ASSERT_OK(PopulatePartitionTable());
  absl::SetFlag(&FLAGS_cloud_spanner_emulator_test_with_fake_partition_table,
                true);
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
  ZETASQL_ASSERT_OK(PopulatePartitionTable());
  absl::SetFlag(&FLAGS_cloud_spanner_emulator_test_with_fake_partition_table,
                true);
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', '$1' , 'initial_token1', 10000 )",
      now_, now_ + absl::Seconds(1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql));
  ASSERT_EQ(change_records.child_partition_records.size(), 1);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 0);
}

TEST_F(ChangeStreamQueryAPITest,
       ExecuteRealTimePartitionQueryEndBeforeTokenEndTime) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto initial_active_token,
                       GetActiveTokenFromInitialQuery(now_));
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', '$1' , '$2', 10000 )",
      now_ + absl::Milliseconds(50), now_ + absl::Milliseconds(51),
      initial_active_token);
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql));
  ASSERT_EQ(change_records.child_partition_records.size(), 0);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  // The two inserted rows might commit within or out of the given tvf start
  // and end, so we skip checking the number of data change records.
  ASSERT_GE(change_records.data_change_records.size(), 0);
}

TEST_F(ChangeStreamQueryAPITest,
       ExecuteRealTimePartitionQueryEndAfterTokenEndTime) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto initial_active_token,
                       GetActiveTokenFromInitialQuery(now_));
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', '$1', '$2', 10000 )",
      now_, now_ + absl::Milliseconds(1100), initial_active_token);
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql));
  ASSERT_EQ(change_records.child_partition_records.size(), 1);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 1);
}

TEST_F(ChangeStreamQueryAPITest,
       ExecuteRealTimePartitionQuerySameStartAndEndTime) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto initial_active_token,
                       GetActiveTokenFromInitialQuery(now_));
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', '$1', '$2', 10000 )",
      now_ + absl::Milliseconds(100), now_ + absl::Milliseconds(100),
      initial_active_token);
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql));
  ASSERT_EQ(change_records.child_partition_records.size(), 0);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  // Skip verification for data change records because if commit timestamp of
  // data change records are exactly at the sa=tart/end time, they will be
  // returned although with very low possibility.
}

TEST_F(ChangeStreamQueryAPITest,
       ExecuteRealTimePartitionQueryOnNullEndPartitionToken) {
  ZETASQL_ASSERT_OK(PopulatePartitionTable());
  absl::SetFlag(&FLAGS_cloud_spanner_emulator_test_with_fake_partition_table,
                true);
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0', '$1', 'null_end_token', 10000 )",
      now_ - absl::Microseconds(2), now_ + absl::Microseconds(500));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql));
  ASSERT_EQ(change_records.child_partition_records.size(), 0);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 0);
}

TEST_F(ChangeStreamQueryAPITest, ExecuteRealTimePartitionQueryThreaded) {
  // Change stream token lifetime to 1~2 hours to ensure the insertion thread
  // can be successfully committed before the token ends.
  absl::SetFlag(&FLAGS_change_stream_churning_interval, absl::Hours(1));
  absl::SetFlag(&FLAGS_change_stream_churn_thread_sleep_interval,
                absl::Hours(1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto initial_active_token,
                       GetActiveTokenFromInitialQuery(now_));
  std::string sql = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0','$1','$2', 10000 )",
      now_, now_ + absl::Seconds(2), initial_active_token);
  std::thread insert_data_record(&ChangeStreamQueryAPITest::InsertOneRow, this,
                                 "test_table");
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql));
  insert_data_record.join();
  ASSERT_EQ(change_records.child_partition_records.size(), 0);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 2);
}

TEST_F(ChangeStreamQueryAPITest, ExecuteRealTimePartitionQueryWithParameter) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto initial_active_token,
                       GetActiveTokenFromInitialQuery(now_));
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
      sql, initial_active_token));
  request.set_session(test_session_uri_);
  std::vector<spanner_api::PartialResultSet> response;
  ZETASQL_ASSERT_OK(ExecuteStreamingSql(request, &response));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto result_set, backend::test::MergePartialResultSets(
                                            response, /*columns_per_row=*/1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       test::GetChangeStreamRecordsFromResultSet(result_set));
  ASSERT_EQ(change_records.child_partition_records.size(), 1);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 1);
}

TEST_F(ChangeStreamQueryAPITest, ExecutePartitionQueryAfterAlterTrackingTable) {
  // Change stream token lifetime to 1~2 hours to ensure the altering operations
  // can be successfully committed before the token ends.
  absl::SetFlag(&FLAGS_change_stream_churning_interval, absl::Hours(1));
  absl::SetFlag(&FLAGS_change_stream_churn_thread_sleep_interval,
                absl::Hours(1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto initial_active_token,
                       GetActiveTokenFromInitialQuery(now_));
  std::string sql_before_alter = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0','$1','$2', 10000 )",
      now_, Clock().Now(), initial_active_token);
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql_before_alter));
  ASSERT_EQ(change_records.data_change_records.size(), 1);
  ASSERT_EQ(change_records.data_change_records[0].table_name.string_value(),
            "test_table");
  // Alter tracking table from test_table to test_table2
  ZETASQL_ASSERT_OK(UpdateSchema(std::vector<std::string>{R"(
     ALTER CHANGE STREAM change_stream_test_table SET FOR test_table2
  )"}));
  absl::Time after_alter = Clock().Now();
  ZETASQL_ASSERT_OK(InsertOneRow("test_table"));
  ZETASQL_ASSERT_OK(InsertOneRow("test_table2"));
  std::string sql_after_alter = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0','$1','$2', 10000 )",
      after_alter, Clock().Now(), initial_active_token);
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records2,
                       ExecuteChangeStreamQuery(sql_after_alter));
  ASSERT_EQ(change_records2.data_change_records.size(), 1);
  ASSERT_EQ(change_records2.data_change_records[0].table_name.string_value(),
            "test_table2");
}

TEST_F(ChangeStreamQueryAPITest, ExecutePartitionQueryAfterDropTrackingTable) {
  // Change stream token lifetime to 1~2 hours to ensure the altering operations
  // can be successfully committed before the token ends.
  absl::SetFlag(&FLAGS_change_stream_churning_interval, absl::Hours(1));
  absl::SetFlag(&FLAGS_change_stream_churn_thread_sleep_interval,
                absl::Hours(1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto initial_active_token,
                       GetActiveTokenFromInitialQuery(now_));
  std::string sql_before_alter = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0','$1','$2', 10000 )",
      now_, Clock().Now(), initial_active_token);
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(sql_before_alter));
  ASSERT_EQ(change_records.data_change_records.size(), 1);
  ASSERT_EQ(change_records.data_change_records[0].table_name.string_value(),
            "test_table");
  // Drop all tracking columns and tables.
  ZETASQL_ASSERT_OK(UpdateSchema(std::vector<std::string>{R"(
     ALTER CHANGE STREAM change_stream_test_table DROP FOR ALL
  )"}));
  absl::Time after_alter = Clock().Now();
  ZETASQL_ASSERT_OK(InsertOneRow("test_table"));
  std::string sql_after_alter = absl::Substitute(
      "SELECT * FROM "
      "READ_change_stream_test_table ('$0','$1','$2', 10000 )",
      after_alter, Clock().Now() + absl::Seconds(2), initial_active_token);
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records2,
                       ExecuteChangeStreamQuery(sql_after_alter));
  ASSERT_EQ(change_records2.data_change_records.size(), 0);
}

}  // namespace

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
