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
#include <optional>
#include <string>
#include <thread>  // NOLINT
#include <vector>

#include "google/protobuf/struct.pb.h"
#include "google/spanner/admin/database/v1/common.pb.h"
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
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "backend/database/change_stream/change_stream_partition_churner.h"
#include "backend/schema/updater/schema_updater.h"
#include "common/clock.h"
#include "frontend/converters/change_streams.h"
#include "frontend/converters/pg_change_streams.h"
#include "frontend/handlers/change_streams.h"
#include "tests/common/chunking.h"
#include "tests/common/proto_matchers.h"
#include "tests/common/scoped_feature_flags_setter.h"
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

class ChangeStreamQueryAPITest
    : public test::ServerTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  void SetUp() override {
    test::ScopedEmulatorFeatureFlagsSetter enabled_flags(
        {.enable_postgresql_interface = true});
    // Set churning interval and churner thread sleep interval to 1 second to
    // prevent long running unit tests.
    absl::SetFlag(&FLAGS_change_stream_churning_interval,
                  absl::Milliseconds(500));
    absl::SetFlag(&FLAGS_change_stream_churn_thread_sleep_interval,
                  absl::Milliseconds(500));
    ZETASQL_ASSERT_OK(CreateTestInstance());
    ZETASQL_ASSERT_OK(CreateTestDatabaseWithChangeStream(GetParam()));
    ZETASQL_ASSERT_OK_AND_ASSIGN(test_session_uri_,
                         CreateTestSession(/*multiplexed=*/false));
    now_ = Clock().Now();
    now_str_ = test::EncodeTimestampString(
        now_, GetParam() == database_api::DatabaseDialect::POSTGRESQL);
    ZETASQL_ASSERT_OK(PopulateTestDatabase());
    transaction_selector_err_msg_ =
        "Change stream queries must be strong reads "
        "executed via single use transactions using the ExecuteStreamingSql "
        "API.";
  }
  absl::FlagSaver flag_saver_;
  std::string test_session_uri_;
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
      ZETASQL_RET_CHECK(response[i].resume_token() == kChangeStreamDummyResumeToken);
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

  std::string ConstructChangeStreamQuery(
      std::optional<absl::Time> start = std::nullopt,
      std::optional<absl::Time> end = std::nullopt,
      std::optional<std::string> token = std::nullopt,
      int heartbeat_milliseconds = 10000) {
    if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
      return absl::Substitute(
          "SELECT * FROM "
          "spanner.read_json_change_stream_test_table ("
          "$0::timestamptz,"
          "$1::timestamptz, $2::text, $3 , NULL::text[] )",
          start.has_value() ? absl::StrCat("'", start.value(), "'") : "NULL",
          end.has_value() ? absl::StrCat("'", end.value(), "'") : "NULL",
          token.has_value() ? absl::StrCat("'", token.value(), "'") : "NULL",
          heartbeat_milliseconds);
    } else {
      return absl::Substitute(
          "SELECT * FROM "
          "READ_change_stream_test_table ($0,$1, $2, $3 )",
          start.has_value() ? absl::StrCat("'", start.value(), "'") : "NULL",
          end.has_value() ? absl::StrCat("'", end.value(), "'") : "NULL",
          token.has_value() ? absl::StrCat("'", token.value(), "'") : "NULL",
          heartbeat_milliseconds);
    }
  }

  absl::StatusOr<std::string> GetActiveTokenFromInitialQuery(absl::Time start) {
    ZETASQL_ASSIGN_OR_RETURN(
        test::ChangeStreamRecords change_records,
        ExecuteChangeStreamQuery(ConstructChangeStreamQuery(start)));
    std::string active_partition_token = "|";
    for (const auto& child_partition_record :
         change_records.child_partition_records) {
      for (const auto& child_partition :
           child_partition_record.child_partitions.values()) {
        if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
          active_partition_token =
              std::min(active_partition_token, child_partition.struct_value()
                                                   .fields()
                                                   .at(kToken)
                                                   .string_value());
        } else {
          active_partition_token =
              std::min(active_partition_token,
                       child_partition.list_value().values(0).string_value());
        }
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

INSTANTIATE_TEST_SUITE_P(
    PerDialectQueryTests, ChangeStreamQueryAPITest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<ChangeStreamQueryAPITest::ParamType>&
           info) { return database_api::DatabaseDialect_Name(info.param); });

TEST_P(ChangeStreamQueryAPITest,
       CanReadWithSingleUseReadOnlyStrongTransaction) {
  ZETASQL_EXPECT_OK(ExecuteChangeStreamQuery(
      ConstructChangeStreamQuery(now_),
      "{ single_use { read_only { strong: true } } }"));
}

TEST_P(ChangeStreamQueryAPITest, CanReadWithEmptyDefaultTransactionSelector) {
  ZETASQL_EXPECT_OK(ExecuteChangeStreamQuery(ConstructChangeStreamQuery(now_), "{}"));
}

TEST_P(ChangeStreamQueryAPITest, CannotReadWithReadWriteTransaction) {
  EXPECT_THAT(ExecuteChangeStreamQuery(ConstructChangeStreamQuery(now_),
                                       "{ begin { read_write {} } }"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr(transaction_selector_err_msg_)));
}

TEST_P(ChangeStreamQueryAPITest, CannotReadWithPartitionedDmlTransaction) {
  EXPECT_THAT(ExecuteChangeStreamQuery(ConstructChangeStreamQuery(now_),
                                       "{ begin { partitioned_dml {} } }"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr(transaction_selector_err_msg_)));
}

TEST_P(ChangeStreamQueryAPITest, CannotReadUsingReadOnlyStrongTransaction) {
  EXPECT_THAT(
      ExecuteChangeStreamQuery(ConstructChangeStreamQuery(now_),
                               "{ begin { read_only { strong: true } } }"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               testing::HasSubstr(transaction_selector_err_msg_)));
}

TEST_P(ChangeStreamQueryAPITest,
       CannotReadUsingSingleUseReadOnlyNonStrongTransaction) {
  EXPECT_THAT(ExecuteChangeStreamQuery(
                  ConstructChangeStreamQuery(now_),
                  "{ begin { read_only { exact_staleness { seconds: 1 } } } }"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr(transaction_selector_err_msg_)));
}

TEST_P(ChangeStreamQueryAPITest, RejectsPlanMode) {
  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(absl::Substitute(
      R"pb(
        transaction { single_use { read_only { strong: true } } }
        query_mode: PLAN
        sql: "$0"
      )pb",
      ConstructChangeStreamQuery(now_)));
  request.set_session(test_session_uri_);
  std::vector<spanner_api::PartialResultSet> response;
  EXPECT_THAT(ExecuteStreamingSql(request, &response),
              StatusIs(absl::StatusCode::kUnimplemented));
}

TEST_P(ChangeStreamQueryAPITest, RejectsRequestWithValidQueryIfNonStreaming) {
  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(absl::Substitute(
      R"pb(
        transaction { single_use { read_only { strong: true } } }
        sql: "$0"
      )pb",
      ConstructChangeStreamQuery(now_)));
  request.set_session(test_session_uri_);

  spanner_api::ResultSet response;
  EXPECT_THAT(ExecuteSql(request, &response),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr("Change stream queries are not "
                                          "supported for the ExecuteSql API")));
}

TEST_P(ChangeStreamQueryAPITest, RejectsRequestWithInvalidQueryIfNonStreaming) {
  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(absl::Substitute(
      R"pb(
        transaction { single_use { read_only { strong: true } } }
        sql: "$0"
      )pb",
      ConstructChangeStreamQuery(absl::Now() + absl::Hours(24))));
  request.set_session(test_session_uri_);

  spanner_api::ResultSet response;
  EXPECT_THAT(ExecuteSql(request, &response),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr("Change stream queries are not "
                                          "supported for the ExecuteSql API")));
}

TEST_P(ChangeStreamQueryAPITest, ExecuteQueryOnJustDroppedChangeStream) {
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
  EXPECT_THAT(ExecuteChangeStreamQuery(
                  ConstructChangeStreamQuery(now_ + absl::Seconds(1))),
              StatusIs(AnyOf(absl::StatusCode::kNotFound,
                             absl::StatusCode::kInvalidArgument)));
  drop_change_stream.join();
}

TEST_P(ChangeStreamQueryAPITest,
       ExecuteQueryStartTimeTooFarInTheFutureInvalid) {
  EXPECT_THAT(
      ExecuteChangeStreamQuery(
          ConstructChangeStreamQuery(now_ + absl::Minutes(11))),
      StatusIs(absl::StatusCode::kOutOfRange,
               testing::HasSubstr(
                   "Specified start_timestamp is too far in the future.")));
}

TEST_P(ChangeStreamQueryAPITest, ExecuteQueryStartTimeTooOldInThePastInvalid) {
  EXPECT_THAT(
      ExecuteChangeStreamQuery(
          ConstructChangeStreamQuery(now_ - absl::Minutes(10))),
      StatusIs(absl::StatusCode::kOutOfRange,
               testing::HasSubstr(
                   "Specified start_timestamp is too far in the past.")));
}

TEST_P(ChangeStreamQueryAPITest, ExecuteQueryStartTimeNotProvidedInvalid) {
  EXPECT_THAT(
      ExecuteChangeStreamQuery(ConstructChangeStreamQuery()),
      StatusIs(absl::StatusCode::kInvalidArgument,
               testing::HasSubstr("start_timestamp must not be null.")));
}

TEST_P(ChangeStreamQueryAPITest,
       ExecutePartitionQueryWithNonExistedPartitionToken) {
  EXPECT_THAT(
      ExecuteChangeStreamQuery(
          ConstructChangeStreamQuery(now_, now_, "non_existed_token")),
      StatusIs(absl::StatusCode::kInvalidArgument,
               testing::HasSubstr("Specified partition_token is invalid")));
}

TEST_P(ChangeStreamQueryAPITest,
       ExecutePartitionQueryWithInvalidPartitionStartTime) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::string initial_active_token,
                       GetActiveTokenFromInitialQuery(now_));
  EXPECT_THAT(
      ExecuteChangeStreamQuery(ConstructChangeStreamQuery(
          now_ + absl::Seconds(10), std::nullopt, initial_active_token)),
      StatusIs(absl::StatusCode::kOutOfRange,
               testing::HasSubstr(
                   "Specified start_timestamp is invalid for the partition.")));
}

TEST_P(ChangeStreamQueryAPITest,
       ExecuteInitialQueryOnInitialBackfilledPartitions) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      test::ChangeStreamRecords change_records,
      ExecuteChangeStreamQuery(ConstructChangeStreamQuery(now_)));
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

TEST_P(ChangeStreamQueryAPITest, ExecuteInitialQueryAfterChurned) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(ConstructChangeStreamQuery(
                           now_ + absl::Milliseconds(1500))));
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
  // Verify that no parents partition tokens are populated for initial query.
  int parent_token_num1, parent_token_num2;
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    parent_token_num1 = change_records.child_partition_records.at(0)
                            .child_partitions.values(0)
                            .struct_value()
                            .fields()
                            .at(kParentPartitionTokens)
                            .list_value()
                            .values_size();
    parent_token_num2 = change_records.child_partition_records.at(1)
                            .child_partitions.values(0)
                            .struct_value()
                            .fields()
                            .at(kParentPartitionTokens)
                            .list_value()
                            .values_size();
  } else {
    parent_token_num1 = change_records.child_partition_records.at(0)
                            .child_partitions.values(0)
                            .list_value()
                            .values(1)
                            .list_value()
                            .values_size();
    parent_token_num2 = change_records.child_partition_records.at(1)
                            .child_partitions.values(0)
                            .list_value()
                            .values(1)
                            .list_value()
                            .values_size();
  }
  EXPECT_EQ(parent_token_num1, 0);
  EXPECT_EQ(parent_token_num2, 0);
}

TEST_P(ChangeStreamQueryAPITest,
       ExecuteHistoricalPartitionQueryStartAtTokenEndTime) {
  ZETASQL_ASSERT_OK(PopulatePartitionTable());
  absl::SetFlag(&FLAGS_cloud_spanner_emulator_test_with_fake_partition_table,
                true);
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(ConstructChangeStreamQuery(
                           now_, std::nullopt, "historical_token1")));
  ASSERT_EQ(change_records.child_partition_records.size(), 1);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 0);
}

TEST_P(ChangeStreamQueryAPITest, VerifyChildPartitionsRecordContent) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto initial_active_token,
                       GetActiveTokenFromInitialQuery(now_));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      test::ChangeStreamRecords change_records,
      ExecuteChangeStreamQuery(ConstructChangeStreamQuery(
          now_, now_ + absl::Milliseconds(1100), initial_active_token)));
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
    // Returned child tokens should be different from the current token, and
    // parent token of the returned child partition token should be exactly the
    // same with current input token.
    if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
      ASSERT_NE(
          child_partition.struct_value().fields().at(kToken).string_value(),
          initial_active_token);
      for (const auto& parent_token : child_partition.struct_value()
                                          .fields()
                                          .at(kParentPartitionTokens)
                                          .list_value()
                                          .values()) {
        ASSERT_EQ(parent_token.string_value(), initial_active_token);
      }
    } else {
      ASSERT_NE(child_partition.list_value().values(0).string_value(),
                initial_active_token);
      for (const auto& parent_token :
           child_partition.list_value().values(1).list_value().values()) {
        ASSERT_EQ(parent_token.string_value(), initial_active_token);
      }
    }
  }
}

TEST_P(ChangeStreamQueryAPITest, VerifyHeartbeatRecordContent) {
  absl::Time query_start = Clock().Now();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto initial_active_token,
                       GetActiveTokenFromInitialQuery(query_start));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      test::ChangeStreamRecords change_records,
      ExecuteChangeStreamQuery(ConstructChangeStreamQuery(
          query_start, std::nullopt, initial_active_token, 100)));
  ASSERT_EQ(change_records.child_partition_records.size(), 1);
  // actual number of heartbeat records may vary due to latencies and lagging
  // churning thread, so for a test token with lifetime in [500ms, 1000ms], we
  // check there are at least 1 heartbeat record returned when heartbeat
  // milliseconds is 100ms.
  ASSERT_GE(change_records.heartbeat_records.size(), 1);
  ASSERT_EQ(change_records.data_change_records.size(), 0);
  std::string last_record_time = test::EncodeTimestampString(
      query_start, GetParam() == database_api::DatabaseDialect::POSTGRESQL);
  for (const auto& heartbeat_record : change_records.heartbeat_records) {
    // Timestamp for heartbeat record should be increasing.
    ASSERT_TRUE(heartbeat_record.timestamp.string_value() > last_record_time);
    last_record_time = heartbeat_record.timestamp.string_value();
  }
}

TEST_P(ChangeStreamQueryAPITest,
       ExecuteHistoricalPartitionQueryEndAtTokenEndTime) {
  ZETASQL_ASSERT_OK(PopulatePartitionTable());
  absl::SetFlag(&FLAGS_cloud_spanner_emulator_test_with_fake_partition_table,
                true);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      test::ChangeStreamRecords change_records,
      ExecuteChangeStreamQuery(ConstructChangeStreamQuery(
          now_ - absl::Microseconds(2), now_, "historical_token1")));
  ASSERT_EQ(change_records.child_partition_records.size(), 1);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 0);
}

TEST_P(ChangeStreamQueryAPITest,
       ExecuteHistoricalPartitionQueryEndAfterTokenEndTime) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto initial_active_token,
                       GetActiveTokenFromInitialQuery(now_));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      test::ChangeStreamRecords change_records,
      ExecuteChangeStreamQuery(ConstructChangeStreamQuery(
          now_, now_ + absl::Seconds(1), initial_active_token)));
  ASSERT_EQ(change_records.child_partition_records.size(), 1);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 1);
}

TEST_P(ChangeStreamQueryAPITest,
       ExecuteHistoricalPartitionQueryEndBeforeTokenEndTime) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto initial_active_token,
                       GetActiveTokenFromInitialQuery(now_));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      test::ChangeStreamRecords change_records,
      ExecuteChangeStreamQuery(ConstructChangeStreamQuery(
          now_ - absl::Microseconds(1), now_, initial_active_token)));
  ASSERT_EQ(change_records.child_partition_records.size(), 0);
  ASSERT_EQ(change_records.heartbeat_records.size(), 1);
  ASSERT_EQ(change_records.data_change_records.size(), 0);
}

TEST_P(ChangeStreamQueryAPITest,
       ExecuteHistoricalPartitionQuerySameStartAndEndTime) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto initial_active_token,
                       GetActiveTokenFromInitialQuery(now_));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(ConstructChangeStreamQuery(
                           now_, now_, initial_active_token)));
  ASSERT_EQ(change_records.child_partition_records.size(), 0);
  ASSERT_EQ(change_records.heartbeat_records.size(), 1);
  ASSERT_EQ(change_records.data_change_records.size(), 0);
}

TEST_P(ChangeStreamQueryAPITest,
       ExecuteHistoricalPartitionQueryOnNullEndPartitionToken) {
  ZETASQL_ASSERT_OK(PopulatePartitionTable());
  absl::SetFlag(&FLAGS_cloud_spanner_emulator_test_with_fake_partition_table,
                true);
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(ConstructChangeStreamQuery(
                           now_ - absl::Microseconds(2),
                           now_ - absl::Microseconds(1), "null_end_token")));
  ASSERT_EQ(change_records.child_partition_records.size(), 0);
  ASSERT_EQ(change_records.heartbeat_records.size(), 1);
  ASSERT_EQ(change_records.data_change_records.size(), 0);
}

TEST_P(ChangeStreamQueryAPITest, ExecuteRealTimePartitionQueryWithStaleToken) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto initial_active_token,
                       GetActiveTokenFromInitialQuery(now_));
  absl::SetFlag(&FLAGS_cloud_spanner_emulator_disable_cs_retention_check, true);
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    ZETASQL_ASSERT_OK(UpdateSchema(std::vector<std::string>{R"(
     ALTER CHANGE STREAM change_stream_test_table SET ( retention_period='0s' )
  )"}));
  } else {
    ZETASQL_ASSERT_OK(UpdateSchema(std::vector<std::string>{R"(
     ALTER CHANGE STREAM change_stream_test_table SET OPTIONS ( retention_period='0s' )
  )"}));
  }
  EXPECT_THAT(
      ExecuteChangeStreamQuery(ConstructChangeStreamQuery(
          now_ + absl::Milliseconds(100), std::nullopt, initial_active_token)),
      StatusIs(absl::StatusCode::kOutOfRange,
               testing::HasSubstr("start_timestamp is too far in the past.")));
}

TEST_P(ChangeStreamQueryAPITest,
       ExecuteRealTimePartitionQueryStartAtTokenEndTime) {
  ZETASQL_ASSERT_OK(PopulatePartitionTable());
  absl::SetFlag(&FLAGS_cloud_spanner_emulator_test_with_fake_partition_table,
                true);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      test::ChangeStreamRecords change_records,
      ExecuteChangeStreamQuery(ConstructChangeStreamQuery(
          now_ + absl::Seconds(1), std::nullopt, "initial_token1")));
  ASSERT_EQ(change_records.child_partition_records.size(), 1);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 0);
}

TEST_P(ChangeStreamQueryAPITest,
       ExecuteRealTimePartitionQueryEndAtTokenEndTime) {
  ZETASQL_ASSERT_OK(PopulatePartitionTable());
  absl::SetFlag(&FLAGS_cloud_spanner_emulator_test_with_fake_partition_table,
                true);
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(ConstructChangeStreamQuery(
                           now_, now_ + absl::Seconds(1), "initial_token1")));
  ASSERT_EQ(change_records.child_partition_records.size(), 1);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 0);
}

TEST_P(ChangeStreamQueryAPITest,
       ExecuteRealTimePartitionQueryEndBeforeTokenEndTime) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto initial_active_token,
                       GetActiveTokenFromInitialQuery(now_));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      test::ChangeStreamRecords change_records,
      ExecuteChangeStreamQuery(ConstructChangeStreamQuery(
          now_ + absl::Milliseconds(50), now_ + absl::Milliseconds(51),
          initial_active_token)));
  ASSERT_EQ(change_records.child_partition_records.size(), 0);
  // The two inserted rows might commit within or out of the given tvf start
  // and end, so we skip checking the number of heartbeat and data change
  // records.
  ASSERT_GE(change_records.heartbeat_records.size(), 0);
  ASSERT_GE(change_records.data_change_records.size(), 0);
}

TEST_P(ChangeStreamQueryAPITest,
       ExecuteRealTimePartitionQueryEndAfterTokenEndTime) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto initial_active_token,
                       GetActiveTokenFromInitialQuery(now_));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      test::ChangeStreamRecords change_records,
      ExecuteChangeStreamQuery(ConstructChangeStreamQuery(
          now_, now_ + absl::Milliseconds(1100), initial_active_token)));
  ASSERT_EQ(change_records.child_partition_records.size(), 1);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 1);
}

TEST_P(ChangeStreamQueryAPITest,
       ExecuteRealTimePartitionQuerySameStartAndEndTime) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto initial_active_token,
                       GetActiveTokenFromInitialQuery(now_ + absl::Seconds(1)));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(ConstructChangeStreamQuery(
                           now_ + absl::Seconds(1), now_ + absl::Seconds(1),
                           initial_active_token)));
  ASSERT_EQ(change_records.child_partition_records.size(), 0);
  ASSERT_EQ(change_records.heartbeat_records.size(), 1);
  ASSERT_EQ(change_records.data_change_records.size(), 0);
  // The only heartbeat record for this empty result query should have timestamp
  // at exactly tvf end.
  ASSERT_EQ(change_records.heartbeat_records[0].timestamp.string_value(),
            test::EncodeTimestampString(
                now_ + absl::Seconds(1),
                GetParam() == database_api::DatabaseDialect::POSTGRESQL));
}

TEST_P(ChangeStreamQueryAPITest,
       ExecuteRealTimePartitionQueryOnNullEndPartitionToken) {
  ZETASQL_ASSERT_OK(PopulatePartitionTable());
  absl::SetFlag(&FLAGS_cloud_spanner_emulator_test_with_fake_partition_table,
                true);
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(ConstructChangeStreamQuery(
                           now_ - absl::Microseconds(2),
                           now_ + absl::Microseconds(500), "null_end_token")));
  ASSERT_EQ(change_records.child_partition_records.size(), 0);
  ASSERT_EQ(change_records.heartbeat_records.size(), 1);
  ASSERT_EQ(change_records.data_change_records.size(), 0);
  // The only heartbeat record for this empty result query should have timestamp
  // at exactly tvf end.
  ASSERT_EQ(change_records.heartbeat_records[0].timestamp.string_value(),
            test::EncodeTimestampString(
                now_ + absl::Microseconds(500),
                GetParam() == database_api::DatabaseDialect::POSTGRESQL));
}

TEST_P(ChangeStreamQueryAPITest, ExecuteRealTimePartitionQueryThreaded) {
  // Change stream token lifetime to 1~2 hours to ensure the insertion thread
  // can be successfully committed before the token ends.
  absl::SetFlag(&FLAGS_change_stream_churning_interval, absl::Hours(1));
  absl::SetFlag(&FLAGS_change_stream_churn_thread_sleep_interval,
                absl::Hours(1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto initial_active_token,
                       GetActiveTokenFromInitialQuery(now_));
  std::thread insert_data_record(&ChangeStreamQueryAPITest::InsertOneRow, this,
                                 "test_table");
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      test::ChangeStreamRecords change_records,
      ExecuteChangeStreamQuery(ConstructChangeStreamQuery(
          now_, now_ + absl::Seconds(2), initial_active_token)));
  insert_data_record.join();
  ASSERT_EQ(change_records.child_partition_records.size(), 0);
  ASSERT_EQ(change_records.heartbeat_records.size(), 0);
  ASSERT_EQ(change_records.data_change_records.size(), 2);
}

TEST_P(ChangeStreamQueryAPITest, ExecuteRealTimePartitionQueryWithParameter) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto initial_active_token,
                       GetActiveTokenFromInitialQuery(now_));
  std::string sql;
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    sql = absl::Substitute(
        "SELECT * FROM "
        "spanner.read_json_change_stream_test_table "
        "('$0'::timestamptz,NULL::timestamptz, $1::text, 10000, NULL::text[] )",
        now_, "$1");
  } else {
    sql = absl::Substitute(
        "SELECT * FROM "
        "READ_change_stream_test_table ('$0',NULL, @p1 , 10000 )",
        now_);
  }
  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(absl::Substitute(
      R"pb(
        transaction { single_use { read_only { strong: true } } }
        sql: "$0"
        params {
          fields {
            key: "p1"
            value { string_value: "$1" }
          }
        }
        param_types {
          key: "p1"
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

TEST_P(ChangeStreamQueryAPITest, ExecutePartitionQueryAfterAlterTrackingTable) {
  // Change stream token lifetime to 1~2 hours to ensure the altering operations
  // can be successfully committed before the token ends.
  absl::SetFlag(&FLAGS_change_stream_churning_interval, absl::Hours(1));
  absl::SetFlag(&FLAGS_change_stream_churn_thread_sleep_interval,
                absl::Hours(1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto initial_active_token,
                       GetActiveTokenFromInitialQuery(now_));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(ConstructChangeStreamQuery(
                           now_, Clock().Now(), initial_active_token)));
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
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records2,
                       ExecuteChangeStreamQuery(ConstructChangeStreamQuery(
                           after_alter, Clock().Now(), initial_active_token)));
  ASSERT_EQ(change_records2.data_change_records.size(), 1);
  ASSERT_EQ(change_records2.data_change_records[0].table_name.string_value(),
            "test_table2");
}

TEST_P(ChangeStreamQueryAPITest, ExecutePartitionQueryAfterDropTrackingTable) {
  // Change stream token lifetime to 1~2 hours to ensure the altering operations
  // can be successfully committed before the token ends.
  absl::SetFlag(&FLAGS_change_stream_churning_interval, absl::Hours(1));
  absl::SetFlag(&FLAGS_change_stream_churn_thread_sleep_interval,
                absl::Hours(1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto initial_active_token,
                       GetActiveTokenFromInitialQuery(now_));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records,
                       ExecuteChangeStreamQuery(ConstructChangeStreamQuery(
                           now_, Clock().Now(), initial_active_token)));
  ASSERT_EQ(change_records.data_change_records.size(), 1);
  ASSERT_EQ(change_records.data_change_records[0].table_name.string_value(),
            "test_table");
  // Drop all tracking columns and tables.
  ZETASQL_ASSERT_OK(UpdateSchema(std::vector<std::string>{R"(
     ALTER CHANGE STREAM change_stream_test_table DROP FOR ALL
  )"}));
  absl::Time after_alter = Clock().Now();
  ZETASQL_ASSERT_OK(InsertOneRow("test_table"));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_records2,
                       ExecuteChangeStreamQuery(ConstructChangeStreamQuery(
                           after_alter, Clock().Now(), initial_active_token)));
  ASSERT_EQ(change_records2.data_change_records.size(), 0);
}

}  // namespace

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
