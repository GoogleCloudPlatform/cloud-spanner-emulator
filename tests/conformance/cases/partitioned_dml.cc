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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/feature_flags.h"
#include "tests/common/proto_matchers.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using zetasql_base::testing::StatusIs;

class PartitionedDmlTest : public DatabaseTest {
 public:
  PartitionedDmlTest()
      : feature_flags_({.enable_bit_reversed_positive_sequences = true}) {}

  absl::Status SetUpDatabase() override {
    ZETASQL_RETURN_IF_ERROR(SetSchema({
        R"(
          CREATE TABLE Users(
            ID       INT64 NOT NULL,
            Name     STRING(MAX),
            Age      INT64,
            Updated  TIMESTAMP OPTIONS (allow_commit_timestamp = true),
          ) PRIMARY KEY (ID)
        )",
        R"(
          CREATE SEQUENCE mysequence OPTIONS (
            sequence_kind = "bit_reversed_positive"
          )
        )"}));

    // Create a raw session for tests which cannot use the C++ client library
    // directly.
    ZETASQL_RETURN_IF_ERROR(CreateSession(database()->FullName()));
    return absl::OkStatus();
  }

  void SetUp() override { DatabaseTest::SetUp(); }

 protected:
  void PopulateDatabase() {
    ZETASQL_EXPECT_OK(CommitDml(
        {SqlStatement("INSERT Users(ID, Name, Age) Values (1, 'Levin', 27), "
                      "(2, 'Mark', 32), (10, 'Douglas', 31)")}));
  }

  absl::Status CreateSession(absl::string_view database_uri) {
    grpc::ClientContext context;
    spanner_api::CreateSessionRequest request;
    request.set_database(std::string(database_uri));  // NOLINT
    spanner_api::Session response;
    ZETASQL_RETURN_IF_ERROR(raw_client()->CreateSession(&context, request, &response));
    session_name_ = response.name();
    return absl::OkStatus();
  }

  absl::StatusOr<std::string> CreatePartitionedDmlTransaction() {
    grpc::ClientContext context;
    spanner_api::Transaction response;
    spanner_api::BeginTransactionRequest request;
    request.set_session(session_name_);
    request.mutable_options()->mutable_partitioned_dml();
    ZETASQL_RETURN_IF_ERROR(
        raw_client()->BeginTransaction(&context, request, &response));
    return response.id();
  }

  // Note: Does not work with parameterized statements.
  absl::StatusOr<spanner_api::ResultSet> ExecutePartitionedDmlInTransaction(
      std::string transaction_id, int seqno, const SqlStatement& statement) {
    grpc::ClientContext context;
    spanner_api::ResultSet response;
    spanner_api::ExecuteSqlRequest request;
    request.set_session(session_name_);
    request.mutable_transaction()->set_id(transaction_id);
    request.set_sql(statement.sql());
    request.set_seqno(seqno);

    ZETASQL_RETURN_IF_ERROR(raw_client()->ExecuteSql(&context, request, &response));
    return response;
  }

  std::string session_name_;

 private:
  test::ScopedEmulatorFeatureFlagsSetter feature_flags_;
};

TEST_F(PartitionedDmlTest, UpdateRowsSucceed) {
  PopulateDatabase();

  ZETASQL_ASSERT_OK_AND_ASSIGN(PartitionedDmlResult result,
                       ExecutePartitionedDml(SqlStatement(
                           "UPDATE Users SET Name = NULL WHERE ID > 1")));
  EXPECT_EQ(result.row_count_lower_bound, 2);
  EXPECT_THAT(Query("SELECT ID, Name, Age FROM Users WHERE Name IS NOT NULL"),
              IsOkAndHoldsRows({{1, "Levin", 27}}));
}

TEST_F(PartitionedDmlTest, UpdateRowsUsingSequenceSucceed) {
  PopulateDatabase();

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      PartitionedDmlResult result,
      ExecutePartitionedDml(SqlStatement(
          "UPDATE Users SET Age = GET_INTERNAL_SEQUENCE_STATE(SEQUENCE "
          "mysequence) WHERE ID = 1")));
  EXPECT_EQ(result.row_count_lower_bound, 1);
  EXPECT_THAT(Query("SELECT count(*) FROM Users WHERE Age IS NULL"),
              IsOkAndHoldsRows({{1}}));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      result, ExecutePartitionedDml(SqlStatement(
                  "UPDATE Users SET Age = GET_NEXT_SEQUENCE_VALUE(SEQUENCE "
                  "mysequence) WHERE ID = 1")));
  EXPECT_EQ(result.row_count_lower_bound, 1);
  EXPECT_THAT(
      Query("SELECT count(*) FROM Users WHERE Age > 32 and AGE IS NOT NULL"),
      IsOkAndHoldsRows({{1}}));
}

TEST_F(PartitionedDmlTest, DeleteAllRowsSucceed) {
  PopulateDatabase();

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      PartitionedDmlResult result,
      ExecutePartitionedDml(SqlStatement("DELETE FROM Users WHERE true")));
  EXPECT_EQ(result.row_count_lower_bound, 3);
  EXPECT_THAT(Query("SELECT ID, Name, Age FROM Users WHERE Name IS NOT NULL"),
              IsOkAndHoldsRows({}));
}

TEST_F(PartitionedDmlTest,
       CannotExecuteSelectStatementUsingPartitionedDmlTransaction) {
  EXPECT_THAT(ExecutePartitionedDml(SqlStatement("SELECT * FROM Users")),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionedDmlTest, CannotInsertUsingPartitionedDml) {
  EXPECT_THAT(ExecutePartitionedDml(SqlStatement(
                  "INSERT Users(ID, Name, Age) Values (10, 'Levin', 27), "
                  "(20, 'Mark', 32), (30, 'Douglas', 31)")),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionedDmlTest, PartitionedDMLOnlySupportsSimpleQuery) {
  EXPECT_THAT(ExecutePartitionedDml(SqlStatement(
                  "UPDATE Users SET Name = 'foo' "
                  "WHERE ID = (SELECT ID FROM Users WHERE Name = 'Levin')")),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionedDmlTest, CannotReadUsingPartitionedDmlTransaction) {
  spanner_api::ReadRequest read_request = PARSE_TEXT_PROTO(R"(
    transaction { begin { partitioned_dml {} } }
    table: "Users"
    columns: "ID"
    columns: "Name"
    key_set { all: true }
  )");
  read_request.set_session(session_name_);

  spanner_api::ResultSet read_response;
  grpc::ClientContext context;
  EXPECT_THAT(raw_client()->Read(&context, read_request, &read_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionedDmlTest,
       CannotExecuteBatchDmlUsingPartitionedDmlTransaction) {
  spanner_api::ExecuteBatchDmlRequest batch_dml_request = PARSE_TEXT_PROTO(R"(
    transaction { begin { partitioned_dml {} } }
    seqno: 1
    statements { sql: "DELETE FROM Users WHERE true" }
  )");
  batch_dml_request.set_session(session_name_);

  spanner_api::ExecuteBatchDmlResponse response;
  grpc::ClientContext context;
  EXPECT_THAT(
      raw_client()->ExecuteBatchDml(&context, batch_dml_request, &response),
      StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionedDmlTest, UpdateRowsWithCommitTimestampSucceed) {
  PopulateDatabase();

  ZETASQL_ASSERT_OK_AND_ASSIGN(PartitionedDmlResult result,
                       ExecutePartitionedDml(SqlStatement(
                           "UPDATE Users SET Updated = "
                           "PENDING_COMMIT_TIMESTAMP() WHERE ID > 1")));
  EXPECT_EQ(result.row_count_lower_bound, 2);
  EXPECT_THAT(Query("SELECT COUNT(1) FROM Users WHERE Updated BETWEEN "
                    "'2000-01-01' AND '3000-01-01'"),
              IsOkAndHoldsRows({{2}}));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
