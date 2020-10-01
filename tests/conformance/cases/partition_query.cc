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
#include "zetasql/base/statusor.h"
#include "absl/strings/substitute.h"
#include "tests/common/proto_matchers.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using zetasql_base::testing::StatusIs;

class PartitionQueryTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    ZETASQL_RETURN_IF_ERROR(SetSchema({
        R"(
          CREATE TABLE Users(
            UserId   INT64 NOT NULL,
            Name STRING(MAX),
            Age  INT64
          ) PRIMARY KEY (UserId)
        )",
        "CREATE INDEX UsersByName ON Users(Name)",
        "CREATE INDEX UsersByNameDescending ON Users(Name DESC)",
        R"(
          CREATE TABLE Threads (
            UserId     INT64 NOT NULL,
            ThreadId   INT64 NOT NULL,
            Starred    BOOL
          ) PRIMARY KEY (UserId, ThreadId),
          INTERLEAVE IN PARENT Users ON DELETE CASCADE
        )"}));
    return absl::OkStatus();
  }

 protected:
  // Creates a new session for tests using raw grpc client.
  zetasql_base::StatusOr<spanner_api::Session> CreateSession() {
    grpc::ClientContext context;
    spanner_api::CreateSessionRequest request;
    request.set_database(std::string(database()->FullName()));  // NOLINT
    spanner_api::Session response;
    ZETASQL_RETURN_IF_ERROR(raw_client()->CreateSession(&context, request, &response));
    return response;
  }

  void PopulateDatabase() {
    // Write fixture data to use in partition query test.
    ZETASQL_EXPECT_OK(CommitDml({SqlStatement(
        "INSERT Users(UserId, Name, Age) Values (1, 'Levin', 27), "
        "(2, 'Mark', 32), (10, 'Douglas', 31)")}));

    ZETASQL_EXPECT_OK(MultiInsert("Threads", {"UserId", "ThreadId", "Starred"},
                          {{1, 1, true},
                           {1, 2, true},
                           {1, 3, true},
                           {1, 4, false},
                           {2, 1, false},
                           {2, 2, true},
                           {10, 1, false}}));
  }
};

// Tests using raw grpc client to test session and transaction validaton.
TEST_F(PartitionQueryTest, CannotQueryWithoutSession) {
  spanner_api::PartitionQueryRequest partition_query_request;

  spanner_api::PartitionResponse partition_query_response;
  grpc::ClientContext context;
  EXPECT_THAT(raw_client()->PartitionQuery(&context, partition_query_request,
                                           &partition_query_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionQueryTest, CannotQueryWithoutTransaction) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());

  spanner_api::PartitionQueryRequest partition_query_request;
  partition_query_request.set_session(session.name());

  spanner_api::PartitionResponse partition_query_response;
  grpc::ClientContext context;
  EXPECT_THAT(raw_client()->PartitionQuery(&context, partition_query_request,
                                           &partition_query_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionQueryTest, CannotQueryUsingSingleUseTransaction) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());

  spanner_api::PartitionQueryRequest partition_query_request =
      PARSE_TEXT_PROTO(absl::Substitute(
          R"(
            session: "$0"
            transaction { single_use { read_only {} } }
          )",
          session.name()));

  spanner_api::PartitionResponse partition_query_response;
  grpc::ClientContext context;
  EXPECT_THAT(raw_client()->PartitionQuery(&context, partition_query_request,
                                           &partition_query_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

// Tests using cpp client library.
TEST_F(PartitionQueryTest, CannotQueryUsingBeginReadWriteTransaction) {
  Transaction txn{Transaction::ReadWriteOptions{}};

  // PartitionQuery using a begin read-write transaction fails.
  EXPECT_THAT(PartitionQuery(txn, "SELECT UserID, Name FROM Users"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionQueryTest, CannotQueryUsingExistingReadWriteTransaction) {
  Transaction txn{Transaction::ReadWriteOptions{}};
  ZETASQL_ASSERT_OK(Read(txn, "Users", {"UserId", "Name"}, KeySet::All()));

  // PartitionQuery using an already started read-write transaction fails.
  EXPECT_THAT(PartitionQuery(txn, "SELECT UserID, Name FROM Users"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionQueryTest, CannotQueryUsingInvalidPartitionOptions) {
  Transaction txn{Transaction::ReadOnlyOptions{}};

  // Test that negative partition_size_bytes is not allowed.
  PartitionOptions partition_options = {.partition_size_bytes = -1,
                                        .max_partitions = 100};
  EXPECT_THAT(
      PartitionQuery(txn, "SELECT UserID, Name FROM Users", partition_options),
      StatusIs(absl::StatusCode::kInvalidArgument));

  // Test that negative max_partitions is not allowed.
  partition_options = {.partition_size_bytes = 10000, .max_partitions = -1};
  EXPECT_THAT(
      PartitionQuery(txn, "SELECT UserID, Name FROM Users", partition_options),
      StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionQueryTest, CanQueryUsingPartitionToken) {
  PopulateDatabase();

  Transaction txn{Transaction::ReadOnlyOptions{}};

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<QueryPartition> partitions,
                       PartitionQuery(txn, "SELECT UserID, Name FROM Users"));

  EXPECT_THAT(
      Query(partitions),
      IsOkAndHoldsUnorderedRows({{1, "Levin"}, {2, "Mark"}, {10, "Douglas"}}));
}

TEST_F(PartitionQueryTest, CanReuseTransactionForPartitionQuery) {
  PopulateDatabase();

  Transaction txn{Transaction::ReadOnlyOptions{}};

  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<QueryPartition> partitions,
                         PartitionQuery(txn, "SELECT UserID, Name FROM Users"));
    EXPECT_GE(partitions.size(), 1);
  }

  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<QueryPartition> partitions,
                         PartitionQuery(txn, "SELECT UserID FROM Users"));
    EXPECT_GE(partitions.size(), 1);
  }
}

TEST_F(PartitionQueryTest, CannotQueryNonRootPartitionableSqlOrderBy) {
  PopulateDatabase();

  Transaction txn{Transaction::ReadOnlyOptions{}};

  EXPECT_THAT(
      PartitionQuery(txn, "SELECT UserID, Name FROM Users ORDER BY Name"),
      StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionQueryTest, CannotQueryNonRootPartitionableSqlNoTable) {
  PopulateDatabase();

  Transaction txn{Transaction::ReadOnlyOptions{}};

  EXPECT_THAT(PartitionQuery(txn, "SELECT a FROM UNNEST([1, 2, 3]) AS a"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionQueryTest, CannotQueryNonRootPartitionableSqlFilterNoTable) {
  PopulateDatabase();

  Transaction txn{Transaction::ReadOnlyOptions{}};

  EXPECT_THAT(
      PartitionQuery(txn, "SELECT a FROM UNNEST([1, 2, 3]) AS a WHERE a = 1"),
      StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionQueryTest, CannotQueryNonRootPartitionableSqlSubquery) {
  PopulateDatabase();

  Transaction txn{Transaction::ReadOnlyOptions{}};

  EXPECT_THAT(PartitionQuery(txn,
                             "SELECT UserID, Name FROM Users WHERE EXISTS "
                             "(SELECT ThreadId FROM Threads)"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionQueryTest, DisableQueryPartitionabilityCheckForValidQuery) {
  PopulateDatabase();

  const std::string valid_query =
      "SELECT ANY_VALUE(STARRED), USERID "
      "FROM Threads WHERE Threadid=1 "
      "GROUP BY UserId ";

  {
    Transaction txn{Transaction::ReadOnlyOptions{}};
    // Query fails without the hint to disable partitionability check in
    // emulator.
    if (in_prod_env()) {
      ZETASQL_EXPECT_OK(PartitionQuery(txn, valid_query));
    } else {
      EXPECT_THAT(PartitionQuery(txn, valid_query),
                  StatusIs(absl::StatusCode::kInvalidArgument));
    }
  }

  // With the hint to disable partitionability check, the Emulator is pacified
  // and prod ignores the hint.
  {
    const auto modified_query = absl::StrCat(
        "@{spanner_emulator.disable_query_partitionability_check=true}",
        valid_query);
    Transaction txn{Transaction::ReadOnlyOptions{}};
    ZETASQL_EXPECT_OK(PartitionQuery(txn, modified_query));
  }
}

TEST_F(PartitionQueryTest, CannotQueryNonRootPartitionableSqlSubqueryInExpr) {
  PopulateDatabase();

  Transaction txn{Transaction::ReadOnlyOptions{}};

  EXPECT_THAT(PartitionQuery(txn,
                             "SELECT UserID, Name FROM Users WHERE UserID IN "
                             "(SELECT ThreadId FROM Threads)"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionQueryTest, CannotQueryNonRootPartitionableSqlTwoTable) {
  PopulateDatabase();

  Transaction txn{Transaction::ReadOnlyOptions{}};

  EXPECT_THAT(PartitionQuery(txn,
                             "SELECT u.UserID, ARRAY(SELECT ThreadId FROM "
                             "Threads) FROM Users AS u"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionQueryTest, CannotQueryNonRootPartitionableSqlOneTableOneIndex) {
  PopulateDatabase();

  Transaction txn{Transaction::ReadOnlyOptions{}};

  EXPECT_THAT(
      PartitionQuery(
          txn, "SELECT u.UserID, b.Name FROM Users AS u, UsersByName AS b"),
      StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionQueryTest, CannotPartitionQueryWithoutSql) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());

  spanner_api::PartitionQueryRequest partition_query_request =
      PARSE_TEXT_PROTO(absl::Substitute(
          R"(
            session: "$0"
            transaction { begin { read_only {} } }
          )",
          session.name()));

  spanner_api::PartitionResponse partition_query_response;
  grpc::ClientContext context;
  EXPECT_THAT(raw_client()->PartitionQuery(&context, partition_query_request,
                                           &partition_query_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionQueryTest, CannotQueryWithDifferentSession) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());

  spanner_api::PartitionQueryRequest partition_query_request =
      PARSE_TEXT_PROTO(absl::Substitute(
          R"(
            session: "$0"
            transaction { begin { read_only {} } }
            sql: "SELECT UserID, Name FROM Users"
          )",
          session.name()));

  spanner_api::PartitionResponse partition_query_response;
  {
    grpc::ClientContext context;
    ZETASQL_ASSERT_OK(raw_client()->PartitionQuery(&context, partition_query_request,
                                           &partition_query_response));
  }
  ASSERT_GT(partition_query_response.partitions().size(), 0);

  // Validate that a different session cannot be used for query using partition
  // token than the one used for partition query.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto query_session, CreateSession());
  spanner_api::ExecuteSqlRequest sql_request =
      PARSE_TEXT_PROTO(absl::Substitute(
          R"(
            session: "$0"
            transaction { begin { read_only {} } }
            sql: "SELECT UserID, Name FROM Users"
            partition_token: "$1"
          )",
          query_session.name(),
          partition_query_response.partitions()[0].partition_token()));

  grpc::ClientContext context;
  spanner_api::ResultSet query_response;
  EXPECT_THAT(raw_client()->ExecuteSql(&context, sql_request, &query_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionQueryTest, CannotQueryWithDifferentTransaction) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());

  spanner_api::PartitionQueryRequest partition_query_request =
      PARSE_TEXT_PROTO(absl::Substitute(
          R"(
            session: "$0"
            transaction { begin { read_only {} } }
            sql: "SELECT UserID, Name FROM Users"
          )",
          session.name()));

  spanner_api::PartitionResponse partition_query_response;
  {
    grpc::ClientContext context;
    ZETASQL_ASSERT_OK(raw_client()->PartitionQuery(&context, partition_query_request,
                                           &partition_query_response));
  }
  ASSERT_GT(partition_query_response.partitions().size(), 0);

  // Validate that a new/different transaction cannot be used for query using
  // partition token than the one used for partition query.
  spanner_api::ExecuteSqlRequest sql_request =
      PARSE_TEXT_PROTO(absl::Substitute(
          R"(
            session: "$0"
            transaction { begin { read_only {} } }
            sql: "SELECT UserID, Name FROM Users"
            partition_token: "$1"
          )",
          session.name(),
          partition_query_response.partitions()[0].partition_token()));

  grpc::ClientContext context;
  spanner_api::ResultSet query_response;
  EXPECT_THAT(raw_client()->ExecuteSql(&context, sql_request, &query_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionQueryTest, CannotQueryWithDifferentSql) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());

  spanner_api::PartitionQueryRequest partition_query_request =
      PARSE_TEXT_PROTO(absl::Substitute(
          R"(
            session: "$0"
            transaction { begin { read_only {} } }
            sql: "SELECT UserID, Name FROM Users"
          )",
          session.name()));

  spanner_api::PartitionResponse partition_query_response;
  {
    grpc::ClientContext context;
    ZETASQL_ASSERT_OK(raw_client()->PartitionQuery(&context, partition_query_request,
                                           &partition_query_response));
  }
  ASSERT_GT(partition_query_response.partitions().size(), 0);

  // Validate that query cannot be performed using a different sql when using
  // partition token.
  spanner_api::ExecuteSqlRequest sql_request =
      PARSE_TEXT_PROTO(absl::Substitute(
          R"(
            session: "$0"
            transaction { id: "$1" }
            sql: "SELECT UserID, Name FROM Users WHERE UserId=1"
            partition_token: "$2"
          )",
          session.name(), partition_query_response.transaction().id(),
          partition_query_response.partitions()[0].partition_token()));

  grpc::ClientContext context;
  spanner_api::ResultSet query_response;
  EXPECT_THAT(raw_client()->ExecuteSql(&context, sql_request, &query_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionQueryTest, CannotQueryWithDifferentParams) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());

  spanner_api::PartitionQueryRequest partition_query_request =
      PARSE_TEXT_PROTO(absl::Substitute(
          R"(
            session: "$0"
            transaction { begin { read_only {} } }
            sql: "SELECT UserID, Name FROM Users WHERE UserId=@id"
            params {
              fields: {
                key: "id",
                value: {
                  number_value: 1
                }
              },
            }
            param_types: {
              key: "id"
              value: { code: 3 }
            }
          )",
          session.name()));

  spanner_api::PartitionResponse partition_query_response;
  {
    grpc::ClientContext context;
    ZETASQL_ASSERT_OK(raw_client()->PartitionQuery(&context, partition_query_request,
                                           &partition_query_response));
  }
  ASSERT_GT(partition_query_response.partitions().size(), 0);

  // Validate that a different set of params cannot be query when using
  // partition token. Note that the value of @id is different (2 vs 1) below.
  spanner_api::ExecuteSqlRequest sql_request =
      PARSE_TEXT_PROTO(absl::Substitute(
          R"(
            session: "$0"
            transaction { id: "$1" }
            sql: "SELECT UserID, Name FROM Users WHERE UserId=@id"
            params {
              fields: {
                key: "id",
                value: {
                  number_value: 2
                }
              },
            }
            param_types: {
              key: "id"
              value: { code: 3 }
            }
            partition_token: "$2"
          )",
          session.name(), partition_query_response.transaction().id(),
          partition_query_response.partitions()[0].partition_token()));

  grpc::ClientContext context;
  spanner_api::ResultSet query_response;
  EXPECT_THAT(raw_client()->ExecuteSql(&context, sql_request, &query_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionQueryTest, CanQueryWithMultipleParams) {
  PopulateDatabase();

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());

  spanner_api::PartitionQueryRequest partition_query_request =
      PARSE_TEXT_PROTO(absl::Substitute(
          R"(
          session: "$0"
          transaction { begin { read_only {} } }
          sql: "SELECT UserID, ThreadId, Starred FROM Threads WHERE UserId=@id AND ThreadId=@tid"
          params {
            fields: {
              key: "id",
              value: {
                number_value: 1
              }
            },
            fields: {
              key: "tid",
              value: {
                number_value: 2
              }
            },
          }
          param_types: {
            key: "id"
            value: { code: 3 }
          }
          param_types: {
            key: "tid"
            value: { code: 3 }
          }
        )",
          session.name()));

  spanner_api::PartitionResponse partition_query_response;
  {
    grpc::ClientContext context;
    ZETASQL_ASSERT_OK(raw_client()->PartitionQuery(&context, partition_query_request,
                                           &partition_query_response));
  }
  ASSERT_GT(partition_query_response.partitions().size(), 0);

  // Validate that a query with same set of params can be executed using the
  // partition token created above.
  spanner_api::ExecuteSqlRequest sql_request =
      PARSE_TEXT_PROTO(absl::Substitute(
          R"(
            session: "$0"
            transaction { id: "$1" }
            sql: "SELECT UserID, ThreadId, Starred FROM Threads WHERE UserId=@id AND ThreadId=@tid"
            params {
              fields: {
                key: "id",
                value: {
                  number_value: 1
                }
              },
              fields: {
                key: "tid",
                value: {
                  number_value: 2
                }
              },
            }
            param_types: {
              key: "id"
              value: { code: 3 }
            }
            param_types: {
              key: "tid"
              value: { code: 3 }
            }
            partition_token: "$2"
          )",
          session.name(), partition_query_response.transaction().id(),
          partition_query_response.partitions()[0].partition_token()));

  grpc::ClientContext context;
  spanner_api::ResultSet query_response;
  ZETASQL_EXPECT_OK(raw_client()->ExecuteSql(&context, sql_request, &query_response));
}

TEST_F(PartitionQueryTest, CannotQueryWithInvalidQueryMode) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());

  spanner_api::PartitionQueryRequest partition_query_request =
      PARSE_TEXT_PROTO(absl::Substitute(
          R"(
            session: "$0"
            transaction { begin { read_only {} } }
            sql: "SELECT UserID, Name FROM Users"
          )",
          session.name()));

  spanner_api::PartitionResponse partition_query_response;
  {
    grpc::ClientContext context;
    ZETASQL_ASSERT_OK(raw_client()->PartitionQuery(&context, partition_query_request,
                                           &partition_query_response));
  }
  ASSERT_GT(partition_query_response.partitions().size(), 0);

  spanner_api::ExecuteSqlRequest sql_request =
      PARSE_TEXT_PROTO(absl::Substitute(
          R"(
            session: "$0"
            transaction { id: "$1" }
            sql: "SELECT UserID, Name FROM Users"
            partition_token: "$2"
          )",
          session.name(), partition_query_response.transaction().id(),
          partition_query_response.partitions()[0].partition_token()));

  // Query mode NORMAL (default) works with partition query.
  {
    sql_request.set_query_mode(spanner_api::ExecuteSqlRequest::NORMAL);
    grpc::ClientContext context;
    spanner_api::ResultSet query_response;
    ZETASQL_EXPECT_OK(raw_client()->ExecuteSql(&context, sql_request, &query_response));
  }

  // Query mode PLAN cannot be used with partition query.
  {
    sql_request.set_query_mode(spanner_api::ExecuteSqlRequest::PLAN);
    grpc::ClientContext context;
    spanner_api::ResultSet query_response;
    EXPECT_THAT(
        raw_client()->ExecuteSql(&context, sql_request, &query_response),
        StatusIs(absl::StatusCode::kInvalidArgument));
  }

  // Query mode PROFILE cannot be used with partition query.
  {
    sql_request.set_query_mode(spanner_api::ExecuteSqlRequest::PROFILE);
    grpc::ClientContext context;
    spanner_api::ResultSet query_response;
    EXPECT_THAT(
        raw_client()->ExecuteSql(&context, sql_request, &query_response),
        StatusIs(absl::StatusCode::kInvalidArgument));
  }
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
