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

#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

// TODO: Replace all uses of internal C++ client library details.
using google::cloud::spanner_internal::MakeSingleUseTransaction;
using zetasql_base::testing::StatusIs;

class BatchDmlTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    return SetSchema({
        R"(
          CREATE TABLE Users(
          ID       INT64 NOT NULL,
          Name     STRING(MAX),
          Age      INT64,
          Updated  TIMESTAMP,
        ) PRIMARY KEY (ID)
      )",
    });
  }
};

TEST_F(BatchDmlTest, EmptyBatchDmlRequestReturnsInvalidArgumentError) {
  // No Dml statements specified in the request.
  auto txn = Transaction(Transaction::ReadWriteOptions());
  auto result = BatchDmlTransaction(txn, {});
  EXPECT_THAT(result.status(), StatusIs(absl::StatusCode::kInvalidArgument));

  // Subsequent operations on the transaction succeed.
  result = BatchDmlTransaction(
      txn, {SqlStatement("INSERT Users(ID, Name, Age) VALUES (1, 'Levin', 27)"),
            SqlStatement("UPDATE Users SET Name = 'Mark' WHERE ID = 1")});
  ZETASQL_ASSERT_OK(ToUtilStatus(result.value().status));
  ZETASQL_ASSERT_OK(CommitTransaction(txn, {}));

  // Verify data after commit.
  EXPECT_THAT(Query("SELECT ID, Name, Age FROM Users"),
              IsOkAndHoldsRows({{1, "Mark", 27}}));
}
TEST_F(BatchDmlTest, ReadYourWrites) {
  auto txn = Transaction(Transaction::ReadWriteOptions());

  // Add rows into the database.
  auto result = BatchDmlTransaction(
      txn, {SqlStatement("INSERT Users(ID, Name, Age) VALUES (1, 'Levin', 27)"),
            SqlStatement("UPDATE Users SET Name = 'Mark' WHERE ID = 1")});
  ZETASQL_ASSERT_OK(ToUtilStatus(result.value().status));

  // Read data before commit.
  EXPECT_THAT(QueryTransaction(txn, "SELECT ID, Name, Age FROM Users"),
              IsOkAndHoldsRows({{1, "Mark", 27}}));
}

TEST_F(BatchDmlTest, DifferentDmlStatementsSucceed) {
  // Add token rows.
  ZETASQL_ASSERT_OK(CommitBatchDml(
      {SqlStatement("INSERT Users(ID, Name, Age) VALUES (1, 'Levin', 27)"),
       SqlStatement("INSERT Users(ID, Name, Age) VALUES (2, 'Mark', 27)")}));

  // Insert, Update and Delete Dml statements all succeed.
  ZETASQL_ASSERT_OK(CommitBatchDml(
      {SqlStatement("INSERT Users(ID, Name, Age) VALUES (3, 'Dan', 27)"),
       SqlStatement("UPDATE Users SET Name = 'Mark' WHERE ID = 1"),
       SqlStatement("DELETE FROM Users WHERE ID = 2")}));

  // Read data to verify database.
  EXPECT_THAT(ReadAll("Users", {"ID", "Name", "Age"}),
              IsOkAndHoldsRows({{1, "Mark", 27}, {3, "Dan", 27}}));
}

TEST_F(BatchDmlTest, ConstraintErrorOnBatchDmlReplaysError) {
  ZETASQL_ASSERT_OK(CommitBatchDml(
      {SqlStatement("INSERT Users(ID, Name, Age) VALUES (1, 'Levin', 27)")}));

  // Verify the row exists in the database.
  EXPECT_THAT(Query("SELECT ID, Name, Age FROM Users"),
              IsOkAndHoldsRows({{1, "Levin", 27}}));

  // BatchDml will fail with a constraint error - key already exists.
  auto txn = Transaction(Transaction::ReadWriteOptions());
  auto result = BatchDmlTransaction(
      txn,
      {SqlStatement("INSERT Users(ID, Name, Age) VALUES (1, 'Levin', 27)")});
  ZETASQL_EXPECT_OK(result);
  EXPECT_THAT(ToUtilStatus(result.value().status),
              StatusIs(absl::StatusCode::kAlreadyExists));

  // Subsequent operation will replay the same error.
  EXPECT_THAT(
      ExecuteDmlTransaction(
          txn, {SqlStatement("UPDATE Users SET Name = 'Mark' WHERE ID = 1")}),
      StatusIs(absl::StatusCode::kAlreadyExists));

  // Subsequent commit will also replay the same error.
  EXPECT_THAT(CommitTransaction(txn, {}),
              StatusIs(absl::StatusCode::kAlreadyExists));
}

TEST_F(BatchDmlTest, QueryNotAllowedInBatchDml) {
  auto txn = Transaction(Transaction::ReadWriteOptions());

  // Invalid argument error for executing non-Dml statement.
  auto result = BatchDmlTransaction(txn, {SqlStatement("SELECT * FROM Users")});
  ZETASQL_EXPECT_OK(result);
  EXPECT_THAT(ToUtilStatus(result.value().status),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Subsequent operations succeed on the same transaction.
  result = BatchDmlTransaction(
      txn,
      {SqlStatement("INSERT Users(ID, Name, Age) VALUES (1, 'Levin', 27)")});
  ZETASQL_ASSERT_OK(ToUtilStatus(result.value().status));
  ZETASQL_ASSERT_OK(CommitTransaction(txn, {}));

  // Verify the row exists in the database.
  EXPECT_THAT(Query("SELECT ID, Name, Age FROM Users"),
              IsOkAndHoldsRows({{1, "Levin", 27}}));
}

TEST_F(BatchDmlTest, MixDmlAndBatchDmlInTransactionSucceeds) {
  auto txn = Transaction(Transaction::ReadWriteOptions());

  // Executing Dml.
  ZETASQL_ASSERT_OK(ExecuteDmlTransaction(
      txn,
      {SqlStatement("INSERT Users(ID, Name, Age) VALUES (1, 'Levin', 27)")}));

  // Executing BatchDml.
  auto result = BatchDmlTransaction(
      txn,
      {SqlStatement("INSERT Users(ID, Name, Age) VALUES (2, 'Mark', 27)")});
  ZETASQL_ASSERT_OK(ToUtilStatus(result.value().status));

  // Commit.
  ZETASQL_ASSERT_OK(CommitTransaction(txn, {}));

  // Read data to verify database.
  EXPECT_THAT(ReadAll("Users", {"ID", "Name", "Age"}),
              IsOkAndHoldsRows({{1, "Levin", 27}, {2, "Mark", 27}}));
}

TEST_F(BatchDmlTest, ConcurrentTransactionWithBatchDmlNotAllowed) {
  auto txn1 = Transaction(Transaction::ReadWriteOptions());
  auto txn2 = Transaction(Transaction::ReadWriteOptions());

  // Start operation on txn1.
  auto result = BatchDmlTransaction(
      txn1,
      {SqlStatement("INSERT Users(ID, Name, Age) VALUES (1, 'Levin', 27)")});
  ZETASQL_ASSERT_OK(result);
  ZETASQL_ASSERT_OK(ToUtilStatus(result.value().status));

  // Subsequent transactions will abort.
  result = BatchDmlTransaction(
      txn2,
      {SqlStatement("INSERT Users(ID, Name, Age) VALUES (2, 'Mark', 37)")});
  // The Status can come from the call Status or the `BatchDmlResult`
  auto status = !result.ok() ? result.status() : ToUtilStatus(result->status);
  EXPECT_THAT(status, StatusIs(in_prod_env() ? absl::StatusCode::kOk
                                             : absl::StatusCode::kAborted));
  // Commit first transaction succeeds.
  ZETASQL_EXPECT_OK(CommitTransaction(txn1, {}));

  if (status.code() == absl::StatusCode::kAborted) {
    // A real application should use the Transaction runner which restarts the
    // transaction after ABORTED; we have to do it ourselves.
    txn2 = MakeReadWriteTransaction(txn2);

    // Subsequent txn2 can now make progress.
    result = BatchDmlTransaction(
        txn2,
        {SqlStatement("INSERT Users(ID, Name, Age) VALUES (2, 'Mark', 37)")});
    // The Status can come from the call Status or the `BatchDmlResult`
    auto status = !result.ok() ? result.status() : ToUtilStatus(result->status);
    EXPECT_THAT(status, StatusIs(absl::StatusCode::kOk));
  }

  // Commit second transaction succeeds.
  ZETASQL_EXPECT_OK(CommitTransaction(txn2, {}));
}

TEST_F(BatchDmlTest, InvalidDmlFailsButCommitSucceeds) {
  auto txn = Transaction(Transaction::ReadWriteOptions());

  // Invalid Dml since table does not exist.
  auto result = BatchDmlTransaction(
      txn, {SqlStatement(
               "INSERT InvalidTable(ID, Name, Age) VALUES (1, 'Levin', 27)")});
  EXPECT_THAT(ToUtilStatus(result.value().status),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Commit succeeds on same transaction.
  result = BatchDmlTransaction(
      txn, {SqlStatement("INSERT Users(ID, Name, Age) VALUES (1, 'Levin', 27)"),
            SqlStatement("UPDATE Users SET Name = 'Mark' WHERE ID = 1")});
  ZETASQL_ASSERT_OK(ToUtilStatus(result.value().status));
  ZETASQL_ASSERT_OK(CommitTransaction(txn, {}));

  // Verify the row exists in the database.
  EXPECT_THAT(Query("SELECT ID, Name, Age FROM Users"),
              IsOkAndHoldsRows({{1, "Mark", 27}}));
}

TEST_F(BatchDmlTest, BatchDmlFailsInReadOnlyTxn) {
  // SingleUse ReadOnly txn not allowed.
  auto txn = MakeSingleUseTransaction(
      Transaction::SingleUseOptions{Transaction::ReadOnlyOptions{}});

  auto result = BatchDmlTransaction(
      txn, {SqlStatement(
               "INSERT InvalidTable(ID, Name, Age) VALUES (1, 'Levin', 27)")});
  EXPECT_THAT(result.status(), StatusIs(absl::StatusCode::kInvalidArgument));

  // ReadOnly txn not allowed.
  txn = Transaction(Transaction::ReadOnlyOptions());
  result = BatchDmlTransaction(
      txn, {SqlStatement(
               "INSERT InvalidTable(ID, Name, Age) VALUES (1, 'Levin', 27)")});
  // The Status can come from the call Status or the `BatchDmlResult`
  auto status = !result.ok() ? result.status() : ToUtilStatus(result->status);
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInvalidArgument));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
