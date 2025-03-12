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

#include "google/spanner/admin/database/v1/common.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "google/cloud/spanner/transaction.h"
#include "common/config.h"
#include "tests/conformance/common/database_test_base.h"
#include "tests/conformance/common/environment.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

// TODO: Replace all uses of internal C++ client library details.
using google::cloud::spanner_internal::MakeSingleUseTransaction;
using zetasql_base::testing::StatusIs;

class BatchDmlTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

  absl::Status SetUpDatabase() override {
    return SetSchemaFromFile("batchdml.test");
  }
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectBatchDmlTest, BatchDmlTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<BatchDmlTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(BatchDmlTest, EmptyBatchDmlRequestReturnsInvalidArgumentError) {
  // No Dml statements specified in the request.
  auto txn = Transaction(Transaction::ReadWriteOptions());
  auto result = BatchDmlTransaction(txn, {});
  EXPECT_THAT(result.status(), StatusIs(absl::StatusCode::kInvalidArgument));

  // Subsequent operations on the transaction succeed.
  result = BatchDmlTransaction(
      txn,
      {SqlStatement("INSERT INTO users(id, name, age) VALUES (1, 'Levin', 27)"),
       SqlStatement("UPDATE users SET name = 'Mark' WHERE id = 1")});
  ZETASQL_ASSERT_OK(ToUtilStatus(result.value().status));
  ZETASQL_ASSERT_OK(CommitTransaction(txn, {}));

  // Verify data after commit.
  EXPECT_THAT(Query("SELECT id, name, age FROM users"),
              IsOkAndHoldsRows({{1, "Mark", 27}}));
}

TEST_P(BatchDmlTest, ReadYourWrites) {
  auto txn = Transaction(Transaction::ReadWriteOptions());

  // Add rows into the database.
  auto result = BatchDmlTransaction(
      txn,
      {SqlStatement("INSERT INTO users(id, name, age) VALUES (1, 'Levin', 27)"),
       SqlStatement("UPDATE users SET name = 'Mark' WHERE id = 1")});
  ZETASQL_ASSERT_OK(ToUtilStatus(result.value().status));

  // Read data before commit.
  EXPECT_THAT(QueryTransaction(txn, "SELECT id, name, age FROM users"),
              IsOkAndHoldsRows({{1, "Mark", 27}}));
}

TEST_P(BatchDmlTest, DifferentDmlStatementsSucceed) {
  // Add token rows.
  ZETASQL_ASSERT_OK(CommitBatchDml(
      {SqlStatement("INSERT INTO users(id, name, age) VALUES (1, 'Levin', 27)"),
       SqlStatement(
           "INSERT INTO users(id, name, age) VALUES (2, 'Mark', 27)")}));

  // Insert, Update and Delete Dml statements all succeed.
  ZETASQL_ASSERT_OK(CommitBatchDml(
      {SqlStatement("INSERT INTO users(id, name, age) VALUES (3, 'Dan', 27)"),
       SqlStatement("UPDATE users SET name = 'Mark' WHERE id = 1"),
       SqlStatement("DELETE FROM users WHERE id = 2")}));

  // Read data to verify database.
  EXPECT_THAT(ReadAll("users", {"id", "name", "age"}),
              IsOkAndHoldsRows({{1, "Mark", 27}, {3, "Dan", 27}}));
}

TEST_P(BatchDmlTest, ConstraintErrorOnBatchDmlReplaysError) {
  ZETASQL_ASSERT_OK(CommitBatchDml({SqlStatement(
      "INSERT INTO users(id, name, age) VALUES (1, 'Levin', 27)")}));

  // Verify the row exists in the database.
  EXPECT_THAT(Query("SELECT id, name, age FROM users"),
              IsOkAndHoldsRows({{1, "Levin", 27}}));

  // BatchDml will fail with a constraint error - key already exists.
  auto txn = Transaction(Transaction::ReadWriteOptions());
  auto result = BatchDmlTransaction(
      txn, {SqlStatement(
               "INSERT INTO users(id, name, age) VALUES (1, 'Levin', 27)")});
  ZETASQL_EXPECT_OK(result);
  EXPECT_THAT(ToUtilStatus(result.value().status),
              StatusIs(absl::StatusCode::kAlreadyExists));

  // Subsequent operation will replay the same error.
  EXPECT_THAT(
      ExecuteDmlTransaction(
          txn, {SqlStatement("UPDATE users SET name = 'Mark' WHERE id = 1")}),
      StatusIs(absl::StatusCode::kAlreadyExists));

  // Subsequent commit will also replay the same error.
  EXPECT_THAT(CommitTransaction(txn, {}),
              StatusIs(absl::StatusCode::kAlreadyExists));
}

TEST_P(BatchDmlTest, QueryNotAllowedInBatchDml) {
  auto txn = Transaction(Transaction::ReadWriteOptions());

  // Invalid argument error for executing non-Dml statement.
  auto result = BatchDmlTransaction(txn, {SqlStatement("SELECT * FROM users")});
  ZETASQL_EXPECT_OK(result);
  EXPECT_THAT(ToUtilStatus(result.value().status),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Subsequent operations succeed on the same transaction.
  result = BatchDmlTransaction(
      txn, {SqlStatement(
               "INSERT INTO users(id, name, age) VALUES (1, 'Levin', 27)")});
  ZETASQL_ASSERT_OK(ToUtilStatus(result.value().status));
  ZETASQL_ASSERT_OK(CommitTransaction(txn, {}));

  // Verify the row exists in the database.
  EXPECT_THAT(Query("SELECT id, name, age FROM users"),
              IsOkAndHoldsRows({{1, "Levin", 27}}));
}

TEST_P(BatchDmlTest, MixDmlAndBatchDmlInTransactionSucceeds) {
  auto txn = Transaction(Transaction::ReadWriteOptions());

  // Executing Dml.
  ZETASQL_ASSERT_OK(ExecuteDmlTransaction(
      txn, {SqlStatement(
               "INSERT INTO users(id, name, age) VALUES (1, 'Levin', 27)")}));

  // Executing BatchDml.
  auto result = BatchDmlTransaction(
      txn, {SqlStatement(
               "INSERT INTO users(id, name, age) VALUES (2, 'Mark', 27)")});
  ZETASQL_ASSERT_OK(ToUtilStatus(result.value().status));

  // Commit.
  ZETASQL_ASSERT_OK(CommitTransaction(txn, {}));

  // Read data to verify database.
  EXPECT_THAT(ReadAll("users", {"id", "name", "age"}),
              IsOkAndHoldsRows({{1, "Levin", 27}, {2, "Mark", 27}}));
}

TEST_P(BatchDmlTest, ConcurrentTransactionWithBatchDmlNotAllowed) {
  auto current_probability = config::abort_current_transaction_probability();
  config::set_abort_current_transaction_probability(0);

  auto txn1 = Transaction(Transaction::ReadWriteOptions());
  auto txn2 = Transaction(Transaction::ReadWriteOptions());

  // Start operation on txn1.
  auto result = BatchDmlTransaction(
      txn1, {SqlStatement(
                "INSERT INTO users(id, name, age) VALUES (1, 'Levin', 27)")});
  ZETASQL_ASSERT_OK(result);
  ZETASQL_ASSERT_OK(ToUtilStatus(result.value().status));

  // Subsequent transactions will abort.
  result = BatchDmlTransaction(
      txn2, {SqlStatement(
                "INSERT INTO users(id, name, age) VALUES (2, 'Mark', 37)")});
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
        txn2, {SqlStatement(
                  "INSERT INTO users(id, name, age) VALUES (2, 'Mark', 37)")});
    // The Status can come from the call Status or the `BatchDmlResult`
    auto status = !result.ok() ? result.status() : ToUtilStatus(result->status);
    EXPECT_THAT(status, StatusIs(absl::StatusCode::kOk));
  }

  // Commit second transaction succeeds.
  ZETASQL_EXPECT_OK(CommitTransaction(txn2, {}));

  config::set_abort_current_transaction_probability(current_probability);
}

TEST_P(BatchDmlTest, InvalidDmlFailsButCommitSucceeds) {
  auto txn = Transaction(Transaction::ReadWriteOptions());

  // Invalid Dml since table does not exist.
  auto result = BatchDmlTransaction(
      txn,
      {SqlStatement(
          "INSERT INTO InvalidTable(id, name, age) VALUES (1, 'Levin', 27)")});
  if (dialect_ == database_api::POSTGRESQL) {
    EXPECT_THAT(ToUtilStatus(result.value().status),
                StatusIs(in_prod_env() ? absl::StatusCode::kInvalidArgument
                                       : absl::StatusCode::kNotFound));

  } else {
    EXPECT_THAT(ToUtilStatus(result.value().status),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }

  // Commit succeeds on same transaction.
  result = BatchDmlTransaction(
      txn,
      {SqlStatement("INSERT INTO users(id, name, age) VALUES (1, 'Levin', 27)"),
       SqlStatement("UPDATE users SET name = 'Mark' WHERE id = 1")});
  ZETASQL_ASSERT_OK(ToUtilStatus(result.value().status));
  ZETASQL_ASSERT_OK(CommitTransaction(txn, {}));

  // Verify the row exists in the database.
  EXPECT_THAT(Query("SELECT id, name, age FROM users"),
              IsOkAndHoldsRows({{1, "Mark", 27}}));
}

TEST_P(BatchDmlTest, BatchDmlFailsInReadOnlyTxn) {
  // SingleUse ReadOnly txn not allowed.
  auto txn = MakeSingleUseTransaction(
      Transaction::SingleUseOptions{Transaction::ReadOnlyOptions{}});

  auto result = BatchDmlTransaction(
      txn,
      {SqlStatement(
          "INSERT INTO InvalidTable(id, name, age) VALUES (1, 'Levin', 27)")});
  EXPECT_THAT(result.status(), StatusIs(absl::StatusCode::kInvalidArgument));

  // ReadOnly txn not allowed.
  txn = Transaction(Transaction::ReadOnlyOptions());
  result = BatchDmlTransaction(
      txn,
      {SqlStatement(
          "INSERT INTO InvalidTable(id, name, age) VALUES (1, 'Levin', 27)")});
  // The Status can come from the call Status or the `BatchDmlResult`
  auto status = !result.ok() ? result.status() : ToUtilStatus(result->status);
  if (dialect_ == database_api::POSTGRESQL) {
    EXPECT_THAT(status,
                StatusIs(in_prod_env() ? absl::StatusCode::kInvalidArgument
                                       : absl::StatusCode::kNotFound));
  } else {
    EXPECT_THAT(status, StatusIs(absl::StatusCode::kInvalidArgument));
  }
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
