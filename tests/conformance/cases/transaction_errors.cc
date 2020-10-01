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
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "absl/strings/str_cat.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using zetasql_base::testing::StatusIs;

class TransactionErrorTest : public DatabaseTest {
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
        R"(
          CREATE TABLE Nullable(
            Key      INT64,
            Value    STRING(MAX),
          ) PRIMARY KEY (Key)
        )",
        R"(
          CREATE TABLE Singleton(
            Col1     STRING(MAX),
            Col2     STRING(MAX),
          ) PRIMARY KEY ()
        )",
        R"(CREATE TABLE ArrayFields(
            Key      INT64,
            ArrayCol ARRAY<INT64>,
          ) PRIMARY KEY(Key)
        )"});
  }

 protected:
  zetasql_base::StatusOr<DmlResult> ExecuteDml(Transaction txn,
                                       const std::string& statement) {
    return ExecuteDmlTransaction(txn, SqlStatement(statement));
  }

  zetasql_base::StatusOr<CommitResult> Commit(Transaction txn, Mutations mutations) {
    return CommitTransaction(txn, mutations);
  }

  zetasql_base::StatusOr<std::vector<ValueRow>> Query(const std::string& query) {
    return DatabaseTest::Query(query);
  }

  zetasql_base::StatusOr<std::vector<ValueRow>> Query(Transaction txn,
                                              const std::string& query) {
    return QueryTransaction(txn, query);
  }
};

TEST_F(TransactionErrorTest, DMLTableNotFoundDoesNotInvalidateTransaction) {
  Transaction txn{Transaction::ReadWriteOptions{}};
  ZETASQL_ASSERT_OK(ExecuteDml(txn, "INSERT INTO Users(ID, Name) VALUES(1, 'abc')"));

  // Try to read from a non-existent table.
  EXPECT_THAT(Query(txn, "SELECT ID, Name FROM FAKE_TABLE"),
              StatusIs(absl::StatusCode::kInvalidArgument));

    // Try to commit the transaction, it should succeed.
    ZETASQL_EXPECT_OK(Commit(txn, {
                              MakeInsert("Users", {"ID"}, 2),
                              MakeInsert("Users", {"ID"}, 3),
                          }));

    EXPECT_THAT(Query("SELECT ID FROM Users"),
                IsOkAndHoldsUnorderedRows({{1}, {2}, {3}}));
}

TEST_F(TransactionErrorTest, DMLCommitTableNotFoundInvalidatesTransaction) {
  Transaction txn{Transaction::ReadWriteOptions{}};
  ZETASQL_ASSERT_OK(ExecuteDml(txn, "INSERT INTO Users(ID, Name) VALUES(1, 'abc')"));

  // Try to commit the transaction, it should fail.
  EXPECT_THAT(Commit(txn,
                     {
                         MakeInsert("FAKE_TABLE", {"ID"}, 2),
                         MakeInsert("Users", {"ID"}, 3),
                     }),
              StatusIs(absl::StatusCode::kNotFound));

  // Second commit should also fail with same error.
  EXPECT_THAT(Commit(txn, {MakeInsert("Users", {"ID"}, 4)}),
              StatusIs(absl::StatusCode::kNotFound));

  EXPECT_THAT(Query("SELECT ID FROM Users"), IsOkAndHoldsRows({}));
}

TEST_F(TransactionErrorTest, DMLInvalidUpdateDoesNotInvalidateTransaction) {
  Transaction txn{Transaction::ReadWriteOptions{}};
  ZETASQL_ASSERT_OK(ExecuteDml(txn, "INSERT INTO Users(ID, Name) VALUES(1, 'abc')"));

  // Key does not exist
  ZETASQL_EXPECT_OK(Query(txn, "UPDATE Users SET Name = 'Test' WHERE ID = 22"));
  // Column does not exist
  EXPECT_THAT(Query(txn, "UPDATE Users SET Fake = 'Test' WHERE ID = 1"),
              StatusIs(absl::StatusCode::kInvalidArgument));
  // Table does not exist
  EXPECT_THAT(Query(txn, "UPDATE Fake_Table SET Name = 'Test' WHERE ID = 1"),
              StatusIs(absl::StatusCode::kInvalidArgument));

  ZETASQL_EXPECT_OK(Commit(txn, {MakeInsert("Users", {"ID"}, 3)}));

  // Commit should have succeeded and populated table.
  EXPECT_THAT(Query("SELECT ID FROM Users"),
              IsOkAndHoldsUnorderedRows({{1}, {3}}));
}

TEST_F(TransactionErrorTest, DMLInvalidInsertInvalidatesTransaction) {
  Transaction txn{Transaction::ReadWriteOptions{}};
  ZETASQL_ASSERT_OK(ExecuteDml(txn, "INSERT INTO Users(ID, Name) VALUES(1, 'abc')"));

  // Insert a row that already exists which causes a constraint failure.
  EXPECT_THAT(Query(txn, "INSERT INTO Users(ID, Name) VALUES(1, 'test')"),
              StatusIs(absl::StatusCode::kAlreadyExists));

  // Attempting another insert fails even with a new row.
  EXPECT_THAT(Query(txn, "INSERT INTO Users(ID, Name) VALUES(2, 'test')"),
              StatusIs(absl::StatusCode::kAlreadyExists));

  // Try to commit the transaction, it should fail with the same error that
  // was reported earlier.
  EXPECT_THAT(Commit(txn,
                     {
                         MakeInsert("Users", {"ID"}, 3),
                         MakeInsert("Users", {"ID"}, 4),
                     }),
              StatusIs(absl::StatusCode::kAlreadyExists));

  // Second commit should also fail with same error.
  EXPECT_THAT(Commit(txn, {MakeInsert("Users", {"ID"}, 5)}),
              StatusIs(absl::StatusCode::kAlreadyExists));

  // Ensure that nothing was committed.
  EXPECT_THAT(Query("SELECT ID FROM Users"), IsOkAndHoldsRows({}));
}

TEST_F(TransactionErrorTest, DMLReadErrorDoesNotInvalidateTransaction) {
  Transaction txn{Transaction::ReadWriteOptions{}};
  ZETASQL_ASSERT_OK(ExecuteDml(txn, "INSERT INTO Users(ID, Name) VALUES(2, 'value')"));

  EXPECT_THAT(Query(txn, "SELECT ID FROM Fake_Table"),
              StatusIs(absl::StatusCode::kInvalidArgument));

  ZETASQL_EXPECT_OK(Commit(txn, {MakeInsert("Users", {"ID", "Name"}, 1, "value")}));

  // Table should be empty
  EXPECT_THAT(Query("SELECT ID FROM Users"), IsOkAndHoldsUnorderedRows({1, 2}));
}

TEST_F(TransactionErrorTest, DeleteErrorInCommitInvalidatesTransaction) {
  Transaction txn{Transaction::ReadWriteOptions{}};
  EXPECT_THAT(Commit(txn, {MakeInsert("Users", {"ID", "Name"}, 1, "abc"),
                           MakeDelete("NONE", KeySet::All())}),
              StatusIs(absl::StatusCode::kNotFound));

  // Second commit should also fail with same error.
  EXPECT_THAT(Commit(txn, {MakeInsert("Users", {"ID"}, 5)}),
              StatusIs(absl::StatusCode::kNotFound));

  // Table should be empty
  EXPECT_THAT(Query("SELECT ID FROM Users"), IsOkAndHoldsRows({}));
}

TEST_F(TransactionErrorTest,
       UpdateWithNonExistentKeyInCommitInvalidatesTransaction) {
  Transaction txn{Transaction::ReadWriteOptions{}};
  // This case checks that an update to a non-existent row causes commit to
  // fail.
  EXPECT_THAT(Commit(txn, {MakeInsert("Users", {"ID", "Name"}, 1, "abc"),
                           MakeUpdate("Users", {"ID", "Name"}, 22, "Test")}),
              StatusIs(absl::StatusCode::kNotFound));

  // Second commit should also fail with same error.
  EXPECT_THAT(Commit(txn, {MakeInsert("Users", {"ID"}, 5)}),
              StatusIs(absl::StatusCode::kNotFound));

  // Table only contain 1
  EXPECT_THAT(Query("SELECT ID FROM Users"), IsOkAndHoldsRows({}));
}

TEST_F(TransactionErrorTest, InvalidUpdateInCommitInvalidatesTransaction) {
  Transaction txn{Transaction::ReadWriteOptions{}};
  // This case checks that an update with an invalid column causes the commit to
  // fail.
  EXPECT_THAT(Commit(txn, {MakeInsert("Users", {"ID", "Name"}, 1, "abc"),
                           MakeUpdate("Users", {"ID", "Fake"}, 1, "Test")}),
              StatusIs(absl::StatusCode::kNotFound));

  // Second commit should also fail with same error.
  EXPECT_THAT(Commit(txn, {MakeInsert("Users", {"ID"}, 5)}),
              StatusIs(absl::StatusCode::kNotFound));

  // Table should be empty
  EXPECT_THAT(Query("SELECT ID FROM Users"), IsOkAndHoldsRows({}));
}

TEST_F(TransactionErrorTest, InsertInvalidColumnsInvalidatesTransaction) {
  Transaction txn{Transaction::ReadWriteOptions{}};
  EXPECT_THAT(Commit(txn, {MakeInsert("Users", {"FakeID", "Data"}, 1, "test")}),
              StatusIs(absl::StatusCode::kNotFound));

  // Second commit should also fail with same error.
  EXPECT_THAT(Commit(txn, {MakeInsert("Users", {"ID"}, 5)}),
              StatusIs(absl::StatusCode::kNotFound));

  // Table should be empty
  EXPECT_THAT(Query("SELECT ID FROM Users"), IsOkAndHoldsRows({}));
}

TEST_F(TransactionErrorTest, RollbackSucceedsAfterInsertInvalidColumns) {
  Transaction txn{Transaction::ReadWriteOptions{}};
  ZETASQL_ASSERT_OK(ExecuteDml(txn, "INSERT INTO Users(ID, Name) VALUES(1, 'abc')"));

  // Insert a row that already exists which causes a constraint failure.
  EXPECT_THAT(Query(txn, "INSERT INTO Users(ID, Name) VALUES(1, 'test')"),
              StatusIs(absl::StatusCode::kAlreadyExists));

  // A rollback against a transaction with a constraint failure should still
  // succeed.
  ZETASQL_EXPECT_OK(Rollback(txn));

  // Cannot commit since it has been rolledback.
  EXPECT_THAT(Commit(txn,
                     {
                         MakeInsert("Users", {"ID"}, 2),
                         MakeInsert("Users", {"ID"}, 3),
                     }),
              StatusIs(absl::StatusCode::kFailedPrecondition));

  // Table should be empty
  EXPECT_THAT(Query("SELECT ID FROM Users"), IsOkAndHoldsRows({}));
}

TEST_F(TransactionErrorTest,
       RollbackOfReadOnlyTransactionFailsButDoesNotInvalidate) {
  Transaction txn{Transaction::ReadOnlyOptions{}};

  // Read all rows.
  ZETASQL_EXPECT_OK(Read(txn, "Users", {"ID", "Name"}, KeySet::All()));

  // A read only transaction cannot be rolledback.
  EXPECT_THAT(Rollback(txn), StatusIs(absl::StatusCode::kFailedPrecondition));

  // Attempt to read again after the failed rollback.
  EXPECT_THAT(Read(txn, "Users", {"ID", "Name"}, KeySet::All()),
              in_prod_env() ? StatusIs(absl::StatusCode::kOk)
                            : StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(TransactionErrorTest, ReadAfterInvalidatedCommitReturnsError) {
  Transaction txn{Transaction::ReadWriteOptions{}};

  // Insert a row.
  ZETASQL_ASSERT_OK(ExecuteDml(txn, "INSERT INTO Users(ID, Name) VALUES(1, 'value')"));

  // Insert the same row that was previously added. This will return an error.
  EXPECT_THAT(ExecuteDml(txn, "INSERT INTO Users(ID, Name) VALUES(1, 'value')"),
              StatusIs(absl::StatusCode::kAlreadyExists));

  EXPECT_THAT(Commit(txn, {}), StatusIs(absl::StatusCode::kAlreadyExists));

  // Attempt to read again after a failed commit. This will not succeed because
  // a read cannot occur after a commit (failed or not).
  EXPECT_THAT(Read(txn, "Users", {"ID", "Name"}, KeySet::All()),
              (in_prod_env() ? StatusIs(absl::StatusCode::kFailedPrecondition)
                             : StatusIs(absl::StatusCode::kAlreadyExists)));
}

TEST_F(TransactionErrorTest, DISABLED_ReadAfterInvalidatedDmlSucceeds) {
  Transaction txn{Transaction::ReadWriteOptions{}};
  // Insert a row.
  ZETASQL_ASSERT_OK(ExecuteDml(txn, "INSERT INTO Users(ID, Name) VALUES(1, 'value')"));

  // Insert the same row that was previously added. This will return an error.
  EXPECT_THAT(ExecuteDml(txn, "INSERT INTO Users(ID, Name) VALUES(1, 'value')"),
              StatusIs(absl::StatusCode::kAlreadyExists));

  // Attempt to read again after an error was encountered. The read should
  // succeed without replaying the error.
  ZETASQL_EXPECT_OK(Read(txn, "Users", {"ID", "Name"}, KeySet::All()));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
