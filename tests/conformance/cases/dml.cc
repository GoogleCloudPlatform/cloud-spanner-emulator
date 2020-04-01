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

#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using zetasql_base::testing::StatusIs;

class DmlTest : public DatabaseTest {
 public:
  zetasql_base::Status SetUpDatabase() override {
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
  void PopulateDatabase() {
    // Write fixure data to use in delete tests.
    ZETASQL_EXPECT_OK(CommitDml(
        {SqlStatement("INSERT Users(ID, Name, Age) Values (1, 'Levin', 27), "
                      "(2, 'Mark', 32), (10, 'Douglas', 31)")}));
  }
};

TEST_F(DmlTest, CanInsertAndUpdateInSameTransaction) {
  // Note that column Age is not part of update columns.
  ZETASQL_ASSERT_OK(CommitDml(
      {SqlStatement("INSERT Users(ID, Name, Age) VALUES (1, 'Levin', 27)"),
       SqlStatement("UPDATE Users SET Name = 'Mark' WHERE ID = 1")}));
  EXPECT_THAT(Query("SELECT ID, Name, Age FROM Users"),
              IsOkAndHoldsRows({{1, "Mark", 27}}));
}

TEST_F(DmlTest, InsertsNullValuesForUnspecifiedColumns) {
  // Nullable columns that are not specified are assigned default null values.
  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement("INSERT Users(ID, Updated) VALUES "
                              "(10, '2015-10-13T02:19:40Z')")}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto timestamp,
                       ParseRFC3339TimeSeconds("2015-10-13T02:19:40Z"));
  EXPECT_THAT(Query("SELECT ID, Name, Age, Updated FROM Users"),
              IsOkAndHoldsRows({{10, Null<std::string>(), Null<std::int64_t>(),
                                 timestamp}}));
}

TEST_F(DmlTest, CanInsertPrimaryKeyOnly) {
  ZETASQL_ASSERT_OK(CommitDml({SqlStatement("INSERT Users(ID) VALUES (10)")}));
  EXPECT_THAT(Query("SELECT ID, Name, Age, Updated FROM Users"),
              IsOkAndHoldsRows({{10, Null<std::string>(), Null<std::int64_t>(),
                                 Null<Timestamp>()}}));
}

TEST_F(DmlTest, CanDeleteFromTable) {
  PopulateDatabase();
  EXPECT_THAT(Query("SELECT ID, Name, Age FROM Users ORDER BY ID"),
              IsOkAndHoldsRows(
                  {{1, "Levin", 27}, {2, "Mark", 32}, {10, "Douglas", 31}}));

  ZETASQL_EXPECT_OK(CommitDml({SqlStatement("DELETE FROM Users WHERE true")}));

  EXPECT_THAT(Query("SELECT ID, Name, Age FROM Users"), IsOkAndHoldsRows({}));
}

TEST_F(DmlTest, CanDeleteRangeFromTable) {
  PopulateDatabase();
  EXPECT_THAT(Query("SELECT ID, Name, Age FROM Users ORDER BY ID"),
              IsOkAndHoldsRows(
                  {{1, "Levin", 27}, {2, "Mark", 32}, {10, "Douglas", 31}}));

  // Should delete only User with ID 2.
  ZETASQL_EXPECT_OK(
      CommitDml({SqlStatement("DELETE FROM Users WHERE ID > 1 AND ID < 10")}));

  EXPECT_THAT(Query("SELECT ID, Name, Age FROM Users ORDER BY ID"),
              IsOkAndHoldsRows({{1, "Levin", 27}, {10, "Douglas", 31}}));
}

TEST_F(DmlTest, DeleteWithEmptyKeysIsNoOp) {
  PopulateDatabase();
  EXPECT_THAT(Query("SELECT ID, Name, Age FROM Users ORDER BY ID"),
              IsOkAndHoldsRows(
                  {{1, "Levin", 27}, {2, "Mark", 32}, {10, "Douglas", 31}}));

  ZETASQL_EXPECT_OK(CommitDml({SqlStatement("DELETE FROM Users WHERE false")}));

  EXPECT_THAT(Query("SELECT ID, Name, Age FROM Users ORDER BY ID"),
              IsOkAndHoldsRows(
                  {{1, "Levin", 27}, {2, "Mark", 32}, {10, "Douglas", 31}}));
}

TEST_F(DmlTest, CanExecuteUpdateAfterDelete) {
  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement("INSERT Users(ID, Updated) Values "
                              "(10, '2015-10-13T02:19:40Z')")}));
  ZETASQL_EXPECT_OK(CommitDml(
      {SqlStatement("DELETE FROM Users WHERE ID = 10"),
       SqlStatement("UPDATE Users SET Name = 'Mark' WHERE ID = 10")}));

  EXPECT_THAT(Query("SELECT ID, Name, Age FROM Users"), IsOkAndHoldsRows({}));
}

TEST_F(DmlTest, CanUpdateWithNullValue) {
  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement("INSERT INTO Users(ID, Name, Age) "
                              "Values (1, 'Levin', 27)")}));
  ASSERT_THAT(Query("SELECT ID, Name, Age FROM Users WHERE Name IS NOT NULL"),
              IsOkAndHoldsRows({{1, "Levin", 27}}));

  // Update Name to Null in the existing row.
  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement("UPDATE Users SET Name = NULL WHERE true")}));
  EXPECT_THAT(Query("SELECT ID, Name, Age FROM Users WHERE Name IS NOT NULL"),
              IsOkAndHoldsRows({}));
}

TEST_F(DmlTest, CanInsertIntoTableWithNullableKey) {
  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement("INSERT INTO Nullable(Key, Value) "
                              "Values (NULL, 'Value1')")}));
  EXPECT_THAT(Query("SELECT Key, Value FROM Nullable"),
              IsOkAndHoldsRows({{Null<std::int64_t>(), "Value1"}}));
}

TEST_F(DmlTest, CannotInsertMultipleRowsIntoSingletonTable) {
  // Cannot insert multiple rows in same dml statement in a singleton table.
  EXPECT_THAT(
      CommitDml({SqlStatement("INSERT INTO Singleton (Col1, Col2) Values "
                              "('val11', 'val21'), ('val12', 'val22')")}),
      StatusIs(in_prod_env() ? zetasql_base::StatusCode::kInvalidArgument
                             : zetasql_base::StatusCode::kAlreadyExists));

  // Cannot insert multiple rows in multiple dml statements either in a
  // singleton table.
  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement("INSERT INTO Singleton (Col1, Col2) Values "
                              "('val11', 'val21')")}));
  EXPECT_THAT(
      CommitDml({SqlStatement("INSERT INTO Singleton (Col1, Col2) Values "
                              "('val12', 'val22')")}),
      StatusIs(zetasql_base::StatusCode::kAlreadyExists));
}

TEST_F(DmlTest, CanUpdateEmptySingletonTable) {
  ZETASQL_EXPECT_OK(CommitDml(
      {SqlStatement("UPDATE Singleton SET Col1 = 'val1' WHERE true")}));
  EXPECT_THAT(Query("SELECT Col1, Col2 FROM Singleton"), IsOkAndHoldsRows({}));
}

TEST_F(DmlTest, CanUpdateSingletonTable) {
  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement("INSERT INTO Singleton (Col1, Col2) Values "
                              "('val11', 'val21')")}));
  ZETASQL_EXPECT_OK(CommitDml({SqlStatement(
      "UPDATE Singleton SET Col1 = 'val12', Col2 = 'val22' WHERE true")}));
  EXPECT_THAT(Query("SELECT Col1, Col2 FROM Singleton"),
              IsOkAndHoldsRows({{"val12", "val22"}}));
}

TEST_F(DmlTest, CanDeleteFromEmptySingletonTable) {
  EXPECT_THAT(Query("SELECT Col1, Col2 FROM Singleton"), IsOkAndHoldsRows({}));
  ZETASQL_ASSERT_OK(CommitDml({SqlStatement("DELETE FROM Singleton WHERE true")}));
  EXPECT_THAT(Query("SELECT Col1, Col2 FROM Singleton"), IsOkAndHoldsRows({}));
}

TEST_F(DmlTest, CanDeleteFromSingletonTable) {
  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement("INSERT INTO Singleton (Col1, Col2) Values "
                              "('val11', 'val21')")}));
  ZETASQL_ASSERT_OK(CommitDml({SqlStatement("DELETE FROM Singleton WHERE true")}));
  EXPECT_THAT(Query("SELECT Col1, Col2 FROM Singleton"), IsOkAndHoldsRows({}));
}

TEST_F(DmlTest, DeleteWithEmptyKeysIsNoOpForSingletonTable) {
  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement("INSERT INTO Singleton (Col1, Col2) Values "
                              "('val11', 'val21')")}));
  ZETASQL_ASSERT_OK(CommitDml({SqlStatement("DELETE FROM Singleton WHERE false")}));
  EXPECT_THAT(Query("SELECT Col1, Col2 FROM Singleton"),
              IsOkAndHoldsRows({{"val11", "val21"}}));
}

TEST_F(DmlTest, CannotUseReadOnlyTransaction) {
  EXPECT_THAT(
      ExecuteDmlTransaction(Transaction(Transaction::ReadOnlyOptions()),
                            SqlStatement("INSERT INTO Users(ID) VALUES(1)")),
      StatusIs(zetasql_base::StatusCode::kInvalidArgument));

  EXPECT_THAT(QuerySingleUseTransaction(
                  Transaction::SingleUseOptions{Transaction::ReadOnlyOptions{}},
                  SqlStatement("INSERT INTO Users(ID) VALUES(1)")),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

TEST_F(DmlTest, CanInsertToArrayColumns) {
  // Array literals.
  ZETASQL_EXPECT_OK(
      CommitDml({SqlStatement("INSERT INTO ArrayFields(Key, ArrayCol) "
                              "VALUES(1, ARRAY<INT64>[10])")}));

  // Array parameters.
  ZETASQL_EXPECT_OK(CommitDml(
      {SqlStatement("INSERT INTO ArrayFields(Key, ArrayCol) "
                    "VALUES(@p1, @p2)",
                    SqlStatement::ParamType{
                        {"p1", Value(2)},
                        {"p2", Value(std::vector<int64_t>{10, 20, 30})}})}));

  EXPECT_THAT(Query("SELECT Key, ArrayCol FROM ArrayFields ORDER BY Key"),
              IsOkAndHoldsRows({
                  {1, Value(std::vector<int64_t>{10})},
                  {2, Value(std::vector<int64_t>{10, 20, 30})},
              }));
}

TEST_F(DmlTest, CanInsertMultipleRowsUsingStructParam) {
  using StructType = std::tuple<int64_t, std::string>;
  ZETASQL_EXPECT_OK(CommitDml({SqlStatement(
      "INSERT INTO Users(ID, Name) "
      "SELECT * FROM UNNEST(@p1)",
      SqlStatement::ParamType{
          {"p1", Value(std::vector<StructType>{StructType{1, "abc"},
                                               StructType{2, "def"}})}})}));

  EXPECT_THAT(Query("SELECT ID, Name FROM Users ORDER BY ID"),
              IsOkAndHoldsRows({{1, "abc"}, {2, "def"}}));
}

TEST_F(DmlTest, CannotCommitWithBadMutation) {
  Transaction txn{Transaction::ReadWriteOptions{}};
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto result,
      ExecuteDmlTransaction(
          txn, SqlStatement("INSERT INTO Users(ID, Name) VALUES(1, \"abc\")")));

  // Check read-your-writes
  EXPECT_THAT(QueryTransaction(txn, "SELECT ID, Name FROM Users"),
              IsOkAndHoldsRow({1, "abc"}));

  // Build an invalid mutation referencing a non-existent column.
  auto mut1 = MakeInsert("Users", {"ID"}, 2);
  auto mut2 = MakeInsert("Users", {"NON_EXISTENT_COLUMN"}, 3);
  auto mut3 = MakeInsert("Users", {"ID"}, 4);

  // Try to commit the transaction, it should fail.
  EXPECT_THAT(
      CommitTransaction(txn,
                        {
                            MakeInsert("Users", {"ID"}, 2),
                            MakeInsert("Users", {"NON_EXISTENT_COLUMN"}, 3),
                            MakeInsert("Users", {"ID"}, 4),
                        }),
      StatusIs(zetasql_base::StatusCode::kNotFound));

  // Check that the DML statement was not committed.
  EXPECT_THAT(Query("SELECT ID FROM Users"), IsOkAndHoldsRows({}));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
