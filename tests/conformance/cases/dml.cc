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
      {SqlStatement("INSERT Users(ID, Name, Age) Values (1, 'Levin', 27)"),
       SqlStatement("UPDATE Users SET Name = 'Mark' WHERE ID = 1")}));
  EXPECT_THAT(Query("SELECT ID, Name, Age FROM Users"),
              IsOkAndHoldsRows({{1, "Mark", 27}}));
}

TEST_F(DmlTest, InsertsNullValuesForUnspecifiedColumns) {
  // Nullable columns that are not specified are assigned default null values.
  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement("INSERT Users(ID, Updated) Values "
                              "(10, '2015-10-13T02:19:40Z')")}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto timestamp,
                       ParseRFC3339TimeSeconds("2015-10-13T02:19:40Z"));
  EXPECT_THAT(Query("SELECT ID, Name, Age, Updated FROM Users"),
              IsOkAndHoldsRows({{10, Null<std::string>(), Null<std::int64_t>(),
                                 timestamp}}));
}

TEST_F(DmlTest, CanInsertPrimaryKeyOnly) {
  ZETASQL_ASSERT_OK(CommitDml({SqlStatement("INSERT Users(ID) Values (10)")}));
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
  ZETASQL_ASSERT_OK(CommitDml({SqlStatement("DELETE FROM Singleton WHERE true")}));
}

TEST_F(DmlTest, CanDeleteFromSingletonTable) {
  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement("INSERT INTO Singleton (Col1, Col2) Values "
                              "('val11', 'val21')")}));
  ZETASQL_ASSERT_OK(CommitDml({SqlStatement("DELETE FROM Singleton WHERE true")}));
  EXPECT_THAT(Query("SELECT Col1, Col2 FROM Singleton"), IsOkAndHoldsRows({}));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
