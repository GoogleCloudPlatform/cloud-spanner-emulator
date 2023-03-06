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

#include <string>
#include <tuple>
#include <vector>

#include "gmock/gmock.h"
#include "absl/status/statusor.h"
#include "google/cloud/spanner/value.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using zetasql_base::testing::StatusIs;

class DmlTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    EmulatorFeatureFlags::Flags flags;
    emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

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
        )",
        R"(CREATE TABLE NumericTable(
            Key    NUMERIC,
            Val    INT64,
          ) PRIMARY KEY(Key)
        )",
        R"(CREATE TABLE JsonTable(
            ID     INT64 NOT NULL,
            Val    JSON,
          ) PRIMARY KEY(ID)
        )",
        R"(CREATE TABLE TableGen(
            K      INT64,
            V1     INT64,
            V2     INT64,
            G1     INT64 AS (G2 + 1) STORED,
            G2     INT64 NOT NULL AS (v1 + v2) STORED,
            G3     INT64 AS (G1) STORED,
            V3     INT64 NOT NULL DEFAULT (2),
          ) PRIMARY KEY (K)
        )",
        "CREATE INDEX NullableIndex ON Nullable(Value)",
    });
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
      StatusIs(in_prod_env() ? absl::StatusCode::kInvalidArgument
                             : absl::StatusCode::kAlreadyExists));

  // Cannot insert multiple rows in multiple dml statements either in a
  // singleton table.
  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement("INSERT INTO Singleton (Col1, Col2) Values "
                              "('val11', 'val21')")}));
  EXPECT_THAT(
      CommitDml({SqlStatement("INSERT INTO Singleton (Col1, Col2) Values "
                              "('val12', 'val22')")}),
      StatusIs(absl::StatusCode::kAlreadyExists));
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
      StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(QuerySingleUseTransaction(
                  Transaction::SingleUseOptions{Transaction::ReadOnlyOptions{}},
                  SqlStatement("INSERT INTO Users(ID) VALUES(1)")),
              StatusIs(absl::StatusCode::kInvalidArgument));
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
      StatusIs(absl::StatusCode::kNotFound));

  // Check that the DML statement was not committed.
  EXPECT_THAT(Query("SELECT ID FROM Users"), IsOkAndHoldsRows({}));
}

TEST_F(DmlTest, CanUseIndexHintInInsertStatement) {
  ZETASQL_EXPECT_OK(Insert("Nullable", {"Key", "Value"}, {1, "Peter"}));

  ZETASQL_EXPECT_OK(
      CommitDml({SqlStatement("INSERT INTO Users(ID, Name) "
                              "SELECT Key, Value "
                              "FROM Nullable@{force_index=NullableIndex} ")}));

  EXPECT_THAT(Query("SELECT ID, Name FROM Users ORDER BY ID"),
              IsOkAndHoldsRow({1, "Peter"}));
}

TEST_F(DmlTest, CanUseIndexHintInUpdateStatement) {
  ZETASQL_EXPECT_OK(Insert("Nullable", {"Key", "Value"}, {1, "Peter"}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name"}, {1, "Paul"}));

  ZETASQL_EXPECT_OK(CommitDml({SqlStatement(
      "UPDATE Users SET Name = 'Peter' "
      "WHERE ID IN "
      "(SELECT Key FROM Nullable@{force_index=NullableIndex})")}));

  EXPECT_THAT(Query("SELECT ID, Name FROM Users ORDER BY ID"),
              IsOkAndHoldsRow({1, "Peter"}));
}

TEST_F(DmlTest, NumericKey) {
  // Insert DML
  ZETASQL_EXPECT_OK(CommitDml({SqlStatement(
      "INSERT NumericTable(Key, Val) Values (NUMERIC'-12.3', -1), "
      "(NUMERIC'0', 0), (NUMERIC'12.3', 1)")}));

  // Update DML
  ZETASQL_EXPECT_OK(CommitDml(
      {SqlStatement("UPDATE NumericTable SET Val = 2 WHERE Key = 12.3")}));

  EXPECT_THAT(Query("SELECT T.Val FROM NumericTable T ORDER BY T.Key"),
              IsOkAndHoldsRows({{-1}, {0}, {2}}));

  // Delete DML
  ZETASQL_EXPECT_OK(
      CommitDml({SqlStatement("DELETE FROM NumericTable T WHERE T.Key < 0")}));

  EXPECT_THAT(Query("SELECT T.Val FROM NumericTable T ORDER BY T.Key"),
              IsOkAndHoldsRows({{0}, {2}}));
}

TEST_F(DmlTest, JsonType) {
  // Insert DML
  ZETASQL_EXPECT_OK(CommitDml({SqlStatement(R"(
        INSERT INTO JsonTable(ID, Val) Values (3, JSON '{"a":"str"}')
  )")}));
  ZETASQL_EXPECT_OK(CommitDml({SqlStatement(R"(
        INSERT INTO JsonTable(ID, Val) Values (4, NULL)
  )")}));

  EXPECT_THAT(
      Query("SELECT TO_JSON_STRING(T.Val) FROM JsonTable T WHERE ID = 3"),
      IsOkAndHoldsRow({R"({"a":"str"})"}));
  EXPECT_THAT(Query("SELECT ID FROM JsonTable T WHERE Val IS NULL"),
              IsOkAndHoldsRow({Value(4)}));
  EXPECT_THAT(Query("SELECT ID FROM JsonTable T WHERE Val IS NOT NULL"),
              IsOkAndHoldsRow({Value(3)}));

  // Update DML
  ZETASQL_EXPECT_OK(CommitDml({SqlStatement(R"(
      UPDATE JsonTable
        SET Val = JSON '{"a":"newstr", "b":123}'
        WHERE ID = 3
  )")}));

  EXPECT_THAT(
      Query("SELECT TO_JSON_STRING(T.Val) FROM JsonTable T WHERE ID = 3"),
      IsOkAndHoldsRow({R"({"a":"newstr","b":123})"}));
}

TEST_F(DmlTest, Returning) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_dml_returning = true;
  emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

  // Insert THEN RETURN
  std::vector<ValueRow> result_for_insert;
  ZETASQL_EXPECT_OK(CommitDmlReturning(
      {SqlStatement("INSERT INTO Users(ID, Name, Age) Values (1, 'Levin', 27) "
                    "THEN RETURN Age;")},
      result_for_insert));

  absl::StatusOr<std::vector<ValueRow>> result_or = result_for_insert;
  EXPECT_THAT(result_or, IsOkAndHoldsRow({Value(27)}));

  EXPECT_THAT(Query("SELECT Age FROM Users WHERE ID = 1;"),
              IsOkAndHoldsRows({{27}}));

  // Update THEN RETURN
  std::vector<ValueRow> result_for_update;
  ZETASQL_EXPECT_OK(CommitDmlReturning(
      {SqlStatement("UPDATE Users SET Age = Age + 1 WHERE ID = 1 "
                    "THEN RETURN Age, Name;")},
      result_for_update));
  result_or = result_for_update;
  EXPECT_THAT(result_or, IsOkAndHoldsRow({28, "Levin"}));

  EXPECT_THAT(Query("SELECT Age, Name FROM Users WHERE ID = 1;"),
              IsOkAndHoldsRow({28, "Levin"}));

  // Delete THEN RETURN
  std::vector<ValueRow> result_for_delete;
  ZETASQL_EXPECT_OK(CommitDmlReturning({SqlStatement("DELETE FROM Users WHERE ID = 1 "
                                             "THEN RETURN Name, Age;")},
                               result_for_delete));
  result_or = result_for_delete;
  EXPECT_THAT(result_or, IsOkAndHoldsRow({"Levin", 28}));
  EXPECT_THAT(Query("SELECT Age, Name FROM Users WHERE ID = 1;"),
              IsOkAndHoldsRows({}));
}

TEST_F(DmlTest, ReturningGeneratedColumns) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_dml_returning = true;
  emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

  // Insert THEN RETURN
  std::vector<ValueRow> result_for_insert;
  ZETASQL_EXPECT_OK(CommitDmlReturning(
      {SqlStatement("INSERT INTO TableGen(K, V1, V2) Values (1, 1, 1) "
                    "THEN RETURN G1, G2, G3 + 1, V3;")},
      result_for_insert));
  absl::StatusOr<std::vector<ValueRow>> result_or = result_for_insert;
  if (!in_prod_env()) {
    // TODO: This shows that the generated column is not evaluated
    // in the googlesql reference implementation, which is also a requirement
    // for GPK support. Once that work is done, this TODO and test should be
    // updated.
    Value null_val = cloud::spanner::MakeNullValue<std::int64_t>();
    EXPECT_THAT(result_or, IsOkAndHoldsRow({null_val, null_val, null_val, 2}));
    return;
  }
  EXPECT_THAT(result_or, IsOkAndHoldsRow({3, 2, 4, 2}));
  EXPECT_THAT(Query("SELECT G1, G2, G3 + 1, V3 FROM TableGen WHERE K = 1;"),
              IsOkAndHoldsRow({3, 2, 4, 2}));

  // Update THEN RETURN
  std::vector<ValueRow> result_for_update;
  ZETASQL_EXPECT_OK(
      CommitDmlReturning({SqlStatement("UPDATE TableGen SET V1 = 3 WHERE K = 1 "
                                       "THEN RETURN G1, G2, G3 + 1;")},
                         result_for_update));
  result_or = result_for_update;
  EXPECT_THAT(result_or, IsOkAndHoldsRow({5, 4, 6}));
  EXPECT_THAT(Query("SELECT G1, G2, G3 + 1 FROM TableGen WHERE K = 1;"),
              IsOkAndHoldsRow({5, 4, 6}));

  // Delete THEN RETURN
  std::vector<ValueRow> result_for_delete;
  ZETASQL_EXPECT_OK(
      CommitDmlReturning({SqlStatement("DELETE FROM TableGen WHERE K = 1 "
                                       "THEN RETURN G1, G2, G3 + 1, V3;")},
                         result_for_delete));
  result_or = result_for_delete;
  EXPECT_THAT(result_or, IsOkAndHoldsRow({5, 4, 6, 2}));
  EXPECT_THAT(Query("SELECT G1, G2, G3 + 1 FROM TableGen WHERE K = 1;"),
              IsOkAndHoldsRows({}));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
