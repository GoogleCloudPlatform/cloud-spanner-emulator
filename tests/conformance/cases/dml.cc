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

class DmlTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  absl::Status SetUpDatabase() override {
    EmulatorFeatureFlags::Flags flags;
    emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);
    return SetSchemaFromFile("dml.test");
  }

 protected:
  void PopulateDatabase() {
    // Write fixure data to use in delete tests.
    ZETASQL_EXPECT_OK(
        CommitDml({SqlStatement("INSERT INTO users(id, name, age) VALUES "
                                "(1, 'Levin', 27), (2, 'Mark', 32), "
                                "(10, 'Douglas', 31)")}));
  }
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectDmlTests, DmlTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL
                    ),
    [](const testing::TestParamInfo<DmlTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(DmlTest, CanInsertAndUpdateInSameTransaction) {
  // Note that column Age is not part of update columns.
  ZETASQL_ASSERT_OK(CommitDml(
      {SqlStatement("INSERT INTO users(id, name, age) VALUES (1, 'Levin', 27)"),
       SqlStatement("UPDATE users SET name = 'Mark' WHERE id = 1")}));
  EXPECT_THAT(Query("SELECT id, name, age FROM users"),
              IsOkAndHoldsRows({{1, "Mark", 27}}));
}

TEST_P(DmlTest, InsertsNullValuesForUnspecifiedColumns) {
  // Nullable columns that are not specified are assigned default null values.
  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement("INSERT INTO users(id, updated) VALUES "
                              "(10, '2015-10-13T02:19:40Z')")}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto timestamp,
                       ParseRFC3339TimeSeconds("2015-10-13T02:19:40Z"));
  EXPECT_THAT(Query("SELECT id, name, age, updated FROM users"),
              IsOkAndHoldsRows({{10, Null<std::string>(), Null<std::int64_t>(),
                                 timestamp}}));
}

TEST_P(DmlTest, CanInsertPrimaryKeyOnly) {
  ZETASQL_ASSERT_OK(CommitDml({SqlStatement("INSERT INTO users(id) VALUES (10)")}));
  EXPECT_THAT(Query("SELECT id, name, age, updated FROM users"),
              IsOkAndHoldsRows({{10, Null<std::string>(), Null<std::int64_t>(),
                                 Null<Timestamp>()}}));
}

TEST_P(DmlTest, CanDeleteFromTable) {
  PopulateDatabase();
  EXPECT_THAT(Query("SELECT id, name, age FROM users ORDER BY id"),
              IsOkAndHoldsRows(
                  {{1, "Levin", 27}, {2, "Mark", 32}, {10, "Douglas", 31}}));

  ZETASQL_EXPECT_OK(CommitDml({SqlStatement("DELETE FROM users WHERE true")}));

  EXPECT_THAT(Query("SELECT id, name, age FROM users"), IsOkAndHoldsRows({}));
}

TEST_P(DmlTest, CanDeleteRangeFromTable) {
  PopulateDatabase();
  EXPECT_THAT(Query("SELECT id, name, age FROM users ORDER BY id"),
              IsOkAndHoldsRows(
                  {{1, "Levin", 27}, {2, "Mark", 32}, {10, "Douglas", 31}}));

  // Should delete only user with id 2.
  ZETASQL_EXPECT_OK(
      CommitDml({SqlStatement("DELETE FROM users WHERE id > 1 AND id < 10")}));

  EXPECT_THAT(Query("SELECT id, name, age FROM users ORDER BY id"),
              IsOkAndHoldsRows({{1, "Levin", 27}, {10, "Douglas", 31}}));
}

TEST_P(DmlTest, DeleteWithEmptyKeysIsNoOp) {
  PopulateDatabase();
  EXPECT_THAT(Query("SELECT id, name, age FROM users ORDER BY id"),
              IsOkAndHoldsRows(
                  {{1, "Levin", 27}, {2, "Mark", 32}, {10, "Douglas", 31}}));

  ZETASQL_EXPECT_OK(CommitDml({SqlStatement("DELETE FROM users WHERE false")}));

  EXPECT_THAT(Query("SELECT id, name, age FROM users ORDER BY id"),
              IsOkAndHoldsRows(
                  {{1, "Levin", 27}, {2, "Mark", 32}, {10, "Douglas", 31}}));
}

TEST_P(DmlTest, CanExecuteUpdateAfterDelete) {
  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement("INSERT INTO users(id, updated) VALUES "
                              "(10, '2015-10-13T02:19:40Z')")}));
  ZETASQL_EXPECT_OK(CommitDml(
      {SqlStatement("DELETE FROM users WHERE id = 10"),
       SqlStatement("UPDATE users SET name = 'Mark' WHERE id = 10")}));

  EXPECT_THAT(Query("SELECT id, name, age FROM users"), IsOkAndHoldsRows({}));
}

TEST_P(DmlTest, CanUpdateWithNullValue) {
  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement("INSERT INTO users(id, name, age) "
                              "VALUES (1, 'Levin', 27)")}));
  ASSERT_THAT(Query("SELECT id, name, age FROM users WHERE name IS NOT NULL"),
              IsOkAndHoldsRows({{1, "Levin", 27}}));

  // Update name to Null in the existing row.
  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement("UPDATE users SET name = NULL WHERE true")}));
  EXPECT_THAT(Query("SELECT id, name, age FROM users WHERE name IS NOT NULL"),
              IsOkAndHoldsRows({}));
}

TEST_P(DmlTest, CanInsertIntoTableWithNullableKey) {
  // Spanner PG dialect doesn't support nullable primary keys.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }
  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement("INSERT INTO nullable(key, value) "
                              "Values (NULL, 'Value1')")}));
  EXPECT_THAT(Query("SELECT key, value FROM nullable"),
              IsOkAndHoldsRows({{Null<std::int64_t>(), "Value1"}}));
}

TEST_P(DmlTest, CannotInsertMultipleRowsIntoSingletonTable) {
  // Spanner PG dialect doesn't support tables without primary keys.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }
  // Cannot insert multiple rows in same dml statement in a singleton table.
  EXPECT_THAT(
      CommitDml({SqlStatement("INSERT INTO singleton (col1, col2) Values "
                              "('val11', 'val21'), ('val12', 'val22')")}),
      StatusIs(in_prod_env() ? absl::StatusCode::kInvalidArgument
                             : absl::StatusCode::kAlreadyExists));

  // Cannot insert multiple rows in multiple dml statements either in a
  // singleton table.
  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement("INSERT INTO singleton (col1, col2) Values "
                              "('val11', 'val21')")}));
  EXPECT_THAT(
      CommitDml({SqlStatement("INSERT INTO singleton (col1, col2) Values "
                              "('val12', 'val22')")}),
      StatusIs(absl::StatusCode::kAlreadyExists));
}

TEST_P(DmlTest, CanUpdateEmptySingletonTable) {
  // Spanner PG dialect doesn't support tables without primary keys.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }
  ZETASQL_EXPECT_OK(CommitDml(
      {SqlStatement("UPDATE singleton SET col1 = 'val1' WHERE true")}));
  EXPECT_THAT(Query("SELECT col1, col2 FROM singleton"), IsOkAndHoldsRows({}));
}

TEST_P(DmlTest, CanUpdateSingletonTable) {
  // Spanner PG dialect doesn't support tables without primary keys.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }
  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement("INSERT INTO singleton (col1, col2) Values "
                              "('val11', 'val21')")}));
  ZETASQL_EXPECT_OK(CommitDml({SqlStatement(
      "UPDATE singleton SET col1 = 'val12', col2 = 'val22' WHERE true")}));
  EXPECT_THAT(Query("SELECT col1, col2 FROM singleton"),
              IsOkAndHoldsRows({{"val12", "val22"}}));
}

TEST_P(DmlTest, CanDeleteFromEmptySingletonTable) {
  // Spanner PG dialect doesn't support tables without primary keys.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }
  EXPECT_THAT(Query("SELECT col1, col2 FROM singleton"), IsOkAndHoldsRows({}));
  ZETASQL_ASSERT_OK(CommitDml({SqlStatement("DELETE FROM singleton WHERE true")}));
  EXPECT_THAT(Query("SELECT col1, col2 FROM singleton"), IsOkAndHoldsRows({}));
}

TEST_P(DmlTest, CanDeleteFromSingletonTable) {
  // Spanner PG dialect doesn't support tables without primary keys.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }
  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement("INSERT INTO singleton (col1, col2) Values "
                              "('val11', 'val21')")}));
  ZETASQL_ASSERT_OK(CommitDml({SqlStatement("DELETE FROM singleton WHERE true")}));
  EXPECT_THAT(Query("SELECT col1, col2 FROM singleton"), IsOkAndHoldsRows({}));
}

TEST_P(DmlTest, DeleteWithEmptyKeysIsNoOpForSingletonTable) {
  // Spanner PG dialect doesn't support tables without primary keys.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }
  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement("INSERT INTO singleton (col1, col2) Values "
                              "('val11', 'val21')")}));
  ZETASQL_ASSERT_OK(CommitDml({SqlStatement("DELETE FROM singleton WHERE false")}));
  EXPECT_THAT(Query("SELECT col1, col2 FROM singleton"),
              IsOkAndHoldsRows({{"val11", "val21"}}));
}

TEST_P(DmlTest, CannotUseReadOnlyTransaction) {
  EXPECT_THAT(
      ExecuteDmlTransaction(Transaction(Transaction::ReadOnlyOptions()),
                            SqlStatement("INSERT INTO users(id) VALUES(1)")),
      StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(QuerySingleUseTransaction(
                  Transaction::SingleUseOptions{Transaction::ReadOnlyOptions{}},
                  SqlStatement("INSERT INTO users(id) VALUES(1)")),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_P(DmlTest, CanInsertToArrayColumns) {
  std::string values = (GetParam() == database_api::DatabaseDialect::POSTGRESQL)
                           ? "VALUES(1, ARRAY [10])"
                           : "VALUES(1, ARRAY<INT64>[10])";

  // Array literals.
  ZETASQL_EXPECT_OK(CommitDml({SqlStatement(
      absl::StrCat("INSERT INTO arrayfields(key, arraycol) ", values))}));

  // Array parameters.
  values = (GetParam() == database_api::DatabaseDialect::POSTGRESQL)
               ? "VALUES($1, $2)"
               : "VALUES(@p1, @p2)";
  ZETASQL_EXPECT_OK(CommitDml(
      {SqlStatement("INSERT INTO arrayfields(key, arraycol) "
                    "VALUES(@p1, @p2)",
                    SqlStatement::ParamType{
                        {"p1", Value(2)},
                        {"p2", Value(std::vector<int64_t>{10, 20, 30})}})}));

  EXPECT_THAT(Query("SELECT key, arraycol FROM arrayfields ORDER BY key"),
              IsOkAndHoldsRows({
                  {1, Value(std::vector<int64_t>{10})},
                  {2, Value(std::vector<int64_t>{10, 20, 30})},
              }));
}

TEST_P(DmlTest, CanInsertMultipleRowsUsingStructParam) {
  // Spanner PG dialect doesn't support the UNNEST.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }
  using StructType = std::tuple<int64_t, std::string>;
  ZETASQL_EXPECT_OK(CommitDml({SqlStatement(
      "INSERT INTO users(id, name) "
      "SELECT * FROM UNNEST(@p1)",
      SqlStatement::ParamType{
          {"p1", Value(std::vector<StructType>{StructType{1, "abc"},
                                               StructType{2, "def"}})}})}));

  EXPECT_THAT(Query("SELECT id, name FROM users ORDER BY id"),
              IsOkAndHoldsRows({{1, "abc"}, {2, "def"}}));
}

TEST_P(DmlTest, CannotCommitWithBadMutation) {
  Transaction txn{Transaction::ReadWriteOptions{}};
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto result,
      ExecuteDmlTransaction(
          txn, SqlStatement("INSERT INTO users(id, name) VALUES(1, \"abc\")")));

  // Check read-your-writes
  EXPECT_THAT(QueryTransaction(txn, "SELECT id, name FROM users"),
              IsOkAndHoldsRow({1, "abc"}));

  // Build an invalid mutation referencing a non-existent column.
  auto mut1 = MakeInsert("users", {"id"}, 2);
  auto mut2 = MakeInsert("users", {"NON_EXISTENT_COLUMN"}, 3);
  auto mut3 = MakeInsert("users", {"id"}, 4);

  // Try to commit the transaction, it should fail.
  EXPECT_THAT(
      CommitTransaction(txn,
                        {
                            MakeInsert("users", {"id"}, 2),
                            MakeInsert("users", {"NON_EXISTENT_COLUMN"}, 3),
                            MakeInsert("users", {"id"}, 4),
                        }),
      StatusIs(absl::StatusCode::kNotFound));

  // Check that the DML statement was not committed.
  EXPECT_THAT(Query("SELECT id FROM users"), IsOkAndHoldsRows({}));
}

TEST_P(DmlTest, CanUseIndexHintInInsertStatement) {
  // Spanner PG dialect doesn't support nullable indexes.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }

  ZETASQL_EXPECT_OK(Insert("nullable", {"key", "value"}, {1, "Peter"}));

  ZETASQL_EXPECT_OK(
      CommitDml({SqlStatement("INSERT INTO users(id, name) "
                              "SELECT key, value "
                              "FROM nullable@{force_index=nullableindex} ")}));

  EXPECT_THAT(Query("SELECT id, name FROM users ORDER BY id"),
              IsOkAndHoldsRow({1, "Peter"}));
}

TEST_P(DmlTest, CanUseIndexHintInUpdateStatement) {
  // Spanner PG dialect doesn't support nullable indexes.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }

  ZETASQL_EXPECT_OK(Insert("nullable", {"key", "value"}, {1, "Peter"}));
  ZETASQL_EXPECT_OK(Insert("users", {"id", "name"}, {1, "Paul"}));

  ZETASQL_EXPECT_OK(CommitDml({SqlStatement(
      "UPDATE users SET name = 'Peter' "
      "WHERE id IN "
      "(SELECT key FROM nullable@{force_index=nullableindex})")}));

  EXPECT_THAT(Query("SELECT id, name FROM users ORDER BY id"),
              IsOkAndHoldsRow({1, "Peter"}));
}

TEST_P(DmlTest, NumericKey) {
  // Spanner PG dialect for the emulator doesn't yet support the PG.NUMERIC
  // type.
  // TODO: Uncomment after PG extended type support is added.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }

  // Insert DML
  ZETASQL_EXPECT_OK(CommitDml({SqlStatement(
      "INSERT INTO numerictable(key, val) VALUES (NUMERIC'-12.3', -1), "
      "(NUMERIC'0', 0), (NUMERIC'12.3', 1)")}));

  // Update DML
  ZETASQL_EXPECT_OK(CommitDml(
      {SqlStatement("UPDATE numerictable SET val = 2 WHERE key = 12.3")}));

  EXPECT_THAT(Query("SELECT t.val FROM numerictable t ORDER BY t.key"),
              IsOkAndHoldsRows({{-1}, {0}, {2}}));

  // Delete DML
  ZETASQL_EXPECT_OK(
      CommitDml({SqlStatement("DELETE FROM numerictable t WHERE t.key < 0")}));

  EXPECT_THAT(Query("SELECT t.val FROM numerictable t ORDER BY t.key"),
              IsOkAndHoldsRows({{0}, {2}}));
}

TEST_P(DmlTest, JsonType) {
  // Spanner PG dialect for the emulator doesn't yet support the PG.JSONB
  // type.
  // TODO: Uncomment after PG extended type support is added.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }

  // Insert DML
  ZETASQL_EXPECT_OK(CommitDml({SqlStatement(R"(
        INSERT INTO jsontable(id, val) VALUES (3, JSON '{"a":"str"}')
  )")}));
  ZETASQL_EXPECT_OK(CommitDml({SqlStatement(R"(
        INSERT INTO jsontable(id, val) VALUES (4, NULL)
  )")}));

  EXPECT_THAT(
      Query("SELECT TO_JSON_STRING(t.Val) FROM jsontable t WHERE id = 3"),
      IsOkAndHoldsRow({R"({"a":"str"})"}));
  EXPECT_THAT(Query("SELECT id FROM jsontable t WHERE val IS NULL"),
              IsOkAndHoldsRow({Value(4)}));
  EXPECT_THAT(Query("SELECT id FROM jsontable t WHERE val IS NOT NULL"),
              IsOkAndHoldsRow({Value(3)}));

  // Update DML
  ZETASQL_EXPECT_OK(CommitDml({SqlStatement(R"(
      UPDATE jsontable
        SET val = JSON '{"a":"newstr", "b":123}'
        WHERE id = 3
  )")}));

  EXPECT_THAT(
      Query("SELECT TO_JSON_STRING(t.Val) FROM jsontable t WHERE id = 3"),
      IsOkAndHoldsRow({R"({"a":"newstr","b":123})"}));
}

TEST_P(DmlTest, Returning) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_dml_returning = true;
  emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

  // Insert THEN RETURN
  std::vector<ValueRow> result_for_insert;
  ZETASQL_EXPECT_OK(CommitDmlReturning(
      {SqlStatement("INSERT INTO users(id, name, age) VALUES (1, 'Levin', 27) "
                    "THEN RETURN age;")},
      result_for_insert));

  absl::StatusOr<std::vector<ValueRow>> result_or = result_for_insert;
  EXPECT_THAT(result_or, IsOkAndHoldsRow({Value(27)}));

  EXPECT_THAT(Query("SELECT age FROM users WHERE id = 1;"),
              IsOkAndHoldsRows({{27}}));

  // Update THEN RETURN
  std::vector<ValueRow> result_for_update;
  ZETASQL_EXPECT_OK(CommitDmlReturning(
      {SqlStatement("UPDATE users SET age = age + 1 WHERE id = 1 "
                    "THEN RETURN age, name;")},
      result_for_update));
  result_or = result_for_update;
  EXPECT_THAT(result_or, IsOkAndHoldsRow({28, "Levin"}));

  EXPECT_THAT(Query("SELECT age, name FROM users WHERE id = 1;"),
              IsOkAndHoldsRow({28, "Levin"}));

  // Delete THEN RETURN
  std::vector<ValueRow> result_for_delete;
  ZETASQL_EXPECT_OK(CommitDmlReturning({SqlStatement("DELETE FROM users WHERE id = 1 "
                                             "THEN RETURN name, age;")},
                               result_for_delete));
  result_or = result_for_delete;
  EXPECT_THAT(result_or, IsOkAndHoldsRow({"Levin", 28}));
  EXPECT_THAT(Query("SELECT age, name FROM users WHERE id = 1;"),
              IsOkAndHoldsRows({}));
}

// TODO: Reenable once fixed
TEST_P(DmlTest, DISABLED_ReturningGeneratedColumns) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_dml_returning = true;
  emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

  // Insert THEN RETURN
  std::vector<ValueRow> result_for_insert;
  ZETASQL_EXPECT_OK(CommitDmlReturning(
      {SqlStatement("INSERT INTO tablegen(k, v1, v2) VALUES (1, 1, 1) "
                    "THEN RETURN g1, g2, g3 + 1, v3;")},
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
  EXPECT_THAT(Query("SELECT g1, g2, g3 + 1, v3 FROM tablegen WHERE k = 1;"),
              IsOkAndHoldsRow({3, 2, 4, 2}));

  // Update THEN RETURN
  std::vector<ValueRow> result_for_update;
  ZETASQL_EXPECT_OK(
      CommitDmlReturning({SqlStatement("UPDATE tablegen SET v1 = 3 WHERE k = 1 "
                                       "THEN RETURN g1, g2, g3 + 1;")},
                         result_for_update));
  result_or = result_for_update;
  EXPECT_THAT(result_or, IsOkAndHoldsRow({5, 4, 6}));
  EXPECT_THAT(Query("SELECT g1, g2, g3 + 1 FROM tablegen WHERE k = 1;"),
              IsOkAndHoldsRow({5, 4, 6}));

  // Delete THEN RETURN
  std::vector<ValueRow> result_for_delete;
  ZETASQL_EXPECT_OK(
      CommitDmlReturning({SqlStatement("DELETE FROM tablegen WHERE k = 1 "
                                       "THEN RETURN g1, g2, g3 + 1, v3;")},
                         result_for_delete));
  result_or = result_for_delete;
  EXPECT_THAT(result_or, IsOkAndHoldsRow({5, 4, 6, 2}));
  EXPECT_THAT(Query("SELECT g1, g2, g3 + 1 FROM tablegen WHERE k = 1;"),
              IsOkAndHoldsRows({}));
}

TEST_P(DmlTest, ReturningStructValues) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_dml_returning = true;
  emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

  // Insert THEN RETURN
  std::vector<ValueRow> result_for_insert;
  EXPECT_THAT(
      CommitDmlReturning(
          {SqlStatement(
              "INSERT INTO users(id, name, age) VALUES (1, 'Levin', 27) "
              "THEN RETURN STRUCT< int64_f INT64 > (100) AS expr0;")},
          result_for_insert),
      StatusIs(absl::StatusCode::kUnimplemented,
               testing::HasSubstr(
                   "A struct value cannot be returned as a column value.")));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
