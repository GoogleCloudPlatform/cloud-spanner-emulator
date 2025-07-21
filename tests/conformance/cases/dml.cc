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

#include <cstdint>
#include <string>
#include <tuple>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/substitute.h"
#include "google/cloud/spanner/json.h"
#include "google/cloud/spanner/numeric.h"
#include "common/feature_flags.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using cloud::spanner::JsonB;
using cloud::spanner::MakePgNumeric;
using cloud::spanner::PgNumeric;
using zetasql_base::testing::StatusIs;

class DmlTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  // TODO: b/296685434 - Remove `enable_upsert_queries` after it is enabled by
  // default.
  DmlTest()
      : feature_flags_({.enable_postgresql_interface = true,
                        .enable_upsert_queries = true}) {}

  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

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

 private:
  test::ScopedEmulatorFeatureFlagsSetter feature_flags_;
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectDmlTests, DmlTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
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

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // PG doesn't need a WHERE clause.
    ZETASQL_EXPECT_OK(CommitDml({SqlStatement("DELETE FROM users")}));
  } else {
    ZETASQL_EXPECT_OK(CommitDml({SqlStatement("DELETE FROM users WHERE true")}));
  }

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
  ZETASQL_EXPECT_OK(CommitDml({SqlStatement(
      absl::StrCat("INSERT INTO arrayfields(key, arraycol) ", values),
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
  std::string value = GetParam() == database_api::DatabaseDialect::POSTGRESQL
                          ? "'abc'"
                          : "\"abc\"";

  Transaction txn{Transaction::ReadWriteOptions{}};
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto result,
      ExecuteDmlTransaction(
          txn, SqlStatement(absl::Substitute(
                   "INSERT INTO users(id, name) VALUES(1, $0)", value))));

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

TEST_P(DmlTest, CanUseIndexHintInDeleteStatement) {
  // Spanner PG dialect doesn't support nullable indexes or this hint syntax.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }

  ZETASQL_EXPECT_OK(Insert("nullable", {"key", "value"}, {1, "Peter"}));

  EXPECT_THAT(Query("SELECT key, value FROM nullable"),
              IsOkAndHoldsRow({1, "Peter"}));

  ZETASQL_EXPECT_OK(CommitDml(
      {SqlStatement("DELETE FROM nullable@{force_index=nullableindex} "
                    "WHERE value = 'Peter'")}));

  EXPECT_THAT(Query("SELECT key, value FROM nullable"), IsOkAndHoldsRows({}));
}

TEST_P(DmlTest, CanUseForceIndexHintInUpdateStatement) {
  // Spanner PG dialect doesn't support nullable indexes or this hint syntax.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }

  ZETASQL_EXPECT_OK(Insert("nullable", {"key", "value"}, {1, "Peter"}));
  ZETASQL_EXPECT_OK(Insert("nullable", {"key", "value"}, {2, "James"}));

  EXPECT_THAT(Query("SELECT key, value FROM nullable ORDER BY key"),
              IsOkAndHoldsRows({{1, "Peter"}, {2, "James"}}));

  // This UPDATE should use the index to find the row.
  ZETASQL_EXPECT_OK(
      CommitDml({SqlStatement("UPDATE nullable@{force_index=nullableindex} n "
                              "SET n.value = 'Smith' "
                              "WHERE n.value = 'Peter'")}));

  EXPECT_THAT(Query("SELECT key, value FROM nullable ORDER BY key"),
              IsOkAndHoldsRows({{1, "Smith"}, {2, "James"}}));
}

TEST_P(DmlTest, GroupByScanOptimizationHintSyntax) {
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) GTEST_SKIP();

  ZETASQL_EXPECT_OK(Insert("users", {"id", "name", "age"}, {1, "John", 30}));
  ZETASQL_EXPECT_OK(Insert("users", {"id", "name", "age"}, {2, "Jane", 30}));

  ZETASQL_EXPECT_OK(Query(
      "SELECT age FROM users@{GROUPBY_SCAN_OPTIMIZATION=TRUE} GROUP BY age"));
  ZETASQL_EXPECT_OK(Query(
      "SELECT age FROM users@{GROUPBY_SCAN_OPTIMIZATION=FALSE} GROUP BY age"));
}

TEST_P(DmlTest, IndexStrategyForceIndexUnionHint) {
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK(UpdateSchema(
      {"CREATE TABLE TableWithMultiIndex (K INT64, ColA INT64, ColB INT64, Val "
       "STRING(MAX)) PRIMARY KEY(K)",
       "CREATE INDEX IndexColA ON TableWithMultiIndex(ColA)",
       "CREATE INDEX IndexColB ON TableWithMultiIndex(ColB)"}));
  ZETASQL_EXPECT_OK(Insert("TableWithMultiIndex", {"K", "ColA", "ColB", "Val"},
                   {1, 10, 100, "A"}));
  ZETASQL_EXPECT_OK(Insert("TableWithMultiIndex", {"K", "ColA", "ColB", "Val"},
                   {2, 20, 200, "B"}));
  ZETASQL_EXPECT_OK(Insert("TableWithMultiIndex", {"K", "ColA", "ColB", "Val"},
                   {3, 10, 300, "C"}));

  EXPECT_THAT(Query("SELECT K, Val FROM "
                    "TableWithMultiIndex@{INDEX_STRATEGY=FORCE_INDEX_UNION} "
                    "WHERE ColA = 10 OR ColB = 200 ORDER BY K"),
              IsOkAndHoldsRows({{1, "A"}, {2, "B"}, {3, "C"}}));
}

TEST_P(DmlTest, HintsWithScanMethod) {
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }
  ZETASQL_EXPECT_OK(Insert("users", {"id", "name"}, {1, "John"}));
  ZETASQL_EXPECT_OK(
      Query("SELECT ID, Name FROM "
            "users@{SCAN_METHOD=BATCH}"));
}
TEST_P(DmlTest, SeekableKeySizeHintTest) {
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK(
      UpdateSchema({"CREATE TABLE SeekTable (K1 INT64, K2 INT64, V INT64) "
                    "PRIMARY KEY(K1, K2)",
                    "CREATE INDEX SeekIndex ON SeekTable(K1, K2)"}));
  ZETASQL_EXPECT_OK(Insert("SeekTable", {"K1", "K2", "V"}, {1, 1, 100}));

  if (in_prod_env()) {
    ZETASQL_EXPECT_OK(
        Query("SELECT V FROM SeekTable@{FORCE_INDEX=SeekIndex, "
              "SEEKABLE_KEY_SIZE=1} WHERE K1 = 1 AND K2 = 1"));
    ZETASQL_EXPECT_OK(
        Query("SELECT V FROM SeekTable@{FORCE_INDEX=SeekIndex, "
              "SEEKABLE_KEY_SIZE=0} WHERE K1 = 1 AND K2 = 1"));
  } else {  // Emulator behavior
    const std::string expected_error = "Unsupported hint: SEEKABLE_KEY_SIZE";
    EXPECT_THAT(Query("SELECT V FROM SeekTable@{FORCE_INDEX=SeekIndex, "
                      "SEEKABLE_KEY_SIZE=1} WHERE K1 = 1 AND K2 = 1"),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         testing::HasSubstr(expected_error)));
    EXPECT_THAT(Query("SELECT V FROM SeekTable@{FORCE_INDEX=SeekIndex, "
                      "SEEKABLE_KEY_SIZE=0} WHERE K1 = 1 AND K2 = 1"),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         testing::HasSubstr(expected_error)));
    EXPECT_THAT(Query("SELECT V FROM SeekTable@{FORCE_INDEX=SeekIndex, "
                      "SEEKABLE_KEY_SIZE=17} WHERE K1 = 1 AND K2 = 1"),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         testing::HasSubstr(expected_error)));
    EXPECT_THAT(Query("SELECT V FROM SeekTable@{SEEKABLE_KEY_SIZE=1} "
                      "WHERE K1 = 1 AND K2 = 1"),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         testing::HasSubstr(expected_error)));
  }
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
  // TODO: Unskip after PG.NUMERIC indexing is supported.
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

TEST_P(DmlTest, NumericType) {
  // TODO: Remove test after PG.NUMERIC indexing is supported and
  // this test can be combined with the NumericKey test above.
  if (GetParam() == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    GTEST_SKIP();
  }

  // Insert DML
  ZETASQL_EXPECT_OK(CommitDml(
      {SqlStatement("INSERT INTO numerictable(id, val) VALUES (-1, -12.3), "
                    "(0, 0.1), (1, 12.3)")}));

  // Update DML
  ZETASQL_EXPECT_OK(CommitDml(
      {SqlStatement("UPDATE numerictable SET val = 2.2 WHERE id = 1")}));

  EXPECT_THAT(Query("SELECT t.val FROM numerictable t ORDER BY t.id"),
              IsOkAndHoldsRows({{*MakePgNumeric("-12.3")},
                                {*MakePgNumeric("0.1")},
                                {*MakePgNumeric("2.2")}}));

  // Delete DML
  ZETASQL_EXPECT_OK(
      CommitDml({SqlStatement("DELETE FROM numerictable t WHERE t.id < 0")}));

  EXPECT_THAT(
      Query("SELECT t.val FROM numerictable t ORDER BY t.id"),
      IsOkAndHoldsRows({{*MakePgNumeric("0.1")}, {*MakePgNumeric("2.2")}}));
}

TEST_P(DmlTest, JsonType) {
  std::string value = GetParam() == database_api::DatabaseDialect::POSTGRESQL
                          ? R"('{"a":"str"}')"
                          : R"(JSON '{"a":"str"}')";

  // Insert DML
  ZETASQL_EXPECT_OK(CommitDml({SqlStatement(absl::Substitute(R"(
        INSERT INTO jsontable(id, val) VALUES (3, $0)
  )",
                                                     value))}));

  ZETASQL_EXPECT_OK(CommitDml({SqlStatement(R"(
        INSERT INTO jsontable(id, val) VALUES (4, NULL)
  )")}));

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // Can't insert a text value as a jsonb value.
    EXPECT_THAT(CommitDml({SqlStatement(R"(
          INSERT INTO jsontable(id, val) VALUES (10, 'abc'::text)
    )")}),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    EXPECT_THAT(Query("SELECT t.val FROM jsontable t WHERE id = 3"),
                IsOkAndHoldsRow({JsonB(R"({"a": "str"})")}));
  } else {
    EXPECT_THAT(
        Query("SELECT TO_JSON_STRING(t.Val) FROM jsontable t WHERE id = 3"),
        IsOkAndHoldsRow({R"({"a":"str"})"}));
  }
  EXPECT_THAT(Query("SELECT id FROM jsontable t WHERE val IS NULL"),
              IsOkAndHoldsRow({Value(4)}));
  EXPECT_THAT(Query("SELECT id FROM jsontable t WHERE val IS NOT NULL"),
              IsOkAndHoldsRow({Value(3)}));

  // Update DML
  value = GetParam() == database_api::DatabaseDialect::POSTGRESQL
              ? R"('{"a":"newstr", "b":123}')"
              : R"(JSON '{"a":"newstr", "b":123}')";
  ZETASQL_EXPECT_OK(CommitDml({SqlStatement(absl::Substitute(
      R"(UPDATE jsontable SET val = $0 WHERE id = 3)", value))}));

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    EXPECT_THAT(Query("SELECT t.Val FROM jsontable t WHERE id = 3"),
                IsOkAndHoldsRow({JsonB(R"({"a": "newstr", "b": 123})")}));
  } else {
    EXPECT_THAT(
        Query("SELECT TO_JSON_STRING(t.Val) FROM jsontable t WHERE id = 3"),
        IsOkAndHoldsRow({R"({"a":"newstr","b":123})"}));
  }
}

TEST_P(DmlTest, Returning) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_dml_returning = true;
  emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

  std::string returning =
      (GetParam() == database_api::DatabaseDialect::POSTGRESQL) ? "RETURNING"
                                                                : "THEN RETURN";

  // Insert THEN RETURN
  std::vector<ValueRow> result_for_insert;
  ZETASQL_EXPECT_OK(CommitDmlReturning(
      {SqlStatement(absl::Substitute(
          "INSERT INTO users(id, name, age) VALUES (1, 'Levin', 27) "
          "$0 age;",
          returning))},
      result_for_insert));

  absl::StatusOr<std::vector<ValueRow>> result_or = result_for_insert;
  EXPECT_THAT(result_or, IsOkAndHoldsRow({Value(27)}));

  EXPECT_THAT(Query("SELECT age FROM users WHERE id = 1;"),
              IsOkAndHoldsRows({{27}}));

  // Update THEN RETURN
  std::vector<ValueRow> result_for_update;
  ZETASQL_EXPECT_OK(
      CommitDmlReturning({SqlStatement(absl::Substitute(
                             "UPDATE users SET age = age + 1 WHERE id = 1 "
                             "$0 age, name;",
                             returning))},
                         result_for_update));
  result_or = result_for_update;
  EXPECT_THAT(result_or, IsOkAndHoldsRow({28, "Levin"}));

  EXPECT_THAT(Query("SELECT age, name FROM users WHERE id = 1;"),
              IsOkAndHoldsRow({28, "Levin"}));

  // Delete THEN RETURN
  std::vector<ValueRow> result_for_delete;
  ZETASQL_EXPECT_OK(CommitDmlReturning(
      {SqlStatement(absl::Substitute("DELETE FROM users WHERE id = 1 "
                                     "$0 name, age;",
                                     returning))},
      result_for_delete));
  result_or = result_for_delete;
  EXPECT_THAT(result_or, IsOkAndHoldsRow({"Levin", 28}));
  EXPECT_THAT(Query("SELECT age, name FROM users WHERE id = 1;"),
              IsOkAndHoldsRows({}));
}

TEST_P(DmlTest, ReturningWithAction) {
  // WITH ACTION is available in ZetaSQL only.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }

  EmulatorFeatureFlags::Flags flags;
  flags.enable_upsert_queries_with_returning = true;
  emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

  // Insert THEN RETURN WITH ACTION returns "INSERT" as action string
  std::vector<ValueRow> result_for_insert;
  ZETASQL_EXPECT_OK(CommitDmlReturning(
      {SqlStatement("INSERT INTO users(id, name, age) VALUES (1, 'Levin', 27) "
                    "THEN RETURN WITH ACTION AS action age;")},
      result_for_insert));

  absl::StatusOr<std::vector<ValueRow>> result_or = result_for_insert;
  EXPECT_THAT(result_or, IsOkAndHoldsRow({Value(27), "INSERT"}));

  // Update THEN RETURN WITH ACTION returns "UPDATE" as action string
  std::vector<ValueRow> result_for_update;
  ZETASQL_EXPECT_OK(CommitDmlReturning(
      {SqlStatement("UPDATE users SET age = age + 1 WHERE id = 1 "
                    "THEN RETURN WITH ACTION AS action age, name;")},
      result_for_update));
  result_or = result_for_update;
  EXPECT_THAT(result_or, IsOkAndHoldsRow({28, "Levin", "UPDATE"}));

  // Delete THEN RETURN WITH ACTION returns "DELETE" as action string
  std::vector<ValueRow> result_for_delete;
  ZETASQL_EXPECT_OK(CommitDmlReturning(
      {SqlStatement("DELETE FROM users WHERE id = 1 "
                    "THEN RETURN WITH ACTION AS action name, age;")},
      result_for_delete));
  result_or = result_for_delete;
  EXPECT_THAT(result_or, IsOkAndHoldsRow({"Levin", 28, "DELETE"}));
}

// TODO: Reenable once fixed
TEST_P(DmlTest, DISABLED_ReturningGeneratedColumns) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_dml_returning = true;
  emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

  std::string returning =
      (GetParam() == database_api::DatabaseDialect::POSTGRESQL) ? "RETURNING"
                                                                : "THEN RETURN";

  // Insert THEN RETURN
  std::vector<ValueRow> result_for_insert;
  ZETASQL_EXPECT_OK(
      CommitDmlReturning({SqlStatement(absl::Substitute(
                             "INSERT INTO tablegen(k, v1, v2) VALUES (1, 1, 1) "
                             "$0 g1, g2, g3 + 1, v3;",
                             returning))},
                         result_for_insert));
  absl::StatusOr<std::vector<ValueRow>> result_or = result_for_insert;

  EXPECT_THAT(result_or, IsOkAndHoldsRow({3, 2, 4, 2}));
  EXPECT_THAT(Query("SELECT g1, g2, g3 + 1, v3 FROM tablegen WHERE k = 1;"),
              IsOkAndHoldsRow({3, 2, 4, 2}));

  // TODO: Add required support for `THEN RETURN` after generated
  // column implementation.
  if (!in_prod_env()) return;

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
  ZETASQL_EXPECT_OK(CommitDmlReturning(
      {SqlStatement(absl::Substitute("DELETE FROM tablegen WHERE k = 1 "
                                     "$0 g1, g2, g3 + 1, v3;",
                                     returning))},
      result_for_delete));
  result_or = result_for_delete;
  EXPECT_THAT(result_or, IsOkAndHoldsRow({5, 4, 6, 2}));
  EXPECT_THAT(Query("SELECT g1, g2, g3 + 1 FROM tablegen WHERE k = 1;"),
              IsOkAndHoldsRows({}));
}

TEST_P(DmlTest, ReadGeneratedColumnsWithIntervalExpression) {
  ZETASQL_EXPECT_OK(CommitDml(
      {SqlStatement("INSERT INTO tablegen(k, v1, v2) VALUES (1, 1, 1) ")}));

  EXPECT_THAT(Query("SELECT v1, v2, b1, b2 FROM tablegen WHERE k = 1;"),
              IsOkAndHoldsRow({1, 1, false, true}));
}

TEST_P(DmlTest, InsertGPK) {
  // Insert with `gpktable1`.
  ZETASQL_EXPECT_OK(CommitDml(
      {SqlStatement("INSERT INTO gpktable1(k1, v1, v2) VALUES (1, 1, 0) ")}));
  EXPECT_THAT(Query("SELECT k1, k2_stored, v2 + 5 FROM gpktable1 WHERE k1 = 1"),
              IsOkAndHoldsRow({1, 2, 5}));

  // Insert with `gpktable2`.
  ZETASQL_EXPECT_OK(
      CommitDml({SqlStatement("INSERT INTO gpktable2(k1, v1) VALUES (1, 1)")}));
  EXPECT_THAT(
      Query("SELECT k1, k2_stored, v2_stored + 5 FROM gpktable2 WHERE k1 = 1"),
      IsOkAndHoldsRow({1, 3, 11}));
}

TEST_P(DmlTest, UpsertDmlSimpleTable) {
  PopulateDatabase();

  // INSERT OR IGNORE DML.
  std::string sql =
      "INSERT $0 INTO users(id, name, age) "
      "VALUES (3, 'John', 30), (1, 'Bob', 31), (5, 'Susan', 28) $1";
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    sql = absl::Substitute(sql, "", "ON CONFLICT(id) DO NOTHING");
  } else {
    sql = absl::Substitute(sql, "OR IGNORE", "");
  }
  ZETASQL_EXPECT_OK(CommitDml({SqlStatement(sql)}));
  EXPECT_THAT(
      Query("SELECT id, name, age FROM users WHERE id < 10 ORDER BY id"),
      IsOkAndHoldsRows({{1, "Levin", 27},
                        {2, "Mark", 32},
                        {3, "John", 30},
                        {5, "Susan", 28}}));

  // INSERT OR UPDATE DML.
  sql =
      "INSERT $0 INTO users(id, name, age) "
      "VALUES (3, 'John', 35), (1, 'Bob', 31), (5, 'Susan', 25) $1";
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    sql = absl::Substitute(sql, "",
                           "ON CONFLICT(id) DO UPDATE SET id = excluded.id, "
                           "name = excluded.name, age = excluded.age");
  } else {
    sql = absl::Substitute(sql, "OR UPDATE", "");
  }
  ZETASQL_EXPECT_OK(CommitDml({SqlStatement(sql)}));
  EXPECT_THAT(
      Query("SELECT id, name, age FROM users WHERE id < 10 ORDER BY id"),
      IsOkAndHoldsRows({{1, "Bob", 31},
                        {2, "Mark", 32},
                        {3, "John", 35},
                        {5, "Susan", 25}}));
}

TEST_P(DmlTest, UpsertDmlGeneratedColumnTable) {
  // Populate `tablegen` table
  ZETASQL_EXPECT_OK(CommitDml(
      {SqlStatement("INSERT INTO tablegen(k, v1, v2) VALUES (1, 1, 1);")}));

  // INSERT OR IGNORE DML.
  std::string sql =
      "INSERT $0 INTO tablegen(k, v1, v2) "
      "VALUES (2, 2, 2), (1, 2, 2), (5, 5, 5) $1";
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    sql = absl::Substitute(sql, "", "ON CONFLICT(k) DO NOTHING");
  } else {
    sql = absl::Substitute(sql, "OR IGNORE", "");
  }
  ZETASQL_EXPECT_OK(CommitDml({SqlStatement(sql)}));
  EXPECT_THAT(Query("SELECT k, g2, v3 FROM tablegen WHERE TRUE ORDER BY k"),
              IsOkAndHoldsRows({{1, 2, 2}, {2, 4, 2}, {5, 10, 2}}));

  // INSERT OR UPDATE DML.
  sql =
      "INSERT $0 INTO tablegen(k, v1, v2, v3) "
      "VALUES (2, 20, 20, 2), (1, 10, 10, 1), (5, 50, 50, 5) $1";
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    sql = absl::Substitute(sql, "",
                           "ON CONFLICT(k) DO UPDATE SET k = excluded.k, "
                           "v1 = excluded.v1, v2 = excluded.v2, "
                           "v3 = excluded.v3");
  } else {
    sql = absl::Substitute(sql, "OR UPDATE", "");
  }
  ZETASQL_EXPECT_OK(CommitDml({SqlStatement(sql)}));
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    EXPECT_THAT(
        Query("SELECT k, g1, g2, v3 FROM tablegen WHERE TRUE ORDER BY k"),
        IsOkAndHoldsRows({{1, 11, 20, 1}, {2, 21, 40, 2}, {5, 51, 100, 5}}));
  } else {
    EXPECT_THAT(
        Query("SELECT k, g1, g2, v3 FROM tablegen WHERE TRUE ORDER BY k"),
        IsOkAndHoldsRows({{1, 21, 20, 1}, {2, 41, 40, 2}, {5, 101, 100, 5}}));
  }
}

TEST_P(DmlTest, ReturningUpsertDml) {
  // Populate `tablegen` table
  ZETASQL_EXPECT_OK(CommitDml(
      {SqlStatement("INSERT INTO tablegen(k, v1, v2) VALUES (1, 1, 1);")}));

  // Test UPSERT queries with returning clause.
  // In GSQL, SQL has WITH ACTION clause as it is supported in GSQL only.
  // In PG, returning generated columns do not work. Test returning with
  // non-generated columns. b/310194797.

  std::vector<ValueRow> result_for_insert_ignore;
  // 1. INSERT OR IGNORE DML with returning clause.
  std::string sql =
      "INSERT $0 INTO tablegen(k, v1, v2) "
      "VALUES (1, 2, 2), (2, 2, 2) $1 $2";
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    sql = absl::Substitute(sql, "", "ON CONFLICT(k) DO NOTHING",
                           "RETURNING k, v1, v2, v3");
  } else {
    sql = absl::Substitute(sql, "OR IGNORE", "",
                           "THEN RETURN WITH ACTION AS action k, g1, g2, v3");
  }
  ZETASQL_EXPECT_OK(CommitDmlReturning({SqlStatement(sql)}, result_for_insert_ignore));
  absl::StatusOr<std::vector<ValueRow>> result_or = result_for_insert_ignore;
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    EXPECT_THAT(result_or, IsOkAndHoldsRow({2, 2, 2, 2}));
  } else {
    EXPECT_THAT(result_or, IsOkAndHoldsRow({2, 5, 4, 2, "INSERT"}));
  }
  // Read generated column values when run in GSQL dialect.
  if (GetParam() == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    EXPECT_THAT(
        Query("SELECT k, g1, g2, v3 FROM tablegen WHERE TRUE ORDER BY k"),
        IsOkAndHoldsRows({{1, 3, 2, 2}, {2, 5, 4, 2}}));
  }

  // 2. Insert new row with INSERT OR UPDATE DML with returning clause.
  std::vector<ValueRow> result_for_insert_update;
  sql =
      "INSERT $0 INTO tablegen(k, v1, v2, v3) "
      "VALUES (5, 50, 50, 5) $1 $2";
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    sql = absl::Substitute(sql, "",
                           "ON CONFLICT(k) DO UPDATE SET k = excluded.k, "
                           "v1 = excluded.v1, v2 = excluded.v2, "
                           "v3 = excluded.v3",
                           "RETURNING k, v1, v2, v3");
  } else {
    sql = absl::Substitute(sql, "OR UPDATE", "",
                           "THEN RETURN WITH ACTION AS action k, g1, g2, v3");
  }
  ZETASQL_EXPECT_OK(CommitDmlReturning({SqlStatement(sql)}, result_for_insert_update));
  absl::StatusOr<std::vector<ValueRow>> result_or1 = result_for_insert_update;
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    EXPECT_THAT(result_or1, IsOkAndHoldsRow({5, 50, 50, 5}));
  } else {
    EXPECT_THAT(result_or1, IsOkAndHoldsRow({5, 101, 100, 5, "INSERT"}));
  }
  // Read generated column values of inserted row when run in GSQL dialect.
  if (GetParam() == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    EXPECT_THAT(
        Query("SELECT k, g1, g2, v3 FROM tablegen WHERE k = 5 ORDER BY k"),
        IsOkAndHoldsRow({5, 101, 100, 5}));
  }

  // 3. Update existing row with INSERT OR UPDATE DML with returning clause.
  std::vector<ValueRow> result_for_insert_update2;
  sql =
      "INSERT $0 INTO tablegen(k, v1, v2, v3) "
      "VALUES (2, 20, 20, 20) $1 $2";
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    sql = absl::Substitute(sql, "",
                           "ON CONFLICT(k) DO UPDATE SET k = excluded.k, "
                           "v1 = excluded.v1, v2 = excluded.v2, "
                           "v3 = excluded.v3",
                           "RETURNING k, v1, v2, v3");
  } else {
    sql = absl::Substitute(sql, "OR UPDATE", "",
                           "THEN RETURN WITH ACTION AS action k, g1, g2, v3");
  }
  ZETASQL_EXPECT_OK(CommitDmlReturning({SqlStatement(sql)}, result_for_insert_update2));
  absl::StatusOr<std::vector<ValueRow>> result_or2 = result_for_insert_update2;
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    EXPECT_THAT(result_or2, IsOkAndHoldsRow({2, 20, 20, 20}));
  } else {
    EXPECT_THAT(result_or2, IsOkAndHoldsRow({2, 41, 40, 20, "UPDATE"}));
  }
  // Read generated column values of updated row when run in GSQL dialect.
  if (GetParam() == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    EXPECT_THAT(
        Query("SELECT k, g1, g2, v3 FROM tablegen WHERE k = 2 ORDER BY k"),
        IsOkAndHoldsRow({2, 41, 40, 20}));
  }
}

TEST_P(DmlTest, UpsertDmlGeneratedPrimaryKeyTable) {
  // TODO: b/310194797 - Generated keys are NULL in POSTGRES dialect.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }
  // Populate `gpktable2` table
  ZETASQL_EXPECT_OK(CommitDml(
      {SqlStatement("INSERT INTO gpktable2(k1, v1) VALUES (1, 1);")}));

  // INSERT OR IGNORE DML.
  std::string sql =
      "INSERT $0 INTO gpktable2(k1, v1) VALUES (2, 2), (1, 2), (5, 5) $1";
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    sql = absl::Substitute(sql, "", "ON CONFLICT(k2_stored) DO NOTHING");
  } else {
    sql = absl::Substitute(sql, "OR IGNORE", "");
  }

  ZETASQL_EXPECT_OK(CommitDml({SqlStatement(sql)}));
  EXPECT_THAT(Query("SELECT k1, k2_stored, v1, v2_stored FROM gpktable2 "
                    "WHERE TRUE ORDER BY k1"),
              IsOkAndHoldsRows({{1, 3, 1, 6}, {2, 6, 2, 7}, {5, 15, 5, 10}}));

  // INSERT OR UPDATE DML.
  sql = "INSERT $0 INTO gpktable2(k1, v1) VALUES (2, 20), (1, 10), (5, 50) $1";
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    sql = absl::Substitute(sql, "",
                           "ON CONFLICT(k2_stored) DO UPDATE "
                           "SET k1 = excluded.k1, v1 = excluded.v1");
  } else {
    sql = absl::Substitute(sql, "OR UPDATE", "");
  }
  ZETASQL_EXPECT_OK(CommitDml({SqlStatement(sql)}));
  EXPECT_THAT(
      Query("SELECT k1, k2_stored, v1, v2_stored FROM gpktable2 "
            "WHERE TRUE ORDER BY k1"),
      IsOkAndHoldsRows({{1, 3, 10, 15}, {2, 6, 20, 25}, {5, 15, 50, 55}}));
}

TEST_P(DmlTest, DISABLED_ReturningGPK) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_dml_returning = true;
  emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

  std::string returning =
      (GetParam() == database_api::DatabaseDialect::POSTGRESQL) ? "RETURNING"
                                                                : "THEN RETURN";

  // Insert THEN RETURN with `gpktable1`.
  std::vector<ValueRow> result_for_insert1;
  ZETASQL_EXPECT_OK(CommitDmlReturning(
      {SqlStatement(
          absl::Substitute("INSERT INTO gpktable1(k1, v1, v2) VALUES (1, 1, 0) "
                           "$0 k1, k2_stored, v2 + 5;",
                           returning))},
      result_for_insert1));
  absl::StatusOr<std::vector<ValueRow>> result_or = result_for_insert1;
  EXPECT_THAT(result_or, IsOkAndHoldsRow({1, 2, 5}));
  EXPECT_THAT(Query("SELECT k1, k2_stored, v2 + 5 FROM gpktable1 WHERE k1 = 1"),
              IsOkAndHoldsRow({1, 2, 5}));

  // Insert THEN RETURN with `gpktable2`.
  std::vector<ValueRow> result_for_insert2;
  ZETASQL_EXPECT_OK(
      CommitDmlReturning({SqlStatement(absl::Substitute(
                             "INSERT INTO gpktable2(k1, v1) VALUES (1, 1) "
                             "$0 k1, k2_stored, v2_stored + 5;",
                             returning))},
                         result_for_insert2));
  result_or = result_for_insert2;
  EXPECT_THAT(result_or, IsOkAndHoldsRow({1, 3, 11}));
  EXPECT_THAT(
      Query("SELECT k1, k2_stored, v2_stored + 5 FROM gpktable2 WHERE k1 = 1"),
      IsOkAndHoldsRow({1, 3, 11}));
}

TEST_P(DmlTest, ReturningStructValues) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_dml_returning = true;
  emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

  // Spanner PG dialect doesn't support STRUCT and array subquery expressions.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }

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
