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

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "google/cloud/spanner/value.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

class InformationSchemaTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

  absl::Status SetUpDatabase() override {
    return SetSchemaFromFile("information_schema.test");
  }

  // Information schema tables not yet supported.
  const std::pair<std::string, Value> kUnsupportedTables{
      "unsupported_tables",
      std::vector<std::string>(
          {GetNameForDialect("CHANGE_STREAMS"),
           GetNameForDialect("CHANGE_STREAM_COLUMNS"),
           GetNameForDialect("CHANGE_STREAM_OPTIONS"),
           GetNameForDialect("CHANGE_STREAM_PRIVILEGES"),
           GetNameForDialect("CHANGE_STREAM_TABLES"),
           GetNameForDialect("MODELS"), GetNameForDialect("MODEL_OPTIONS"),
           GetNameForDialect("MODEL_COLUMNS"),
           GetNameForDialect("MODEL_COLUMN_OPTIONS"),
           GetNameForDialect("VIEWS"), GetNameForDialect("ROLES"),
           GetNameForDialect("ROLE_GRANTEES"),
           GetNameForDialect("TABLE_PRIVILEGES"),
           GetNameForDialect("COLUMN_PRIVILEGES"),
           GetNameForDialect("PARAMETERS"), GetNameForDialect("ROUTINES"),
           GetNameForDialect("ROUTINE_OPTIONS"),
           GetNameForDialect("ROUTINE_PRIVILEGES"),
           GetNameForDialect("ROLE_TABLE_GRANTS"),
           GetNameForDialect("ROLE_COLUMN_GRANTS"),
           GetNameForDialect("ROLE_CHANGE_STREAM_GRANTS"),
           GetNameForDialect("ROLE_ROUTINE_GRANTS"),
           GetNameForDialect("TABLE_SYNONYMS"),
           // Unsupported PG-specific tables.
           "applicable_roles", "enabled_roles",
           "information_schema_catalog_name"})};

  // Information schema columns not yet supported.
  const std::pair<std::string, Value> kUnsupportedColumns{
      "unsupported_columns", std::vector<std::string>({})};

  // Information schema constraints not yet supported.
  const std::pair<std::string, Value> kUnsupportedConstraints{
      "unsupported_constraints", std::vector<std::string>({
                                     "CK_IS_NOT_NULL_TABLES_TABLE_TYPE",
                                     "CK_IS_NOT_NULL_VIEWS_SECURITY_TYPE",
                                 })};

  // Returns the given rows, replacing matching string patterns with their
  // actual values from the given results.
  static std::vector<ValueRow> ExpectedRows(
      const absl::StatusOr<std::vector<ValueRow>>& results,
      const std::vector<ValueRow> rows) {
    if (!results.ok()) {
      return rows;
    }
    std::vector<ValueRow> expected;
    for (const ValueRow& row : rows) {
      ValueRow next;
      for (int i = 0; i < row.values().size(); ++i) {
        Value value = row.values()[i];
        if (value.get<std::string>().ok()) {
          std::string pattern = value.get<std::string>().value();
          value = Value(FindString(results, i, pattern));
        }
        next.add(value);
      }
      expected.push_back(next);
    }
    return expected;
  }

  // Returns the first result string that matches a pattern. Returns the pattern
  // if none match. One use case is to match generated names that have
  // different signatures between production and emulator.
  static std::string FindString(
      const absl::StatusOr<std::vector<ValueRow>>& results, int field_index,
      const std::string& pattern) {
    for (const auto& row : results.value()) {
      auto value = row.values()[field_index].get<std::string>().value();
      if (RE2::FullMatch(value, pattern)) {
        return value;
      }
    }
    return pattern;
  }

  static void LogResults(const absl::StatusOr<std::vector<ValueRow>>& results) {
  }

  inline std::string GetNameForDialect(absl::string_view name) {
    // The system tables and associated columns are all defined in lowercase for
    // the PG dialect. The constants defined for the InformationSchema are for
    // the ZetaSQL dialect which are all uppercase. So we lowercase them here
    // for PG.
    if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
      return absl::AsciiStrToLower(name);
    }
    return std::string(name);
  }

  std::string GetUnsupportedTablesAsString() {
    struct formatter {
      void operator()(std::string* out, std::string s) const {
        out->append(absl::StrCat("'", s, "'"));
      }
    };
    return absl::StrCat(
        "array[",
        absl::StrJoin(
            *(kUnsupportedTables.second).get<std::vector<std::string>>(), ", ",
            formatter()),
        "]");
  }

  // For most of the tests, the table_catalog column is the first column in a
  // result. For PG, we currently don't set the value of this column correctly.
  // So we use this function to get a vector of rows with the first column
  // stripped.
  std::vector<ValueRow> StripFirstColumnFromRows(
      const std::vector<ValueRow>& rows) {
    std::vector<ValueRow> new_rows;
    for (const auto& row : rows) {
      ValueRow new_row;
      for (auto& value : row.values().subspan(1)) {
        new_row.add(value);
      }
      new_rows.push_back(new_row);
    }
    return new_rows;
  }

  // Aliases so test expectations read more clearly.
  cloud::spanner::Value Nb() { return Null<Bytes>(); }
  cloud::spanner::Value Ns() { return Null<std::string>(); }
  cloud::spanner::Value Ni() { return Null<std::int64_t>(); }
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectInformationSchemaTests, InformationSchemaTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<InformationSchemaTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(InformationSchemaTest, Schemata) {
  auto results = Query(R"(
      select
        s.catalog_name,
        s.schema_name
      from
        information_schema.schemata AS s
      order by
        s.catalog_name,
        s.schema_name
      limit 2
    )");
  LogResults(results);

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    ZETASQL_EXPECT_OK(results);
    EXPECT_GT((*results).size(), 0);
    ValueRow row = (*results)[0];
    Value schema_name = row.values()[1];
    EXPECT_THAT(*schema_name.get<std::string>(), "information_schema");
  } else {
    auto expected =
        std::vector<ValueRow>({{"", ""}, {"", "INFORMATION_SCHEMA"}});
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  }
}

TEST_P(InformationSchemaTest, MetaTables) {
  // The documented set of tables that should be returned is at:
  // ZetaSQL: https://cloud.google.com/spanner/docs/information-schema
  // PostgreSQL: https://cloud.google.com/spanner/docs/information-schema-pg
  //
  // The tables filtered out by the WHERE clause are not currently available in
  // the emulator. This test should not need to filter on table_name.

  std::string table_schema = GetNameForDialect("INFORMATION_SCHEMA");
  std::string filter;
  std::string collation;
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // The PG dialect doesn't support UNNEST or ANY on parameters so we have to
    // provide a static array of unsupported tables. The array is substituted in
    // below when running the query.
    filter = "NOT (t.table_name = ANY ($0))";
  } else {
    filter = "t.table_name not in unnest(@unsupported_tables)";
    // PG sorts strings differently to GSQL (underscores come before
    // alpha-numeric characters). In GSQL: "COLUMNS" "COLUMN_OPTIONS" In PG:
    // "COLUMN_OPTIONS"
    // "COLUMNS"
    // Since the Spanner PG dialect doesn't yet support specifying the
    // collation, we update the GSQL collation to return results in the same
    // order as PG.
    collation = "COLLATE \"en_US:cs\"";
  }

  std::string query = absl::Substitute(R"(
      select
        t.table_catalog,
        t.table_schema,
        t.table_type,
        t.table_name,
        t.parent_table_name,
        t.on_delete_action,
        t.spanner_state,
        t.row_deletion_policy_expression
      from
        information_schema.tables AS t
      where
        t.table_schema = '$0'
        and $1
      order by
        t.table_name
      $2
    )",
                                       table_schema, filter, collation);

  // clang-format off
  auto expected = std::vector<ValueRow>({
    {"", table_schema, "VIEW", GetNameForDialect("CHECK_CONSTRAINTS"), Ns(), Ns(), Ns(), Ns()},  // NOLINT
    {"", table_schema, "VIEW", GetNameForDialect("COLUMN_COLUMN_USAGE"), Ns(), Ns(), Ns(), Ns()},  // NOLINT
    {"", table_schema, "VIEW", GetNameForDialect("COLUMN_OPTIONS"), Ns(), Ns(), Ns(), Ns()},  // NOLINT
    {"", table_schema, "VIEW", GetNameForDialect("COLUMNS"), Ns(), Ns(), Ns(), Ns()},  // NOLINT
    {"", table_schema, "VIEW", GetNameForDialect("CONSTRAINT_COLUMN_USAGE"), Ns(), Ns(), Ns(), Ns()},  // NOLINT
    {"", table_schema, "VIEW", GetNameForDialect("CONSTRAINT_TABLE_USAGE"), Ns(), Ns(), Ns(), Ns()},  // NOLINT
    {"", table_schema, "VIEW", GetNameForDialect("DATABASE_OPTIONS"), Ns(), Ns(), Ns(), Ns()},  // NOLINT
    {"", table_schema, "VIEW", GetNameForDialect("INDEX_COLUMNS"), Ns(), Ns(), Ns(), Ns()},  // NOLINT
    {"", table_schema, "VIEW", GetNameForDialect("INDEXES"), Ns(), Ns(), Ns(), Ns()},  // NOLINT
    {"", table_schema, "VIEW", GetNameForDialect("KEY_COLUMN_USAGE"), Ns(), Ns(), Ns(), Ns()},  // NOLINT
    {"", table_schema, "VIEW", GetNameForDialect("REFERENTIAL_CONSTRAINTS"), Ns(), Ns(), Ns(), Ns()},  // NOLINT
    {"", table_schema, "VIEW", GetNameForDialect("SCHEMATA"), Ns(), Ns(), Ns(), Ns()},  // NOLINT
    {"", table_schema, "VIEW", GetNameForDialect("SPANNER_STATISTICS"), Ns(), Ns(), Ns(), Ns()},  // NOLINT
    {"", table_schema, "VIEW", GetNameForDialect("TABLE_CONSTRAINTS"), Ns(), Ns(), Ns(), Ns()},  // NOLINT
    {"", table_schema, "VIEW", GetNameForDialect("TABLES"), Ns(), Ns(), Ns(), Ns()},  // NOLINT
  });
  // clang-format on
  absl::StatusOr<std::vector<ValueRow>> results;
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // Remove the table_catalog column from the expected results since we don't
    // currently set that to its correct value for PG.
    std::vector<ValueRow> pg_expected = StripFirstColumnFromRows(expected);

    // Substituting in the static array of unsupported tables.
    results = Query(absl::Substitute(query, GetUnsupportedTablesAsString()));
    LogResults(results);
    ZETASQL_EXPECT_OK(results);
    EXPECT_THAT(StripFirstColumnFromRows(*results), pg_expected);
  } else {
    results = QueryWithParams(query, {kUnsupportedTables});
    LogResults(results);
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  }
}

TEST_P(InformationSchemaTest, GSQLMetaColumns) {
  // Since the query and output for the PG dialect is significantly different,
  // we test this logic in a separate function just for the PG dialect.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }

  // The tables and columns filtered out by the WHERE clause are not currently
  // available in the emulator. This test should not need to filter on
  // table_name.
  //
  // This test currently ignores the ORDINAL_POSITION column as the emulator
  // reports a different value because production has additional columns that
  // the emulator does not yet support.
  auto results = QueryWithParams(R"(
      select
        t.table_catalog,
        t.table_schema,
        t.table_name,
        t.column_name,
        t.column_default,
        t.data_type,
        t.is_nullable,
        t.spanner_type,
        t.is_generated,
        t.generation_expression,
        t.is_stored,
        t.spanner_state
      from
        information_schema.columns as t
      where
        t.table_schema = 'INFORMATION_SCHEMA'
        and t.table_name not in unnest(@unsupported_tables)
        and not (t.table_name = 'COLUMNS' and t.column_name in unnest(@unsupported_columns))
        and not (t.table_name = 'TABLES' and t.column_name = 'INTERLEAVE_TYPE')
        and not (t.table_name = 'TABLES' and t.column_name = 'ROW_DELETION_POLICY_EXPRESSION')
        and not (t.table_name = 'TABLES' and t.column_name = 'TABLE_TYPE')
        and not (t.table_name = 'SCHEMATA' and t.column_name = 'EFFECTIVE_TIMESTAMP')
        and not (t.table_name = 'SCHEMATA' and t.column_name = 'SCHEMA_OWNER')
      order by
        t.table_name,
        t.column_name
    )",
                                 {kUnsupportedTables, kUnsupportedColumns});
  LogResults(results);
  // clang-format off
  auto expected = std::vector<ValueRow>({
    {"", "INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "CHECK_CLAUSE", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "CONSTRAINT_CATALOG", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "CONSTRAINT_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "CONSTRAINT_SCHEMA", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "SPANNER_STATE", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "COLUMNS", "COLUMN_DEFAULT", Ns(), Ns(), "YES", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "COLUMNS", "COLUMN_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "COLUMNS", "DATA_TYPE", Ns(), Ns(), "YES", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "COLUMNS", "GENERATION_EXPRESSION", Ns(), Ns(), "YES", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "COLUMNS", "IS_GENERATED", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "COLUMNS", "IS_NULLABLE", Ns(), Ns(), "YES", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "COLUMNS", "IS_STORED", Ns(), Ns(), "YES", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "COLUMNS", "ORDINAL_POSITION", Ns(), Ns(), "NO", "INT64", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "COLUMNS", "SPANNER_STATE", Ns(), Ns(), "YES", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "COLUMNS", "SPANNER_TYPE", Ns(), Ns(), "YES", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "COLUMNS", "TABLE_CATALOG", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "COLUMNS", "TABLE_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "COLUMNS", "TABLE_SCHEMA", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "COLUMN_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "DEPENDENT_COLUMN", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "TABLE_CATALOG", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "TABLE_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "TABLE_SCHEMA", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "COLUMN_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "OPTION_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "OPTION_TYPE", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "OPTION_VALUE", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "TABLE_CATALOG", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "TABLE_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "TABLE_SCHEMA", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "COLUMN_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "CONSTRAINT_CATALOG", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "CONSTRAINT_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "CONSTRAINT_SCHEMA", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "TABLE_CATALOG", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "TABLE_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "TABLE_SCHEMA", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "CONSTRAINT_CATALOG", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "CONSTRAINT_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "CONSTRAINT_SCHEMA", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "TABLE_CATALOG", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "TABLE_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "TABLE_SCHEMA", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "DATABASE_OPTIONS", "CATALOG_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "DATABASE_OPTIONS", "OPTION_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "DATABASE_OPTIONS", "OPTION_TYPE", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "DATABASE_OPTIONS", "OPTION_VALUE", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "DATABASE_OPTIONS", "SCHEMA_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "INDEXES", "INDEX_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "INDEXES", "INDEX_STATE", Ns(), Ns(), "NO", "STRING(100)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "INDEXES", "INDEX_TYPE", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "INDEXES", "IS_NULL_FILTERED", Ns(), Ns(), "NO", "BOOL", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "INDEXES", "IS_UNIQUE", Ns(), Ns(), "NO", "BOOL", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "INDEXES", "PARENT_TABLE_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "INDEXES", "SPANNER_IS_MANAGED", Ns(), Ns(), "NO", "BOOL", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "INDEXES", "TABLE_CATALOG", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "INDEXES", "TABLE_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "INDEXES", "TABLE_SCHEMA", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "COLUMN_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "COLUMN_ORDERING", Ns(), Ns(), "YES", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "INDEX_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "INDEX_TYPE", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "IS_NULLABLE", Ns(), Ns(), "YES", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "ORDINAL_POSITION", Ns(), Ns(), "YES", "INT64", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "SPANNER_TYPE", Ns(), Ns(), "YES", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "TABLE_CATALOG", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "TABLE_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "TABLE_SCHEMA", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "COLUMN_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "CONSTRAINT_CATALOG", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "CONSTRAINT_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "CONSTRAINT_SCHEMA", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "ORDINAL_POSITION", Ns(), Ns(), "NO", "INT64", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "POSITION_IN_UNIQUE_CONSTRAINT", Ns(), Ns(), "YES", "INT64", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "TABLE_CATALOG", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "TABLE_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "TABLE_SCHEMA", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "CONSTRAINT_CATALOG", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "CONSTRAINT_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "CONSTRAINT_SCHEMA", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "DELETE_RULE", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "MATCH_OPTION", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "SPANNER_STATE", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "UNIQUE_CONSTRAINT_CATALOG", Ns(), Ns(), "YES", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "UNIQUE_CONSTRAINT_NAME", Ns(), Ns(), "YES", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "UNIQUE_CONSTRAINT_SCHEMA", Ns(), Ns(), "YES", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "UPDATE_RULE", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "SCHEMATA", "CATALOG_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "SCHEMATA", "SCHEMA_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "SPANNER_STATISTICS", "ALLOW_GC", Ns(), Ns(), "NO", "BOOL", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "SPANNER_STATISTICS", "CATALOG_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "SPANNER_STATISTICS", "PACKAGE_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "SPANNER_STATISTICS", "SCHEMA_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "TABLES", "ON_DELETE_ACTION", Ns(), Ns(), "YES", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "TABLES", "PARENT_TABLE_NAME", Ns(), Ns(), "YES", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "TABLES", "SPANNER_STATE", Ns(), Ns(), "YES", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "TABLES", "TABLE_CATALOG", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "TABLES", "TABLE_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "TABLES", "TABLE_SCHEMA", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "CONSTRAINT_CATALOG", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "CONSTRAINT_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "CONSTRAINT_SCHEMA", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "CONSTRAINT_TYPE", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "ENFORCED", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "INITIALLY_DEFERRED", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "IS_DEFERRABLE", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "TABLE_CATALOG", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "TABLE_NAME", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
    {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "TABLE_SCHEMA", Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), Ns()},  // NOLINT
  });
  // clang-format on
  EXPECT_THAT(results, IsOkAndHoldsRows(expected));
}

TEST_P(InformationSchemaTest, PGMetaColumns) {
  // Since the query and output for the PG dialect is significantly different,
  // we test this logic in a separate function for the GSQL dialect.
  if (GetParam() == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    GTEST_SKIP();
  }

  // This test currently ignores the ORDINAL_POSITION column as the emulator
  // reports a different value because production has additional columns that
  // the emulator does not yet support.
  //
  auto results = Query(R"(
      select
        t.table_schema,
        t.table_name,
        t.column_name,
        t.column_default,
        t.data_type,
        t.is_nullable,
        t.spanner_type,
        t.is_generated,
        t.generation_expression,
        t.is_stored,
        t.spanner_state,
        t.character_maximum_length,
        t.numeric_precision,
        t.numeric_precision_radix,
        t.numeric_scale
      from
        information_schema.columns as t
      where
        t.table_schema = 'information_schema'
        and t.table_name = any (array['columns', 'tables'])
        and t.column_name = any (array['table_catalog', 'ordinal_position'])
      order by
        t.table_name,
        t.column_name
    )");
  LogResults(results);
  // We keep this test simple and don't test for all possible tables and columns
  // that could be in this table. The PG version of the information schema
  // doesn't have a lot of variety in terms of types. So we test for the two
  // types that we know exist in the schema. The remaining column values will be
  // the same for all columns.
  // clang-format off
  auto expected = std::vector<ValueRow>({
    {"information_schema", "columns", "ordinal_position", Ns(), "bigint", "YES", "bigint", "NEVER", Ns(), Ns(), Ns(), Ni(), 64, 2, 0},  // NOLINT
    {"information_schema", "columns", "table_catalog", Ns(), "character varying", "YES", "character varying", "NEVER", Ns(), Ns(), Ns(), Ni(), Ni(), Ni(), Ni()},  // NOLINT
    {"information_schema", "tables", "table_catalog", Ns(), "character varying", "YES", "character varying", "NEVER", Ns(), Ns(), Ns(), Ni(), Ni(), Ni(), Ni()},  // NOLINT
  });
  // clang-format on
  EXPECT_THAT(results, IsOkAndHoldsRows(expected));
}

TEST_P(InformationSchemaTest, MetaIndexes) {
  std::string table_schema = GetNameForDialect("INFORMATION_SCHEMA");
  std::string filter;
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // The PG dialect doesn't support UNNEST or ANY on parameters so we have to
    // provide a static array of unsupported tables. The array is substituted in
    // below when running the query.
    filter = "NOT (t.table_name = ANY ($0))";
  } else {
    filter = "t.table_name not in unnest(@unsupported_tables)";
  }

  std::string query = absl::Substitute(R"(
      select
        t.table_catalog,
        t.table_schema,
        t.table_name,
        t.index_name,
        t.index_type,
        t.parent_table_name,
        t.is_unique,
        t.is_null_filtered,
        t.index_state
      from
        information_schema.indexes as t
      where
        t.table_schema = '$0'
        and $1
      order by
        t.table_name,
        t.index_name
    )",
                                       table_schema, filter);

  absl::StatusOr<std::vector<ValueRow>> results;
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // PG dialect doesn't store information schema indexes in this table.
    auto expected = std::vector<ValueRow>({});
    // Substituting in the static array of unsupported tables.
    results = Query(absl::Substitute(query, GetUnsupportedTablesAsString()));
    LogResults(results);
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  } else {
    // clang-format off
    auto expected = std::vector<ValueRow>({
        {"", "INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, Ns()},  // NOLINT
        {"", "INFORMATION_SCHEMA", "COLUMNS", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, Ns()},  // NOLINT
        {"", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, Ns()},  // NOLINT
        {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, Ns()},  // NOLINT
        {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, Ns()},  // NOLINT
        {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, Ns()},  // NOLINT
        {"", "INFORMATION_SCHEMA", "DATABASE_OPTIONS", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, Ns()},  // NOLINT
        {"", "INFORMATION_SCHEMA", "INDEXES", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, Ns()},  // NOLINT
        {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, Ns()},  // NOLINT
        {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, Ns()},  // NOLINT
        {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, Ns()},  // NOLINT
        {"", "INFORMATION_SCHEMA", "SCHEMATA", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, Ns()},  // NOLINT
        {"", "INFORMATION_SCHEMA", "SPANNER_STATISTICS", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, Ns()},  // NOLINT
        {"", "INFORMATION_SCHEMA", "TABLES", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, Ns()},  // NOLINT
        {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, Ns()},  // NOLINT
    });
    // clang-format on
    results = QueryWithParams(query, {kUnsupportedTables});
    LogResults(results);
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  }
}

TEST_P(InformationSchemaTest, MetaIndexColumns) {
  std::string table_schema = GetNameForDialect("INFORMATION_SCHEMA");
  std::string filter;
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // The PG dialect doesn't support UNNEST or ANY on parameters so we have to
    // provide a static array of unsupported tables. The array is substituted in
    // below when running the query.
    filter = "NOT (t.table_name = ANY ($0))";
  } else {
    filter = "t.table_name not in unnest(@unsupported_tables)";
  }

  std::string query = absl::Substitute(R"(
      select
        t.table_schema,
        t.table_name,
        t.index_name,
        t.index_type,
        t.column_name,
        t.ordinal_position,
        t.column_ordering,
        t.is_nullable,
        t.spanner_type
      from
        information_schema.index_columns as t
      where
        t.table_schema = '$0'
        and $1
      order by
        t.table_name,
        t.index_name,
        t.ordinal_position
    )",
                                       table_schema, filter);

  absl::StatusOr<std::vector<ValueRow>> results;
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // PG dialect doesn't store information schema columns in this table.
    auto expected = std::vector<ValueRow>({});
    // Substituting in the static array of unsupported tables.
    results = Query(absl::Substitute(query, GetUnsupportedTablesAsString()));
    LogResults(results);
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  } else {
    // clang-format off
    auto expected = std::vector<ValueRow>({
      {"INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_CATALOG", 1, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_SCHEMA", 2, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_NAME", 3, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "COLUMNS", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_CATALOG", 1, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "COLUMNS", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_SCHEMA", 2, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "COLUMNS", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_NAME", 3, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "COLUMNS", "PRIMARY_KEY", "PRIMARY_KEY", "COLUMN_NAME", 4, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_CATALOG", 1, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_SCHEMA", 2, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_NAME", 3, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "COLUMN_NAME", 4, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "DEPENDENT_COLUMN", 5, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "COLUMN_OPTIONS", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_CATALOG", 1, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "COLUMN_OPTIONS", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_SCHEMA", 2, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "COLUMN_OPTIONS", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_NAME", 3, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "COLUMN_OPTIONS", "PRIMARY_KEY", "PRIMARY_KEY", "COLUMN_NAME", 4, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "COLUMN_OPTIONS", "PRIMARY_KEY", "PRIMARY_KEY", "OPTION_NAME", 5, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_CATALOG", 1, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_SCHEMA", 2, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_NAME", 3, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "COLUMN_NAME", 4, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_CATALOG", 1, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_SCHEMA", 2, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_NAME", 3, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_CATALOG", 4, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_SCHEMA", 5, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_NAME", 6, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "DATABASE_OPTIONS", "PRIMARY_KEY", "PRIMARY_KEY", "CATALOG_NAME", 1, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "DATABASE_OPTIONS", "PRIMARY_KEY", "PRIMARY_KEY", "SCHEMA_NAME", 2, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "DATABASE_OPTIONS", "PRIMARY_KEY", "PRIMARY_KEY", "OPTION_NAME", 3, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "INDEXES", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_CATALOG", 1, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "INDEXES", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_SCHEMA", 2, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "INDEXES", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_NAME", 3, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "INDEXES", "PRIMARY_KEY", "PRIMARY_KEY", "INDEX_NAME", 4, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "INDEXES", "PRIMARY_KEY", "PRIMARY_KEY", "INDEX_TYPE", 5, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "INDEX_COLUMNS", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_CATALOG", 1, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "INDEX_COLUMNS", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_SCHEMA", 2, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "INDEX_COLUMNS", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_NAME", 3, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "INDEX_COLUMNS", "PRIMARY_KEY", "PRIMARY_KEY", "INDEX_NAME", 4, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "INDEX_COLUMNS", "PRIMARY_KEY", "PRIMARY_KEY", "INDEX_TYPE", 5, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "INDEX_COLUMNS", "PRIMARY_KEY", "PRIMARY_KEY", "COLUMN_NAME", 6, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_CATALOG", 1, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_SCHEMA", 2, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_NAME", 3, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "COLUMN_NAME", 4, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_CATALOG", 1, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_SCHEMA", 2, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_NAME", 3, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "SCHEMATA", "PRIMARY_KEY", "PRIMARY_KEY", "CATALOG_NAME", 1, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "SCHEMATA", "PRIMARY_KEY", "PRIMARY_KEY", "SCHEMA_NAME", 2, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "SPANNER_STATISTICS", "PRIMARY_KEY", "PRIMARY_KEY", "CATALOG_NAME", 1, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "SPANNER_STATISTICS", "PRIMARY_KEY", "PRIMARY_KEY", "SCHEMA_NAME", 2, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "SPANNER_STATISTICS", "PRIMARY_KEY", "PRIMARY_KEY", "PACKAGE_NAME", 3, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "TABLES", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_CATALOG", 1, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "TABLES", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_SCHEMA", 2, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "TABLES", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_NAME", 3, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_CATALOG", 1, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_SCHEMA", 2, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_NAME", 3, "ASC", "NO", "STRING(MAX)"},  // NOLINT
    });
    // clang-format on
    results = QueryWithParams(query, {kUnsupportedTables});
    LogResults(results);
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  }
}

TEST_P(InformationSchemaTest, MetaTableConstraints) {
  std::string filter;
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    filter = "t.constraint_schema = 'information_schema'";
  } else {
    filter = R"(
        t.constraint_catalog = ''
        and t.constraint_schema = 'INFORMATION_SCHEMA'
        and t.table_name not in unnest(@unsupported_tables)
        and t.constraint_name not in unnest(@unsupported_constraints))";
  }

  std::string query = absl::Substitute(R"(
      select
        t.constraint_catalog,
        t.constraint_schema,
        t.constraint_name,
        t.table_catalog,
        t.table_schema,
        t.table_name,
        t.constraint_type,
        t.is_deferrable,
        t.initially_deferred,
        t.enforced
      from
        information_schema.table_constraints as t
      where
        $0
      order by
        t.constraint_name
  )",
                                       filter);
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    auto results = Query(query);
    LogResults(results);
    // Production doesn't add check constraints for the information schema
    // so we expect no results to be returned.
    auto expected = std::vector<ValueRow>({});
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  } else {
    auto results =
        QueryWithParams(query, {kUnsupportedTables, kUnsupportedConstraints});
    LogResults(results);
    // clang-format off
    auto expected = std::vector<ValueRow>({
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CHECK_CONSTRAINTS_CHECK_CLAUSE", "", "INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CHECK_CONSTRAINTS_CONSTRAINT_CATALOG", "", "INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CHECK_CONSTRAINTS_CONSTRAINT_NAME", "", "INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CHECK_CONSTRAINTS_CONSTRAINT_SCHEMA", "", "INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CHECK_CONSTRAINTS_SPANNER_STATE", "", "INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMNS_COLUMN_NAME", "", "INFORMATION_SCHEMA", "COLUMNS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMNS_IS_GENERATED", "", "INFORMATION_SCHEMA", "COLUMNS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMNS_ORDINAL_POSITION", "", "INFORMATION_SCHEMA", "COLUMNS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMNS_TABLE_CATALOG", "", "INFORMATION_SCHEMA", "COLUMNS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMNS_TABLE_NAME", "", "INFORMATION_SCHEMA", "COLUMNS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMNS_TABLE_SCHEMA", "", "INFORMATION_SCHEMA", "COLUMNS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_COLUMN_USAGE_COLUMN_NAME", "", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_COLUMN_USAGE_DEPENDENT_COLUMN", "", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_COLUMN_USAGE_TABLE_CATALOG", "", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_COLUMN_USAGE_TABLE_NAME", "", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_COLUMN_USAGE_TABLE_SCHEMA", "", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_OPTIONS_COLUMN_NAME", "", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_OPTIONS_OPTION_NAME", "", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_OPTIONS_OPTION_TYPE", "", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_OPTIONS_OPTION_VALUE", "", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_OPTIONS_TABLE_CATALOG", "", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_OPTIONS_TABLE_NAME", "", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_OPTIONS_TABLE_SCHEMA", "", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_COLUMN_USAGE_COLUMN_NAME", "", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_COLUMN_USAGE_CONSTRAINT_CATALOG", "", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_COLUMN_USAGE_CONSTRAINT_NAME", "", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_COLUMN_USAGE_CONSTRAINT_SCHEMA", "", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_COLUMN_USAGE_TABLE_CATALOG", "", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_COLUMN_USAGE_TABLE_NAME", "", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_COLUMN_USAGE_TABLE_SCHEMA", "", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_TABLE_USAGE_CONSTRAINT_CATALOG", "", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_TABLE_USAGE_CONSTRAINT_NAME", "", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_TABLE_USAGE_CONSTRAINT_SCHEMA", "", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_TABLE_USAGE_TABLE_CATALOG", "", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_TABLE_USAGE_TABLE_NAME", "", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_TABLE_USAGE_TABLE_SCHEMA", "", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_DATABASE_OPTIONS_CATALOG_NAME", "", "INFORMATION_SCHEMA", "DATABASE_OPTIONS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_DATABASE_OPTIONS_OPTION_NAME", "", "INFORMATION_SCHEMA", "DATABASE_OPTIONS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_DATABASE_OPTIONS_OPTION_TYPE", "", "INFORMATION_SCHEMA", "DATABASE_OPTIONS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_DATABASE_OPTIONS_OPTION_VALUE", "", "INFORMATION_SCHEMA", "DATABASE_OPTIONS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_DATABASE_OPTIONS_SCHEMA_NAME", "", "INFORMATION_SCHEMA", "DATABASE_OPTIONS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_INDEX_NAME", "", "INFORMATION_SCHEMA", "INDEXES", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_INDEX_STATE", "", "INFORMATION_SCHEMA", "INDEXES", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_INDEX_TYPE", "", "INFORMATION_SCHEMA", "INDEXES", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_IS_NULL_FILTERED", "", "INFORMATION_SCHEMA", "INDEXES", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_IS_UNIQUE", "", "INFORMATION_SCHEMA", "INDEXES", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_PARENT_TABLE_NAME", "", "INFORMATION_SCHEMA", "INDEXES", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_SPANNER_IS_MANAGED", "", "INFORMATION_SCHEMA", "INDEXES", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_TABLE_CATALOG", "", "INFORMATION_SCHEMA", "INDEXES", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_TABLE_NAME", "", "INFORMATION_SCHEMA", "INDEXES", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_TABLE_SCHEMA", "", "INFORMATION_SCHEMA", "INDEXES", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEX_COLUMNS_COLUMN_NAME", "", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEX_COLUMNS_INDEX_NAME", "", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEX_COLUMNS_INDEX_TYPE", "", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEX_COLUMNS_TABLE_CATALOG", "", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEX_COLUMNS_TABLE_NAME", "", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEX_COLUMNS_TABLE_SCHEMA", "", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_COLUMN_NAME", "", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_CONSTRAINT_CATALOG", "", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_CONSTRAINT_NAME", "", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_CONSTRAINT_SCHEMA", "", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_ORDINAL_POSITION", "", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_TABLE_CATALOG", "", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_TABLE_NAME", "", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_TABLE_SCHEMA", "", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_REFERENTIAL_CONSTRAINTS_CONSTRAINT_CATALOG", "", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_REFERENTIAL_CONSTRAINTS_CONSTRAINT_NAME", "", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_REFERENTIAL_CONSTRAINTS_CONSTRAINT_SCHEMA", "", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_REFERENTIAL_CONSTRAINTS_DELETE_RULE", "", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_REFERENTIAL_CONSTRAINTS_MATCH_OPTION", "", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_REFERENTIAL_CONSTRAINTS_SPANNER_STATE", "", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_REFERENTIAL_CONSTRAINTS_UPDATE_RULE", "", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_SCHEMATA_CATALOG_NAME", "", "INFORMATION_SCHEMA", "SCHEMATA", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_SCHEMATA_SCHEMA_NAME", "", "INFORMATION_SCHEMA", "SCHEMATA", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_SPANNER_STATISTICS_ALLOW_GC", "", "INFORMATION_SCHEMA", "SPANNER_STATISTICS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_SPANNER_STATISTICS_CATALOG_NAME", "", "INFORMATION_SCHEMA", "SPANNER_STATISTICS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_SPANNER_STATISTICS_PACKAGE_NAME", "", "INFORMATION_SCHEMA", "SPANNER_STATISTICS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_SPANNER_STATISTICS_SCHEMA_NAME", "", "INFORMATION_SCHEMA", "SPANNER_STATISTICS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLES_TABLE_CATALOG", "", "INFORMATION_SCHEMA", "TABLES", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLES_TABLE_NAME", "", "INFORMATION_SCHEMA", "TABLES", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLES_TABLE_SCHEMA", "", "INFORMATION_SCHEMA", "TABLES", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_CONSTRAINT_CATALOG", "", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_CONSTRAINT_NAME", "", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_CONSTRAINT_SCHEMA", "", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_CONSTRAINT_TYPE", "", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_ENFORCED", "", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_INITIALLY_DEFERRED", "", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_IS_DEFERRABLE", "", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_TABLE_CATALOG", "", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_TABLE_NAME", "", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_TABLE_SCHEMA", "", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_CHECK_CONSTRAINTS", "", "INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "PRIMARY KEY", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_COLUMNS", "", "INFORMATION_SCHEMA", "COLUMNS", "PRIMARY KEY", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_COLUMN_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "PRIMARY KEY", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_COLUMN_OPTIONS", "", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "PRIMARY KEY", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_CONSTRAINT_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "PRIMARY KEY", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_CONSTRAINT_TABLE_USAGE", "", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "PRIMARY KEY", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_DATABASE_OPTIONS", "", "INFORMATION_SCHEMA", "DATABASE_OPTIONS", "PRIMARY KEY", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_INDEXES", "", "INFORMATION_SCHEMA", "INDEXES", "PRIMARY KEY", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_INDEX_COLUMNS", "", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "PRIMARY KEY", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_KEY_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "PRIMARY KEY", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_REFERENTIAL_CONSTRAINTS", "", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "PRIMARY KEY", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_SCHEMATA", "", "INFORMATION_SCHEMA", "SCHEMATA", "PRIMARY KEY", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_SPANNER_STATISTICS", "", "INFORMATION_SCHEMA", "SPANNER_STATISTICS", "PRIMARY KEY", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_TABLES", "", "INFORMATION_SCHEMA", "TABLES", "PRIMARY KEY", "NO", "NO", "YES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_TABLE_CONSTRAINTS", "", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "PRIMARY KEY", "NO", "NO", "YES"},  // NOLINT
    });
    // clang-format on
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  }
}

TEST_P(InformationSchemaTest, MetaCheckConstraints) {
  std::string filter;
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    filter = "t.constraint_schema = 'information_schema'";
  } else {
    filter = R"(t.constraint_schema = 'INFORMATION_SCHEMA'
        and t.constraint_catalog = ''
        and t.constraint_name not in unnest(@unsupported_constraints)
        and t.constraint_name NOT LIKE 'CK_IS_NOT_NULL_CHANGE_STREAM%'
        and t.constraint_name NOT LIKE 'CK_IS_NOT_NULL_DATABASE_OPTIONS%'
        and t.constraint_name NOT LIKE 'CK_IS_NOT_NULL_VIEWS_TABLE%'
        and t.constraint_name NOT LIKE 'CK_IS_NOT_NULL_COLUMN_PRIVILEGES%'
        and t.constraint_name NOT LIKE 'CK_IS_NOT_NULL_TABLE_PRIVILEGES%'
        and t.constraint_name NOT LIKE 'CK_IS_NOT_NULL_ROLES%'
        and t.constraint_name NOT LIKE 'CK_IS_NOT_NULL_ROLE_GRANTEES%'
        and t.constraint_name NOT LIKE 'CK_IS_NOT_NULL_ROLE_TABLE_GRANTS%'
        and t.constraint_name NOT LIKE 'CK_IS_NOT_NULL_ROLE_COLUMN_GRANTS%'
        and t.constraint_name NOT LIKE 'CK_IS_NOT_NULL_ROLE_CHANGE_STREAM_GRANTS%'
        and t.constraint_name NOT LIKE 'CK_IS_NOT_NULL_ROLE_ROUTINE_GRANTS%'
        and t.constraint_name NOT LIKE 'CK_IS_NOT_NULL_MODELS%'
        and t.constraint_name NOT LIKE 'CK_IS_NOT_NULL_MODEL_OPTIONS%'
        and t.constraint_name NOT LIKE 'CK_IS_NOT_NULL_MODEL_COLUMNS%'
        and t.constraint_name NOT LIKE 'CK_IS_NOT_NULL_MODEL_COLUMN_OPTIONS%'
        and t.constraint_name NOT LIKE 'CK_IS_NOT_NULL_PARAMETERS%'
        and t.constraint_name NOT LIKE 'CK_IS_NOT_NULL_ROUTINES%'
        and t.constraint_name NOT LIKE 'CK_IS_NOT_NULL_ROUTINE_OPTIONS%'
        and t.constraint_name NOT LIKE 'CK_IS_NOT_NULL_ROUTINE_PRIVILEGES%'
        and t.constraint_name NOT LIKE 'CK_IS_NOT_NULL_TABLE_SYNONYMS%')";
  }

  std::string query = absl::Substitute(R"(
      select
        t.constraint_catalog,
        t.constraint_schema,
        t.constraint_name,
        t.check_clause,
        t.spanner_state
      from
        information_schema.check_constraints as t
      where
        $0
      order by
        t.constraint_name
  )",
                                       filter);

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    auto results = Query(query);
    LogResults(results);
    // Production doesn't add check constraints for the information schema
    // so we expect no results to be returned.
    auto expected = std::vector<ValueRow>({});
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  } else {
    auto results = QueryWithParams(query, {kUnsupportedConstraints});
    LogResults(results);
    // clang-format off
    auto expected = std::vector<ValueRow>({
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CHECK_CONSTRAINTS_CHECK_CLAUSE", "CHECK_CLAUSE IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CHECK_CONSTRAINTS_CONSTRAINT_CATALOG", "CONSTRAINT_CATALOG IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CHECK_CONSTRAINTS_CONSTRAINT_NAME", "CONSTRAINT_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CHECK_CONSTRAINTS_CONSTRAINT_SCHEMA", "CONSTRAINT_SCHEMA IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CHECK_CONSTRAINTS_SPANNER_STATE", "SPANNER_STATE IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMNS_COLUMN_NAME", "COLUMN_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMNS_IS_GENERATED", "IS_GENERATED IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMNS_ORDINAL_POSITION", "ORDINAL_POSITION IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMNS_TABLE_CATALOG", "TABLE_CATALOG IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMNS_TABLE_NAME", "TABLE_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMNS_TABLE_SCHEMA", "TABLE_SCHEMA IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_COLUMN_USAGE_COLUMN_NAME", "COLUMN_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_COLUMN_USAGE_DEPENDENT_COLUMN", "DEPENDENT_COLUMN IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_COLUMN_USAGE_TABLE_CATALOG", "TABLE_CATALOG IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_COLUMN_USAGE_TABLE_NAME", "TABLE_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_COLUMN_USAGE_TABLE_SCHEMA", "TABLE_SCHEMA IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_OPTIONS_COLUMN_NAME", "COLUMN_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_OPTIONS_OPTION_NAME", "OPTION_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_OPTIONS_OPTION_TYPE", "OPTION_TYPE IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_OPTIONS_OPTION_VALUE", "OPTION_VALUE IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_OPTIONS_TABLE_CATALOG", "TABLE_CATALOG IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_OPTIONS_TABLE_NAME", "TABLE_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_OPTIONS_TABLE_SCHEMA", "TABLE_SCHEMA IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_COLUMN_USAGE_COLUMN_NAME", "COLUMN_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_COLUMN_USAGE_CONSTRAINT_CATALOG", "CONSTRAINT_CATALOG IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_COLUMN_USAGE_CONSTRAINT_NAME", "CONSTRAINT_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_COLUMN_USAGE_CONSTRAINT_SCHEMA", "CONSTRAINT_SCHEMA IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_COLUMN_USAGE_TABLE_CATALOG", "TABLE_CATALOG IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_COLUMN_USAGE_TABLE_NAME", "TABLE_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_COLUMN_USAGE_TABLE_SCHEMA", "TABLE_SCHEMA IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_TABLE_USAGE_CONSTRAINT_CATALOG", "CONSTRAINT_CATALOG IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_TABLE_USAGE_CONSTRAINT_NAME", "CONSTRAINT_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_TABLE_USAGE_CONSTRAINT_SCHEMA", "CONSTRAINT_SCHEMA IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_TABLE_USAGE_TABLE_CATALOG", "TABLE_CATALOG IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_TABLE_USAGE_TABLE_NAME", "TABLE_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_TABLE_USAGE_TABLE_SCHEMA", "TABLE_SCHEMA IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_INDEX_NAME", "INDEX_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_INDEX_STATE", "INDEX_STATE IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_INDEX_TYPE", "INDEX_TYPE IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_IS_NULL_FILTERED", "IS_NULL_FILTERED IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_IS_UNIQUE", "IS_UNIQUE IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_PARENT_TABLE_NAME", "PARENT_TABLE_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_SPANNER_IS_MANAGED", "SPANNER_IS_MANAGED IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_TABLE_CATALOG", "TABLE_CATALOG IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_TABLE_NAME", "TABLE_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_TABLE_SCHEMA", "TABLE_SCHEMA IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEX_COLUMNS_COLUMN_NAME", "COLUMN_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEX_COLUMNS_INDEX_NAME", "INDEX_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEX_COLUMNS_INDEX_TYPE", "INDEX_TYPE IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEX_COLUMNS_TABLE_CATALOG", "TABLE_CATALOG IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEX_COLUMNS_TABLE_NAME", "TABLE_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEX_COLUMNS_TABLE_SCHEMA", "TABLE_SCHEMA IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_COLUMN_NAME", "COLUMN_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_CONSTRAINT_CATALOG", "CONSTRAINT_CATALOG IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_CONSTRAINT_NAME", "CONSTRAINT_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_CONSTRAINT_SCHEMA", "CONSTRAINT_SCHEMA IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_ORDINAL_POSITION", "ORDINAL_POSITION IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_TABLE_CATALOG", "TABLE_CATALOG IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_TABLE_NAME", "TABLE_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_TABLE_SCHEMA", "TABLE_SCHEMA IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_REFERENTIAL_CONSTRAINTS_CONSTRAINT_CATALOG", "CONSTRAINT_CATALOG IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_REFERENTIAL_CONSTRAINTS_CONSTRAINT_NAME", "CONSTRAINT_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_REFERENTIAL_CONSTRAINTS_CONSTRAINT_SCHEMA", "CONSTRAINT_SCHEMA IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_REFERENTIAL_CONSTRAINTS_DELETE_RULE", "DELETE_RULE IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_REFERENTIAL_CONSTRAINTS_MATCH_OPTION", "MATCH_OPTION IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_REFERENTIAL_CONSTRAINTS_SPANNER_STATE", "SPANNER_STATE IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_REFERENTIAL_CONSTRAINTS_UPDATE_RULE", "UPDATE_RULE IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_SCHEMATA_CATALOG_NAME", "CATALOG_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_SCHEMATA_SCHEMA_NAME", "SCHEMA_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_SPANNER_STATISTICS_ALLOW_GC", "ALLOW_GC IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_SPANNER_STATISTICS_CATALOG_NAME", "CATALOG_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_SPANNER_STATISTICS_PACKAGE_NAME", "PACKAGE_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_SPANNER_STATISTICS_SCHEMA_NAME", "SCHEMA_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLES_TABLE_CATALOG", "TABLE_CATALOG IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLES_TABLE_NAME", "TABLE_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLES_TABLE_SCHEMA", "TABLE_SCHEMA IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_CONSTRAINT_CATALOG", "CONSTRAINT_CATALOG IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_CONSTRAINT_NAME", "CONSTRAINT_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_CONSTRAINT_SCHEMA", "CONSTRAINT_SCHEMA IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_CONSTRAINT_TYPE", "CONSTRAINT_TYPE IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_ENFORCED", "ENFORCED IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_INITIALLY_DEFERRED", "INITIALLY_DEFERRED IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_IS_DEFERRABLE", "IS_DEFERRABLE IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_TABLE_CATALOG", "TABLE_CATALOG IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_TABLE_NAME", "TABLE_NAME IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_TABLE_SCHEMA", "TABLE_SCHEMA IS NOT NULL", "COMMITTED"},  // NOLINT
    });
    // clang-format on
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  }
}

TEST_P(InformationSchemaTest, MetaConstraintTableUsage) {
  std::string filter;
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    filter = R"(
        t.table_schema = 'information_schema'
    )";
  } else {
    filter = R"(
        t.table_catalog = ''
        and t.table_schema = 'INFORMATION_SCHEMA'
        and t.table_name not in unnest(@unsupported_tables)
        and t.constraint_name not in unnest(@unsupported_constraints)
    )";
  }

  std::string query = absl::Substitute(R"(
      select
        t.table_catalog,
        t.table_schema,
        t.table_name,
        t.constraint_catalog,
        t.constraint_schema,
        t.constraint_name
      from
        information_schema.constraint_table_usage as t
      where
        $0
      order by
        t.table_name,
        t.constraint_name
    )",
                                       filter);

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    auto results = Query(query);
    LogResults(results);
    // Production doesn't add constraint table usage for the information schema
    // so we expect no results to be returned.
    auto expected = std::vector<ValueRow>({});
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  } else {
    auto results =
        QueryWithParams(query, {kUnsupportedTables, kUnsupportedConstraints});
    LogResults(results);
    // clang-format off
    auto expected = std::vector<ValueRow>({
      {"", "INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CHECK_CONSTRAINTS_CHECK_CLAUSE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CHECK_CONSTRAINTS_CONSTRAINT_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CHECK_CONSTRAINTS_CONSTRAINT_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CHECK_CONSTRAINTS_CONSTRAINT_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CHECK_CONSTRAINTS_SPANNER_STATE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "", "INFORMATION_SCHEMA", "PK_CHECK_CONSTRAINTS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMNS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMNS_COLUMN_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMNS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMNS_IS_GENERATED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMNS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMNS_ORDINAL_POSITION"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMNS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMNS_TABLE_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMNS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMNS_TABLE_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMNS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMNS_TABLE_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMNS", "", "INFORMATION_SCHEMA", "PK_COLUMNS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_COLUMN_USAGE_COLUMN_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_COLUMN_USAGE_DEPENDENT_COLUMN"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_COLUMN_USAGE_TABLE_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_COLUMN_USAGE_TABLE_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_COLUMN_USAGE_TABLE_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "PK_COLUMN_COLUMN_USAGE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_OPTIONS_COLUMN_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_OPTIONS_OPTION_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_OPTIONS_OPTION_TYPE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_OPTIONS_OPTION_VALUE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_OPTIONS_TABLE_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_OPTIONS_TABLE_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_OPTIONS_TABLE_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "", "INFORMATION_SCHEMA", "PK_COLUMN_OPTIONS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_COLUMN_USAGE_COLUMN_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_COLUMN_USAGE_CONSTRAINT_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_COLUMN_USAGE_CONSTRAINT_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_COLUMN_USAGE_CONSTRAINT_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_COLUMN_USAGE_TABLE_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_COLUMN_USAGE_TABLE_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_COLUMN_USAGE_TABLE_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "PK_CONSTRAINT_COLUMN_USAGE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_TABLE_USAGE_CONSTRAINT_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_TABLE_USAGE_CONSTRAINT_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_TABLE_USAGE_CONSTRAINT_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_TABLE_USAGE_TABLE_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_TABLE_USAGE_TABLE_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_TABLE_USAGE_TABLE_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "", "INFORMATION_SCHEMA", "PK_CONSTRAINT_TABLE_USAGE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "DATABASE_OPTIONS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_DATABASE_OPTIONS_CATALOG_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "DATABASE_OPTIONS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_DATABASE_OPTIONS_OPTION_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "DATABASE_OPTIONS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_DATABASE_OPTIONS_OPTION_TYPE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "DATABASE_OPTIONS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_DATABASE_OPTIONS_OPTION_VALUE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "DATABASE_OPTIONS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_DATABASE_OPTIONS_SCHEMA_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "DATABASE_OPTIONS", "", "INFORMATION_SCHEMA", "PK_DATABASE_OPTIONS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEXES", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_INDEX_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEXES", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_INDEX_STATE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEXES", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_INDEX_TYPE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEXES", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_IS_NULL_FILTERED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEXES", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_IS_UNIQUE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEXES", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_PARENT_TABLE_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEXES", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_SPANNER_IS_MANAGED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEXES", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_TABLE_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEXES", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_TABLE_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEXES", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_TABLE_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEXES", "", "INFORMATION_SCHEMA", "PK_INDEXES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEX_COLUMNS_COLUMN_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEX_COLUMNS_INDEX_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEX_COLUMNS_INDEX_TYPE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEX_COLUMNS_TABLE_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEX_COLUMNS_TABLE_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEX_COLUMNS_TABLE_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "", "INFORMATION_SCHEMA", "PK_INDEX_COLUMNS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_COLUMN_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_CONSTRAINT_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_CONSTRAINT_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_CONSTRAINT_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_ORDINAL_POSITION"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_TABLE_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_TABLE_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_TABLE_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "PK_KEY_COLUMN_USAGE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_REFERENTIAL_CONSTRAINTS_CONSTRAINT_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_REFERENTIAL_CONSTRAINTS_CONSTRAINT_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_REFERENTIAL_CONSTRAINTS_CONSTRAINT_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_REFERENTIAL_CONSTRAINTS_DELETE_RULE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_REFERENTIAL_CONSTRAINTS_MATCH_OPTION"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_REFERENTIAL_CONSTRAINTS_SPANNER_STATE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_REFERENTIAL_CONSTRAINTS_UPDATE_RULE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "", "INFORMATION_SCHEMA", "PK_REFERENTIAL_CONSTRAINTS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "SCHEMATA", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_SCHEMATA_CATALOG_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "SCHEMATA", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_SCHEMATA_SCHEMA_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "SCHEMATA", "", "INFORMATION_SCHEMA", "PK_SCHEMATA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "SPANNER_STATISTICS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_SPANNER_STATISTICS_ALLOW_GC"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "SPANNER_STATISTICS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_SPANNER_STATISTICS_CATALOG_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "SPANNER_STATISTICS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_SPANNER_STATISTICS_PACKAGE_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "SPANNER_STATISTICS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_SPANNER_STATISTICS_SCHEMA_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "SPANNER_STATISTICS", "", "INFORMATION_SCHEMA", "PK_SPANNER_STATISTICS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLES", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLES_TABLE_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLES", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLES_TABLE_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLES", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLES_TABLE_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLES", "", "INFORMATION_SCHEMA", "PK_TABLES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_CONSTRAINT_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_CONSTRAINT_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_CONSTRAINT_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_CONSTRAINT_TYPE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_ENFORCED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_INITIALLY_DEFERRED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_IS_DEFERRABLE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_TABLE_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_TABLE_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_TABLE_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "", "INFORMATION_SCHEMA", "PK_TABLE_CONSTRAINTS"},  // NOLINT
    });
    // clang-format on
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  }
}

TEST_P(InformationSchemaTest, MetaReferentialConstraints) {
  auto results =
      Query(absl::Substitute(R"(
      select
        t.constraint_catalog,
        t.constraint_schema,
        t.constraint_name,
        t.unique_constraint_catalog,
        t.unique_constraint_schema,
        t.unique_constraint_name,
        t.match_option,
        t.update_rule,
        t.delete_rule,
        t.spanner_state
      from
        information_schema.referential_constraints as t
      where
        t.constraint_schema = '$0'
    )",
                             GetNameForDialect("INFORMATION_SCHEMA")));
  EXPECT_THAT(results, IsOkAndHoldsRows({}));
}

TEST_P(InformationSchemaTest, MetaKeyColumnUsage) {
  std::string filter;
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    filter = R"(
        t.constraint_schema = 'information_schema'
    )";
  } else {
    filter = R"(
        t.constraint_catalog = ''
        and t.constraint_schema = 'INFORMATION_SCHEMA'
        and t.table_name not in unnest(@unsupported_tables)
    )";
  }

  std::string query = absl::Substitute(R"(
      select
        t.constraint_catalog,
        t.constraint_schema,
        t.constraint_name,
        t.table_catalog,
        t.table_schema,
        t.table_name,
        t.column_name,
        t.ordinal_position,
        t.position_in_unique_constraint
      from
        information_schema.key_column_usage as t
      where
        $0
      order by
        t.constraint_name,
        t.table_name,
        t.ordinal_position
    )",
                                       filter);

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // PG dialect doesn't store information schema key column usage in
    // this table.
    auto expected = std::vector<ValueRow>({});
    auto results = Query(query);
    LogResults(results);
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  } else {
    auto results = QueryWithParams(query, {kUnsupportedTables});
    LogResults(results);
    // clang-format off
    auto expected = std::vector<ValueRow>({
      {"", "INFORMATION_SCHEMA", "PK_CHECK_CONSTRAINTS", "", "INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "CONSTRAINT_CATALOG", 1, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_CHECK_CONSTRAINTS", "", "INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "CONSTRAINT_SCHEMA", 2, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_CHECK_CONSTRAINTS", "", "INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "CONSTRAINT_NAME", 3, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_COLUMNS", "", "INFORMATION_SCHEMA", "COLUMNS", "TABLE_CATALOG", 1, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_COLUMNS", "", "INFORMATION_SCHEMA", "COLUMNS", "TABLE_SCHEMA", 2, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_COLUMNS", "", "INFORMATION_SCHEMA", "COLUMNS", "TABLE_NAME", 3, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_COLUMNS", "", "INFORMATION_SCHEMA", "COLUMNS", "COLUMN_NAME", 4, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_COLUMN_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "TABLE_CATALOG", 1, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_COLUMN_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "TABLE_SCHEMA", 2, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_COLUMN_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "TABLE_NAME", 3, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_COLUMN_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "COLUMN_NAME", 4, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_COLUMN_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "DEPENDENT_COLUMN", 5, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_COLUMN_OPTIONS", "", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "TABLE_CATALOG", 1, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_COLUMN_OPTIONS", "", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "TABLE_SCHEMA", 2, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_COLUMN_OPTIONS", "", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "TABLE_NAME", 3, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_COLUMN_OPTIONS", "", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "COLUMN_NAME", 4, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_COLUMN_OPTIONS", "", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "OPTION_NAME", 5, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_CONSTRAINT_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "CONSTRAINT_CATALOG", 1, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_CONSTRAINT_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "CONSTRAINT_SCHEMA", 2, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_CONSTRAINT_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "CONSTRAINT_NAME", 3, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_CONSTRAINT_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "COLUMN_NAME", 4, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_CONSTRAINT_TABLE_USAGE", "", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "TABLE_CATALOG", 1, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_CONSTRAINT_TABLE_USAGE", "", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "TABLE_SCHEMA", 2, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_CONSTRAINT_TABLE_USAGE", "", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "TABLE_NAME", 3, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_CONSTRAINT_TABLE_USAGE", "", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "CONSTRAINT_CATALOG", 4, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_CONSTRAINT_TABLE_USAGE", "", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "CONSTRAINT_SCHEMA", 5, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_CONSTRAINT_TABLE_USAGE", "", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "CONSTRAINT_NAME", 6, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_DATABASE_OPTIONS", "", "INFORMATION_SCHEMA", "DATABASE_OPTIONS", "CATALOG_NAME", 1, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_DATABASE_OPTIONS", "", "INFORMATION_SCHEMA", "DATABASE_OPTIONS", "SCHEMA_NAME", 2, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_DATABASE_OPTIONS", "", "INFORMATION_SCHEMA", "DATABASE_OPTIONS", "OPTION_NAME", 3, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_INDEXES", "", "INFORMATION_SCHEMA", "INDEXES", "TABLE_CATALOG", 1, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_INDEXES", "", "INFORMATION_SCHEMA", "INDEXES", "TABLE_SCHEMA", 2, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_INDEXES", "", "INFORMATION_SCHEMA", "INDEXES", "TABLE_NAME", 3, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_INDEXES", "", "INFORMATION_SCHEMA", "INDEXES", "INDEX_NAME", 4, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_INDEXES", "", "INFORMATION_SCHEMA", "INDEXES", "INDEX_TYPE", 5, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_INDEX_COLUMNS", "", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "TABLE_CATALOG", 1, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_INDEX_COLUMNS", "", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "TABLE_SCHEMA", 2, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_INDEX_COLUMNS", "", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "TABLE_NAME", 3, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_INDEX_COLUMNS", "", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "INDEX_NAME", 4, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_INDEX_COLUMNS", "", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "INDEX_TYPE", 5, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_INDEX_COLUMNS", "", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "COLUMN_NAME", 6, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_KEY_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "CONSTRAINT_CATALOG", 1, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_KEY_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "CONSTRAINT_SCHEMA", 2, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_KEY_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "CONSTRAINT_NAME", 3, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_KEY_COLUMN_USAGE", "", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "COLUMN_NAME", 4, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_REFERENTIAL_CONSTRAINTS", "", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "CONSTRAINT_CATALOG", 1, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_REFERENTIAL_CONSTRAINTS", "", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "CONSTRAINT_SCHEMA", 2, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_REFERENTIAL_CONSTRAINTS", "", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "CONSTRAINT_NAME", 3, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_SCHEMATA", "", "INFORMATION_SCHEMA", "SCHEMATA", "CATALOG_NAME", 1, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_SCHEMATA", "", "INFORMATION_SCHEMA", "SCHEMATA", "SCHEMA_NAME", 2, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_SPANNER_STATISTICS", "", "INFORMATION_SCHEMA", "SPANNER_STATISTICS", "CATALOG_NAME", 1, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_SPANNER_STATISTICS", "", "INFORMATION_SCHEMA", "SPANNER_STATISTICS", "SCHEMA_NAME", 2, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_SPANNER_STATISTICS", "", "INFORMATION_SCHEMA", "SPANNER_STATISTICS", "PACKAGE_NAME", 3, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_TABLES", "", "INFORMATION_SCHEMA", "TABLES", "TABLE_CATALOG", 1, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_TABLES", "", "INFORMATION_SCHEMA", "TABLES", "TABLE_SCHEMA", 2, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_TABLES", "", "INFORMATION_SCHEMA", "TABLES", "TABLE_NAME", 3, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_TABLE_CONSTRAINTS", "", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "CONSTRAINT_CATALOG", 1, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_TABLE_CONSTRAINTS", "", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "CONSTRAINT_SCHEMA", 2, Ni()},  // NOLINT
      {"", "INFORMATION_SCHEMA", "PK_TABLE_CONSTRAINTS", "", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "CONSTRAINT_NAME", 3, Ni()},  // NOLINT
    });
    // clang-format on
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  }
}

TEST_P(InformationSchemaTest, MetaConstraintColumnUsage) {
  std::string filter;
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    filter = R"(
        t.table_schema = 'information_schema'
    )";
  } else {
    filter = R"(
        t.table_catalog = ''
        and t.table_schema = 'INFORMATION_SCHEMA'
        and t.table_name not in unnest(@unsupported_tables)
        and t.constraint_name not in unnest(@unsupported_constraints)
    )";
  }

  std::string query = absl::Substitute(R"(
      select
        t.table_catalog,
        t.table_schema,
        t.table_name,
        t.column_name,
        t.constraint_catalog,
        t.constraint_schema,
        t.constraint_name
      from
        information_schema.constraint_column_usage as t
      where
        $0
      order by
        t.table_name,
        t.column_name,
        t.constraint_name
    )",
                                       filter);

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // PG dialect doesn't store information schema constraint column usage in
    // this table.
    auto expected = std::vector<ValueRow>({});
    auto results = Query(query);
    LogResults(results);
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  } else {
    auto results =
        QueryWithParams(query, {kUnsupportedTables, kUnsupportedConstraints});
    LogResults(results);
    // clang-format off
    auto expected = std::vector<ValueRow>({
      {"", "INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "CHECK_CLAUSE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CHECK_CONSTRAINTS_CHECK_CLAUSE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "CONSTRAINT_CATALOG", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CHECK_CONSTRAINTS_CONSTRAINT_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "CONSTRAINT_CATALOG", "", "INFORMATION_SCHEMA", "PK_CHECK_CONSTRAINTS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "CONSTRAINT_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CHECK_CONSTRAINTS_CONSTRAINT_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "CONSTRAINT_NAME", "", "INFORMATION_SCHEMA", "PK_CHECK_CONSTRAINTS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "CONSTRAINT_SCHEMA", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CHECK_CONSTRAINTS_CONSTRAINT_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "CONSTRAINT_SCHEMA", "", "INFORMATION_SCHEMA", "PK_CHECK_CONSTRAINTS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CHECK_CONSTRAINTS", "SPANNER_STATE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CHECK_CONSTRAINTS_SPANNER_STATE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMNS", "COLUMN_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMNS_COLUMN_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMNS", "COLUMN_NAME", "", "INFORMATION_SCHEMA", "PK_COLUMNS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMNS", "IS_GENERATED", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMNS_IS_GENERATED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMNS", "ORDINAL_POSITION", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMNS_ORDINAL_POSITION"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMNS", "TABLE_CATALOG", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMNS_TABLE_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMNS", "TABLE_CATALOG", "", "INFORMATION_SCHEMA", "PK_COLUMNS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMNS", "TABLE_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMNS_TABLE_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMNS", "TABLE_NAME", "", "INFORMATION_SCHEMA", "PK_COLUMNS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMNS", "TABLE_SCHEMA", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMNS_TABLE_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMNS", "TABLE_SCHEMA", "", "INFORMATION_SCHEMA", "PK_COLUMNS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "COLUMN_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_COLUMN_USAGE_COLUMN_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "COLUMN_NAME", "", "INFORMATION_SCHEMA", "PK_COLUMN_COLUMN_USAGE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "DEPENDENT_COLUMN", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_COLUMN_USAGE_DEPENDENT_COLUMN"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "DEPENDENT_COLUMN", "", "INFORMATION_SCHEMA", "PK_COLUMN_COLUMN_USAGE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "TABLE_CATALOG", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_COLUMN_USAGE_TABLE_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "TABLE_CATALOG", "", "INFORMATION_SCHEMA", "PK_COLUMN_COLUMN_USAGE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "TABLE_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_COLUMN_USAGE_TABLE_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "TABLE_NAME", "", "INFORMATION_SCHEMA", "PK_COLUMN_COLUMN_USAGE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "TABLE_SCHEMA", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_COLUMN_USAGE_TABLE_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_COLUMN_USAGE", "TABLE_SCHEMA", "", "INFORMATION_SCHEMA", "PK_COLUMN_COLUMN_USAGE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "COLUMN_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_OPTIONS_COLUMN_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "COLUMN_NAME", "", "INFORMATION_SCHEMA", "PK_COLUMN_OPTIONS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "OPTION_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_OPTIONS_OPTION_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "OPTION_NAME", "", "INFORMATION_SCHEMA", "PK_COLUMN_OPTIONS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "OPTION_TYPE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_OPTIONS_OPTION_TYPE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "OPTION_VALUE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_OPTIONS_OPTION_VALUE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "TABLE_CATALOG", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_OPTIONS_TABLE_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "TABLE_CATALOG", "", "INFORMATION_SCHEMA", "PK_COLUMN_OPTIONS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "TABLE_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_OPTIONS_TABLE_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "TABLE_NAME", "", "INFORMATION_SCHEMA", "PK_COLUMN_OPTIONS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "TABLE_SCHEMA", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_COLUMN_OPTIONS_TABLE_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "TABLE_SCHEMA", "", "INFORMATION_SCHEMA", "PK_COLUMN_OPTIONS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "COLUMN_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_COLUMN_USAGE_COLUMN_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "COLUMN_NAME", "", "INFORMATION_SCHEMA", "PK_CONSTRAINT_COLUMN_USAGE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "CONSTRAINT_CATALOG", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_COLUMN_USAGE_CONSTRAINT_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "CONSTRAINT_CATALOG", "", "INFORMATION_SCHEMA", "PK_CONSTRAINT_COLUMN_USAGE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "CONSTRAINT_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_COLUMN_USAGE_CONSTRAINT_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "CONSTRAINT_NAME", "", "INFORMATION_SCHEMA", "PK_CONSTRAINT_COLUMN_USAGE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "CONSTRAINT_SCHEMA", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_COLUMN_USAGE_CONSTRAINT_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "CONSTRAINT_SCHEMA", "", "INFORMATION_SCHEMA", "PK_CONSTRAINT_COLUMN_USAGE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "TABLE_CATALOG", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_COLUMN_USAGE_TABLE_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "TABLE_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_COLUMN_USAGE_TABLE_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "TABLE_SCHEMA", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_COLUMN_USAGE_TABLE_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "CONSTRAINT_CATALOG", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_TABLE_USAGE_CONSTRAINT_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "CONSTRAINT_CATALOG", "", "INFORMATION_SCHEMA", "PK_CONSTRAINT_TABLE_USAGE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "CONSTRAINT_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_TABLE_USAGE_CONSTRAINT_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "CONSTRAINT_NAME", "", "INFORMATION_SCHEMA", "PK_CONSTRAINT_TABLE_USAGE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "CONSTRAINT_SCHEMA", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_TABLE_USAGE_CONSTRAINT_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "CONSTRAINT_SCHEMA", "", "INFORMATION_SCHEMA", "PK_CONSTRAINT_TABLE_USAGE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "TABLE_CATALOG", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_TABLE_USAGE_TABLE_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "TABLE_CATALOG", "", "INFORMATION_SCHEMA", "PK_CONSTRAINT_TABLE_USAGE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "TABLE_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_TABLE_USAGE_TABLE_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "TABLE_NAME", "", "INFORMATION_SCHEMA", "PK_CONSTRAINT_TABLE_USAGE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "TABLE_SCHEMA", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_CONSTRAINT_TABLE_USAGE_TABLE_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "TABLE_SCHEMA", "", "INFORMATION_SCHEMA", "PK_CONSTRAINT_TABLE_USAGE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "DATABASE_OPTIONS", "CATALOG_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_DATABASE_OPTIONS_CATALOG_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "DATABASE_OPTIONS", "CATALOG_NAME", "", "INFORMATION_SCHEMA", "PK_DATABASE_OPTIONS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "DATABASE_OPTIONS", "OPTION_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_DATABASE_OPTIONS_OPTION_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "DATABASE_OPTIONS", "OPTION_NAME", "", "INFORMATION_SCHEMA", "PK_DATABASE_OPTIONS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "DATABASE_OPTIONS", "OPTION_TYPE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_DATABASE_OPTIONS_OPTION_TYPE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "DATABASE_OPTIONS", "OPTION_VALUE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_DATABASE_OPTIONS_OPTION_VALUE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "DATABASE_OPTIONS", "SCHEMA_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_DATABASE_OPTIONS_SCHEMA_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "DATABASE_OPTIONS", "SCHEMA_NAME", "", "INFORMATION_SCHEMA", "PK_DATABASE_OPTIONS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEXES", "INDEX_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_INDEX_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEXES", "INDEX_NAME", "", "INFORMATION_SCHEMA", "PK_INDEXES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEXES", "INDEX_STATE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_INDEX_STATE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEXES", "INDEX_TYPE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_INDEX_TYPE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEXES", "INDEX_TYPE", "", "INFORMATION_SCHEMA", "PK_INDEXES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEXES", "IS_NULL_FILTERED", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_IS_NULL_FILTERED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEXES", "IS_UNIQUE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_IS_UNIQUE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEXES", "PARENT_TABLE_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_PARENT_TABLE_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEXES", "SPANNER_IS_MANAGED", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_SPANNER_IS_MANAGED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEXES", "TABLE_CATALOG", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_TABLE_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEXES", "TABLE_CATALOG", "", "INFORMATION_SCHEMA", "PK_INDEXES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEXES", "TABLE_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_TABLE_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEXES", "TABLE_NAME", "", "INFORMATION_SCHEMA", "PK_INDEXES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEXES", "TABLE_SCHEMA", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEXES_TABLE_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEXES", "TABLE_SCHEMA", "", "INFORMATION_SCHEMA", "PK_INDEXES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "COLUMN_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEX_COLUMNS_COLUMN_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "COLUMN_NAME", "", "INFORMATION_SCHEMA", "PK_INDEX_COLUMNS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "INDEX_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEX_COLUMNS_INDEX_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "INDEX_NAME", "", "INFORMATION_SCHEMA", "PK_INDEX_COLUMNS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "INDEX_TYPE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEX_COLUMNS_INDEX_TYPE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "INDEX_TYPE", "", "INFORMATION_SCHEMA", "PK_INDEX_COLUMNS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "TABLE_CATALOG", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEX_COLUMNS_TABLE_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "TABLE_CATALOG", "", "INFORMATION_SCHEMA", "PK_INDEX_COLUMNS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "TABLE_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEX_COLUMNS_TABLE_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "TABLE_NAME", "", "INFORMATION_SCHEMA", "PK_INDEX_COLUMNS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "TABLE_SCHEMA", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_INDEX_COLUMNS_TABLE_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "TABLE_SCHEMA", "", "INFORMATION_SCHEMA", "PK_INDEX_COLUMNS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "COLUMN_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_COLUMN_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "COLUMN_NAME", "", "INFORMATION_SCHEMA", "PK_KEY_COLUMN_USAGE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "CONSTRAINT_CATALOG", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_CONSTRAINT_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "CONSTRAINT_CATALOG", "", "INFORMATION_SCHEMA", "PK_KEY_COLUMN_USAGE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "CONSTRAINT_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_CONSTRAINT_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "CONSTRAINT_NAME", "", "INFORMATION_SCHEMA", "PK_KEY_COLUMN_USAGE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "CONSTRAINT_SCHEMA", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_CONSTRAINT_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "CONSTRAINT_SCHEMA", "", "INFORMATION_SCHEMA", "PK_KEY_COLUMN_USAGE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "ORDINAL_POSITION", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_ORDINAL_POSITION"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "TABLE_CATALOG", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_TABLE_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "TABLE_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_TABLE_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "TABLE_SCHEMA", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_KEY_COLUMN_USAGE_TABLE_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "CONSTRAINT_CATALOG", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_REFERENTIAL_CONSTRAINTS_CONSTRAINT_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "CONSTRAINT_CATALOG", "", "INFORMATION_SCHEMA", "PK_REFERENTIAL_CONSTRAINTS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "CONSTRAINT_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_REFERENTIAL_CONSTRAINTS_CONSTRAINT_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "CONSTRAINT_NAME", "", "INFORMATION_SCHEMA", "PK_REFERENTIAL_CONSTRAINTS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "CONSTRAINT_SCHEMA", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_REFERENTIAL_CONSTRAINTS_CONSTRAINT_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "CONSTRAINT_SCHEMA", "", "INFORMATION_SCHEMA", "PK_REFERENTIAL_CONSTRAINTS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "DELETE_RULE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_REFERENTIAL_CONSTRAINTS_DELETE_RULE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "MATCH_OPTION", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_REFERENTIAL_CONSTRAINTS_MATCH_OPTION"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "SPANNER_STATE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_REFERENTIAL_CONSTRAINTS_SPANNER_STATE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "UPDATE_RULE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_REFERENTIAL_CONSTRAINTS_UPDATE_RULE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "SCHEMATA", "CATALOG_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_SCHEMATA_CATALOG_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "SCHEMATA", "CATALOG_NAME", "", "INFORMATION_SCHEMA", "PK_SCHEMATA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "SCHEMATA", "SCHEMA_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_SCHEMATA_SCHEMA_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "SCHEMATA", "SCHEMA_NAME", "", "INFORMATION_SCHEMA", "PK_SCHEMATA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "SPANNER_STATISTICS", "ALLOW_GC", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_SPANNER_STATISTICS_ALLOW_GC"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "SPANNER_STATISTICS", "CATALOG_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_SPANNER_STATISTICS_CATALOG_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "SPANNER_STATISTICS", "CATALOG_NAME", "", "INFORMATION_SCHEMA", "PK_SPANNER_STATISTICS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "SPANNER_STATISTICS", "PACKAGE_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_SPANNER_STATISTICS_PACKAGE_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "SPANNER_STATISTICS", "PACKAGE_NAME", "", "INFORMATION_SCHEMA", "PK_SPANNER_STATISTICS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "SPANNER_STATISTICS", "SCHEMA_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_SPANNER_STATISTICS_SCHEMA_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "SPANNER_STATISTICS", "SCHEMA_NAME", "", "INFORMATION_SCHEMA", "PK_SPANNER_STATISTICS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLES", "TABLE_CATALOG", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLES_TABLE_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLES", "TABLE_CATALOG", "", "INFORMATION_SCHEMA", "PK_TABLES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLES", "TABLE_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLES_TABLE_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLES", "TABLE_NAME", "", "INFORMATION_SCHEMA", "PK_TABLES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLES", "TABLE_SCHEMA", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLES_TABLE_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLES", "TABLE_SCHEMA", "", "INFORMATION_SCHEMA", "PK_TABLES"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "CONSTRAINT_CATALOG", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_CONSTRAINT_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "CONSTRAINT_CATALOG", "", "INFORMATION_SCHEMA", "PK_TABLE_CONSTRAINTS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "CONSTRAINT_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_CONSTRAINT_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "CONSTRAINT_NAME", "", "INFORMATION_SCHEMA", "PK_TABLE_CONSTRAINTS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "CONSTRAINT_SCHEMA", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_CONSTRAINT_SCHEMA"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "CONSTRAINT_SCHEMA", "", "INFORMATION_SCHEMA", "PK_TABLE_CONSTRAINTS"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "CONSTRAINT_TYPE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_CONSTRAINT_TYPE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "ENFORCED", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_ENFORCED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "INITIALLY_DEFERRED", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_INITIALLY_DEFERRED"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "IS_DEFERRABLE", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_IS_DEFERRABLE"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "TABLE_CATALOG", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_TABLE_CATALOG"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "TABLE_NAME", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_TABLE_NAME"},  // NOLINT
      {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "TABLE_SCHEMA", "", "INFORMATION_SCHEMA", "CK_IS_NOT_NULL_TABLE_CONSTRAINTS_TABLE_SCHEMA"},  // NOLINT
    });
    // clang-format off
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  }
}

TEST_P(InformationSchemaTest, DefaultTables) {
  // GSQL uses an empty string for the default schema and PG doesn't.
  std::string filter = "";
  if (GetParam() == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    filter = "t.table_catalog = '' and ";
  }
  auto results = Query(absl::Substitute(
      R"(
        select
          t.table_type,
          t.table_name,
          t.parent_table_name,
          t.spanner_state,
          t.on_delete_action,
          t.row_deletion_policy_expression
        from
          information_schema.tables AS t
        where
          $0
          t.table_schema = '$1'
        order by
          t.table_catalog,
          t.table_schema,
          t.table_name
      )",
      filter,
      (GetParam() == database_api::DatabaseDialect::POSTGRESQL ? "public" :
       "")));
  LogResults(results);
  std::string expected_interval =
      (GetParam() == database_api::DatabaseDialect::POSTGRESQL)
      ?  "INTERVAL '7 DAYS' ON created_at"
      : "OLDER_THAN(created_at, INTERVAL 7 DAY)";
  // clang-format off
  auto expected = std::vector<ValueRow>({
    {"BASE TABLE", "base", Ns(), "COMMITTED", Ns(), Ns()},
    {"VIEW", "base_view", Ns(), Ns(), Ns(), Ns()},
    {"BASE TABLE", "cascade_child", "base", "COMMITTED", "CASCADE", Ns()}, // NOLINT
    {"BASE TABLE", "no_action_child", "base", "COMMITTED", "NO ACTION", Ns()},
    {"BASE TABLE", "row_deletion_policy", Ns(), "COMMITTED", Ns(), expected_interval}, // NOLINT
  });
  // clang-format on
  EXPECT_THAT(results, IsOkAndHoldsRows(expected));
}

TEST_P(InformationSchemaTest, GSQLDefaultColumns) {
  // Since the query and output for the PG dialect is significantly different,
  // we test this logic in a separate function just for the GSQL dialect.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }

  auto results = Query(R"(
      select
        t.table_catalog,
        t.table_schema,
        t.table_name,
        t.column_name,
        t.ordinal_position,
        t.column_default,
        t.data_type,
        t.is_nullable,
        t.spanner_type,
        t.is_generated,
        t.generation_expression,
        t.is_stored,
        t.spanner_state
      from
        information_schema.columns AS t
      where
        t.table_catalog = ''
        and t.table_schema = ''
      order by
        t.table_name,
        t.ordinal_position
    )");
  LogResults(results);
  // clang-format off
  auto expected = std::vector<ValueRow>({
    {"", "", "base", "key1", 1, Ns(), Ns(), "YES", "INT64", "NEVER", Ns(), Ns(), "COMMITTED"},  // NOLINT
    {"", "", "base", "key2", 2, Ns(), Ns(), "YES", "STRING(256)", "NEVER", Ns(), Ns(), "COMMITTED"},  // NOLINT
    {"", "", "base", "bool_value", 3, Ns(), Ns(), "YES", "BOOL", "NEVER", Ns(), Ns(), "COMMITTED"},  // NOLINT
    {"", "", "base", "int_value", 4, Ns(), Ns(), "NO", "INT64", "NEVER", Ns(), Ns(), "COMMITTED"},  // NOLINT
    {"", "", "base", "double_value", 5, Ns(), Ns(), "YES", "FLOAT64", "NEVER", Ns(), Ns(), "COMMITTED"},  // NOLINT
    {"", "", "base", "str_value", 6, Ns(), Ns(), "YES", "STRING(MAX)", "NEVER", Ns(), Ns(), "COMMITTED"},  // NOLINT
    {"", "", "base", "byte_value", 7, Ns(), Ns(), "YES", "BYTES(256)", "NEVER", Ns(), Ns(), "COMMITTED"},  // NOLINT
    {"", "", "base", "timestamp_value", 8, Ns(), Ns(), "YES", "TIMESTAMP", "NEVER", Ns(), Ns(), "COMMITTED"},  // NOLINT
    {"", "", "base", "date_value", 9, Ns(), Ns(), "YES", "DATE", "NEVER", Ns(), Ns(), "COMMITTED"},  // NOLINT
    {"", "", "base", "bool_array", 10, Ns(), Ns(), "NO", "ARRAY<BOOL>", "NEVER", Ns(), Ns(), "COMMITTED"},  // NOLINT
    {"", "", "base", "int_array", 11, Ns(), Ns(), "YES", "ARRAY<INT64>", "NEVER", Ns(), Ns(), "COMMITTED"},  // NOLINT
    {"", "", "base", "double_array", 12, Ns(), Ns(), "YES", "ARRAY<FLOAT64>", "NEVER", Ns(), Ns(), "COMMITTED"},  // NOLINT
    {"", "", "base", "str_array", 13, Ns(), Ns(), "YES", "ARRAY<STRING(256)>", "NEVER", Ns(), Ns(), "COMMITTED"},  // NOLINT
    {"", "", "base", "byte_array", 14, Ns(), Ns(), "YES", "ARRAY<BYTES(MAX)>", "NEVER", Ns(), Ns(), "COMMITTED"},  // NOLINT
    {"", "", "base", "timestamp_array", 15, Ns(), Ns(), "YES", "ARRAY<TIMESTAMP>", "NEVER", Ns(), Ns(), "COMMITTED"},  // NOLINT
    {"", "", "base", "date_array", 16, Ns(), Ns(), "YES", "ARRAY<DATE>", "NEVER", Ns(), Ns(), "COMMITTED"},  // NOLINT
    {"", "", "base", "gen_value", 17, Ns(), Ns(), "YES", "INT64", "ALWAYS", "key1 + 1", "YES", "COMMITTED"},  // NOLINT
    {"", "", "base", "gen_function_value", 18, Ns(), Ns(), "YES", "INT64", "ALWAYS", "LENGTH(key2)", "YES", "COMMITTED"},  // NOLINT
    {"", "", "base", "default_col_value", 19, "100", Ns(), "YES", "INT64", "NEVER", Ns(), Ns(), "COMMITTED"},  // NOLINT
    {"", "", "base", "default_timestamp_col_value", 20, "CURRENT_TIMESTAMP()", Ns(), "YES", "TIMESTAMP", "NEVER", Ns(), Ns(), "COMMITTED"},  // NOLINT
    {"", "", "base_view", "key1", 1, Ns(), Ns(), "YES", "INT64", "NEVER", Ns(), Ns(), "COMMITTED"},  // NOLINT
    {"", "", "cascade_child", "key1", 1, Ns(), Ns(), "YES", "INT64", "NEVER", Ns(), Ns(), "COMMITTED"},  // NOLINT
    {"", "", "cascade_child", "key2", 2, Ns(), Ns(), "YES", "STRING(256)", "NEVER", Ns(), Ns(), "COMMITTED"},  // NOLINT
    {"", "", "cascade_child", "child_key", 3, Ns(), Ns(), "YES", "BOOL", "NEVER", Ns(), Ns(), "COMMITTED"},  // NOLINT
    {"", "", "cascade_child", "value1", 4, Ns(), Ns(), "NO", "STRING(MAX)", "NEVER", Ns(), Ns(), "COMMITTED"},  // NOLINT
    {"", "", "cascade_child", "value2", 5, Ns(), Ns(), "YES", "BOOL", "NEVER", Ns(), Ns(), "COMMITTED"},  // NOLINT
    {"", "", "cascade_child", "created_at", 6, Ns(), Ns(), "YES", "TIMESTAMP", "NEVER", Ns(), Ns(), "COMMITTED"}, // NOLINT
    {"", "", "no_action_child", "key1", 1, Ns(), Ns(), "YES", "INT64", "NEVER", Ns(), Ns(), "COMMITTED"},  // NOLINT
    {"", "", "no_action_child", "key2", 2, Ns(), Ns(), "YES", "STRING(256)", "NEVER", Ns(), Ns(), "COMMITTED"},  // NOLINT
    {"", "", "no_action_child", "child_key", 3, Ns(), Ns(), "YES", "BOOL", "NEVER", Ns(), Ns(), "COMMITTED"},  // NOLINT
    {"", "", "no_action_child", "value", 4, Ns(), Ns(), "YES", "STRING(MAX)", "NEVER", Ns(), Ns(), "COMMITTED"},  // NOLINT
    {"", "", "row_deletion_policy", "key", 1, Ns(), Ns(), "YES", "INT64", "NEVER", Ns(), Ns(), "COMMITTED"},  // NOLINT
    {"", "", "row_deletion_policy", "created_at", 2, Ns(), Ns(), "YES", "TIMESTAMP", "NEVER", Ns(), Ns(), "COMMITTED"},  // NOLINT
  });
  // clang-format on
  EXPECT_THAT(results, IsOkAndHoldsRows(expected));
}

TEST_P(InformationSchemaTest, PGDefaultColumns) {
  // Since the query and output for the PG dialect is significantly different,
  // we test this logic in a separate function for the PG dialect.
  if (GetParam() == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    GTEST_SKIP();
  }

  auto results = Query(R"(
      select
        t.table_schema,
        t.table_name,
        t.column_name,
        t.ordinal_position,
        t.column_default,
        t.data_type,
        t.is_nullable,
        t.spanner_type,
        t.is_generated,
        t.generation_expression,
        t.is_stored,
        t.spanner_state,
        t.character_maximum_length,
        t.numeric_precision,
        t.numeric_precision_radix,
        t.numeric_scale
      from
        information_schema.columns AS t
      where
        t.table_schema = 'public'
        and t.table_name = any (array['base', 'base_view'])
      order by
        t.table_name,
        t.ordinal_position
    )");
  LogResults(results);
  // clang-format off
  auto expected = std::vector<ValueRow>({
    {"public", "base", "key1", 1, Ns(), "bigint", "NO", "bigint", "NEVER", Ns(), Ns(), "COMMITTED", Ni(), 64, 2, 0},  // NOLINT
    {"public", "base", "key2", 2, Ns(), "character varying", "NO", "character varying(256)", "NEVER", Ns(), Ns(), "COMMITTED", 256, Ni(), Ni(), Ni()},  // NOLINT
    {"public", "base", "bool_value", 3, Ns(), "boolean", "YES", "boolean", "NEVER", Ns(), Ns(), "COMMITTED", Ni(), Ni(), Ni(), Ni()},  // NOLINT
    {"public", "base", "int_value", 4, Ns(), "bigint", "NO", "bigint", "NEVER", Ns(), Ns(), "COMMITTED", Ni(), 64, 2, 0},  // NOLINT
    {"public", "base", "double_value", 5, Ns(), "double precision", "YES", "double precision", "NEVER", Ns(), Ns(), "COMMITTED", Ni(), 53, 2, Ni()},  // NOLINT
    {"public", "base", "str_value", 6, Ns(), "character varying", "YES", "character varying", "NEVER", Ns(), Ns(), "COMMITTED", Ni(), Ni(), Ni(), Ni()},  // NOLINT
    {"public", "base", "byte_value", 7, Ns(), "bytea", "YES", "bytea", "NEVER", Ns(), Ns(), "COMMITTED", Ni(), Ni(), Ni(), Ni()},  // NOLINT
    {"public", "base", "timestamp_value", 8, Ns(), "spanner.commit_timestamp", "YES", "spanner.commit_timestamp", "NEVER", Ns(), Ns(), "COMMITTED", Ni(), Ni(), Ni(), Ni()},  // NOLINT
    {"public", "base", "date_value", 9, Ns(), "date", "YES", "date", "NEVER", Ns(), Ns(), "COMMITTED", Ni(), Ni(), Ni(), Ni()},  // NOLINT
    {"public", "base", "bool_array", 10, Ns(), "ARRAY", "NO", "boolean[]", "NEVER", Ns(), Ns(), "COMMITTED", Ni(), Ni(), Ni(), Ni()},  // NOLINT
    {"public", "base", "int_array", 11, Ns(), "ARRAY", "YES", "bigint[]", "NEVER", Ns(), Ns(), "COMMITTED", Ni(), Ni(), Ni(), Ni()},  // NOLINT
    {"public", "base", "double_array", 12, Ns(), "ARRAY", "YES", "double precision[]", "NEVER", Ns(), Ns(), "COMMITTED", Ni(), Ni(), Ni(), Ni()},  // NOLINT
    {"public", "base", "str_array", 13, Ns(), "ARRAY", "YES", "character varying[]", "NEVER", Ns(), Ns(), "COMMITTED", Ni(), Ni(), Ni(), Ni()},  // NOLINT
    {"public", "base", "byte_array", 14, Ns(), "ARRAY", "YES", "bytea[]", "NEVER", Ns(), Ns(), "COMMITTED", Ni(), Ni(), Ni(), Ni()},  // NOLINT
    {"public", "base", "timestamp_array", 15, Ns(), "ARRAY", "YES", "timestamp with time zone[]", "NEVER", Ns(), Ns(), "COMMITTED", Ni(), Ni(), Ni(), Ni()},  // NOLINT
    {"public", "base", "date_array", 16, Ns(), "ARRAY", "YES", "date[]", "NEVER", Ns(), Ns(), "COMMITTED", Ni(), Ni(), Ni(), Ni()},  // NOLINT
    {"public", "base", "gen_value", 17, Ns(), "bigint", "YES", "bigint", "ALWAYS", "(key1 + '1'::bigint)", "YES", "COMMITTED", Ni(), 64, 2, 0},  // NOLINT
    {"public", "base", "gen_function_value", 18, Ns(), "bigint", "YES", "bigint", "ALWAYS", "length(key2)", "YES", "COMMITTED", Ni(), 64, 2, 0},  // NOLINT
    {"public", "base", "default_col_value", 19, "'100'::bigint", "bigint", "YES", "bigint", "NEVER", Ns(), Ns(), "COMMITTED", Ni(), 64, 2, 0},  // NOLINT
    {"public", "base", "default_timestamp_col_value", 20, "CURRENT_TIMESTAMP", "timestamp with time zone", "YES", "timestamp with time zone", "NEVER", Ns(), Ns(), "COMMITTED", Ni(), Ni(), Ni(), Ni()},  // NOLINT
    {"public", "base_view", "key1", 1, Ns(), "bigint", "YES", "bigint", "NEVER", Ns(), Ns(), "COMMITTED", Ni(), 64, 2, 0},  // NOLINT
  });
  // clang-format on
  EXPECT_THAT(results, IsOkAndHoldsRows(expected));
}

TEST_P(InformationSchemaTest, DefaultIndexes) {
  // GSQL uses an empty string for the default schema and PG doesn't.
  std::string filter = "";
  if (GetParam() == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    filter = "t.table_catalog = '' and ";
  }

  auto results = Query(absl::Substitute(
      R"(
      select
        t.table_catalog,
        t.table_schema,
        t.table_name,
        t.index_name,
        t.index_type,
        t.parent_table_name,
        t.is_unique,
        t.is_null_filtered,
        t.index_state,
        t.spanner_is_managed
      from
        information_schema.indexes AS t
      where
        $0
        t.table_schema = '$1'
      order by
        t.table_name,
        t.index_name
    )",
      filter,
      (GetParam() == database_api::DatabaseDialect::POSTGRESQL ? "public"
                                                               : "")));
  LogResults(results);
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // clang-format off
    auto expected = ExpectedRows(StripFirstColumnFromRows(*results), {
      {"public", "base", "IDX_base_bool_value_key2_N_\\w{16}", "INDEX", "", "NO", "YES", "READ_WRITE", "YES"},  // NOLINT
      {"public", "base", "PRIMARY_KEY", "PRIMARY_KEY", "", "YES", "NO", Ns(), "NO"},  // NOLINT
      {"public", "cascade_child", "IDX_cascade_child_child_key_value1_U_\\w{16}", "INDEX", "", "YES", "NO", "READ_WRITE", "YES"},  // NOLINT
      {"public", "cascade_child", "PRIMARY_KEY", "PRIMARY_KEY", "", "YES", "NO", Ns(), "NO"},  // NOLINT
      {"public", "cascade_child", "cascade_child_by_value", "INDEX", "base", "YES", "NO", "READ_WRITE", "NO"},  // NOLINT
      {"public", "no_action_child", "PRIMARY_KEY", "PRIMARY_KEY", "", "YES", "NO", Ns(), "NO"},  // NOLINT
      {"public", "no_action_child", "no_action_child_by_value", "INDEX", "", "NO", "NO", "READ_WRITE", "NO"},  // NOLINT
      {"public", "row_deletion_policy", "PRIMARY_KEY", "PRIMARY_KEY", "", "YES", "NO", Ns(), "NO"},  // NOLINT
    });
    // clang-format on
    // Remove the table_catalog column from the expected results since we don't
    // currently set that to its correct value for PG.
    EXPECT_THAT(StripFirstColumnFromRows(*results), expected);
  } else {
    // clang-format off
    auto expected = ExpectedRows(results, {
      {"", "", "base", "IDX_base_bool_value_key2_N_\\w{16}", "INDEX", "", false, true, "READ_WRITE", true},  // NOLINT
      {"", "", "base", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, Ns(), false},  // NOLINT
      {"", "", "cascade_child", "IDX_cascade_child_child_key_value1_U_\\w{16}", "INDEX", "", true, true, "READ_WRITE", true},  // NOLINT
      {"", "", "cascade_child", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, Ns(), false},  // NOLINT
      {"", "", "cascade_child", "cascade_child_by_value", "INDEX", "base", true, true, "READ_WRITE", false},  // NOLINT
      {"", "", "no_action_child", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, Ns(), false},  // NOLINT
      {"", "", "no_action_child", "no_action_child_by_value", "INDEX", "", false, false, "READ_WRITE", false},  // NOLINT
      {"", "", "row_deletion_policy", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, Ns(), false},  // NOLINT
    });
    // clang-format on
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  }
}

TEST_P(InformationSchemaTest, DefaultIndexColumns) {
  // GSQL uses an empty string for the default schema and PG doesn't.
  std::string filter = "";
  if (GetParam() == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    filter = "t.table_catalog = '' and ";
  } else {
    // TODO: value2 in cascade_child_by_value is a
    // null-filtered-column by WHERE syntax. This feature is not supported yet
    // in the emulator for either dialect so the emulator thinks value2 is
    // nullable when it's not. So we have to filter out this row until the
    // feature is supported. PG doesn't support the NULL_FILTERED clause for us
    // to use as an alternative, which is supported in GSQL.
    filter = "t.column_name <> 'value2' and ";
  }

  auto results = Query(absl::Substitute(
      R"(
      select
        t.table_catalog,
        t.table_schema,
        t.table_name,
        t.index_name,
        t.column_name,
        t.ordinal_position,
        t.column_ordering,
        t.is_nullable,
        t.spanner_type
      from
        information_schema.index_columns AS t
      where
        $0
        t.table_schema = '$1'
      order by
        t.table_name,
        t.index_name,
        t.ordinal_position
    )",
      filter,
      (GetParam() == database_api::DatabaseDialect::POSTGRESQL ? "public"
                                                               : "")));
  LogResults(results);
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // clang-format off
    auto expected = ExpectedRows(StripFirstColumnFromRows(*results), {
      {"public", "base", "IDX_base_bool_value_key2_N_\\w{16}", "bool_value", 1, "ASC NULLS FIRST", "NO", "boolean"},  // NOLINT
      {"public", "base", "IDX_base_bool_value_key2_N_\\w{16}", "key2", 2, "ASC", "NO", "character varying"},  // NOLINT
      {"public", "base", "PRIMARY_KEY", "key1", 1, "ASC", "NO", "bigint"},  // NOLINT
      {"public", "base", "PRIMARY_KEY", "key2", 2, "ASC", "NO", "character varying"},  // NOLINT
      {"public", "cascade_child", "IDX_cascade_child_child_key_value1_U_\\w{16}", "child_key", 1, "ASC", "NO", "boolean"},  // NOLINT
      {"public", "cascade_child", "IDX_cascade_child_child_key_value1_U_\\w{16}", "value1", 2, "ASC NULLS FIRST", "NO", "character varying"},  // NOLINT
      {"public", "cascade_child", "PRIMARY_KEY", "key1", 1, "ASC", "NO", "bigint"},  // NOLINT
      {"public", "cascade_child", "PRIMARY_KEY", "key2", 2, "ASC", "NO", "character varying"},  // NOLINT
      {"public", "cascade_child", "PRIMARY_KEY", "child_key", 3, "ASC", "NO", "boolean"},  // NOLINT
      {"public", "cascade_child", "cascade_child_by_value", "key1", 1, "ASC", "NO", "bigint"},  // NOLINT
      {"public", "cascade_child", "cascade_child_by_value", "key2", 2, "ASC", "NO", "character varying"},  // NOLINT
      {"public", "cascade_child", "cascade_child_by_value", "value1", Ni(), Ns(), "NO", "character varying"},  // NOLINT
      {"public", "no_action_child", "PRIMARY_KEY", "key1", 1, "ASC", "NO", "bigint"},  // NOLINT
      {"public", "no_action_child", "PRIMARY_KEY", "key2", 2, "ASC", "NO", "character varying"},  // NOLINT
      {"public", "no_action_child", "PRIMARY_KEY", "child_key", 3, "ASC", "NO", "boolean"},  // NOLINT
      {"public", "no_action_child", "no_action_child_by_value", "value", 1, "ASC", "YES", "character varying"},  // NOLINT
      {"public", "row_deletion_policy", "PRIMARY_KEY", "key", 1, "ASC", "NO", "bigint"},  // NOLINT
    });
    // clang-format on
    // Remove the table_catalog column from the expected results since we don't
    // currently set that to its correct value for PG.
    EXPECT_THAT(StripFirstColumnFromRows(*results), expected);
  } else {
    // clang-format off
    auto expected = ExpectedRows(results, {
      {"", "", "base", "IDX_base_bool_value_key2_N_\\w{16}", "bool_value", 1, "ASC", "NO", "BOOL"},  // NOLINT
      {"", "", "base", "IDX_base_bool_value_key2_N_\\w{16}", "key2", 2, "DESC", "NO", "STRING(256)"},  // NOLINT
      {"", "", "base", "PRIMARY_KEY", "key1", 1, "ASC", "YES", "INT64"},  // NOLINT
      {"", "", "base", "PRIMARY_KEY", "key2", 2, "DESC", "YES", "STRING(256)"},  // NOLINT
      {"", "", "cascade_child", "IDX_cascade_child_child_key_value1_U_\\w{16}", "child_key", 1, "ASC", "NO", "BOOL"},  // NOLINT
      {"", "", "cascade_child", "IDX_cascade_child_child_key_value1_U_\\w{16}", "value1", 2, "ASC", "NO", "STRING(MAX)"},  // NOLINT
      {"", "", "cascade_child", "PRIMARY_KEY", "key1", 1, "ASC", "YES", "INT64"},  // NOLINT
      {"", "", "cascade_child", "PRIMARY_KEY", "key2", 2, "DESC", "YES", "STRING(256)"},  // NOLINT
      {"", "", "cascade_child", "PRIMARY_KEY", "child_key", 3, "ASC", "YES", "BOOL"},  // NOLINT
      {"", "", "cascade_child", "cascade_child_by_value", "value1", Ni(), Ns(), "NO", "STRING(MAX)"},  // NOLINT
      {"", "", "cascade_child", "cascade_child_by_value", "key1", 1, "ASC", "NO", "INT64"},  // NOLINT
      {"", "", "cascade_child", "cascade_child_by_value", "key2", 2, "DESC", "NO", "STRING(256)"},  // NOLINT
      {"", "", "cascade_child", "cascade_child_by_value", "value2", 3, "ASC", "NO", "BOOL"},  // NOLINT
      {"", "", "no_action_child", "PRIMARY_KEY", "key1", 1, "ASC", "YES", "INT64"},  // NOLINT
      {"", "", "no_action_child", "PRIMARY_KEY", "key2", 2, "DESC", "YES", "STRING(256)"},  // NOLINT
      {"", "", "no_action_child", "PRIMARY_KEY", "child_key", 3, "ASC", "YES", "BOOL"},  // NOLINT
      {"", "", "no_action_child", "no_action_child_by_value", "value", 1, "ASC", "YES", "STRING(MAX)"},  // NOLINT
      {"", "", "row_deletion_policy", "PRIMARY_KEY", "key", 1, "ASC", "YES", "INT64"},  // NOLINT
    });
    // clang-format on
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  }
}

TEST_P(InformationSchemaTest, DefaultDatabaseOptions) {
  auto results = Query(R"(
      select
        t.option_name,
        t.option_type,
        t.option_value
      from
        information_schema.database_options AS t
      where
        t.option_name = 'database_dialect'
      order by
        t.option_name
    )");
  LogResults(results);
  std::string type = (GetParam() == database_api::DatabaseDialect::POSTGRESQL)
                         ? "character varying"
                         : "STRING";
  // clang-format off
  auto expected = std::vector<ValueRow>({
    {"database_dialect", type, database_api::DatabaseDialect_Name(GetParam())},  // NOLINT
  });
  // clang-format on
  EXPECT_THAT(results, IsOkAndHoldsRows(expected));
}

TEST_P(InformationSchemaTest, DefaultColumnOptions) {
  // Currently unsupported for PG in the emulator information schema.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }

  auto results = Query(R"(
      select
        t.table_catalog,
        t.table_schema,
        t.table_name,
        t.column_name,
        t.option_name,
        t.option_type,
        t.option_value
      from
        information_schema.column_options AS t
      where
        t.table_catalog = ''
        and t.table_schema = ''
      order by
        t.table_name,
        t.column_name,
        t.option_name
    )");
  LogResults(results);
  // clang-format off
  auto expected = std::vector<ValueRow>({
    {"", "", "base", "timestamp_value", "allow_commit_timestamp", "BOOL", "TRUE"},  // NOLINT
  });
  // clang-format on
  EXPECT_THAT(results, IsOkAndHoldsRows(expected));
}

TEST_P(InformationSchemaTest, DefaultTableConstraints) {
  std::string default_schema = "";
  std::string constraint_catalog_col = "";
  std::string table_catalog_col = "";
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    default_schema = "public";
    // We don't include the catalog columns as the name is different between
    // production and the emulator due to the emulator information schema
    // catalog not having access to the database name.
  } else {
    constraint_catalog_col = "t.constraint_catalog,";
    table_catalog_col = "t.table_catalog,";
  }

  auto results = Query(absl::Substitute(R"(
      select
        $0
        t.constraint_schema,
        t.constraint_name,
        $1
        t.table_schema,
        t.table_name,
        t.constraint_type,
        t.is_deferrable,
        t.initially_deferred,
        t.enforced
      from
        information_schema.table_constraints as t
      where
        t.constraint_schema = '$2'
      order by
        t.constraint_name
  )",
                                        constraint_catalog_col,
                                        table_catalog_col, default_schema));
  LogResults(results);
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // clang-format off
    auto expected = ExpectedRows(results, {
      {"public", "CK_IS_NOT_NULL_base_bool_array", "public", "base", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"public", "CK_IS_NOT_NULL_base_int_value", "public", "base", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"public", "CK_IS_NOT_NULL_base_key1", "public", "base", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"public", "CK_IS_NOT_NULL_base_key2", "public", "base", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"public", "CK_IS_NOT_NULL_cascade_child_child_key", "public", "cascade_child", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"public", "CK_IS_NOT_NULL_cascade_child_key1", "public", "cascade_child", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"public", "CK_IS_NOT_NULL_cascade_child_key2", "public", "cascade_child", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"public", "CK_IS_NOT_NULL_cascade_child_value1", "public", "cascade_child", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"public", "CK_IS_NOT_NULL_no_action_child_child_key", "public", "no_action_child", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"public", "CK_IS_NOT_NULL_no_action_child_key1", "public", "no_action_child", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"public", "CK_IS_NOT_NULL_no_action_child_key2", "public", "no_action_child", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"public", "CK_IS_NOT_NULL_row_deletion_policy_key", "public", "row_deletion_policy", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"public", "CK_base_\\w{16}_1", "public", "base", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"public", "IDX_cascade_child_child_key_value1_U_\\w{16}", "public", "cascade_child", "UNIQUE", "NO", "NO", "YES"},  // NOLINT
      {"public", "PK_base", "public", "base", "PRIMARY KEY", "NO", "NO", "YES"},  // NOLINT
      {"public", "PK_cascade_child", "public", "cascade_child", "PRIMARY KEY", "NO", "NO", "YES"},  // NOLINT
      {"public", "PK_no_action_child", "public", "no_action_child", "PRIMARY KEY", "NO", "NO", "YES"},  // NOLINT
      {"public", "PK_row_deletion_policy", "public", "row_deletion_policy", "PRIMARY KEY", "NO", "NO", "YES"},  // NOLINT
      {"public", "check_constraint_name", "public", "base", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"public", "fk_base_cascade_child", "public", "base", "FOREIGN KEY", "NO", "NO", "YES"},  // NOLINT
    });
    // clang-format on
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  } else {
    // clang-format off
    auto expected = ExpectedRows(results, {
      {"", "", "CK_IS_NOT_NULL_base_bool_array", "", "", "base", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "", "CK_IS_NOT_NULL_base_int_value", "", "", "base", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "", "CK_IS_NOT_NULL_cascade_child_value1", "", "", "cascade_child", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "", "CK_base_\\w{16}_1", "", "", "base", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "", "IDX_cascade_child_child_key_value1_U_\\w{16}", "", "", "cascade_child", "UNIQUE", "NO", "NO", "YES"},  // NOLINT
      {"", "", "PK_base", "", "", "base", "PRIMARY KEY", "NO", "NO", "YES"},  // NOLINT
      {"", "", "PK_cascade_child", "", "", "cascade_child", "PRIMARY KEY", "NO", "NO", "YES"},  // NOLINT
      {"", "", "PK_no_action_child", "", "", "no_action_child", "PRIMARY KEY", "NO", "NO", "YES"},  // NOLINT
      {"", "", "PK_row_deletion_policy", "", "", "row_deletion_policy", "PRIMARY KEY", "NO", "NO", "YES"},  // NOLINT
      {"", "", "check_constraint_name", "", "", "base", "CHECK", "NO", "NO", "YES"},  // NOLINT
      {"", "", "fk_base_cascade_child", "", "", "base", "FOREIGN KEY", "NO", "NO", "YES"},  // NOLINT
    });
    // clang-format on
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  }
}

TEST_P(InformationSchemaTest, DefaultConstraintTableUsage) {
  std::string schema = "";
  std::string table_catalog_col = "";
  std::string constraint_catalog_col = "";
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    schema = "public";
    // We don't include the catalog columns as the name is different between
    // production and the emulator due to the emulator information schema
    // catalog not having access to the database name.
  } else {
    table_catalog_col = "t.table_catalog,";
    constraint_catalog_col = "t.constraint_catalog,";
  }

  auto results = Query(absl::Substitute(R"(
      select
        $0
        t.table_schema,
        t.table_name,
        $1
        t.constraint_schema,
        t.constraint_name
      from
        information_schema.constraint_table_usage as t
      where
        t.table_schema = '$2'
      order by
        t.table_name,
        t.constraint_name
  )",
                                        table_catalog_col,
                                        constraint_catalog_col, schema));
  LogResults(results);

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // clang-format off
    auto expected = ExpectedRows(results, {
      {schema, "base", schema, "PK_base"},  // NOLINT
      {schema, "cascade_child", schema, "IDX_cascade_child_child_key_value1_U_\\w{16}"},  // NOLINT
      {schema, "cascade_child", schema, "PK_cascade_child"},  // NOLINT
      {schema, "cascade_child", schema, "fk_base_cascade_child"},  // NOLINT
      {schema, "no_action_child", schema, "PK_no_action_child"},  // NOLINT
      {schema, "row_deletion_policy", schema, "PK_row_deletion_policy"},  // NOLINT
    });
    // clang-format on
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  } else {
    // clang-format off
    auto expected = ExpectedRows(results, {
      {"", "", "base", "", "", "CK_IS_NOT_NULL_base_bool_array"},  // NOLINT
      {"", "", "base", "", "", "CK_IS_NOT_NULL_base_int_value"},  // NOLINT
      {"", "", "base", "", "", "CK_base_\\w{16}_1"},  // NOLINT
      {"", "", "base", "", "", "PK_base"},  // NOLINT
      {"", "", "base", "", "", "check_constraint_name"},  // NOLINT
      {"", "", "cascade_child", "", "", "CK_IS_NOT_NULL_cascade_child_value1"},  // NOLINT
      {"", "", "cascade_child", "", "", "IDX_cascade_child_child_key_value1_U_\\w{16}"},  // NOLINT
      {"", "", "cascade_child", "", "", "PK_cascade_child"},  // NOLINT
      {"", "", "cascade_child", "", "", "fk_base_cascade_child"},  // NOLINT
      {"", "", "no_action_child", "", "", "PK_no_action_child"},  // NOLINT
      {"", "", "row_deletion_policy", "", "", "PK_row_deletion_policy"},  // NOLINT
    });
    // clang-format on
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  }
}

TEST_P(InformationSchemaTest, DefaultReferentialConstraints) {
  std::string schema = "";
  std::string constraint_catalog_col = "";
  std::string unique_constraint_catalog_col = "";
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    schema = "public";
    // Since we don't set the catalog name to the value expected by PG due to
    // the database name not being available to the information schema catalog
    // that populates the data, we exclude these two columns from the PG query.
  } else {
    constraint_catalog_col = "t.constraint_catalog,";
    unique_constraint_catalog_col = "t.unique_constraint_catalog,";
  }

  auto results = Query(absl::Substitute(R"(
      select
        $0
        t.constraint_schema,
        t.constraint_name,
        $1
        t.unique_constraint_schema,
        t.unique_constraint_name,
        t.match_option,
        t.update_rule,
        t.delete_rule,
        t.spanner_state
      from
        information_schema.referential_constraints as t
      where
        t.constraint_schema = '$2'
      order by
        t.constraint_name
  )",
                                        constraint_catalog_col,
                                        unique_constraint_catalog_col, schema));
  LogResults(results);

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // clang-format off
    auto expected = ExpectedRows(results, {
        {schema, "fk_base_cascade_child", schema, "IDX_cascade_child_child_key_value1_U_\\w{16}", "NONE", "NO ACTION", "NO ACTION", "COMMITTED"},  // NOLINT
    });
    // clang-format on
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  } else {
    // clang-format off
    auto expected = ExpectedRows(results, {
        {"", schema, "fk_base_cascade_child", "", schema, "IDX_cascade_child_child_key_value1_U_\\w{16}", "SIMPLE", "NO ACTION", "NO ACTION", "COMMITTED"},  // NOLINT
    });
    // clang-format on
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  }
}

TEST_P(InformationSchemaTest, DefaultKeyColumnUsage) {
  std::string schema = "";
  std::string constraint_catalog_col = "";
  std::string table_catalog_col = "";
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    schema = "public";
    // We currently don't store the correct value for the catalog name in PG as
    // the emulator information schema doesn't have access to the database name
    // when populating the information schema tables. So we don't query these
    // columns in PG.
  } else {
    constraint_catalog_col = "t.constraint_catalog,";
    table_catalog_col = "t.table_catalog,";
  }

  auto results = Query(absl::Substitute(R"(
      select
        $0
        t.constraint_schema,
        t.constraint_name,
        $1
        t.table_schema,
        t.table_name,
        t.column_name,
        t.ordinal_position,
        t.position_in_unique_constraint
      from
        information_schema.key_column_usage as t
      where
        t.constraint_schema = '$2'
      order by
        t.constraint_name,
        t.table_name,
        t.ordinal_position
  )",
                                        constraint_catalog_col,
                                        table_catalog_col, schema));
  LogResults(results);

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // clang-format off
    auto expected = ExpectedRows(results, {
      {schema, "IDX_cascade_child_child_key_value1_U_\\w{16}", schema, "cascade_child", "child_key", 1, Ni()},  // NOLINT
      {schema, "IDX_cascade_child_child_key_value1_U_\\w{16}", schema, "cascade_child", "value1", 2, Ni()},  // NOLINT
      {schema, "PK_base", schema, "base", "key1", 1, Ni()},  // NOLINT
      {schema, "PK_base", schema, "base", "key2", 2, Ni()},  // NOLINT
      {schema, "PK_cascade_child", schema, "cascade_child", "key1", 1, Ni()},  // NOLINT
      {schema, "PK_cascade_child", schema, "cascade_child", "key2", 2, Ni()},  // NOLINT
      {schema, "PK_cascade_child", schema, "cascade_child", "child_key", 3, Ni()},  // NOLINT
      {schema, "PK_no_action_child", schema, "no_action_child", "key1", 1, Ni()},  // NOLINT
      {schema, "PK_no_action_child", schema, "no_action_child", "key2", 2, Ni()},  // NOLINT
      {schema, "PK_no_action_child", schema, "no_action_child", "child_key", 3, Ni()},  // NOLINT
      {schema, "PK_row_deletion_policy", schema, "row_deletion_policy", "key", 1, Ni()},  // NOLINT
      {schema, "fk_base_cascade_child", schema, "base", "bool_value", 1, 1},  // NOLINT
      {schema, "fk_base_cascade_child", schema, "base", "key2", 2, 2},  // NOLINT
    });
    // clang-format on
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  } else {
    // clang-format off
    auto expected = ExpectedRows(results, {
      {"", "", "IDX_cascade_child_child_key_value1_U_\\w{16}", "", "", "cascade_child", "child_key", 1, Ni()},  // NOLINT
      {"", "", "IDX_cascade_child_child_key_value1_U_\\w{16}", "", "", "cascade_child", "value1", 2, Ni()},  // NOLINT
      {"", "", "PK_base", "", "", "base", "key1", 1, Ni()},  // NOLINT
      {"", "", "PK_base", "", "", "base", "key2", 2, Ni()},  // NOLINT
      {"", "", "PK_cascade_child", "", "", "cascade_child", "key1", 1, Ni()},  // NOLINT
      {"", "", "PK_cascade_child", "", "", "cascade_child", "key2", 2, Ni()},  // NOLINT
      {"", "", "PK_cascade_child", "", "", "cascade_child", "child_key", 3, Ni()},  // NOLINT
      {"", "", "PK_no_action_child", "", "", "no_action_child", "key1", 1, Ni()},  // NOLINT
      {"", "", "PK_no_action_child", "", "", "no_action_child", "key2", 2, Ni()},  // NOLINT
      {"", "", "PK_no_action_child", "", "", "no_action_child", "child_key", 3, Ni()},  // NOLINT
      {"", "", "PK_row_deletion_policy", "", "", "row_deletion_policy", "key", 1, Ni()},  // NOLINT
      {"", "", "fk_base_cascade_child", "", "", "base", "bool_value", 1, 1},  // NOLINT
      {"", "", "fk_base_cascade_child", "", "", "base", "key2", 2, 2},  // NOLINT
    });
    // clang-format on
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  }
}

TEST_P(InformationSchemaTest, DefaultConstraintColumnUsage) {
  std::string schema = "";
  std::string table_catalog_col = "";
  std::string constraint_catalog_col = "";
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    schema = "public";
    // We currently don't store the correct value for the catalog name in PG as
    // the emulator information schema doesn't have access to the database name
    // when populating the information schema tables. So we don't query these
    // columns in PG.
  } else {
    table_catalog_col = "t.table_catalog,";
    constraint_catalog_col = "t.constraint_catalog,";
  }
  auto results = Query(absl::Substitute(R"(
      select
        $0
        t.table_schema,
        t.table_name,
        t.column_name,
        $1
        t.constraint_schema,
        t.constraint_name
      from
        information_schema.constraint_column_usage as t
      where
        t.table_schema = '$2'
      order by
        t.table_name,
        t.column_name,
        t.constraint_name
  )",
                                        table_catalog_col,
                                        constraint_catalog_col, schema));
  LogResults(results);

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // clang-format off
    auto expected = ExpectedRows(results, {
      {schema, "base", "bool_array", schema, "CK_IS_NOT_NULL_base_bool_array"},  // NOLINT
      {schema, "base", "int_value", schema, "CK_IS_NOT_NULL_base_int_value"},  // NOLINT
      {schema, "base", "int_value", schema, "CK_base_\\w{16}_1"},  // NOLINT
      {schema, "base", "int_value", schema, "check_constraint_name"},  // NOLINT
      {schema, "base", "key1", schema, "CK_IS_NOT_NULL_base_key1"},  // NOLINT
      {schema, "base", "key1", schema, "PK_base"},  // NOLINT
      {schema, "base", "key2", schema, "CK_IS_NOT_NULL_base_key2"},  // NOLINT
      {schema, "base", "key2", schema, "PK_base"},  // NOLINT
      {schema, "cascade_child", "child_key", schema, "CK_IS_NOT_NULL_cascade_child_child_key"},  // NOLINT
      {schema, "cascade_child", "child_key", schema, "IDX_cascade_child_child_key_value1_U_\\w{16}"},  // NOLINT
      {schema, "cascade_child", "child_key", schema, "PK_cascade_child"},  // NOLINT
      {schema, "cascade_child", "child_key", schema, "fk_base_cascade_child"},  // NOLINT
      {schema, "cascade_child", "key1", schema, "CK_IS_NOT_NULL_cascade_child_key1"},  // NOLINT
      {schema, "cascade_child", "key1", schema, "PK_cascade_child"},  // NOLINT
      {schema, "cascade_child", "key2", schema, "CK_IS_NOT_NULL_cascade_child_key2"},  // NOLINT
      {schema, "cascade_child", "key2", schema, "PK_cascade_child"},  // NOLINT
      {schema, "cascade_child", "value1", schema, "CK_IS_NOT_NULL_cascade_child_value1"},  // NOLINT
      {schema, "cascade_child", "value1", schema, "IDX_cascade_child_child_key_value1_U_\\w{16}"},  // NOLINT
      {schema, "cascade_child", "value1", schema, "fk_base_cascade_child"},  // NOLINT
      {schema, "no_action_child", "child_key", schema, "CK_IS_NOT_NULL_no_action_child_child_key"},  // NOLINT
      {schema, "no_action_child", "child_key", schema, "PK_no_action_child"},  // NOLINT
      {schema, "no_action_child", "key1", schema, "CK_IS_NOT_NULL_no_action_child_key1"},  // NOLINT
      {schema, "no_action_child", "key1", schema, "PK_no_action_child"},  // NOLINT
      {schema, "no_action_child", "key2", schema, "CK_IS_NOT_NULL_no_action_child_key2"},  // NOLINT
      {schema, "no_action_child", "key2", schema, "PK_no_action_child"},  // NOLINT
      {schema, "row_deletion_policy", "key", schema, "CK_IS_NOT_NULL_row_deletion_policy_key"},  // NOLINT
      {schema, "row_deletion_policy", "key", schema, "PK_row_deletion_policy"},  // NOLINT
    });
    // clang-format on
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  } else {
    // clang-format off
    auto expected = ExpectedRows(results, {
      {"", "", "base", "bool_array", "", "", "CK_IS_NOT_NULL_base_bool_array"},  // NOLINT
      {"", "", "base", "int_value", "", "", "CK_IS_NOT_NULL_base_int_value"},  // NOLINT
      {"", "", "base", "int_value", "", "", "CK_base_\\w{16}_1"},  // NOLINT
      {"", "", "base", "int_value", "", "", "check_constraint_name"},  // NOLINT
      {"", "", "base", "key1", "", "", "PK_base"},  // NOLINT
      {"", "", "base", "key2", "", "", "PK_base"},  // NOLINT
      {"", "", "cascade_child", "child_key", "", "", "IDX_cascade_child_child_key_value1_U_\\w{16}"},  // NOLINT
      {"", "", "cascade_child", "child_key", "", "", "PK_cascade_child"},  // NOLINT
      {"", "", "cascade_child", "child_key", "", "", "fk_base_cascade_child"},  // NOLINT
      {"", "", "cascade_child", "key1", "", "", "PK_cascade_child"},  // NOLINT
      {"", "", "cascade_child", "key2", "", "", "PK_cascade_child"},  // NOLINT
      {"", "", "cascade_child", "value1", "", "", "CK_IS_NOT_NULL_cascade_child_value1"},  // NOLINT
      {"", "", "cascade_child", "value1", "", "", "IDX_cascade_child_child_key_value1_U_\\w{16}"},  // NOLINT
      {"", "", "cascade_child", "value1", "", "", "fk_base_cascade_child"},  // NOLINT
      {"", "", "no_action_child", "child_key", "", "", "PK_no_action_child"},  // NOLINT
      {"", "", "no_action_child", "key1", "", "", "PK_no_action_child"},  // NOLINT
      {"", "", "no_action_child", "key2", "", "", "PK_no_action_child"},  // NOLINT
      {"", "", "row_deletion_policy", "key", "", "", "PK_row_deletion_policy"},  // NOLINT
    });
    // clang-format on
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  }
}

// Tests information schema behavior in the presence of generated columns.
class ColumnColumnUsageInformationSchemaTest : public InformationSchemaTest {
 public:
  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

  absl::Status SetUpDatabase() override {
    if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
      return SetSchema({R"(
        CREATE TABLE generated_columns (
          user_id bigint,
          first_name varchar(100),
          last_name varchar(100),
          full_name varchar(200) GENERATED ALWAYS AS (CONCAT(first_name, ', '::text, last_name)) STORED,
          uppercase_name varchar GENERATED ALWAYS AS (UPPER(first_name)) STORED,
          PRIMARY KEY(user_id)
        )
      )"});
    } else {
      return SetSchema({R"(
        CREATE TABLE generated_columns (
          user_id INT64,
          first_name STRING(100),
          last_name STRING(100),
          full_name STRING(200) AS (CONCAT(first_name, ", ", last_name)) STORED,
          uppercase_name STRING(MAX) AS (UPPER(first_name)) STORED,
        ) PRIMARY KEY(user_id)
      )"});
    }
  }
};

TEST_P(InformationSchemaTest, DefaultViews) {
  std::string filter;
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    filter = "t.table_schema = 'public'";
  } else {
    filter = "t.table_catalog = '' and t.table_schema = ''";
  }
  auto results = Query(absl::Substitute(R"(
      select
        t.table_catalog,
        t.table_schema,
        t.table_name,
        t.view_definition
      from
        information_schema.views AS t
      where
        $0
      order by
        t.table_name
    )",
                                        filter));
  LogResults(results);

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    auto expected = std::vector<ValueRow>({
        {"public", "base_view", "SELECT key1 FROM base"},
    });
    ZETASQL_EXPECT_OK(results);
    // Remove the table_catalog column from the expected results since we don't
    // currently set that to its correct value for PG.
    EXPECT_THAT(StripFirstColumnFromRows(*results), expected);
  } else {
    auto expected = std::vector<ValueRow>({
        {"", "", "base_view", "SELECT base.key1 FROM base"},
    });
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  }
}

TEST_P(InformationSchemaTest, DefaultCheckConstraints) {
  auto results = Query(absl::Substitute(
      R"(
      select
        t.constraint_catalog,
        t.constraint_schema,
        t.constraint_name,
        t.check_clause,
        t.spanner_state
      from
        information_schema.check_constraints as t
      where
        t.constraint_schema = '$0'
      order by
        t.constraint_name
  )",
      (GetParam() == database_api::DatabaseDialect::POSTGRESQL ? "public"
                                                               : "")));
  LogResults(results);

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // clang-format off
    auto expected = ExpectedRows(StripFirstColumnFromRows(*results), {
      {"public", "CK_IS_NOT_NULL_base_bool_array", "bool_array IS NOT NULL", "COMMITTED"},  // NOLINT
      {"public", "CK_IS_NOT_NULL_base_int_value", "int_value IS NOT NULL", "COMMITTED"},  // NOLINT
      {"public", "CK_IS_NOT_NULL_base_key1", "key1 IS NOT NULL", "COMMITTED"},  // NOLINT
      {"public", "CK_IS_NOT_NULL_base_key2", "key2 IS NOT NULL", "COMMITTED"},  // NOLINT
      {"public", "CK_IS_NOT_NULL_cascade_child_child_key", "child_key IS NOT NULL", "COMMITTED"},  // NOLINT
      {"public", "CK_IS_NOT_NULL_cascade_child_key1", "key1 IS NOT NULL", "COMMITTED"},  // NOLINT
      {"public", "CK_IS_NOT_NULL_cascade_child_key2", "key2 IS NOT NULL", "COMMITTED"},  // NOLINT
      {"public", "CK_IS_NOT_NULL_cascade_child_value1", "value1 IS NOT NULL", "COMMITTED"},  // NOLINT
      {"public", "CK_IS_NOT_NULL_no_action_child_child_key", "child_key IS NOT NULL", "COMMITTED"},  // NOLINT
      {"public", "CK_IS_NOT_NULL_no_action_child_key1", "key1 IS NOT NULL", "COMMITTED"},  // NOLINT
      {"public", "CK_IS_NOT_NULL_no_action_child_key2", "key2 IS NOT NULL", "COMMITTED"},  // NOLINT
      {"public", "CK_IS_NOT_NULL_row_deletion_policy_key", "key IS NOT NULL", "COMMITTED"},  // NOLINT
      {"public", "CK_base_\\w{16}_1", "(int_value > '0'::bigint)", "COMMITTED"},  // NOLINT
      {"public", "check_constraint_name", "(int_value > '0'::bigint)", "COMMITTED"},  // NOLINT
    });
    // clang-format on
    // Remove the table_catalog column from the expected results since we don't
    // currently set that to its correct value for PG.
    EXPECT_THAT(StripFirstColumnFromRows(*results), expected);
  } else {
    // clang-format off
    auto expected = ExpectedRows(results, std::vector<ValueRow>({
      {"", "", "CK_IS_NOT_NULL_base_bool_array", "bool_array IS NOT NULL", "COMMITTED"},  // NOLINT
      {"" , "", "CK_IS_NOT_NULL_base_int_value", "int_value IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "", "CK_IS_NOT_NULL_cascade_child_value1", "value1 IS NOT NULL", "COMMITTED"},  // NOLINT
      {"", "", "CK_base_\\w{16}_1", "int_value > 0", "COMMITTED"},  // NOLINT
      {"", "", "check_constraint_name", "int_value > 0", "COMMITTED"}  // NOLINT
    }));
    // clang-format on
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  }
}

INSTANTIATE_TEST_SUITE_P(
    PerDialectColumnColumnUsageInformationSchemaTests,
    ColumnColumnUsageInformationSchemaTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL
                    ,
                    database_api::DatabaseDialect::POSTGRESQL
                    ),
    [](const testing::TestParamInfo<
        ColumnColumnUsageInformationSchemaTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(ColumnColumnUsageInformationSchemaTest, DefaultColumnColumnUsage) {
  // GSQL uses an empty string for the default schema and PG doesn't.
  std::string filter = "";
  if (GetParam() == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    filter = "t.table_catalog = '' and ";
  }
  auto results = Query(absl::Substitute(
      R"(
      select
        t.table_name,
        t.dependent_column,
        t.column_name
      from
        information_schema.column_column_usage as t
      where
        $0
        t.table_schema = '$1'
      order by
        t.table_name, t.dependent_column, t.column_name
  )",
      filter,
      (GetParam() == database_api::DatabaseDialect::POSTGRESQL ? "public"
                                                               : "")));
  LogResults(results);
  // clang-format off
  auto expected = ExpectedRows(results, {
    {"generated_columns",  "full_name",       "first_name"},
    {"generated_columns",  "full_name",       "last_name"},
    {"generated_columns",  "uppercase_name",  "first_name"},
  });
  // clang-format on
  EXPECT_THAT(results, IsOkAndHoldsRows(expected));
}

// Tests information schema behavior in the presence of a foreign key which
// uses the referenced table's primary key as the backing index. Inspired by
// https://github.com/GoogleCloudPlatform/cloud-spanner-emulator/issues/10
class ForeignKeyInformationSchemaTest : public InformationSchemaTest {
 public:
  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

  absl::Status SetUpDatabase() override {
    if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
      return SetSchema({R"(
        CREATE TABLE table1 (
          name1  character varying(64)  NOT NULL,
          value1 bigint                 NOT NULL,
          PRIMARY KEY(name1)
        )
      )",
                        R"(
        CREATE TABLE table2 (
          name2      character varying(64)  NOT NULL,
          value2     bigint                 NOT NULL,
          other_name character varying(64)  NOT NULL,
          PRIMARY KEY(name2)
        )
      )",
                        R"(
        ALTER TABLE table2 ADD FOREIGN KEY(other_name) REFERENCES table1(name1)
      )"});
    } else {
      return SetSchema({R"(
        CREATE TABLE table1 (
          name1  STRING(64)  NOT NULL,
          value1 INT64       NOT NULL,
        ) PRIMARY KEY(name1)
      )",
                        R"(
        CREATE TABLE table2 (
          name2      STRING(64)  NOT NULL,
          value2     INT64       NOT NULL,
          other_name STRING(64)  NOT NULL,
        ) PRIMARY KEY(name2)
      )",
                        R"(
        ALTER TABLE table2 ADD FOREIGN KEY(other_name) REFERENCES table1(name1)
      )"});
    }
  }
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectForeignKeyInformationSchemaTests, ForeignKeyInformationSchemaTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL
                    ,
                    database_api::DatabaseDialect::POSTGRESQL
                    ),
    [](const testing::TestParamInfo<ForeignKeyInformationSchemaTest::ParamType>&
           info) { return database_api::DatabaseDialect_Name(info.param); });

TEST_P(ForeignKeyInformationSchemaTest, DefaultTableConstraints) {
  auto results = Query(absl::Substitute(
      R"(
      select
        t.constraint_name,
        t.table_name,
        t.constraint_type
      from
        information_schema.table_constraints as t
      where
        t.constraint_schema = '$0'
      order by
        t.constraint_name
  )",
      (GetParam() == database_api::DatabaseDialect::POSTGRESQL ? "public"
                                                               : "")));
  LogResults(results);
  // clang-format off
  auto expected = ExpectedRows(results, {
    {"CK_IS_NOT_NULL_table1_name1",      "table1", "CHECK"},
    {"CK_IS_NOT_NULL_table1_value1",     "table1", "CHECK"},
    {"CK_IS_NOT_NULL_table2_name2",      "table2", "CHECK"},
    {"CK_IS_NOT_NULL_table2_other_name", "table2", "CHECK"},
    {"CK_IS_NOT_NULL_table2_value2",     "table2", "CHECK"},
    {"FK_table2_table1_\\w{16}_1",       "table2", "FOREIGN KEY"},
    {"PK_table1",                        "table1", "PRIMARY KEY"},
    {"PK_table2",                        "table2", "PRIMARY KEY"},
  });
  // clang-format on
  EXPECT_THAT(results, IsOkAndHoldsRows(expected));
}

TEST_P(ForeignKeyInformationSchemaTest, DefaultConstraintTableUsage) {
  auto results = Query(absl::Substitute(
      R"(
      select
        t.table_name,
        t.constraint_name
      from
        information_schema.constraint_table_usage as t
      where
        t.table_schema = '$0'
      order by
        t.table_name,
        t.constraint_name
  )",
      GetParam() == database_api::DatabaseDialect::POSTGRESQL ? "public" : ""));
  LogResults(results);

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // clang-format off
    auto expected = ExpectedRows(results, {
      {"table1", "FK_table2_table1_\\w{16}_1"},
      {"table1", "PK_table1"},
      {"table2", "PK_table2"},
    });
    // clang-format on
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  } else {
    // clang-format off
    auto expected = ExpectedRows(results, {
      {"table1", "CK_IS_NOT_NULL_table1_name1"},
      {"table1", "CK_IS_NOT_NULL_table1_value1"},
      {"table1", "FK_table2_table1_\\w{16}_1"},
      {"table1", "PK_table1"},
      {"table2", "CK_IS_NOT_NULL_table2_name2"},
      {"table2", "CK_IS_NOT_NULL_table2_other_name"},
      {"table2", "CK_IS_NOT_NULL_table2_value2"},
      {"table2", "PK_table2"},
    });
    // clang-format on
    EXPECT_THAT(results, IsOkAndHoldsRows(expected));
  }
}

TEST_P(ForeignKeyInformationSchemaTest, DefaultKeyColumnUsage) {
  auto results = Query(absl::Substitute(
      R"(
      select
        t.constraint_name,
        t.table_name,
        t.column_name,
        t.ordinal_position,
        t.position_in_unique_constraint
      from
        information_schema.key_column_usage as t
      where
        t.constraint_schema = '$0'
      order by
        t.constraint_name,
        t.table_name,
        t.ordinal_position
  )",
      GetParam() == database_api::DatabaseDialect::POSTGRESQL ? "public" : ""));
  LogResults(results);
  // clang-format off
  auto expected = ExpectedRows(results, {
    {"FK_table2_table1_\\w{16}_1", "table2", "other_name", 1, 1},
    {"PK_table1", "table1", "name1", 1, Ni()},
    {"PK_table2", "table2", "name2", 1, Ni()},
  });
  // clang-format on
  EXPECT_THAT(results, IsOkAndHoldsRows(expected));
}

TEST_P(ForeignKeyInformationSchemaTest, DefaultReferentialConstraints) {
  auto results = Query(absl::Substitute(
      R"(
      select
        t.constraint_name,
        t.unique_constraint_name
      from
        information_schema.referential_constraints as t
      where
        t.constraint_schema = '$0'
      order by
        t.constraint_name
  )",
      (GetParam() == database_api::DatabaseDialect::POSTGRESQL ? "public"
                                                               : "")));
  LogResults(results);
  // clang-format off
  auto expected = ExpectedRows(results, {
      {"FK_table2_table1_\\w{16}_1", "PK_table1"},
  });
  // clang-format on
  EXPECT_THAT(results, IsOkAndHoldsRows(expected));
}

TEST_P(ForeignKeyInformationSchemaTest, DefaultConstraintColumnUsage) {
  auto results = Query(absl::Substitute(
      R"(
      select
        t.table_name,
        t.column_name,
        t.constraint_name
      from
        information_schema.constraint_column_usage as t
      where
        t.table_schema = '$0'
      order by
        t.table_name,
        t.column_name,
        t.constraint_name
  )",
      GetParam() == database_api::DatabaseDialect::POSTGRESQL ? "public" : ""));
  LogResults(results);
  // clang-format off
  auto expected = ExpectedRows(results, {
    {"table1", "name1",      "CK_IS_NOT_NULL_table1_name1"},
    {"table1", "name1",      "FK_table2_table1_\\w{16}_1"},
    {"table1", "name1",      "PK_table1"},
    {"table1", "value1",     "CK_IS_NOT_NULL_table1_value1"},
    {"table2", "name2",      "CK_IS_NOT_NULL_table2_name2"},
    {"table2", "name2",      "PK_table2"},
    {"table2", "other_name", "CK_IS_NOT_NULL_table2_other_name"},
    {"table2", "value2",     "CK_IS_NOT_NULL_table2_value2"},
  });
  // clang-format on
  EXPECT_THAT(results, IsOkAndHoldsRows(expected));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
