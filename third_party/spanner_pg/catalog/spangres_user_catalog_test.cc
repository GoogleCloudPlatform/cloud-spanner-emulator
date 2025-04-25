//
// PostgreSQL is released under the PostgreSQL License, a liberal Open Source
// license, similar to the BSD or MIT licenses.
//
// PostgreSQL Database Management System
// (formerly known as Postgres, then as Postgres95)
//
// Portions Copyright © 1996-2020, The PostgreSQL Global Development Group
//
// Portions Copyright © 1994, The Regents of the University of California
//
// Portions Copyright 2023 Google LLC
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
// EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
// FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN
// "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
// MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
//------------------------------------------------------------------------------

#include "third_party/spanner_pg/catalog/spangres_user_catalog.h"

#include <memory>

#include "zetasql/public/table_valued_function.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "third_party/spanner_pg/catalog/spangres_system_catalog.h"
#include "third_party/spanner_pg/catalog/table_name.h"
#include "third_party/spanner_pg/test_catalog/spanner_test_catalog.h"
#include "third_party/spanner_pg/test_catalog/test_catalog.h"

namespace postgres_translator {
class EngineUserCatalogTestPeer{
 public:
  static zetasql::EnumerableCatalog* engine_provided_catalog(
      const spangres::SpangresUserCatalog* catalog) {
    return catalog->engine_provided_catalog_;
  }
};
namespace spangres {

namespace {

using ::postgres_translator::spangres::test::GetSpangresTestSpannerUserCatalog;
using ::postgres_translator::spangres::test::GetSpangresTestSystemCatalog;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

TEST(FindTable, SchemaNameMappingPublic) {
  auto catalog = std::make_unique<SpangresUserCatalog>(
      GetSpangresTestSpannerUserCatalog());

  // "public" can be used to find table in the top level schema.
  const zetasql::Table* table;
  ZETASQL_EXPECT_OK(catalog->FindTable({"public", "keyvalue"}, &table));
  EXPECT_NE(table, nullptr);
  EXPECT_EQ(table->FullName(), "keyvalue");

  // Case mismatched "public" doesn't count.
  EXPECT_THAT(catalog->FindTable({"Public", "keyvalue"}, &table),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST(FindTable, SchemaNameMappingSuccessfulQueries) {
  auto user_catalog = std::make_unique<SpangresUserCatalog>(
      GetSpangresTestSpannerUserCatalog());

  // information_schema queries should return pg_information_schema tables
  // because of the default mapping in the catalog constructor.
  const zetasql::Table* pg_info_schema_schemata_mapped = nullptr;
  ZETASQL_EXPECT_OK(user_catalog->FindTable({"information_schema", "schemata"},
                                    &pg_info_schema_schemata_mapped));
  EXPECT_NE(pg_info_schema_schemata_mapped, nullptr);

  // Verify existence of PG column in the returned table.
  const zetasql::Column* pg_column =
      pg_info_schema_schemata_mapped->FindColumnByName(
          "default_character_set_catalog");
  EXPECT_NE(pg_column, nullptr);
  EXPECT_EQ(pg_column->Name(), "default_character_set_catalog");
}

TEST(FindTable, ReturnsErrorWhenCatalogNameIsSpecified) {
  auto user_catalog = std::make_unique<SpangresUserCatalog>(
      GetSpangresTestSpannerUserCatalog());
  const zetasql::Table* table = nullptr;

  // Access table using catalog_name prefix.
  EXPECT_THAT(user_catalog->FindTable(
                  {"dbname", "information_schema", "schemata"}, &table),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cross-database references are not implemented: "
                       "\"dbname.information_schema.schemata\""));
}

TEST(FindTable, SchemaNameMappingHideInternalSchema) {
  auto user_catalog = std::make_unique<SpangresUserCatalog>(
      GetSpangresTestSpannerUserCatalog());

  // Direct access to pg_information_schema in user queries should not be
  // allowed because it is an internal implementation detail that could change.
  const zetasql::Table* table = nullptr;
  EXPECT_THAT(
      user_catalog->FindTable({"pg_information_schema", "schemata"}, &table),
      StatusIs(absl::StatusCode::kNotFound));
  EXPECT_EQ(table, nullptr);

  EXPECT_THAT(
      user_catalog->FindTable({"PG_INFORMATION_SCHEMA", "schemata"}, &table),
      StatusIs(absl::StatusCode::kNotFound));
  EXPECT_EQ(table, nullptr);

  EXPECT_THAT(
      user_catalog->FindTable({"Pg_Information_Schema", "schemata"}, &table),
      StatusIs(absl::StatusCode::kNotFound));
  EXPECT_EQ(table, nullptr);
}

TEST(FindTable, SchemaNameMappingCaseSensitivity) {
  auto user_catalog = std::make_unique<SpangresUserCatalog>(
      GetSpangresTestSpannerUserCatalog());

  // PG table lookup is case-sensitive and unquoted identifiers get converted to
  // lowercase before we get to `catalog::FindTable()`.
  // The only way we'll see upper or mixed case identifiers is if they were
  // quoted in user queries. Such queries should fail because PG system tables
  // have lowercase names.

  // lowercase schema name should succeed.
  const zetasql::Table* schemata_table = nullptr;
  ZETASQL_EXPECT_OK(user_catalog->FindTable({"information_schema", "schemata"},
                                    &schemata_table));
  EXPECT_NE(schemata_table, nullptr);

  // Mixedcase schema name should fail
  const zetasql::Table* null_table = nullptr;
  EXPECT_THAT(
      user_catalog->FindTable({"Information_Schema", "schemata"}, &null_table),
      StatusIs(absl::StatusCode::kNotFound));
  EXPECT_EQ(null_table, nullptr);

  // Uppercase schema name should fail
  EXPECT_THAT(
      user_catalog->FindTable({"INFORMATION_SCHEMA", "schemata"}, &null_table),
      StatusIs(absl::StatusCode::kNotFound));
  EXPECT_EQ(null_table, nullptr);
}

  // Disabled in the emualtor since SPANNER_SYS is not yet supported in the
  // emulator.
  TEST(FindTable, DISABLED_InUpperCaseCatalogPath) {
  auto catalog = std::make_unique<SpangresUserCatalog>(
      GetSpangresTestSpannerUserCatalog());

  // The test for a table with the "SPANNER_SYS" schema should return true,
  // since it was added to the upper case schema list.
  const zetasql::Table* stats_table;
  ZETASQL_EXPECT_OK(catalog->FindTable({"SPANNER_SYS", "QUERY_STATS_TOP_MINUTE"},
                               &stats_table));
  EXPECT_NE(stats_table, nullptr);
  EXPECT_EQ(stats_table->FullName(), "SPANNER_SYS.QUERY_STATS_TOP_MINUTE");
  EXPECT_EQ(
      catalog->GetCatalogPathForTable(stats_table),
      std::vector<std::string>({"SPANNER_SYS", "QUERY_STATS_TOP_MINUTE"}));
  absl::StatusOr<bool> in_stats_path =
      catalog->InUppercaseCatalogPath(stats_table);
  ZETASQL_EXPECT_OK(in_stats_path.status());
  EXPECT_TRUE(in_stats_path.value());

  ZETASQL_EXPECT_OK(catalog->FindTable({"spanner_sys", "query_stats_top_minute"},
                               &stats_table));
  EXPECT_NE(stats_table, nullptr);
  EXPECT_EQ(stats_table->FullName(), "SPANNER_SYS.QUERY_STATS_TOP_MINUTE");
  EXPECT_EQ(
      catalog->GetCatalogPathForTable(stats_table),
      std::vector<std::string>({"SPANNER_SYS", "QUERY_STATS_TOP_MINUTE"}));
  in_stats_path = catalog->InUppercaseCatalogPath(stats_table);
  ZETASQL_EXPECT_OK(in_stats_path.status());
  EXPECT_TRUE(in_stats_path.value());

  // The test for the keyvalue table will return false because it has an empty
  // schema.
  const zetasql::Table* kv_table;
  ZETASQL_EXPECT_OK(catalog->FindTable({"keyvalue"}, &kv_table));
  EXPECT_NE(kv_table, nullptr);
  EXPECT_EQ(kv_table->FullName(), "keyvalue");
  absl::StatusOr<bool> in_kv_path = catalog->InUppercaseCatalogPath(kv_table);
  ZETASQL_EXPECT_OK(in_kv_path.status());
  EXPECT_FALSE(in_kv_path.value());

  // The test for a table with the "information_schema" schema should return
  // false since the schema was not added to the uppercase schema list.
  const zetasql::Table* info_table;
  ZETASQL_EXPECT_OK(
      catalog->FindTable({"information_schema", "schemata"}, &info_table));
  EXPECT_NE(info_table, nullptr);
  EXPECT_EQ(info_table->FullName(), "pg_information_schema.schemata");
  absl::StatusOr<bool> in_info_path =
      catalog->InUppercaseCatalogPath(info_table);
  ZETASQL_EXPECT_OK(in_info_path.status());
  EXPECT_FALSE(in_info_path.value());
}

  // Disabled in the emualtor since SPANNER_SYS is not yet supported in the
  // emulator.
  TEST(FindTable, DISABLED_GetTableNameFromGsqlTable) {
  auto catalog = std::make_unique<SpangresUserCatalog>(
      GetSpangresTestSpannerUserCatalog());
  const zetasql::Table* table;
  ZETASQL_EXPECT_OK(catalog->FindTable({"spanner_sys", "query_stats_top_minute"},
                               &table));
  // Table's own name is upper case.
  EXPECT_NE(table, nullptr);
  EXPECT_EQ(table->FullName(), "SPANNER_SYS.QUERY_STATS_TOP_MINUTE");
  EXPECT_NE(table, nullptr);
  // Returns lower case names for table that is "in upper case path".
  EXPECT_THAT(
      catalog->GetTableNameForGsqlTable(*table),
      IsOkAndHolds(TableName({"spanner_sys", "query_stats_top_minute"})));

  ZETASQL_EXPECT_OK(catalog->FindTable({"keyvalue"}, &table));
  EXPECT_NE(table, nullptr);
  EXPECT_THAT(
      catalog->GetTableNameForGsqlTable(*table),
      IsOkAndHolds(TableName({"keyvalue"})));

  // Table with mixed case names is preserved.
  ZETASQL_EXPECT_OK(catalog->FindTable({"AllSpangresTypes"}, &table));
  EXPECT_NE(table, nullptr);
  EXPECT_THAT(
      catalog->GetTableNameForGsqlTable(*table),
      IsOkAndHolds(TableName({"AllSpangresTypes"})));

  ZETASQL_EXPECT_OK(
      catalog->FindTable({"information_schema", "schemata"}, &table));
  EXPECT_NE(table, nullptr);
  EXPECT_EQ(table->FullName(), "pg_information_schema.schemata");
  // Returns the postgres name for this table (which doesn't have "pg_").
  EXPECT_THAT(
      catalog->GetTableNameForGsqlTable(*table),
      IsOkAndHolds(TableName({"information_schema", "schemata"})));

  ZETASQL_EXPECT_OK(
      EngineUserCatalogTestPeer::engine_provided_catalog(catalog.get())
          ->FindTable({"information_schema", "schemata"}, &table));
  EXPECT_NE(table, nullptr);
  // This is a table that is hidden in postgres (this is the spanner information
  // schema), so return an error.
  EXPECT_THAT(
      catalog->GetTableNameForGsqlTable(*table),
      StatusIs(
          absl::StatusCode::kInternal,
          testing::HasSubstr(
              "Attempting to get postgres name from table in hidden or "
              "blocked schemas: INFORMATION_SCHEMA.SCHEMATA")));
}

  // Disabled in the emualtor since SPANNER_SYS is not yet supported in the
  // emulator.
  TEST(FindTable, DISABLED_GetCatalogPathForTable) {
  auto catalog = std::make_unique<SpangresUserCatalog>(
      GetSpangresTestSpannerUserCatalog());

  const zetasql::Table* stats_table;
  ZETASQL_EXPECT_OK(catalog->FindTable({"SPANNER_SYS", "QUERY_STATS_TOP_MINUTE"},
                               &stats_table));

  EXPECT_EQ(
      catalog->GetCatalogPathForTable(stats_table),
      std::vector<std::string>({"SPANNER_SYS", "QUERY_STATS_TOP_MINUTE"}));

  EXPECT_NE(catalog->GetCatalogPathForTable(stats_table),
            std::vector<std::string>({"keyvalue"}));

  const zetasql::Table* kv_table;
  ZETASQL_EXPECT_OK(catalog->FindTable({"keyvalue"}, &kv_table));
  EXPECT_EQ(catalog->GetCatalogPathForTable(kv_table),
            std::vector<std::string>({"keyvalue"}));
}

TEST(FindTableValuedFunction, FindChangeStreamTVFNotUDF) {
  auto catalog = absl::make_unique<SpangresUserCatalog>(
      GetSpangresTestSpannerUserCatalog());

  const zetasql::TableValuedFunction* tvf;
  // Matching the Spanner (SqlCatalog) engine-provided catalog with the prod
  // SpangresUserCatalog, we should correctly identify our TVF as a UDF.
  ZETASQL_EXPECT_OK(catalog->FindTableValuedFunction(
      {"read_json_keyvalue_change_stream"}, &tvf));
  EXPECT_NE(tvf, nullptr);
}

}  // namespace
}  // namespace spangres
}  // namespace postgres_translator

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
