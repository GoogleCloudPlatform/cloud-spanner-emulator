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

#include "third_party/spanner_pg/catalog/catalog_adapter.h"

#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/memory/memory.h"
#include "third_party/spanner_pg/catalog/spangres_user_catalog.h"
#include "third_party/spanner_pg/test_catalog/spanner_test_catalog.h"
#include "third_party/spanner_pg/test_catalog/test_catalog.h"
#include "third_party/spanner_pg/util/valid_memory_context_fixture.h"

namespace postgres_translator::test {
namespace {

using ::postgres_translator::spangres::SpangresUserCatalog;
using ::postgres_translator::spangres::test::GetSpangresTestAnalyzerOptions;
using ::postgres_translator::spangres::test::GetSpangresTestCatalogAdapter;
using ::postgres_translator::spangres::test::GetSpangresTestSpannerUserCatalog;
using ::postgres_translator::spangres::test::GetSpangresTestSystemCatalog;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

using CatalogAdapterTest = ValidMemoryContext;

TEST_F(CatalogAdapterTest, TestWithOneTable) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapter> catalog_adapter =
      GetSpangresTestCatalogAdapter(analyzer_options);

  // Test negative cases first.
  TableName mytable({"public", "mytable"});
  EXPECT_THAT(catalog_adapter->GetTableNameFromOid(
                  CatalogAdapter::kOidCounterStart - 1),
              StatusIs(absl::StatusCode::kInternal));

  // Insert 1 table to the adapter and validate correct mappings
  ZETASQL_ASSERT_OK_AND_ASSIGN(Oid mytable_oid,
                       catalog_adapter->GetOrGenerateOidFromTableName(mytable));

  EXPECT_THAT(catalog_adapter->GetOrGenerateOidFromTableName(mytable),
              IsOkAndHolds(mytable_oid));
  EXPECT_THAT(catalog_adapter->GetTableNameFromOid(mytable_oid),
              IsOkAndHolds(mytable));

  // Try retrieving table name of an unknown table, expect failure:
  EXPECT_THAT(catalog_adapter->GetTableNameFromOid(mytable_oid - 20),
              StatusIs(absl::StatusCode::kInternal));

  // Try getting oid of the same table again, expect same oid:
  ZETASQL_ASSERT_OK_AND_ASSIGN(Oid mytable_oid_take2,
                       catalog_adapter->GetOrGenerateOidFromTableName(mytable));
  EXPECT_EQ(mytable_oid, mytable_oid_take2);
}

TEST_F(CatalogAdapterTest, TestWithTwoTables) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapter> catalog_adapter =
      GetSpangresTestCatalogAdapter(analyzer_options);

  // Test 2 tables, expect 2 different oids
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Oid mytable_oid,
      catalog_adapter->GetOrGenerateOidFromTableName(TableName({"mytable"})));

  TableName mytable2({"public", "mytable2"});
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Oid mytable2_oid,
      catalog_adapter->GetOrGenerateOidFromTableName(mytable2));
  EXPECT_THAT(catalog_adapter->GetOrGenerateOidFromTableName(mytable2),
              IsOkAndHolds(mytable2_oid));
  EXPECT_THAT(catalog_adapter->GetTableNameFromOid(mytable2_oid),
              IsOkAndHolds(mytable2));

  EXPECT_NE(mytable_oid, mytable2_oid);
}

TEST_F(CatalogAdapterTest, ColumnId) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapter> catalog_adapter =
      GetSpangresTestCatalogAdapter(analyzer_options);

  ZETASQL_ASSERT_OK_AND_ASSIGN(int id1, catalog_adapter->AllocateColumnId());
  EXPECT_EQ(id1, 1);
  EXPECT_EQ(catalog_adapter->max_column_id(), 1);

  ZETASQL_ASSERT_OK_AND_ASSIGN(int id2, catalog_adapter->AllocateColumnId());
  EXPECT_EQ(id2, 2);
  EXPECT_EQ(catalog_adapter->max_column_id(), 2);

  ZETASQL_ASSERT_OK_AND_ASSIGN(int id3, catalog_adapter->AllocateColumnId());
  EXPECT_EQ(id3, 3);
  EXPECT_EQ(catalog_adapter->max_column_id(), 3);
}

TEST_F(CatalogAdapterTest, GooglesqlCatalogTest) {
  EngineSystemCatalog* engine_system_catalog = GetSpangresTestSystemCatalog();
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();

  // Passing in a nullptr instead of a ZetaSQL catalog, expect error:
  EXPECT_THAT(CatalogAdapter::Create(/*engine_user_catalog=*/nullptr,
                                     engine_system_catalog, analyzer_options,
                                     /*token_locations=*/{}),
              StatusIs(absl::StatusCode::kInternal));

  std::unique_ptr<CatalogAdapter> catalog_adapter =
      GetSpangresTestCatalogAdapter(analyzer_options);
  ASSERT_NE(catalog_adapter->GetEngineUserCatalog(), nullptr);

  // Try looking for a table, expect to find it:
  const zetasql::Table* key_value_table;
  ZETASQL_ASSERT_OK(catalog_adapter->GetEngineUserCatalog()->FindTable(
      {"KeyValue"}, &key_value_table));
  ASSERT_NE(key_value_table, nullptr);
  ASSERT_EQ(key_value_table->Name(), "keyvalue");
  ASSERT_EQ(key_value_table->GetColumn(0)->Name(), "key");
  ASSERT_EQ(key_value_table->GetColumn(1)->Name(), "value");
}

TEST_F(CatalogAdapterTest, EngineCatalogsTest) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  zetasql::EnumerableCatalog* test_user_catalog =
      GetSpangresTestSpannerUserCatalog();

  // Wrap catalog with EngineUserCatalog wrapper. EngineUserCatalog wrapper
  // provides schema_name mapping and other PostgreSQL
  // features.
  auto engine_user_catalog =
      std::make_unique<SpangresUserCatalog>(test_user_catalog);

  EngineSystemCatalog* engine_system_catalog = GetSpangresTestSystemCatalog();

  // Passing in a nullptr instead of an EngineSystemCatalog, expect error:
  EXPECT_THAT(CatalogAdapter::Create(std::move(engine_user_catalog),
                                     /*engine_system_catalog=*/nullptr,
                                     analyzer_options,
                                     /*token_locations=*/{}),
              StatusIs(absl::StatusCode::kInternal));
  {
    // Create a new catalog adapter, make sure the engine catalogs are set
    // properly:
    std::unique_ptr<CatalogAdapter> catalog_adapter =
        GetSpangresTestCatalogAdapter(analyzer_options);
    EXPECT_NE(catalog_adapter->GetEngineUserCatalog(), nullptr);
    EXPECT_NE(catalog_adapter->GetEngineSystemCatalog(), nullptr);
  }
  // Make sure that the original catalogs are still intact, i.e. the catalog
  // adapter doesn't own them:
  EXPECT_NE(test_user_catalog, nullptr);
  EXPECT_NE(engine_system_catalog, nullptr);
}

TEST_F(CatalogAdapterTest, OidCounterTest) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();

  // Verify two instances of CatalogAdapter generate oids starting from the same
  // value, as each of them has an oid counter with the same starting value.
  Oid table_oid;
  TableName mytable({"mytable"});
  {
    std::unique_ptr<CatalogAdapter> catalog_adapter =
        GetSpangresTestCatalogAdapter(analyzer_options);
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        table_oid, catalog_adapter->GetOrGenerateOidFromTableName(mytable));
  }

  {
    std::unique_ptr<CatalogAdapter> catalog_adapter =
        GetSpangresTestCatalogAdapter(analyzer_options);
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        Oid second_table_oid,
        catalog_adapter->GetOrGenerateOidFromTableName(mytable));
    EXPECT_EQ(second_table_oid, table_oid);
  }
}

TEST_F(CatalogAdapterTest, NamespaceAndPublic) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapter> catalog_adapter =
      GetSpangresTestCatalogAdapter(analyzer_options);

  // "t" is same as "public.t" and "dbname.public.t"
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Oid table1_oid,
      catalog_adapter->GetOrGenerateOidFromTableName(TableName({"table1"})));
  // Can find this as "public.table1".
  ZETASQL_ASSERT_OK_AND_ASSIGN(Oid table1_oid_from_public,
                       catalog_adapter->GetOrGenerateOidFromTableName(
                           TableName({"public", "table1"})));
  EXPECT_EQ(table1_oid, table1_oid_from_public);
  ZETASQL_ASSERT_OK_AND_ASSIGN(Oid table1_oid_from_public_with_db,
                       catalog_adapter->GetOrGenerateOidFromTableName(
                           TableName({"dbname", "public", "table1"})));
  EXPECT_EQ(table1_oid, table1_oid_from_public_with_db);
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto table1_name,
                       catalog_adapter->GetTableNameFromOid(table1_oid));
  EXPECT_EQ(table1_name, TableName({"public", "table1"}));

  // The name we get from GetTableNameFromOid is always <namespace>.<table>.
  ZETASQL_ASSERT_OK_AND_ASSIGN(Oid table2_oid_from_public_with_db,
                       catalog_adapter->GetOrGenerateOidFromTableName(
                           TableName({"dbname", "public", "table2"})));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Oid table2_oid,
      catalog_adapter->GetOrGenerateOidFromTableName(TableName({"table2"})));
  EXPECT_EQ(table2_oid, table2_oid_from_public_with_db);
  ZETASQL_ASSERT_OK_AND_ASSIGN(Oid table2_oid_from_public,
                       catalog_adapter->GetOrGenerateOidFromTableName(
                           TableName({"public", "table2"})));
  EXPECT_EQ(table2_oid, table2_oid_from_public);
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto table2_name,
                       catalog_adapter->GetTableNameFromOid(table2_oid));
  EXPECT_EQ(table2_name, TableName({"public", "table2"}));

  // Names in different namespaces are different.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Oid table3_oid,
      catalog_adapter->GetOrGenerateOidFromTableName(TableName({"table3"})));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Oid namespace1_table3_oid,
      catalog_adapter->GetOrGenerateOidFromTableName(
          TableName({"ns1", "table3"})));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Oid namespace2_table3_oid,
      catalog_adapter->GetOrGenerateOidFromTableName(
          TableName({"ns2", "table3"})));
  EXPECT_NE(table3_oid, namespace1_table3_oid);
  EXPECT_NE(table3_oid, namespace2_table3_oid);
  EXPECT_NE(namespace1_table3_oid, namespace2_table3_oid);
}

TEST_F(CatalogAdapterTest, GeneratesOidForUDFProc) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapter> catalog_adapter =
      GetSpangresTestCatalogAdapter(analyzer_options);

  FormData_pg_proc* proc =
      reinterpret_cast<FormData_pg_proc*>(palloc(sizeof(FormData_pg_proc)));
  proc->oid = InvalidOid;

  // Placeholder TVF pointer just needs to be non-null. Instantiating a real TVF
  // is complex and would make this test less readable.
  const zetasql::TableValuedFunction* tvf =
      reinterpret_cast<const zetasql::TableValuedFunction*>(1);

  ZETASQL_ASSERT_OK_AND_ASSIGN(Oid oid,
                       catalog_adapter->GenerateAndStoreUDFProcOid(proc, tvf));

  EXPECT_EQ(oid, proc->oid);
  EXPECT_NE(oid, InvalidOid);
  EXPECT_THAT(catalog_adapter->GetUDFProcFromOid(oid), IsOkAndHolds(proc));
  EXPECT_THAT(catalog_adapter->GetTVFFromOid(oid), IsOkAndHolds(tvf));
}

using ::testing::HasSubstr;
TEST_F(CatalogAdapterTest, RejectsInvalidUDFProc) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapter> catalog_adapter =
      GetSpangresTestCatalogAdapter(analyzer_options);

  FormData_pg_proc* proc =
      reinterpret_cast<FormData_pg_proc*>(palloc(sizeof(FormData_pg_proc)));
  proc->oid = INT8OID;  // Anything other than InvalidOid should be rejected.

  // Placeholder TVF pointer just needs to be non-null. Instantiating a real TVF
  // is complex and would make this test less readable.
  const zetasql::TableValuedFunction* tvf =
      reinterpret_cast<const zetasql::TableValuedFunction*>(1);

  EXPECT_THAT(catalog_adapter->GenerateAndStoreUDFProcOid(proc, tvf),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("pg_proc already has an oid assigned")));
}

TEST_F(CatalogAdapterTest, RejectsNullArgs) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapter> catalog_adapter =
      GetSpangresTestCatalogAdapter(analyzer_options);

  // Placeholder pointers just need to be non-null.
  const zetasql::TableValuedFunction* tvf =
      reinterpret_cast<const zetasql::TableValuedFunction*>(1);
  FormData_pg_proc* proc = reinterpret_cast<FormData_pg_proc*>(2);

  EXPECT_THAT(catalog_adapter->GenerateAndStoreUDFProcOid(nullptr, tvf),
              StatusIs(absl::StatusCode::kInternal));
  EXPECT_THAT(catalog_adapter->GenerateAndStoreUDFProcOid(proc, nullptr),
              StatusIs(absl::StatusCode::kInternal));
}

TEST_F(CatalogAdapterTest, FailedTVFLookup) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapter> catalog_adapter =
      GetSpangresTestCatalogAdapter(analyzer_options);

  EXPECT_THAT(catalog_adapter->GetTVFFromOid(InvalidOid),
              StatusIs(absl::StatusCode::kNotFound));
}

}  // namespace
}  // namespace postgres_translator::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
