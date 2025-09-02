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

#include "third_party/spanner_pg/interface/catalog_wrappers.h"

#include <memory>

#include "zetasql/public/catalog.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/log/scoped_mock_log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "third_party/spanner_pg/bootstrap_catalog/bootstrap_catalog.h"
#include "third_party/spanner_pg/catalog/catalog_adapter_holder.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/interface/ereport.h"
#include "third_party/spanner_pg/test_catalog/test_catalog.h"
#include "third_party/spanner_pg/util/postgres.h"
#include "third_party/spanner_pg/util/valid_memory_context_fixture.h"
#include "zetasql/base/ret_check.h"

namespace postgres_translator::test {
namespace {

using ::postgres_translator::spangres::test::GetSpangresTestAnalyzerOptions;
using ::postgres_translator::spangres::test::
    GetSpangresTestCatalogAdapterHolder;
using ::testing::_;
using ::testing::AnyNumber;
using ::testing::AtLeast;
using ::testing::HasSubstr;
using ::testing::UnorderedElementsAre;
using ::zetasql_base::testing::StatusIs;

using CatalogWrappersTest = ValidMemoryContext;

// Helper function for tests using RangeVars as input. Strings are copied to a
// PostgreSQL MemoryContext.
// All arguments are optional in a valid RangeVar except for `table_name`. For
// testing we permit a null table_name, which will cause an error to be
// reported by the translation code.
RangeVar* MakeRangeVar(const char* table_name,
                       const char* schema_name = nullptr,
                       const char* catalog_name = nullptr) {
  char* cat_string = catalog_name == nullptr ? nullptr : pstrdup(catalog_name);
  char* sch_string = schema_name == nullptr ? nullptr : pstrdup(schema_name);
  char* rel_string = table_name == nullptr ? nullptr : pstrdup(table_name);
  RangeVar* rv = makeRangeVar(sch_string, rel_string, /*location=*/-1);
  rv->catalogname = cat_string;
  return rv;
}

std::vector<Oid> GetProcOids(const std::string& proc_name) {
  const FormData_pg_proc** proc_list;
  size_t proc_count;
  GetProcsByName(proc_name.c_str(), &proc_list, &proc_count);

  std::vector<Oid> proc_oids;
  for (int proc_index = 0; proc_index < proc_count; ++proc_index) {
    const FormData_pg_proc* procform = proc_list[proc_index];
    proc_oids.push_back(procform->oid);
  }
  return proc_oids;
}

std::vector<const FormData_pg_amop*> GetAmopsByOprOid(const Oid& opid) {
  const FormData_pg_amop* const* amop_list;
  size_t amop_count;
  GetAmopsByAmopOpIdFromBootstrapCatalog(opid, &amop_list,
                                         &amop_count);

  std::vector<const FormData_pg_amop*> amops;
  for (int amop_index = 0; amop_index < amop_count; ++amop_index) {
    amops.push_back(amop_list[amop_index]);
  }
  return amops;
}

// Failed table lookups.
TEST_F(CatalogWrappersTest, FailedTableLookup) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);
  ParseState* parse_state = make_parsestate(/*parentParseState=*/nullptr);
  RangeVar* rv = MakeRangeVar("no_such_table");

  bool entered_catch = false;
  try {
    AddRangeTableEntryC(parse_state, rv, /*alias=*/nullptr,
                        /*inh=*/true, /*inFromCl=*/true);
  } catch (const postgres_translator::PostgresEreportException& exc) {
    entered_catch = true;
    EXPECT_STREQ(exc.what(),
                 "[ERROR] relation \"no_such_table\" does not exist");
  }
  ASSERT_TRUE(entered_catch);

  free_parsestate(parse_state);
}

TEST_F(CatalogWrappersTest, SuccessfulTableLookup) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);
  ParseState* parse_state = make_parsestate(/*parentParseState=*/nullptr);
  RangeVar* rv = MakeRangeVar("keyvalue");

  RangeTblEntry* rte = AddRangeTableEntryC(parse_state, rv, /*alias=*/nullptr,
                                           /*inh=*/true, /*inFromCl=*/true);

  ASSERT_NE(rte, nullptr);
  EXPECT_STREQ(rte->eref->aliasname, "keyvalue");
  EXPECT_EQ(
      reinterpret_cast<RangeTblEntry*>(list_nth(parse_state->p_rtable, 0)),
      rte);

  free_parsestate(parse_state);
}

TEST_F(CatalogWrappersTest, FailedTableLookupInvalidSchema) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);
  ParseState* parse_state = make_parsestate(/*parentParseState=*/nullptr);
  RangeVar* rv = MakeRangeVar("no_such_table", "no_such_schema");

  bool entered_catch = false;
  try {
    AddRangeTableEntryC(parse_state, rv, /*alias=*/nullptr,
                        /*inh=*/true, /*inFromCl=*/true);
  } catch (const postgres_translator::PostgresEreportException& exc) {
    entered_catch = true;
    EXPECT_STREQ(
        exc.what(),
        "[ERROR] relation \"no_such_schema.no_such_table\" does not exist");
  }
  ASSERT_TRUE(entered_catch);

  free_parsestate(parse_state);
}

TEST_F(CatalogWrappersTest, AddRangeTableEntryCSucceeds) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  ParseState* parse_state = make_parsestate(nullptr);

  RangeVar* relation = makeNode(RangeVar);
  Alias* alias = makeAlias("test_table_name", /*colnames=*/nullptr);

  absl::ScopedMockLog log(absl::MockLogDefault::kDisallowUnexpected);
  EXPECT_CALL(log, Log).Times(AnyNumber());
  EXPECT_CALL(log, Log(absl::LogSeverity::kError, _, _)).Times(0);
  log.StartCapturingLogs();

  relation->relname = pstrdup("AllSpangresTypes");
  RangeTblEntry* rte = AddRangeTableEntryC(parse_state, relation, alias,
                                           /*inh=*/true, /*inFromCl=*/true);
  EXPECT_NE(rte, nullptr);

  free_parsestate(parse_state);
}

TEST_F(CatalogWrappersTest, FailedTableLookupValidSchema) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);
  ParseState* parse_state = make_parsestate(/*parentParseState=*/nullptr);
  RangeVar* rv = MakeRangeVar("no_such_table", "information_schema");

  bool entered_catch = false;
  try {
    AddRangeTableEntryC(parse_state, rv, /*alias=*/nullptr,
                        /*inh=*/true, /*inFromCl=*/true);
  } catch (const postgres_translator::PostgresEreportException& exc) {
    entered_catch = true;
    EXPECT_STREQ(
        exc.what(),
        "[ERROR] relation \"information_schema.no_such_table\" does not exist");
  }
  ASSERT_TRUE(entered_catch);

  free_parsestate(parse_state);
}

TEST_F(CatalogWrappersTest, SuccessfulTableLookupWithSchema) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);
  ParseState* parse_state = make_parsestate(/*parentParseState=*/nullptr);
  RangeVar* rv = MakeRangeVar("tables", "information_schema");

  RangeTblEntry* rte = AddRangeTableEntryC(parse_state, rv, /*alias=*/nullptr,
                                           /*inh=*/true, /*inFromCl=*/true);
  ASSERT_NE(rte, nullptr);
  EXPECT_STREQ(rte->eref->aliasname, "tables");
  EXPECT_EQ(
      reinterpret_cast<RangeTblEntry*>(list_nth(parse_state->p_rtable, 0)),
      rte);

  free_parsestate(parse_state);
}

TEST_F(CatalogWrappersTest, TableLookupDatabase) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);
  ParseState* parse_state = make_parsestate(/*parentParseState=*/nullptr);
  RangeVar* rv = MakeRangeVar("some_table", "some_schema");

  bool entered_catch = false;
  try {
    AddRangeTableEntryC(parse_state, rv, /*alias=*/nullptr,
                        /*inh=*/true, /*inFromCl=*/true);
  } catch (const postgres_translator::PostgresEreportException& exc) {
    entered_catch = true;
    EXPECT_STREQ(exc.what(),
                 "[ERROR] relation \"some_schema.some_table\" does not exist");
  }
  ASSERT_TRUE(entered_catch);

  free_parsestate(parse_state);
}

TEST_F(CatalogWrappersTest, AddRangeTableEntryByOidCInvalidOid) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  ParseState* parse_state = make_parsestate(nullptr);
  Alias* alias = makeAlias("test_table_name", /*colnames=*/nullptr);

  absl::ScopedMockLog log(absl::MockLogDefault::kDisallowUnexpected);
  // Ignore other messages.
  EXPECT_CALL(log, Log).Times(AnyNumber());
  // Expect that the error is logged at least once
  EXPECT_CALL(log,
              Log(absl::LogSeverity::kError, _, HasSubstr("Valid table oids")))
      .Times(AtLeast(1));
  log.StartCapturingLogs();

  RangeTblEntry* rte =
      AddRangeTableEntryByOidC(parse_state, InvalidOid, alias, /*inh=*/true,
                               /*inFromCl=*/true);

  EXPECT_EQ(rte, nullptr);

  free_parsestate(parse_state);
}

TEST_F(CatalogWrappersTest, AddRangeTableEntryByOidCNoTableWithOid) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  ParseState* parse_state = make_parsestate(nullptr);
  Alias* alias = makeAlias("test_table_name", /*colnames=*/nullptr);

  absl::ScopedMockLog log(absl::MockLogDefault::kDisallowUnexpected);
  // Ignore other messages.
  EXPECT_CALL(log, Log).Times(AnyNumber());
  // Expect that the error is logged at least once
  EXPECT_CALL(log,
              Log(absl::LogSeverity::kError, _, HasSubstr("No table with oid")))
      .Times(AtLeast(1));
  log.StartCapturingLogs();

  RangeTblEntry* rte = AddRangeTableEntryByOidC(
      parse_state, CatalogAdapter::kOidCounterEnd + 1, alias, /*inh=*/true,
      /*inFromCl=*/true);
  EXPECT_EQ(rte, nullptr);

  free_parsestate(parse_state);
}

TEST_F(CatalogWrappersTest, AddRangeTableEntryByOidCSucceeds) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  ParseState* parse_state = make_parsestate(nullptr);
  Alias* alias = makeAlias("test_table_name", /*colnames=*/nullptr);

  absl::ScopedMockLog log(absl::MockLogDefault::kDisallowUnexpected);
  EXPECT_CALL(log, Log).Times(AnyNumber());
  EXPECT_CALL(log, Log(absl::LogSeverity::kError, _, _)).Times(0);
  log.StartCapturingLogs();

  CatalogAdapter* catalog_adapter;
  ZETASQL_ASSERT_OK_AND_ASSIGN(catalog_adapter, GetCatalogAdapter());
  ZETASQL_ASSERT_OK_AND_ASSIGN(Oid relation_oid,
                       catalog_adapter->GetOrGenerateOidFromTableName(
                           TableName({"AllSpangresTypes"})));
  RangeTblEntry* rte = AddRangeTableEntryByOidC(
      parse_state, relation_oid, alias, /*inh=*/true, /*inFromCl=*/true);
  EXPECT_NE(rte, nullptr);

  free_parsestate(parse_state);
}

TEST_F(CatalogWrappersTest, addRangeTableEntry) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  // Leave most fields blank (null or otherwise defaulted).
  // We're not trying to do any work, just prove that the shim get called.
  ParseState* parse_state = make_parsestate(/*parentParseState=*/nullptr);

  // "location=0" is a valid and syntactically-meaningful location.
  // Since we're setting location explicitly (and future devs may copy this),
  // set it to -1, which is PostgreSQL's magic value for "unknown location".
  RangeVar* relation = makeRangeVar(/*schemaname=*/nullptr, /*relname=*/nullptr,
                                    /*location=*/-1);

  // Aliases with null names do seem to generally work, but the `makeAlias()`
  // helper can't construct them directly.
  // Just use the empty string.  It's fine for this test.
  Alias* alias = makeAlias("", nullptr);

  ParseNamespaceItem* nsitem;
  bool entered_catch = false;

  try {
    nsitem = addRangeTableEntry(parse_state, relation, alias,
                                /*inh=*/true, /*inFromCl=*/true);
  } catch (const postgres_translator::PostgresEreportException& exc) {
    entered_catch = true;
    EXPECT_STREQ(
        exc.error_data().message,
        "Error occurred during RangeTblEntry construction: invalid relation "
        "'relname'");
  }

  EXPECT_TRUE(entered_catch);

  free_parsestate(parse_state);
}

// Test that without setting up a thread-local catalog, we fail any table
// translation.
TEST_F(CatalogWrappersTest, NoCatalogTest) {
  ASSERT_THAT(GetCatalogAdapter(), StatusIs(absl::StatusCode::kInternal,
                                            HasSubstr("not initialized")));
  RangeVar* relation = makeNode(RangeVar);

  absl::ScopedMockLog log(absl::MockLogDefault::kDisallowUnexpected);
  // Ignore other messages.
  EXPECT_CALL(log, Log).Times(AnyNumber());
  // Expect that the error is logged at least once
  EXPECT_CALL(
      log, Log(absl::LogSeverity::kError, _,
               HasSubstr("CatalogAdapter is not initialized on this thread.")))
      .Times(1);
  log.StartCapturingLogs();

  RangeTblEntry* rte = AddRangeTableEntryC(
      /*pstate=*/nullptr, relation,
      /*alias=*/nullptr, /*inh=*/false,
      /*inFromCl=*/false);
  EXPECT_EQ(rte, nullptr);
}

// Test that translating with no input table specified fails.
TEST_F(CatalogWrappersTest, NoRelationTranslateTest) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  ZETASQL_ASSERT_OK(GetCatalogAdapter());

  absl::ScopedMockLog log(absl::MockLogDefault::kDisallowUnexpected);
  // Ignore other messages.
  EXPECT_CALL(log, Log).Times(AnyNumber());
  // Expect that the error is logged at least once
  EXPECT_CALL(log, Log(absl::LogSeverity::kError, _,
                       HasSubstr("No RangeVar specified for catalog lookup.")))
      .Times(1);
  log.StartCapturingLogs();

  RangeTblEntry* rte = AddRangeTableEntryC(
      /*pstate=*/nullptr, /*relation=*/nullptr, /*alias=*/nullptr,
      /*inh=*/false, /*inFromCl=*/false);
  EXPECT_EQ(rte, nullptr);
}

// Test the attribute name lookup wrapper.
TEST_F(CatalogWrappersTest, AttributeNameLookupTest) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  // Teach the catalog adapter about our table.
  CatalogAdapter* catalog_adapter;
  ZETASQL_ASSERT_OK_AND_ASSIGN(catalog_adapter, GetCatalogAdapter());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Oid table_oid,
      catalog_adapter->GetOrGenerateOidFromTableName(TableName({"keyvalue"})));

  const char* key_col_name =
      GetAttributeNameC(table_oid, /*attnum=*/1, /*missing_ok=*/false);
  const char* value_col_name =
      GetAttributeNameC(table_oid, /*attnum=*/2, /*missing_ok=*/true);

  EXPECT_STREQ(key_col_name, "key");
  EXPECT_STREQ(value_col_name, "value");

  const char* invalid_col =
      GetAttributeNameC(table_oid, /*attnum=*/-1, /*missing_ok=*/true);
  EXPECT_EQ(invalid_col, nullptr);
}

TEST_F(CatalogWrappersTest, AttributeNameFailedLookupTest) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  EXPECT_THROW(
      GetAttributeNameC(InvalidOid, /*attnum=*/1, /*missing_ok=*/false),
      postgres_translator::PostgresEreportException);
}

TEST_F(CatalogWrappersTest, TableNameLookupTest) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  CatalogAdapter* catalog_adapter;
  ZETASQL_ASSERT_OK_AND_ASSIGN(catalog_adapter, GetCatalogAdapter());

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Oid table_oid,
      catalog_adapter->GetOrGenerateOidFromTableName(TableName({"KeyValue"})));

  const char* table_name = GetTableNameC(table_oid);
  EXPECT_STREQ(table_name, "KeyValue");
}

TEST_F(CatalogWrappersTest, ColumnsNameLookupTest) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  CatalogAdapter* catalog_adapter;
  ZETASQL_ASSERT_OK_AND_ASSIGN(catalog_adapter, GetCatalogAdapter());

  // Table oid doesn't exist in the adapter, expects error.
  // 16385 is in the valid oid range that a catalog adapter can accept. However,
  // since there is no table tracked by the adapter yet, this call should
  // return error.
  EXPECT_THAT(catalog_adapter->GetTableNameFromOid(16385),
              StatusIs(absl::StatusCode::kInternal));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Oid table_oid,
      catalog_adapter->GetOrGenerateOidFromTableName(TableName({"KeyValue"})));

  char** real_colnames;
  int ncolumns;
  GetColumnNamesC(table_oid, &real_colnames, &ncolumns);

  // Expect 2 columns, key and value.
  EXPECT_EQ(ncolumns, 2);
  EXPECT_STREQ(real_colnames[0], "key");
  EXPECT_STREQ(real_colnames[1], "value");
}

// Test the attribute type lookup wrapper.
TEST_F(CatalogWrappersTest, AttributeTypeLookupTest) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  CatalogAdapter* catalog_adapter;
  ZETASQL_ASSERT_OK_AND_ASSIGN(catalog_adapter, GetCatalogAdapter());

  // Teach the catalog adapter about our table.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Oid table_oid,
      catalog_adapter->GetOrGenerateOidFromTableName(TableName({"keyvalue"})));

  Oid type_oid;
  int32_t typmod;
  Oid collation_id;
  GetAttributeTypeC(table_oid, /*attnum=*/1, &type_oid, &typmod, &collation_id);

  EXPECT_EQ(type_oid, INT8OID);
  ZETASQL_ASSERT_OK_AND_ASSIGN(const FormData_pg_type* int8_type,
                       PgBootstrapCatalog::Default()->GetType(INT8OID));
  EXPECT_EQ(typmod, int8_type->typtypmod);
  EXPECT_EQ(collation_id, int8_type->typcollation);
}

// These tests mimic the cases in bootstrap_catalog_test, but via the
// C-compatible interface.
TEST(GetProc, Int4int8Proc) {
  const FormData_pg_cast* int4_to_int8 =
      GetCastFromBootstrapCatalog(INT4OID, INT8OID);
  ASSERT_NE(int4_to_int8, nullptr);

  EXPECT_EQ(int4_to_int8->castsource, INT4OID);
  EXPECT_EQ(int4_to_int8->casttarget, INT8OID);
  // Casting to larger size is implicit.
  EXPECT_EQ(int4_to_int8->castcontext, 'i');
}

TEST(GetType, Bool) {
  const FormData_pg_type* data = GetTypeFromBootstrapCatalog(BOOLOID);

  ASSERT_NE(data, nullptr);
  EXPECT_STREQ(NameStr(data->typname), "bool");
}

TEST(GetProc, Int8SumProc) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  constexpr Oid sum_oid =
      1842;  // Comes from pg_proc.h, which does not directly define this value.
             // Rather, it is bootstrap data for the postgres database that we
             // use here for a test value. In postgres, this value never exists
             // as a C value, only a database entry.
  const FormData_pg_proc* pg_proc = GetProcByOid(sum_oid);

  ASSERT_NE(pg_proc, nullptr);
  EXPECT_STREQ(NameStr(pg_proc->proname), "int8_sum");
}

TEST(CatalogWrappers, GetTypesByName) {
  const FormData_pg_type* const* type_list;
  size_t type_count;
  GetTypesByNameFromBootstrapCatalog("int4", &type_list, &type_count);
  EXPECT_EQ(type_count, 1);
  EXPECT_EQ(type_list[0]->oid, INT4OID);

  GetTypesByNameFromBootstrapCatalog("bool", &type_list, &type_count);
  EXPECT_EQ(type_count, 1);
  EXPECT_EQ(type_list[0]->oid, BOOLOID);

  GetTypesByNameFromBootstrapCatalog("no such type", &type_list, &type_count);
  EXPECT_EQ(type_count, 0);
  EXPECT_EQ(type_list, nullptr);
}

TEST(GetOperator, EqualsOperator) {
  // Comes from pg_operator.h, which does not directly define this value.
  // Rather, it is bootstrap data for the postgres database that we use here for
  // a test value. In postgres, this value never exists as a C value, only a
  // database entry.
  constexpr Oid equals_oid = 96;

  const FormData_pg_operator* equals_data =
      GetOperatorFromBootstrapCatalog(equals_oid);

  ASSERT_NE(equals_data, nullptr);
  EXPECT_STREQ(NameStr(equals_data->oprname), "=");
}

TEST(GetOperator, NegationOperator) {
  // Comes from pg_operator.h, which does not directly define this value.
  // Rather, it is bootstrap data for the postgres database that we use here for
  // a test value. In postgres, this value never exists as a C value, only a
  // database entry.
  constexpr Oid negation_oid = 484;

  const FormData_pg_operator* negation_data =
      GetOperatorFromBootstrapCatalog(negation_oid);

  ASSERT_NE(negation_data, nullptr);
  EXPECT_STREQ(NameStr(negation_data->oprname), "-");
}

TEST(GetOperatorList, PercentOperator) {
  const Oid* oidlist = nullptr;
  size_t count = 0;

  GetOperatorsByNameFromBootstrapCatalog("%", &oidlist, &count);

  EXPECT_THAT(
      absl::Span<const Oid>(oidlist, count),
      // Expected values come from pg_operator.h, which does not directly define
      // them. Rather, it is bootstrap data for the postgres database that we
      // use here for a test value. In postgres, this list never exists as a C
      // value, only a database entry.
      UnorderedElementsAre(439, 529, 530, 1762));
}

TEST(GetOperatorList, FailedOperator) {
  Oid unused = InvalidOid;
  const Oid* oidlist = &unused;
  size_t count = 1;  // Initialize to non-zero because we expect 0 after.

  GetOperatorsByNameFromBootstrapCatalog("no_such_operator", &oidlist, &count);

  EXPECT_EQ(count, 0);
  EXPECT_EQ(oidlist, nullptr);
}

TEST_F(CatalogWrappersTest, BoolInProc) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);
  std::vector<Oid> oidlist = GetProcOids("boolin");

  EXPECT_THAT(oidlist, UnorderedElementsAre(F_BOOLIN));
}

TEST_F(CatalogWrappersTest, Int8Proc) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);
  std::vector<Oid> oidlist = GetProcOids("int8");

  EXPECT_THAT(oidlist,
              UnorderedElementsAre(F_INT8_INT4, F_INT8_FLOAT8, F_INT8_FLOAT4,
                                   F_INT8_INT2, F_INT8_OID, F_INT8_NUMERIC,
                                   F_INT8_JSONB, F_INT8_BIT));
}

TEST_F(CatalogWrappersTest, FailedProc) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);
  std::vector<Oid> oidlist = GetProcOids("no_such_proc");

  EXPECT_TRUE(oidlist.empty());
}

TEST(GetAmopList, SuccessfulAmop) {
  // Get a sample operator oid.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const Oid int8_eq_oid,
      PgBootstrapCatalog::Default()->GetOperatorOidByOprLeftRight(
          "=", INT8OID, INT8OID));
  std::vector<const FormData_pg_amop*> amopList =
      GetAmopsByOprOid(int8_eq_oid);

  EXPECT_EQ(amopList.size(), 5);
  EXPECT_EQ(amopList[0]->amopopr, int8_eq_oid);
}

TEST(GetAmopList, FailedAmop) {
  std::vector<const FormData_pg_amop*> amopList =
      GetAmopsByOprOid(InvalidOid);
  EXPECT_TRUE(amopList.empty());
}

TEST(GetAggregateTest, Int4Sum) {
  // Comes from pg_aggregate.h, which does not directly define this value.
  // Rather, it is bootstrap data for the postgres database that we use here for
  // a test value. In postgres, this value never exists as a C value, only a
  // database entry.
  constexpr Oid int4_sum_oid = 2108;

  const FormData_pg_aggregate* agg_data =
      GetAggregateFromBootstrapCatalog(int4_sum_oid);

  // Verify a sampling of fields.
  ASSERT_NE(agg_data, nullptr);
  EXPECT_EQ(agg_data->aggkind, AGGKIND_NORMAL);
  EXPECT_EQ(agg_data->aggnumdirectargs, 0);
  EXPECT_EQ(agg_data->aggtransfn, F_INT4_SUM);
  EXPECT_EQ(agg_data->aggfinalfn, InvalidOid);
  EXPECT_EQ(agg_data->aggcombinefn, F_INT8PL);
  EXPECT_EQ(agg_data->aggserialfn, InvalidOid);
  EXPECT_EQ(agg_data->aggdeserialfn, InvalidOid);
}

TEST(GetAggregateTest, FailedAggregate) {
  const FormData_pg_aggregate* agg_data =
      GetAggregateFromBootstrapCatalog(InvalidOid);

  EXPECT_EQ(agg_data, nullptr);
}

// Setup helper function for ExpandRelation test cases: select a user table from
// the test catalog and pass it through the catalog adapter to get an assigned
// Oid. ExpandRelation will use this Oid to look up the table's attribute
// information.
absl::StatusOr<Oid> GetTableAndOid(const char table_name_cstr[],
                                   const zetasql::Table** table) {
  CatalogAdapter* catalog_adapter = nullptr;  // Declare here due to b/17073013.
  ZETASQL_ASSIGN_OR_RETURN(catalog_adapter, GetCatalogAdapter());
  TableName table_name({table_name_cstr});
  ZETASQL_RET_CHECK_OK(catalog_adapter->GetEngineUserCatalog()->FindTable(
      table_name.AsSpan(), table));
  ZETASQL_RET_CHECK_NE(*table, nullptr);
  return catalog_adapter->GetOrGenerateOidFromTableName(table_name);
}
// Arbitrary test constants for ExpandRelation test cases.
constexpr char kTableName[] = "keyvalue";
constexpr int kLocation = 12;
constexpr int kRTIndex = 4;
constexpr int kSublevelsUp = 2;

// Basic ExpandRelation test: no column aliases, verify both out args.
TEST_F(CatalogWrappersTest, ExpandRelationBasicTest) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  const zetasql::Table* table;
  ZETASQL_ASSERT_OK_AND_ASSIGN(Oid table_oid, GetTableAndOid(kTableName, &table));
  Alias* eref = makeAlias(kTableName, /*colnames=*/nullptr);

  List* colnames = nullptr;
  List* colvars = nullptr;
  ExpandRelationC(table_oid, eref, kRTIndex, kSublevelsUp, kLocation, &colnames,
                  &colvars);

  for (int i = 0; i < table->NumColumns(); ++i) {
    const zetasql::Column* column = table->GetColumn(i);
    ASSERT_NE(column, nullptr);
    // We don't expect to find Pseudo Columns, so exclude them.
    if (column->IsPseudoColumn()) {
      continue;
    }
    // We didn't provide column aliases, so these should match the catalog.
    ASSERT_LE(i, list_length(colnames));
    EXPECT_EQ(strVal(list_nth(colnames, i)), column->Name());
    Var* node = internal::PostgresCastNode(Var, list_nth(colvars, i));
    CatalogAdapter* catalog_adapter = nullptr;  // Separate due to b/17073013.
    ZETASQL_ASSERT_OK_AND_ASSIGN(catalog_adapter, GetCatalogAdapter());
    const Oid expected_type_oid =
        catalog_adapter->GetEngineSystemCatalog()
            ->GetTypeFromReverseMapping(column->GetType())
            ->PostgresTypeOid();
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const FormData_pg_type* pg_type,
        PgBootstrapCatalog::Default()->GetType(expected_type_oid));
    Var* expected =
        makeVar(kRTIndex, i + 1 /* 1-indexed */, expected_type_oid,
                pg_type->typtypmod, pg_type->typcollation, kSublevelsUp);
    expected->location = kLocation;
    EXPECT_TRUE(equal(node, expected))
        << "Vars don't match.\nExpected: " << nodeToString(expected)
        << "\nActual:   " << nodeToString(node);
  }
}

// ExpandRelation test with column aliases.
TEST_F(CatalogWrappersTest, ExpandRelationColumnAliasesTest) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  const zetasql::Table* table;
  ZETASQL_ASSERT_OK_AND_ASSIGN(Oid table_oid, GetTableAndOid(kTableName, &table));
  List* column_aliases = list_make2(makeString(pstrdup("key_alias")),
                                    makeString(pstrdup("value_alias")));
  Alias* eref = makeAlias(kTableName, column_aliases);

  List* colnames = nullptr;
  List* colvars = nullptr;
  ExpandRelationC(table_oid, eref, kRTIndex, kSublevelsUp, kLocation, &colnames,
                  &colvars);

  for (int i = 0; i < table->NumColumns(); ++i) {
    const zetasql::Column* column = table->GetColumn(i);
    ASSERT_NE(column, nullptr);
    // We don't expect to find Pseudo Columns, so exclude them.
    if (column->IsPseudoColumn()) {
      continue;
    }
    // Match column alises where available, column names otherwise.
    ASSERT_LE(i, list_length(colnames));
    if (i < list_length(column_aliases)) {
      EXPECT_STREQ(strVal(list_nth(colnames, i)),
                   strVal(list_nth(column_aliases, i)));
    } else {
      EXPECT_EQ(strVal(list_nth(colnames, i)), column->Name());
    }
    // We already tested column types in the basic, no-alias case above.
  }
}

// ExpandRelation test with no requested out args.
TEST_F(CatalogWrappersTest, ExpandRelationNullOutargsTest) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  const zetasql::Table* table;
  ZETASQL_ASSERT_OK_AND_ASSIGN(Oid table_oid, GetTableAndOid(kTableName, &table));
  Alias* eref = makeAlias(kTableName, /*colnames=*/nullptr);

  List** null_colnames = nullptr;
  List** null_colvars = nullptr;
  ExpandRelationC(table_oid, eref, kRTIndex, kSublevelsUp, kLocation,
                  null_colnames, null_colvars);

  // There's no output produced. Test is a PASS if it didn't throw or crash.
  GTEST_SUCCEED();
}

TEST(GetOpclass, DateOps) {
  const FormData_pg_opclass* date_ops_opclass =
      GetOpclassFromBootstrapCatalog(DATE_BTREE_OPS_OID);

  ASSERT_NE(date_ops_opclass, nullptr);
  EXPECT_STREQ(NameStr(date_ops_opclass->opcname), "date_ops");
}

TEST(GetOpclass, BTreeAccessMethod) {
  const Oid* oid_list;
  size_t oid_count;
  GetOpclassesByAccessMethodFromBootstrapCatalog(BTREE_AM_OID, &oid_list,
                                                 &oid_count);

  ASSERT_GT(oid_count, 0);
  ASSERT_NE(oid_list, nullptr);
  for (int i = 0; i < oid_count; ++i) {
    const FormData_pg_opclass* opclass =
        GetOpclassFromBootstrapCatalog(oid_list[i]);
    ASSERT_NE(opclass, nullptr);
    EXPECT_EQ(opclass->opcmethod, BTREE_AM_OID);
  }
}

TEST(GetLanguage, PgLanguage) {
  const FormData_pg_language* language =
      GetLanguageByNameFromBootstrapCatalog("sql");
  ASSERT_NE(language, nullptr);
  EXPECT_EQ(language->lanvalidator, F_FMGR_SQL_VALIDATOR);

  language = GetLanguageByNameFromBootstrapCatalog("invalid_language");
  EXPECT_EQ(language, nullptr);
}

TEST(GetAccessMethodOperator, Basic) {
  // Get a sample opfamily.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const FormData_pg_opclass* int8_opclass,
      PgBootstrapCatalog::Default()->GetOpclass(INT8_BTREE_OPS_OID));
  ASSERT_NE(int8_opclass, nullptr);
  Oid int8_opfamily = int8_opclass->opcfamily;
  const FormData_pg_amop* amop = GetAmopByFamilyFromBootstrapCatalog(
      int8_opfamily, /*lefttype=*/INT8OID, /*righttype=*/INT8OID,
      /*strategy=*/BTLessStrategyNumber);

  // Most properties are already verified by the bootstrap unit test. Just check
  // a sample.
  ASSERT_NE(amop, nullptr);
  EXPECT_EQ(amop->amopfamily, int8_opfamily);
  EXPECT_EQ(amop->amopopr, Int8LessOperator);
}

TEST(GetAccessMethodOperator, Failure) {
  const FormData_pg_amop* amop = GetAmopByFamilyFromBootstrapCatalog(
      InvalidOid, /*lefttype=*/INT8OID, /*righttype=*/INT8OID,
      /*strategy=*/BTLessStrategyNumber);

  // Lookup should fail and return nullptr but not crash or throw.
  EXPECT_EQ(amop, nullptr);
}

TEST(GetAccessMethodProcedure, Basic) {
  // Get a sample opfamily.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const FormData_pg_opclass* int8_opclass,
      PgBootstrapCatalog::Default()->GetOpclass(INT8_BTREE_OPS_OID));
  ASSERT_NE(int8_opclass, nullptr);
  Oid int8_opfamily = int8_opclass->opcfamily;
  const FormData_pg_amproc* amproc = GetAmprocByFamilyFromBootstrapCatalog(
      int8_opfamily, /*lefttype=*/INT8OID, /*righttype=*/INT8OID,
      /*index=*/BTORDER_PROC);

  // Most properties are already verified by the bootstrap unit test. Just check
  // a sample.
  ASSERT_NE(amproc, nullptr);
  EXPECT_EQ(amproc->amprocfamily, int8_opfamily);
  EXPECT_EQ(amproc->amprocnum, BTORDER_PROC);
}

TEST(GetAccessMethodProcedure, Failure) {
  const FormData_pg_amproc* amproc = GetAmprocByFamilyFromBootstrapCatalog(
      InvalidOid, /*lefttype=*/INT8OID, /*righttype=*/INT8OID,
      /*index=*/BTORDER_PROC);

  // Lookup should fail and return nullptr but not crash or throw.
  EXPECT_EQ(amproc, nullptr);
}

TEST_F(CatalogWrappersTest, GetsTableName) {
  const TableName kTableName({"my_table"});
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);
  // Teach the adapter about our table.
  CatalogAdapter* catalog_adapter = nullptr;  // Separate due to b/17073013.
  ZETASQL_ASSERT_OK_AND_ASSIGN(catalog_adapter, GetCatalogAdapter());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Oid my_oid, catalog_adapter->GetOrGenerateOidFromTableName(kTableName));

  EXPECT_EQ(
      GetOrGenerateOidFromTableNameC(kTableName.UnqualifiedName().c_str()),
      my_oid);
  EXPECT_STREQ(GetTableNameC(my_oid), "my_table");
  EXPECT_STREQ(GetNamespaceNameC(my_oid), "public");
}

TEST_F(CatalogWrappersTest, GetsTableNameInNamespace) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  CatalogAdapter* catalog_adapter = nullptr;  // Separate due to b/17073013.
  ZETASQL_ASSERT_OK_AND_ASSIGN(catalog_adapter, GetCatalogAdapter());

  // Register table names and oids with the catalog adapter.
  std::vector<std::string> name_in_namespace{"namespace_name", "rel_name"};
  std::vector<std::string> name_in_public_namespace{"public", "rel_name"};
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Oid table_oid,
      catalog_adapter->GetOrGenerateOidFromTableName(
          TableName(name_in_namespace)));
  Oid namespace_oid = GetOrGenerateOidFromNamespaceNameC("namespace_name");
  EXPECT_NE(namespace_oid, InvalidOid);
  EXPECT_NE(namespace_oid, table_oid);
  EXPECT_EQ(GetOrGenerateOidFromNamespaceOidAndRelationNameC(namespace_oid,
                                                             "rel_name"),
            table_oid);
  EXPECT_STREQ(GetTableNameC(table_oid), "rel_name");
  EXPECT_STREQ(GetNamespaceNameC(table_oid), "namespace_name");

  // We will output the name as originally registered through
  // GetOrGenerateOidFromTableName, even though we will always find the same
  // table oid for "public.rel_name" or "rel_name". Reverse translation from
  // googlesql will never trigger this case, because the table name we get from
  // googlesql will always be "rel_name", and never "public.rel_name".
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Oid table_oid_in_public,
      catalog_adapter->GetOrGenerateOidFromTableName(
          TableName(name_in_public_namespace)));

  EXPECT_STREQ(GetTableNameC(table_oid_in_public), "rel_name");
  EXPECT_STREQ(GetNamespaceNameC(table_oid_in_public), "public");
}

// Helper function to make a List* of T_String Value nodes from a vector of
// strings. Useful for building eref column lists.
List* MakeNameList(std::vector<const char*>& names) {
  List* list = NIL;
  for (auto& name : names) {
    list = lappend(list, makeString(pstrdup(name)));
  }
  ABSL_CHECK_EQ(list_length(list), names.size());
  return list;
}

// Modeled after transformFromClauseItem when the input is a join.
absl::StatusOr<ParseNamespaceItem*> BuildPartialJoinNSItem(
    ParseState* pstate, ParseNamespaceItem* l_nsitem,
    ParseNamespaceItem* r_nsitem) {
  List* l_colnames = l_nsitem->p_rte->eref->colnames;
  List* r_colnames = r_nsitem->p_rte->eref->colnames;

  // Build the final column name list.
  List* column_names = list_concat_copy(l_colnames, r_colnames);

  // Build the final ParseNamespaceColumn list.
  ParseNamespaceColumn* l_nscolumns = l_nsitem->p_nscolumns;
  ParseNamespaceColumn* r_nscolumns = r_nsitem->p_nscolumns;
  int num_lcols = list_length(l_colnames);
  int num_rcols = list_length(r_colnames);

  // This may be larger than needed, but it's not worth being exact.
  ParseNamespaceColumn* res_nscolumns = reinterpret_cast<ParseNamespaceColumn*>(
      palloc0((num_lcols + num_rcols) * sizeof(ParseNamespaceColumn)));

  for (int i = 0; i < num_lcols; ++i) {
    res_nscolumns[i] = l_nscolumns[i];
  }
  for (int i = 0; i < num_rcols; ++i) {
    res_nscolumns[i + num_lcols] = r_nscolumns[i];
  }

  ParseNamespaceItem* nsitem = addRangeTableEntryForJoin(
      pstate, column_names, res_nscolumns, JOIN_INNER,
      /*nummergedcols=*/0, /*aliasvars=*/nullptr, /*leftcols=*/nullptr,
      /*rightcols=*/nullptr, /*joinalias=*/nullptr, /*alias=*/nullptr,
      /*inFromCl=*/true);
  return nsitem;
}

// Sets up a Join between "keyvalue" and "keyvalue" by creating a
// ParseNamespaceItem for that Join, then calls ExpandNSItemVarsForJoinC on the
// ParseNamespaceItem verifies that pseudo columns in the underlying tables are
// excluded when asked.
TEST_F(CatalogWrappersTest, ExpandJoinTableToTable) {
  // Teach CatalogAdapter about our table (keyvalue).
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  // Build the "keyvalue" ParseNamespaceItem.
  ParseState* parse_state = make_parsestate(/*parentParseState=*/nullptr);
  RangeVar* rv = MakeRangeVar(kTableName);
  ParseNamespaceItem* table_nsitem =
      addRangeTableEntry(parse_state, rv, /*alias=*/nullptr,
                         /*inh=*/true, /*inFromCl=*/true);
  // "keyvalue" has two columns: key and value.
  ASSERT_EQ(list_length(table_nsitem->p_rte->eref->colnames), 2);

  // Build the join ParseNamespaceItem.
  ParseNamespaceItem* join_nsitem;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      join_nsitem,
      BuildPartialJoinNSItem(parse_state, table_nsitem, table_nsitem));
  // Expand the join.
  List* colnames = NIL;
  bool error;
  List* result = ExpandNSItemVarsForJoinC(parse_state->p_rtable, join_nsitem,
                                          /*sublevels_up=*/0,
                                          /*location=*/-1, &colnames, &error);
  ASSERT_FALSE(error);
}

// Verify we can look up (by name) a UDF and get a reasonable-looking proc.
TEST_F(CatalogWrappersTest, LooksUpUdfProcByName) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  const std::string kTvfName = "read_json_keyvalue_change_stream";
  const FormData_pg_proc** proc_list;
  size_t proc_count;
  GetProcsByName(kTvfName.c_str(), &proc_list, &proc_count);

  ASSERT_EQ(proc_count, 1);
  const FormData_pg_proc* proc = proc_list[0];
  // TODO: enable this check after making lookup case sensitive.
  // EXPECT_STREQ(NameStr(proc->proname), kTvfName.c_str());
  EXPECT_NE(proc->oid, InvalidOid);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const Oid namespace_id,
      PgBootstrapCatalog::Default()->GetNamespaceOid("spanner"));
  EXPECT_EQ(proc->pronamespace, namespace_id);
  EXPECT_EQ(proc->pronargs, 5);
  EXPECT_EQ(proc->prorettype, JSONBOID);
}

TEST_F(CatalogWrappersTest, LooksUpUdfProcBySchemaAndFuncNames) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  // `pg_catalog` namespace
  {
    const std::string kSchemaName = "pg_catalog";
    const std::string kUdfName = "now";
    const FormData_pg_proc** proc_list;
    size_t proc_count;
    GetProcsBySchemaAndFuncNames(kSchemaName.c_str(), kUdfName.c_str(),
                                 &proc_list, &proc_count);

    EXPECT_EQ(proc_count, 1);
    if (proc_count > 0) {
      const FormData_pg_proc* proc = proc_list[0];
      EXPECT_STREQ(NameStr(proc->proname), kUdfName.c_str());
      EXPECT_NE(proc->oid, InvalidOid);
      EXPECT_NE(proc->pronamespace, InvalidOid);
    }
  }
  // `spanner` namespace
  {
    const std::string kSchemaName = "spanner";
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const Oid namespace_id,
        PgBootstrapCatalog::Default()->GetNamespaceOid(kSchemaName));

    const std::string kUdfName = "bool_array";
    const FormData_pg_proc** proc_list;
    size_t proc_count;
    GetProcsBySchemaAndFuncNames(kSchemaName.c_str(), kUdfName.c_str(),
                                 &proc_list, &proc_count);

    EXPECT_EQ(proc_count, 1);
    if (proc_count > 0) {
      const FormData_pg_proc* proc = proc_list[0];
      EXPECT_STREQ(NameStr(proc->proname), kUdfName.c_str());
      EXPECT_NE(proc->oid, InvalidOid);
      EXPECT_EQ(proc->pronamespace, namespace_id);
    }
  }
}

// Verify we can look up (by oid) a UDF and get a reasonable-looking proc.
TEST_F(CatalogWrappersTest, LooksUpUdfProcByOid) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);
  const std::string kTvfName = "read_json_keyvalue_change_stream";
  const FormData_pg_proc** proc_list;
  size_t proc_count;
  GetProcsByName(kTvfName.c_str(), &proc_list, &proc_count);
  ASSERT_EQ(proc_count, 1);

  const FormData_pg_proc* proc = GetProcByOid(proc_list[0]->oid);

  ASSERT_NE(proc, nullptr);
  EXPECT_NE(proc->oid, InvalidOid);
  EXPECT_TRUE(proc->proretset);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const Oid namespace_id,
      PgBootstrapCatalog::Default()->GetNamespaceOid("spanner"));
  EXPECT_EQ(proc->pronamespace, namespace_id);
  EXPECT_EQ(proc->pronargs, 5);
  EXPECT_EQ(proc->prorettype, JSONBOID);
}

// Verify UDF lookup is case-sensitive.
TEST_F(CatalogWrappersTest, UdfLookUpCaseSensitive) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  const std::string kTvfNameCorrectCase = "read_json_keyvalue_change_stream";
  const std::string kTvfNameIncorrectCase = "read_json_keyvalue_CHANGE_STREAM";
  ASSERT_TRUE(
      absl::EqualsIgnoreCase(kTvfNameCorrectCase, kTvfNameIncorrectCase));
  const FormData_pg_proc** proc_list;
  size_t proc_count;
  // With the correct case, expect to find our UDF (no error throw).
  GetProcsByName(kTvfNameCorrectCase.c_str(), &proc_list, &proc_count);
  ASSERT_EQ(proc_count, 1);

  // With the incorrect case, we don't find it.
  GetProcsByName(kTvfNameIncorrectCase.c_str(), &proc_list, &proc_count);
  EXPECT_EQ(proc_count, 0);
}

TEST_F(CatalogWrappersTest, GetFunctionArgInfo) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  Oid * argtypes;
  char ** argnames;
  char * argmodes;

  // All arrays are correctly allocating memory and returning the right values.
  constexpr Oid jsonb_array_elements_oid = 3219;
  int nargs = GetFunctionArgInfo(jsonb_array_elements_oid, &argtypes,
                                 &argnames, &argmodes);
  ASSERT_EQ(nargs, 2);
  ASSERT_NE(argtypes, nullptr);
  EXPECT_EQ(argtypes[0], 3802);
  EXPECT_EQ(argtypes[1], 3802);
  ASSERT_NE(argnames, nullptr);
  EXPECT_STREQ(argnames[0], "from_json");
  EXPECT_STREQ(argnames[1], "value");
  ASSERT_NE(argmodes, nullptr);
  EXPECT_EQ(argmodes[0], FUNC_PARAM_IN);
  EXPECT_EQ(argmodes[1], FUNC_PARAM_OUT);

  // Empty arrays should be set to nullptr.
  constexpr Oid int84eq_oid = 474;
  nargs = GetFunctionArgInfo(int84eq_oid, &argtypes, &argnames, &argmodes);
  ASSERT_EQ(nargs, 2);
  ASSERT_NE(argtypes, nullptr);
  EXPECT_EQ(argtypes[0], 20);
  EXPECT_EQ(argtypes[1], 23);
  EXPECT_EQ(argnames, nullptr);
  EXPECT_EQ(argmodes, nullptr);

  // NotFound errors should return 0 and set the output pointers to nullptr.
  nargs = GetFunctionArgInfo(InvalidOid, &argtypes, &argnames, &argmodes);
  EXPECT_EQ(nargs, 0);
  EXPECT_EQ(argtypes, nullptr);
  EXPECT_EQ(argnames, nullptr);
  EXPECT_EQ(argmodes, nullptr);

  // UDFs rely on a lookup map that is constructed when creating pg_proc structs
  // which would happen during parsing. This call to GetProcsByName initializes
  // the map for this test.
  const std::string kTvfName = "read_json_keyvalue_change_stream";
  const FormData_pg_proc** proc_list;
  size_t proc_count;
  GetProcsByName(kTvfName.c_str(), &proc_list, &proc_count);

  ASSERT_EQ(proc_count, 1);
  const Oid read_json_keyvalue_change_stream_oid = proc_list[0]->oid;
  nargs = GetFunctionArgInfo(read_json_keyvalue_change_stream_oid, &argtypes,
                             &argnames, &argmodes);
  ASSERT_EQ(nargs, 5);
  ASSERT_NE(argtypes, nullptr);
  EXPECT_EQ(argtypes[0], 1184);
  EXPECT_EQ(argtypes[1], 1184);
  EXPECT_EQ(argtypes[2], 25);
  EXPECT_EQ(argtypes[3], 20);
  EXPECT_EQ(argtypes[4], 1009);
  ASSERT_NE(argnames, nullptr);
  EXPECT_STREQ(argnames[0], "start_timestamp");
  EXPECT_STREQ(argnames[1], "end_timestamp");
  EXPECT_STREQ(argnames[2], "partition_token");
  EXPECT_STREQ(argnames[3], "heartbeat_milliseconds");
  EXPECT_STREQ(argnames[4], "read_options");
  ASSERT_EQ(argmodes, nullptr);
}

}  // namespace
}  // namespace postgres_translator::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
