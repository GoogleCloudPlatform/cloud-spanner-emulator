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

#include "third_party/spanner_pg/shims/catalog_shim.h"

#include <ostream>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "third_party/spanner_pg/bootstrap_catalog/bootstrap_catalog.h"
#include "third_party/spanner_pg/catalog/catalog_adapter_holder.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/postgres_includes/deparser.h"
#include "third_party/spanner_pg/shims/catalog_shim_transforms.h"
#include "third_party/spanner_pg/shims/ereport_shim.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "third_party/spanner_pg/shims/parser_shim.h"
#include "third_party/spanner_pg/src/backend/catalog/pg_operator_d.h"
#include "third_party/spanner_pg/test_catalog/test_catalog.h"
#include "third_party/spanner_pg/util/nodetag_to_string.h"
#include "third_party/spanner_pg/util/pg_list_iterators.h"
#include "third_party/spanner_pg/util/postgres.h"
#include "third_party/spanner_pg/util/unittest_utils.h"
#include "third_party/spanner_pg/util/valid_memory_context_fixture.h"

namespace postgres_translator {
namespace {

using ::postgres_translator::spangres::test::GetSpangresTestAnalyzerOptions;
using ::postgres_translator::spangres::test::
    GetSpangresTestCatalogAdapterHolder;
using ::testing::UnorderedElementsAreArray;

List* MakeFunctionName(const char* namespace_name, const char* function_name) {
  if (namespace_name != nullptr) {
    return list_make2(makeString(pstrdup(namespace_name)),
                      makeString(pstrdup(function_name)));
  } else {
    return list_make1(makeString(pstrdup(function_name)));
  }
}

// Helper function to take a string name and get a PostgreSQL List* of names
// that places our name in the pg_catalog namespace. This is the default
// namespace used extensively in the analyzer.
// User's string is copied into the output.
List* MakeNamespacedName(const char* name) {
  return MakeFunctionName("pg_catalog", name);
}

// Helper function to create a Const node containing a specified int4 value (or
// null).
Const* MakeInt4Const(int value, bool isnull) {
  return makeConst(INT4OID, -1, /*constcollid=*/InvalidOid, sizeof(int32_t),
                   Int32GetDatum(value), isnull,
                   /*constbyval=*/true);
}

// Helper function to parse, analyze, and deparse a query to confirm that a SQL
// statement can be roundtripped.
void RoundTripQuery(const std::string& sql) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  // Parse and analyze the query.
  List* pg_parse_tree;
  ZETASQL_ASSERT_OK_AND_ASSIGN(pg_parse_tree,
                       spangres::test::ParseFromPostgres(sql));
  ASSERT_NE(pg_parse_tree, nullptr);
  Query* pg_query;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      pg_query, spangres::test::AnalyzeFromPostgresForTest(
                    sql, pg_parse_tree, GetSpangresTestAnalyzerOptions()));

  // Run the deparser.
  char* deparsed_query = deparse_query(pg_query, /*prettyPrint=*/false);
  EXPECT_EQ(deparsed_query, sql);
}

bool FunctionFound(const char* namespace_name, const char* function_name,
                   int nargs = 0) {
  List* funcname = MakeFunctionName(namespace_name, function_name);
  FuncCandidateList candidate_list = FuncnameGetCandidates(
      funcname, nargs, /*argnames=*/NIL, /*expand_variadic=*/false,
      /*expand_defaults=*/false, /*include_out_arguments=*/false,
      /*missing_ok=*/false);
  return candidate_list != nullptr;
}

absl::StatusOr<std::vector<Oid>> GetProcOids(absl::string_view name) {
  ZETASQL_ASSIGN_OR_RETURN(absl::Span<const FormData_pg_proc* const> procs,
                   PgBootstrapCatalog::Default()->GetProcsByName(name));

  std::vector<Oid> proc_oids;
  for (const FormData_pg_proc* proc : procs) {
    proc_oids.push_back(proc->oid);
  }
  return proc_oids;
}

// Test that we find a basic Int4->Int8 explicit coercion path.
TEST(CatalogShimTest, FindCoercionPathwayBasicTest) {
  CoercionContext ccontext = COERCION_EXPLICIT;
  Oid source_typeid = INT4OID;
  Oid target_typeid = INT8OID;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const FormData_pg_cast* pg_cast,
      PgBootstrapCatalog::Default()->GetCast(source_typeid, target_typeid));
  Oid expected_funcid = pg_cast->castfunc;

  Oid found_funcid = InvalidOid;
  CoercionPathType type = find_coercion_pathway(
      // NB: source and target arguments are opposite of the bootstrap_catalog
      // lookup function, and most people's intuition.
      target_typeid, source_typeid, ccontext, &found_funcid);

  EXPECT_EQ(type, COERCION_PATH_FUNC);
  EXPECT_EQ(found_funcid, expected_funcid);
}

// Test that we don't find an explicit Int4->Timestamp explicit coercion path.
TEST(CatalogShimTest, FindCoercionPathwayNegativeTest) {
  CoercionContext ccontext = COERCION_EXPLICIT;
  Oid source_typeid = INT4OID;
  Oid target_typeid = TIMESTAMPOID;

  using ::testing::Not;
  using ::zetasql_base::testing::IsOk;

  ASSERT_THAT(
      PgBootstrapCatalog::Default()->GetCast(source_typeid, target_typeid),
      Not(IsOk()));

  Oid expected_funcid = InvalidOid;
  Oid found_funcid;
  CoercionPathType type = find_coercion_pathway(
      // NB: source and target arguments are opposite of the bootstrap_catalog
      // lookup function, and most people's intuition.
      target_typeid, source_typeid, ccontext, &found_funcid);

  EXPECT_EQ(type, COERCION_PATH_NONE);
  EXPECT_EQ(found_funcid, expected_funcid);
}

// Trivial test case where the source and target are the same.
TEST(CatalogShimTest, CanCoerceTypeTrivialTest) {
  CoercionContext ccontext = COERCION_EXPLICIT;
  Oid typeoid = INT4OID;

  EXPECT_TRUE(can_coerce_type(/*nargs=*/1, &typeoid, &typeoid, ccontext));
}

// Like FindCoercionPathwayBasicTest above, but using the higher-level
// can_coerce_type API.
TEST(CatalogShimTest, CanCoerceTypeBasicTest) {
  CoercionContext ccontext = COERCION_EXPLICIT;
  Oid source_typeid = INT4OID;
  Oid target_typeid = INT8OID;

  EXPECT_TRUE(
      can_coerce_type(/*nargs=*/1, &source_typeid, &target_typeid, ccontext));
}

// Like FindCoercionPathwayNegativeTest above, but using the higher-level
// can_coerce_type API.
TEST(CatalogShimTest, CanCoerceTypeNegativeTest) {
  CoercionContext ccontext = COERCION_EXPLICIT;
  Oid source_typeid = INT4OID;
  Oid target_typeid = TIMESTAMPOID;

  EXPECT_FALSE(
      can_coerce_type(/*nargs=*/1, &source_typeid, &target_typeid, ccontext));
}

// Generics have separate and very specific coercion rules.
TEST(CatalogShimTest, CanCoerceTypeGenericsTest) {
  CoercionContext ccontext = COERCION_EXPLICIT;

  Oid source_regular = INT8OID;
  Oid target_regular = ANYELEMENTOID;
  EXPECT_TRUE(
      can_coerce_type(/*nargs=*/1, &source_regular, &target_regular, ccontext));

  Oid source_multi[2] = {INT8OID, INT8OID};
  Oid target_multi[2] = {ANYELEMENTOID, ANYELEMENTOID};
  EXPECT_TRUE(
      can_coerce_type(/*nargs=*/2, source_multi, target_multi, ccontext));

  Oid source_invalid[2] = {NUMERICOID, INT8OID};
  Oid target_invalid[2] = {ANYELEMENTOID, ANYELEMENTOID};
  EXPECT_FALSE(
      can_coerce_type(/*nargs=*/2, source_invalid, target_invalid, ccontext));
}

// Test that looping through arrays of types works.
TEST(CatalogShimTest, CanCoerceTypeArrayTest) {
  CoercionContext ccontext = COERCION_EXPLICIT;
  Oid source_typeids[] = {INT2OID, INT2OID, INT4OID};
  Oid target_typeids[] = {FLOAT8OID, INT8OID, INT8OID};

  EXPECT_TRUE(
      can_coerce_type(/*nargs=*/3, source_typeids, target_typeids, ccontext));
}

// Test that looping through arrays of types correctly finds an invalid case.
TEST(CatalogShimTest, CanCoerceTypeArrayFailureTest) {
  CoercionContext ccontext = COERCION_EXPLICIT;
  // Cannot explicitly cast INT4OID to TIMESTAMPOID
  Oid source_typeids[] = {INT2OID, INT4OID, INT4OID};
  Oid target_typeids[] = {INT4OID, TIMESTAMPOID, INT8OID};

  EXPECT_FALSE(
      can_coerce_type(/*nargs=*/3, source_typeids, target_typeids, ccontext));
}

using CatalogShimTestWithMemory =
    ::postgres_translator::test::ValidMemoryContext;
// Test types print as expected using the external format_type APIs which
// internally call our shimmed format_type_internal_spangres.
TEST_F(CatalogShimTestWithMemory, FormatTypeBasicTest) {
  EXPECT_STREQ(format_type_be(FLOAT4OID), "real");
  EXPECT_STREQ(format_type_be(FLOAT8OID), "double precision");
  EXPECT_STREQ(format_type_be(INT2OID), "smallint");
  EXPECT_STREQ(format_type_be(INT4OID), "integer");
  EXPECT_STREQ(format_type_be(INT8OID), "bigint");
  EXPECT_STREQ(format_type_be(NUMERICOID), "numeric");
  EXPECT_STREQ(format_type_be(TIMESTAMPOID), "timestamp without time zone");
  EXPECT_STREQ(format_type_be(TIMESTAMPTZOID), "timestamp with time zone");
  EXPECT_STREQ(format_type_be(VARCHAROID), "character varying");
  EXPECT_STREQ(format_type_with_typemod(NUMERICOID, 262150), "numeric(4,2)");
  EXPECT_STREQ(format_type_with_typemod(TIMESTAMPOID, 4),
               "timestamp(4) without time zone");
  EXPECT_STREQ(format_type_with_typemod(TIMESTAMPTZOID, 2),
               "timestamp(2) with time zone");
  EXPECT_STREQ(format_type_with_typemod(DATEOID, 2), "date(2)");
  EXPECT_STREQ(format_type_with_typemod(BITOID, 2), "bit(2)");
  EXPECT_STREQ(format_type_with_typemod(VARCHAROID, 20),
               "character varying(16)");
}

TEST_F(CatalogShimTestWithMemory, GenerateTypeHeapTupleTest) {
  Type tuple = PgTypeFormHeapTuple(INT4OID);

  // Run through most of the public HeapTuple API and verify it works.
  ASSERT_NE(tuple, nullptr);
  EXPECT_TRUE(HeapTupleIsValid(tuple));
  EXPECT_TRUE(HeapTupleHasNulls(tuple));
  EXPECT_FALSE(HeapTupleNoNulls(tuple));
  EXPECT_TRUE(HeapTupleHasVarWidth(tuple));
  EXPECT_FALSE(HeapTupleAllFixed(tuple));
  EXPECT_EQ(HeapTupleHeaderGetNatts(tuple->t_data), Natts_pg_type);

  // Check null attributes. Hard-coded based on pg_type.h.
  EXPECT_TRUE(heap_attisnull(tuple, Anum_pg_type_typdefaultbin, nullptr));
  EXPECT_TRUE(heap_attisnull(tuple, Anum_pg_type_typdefault, nullptr));
  EXPECT_TRUE(heap_attisnull(tuple, Anum_pg_type_typacl, nullptr));

  // Verify we can get the struct and a few basic properties. Struct contents
  // are already validated in bootstrap_catalog_test.
  FormData_pg_type* type_struct =
      reinterpret_cast<FormData_pg_type*>(GETSTRUCT(tuple));
  ASSERT_NE(type_struct, nullptr);
  EXPECT_EQ(type_struct->typtype, 'b');
  EXPECT_STREQ(NameStr(type_struct->typname), "int4");
  EXPECT_EQ(type_struct->oid, INT4OID);
}

TEST_F(CatalogShimTestWithMemory, GenerateTypeHeapTupleInvalidOidTest) {
  Type tuple = PgTypeFormHeapTuple(InvalidOid);

  ASSERT_EQ(tuple, nullptr);
  EXPECT_FALSE(HeapTupleIsValid(tuple));
}

// Simple test of typeidType. Properties of the returned HeapTuple are
// more fully tested elsewhere.
TEST_F(CatalogShimTestWithMemory, TypeidTypeTest) {
  const Type type = typeidType(INT4OID);

  ASSERT_NE(type, nullptr);
  EXPECT_TRUE(HeapTupleIsValid(type));
  FormData_pg_type* type_form =
      reinterpret_cast<FormData_pg_type*>(GETSTRUCT(type));
  EXPECT_EQ(type_form->oid, INT4OID);
}

// Simple test of LookupTypeNameExtended from an already-known Oid.
// Properties of the returned HeapTuple are more fully tested elsewhere.
TEST_F(CatalogShimTestWithMemory, LookupTypeNameWithOidTest) {
  const TypeName* type_name = makeTypeNameFromOid(INT4OID, /*typmod=*/-1);
  int32_t typmod_outarg;

  const Type type = LookupTypeNameExtended(
      /*pstate=*/nullptr, type_name, &typmod_outarg, /*temp_ok=*/false,
      /*missing_ok=*/false);

  ASSERT_NE(type, nullptr);
  EXPECT_EQ(typmod_outarg, -1);
  EXPECT_TRUE(HeapTupleIsValid(type));
  FormData_pg_type* type_form =
      reinterpret_cast<FormData_pg_type*>(GETSTRUCT(type));
  EXPECT_EQ(type_form->oid, INT4OID);
}

// Simple test of LookupTypeNameExtended from a name (Oid unknown).
// Properties of the returned HeapTuple are more fully tested elsewhere.
TEST_F(CatalogShimTestWithMemory, LookupTypeNameWithNameTest) {
  const TypeName* type_name = makeTypeName(pstrdup("int4"));
  int32_t typmod_outarg;

  const Type type = LookupTypeNameExtended(
      /*pstate=*/nullptr, type_name, &typmod_outarg, /*temp_ok=*/false,
      /*missing_ok=*/false);

  ASSERT_NE(type, nullptr);
  EXPECT_EQ(typmod_outarg, -1);
  EXPECT_TRUE(HeapTupleIsValid(type));
  FormData_pg_type* type_form =
      reinterpret_cast<FormData_pg_type*>(GETSTRUCT(type));
  EXPECT_EQ(type_form->oid, INT4OID);
}

// Simple test of LookupTypeNameExtended from a namespace-qualified name
// (Oid unknown). We don't support actual namespaces, but postgres adds them
// implicitly so this verifies that we correctly find the type. Properties of
// the returned HeapTuple are more fully tested elsewhere.
TEST_F(CatalogShimTestWithMemory, LookupTypeNameWithQualifiedNameTest) {
  // NB: Non-const because the PostgreSQL API requires that.
  List* name_list = MakeNamespacedName("int4");
  const TypeName* type_name = makeTypeNameFromNameList(name_list);
  int32_t typmod_outarg;

  const Type type = LookupTypeNameExtended(
      /*pstate=*/nullptr, type_name, &typmod_outarg, /*temp_ok=*/false,
      /*missing_ok=*/false);

  ASSERT_NE(type, nullptr);
  EXPECT_EQ(typmod_outarg, -1);
  EXPECT_TRUE(HeapTupleIsValid(type));
  FormData_pg_type* type_form =
      reinterpret_cast<FormData_pg_type*>(GETSTRUCT(type));
  EXPECT_EQ(type_form->oid, INT4OID);
}

// Simple test of LookupTypeNameExtended from an array type name.
// Properties of the returned HeapTuple are more fully tested elsewhere.
TEST_F(CatalogShimTestWithMemory, LookupArrayTypeNameTest) {
  TypeName* type_name = makeTypeName(pstrdup("int8"));
  // The actual value of this integer is irrelevant (pg ignores it). Anything
  // non-zero means "this is an array."
  type_name->arrayBounds = list_make1(makeInteger(5));
  int32_t typmod_outarg;

  const Type type = LookupTypeNameExtended(
      /*pstate=*/nullptr, type_name, &typmod_outarg, /*temp_ok=*/false,
      /*missing_ok=*/false);

  ASSERT_NE(type, nullptr);
  EXPECT_EQ(typmod_outarg, -1);
  EXPECT_TRUE(HeapTupleIsValid(type));
  FormData_pg_type* type_form =
      reinterpret_cast<FormData_pg_type*>(GETSTRUCT(type));
  EXPECT_EQ(type_form->oid, INT8ARRAYOID);
}

// Simple test of LookupTypeNameExtended from a namespace-unknown name
// (Oid unknown).
TEST_F(CatalogShimTestWithMemory, LookupTypeNameWithUnknownNamespaceTest) {
  // NB: Non-const because the PostgreSQL API requires that.
  List* name_list = MakeFunctionName("unknown_catalog", "int4");
  const TypeName* type_name = makeTypeNameFromNameList(name_list);
  int32_t typmod_outarg;

  const Type type = LookupTypeNameExtended(
      /*pstate=*/nullptr, type_name, &typmod_outarg, /*temp_ok=*/false,
      /*missing_ok=*/false);

  ASSERT_EQ(type, nullptr);
}

TEST_F(CatalogShimTestWithMemory, GenerateOperatorHeapTupleTest) {
  Operator tuple = PgOperatorFormHeapTuple(Int4EqualOperator);

  // Run through most of the public HeapTuple API and verify it works.
  ASSERT_NE(tuple, nullptr);
  EXPECT_TRUE(HeapTupleIsValid(tuple));
  EXPECT_FALSE(HeapTupleHasNulls(tuple));
  EXPECT_TRUE(HeapTupleNoNulls(tuple));
  EXPECT_FALSE(HeapTupleHasVarWidth(tuple));
  EXPECT_TRUE(HeapTupleAllFixed(tuple));

  // Verify we can get the struct and a few basic properties. Struct contents
  // are already validated in bootstrap_catalog_test.
  FormData_pg_operator* operator_struct =
      reinterpret_cast<FormData_pg_operator*>(GETSTRUCT(tuple));
  EXPECT_EQ(operator_struct->oid, Int4EqualOperator);
  EXPECT_EQ(HeapTupleHeaderGetNatts(tuple->t_data), Natts_pg_operator);

  ASSERT_NE(operator_struct, nullptr);
  EXPECT_EQ(operator_struct->oprkind, 'b');
  EXPECT_TRUE(operator_struct->oprcanmerge);
  EXPECT_TRUE(operator_struct->oprcanhash);
  EXPECT_STREQ(NameStr(operator_struct->oprname), "=");
}

TEST_F(CatalogShimTestWithMemory, GenerateOperatorHeapTupleInvalidOidTest) {
  Operator tuple = PgOperatorFormHeapTuple(InvalidOid);

  ASSERT_EQ(tuple, nullptr);
  EXPECT_FALSE(HeapTupleIsValid(tuple));
}

TEST_F(CatalogShimTestWithMemory, BuildCoercionExpressionTest) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  // Make a placeholder node as the input so we can check for it in the output
  // args.
  Node* placeholder_node = makeBoolConst(/*value=*/false, /*isnull=*/false);

  constexpr Oid input_type = INT4OID;
  constexpr Oid output_type = INT8OID;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const FormData_pg_cast* cast_data,
      PgBootstrapCatalog::Default()->GetCast(input_type, output_type));

  Node* expression_tree = build_coercion_expression(
      placeholder_node, COERCION_PATH_FUNC, cast_data->castfunc, output_type,
      /*targetTypMod=*/-1, COERCION_EXPLICIT, COERCE_EXPLICIT_CAST,
      /*location=*/-1);

  ASSERT_NE(expression_tree, nullptr);
  ASSERT_EQ(NodeTagToString(expression_tree->type), "T_FuncExpr");
  FuncExpr* expr = internal::PostgresCastNode(FuncExpr, expression_tree);
  EXPECT_EQ(expr->funcid, cast_data->castfunc);
  EXPECT_EQ(expr->funcresulttype, output_type);
  EXPECT_EQ(expr->funcformat, COERCE_EXPLICIT_CAST);
  EXPECT_EQ(linitial_node(Const, expr->args),
            internal::PostgresCastNode(Const, placeholder_node));
  EXPECT_EQ(list_length(expr->args), 1);
}

TEST_F(CatalogShimTestWithMemory, GetAttnameTest) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  // clang-format off
  ZETASQL_ASSERT_OK_AND_ASSIGN(CatalogAdapter* catalog_adapter, GetCatalogAdapter());
  // clang-format on

  // Teach the catalog adapter about our table.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Oid table_oid,
      catalog_adapter->GetOrGenerateOidFromTableName(TableName({"keyvalue"})));

  // KeyValue table has attributes, "key" and "value".
  EXPECT_STREQ(get_attname(table_oid, 1, /*missing_ok=*/false), "key");
  EXPECT_STREQ(get_attname(table_oid, 2, /*missing_ok=*/false), "value");
  // KeyValue table has only 2 attributes. Higher attnums should return nullptr.
  EXPECT_EQ(get_attname(table_oid, 0, /*missing_ok=*/true), nullptr);
  EXPECT_EQ(get_attname(table_oid, 4, /*missing_ok=*/true), nullptr);
  // InvalidOid table does not exist. Should get nullptr for all attributes.
  EXPECT_EQ(get_attname(InvalidOid, 1, /*missing_ok=*/true), nullptr);
  EXPECT_EQ(get_attname(InvalidOid, 0, /*missing_ok=*/true), nullptr);
  EXPECT_EQ(get_attname(InvalidOid, 10, /*missing_ok=*/true), nullptr);
  EXPECT_EQ(get_attname(InvalidOid, -1, /*missing_ok=*/true), nullptr);
  EXPECT_EQ(get_attname(InvalidOid, -2, /*missing_ok=*/true), nullptr);
}

// Tests the fmgr_info_cxt_security shim function via the more public
// fmgr_info API.
TEST_F(CatalogShimTestWithMemory, FmgrInfoTest) {
  FmgrInfo fmgr_info_struct;
  // Use the boolean equals function as our test function id.
  constexpr char bool_eq_function_name[] = "booleq";
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<Oid> function_ids,
                       GetProcOids(bool_eq_function_name));
  const Oid function_id = function_ids[0];  // Any id will do. Just use first.

  fmgr_info(function_id, &fmgr_info_struct);

  EXPECT_NE(fmgr_info_struct.fn_addr, nullptr);
  EXPECT_EQ(fmgr_info_struct.fn_oid, function_id);
  EXPECT_EQ(fmgr_info_struct.fn_nargs, 2);
  EXPECT_TRUE(fmgr_info_struct.fn_strict);
  EXPECT_FALSE(fmgr_info_struct.fn_retset);
  EXPECT_EQ(fmgr_info_struct.fn_stats, TRACK_FUNC_ALL);
  EXPECT_EQ(fmgr_info_struct.fn_mcxt, CurrentMemoryContext);
  EXPECT_EQ(fmgr_info_struct.fn_extra, nullptr);
  EXPECT_EQ(fmgr_info_struct.fn_expr, nullptr);
}

// Tests fmgr_info for a bad lookup. This throws an exception.
TEST_F(CatalogShimTestWithMemory, FmgrInfoNegativeTest) {
  FmgrInfo fmgr_info_struct;
  bool caught_exception = false;

  try {
    fmgr_info(/*functionId=*/InvalidOid, &fmgr_info_struct);
  } catch (PostgresEreportException exception) {
    caught_exception = true;
    EXPECT_STREQ(exception.what(),
                 "[ERROR] Unknown function or function is not a built-in: 0");
  }

  EXPECT_TRUE(caught_exception);
  // fn_oid must be InvalidOid if the function failed to indicate that FmgrInfo
  // is invalid.
  EXPECT_EQ(fmgr_info_struct.fn_oid, InvalidOid);
}

// Tests that OpernameGetCandidates produces a list of expected size and
// contents for a given operator name.
TEST_F(CatalogShimTestWithMemory, OpernameGetCandidatesTest) {
  List* name_list = MakeNamespacedName("%");
  const char oprkind = 'b';  // binary operator
  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Span<const Oid> expected_oids,
                       PgBootstrapCatalog::Default()->GetOperatorOids("%"));

  FuncCandidateList candidate_list =
      OpernameGetCandidates(name_list, oprkind, /*missing_schema_ok=*/false);

  // Verify that we get the expected candidate operators for '%' and that the
  // operator's properties match bootstrap_catalog.
  std::vector<Oid> found_oids;
  for (FuncCandidateList candidate = candidate_list; candidate != nullptr;
       candidate = candidate->next) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const FormData_pg_operator* operator_form,
        PgBootstrapCatalog::Default()->GetOperator(candidate->oid));
    found_oids.push_back(candidate->oid);
    EXPECT_EQ(candidate->nargs, 2);
    EXPECT_EQ(candidate->nvargs, 0);
    EXPECT_EQ(candidate->ndargs, 0);
    EXPECT_EQ(candidate->argnumbers, nullptr);
    EXPECT_EQ(candidate->args[0], operator_form->oprleft);
    EXPECT_EQ(candidate->args[1], operator_form->oprright);
  }
  EXPECT_THAT(found_oids, UnorderedElementsAreArray(expected_oids));
}

// Test that OpernameGetCandidates produces an empty list from a
// namespace-unknown name.
TEST_F(CatalogShimTestWithMemory,
       OpernameGetCandidatesWithUnknownNamespaceTest) {
  List* name_list = MakeFunctionName("unknown_catalog", "%");
  const char oprkind = 'b';  // binary operator

  FuncCandidateList candidate_list =
      OpernameGetCandidates(name_list, oprkind, /*missing_schema_ok=*/false);

  ASSERT_EQ(candidate_list, nullptr);
}

// Test that OpernameGetOprid finds an exact match for int4 equals operator.
TEST_F(CatalogShimTestWithMemory, OprnameGetOpridTest) {
  List* name_list = MakeNamespacedName("=");
  const Oid left_arg = INT4OID;
  const Oid right_arg = INT4OID;

  EXPECT_EQ(OpernameGetOprid(name_list, left_arg, right_arg),
            Int4EqualOperator);
}

// Test that OpernameGetOprid finds no match for equality between text and int4.
TEST_F(CatalogShimTestWithMemory, OprnameGetOpridNegativeTest) {
  List* name_list = MakeNamespacedName("=");
  const Oid left_arg = TEXTOID;
  const Oid right_arg = INT4OID;

  EXPECT_EQ(OpernameGetOprid(name_list, left_arg, right_arg), InvalidOid);
}

// Test that OpernameGetOprid finds no match for int4 equals operator with an
// invalid namespace.
TEST_F(CatalogShimTestWithMemory, OprnameGetOpridWithUnknownNamespaceTest) {
  List* name_list = MakeFunctionName("unknown_catalog", "=");
  const Oid left_arg = INT4OID;
  const Oid right_arg = INT4OID;

  EXPECT_EQ(OpernameGetOprid(name_list, left_arg, right_arg), InvalidOid);
}

// Test that oper() produces the expected operator for exactly matching args.
TEST_F(CatalogShimTestWithMemory, OperEqualsTest) {
  List* name_list = MakeNamespacedName("=");
  const Oid left_arg = INT4OID;
  const Oid right_arg = INT4OID;

  Operator op = oper(/*pstate=*/nullptr, name_list, left_arg, right_arg,
                     /*noError=*/false, /*location=*/-1);

  ASSERT_TRUE(HeapTupleIsValid(op));

  FormData_pg_operator* operator_struct =
      reinterpret_cast<FormData_pg_operator*>(GETSTRUCT(op));
  EXPECT_EQ(operator_struct->oid, Int4EqualOperator);
}

// Test that oper() produces the expected operator when coercion is required to
// match arg types.
TEST_F(CatalogShimTestWithMemory, OperCoerceTest) {
  List* name_list = MakeNamespacedName("%");
  const Oid left_arg = INT4OID;
  const Oid right_arg = INT8OID;

  Operator op = oper(/*pstate=*/nullptr, name_list, left_arg, right_arg,
                     /*noError=*/false, /*location=*/-1);

  ASSERT_TRUE(HeapTupleIsValid(op));

  FormData_pg_operator* operator_struct =
      reinterpret_cast<FormData_pg_operator*>(GETSTRUCT(op));
  EXPECT_EQ(operator_struct->oid, 439);
  // Verify this is the (Int8, Int8) modulus operator. This ensures the test
  // isn't invalidated by someone later adding an (Int4,Int8) version.
  auto oper_form = reinterpret_cast<const FormData_pg_operator*>(GETSTRUCT(op));
  EXPECT_EQ(oper_form->oprleft, INT8OID);
  EXPECT_EQ(oper_form->oprright, INT8OID);
}

// Tests for oper() failure modes.

TEST_F(CatalogShimTestWithMemory, OperErrorFailureTest) {
  List* name_list = MakeNamespacedName("=");
  const Oid left_arg = INT4OID;
  const Oid right_arg = TEXTOID;

  // There is no INT4 = TEXT operator and coercion cannot come up with one.
  // Verify that with noError=false, it throws an error.
  EXPECT_THROW(oper(/*pstate=*/nullptr, name_list, left_arg, right_arg,
                    /*noError=*/false, /*location=*/-1),
               PostgresEreportException);
}

TEST_F(CatalogShimTestWithMemory, OperNoErrorFailureTest) {
  List* name_list = MakeNamespacedName("=");
  const Oid left_arg = INT4OID;
  const Oid right_arg = TEXTOID;

  // There is no INT4 = TEXT operator and coercion cannot come up with one.
  // Verify that with noError=true, we just get an invalid pointer back (no
  // throw).
  Operator op = oper(/*pstate=*/nullptr, name_list, left_arg, right_arg,
                     /*noError=*/true, /*location=*/-1);

  EXPECT_FALSE(HeapTupleIsValid(op));
}

// Test that left_oper() produces the expected operator for exactly matching
// args.
TEST_F(CatalogShimTestWithMemory, LeftOperNegateTest) {
  List* name_list = MakeNamespacedName("-");
  const Oid right_arg = INT4OID;

  Operator oper = left_oper(/*pstate=*/nullptr, name_list, right_arg,
                            /*noError=*/false, /*location=*/-1);

  ASSERT_TRUE(HeapTupleIsValid(oper));
  FormData_pg_operator* operator_struct =
      reinterpret_cast<FormData_pg_operator*>(GETSTRUCT(oper));
  EXPECT_EQ(operator_struct->oid, 558);  // Int4 Negate from pg_operator.h.
}

// Test that left_oper() produces the expected operator when coercion is
// required to match arg types.
TEST_F(CatalogShimTestWithMemory, LeftOperCoerceTest) {
  char operator_name_string[] = "|/";
  List* name_list = MakeNamespacedName(operator_name_string);
  const Oid right_arg = INT4OID;

  Operator oper = left_oper(/*pstate=*/nullptr, name_list, right_arg,
                            /*noError=*/false, /*location=*/-1);

  ASSERT_TRUE(HeapTupleIsValid(oper));
  FormData_pg_operator* operator_struct =
      reinterpret_cast<FormData_pg_operator*>(GETSTRUCT(oper));
  EXPECT_EQ(operator_struct->oid, 596);  // Float8 Square Root.
  auto oper_form =
      reinterpret_cast<const FormData_pg_operator*>(GETSTRUCT(oper));
  EXPECT_EQ(oper_form->oprleft, 0);
  EXPECT_EQ(oper_form->oprright, FLOAT8OID);
}

// Tests for left_oper() failure modes.

TEST_F(CatalogShimTestWithMemory, LeftOperErrorFailureTest) {
  List* name_list = MakeNamespacedName("-");
  const Oid right_arg = TEXTOID;

  // There is no - TEXT operator and coercion cannot come up with one.
  // Verify that with noError=false, it throws an error.
  EXPECT_THROW(left_oper(/*pstate=*/nullptr, name_list, right_arg,
                         /*noError=*/false, /*location=*/-1),
               PostgresEreportException);
}

TEST_F(CatalogShimTestWithMemory, LeftOperNoErrorFailureTest) {
  List* name_list = MakeNamespacedName("-");
  const Oid right_arg = TEXTOID;

  // There is no - TEXT operator and coercion cannot come up with one.
  // Verify that with noError=true, we just get an invalid pointer back (no
  // throw).
  Operator oper = left_oper(/*pstate=*/nullptr, name_list, right_arg,
                            /*noError=*/true, /*location=*/-1);

  EXPECT_FALSE(HeapTupleIsValid(oper));
}

TEST_F(CatalogShimTestWithMemory, GetTypeInputInfoSpangresBool) {
  Oid typInput;
  Oid typIOParam;
  getTypeInputInfo(BOOLOID, &typInput, &typIOParam);
  EXPECT_EQ(typInput, 1242);  // boolin
  EXPECT_EQ(typIOParam, BOOLOID);
}

TEST_F(CatalogShimTestWithMemory, GetTypeInputInfoSpangresBoolArray) {
  Oid typInput;
  Oid typIOParam;
  // Oid(_bool) = 1000
  getTypeInputInfo(/*type=*/1000, &typInput, &typIOParam);
  EXPECT_EQ(typInput, 750);  // array_in
  EXPECT_EQ(typIOParam, BOOLOID);
}

TEST_F(CatalogShimTestWithMemory, GetTypeInputInfoSpangresUnsupported) {
  Oid typInput;
  Oid typIOParam;
  bool caught_exception = false;
  try {
    getTypeInputInfo(InvalidOid, &typInput, &typIOParam);
  } catch (PostgresEreportException exception) {
    caught_exception = true;
    EXPECT_STREQ(exception.what(), "[ERROR] type id 0 is unsupported");
  }
  EXPECT_TRUE(caught_exception);
}

// Most cases return InvalidOid because they aren't composite types. Rows of
// system tables are the only currently-supported complex types.
TEST(CatalogShimTest, TypeidTypeRelid) {
  EXPECT_EQ(typeidTypeRelid(INT4OID), InvalidOid);
  EXPECT_EQ(typeidTypeRelid(REGPROCOID), InvalidOid);
  EXPECT_EQ(typeidTypeRelid(TEXTOID), InvalidOid);
  EXPECT_EQ(typeidTypeRelid(OIDVECTOROID), InvalidOid);
  EXPECT_EQ(typeidTypeRelid(TypeRelation_Rowtype_Id), TypeRelationId);
}

// When a Type can't be found, expect a throw.
TEST(CatalogShimTest, TypeidTypeRelidInvalid) {
  EXPECT_THROW(typeidTypeRelid(InvalidOid), PostgresEreportException);
}

TEST(CatalogShimTest, GetTypType) {
  EXPECT_EQ(get_typtype(INT4OID), TYPTYPE_BASE);
  EXPECT_EQ(get_typtype(TEXTOID), TYPTYPE_BASE);
  EXPECT_EQ(get_typtype(OIDVECTOROID), TYPTYPE_BASE);
  EXPECT_EQ(get_typtype(TypeRelation_Rowtype_Id), TYPTYPE_COMPOSITE);
  EXPECT_EQ(get_typtype(UNKNOWNOID), TYPTYPE_PSEUDO);
  EXPECT_EQ(get_typtype(INT4RANGEOID), TYPTYPE_RANGE);
  EXPECT_EQ(get_typtype(InvalidOid), '\0');
}

TEST_F(CatalogShimTestWithMemory, RelationColumnNamesTest) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  // clang-format off
  ZETASQL_ASSERT_OK_AND_ASSIGN(CatalogAdapter* catalog_adapter, GetCatalogAdapter());
  // clang-format on

  // Register a table name and oid with the catalog adapter.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Oid table_oid,
      catalog_adapter->GetOrGenerateOidFromTableName(TableName({"KeyValue"})));

  // Build a RangeTblEntry representing the KeyValue table, not using
  // any alias:
  RangeTblEntry* rte = makeNode(RangeTblEntry);
  rte->rtekind = RTE_RELATION;
  rte->relid = table_oid;
  rte->eref = makeAlias("KeyValue", NIL);

  deparse_namespace dpns;
  memset(&dpns, 0, sizeof(deparse_namespace));
  deparse_columns* column_info =
      reinterpret_cast<deparse_columns*>(palloc0(sizeof(deparse_columns)));
  set_relation_column_names(&dpns, rte, column_info);

  // column_info should be filled with real column names as in the Spangres
  // test catalog:
  int num_cols = 2;
  EXPECT_EQ(column_info->num_cols, num_cols);
  EXPECT_EQ(column_info->num_new_cols, num_cols);
  EXPECT_EQ(column_info->printaliases, false);

  EXPECT_STREQ(column_info->colnames[0], "key");
  EXPECT_STREQ(column_info->new_colnames[0], "key");
  EXPECT_STREQ(column_info->colnames[1], "value");
  EXPECT_STREQ(column_info->new_colnames[1], "value");
}

TEST_F(CatalogShimTestWithMemory, RelationColumnNamesWithAliasTest) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  // clang-format off
  ZETASQL_ASSERT_OK_AND_ASSIGN(CatalogAdapter* catalog_adapter, GetCatalogAdapter());
  // clang-format on

  // Register a table name and oid with the catalog adapter.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Oid table_oid,
      catalog_adapter->GetOrGenerateOidFromTableName(TableName({"KeyValue"})));

  // Build a RangeTblEntry representing the KeyValue table, also mimick
  // user-specified aliases:
  RangeTblEntry* rte = makeNode(RangeTblEntry);
  rte->rtekind = RTE_RELATION;
  rte->relid = table_oid;
  rte->eref = makeAlias("KeyValue", NIL);

  List* colnames =
      list_make3(makeString(pstrdup("k")), makeString(pstrdup("v")),
                 makeString(pstrdup("e")));

  rte->alias = makeAlias("KeyValue", colnames);

  deparse_namespace dpns;
  memset(&dpns, 0, sizeof(deparse_namespace));
  deparse_columns* column_info =
      reinterpret_cast<deparse_columns*>(palloc0(sizeof(deparse_columns)));
  set_relation_column_names(&dpns, rte, column_info);

  // column_info should be filled with column aliases
  int num_cols = 2;
  EXPECT_EQ(column_info->num_cols, num_cols);
  EXPECT_EQ(column_info->num_new_cols, num_cols);
  EXPECT_EQ(column_info->printaliases, true);

  EXPECT_STREQ(column_info->colnames[0], "k");
  EXPECT_STREQ(column_info->new_colnames[0], "k");
  EXPECT_STREQ(column_info->colnames[1], "v");
  EXPECT_STREQ(column_info->new_colnames[1], "v");
}

TEST_F(CatalogShimTestWithMemory, FuncnameGetCandidatesSum) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  List* funcname = MakeNamespacedName("sum");

  FuncCandidateList candidate_list = FuncnameGetCandidates(
      funcname, /*nargs=*/1, /*argnames=*/NIL, /*expand_variadic=*/false,
      /*expand_defaults=*/false, /*include_out_arguments=*/false,
      /*missing_ok=*/false);

  EXPECT_NE(candidate_list, nullptr);
  // Loop through the list and make sure each entry has the right name and args.
  // Also get the selected Oids for comparison to expected.
  std::vector<Oid> found_oids;
  for (FuncCandidateList list_entry = candidate_list; list_entry != nullptr;
       list_entry = list_entry->next) {
    EXPECT_EQ(list_entry->pathpos, 0);  // Not used by Spangres.
    found_oids.push_back(list_entry->oid);
    EXPECT_EQ(list_entry->nargs, 1);
    EXPECT_EQ(list_entry->nvargs, 0);
    EXPECT_EQ(list_entry->ndargs, 0);
    EXPECT_EQ(list_entry->argnumbers, nullptr);
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const FormData_pg_proc* proc_data,
        PgBootstrapCatalog::Default()->GetProc(list_entry->oid));
    EXPECT_EQ(list_entry->args[0], proc_data->proargtypes.values[0]);
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<Oid> expected_oids, GetProcOids("sum"));
  EXPECT_THAT(found_oids, UnorderedElementsAreArray(expected_oids));
}

TEST_F(CatalogShimTestWithMemory, FuncnameGetCandidatesNamespace) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  // No namespace, defaults to pg_catalog.
  EXPECT_TRUE(FunctionFound(/*namespace_name=*/nullptr, "count"));

  // Explicitly state pg_catalog.
  EXPECT_TRUE(FunctionFound("pg_catalog", "count"));

  // Explicitly state a different catalog.
  EXPECT_FALSE(FunctionFound("public", "count"));

  // Explicitly state an invalid catalog.
  EXPECT_FALSE(FunctionFound("unknown_catalog", "count"));

  // Spanner catalog function with an explicit catalog.
  EXPECT_TRUE(FunctionFound("spanner", "pending_commit_timestamp"));
  EXPECT_TRUE(FunctionFound("spanner", "bit_reverse", /*nargs=*/-1));
  EXPECT_TRUE(FunctionFound("spanner", "get_internal_sequence_state",
                            /*nargs=*/-1));

  // Missing catalog for Spanner function.
  EXPECT_FALSE(
      FunctionFound(/*namespace_name=*/nullptr, "pending_commit_timestamp"));
  EXPECT_FALSE(FunctionFound(/*namespace_name=*/nullptr, "bit_reverse"));
  EXPECT_FALSE(
      FunctionFound(/*namespace_name=*/nullptr, "get_internal_sequence_state"));

  // Incorrect catalog for Spanner function.
  EXPECT_FALSE(FunctionFound("pg_catalog", "pending_commit_timestamp"));
  EXPECT_FALSE(FunctionFound("pg_catalog", "bit_reverse"));
  EXPECT_FALSE(FunctionFound("pg_catalog", "get_internal_sequence_state"));

}

TEST_F(CatalogShimTestWithMemory, FuncGetDetailInt4Minus) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);
  // This value comes from pg_proc.h entry for Int4 Unary Minus. It is not
  // defined as a code constant for us to reference.
  constexpr Oid int4um_oid = 212;

  // In arguments.
  List* funcname = MakeNamespacedName("int4um");
  List* fargs = list_make1(MakeInt4Const(-1, /*isnull*/ false));
  Oid argtypes[] = {INT4OID};

  // Out arguments.
  Oid funcid;
  Oid rettype;
  bool retset;
  int nvargs;
  Oid vatype;
  Oid* true_typeids;
  List* argdefaults;

  FuncDetailCode code =
      func_get_detail(funcname, fargs, /*fargnames=*/NIL, /*nargs=*/1, argtypes,
                      /*expand_variadic=*/false, /*expand_defaults=*/false,
                      /*include_out_arguments=*/false, &funcid, &rettype,
                      &retset, &nvargs, &vatype, &true_typeids, &argdefaults);

  EXPECT_EQ(code, FUNCDETAIL_NORMAL);
  EXPECT_EQ(funcid, int4um_oid);
  EXPECT_EQ(rettype, INT4OID);
  EXPECT_FALSE(retset);
  EXPECT_EQ(nvargs, 0);
  EXPECT_EQ(vatype, InvalidOid);
  EXPECT_EQ(true_typeids[0], INT4OID);
  EXPECT_EQ(argdefaults, nullptr);
}

TEST_F(CatalogShimTestWithMemory, FuncGetDetailSum) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);
  // This value comes from pg_proc.h entry for the sum of Int4s. It is not
  // defined as a code constant for us to reference.
  constexpr Oid int_sum_oid = 2108;

  // In arguments.
  List* funcname = MakeNamespacedName("sum");
  List* fargs = list_make1(MakeInt4Const(1, /*isnull*/ false));
  Oid argtypes[] = {INT4OID};

  // Out arguments.
  Oid funcid;
  Oid rettype;
  bool retset;
  int nvargs;
  Oid vatype;
  Oid* true_typeids;
  List* argdefaults;

  FuncDetailCode code =
      func_get_detail(funcname, fargs, /*fargnames=*/NIL, /*nargs=*/1, argtypes,
                      /*expand_variadic=*/false, /*expand_defaults=*/false,
                      /*include_out_arguments=*/false, &funcid, &rettype,
                      &retset, &nvargs, &vatype, &true_typeids, &argdefaults);

  EXPECT_EQ(code, FUNCDETAIL_AGGREGATE);
  EXPECT_EQ(funcid, int_sum_oid);
  EXPECT_EQ(rettype, INT8OID);
  EXPECT_FALSE(retset);
  EXPECT_EQ(nvargs, 0);
  EXPECT_EQ(vatype, InvalidOid);
  EXPECT_EQ(true_typeids[0], INT4OID);
  EXPECT_EQ(argdefaults, nullptr);
}

TEST_F(CatalogShimTestWithMemory, ParseFuncOrColumn) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);
  // Set up inputs for "SELECT sum(1)" test query.
  ParseState* pstate = make_parsestate(/*parentParseState=*/nullptr);
  pstate->p_expr_kind = EXPR_KIND_SELECT_TARGET;
  List* funcname = MakeNamespacedName("sum");
  List* fargs = list_make1(MakeInt4Const(1, /*isnull=*/false));
  const int location = -1;  // "Unknown location"
  FuncCall* fn = makeFuncCall(funcname, fargs, COERCE_EXPLICIT_CALL, location);

  // This calling convention (passing fields redundantly) matches the real
  // PostgreSQL call we are supporting. It is redundant for functions but other
  // cases (column names) use these fields differently.
  Node* tree =
      ParseFuncOrColumn(pstate, fn->funcname, fargs, pstate->p_last_srf, fn,
                        /*proc_call=*/false, fn->location);

  EXPECT_NE(tree, nullptr);
  Aggref* aggref = internal::PostgresCastNode(Aggref, tree);
  ASSERT_NE(aggref, nullptr);
  // This value comes from pg_proc.h entry for the sum of Int4s. It is not
  // defined as a code constant for us to reference.
  constexpr Oid int_sum_oid = 2108;
  EXPECT_EQ(aggref->aggfnoid, int_sum_oid);
  EXPECT_EQ(aggref->aggtype, INT8OID);
  EXPECT_EQ(list_nth_oid(aggref->aggargtypes, 0), INT4OID);
  EXPECT_EQ(aggref->aggfilter, nullptr);
  EXPECT_FALSE(aggref->aggstar);
  EXPECT_FALSE(aggref->aggvariadic);
  EXPECT_EQ(aggref->aggkind, AGGKIND_NORMAL);
  EXPECT_EQ(aggref->location, location);
}

TEST_F(CatalogShimTestWithMemory, ParseFuncOrColumnError) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);
  // Set up inputs for "SELECT no_such_function(1)" test query.
  ParseState* pstate = make_parsestate(/*parentParseState=*/nullptr);
  pstate->p_expr_kind = EXPR_KIND_SELECT_TARGET;
  List* funcname = MakeNamespacedName("no_such_function");
  List* fargs = list_make1(MakeInt4Const(1, /*isnull=*/false));
  FuncCall* fn =
      makeFuncCall(funcname, fargs, COERCE_EXPLICIT_CALL, /*location=*/-1);

  // When fn is not NULL, this should throw on failure.
  EXPECT_THROW(
      ParseFuncOrColumn(pstate, fn->funcname, fargs, pstate->p_last_srf, fn,
                        /*proc_call=*/false, fn->location),
      PostgresEreportException);
}

TEST_F(CatalogShimTestWithMemory, ParseFuncOrColumnNull) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);
  // Set up inputs for "SELECT no_such_function(1)" test query.
  ParseState* pstate = make_parsestate(/*parentParseState=*/nullptr);
  pstate->p_expr_kind = EXPR_KIND_SELECT_TARGET;
  List* funcname = MakeNamespacedName("no_such_function");
  List* fargs = list_make1(MakeInt4Const(1, /*isnull=*/false));
  FuncCall* fn = nullptr;

  // When fn is nullptr, this should return nullptr and not throw on failure.
  Node* node =
      ParseFuncOrColumn(pstate, funcname, fargs, pstate->p_last_srf, fn,
                        /*proc_call=*/false, /*location=*/-1);

  EXPECT_EQ(node, nullptr);
}

// catalog_shim_cc_wrappers_test already covers standard functionality so we'll
// just test error behavior here.
TEST_F(CatalogShimTestWithMemory, ExpandRelationError) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  Alias* eref = makeAlias("no-such-table", /*colnames=*/nullptr);
  // Arbitrary test constants.
  constexpr int kLocation = 12;
  constexpr int kRTIndex = 4;
  constexpr int kSublevelsUp = 2;

  List* colnames = nullptr;
  List* colvars = nullptr;
  EXPECT_THROW(
      expandRelation(InvalidOid, eref, kRTIndex, kSublevelsUp, kLocation,
                     /*include_dropped=*/false, &colnames, &colvars),
      PostgresEreportException);

  EXPECT_EQ(colnames, nullptr);
  EXPECT_EQ(colvars, nullptr);
}

TEST(CatalogShimTest, InternalGetResultType) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  // Unused in-arguments.
  Node* call_expr = nullptr;
  ReturnSetInfo* rsinfo = nullptr;
  // Out-arguments.
  Oid resultTypeId = InvalidOid;
  TupleDesc resultTupleDesc = nullptr;

  EXPECT_EQ(internal_get_result_type(/*funcid=*/F_TEXTLEN, call_expr, rsinfo,
                                     &resultTypeId, &resultTupleDesc),
            TYPEFUNC_SCALAR);
  EXPECT_EQ(resultTypeId, INT4OID);
}

// RECORD return types are unsupported. Verify we throw and do not crash.
TEST_F(CatalogShimTestWithMemory, InternalGetResultTypeRecord) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<Oid> func_ids, GetProcOids("aclexplode"));
  ASSERT_EQ(func_ids.size(), 1);
  // In-arguments.
  Oid func_id = func_ids[0];
  Node* call_expr = nullptr;
  ReturnSetInfo* rsinfo = nullptr;
  // Out-arguments.
  Oid resultTypeId = InvalidOid;
  TupleDesc resultTupleDesc = nullptr;

  EXPECT_THROW(internal_get_result_type(func_id, call_expr, rsinfo,
                                        &resultTypeId, &resultTupleDesc),
               PostgresEreportException);
}

// Polymorphic return types are unsupported. Verify we throw and do not crash.
TEST_F(CatalogShimTestWithMemory, InternalGetResultTypeArray) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<Oid> func_ids, GetProcOids("array_append"));
  ASSERT_EQ(func_ids.size(), 1);
  // In-arguments.
  Oid func_id = func_ids[0];
  Node* call_expr = nullptr;
  ReturnSetInfo* rsinfo = nullptr;
  // Out-arguments.
  Oid resultTypeId = InvalidOid;
  TupleDesc resultTupleDesc = nullptr;

  EXPECT_THROW(internal_get_result_type(func_id, call_expr, rsinfo,
                                        &resultTypeId, &resultTupleDesc),
               PostgresEreportException);
}

// All types are binary coercible to themselves.
TEST_F(CatalogShimTestWithMemory, IsBinaryCoercibleSameType) {
  EXPECT_TRUE(IsBinaryCoercible(INT4OID, INT4OID));
  EXPECT_TRUE(IsBinaryCoercible(INT8OID, INT8OID));
  EXPECT_TRUE(IsBinaryCoercible(TEXTOID, TEXTOID));
}

// Any type is binary coercible to ANYOID.
TEST_F(CatalogShimTestWithMemory, IsBinaryCoercibleToAny) {
  EXPECT_TRUE(IsBinaryCoercible(TEXTOID, ANYOID));
  EXPECT_TRUE(IsBinaryCoercible(DATEOID, ANYOID));
  EXPECT_TRUE(IsBinaryCoercible(ANYOID, ANYOID));
}

// Other coercible types are catalog-dependent and are looked up in pg_cast.
TEST_F(CatalogShimTestWithMemory, IsBinaryCoercibleCast) {
  EXPECT_TRUE(IsBinaryCoercible(INT4OID, OIDOID));
  EXPECT_TRUE(IsBinaryCoercible(TEXTOID, VARCHAROID));
  EXPECT_TRUE(IsBinaryCoercible(VARCHAROID, TEXTOID));
}

// Test some negative cases.
TEST_F(CatalogShimTestWithMemory, IsBinaryCoercibleNegative) {
  EXPECT_FALSE(IsBinaryCoercible(OIDOID, INT8OID));
  EXPECT_FALSE(IsBinaryCoercible(OIDOID, INT4OID));
  EXPECT_FALSE(IsBinaryCoercible(TEXTOID, OIDOID));
  EXPECT_FALSE(IsBinaryCoercible(TEXTOID, CHAROID));
  EXPECT_FALSE(IsBinaryCoercible(INT8OID, TEXTOID));
}

TEST_F(CatalogShimTestWithMemory, GetDefaultOpClass) {
  Oid opclass_oid = GetDefaultOpClass(INT4OID, BTREE_AM_OID);

  ASSERT_NE(opclass_oid, InvalidOid);
  ZETASQL_ASSERT_OK_AND_ASSIGN(const FormData_pg_opclass* opclass,
                       PgBootstrapCatalog::Default()->GetOpclass(opclass_oid));
  EXPECT_EQ(opclass->opcintype, INT4OID);
  EXPECT_EQ(opclass->opcmethod, BTREE_AM_OID);
  EXPECT_STREQ(NameStr(opclass->opcname), "int4_ops");
}

TEST_F(CatalogShimTestWithMemory, GetOpclassFamily) {
  EXPECT_EQ(get_opclass_family(OID_BTREE_OPS_OID), OID_BTREE_FAM_OID);
  EXPECT_EQ(get_opclass_family(INT4_BTREE_OPS_OID), INTEGER_BTREE_FAM_OID);
  EXPECT_EQ(get_opclass_family(INT8_BTREE_OPS_OID), INTEGER_BTREE_FAM_OID);
  // Negative case.
  EXPECT_THROW(get_opclass_family(InvalidOid), PostgresEreportException);
}

TEST_F(CatalogShimTestWithMemory, GetOpclassInputType) {
  EXPECT_EQ(get_opclass_input_type(OID_BTREE_OPS_OID), OIDOID);
  EXPECT_EQ(get_opclass_input_type(INT4_BTREE_OPS_OID), INT4OID);
  EXPECT_EQ(get_opclass_input_type(INT8_BTREE_OPS_OID), INT8OID);
  // Negative case.
  EXPECT_THROW(get_opclass_input_type(InvalidOid), PostgresEreportException);
}

TEST_F(CatalogShimTestWithMemory, GetOpfamilyMember) {
  // Lookup an opfamily to use in this test.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const FormData_pg_opclass* int4_opclass,
      PgBootstrapCatalog::Default()->GetOpclass(INT4_BTREE_OPS_OID));
  ASSERT_NE(int4_opclass, nullptr);
  Oid int4_opfamily = int4_opclass->opcfamily;

  EXPECT_EQ(get_opfamily_member(int4_opfamily, /*lefttype=*/INT4OID,
                                /*righttype=*/INT4OID,
                                /*strategy=*/BTLessStrategyNumber),
            Int4LessOperator);
  EXPECT_EQ(get_opfamily_member(int4_opfamily, /*lefttype=*/INT4OID,
                                /*righttype=*/INT4OID,
                                /*strategy=*/BTEqualStrategyNumber),
            Int4EqualOperator);
}

TEST_F(CatalogShimTestWithMemory, GetOpfamilyMemberFailure) {
  // Lookup an opfamily to use in this test.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const FormData_pg_opclass* int4_opclass,
      PgBootstrapCatalog::Default()->GetOpclass(INT4_BTREE_OPS_OID));
  ASSERT_NE(int4_opclass, nullptr);
  Oid int4_opfamily = int4_opclass->opcfamily;

  // There is no operator in the in4 family with Text inputs.
  EXPECT_EQ(get_opfamily_member(int4_opfamily, /*lefttype=*/TEXTOID,
                                /*righttype=*/TEXTOID,
                                /*strategy=*/BTLessStrategyNumber),
            InvalidOid);
}

TEST_F(CatalogShimTestWithMemory, GetOrderingOpProperties) {
  // Get a sample operator oid for an operator with a btree interpretation.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const Oid int8_lt_oid,
      PgBootstrapCatalog::Default()->GetOperatorOidByOprLeftRight("<", INT8OID,
                                                                  INT8OID));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const FormData_pg_opclass* int8_opclass,
      PgBootstrapCatalog::Default()->GetOpclass(INT8_BTREE_OPS_OID));

  Oid opfamily, opcintype;
  int16_t strategy;

  EXPECT_TRUE(get_ordering_op_properties(int8_lt_oid, &opfamily, &opcintype,
                                         &strategy));
  EXPECT_EQ(opfamily, int8_opclass->opcfamily);
  EXPECT_EQ(opcintype, int8_opclass->opcintype);
  EXPECT_EQ(strategy, BTLessStrategyNumber);
}

TEST_F(CatalogShimTestWithMemory, GetOrderingOpPropertiesFailure) {
  Oid opfamily, opcintype;
  int16_t strategy;
  EXPECT_FALSE(
      get_ordering_op_properties(InvalidOid, &opfamily, &opcintype, &strategy));
  EXPECT_EQ(opfamily, InvalidOid);
  EXPECT_EQ(opcintype, InvalidOid);
  EXPECT_EQ(strategy, 0);
}

TEST_F(CatalogShimTestWithMemory, GetOpfamilyProc) {
  // Lookup an opfamily to use in this test.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const FormData_pg_opclass* int4_opclass,
      PgBootstrapCatalog::Default()->GetOpclass(INT4_BTREE_OPS_OID));
  ASSERT_NE(int4_opclass, nullptr);
  Oid int4_opfamily = int4_opclass->opcfamily;

  Oid proc_oid = get_opfamily_proc(int4_opfamily, /*lefttype=*/INT4OID,
                                   /*righttype=*/INT4OID,
                                   /*procnum=*/BTORDER_PROC);

  // Find the expected function Oid.
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<Oid> expected_proc_oids,
                       GetProcOids("btint4cmp"));
  ASSERT_EQ(expected_proc_oids.size(), 1);
  EXPECT_EQ(proc_oid, expected_proc_oids[0]);
}

TEST_F(CatalogShimTestWithMemory, GetsOpBTreeInterpretationOperator) {
  // Get a sample operator oid for an operator with a btree interpretation.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const Oid int8_eq_oid,
      PgBootstrapCatalog::Default()->GetOperatorOidByOprLeftRight("=", INT8OID,
                                                                  INT8OID));

  List* list = get_op_btree_interpretation(int8_eq_oid);
  for (int i = 0; i < list_length(list); ++i) {
    OpBtreeInterpretation* interpret =
        (OpBtreeInterpretation*)list->elements[i].ptr_value;
    ZETASQL_ASSERT_OK_AND_ASSIGN(const FormData_pg_amop* int8_eq_amop,
                         PgBootstrapCatalog::Default()->GetAmopByFamily(
                             interpret->opfamily_id, interpret->oplefttype,
                             interpret->oprighttype, interpret->strategy));
    // EXPECT_EQ(int8_eq_amop->amopmethod, BTREE_AM_OID);
    // EXPECT_EQ(int8_eq_amop->amopopr, int8_eq_oid);
    EXPECT_GE(int8_eq_amop->amopstrategy, 1);
    EXPECT_LE(int8_eq_amop->amopstrategy, 5);
  }
}

TEST_F(CatalogShimTestWithMemory, GetsOpBTreeInterpretationNegator) {
  // Get a sample operator oid where only the negation has a btree
  // interpretation.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const Oid int8_ne_oid,
      PgBootstrapCatalog::Default()->GetOperatorOidByOprLeftRight("<>", INT8OID,
                                                                  INT8OID));
  Oid int8_eq_oid = get_negator(int8_ne_oid);

  List* list = get_op_btree_interpretation(int8_ne_oid);
  for (int i = 0; i < list_length(list); ++i) {
    OpBtreeInterpretation* interpret =
        (OpBtreeInterpretation*)list->elements[i].ptr_value;
    // When the interpretation comes from the negator the strategy is set to
    // ROWCOUNT_NE which will not be found in the catalog. The lookup needs to
    // use the original strategy of the negator.
    constexpr int kInt8EqStrategy = 3;  // from pg_operator.dat.
    ZETASQL_ASSERT_OK_AND_ASSIGN(const FormData_pg_amop* int8_eq_amop,
                         PgBootstrapCatalog::Default()->GetAmopByFamily(
                             interpret->opfamily_id, interpret->oplefttype,
                             interpret->oprighttype, kInt8EqStrategy));
    EXPECT_EQ(int8_eq_amop->amopmethod, BTREE_AM_OID);
    EXPECT_EQ(int8_eq_amop->amopopr, int8_eq_oid);

    constexpr int kRowCountNeStrategy = 6;  // from primnodes.h
    EXPECT_EQ(interpret->strategy, kRowCountNeStrategy);
  }
}

TEST_F(CatalogShimTestWithMemory, GetsOpBTreeInterpretationNil) {
  // Get a sample operator oid for an operator without a btree interpretation.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const Oid xid_eq_oid,
      PgBootstrapCatalog::Default()->GetOperatorOidByOprLeftRight("=", XIDOID,
                                                                  XIDOID));
  List* list = get_op_btree_interpretation(xid_eq_oid);
  EXPECT_EQ(list_length(list), 0);

  // Invalid case.
  list = get_op_btree_interpretation(InvalidOid);
  EXPECT_EQ(list_length(list), 0);
}

TEST_F(CatalogShimTestWithMemory, GetOpfamilyProcFailure) {
  // Lookup an opfamily to use in this test.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const FormData_pg_opclass* int4_opclass,
      PgBootstrapCatalog::Default()->GetOpclass(INT4_BTREE_OPS_OID));
  ASSERT_NE(int4_opclass, nullptr);
  Oid int4_opfamily = int4_opclass->opcfamily;

  // There is no BTree order proc in the in4 family with Text inputs.
  EXPECT_EQ(get_opfamily_proc(int4_opfamily, /*lefttype=*/TEXTOID,
                              /*righttype=*/TEXTOID,
                              /*procnum=*/BTORDER_PROC),
            InvalidOid);
}

TEST(CatalogShimTest, GetOpcode) {
  EXPECT_EQ(get_opcode(BooleanEqualOperator), F_BOOLEQ);
  EXPECT_EQ(get_opcode(BooleanNotEqualOperator), F_BOOLNE);
  EXPECT_EQ(get_opcode(Int4LessOperator), F_INT4LT);
}

TEST_F(CatalogShimTestWithMemory, LookupTypeCache) {
  const Oid test_typeid = INT4OID;
  // Set all the flags to request all fields get populated.
  // Flags are defined in typecache.h.
  const int flags =
      TYPECACHE_EQ_OPR | TYPECACHE_LT_OPR | TYPECACHE_GT_OPR |
      TYPECACHE_CMP_PROC | TYPECACHE_HASH_PROC | TYPECACHE_EQ_OPR_FINFO |
      TYPECACHE_CMP_PROC_FINFO | TYPECACHE_HASH_PROC_FINFO | TYPECACHE_TUPDESC |
      TYPECACHE_BTREE_OPFAMILY | TYPECACHE_HASH_OPFAMILY |
      TYPECACHE_RANGE_INFO | TYPECACHE_DOMAIN_BASE_INFO |
      TYPECACHE_DOMAIN_CONSTR_INFO | TYPECACHE_HASH_EXTENDED_PROC |
      TYPECACHE_HASH_EXTENDED_PROC_FINFO;

  TypeCacheEntry* cache_entry = lookup_type_cache(test_typeid, flags);

  ASSERT_NE(cache_entry, nullptr);
  EXPECT_EQ(cache_entry->type_id, test_typeid);
  // These values come from pg_type for int4.
  ZETASQL_ASSERT_OK_AND_ASSIGN(const FormData_pg_type* pg_type,
                       PgBootstrapCatalog::Default()->GetType(test_typeid));
  EXPECT_EQ(cache_entry->typlen, 4);
  EXPECT_EQ(cache_entry->typbyval, pg_type->typbyval);
  EXPECT_EQ(cache_entry->typalign, pg_type->typalign);
  EXPECT_EQ(cache_entry->typstorage, pg_type->typstorage);
  EXPECT_EQ(cache_entry->typtype, pg_type->typtype);
  EXPECT_EQ(cache_entry->typrelid, pg_type->typrelid);
  EXPECT_EQ(cache_entry->typelem, pg_type->typelem);
  // These values come from pg_opfamily for the default BTREE operator class for
  // our type.
  const Oid btree_opclass_id = GetDefaultOpClass(test_typeid, BTREE_AM_OID);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const FormData_pg_opclass* btree_opclass,
      PgBootstrapCatalog::Default()->GetOpclass(btree_opclass_id));
  EXPECT_EQ(cache_entry->btree_opf, btree_opclass->opcfamily);
  EXPECT_EQ(cache_entry->btree_opintype, btree_opclass->opcintype);
  // These values come from the pg_opfamily for the default HASH operator class
  // for our type.
  const Oid hash_opclass_id = GetDefaultOpClass(test_typeid, HASH_AM_OID);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const FormData_pg_opclass* hash_opclass,
      PgBootstrapCatalog::Default()->GetOpclass(hash_opclass_id));
  EXPECT_EQ(cache_entry->hash_opf, hash_opclass->opcfamily);
  EXPECT_EQ(cache_entry->hash_opintype, hash_opclass->opcintype);
  // Equal, less than, and greater than operators come from BTREE operator
  // families for our type. Ditto the procedures for these.
  EXPECT_EQ(
      cache_entry->eq_opr,
      get_opfamily_member(cache_entry->btree_opf, cache_entry->btree_opintype,
                          cache_entry->btree_opintype, BTEqualStrategyNumber));
  EXPECT_EQ(
      cache_entry->lt_opr,
      get_opfamily_member(cache_entry->btree_opf, cache_entry->btree_opintype,
                          cache_entry->btree_opintype, BTLessStrategyNumber));
  EXPECT_EQ(cache_entry->gt_opr,
            get_opfamily_member(
                cache_entry->btree_opf, cache_entry->btree_opintype,
                cache_entry->btree_opintype, BTGreaterStrategyNumber));
  EXPECT_EQ(
      cache_entry->cmp_proc,
      get_opfamily_proc(cache_entry->btree_opf, cache_entry->btree_opintype,
                        cache_entry->btree_opintype, BTORDER_PROC));
  EXPECT_EQ(cache_entry->hash_proc,
            get_opfamily_proc(cache_entry->hash_opf, cache_entry->hash_opintype,
                              cache_entry->hash_opintype, HASHSTANDARD_PROC));
  EXPECT_EQ(cache_entry->hash_extended_proc,
            get_opfamily_proc(cache_entry->hash_opf, cache_entry->hash_opintype,
                              cache_entry->hash_opintype, HASHEXTENDED_PROC));
  // Other fields are mostly not applicable to Spangres because they deal with
  // unsupported type categories.
  EXPECT_EQ(cache_entry->tupDesc, nullptr);
  EXPECT_EQ(cache_entry->tupDesc_identifier, 0);
  EXPECT_EQ(cache_entry->rngelemtype, nullptr);
  EXPECT_EQ(cache_entry->rng_collation, InvalidOid);
  EXPECT_EQ(cache_entry->rng_cmp_proc_finfo.fn_addr, nullptr);
  EXPECT_EQ(cache_entry->rng_cmp_proc_finfo.fn_oid, InvalidOid);
  EXPECT_EQ(cache_entry->rng_canonical_finfo.fn_addr, nullptr);
  EXPECT_EQ(cache_entry->rng_canonical_finfo.fn_oid, InvalidOid);
  EXPECT_EQ(cache_entry->rng_subdiff_finfo.fn_addr, nullptr);
  EXPECT_EQ(cache_entry->rng_subdiff_finfo.fn_oid, InvalidOid);
  EXPECT_EQ(cache_entry->domainBaseType, InvalidOid);
  // NB: Here 0 is invalid instead of the usual -1 for typmods.
  EXPECT_EQ(cache_entry->domainBaseTypmod, 0);
  EXPECT_EQ(cache_entry->domainData, nullptr);
  EXPECT_EQ(cache_entry->enumData, nullptr);
  EXPECT_EQ(cache_entry->nextDomain, nullptr);
}

// Common LookupTypeCache behavior is tested in the preceding test case. Just
// verify the array-specifc behavior.
TEST_F(CatalogShimTestWithMemory, LookupTypeCacheForArray) {
  const Oid test_typeid = INT8ARRAYOID;
  // Flags relevant to arrays are: equality, greater/less than, compare, hash,
  // and hash extended.
  const int flags = TYPECACHE_EQ_OPR | TYPECACHE_LT_OPR | TYPECACHE_GT_OPR |
                    TYPECACHE_CMP_PROC | TYPECACHE_HASH_PROC |
                    TYPECACHE_HASH_EXTENDED_PROC;

  TypeCacheEntry* cache_entry = lookup_type_cache(test_typeid, flags);

  ASSERT_NE(cache_entry, nullptr);
  EXPECT_EQ(cache_entry->type_id, test_typeid);
  // int8_t supports all the operations we requested, so the array should too:
  EXPECT_NE(cache_entry->eq_opr, InvalidOid);
  EXPECT_NE(cache_entry->lt_opr, InvalidOid);
  EXPECT_NE(cache_entry->gt_opr, InvalidOid);
  EXPECT_NE(cache_entry->cmp_proc, InvalidOid);
  EXPECT_NE(cache_entry->hash_proc, InvalidOid);
  EXPECT_NE(cache_entry->hash_extended_proc, InvalidOid);
}

TEST(GetRangeSubtypeTest, NonRange) {
  // These are not ranges and should always return InvalidOid.
  EXPECT_EQ(get_range_subtype(INT4OID), InvalidOid);
  EXPECT_EQ(get_range_subtype(InvalidOid), InvalidOid);
  EXPECT_EQ(get_range_subtype(TypeRelationId), InvalidOid);
}

TEST(GetRangeSubtypeTest, RangeType) {
  // These are range types but Spangres doesn't support those yet. They should
  // be InvalidOid now, but when range support is added they should reflect the
  // subtype.
  EXPECT_EQ(get_range_subtype(INT4RANGEOID), InvalidOid);
  EXPECT_EQ(get_range_subtype(INT8RANGEARRAYOID), InvalidOid);
  EXPECT_EQ(get_range_subtype(ANYRANGEOID), InvalidOid);
}

TEST_F(CatalogShimTestWithMemory, GetNamespaceOidMissingOk) {
  EXPECT_EQ(get_namespace_oid("namespace_name", /*missing_ok=*/true),
            InvalidOid);
}

TEST_F(CatalogShimTestWithMemory, GetNamespaceOidMissingNotOk) {
  EXPECT_THROW(get_namespace_oid("namespace_name", /*missing_ok=*/false),
               PostgresEreportException);
}

TEST_F(CatalogShimTestWithMemory, GetNamespaceOidPopulated) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  CatalogAdapter* catalog_adapter = nullptr;  // Separate due to b/17073013.
  ZETASQL_ASSERT_OK_AND_ASSIGN(catalog_adapter, GetCatalogAdapter());

  // Register a table name and oid with the catalog adapter.
  std::vector<std::string> name_in_namespace{"namespace_name", "rel_name"};
  ZETASQL_ASSERT_OK_AND_ASSIGN(Oid table_oid,
                       catalog_adapter->GetOrGenerateOidFromTableName(
                           TableName(name_in_namespace)));
  EXPECT_NE(get_namespace_oid("namespace_name", /*missing_ok=*/false),
            InvalidOid);
  EXPECT_NE(get_namespace_oid("namespace_name", /*missing_ok=*/false),
            table_oid);
  EXPECT_NE(get_namespace_oid("public", /*missing_ok=*/false), InvalidOid);
  EXPECT_NE(get_namespace_oid("namespace_name", /*missing_ok=*/false),
            InvalidOid);
}

TEST_F(CatalogShimTestWithMemory, GenerateRelationName) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  CatalogAdapter* catalog_adapter = nullptr;  // Separate due to b/17073013.
  ZETASQL_ASSERT_OK_AND_ASSIGN(catalog_adapter, GetCatalogAdapter());

  // Register table names and oids with the catalog adapter.
  std::vector<std::string> name_in_namespace{"namespace_name", "rel_name"};
  std::vector<std::string> name_need_quote_in_namespace{"Namespace_name",
                                                        "Rel_name"};
  std::vector<std::string> name_in_public_namespace{"public", "rel_name"};
  ZETASQL_ASSERT_OK_AND_ASSIGN(Oid table_oid_in_namespace,
                       catalog_adapter->GetOrGenerateOidFromTableName(
                           TableName(name_in_namespace)));
  ZETASQL_ASSERT_OK_AND_ASSIGN(Oid table_oid_need_quote_in_namespace,
                       catalog_adapter->GetOrGenerateOidFromTableName(
                           TableName(name_need_quote_in_namespace)));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Oid table_oid_not_in_namespace,
      catalog_adapter->GetOrGenerateOidFromTableName(TableName({"KeyValue"})));
  ZETASQL_ASSERT_OK_AND_ASSIGN(Oid table_oid_in_public_namespace,
                       catalog_adapter->GetOrGenerateOidFromTableName(
                           TableName(name_in_public_namespace)));

  EXPECT_STREQ(generate_relation_name(table_oid_in_namespace, NIL),
               "namespace_name.rel_name");
  EXPECT_STREQ(generate_relation_name(table_oid_need_quote_in_namespace, NIL),
               "\"Namespace_name\".\"Rel_name\"");
  EXPECT_STREQ(generate_relation_name(table_oid_not_in_namespace, NIL),
               "\"KeyValue\"");
  // "public" is in the simulated search path, so it is omitted.
  EXPECT_STREQ(generate_relation_name(table_oid_in_public_namespace, NIL),
               "rel_name");

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Oid table_oid_not_qualified,
      catalog_adapter->GetOrGenerateOidFromTableName(TableName({"rel_name"})));
  EXPECT_EQ(table_oid_not_qualified, table_oid_in_public_namespace);
}

TEST_F(CatalogShimTestWithMemory, GetDatabaseOidMissingOk) {
  EXPECT_EQ(get_database_oid("database_name", /*missing_ok=*/true), InvalidOid);
}

TEST_F(CatalogShimTestWithMemory, GetDatabaseOidMissingNotOk) {
  EXPECT_THROW(get_database_oid("database_name", /*missing_ok=*/false),
               PostgresEreportException);
}

TEST(OperatorCommutatorTest, GetsCommutator) {
  // Equals commutes with itself.
  EXPECT_EQ(get_commutator(Int4EqualOperator), Int4EqualOperator);
  // Array Concat has no commutator.
  constexpr Oid kArrayConcatOperatorOid = 349;  // from pg_operator.dat.
  EXPECT_EQ(get_commutator(kArrayConcatOperatorOid), InvalidOid);
}

TEST(OperatorCommutatorTest, ReturnsInvalid) {
  EXPECT_EQ(get_commutator(InvalidOid), InvalidOid);
}

TEST(OperatorNegatorTest, GetsNegator) {
  // Negated negator is equal to original.
  EXPECT_EQ(get_negator(get_negator(Int4EqualOperator)), Int4EqualOperator);

  // Array Concat has no negator.
  constexpr Oid kArrayConcatOperatorOid = 349;  // from pg_operator.dat.
  EXPECT_EQ(get_negator(kArrayConcatOperatorOid), InvalidOid);
}

TEST(OperatorNegatorTest, ReturnsInvalid) {
  EXPECT_EQ(get_negator(InvalidOid), InvalidOid);
}

TEST(CatalogShimTest, FindTypmodCoercionFunction) {
  Oid funcid = InvalidOid;

  // Basic types that don't need a coercion function.
  EXPECT_EQ(find_typmod_coercion_function(INT8OID, &funcid),
            COERCION_PATH_NONE);
  EXPECT_EQ(funcid, InvalidOid);
  EXPECT_EQ(find_typmod_coercion_function(BOOLOID, &funcid),
            COERCION_PATH_NONE);
  EXPECT_EQ(funcid, InvalidOid);
  EXPECT_EQ(find_typmod_coercion_function(FLOAT8OID, &funcid),
            COERCION_PATH_NONE);
  EXPECT_EQ(funcid, InvalidOid);
  EXPECT_EQ(find_typmod_coercion_function(TEXTOID, &funcid),
            COERCION_PATH_NONE);
  EXPECT_EQ(funcid, InvalidOid);

  // Basic types that need a coercion function.
  EXPECT_EQ(find_typmod_coercion_function(VARCHAROID, &funcid),
            COERCION_PATH_FUNC);
  EXPECT_EQ(funcid, 669);
  EXPECT_EQ(find_typmod_coercion_function(TIMESTAMPOID, &funcid),
            COERCION_PATH_FUNC);
  EXPECT_EQ(funcid, 1961);

  // Array types that don't need a coercion function.
  EXPECT_EQ(find_typmod_coercion_function(TEXTARRAYOID, &funcid),
            COERCION_PATH_NONE);
  EXPECT_EQ(funcid, InvalidOid);

  // Array types that need a coercion function.
  EXPECT_EQ(find_typmod_coercion_function(TIMESTAMPARRAYOID, &funcid),
            COERCION_PATH_ARRAYCOERCE);
  EXPECT_EQ(funcid, 1961);
}

TEST_F(CatalogShimTestWithMemory, SimpleDelete) {
  const std::string sql =
      "DELETE FROM keyvalue kv WHERE (value = 'value'::text)";

  // Test roundtripping the query, which will call get_delete_query_def_spangres
  // when deparsing.
  RoundTripQuery(sql);
}

TEST_F(CatalogShimTestWithMemory, DeleteWithUsing) {
  const std::string sql =
      "DELETE FROM keyvalue USING \"AllSpangresTypes\" WHERE "
      "(keyvalue.key = \"AllSpangresTypes\".int64_value)";

  // Test roundtripping the query, which will call get_delete_query_def_spangres
  // when deparsing.
  RoundTripQuery(sql);
}

TEST_F(CatalogShimTestWithMemory, DeleteWithUsingSelfJoin) {
  const std::string sql =
      "DELETE FROM keyvalue kv1 USING keyvalue kv2 WHERE ((kv1.key = "
      "kv2.key) AND (kv2.value = 'abc'::text))";

  // Test roundtripping the query, which will call get_delete_query_def_spangres
  // when deparsing.
  RoundTripQuery(sql);
}

TEST_F(CatalogShimTestWithMemory, SimpleUpdate) {
  const std::string sql =
      "UPDATE keyvalue SET key = '1234567890123'::bigint WHERE true";

  // Test roundtripping the query, which will call get_update_query_def_spangres
  // when deparsing.
  RoundTripQuery(sql);
}

TEST_F(CatalogShimTestWithMemory, UpdateWithFrom) {
  const std::string sql =
      "UPDATE keyvalue SET key = '123'::bigint FROM "
      "\"AllSpangresTypes\" "
      "WHERE (keyvalue.key = \"AllSpangresTypes\".int64_value)";

  // Test roundtripping the query, which will call get_update_query_def_spangres
  // when deparsing.
  RoundTripQuery(sql);
}

TEST_F(CatalogShimTestWithMemory, UpdateWithFromSelfJoin) {
  const std::string sql =
      "UPDATE keyvalue kv1 SET key = '123'::bigint FROM keyvalue kv2 "
      "WHERE ((kv1.key = kv2.key) AND (kv2.value = 'abc'::text))";

  // Test roundtripping the query, which will call get_update_query_def_spangres
  // when deparsing.
  RoundTripQuery(sql);
}

TEST_F(CatalogShimTestWithMemory, DeparseSetOperationSubqueryWithSetOperation) {
  const std::string sql =
      "SELECT '1'::bigint AS int8_t UNION ALL (SELECT '1'::bigint AS int8_t UNION "
      "ALL"
      " SELECT '1'::bigint AS int8_t LIMIT '1'::bigint)";

  // Test roundtripping the query, which will call get_setop_query when
  // deparsing.
  RoundTripQuery(sql);
}

// Helper function for testing with parse tree nodes. Near-direct copy of the
// static helper in gram.y.
Node* makeIntConst(int val) {
  A_Const* n = makeNode(A_Const);
  n->val.type = T_Integer;
  n->val.val.ival = val;
  n->location = -1;
  return internal::PostgresCastToNode(n);
}

using HintTransformTest = ::postgres_translator::test::ValidMemoryContext;
TEST_F(HintTransformTest, Integer) {
  ParseState* pstate = make_parsestate(/*parentParseState=*/nullptr);
  DefElem* hint = makeNode(DefElem);
  ASSERT_NE(hint, nullptr);
  hint->defname = pstrdup("hint_name");
  hint->arg = makeIntConst(21);

  DefElem* transformed_hint =
      internal::PostgresCastNode(DefElem, transformSpangresHint(pstate, hint));

  EXPECT_EQ(hint, transformed_hint);
  EXPECT_STREQ(transformed_hint->defname, "hint_name");
  Const* val = internal::PostgresCastNode(Const, transformed_hint->arg);
  EXPECT_EQ(DatumGetInt64(val->constvalue), 21);
  EXPECT_EQ(val->consttype, INT8OID);  // INT8 because of the Spangres override.
  free_parsestate(pstate);
}

// Helper function for testing with parse tree nodes. Near-direct copy of the
// static helper in gram.y.
Node* makeTypeCast(Node* arg, TypeName* type_name, int location) {
  TypeCast* n = makeNode(TypeCast);
  n->arg = arg;
  n->typeName = type_name;
  n->location = location;
  return internal::PostgresCastToNode(n);
}

// Helper function for testing with parse tree nodes. Near-direct copy of the
// static helper in gram.y.
Node* makeBoolAConst(bool state) {
  A_Const* n = makeNode(A_Const);
  n->val.type = T_String;
  n->val.val.str = (state ? pstrdup("t") : pstrdup("f"));
  n->location = -1;
  return makeTypeCast(internal::PostgresCastToNode(n),
                      SystemTypeName(pstrdup("bool")), -1);
}

// A literal boolean value ("true", "false") parses as a string with a cast
// function. The analyzer should resolve it into a bool const.
TEST_F(HintTransformTest, Boolean) {
  ParseState* pstate = make_parsestate(/*parentParseState=*/nullptr);

  DefElem* hint = makeNode(DefElem);
  hint->defname = pstrdup("hint_name");
  hint->arg = makeBoolAConst(true);

  DefElem* transformed_hint =
      internal::PostgresCastNode(DefElem, transformSpangresHint(pstate, hint));

  EXPECT_EQ(hint, transformed_hint);
  EXPECT_STREQ(transformed_hint->defname, "hint_name");
  Const* val = internal::PostgresCastNode(Const, transformed_hint->arg);
  EXPECT_EQ(val->consttype, BOOLOID);
  EXPECT_TRUE(DatumGetBool(val->constvalue));

  free_parsestate(pstate);
}

// Helper function for testing with parse tree nodes. Near-direct copy of the
// static helper in gram.y.
Node* makeStringConst(char* string) {
  A_Const* n = makeNode(A_Const);
  n->val.type = T_String;
  n->val.val.str = string;
  n->location = -1;
  return internal::PostgresCastToNode(n);
}

// Test hints with string constants
TEST_F(HintTransformTest, String) {
  ParseState* pstate = make_parsestate(/*parentParseState=*/nullptr);

  DefElem* hint = makeNode(DefElem);
  hint->defname = pstrdup("hint_name");
  hint->arg = makeStringConst(pstrdup("hint_value"));

  DefElem* transformed_hint =
      internal::PostgresCastNode(DefElem, transformSpangresHint(pstate, hint));

  EXPECT_EQ(hint, transformed_hint);
  EXPECT_STREQ(transformed_hint->defname, "hint_name");
  Const* val = internal::PostgresCastNode(Const, transformed_hint->arg);
  EXPECT_EQ(val->consttype, TEXTOID);
  EXPECT_STREQ(TextDatumGetCString(val->constvalue), "hint_value");

  free_parsestate(pstate);
}

// Test the special rule that converts an identifier (ColumnRef) arg to string.
TEST_F(HintTransformTest, Identifier) {
  ParseState* pstate = make_parsestate(/*parentParseState=*/nullptr);

  DefElem* hint = makeNode(DefElem);
  hint->defname = pstrdup("hint_name");
  ColumnRef* col_ref = makeNode(ColumnRef);
  col_ref->fields = list_make1(makeString(pstrdup("hint_value")));
  hint->arg = internal::PostgresCastToNode(col_ref);

  DefElem* transformed_hint =
      internal::PostgresCastNode(DefElem, transformSpangresHint(pstate, hint));
  EXPECT_EQ(hint, transformed_hint);
  EXPECT_STREQ(transformed_hint->defname, "hint_name");
  // ColumnRef should get turned into a const string.
  Const* val = internal::PostgresCastNode(Const, transformed_hint->arg);
  EXPECT_EQ(val->consttype, TEXTOID);
  EXPECT_STREQ(TextDatumGetCString(val->constvalue), "hint_value");

  free_parsestate(pstate);
}

// Helper function to create a 2-member hint list for analyzer testing.
List* CreateHintList() {
  List* result = NIL;
  DefElem* hint1 = makeNode(DefElem);
  hint1->defname = pstrdup("hint_name");
  hint1->arg = makeBoolAConst(true);
  result = lappend(result, hint1);
  DefElem* hint2 = makeNode(DefElem);
  hint2->defname = pstrdup("hint_name");
  hint2->arg = makeBoolAConst(false);
  result = lappend(result, hint2);
  return result;
}

// Unit test statement hint transformation from the public analyzer API.
TEST_F(CatalogShimTestWithMemory, TopLevelStatementHintTransforms) {
  ParseState* pstate = make_parsestate(/*parentParseState=*/nullptr);
  RawStmt* stmt = makeNode(RawStmt);
  stmt->statementHints = CreateHintList();
  // We only care about the hint transformation, which is independent of
  // statement type. Setting the statement type to T_DropStmt, bypasses all
  // other analyzer work.
  stmt->stmt = internal::PostgresCastToNode(makeNode(DropStmt));

  Query* query = transformTopLevelStmt(pstate, stmt);

  ASSERT_NE(query, nullptr);
  ASSERT_NE(query->statementHints, nullptr);
  EXPECT_EQ(list_length(query->statementHints),
            list_length(stmt->statementHints));
  for (DefElem* def_elem : StructList<DefElem*>(query->statementHints)) {
    // Const (and not A_Const) verifies it went through the Analyzer.
    EXPECT_TRUE(IsA(def_elem->arg, Const));
  }

  free_parsestate(pstate);
}

// Unit test JoinExpr hint transformation.
// The public Join transform API is tied to full FROM clause transformation, so
// we need a catalog and a primitive join tree: (keyvalue CROSS JOIN KeyValue2).
TEST_F(CatalogShimTestWithMemory, JoinExprHintTransforms) {
  ParseState* pstate = make_parsestate(/*parentParseState=*/nullptr);
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);
  RangeVar* left_table = makeRangeVar(/*schemaname=*/nullptr,
                                      pstrdup("keyvalue"), /*location=*/-1);
  RangeVar* right_table = makeRangeVar(/*schemaname=*/nullptr,
                                       pstrdup("KeyValue2"), /*location=*/-1);
  JoinExpr* join_expr = makeNode(JoinExpr);
  join_expr->larg = internal::PostgresCastToNode(left_table);
  join_expr->rarg = internal::PostgresCastToNode(right_table);
  join_expr->joinHints = CreateHintList();

  List* from_list = list_make1(join_expr);
  transformFromClause(pstate, from_list);

  JoinExpr* transformed_expr = linitial_node(JoinExpr, pstate->p_joinlist);
  ASSERT_NE(transformed_expr, nullptr);
  ASSERT_NE(transformed_expr->joinHints, nullptr);
  EXPECT_EQ(list_length(transformed_expr->joinHints),
            list_length(join_expr->joinHints));
  for (DefElem* def_elem : StructList<DefElem*>(transformed_expr->joinHints)) {
    // Const (and not A_Const) verifies it went through the Analyzer.
    EXPECT_TRUE(IsA(def_elem->arg, Const));
  }

  free_parsestate(pstate);
}

// Helper function to create a dead-simple SELECT true parse tree.
Node* CreateSelectTrue() {
  SelectStmt* select = makeNode(SelectStmt);
  ResTarget* target = makeNode(ResTarget);
  target->val = makeBoolAConst(true);
  select->targetList = list_make1(target);
  return internal::PostgresCastToNode(select);
}

// Unit test sublink join hint transformation.
TEST_F(CatalogShimTestWithMemory, SubLinkHintTransforms) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  ParseState* pstate = make_parsestate(/*parentParseState=*/nullptr);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto sublink,
      internal::makeSubLink(SubLinkType::ANY_SUBLINK, /*subLinkId=*/0,
                            /*testexpr=*/makeBoolAConst(true),
                            /*operName=*/NIL, /*subselect=*/CreateSelectTrue(),
                            /*location=*/-1));
  sublink->joinHints = CreateHintList();

  SubLink* transformed_link = castNode(
      SubLink, transformExpr(pstate, internal::PostgresCastToNode(sublink),
                             EXPR_KIND_OTHER));

  ASSERT_NE(transformed_link, nullptr);
  ASSERT_NE(transformed_link->joinHints, nullptr);
  EXPECT_EQ(list_length(transformed_link->joinHints),
            list_length(sublink->joinHints));
  for (DefElem* def_elem : StructList<DefElem*>(transformed_link->joinHints)) {
    // Const (and not A_Const) verifies it went through the Analzyer.
    EXPECT_TRUE(IsA(def_elem->arg, Const));
  }

  free_parsestate(pstate);
}

// Unit test table (RangeVar into RangeTblEntry) hint transformation.
TEST_F(CatalogShimTestWithMemory, TableHintTransforms) {
  ParseState* pstate = make_parsestate(/*parentParseState=*/nullptr);
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);
  RangeVar* range_var = makeRangeVar(/*schemaname=*/nullptr,
                                     pstrdup("keyvalue"), /*location=*/-1);
  range_var->tableHints = CreateHintList();
  List* from_clause = list_make1(range_var);

  transformFromClause(pstate, from_clause);

  RangeTblEntry* rte = linitial_node(RangeTblEntry, pstate->p_rtable);
  ASSERT_NE(rte, nullptr);
  ASSERT_NE(rte->tableHints, nullptr);
  EXPECT_EQ(list_length(rte->tableHints), list_length(range_var->tableHints));
  for (DefElem* def_elem : StructList<DefElem*>(rte->tableHints)) {
    // Const (and not A_Const) verifies it went through the Analzyer.
    EXPECT_TRUE(IsA(def_elem->arg, Const));
  }

  free_parsestate(pstate);
}

TEST_F(CatalogShimTestWithMemory, StatementHintGrammarParsesOneHint) {
  const char* test_string = "/*@ statement_hint = 1 */ select 1";
  SpangresTokenLocations locations;

  List* parse_tree =
      raw_parser_spangres(test_string, RAW_PARSE_DEFAULT, &locations);

  ASSERT_EQ(list_length(parse_tree), 1);
  RawStmt* stmt = list_nth_node(RawStmt, parse_tree, 0);

  ASSERT_NE(stmt->statementHints, nullptr);
  ASSERT_EQ(list_length(stmt->statementHints), 1);
  DefElem* hint = list_nth_node(DefElem, stmt->statementHints, 0);
  EXPECT_STREQ(hint->defname, "statement_hint");
  ASSERT_TRUE(IsA(hint->arg, A_Const));
  EXPECT_EQ(intVal(&castNode(A_Const, hint->arg)->val), 1);
}

TEST_F(CatalogShimTestWithMemory, StatementHintGrammarParsesNamespacedHint) {
  const char* test_string = "/*@ hint_namespace.statement_hint = 1 */ select 1";
  SpangresTokenLocations locations;

  List* parse_tree =
      raw_parser_spangres(test_string, RAW_PARSE_DEFAULT, &locations);

  ASSERT_EQ(list_length(parse_tree), 1);
  RawStmt* stmt = list_nth_node(RawStmt, parse_tree, 0);

  ASSERT_NE(stmt->statementHints, nullptr);
  ASSERT_EQ(list_length(stmt->statementHints), 1);
  DefElem* hint = list_nth_node(DefElem, stmt->statementHints, 0);
  EXPECT_STREQ(hint->defnamespace, "hint_namespace");
  EXPECT_STREQ(hint->defname, "statement_hint");
  ASSERT_TRUE(IsA(hint->arg, A_Const));
  EXPECT_EQ(intVal(&castNode(A_Const, hint->arg)->val), 1);
}

TEST_F(CatalogShimTestWithMemory, StatementHintGrammarParsesTwoHints) {
  const char* test_string =
      "/*@ statement_hint1 = 1, statement_hint2 = 2 */ select 1";
  SpangresTokenLocations locations;

  List* parse_tree =
      raw_parser_spangres(test_string, RAW_PARSE_DEFAULT, &locations);

  ASSERT_EQ(list_length(parse_tree), 1);
  RawStmt* stmt = list_nth_node(RawStmt, parse_tree, 0);

  ASSERT_NE(stmt->statementHints, nullptr);
  ASSERT_EQ(list_length(stmt->statementHints), 2);
  DefElem* hint1 = list_nth_node(DefElem, stmt->statementHints, 0);
  EXPECT_STREQ(hint1->defname, "statement_hint1");
  ASSERT_TRUE(IsA(hint1->arg, A_Const));
  EXPECT_EQ(intVal(&castNode(A_Const, hint1->arg)->val), 1);
  DefElem* hint2 = list_nth_node(DefElem, stmt->statementHints, 1);
  EXPECT_STREQ(hint2->defname, "statement_hint2");
  ASSERT_TRUE(IsA(hint2->arg, A_Const));
  EXPECT_EQ(intVal(&castNode(A_Const, hint2->arg)->val), 2);
}

// Test all PostgreSQL relation syntaxes to ensure hints are picked up by them.
// We don't expect users to use the inheritence syntaxes, but as long as we
// aren't blocking them, we should support hints in them as well.
TEST_F(CatalogShimTestWithMemory, TableHintGrammarParsesHint) {
  // These are all equivalent for the parts we care about.
  std::vector<std::string> test_strings{
      "select 1 from keyvalue /*@ table_hint = 1 */",
      "select 1 from keyvalue * /*@ table_hint = 1 */",
      "select 1 from ONLY keyvalue /*@ table_hint = 1 */",
      "select 1 from ONLY (keyvalue) /*@ table_hint = 1 */"};

  for (const std::string& test_case : test_strings) {
    SpangresTokenLocations locations;

    List* parse_tree =
        raw_parser_spangres(test_case.c_str(), RAW_PARSE_DEFAULT, &locations);

    ASSERT_EQ(list_length(parse_tree), 1);
    RawStmt* stmt = list_nth_node(RawStmt, parse_tree, 0);

    SelectStmt* select = castNode(SelectStmt, stmt->stmt);
    RangeVar* range_var = list_nth_node(RangeVar, select->fromClause, 0);
    ASSERT_NE(range_var->tableHints, nullptr);
    ASSERT_EQ(list_length(range_var->tableHints), 1);
    DefElem* hint = list_nth_node(DefElem, range_var->tableHints, 0);
    EXPECT_STREQ(hint->defname, "table_hint");
    ASSERT_TRUE(IsA(hint->arg, A_Const));
    EXPECT_EQ(intVal(&castNode(A_Const, hint->arg)->val), 1);
  }
}

// Test to make sure the unreserved keywords we add to postgres are indeed
// unreserved.
TEST_F(CatalogShimTestWithMemory, UnreservedKeywordsAreUnreserved) {
  std::vector<std::string> test_strings{
    "select 1 as ttl from keyvalue",
    "select 1 as sequences from keyvalue",
    "select 1 as bit_reversed_positive from keyvalue",
    "select 1 as counter from keyvalue"
  };

  for (const std::string& test_case : test_strings) {
    SpangresTokenLocations locations;

    List* parse_tree =
        raw_parser_spangres(test_case.c_str(), RAW_PARSE_DEFAULT, &locations);

    ASSERT_EQ(list_length(parse_tree), 1);
  }
}

// Test all PostgreSQL JOIN syntaxes to ensure hints are picked up by them.
TEST_F(CatalogShimTestWithMemory, TableJoinGrammarParsesHint) {
  // These are all equivalent for the parts we care about.
  std::vector<std::string> test_strings{
      "select 1 from t1 join /*@ join_hint = 1 */ keyvalue t2 on true",
      "select 1 from t1 join /*@ join_hint = 1 */ keyvalue t2 using (cols)",
      "select 1 from t1 cross join /*@ join_hint = 1 */ t2",
      "select 1 from t1 full outer join /*@ join_hint = 1 */ t2 on true",
      "select 1 from t1 inner join /*@ join_hint = 1 */ t2 on true"};

  for (const std::string& test_case : test_strings) {
    SpangresTokenLocations locations;

    List* parse_tree =
        raw_parser_spangres(test_case.c_str(), RAW_PARSE_DEFAULT, &locations);

    ASSERT_EQ(list_length(parse_tree), 1);
    RawStmt* stmt = list_nth_node(RawStmt, parse_tree, 0);

    SelectStmt* select = castNode(SelectStmt, stmt->stmt);
    JoinExpr* join_expr = list_nth_node(JoinExpr, select->fromClause, 0);
    ASSERT_NE(join_expr->joinHints, nullptr);
    ASSERT_EQ(list_length(join_expr->joinHints), 1);
    DefElem* hint = list_nth_node(DefElem, join_expr->joinHints, 0);
    EXPECT_STREQ(hint->defname, "join_hint");
    ASSERT_TRUE(IsA(hint->arg, A_Const));
    EXPECT_EQ(intVal(&castNode(A_Const, hint->arg)->val), 1);
  }
}

// Test PostgreSQL IN List syntaxe to ensure hints are picked up.
TEST_F(CatalogShimTestWithMemory, InListGrammarParsesHint) {
  std::string test_string =
      "select 1 from t1 where a IN /*@ join_hint = 1 */ (select 1)";

  SpangresTokenLocations locations;

  List* parse_tree =
      raw_parser_spangres(test_string.c_str(), RAW_PARSE_DEFAULT, &locations);

  ASSERT_EQ(list_length(parse_tree), 1);
  RawStmt* stmt = list_nth_node(RawStmt, parse_tree, 0);

  SelectStmt* select = castNode(SelectStmt, stmt->stmt);
  ASSERT_NE(select, nullptr);
  ASSERT_NE(select->whereClause, nullptr);
  SubLink* sublink = castNode(SubLink, select->whereClause);
  ASSERT_NE(sublink->joinHints, nullptr);
  ASSERT_EQ(list_length(sublink->joinHints), 1);
  DefElem* hint = list_nth_node(DefElem, sublink->joinHints, 0);
  EXPECT_STREQ(hint->defname, "join_hint");
  ASSERT_TRUE(IsA(hint->arg, A_Const));
  EXPECT_EQ(intVal(&castNode(A_Const, hint->arg)->val), 1);
}

// Test PostgreSQL NOT IN List syntax to ensure hints are picked up.
TEST_F(CatalogShimTestWithMemory, NotInListGrammarParsesHint) {
  std::string test_string =
      "select 1 from t1 where a NOT IN /*@ join_hint = 1 */ (select 1)";

  SpangresTokenLocations locations;

  List* parse_tree =
      raw_parser_spangres(test_string.c_str(), RAW_PARSE_DEFAULT, &locations);

  ASSERT_EQ(list_length(parse_tree), 1);
  RawStmt* stmt = list_nth_node(RawStmt, parse_tree, 0);

  SelectStmt* select = castNode(SelectStmt, stmt->stmt);
  ASSERT_NE(select, nullptr);
  ASSERT_NE(select->whereClause, nullptr);
  BoolExpr* boolexpr = castNode(BoolExpr, select->whereClause);
  ASSERT_EQ(list_length(boolexpr->args), 1);
  SubLink* sublink = linitial_node(SubLink, boolexpr->args);
  ASSERT_NE(sublink->joinHints, nullptr);
  ASSERT_EQ(list_length(sublink->joinHints), 1);
  DefElem* hint = list_nth_node(DefElem, sublink->joinHints, 0);
  EXPECT_STREQ(hint->defname, "join_hint");
  ASSERT_TRUE(IsA(hint->arg, A_Const));
  EXPECT_EQ(intVal(&castNode(A_Const, hint->arg)->val), 1);
}

using ::testing::HasSubstr;
using ::zetasql_base::testing::StatusIs;
TEST_F(CatalogShimTestWithMemory, InValuesListHintErrors) {
  // These should error because IN with Values isn't a hintable Join.
  std::vector<std::string> test_strings{
      "select 1 from t1 where a IN /*@ join_hint = 1 */ (1, 2)",
      "select 1 from t1 where a NOT IN /*@ join_hint = 1 */ (1, 2)"};

  for (const std::string& test_case : test_strings) {
    EXPECT_THAT(
        CheckedPgRawParser(test_case.c_str()),
        StatusIs(
            absl::StatusCode::kInvalidArgument,
            HasSubstr(
                "HINTs cannot be specified on IN clause with value list")));
  }
}

void VerifyViewStmtText(absl::string_view expected, std::string& input,
                        int rawstmt_index) {
  SpangresTokenLocations locations;
  List* parse_tree =
      raw_parser_spangres(input.c_str(), RAW_PARSE_DEFAULT, &locations);
  RawStmt* stmt = list_nth_node(RawStmt, parse_tree, rawstmt_index);
  ViewStmt* view = castNode(ViewStmt, stmt->stmt);
  ASSERT_EQ(expected, absl::StripAsciiWhitespace(view->query_string));
}

TEST_F(CatalogShimTestWithMemory, SingleViewStmt) {
  const std::string view_query_body = "select 1 as c1, 2 as c2";
  std::string test_string = "CREATE VIEW test AS " + view_query_body;

  VerifyViewStmtText(view_query_body, test_string, 0);

  test_string = test_string + " WITH LOCAL CHECK OPTION";
  VerifyViewStmtText(view_query_body, test_string, 0);

  std::string comments = "/*comments*/";
  test_string = "CREATE VIEW test AS " + view_query_body + comments +
                " WITH CHECK OPTION";

  // Comments will be included from the extracted text
  VerifyViewStmtText(view_query_body + comments, test_string, 0);
}

TEST_F(CatalogShimTestWithMemory, ViewInMultipleStmt) {
  std::string select = "select 1 as c1, 2 as c2;";
  const std::string view_query_body = "select 1 as c1, 2 as c2";
  std::string test_string = select + "CREATE VIEW test AS " + view_query_body;

  VerifyViewStmtText(view_query_body, test_string, 1);

  test_string = test_string + " WITH LOCAL CHECK OPTION";
  VerifyViewStmtText(view_query_body, test_string, 1);

  std::string comments = "/*comments*/";
  test_string = select + "CREATE VIEW test AS " + view_query_body + comments +
                " WITH CHECK OPTION;" + select;
  // Comments will be included from the extracted text
  VerifyViewStmtText(view_query_body + comments, test_string, 1);
}

void VerifyColumnConstraintExprString(
    CreateStmt* create, int nth, absl::string_view costraint_expression_text) {
  ColumnDef* column = list_nth_node(ColumnDef, create->tableElts, nth);
  Constraint* constraint = list_nth_node(Constraint, column->constraints, 0);
  EXPECT_EQ(costraint_expression_text, constraint->constraint_expr_string);
}

TEST_F(CatalogShimTestWithMemory, ExprStringCheckConstraint) {
  std::string create_table =
      "create table users (age bigint check(age > 0) not null); create table "
      "users2 (id bigint, age bigint, check(age > id));";
  SpangresTokenLocations locations;
  List* parse_tree =
      raw_parser_spangres(create_table.c_str(), RAW_PARSE_DEFAULT, &locations);
  ASSERT_EQ(list_length(parse_tree), 2);
  RawStmt* stmt = list_nth_node(RawStmt, parse_tree, 0);
  VerifyColumnConstraintExprString(castNode(CreateStmt, stmt->stmt), 0,
                                   "age > 0");
  CreateStmt* create =
      castNode(CreateStmt, list_nth_node(RawStmt, parse_tree, 1)->stmt);
  EXPECT_EQ("age > id",
            absl::string_view{list_nth_node(Constraint, create->tableElts, 2)
                                  ->constraint_expr_string});
}

TEST_F(CatalogShimTestWithMemory, ExprStringDefault) {
  std::string create_table =
      "create table users (id bigint primary key, age bigint default 9);create "
      "table users2 (id bigint primary key, age bigint default (1+1*9)         "
      "  not null);"
      "alter table users2 alter column age set default 30;";
  SpangresTokenLocations locations;
  List* parse_tree =
      raw_parser_spangres(create_table.c_str(), RAW_PARSE_DEFAULT, &locations);
  RawStmt* stmt = list_nth_node(RawStmt, parse_tree, 0);
  VerifyColumnConstraintExprString(castNode(CreateStmt, stmt->stmt), 1, "9");

  stmt = list_nth_node(RawStmt, parse_tree, 1);
  // The extra space will be included in the extracted string
  VerifyColumnConstraintExprString(castNode(CreateStmt, stmt->stmt), 1,
                                   "(1+1*9)           ");

  stmt = list_nth_node(RawStmt, parse_tree, 2);
  AlterTableStmt* alter = castNode(AlterTableStmt, stmt->stmt);
  AlterTableCmd* cmd = list_nth_node(AlterTableCmd, alter->cmds, 0);
  EXPECT_STREQ(cmd->raw_expr_string, "30");
}

TEST_F(CatalogShimTestWithMemory, ExprStringGeneratedColumn) {
  std::string create_table =
      "create table users (id bigint primary key, age bigint generated always "
      "as (id+1) stored);create table users2 (id bigint primary key, age "
      "bigint generated always as ('100'::bigint) stored);";
  SpangresTokenLocations locations;
  List* parse_tree =
      raw_parser_spangres(create_table.c_str(), RAW_PARSE_DEFAULT, &locations);
  RawStmt* stmt = list_nth_node(RawStmt, parse_tree, 0);
  VerifyColumnConstraintExprString(castNode(CreateStmt, stmt->stmt), 1, "id+1");

  stmt = list_nth_node(RawStmt, parse_tree, 1);
  VerifyColumnConstraintExprString(castNode(CreateStmt, stmt->stmt), 1,
                                   "'100'::bigint");
}

TEST_F(CatalogShimTestWithMemory, GenerateFunctionName) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);
  std::vector<Oid> argument_types;
  Oid argtypes[1];
  ZETASQL_ASSERT_OK_AND_ASSIGN(Oid count_oid,
                       PgBootstrapCatalog::Default()->GetProcOid(
                           "pg_catalog", "count", argument_types));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Oid pending_commit_timestamp_oid,
      PgBootstrapCatalog::Default()->GetProcOid(
          "spanner", "pending_commit_timestamp", argument_types));

  ZETASQL_ASSERT_OK_AND_ASSIGN(Oid bit_reverse_oid,
                       PgBootstrapCatalog::Default()->GetProcOid(
                           "spanner", "bit_reverse", {INT8OID, BOOLOID}));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Oid get_internal_sequence_state_oid,
      PgBootstrapCatalog::Default()->GetProcOid(
          "spanner", "get_internal_sequence_state", {VARCHAROID}));

  // Functions in pg_catalog should deparse without a namespace.
  char* count_deparsed_name =
      generate_function_name(count_oid, /*nargs=*/0,
                             /*argnames=*/NIL, argtypes,
                             /*has_variadic=*/false,
                             /*use_variadic_p=*/NULL,
                             /*special_exprkind=*/EXPR_KIND_NONE);
  EXPECT_STREQ(count_deparsed_name, "count");

  // Functions outside of pg_catalog should deparse with a namespace.
  char* pending_deparsed_name =
      generate_function_name(pending_commit_timestamp_oid, /*nargs=*/0,
                             /*argnames=*/NIL, argtypes,
                             /*has_variadic=*/false,
                             /*use_variadic_p=*/NULL,
                             /*special_exprkind=*/EXPR_KIND_NONE);
  EXPECT_STREQ(pending_deparsed_name, "spanner.pending_commit_timestamp");

  char* bit_reverse_deparsed_name =
      generate_function_name(bit_reverse_oid, /*nargs=*/0,
                             /*argnames=*/NIL, argtypes,
                             /*has_variadic=*/false,
                             /*use_variadic_p=*/NULL,
                             /*special_exprkind=*/EXPR_KIND_NONE);
  EXPECT_STREQ(bit_reverse_deparsed_name, "spanner.bit_reverse");

  char* get_internal_sequence_state_deparsed_name =
      generate_function_name(get_internal_sequence_state_oid, /*nargs=*/0,
                             /*argnames=*/NIL, argtypes,
                             /*has_variadic=*/false,
                             /*use_variadic_p=*/NULL,
                             /*special_exprkind=*/EXPR_KIND_NONE);
  EXPECT_STREQ(get_internal_sequence_state_deparsed_name,
      "spanner.get_internal_sequence_state");
}

TEST(CatalogShimTest, GetsTyplenByvalAlign) {
  Oid typid = INT8ARRAYOID;
  int16_t typlen;
  bool typbyval;
  char typalign;

  get_typlenbyvalalign(typid, &typlen, &typbyval, &typalign);

  // Get the pg_type entry to verify results.
  ZETASQL_ASSERT_OK_AND_ASSIGN(const FormData_pg_type* pg_type,
                       PgBootstrapCatalog::Default()->GetType(typid));
  EXPECT_EQ(typlen, pg_type->typlen);
  EXPECT_EQ(typbyval, pg_type->typbyval);
  EXPECT_EQ(typalign, pg_type->typalign);

  // Negative case.
  EXPECT_THROW(get_typlenbyvalalign(InvalidOid, &typlen, &typbyval, &typalign),
               PostgresEreportException);
}

TEST(CatalogShimTest, GetsFuncVariadicType) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  // Valid cases: {variadic, non-variadic}
  std::vector<Oid> test_cases = {F_CONCAT, F_BOOLEQ};

  for (const Oid funcid : test_cases) {
    Oid variadictype = get_func_variadictype(funcid);

    // Get the pg_proc entry to verify results.
    ZETASQL_ASSERT_OK_AND_ASSIGN(const FormData_pg_proc* pg_proc,
                         PgBootstrapCatalog::Default()->GetProc(funcid));
    EXPECT_EQ(variadictype, pg_proc->provariadic);
  }

  // Invalid case.
  EXPECT_THROW(get_func_variadictype(InvalidOid), PostgresEreportException);
}

TEST(CatalogShimTest, GetsRetSet) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  // Valid cases: {true, false}
  std::vector<Oid> test_cases = {F_UNNEST_ANYARRAY, F_BOOLEQ};

  for (const Oid funcid : test_cases) {
    Oid retset = get_func_retset(funcid);

    // Get the pg_proc entry to verify results.
    ZETASQL_ASSERT_OK_AND_ASSIGN(const FormData_pg_proc* pg_proc,
                         PgBootstrapCatalog::Default()->GetProc(funcid));
    EXPECT_EQ(retset, pg_proc->proretset);
  }

  // Invalid case.
  EXPECT_THROW(get_func_retset(InvalidOid), PostgresEreportException);
}

TEST_F(CatalogShimTestWithMemory, TransformContainerType) {
  // Note: typmod is ignored in all cases.
  int32_t containerTypmod = -1;

  // Simple cases just get the element type and leave container type unchanged.
  Oid containerType = INT8ARRAYOID;
  transformContainerType(&containerType, &containerTypmod);
  EXPECT_EQ(containerType, INT8ARRAYOID);
  containerType = TEXTARRAYOID;
  transformContainerType(&containerType, &containerTypmod);
  EXPECT_EQ(containerType, TEXTARRAYOID);

  // Special cases will modify VECTOR containerType to the more common ARRAY
  // type for INT2 and OID.
  containerType = INT2VECTOROID;
  transformContainerType(&containerType, &containerTypmod);
  EXPECT_EQ(containerType, INT2ARRAYOID);
  containerType = OIDVECTOROID;
  transformContainerType(&containerType, &containerTypmod);
  EXPECT_EQ(containerType, OIDARRAYOID);
}

// Helper function to create a A_Const (parse tree) node containing a specified
// integer value.
static Node* MakeIntConst(int val, int location) {
  A_Const* n = makeNode(A_Const);

  n->val.type = T_Integer;
  n->val.val.ival = val;
  n->location = location;

  return reinterpret_cast<Node*>(n);
}

// Helper function to create a Const (query tree) node containing a specified
// int8_t value (or null).
static Const* MakeInt8Const(int value, bool isnull) {
  return makeConst(INT8OID, -1, /*constcollid=*/InvalidOid, sizeof(int64_t),
                   Int64GetDatum(value), isnull,
                   /*constbyval=*/true);
}

// transformContainerSubscripts is used for (SELECT arr[1]).
//
// We're only testing supported functionality here:
//   Single indirection, no multi-dimensionality
//   Single index, no slices
TEST_F(CatalogShimTestWithMemory, TransformContainerSubscripts) {
  ParseState* pstate = make_parsestate(/*parentParseState=*/nullptr);
  pstate->p_expr_kind = EXPR_KIND_SELECT_TARGET;
  Node* containerBase =
      makeIntConst(5);  // Placeholder only, returned unchanged in output.
  A_Indices* a_indices = makeNode(A_Indices);
  a_indices->is_slice = false;  // Not a slice
  a_indices->lidx = nullptr;    // Not a slice
  a_indices->uidx = MakeIntConst(/*val=*/13, /*location=*/-1);
  List* indirection = list_make1(a_indices);

  const SubscriptingRef* result = transformContainerSubscripts(
      pstate, containerBase, /*containerType=*/INT8ARRAYOID,
      /*containerTypMod=*/-1, indirection, /*isAssignment=*/false);

  ASSERT_NE(result, nullptr);
  ASSERT_TRUE(IsA(result, SubscriptingRef));
  EXPECT_EQ(result->refcontainertype, INT8ARRAYOID);
  EXPECT_EQ(result->refelemtype, INT8OID);
  EXPECT_EQ(result->reftypmod, -1);
  EXPECT_EQ(result->reflowerindexpr, nullptr);  // Not a slice
  const Const* thirteen_const = MakeInt8Const(13, /*isnull=*/false);
  EXPECT_TRUE(
      equal(linitial_node(Const, result->refupperindexpr), thirteen_const))
      << nodeToString(linitial_node(Const, result->refupperindexpr))
      << " does not match expected: " << nodeToString(thirteen_const);
  EXPECT_EQ(reinterpret_cast<Node*>(result->refexpr), containerBase);
  EXPECT_TRUE(result->refassgnexpr == NULL)
      << " is not null: "
      << nodeToString(reinterpret_cast<Node*>(result->refassgnexpr));

  // Should error if input is not a container.
  EXPECT_THROW(transformContainerSubscripts(
                   pstate, containerBase, /*containerType=*/InvalidOid,
                   /*containerTypMod=*/-1, indirection, /*isAssignment=*/false),
               PostgresEreportException);
  EXPECT_THROW(transformContainerSubscripts(
                   pstate, containerBase, /*containerType=*/INT8OID,
                   /*containerTypMod=*/-1, indirection, /*isAssignment=*/false),
               PostgresEreportException);
}

TEST_F(CatalogShimTestWithMemory, LookupExplicitNamespace) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  // Unknown namespace returns InvalidOid.
  EXPECT_EQ(LookupExplicitNamespace("unknown namespace", /*missing_ok=*/true),
            InvalidOid);
  // Built-in namespaces match bootstrap.
  // missing_ok: return InvalidOid instead of throwing an exception.
  EXPECT_EQ(LookupExplicitNamespace("public", /*missing_ok=*/true),
            PG_PUBLIC_NAMESPACE);
  EXPECT_EQ(LookupExplicitNamespace("pg_catalog", /*missing_ok=*/true),
            PG_CATALOG_NAMESPACE);

  // Teach catalog adapter about a non-bootstrap namespace.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto adapter, GetCatalogAdapter());
  ZETASQL_ASSERT_OK_AND_ASSIGN(Oid new_oid, adapter->GetOrGenerateOidFromNamespaceName(
                                        "test namespace name"));
  // Expect to find a match.
  EXPECT_EQ(LookupExplicitNamespace("test namespace name", /*missing_ok=*/true),
            new_oid);
}

TEST_F(CatalogShimTestWithMemory, LookupCollationBadNamespace) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  // A lookup to a non pg-catalog namespace will throw an error.
  EXPECT_ANY_THROW(
      lookup_collation("C", /*namespace=*/InvalidOid, /*encoding=*/0));
}

TEST_F(CatalogShimTestWithMemory, LookupCollationBadCollationName) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  // A pg_catalog namespace with invalid collation will return InvalidOid
  EXPECT_EQ(
      lookup_collation("fake collation", PG_CATALOG_NAMESPACE, /*encoding=*/0),
      InvalidOid);
}

TEST_F(CatalogShimTestWithMemory, LookupCollationCorrect) {
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options);

  // Expect to return the C collation oid when looking in the pg_catalog
  EXPECT_EQ(lookup_collation("C", PG_CATALOG_NAMESPACE, /*encoding=*/0),
            C_COLLATION_OID);
}

}  // namespace
}  // namespace postgres_translator

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
