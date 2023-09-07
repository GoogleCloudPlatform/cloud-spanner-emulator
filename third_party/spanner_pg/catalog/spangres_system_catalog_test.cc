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

#include "third_party/spanner_pg/catalog/spangres_system_catalog.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/container/flat_hash_set.h"
#include "absl/flags/declare.h"
#include "absl/flags/flag.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "third_party/spanner_pg/bootstrap_catalog/bootstrap_catalog.h"
#include "third_party/spanner_pg/catalog/function_identifier.h"
#include "third_party/spanner_pg/test_catalog/test_catalog.h"
#include "third_party/spanner_pg/util/valid_memory_context_fixture.h"

namespace postgres_translator {
namespace spangres {

namespace {

using ::testing::UnorderedPointwise;
using ::zetasql_base::testing::StatusIs;
using gsql_value = ::zetasql::Value;

using SpangresSystemCatalogTest =
    ::postgres_translator::test::ValidMemoryContext;

const zetasql::Type* gsql_bool = zetasql::types::BoolType();
const zetasql::Type* gsql_bytes = zetasql::types::BytesType();
const zetasql::Type* gsql_int64 = zetasql::types::Int64Type();
const zetasql::Type* gsql_double = zetasql::types::DoubleType();
const zetasql::Type* gsql_string = zetasql::types::StringType();
const zetasql::Type* gsql_date = zetasql::types::DateType();
const zetasql::Type* gsql_timestamp = zetasql::types::TimestampType();

const zetasql::Type* gsql_int64_array = zetasql::types::Int64ArrayType();
const zetasql::Type* gsql_string_array = zetasql::types::StringArrayType();
const zetasql::Type* gsql_bool_array = zetasql::types::BoolArrayType();
const zetasql::Type* gsql_double_array = zetasql::types::DoubleArrayType();
const zetasql::Type* gsql_bytes_array = zetasql::types::BytesArrayType();
const zetasql::Type* gsql_date_array = zetasql::types::DateArrayType();
const zetasql::Type* gsql_timestamp_array =
    zetasql::types::TimestampArrayType();

static zetasql::LanguageOptions GetLanguageOptions() {
  zetasql::LanguageOptions options;
  options.set_product_mode(zetasql::PRODUCT_EXTERNAL);
  return options;
}

static EngineSystemCatalog* GetSpangresSystemCatalog() {
  static EngineSystemCatalog* catalog = []() {
    zetasql::LanguageOptions language_options = GetLanguageOptions();
    ABSL_CHECK(SpangresSystemCatalog::TryInitializeEngineSystemCatalog(
              test::GetSpangresTestBuiltinFunctionCatalog(language_options),
              language_options)
              .value());
    return EngineSystemCatalog::GetEngineSystemCatalog();
  }();

  return catalog;
}

zetasql::TypeFactory* GetTypeFactory() {
  static zetasql::TypeFactory* s_type_factory =
      new zetasql::TypeFactory(zetasql::TypeFactoryOptions{
          .keep_alive_while_referenced_from_value = false});
  return s_type_factory;
}

MATCHER(TypeEquals, "") {
  return std::get<0>(arg) != nullptr &&
         std::get<0>(arg)->Equals(std::get<1>(arg));
}

TEST_F(SpangresSystemCatalogTest, GetTypes) {
  EngineSystemCatalog* catalog = GetSpangresSystemCatalog();
  absl::flat_hash_set<const zetasql::Type*> types;
  ZETASQL_ASSERT_OK(catalog->GetTypes(&types));
  std::vector<const zetasql::Type*> expected_types{
      gsql_bool,
      gsql_int64,
      gsql_double,
      gsql_string,
      gsql_bytes,
      gsql_timestamp,
      gsql_date,
      zetasql::types::BoolArrayType(),
      zetasql::types::Int64ArrayType(),
      zetasql::types::DoubleArrayType(),
      zetasql::types::StringArrayType(),
      zetasql::types::BytesArrayType(),
      zetasql::types::TimestampArrayType(),
      zetasql::types::DateArrayType()};

  EXPECT_THAT(types, UnorderedPointwise(TypeEquals(), expected_types));
}

  // Disabling in the emulator as it doesn't block access to these functions,
  // unlike prod.
  TEST_F(SpangresSystemCatalogTest, DISABLED_UnsupportedBuiltinFunctions) {
  EngineSystemCatalog* catalog = GetSpangresSystemCatalog();
  absl::flat_hash_set<const zetasql::Function*> functions;
  ZETASQL_ASSERT_OK(catalog->GetFunctions(&functions));
  EXPECT_NE(functions.size(), 0);

  // Collect all the function names.
  absl::flat_hash_set<absl::string_view> function_names;
  for (const zetasql::Function* function : functions) {
    function_names.insert(function->Name());
  }

  // These functions are supported in ZetaSQL but not in Spanner.
  EXPECT_FALSE(function_names.contains("ascii"));
  EXPECT_FALSE(function_names.contains("left"));
  EXPECT_FALSE(function_names.contains("right"));

  // A few functions are supported even though they don't appear in the
  // FunctionKindByName map.
  EXPECT_TRUE(function_names.contains("$not_equal"));
}

TEST_F(SpangresSystemCatalogTest, SpecialBuiltinFunctions) {
  zetasql::LanguageOptions language_options = GetLanguageOptions();
  EngineSystemCatalog* catalog = GetSpangresSystemCatalog();

  std::vector<zetasql::InputArgumentType> input_types;
  input_types.push_back(zetasql::InputArgumentType(gsql_int64));
  input_types.push_back(zetasql::InputArgumentType(gsql_int64));
  ZETASQL_ASSERT_OK_AND_ASSIGN(Oid int8_ne_oid,
                       catalog->GetPgProcOidFromReverseMapping(
                           "$not_equal", input_types, language_options));
  EXPECT_EQ(int8_ne_oid, 468);
}

TEST_F(SpangresSystemCatalogTest, SpannerPendingCommitTimestampFunction) {
  zetasql::LanguageOptions language_options = GetLanguageOptions();
  EngineSystemCatalog* catalog = GetSpangresSystemCatalog();
  std::vector<zetasql::InputArgumentType> input_types;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Oid pending_commit_timestamp_oid,
      catalog->GetPgProcOidFromReverseMapping("pending_commit_timestamp",
                                              input_types, language_options));
  ASSERT_NE(pending_commit_timestamp_oid, InvalidOid);

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      FunctionAndSignature function_and_signature,
      catalog->GetFunctionAndSignature(pending_commit_timestamp_oid,
                                       input_types, language_options));
  ASSERT_NE(function_and_signature.function(), nullptr);
  EXPECT_EQ(function_and_signature.function()->Name(),
            "pending_commit_timestamp");
}

TEST_F(SpangresSystemCatalogTest, SpannerGenerateUuidFunction) {
  zetasql::LanguageOptions language_options = GetLanguageOptions();
  EngineSystemCatalog* catalog = GetSpangresSystemCatalog();
  std::vector<zetasql::InputArgumentType> input_types;
  ZETASQL_ASSERT_OK_AND_ASSIGN(Oid function_oid,
                       catalog->GetPgProcOidFromReverseMapping(
                           "generate_uuid", input_types,
                           language_options));
  ASSERT_NE(function_oid, InvalidOid);

  ZETASQL_ASSERT_OK_AND_ASSIGN(FunctionAndSignature function_and_signature,
                       catalog->GetFunctionAndSignature(
                           function_oid, input_types, language_options));
  ASSERT_NE(function_and_signature.function(), nullptr);
  EXPECT_EQ(function_and_signature.function()->Name(), "generate_uuid");
}

TEST_F(SpangresSystemCatalogTest, SpannerTimestampFromUnixMicrosFunction) {
  zetasql::LanguageOptions language_options = GetLanguageOptions();
  EngineSystemCatalog* catalog = GetSpangresSystemCatalog();
  for (const zetasql::Type* input_type : {gsql_int64, gsql_timestamp}) {
    std::vector<zetasql::InputArgumentType> input_types;
    input_types.push_back(zetasql::InputArgumentType(input_type));
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        Oid function_oid,
        catalog->GetPgProcOidFromReverseMapping("timestamp_from_unix_micros",
                                                input_types, language_options));
    ASSERT_NE(function_oid, InvalidOid);

    ZETASQL_ASSERT_OK_AND_ASSIGN(FunctionAndSignature function_and_signature,
                         catalog->GetFunctionAndSignature(
                             function_oid, input_types, language_options));
    ASSERT_NE(function_and_signature.function(), nullptr);
    EXPECT_EQ(function_and_signature.function()->Name(),
              "timestamp_from_unix_micros");
  }
}

TEST_F(SpangresSystemCatalogTest, SpannerTimestampFromUnixMillisFunction) {
  zetasql::LanguageOptions language_options = GetLanguageOptions();
  EngineSystemCatalog* catalog = GetSpangresSystemCatalog();
  for (const zetasql::Type* input_type : {gsql_int64, gsql_timestamp}) {
    std::vector<zetasql::InputArgumentType> input_types;
    input_types.push_back(zetasql::InputArgumentType(input_type));
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        Oid function_oid,
        catalog->GetPgProcOidFromReverseMapping("timestamp_from_unix_millis",
                                                input_types, language_options));
    ASSERT_NE(function_oid, InvalidOid);

    ZETASQL_ASSERT_OK_AND_ASSIGN(FunctionAndSignature function_and_signature,
                         catalog->GetFunctionAndSignature(
                             function_oid, input_types, language_options));
    ASSERT_NE(function_and_signature.function(), nullptr);
    EXPECT_EQ(function_and_signature.function()->Name(),
              "timestamp_from_unix_millis");
  }
}

TEST_F(SpangresSystemCatalogTest, ArrayAtFunction) {
  zetasql::LanguageOptions language_options = GetLanguageOptions();
  EngineSystemCatalog* catalog = GetSpangresSystemCatalog();
  // Inputs are 1: any array (Var, Const, etc.), 2: int8_t Const array index.
  std::vector<zetasql::InputArgumentType> input_types{
      zetasql::InputArgumentType(zetasql::types::TimestampArrayType()),
      zetasql::InputArgumentType(gsql_int64)};

  ZETASQL_ASSERT_OK_AND_ASSIGN(FunctionAndSignature function_and_signature,
                       catalog->GetFunctionAndSignature(
                           PostgresExprIdentifier::Expr(T_SubscriptingRef),
                           input_types, language_options));

  ASSERT_NE(function_and_signature.function(), nullptr);
  EXPECT_EQ(function_and_signature.function()->Name(),
            "$safe_array_at_ordinal");
}

TEST_F(SpangresSystemCatalogTest, MakeArrayFunction) {
  zetasql::LanguageOptions language_options = GetLanguageOptions();
  EngineSystemCatalog* catalog = GetSpangresSystemCatalog();
  // Inputs are a list of of elements with the same type, but can be a mix of
  // Var, Const, other Expr.
  std::vector<zetasql::InputArgumentType> input_types{
      zetasql::InputArgumentType(gsql_int64),
      zetasql::InputArgumentType(zetasql::values::Int64(42)),
      zetasql::InputArgumentType(gsql_int64,
                                   /*is_query_parameter=*/true)};

  ZETASQL_ASSERT_OK_AND_ASSIGN(FunctionAndSignature function_and_signature,
                       catalog->GetFunctionAndSignature(
                           PostgresExprIdentifier::Expr(T_ArrayExpr),
                           input_types, language_options));

  ASSERT_NE(function_and_signature.function(), nullptr);
  EXPECT_EQ(function_and_signature.function()->Name(), "$make_array");
}

TEST_F(SpangresSystemCatalogTest, ArrayCatFunctions) {
  zetasql::LanguageOptions language_options = GetLanguageOptions();
  EngineSystemCatalog* catalog = GetSpangresSystemCatalog();
  // For each array type, check that we have a supported array_cat function.
  const std::vector<const zetasql::Type*> array_types{
      zetasql::types::Int64ArrayType(),
      zetasql::types::BoolArrayType(),
      zetasql::types::DoubleArrayType(),
      zetasql::types::StringArrayType(),
      zetasql::types::BytesArrayType(),
      zetasql::types::TimestampArrayType(),
      zetasql::types::DateArrayType()};

  for (const zetasql::Type* array_type : array_types) {
    std::vector<zetasql::InputArgumentType> input_types{
        zetasql::InputArgumentType(array_type),
        zetasql::InputArgumentType(array_type)};

    ZETASQL_ASSERT_OK_AND_ASSIGN(Oid array_cat_oid,
                         catalog->GetPgProcOidFromReverseMapping(
                             "array_concat", input_types, language_options));

    ZETASQL_ASSERT_OK_AND_ASSIGN(const FormData_pg_proc* proc,
                         PgBootstrapCatalog::Default()->GetProc(array_cat_oid));

    EXPECT_STREQ(NameStr(proc->proname), "array_cat");
  }
}

TEST_F(SpangresSystemCatalogTest,
       IsTransformationRequiredForComparisonDoubleTest) {
  EngineSystemCatalog* catalog = GetSpangresSystemCatalog();
  std::unique_ptr<zetasql::ResolvedExpr> literal =
      zetasql::MakeResolvedLiteral(gsql_double,
                                     gsql_value::Double(3.141592653589793));
  EXPECT_TRUE(catalog->IsTransformationRequiredForComparison(*literal));
}

TEST_F(SpangresSystemCatalogTest,
       IsTransformationRequiredForComparisonNonDoubleTest) {
  EngineSystemCatalog* catalog = GetSpangresSystemCatalog();
  std::unique_ptr<zetasql::ResolvedExpr> literal =
      zetasql::MakeResolvedLiteral(gsql_string,
                                     gsql_value::String("test value"));
  EXPECT_FALSE(catalog->IsTransformationRequiredForComparison(*literal));
}

TEST_F(SpangresSystemCatalogTest, GetResolvedExprForComparisonDoubleTest) {
  zetasql::LanguageOptions language_options = GetLanguageOptions();
  EngineSystemCatalog* catalog = GetSpangresSystemCatalog();
  std::unique_ptr<zetasql::ResolvedExpr> literal =
      zetasql::MakeResolvedLiteral(gsql_double,
                                     gsql_value::Double(3.141592653589793));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<zetasql::ResolvedExpr> mapped,
                       catalog->GetResolvedExprForComparison(std::move(literal),
                                                             language_options));
  EXPECT_TRUE(mapped->Is<zetasql::ResolvedFunctionCall>());
  EXPECT_EQ(gsql_int64, mapped->type());
  EXPECT_TRUE(catalog->IsResolvedExprForComparison(*mapped));
  ZETASQL_ASSERT_OK_AND_ASSIGN(const zetasql::ResolvedExpr* unmapped,
                       catalog->GetOriginalExprFromComparisonExpr(*mapped));
  EXPECT_NE(unmapped, nullptr);
  EXPECT_TRUE(unmapped->Is<zetasql::ResolvedLiteral>());
  EXPECT_TRUE(unmapped->type()->IsDouble());
}

TEST_F(SpangresSystemCatalogTest, GetResolvedExprForComparisonNonDoubleTest) {
  zetasql::LanguageOptions language_options = GetLanguageOptions();
  EngineSystemCatalog* catalog = GetSpangresSystemCatalog();
  std::unique_ptr<zetasql::ResolvedExpr> literal =
      zetasql::MakeResolvedLiteral(gsql_string,
                                     gsql_value::String("test value"));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<zetasql::ResolvedExpr> mapped,
                       catalog->GetResolvedExprForComparison(std::move(literal),
                                                             language_options));

  // Returned ResolvedExpr should be unmodified for non-double type.
  EXPECT_TRUE(mapped->Is<zetasql::ResolvedLiteral>());
  EXPECT_EQ(gsql_string, mapped->type());
  EXPECT_FALSE(catalog->IsResolvedExprForComparison(*mapped));
}

TEST_F(SpangresSystemCatalogTest, StringToDateCastOverride) {
  EngineSystemCatalog* catalog = GetSpangresSystemCatalog();
  EXPECT_TRUE(catalog->HasCastOverrideFunction(gsql_string, gsql_date));
  ZETASQL_ASSERT_OK_AND_ASSIGN(FunctionAndSignature function_and_signature,
                       catalog->GetCastOverrideFunctionAndSignature(
                           gsql_string, gsql_date, GetLanguageOptions()));
  ASSERT_NE(function_and_signature.function(), nullptr);
  EXPECT_EQ(
      function_and_signature.function()->FullName(/*include_group=*/false),
      "pg.cast_to_date");
  ASSERT_EQ(function_and_signature.signature().arguments().size(), 1);
  EXPECT_EQ(function_and_signature.signature().argument(0).type(), gsql_string);
  EXPECT_EQ(function_and_signature.signature().result_type().type(), gsql_date);
}

TEST_F(SpangresSystemCatalogTest, MinAggregateRemapTest) {
  EngineSystemCatalog* catalog = GetSpangresSystemCatalog();
  const PostgresExtendedFunction* min_function = catalog->GetFunction("min");
  ASSERT_NE(min_function, nullptr);

  bool has_signature_for_double = false;
  for (const std::unique_ptr<PostgresExtendedFunctionSignature>& signature :
       min_function->GetPostgresSignatures()) {
    ASSERT_EQ(signature->arguments().size(), 1);
    const zetasql::Type* argument_type = signature->argument(0).type();
    if (argument_type->IsDouble()) {
      has_signature_for_double = true;
      EXPECT_EQ(signature->mapped_function()->FullName(/*include_group=*/false),
                "pg.min");
    } else {
      EXPECT_EQ(signature->mapped_function()->FullName(/*include_group=*/false),
                "min");
    }
  }
  ASSERT_TRUE(has_signature_for_double);
}

TEST_F(SpangresSystemCatalogTest, NanOrderingFunctionsEnabled) {
  EngineSystemCatalog* catalog = GetSpangresSystemCatalog();

  std::vector<zetasql::InputArgumentType> input_argument_types;
  input_argument_types.emplace_back(zetasql::types::DoubleType());
  ZETASQL_ASSERT_OK_AND_ASSIGN(Oid min_oid, PgBootstrapCatalog::Default()->GetProcOid(
                                        "pg_catalog", "min", {FLOAT8OID}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      FunctionAndSignature function_and_signature,
      catalog->GetFunctionAndSignature(min_oid, input_argument_types,
                                       GetLanguageOptions()));

  EXPECT_EQ(
      function_and_signature.function()->FullName(/*include_group=*/false),
      "pg.min");

  ZETASQL_ASSERT_OK_AND_ASSIGN(FunctionAndSignature least_function,
                       catalog->GetFunctionAndSignature(
                           PostgresExprIdentifier::MinMaxExpr(IS_LEAST),
                           input_argument_types, GetLanguageOptions()));

  EXPECT_EQ(least_function.function()->FullName(/*include_group=*/false),
            "pg.least");

  ZETASQL_ASSERT_OK_AND_ASSIGN(FunctionAndSignature greatest_function,
                       catalog->GetFunctionAndSignature(
                           PostgresExprIdentifier::MinMaxExpr(IS_GREATEST),
                           input_argument_types, GetLanguageOptions()));

  EXPECT_EQ(greatest_function.function()->FullName(/*include_group=*/false),
            "pg.greatest");
}

static void AssertPGFunctionIsRegistered(
    absl::string_view function_name, absl::Span<const Oid> oid_argument_types,
    std::vector<zetasql::InputArgumentType> gsql_argument_types) {
  EngineSystemCatalog* catalog = GetSpangresSystemCatalog();

  ZETASQL_ASSERT_OK_AND_ASSIGN(Oid function_oid,
                       PgBootstrapCatalog::Default()->GetProcOid(
                           "pg_catalog", function_name, oid_argument_types));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      FunctionAndSignature function_and_signature,
      catalog->GetFunctionAndSignature(function_oid, gsql_argument_types,
                                       GetLanguageOptions()));
  EXPECT_EQ(
      function_and_signature.function()->FullName(/*include_group=*/false),
      absl::StrCat("pg.", function_name));
}
}  // namespace

}  // namespace spangres
}  // namespace postgres_translator

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
