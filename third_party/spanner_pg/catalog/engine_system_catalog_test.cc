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

#include "third_party/spanner_pg/catalog/engine_system_catalog.h"

#include <memory>
#include <type_traits>

#include "zetasql/public/language_options.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/container/flat_hash_set.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "third_party/spanner_pg/catalog/function_identifier.h"
#include "third_party/spanner_pg/catalog/spangres_type.h"
#include "third_party/spanner_pg/catalog/type.h"
#include "third_party/spanner_pg/interface/stub_builtin_function_catalog.h"
#include "third_party/spanner_pg/util/valid_memory_context_fixture.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {
namespace {

using ::testing::HasSubstr;
using ::testing::UnorderedElementsAre;
using ::zetasql_base::testing::StatusIs;

using EngineSystemCatalogTest = ::postgres_translator::test::ValidMemoryContext;

const zetasql::Type* gsql_bool = zetasql::types::BoolType();
const zetasql::Type* gsql_bytes = zetasql::types::BytesType();
const zetasql::Type* gsql_int32 = zetasql::types::Int32Type();
const zetasql::Type* gsql_int64 = zetasql::types::Int64Type();
const zetasql::Type* gsql_double = zetasql::types::DoubleType();
const zetasql::Type* gsql_string = zetasql::types::StringType();
const zetasql::Type* gsql_timestamp = zetasql::types::TimestampType();
const zetasql::Type* gsql_date = zetasql::types::DateType();
const zetasql::Type* gsql_interval = zetasql::types::IntervalType();

const zetasql::Type* gsql_bool_array = zetasql::types::BoolArrayType();
const zetasql::Type* gsql_bytes_array = zetasql::types::BytesArrayType();
const zetasql::Type* gsql_int64_array = zetasql::types::Int64ArrayType();
const zetasql::Type* gsql_double_array = zetasql::types::DoubleArrayType();
const zetasql::Type* gsql_string_array = zetasql::types::StringArrayType();
const zetasql::Type* gsql_timestamp_array =
    zetasql::types::TimestampArrayType();
const zetasql::Type* gsql_date_array = zetasql::types::DateArrayType();
const zetasql::Type* gsql_interval_array =
    zetasql::types::IntervalArrayType();

static zetasql::LanguageOptions GetLanguageOptions() {
  return zetasql::LanguageOptions();
}

// A test catalog that starts out empty.
// The EngineSystemCatalogTests can add types and functions to the catalog and
// test the lookup mechanisms.
class TestSystemCatalog : public EngineSystemCatalog {
 public:
  using EngineSystemCatalog::AddCastOverrideFunction;

  // Group the constructor and SetUp.
  static absl::StatusOr<std::unique_ptr<TestSystemCatalog>> GetTestCatalog(
      absl::flat_hash_set<zetasql::LanguageFeature> language_features = {}) {
    zetasql::LanguageOptions language_options;
    language_options.SetEnabledLanguageFeatures(language_features);

    // We have to use WrapUnique because absl::make_unique cannot access
    // private constructors.
    std::unique_ptr<TestSystemCatalog> catalog =
        absl::WrapUnique(new TestSystemCatalog(language_options));
    ZETASQL_RETURN_IF_ERROR(catalog->SetUp(language_options));
    return catalog;
  }

  absl::Status AddTestType(const PostgresTypeMapping* type) {
    return AddType(type, GetLanguageOptions());
  }

  absl::Status AddTestFunction(
      const PostgresFunctionArguments& function_arguments) {
    return AddFunction(function_arguments, GetLanguageOptions());
  }

  absl::Status AddTestExprFunction(const PostgresExprIdentifier& expr_type,
                                   const std::string& builtin_function_name) {
    return AddExprFunction(expr_type, builtin_function_name);
  }

  absl::Status AddTypeIfNotPresent(const PostgresTypeMapping* type) {
    if (GetType(std::string(type->raw_type_name())) == nullptr) {
      return AddType(type, GetLanguageOptions());
    }
    return absl::OkStatus();
  }

  // A function with signatures that aren't used by operators.
  absl::Status AddAbsFunction(
      absl::string_view postgres_namespace = "pg_catalog") {
    ZETASQL_RETURN_IF_ERROR(AddTypeIfNotPresent(types::PgInt8Mapping()));
    ZETASQL_RETURN_IF_ERROR(AddTypeIfNotPresent(types::PgFloat8Mapping()));

    PostgresFunctionArguments arguments(
        "abs", "abs",
        {{{gsql_int64, {gsql_int64}, /*context_ptr=*/nullptr}},
         {{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}},
        /*mode=*/zetasql::Function::SCALAR, postgres_namespace);
    ZETASQL_RETURN_IF_ERROR(AddFunction(arguments, GetLanguageOptions()));
    return absl::OkStatus();
  }

  // A function which is used by the '@' operator.
  absl::Status AddFloat8AbsFunction() {
    ZETASQL_RETURN_IF_ERROR(AddTypeIfNotPresent(types::PgFloat8Mapping()));

    PostgresFunctionArguments arguments(
        "float8abs", "abs",
        {{{gsql_double, {gsql_double}, /*context_ptr=*/nullptr}}});
    ZETASQL_RETURN_IF_ERROR(AddFunction(arguments, GetLanguageOptions()));
    return absl::OkStatus();
  }

  // A function with fixed args in PG and variable args in ZetaSQL.
  absl::Status AddConcatFunction() {
    ZETASQL_RETURN_IF_ERROR(AddTypeIfNotPresent(types::PgTextMapping()));

    PostgresFunctionArguments arguments(
        "textcat", "concat",
        {{{gsql_string, {gsql_string, gsql_string}, /*context_ptr=*/nullptr}}});
    ZETASQL_RETURN_IF_ERROR(AddFunction(arguments, GetLanguageOptions()));
    return absl::OkStatus();
  }

  // An aggregate function.
  absl::Status AddBoolAndFunction() {
    ZETASQL_RETURN_IF_ERROR(AddTypeIfNotPresent(types::PgBoolMapping()));

    PostgresFunctionArguments arguments(
        {"bool_and",
         "logical_and",
         {{{gsql_bool, {gsql_bool}, /*context_ptr=*/nullptr}}},
         zetasql::Function::AGGREGATE});
    ZETASQL_RETURN_IF_ERROR(AddFunction(arguments, GetLanguageOptions()));
    return absl::OkStatus();
  }

  // A set returning function.
  absl::Status AddGenerateSeriesFunction(bool set_oid) {
    ZETASQL_RETURN_IF_ERROR(AddTypeIfNotPresent(types::PgInt8Mapping()));
    ZETASQL_RETURN_IF_ERROR(AddTypeIfNotPresent(types::PgInt8ArrayMapping()));

    PostgresFunctionArguments arguments(
        {"generate_series",
         "generate_array",
         {{{gsql_int64_array,
            {gsql_int64, gsql_int64},
            /*context_ptr=*/nullptr},
           /*has_mapped_function=*/true,
           /*explicit_mapped_function_name=*/"",
           set_oid ? F_GENERATE_SERIES_INT8_INT8 : InvalidOid}}});
    ZETASQL_RETURN_IF_ERROR(AddFunction(arguments, GetLanguageOptions()));
    return absl::OkStatus();
  }

  absl::StatusOr<FunctionAndSignature> GetPgNumericCastFunction(
      const zetasql::Type* source_type, const zetasql::Type* target_type,
      const zetasql::LanguageOptions& language_options) override {
    return absl::UnimplementedError(
        "PgNumeric casts not present in TestSystemCatalog");
  }

 private:
  TestSystemCatalog(zetasql::LanguageOptions language_options)
      : EngineSystemCatalog(
            "test",
            std::make_unique<StubBuiltinFunctionCatalog>(language_options)) {}

  absl::Status AddTypes(
      const zetasql::LanguageOptions& language_options) override {
    return absl::OkStatus();
  }

  absl::Status AddFunctions(
      const zetasql::LanguageOptions& language_options) override {
    return absl::OkStatus();
  }
};

void CheckType(const PostgresTypeMapping* pg_mapping,
               const std::string& pg_type_name, Oid pg_type_oid,
               const zetasql::Type* mapped_type) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestSystemCatalog> catalog,
                       TestSystemCatalog::GetTestCatalog());
  ZETASQL_ASSERT_OK(catalog->AddTestType(pg_mapping));

  const PostgresTypeMapping* catalog_type;
  // Look up the type by PG name.
  catalog_type = catalog->GetType(pg_type_name);
  EXPECT_EQ(catalog_type, pg_mapping);

  // Look up the type by PG oid.
  catalog_type = catalog->GetType(pg_type_oid);
  EXPECT_EQ(catalog_type, pg_mapping);

  // If there is a mapped ZetaSQL type, reverse look up the type.
  // Otherwise, the type is not supported.
  if (mapped_type != nullptr) {
    catalog_type = catalog->GetTypeFromReverseMapping(mapped_type);
    EXPECT_EQ(catalog_type, pg_mapping);
  }
}

// For each type, add the type to the catalog and test lookups by name and oid.
// For supported types, also test reverse lookups by the mapped type name.
TEST_F(EngineSystemCatalogTest, BoolTest) {
  // Supported Scalar Types.
  CheckType(types::PgBoolMapping(), "bool", BOOLOID, gsql_bool);
  CheckType(types::PgInt8Mapping(), "int8", INT8OID, gsql_int64);
  CheckType(types::PgFloat8Mapping(), "float8", FLOAT8OID, gsql_double);
  // No reverse lookup for varchar because text and varchar are both mapped to
  // ZetaSQL STRING and the reverse mapping defaults to text.
  CheckType(types::PgVarcharMapping(), "varchar", VARCHAROID,
            /*mapped_type=*/nullptr);
  CheckType(types::PgTextMapping(), "text", TEXTOID, gsql_string);
  CheckType(types::PgByteaMapping(), "bytea", BYTEAOID, gsql_bytes);
  CheckType(types::PgTimestamptzMapping(), "timestamptz", TIMESTAMPTZOID,
            gsql_timestamp);
  CheckType(types::PgDateMapping(), "date", DATEOID, gsql_date);
  CheckType(types::PgIntervalMapping(), "interval", INTERVALOID, gsql_interval);

  // Supported Array Types.
  CheckType(types::PgBoolArrayMapping(), "_bool", BOOLARRAYOID,
            gsql_bool_array);
  CheckType(types::PgInt8ArrayMapping(), "_int8", INT8ARRAYOID,
            gsql_int64_array);
  CheckType(types::PgFloat8ArrayMapping(), "_float8", FLOAT8ARRAYOID,
            gsql_double_array);
  // No reverse lookup for varchar array because text array and varchar array
  // are both mapped to ZetaSQL STRING array and the reverse mapping defaults
  // to text array.
  CheckType(types::PgVarcharArrayMapping(), "_varchar", VARCHARARRAYOID,
            /*mapped_type=*/nullptr);
  CheckType(types::PgTextArrayMapping(), "_text", TEXTARRAYOID,
            gsql_string_array);
  CheckType(types::PgByteaArrayMapping(), "_bytea", BYTEAARRAYOID,
            gsql_bytes_array);
  CheckType(types::PgTimestamptzArrayMapping(), "_timestamptz",
            TIMESTAMPTZARRAYOID, gsql_timestamp_array);
  CheckType(types::PgDateArrayMapping(), "_date", DATEARRAYOID,
            gsql_date_array);
  CheckType(types::PgIntervalArrayMapping(), "_interval", INTERVALARRAYOID,
            gsql_interval_array);
}

// GetTypes is only used by the RQG and will return the mapped ZetaSQL types.
TEST_F(EngineSystemCatalogTest, GetTypes) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestSystemCatalog> catalog,
                       TestSystemCatalog::GetTestCatalog());
  // Supported Scalar Types.
  ZETASQL_ASSERT_OK(catalog->AddTestType(types::PgBoolMapping()));
  ZETASQL_ASSERT_OK(catalog->AddTestType(types::PgInt8Mapping()));
  ZETASQL_ASSERT_OK(catalog->AddTestType(types::PgFloat8Mapping()));
  ZETASQL_ASSERT_OK(catalog->AddTestType(types::PgVarcharMapping()));
  ZETASQL_ASSERT_OK(catalog->AddTestType(types::PgTextMapping()));
  ZETASQL_ASSERT_OK(catalog->AddTestType(types::PgByteaMapping()));
  ZETASQL_ASSERT_OK(catalog->AddTestType(types::PgTimestamptzMapping()));
  ZETASQL_ASSERT_OK(catalog->AddTestType(types::PgDateMapping()));

  // Supported Array Types.
  ZETASQL_ASSERT_OK(catalog->AddTestType(types::PgBoolArrayMapping()));
  ZETASQL_ASSERT_OK(catalog->AddTestType(types::PgInt8ArrayMapping()));
  ZETASQL_ASSERT_OK(catalog->AddTestType(types::PgFloat8ArrayMapping()));
  ZETASQL_ASSERT_OK(catalog->AddTestType(types::PgVarcharArrayMapping()));
  ZETASQL_ASSERT_OK(catalog->AddTestType(types::PgTextArrayMapping()));
  ZETASQL_ASSERT_OK(catalog->AddTestType(types::PgByteaArrayMapping()));
  ZETASQL_ASSERT_OK(catalog->AddTestType(types::PgTimestamptzArrayMapping()));
  ZETASQL_ASSERT_OK(catalog->AddTestType(types::PgDateArrayMapping()));

  absl::flat_hash_set<const zetasql::Type*> types;
  ZETASQL_ASSERT_OK(catalog->GetTypes(&types));
  EXPECT_THAT(
      types,
      UnorderedElementsAre(
          zetasql::types::BoolType(), zetasql::types::Int64Type(),
          zetasql::types::DoubleType(), zetasql::types::StringType(),
          zetasql::types::BytesType(), zetasql::types::TimestampType(),
          zetasql::types::DateType(), zetasql::types::BoolArrayType(),
          zetasql::types::Int64ArrayType(),
          zetasql::types::DoubleArrayType(),
          zetasql::types::StringArrayType(),
          zetasql::types::BytesArrayType(),
          zetasql::types::TimestampArrayType(),
          zetasql::types::DateArrayType()));

  absl::flat_hash_set<const PostgresTypeMapping*> type_mappings;
  ZETASQL_ASSERT_OK(catalog->GetPostgreSQLTypes(&type_mappings));
  EXPECT_THAT(
      type_mappings,
      UnorderedElementsAre(
          // Scalar type mappings.
          types::PgBoolMapping(), types::PgInt8Mapping(),
          types::PgFloat8Mapping(), types::PgVarcharMapping(),
          types::PgTextMapping(), types::PgByteaMapping(),
          types::PgTimestamptzMapping(), types::PgDateMapping(),

          // Array type mappings.
          types::PgBoolArrayMapping(), types::PgInt8ArrayMapping(),
          types::PgFloat8ArrayMapping(), types::PgVarcharArrayMapping(),
          types::PgTextArrayMapping(), types::PgByteaArrayMapping(),
          types::PgTimestamptzArrayMapping(), types::PgDateArrayMapping()));
}

TEST_F(EngineSystemCatalogTest, TypeNotFound) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestSystemCatalog> catalog,
                       TestSystemCatalog::GetTestCatalog());
  const zetasql::Type* type;
  ZETASQL_ASSERT_OK(catalog->GetType("nonexistent_type", &type));
  EXPECT_EQ(type, nullptr);
}

TEST_F(EngineSystemCatalogTest, TypeReverseNotFound) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestSystemCatalog> catalog,
                       TestSystemCatalog::GetTestCatalog());
  const PostgresTypeMapping* type =
      catalog->GetTypeFromReverseMapping(zetasql::types::Uint32Type());
  EXPECT_EQ(type, nullptr);
}

TEST_F(EngineSystemCatalogTest, FunctionSignaturesByIdx) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestSystemCatalog> catalog,
                       TestSystemCatalog::GetTestCatalog());
  ZETASQL_ASSERT_OK(catalog->AddAbsFunction());

  const PostgresExtendedFunction* function = nullptr;
  // Look at abs, which has mapped googlesql functions.
  function = catalog->GetFunction("pg_catalog", "abs");
  ASSERT_NE(function, nullptr);
  ASSERT_EQ(function->mode(), zetasql::Function::SCALAR);
  ASSERT_EQ(function->NumSignatures(), 2);
  const PostgresExtendedFunctionSignature* signature =
      function->GetPostgresSignature(0);
  ASSERT_NE(signature, nullptr);
  EXPECT_NE(signature->mapped_function(), nullptr);
  EXPECT_EQ(signature->postgres_proc_oid(), 1396);
  ASSERT_EQ(signature->arguments().size(), 1);
  EXPECT_EQ(signature->argument(0).type(), zetasql::types::Int64Type());

  signature = function->GetPostgresSignature(1);
  ASSERT_NE(signature, nullptr);
  EXPECT_NE(signature->mapped_function(), nullptr);
  EXPECT_EQ(signature->postgres_proc_oid(), 1395);
  ASSERT_EQ(signature->arguments().size(), 1);
  EXPECT_EQ(signature->argument(0).type(), zetasql::types::DoubleType());
}

TEST_F(EngineSystemCatalogTest, FunctionSignaturesByOid) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestSystemCatalog> catalog,
                       TestSystemCatalog::GetTestCatalog());
  ZETASQL_ASSERT_OK(catalog->AddAbsFunction());

  std::vector<zetasql::InputArgumentType> input_argument_types;
  input_argument_types.emplace_back(zetasql::types::Int64Type());

  // Get the function and signature for PG proc abs(int8_t) -> int8_t.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      FunctionAndSignature function_and_signature,
      catalog->GetFunctionAndSignature(F_ABS_INT8, input_argument_types,
                                       GetLanguageOptions()));
  ASSERT_EQ(function_and_signature.signature().arguments().size(), 1);
  EXPECT_EQ(function_and_signature.signature().argument(0).type(),
            zetasql::types::Int64Type());

  input_argument_types.clear();
  input_argument_types.emplace_back(zetasql::types::DoubleType());

  // Get the function and signature for PG proc abs(float8) -> float8.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      FunctionAndSignature double_function_and_signature,
      catalog->GetFunctionAndSignature(F_ABS_FLOAT8, input_argument_types,
                                       GetLanguageOptions()));
  ASSERT_EQ(double_function_and_signature.signature().arguments().size(), 1);
  EXPECT_EQ(double_function_and_signature.signature().argument(0).type(),
            zetasql::types::DoubleType());
}

// GetFunctions is only used by the RQG and will return the mapped functions.
TEST_F(EngineSystemCatalogTest, GetFunctions) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestSystemCatalog> catalog,
                       TestSystemCatalog::GetTestCatalog());
  // Add a function.
  ZETASQL_ASSERT_OK(catalog->AddAbsFunction());
  // Add an ExprType.
  ZETASQL_ASSERT_OK(catalog->AddTestExprFunction(
      PostgresExprIdentifier::Expr(T_CoalesceExpr), "coalesce"));
  // Add a Bool ExprType.
  ZETASQL_ASSERT_OK(catalog->AddTestExprFunction(
      PostgresExprIdentifier::BoolExpr(OR_EXPR), "$or"));

  absl::flat_hash_set<const zetasql::Function*> functions;
  ZETASQL_ASSERT_OK(catalog->GetFunctions(&functions));

  // Collect all the function names and also the set of return types for
  // functions named "abs".
  absl::flat_hash_set<absl::string_view> function_names;
  absl::flat_hash_set<const zetasql::Type*> abs_result_types;
  for (const zetasql::Function* function : functions) {
    function_names.insert(function->Name());
    if (function->Name() == "abs") {
      abs_result_types.insert(function->GetSignature(0)->result_type().type());
    }
  }

  // Check the expected set of function names.
  EXPECT_THAT(function_names, UnorderedElementsAre("abs", "coalesce", "$or"));

  // Check that the signatures are included.
  EXPECT_THAT(abs_result_types,
              UnorderedElementsAre(zetasql::types::Int64Type(),
                                   zetasql::types::DoubleType()));
}

TEST_F(EngineSystemCatalogTest, GetSetReturningFunctions) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestSystemCatalog> catalog,
                       TestSystemCatalog::GetTestCatalog());
  // Add a function.
  ZETASQL_ASSERT_OK(catalog->AddGenerateSeriesFunction(/*set_oid=*/true));

  absl::flat_hash_set<const zetasql::Function*> functions;
  ZETASQL_ASSERT_OK(catalog->GetSetReturningFunctions(&functions));

  // Collect all the function names.
  absl::flat_hash_set<absl::string_view> function_names;
  for (const zetasql::Function* function : functions) {
    function_names.insert(function->Name());
  }

  // Check the expected set of function names.
  EXPECT_THAT(function_names, UnorderedElementsAre("generate_array"));
}

TEST_F(EngineSystemCatalogTest, GetSetReturningFunctionsWithoutSetOid) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestSystemCatalog> catalog,
                       TestSystemCatalog::GetTestCatalog());
  // Add a function.
  absl::Status actual_status =
      catalog->AddGenerateSeriesFunction(/*set_oid=*/false);

  absl::flat_hash_set<const zetasql::Function*> functions;
  ZETASQL_ASSERT_OK(catalog->GetSetReturningFunctions(&functions));

  // Collect all the function names.
  absl::flat_hash_set<absl::string_view> function_names;
  for (const zetasql::Function* function : functions) {
    function_names.insert(function->Name());
  }

  // Check the expected set of function names.
  EXPECT_THAT(function_names, UnorderedElementsAre("generate_array"));
}

TEST_F(EngineSystemCatalogTest, ReverseFunctionSignatureLookup) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestSystemCatalog> catalog,
                       TestSystemCatalog::GetTestCatalog());
  ZETASQL_ASSERT_OK(catalog->AddAbsFunction());

  std::vector<zetasql::InputArgumentType> input_argument_types;
  input_argument_types.emplace_back(zetasql::types::Int64Type());
  ZETASQL_ASSERT_OK_AND_ASSIGN(Oid proc_oid,
                       catalog->GetPgProcOidFromReverseMapping(
                           "abs", input_argument_types, GetLanguageOptions()));
  // Expect PG proc abs(int8_t) -> int8_t.
  EXPECT_EQ(proc_oid, F_ABS_INT8);

  input_argument_types.clear();
  input_argument_types.emplace_back(zetasql::types::DoubleType());
  ZETASQL_ASSERT_OK_AND_ASSIGN(proc_oid,
                       catalog->GetPgProcOidFromReverseMapping(
                           "abs", input_argument_types, GetLanguageOptions()));
  // Expect PG proc abs(float8) -> float8.
  EXPECT_EQ(proc_oid, F_ABS_FLOAT8);
}

TEST_F(EngineSystemCatalogTest, AnyArgTypeMatch) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestSystemCatalog> catalog,
                       TestSystemCatalog::GetTestCatalog());
  ZETASQL_ASSERT_OK(catalog->AddTestType(types::PgBoolMapping()));

  // The ZetaSQL $equals function accepts two ARG_TYPE_ANY_1 input parameters
  // of the same type.
  PostgresFunctionArguments function_arguments{
      "booleq",
      "$equal",
      {{{gsql_bool, {gsql_bool, gsql_bool}, /*context_ptr=*/nullptr}}}};
  ZETASQL_ASSERT_OK(catalog->AddTestFunction(function_arguments));
}

TEST_F(EngineSystemCatalogTest, ArrayTypeMatch) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestSystemCatalog> catalog,
                       TestSystemCatalog::GetTestCatalog());
  ZETASQL_ASSERT_OK(catalog->AddTestType(types::PgInt8ArrayMapping()));
  ZETASQL_ASSERT_OK(catalog->AddTestType(types::PgFloat8ArrayMapping()));

  // The correct signature is float8_combine(float8_array, float8_array).
  // Attempting to register the mapping as (float8_array, int8_array) will fail.
  PostgresFunctionArguments function_arguments{
      "float8_combine",
      "float8_combine",
      {{{gsql_double_array,
         {gsql_double_array, gsql_int64_array},
         /*context_ptr=*/nullptr}}}};
  EXPECT_THAT(catalog->AddTestFunction(function_arguments),
              StatusIs(absl::StatusCode::kUnimplemented,
                       HasSubstr("No Postgres proc oid found")));
}

TEST_F(EngineSystemCatalogTest, ReverseFunctionOperatorOverride) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestSystemCatalog> catalog,
                       TestSystemCatalog::GetTestCatalog());
  ZETASQL_ASSERT_OK(catalog->AddAbsFunction());

  std::vector<zetasql::InputArgumentType> input_argument_types;
  input_argument_types.emplace_back(zetasql::types::DoubleType());

  // Only the non-operator version of abs has been added,
  ZETASQL_ASSERT_OK_AND_ASSIGN(Oid proc_oid,
                       catalog->GetPgProcOidFromReverseMapping(
                           "abs", input_argument_types, GetLanguageOptions()));
  // Expect PG proc abs(float8) -> float8.
  EXPECT_EQ(proc_oid, F_ABS_FLOAT8);

  // Add the operator version of abs. The reverse lookup should choose the
  // operator function over the non-operator function
  ZETASQL_ASSERT_OK(catalog->AddFloat8AbsFunction());
  ZETASQL_ASSERT_OK_AND_ASSIGN(proc_oid,
                       catalog->GetPgProcOidFromReverseMapping(
                           "abs", input_argument_types, GetLanguageOptions()));
  // Expect PG proc float8abs(float8) -> float8.
  EXPECT_EQ(proc_oid, F_FLOAT8ABS);
}

TEST_F(EngineSystemCatalogTest, VariableArgFunction) {
  // The PG textcat 2-arg function is mapped to the GSQL concat variable-arg
  // function.
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestSystemCatalog> catalog,
                       TestSystemCatalog::GetTestCatalog());
  ZETASQL_ASSERT_OK(catalog->AddConcatFunction());

  // Two string inputs.
  std::vector<zetasql::InputArgumentType> input_argument_types;
  input_argument_types.emplace_back(zetasql::types::StringType());
  input_argument_types.emplace_back(zetasql::types::StringType());

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Oid proc_oid, catalog->GetPgProcOidFromReverseMapping(
                        "concat", input_argument_types, GetLanguageOptions()));
  // Expect PG proc textcat(text, text) -> text.
  EXPECT_EQ(proc_oid, F_TEXTCAT);

  // However, more than 2 inputs will fail in the reverse transformer.
  input_argument_types.emplace_back(zetasql::types::StringType());
  EXPECT_THAT(catalog->GetPgProcOidFromReverseMapping(
                  "concat", input_argument_types, GetLanguageOptions()),
              StatusIs(absl::StatusCode::kUnimplemented,
                       HasSubstr("No Postgres proc oid found")));
}

TEST_F(EngineSystemCatalogTest, NoMappedFunction) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestSystemCatalog> catalog,
                       TestSystemCatalog::GetTestCatalog());

  ZETASQL_ASSERT_OK(catalog->AddTypeIfNotPresent(types::PgInt8Mapping()));
  ZETASQL_ASSERT_OK(catalog->AddTypeIfNotPresent(types::PgFloat8Mapping()));

  // Add a PostgreSQL function without a mapped function.
  PostgresFunctionArguments arguments(
      "float8", /*mapped_function_name=*/"",
      {{{gsql_double, {gsql_int64}, /*context_ptr=*/nullptr},
        /*has_mapped_function=*/false}});
  ZETASQL_ASSERT_OK(catalog->AddTestFunction(arguments));

  std::vector<zetasql::InputArgumentType> input_argument_types;
  input_argument_types.emplace_back(zetasql::types::Int64Type());

  // Get the function and signature for casting function PG proc
  // float8(bigint) -> float8.
  EXPECT_THAT(catalog->GetFunctionAndSignature(
                  F_FLOAT8_INT8, input_argument_types, GetLanguageOptions()),
              StatusIs(absl::StatusCode::kUnimplemented,
                       HasSubstr("requires an explicit cast")));
}

TEST_F(EngineSystemCatalogTest, MismatchedFunction) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestSystemCatalog> catalog,
                       TestSystemCatalog::GetTestCatalog());

  ZETASQL_ASSERT_OK(catalog->AddTypeIfNotPresent(types::PgInt8Mapping()));

  // Try to map two functions with completely different sigantures.
  // abs has one input and $equal has two.
  PostgresFunctionArguments arguments(
      "abs", "$equal", {{{gsql_int64, {gsql_int64}, /*context_ptr=*/nullptr}}});
  EXPECT_THAT(catalog->AddTestFunction(arguments),
              StatusIs(absl::StatusCode::kNotFound,
                       HasSubstr("does not have signature")));
}

TEST_F(EngineSystemCatalogTest, UnvalidatedFunctionSignature) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestSystemCatalog> catalog,
                       TestSystemCatalog::GetTestCatalog());

  zetasql::FunctionArgumentTypeOptions repeated(
      zetasql::FunctionArgumentType::REPEATED);
  PostgresFunctionArguments arguments("concat", "concat",
                                      {{{gsql_string,
                                         {gsql_string, {gsql_string, repeated}},
                                         /*context_ptr=*/nullptr},
                                        /*has_mapped_function=*/true,
                                        /*explicit_mapped_function_name=*/"",
                                        F_CONCAT}});
  ZETASQL_ASSERT_OK(catalog->AddTestFunction(arguments));

  std::vector<zetasql::InputArgumentType> input_argument_types;

  // Function calls with one or more arguments are supported.
  for (int i = 0; i < 20; ++i) {
    input_argument_types.emplace_back(zetasql::types::StringType());
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        FunctionAndSignature concat,
        catalog->GetFunctionAndSignature(F_CONCAT, input_argument_types,
                                         GetLanguageOptions()));
  }
}

TEST_F(EngineSystemCatalogTest, AnyOidAggregateFunction) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestSystemCatalog> catalog,
                       TestSystemCatalog::GetTestCatalog());

  ZETASQL_ASSERT_OK(catalog->AddBoolAndFunction());
  const PostgresExtendedFunction* function =
      catalog->GetFunction("pg_catalog", "bool_and");
  ASSERT_NE(function, nullptr);
  ASSERT_EQ(function->mode(), zetasql::Function::AGGREGATE);
  ASSERT_EQ(function->NumSignatures(), 1);
}

TEST_F(EngineSystemCatalogTest, UnsupportedExpr) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestSystemCatalog> catalog,
                       TestSystemCatalog::GetTestCatalog());
  ZETASQL_ASSERT_OK(catalog->AddTypeIfNotPresent(types::PgBoolMapping()));

  PostgresExprIdentifier identifier =
      PostgresExprIdentifier::Expr(T_AlternativeSubPlan);
  std::vector<zetasql::InputArgumentType> input_argument_types;
  input_argument_types.emplace_back(zetasql::types::BoolType());

  EXPECT_THAT(catalog->GetFunctionAndSignature(identifier, input_argument_types,
                                               GetLanguageOptions()),
              StatusIs(absl::StatusCode::kUnimplemented,
                       HasSubstr("AlternativeSubPlan, Arguments: (boolean)")));
}

TEST_F(EngineSystemCatalogTest, BoolExpr) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestSystemCatalog> catalog,
                       TestSystemCatalog::GetTestCatalog());

  absl::flat_hash_map<BoolExprType, std::string> bool_exprs = {
      {AND_EXPR, "$and"}, {NOT_EXPR, "$not"}, {OR_EXPR, "$or"}};

  // AND and OR are variadic and can have an unlimited number of inputs. Use 5
  // and 20 as a random example of a long list of inputs.
  // NOT can only accept one input.
  absl::flat_hash_map<BoolExprType, int> num_arguments = {
      {AND_EXPR, 5}, {NOT_EXPR, 1}, {OR_EXPR, 20}};

  std::vector<zetasql::InputArgumentType> input_argument_types;
  for (const auto& [bool_type, builtin_function_name] : bool_exprs) {
    PostgresExprIdentifier identifier =
        PostgresExprIdentifier::BoolExpr(bool_type);

    // Add the bool expr -> builtin function mapping.
    ZETASQL_ASSERT_OK(catalog->AddTestExprFunction(identifier, builtin_function_name));

    // Construct the input arguments.
    input_argument_types.clear();
    for (int i = 0; i < num_arguments[bool_type]; ++i) {
      input_argument_types.emplace_back(zetasql::types::BoolType());
    }

    // Get the function and signature in the forward direction.
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        FunctionAndSignature function_and_signature,
        catalog->GetFunctionAndSignature(identifier, input_argument_types,
                                         GetLanguageOptions()));
    ASSERT_NE(function_and_signature.function(), nullptr);
    ASSERT_EQ(function_and_signature.function()->Name(), builtin_function_name);

    // Verify the signature argument types.
    ASSERT_EQ(function_and_signature.signature().NumConcreteArguments(),
              input_argument_types.size());
    for (int i = 0; i < input_argument_types.size(); ++i) {
      EXPECT_EQ(function_and_signature.signature().ConcreteArgumentType(i),
                input_argument_types[i].type());
    }

    // Get the BoolExprType in the reverse direction.
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        PostgresExprIdentifier reverse_expr,
        catalog->GetPostgresExprIdentifier(builtin_function_name));
    EXPECT_EQ(reverse_expr, identifier);
  }
}

TEST_F(EngineSystemCatalogTest, CoalesceExpr) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestSystemCatalog> catalog,
                       TestSystemCatalog::GetTestCatalog());

  ZETASQL_ASSERT_OK(catalog->AddTestExprFunction(
      PostgresExprIdentifier::Expr(T_CoalesceExpr), "coalesce"));

  // Coalesce is variadic and can have an unlimited number of inputs.
  // Use 6 as a random example of a long list of inputs.
  int num_inputs = 6;
  std::vector<zetasql::InputArgumentType> input_argument_types;
  for (int i = 0; i < num_inputs; ++i) {
    input_argument_types.emplace_back(zetasql::types::Int64Type());
  }

  // Get the function and signature in the forward direction.
  ZETASQL_ASSERT_OK_AND_ASSIGN(FunctionAndSignature function_and_signature,
                       catalog->GetFunctionAndSignature(
                           PostgresExprIdentifier::Expr(T_CoalesceExpr),
                           input_argument_types, GetLanguageOptions()));
  ASSERT_NE(function_and_signature.function(), nullptr);
  ASSERT_EQ(function_and_signature.function()->Name(), "coalesce");

  // Verify the signature argument types.
  ASSERT_EQ(function_and_signature.signature().NumConcreteArguments(),
            input_argument_types.size());
  for (int i = 0; i < input_argument_types.size(); ++i) {
    EXPECT_EQ(function_and_signature.signature().ConcreteArgumentType(i),
              input_argument_types[i].type());
  }

  // Get the ExprType in the reverse direction.
  ZETASQL_ASSERT_OK_AND_ASSIGN(const PostgresExprIdentifier reverse_expr_node_tag,
                       catalog->GetPostgresExprIdentifier("coalesce"));
  EXPECT_EQ(reverse_expr_node_tag,
            PostgresExprIdentifier::Expr(T_CoalesceExpr));
}

TEST_F(EngineSystemCatalogTest, DuplicateFunction) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestSystemCatalog> catalog,
                       TestSystemCatalog::GetTestCatalog());
  ZETASQL_ASSERT_OK(catalog->AddTypeIfNotPresent(types::PgInt8Mapping()));

  PostgresFunctionArguments arguments(
      "abs", "abs", {{{gsql_int64, {gsql_int64}, /*context_ptr=*/nullptr}}});
  ZETASQL_ASSERT_OK(catalog->AddTestFunction(arguments));

  // Catalog should return an error if the function is added twice.
  EXPECT_THAT(catalog->AddTestFunction(arguments),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("already added to the catalog")));
}

TEST_F(EngineSystemCatalogTest, ExplicitMappedFunction) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestSystemCatalog> catalog,
                       TestSystemCatalog::GetTestCatalog());
  ZETASQL_ASSERT_OK(catalog->AddTestType(types::PgBoolMapping()));
  ZETASQL_ASSERT_OK(catalog->AddTestType(types::PgByteaMapping()));
  ZETASQL_ASSERT_OK(catalog->AddTestType(types::PgInt8Mapping()));
  ZETASQL_ASSERT_OK(catalog->AddTestType(types::PgFloat8Mapping()));
  ZETASQL_ASSERT_OK(catalog->AddTestType(types::PgTextMapping()));

  // The count(bool/bytes/int64_t/double/string/date) function calls should match
  // to the "count" builtin function, but count(*) should match to the
  // "$count_star" builtin function.
  PostgresFunctionArguments function_arguments{
      "count",
      "count",
      {{{gsql_int64, {gsql_bool}, /*context_ptr=*/nullptr}},
       {{gsql_int64, {gsql_bytes}, /*context_ptr=*/nullptr}},
       {{gsql_int64, {gsql_int64}, /*context_ptr=*/nullptr}},
       {{gsql_int64, {gsql_double}, /*context_ptr=*/nullptr}},
       {{gsql_int64, {gsql_string}, /*context_ptr=*/nullptr}},
       {{gsql_int64, {gsql_date}, /*context_ptr=*/nullptr}},
       {{gsql_int64, {}, /*context_ptr=*/nullptr},
        /*has_mapped_function=*/true,
        /*explicit_mapped_function_name=*/"$count_star"}},
      zetasql::Function::AGGREGATE};

  ZETASQL_ASSERT_OK(catalog->AddTestFunction(function_arguments));

  // select count(int8_t) should map to the "count" builtin function.
  std::vector<zetasql::InputArgumentType> input_argument_types;
  input_argument_types.emplace_back(zetasql::types::Int64Type());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      FunctionAndSignature count_int8,
      catalog->GetFunctionAndSignature(F_COUNT_ANY, input_argument_types,
                                       GetLanguageOptions()));
  ASSERT_NE(count_int8.function(), nullptr);
  EXPECT_EQ(count_int8.function()->Name(), "count");
  ASSERT_EQ(count_int8.signature().arguments().size(), 1);
  EXPECT_EQ(count_int8.signature().argument(0).type(),
            zetasql::types::Int64Type());

  // select count(*) should map to the $count_star builtin function.
  input_argument_types.clear();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      FunctionAndSignature count_star,
      catalog->GetFunctionAndSignature(F_COUNT_, input_argument_types,
                                       GetLanguageOptions()));
  ASSERT_NE(count_star.function(), nullptr);
  EXPECT_EQ(count_star.function()->Name(), "$count_star");
  EXPECT_EQ(count_star.signature().arguments().size(), 0);
}

TEST_F(EngineSystemCatalogTest, AnyoidFunction) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestSystemCatalog> catalog,
                       TestSystemCatalog::GetTestCatalog());
  ZETASQL_ASSERT_OK(catalog->AddTestType(types::PgTextMapping()));

  PostgresFunctionArguments function_arguments{
      "concat",
      "concat",
      {{{gsql_string, {gsql_string, gsql_string}, /*context_ptr=*/nullptr}}}};

  // Concat is a variadic  function in postgres that accepts any types. Confirm
  // that Spangres is able to match a signature with two inputs to the concat
  // function.
  ZETASQL_ASSERT_OK(catalog->AddTestFunction(function_arguments));

  std::vector<zetasql::InputArgumentType> input_argument_types;

  // Function calls with one argument are unsupported.
  input_argument_types.emplace_back(zetasql::types::StringType());
  EXPECT_THAT(catalog->GetFunctionAndSignature(F_CONCAT, input_argument_types,
                                               GetLanguageOptions()),
              StatusIs(absl::StatusCode::kUnimplemented,
                       HasSubstr("(text) is not supported")));

  // Function calls with two arguments are supported.
  input_argument_types.emplace_back(zetasql::types::StringType());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      FunctionAndSignature concat,
      catalog->GetFunctionAndSignature(F_CONCAT, input_argument_types,
                                       GetLanguageOptions()));

  // Function calls with three arguments are unsupported.
  input_argument_types.emplace_back(zetasql::types::StringType());
  EXPECT_THAT(catalog->GetFunctionAndSignature(F_CONCAT, input_argument_types,
                                               GetLanguageOptions()),
              StatusIs(absl::StatusCode::kUnimplemented,
                       HasSubstr("(text, text, text) is not supported")));
}

TEST_F(EngineSystemCatalogTest, InvalidReturnType) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestSystemCatalog> catalog,
                       TestSystemCatalog::GetTestCatalog());

  ZETASQL_ASSERT_OK(catalog->AddTypeIfNotPresent(types::PgByteaMapping()));
  ZETASQL_ASSERT_OK(catalog->AddTypeIfNotPresent(types::PgTextMapping()));

  // The PG md5 signature returns text and the ZetaSQL signature returns
  // bytes.
  PostgresFunctionArguments pg_arguments(
      "md5", "md5", {{{gsql_string, {gsql_string}, /*context_ptr=*/nullptr}}});
  EXPECT_THAT(catalog->AddTestFunction(pg_arguments),
              StatusIs(absl::StatusCode::kNotFound,
                       HasSubstr("does not have signature")));

  PostgresFunctionArguments gsql_arguments(
      "md5", "md5", {{{gsql_bytes, {gsql_string}, /*context_ptr=*/nullptr}}});
  EXPECT_THAT(catalog->AddTestFunction(gsql_arguments),
              StatusIs(absl::StatusCode::kUnimplemented,
                       HasSubstr("No Postgres proc oid found")));
}

TEST_F(EngineSystemCatalogTest, InvalidNamespace) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestSystemCatalog> catalog,
                       TestSystemCatalog::GetTestCatalog());
  EXPECT_THAT(catalog->AddAbsFunction("public"),
              StatusIs(absl::StatusCode::kUnimplemented,
                       HasSubstr("No Postgres proc oid found")));

  EXPECT_THAT(
      catalog->AddAbsFunction("invalid_namespace"),
      StatusIs(absl::StatusCode::kNotFound,
               HasSubstr("Namespace with name invalid_namespace not found")));
}

TEST_F(EngineSystemCatalogTest, CastOverrideFunction) {
  // The default ZetaSQL catalog doesn't include date constructors.
  // Enable the feature so we have something to test  with.
  absl::flat_hash_set<zetasql::LanguageFeature> language_features = {
      zetasql::FEATURE_V_1_2_CIVIL_TIME,
      zetasql::FEATURE_V_1_3_DATE_TIME_CONSTRUCTORS};
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestSystemCatalog> catalog,
                       TestSystemCatalog::GetTestCatalog(language_features));
  zetasql::LanguageOptions language_options;
  language_options.SetEnabledLanguageFeatures(language_features);

  EXPECT_FALSE(catalog->HasCastOverrideFunction(gsql_string, gsql_date));
  EXPECT_THAT(catalog->GetCastOverrideFunctionAndSignature(
                  gsql_string, gsql_date, language_options),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("override function from <STRING> to <DATE>")));
  // If the signature doesn't match, we can't add the cast override function.
  EXPECT_THAT(
      catalog->AddCastOverrideFunction(gsql_string, gsql_date, "substr",
                                       language_options),
      StatusIs(
          absl::StatusCode::kNotFound,
          HasSubstr(
              "function named \"substr\" for casting <STRING> to <DATE>")));

  ZETASQL_ASSERT_OK(catalog->AddCastOverrideFunction(gsql_string, gsql_date, "date",
                                             language_options));
  EXPECT_TRUE(catalog->HasCastOverrideFunction(gsql_string, gsql_date));
  ZETASQL_ASSERT_OK_AND_ASSIGN(FunctionAndSignature function_and_signature,
                       catalog->GetCastOverrideFunctionAndSignature(
                           gsql_string, gsql_date, language_options));
  ASSERT_NE(function_and_signature.function(), nullptr);
  EXPECT_EQ(function_and_signature.function()->Name(), "date");
  ASSERT_EQ(function_and_signature.signature().arguments().size(), 1);
  EXPECT_EQ(function_and_signature.signature().argument(0).type(), gsql_string);
  EXPECT_EQ(function_and_signature.signature().result_type().type(), gsql_date);
  EXPECT_TRUE(catalog->IsGsqlFunctionMappedToPgCast("date"));
}

TEST_F(EngineSystemCatalogTest, SqlRewriteFunctionWithNamedArgs) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestSystemCatalog> catalog,
                       TestSystemCatalog::GetTestCatalog());
  ZETASQL_ASSERT_OK(catalog->AddTestType(types::PgBoolMapping()));
  ZETASQL_ASSERT_OK(catalog->AddTestType(types::PgInt8ArrayMapping()));

  // Add a PostgreSQL function that maps to a googlesql function that is
  // implemented as a rewrite with named args.
  PostgresFunctionArguments arguments{"arrayoverlap",
                                      "array_includes_any",
                                      {{{gsql_bool,
                                         {gsql_int64_array, gsql_int64_array},
                                         /*context_ptr=*/nullptr}}}};
  ZETASQL_ASSERT_OK(catalog->AddTestFunction(arguments));

  std::vector<zetasql::InputArgumentType> input_argument_types;
  input_argument_types.emplace_back(zetasql::types::Int64ArrayType());
  input_argument_types.emplace_back(zetasql::types::Int64ArrayType());

  // Get the function and signature for checking whether two arrays overlap.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      FunctionAndSignature function_and_signature,
      catalog->GetFunctionAndSignature(F_ARRAYOVERLAP, input_argument_types,
                                       GetLanguageOptions()));
  ASSERT_NE(function_and_signature.function(), nullptr);
  EXPECT_EQ(function_and_signature.function()->Name(), "array_includes_any");
  ASSERT_EQ(function_and_signature.signature().arguments().size(), 2);
  EXPECT_TRUE(
      function_and_signature.signature().argument(0).has_argument_name());
  EXPECT_EQ(function_and_signature.signature().argument(0).argument_name(),
            "array_to_search");
  EXPECT_EQ(function_and_signature.signature().argument(1).argument_name(),
            "search_values");
}

}  // namespace
}  // namespace postgres_translator
