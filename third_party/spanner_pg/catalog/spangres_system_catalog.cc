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

#include <optional>

#include "zetasql/public/builtin_function.h"
#include "zetasql/public/builtin_function_options.h"
#include "zetasql/analyzer/function_signature_matcher.h"
#include "zetasql/public/input_argument_type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/flags/flag.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "third_party/spanner_pg/bootstrap_catalog/bootstrap_catalog.h"
#include "third_party/spanner_pg/catalog/builtin_function.h"
#include "third_party/spanner_pg/catalog/builtin_rewrite_functions.h"
#include "third_party/spanner_pg/catalog/builtin_spanner_functions.h"
#include "third_party/spanner_pg/catalog/engine_system_catalog.h"
#include "third_party/spanner_pg/catalog/function.h"
#include "third_party/spanner_pg/catalog/spangres_type.h"
#include "third_party/spanner_pg/catalog/type.h"
#include "third_party/spanner_pg/datatypes/extended/conversion_finder.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_numeric_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_oid_type.h"
#include "third_party/spanner_pg/interface/emulator_builtin_function_catalog.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include <map>
#include <set>
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {
namespace spangres {

namespace builtin_types = ::postgres_translator::types;
namespace spangres_types = ::postgres_translator::spangres::types;

    const auto kComparisonOidMap = std::map<Oid, Oid>({
    {F_FLOAT8LT, F_INT8LT},  {F_FLOAT8GT, F_INT8GT},  {F_FLOAT8EQ, F_INT8EQ},
    {F_FLOAT8LE, F_INT8LE},  {F_FLOAT8GE, F_INT8GE},  {F_FLOAT8NE, F_INT8NE},
    {F_FLOAT4LT, F_INT8LT},  {F_FLOAT4GT, F_INT8GT},  {F_FLOAT4EQ, F_INT8EQ},
    {F_FLOAT4LE, F_INT8LE},  {F_FLOAT4GE, F_INT8GE},  {F_FLOAT4NE, F_INT8NE},
    {F_FLOAT48LT, F_INT8LT}, {F_FLOAT48GT, F_INT8GT}, {F_FLOAT48EQ, F_INT8EQ},
    {F_FLOAT48LE, F_INT8LE}, {F_FLOAT48GE, F_INT8GE}, {F_FLOAT48NE, F_INT8NE},
    {F_FLOAT84LT, F_INT8LT}, {F_FLOAT84GT, F_INT8GT}, {F_FLOAT84EQ, F_INT8EQ},
    {F_FLOAT84LE, F_INT8LE}, {F_FLOAT84GE, F_INT8GE}, {F_FLOAT84NE, F_INT8NE},
});

const auto kNanOrderingFunctions =
    std::set<absl::string_view>(
        {"pg.min", "pg.least", "pg.greatest", "pg.map_double_to_int"});

static bool FunctionNameSupportedInSpanner(
    const std::string& function_name,
    const zetasql::LanguageOptions& language_options,
    zetasql::TypeFactory* type_factory) {
  zetasql::BuiltinFunctionOptions function_options(language_options);
  absl::flat_hash_map<std::string, std::unique_ptr<zetasql::Function>>
      spanner_function_map;
  absl::flat_hash_map<std::string, const zetasql::Type*> types;
  zetasql::GetBuiltinFunctionsAndTypes(function_options, *type_factory,
                               spanner_function_map, types);
  if (spanner_function_map.find(function_name) !=
      spanner_function_map.end()) {
    return true;
  }

  // Several functions are special cased in the algebrizer and may not appear
  // in FunctionKindByName(). See GsqlAlgebrizer::AlgebrizeFunction.
  // When $extract_date and other special cases are added to the catalog, add
  // exceptions for them here.
  if (function_name == "$not_equal" ||
      function_name == "pending_commit_timestamp" ||
      function_name == "$make_array") {
    return true;
  }

  // The function is not supported in Spanner.
  return false;
}

absl::StatusOr<bool> SpangresSystemCatalog::TryInitializeEngineSystemCatalog(
    std::unique_ptr<EngineBuiltinFunctionCatalog> builtin_function_catalog,
    const zetasql::LanguageOptions& language_options) {
  absl::WriterMutexLock l(&engine_system_catalog_mutex);
  EngineSystemCatalog** engine_system_catalog =
      EngineSystemCatalog::GetEngineSystemCatalogPtr();
  if (*engine_system_catalog != nullptr) {
    // The EngineSystemCatalog singleton was already initialized.
      EmulatorBuiltinFunctionCatalog* source_builtin_function_catalog =
          static_cast<EmulatorBuiltinFunctionCatalog*>(
              builtin_function_catalog.get());
      EmulatorBuiltinFunctionCatalog* target_builtin_function_catalog =
          static_cast<EmulatorBuiltinFunctionCatalog*>(
              (*engine_system_catalog)->builtin_function_catalog());
      target_builtin_function_catalog->SetLatestSchema(
          source_builtin_function_catalog->GetLatestSchema());
    return false;
  }

  // Create and setup a new catalog. If setup is successful, set the
  // EngineSystemCatalog singleton to the new catalog. Otherwise, delete the
  // old catalog and return an error.
  EngineSystemCatalog* catalog =
      new SpangresSystemCatalog(std::move(builtin_function_catalog));
  absl::Status setup_status = catalog->SetUp(language_options);
  if (setup_status.ok()) {
    *engine_system_catalog = catalog;
    return true;
  } else {
    delete catalog;
    return setup_status;
  }
}

void SpangresSystemCatalog::ResetEngineSystemCatalogForTest() {
  absl::WriterMutexLock l(&engine_system_catalog_mutex);
  EngineSystemCatalog** engine_system_catalog =
      EngineSystemCatalog::GetEngineSystemCatalogPtr();
  if (*engine_system_catalog != nullptr) {
    delete *engine_system_catalog;
    *engine_system_catalog = nullptr;
  }
}

const PostgresTypeMapping* SpangresSystemCatalog::GetType(
    const std::string& name) const {
  const PostgresTypeMapping* type = EngineSystemCatalog::GetType(name);
  const zetasql::Type* gsql_pg_numeric =
      types::PgNumericMapping()->mapped_type();

  return type;
}

absl::Status SpangresSystemCatalog::GetCustomErrorForProc(Oid proc_oid) const {
  return absl::OkStatus();
}

static absl::StatusOr<std::string> GetPgNumericCastFunctionName(
    const zetasql::Type* source_type, const zetasql::Type* target_type,
    const zetasql::ProductMode product_mode, bool& is_fixed_precision_cast) {
  const zetasql::Type* gsql_pg_numeric =
      types::PgNumericMapping()->mapped_type();

  if (source_type->Equals(gsql_pg_numeric) &&
      target_type->Equals(gsql_pg_numeric)) {
    is_fixed_precision_cast = true;
    return "pg.cast_to_numeric";
  }

  is_fixed_precision_cast = false;
  std::string function_name;
  if (source_type->Equals(gsql_pg_numeric)) {
    switch (target_type->kind()) {
      case zetasql::TypeKind::TYPE_INT64:
        return "pg.cast_to_int64";
      case zetasql::TypeKind::TYPE_DOUBLE:
        return "pg.cast_to_double";
      case zetasql::TypeKind::TYPE_STRING:
        return "pg.cast_to_string";
      default:
        break;
    }
  }

  if (target_type->Equals(gsql_pg_numeric)) {
    switch (source_type->kind()) {
      case zetasql::TypeKind::TYPE_INT64:
      case zetasql::TypeKind::TYPE_DOUBLE:
      case zetasql::TypeKind::TYPE_STRING:
        return "pg.cast_to_numeric";
      default:
        break;
    }
  }

  return absl::NotFoundError(
      absl::StrCat("No cast found from ", source_type->TypeName(product_mode),
                   " to ", target_type->TypeName(product_mode)));
}

absl::StatusOr<FunctionAndSignature>
SpangresSystemCatalog::GetPgNumericCastFunction(
    const zetasql::Type* source_type, const zetasql::Type* target_type,
    const zetasql::LanguageOptions& language_options) {
  bool is_fixed_precision_cast;
  ZETASQL_ASSIGN_OR_RETURN(std::string function_name,
                   GetPgNumericCastFunctionName(source_type, target_type,
                                                language_options.product_mode(),
                                                is_fixed_precision_cast));
  ZETASQL_ASSIGN_OR_RETURN(const zetasql::Function* builtin_function,
                   GetBuiltinFunction(function_name));

  // Try find the matching signature. Run the ZetaSQL Function Signature
  // Matcher to determine if the input arguments exactly match the signature
  bool found_signature = false;
  std::vector<zetasql::InputArgumentType> input_argument_types;
  input_argument_types.push_back(zetasql::InputArgumentType(source_type));
  if (is_fixed_precision_cast) {
    input_argument_types.push_back(
        zetasql::InputArgumentType(zetasql::types::Int64Type()));
    input_argument_types.push_back(
        zetasql::InputArgumentType(zetasql::types::Int64Type()));
  }

  zetasql::Coercer coercer(type_factory(), &language_options);
  const std::vector<const zetasql::ASTNode*> arg_ast_nodes = {};
  std::unique_ptr<zetasql::FunctionSignature> result_signature;

  for (const zetasql::FunctionSignature& signature :
       builtin_function->signatures()) {
    zetasql::SignatureMatchResult signature_match_result;
    ZETASQL_ASSIGN_OR_RETURN(
        bool function_signature_matches,
        zetasql::FunctionSignatureMatchesWithStatus(
            language_options, coercer, arg_ast_nodes, input_argument_types,
            signature, /*allow_argument_coercion=*/false, type_factory(),
            /*resolve_lambda_callback=*/nullptr, &result_signature,
            &signature_match_result,
            /*arg_index_mapping=*/nullptr, /*arg_overrides=*/nullptr));

    found_signature = function_signature_matches &&
                      signature_match_result.non_matched_arguments() == 0 &&
                      signature_match_result.non_literals_coerced() == 0 &&
                      signature_match_result.literals_coerced() == 0;
    if (found_signature) {
      break;
    }
  }

  if (!found_signature) {
    return absl::NotFoundError(absl::StrCat(
        "No cast signature found from ",
        source_type->TypeName(language_options.product_mode()), " to ",
        target_type->TypeName(language_options.product_mode())));
  }

  return FunctionAndSignature(builtin_function, *result_signature);
}

bool SpangresSystemCatalog::IsTransformationRequiredForComparison(
    const zetasql::ResolvedExpr& gsql_expr) {

  // Transformation is required for expressions of type double and float as
  // Postgres' comparison semantics for double and float differ from Spanner
  // native double comparison semantics.
  if (gsql_expr.type()->IsDouble() || gsql_expr.type()->IsFloat()) {
    return true;
  }

  return false;
}

// Wraps the input `gsql_expr` with required transformations to preserve
// Postgres' comparison/order semantics.
absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
SpangresSystemCatalog::GetResolvedExprForComparison(
    std::unique_ptr<zetasql::ResolvedExpr> gsql_expr,
    const zetasql::LanguageOptions& language_options) {
  if (gsql_expr == nullptr ||
      !IsTransformationRequiredForComparison(*gsql_expr)) {
    return gsql_expr;
  }

  return GetResolvedExprForDoubleComparison(std::move(gsql_expr),
                                            language_options);
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
SpangresSystemCatalog::GetResolvedExprForDoubleComparison(
    std::unique_ptr<zetasql::ResolvedExpr> gsql_expr,
    const zetasql::LanguageOptions& language_options) {
  if (!(gsql_expr->type()->IsDouble() || gsql_expr->type()->IsFloat())) {
    return gsql_expr;
  }

  if (gsql_expr->type()->IsFloat()) {
    // First cast the float to a double, then use the standard mapping from
    // double to int64_t.
    gsql_expr = zetasql::MakeResolvedCast(zetasql::types::DoubleType(),
                                            std::move(gsql_expr),
                                            /*return_null_on_error=*/false);
  }

  ZETASQL_ASSIGN_OR_RETURN(FunctionAndSignature function_and_signature,
                   GetMapDoubleToIntFunction(language_options));

  std::vector<std::unique_ptr<zetasql::ResolvedExpr>> argument_list;
  argument_list.emplace_back(std::move(gsql_expr));

  return zetasql::MakeResolvedFunctionCall(
      zetasql::types::Int64Type(), function_and_signature.function(),
      function_and_signature.signature(), std::move(argument_list),
      zetasql::ResolvedFunctionCallBase::DEFAULT_ERROR_MODE);
}

bool SpangresSystemCatalog::IsResolvedExprForComparison(
    const zetasql::ResolvedExpr& gsql_expr) const {
  if (!gsql_expr.Is<zetasql::ResolvedFunctionCall>()) {
    return false;
  }

  auto func = gsql_expr.GetAs<zetasql::ResolvedFunctionCall>();
  return func->function()->FullName(/*include_group=*/false) ==
         "pg.map_double_to_int";
}

absl::StatusOr<const zetasql::ResolvedExpr*>
SpangresSystemCatalog::GetOriginalExprFromComparisonExpr(
    const zetasql::ResolvedExpr& mapped_gsql_expr) const {
  ZETASQL_RET_CHECK(IsResolvedExprForComparison(mapped_gsql_expr));
  const zetasql::ResolvedFunctionCall* func =
      mapped_gsql_expr.GetAs<zetasql::ResolvedFunctionCall>();
  const std::vector<std::unique_ptr<const zetasql::ResolvedExpr>>& args =
      func->argument_list();
  ZETASQL_RET_CHECK_EQ(func->argument_list_size(), 1);
  return args[0].get();
}

std::optional<Oid> SpangresSystemCatalog::GetMappedOidForComparisonFuncid(
    Oid funcid) const {

  const auto it = kComparisonOidMap.find(funcid);
  if (it != kComparisonOidMap.end()) {
    return it->second;
  }
  return std::nullopt;
}

absl::StatusOr<FunctionAndSignature>
SpangresSystemCatalog::GetMapDoubleToIntFunction(
    const zetasql::LanguageOptions& language_options) {
  // TOD(b/228246295): Lookup this FunctionAndSignature only once.
  static const std::string function_name = "pg.map_double_to_int";

  ZETASQL_ASSIGN_OR_RETURN(const zetasql::Function* builtin_function,
                   GetBuiltinFunction(function_name));

  ZETASQL_RET_CHECK(builtin_function) << "Cannot find pg.map_double_to_int in the list "
                                 "of builtin function of SpangresSystemCatalog";

  // Try find the matching signature. Run the ZetaSQL Function Signature
  // Matcher to determine if the input arguments exactly match the signature
  bool found_signature = false;
  static const std::vector<zetasql::InputArgumentType> input_argument_types{
      zetasql::InputArgumentType(zetasql::types::DoubleType())};

  zetasql::Coercer coercer(type_factory(), &language_options, this);
  const std::vector<const zetasql::ASTNode*> arg_ast_nodes;
  std::unique_ptr<zetasql::FunctionSignature> result_signature;

  for (const zetasql::FunctionSignature& signature :
       builtin_function->signatures()) {
    zetasql::SignatureMatchResult signature_match_result;
    absl::StatusOr<bool> function_signature_matches_or =
        zetasql::FunctionSignatureMatchesWithStatus(
            language_options, coercer, arg_ast_nodes, input_argument_types,
            signature, /*allow_argument_coercion=*/false, type_factory(),
            /*resolve_lambda_callback=*/nullptr, &result_signature,
            &signature_match_result,
            /*arg_index_mapping=*/nullptr, /*arg_overrides=*/nullptr);
    ABSL_DCHECK_OK(function_signature_matches_or.status());

    found_signature = function_signature_matches_or.value_or(false) &&
                      signature_match_result.non_matched_arguments() == 0 &&
                      signature_match_result.non_literals_coerced() == 0 &&
                      signature_match_result.literals_coerced() == 0;
    if (found_signature) {
      break;
    }
  }

  ZETASQL_RET_CHECK(found_signature) << "Could not find a matching signature for "
                                "pg.map_double_to_int function.";

  return FunctionAndSignature(builtin_function, *result_signature);
}

absl::StatusOr<zetasql::TypeListView>
SpangresSystemCatalog::GetExtendedTypeSuperTypes(const zetasql::Type* type) {
  // None of existing Spanner extended types currently support supertyping.
  return zetasql::TypeListView{};
}

absl::Status SpangresSystemCatalog::FindConversion(
    const zetasql::Type* from_type, const zetasql::Type* to_type,
    const FindConversionOptions& options, zetasql::Conversion* conversion) {
    ZETASQL_ASSIGN_OR_RETURN(
        *conversion,
        ::postgres_translator::spangres::datatypes::FindExtendedTypeConversion(
            from_type, to_type, options));
  return absl::OkStatus();
}

// Support for new datatypes should also be added in the PG Worker Proxy
// that is is used to check query compatibility between spangres and postgres.
// (spanner/tests/spangres/pg_worker_proxy.cc)
absl::Status SpangresSystemCatalog::AddTypes(
    const zetasql::LanguageOptions& language_options) {
  // Scalar Types.
  ZETASQL_RETURN_IF_ERROR(AddType(builtin_types::PgBoolMapping(), language_options));
  ZETASQL_RETURN_IF_ERROR(AddType(builtin_types::PgInt8Mapping(), language_options));
  ZETASQL_RETURN_IF_ERROR(AddType(builtin_types::PgFloat8Mapping(), language_options));
  ZETASQL_RETURN_IF_ERROR(AddType(builtin_types::PgVarcharMapping(), language_options));
  ZETASQL_RETURN_IF_ERROR(AddType(builtin_types::PgTextMapping(), language_options));
  ZETASQL_RETURN_IF_ERROR(AddType(builtin_types::PgByteaMapping(), language_options));
  ZETASQL_RETURN_IF_ERROR(
      AddType(builtin_types::PgTimestamptzMapping(), language_options));
  ZETASQL_RETURN_IF_ERROR(AddType(builtin_types::PgDateMapping(), language_options));

  // Array Types.
  ZETASQL_RETURN_IF_ERROR(
      AddType(builtin_types::PgBoolArrayMapping(), language_options));
  ZETASQL_RETURN_IF_ERROR(
      AddType(builtin_types::PgInt8ArrayMapping(), language_options));
  ZETASQL_RETURN_IF_ERROR(
      AddType(builtin_types::PgFloat8ArrayMapping(), language_options));
  ZETASQL_RETURN_IF_ERROR(
      AddType(builtin_types::PgVarcharArrayMapping(), language_options));
  ZETASQL_RETURN_IF_ERROR(
      AddType(builtin_types::PgTextArrayMapping(), language_options));
  ZETASQL_RETURN_IF_ERROR(
      AddType(builtin_types::PgByteaArrayMapping(), language_options));
  ZETASQL_RETURN_IF_ERROR(
      AddType(builtin_types::PgTimestamptzArrayMapping(), language_options));
  ZETASQL_RETURN_IF_ERROR(
      AddType(builtin_types::PgDateArrayMapping(), language_options));

    ZETASQL_RETURN_IF_ERROR(
        AddType(spangres_types::PgNumericMapping(), language_options));
    ZETASQL_RETURN_IF_ERROR(
        AddType(spangres_types::PgNumericArrayMapping(), language_options));

    ZETASQL_RETURN_IF_ERROR(
        AddType(spangres_types::PgJsonbMapping(), language_options));
    ZETASQL_RETURN_IF_ERROR(
        AddType(spangres_types::PgJsonbArrayMapping(), language_options));

    ZETASQL_RETURN_IF_ERROR(
        AddType(spangres_types::PgOidMapping(), language_options));
    ZETASQL_RETURN_IF_ERROR(
        AddType(spangres_types::PgOidArrayMapping(), language_options));

  return absl::OkStatus();
}

bool IsSpangresSqlRewriteFunction(const PostgresFunctionArguments& function) {
  for (const auto& signature : function.signature_arguments()) {
    if (!signature.signature().options().rewrite_options() ||
        signature.signature().options().rewrite_options()->sql().empty()) {
      return false;
    }
  }
  return true;
}

absl::Status SpangresSystemCatalog::AddFunctions(
    const zetasql::LanguageOptions& language_options) {
  // Populate the set of ZetaSQL functions supported in Spangres.
  std::vector<PostgresFunctionArguments> functions;
  AddAggregateFunctions(functions);
  AddArithmeticFunctions(functions);
  AddBitwiseFunctions(functions);
  AddBooleanFunctions(functions);
  AddDatetimeCurrentFunctions(functions);
  AddMiscellaneousFunctions(functions);
  AddNumericFunctions(functions);
  AddRegexFunctions(functions);
  AddStringFunctions(functions);
  AddTrigonometricFunctions(functions);
  AddDatetimeConversionFunctions(functions);
  AddHashingFunctions(functions);
  AddSpannerFunctions(functions);
  AddSequenceFunctions(functions);

    spangres::AddPgNumericFunctions(functions);
      ZETASQL_RETURN_IF_ERROR(AddPgNumericCastFunction("pg.cast_to_numeric"));
      ZETASQL_RETURN_IF_ERROR(AddPgNumericCastFunction("pg.cast_to_int64"));
      ZETASQL_RETURN_IF_ERROR(AddPgNumericCastFunction("pg.cast_to_double"));
      ZETASQL_RETURN_IF_ERROR(AddPgNumericCastFunction("pg.cast_to_string"));

  // Add casting override functions for STRING->DATE and STRING->TIMESTAMP.
  ZETASQL_RETURN_IF_ERROR(AddCastOverrideFunction(
      zetasql::types::StringType(), zetasql::types::DateType(),
      "pg.cast_to_date", language_options));
  ZETASQL_RETURN_IF_ERROR(AddCastOverrideFunction(
      zetasql::types::StringType(), zetasql::types::TimestampType(),
      "pg.cast_to_timestamp", language_options));

    spangres::AddPgJsonbFunctions(functions);

  spangres::AddPgArrayFunctions(functions);
  spangres::AddPgComparisonFunctions(functions);
  spangres::AddPgDatetimeFunctions(functions);
  spangres::AddPgFormattingFunctions(functions);
  spangres::AddPgStringFunctions(functions);

  // Map some Postgres functions to custom Spanner functions to match Postgres'
  // order semantics (e.g. PG.MIN).
  RemapFunctionsForSpanner(functions);

  // Add each function to the catalog if it is supported in Spanner.
  for (const PostgresFunctionArguments& function : functions) {
    bool is_builtin_sql_rewrite_function =
        IsBuiltinSqlRewriteFunction(function.mapped_function_name(),
                                    language_options, type_factory());
    if (FunctionNameSupportedInSpanner(function.mapped_function_name(),
                                       language_options, type_factory()) ||
        IsSpangresSqlRewriteFunction(function) ||
        is_builtin_sql_rewrite_function) {
      ZETASQL_RETURN_IF_ERROR(AddFunction(function, language_options));
    } else if (GetBuiltinFunction(function.mapped_function_name()).ok()) {
      // Checked that the function is registered directly in the emulator
      // function catalog since the function wasn't available in the ZetaSQL
      // catalog.
      ZETASQL_RETURN_IF_ERROR(AddFunction(function, language_options));
    }
  }

  // Populate the set of expression types supported in Spangres.
  absl::flat_hash_map<PostgresExprIdentifier, std::string> expr_functions;
  AddExprFunctions(expr_functions);
  AddBoolExprFunctions(expr_functions);
  AddNullTestFunctions(expr_functions);
  AddBooleanTestFunctions(expr_functions);
  AddSQLValueFunctions(expr_functions);
  AddArrayAtFunctions(expr_functions);
  AddMakeArrayFunctions(expr_functions);

  AddPgLeastGreatestFunctions(expr_functions);

  // Add each expression type to the catalog.
  for (const auto& [expr_id, builtin_function_name] : expr_functions) {
    if (FunctionNameSupportedInSpanner(builtin_function_name, language_options,
                                       type_factory())) {
      ZETASQL_RETURN_IF_ERROR(AddExprFunction(expr_id, builtin_function_name));
    } else if (GetBuiltinFunction(builtin_function_name).ok()) {
      // Checked that the function is registered directly in the emulator
      // function catalog since the function wasn't available in the ZetaSQL
      // catalog.
      ZETASQL_RETURN_IF_ERROR(AddExprFunction(expr_id, builtin_function_name));
    }
  }

  return absl::OkStatus();
}

absl::StatusOr<FunctionAndSignature>
SpangresSystemCatalog::GetFunctionAndSignature(
    Oid proc_oid,
    const std::vector<zetasql::InputArgumentType>& input_argument_types,
    const zetasql::LanguageOptions& language_options) {
  return EngineSystemCatalog::GetFunctionAndSignature(
      proc_oid, input_argument_types, language_options);
}

absl::StatusOr<FunctionAndSignature>
SpangresSystemCatalog::GetFunctionAndSignature(
    const PostgresExprIdentifier& expr_id,
    const std::vector<zetasql::InputArgumentType>& input_argument_types,
    const zetasql::LanguageOptions& language_options) {
  return EngineSystemCatalog::GetFunctionAndSignature(
      expr_id, input_argument_types, language_options);
}
}  // namespace spangres
}  // namespace postgres_translator
