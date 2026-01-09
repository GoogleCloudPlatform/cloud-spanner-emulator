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

#include "backend/query/query_engine_util.h"

#include <memory>
#include <string>
#include <vector>

#include "zetasql/analyzer/function_signature_matcher.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/coercer.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/input_argument_type.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/signature_match_result.h"
#include "zetasql/public/type.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/status/statusor.h"
#include "backend/query/function_catalog.h"
#include "common/errors.h"
#include "common/limits.h"
#include "third_party/spanner_pg/interface/emulator_parser.h"
#include "third_party/spanner_pg/interface/pg_arena.h"
#include "third_party/spanner_pg/shims/memory_context_pg_arena.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Uses googlesql/public/analyzer to build an AnalyzerOutput for a query.
// We need to analyze the SQL before executing it in order to determine what
// kind of statement (query or DML) it is.
absl::StatusOr<std::unique_ptr<const zetasql::AnalyzerOutput>> Analyze(
    const std::string& sql, zetasql::Catalog* catalog,
    const zetasql::AnalyzerOptions& options,
    zetasql::TypeFactory* type_factory) {
  // Check the overall length of the query string.
  if (sql.size() > limits::kMaxQueryStringSize) {
    return error::QueryStringTooLong(sql.size(), limits::kMaxQueryStringSize);
  }

  std::unique_ptr<const zetasql::AnalyzerOutput> output;
  ZETASQL_RETURN_IF_ERROR(zetasql::AnalyzeStatement(sql, options, catalog,
                                              type_factory, &output));
  return output;
}

absl::StatusOr<std::unique_ptr<const zetasql::AnalyzerOutput>>
AnalyzePostgreSQL(const std::string& sql, zetasql::EnumerableCatalog* catalog,
                  zetasql::AnalyzerOptions& options,
                  zetasql::TypeFactory* type_factory,
                  const FunctionCatalog* function_catalog) {
  // Check the overall length of the query string.
  if (sql.size() > limits::kMaxQueryStringSize) {
    return error::QueryStringTooLong(sql.size(), limits::kMaxQueryStringSize);
  }

  options.CreateDefaultArenasIfNotSet();
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<postgres_translator::interfaces::PGArena> arena,
      postgres_translator::spangres::MemoryContextPGArena::Init(nullptr));
  // PG needs ASC NULLS LAST and DESC NULLS FIRST for functions implemented as
  // a SQL rewrite.
  options.mutable_language()->EnableLanguageFeature(
      zetasql::FEATURE_V_1_3_NULLS_FIRST_LAST_IN_ORDER_BY);
  return postgres_translator::spangres::ParseAndAnalyzePostgreSQL(
      sql, catalog, options, type_factory,
      std::make_unique<FunctionCatalog>(
          type_factory,
          /*catalog_name=*/kCloudSpannerEmulatorFunctionCatalogName,
          /*schema=*/function_catalog->GetLatestSchema()));
}

absl::StatusOr<std::unique_ptr<const zetasql::ResolvedFunctionCall>>
BuildPendingCommitTimestampFunction(zetasql::TypeFactory& type_factory,
                                    zetasql::Catalog& catalog) {
  const zetasql::Function* pct_function = nullptr;
  ZETASQL_RETURN_IF_ERROR(
      catalog.FindFunction({"pending_commit_timestamp"}, &pct_function));
  ZETASQL_RET_CHECK(pct_function != nullptr);
  ZETASQL_RET_CHECK_EQ(pct_function->signatures().size(), 1);
  std::vector<zetasql::InputArgumentType> input_argument_types;
  zetasql::LanguageOptions language_options;
  zetasql::Coercer coercer(&type_factory, &language_options);
  std::unique_ptr<zetasql::FunctionSignature> result_signature;
  zetasql::SignatureMatchResult signature_match_result;
  ZETASQL_ASSIGN_OR_RETURN(
      bool function_signature_matches,
      zetasql::FunctionSignatureMatchesWithStatus(
          language_options, coercer, /*arg_ast_nodes=*/{}, input_argument_types,
          pct_function->signatures().at(0),
          /*allow_argument_coercion=*/false, &type_factory,
          /*resolve_lambda_callback=*/nullptr, &result_signature,
          &signature_match_result,
          /*arg_index_mapping=*/nullptr, /*arg_overrides=*/nullptr));
  ZETASQL_RET_CHECK(result_signature->IsConcrete());
  ZETASQL_RET_CHECK(function_signature_matches);
  return zetasql::MakeResolvedFunctionCall(
      result_signature->result_type().type(), pct_function, *result_signature,
      {}, zetasql::ResolvedFunctionCall::DEFAULT_ERROR_MODE);
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
