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

#include "backend/query/queryable_udf.h"

#include <memory>
#include <string>
#include <vector>

#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/evaluator.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_split.h"
#include "absl/types/span.h"
#include "backend/query/analyzer_options.h"
#include "backend/query/feature_filter/query_size_limits_checker.h"
#include "backend/query/function_catalog.h"
#include "backend/query/hint_rewriter.h"
#include "backend/query/index_hint_validator.h"
#include "backend/schema/catalog/udf.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

QueryableUdf::QueryableUdf(const backend::Udf* backend_udf,
                           const std::string default_time_zone,
                           zetasql::Catalog* catalog,
                           zetasql::TypeFactory* type_factory)
    : zetasql::Function(
          absl::StrSplit(backend_udf->Name(), '.'),
          /*group=*/kSqlUdfGroup, zetasql::Function::SCALAR,
          std::vector<zetasql::FunctionSignature>{*backend_udf->signature()},
          CreateFunctionOptions(backend_udf, default_time_zone, catalog,
                                type_factory)),
      wrapped_udf_(backend_udf) {}

zetasql::FunctionOptions QueryableUdf::CreateFunctionOptions(
    const backend::Udf* udf, const std::string default_time_zone,
    zetasql::Catalog* catalog, zetasql::TypeFactory* type_factory) {
  auto evaluator = [=](absl::Span<const zetasql::Value> args)
      -> absl::StatusOr<zetasql::Value> {
    std::unique_ptr<const zetasql::AnalyzerOutput> output;
    zetasql::AnalyzerOptions options =
        MakeGoogleSqlAnalyzerOptions(default_time_zone);
    zetasql::ParameterValueMap columns;

    for (int i = 0; i < udf->signature()->arguments().size(); i++) {
      const auto& arg = udf->signature()->arguments()[i];
      columns[arg.argument_name()] = args[i];
      // Add expression columns to analyzer options.
      ZETASQL_RETURN_IF_ERROR(
          options.AddExpressionColumn(arg.argument_name(), arg.type()));
    }

    ZETASQL_RETURN_IF_ERROR(zetasql::AnalyzeExpression(udf->body(), options, catalog,
                                                 type_factory, &output));
    ZETASQL_RET_CHECK_NE(output->resolved_expr(), nullptr);

    HintRewriter rewriter;
    ZETASQL_RETURN_IF_ERROR(output->resolved_expr()->Accept(&rewriter));
    ZETASQL_ASSIGN_OR_RETURN(auto resolved_expr,
                     rewriter.ConsumeRootNode<zetasql::ResolvedExpr>());

    IndexHintValidator index_hint_validator{
        /*schema=*/nullptr,
        /*disable_query_null_filtered_index_check=*/false,
        /*allow_search_indexes_in_transaction=*/false,
        /*in_partition_query=*/false};
    ZETASQL_RETURN_IF_ERROR(resolved_expr->Accept(&index_hint_validator));

    // Check the query size limits
    // https://cloud.google.com/spanner/quotas#query_limits
    QuerySizeLimitsChecker checker;
    ZETASQL_RETURN_IF_ERROR(checker.CheckQueryAgainstLimits(resolved_expr.get()));

    zetasql::EvaluatorOptions evaluator_options;
    zetasql::PreparedExpression expr(resolved_expr.get(), evaluator_options);
    ZETASQL_RETURN_IF_ERROR(expr.Prepare(options, nullptr));

    ZETASQL_ASSIGN_OR_RETURN(zetasql::Value val, expr.Execute(columns));
    return val;
  };

  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(zetasql::FunctionEvaluator(evaluator));
  return function_options;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
