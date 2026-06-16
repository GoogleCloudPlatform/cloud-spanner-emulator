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
#include <utility>
#include <vector>

#include "googlesql/public/analyzer.h"
#include "googlesql/public/analyzer_options.h"
#include "googlesql/public/evaluator.h"
#include "googlesql/public/function.h"
#include "googlesql/public/function_signature.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/public/value.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_split.h"
#include "absl/types/span.h"
#include "backend/query/analyzer_options.h"
#include "backend/query/feature_filter/query_size_limits_checker.h"
#include "backend/query/hint_rewriter.h"
#include "backend/query/index_hint_validator.h"
#include "backend/query/remote_udf/remote_udf_evaluator.h"
#include "backend/schema/catalog/udf.h"
#include "googlesql/base/ret_check.h"
#include "googlesql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

absl::StatusOr<std::unique_ptr<QueryableUdf>> QueryableUdf::Create(
    const backend::Udf* backend_udf, std::string default_time_zone,
    googlesql::Catalog* catalog, googlesql::TypeFactory* type_factory) {
  GOOGLESQL_ASSIGN_OR_RETURN(auto function_options,
                   CreateFunctionOptions(backend_udf, default_time_zone,
                                         catalog, type_factory));
  return std::unique_ptr<QueryableUdf>(
      new QueryableUdf(backend_udf, std::move(function_options)));
}

absl::StatusOr<googlesql::FunctionOptions> QueryableUdf::CreateFunctionOptions(
    const backend::Udf* udf, std::string default_time_zone,
    googlesql::Catalog* catalog, googlesql::TypeFactory* type_factory) {
  googlesql::FunctionEvaluator evaluator;
  if (udf->language() == backend::Udf::Language::REMOTE || udf->is_remote()) {
    GOOGLESQL_RET_CHECK(udf->endpoint().has_value());
    evaluator = RemoteUdfEvaluator::BuildEvaluator(
        std::string(*udf->endpoint()), udf->Name(),
        udf->signature()->result_type().type());
  } else {
    evaluator = [=](absl::Span<const googlesql::Value> args)
        -> absl::StatusOr<googlesql::Value> {
      std::unique_ptr<const googlesql::AnalyzerOutput> output;
      googlesql::AnalyzerOptions options =
          MakeGoogleSqlAnalyzerOptions(default_time_zone);
      googlesql::ParameterValueMap columns;

      for (int i = 0; i < udf->signature()->arguments().size(); i++) {
        const auto& arg = udf->signature()->arguments()[i];
        columns[arg.argument_name()] = args[i];
        // Add expression columns to analyzer options.
        GOOGLESQL_RETURN_IF_ERROR(
            options.AddExpressionColumn(arg.argument_name(), arg.type()));
      }

      GOOGLESQL_RETURN_IF_ERROR(googlesql::AnalyzeExpression(
          udf->body(), options, catalog, type_factory, &output));
      GOOGLESQL_RET_CHECK_NE(output->resolved_expr(), nullptr);

      HintRewriter rewriter;
      GOOGLESQL_RETURN_IF_ERROR(output->resolved_expr()->Accept(&rewriter));
      GOOGLESQL_ASSIGN_OR_RETURN(auto resolved_expr,
                       rewriter.ConsumeRootNode<googlesql::ResolvedExpr>());

      IndexHintValidator index_hint_validator{
          /*schema=*/nullptr,
          /*disable_query_null_filtered_index_check=*/false,
          /*allow_search_indexes_in_transaction=*/false,
          /*in_partition_query=*/false};
      GOOGLESQL_RETURN_IF_ERROR(resolved_expr->Accept(&index_hint_validator));

      // Check the query size limits
      // https://cloud.google.com/spanner/quotas#query_limits
      QuerySizeLimitsChecker checker;
      GOOGLESQL_RETURN_IF_ERROR(checker.CheckQueryAgainstLimits(resolved_expr.get()));

      googlesql::EvaluatorOptions evaluator_options;
      googlesql::PreparedExpression expr(resolved_expr.get(),
                                         evaluator_options);
      GOOGLESQL_RETURN_IF_ERROR(expr.Prepare(options, nullptr));

      GOOGLESQL_ASSIGN_OR_RETURN(googlesql::Value val, expr.Execute(columns));
      return val;
    };
  }

  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(evaluator);
  return function_options;
}

QueryableUdf::QueryableUdf(const backend::Udf* backend_udf,
                           googlesql::FunctionOptions function_options)
    : googlesql::Function(
          absl::StrSplit(backend_udf->Name(), '.'),
          /*group=*/kSqlUdfGroup, googlesql::Function::SCALAR,
          std::vector<googlesql::FunctionSignature>{*backend_udf->signature()},
          std::move(function_options)),
      wrapped_udf_(backend_udf) {}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
