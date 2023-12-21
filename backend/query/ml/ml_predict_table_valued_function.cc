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

#include "backend/query/ml/ml_predict_table_valued_function.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/ret_check.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

constexpr static absl::string_view kSafe = "SAFE";
constexpr static absl::string_view kMlFunctionNamespace = "ML";
constexpr static absl::string_view kFunctionName = "PREDICT";

std::vector<std::string> FunctionName(bool safe) {
  if (safe) {
    return {std::string(kSafe), std::string(kMlFunctionNamespace),
            std::string(kFunctionName)};
  }
  return {std::string(kMlFunctionNamespace), std::string(kFunctionName)};
}

}  // namespace

MlPredictTableValuedFunction::MlPredictTableValuedFunction(bool safe)
    : zetasql::TableValuedFunction(
          FunctionName(safe),
          zetasql::FunctionSignature(
              /*result_type=*/zetasql::FunctionArgumentType::AnyRelation(),
              /*arguments=*/
              {
                  zetasql::FunctionArgumentType::AnyModel(),
                  zetasql::FunctionArgumentType::AnyRelation(),
                  {
                      /*kind=*/zetasql::ARG_STRUCT_ANY,
                      /*options=*/
                      zetasql::FunctionArgumentTypeOptions(
                          zetasql::FunctionArgumentType::OPTIONAL),
                  },
              },
              /*context_ptr=*/nullptr)),
      safe_(safe) {}

absl::Status MlPredictTableValuedFunction::Resolve(
    const zetasql::AnalyzerOptions* analyzer_options,
    const std::vector<zetasql::TVFInputArgumentType>& actual_arguments,
    const zetasql::FunctionSignature& concrete_signature,
    zetasql::Catalog* catalog, zetasql::TypeFactory* type_factory,
    std::shared_ptr<zetasql::TVFSignature>* output_tvf_signature) const {
  ZETASQL_RET_CHECK_GE(actual_arguments.size(), 2);
  ZETASQL_RET_CHECK_LE(actual_arguments.size(), 3);

  const zetasql::TVFInputArgumentType& model_argument = actual_arguments[0];
  ZETASQL_RET_CHECK(model_argument.is_model());
  ZETASQL_RET_CHECK_NE(model_argument.model().model(), nullptr);
  const zetasql::Model& model = *model_argument.model().model();

  const zetasql::TVFInputArgumentType& relation_argument =
      actual_arguments[1];
  ZETASQL_RET_CHECK(relation_argument.is_relation());
  const zetasql::TVFRelation& relation = relation_argument.relation();

  std::vector<zetasql::TVFRelation::Column> output_columns;
  output_columns.reserve(model.NumOutputs() + relation.num_columns());
  absl::flat_hash_set<std::string> model_output_column_names;
  model_output_column_names.reserve(model.NumOutputs());
  ZETASQL_RET_CHECK_GT(model.NumOutputs(), 0);
  for (int i = 0; i < model.NumOutputs(); ++i) {
    const zetasql::Column* model_column = model.GetOutput(i);
    ZETASQL_RET_CHECK_NE(model_column, nullptr);
    output_columns.emplace_back(model_column->Name(), model_column->GetType(),
                                false);
    model_output_column_names.emplace(
        absl::AsciiStrToLower(model_column->Name()));
  }

  for (const zetasql::TVFSchemaColumn& relation_column : relation.columns()) {
    if (!model_output_column_names.contains(
            absl::AsciiStrToLower(relation_column.name))) {
      output_columns.push_back(relation_column);
    }
  }

  ZETASQL_RET_CHECK_NE(output_tvf_signature, nullptr);
  *output_tvf_signature = std::make_shared<zetasql::TVFSignature>(
      actual_arguments, zetasql::TVFRelation(std::move(output_columns)));

  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
