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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_ML_ML_PREDICT_TABLE_VALUED_FUNCTION_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_ML_ML_PREDICT_TABLE_VALUED_FUNCTION_H_

#include <memory>
#include <vector>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/evaluator_table_iterator.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace google::spanner::emulator::backend {

// Implementation of ML.PREDICT function.
class MlPredictTableValuedFunction : public zetasql::TableValuedFunction {
 public:
  explicit MlPredictTableValuedFunction(bool safe);

  bool is_safe() const { return safe_; }

  // Resolves output schema by combining model output and pass-through columns.
  absl::Status Resolve(
      const zetasql::AnalyzerOptions* analyzer_options,
      const std::vector<zetasql::TVFInputArgumentType>& actual_arguments,
      const zetasql::FunctionSignature& concrete_signature,
      zetasql::Catalog* catalog, zetasql::TypeFactory* type_factory,
      std::shared_ptr<zetasql::TVFSignature>* output_tvf_signature)
      const override;

  // Creates evaluator for this function.
  absl::StatusOr<std::unique_ptr<zetasql::EvaluatorTableIterator>>
  CreateEvaluator(std::vector<TvfEvaluatorArg> input_arguments,
                  const std::vector<zetasql::TVFSchemaColumn>& output_columns,
                  const zetasql::FunctionSignature* function_call_signature)
      const override;

 private:
  const bool safe_ = false;
};

}  // namespace google::spanner::emulator::backend

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_ML_ML_PREDICT_TABLE_VALUED_FUNCTION_H_
