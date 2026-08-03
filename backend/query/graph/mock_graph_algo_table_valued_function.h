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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_GRAPH_MOCK_GRAPH_ALGO_TABLE_VALUED_FUNCTION_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_GRAPH_MOCK_GRAPH_ALGO_TABLE_VALUED_FUNCTION_H_

#include <memory>
#include <string>
#include <vector>

#include "googlesql/public/analyzer_options.h"
#include "googlesql/public/catalog.h"
#include "googlesql/public/evaluator_table_iterator.h"
#include "googlesql/public/function_signature.h"
#include "googlesql/public/table_valued_function.h"
#include "googlesql/public/types/type_factory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "backend/common/case.h"

namespace google::spanner::emulator::backend {

class MockGraphAlgoTableValuedFunction : public googlesql::TableValuedFunction {
 public:
  MockGraphAlgoTableValuedFunction(
      absl::string_view name, const googlesql::FunctionSignature& signature);

  absl::Status Resolve(
      const googlesql::AnalyzerOptions* analyzer_options,
      const std::vector<googlesql::TVFInputArgumentType>& actual_arguments,
      const googlesql::FunctionSignature& concrete_signature,
      googlesql::Catalog* catalog, googlesql::TypeFactory* type_factory,
      std::shared_ptr<googlesql::TVFSignature>* output_tvf_signature)
      const override;

  absl::StatusOr<std::unique_ptr<googlesql::EvaluatorTableIterator>>
  CreateEvaluator(std::vector<TvfEvaluatorArg> input_arguments,
                  const std::vector<googlesql::TVFSchemaColumn>& output_columns,
                  const googlesql::FunctionSignature* function_call_signature)
      const override;
};

void AddMockGraphAlgoFunctions(
    CaseInsensitiveStringMap<std::unique_ptr<googlesql::TableValuedFunction>>&
        table_valued_functions);

}  // namespace google::spanner::emulator::backend

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_GRAPH_MOCK_GRAPH_ALGO_TABLE_VALUED_FUNCTION_H_
