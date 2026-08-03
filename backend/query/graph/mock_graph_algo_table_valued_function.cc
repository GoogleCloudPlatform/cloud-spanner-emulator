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

#include "backend/query/graph/mock_graph_algo_table_valued_function.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "googlesql/public/analyzer_options.h"
#include "googlesql/public/catalog.h"
#include "googlesql/public/evaluator_table_iterator.h"
#include "googlesql/public/function_signature.h"
#include "googlesql/public/table_valued_function.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/public/value.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "backend/common/case.h"

namespace google::spanner::emulator::backend {
namespace {

class MockGraphAlgoTableValuedFunctionEvaluator
    : public googlesql::EvaluatorTableIterator {
 public:
  explicit MockGraphAlgoTableValuedFunctionEvaluator(
      const std::vector<googlesql::TVFSchemaColumn>& output_columns)
      : output_columns_(output_columns) {}

  int NumColumns() const override {
    return static_cast<int>(output_columns_.size());
  }
  std::string GetColumnName(int i) const override {
    DCHECK_GE(i, 0);
    DCHECK_LT(i, output_columns_.size());
    return output_columns_[i].name;
  }
  const googlesql::Type* GetColumnType(int i) const override {
    DCHECK_GE(i, 0);
    DCHECK_LT(i, output_columns_.size());
    return output_columns_[i].type;
  }
  const googlesql::Value& GetValue(int i) const override {
    DCHECK_GE(i, 0);
    DCHECK_LT(i, output_columns_.size());
    output_values_.resize(output_columns_.size());
    output_values_[i] = googlesql::Value::Null(output_columns_[i].type);
    return output_values_[i];
  }
  absl::Status Status() const override { return status_; }
  absl::Status Cancel() override { return absl::OkStatus(); }

  bool NextRow() override {
    return false;  // Return empty result set for mock.
  }

 private:
  const std::vector<googlesql::TVFSchemaColumn> output_columns_;
  mutable std::vector<googlesql::Value> output_values_;
  absl::Status status_;
};

}  // namespace

MockGraphAlgoTableValuedFunction::MockGraphAlgoTableValuedFunction(
    absl::string_view name, const googlesql::FunctionSignature& signature)
    : googlesql::TableValuedFunction({std::string(name)}, signature) {}

absl::Status MockGraphAlgoTableValuedFunction::Resolve(
    const googlesql::AnalyzerOptions* analyzer_options,
    const std::vector<googlesql::TVFInputArgumentType>& actual_arguments,
    const googlesql::FunctionSignature& concrete_signature,
    googlesql::Catalog* catalog, googlesql::TypeFactory* type_factory,
    std::shared_ptr<googlesql::TVFSignature>* output_tvf_signature) const {
  std::vector<googlesql::TVFRelation::Column> output_columns;
  const auto& result_type = concrete_signature.result_type();
  if (result_type.options().has_relation_input_schema()) {
    output_columns.reserve(
        result_type.options().relation_input_schema().columns().size());
    for (const auto& col :
         result_type.options().relation_input_schema().columns()) {
      output_columns.push_back(col);
    }
  }

  *output_tvf_signature = std::make_shared<googlesql::TVFSignature>(
      actual_arguments, googlesql::TVFRelation(std::move(output_columns)));
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<googlesql::EvaluatorTableIterator>>
MockGraphAlgoTableValuedFunction::CreateEvaluator(
    std::vector<TvfEvaluatorArg> input_arguments,
    const std::vector<googlesql::TVFSchemaColumn>& output_columns,
    const googlesql::FunctionSignature* function_call_signature) const {
  return std::make_unique<MockGraphAlgoTableValuedFunctionEvaluator>(
      output_columns);
}

void AddMockGraphAlgoFunctions(
    CaseInsensitiveStringMap<std::unique_ptr<googlesql::TableValuedFunction>>&
        table_valued_functions) {
  auto add_algo = [&](const std::string& name,
                      const std::vector<googlesql::TVFSchemaColumn>& cols) {
    googlesql::TVFRelation output_schema(cols);
    googlesql::FunctionSignature signature(
        googlesql::FunctionArgumentType::RelationWithSchema(
            output_schema, /*extra_relation_input_columns_allowed=*/false),
        {},
        /*context_ptr=*/nullptr);
    auto algo =
        std::make_unique<MockGraphAlgoTableValuedFunction>(name, signature);
    table_valued_functions.insert({algo->FullName(), std::move(algo)});
  };

  add_algo("PageRank", {{"node", googlesql::types::StringType()},
                        {"score", googlesql::types::DoubleType()}});
  add_algo("BetweennessCentrality",
           {{"node", googlesql::types::StringType()},
            {"score", googlesql::types::DoubleType()}});
  add_algo("ClosenessCentrality", {{"node", googlesql::types::StringType()},
                                   {"score", googlesql::types::DoubleType()}});
  add_algo("ModularityClustering",
           {{"node", googlesql::types::StringType()},
            {"cluster", googlesql::types::Int64Type()}});
  add_algo("CorrelationClustering",
           {{"node", googlesql::types::StringType()},
            {"cluster", googlesql::types::Int64Type()}});
  add_algo("WeaklyConnectedComponents",
           {{"node", googlesql::types::StringType()},
            {"component", googlesql::types::Int64Type()}});
  add_algo("JaccardSimilarity",
           {{"source_node", googlesql::types::StringType()},
            {"target_node", googlesql::types::StringType()},
            {"similarity", googlesql::types::DoubleType()}});
  add_algo("CosineSimilarity",
           {{"source_node", googlesql::types::StringType()},
            {"target_node", googlesql::types::StringType()},
            {"similarity", googlesql::types::DoubleType()}});
  add_algo("CommonNeighborsSimilarity",
           {{"source_node", googlesql::types::StringType()},
            {"target_node", googlesql::types::StringType()},
            {"similarity", googlesql::types::DoubleType()}});
  add_algo("TotalNeighborsSimilarity",
           {{"source_node", googlesql::types::StringType()},
            {"target_node", googlesql::types::StringType()},
            {"similarity", googlesql::types::DoubleType()}});
  add_algo("ShortestPath", {{"source_node", googlesql::types::StringType()},
                            {"target_node", googlesql::types::StringType()},
                            {"distance", googlesql::types::DoubleType()}});
  add_algo("LabelPropagation", {{"node", googlesql::types::StringType()},
                                {"cluster", googlesql::types::Int64Type()}});
  add_algo("CliqueFinding", {{"node", googlesql::types::StringType()},
                             {"clique", googlesql::types::Int64Type()}});
}

}  // namespace google::spanner::emulator::backend
