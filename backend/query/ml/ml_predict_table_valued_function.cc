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

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/evaluator_table_iterator.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/string_view.h"
#include "backend/common/case.h"
#include "backend/query/ml/model_evaluator.h"
#include "backend/query/queryable_model.h"
#include "common/errors.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google::spanner::emulator::backend {
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

class MlPredictTableValuedFunctionEvaluator
    : public zetasql::EvaluatorTableIterator {
 public:
  MlPredictTableValuedFunctionEvaluator(
      const zetasql::Model* model,
      std::unique_ptr<EvaluatorTableIterator> input,
      zetasql::Value parameters,
      const std::vector<zetasql::TVFSchemaColumn>& output_columns)
      : model_(model),
        input_(std::move(input)),
        parameters_(std::move(parameters)),
        output_columns_(output_columns) {}

  // Validates inputs and initializes evaluator's state.
  absl::Status Init();

  int NumColumns() const override {
    return static_cast<int>(output_columns_.size());
  }

  std::string GetColumnName(int i) const override {
    DCHECK_GE(i, 0);
    DCHECK_LT(i, output_columns_.size());
    return output_columns_[i].name;
  }

  const zetasql::Type* GetColumnType(int i) const override {
    DCHECK_GE(i, 0);
    DCHECK_LT(i, output_columns_.size());
    return output_columns_[i].type;
  }

  const zetasql::Value& GetValue(int i) const override {
    DCHECK_GE(i, 0);
    DCHECK_LT(i, output_columns_.size());
    return output_values_[i];
  }

  absl::Status Status() const override { return status_; }

  absl::Status Cancel() override { return input_->Cancel(); }

  bool NextRow() override {
    // Advance input iterator, stop if there is an error.
    if (!input_->NextRow()) {
      status_ = input_->Status();
      return false;
    }

    // Get all the input values and populate pass-through columns.
    for (auto& input_column : input_columns_) {
      *input_column.value = input_->GetValue(input_column.input_index);
    }

    // Invoke model evaluator to populate output values.
    status_ = ModelEvaluator::Predict(model_, model_inputs_, model_outputs_);
    return status_.ok();
  }

 private:
  // The model argument of ML.PREDICT function.
  const zetasql::Model* const model_;
  // The relation argument of ML.PREDICT function.
  std::unique_ptr<EvaluatorTableIterator> input_;
  // The parameters argument of ML.PREDICT function.
  const zetasql::Value parameters_;
  // Selected output columns: model outputs and pass-through columns.
  const std::vector<zetasql::TVFSchemaColumn> output_columns_;
  // Maps input iterator column index to either input_values_ for model inputs
  // or output_values_ for pass-through columns.
  struct InputColumn {
    // Index of the input column value to be read.
    int64_t input_index;
    // Pointer to the value to be set.
    zetasql::Value* value;
  };
  std::vector<InputColumn> input_columns_;
  // Model input columns sent as arguments to ModelEvaluator.
  CaseInsensitiveStringMap<const ModelEvaluator::ModelColumn> model_inputs_;
  // Model output columns values of which are set by ModelEvaluator.
  CaseInsensitiveStringMap<ModelEvaluator::ModelColumn> model_outputs_;
  // Vector of values referenced by model_inputs_.
  std::vector<zetasql::Value> input_values_;
  // Vector of values accessible through GetValue().
  std::vector<zetasql::Value> output_values_;
  // Status of the iterator.
  absl::Status status_;
};

absl::Status MlPredictTableValuedFunctionEvaluator::Init() {
  // Create index of input columns.
  CaseInsensitiveStringMap<std::vector<int64_t>> input_columns_by_name;
  for (int i = 0; i < input_->NumColumns(); ++i) {
    input_columns_by_name[input_->GetColumnName(i)].emplace_back(i);
  }

  // Validate that model inputs are satisfied and build model_inputs_.
  input_values_.resize(model_->NumInputs());
  for (int i = 0; i < model_->NumInputs(); ++i) {
    const QueryableModelColumn* model_column =
        model_->GetInput(i)->GetAs<QueryableModelColumn>();
    ZETASQL_RET_CHECK(model_column);

    // Find matching input column by name.
    auto input_column = input_columns_by_name.find(model_column->Name());
    if (input_column == input_columns_by_name.end()) {
      // If column is required, fail the query.
      if (model_column->required()) {
        return error::MlInputColumnMissing(
            model_column->Name(),
            model_column->GetType()->TypeName(zetasql::PRODUCT_EXTERNAL,
                                              /*use_external_float32=*/true));
      }
      // Ignore missing optional columns.
      continue;
    }

    // If there is more than one matching input column, raise ambiguous error.
    if (input_column->second.size() > 1) {
      return error::MlInputColumnAmbiguous(model_column->Name());
    }

    ZETASQL_RET_CHECK_EQ(input_column->second.size(), 1);
    int64_t input_column_index = input_column->second.front();

    const zetasql::Type* input_column_type =
        input_->GetColumnType(input_column_index);
    if (!input_column_type->Equals(model_column->GetType())) {
      return error::MlInputColumnTypeMismatch(
          model_column->Name(),
          input_column_type->TypeName(zetasql::PRODUCT_EXTERNAL,
                                      /*use_external_float32=*/true),
          model_column->GetType()->TypeName(zetasql::PRODUCT_EXTERNAL,
                                            /*use_external_float32=*/true));
    }

    input_columns_.push_back(InputColumn{.input_index = input_column_index,
                                         .value = &input_values_[i]});

    model_inputs_.insert(
        {model_column->Name(),
         ModelEvaluator::ModelColumn{.model_column = model_column,
                                     .value = &input_values_[i]}});
  }

  // Map output columns to model outputs or passthrough columns.
  output_values_.resize(output_columns_.size());
  for (int i = 0; i < output_columns_.size(); ++i) {
    const std::string& column_name = output_columns_[i].name;
    const zetasql::Type* column_type = output_columns_[i].type;

    // Output of the model, not a pass through column.
    const zetasql::Column* model_column =
        model_->FindOutputByName(column_name);
    if (model_column != nullptr) {
      ZETASQL_RET_CHECK(model_column->Is<QueryableModelColumn>());
      ZETASQL_RET_CHECK(model_column->GetType()->Equals(column_type));
      model_outputs_.insert(
          {model_column->Name(),
           ModelEvaluator::ModelColumn{
               .model_column = model_column->GetAs<QueryableModelColumn>(),
               .value = &output_values_[i]}});
      continue;
    }

    // If the output column matches an input column, it's a pass-through column.
    auto input_column = input_columns_by_name.find(column_name);
    if (input_column != input_columns_by_name.end()) {
      if (input_column->second.size() > 1) {
        return error::MlPassThroughColumnAmbiguous(column_name);
      }
      ZETASQL_RET_CHECK_EQ(input_column->second.size(), 1);
      int64_t input_column_index = input_column->second.front();
      const zetasql::Type* input_column_type =
          input_->GetColumnType(input_column_index);
      ZETASQL_RET_CHECK(column_type->Equals(input_column_type));
      input_columns_.push_back(InputColumn{.input_index = input_column_index,
                                           .value = &output_values_[i]});
      continue;
    }

    ZETASQL_RET_CHECK_FAIL() << "Could not match ML TVF Scan column " << column_name
                     << ". Matches should be ensured when resolving the TVF";
  }

  return absl::OkStatus();
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

absl::StatusOr<std::unique_ptr<zetasql::EvaluatorTableIterator>>
MlPredictTableValuedFunction::CreateEvaluator(
    std::vector<TvfEvaluatorArg> input_arguments,
    const std::vector<zetasql::TVFSchemaColumn>& output_columns,
    const zetasql::FunctionSignature* function_call_signature) const {
  ZETASQL_RET_CHECK_GE(input_arguments.size(), 2);
  ZETASQL_RET_CHECK_LE(input_arguments.size(), 3);

  ZETASQL_RET_CHECK(input_arguments[0].model);
  const zetasql::Model* model = input_arguments[0].model;

  ZETASQL_RET_CHECK(input_arguments[1].relation);
  std::unique_ptr<zetasql::EvaluatorTableIterator> input =
      std::move(input_arguments[1].relation);

  zetasql::Value parameters;
  if (input_arguments.size() >= 3) {
    ZETASQL_RET_CHECK(input_arguments[2].value);
    parameters = *input_arguments[2].value;
  }

  auto evaluator = std::make_unique<MlPredictTableValuedFunctionEvaluator>(
      model, std::move(input), parameters, std::move(output_columns));
  ZETASQL_RETURN_IF_ERROR(evaluator->Init());
  return std::move(evaluator);
}

}  // namespace google::spanner::emulator::backend
