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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_ML_MODEL_EVALUATOR_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_ML_MODEL_EVALUATOR_H_

#include <memory>

#include "googlesql/public/catalog.h"
#include "googlesql/public/json_value.h"
#include "googlesql/public/value.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "backend/common/case.h"
#include "backend/query/queryable_model.h"

namespace google::spanner::emulator::backend {

// Evaluates model predictions. Shared by both GSQL and PG ML functions.
class ModelEvaluator {
 public:
  // Customizable callback that allows arbitrary model prediction logic.
  // If UNIMPLEMENTED error is returned, evaluator will fallback to
  // DefaultModel.
  static absl::Status Predict(
      const googlesql::Model* model,
      const CaseInsensitiveStringMap<const googlesql::Value*>& model_inputs,
      const CaseInsensitiveStringMap<const googlesql::Value*>& model_params,
      const CaseInsensitiveStringMap<googlesql::Value*>& model_outputs);

  // Prediction function for PG dialect which operates on JSONB values.
  static absl::Status PgPredict(absl::string_view endpoint,
                                const googlesql::JSONValueConstRef& instance,
                                const googlesql::JSONValueConstRef& parameters,
                                googlesql::JSONValueRef prediction);

  // Returns the default LLM model for AI functions.
  static std::unique_ptr<QueryableModel> GetDefaultLlmModel();
};

}  // namespace google::spanner::emulator::backend

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_ML_MODEL_EVALUATOR_H_
