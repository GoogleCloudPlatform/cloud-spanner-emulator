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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_ML_ML_PREDICT_ROW_FUNCTION_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_ML_ML_PREDICT_ROW_FUNCTION_H_

#include "zetasql/public/value.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"

namespace google::spanner::emulator::backend {

static constexpr char kMlPredictRowParamModelEndpoint[] = "model_endpoint";
static constexpr char kMlPredictRowParamArgs[] = "args";

// Implementation of scalar ML_PREDICT_ROW function.
absl::StatusOr<zetasql::Value> EvalMlPredictRow(
    absl::Span<const zetasql::Value> args);

}  // namespace google::spanner::emulator::backend

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_ML_ML_PREDICT_ROW_FUNCTION_H_
