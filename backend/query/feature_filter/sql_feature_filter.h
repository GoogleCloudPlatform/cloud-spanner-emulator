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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_FEATURE_FILTER_SQL_FEATURE_FILTER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_FEATURE_FILTER_SQL_FEATURE_FILTER_H_

#include "zetasql/public/language_options.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/status/status.h"
#include "backend/query/feature_filter/sql_features_view.h"

namespace google::spanner::emulator::backend {
absl::Status FilterResolvedFunction(
    const zetasql::LanguageOptions& language_options,
    const SqlFeaturesView& query_features,
    const zetasql::ResolvedFunctionCall& function_call);
absl::Status FilterResolvedAggregateFunction(
    const SqlFeaturesView& query_features,
    const zetasql::ResolvedAggregateFunctionCall& aggregate_function);
absl::Status FilterSafeModeFunction(
    const zetasql::ResolvedFunctionCallBase& function_call);

}  // namespace google::spanner::emulator::backend

#endif
