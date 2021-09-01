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

#include "backend/query/feature_filter/sql_feature_filter.h"

#include <string>

#include "zetasql/base/logging.h"
#include "zetasql/public/function.h"
#include "zetasql/public/functions/datetime.pb.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_enums.pb.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "backend/query/feature_filter/error_mode_util.h"
#include "backend/query/feature_filter/sql_features_view.h"
#include "common/errors.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google::spanner::emulator::backend {
namespace {

absl::Status CheckSafeIsProperlySupported(const zetasql::Function* func) {
  if (!SupportsSafeErrorMode(func)) {
    return error::UnsupportedFeatureSafe(
        absl::StrCat("SAFE.", func->FullName(/*include_group=*/false)),
        "Spanner does not support function in safe error mode.");
  }
  return absl::OkStatus();
}

bool IsDisabledAggregateFunction(const SqlFeaturesView& query_features,
                                 absl::string_view name) {
  if (!query_features.enable_stddev() &&
      (name == "stddev" || name == "stddev_samp")) {
    return true;
  }

  if (!query_features.enable_variance() &&
      (name == "variance" || name == "var_samp")) {
    return true;
  }

  static const auto* enabled_aggregate_functions =
      new absl::flat_hash_set<absl::string_view>{
          "any_value",  "array_agg",   "array_concat_agg",
          "avg",        "bit_and",     "bit_or",
          "bit_xor",    "count",       "$count_star",
          "countif",    "logical_and", "logical_or",
          "logical_or", "max",         "min",
          "sum",        "stddev",      "stddev_samp",
          "string_agg", "variance",    "var_samp",
      };
  return !enabled_aggregate_functions->contains(name);
}

bool IsDisabledFunction(const SqlFeaturesView& query_features,
                        absl::string_view name) {
  if (!query_features.enable_error_function() && name == "error") {
    return true;
  }

  if (name == "supported_optimizer_versions" &&
      !query_features.enable_supported_optimizer_version_function()) {
    return true;
  }

  return false;
}
absl::Status IsNotIsoDatePart(const zetasql::ResolvedExpr* arg,
                              const std::string& name) {
  ZETASQL_RET_CHECK_EQ(arg->node_kind(), zetasql::RESOLVED_LITERAL);
  auto constant = arg->GetAs<zetasql::ResolvedLiteral>();
  const zetasql::Value& lit = constant->value();
  if (lit.is_null()) {
    return absl::OkStatus();
  }
  ZETASQL_RET_CHECK(lit.type()->IsEnum());
  switch (lit.enum_value()) {
    case zetasql::functions::ISOWEEK:
      return error::UnsupportedFunction(absl::StrCat(name, "_ISOWEEK"));
    case zetasql::functions::ISOYEAR:
      return error::UnsupportedFunction(absl::StrCat(name, "_ISOYEAR"));
    default:
      return absl::OkStatus();
  }
}

}  // namespace

absl::Status FilterResolvedAggregateFunction(
    const SqlFeaturesView& query_features,
    const zetasql::ResolvedAggregateFunctionCall& aggregate_function) {
  const std::string name = aggregate_function.function()->Name();

  if (IsDisabledAggregateFunction(query_features, name)) {
    return error::UnsupportedFeatureSafe("aggregate function", name);
  }
  if (aggregate_function.null_handling_modifier() !=
          zetasql::ResolvedNonScalarFunctionCallBaseEnums::
              DEFAULT_NULL_HANDLING &&
      name != "array_agg") {
    return error::UnsupportedIgnoreNullsInAggregateFunctions();
  }

  if (aggregate_function.having_modifier() && aggregate_function.distinct()) {
    return error::UnsupportedHavingModifierWithDistinct();
  }

  return absl::OkStatus();
}

absl::Status FilterResolvedFunction(
    const zetasql::LanguageOptions& language_options,
    const SqlFeaturesView& query_features,
    const zetasql::ResolvedFunctionCall& function_call) {
  const std::string name = function_call.function()->FullName(false);

  if (IsDisabledFunction(query_features, name)) {
    return error::UnsupportedFunction(name);
  }

  if (name == "is_not_distinct_from") {
    if (!function_call.argument_list(0)->type()->SupportsEquality(
            language_options)) {
      return error::ComparisonNotSupported(/*arg_num=*/0, name);
    }
    if (function_call.argument_list(0)->type()->IsStruct()) {
      return error::StructComparisonNotSupported(name);
    }
  }

  if (name == "pending_commit_timestamp" ||
      name == "pending_commit_timestamp_int64") {
    return error::PendingCommitTimestampDmlValueOnly();
  }

  if (name == "date_trunc" && !query_features.iso_date_parts()) {
    return IsNotIsoDatePart(function_call.argument_list(1), "DATE_TRUNC");
  }

  if (name == "timestamp_trunc" && !query_features.iso_date_parts()) {
    return IsNotIsoDatePart(function_call.argument_list(1), "TIMESTAMP_TRUNC");
  }

  if (name == "date_diff" && !query_features.iso_date_parts()) {
    return IsNotIsoDatePart(function_call.argument_list(2), "DATE_DIFF");
  }
  if (name == "nullif" && function_call.argument_list(0)->type()->IsStruct()) {
    return error::NullifStructNotSupported();
  }

  return absl::OkStatus();
}

absl::Status FilterSafeModeFunction(
    const zetasql::ResolvedFunctionCallBase& function_call) {
  bool safe_mode = false;
  if (function_call.error_mode() ==
      zetasql::ResolvedFunctionCallBaseEnums::SAFE_ERROR_MODE) {
    safe_mode = true;
  } else {
    ZETASQL_DCHECK_EQ(function_call.error_mode(),
              zetasql::ResolvedFunctionCallBaseEnums::DEFAULT_ERROR_MODE);
  }
  ZETASQL_RET_CHECK(!safe_mode || function_call.function()->SupportsSafeErrorMode())
      << "Invalid ResolvedAST.";

  if (safe_mode) {
    ZETASQL_RETURN_IF_ERROR(CheckSafeIsProperlySupported(function_call.function()));
  }
  return absl::OkStatus();
}

}  // namespace google::spanner::emulator::backend
