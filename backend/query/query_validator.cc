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

#include "backend/query/query_validator.h"

#include <string>
#include <vector>

#include "zetasql/public/type.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "backend/common/case.h"
#include "backend/query/feature_filter/gsql_supported_functions.h"
#include "backend/query/feature_filter/sql_feature_filter.h"
#include "common/errors.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

constexpr absl::string_view kSpannerQueryEngineHintPrefix = "spanner";

// Hints related to adaptive execution.
constexpr absl::string_view kHintEnableAdaptivePlans = "enable_adaptive_plans";

// Parameter sensitive plans hint
constexpr absl::string_view kHintParameterSensitive = "parameter_sensitive";
constexpr absl::string_view kHintParameterSensitiveAlways = "always";
constexpr absl::string_view kHintParameterSensitiveAuto = "auto";
constexpr absl::string_view kHintParameterSensitiveNever = "never";

// Indexes
constexpr absl::string_view kHintForceIndex = "force_index";

// Joins
constexpr absl::string_view kHintJoinForceOrder = "force_join_order";
constexpr absl::string_view kHintJoinMethod = "join_method";
constexpr absl::string_view kHintJoinTypeApply = "apply_join";
constexpr absl::string_view kHintJoinTypeHash = "hash_join";
constexpr absl::string_view kHintJoinTypeMerge = "merge_join";

constexpr absl::string_view kHintJoinBatch = "batch_mode";

// For join_method = hash_join
constexpr absl::string_view kHashJoinBuildSide = "hash_join_build_side";
constexpr absl::string_view kHashJoinBuildSideLeft = "build_left";
constexpr absl::string_view kHashJoinBuildSideRight = "build_right";

// Group by
constexpr absl::string_view kHintGroupMethod = "group_method";
constexpr absl::string_view kHintGroupMethodHash = "hash_group";
constexpr absl::string_view kHintGroupMethodStream = "stream_group";

// Specialized transformations
constexpr absl::string_view kHintConstantFolding = "constant_folding";
constexpr absl::string_view kHintTableScanGroupByScanOptimization =
    "groupby_scan_optimization";

// Runtime hints
constexpr absl::string_view kUseAdditionalParallelism =
    "use_additional_parallelism";

constexpr absl::string_view kHintGroupTypeDeprecated = "group_type";

constexpr absl::string_view kHintJoinTypeDeprecated = "join_type";
constexpr absl::string_view kHintJoinTypeNestedLoopDeprecated = "loop_join";

// Emulator-specific hints that can be used to set QueryEngineOptions.
constexpr absl::string_view kEmulatorQueryEngineHintPrefix = "spanner_emulator";

constexpr absl::string_view kHintDisableQueryPartitionabilityCheck =
    "disable_query_partitionability_check";

constexpr absl::string_view kHintDisableQueryNullFilteredIndexCheck =
    "disable_query_null_filtered_index_check";

absl::Status CollectHintsForNode(
    const zetasql::ResolvedOption* hint,
    absl::flat_hash_map<absl::string_view, zetasql::Value>* node_hint_map) {
  ZETASQL_RET_CHECK_EQ(hint->value()->node_kind(), zetasql::RESOLVED_LITERAL);
  const zetasql::Value& value =
      hint->value()->GetAs<zetasql::ResolvedLiteral>()->value();
  if (node_hint_map->contains(hint->name())) {
    return error::MultipleValuesForSameHint(hint->name());
  }
  (*node_hint_map)[hint->name()] = value;
  return absl::OkStatus();
}

}  // namespace

absl::Status QueryValidator::ValidateHints(
    const zetasql::ResolvedNode* node) const {
  std::vector<const zetasql::ResolvedNode*> child_nodes;
  node->GetChildNodes(&child_nodes);
  // Process the hints for each node, using maps to keep track of
  // the hints for each node.
  absl::flat_hash_map<absl::string_view, zetasql::Value> hint_map;
  absl::flat_hash_map<absl::string_view, zetasql::Value> emulator_hint_map;
  for (const zetasql::ResolvedNode* child_node : child_nodes) {
    if (child_node->node_kind() == zetasql::RESOLVED_OPTION) {
      const zetasql::ResolvedOption* hint =
          child_node->GetAs<zetasql::ResolvedOption>();
      if (absl::EqualsIgnoreCase(hint->qualifier(),
                                 kSpannerQueryEngineHintPrefix) ||
          hint->qualifier().empty()) {
        ZETASQL_RETURN_IF_ERROR(CheckSpannerHintName(hint->name(), node->node_kind()));
        ZETASQL_RETURN_IF_ERROR(CollectHintsForNode(hint, &hint_map));
      } else if (absl::EqualsIgnoreCase(hint->qualifier(),
                                        kEmulatorQueryEngineHintPrefix)) {
        ZETASQL_RETURN_IF_ERROR(CheckEmulatorHintName(hint->name(), node->node_kind()));
        ZETASQL_RETURN_IF_ERROR(CollectHintsForNode(hint, &emulator_hint_map));
      } else {
        // Ignore hints intended for other engines. Mark the value used so an
        // 'Unimplemented' error is not raised.
        hint->value()->MarkFieldsAccessed();
        continue;
      }
    }
  }

  for (const auto& [hint_name, hint_value] : hint_map) {
    ZETASQL_RETURN_IF_ERROR(
        CheckHintValue(hint_name, hint_value, node->node_kind(), hint_map));
  }

  // Extract any Emulator-engine options from the hints for this node.
  return ExtractEmulatorOptionsForNode(emulator_hint_map);
}

absl::Status QueryValidator::CheckSpannerHintName(
    absl::string_view name, const zetasql::ResolvedNodeKind node_kind) const {
  static const auto* supported_hints = new const absl::flat_hash_map<
      const zetasql::ResolvedNodeKind,
      absl::flat_hash_set<absl::string_view, zetasql_base::StringViewCaseHash,
                          zetasql_base::StringViewCaseEqual>>{
      {zetasql::RESOLVED_TABLE_SCAN,
       {kHintForceIndex, kHintTableScanGroupByScanOptimization}},
      {zetasql::RESOLVED_JOIN_SCAN,
       {kHintJoinTypeDeprecated, kHintJoinMethod, kHashJoinBuildSide,
        kHintJoinForceOrder}},
      {zetasql::RESOLVED_AGGREGATE_SCAN,
       {kHintGroupTypeDeprecated, kHintGroupMethod}},
      {zetasql::RESOLVED_ARRAY_SCAN,
       {kHintJoinTypeDeprecated, kHintJoinMethod, kHashJoinBuildSide,
        kHintJoinBatch, kHintJoinForceOrder}},
      {zetasql::RESOLVED_QUERY_STMT,
       {kHintForceIndex, kHintJoinTypeDeprecated, kHintJoinMethod,
        kHashJoinBuildSide, kHintJoinForceOrder, kHintConstantFolding,
        kUseAdditionalParallelism, kHintEnableAdaptivePlans,
        kHintParameterSensitive}},
      {zetasql::RESOLVED_SUBQUERY_EXPR,
       {kHintJoinTypeDeprecated, kHintJoinMethod, kHashJoinBuildSide,
        kHintJoinBatch, kHintJoinForceOrder}},
      {zetasql::RESOLVED_SET_OPERATION_SCAN,
       {kHintJoinMethod, kHintJoinForceOrder}}};

  const auto& iter = supported_hints->find(node_kind);
  if (iter == supported_hints->end() || !iter->second.contains(name)) {
    return error::InvalidHint(name);
  }
  return absl::OkStatus();
}

absl::Status QueryValidator::CheckEmulatorHintName(
    absl::string_view name, const zetasql::ResolvedNodeKind node_kind) const {
  static const auto* supported_hints = new const absl::flat_hash_map<
      const zetasql::ResolvedNodeKind,
      absl::flat_hash_set<absl::string_view, zetasql_base::StringViewCaseHash,
                          zetasql_base::StringViewCaseEqual>>{
      {zetasql::RESOLVED_TABLE_SCAN,
       {kHintDisableQueryNullFilteredIndexCheck}},
      {zetasql::RESOLVED_QUERY_STMT,
       {kHintDisableQueryPartitionabilityCheck}},
  };

  const auto& iter = supported_hints->find(node_kind);
  if (iter == supported_hints->end() || !iter->second.contains(name)) {
    return error::InvalidEmulatorHint(name);
  }
  return absl::OkStatus();
}

absl::Status QueryValidator::CheckHintValue(
    absl::string_view name, const zetasql::Value& value,
    const zetasql::ResolvedNodeKind node_kind,
    const absl::flat_hash_map<absl::string_view, zetasql::Value>& hint_map)
    const {
  static const auto* supported_hint_types =
      new absl::flat_hash_map<absl::string_view, const zetasql::Type*,
                              zetasql_base::StringViewCaseHash, zetasql_base::StringViewCaseEqual>{{
          {kHintJoinTypeDeprecated, zetasql::types::StringType()},
          {kHintParameterSensitive, zetasql::types::StringType()},
          {kHintJoinMethod, zetasql::types::StringType()},
          {kHashJoinBuildSide, zetasql::types::StringType()},
          {kHintJoinBatch, zetasql::types::BoolType()},
          {kHintJoinForceOrder, zetasql::types::BoolType()},
          {kHintGroupTypeDeprecated, zetasql::types::StringType()},
          {kHintGroupMethod, zetasql::types::StringType()},
          {kHintForceIndex, zetasql::types::StringType()},
          {kUseAdditionalParallelism, zetasql::types::BoolType()},
          {kHintConstantFolding, zetasql::types::BoolType()},
          {kHintTableScanGroupByScanOptimization, zetasql::types::BoolType()},
          {kHintEnableAdaptivePlans, zetasql::types::BoolType()},
      }};

  const auto& iter = supported_hint_types->find(name);
  ZETASQL_RET_CHECK(iter != supported_hint_types->cend());
  if (!value.type()->Equals(iter->second)) {
    return error::InvalidHintValue(name, value.DebugString());
  }
  if (absl::EqualsIgnoreCase(name, kHintForceIndex)) {
    const std::string& string_value = value.string_value();
    // Statement-level FORCE_INDEX hints can only be '_BASE_TABLE'.
    if (node_kind == zetasql::RESOLVED_QUERY_STMT &&
        !absl::EqualsIgnoreCase(string_value, "_BASE_TABLE")) {
      return error::InvalidStatementHintValue(name, value.DebugString());
    }
    if (schema_->FindIndex(string_value) == nullptr &&
        !absl::EqualsIgnoreCase(string_value, "_BASE_TABLE")) {
      return error::InvalidHintValue(name, value.DebugString());
    }
  } else if (absl::EqualsIgnoreCase(name, kHintJoinMethod) ||
             absl::EqualsIgnoreCase(name, kHintJoinTypeDeprecated)) {
    const std::string& string_value = value.string_value();
    if (!(absl::EqualsIgnoreCase(string_value, kHintJoinTypeApply) ||
          absl::EqualsIgnoreCase(string_value, kHintJoinTypeHash) ||
          absl::EqualsIgnoreCase(string_value, kHintJoinTypeMerge) ||
          absl::EqualsIgnoreCase(string_value,
                                 kHintJoinTypeNestedLoopDeprecated))) {
      return error::InvalidHintValue(name, value.DebugString());
    }
  } else if (absl::EqualsIgnoreCase(name, kHintParameterSensitive)) {
    const std::string& string_value = value.string_value();
    if (!(absl::EqualsIgnoreCase(string_value, kHintParameterSensitiveAlways) ||
          absl::EqualsIgnoreCase(string_value, kHintParameterSensitiveAuto) ||
          absl::EqualsIgnoreCase(string_value, kHintParameterSensitiveNever))) {
      return error::InvalidHintValue(name, value.DebugString());
    }
  } else if (absl::EqualsIgnoreCase(name, kHashJoinBuildSide)) {
    bool is_hash_join = [&]() {
      auto it = hint_map.find(kHintJoinMethod);
      if (it != hint_map.end() &&
          absl::EqualsIgnoreCase(it->second.string_value(),
                                 kHintJoinTypeHash)) {
        return true;
      }
      it = hint_map.find(kHintJoinTypeDeprecated);
      if (it != hint_map.end() &&
          absl::EqualsIgnoreCase(it->second.string_value(),
                                 kHintJoinTypeHash)) {
        return true;
      }
      return false;
    }();
    if (!is_hash_join) {
      return error::InvalidHintForNode(kHashJoinBuildSide, "HASH joins");
    }
    const std::string& string_value = value.string_value();
    if (!(absl::EqualsIgnoreCase(string_value, kHashJoinBuildSideLeft) ||
          absl::EqualsIgnoreCase(string_value, kHashJoinBuildSideRight))) {
      return error::InvalidHintValue(name, value.DebugString());
    }
  } else if (absl::EqualsIgnoreCase(name, kHintGroupMethod) ||
             absl::EqualsIgnoreCase(name, kHintGroupTypeDeprecated)) {
    const std::string& string_value = value.string_value();
    if (!(absl::EqualsIgnoreCase(string_value, kHintGroupMethodHash) ||
          absl::EqualsIgnoreCase(string_value, kHintGroupMethodStream))) {
      return error::InvalidHintValue(name, value.DebugString());
    }
  }
  return absl::OkStatus();
}

absl::Status QueryValidator::ExtractEmulatorOptionsForNode(
    const absl::flat_hash_map<absl::string_view, zetasql::Value>&
        node_hint_map) const {
  for (const auto& [hint_name, hint_value] : node_hint_map) {
    if (absl::EqualsIgnoreCase(hint_name,
                               kHintDisableQueryPartitionabilityCheck)) {
      if (!hint_value.type()->IsBool()) {
        return error::InvalidEmulatorHintValue(hint_name,
                                               hint_value.DebugString());
      } else {
        extracted_options_->disable_query_partitionability_check =
            hint_value.bool_value();
      }
    }
    if (absl::EqualsIgnoreCase(hint_name,
                               kHintDisableQueryNullFilteredIndexCheck)) {
      if (!hint_value.type()->IsBool()) {
        return error::InvalidEmulatorHintValue(hint_name,
                                               hint_value.DebugString());
      } else {
        extracted_options_->disable_query_null_filtered_index_check =
            hint_value.bool_value();
      }
    }
  }
  return absl::OkStatus();
}

namespace {

absl::Status CheckAllowedCasts(const zetasql::Type* from_type,
                               const zetasql::Type* to_type) {
  if (to_type->IsArray() && from_type->IsArray() &&
      !to_type->AsArray()->element_type()->Equals(
          from_type->AsArray()->element_type())) {
    return error::NoFeatureSupportDifferentTypeArrayCasts(
        from_type->DebugString(), to_type->DebugString());
  }
  return absl::OkStatus();
}

}  // namespace

absl::Status QueryValidator::DefaultVisit(const zetasql::ResolvedNode* node) {
  ZETASQL_RETURN_IF_ERROR(ValidateHints(node));
  return zetasql::ResolvedASTVisitor::DefaultVisit(node);
}

absl::Status QueryValidator::VisitResolvedQueryStmt(
    const zetasql::ResolvedQueryStmt* node) {
  for (const auto& column : node->output_column_list()) {
    if (column->column().type()->IsStruct())
      return error::UnsupportedReturnStructAsColumn();
  }

  return DefaultVisit(node);
}

absl::Status QueryValidator::VisitResolvedLiteral(
    const zetasql::ResolvedLiteral* node) {
  if (node->type()->IsArray() &&
      node->type()->AsArray()->element_type()->IsStruct() &&
      node->value().is_empty_array()) {
    return error::UnsupportedArrayConstructorSyntaxForEmptyStructArray();
  }

  return DefaultVisit(node);
}

absl::Status QueryValidator::VisitResolvedFunctionCall(
    const zetasql::ResolvedFunctionCall* node) {
  // Check if function is part of supported subset of ZetaSQL
  if (node->function()->IsZetaSQLBuiltin()) {
    bool func_not_supported = !IsSupportedZetaSQLFunction(*node->function());
    if (language_options_.LanguageFeatureEnabled(
            zetasql::FEATURE_JSON_TYPE)) {
      func_not_supported &= !IsSupportedJsonFunction(*node->function());
    }
    if (func_not_supported) {
      return error::UnsupportedFunction(node->function()->SQLName());
    }
  }

  // Out of the supported subset of ZetaSQL, filter out functions that
  // are unimplemented or may require additional validation of arguments.
  ZETASQL_RETURN_IF_ERROR(FilterSafeModeFunction(*node));
  ZETASQL_RETURN_IF_ERROR(
      FilterResolvedFunction(language_options_, sql_features_, *node));

  if (node->function()->FullName(/*include_group=*/false) == "cast") {
    ZETASQL_RETURN_IF_ERROR(
        CheckAllowedCasts(node->argument_list(0)->type(), node->type()));
  }

  return DefaultVisit(node);
}

absl::Status QueryValidator::VisitResolvedAggregateFunctionCall(
    const zetasql::ResolvedAggregateFunctionCall* node) {
  // Check if function is part of supported subset of ZetaSQL
  if (node->function()->IsZetaSQLBuiltin()) {
    bool func_not_supported = !IsSupportedZetaSQLFunction(*node->function());
    if (language_options_.LanguageFeatureEnabled(
            zetasql::FEATURE_JSON_TYPE)) {
      func_not_supported &= !IsSupportedJsonFunction(*node->function());
    }
    if (func_not_supported) {
      return error::UnsupportedFunction(node->function()->SQLName());
    }
  }

  // Out of the supported subset of ZetaSQL, filter out functions that
  // are unimplemented or may require additional validation of arguments.
  ZETASQL_RETURN_IF_ERROR(FilterSafeModeFunction(*node));
  ZETASQL_RETURN_IF_ERROR(FilterResolvedAggregateFunction(sql_features_, *node));
  return DefaultVisit(node);
}

absl::Status QueryValidator::VisitResolvedCast(
    const zetasql::ResolvedCast* node) {
  ZETASQL_RETURN_IF_ERROR(CheckAllowedCasts(node->expr()->type(), node->type()));
  return DefaultVisit(node);
}

absl::Status QueryValidator::VisitResolvedSampleScan(
    const zetasql::ResolvedSampleScan* node) {
  if (node->repeatable_argument()) {
    return error::UnsupportedTablesampleRepeatable();
  }

  if (absl::EqualsIgnoreCase(node->method(), "system")) {
    return error::UnsupportedTablesampleSystem();
  }

  return DefaultVisit(node);
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
