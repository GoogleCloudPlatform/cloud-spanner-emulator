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

#include "backend/query/hint_validator.h"

#include <string>
#include <vector>

#include "zetasql/public/type.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "backend/common/case.h"
#include "common/errors.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

// Hints related to adaptive execution.
constexpr absl::string_view kHintEnableAdaptivePlans = "enable_adaptive_plans";

// Indexes
constexpr absl::string_view kHintForceIndex = "force_index";

// Joins
constexpr absl::string_view kHintJoinForceOrder = "force_join_order";
constexpr absl::string_view kHintJoinMethod = "join_method";
constexpr absl::string_view kHintJoinTypeHash = "hash_join";
constexpr absl::string_view kHintJoinTypeApply = "apply_join";

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

}  // namespace

absl::Status HintValidator::ValidateHints(
    const zetasql::ResolvedNode* node) const {
  std::vector<const zetasql::ResolvedNode*> child_nodes;
  node->GetChildNodes(&child_nodes);
  // Build a hint map.
  absl::flat_hash_map<absl::string_view, zetasql::Value> hint_map;
  for (const zetasql::ResolvedNode* child_node : child_nodes) {
    if (child_node->node_kind() == zetasql::RESOLVED_OPTION) {
      const zetasql::ResolvedOption* hint =
          child_node->GetAs<zetasql::ResolvedOption>();
      if (absl::EqualsIgnoreCase(hint->qualifier(), "spanner") ||
          hint->qualifier().empty()) {
        ZETASQL_RETURN_IF_ERROR(CheckHintName(hint->name(), node->node_kind()));
      }
      ZETASQL_RET_CHECK_EQ(hint->value()->node_kind(), zetasql::RESOLVED_LITERAL);
      const zetasql::Value& value =
          hint->value()->GetAs<zetasql::ResolvedLiteral>()->value();
      if (hint_map.contains(hint->name())) {
        return error::MultipleValuesForSameHint(hint->name());
      }
      hint_map[hint->name()] = value;
    }
  }

  for (const auto& [hint_name, hint_value] : hint_map) {
    ZETASQL_RETURN_IF_ERROR(
        CheckHintValue(hint_name, hint_value, node->node_kind(), hint_map));
  }

  return absl::OkStatus();
}

absl::Status HintValidator::CheckHintName(
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
        kUseAdditionalParallelism, kHintEnableAdaptivePlans}},
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

absl::Status HintValidator::CheckHintValue(
    absl::string_view name, const zetasql::Value& value,
    const zetasql::ResolvedNodeKind node_kind,
    const absl::flat_hash_map<absl::string_view, zetasql::Value>& hint_map)
    const {
  static const auto* supported_hint_types =
      new absl::flat_hash_map<absl::string_view, const zetasql::Type*,
                              zetasql_base::StringViewCaseHash, zetasql_base::StringViewCaseEqual>{{
          {kHintJoinTypeDeprecated, zetasql::types::StringType()},
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
          absl::EqualsIgnoreCase(string_value,
                                 kHintJoinTypeNestedLoopDeprecated))) {
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

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
