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

#include "backend/query/feature_filter/query_size_limits_checker.h"

#include <map>
#include <queue>
#include <set>
#include <utility>
#include <vector>

#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/status/status.h"
#include "common/errors.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

using zetasql::ResolvedAggregateScan;
using zetasql::ResolvedMakeStruct;
using zetasql::ResolvedNode;
using zetasql::ResolvedNodeKind;
using zetasql::ResolvedParameter;
using zetasql::ResolvedProjectScan;
using zetasql::ResolvedSetOperationScan;

namespace google::spanner::emulator::backend {

const int QuerySizeLimitsChecker::kMaxJoins = 20;
const int QuerySizeLimitsChecker::kMaxNestedFunctionNodes = 75;
const int QuerySizeLimitsChecker::kMaxNestedSubqueryExpressions = 25;
const int QuerySizeLimitsChecker::kMaxNestedSubselects = 60;
const int QuerySizeLimitsChecker::kMaxNestedGroupBy = 35;
const int QuerySizeLimitsChecker::kMaxUnionsInQuery = 200;
const int QuerySizeLimitsChecker::kMaxSubqueryExpressionChildren = 50;
const int QuerySizeLimitsChecker::kMaxFunctionNodes = 1000;
const int QuerySizeLimitsChecker::kMaxColumnsInGroupBy = 1000;
const int QuerySizeLimitsChecker::kMaxParameters = 950;
const int QuerySizeLimitsChecker::kMaxStructFields = 1000;
const int QuerySizeLimitsChecker::kMaxNestedStructDepth = 15;

absl::Status QuerySizeLimitsChecker::CheckQueryAgainstLimits(
    const ResolvedNode* ast_root) {
  std::set<ResolvedNodeKind> interest_nodes;
  interest_nodes.insert(ResolvedNodeKind::RESOLVED_JOIN_SCAN);
  interest_nodes.insert(ResolvedNodeKind::RESOLVED_PROJECT_SCAN);
  interest_nodes.insert(ResolvedNodeKind::RESOLVED_SUBQUERY_EXPR);
  interest_nodes.insert(ResolvedNodeKind::RESOLVED_AGGREGATE_SCAN);
  interest_nodes.insert(ResolvedNodeKind::RESOLVED_FUNCTION_CALL);
  interest_nodes.insert(ResolvedNodeKind::RESOLVED_PARAMETER);
  std::map<ResolvedNodeKind, NodeCounts> collected_node_counts;
  ZETASQL_RETURN_IF_ERROR(GetNodeMetricsAndRunLocalChecks(ast_root, interest_nodes,
                                                  &collected_node_counts));
  ZETASQL_RETURN_IF_ERROR(RunGlobalChecks(collected_node_counts));
  return absl::OkStatus();
}

absl::Status QuerySizeLimitsChecker::CheckNumPredicates(
    const std::map<ResolvedNodeKind, NodeCounts>& collected_node_counts) {
  auto it =
      collected_node_counts.find(ResolvedNodeKind::RESOLVED_FUNCTION_CALL);
  if (it == collected_node_counts.end()) {
    return absl::OkStatus();
  }
  const auto node_counts = it->second;
  if (node_counts.total_occurrences > kMaxFunctionNodes) {
    return error::TooManyFunctions(kMaxFunctionNodes);
  }
  return absl::OkStatus();
}

absl::Status QuerySizeLimitsChecker::CheckPredicateBooleanExpressionDepth(
    const std::map<ResolvedNodeKind, NodeCounts>& collected_node_counts) {
  auto it =
      collected_node_counts.find(ResolvedNodeKind::RESOLVED_FUNCTION_CALL);
  if (it == collected_node_counts.end()) {
    return absl::OkStatus();
  }
  const auto node_counts = it->second;
  if (node_counts.nested_occurrences > kMaxNestedFunctionNodes) {
    return error::TooManyNestedBooleanPredicates(kMaxNestedFunctionNodes);
  }
  return absl::OkStatus();
}

absl::Status QuerySizeLimitsChecker::CheckNumJoins(
    const std::map<ResolvedNodeKind, NodeCounts>& collected_node_counts) {
  auto it = collected_node_counts.find(ResolvedNodeKind::RESOLVED_JOIN_SCAN);
  if (it == collected_node_counts.end()) {
    return absl::OkStatus();
  }
  const auto node_counts = it->second;
  if (node_counts.total_occurrences > kMaxJoins) {
    return error::TooManyJoins(kMaxJoins);
  }
  return absl::OkStatus();
}

absl::Status QuerySizeLimitsChecker::CheckSubqueryExpressionDepth(
    const std::map<ResolvedNodeKind, NodeCounts>& collected_node_counts) {
  auto it =
      collected_node_counts.find(ResolvedNodeKind::RESOLVED_SUBQUERY_EXPR);
  if (it == collected_node_counts.end()) {
    return absl::OkStatus();
  }
  const auto node_counts = it->second;
  if (node_counts.nested_occurrences > kMaxNestedSubqueryExpressions) {
    return error::TooManyNestedSubqueries(kMaxNestedSubqueryExpressions);
  }
  return absl::OkStatus();
}

absl::Status QuerySizeLimitsChecker::CheckSubselectDepth(
    const std::map<ResolvedNodeKind, NodeCounts>& collected_node_counts) {
  auto it = collected_node_counts.find(ResolvedNodeKind::RESOLVED_PROJECT_SCAN);
  if (it == collected_node_counts.end()) {
    return absl::OkStatus();
  }
  const auto node_counts = it->second;
  if (node_counts.nested_occurrences > kMaxNestedSubselects) {
    return error::TooManyNestedSubselects(kMaxNestedSubselects);
  }
  return absl::OkStatus();
}

absl::Status QuerySizeLimitsChecker::CheckGroupByDepth(
    const std::map<ResolvedNodeKind, NodeCounts>& collected_node_counts) {
  auto it =
      collected_node_counts.find(ResolvedNodeKind::RESOLVED_AGGREGATE_SCAN);
  if (it == collected_node_counts.end()) {
    return absl::OkStatus();
  }
  const auto node_counts = it->second;
  if (node_counts.nested_occurrences > kMaxNestedGroupBy) {
    return error::TooManyNestedAggregates(kMaxNestedGroupBy);
  }
  return absl::OkStatus();
}

absl::Status QuerySizeLimitsChecker::CheckNumParameters(
    const std::map<ResolvedNodeKind, NodeCounts>& collected_node_counts) {
  auto it = collected_node_counts.find(ResolvedNodeKind::RESOLVED_PARAMETER);
  if (it == collected_node_counts.end()) {
    return absl::OkStatus();
  }
  const auto node_counts = it->second;
  if (node_counts.total_occurrences > kMaxParameters) {
    return error::TooManyParameters(kMaxParameters);
  }
  return absl::OkStatus();
}

absl::Status QuerySizeLimitsChecker::CheckNumColumnsInGroupBy(
    const ResolvedNode* node) {
  const ResolvedAggregateScan* aggregate_scan =
      node->GetAs<ResolvedAggregateScan>();
  ZETASQL_RET_CHECK(aggregate_scan != nullptr);
  if (aggregate_scan->group_by_list().size() > kMaxColumnsInGroupBy) {
    return error::TooManyAggregates(kMaxColumnsInGroupBy);
  }
  return absl::OkStatus();
}

absl::Status QuerySizeLimitsChecker::CheckNumUnions(const ResolvedNode* node) {
  const ResolvedSetOperationScan* union_scan =
      node->GetAs<ResolvedSetOperationScan>();
  ZETASQL_RET_CHECK(union_scan != nullptr);
  if (union_scan->input_item_list().size() > kMaxUnionsInQuery) {
    return error::TooManyUnions(kMaxUnionsInQuery);
  }
  return absl::OkStatus();
}

absl::Status QuerySizeLimitsChecker::CheckNumSubQueriesInSelectList(
    const ResolvedNode* node) {
  const ResolvedProjectScan* project_scan = node->GetAs<ResolvedProjectScan>();
  ZETASQL_RET_CHECK(project_scan != nullptr);
  if (project_scan->expr_list_size() > kMaxSubqueryExpressionChildren) {
    return error::TooManySubqueryChildren(kMaxSubqueryExpressionChildren);
  }
  return absl::OkStatus();
}

absl::Status QuerySizeLimitsChecker::CheckNumFieldsInStruct(
    const ResolvedNode* node) {
  const ResolvedMakeStruct* struct_node = node->GetAs<ResolvedMakeStruct>();
  ZETASQL_RET_CHECK(struct_node != nullptr);
  if (struct_node->field_list().size() > kMaxStructFields) {
    return error::TooManyStructFields(kMaxStructFields);
  }
  return absl::OkStatus();
}

absl::Status QuerySizeLimitsChecker::CheckStructParameterBreadthAndDepth(
    const ResolvedNode* node) {
  const ResolvedParameter* param_node = node->GetAs<ResolvedParameter>();
  ZETASQL_RET_CHECK(param_node != nullptr);
  if (!param_node->type()->IsStruct() &&
      !(param_node->type()->IsArray() &&
        param_node->type()->AsArray()->element_type()->IsStruct())) {
    return absl::OkStatus();
  }
  const zetasql::StructType* struct_type =
      param_node->type()->IsStruct()
          ? param_node->type()->AsStruct()
          : param_node->type()->AsArray()->element_type()->AsStruct();

  std::queue<const zetasql::StructType*> type_queue;
  int depth = 1;
  type_queue.push(struct_type);
  type_queue.push(nullptr);
  while (!type_queue.empty()) {
    const auto* type = type_queue.front();
    type_queue.pop();

    if (type == nullptr) {
      if (!type_queue.empty()) {
        type_queue.push(nullptr);
        ++depth;
        if (depth > kMaxNestedStructDepth) {
          return error::TooManyNestedStructs(kMaxNestedStructDepth);
        }
      }
      continue;
    }
    if (type->num_fields() > kMaxStructFields) {
      return error::TooManyStructFields(kMaxStructFields);
    }

    for (int i = 0; i < type->num_fields(); ++i) {
      const zetasql::Type* next_type = nullptr;
      if (type->field(i).type->IsArray()) {
        next_type = type->field(i).type->AsArray()->element_type();
      } else {
        next_type = type->field(i).type;
      }
      if (next_type->IsStruct()) {
        type_queue.push(next_type->AsStruct());
      }
    }
  }
  return absl::OkStatus();
}

absl::Status QuerySizeLimitsChecker::RunGlobalChecks(
    const std::map<zetasql::ResolvedNodeKind, NodeCounts>&
        collected_node_counts) {
  ZETASQL_RETURN_IF_ERROR(CheckNumPredicates(collected_node_counts));
  ZETASQL_RETURN_IF_ERROR(CheckPredicateBooleanExpressionDepth(collected_node_counts));
  ZETASQL_RETURN_IF_ERROR(CheckNumJoins(collected_node_counts));
  ZETASQL_RETURN_IF_ERROR(CheckSubqueryExpressionDepth(collected_node_counts));
  ZETASQL_RETURN_IF_ERROR(CheckSubselectDepth(collected_node_counts));
  ZETASQL_RETURN_IF_ERROR(CheckGroupByDepth(collected_node_counts));
  ZETASQL_RETURN_IF_ERROR(CheckNumParameters(collected_node_counts));
  return absl::OkStatus();
}

absl::Status QuerySizeLimitsChecker::RunNodeLocalChecks(
    const ResolvedNode* node) {
  switch (node->node_kind()) {
    case ResolvedNodeKind::RESOLVED_AGGREGATE_SCAN:
      ZETASQL_RETURN_IF_ERROR(CheckNumColumnsInGroupBy(node));
      break;
    case ResolvedNodeKind::RESOLVED_SET_OPERATION_SCAN:
      ZETASQL_RETURN_IF_ERROR(CheckNumUnions(node));
      break;
    case ResolvedNodeKind::RESOLVED_MAKE_STRUCT:
      ZETASQL_RETURN_IF_ERROR(CheckNumFieldsInStruct(node));
      break;
    case ResolvedNodeKind::RESOLVED_PROJECT_SCAN:
      ZETASQL_RETURN_IF_ERROR(CheckNumSubQueriesInSelectList(node));
      break;
    case ResolvedNodeKind::RESOLVED_PARAMETER:
      ZETASQL_RETURN_IF_ERROR(CheckStructParameterBreadthAndDepth(node));
      break;
    default:
      return absl::OkStatus();
  }
  return absl::OkStatus();
}

absl::Status QuerySizeLimitsChecker::GetNodeMetricsAndRunLocalChecks(
    const ResolvedNode* ast_root,
    const std::set<ResolvedNodeKind>& interest_nodes,
    std::map<ResolvedNodeKind, NodeCounts>* collected_node_counts) {
  std::queue<const ResolvedNode*> node_queue;
  node_queue.push(ast_root);
  node_queue.push(nullptr);
  int tree_depth = 0;
  while (!node_queue.empty()) {
    const ResolvedNode* node = node_queue.front();
    node_queue.pop();
    if (node == nullptr) {
      if (!node_queue.empty()) {
        node_queue.push(nullptr);
        tree_depth++;
      }
      continue;
    }
    if (interest_nodes.find(node->node_kind()) != interest_nodes.end()) {
      auto it = collected_node_counts->find(node->node_kind());
      if (it != collected_node_counts->end()) {
        NodeCounts& count_info = it->second;
        count_info.total_occurrences++;
        if (tree_depth > count_info.max_occurrence_depth) {
          count_info.nested_occurrences++;
        }
        count_info.max_occurrence_depth = tree_depth;
      } else {
        NodeCounts node_counts;
        node_counts.max_occurrence_depth = tree_depth;
        node_counts.nested_occurrences = 1;
        node_counts.total_occurrences = 1;
        (*collected_node_counts)[node->node_kind()] = node_counts;
      }
    }
    ZETASQL_RETURN_IF_ERROR(RunNodeLocalChecks(node));
    std::vector<const ResolvedNode*> child_nodes;
    node->GetChildNodes(&child_nodes);
    for (const ResolvedNode* child_node : child_nodes) {
      node_queue.push(child_node);
    }
  }
  return absl::OkStatus();
}

}  // namespace google::spanner::emulator::backend
