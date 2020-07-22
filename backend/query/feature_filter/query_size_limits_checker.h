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


#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_FEATURE_FILTER_QUERY_SIZE_LIMITS_CHECKER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_FEATURE_FILTER_QUERY_SIZE_LIMITS_CHECKER_H_

#include <map>
#include <set>

#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/status/status.h"

namespace google::spanner::emulator::backend {

class QuerySizeLimitsChecker {
 public:
  absl::Status CheckQueryAgainstLimits(const zetasql::ResolvedNode* ast_root);
  virtual ~QuerySizeLimitsChecker() {}
  static const int kMaxJoins;
  static const int kMaxNestedSubqueryExpressions;
  static const int kMaxNestedSubselects;
  static const int kMaxNestedFunctionNodes;
  static const int kMaxFunctionNodes;
  static const int kMaxColumnsInGroupBy;
  static const int kMaxNestedGroupBy;
  static const int kMaxParameters;
  static const int kMaxStructFields;
  static const int kMaxNestedStructDepth;
  static const int kMaxUnionsInQuery;
  static const int kMaxSubqueryExpressionChildren;

 private:
  struct NodeCounts {
    int max_occurrence_depth;
    int total_occurrences;
    int nested_occurrences;
  };
  absl::Status GetNodeMetricsAndRunLocalChecks(
      const zetasql::ResolvedNode* ast_root,
      const std::set<zetasql::ResolvedNodeKind>& interest_nodes,
      std::map<zetasql::ResolvedNodeKind, NodeCounts>* collected_node_counts);
  absl::Status CheckNumPredicates(
      const std::map<zetasql::ResolvedNodeKind, NodeCounts>&
          collected_node_counts);
  absl::Status CheckPredicateBooleanExpressionDepth(
      const std::map<zetasql::ResolvedNodeKind, NodeCounts>&
          collected_node_counts);
  absl::Status CheckNumJoins(const std::map<zetasql::ResolvedNodeKind,
                                            NodeCounts>& collected_node_counts);
  absl::Status CheckSubqueryExpressionDepth(
      const std::map<zetasql::ResolvedNodeKind, NodeCounts>&
          collected_node_counts);
  absl::Status CheckSubselectDepth(
      const std::map<zetasql::ResolvedNodeKind, NodeCounts>&
          collected_node_counts);
  absl::Status CheckGroupByDepth(
      const std::map<zetasql::ResolvedNodeKind, NodeCounts>&
          collected_node_counts);
  absl::Status CheckNumParameters(
      const std::map<zetasql::ResolvedNodeKind, NodeCounts>&
          collected_node_counts);
  absl::Status RunGlobalChecks(
      const std::map<zetasql::ResolvedNodeKind, NodeCounts>&
          collected_node_counts);
  absl::Status CheckNumColumnsInGroupBy(const zetasql::ResolvedNode* node);
  absl::Status CheckNumFieldsInStruct(const zetasql::ResolvedNode* node);
  absl::Status CheckStructParameterBreadthAndDepth(
      const zetasql::ResolvedNode* node);
  absl::Status CheckNumUnions(const zetasql::ResolvedNode* node);
  absl::Status CheckNumSubQueriesInSelectList(
      const zetasql::ResolvedNode* node);
  absl::Status RunNodeLocalChecks(const zetasql::ResolvedNode* node);
};

}  // namespace google::spanner::emulator::backend

#endif
