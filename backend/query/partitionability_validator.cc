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

#include "backend/query/partitionability_validator.h"

#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/status/status.h"
#include "common/errors.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

absl::Status PartitionabilityValidator::ValidatePartitionability(
    const zetasql::ResolvedNode* node) {
  ZETASQL_DCHECK(node->node_kind() == zetasql::RESOLVED_QUERY_STMT);
  if (HasSubquery(node)) {
    return error::NonPartitionableQuery("Query contains subquery.");
  }
  return ValidateSimpleScan(node);
}

// Verifies that query is a simple scan on one table.
//
// Example query:
//   SELECT o.obj_k FROM Obj o
// ResolvedAST:
// QueryStmt
// +-output_column_list=
// | +-Obj.obj_k#2 AS obj_k [STRING]
// +-query=
//   +-ProjectScan
//     +-column_list=[Obj.obj_k#2]
//     +-input_scan=
//       +-TableScan(column_list=[Obj.obj_k#2], table=Obj)
//
absl::Status PartitionabilityValidator::ValidateSimpleScan(
    const zetasql::ResolvedNode* node) {
  absl::Status error_status =
      error::NonPartitionableQuery("Query is not a simple table scan.");
  const zetasql::ResolvedNode* current_node = node;
  while (true) {
    switch (current_node->node_kind()) {
      case zetasql::RESOLVED_QUERY_STMT:
        current_node =
            current_node->GetAs<zetasql::ResolvedQueryStmt>()->query();
        if (current_node != nullptr &&
            current_node->node_kind() == zetasql::RESOLVED_PROJECT_SCAN) {
          break;
        }
        return error_status;
      case zetasql::RESOLVED_PROJECT_SCAN:
        current_node =
            current_node->GetAs<zetasql::ResolvedProjectScan>()->input_scan();
        if (current_node != nullptr &&
            (current_node->node_kind() == zetasql::RESOLVED_TABLE_SCAN ||
             current_node->node_kind() == zetasql::RESOLVED_FILTER_SCAN)) {
          break;
        }
        return error_status;
      case zetasql::RESOLVED_FILTER_SCAN:
        current_node =
            current_node->GetAs<zetasql::ResolvedFilterScan>()->input_scan();
        if (current_node != nullptr &&
            current_node->node_kind() == zetasql::RESOLVED_TABLE_SCAN) {
          break;
        }
        return error_status;
      case zetasql::RESOLVED_TABLE_SCAN:
        return absl::OkStatus();
      default:
        return error_status;
    }
  }
}

bool PartitionabilityValidator::HasSubquery(
    const zetasql::ResolvedNode* node) {
  ZETASQL_DCHECK(node != nullptr);
  if (node->node_kind() == zetasql::RESOLVED_SUBQUERY_EXPR) {
    return true;
  }
  std::vector<const zetasql::ResolvedNode*> child_nodes;
  node->GetChildNodes(&child_nodes);  // only non-null children returned.
  for (const zetasql::ResolvedNode* child : child_nodes) {
    if (HasSubquery(child)) {
      return true;
    }
  }
  return false;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
