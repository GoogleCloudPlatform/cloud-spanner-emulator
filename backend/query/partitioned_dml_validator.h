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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_PARTITIONED_DML_VALIDATOR_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_PARTITIONED_DML_VALIDATOR_H_

#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_visitor.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/status/status.h"
#include "common/errors.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Validates whether a DML update or delete statement can be executed through
// partitioned DML.
class PartitionedDMLValidator : public zetasql::ResolvedASTVisitor {
 public:
  PartitionedDMLValidator() = default;

  absl::Status DefaultVisit(const zetasql::ResolvedNode* node) override {
    if (node->node_kind() == zetasql::RESOLVED_INSERT_STMT) {
      return error::NoInsertForPartitionedDML();
    }
    // For other kinds of DML statements, visit the full tree to collect
    // the number of tables involved.
    return zetasql::ResolvedASTVisitor::DefaultVisit(node);
  }

 private:
  absl::Status VisitResolvedTableScan(
      const zetasql::ResolvedTableScan* scan) final {
    num_tables_++;
    // Partitioned DML does not support more than 1 table.
    if (num_tables_ > 1) {
      return error::PartitionedDMLOnlySupportsSimpleQuery();
    }
    // Continue to visit the rest of the tree.
    return zetasql::ResolvedASTVisitor::DefaultVisit(scan);
  }

  // Number of tables referenced in the DML statement.
  int num_tables_ = 0;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_PARTITIONED_DML_VALIDATOR_H_
