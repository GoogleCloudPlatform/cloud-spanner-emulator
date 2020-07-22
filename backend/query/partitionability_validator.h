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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_PARTITIONABILITY_VALIDATOR_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_PARTITIONABILITY_VALIDATOR_H_

#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_visitor.h"
#include "absl/status/status.h"
#include "backend/schema/catalog/schema.h"
#include "common/errors.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Implements a ResolvedASTVisitor to validate partitionability for
// partition query.
class PartitionabilityValidator : public zetasql::ResolvedASTVisitor {
 public:
  explicit PartitionabilityValidator(const Schema* schema) : schema_(schema) {}

  absl::Status DefaultVisit(const zetasql::ResolvedNode* node) override {
    if (node->node_kind() == zetasql::RESOLVED_QUERY_STMT) {
      ZETASQL_RETURN_IF_ERROR(ValidatePartitionability(node));
    }
    return zetasql::ResolvedASTVisitor::DefaultVisit(node);
  }

 private:
  // Returns OK if query is partitionable.
  absl::Status ValidatePartitionability(const zetasql::ResolvedNode* node);

  // Returns OK if query is a simple scan on one table.
  absl::Status ValidateSimpleScan(const zetasql::ResolvedNode* node);

  // Returns true if node itself or one of its descendants is a subquery expr.
  bool HasSubquery(const zetasql::ResolvedNode* node);

  const Schema* schema_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_PARTITIONABILITY_VALIDATOR_H_
