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

#include "backend/schema/catalog/check_constraint.h"

#include "backend/schema/catalog/table.h"
#include "backend/schema/graph/schema_graph_editor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

absl::Status CheckConstraint::Validate(SchemaValidationContext* context) const {
  return validate_(this, context);
}

absl::Status CheckConstraint::ValidateUpdate(
    const SchemaNode* orig, SchemaValidationContext* context) const {
  return validate_update_(this, orig->As<const CheckConstraint>(), context);
}

std::string CheckConstraint::DebugString() const {
  return absl::Substitute("CK:$0($1)", Name(), expression_);
}

absl::Status CheckConstraint::DeepClone(SchemaGraphEditor* editor,
                                        const SchemaNode* orig) {
  ZETASQL_ASSIGN_OR_RETURN(const auto* table_clone, editor->Clone(table_));
  table_ = table_clone->As<const Table>();
  // The Check Constraint should be deleted if the table containing the Check
  // Constraint is deleted.
  if (table_->is_deleted()) {
    MarkDeleted();
  }

  for (const Column*& column : dependent_columns_) {
    ZETASQL_ASSIGN_OR_RETURN(const auto* schema_node, editor->Clone(column));
    column = schema_node->As<const Column>();
  }

  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
