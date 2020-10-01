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

#include "backend/schema/catalog/foreign_key.h"

#include "absl/strings/substitute.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/graph/schema_graph_editor.h"
#include "common/errors.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

const Table* ForeignKey::referencing_data_table() const {
  return referencing_index_ == nullptr ? referencing_table_
                                       : referencing_index_->index_data_table();
}

const Table* ForeignKey::referenced_data_table() const {
  return referenced_index_ == nullptr ? referenced_table_
                                      : referenced_index_->index_data_table();
}

absl::Status ForeignKey::Validate(SchemaValidationContext* context) const {
  return validate_(this, context);
}

absl::Status ForeignKey::ValidateUpdate(
    const SchemaNode* orig, SchemaValidationContext* context) const {
  return validate_update_(this, orig->As<const ForeignKey>(), context);
}

absl::Status ForeignKey::DeepClone(SchemaGraphEditor* editor,
                                   const SchemaNode* orig) {
  for (const Table** table : {&referencing_table_, &referenced_table_}) {
    ZETASQL_ASSIGN_OR_RETURN(const auto* schema_node, editor->Clone(*table));
    *table = schema_node->As<const Table>();
  }
  for (auto* columns : {&referencing_columns_, &referenced_columns_}) {
    for (const Column*& column : *columns) {
      ZETASQL_ASSIGN_OR_RETURN(const auto* schema_node, editor->Clone(column));
      column = schema_node->As<const Column>();
    }
  }
  for (const Index** index : {&referencing_index_, &referenced_index_}) {
    if (*index != nullptr) {
      ZETASQL_ASSIGN_OR_RETURN(const auto* schema_node, editor->Clone(*index));
      *index = schema_node->As<const Index>();
    }
  }
  // Dropping a table automatically drops its foreign keys and backing indexes.
  if (referencing_table_->is_deleted()) {
    MarkDeleted();
  }
  return absl::OkStatus();
}

std::string ForeignKey::DebugString() const {
  auto column_names = [](const std::vector<const Column*>& columns) {
    return absl::StrJoin(columns, ",",
                         [](std::string* out, const Column* column) {
                           absl::StrAppend(out, column->Name());
                         });
  };
  return absl::Substitute(
      "FK:$0:$1($2)[$3]:$4($5)[$6]", Name(), referencing_table_->Name(),
      column_names(referencing_columns_),
      referencing_index_ == nullptr ? "PK" : referencing_index_->Name(),
      referenced_table_->Name(), column_names(referenced_columns_),
      referenced_index_ == nullptr ? "PK" : referenced_index_->Name());
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
