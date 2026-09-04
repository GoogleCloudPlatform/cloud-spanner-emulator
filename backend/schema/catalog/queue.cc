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

#include "backend/schema/catalog/queue.h"

#include <algorithm>
#include <string>

#include "absl/status/status.h"
#include "backend/common/case.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/graph/schema_graph_editor.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "googlesql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

const Column* Queue::FindColumn(const std::string& column_name) const {
  auto itr = columns_map_.find(column_name);
  if (itr == columns_map_.end()) {
    return nullptr;
  }
  return itr->second;
}

const Column* Queue::FindColumnCaseSensitive(
    const std::string& column_name) const {
  const Column* column = FindColumn(column_name);
  if (!column || column->Name() != column_name) {
    return nullptr;
  }
  return column;
}

const KeyColumn* Queue::FindKeyColumn(const std::string& column_name) const {
  const Column* column = FindColumn(column_name);
  if (column == nullptr) {
    return nullptr;
  }
  auto it = std::find_if(primary_key_.begin(), primary_key_.end(),
                         [column](const KeyColumn* key_column) {
                           return key_column->column() == column;
                         });
  if (it == primary_key_.end()) {
    return nullptr;
  }
  return *it;
}

absl::Status Queue::Validate(SchemaValidationContext* context) const {
  return validate_(this, context);
}

absl::Status Queue::ValidateUpdate(const SchemaNode* orig,
                                   SchemaValidationContext* context) const {
  return validate_update_(this, orig->As<const Queue>(), context);
}

absl::Status Queue::DeepClone(SchemaGraphEditor* editor,
                              const SchemaNode* orig) {
  if (parent_table_) {
    GOOGLESQL_ASSIGN_OR_RETURN(const SchemaNode* schema_node,
                     editor->Clone(parent_table_));
    parent_table_ = schema_node->As<const Table>();
  }

  for (auto it = columns_.begin(); it != columns_.end();) {
    GOOGLESQL_ASSIGN_OR_RETURN(const SchemaNode* schema_node, editor->Clone(*it));
    if (schema_node->is_deleted()) {
      columns_map_.erase((*it)->Name());
      it = columns_.erase(it);
    } else {
      const Column* cloned_column = schema_node->As<const Column>();
      *it = cloned_column;
      columns_map_[cloned_column->Name()] = cloned_column;
      ++it;
    }
  }

  for (const KeyColumn*& key_column : primary_key_) {
    GOOGLESQL_ASSIGN_OR_RETURN(const SchemaNode* schema_node, editor->Clone(key_column));
    key_column = schema_node->As<const KeyColumn>();
  }

  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
