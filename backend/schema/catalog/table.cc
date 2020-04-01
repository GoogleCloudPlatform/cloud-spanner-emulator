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

#include "backend/schema/catalog/table.h"

#include "zetasql/public/type.h"
#include "absl/container/flat_hash_map.h"
#include "absl/memory/memory.h"
#include "zetasql/base/status.h"
#include "zetasql/base/case.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/substitute.h"
#include "backend/common/case.h"
#include "backend/datamodel/types.h"
#include "backend/schema/catalog/column.h"
#include "common/errors.h"
#include "common/limits.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

const Column* Table::FindColumn(const std::string& column_name) const {
  auto itr = columns_map_.find(column_name);
  if (itr == columns_map_.end()) {
    return nullptr;
  }
  return itr->second;
}

const Column* Table::FindColumnCaseSensitive(
    const std::string& column_name) const {
  auto column = FindColumn(column_name);
  if (!column || column->Name() != column_name) {
    return nullptr;
  }
  return column;
}

const KeyColumn* Table::FindKeyColumn(const std::string& column_name) const {
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

std::string Table::PrimaryKeyDebugString() const {
  std::string result;
  for (int i = 0; i < primary_key_.size(); ++i) {
    if (i == 0) {
      absl::StrAppend(&result, "<", primary_key_[i]->column()->Name(), ">");
    } else {
      absl::StrAppend(&result, ", <", primary_key_[i]->column()->Name(), ">");
    }
  }
  return result;
}

std::string OwningObjectName(const Table* table) {
  return table->owner_index() ? table->owner_index()->Name() : table->Name();
}

std::string OwningObjectType(const Table* table) {
  return table->owner_index() ? "Index" : "Table";
}

zetasql_base::Status Table::Validate(SchemaValidationContext* context) const {
  return validate_(this, context);
}

zetasql_base::Status Table::ValidateUpdate(const SchemaNode* orig,
                                   SchemaValidationContext* context) const {
  return validate_update_(this, orig->As<const Table>(), context);
}

zetasql_base::Status Table::DeepClone(SchemaGraphEditor* editor,
                              const SchemaNode* orig) {
  if (parent_table_) {
    ZETASQL_ASSIGN_OR_RETURN(const auto* schema_node, editor->Clone(parent_table_));
    parent_table_ = schema_node->As<const Table>();
  }

  for (auto it = columns_.begin(); it != columns_.end();) {
    ZETASQL_ASSIGN_OR_RETURN(const auto* schema_node, editor->Clone(*it));
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

  for (auto& key_column : primary_key_) {
    ZETASQL_ASSIGN_OR_RETURN(const auto* schema_node, editor->Clone(key_column));
    key_column = schema_node->As<const KeyColumn>();
  }

  for (auto it = child_tables_.begin(); it != child_tables_.end();) {
    ZETASQL_ASSIGN_OR_RETURN(const auto* schema_node, editor->Clone(*it));
    if (schema_node->is_deleted()) {
      it = child_tables_.erase(it);
    } else {
      *it = schema_node->As<const Table>();
      ++it;
    }
  }

  for (auto it = indexes_.begin(); it != indexes_.end();) {
    ZETASQL_ASSIGN_OR_RETURN(const auto* schema_node, editor->Clone(*it));
    if (schema_node->is_deleted()) {
      it = indexes_.erase(it);
    } else {
      *it = schema_node->As<const Index>();
      ++it;
    }
  }

  if (owner_index_) {
    ZETASQL_ASSIGN_OR_RETURN(const auto* schema_node, editor->Clone(owner_index_));
    owner_index_ = schema_node->As<const Index>();
    if (owner_index_->is_deleted()) {
      MarkDeleted();
    }
  }

  return zetasql_base::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
