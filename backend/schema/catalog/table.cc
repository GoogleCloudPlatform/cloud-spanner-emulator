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

namespace {

zetasql_base::Status CheckKeyPartCompatibility(const Table* interleaved_table,
                                       const KeyColumn* parent_key,
                                       const KeyColumn* child_key,
                                       bool ignore_nullability) {
  const std::string object_type = OwningObjectType(interleaved_table);
  const std::string object_name = OwningObjectName(interleaved_table);
  const Column* parent_key_col = parent_key->column();
  const Column* child_key_col = child_key->column();

  if (!zetasql_base::CaseEqual(child_key_col->Name(), parent_key_col->Name())) {
    // The parent key column does not match the child key column. But perhaps
    // the child declared the key column in a different position. Provide a
    // more helpful error message in this case (as they do refer to the parent
    // column, just at the wrong location).
    auto child_pk = interleaved_table->owner_index()
                        ? interleaved_table->owner_index()->key_columns()
                        : interleaved_table->primary_key();
    for (int i = 0; i < child_pk.size(); ++i) {
      if (zetasql_base::CaseEqual(child_pk[i]->column()->Name(), parent_key_col->Name())) {
        return error::IncorrectParentKeyPosition(object_type, object_name,
                                                 parent_key_col->Name(), i);
      }
    }
    return error::MustReferenceParentKeyColumn(object_type, object_name,
                                               parent_key_col->Name());
  }

  // Parent and child key sort orders should match.
  if (child_key->is_descending() != parent_key->is_descending()) {
    return error::IncorrectParentKeyOrder(
        object_type, object_name, parent_key_col->Name(),
        child_key->is_descending() ? "ASC" : "DESC");
  }

  // Parent and child key column types should match.
  if (!child_key_col->GetType()->Equals(parent_key_col->GetType())) {
    return error::IncorrectParentKeyType(object_type, object_name,
                                         parent_key_col->Name(),
                                         ToString(child_key_col->GetType()),
                                         ToString(parent_key_col->GetType()));
  }

  // We already checked the type. Check the type length.
  if (child_key_col->declared_max_length() !=
      parent_key_col->declared_max_length()) {
    auto column_length = [](const Column* column) {
      return column->declared_max_length().has_value()
                 ? absl::StrCat(column->declared_max_length().value())
                 : "MAX";
    };
    return error::IncorrectParentKeyLength(
        object_type, object_name, parent_key_col->Name(),
        column_length(child_key_col), column_length(parent_key_col));
  }

  // We ignore nullability check for scenarios where the child table
  // keys belong to a null filtered interleaved index.
  if (ignore_nullability) {
    return zetasql_base::OkStatus();
  }

  // Parent and child key column nullability should match.
  if (child_key_col->is_nullable() != parent_key_col->is_nullable()) {
    return error::IncorrectParentKeyNullability(
        object_type, object_name, parent_key_col->Name(),
        parent_key_col->is_nullable() ? "nullable" : "not null",
        child_key_col->is_nullable() ? "nullable" : "not null");
  }

  return zetasql_base::Status();
}

zetasql_base::Status CheckInterleaveDepthLimit(const Table* table) {
  int depth = 1;
  const Table* to_test = table;
  while (to_test->parent()) {
    to_test = to_test->parent();
    ++depth;
    if (depth > limits::kMaxInterleavingDepth) {
      return error::DeepNesting(OwningObjectType(table),
                                OwningObjectName(table),
                                limits::kMaxInterleavingDepth);
    }
  }
  return zetasql_base::OkStatus();
}

}  // namespace

zetasql_base::Status Table::Validate(SchemaValidationContext* context) const {
  ZETASQL_RET_CHECK(!name_.empty());
  ZETASQL_RET_CHECK(!id_.empty());

  if (is_public()) {
    if (name_.length() > limits::kMaxSchemaIdentifierLength) {
      return error::InvalidSchemaName("Table", name_,
                                      "name exceeds maximum allowed "
                                      "identifier length");
    }
  }

  const std::string object_type = OwningObjectType(this);
  const std::string object_name = OwningObjectName(this);

  // Validate that all columns are unique.
  CaseInsensitiveStringSet unique_columns;
  for (const Column* column : columns_) {
    ZETASQL_RET_CHECK_NE(column, nullptr);
    const std::string& column_name = column->Name();
    ZETASQL_RET_CHECK_EQ(column->table(), this);
    if (unique_columns.contains(column_name)) {
      return error::DuplicateColumnName(column->FullName());
    }
    unique_columns.insert(column_name);
  }

  if (columns_.size() > limits::kMaxColumnsPerTable) {
    return error::TooManyColumns(object_type, object_name,
                                 limits::kMaxColumnsPerTable);
  }

  // Validate that all key columns are unique.
  CaseInsensitiveStringSet unique_keys;
  for (const KeyColumn* key_column : primary_key_) {
    ZETASQL_RET_CHECK_NE(key_column, nullptr);
    const Column* column = key_column->column();
    ZETASQL_RET_CHECK_NE(column, nullptr);
    const Column* table_column = FindColumn(column->Name());
    ZETASQL_RET_CHECK_EQ(table_column, column);
    if (unique_keys.contains(column->Name())) {
      return error::MultipleRefsToKeyColumn(object_type, object_name,
                                            column->Name());
    }
    unique_keys.insert(column->Name());
  }

  if (primary_key_.size() > limits::kMaxKeyColumns) {
    return error::TooManyKeys(object_type, object_name, primary_key_.size(),
                              limits::kMaxKeyColumns);
  }

  if (!indexes_.empty()) {
    ZETASQL_RET_CHECK(!columns_.empty());
    ZETASQL_RET_CHECK(!primary_key_.empty());
  }

  for (const Index* index : indexes_) {
    ZETASQL_RET_CHECK_NE(index, nullptr);
    ZETASQL_RET_CHECK_EQ(index->indexed_table(), this);
  }

  if (indexes_.size() > limits::kMaxIndexesPerTable) {
    const Index* last_index = indexes_[limits::kMaxIndexesPerTable];
    return error::TooManyIndicesPerTable(last_index->Name(), Name(),
                                         limits::kMaxIndexesPerTable);
  }

  // Check interleave compatibility.
  if (!parent_table_) {
    if (on_delete_action_.has_value()) {
      return error::SetOnDeleteWithoutInterleaving(Name());
    }
  } else {
    bool ignore_nullability =
        owner_index() != nullptr && owner_index()->is_null_filtered();
    ZETASQL_RET_CHECK(parent_table_->is_public());
    const auto& parent_pk = parent_table_->primary_key();
    for (int i = 0; i < parent_pk.size(); ++i) {
      // The child has fewer primary key parts than the parent.
      if (i >= primary_key_.size()) {
        return error::MustReferenceParentKeyColumn(
            OwningObjectType(this), OwningObjectName(this),
            parent_pk[i]->column()->Name());
      }

      ZETASQL_RETURN_IF_ERROR(CheckKeyPartCompatibility(
          this, parent_pk[i], primary_key_[i], ignore_nullability));
    }
    ZETASQL_RETURN_IF_ERROR(CheckInterleaveDepthLimit(this));

    // Cannot add a table with no columns as a child.
    if (columns_.empty()) {
      return error::NoColumnsTable(OwningObjectType(this),
                                   OwningObjectName(this));
    }
  }

  for (const Table* child : child_tables_) {
    ZETASQL_RET_CHECK_NE(child, nullptr);
    ZETASQL_RET_CHECK_EQ(child->parent(), this);
  }

  if (owner_index_) {
    ZETASQL_RET_CHECK_EQ(indexes_.size(), 0);
    ZETASQL_RET_CHECK_EQ(child_tables_.size(), 0);
    ZETASQL_RET_CHECK(!columns_.empty());
    ZETASQL_RET_CHECK(!primary_key_.empty());
    ZETASQL_RET_CHECK_EQ(owner_index_->index_data_table(), this);
  }

  return zetasql_base::OkStatus();
}

zetasql_base::Status Table::ValidateUpdate(const SchemaNode* orig,
                                   SchemaValidationContext* context) const {
  if (is_deleted()) {
    ZETASQL_RET_CHECK(!owner_index_ || owner_index()->is_deleted());
    if (!child_tables_.empty()) {
      // Build a sorted list of interleaved child tables and indexes.
      std::vector<std::string> interleaved_tables;
      std::vector<std::string> interleaved_indices;
      for (const auto& entry : child_tables_) {
        if (entry->owner_index()) {
          interleaved_indices.push_back(entry->owner_index()->Name());
        } else {
          interleaved_tables.push_back(entry->Name());
        }
      }
      std::sort(interleaved_tables.begin(), interleaved_tables.end());
      std::sort(interleaved_indices.begin(), interleaved_indices.end());

      // Cannot drop a table with interleaved child tables or indexes.
      if (!interleaved_tables.empty()) {
        return error::DropTableWithInterleavedTables(
            name_, absl::StrJoin(interleaved_tables, ","));
      } else if (!interleaved_indices.empty()) {
        return error::DropTableWithDependentIndices(
            name_, absl::StrJoin(interleaved_indices, ","));
      }
    }
    if (!indexes_.empty()) {
      return error::DropTableWithDependentIndices(
          name_, absl::StrJoin(indexes_.begin(), indexes_.end(), ",",
                               [](std::string* out, const Index* child) {
                                 return out->append(child->Name());
                               }));
    }
    return zetasql_base::OkStatus();
  }

  const Table* orig_table = orig->As<const Table>();

  // Name and ID should not change during cloning.
  ZETASQL_RET_CHECK_EQ(Name(), orig_table->Name());
  ZETASQL_RET_CHECK_EQ(id(), orig_table->id());

  if (owner_index_) {
    ZETASQL_RET_CHECK(!owner_index_->is_deleted());
  }

  // Check additional constraints on new columns.
  for (const Column* column : columns()) {
    // Ignore old columns.
    if (orig_table->FindColumn(column->Name()) != nullptr) {
      continue;
    }

    // New columns cannot be nullable.
    if (!column->is_nullable()) {
      return error::AddingNotNullColumn(Name(), column->Name());
    }
  }

  // Cannot drop key columns, change their order or nullability.
  ZETASQL_RET_CHECK_EQ(primary_key_.size(), orig_table->primary_key_.size());
  for (int i = 0; i < primary_key_.size(); ++i) {
    if (primary_key_[i]->is_deleted()) {
      return error::InvalidDropKeyColumn(primary_key_[i]->column()->Name(),
                                         name_);
    }
    ZETASQL_RET_CHECK_EQ(primary_key_[i]->is_descending(),
                 orig_table->primary_key()[i]->is_descending());
    if (primary_key_[i]->column()->is_nullable() !=
        orig_table->primary_key()[i]->column()->is_nullable()) {
      std::string reason = absl::Substitute(
          "from $0 to $1",
          orig_table->primary_key()[i]->column()->is_nullable() ? "NULL"
                                                                : "NOT NULL",
          primary_key_[i]->column()->is_nullable() ? "NULL" : "NOT NULL");
      return error::CannotChangeKeyColumn(
          absl::StrCat(name_, ".", primary_key_[i]->column()->Name()), reason);
    }
  }

  return zetasql_base::OkStatus();
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
