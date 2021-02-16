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

#include "backend/schema/validators/table_validator.h"

#include "zetasql/public/type.h"
#include "absl/container/flat_hash_map.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/substitute.h"
#include "backend/common/case.h"
#include "backend/common/graph_dependency_helper.h"
#include "backend/datamodel/types.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/foreign_key.h"
#include "backend/schema/updater/global_schema_names.h"
#include "common/errors.h"
#include "common/limits.h"
#include "zetasql/base/ret_check.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

absl::Status CheckKeyPartCompatibility(const Table* interleaved_table,
                                       const KeyColumn* parent_key,
                                       const KeyColumn* child_key,
                                       bool ignore_nullability) {
  const std::string object_type = OwningObjectType(interleaved_table);
  const std::string object_name = OwningObjectName(interleaved_table);
  const Column* parent_key_col = parent_key->column();
  const Column* child_key_col = child_key->column();

  if (!absl::EqualsIgnoreCase(child_key_col->Name(), parent_key_col->Name())) {
    // The parent key column does not match the child key column. But perhaps
    // the child declared the key column in a different position. Provide a
    // more helpful error message in this case (as they do refer to the parent
    // column, just at the wrong location).
    auto child_pk = interleaved_table->owner_index()
                        ? interleaved_table->owner_index()->key_columns()
                        : interleaved_table->primary_key();
    for (int i = 0; i < child_pk.size(); ++i) {
      if (absl::EqualsIgnoreCase(child_pk[i]->column()->Name(),
                                 parent_key_col->Name())) {
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
    return absl::OkStatus();
  }

  // Parent and child key column nullability should match.
  if (child_key_col->is_nullable() != parent_key_col->is_nullable()) {
    return error::IncorrectParentKeyNullability(
        object_type, object_name, parent_key_col->Name(),
        parent_key_col->is_nullable() ? "nullable" : "not null",
        child_key_col->is_nullable() ? "nullable" : "not null");
  }

  return absl::Status();
}

absl::Status CheckInterleaveDepthLimit(const Table* table) {
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
  return absl::OkStatus();
}

absl::string_view GetColumnName(const Column* const& column) {
  return column->Name();
}

}  // namespace

absl::Status TableValidator::Validate(const Table* table,
                                      SchemaValidationContext* context) {
  ZETASQL_RET_CHECK(!table->name_.empty());
  ZETASQL_RET_CHECK(!table->id_.empty());

  if (table->is_public()) {
    ZETASQL_RETURN_IF_ERROR(
        GlobalSchemaNames::ValidateSchemaName("Table", table->name_));
  }

  const std::string object_type = OwningObjectType(table);
  const std::string object_name = OwningObjectName(table);

  // Validate that all columns are unique.
  CaseInsensitiveStringSet unique_columns;
  for (const Column* column : table->columns_) {
    ZETASQL_RET_CHECK_NE(column, nullptr);
    std::string column_name = column->Name();
    ZETASQL_RET_CHECK_EQ(column->table(), table);
    if (unique_columns.contains(column_name)) {
      return error::DuplicateColumnName(column->FullName());
    }
    unique_columns.insert(column_name);
  }

  if (table->columns_.size() > limits::kMaxColumnsPerTable) {
    return error::TooManyColumns(object_type, object_name,
                                 limits::kMaxColumnsPerTable);
  }

  // Validate that all key columns are unique.
  CaseInsensitiveStringSet unique_keys;
  for (const KeyColumn* key_column : table->primary_key_) {
    ZETASQL_RET_CHECK_NE(key_column, nullptr);
    const Column* column = key_column->column();
    ZETASQL_RET_CHECK_NE(column, nullptr);
    const Column* table_column = table->FindColumn(column->Name());
    ZETASQL_RET_CHECK_EQ(table_column, column);
    if (unique_keys.contains(column->Name())) {
      return error::MultipleRefsToKeyColumn(object_type, object_name,
                                            column->Name());
    }
    unique_keys.insert(column->Name());
  }

  if (table->primary_key_.size() > limits::kMaxKeyColumns) {
    return error::TooManyKeys(object_type, object_name,
                              table->primary_key_.size(),
                              limits::kMaxKeyColumns);
  }

  if (!table->indexes_.empty()) {
    ZETASQL_RET_CHECK(!table->columns_.empty());
    ZETASQL_RET_CHECK(!table->primary_key_.empty());
  }

  for (const Index* index : table->indexes_) {
    ZETASQL_RET_CHECK_NE(index, nullptr);
    ZETASQL_RET_CHECK_EQ(index->indexed_table(), table);
  }

  if (table->indexes_.size() > limits::kMaxIndexesPerTable) {
    const Index* last_index = table->indexes_[limits::kMaxIndexesPerTable];
    return error::TooManyIndicesPerTable(last_index->Name(), table->Name(),
                                         limits::kMaxIndexesPerTable);
  }

  // Check interleave compatibility.
  if (!table->parent_table_) {
    if (table->on_delete_action_.has_value()) {
      return error::SetOnDeleteWithoutInterleaving(table->Name());
    }
  } else {
    bool ignore_nullability = table->owner_index() != nullptr &&
                              table->owner_index()->is_null_filtered();
    ZETASQL_RET_CHECK(table->parent_table_->is_public());
    auto parent_pk = table->parent_table_->primary_key();
    for (int i = 0; i < parent_pk.size(); ++i) {
      // The child has fewer primary key parts than the parent.
      if (i >= table->primary_key_.size()) {
        return error::MustReferenceParentKeyColumn(
            OwningObjectType(table), OwningObjectName(table),
            parent_pk[i]->column()->Name());
      }

      ZETASQL_RETURN_IF_ERROR(CheckKeyPartCompatibility(
          table, parent_pk[i], table->primary_key_[i], ignore_nullability));
    }
    ZETASQL_RETURN_IF_ERROR(CheckInterleaveDepthLimit(table));

    // Cannot add a table with no columns as a child.
    if (table->columns_.empty()) {
      return error::NoColumnsTable(OwningObjectType(table),
                                   OwningObjectName(table));
    }
  }

  for (const Table* child : table->child_tables_) {
    ZETASQL_RET_CHECK_NE(child, nullptr);
    ZETASQL_RET_CHECK_EQ(child->parent(), table);
  }

  for (const ForeignKey* foreign_key : table->foreign_keys_) {
    ZETASQL_RET_CHECK_NE(foreign_key, nullptr);
    ZETASQL_RET_CHECK_EQ(foreign_key->referencing_table(), table);
  }
  for (const ForeignKey* referencing_foreign_key :
       table->referencing_foreign_keys_) {
    ZETASQL_RET_CHECK_NE(referencing_foreign_key, nullptr);
    ZETASQL_RET_CHECK_EQ(referencing_foreign_key->referenced_table(), table);
  }

  if (table->owner_index_) {
    ZETASQL_RET_CHECK_EQ(table->indexes_.size(), 0);
    ZETASQL_RET_CHECK_EQ(table->child_tables_.size(), 0);
    ZETASQL_RET_CHECK(!table->columns_.empty());
    ZETASQL_RET_CHECK(!table->primary_key_.empty());
    ZETASQL_RET_CHECK_EQ(table->owner_index_->index_data_table(), table);
  }

  // Validate generated columns.
  GraphDependencyHelper<const Column*, GetColumnName> cycle_detector(
      /*object_type=*/"generated column");
  for (const Column* column : table->columns()) {
    ZETASQL_RETURN_IF_ERROR(cycle_detector.AddNodeIfNotExists(column));
  }
  for (const Column* column : table->columns()) {
    if (column->is_generated()) {
      for (const Column* dep : column->dependent_columns()) {
        ZETASQL_RETURN_IF_ERROR(
            cycle_detector.AddEdgeIfNotExists(column->Name(), dep->Name()));
      }
    }
  }
  ZETASQL_RETURN_IF_ERROR(cycle_detector.DetectCycle());

  return absl::OkStatus();
}

absl::Status TableValidator::ValidateUpdate(const Table* table,
                                            const Table* old_table,
                                            SchemaValidationContext* context) {
  if (table->is_deleted()) {
    ZETASQL_RET_CHECK(!table->owner_index_ || table->owner_index_->is_deleted());
    if (!table->child_tables_.empty()) {
      // Build a sorted list of interleaved child tables and indexes.
      std::vector<std::string> interleaved_tables;
      std::vector<std::string> interleaved_indices;
      for (const auto& entry : table->child_tables_) {
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
            table->name_, absl::StrJoin(interleaved_tables, ","));
      } else if (!interleaved_indices.empty()) {
        return error::DropTableWithDependentIndices(
            table->name_, absl::StrJoin(interleaved_indices, ","));
      }
    }
    if (!table->indexes_.empty()) {
      return error::DropTableWithDependentIndices(
          table->name_,
          absl::StrJoin(table->indexes_.begin(), table->indexes_.end(), ",",
                        [](std::string* out, const Index* child) {
                          return out->append(child->Name());
                        }));
    }
    context->global_names()->RemoveName(table->Name());
    return absl::OkStatus();
  }

  // Name and ID should not change during cloning.
  ZETASQL_RET_CHECK_EQ(table->Name(), old_table->Name());
  ZETASQL_RET_CHECK_EQ(table->id(), old_table->id());

  if (table->owner_index_) {
    ZETASQL_RET_CHECK(!table->owner_index_->is_deleted());
  }

  // Check additional constraints on new columns.
  for (const Column* column : table->columns()) {
    // Ignore old columns.
    if (old_table->FindColumn(column->Name()) != nullptr) {
      continue;
    }

    // New columns cannot be nullable unless it is a generated column.
    if (!column->is_nullable() && !column->is_generated()) {
      return error::AddingNotNullColumn(table->name_, column->Name());
    }
  }

  // Cannot drop key columns, change their order or nullability.
  ZETASQL_RET_CHECK_EQ(table->primary_key_.size(), old_table->primary_key_.size());
  for (int i = 0; i < table->primary_key_.size(); ++i) {
    if (table->primary_key_[i]->is_deleted()) {
      return error::InvalidDropKeyColumn(
          table->primary_key_[i]->column()->Name(), table->name_);
    }
    ZETASQL_RET_CHECK_EQ(table->primary_key_[i]->is_descending(),
                 old_table->primary_key()[i]->is_descending());
    if (table->primary_key_[i]->column()->is_nullable() !=
        old_table->primary_key()[i]->column()->is_nullable()) {
      std::string reason = absl::Substitute(
          "from $0 to $1",
          old_table->primary_key()[i]->column()->is_nullable() ? "NULL"
                                                               : "NOT NULL",
          table->primary_key_[i]->column()->is_nullable() ? "NULL"
                                                          : "NOT NULL");
      return error::CannotChangeKeyColumn(
          absl::StrCat(table->name_, ".",
                       table->primary_key_[i]->column()->Name()),
          reason);
    }
  }

  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
