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

#include "backend/schema/catalog/index.h"

#include "absl/memory/memory.h"
#include "zetasql/base/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/substitute.h"
#include "backend/datamodel/types.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
#include "common/errors.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

std::string Index::FullDebugString() const {
  std::string result;
  absl::StrAppend(&result, "Index ", name_, " on table ",
                  indexed_table_->Name(), "\n");
  if (parent() != nullptr) {
    absl::StrAppend(&result, "Interleaved in ", parent()->Name(), "\n");
    absl::StrAppend(&result, "Parent Table Primary Key: ",
                    parent()->PrimaryKeyDebugString(), "\n");
  }
  absl::StrAppend(&result, "Indexed Table Primary Key: ",
                  indexed_table_->PrimaryKeyDebugString(), "\n");
  absl::StrAppend(&result, "Index Data Table Primary Key: ",
                  index_data_table_->PrimaryKeyDebugString(), "\n");
  absl::StrAppend(&result, "Unique: ", (is_unique_ ? "true" : "false"), "\n");
  absl::StrAppend(&result,
                  "Null Filtering: ", (is_null_filtered_ ? "true" : "false"),
                  "\n");
  absl::StrAppend(&result, "Column :: Source Column :\n");
  for (const Column* column : index_data_table_->columns()) {
    absl::StrAppend(&result, column->Name(), " :: ",
                    ((column->source_column() == nullptr)
                         ? ""
                         : column->source_column()->Name()),
                    "\n");
  }
  return result;
}

std::string Index::DebugString() const {
  return absl::Substitute("I:$0[$1]", name_, index_data_table_->Name());
}

const Table* Index::parent() const { return index_data_table_->parent(); }

zetasql_base::Status Index::Validate(SchemaValidationContext* context) const {
  ZETASQL_RET_CHECK(!name_.empty());
  ZETASQL_RET_CHECK_NE(indexed_table_, nullptr);
  ZETASQL_RET_CHECK_NE(index_data_table_, nullptr);

  if (name_.length() > limits::kMaxSchemaIdentifierLength) {
    return error::InvalidSchemaName("Index", name_,
                                    "name exceeds maximum allowed "
                                    "identifier length");
  }

  if (zetasql_base::CaseEqual(name_, "PRIMARY_KEY")) {
    return error::CannotNameIndexPrimaryKey();
  }

  if (absl::StartsWith(name_, "Dir_")) {
    return error::InvalidSchemaName("Index", name_);
  }

  // copybara_strip:begin(internal-comment)
  //   TODO : This should ideally be fixed in the parser.
  //   The parser should not allow this.
  // copybara_strip:end
  if (key_columns_.empty()) {
    return error::IndexWithNoKeys(name_);
  }

  CaseInsensitiveStringSet keys_set;
  for (const auto* key_column : key_columns_) {
    const std::string& column_name = key_column->column()->Name();
    const auto* column_type = key_column->column()->GetType();
    if (!IsSupportedKeyColumnType(column_type)) {
      return error::IndexRefsUnsupportedColumn(name_, ToString(column_type));
    }
    if (keys_set.contains(column_name)) {
      return error::IndexRefsColumnTwice(name_, column_name);
    }
    if (is_null_filtered_) {
      ZETASQL_RET_CHECK(!key_column->column()->is_nullable());
    }
    keys_set.insert(column_name);
  }

  CaseInsensitiveStringSet stored_set;
  for (const auto* column : stored_columns_) {
    const std::string& column_name = column->Name();
    if (keys_set.contains(column_name)) {
      return error::IndexRefsKeyAsStoredColumn(name_, column_name);
    }
    if (indexed_table_->FindKeyColumn(column_name) != nullptr) {
      return error::IndexRefsTableKeyAsStoredColumn(name_, column_name,
                                                    indexed_table_->Name());
    }
    if (stored_set.contains(column_name)) {
      return error::IndexRefsColumnTwice(name_, column_name);
    }
    stored_set.insert(column_name);
  }

  if (parent()) {
    const Table* table = indexed_table_;
    while (table != parent() && table->parent()) {
      table = table->parent();
    }
    if (table != parent()) {
      return error::IndexInterleaveTableUnacceptable(
          name_, indexed_table_->Name(), parent()->Name());
    }
  }

  return zetasql_base::OkStatus();
}

zetasql_base::Status Index::ValidateUpdate(const SchemaNode* old,
                                   SchemaValidationContext* context) const {
  if (is_deleted()) {
    ZETASQL_RET_CHECK(index_data_table_->is_deleted());
    return zetasql_base::OkStatus();
  }

  ZETASQL_RET_CHECK(!index_data_table()->is_deleted());

  const Index* old_index = old->As<const Index>();
  ZETASQL_RET_CHECK_EQ(name_, old_index->name_);
  ZETASQL_RET_CHECK_EQ(is_null_filtered_, old_index->is_null_filtered_);
  ZETASQL_RET_CHECK_EQ(is_unique_, old_index->is_unique_);
  ZETASQL_RET_CHECK_EQ(key_columns_.size(), old_index->key_columns_.size());
  ZETASQL_RET_CHECK_EQ(stored_columns_.size(), old_index->stored_columns_.size());

  for (int i = 0; i < key_columns_.size(); ++i) {
    const KeyColumn* new_key = key_columns_[i];
    const KeyColumn* old_key = old_index->key_columns_[i];
    if (is_null_filtered_) {
      // For null-filtered indexes, we need not check for nullability changes
      // of the source column
      ZETASQL_RET_CHECK(!new_key->column()->is_nullable());
    } else {
      // Cannot change nullability of key columns for non-null filtered
      // indexes.
      if (old_key->column()->is_nullable() !=
          new_key->column()->is_nullable()) {
        return error::ChangingNullConstraintOnIndexedColumn(
            new_key->column()->Name(), name_);
      }
    }
  }

  return zetasql_base::OkStatus();
}

zetasql_base::Status Index::DeepClone(SchemaGraphEditor* editor,
                              const SchemaNode* orig) {
  ZETASQL_ASSIGN_OR_RETURN(const auto* indexed_table, editor->Clone(indexed_table_));
  indexed_table_ = indexed_table->As<const Table>();

  ZETASQL_ASSIGN_OR_RETURN(const auto* index_data_table,
                   editor->Clone(index_data_table_));
  index_data_table_ = index_data_table->As<const Table>();

  for (const KeyColumn*& key_column : key_columns_) {
    ZETASQL_ASSIGN_OR_RETURN(const auto* schema_node, editor->Clone(key_column));
    key_column = schema_node->As<const KeyColumn>();
  }

  for (const Column*& stored_column : stored_columns_) {
    ZETASQL_ASSIGN_OR_RETURN(const auto* schema_node, editor->Clone(stored_column));
    stored_column = schema_node->As<const Column>();
  }

  return zetasql_base::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
