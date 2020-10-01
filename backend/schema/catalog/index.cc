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
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/substitute.h"
#include "backend/datamodel/types.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
#include "common/errors.h"
#include "absl/status/status.h"
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
  absl::StrAppend(&result, "Managed: ", (is_managed() ? "true" : "false"),
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

absl::Status Index::Validate(SchemaValidationContext* context) const {
  return validate_(this, context);
}

absl::Status Index::ValidateUpdate(const SchemaNode* old,
                                   SchemaValidationContext* context) const {
  return validate_update_(this, old->template As<const Index>(), context);
}

absl::Status Index::DeepClone(SchemaGraphEditor* editor,
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

  if (!managing_nodes_.empty()) {
    ZETASQL_RETURN_IF_ERROR(editor->CloneVector(&managing_nodes_));
    if (managing_nodes_.empty()) {
      MarkDeleted();
    }
  }

  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
