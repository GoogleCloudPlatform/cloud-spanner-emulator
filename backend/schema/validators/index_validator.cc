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

#include "backend/schema/validators/index_validator.h"

#include <cstdint>
#include <string>
#include <vector>

#include "zetasql/public/types/type.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "backend/common/case.h"
#include "backend/datamodel/types.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/updater/global_schema_names.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "common/errors.h"
#include "common/feature_flags.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

absl::Status GetDropManagedIndexError(
    absl::string_view index_name,
    absl::Span<const SchemaNode* const> managing_nodes) {
  std::vector<std::string> foreign_key_names;
  std::vector<std::string> other_names;
  for (const SchemaNode* node : managing_nodes) {
    auto name_info = node->GetSchemaNameInfo();
    if (name_info.has_value()) {
      if (name_info->kind == "Foreign Key") {
        foreign_key_names.push_back(absl::StrCat("`", name_info->name, "`"));
      } else {
        other_names.push_back(
            absl::StrCat("`", name_info->name, "` (", name_info->kind, ")"));
      }
    } else {
      other_names.push_back("(no schema name info available)");
    }
  }
  if (!foreign_key_names.empty()) {
    return error::DropForeignKeyManagedIndex(
        index_name, absl::StrJoin(foreign_key_names, ", "));
  }
  ZETASQL_RET_CHECK_FAIL() << "Unknown type of managed index: index name=`"
                   << index_name << "` managing nodes=["
                   << absl::StrJoin(other_names, ", ") << "]";
}

}  // namespace

absl::Status IndexValidator::Validate(const Index* index,
                                      SchemaValidationContext* context) {
  ZETASQL_RET_CHECK(!index->name_.empty());
  ZETASQL_RET_CHECK_NE(index->indexed_table_, nullptr);
  ZETASQL_RET_CHECK_NE(index->index_data_table_, nullptr);
  if (context->is_postgresql_dialect()) {
    ZETASQL_RET_CHECK(index->postgresql_oid().has_value());
  } else {
    ZETASQL_RET_CHECK(!index->postgresql_oid().has_value());
  }

  ZETASQL_RETURN_IF_ERROR(GlobalSchemaNames::ValidateSchemaName("Index", index->name_));
  if (!index->is_managed() &&
      !SDLObjectName::InSameSchema(index->name_,
                                   index->indexed_table_->Name())) {
    return error::IndexInDifferentSchema(index->name_,
                                         index->indexed_table_->Name());
  }
  if (absl::EqualsIgnoreCase(index->name_, "PRIMARY_KEY")) {
    return error::CannotNameIndexPrimaryKey();
  }
  if (absl::StartsWith(index->name_, "Dir_")) {
    return error::InvalidSchemaName("Index", index->name_);
  }

  if (index->key_columns_.empty()) {
    return error::IndexWithNoKeys(index->name_);
  }

  CaseInsensitiveStringSet keys_set;
  for (const auto* key_column : index->key_columns_) {
    std::string column_name = key_column->column()->Name();
    if (keys_set.contains(column_name)) {
      return error::IndexRefsColumnTwice(index->name_, column_name);
    }

    const auto* column_type = key_column->column()->GetType();
    if (index->is_search_index() && column_type->IsTokenListType()) {
      keys_set.insert(column_name);
      continue;
    }
    if (!IsSupportedKeyColumnType(column_type, index->is_vector_index())) {
      return error::IndexRefsUnsupportedColumn(index->name_,
                                               ToString(column_type));
    }
    if (index->is_null_filtered_) {
      ZETASQL_RET_CHECK(!key_column->column()->is_nullable());
    }
    keys_set.insert(column_name);
  }

  CaseInsensitiveStringSet stored_set;
  for (const auto* column : index->stored_columns_) {
    std::string column_name = column->Name();
    if (keys_set.contains(column_name)) {
      return error::IndexRefsKeyAsStoredColumn(index->name_, column_name);
    }
    if (index->indexed_table_->FindKeyColumn(column_name) != nullptr) {
      return error::IndexRefsTableKeyAsStoredColumn(
          index->name_, column_name, index->indexed_table_->Name());
    }
    if (stored_set.contains(column_name)) {
      return error::IndexRefsColumnTwice(index->name_, column_name);
    }
    stored_set.insert(column_name);
  }

  if (index->parent()) {
    const Table* table = index->indexed_table_;
    while (table != index->parent() && table->parent()) {
      table = table->parent();
    }
    if (table != index->parent() &&
        !EmulatorFeatureFlags::instance().flags().enable_interleave_in) {
      return error::IndexInterleaveTableUnacceptable(
          index->name_, index->indexed_table_->Name(), index->parent()->Name());
    }
  }

  if (index->is_search_index()) {
    // check partition by restrictions
    for (const auto* column : index->partition_by_) {
      const auto* column_type = column->GetType();
      if (column_type->IsTokenListType()) {
        return error::SearchIndexNotPartitionByokenListType(index->name_,
                                                            column->Name());
      }
    }

    // check order by restrictions
    for (const auto* key_column : index->order_by_) {
      auto column = key_column->column();
      if (column->is_nullable() && !index->is_null_filtered_column(column)) {
        return error::SearchIndexSortMustBeNotNullError(column->Name(),
                                                        index->name_);
      }

      const auto* column_type = column->GetType();
      if (!column_type->IsInteger()) {
        return error::SearchIndexOrderByMustBeIntegerType(
            index->name_, column->Name(), ToString(column_type));
      }
    }
  }

  if (index->is_vector_index()) {
    if (!index->partition_by_.empty()) {
      return error::VectorIndexPartitionByUnsupported(index->name_);
    }

    ZETASQL_RET_CHECK(index->key_columns_.size() == 1);
    const KeyColumn* key_column = index->key_columns_[0];
    if (!key_column->column()->GetType()->IsArray()) {
      return error::VectorIndexNonArrayKey(key_column->column()->Name(),
                                           index->name_);
    }

    const zetasql::Type* element_type =
        key_column->column()->GetType()->AsArray()->element_type();
    if (!element_type->IsFloat() && !element_type->IsDouble()) {
      return error::VectorIndexArrayKeyMustBeFloatOrDouble(
          key_column->column()->Name(), index->name_);
    }

    if (!key_column->column()->has_vector_length()) {
      return error::VectorIndexArrayKeyMustHaveVectorLength(
          key_column->column()->Name(), index->name_);
    }

    const int32_t max_vector_length = 8000;
    if (key_column->column()->vector_length() > max_vector_length) {
      return error::VectorIndexArrayKeyVectorLengthTooLarge(
          key_column->column()->Name(), index->name_,
          key_column->column()->vector_length().value(), max_vector_length);
    }

    bool is_key_null_filtered = false;
    for (const auto* column : index->null_filtered_columns_) {
      if (key_column->column()->Name() == column->Name()) {
        is_key_null_filtered = true;
        break;
      }
    }
    if (key_column->column()->is_nullable() && !is_key_null_filtered) {
      return error::VectorIndexKeyNotNullFiltered(key_column->column()->Name(),
                                                  index->name_);
    }
  }
  return absl::OkStatus();
}

absl::Status IndexValidator::ValidateUpdate(const Index* index,
                                            const Index* old_index,
                                            SchemaValidationContext* context) {
  if (index->is_deleted()) {
    if (!index->managing_nodes_.empty()) {
      return GetDropManagedIndexError(index->name_, index->managing_nodes_);
    }
    ZETASQL_RET_CHECK(index->index_data_table_->is_deleted());
    context->global_names()->RemoveName(index->Name());
    return absl::OkStatus();
  }

  ZETASQL_RET_CHECK(!index->index_data_table()->is_deleted());
  for (const SchemaNode* managing_node : index->managing_nodes_) {
    ZETASQL_RET_CHECK(!managing_node->is_deleted());
  }

  ZETASQL_RET_CHECK_EQ(index->name_, old_index->name_);
  ZETASQL_RET_CHECK_EQ(index->is_null_filtered_, old_index->is_null_filtered_);
  ZETASQL_RET_CHECK_EQ(index->is_unique_, old_index->is_unique_);
  ZETASQL_RET_CHECK_EQ(index->key_columns_.size(), old_index->key_columns_.size());

  if (context->is_postgresql_dialect()) {
    ZETASQL_RET_CHECK(index->postgresql_oid().has_value());
    ZETASQL_RET_CHECK(old_index->postgresql_oid().has_value());
    ZETASQL_RET_CHECK_EQ(index->postgresql_oid().value(),
                 old_index->postgresql_oid().value());
  } else {
    ZETASQL_RET_CHECK(!index->postgresql_oid().has_value());
    ZETASQL_RET_CHECK(!old_index->postgresql_oid().has_value());
  }

  for (int i = 0; i < index->key_columns_.size(); ++i) {
    const KeyColumn* new_key = index->key_columns_[i];
    const KeyColumn* old_key = old_index->key_columns_[i];
    if (index->is_null_filtered_) {
      // For null-filtered indexes, we need not check for nullability changes
      // of the source column
      ZETASQL_RET_CHECK(!new_key->column()->is_nullable());
    } else {
      // Cannot change nullability of key columns for non-null filtered
      // indexes.
      if (old_key->column()->is_nullable() !=
          new_key->column()->is_nullable()) {
        return error::ChangingNullConstraintOnIndexedColumn(
            new_key->column()->Name(), index->name_);
      }
    }
  }

  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
