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

#include "backend/schema/validators/foreign_key_validator.h"

#include <utility>

#include "absl/algorithm/container.h"
#include "backend/common/case.h"
#include "backend/common/ids.h"
#include "backend/datamodel/types.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
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

struct NameFormatter {
  void operator()(std::string* out, const ForeignKey* foreign_key) const {
    out->append("`").append(foreign_key->Name()).append("`");
  }
};

std::string Names(absl::Span<const ForeignKey* const> foreign_keys) {
  return absl::StrJoin(foreign_keys, ", ", NameFormatter());
}

std::string ColumnUses(const Column* column) {
  ColumnID column_id = column->id();
  auto column_matcher = [&column_id](const Column* candidate) {
    return candidate->id() == column_id;
  };
  std::vector<const ForeignKey*> foreign_keys;
  for (const auto& foreign_key_vector :
       {column->table()->foreign_keys(),
        column->table()->referencing_foreign_keys()}) {
    for (const ForeignKey* foreign_key : foreign_key_vector) {
      if (absl::c_find_if(foreign_key->referencing_columns(), column_matcher) !=
              foreign_key->referencing_columns().end() ||
          absl::c_find_if(foreign_key->referenced_columns(), column_matcher) !=
              foreign_key->referenced_columns().end()) {
        foreign_keys.push_back(foreign_key);
      }
    }
  }
  return Names(foreign_keys);
}

}  // namespace

absl::Status ForeignKeyValidator::Validate(const ForeignKey* foreign_key,
                                           SchemaValidationContext* context) {
  ZETASQL_RET_CHECK_NE(foreign_key->referencing_table_, nullptr);
  ZETASQL_RET_CHECK_NE(foreign_key->referenced_table_, nullptr);
  ZETASQL_RET_CHECK_EQ(
      foreign_key->referencing_table_->FindForeignKey(foreign_key->Name()),
      foreign_key);
  ZETASQL_RET_CHECK_EQ(foreign_key->referenced_table_->FindReferencingForeignKey(
                   foreign_key->Name()),
               foreign_key);
  ZETASQL_RET_CHECK_EQ(foreign_key->referencing_data_table(),
               foreign_key->referencing_index_ == nullptr
                   ? foreign_key->referencing_table_
                   : foreign_key->referencing_index_->index_data_table());
  ZETASQL_RET_CHECK_EQ(foreign_key->referenced_data_table(),
               foreign_key->referenced_index_ == nullptr
                   ? foreign_key->referenced_table_
                   : foreign_key->referenced_index_->index_data_table());

  std::string referencing_table_name = foreign_key->referencing_table_->Name();
  std::string referenced_table_name = foreign_key->referenced_table_->Name();
  const std::string& foreign_key_name = foreign_key->Name();
  ZETASQL_RETURN_IF_ERROR(GlobalSchemaNames::ValidateConstraintName(
      referencing_table_name, "Foreign Key", foreign_key_name));

  if (foreign_key->referencing_columns_.empty() ||
      foreign_key->referenced_columns_.empty()) {
    return error::ForeignKeyColumnsRequired(referencing_table_name,
                                            foreign_key_name);
  }
  if (foreign_key->referencing_columns_.size() !=
      foreign_key->referenced_columns_.size()) {
    return error::ForeignKeyColumnCountMismatch(
        referencing_table_name, referenced_table_name, foreign_key_name);
  }
  for (const auto& columns :
       {foreign_key->referencing_columns_, foreign_key->referenced_columns_}) {
    CaseInsensitiveStringSet names;
    for (const Column* column : columns) {
      if (!names.insert(column->Name()).second) {
        return error::ForeignKeyDuplicateColumn(
            column->Name(), column->table()->Name(), foreign_key_name);
      }
      if (!IsSupportedKeyColumnType(column->GetType())) {
        return error::ForeignKeyColumnTypeUnsupported(
            column->Name(), column->table()->Name(), foreign_key_name);
      }
      if (column->allows_commit_timestamp()) {
        return error::ForeignKeyCommitTimestampColumnUnsupported(
            column->Name(), column->table()->Name(), foreign_key_name);
      }
    }
  }
  for (int i = 0; i < foreign_key->referencing_columns_.size(); ++i) {
    const auto* referencing_column = foreign_key->referencing_columns_[i];
    const auto* referenced_column = foreign_key->referenced_columns_[i];
    if (!referencing_column->GetType()->Equals(referenced_column->GetType())) {
      return error::ForeignKeyColumnTypeMismatch(
          referencing_column->Name(), referencing_table_name,
          referenced_column->Name(), referenced_table_name, foreign_key_name);
    }
  }
  return absl::OkStatus();
}

absl::Status ForeignKeyValidator::ValidateUpdate(
    const ForeignKey* foreign_key, const ForeignKey* old_foreign_key,
    SchemaValidationContext* context) {
  // Check for deletion of the foreign key and its table.
  if (foreign_key->is_deleted()) {
    context->global_names()->RemoveName(foreign_key->Name());
    return absl::OkStatus();
  }
  // If the table was deleted, the foreign key should have been marked deleted.
  ZETASQL_RET_CHECK(!foreign_key->referencing_table_->is_deleted());

  // Foreign keys cannot be modified once created.
  ZETASQL_RET_CHECK_EQ(foreign_key->constraint_name_,
               old_foreign_key->constraint_name_);
  ZETASQL_RET_CHECK_EQ(foreign_key->generated_name_, old_foreign_key->generated_name_);
  ZETASQL_RET_CHECK_EQ(foreign_key->referencing_table_->Name(),
               old_foreign_key->referencing_table_->Name());
  ZETASQL_RET_CHECK_EQ(foreign_key->referenced_table_->Name(),
               old_foreign_key->referenced_table_->Name());

  // Foreign keys must be dropped before their referenced tables.
  if (foreign_key->referenced_table_->is_deleted()) {
    return error::ForeignKeyReferencedTableDropNotAllowed(
        foreign_key->referenced_table_->Name(),
        Names(foreign_key->referenced_table_->referencing_foreign_keys()));
  }

  // Validate each pair of referencing and referenced column updates.
  for (auto [columns, old_columns] :
       {std::make_pair(&foreign_key->referencing_columns_,
                       &old_foreign_key->referencing_columns_),
        std::make_pair(&foreign_key->referenced_columns_,
                       &old_foreign_key->referenced_columns_)}) {
    // The list of referencing and referenced columns cannot be modified.
    ZETASQL_RET_CHECK_EQ(columns->size(), old_columns->size());
    for (int i = 0; i < columns->size(); ++i) {
      const Column* column = columns->at(i);
      const Column* old_column = old_columns->at(i);
      // The order of columns cannot change.
      ZETASQL_RET_CHECK_EQ(column->Name(), old_column->Name());
      // Validate column updates.
      if (column->is_deleted()) {
        return error::ForeignKeyColumnDropNotAllowed(
            column->Name(), column->table()->Name(), ColumnUses(column));
      }
      if (column->GetType() != old_column->GetType()) {
        return error::ForeignKeyColumnTypeChangeNotAllowed(
            column->Name(), column->table()->Name(), ColumnUses(column));
      }
      if (column->is_nullable() != old_column->is_nullable()) {
        return error::ForeignKeyColumnNullabilityChangeNotAllowed(
            column->Name(), column->table()->Name(), ColumnUses(column));
      }
      if (column->allows_commit_timestamp()) {
        return error::ForeignKeyColumnSetCommitTimestampOptionNotAllowed(
            column->Name(), column->table()->Name(), ColumnUses(column));
      }
    }
  }
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
