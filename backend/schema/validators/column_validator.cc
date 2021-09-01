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

#include "backend/schema/validators/column_validator.h"

#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.pb.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/substitute.h"
#include "backend/datamodel/types.h"
#include "backend/schema/backfills/column_value_backfill.h"
#include "backend/schema/catalog/check_constraint.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "backend/schema/verifiers/column_value_verifiers.h"
#include "common/errors.h"
#include "common/limits.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

bool IsResizeable(const zetasql::Type* type) {
  if (type->IsString() || type->IsBytes()) {
    return true;
  }
  return false;
}

bool IsAllowedTypeChange(const zetasql::Type* old_column_type,
                         const zetasql::Type* new_column_type) {
  if (old_column_type->Equals(new_column_type)) {
    return true;
  }

  if (old_column_type->IsArray() != new_column_type->IsArray()) {
    return false;
  }

  if (old_column_type->IsArray()) {
    return IsAllowedTypeChange(BaseType(old_column_type),
                               BaseType(new_column_type));
  }

  // Only allow conversions from BYTES to STRING and STRING to BYTES.
  return (new_column_type->IsString() && old_column_type->IsBytes()) ||
         (new_column_type->IsBytes() && old_column_type->IsString());
}

// Validates size reductions and column type changes.
absl::Status CheckAllowedColumnTypeChange(
    const Column* old_column, const Column* new_column,
    const zetasql::Type* old_column_type,
    const zetasql::Type* new_column_type, SchemaValidationContext* context) {
  if (!IsAllowedTypeChange(old_column_type, new_column_type)) {
    return error::CannotChangeColumnType(new_column->Name(),
                                         ToString(old_column_type),
                                         ToString(new_column_type));
  }

  const auto* old_base_type = BaseType(old_column_type);
  const auto* new_base_type = BaseType(new_column_type);
  if (new_base_type->Equals(old_base_type)) {
    if (IsResizeable(old_base_type) && new_column->effective_max_length() <
                                           old_column->effective_max_length()) {
      context->AddAction([old_column,
                          new_column](const SchemaValidationContext* context) {
        return VerifyColumnLength(old_column->table(), old_column,
                                  new_column->effective_max_length(), context);
      });
    }
  } else {
    context->AddAction(
        [old_column, new_column](const SchemaValidationContext* context) {
          return VerifyColumnTypeChange(old_column->table(), old_column,
                                        new_column, context);
        });
    // After verifying that the type change is acceptable, run a backfill
    // to apply the type change to the column values in storage.
    context->AddAction(
        [old_column, new_column](const SchemaValidationContext* context) {
          return BackfillColumnValue(old_column, new_column, context);
        });
  }
  return absl::OkStatus();
}

}  // namespace

absl::Status ColumnValidator::Validate(const Column* column,
                                       SchemaValidationContext* context) {
  ZETASQL_RET_CHECK_NE(column->table_, nullptr);
  ZETASQL_RET_CHECK(!column->name_.empty());
  ZETASQL_RET_CHECK(!column->id_.empty());
  ZETASQL_RET_CHECK(column->type_ != nullptr && IsSupportedColumnType(column->type_));
  ZETASQL_RET_CHECK(!column->declared_max_length_.has_value() ||
            (BaseType(column->type_)->IsString() ||
             BaseType(column->type_)->IsBytes()));

  if (column->name_.length() > limits::kMaxSchemaIdentifierLength) {
    return error::InvalidSchemaName("Column", column->Name());
  }

  if (column->source_column_) {
    ZETASQL_RET_CHECK(column->type_->Equals(column->source_column_->type_));
    ZETASQL_RET_CHECK(column->declared_max_length_ ==
              column->source_column_->declared_max_length_);
  }

  if (column->declared_max_length_.has_value()) {
    const zetasql::Type* base_type = BaseType(column->type_);
    if (base_type->IsString() && (column->declared_max_length_.value() == 0 ||
                                  column->declared_max_length_.value() >
                                      limits::kMaxStringColumnLength)) {
      return error::InvalidColumnLength(column->FullName(),
                                        column->declared_max_length_.value(), 1,
                                        limits::kMaxStringColumnLength);
    }
    if (base_type->IsBytes() && (column->declared_max_length_.value() == 0 ||
                                 column->declared_max_length_.value() >
                                     limits::kMaxBytesColumnLength)) {
      return error::InvalidColumnLength(column->FullName(),
                                        column->declared_max_length_.value(), 1,
                                        limits::kMaxBytesColumnLength);
    }
  }

  if (column->has_allows_commit_timestamp() && !column->type_->IsTimestamp()) {
    return error::UnallowedCommitTimestampOption(column->FullName());
  }

  if (column->is_generated()) {
    if (column->table()->FindKeyColumn(column->Name())) {
      return error::CannotUseGeneratedColumnInPrimaryKey(
          column->table()->Name(), column->Name());
    }
    for (const Column* dep : column->dependent_columns()) {
      if (dep->allows_commit_timestamp()) {
        return error::CannotUseCommitTimestampOnGeneratedColumnDependency(
            dep->Name());
      }
    }
  }

  return absl::OkStatus();
}

absl::Status ColumnValidator::ValidateUpdate(const Column* column,
                                             const Column* old_column,
                                             SchemaValidationContext* context) {
  if (column->is_deleted()) {
    return absl::OkStatus();
  }

  // Once set, column ID should never change.
  ZETASQL_RET_CHECK_EQ(column->id(), old_column->id());

  // For a non-deleted column, the objects it depends on should
  // also be alive.
  ZETASQL_RET_CHECK(!column->table_->is_deleted());
  // It is invalid to drop a column which is referenced by a generated column.
  for (const Column* dep : column->dependent_columns()) {
    if (dep->is_deleted()) {
      return error::InvalidDropColumnReferencedByGeneratedColumn(
          dep->Name(), column->table()->Name(), column->Name());
    }
  }
  if (column->is_generated() && !old_column->is_generated()) {
    return error::CannotConvertRegularColumnToGeneratedColumn(
        column->table()->Name(), column->Name());
  }
  if (!column->is_generated() && old_column->is_generated()) {
    return error::CannotConvertGeneratedColumnToRegularColumn(
        column->table()->Name(), column->Name());
  }
  if (column->is_generated() && old_column->is_generated()) {
    if (!column->GetType()->Equals(old_column->GetType())) {
      return error::CannotAlterStoredGeneratedColumnDataType(
          column->table()->Name(), column->Name());
    }
    if (column->expression().value() != old_column->expression().value()) {
      return error::CannotAlterGeneratedColumnExpression(
          column->table()->Name(), column->Name());
    }
  }
  if (!column->GetType()->Equals(old_column->GetType())) {
    for (const Column* generated_column : column->table()->columns()) {
      if (generated_column->is_generated()) {
        for (const Column* dep : generated_column->dependent_columns()) {
          if (column == dep) {
            return error::
                CannotAlterColumnDataTypeWithDependentStoredGeneratedColumn(
                    column->Name());
          }
        }
      }
    }
  }

  if (column->source_column_) {
    // There is no valid scenario under which a source column drop should
    // trigger a cascading drop on referencing column.
    if (column->source_column_->is_deleted()) {
      ZETASQL_RET_CHECK_NE(column->table_->owner_index(), nullptr);
      return error::InvalidDropColumnWithDependency(
          column->name_, column->table_->owner_index()->indexed_table()->Name(),
          column->table_->owner_index()->Name());
    }
  }

  if (old_column->is_nullable_ && !column->is_nullable_) {
    context->AddAction([old_column](const SchemaValidationContext* context) {
      return VerifyColumnNotNull(old_column->table(), old_column, context);
    });
  }

  // Check for size reduction and type change.
  ZETASQL_RETURN_IF_ERROR(CheckAllowedColumnTypeChange(
      old_column, column, old_column->GetType(), column->type_, context));

  if (column->type_->IsTimestamp()) {
    if (column->allows_commit_timestamp() &&
        !old_column->allows_commit_timestamp()) {
      context->AddAction([column](const SchemaValidationContext* context) {
        return VerifyColumnCommitTimestamp(column->table_, column, context);
      });
    }
  }

  return absl::OkStatus();
}

absl::Status KeyColumnValidator::Validate(const KeyColumn* key_column,
                                          SchemaValidationContext* context) {
  ZETASQL_RET_CHECK_NE(key_column->column_, nullptr);
  const std::string type_name =
      key_column->column_->GetType()->IsArray()
          ? "ARRAY"
          : key_column->column_->GetType()->ShortTypeName(
                zetasql::PRODUCT_EXTERNAL);
  if (!IsSupportedKeyColumnType(key_column->column_->GetType())) {
    if (key_column->column()->table()->owner_index()) {
      return error::CannotCreateIndexOnColumn(
          key_column->column()->table()->owner_index()->Name(),
          key_column->column()->Name(), type_name);
    }
    return error::InvalidPrimaryKeyColumnType(key_column->column_->FullName(),
                                              type_name);
  }
  return absl::OkStatus();
}

absl::Status KeyColumnValidator::ValidateUpdate(
    const KeyColumn* key_column, const KeyColumn* old_key_column,
    SchemaValidationContext* context) {
  if (key_column->is_deleted()) {
    return absl::OkStatus();
  }

  const auto* column = key_column->column_;
  // If the underlying column of the key column has been altered,
  // reject the update if the column is also a parent key column,
  // unless it is a timestamp typed column and the update involves
  // chaging the allow timestamp option.
  if (context->IsModifiedNode(column)) {
    bool is_commit_timestamp_option_change =
        column->allows_commit_timestamp() !=
        old_key_column->column_->allows_commit_timestamp();
    if (!is_commit_timestamp_option_change) {
      // If the key column is a child table column.
      const auto* table_parent = column->table()->parent();
      if (table_parent != nullptr) {
        const auto* parent_column = table_parent->FindColumn(column->Name());
        if (parent_column != nullptr) {
          return error::AlteringParentColumn(column->FullName());
        }
      }

      // If the key column is a parent table column.
      for (const auto* child_table : column->table()->children()) {
        if (child_table->FindKeyColumn(column->Name())) {
          return error::CannotChangeKeyColumnWithChildTables(
              column->FullName());
        }
      }
    }
  }

  ZETASQL_RET_CHECK(!key_column->column_->is_deleted());
  ZETASQL_RET_CHECK_EQ(key_column->is_descending_, old_key_column->is_descending_);
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
