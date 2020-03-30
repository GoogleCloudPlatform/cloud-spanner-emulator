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

#include "backend/actions/column_value.h"

#include "zetasql/public/functions/string.h"
#include "absl/container/flat_hash_set.h"
#include "absl/strings/string_view.h"
#include "backend/actions/action.h"
#include "backend/actions/context.h"
#include "backend/actions/ops.h"
#include "backend/datamodel/key.h"
#include "common/clock.h"
#include "common/constants.h"
#include "common/errors.h"
#include "common/limits.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

zetasql_base::Status ValidateNotNullColumnsPresent(
    const Table* table, const std::vector<const Column*>& columns) {
  // TODO: Find a way of doing this without creating a hash set for
  // every insert.
  absl::flat_hash_set<const Column*> inserted_columns;
  for (const auto column : columns) {
    if (!column->is_nullable()) {
      inserted_columns.insert(column);
    }
  }
  for (const auto column : table->columns()) {
    if (!column->is_nullable() && !inserted_columns.contains(column)) {
      return error::NonNullValueNotSpecifiedForInsert(table->Name(),
                                                      column->Name());
    }
  }
  return zetasql_base::OkStatus();
}

zetasql_base::Status ValidateColumnValueType(const Table* table,
                                     const Column* const column,
                                     const zetasql::Value& value) {
  // Check that type is same for both column and the corresponding value.
  if (column->GetType()->kind() != value.type()->kind()) {
    return error::ColumnValueTypeMismatch(table->Name(),
                                          column->GetType()->DebugString(),
                                          value.type()->DebugString());
  }

  // Check that we are not attempting to write null values to non-nullable
  // columns.
  if (value.is_null() && !column->is_nullable()) {
    return error::NullValueForNotNullColumn(table->Name(), column->FullName());
  }

  return zetasql_base::OkStatus();
}

zetasql_base::Status ValidateKeyNotNull(const Table* table, const Key& key) {
  // Incoming key should already have the correct number of columns
  // corresponding to the primary key of the given table. So we do not check it
  // here.
  absl::Span<const KeyColumn* const> primary_key = table->primary_key();
  for (int i = 0; i < primary_key.size(); ++i) {
    if (!primary_key.at(i)->column()->is_nullable() &&
        key.ColumnValue(i).is_null()) {
      return error::CannotParseKeyValue(
          table->Name(), primary_key.at(i)->column()->Name(),
          primary_key.at(i)->column()->GetType()->DebugString());
    }
  }
  return zetasql_base::OkStatus();
}

zetasql_base::Status ValidateColumnStringValue(const Table* table, const Column* column,
                                       const zetasql::Value& value) {
  // Validate that strings do not exceed max length.
  if (!value.is_null()) {
    zetasql_base::Status error;
    int64_t encoded_chars = 0;
    if (!zetasql::functions::LengthUtf8(value.string_value(), &encoded_chars,
                                          &error)) {
      return error::InvalidStringEncoding(table->Name(), column->Name());
    }
    if (encoded_chars > column->effective_max_length()) {
      return error::ValueExceedsLimit(column->FullName(), encoded_chars,
                                      column->effective_max_length());
    }
  }
  return zetasql_base::OkStatus();
}

zetasql_base::Status ValidateColumnBytesValue(const Table* table, const Column* column,
                                      const zetasql::Value& value) {
  // Validate that bytes do not exceed max length.
  if (!value.is_null()) {
    if (value.bytes_value().size() > column->effective_max_length()) {
      return error::ValueExceedsLimit(column->FullName(),
                                      value.bytes_value().size(),
                                      column->effective_max_length());
    }
  }
  return zetasql_base::OkStatus();
}

zetasql_base::Status ValidateColumnArrayValue(const Table* table, const Column* column,
                                      const zetasql::Value& value) {
  // Validate that bytes and string array element types do not exceed max
  // length.
  if (!value.is_null()) {
    if (value.type()->AsArray()->element_type()->IsString()) {
      for (const auto& element : value.elements()) {
        ZETASQL_RETURN_IF_ERROR(ValidateColumnStringValue(table, column, element));
      }
    } else if (value.type()->AsArray()->element_type()->IsBytes()) {
      for (const auto& element : value.elements()) {
        ZETASQL_RETURN_IF_ERROR(ValidateColumnBytesValue(table, column, element));
      }
    }
  }
  return zetasql_base::OkStatus();
}

zetasql_base::Status ValidateColumnTimestampValue(const Column* const column,
                                          const zetasql::Value& value,
                                          Clock* clock) {
  // Check that user provided timestamp value is not in future. Sentinel max
  // timestamp value for commit timestamp column can only be set internally.
  if (column->allows_commit_timestamp() && !value.is_null() &&
      value.ToTime() != kCommitTimestampValueSentinel &&
      value.ToTime() > clock->Now()) {
    return error::CommitTimestampInFuture(value.ToTime());
  }
  return zetasql_base::OkStatus();
}

zetasql_base::Status ValidateInsertUpdateOp(const Table* table,
                                    const std::vector<const Column*>& columns,
                                    const std::vector<zetasql::Value>& values,
                                    Clock* clock) {
  for (int i = 0; i < columns.size(); i++) {
    ZETASQL_RETURN_IF_ERROR(ValidateColumnValueType(table, columns[i], values[i]));
    switch (columns[i]->GetType()->kind()) {
      case zetasql::TYPE_ARRAY:
        ZETASQL_RETURN_IF_ERROR(ValidateColumnArrayValue(table, columns[i], values[i]));
        break;
      case zetasql::TYPE_BYTES:
        ZETASQL_RETURN_IF_ERROR(ValidateColumnBytesValue(table, columns[i], values[i]));
        break;
      case zetasql::TYPE_STRING:
        ZETASQL_RETURN_IF_ERROR(
            ValidateColumnStringValue(table, columns[i], values[i]));
        break;
      case zetasql::TYPE_TIMESTAMP:
        ZETASQL_RETURN_IF_ERROR(
            ValidateColumnTimestampValue(columns[i], values[i], clock));
        break;
      default:
        continue;
    }
  }
  return zetasql_base::OkStatus();
}

}  //  namespace

zetasql_base::Status ColumnValueValidator::Validate(const ActionContext* ctx,
                                            const InsertOp& op) const {
  ZETASQL_RETURN_IF_ERROR(ValidateNotNullColumnsPresent(op.table, op.columns));
  return ValidateInsertUpdateOp(op.table, op.columns, op.values, ctx->clock());
}

zetasql_base::Status ColumnValueValidator::Validate(const ActionContext* ctx,
                                            const UpdateOp& op) const {
  return ValidateInsertUpdateOp(op.table, op.columns, op.values, ctx->clock());
}

zetasql_base::Status ColumnValueValidator::Validate(const ActionContext* ctx,
                                            const DeleteOp& op) const {
  return ValidateKeyNotNull(op.table, op.key);
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
