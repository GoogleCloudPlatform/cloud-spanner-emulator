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

#include <cstdint>
#include <vector>

#include "zetasql/public/functions/string.h"
#include "zetasql/public/value.h"
#include "absl/status/status.h"
#include "absl/types/span.h"
#include "backend/actions/context.h"
#include "backend/actions/ops.h"
#include "backend/datamodel/key.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
#include "common/clock.h"
#include "common/constants.h"
#include "common/errors.h"
#include "common/limits.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

absl::Status ColumnValueValidator::ValidateColumnValueType(
    const Table* table, const Column* const column,
    const zetasql::Value& value) const {
  // Check that type is same for both column and the corresponding value.
  if (column->GetType()->kind() != value.type()->kind()) {
    return error::ColumnValueTypeMismatch(table->Name(),
                                          column->GetType()->DebugString(),
                                          value.type()->DebugString());
  }

  // Check that we are not attempting to write null values to non-nullable
  // columns. Writing null to non-nullable evaluated columns is temporarily
  // fine, since the violation may be fixed later by an operation to update the
  // column.
  if (value.is_null() && !column->is_nullable() && !column->is_generated()) {
    return error::NullValueForNotNullColumn(table->Name(), column->FullName());
  }

  return absl::OkStatus();
}

absl::Status ColumnValueValidator::ValidateKeyNotNull(const Table* table,
                                                      const Key& key) const {
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
  return absl::OkStatus();
}

absl::Status ColumnValueValidator::ValidateColumnStringValue(
    const Table* table, const Column* column,
    const zetasql::Value& value) const {
  // Validate that strings do not exceed max length.
  if (!value.is_null()) {
    absl::Status error;
    int64_t encoded_chars = 0;
    if (!zetasql::functions::LengthUtf8(value.string_value(), &encoded_chars,
                                          &error)) {
      return error::InvalidStringEncoding(table->Name(), column->Name());
    }
    if (encoded_chars > column->effective_max_length()) {
      return error::ValueExceedsLimit(column->FullName(), encoded_chars,
                                      column->effective_max_length());
    }

    if (column->is_placement_key() && !value.string_value().empty() &&
        !placements_.contains(value.string_value())) {
      return error::UnknownPlacement(value.string_value());
    }
  }
  return absl::OkStatus();
}

absl::Status ColumnValueValidator::ValidateColumnBytesValue(
    const Table* table, const Column* column,
    const zetasql::Value& value) const {
  // Validate that bytes do not exceed max length.
  if (!value.is_null()) {
    if (value.bytes_value().size() > column->effective_max_length()) {
      return error::ValueExceedsLimit(column->FullName(),
                                      value.bytes_value().size(),
                                      column->effective_max_length());
    }
  }
  return absl::OkStatus();
}

absl::Status ColumnValueValidator::ValidateColumnArrayValue(
    const Table* table, const Column* column,
    const zetasql::Value& value) const {
  // Validate the vector search array length.
  if (column->has_vector_length() && !value.is_null()) {
    int array_length = value.elements().size();
    int expected_length = column->vector_length().value();
    if (array_length > expected_length) {
      return error::VectorLengthExceedsLimit(column->FullName(), array_length,
                                             expected_length);
    } else if (array_length < expected_length) {
      return error::VectorLengthLessThanLimit(column->FullName(), array_length,
                                              expected_length);
    }
  }
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
    // Validate no element in vector search array is null.
    // Currently vector length can only be applied on ARRAY containing FLOAT32
    // or FLOAT64 elements.
    if (column->has_vector_length()) {
      for (const auto& element : value.elements()) {
        if (element.is_null()) {
          return error::DisallowNullsInSearchArray(column->FullName());
        }
      }
    }
  }
  return absl::OkStatus();
}

absl::Status ColumnValueValidator::ValidateColumnTimestampValue(
    const Column* const column, const zetasql::Value& value,
    Clock* clock) const {
  // Check that user provided timestamp value is not in future. Sentinel max
  // timestamp value for commit timestamp column can only be set internally.
  if (column->allows_commit_timestamp() && !value.is_null() &&
      value.ToTime() != kCommitTimestampValueSentinel &&
      value.ToTime() > clock->Now()) {
    return error::CommitTimestampInFuture(value.ToTime());
  }
  return absl::OkStatus();
}

absl::Status ColumnValueValidator::ValidateKeySize(const Table* table,
                                                   const Key& key) const {
  int64_t key_size = key.LogicalSizeInBytes();
  if (key_size > limits::kMaxKeySizeBytes) {
    return error::KeyTooLarge(table->Name(), key_size,
                              limits::kMaxKeySizeBytes);
  }
  return absl::OkStatus();
}

absl::Status ColumnValueValidator::ValidateInsertUpdateOp(
    const Table* table, const std::vector<const Column*>& columns,
    const std::vector<zetasql::Value>& values, Clock* clock) const {
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
  return absl::OkStatus();
}

absl::Status ColumnValueValidator::Validate(const ActionContext* ctx,
                                            const InsertOp& op) const {
  ZETASQL_RETURN_IF_ERROR(ValidateKeySize(op.table, op.key));
  return ValidateInsertUpdateOp(op.table, op.columns, op.values, ctx->clock());
}

absl::Status ColumnValueValidator::Validate(const ActionContext* ctx,
                                            const UpdateOp& op) const {
  return ValidateInsertUpdateOp(op.table, op.columns, op.values, ctx->clock());
}

absl::Status ColumnValueValidator::Validate(const ActionContext* ctx,
                                            const DeleteOp& op) const {
  return ValidateKeyNotNull(op.table, op.key);
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
