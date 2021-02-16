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

#include "backend/schema/verifiers/column_value_verifiers.h"

#include <string>

#include "zetasql/public/functions/string.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "absl/types/optional.h"
#include "backend/common/ids.h"
#include "backend/common/indexing.h"
#include "backend/common/rows.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/types.h"
#include "backend/datamodel/value.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "backend/storage/in_memory_storage.h"
#include "backend/storage/iterator.h"
#include "common/errors.h"
#include "common/limits.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

absl::Status VerifyColumnValue(
    const SchemaValidationContext* context, const Table* table,
    const Column* column,
    const std::function<absl::Status(const zetasql::Value& column_value,
                                     const Key& key)>& verifier) {
  std::unique_ptr<StorageIterator> itr;
  ZETASQL_RETURN_IF_ERROR(context->storage()->Read(context->pending_commit_timestamp(),
                                           table->id(), KeyRange::All(),
                                           {column->id()}, &itr));

  while (itr->Next()) {
    for (int i = 0; i < itr->NumColumns(); ++i) {
      ZETASQL_RETURN_IF_ERROR(verifier(itr->ColumnValue(i), itr->Key()));
    }
  }
  return absl::OkStatus();
}

absl::Status VerifyStringColumnValue(absl::string_view table_name,
                                     absl::string_view column_name,
                                     const zetasql::Value& value,
                                     const Key& key,
                                     const zetasql::Type* new_column_type,
                                     int64_t new_max_length) {
  ZETASQL_RET_CHECK(value.type()->IsString());
  absl::Status error;
  int64_t value_length;
  if (!zetasql::functions::LengthUtf8(value.string_value(), &value_length,
                                        &error)) {
    return error::InvalidStringEncoding(table_name, column_name);
  }
  if (new_column_type->IsBytes()) {
    value_length = value.string_value().length();
  }
  if (value_length > new_max_length) {
    return error::InvalidColumnSizeReduction(column_name, new_max_length,
                                             value_length, key.DebugString());
  }
  return absl::OkStatus();
}

absl::Status VerifyBytesColumnValue(absl::string_view table_name,
                                    absl::string_view column_name,
                                    const zetasql::Value& value,
                                    const Key& key,
                                    const zetasql::Type* new_column_type,
                                    int64_t new_max_length) {
  ZETASQL_RET_CHECK(value.type()->IsBytes());
  if (new_column_type->IsBytes()) {
    if (value.bytes_value().size() > new_max_length) {
      return error::InvalidColumnSizeReduction(column_name, new_max_length,
                                               value.bytes_value().size(),
                                               key.DebugString());
    }
  }

  ZETASQL_RET_CHECK(new_column_type->IsString());

  // Check that it is valid UTF-8 encoding.
  absl::Status error;
  int64_t encoded_chars;
  if (!zetasql::functions::LengthUtf8(value.bytes_value(), &encoded_chars,
                                        &error)) {
    return error::UTF8StringColumn(column_name, key.DebugString());
  }

  // Validate length of new column.
  if (encoded_chars > new_max_length) {
    return error::InvalidColumnSizeReduction(column_name, new_max_length,
                                             encoded_chars, key.DebugString());
  }
  return absl::OkStatus();
}

absl::Status VerifyColumnValueOnTypeChange(
    absl::string_view table_name, absl::string_view column_name,
    const zetasql::Value& value, const Key& key,
    const zetasql::Type* old_column_type,
    const zetasql::Type* new_column_type, int64_t new_max_length) {
  ZETASQL_RET_CHECK(old_column_type != nullptr && new_column_type != nullptr);

  // Check for null-ness before accessing value.
  if (!value.is_valid() || value.is_null()) {
    return absl::OkStatus();
  }

  if (old_column_type->IsArray()) {
    ZETASQL_RET_CHECK(new_column_type->IsArray());
    const auto* old_elem_type = BaseType(old_column_type);
    const auto* new_elem_type = BaseType(new_column_type);
    for (const auto& element : value.elements()) {
      ZETASQL_RETURN_IF_ERROR(VerifyColumnValueOnTypeChange(
          table_name, column_name, element, key, old_elem_type, new_elem_type,
          new_max_length));
    }
    return absl::OkStatus();
  }

  if (old_column_type->IsString()) {
    // We allow changing STRING to BYTES, but the BYTES column must be large
    // enough to handle the conversion since each UTF8 character could
    // potentially be up to 4 bytes.
    ZETASQL_RETURN_IF_ERROR(VerifyStringColumnValue(table_name, column_name, value, key,
                                            new_column_type, new_max_length));
  }

  if (old_column_type->IsBytes()) {
    // Bytes must be valid UTF8 to convert to a string.
    ZETASQL_RETURN_IF_ERROR(VerifyBytesColumnValue(table_name, column_name, value, key,
                                           new_column_type, new_max_length));
  }
  return absl::OkStatus();
}

absl::Status VerifyColumnValuesOnTypeChange(
    const Table* table, const Column* column,
    const zetasql::Type* old_column_type,
    const zetasql::Type* new_column_type, int64_t new_max_length,
    const SchemaValidationContext* context) {
  return VerifyColumnValue(
      context, table, column,
      [&](const zetasql::Value& value, const Key& key) -> absl::Status {
        return VerifyColumnValueOnTypeChange(table->Name(), column->Name(),
                                             value, key, old_column_type,
                                             new_column_type, new_max_length);
      });
}

}  // namespace

absl::Status VerifyColumnNotNull(const Table* table, const Column* column,
                                 const SchemaValidationContext* context) {
  return VerifyColumnValue(
      context, table, column,
      [&](const zetasql::Value& value, const Key& key) -> absl::Status {
        if (!value.is_valid() || value.is_null()) {
          return error::NullValueForNotNullColumn(table->Name(), column->Name(),
                                                  key.DebugString());
        }
        return absl::OkStatus();
      });
}

absl::Status VerifyColumnLength(const Table* table, const Column* column,
                                int64_t new_max_length,
                                const SchemaValidationContext* context) {
  const auto* column_type = column->GetType();
  return VerifyColumnValuesOnTypeChange(table, column, column_type, column_type,
                                        new_max_length, context);
}

absl::Status VerifyColumnTypeChange(const Table* table,
                                    const Column* old_column,
                                    const Column* new_column,
                                    const SchemaValidationContext* context) {
  return VerifyColumnValuesOnTypeChange(
      table, old_column, old_column->GetType(), new_column->GetType(),
      new_column->effective_max_length(), context);
}

absl::Status VerifyColumnCommitTimestamp(
    const Table* table, const Column* column,
    const SchemaValidationContext* context) {
  return VerifyColumnValue(
      context, table, column,
      [&](const zetasql::Value& value, const Key& key) -> absl::Status {
        if (!value.is_valid() || value.is_null()) {
          return absl::OkStatus();
        }
        // Check that timestamp is not greater than commit time.
        if (value.type()->IsTimestamp() &&
            value.ToTime() >= context->pending_commit_timestamp()) {
          return error::CommitTimestampNotInFuture(
              column->Name(), key.DebugString(), value.ToTime());
        }
        return absl::OkStatus();
      });
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
