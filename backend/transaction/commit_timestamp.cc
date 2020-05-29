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

#include "backend/transaction/commit_timestamp.h"

#include "backend/schema/catalog/table.h"
#include "common/constants.h"
#include "common/errors.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

absl::Status ValidateCommitTimestampKeyForDeleteOp(const Table* table,
                                                   const Key& key,
                                                   absl::Time now) {
  const absl::Span<const KeyColumn* const> primary_key = table->primary_key();
  for (int i = 0; i < key.NumColumns(); ++i) {
    const Column* column = primary_key.at(i)->column();
    if (column->GetType()->IsTimestamp() && column->allows_commit_timestamp()) {
      ZETASQL_RETURN_IF_ERROR(
          ValidateCommitTimestampValueNotInFuture(key.ColumnValue(i), now));
    }
  }
  return absl::OkStatus();
}

zetasql_base::StatusOr<zetasql::Value> MaybeSetCommitTimestampSentinel(
    const Column* column, zetasql::Value column_value) {
  if (column->GetType()->IsTimestamp() && column->allows_commit_timestamp()) {
    if (!column_value.is_null() && column_value.type()->IsString() &&
        column_value.string_value() == kCommitTimestampIdentifier) {
      return zetasql::values::Timestamp(kCommitTimestampValueSentinel);
    } else if (!column_value.is_null() && column_value.type()->IsTimestamp() &&
               column_value.ToTime() == kCommitTimestampValueSentinel) {
      return error::CommitTimestampInFuture(column_value.ToTime());
    }
  }
  return column_value;
}

zetasql_base::StatusOr<Key> MaybeSetCommitTimestampSentinel(
    absl::Span<const KeyColumn* const> primary_key, Key key) {
  for (int i = 0; i < key.NumColumns(); i++) {
    ZETASQL_ASSIGN_OR_RETURN(zetasql::Value value,
                     MaybeSetCommitTimestampSentinel(primary_key[i]->column(),
                                                     key.ColumnValue(i)));
    key.SetColumnValue(i, value);
  }
  return key;
}

// Returns true if the given column value contains sentinel timestamp value
// and column allows commit timestamp to be set automatically. Signals that
// value should be replaced with commit timestamp of transaction during flush.
bool IsPendingCommitTimestamp(const Column* column,
                              const zetasql::Value& column_value) {
  if (column->allows_commit_timestamp() && !column_value.is_null() &&
      column_value.type()->IsTimestamp() &&
      column_value.ToTime() == kCommitTimestampValueSentinel) {
    return true;
  }
  return false;
}

}  // namespace

absl::Status ValidateCommitTimestampValueNotInFuture(
    const zetasql::Value& value, absl::Time now) {
  if (!value.is_null() && value.type()->IsTimestamp() && value.ToTime() > now) {
    return error::CommitTimestampInFuture(value.ToTime());
  }
  return absl::OkStatus();
}

absl::Status ValidateCommitTimestampKeySetForDeleteOp(const Table* table,
                                                      const KeySet& set,
                                                      absl::Time now) {
  for (const Key& key : set.keys()) {
    ZETASQL_RETURN_IF_ERROR(ValidateCommitTimestampKeyForDeleteOp(table, key, now));
  }

  for (const KeyRange& key_range : set.ranges()) {
    auto closed_open = key_range.ToClosedOpen();
    if (closed_open.start_key() >= closed_open.limit_key()) {
      // No-op empty key ranges are ignored.
      continue;
    }

    ZETASQL_RETURN_IF_ERROR(ValidateCommitTimestampKeyForDeleteOp(
        table, key_range.start_key(), now));
    ZETASQL_RETURN_IF_ERROR(ValidateCommitTimestampKeyForDeleteOp(
        table, key_range.limit_key(), now));
  }
  return absl::OkStatus();
}

zetasql_base::StatusOr<ValueList> MaybeSetCommitTimestampSentinel(
    absl::Span<const Column* const> columns, const ValueList& row) {
  if (row.empty()) return row;
  ValueList ret_val;
  for (int i = 0; i < row.size(); i++) {
    ZETASQL_ASSIGN_OR_RETURN(ret_val.emplace_back(),
                     MaybeSetCommitTimestampSentinel(columns[i], row[i]));
  }
  return ret_val;
}

zetasql_base::StatusOr<KeyRange> MaybeSetCommitTimestampSentinel(
    absl::Span<const KeyColumn* const> primary_key, const KeyRange& key_range) {
  ZETASQL_RET_CHECK(key_range.IsClosedOpen());
  if (key_range.start_key() >= key_range.limit_key()) {
    // Nothing to be done for empty key range.
    return key_range;
  }
  ZETASQL_ASSIGN_OR_RETURN(Key start_key, MaybeSetCommitTimestampSentinel(
                                      primary_key, key_range.start_key()));
  ZETASQL_ASSIGN_OR_RETURN(Key limit_key, MaybeSetCommitTimestampSentinel(
                                      primary_key, key_range.limit_key()));
  return KeyRange(key_range.start_type(), start_key, key_range.limit_type(),
                  limit_key);
}

bool HasPendingCommitTimestampInReadResult(
    const Table* table, absl::Span<const Column* const> columns,
    const std::vector<FixedRowStorageIterator::Row>& rows) {
  absl::Span<const KeyColumn* const> primary_key = table->primary_key();
  for (const auto& [key, values] : rows) {
    // Check that primary key of the row being read does not contain pending
    // commit timestamp value(s).
    for (int i = 0; i < key.NumColumns(); i++) {
      if (IsPendingCommitTimestamp(primary_key[i]->column(),
                                   key.ColumnValue(i))) {
        return true;
      }
    }

    // Check that non-primary-key columns being explicitly read do not contain
    // pending commit timestamp value(s).
    for (int i = 0; i < columns.size(); i++) {
      if (IsPendingCommitTimestamp(columns[i], values[i])) {
        return true;
      }
    }
  }
  return false;
}

zetasql::Value MaybeSetCommitTimestamp(const Column* column,
                                         const zetasql::Value& column_value,
                                         absl::Time commit_timestamp) {
  if (IsPendingCommitTimestamp(column, column_value)) {
    return zetasql::values::Timestamp(commit_timestamp);
  }
  return column_value;
}

Key MaybeSetCommitTimestamp(absl::Span<const KeyColumn* const> primary_key,
                            Key key, absl::Time commit_timestamp) {
  for (int i = 0; i < key.NumColumns(); i++) {
    key.SetColumnValue(
        i, MaybeSetCommitTimestamp(primary_key[i]->column(), key.ColumnValue(i),
                                   commit_timestamp));
  }
  return key;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
