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

#include <queue>
#include <variant>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "backend/actions/ops.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/value.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
#include "common/constants.h"
#include "common/errors.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

bool IsPendingCommitTimestampStringValue(zetasql::Value column_value) {
  return (!column_value.is_null() && column_value.type()->IsString() &&
          column_value.string_value() == kCommitTimestampIdentifier);
}

bool IsPendingCommitTimestampSentinelValue(zetasql::Value column_value) {
  return (!column_value.is_null() && column_value.type()->IsTimestamp() &&
          column_value.ToTime() == kCommitTimestampValueSentinel);
}

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

absl::Status ValidateCommitTimestampEnabledInHeirarchy(const Column* column) {
  const Table* table = column->table();

  // Validate that each of the parent table which contains this column as part
  // of a primary key has commit timestamp enabled.
  while (table->parent() != nullptr) {
    table = table->parent();
    const KeyColumn* key_column = table->FindKeyColumn(column->Name());
    if (key_column == nullptr) {
      break;
    }
    if (!key_column->column()->allows_commit_timestamp()) {
      return error::CommitTimestampOptionNotEnabled(column->FullName());
    }
  }

  // If the given column is a key_column, check that all the children which
  // must contain the same key as part of their primary keys, also have
  // commit timestamp enabled. Perform the validation in a breadth first order.
  std::queue<const Table*> children_tables;
  children_tables.push(table);
  while (!children_tables.empty()) {
    const Table* child_table = children_tables.front();
    children_tables.pop();

    const KeyColumn* key_column = child_table->FindKeyColumn(column->Name());
    if (key_column == nullptr) {
      // This shouldn't really happen, but would be caught by later action
      // framework.
      continue;
    }
    if (!key_column->column()->allows_commit_timestamp()) {
      return error::CommitTimestampOptionNotEnabled(column->FullName());
    }
    for (const Table* child : child_table->children()) {
      children_tables.push(child);
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<zetasql::Value> MaybeSetCommitTimestampSentinel(
    const Column* column, zetasql::Value column_value) {
  if (!column->GetType()->IsTimestamp()) return column_value;

  if (column->allows_commit_timestamp()) {
    if (IsPendingCommitTimestampStringValue(column_value)) {
      ZETASQL_RETURN_IF_ERROR(ValidateCommitTimestampEnabledInHeirarchy(column));
      return zetasql::values::Timestamp(kCommitTimestampValueSentinel);
    } else if (IsPendingCommitTimestampSentinelValue(column_value)) {
      return error::CommitTimestampInFuture(column_value.ToTime());
    }
  } else if (IsPendingCommitTimestampStringValue(column_value)) {
    return error::CommitTimestampOptionNotEnabled(column->FullName());
  }
  return column_value;
}

absl::StatusOr<Key> MaybeSetCommitTimestampSentinel(
    absl::Span<const KeyColumn* const> primary_key, Key key) {
  for (int i = 0; i < key.NumColumns(); i++) {
    ZETASQL_ASSIGN_OR_RETURN(zetasql::Value value,
                     MaybeSetCommitTimestampSentinel(primary_key[i]->column(),
                                                     key.ColumnValue(i)));
    key.SetColumnValue(i, value);
  }
  return key;
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

absl::StatusOr<ValueList> MaybeSetCommitTimestampSentinel(
    absl::Span<const Column* const> columns, const ValueList& row) {
  if (row.empty()) return row;
  ValueList ret_val;
  for (int i = 0; i < row.size(); i++) {
    ZETASQL_ASSIGN_OR_RETURN(ret_val.emplace_back(),
                     MaybeSetCommitTimestampSentinel(columns[i], row[i]));
  }
  return ret_val;
}

absl::StatusOr<KeyRange> MaybeSetCommitTimestampSentinel(
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

// Returns true if the given column value contains sentinel timestamp value
// and column allows commit timestamp to be set automatically. Signals that
// value should be replaced with commit timestamp of transaction during flush.
bool IsPendingCommitTimestamp(const Column* column,
                              const zetasql::Value& column_value) {
  if ((column->allows_commit_timestamp() ||
       (column->source_column() &&
        column->source_column()->allows_commit_timestamp())) &&
      IsPendingCommitTimestampSentinelValue(column_value)) {
    return true;
  }
  return false;
}

bool HasPendingCommitTimestampInKey(const Table* table, const Key& key) {
  for (int i = 0; i < key.NumColumns(); i++) {
    if (IsPendingCommitTimestamp(table->primary_key()[i]->column(),
                                 key.ColumnValue(i))) {
      return true;
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

absl::Status CommitTimestampTracker::CheckRead(
    const Table* table, absl::Span<const Column* const> columns) const {
  absl::MutexLock lock(&mu_);
  if (commit_ts_tables_.contains(table)) {
    return error::CannotReadPendingCommitTimestamp(
        absl::StrCat("Table ", table->Name()));
  }
  for (const auto column : columns) {
    if (commit_ts_columns_.contains(column) ||
        (column->source_column() != nullptr &&
         commit_ts_columns_.contains(column->source_column()))) {
      return error::CannotReadPendingCommitTimestamp(
          absl::StrCat("Column ", column->Name()));
    }
  }
  return absl::OkStatus();
}

void CommitTimestampTracker::TrackColumns(
    absl::Span<const Column* const> columns, const ValueList& values) {
  ABSL_DCHECK_EQ(columns.size(), values.size());
  for (int i = 0; i < columns.size(); ++i) {
    if (IsPendingCommitTimestamp(columns[i], values[i])) {
      commit_ts_columns_.insert(columns[i]);
    }
  }
}

void CommitTimestampTracker::TrackTable(const Table* table, const Key& key) {
  if (HasPendingCommitTimestampInKey(table, key)) {
    commit_ts_tables_.insert(table);

    // Any time a table has a pending commit-ts in key, include all its indexes.
    for (const Index* index : table->indexes()) {
      commit_ts_tables_.insert(index->index_data_table());
    }
  }
}

void CommitTimestampTracker::Track(absl::Span<const WriteOp> write_ops) {
  absl::MutexLock lock(&mu_);
  for (auto& op : write_ops) {
    if (std::holds_alternative<InsertOp>(op)) {
      const InsertOp& insert = std::get<InsertOp>(op);
      TrackColumns(insert.columns, insert.values);
      TrackTable(insert.table, insert.key);
    } else if (std::holds_alternative<UpdateOp>(op)) {
      const UpdateOp& update = std::get<UpdateOp>(op);
      TrackColumns(update.columns, update.values);
      TrackTable(update.table, update.key);
    } else {
      const DeleteOp& delete_op = std::get<DeleteOp>(op);
      TrackTable(delete_op.table, delete_op.key);
    }
  }
}
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
