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

#include "backend/transaction/transaction_store.h"

#include <vector>

#include "zetasql/public/value.h"
#include "absl/container/flat_hash_map.h"
#include "zetasql/base/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/time/time.h"
#include "backend/common/rows.h"
#include "backend/datamodel/key_range.h"
#include "backend/locking/request.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/catalog/table.h"
#include "backend/storage/iterator.h"
#include "backend/transaction/commit_timestamp.h"
#include "common/errors.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

bool SortByKey(const FixedRowStorageIterator::Row& a,
               const FixedRowStorageIterator::Row& b) {
  return a.first < b.first;
}

void ResetInvalidValuesToNull(absl::Span<const Column* const> columns,
                              ValueList* values) {
  if (!values) {
    return;
  }

  ZETASQL_DCHECK(columns.size() == values->size());

  for (int i = 0; i < columns.size(); ++i) {
    if (!values->at(i).is_valid()) {
      values->at(i) = zetasql::values::Null(columns[i]->GetType());
    }
  }
}

}  // namespace

absl::Status TransactionStore::AcquireReadLock(
    const Table* table, const KeyRange& key_range,
    absl::Span<const Column* const> columns) const {
  lock_handle_->EnqueueLock(LockRequest(LockMode::kShared, table->id(),
                                        key_range, GetColumnIDs(columns)));
  return lock_handle_->Wait();
}

absl::Status TransactionStore::AcquireWriteLock(
    const Table* table, const KeyRange& key_range,
    absl::Span<const Column* const> columns) const {
  lock_handle_->EnqueueLock(LockRequest(LockMode::kExclusive, table->id(),
                                        key_range, GetColumnIDs(columns)));
  return lock_handle_->Wait();
}

absl::Status TransactionStore::BufferInsert(
    const Table* table, const Key& key, absl::Span<const Column* const> columns,
    const ValueList& values) {
  // Acquire locks to prevent another transaction to modify this entity.
  ZETASQL_RETURN_IF_ERROR(AcquireWriteLock(table, KeyRange::Point(key), columns));

  RowOp row_op;
  bool row_exists = RowExistsInBuffer(table, key, &row_op);
  Row row_values;
  if (row_exists) {
    // There is an existing delete on this row. Normalize this insert
    // with the previous delete mutation.
    row_values = row_op.second;
  }

  // Buffer the insert mutation with the row values to be inserted.
  for (int i = 0; i < columns.size(); ++i) {
    row_values[columns[i]] = values[i];
  }
  buffered_ops_[table][key] = std::make_pair(OpType::kInsert, row_values);

  TrackColumnsForCommitTimestamp(columns, values);
  TrackTableForCommitTimestamp(table, key);
  return absl::OkStatus();
}

absl::Status TransactionStore::BufferUpdate(
    const Table* table, const Key& key, absl::Span<const Column* const> columns,
    const ValueList& values) {
  // Acquire locks to prevent another transaction to modify this entity.
  ZETASQL_RETURN_IF_ERROR(AcquireWriteLock(table, KeyRange::Point(key), columns));

  RowOp row_op;
  bool row_exists = RowExistsInBuffer(table, key, &row_op);

  // Buffer the update mutation with the cell values to be updated.
  OpType op_type;
  Row row_values;
  if (row_exists) {
    // There is an existing insert or update on this row. Normalize this update
    // with the previous mutation.
    row_values = row_op.second;
    // If the previous mutation on this row is an insert, keep the OpType
    // as insert, since updating an inserted row will appear as a single insert.
    op_type = row_op.first;
  } else {
    op_type = OpType::kUpdate;
  }

  for (int i = 0; i < columns.size(); ++i) {
    row_values[columns[i]] = values[i];
  }
  buffered_ops_[table][key] = std::make_pair(op_type, row_values);

  TrackColumnsForCommitTimestamp(columns, values);
  TrackTableForCommitTimestamp(table, key);
  return absl::OkStatus();
}

absl::Status TransactionStore::BufferDelete(const Table* table,
                                            const Key& key) {
  // Acquire locks to prevent another transaction to modify this entity.
  ZETASQL_RETURN_IF_ERROR(AcquireWriteLock(table, KeyRange::Point(key), {}));

  // Marking all columns null to indicate a delete.
  Row row_values;
  for (auto column : table->columns()) {
    row_values[column] = zetasql::values::Null(column->GetType());
  }
  buffered_ops_[table][key] = std::make_pair(OpType::kDelete, row_values);

  TrackTableForCommitTimestamp(table, key);
  return absl::OkStatus();
}

absl::Status TransactionStore::BufferWriteOp(const WriteOp& op) {
  return std::visit(
      overloaded{
          [&](const InsertOp& op) {
            return BufferInsert(op.table, op.key, op.columns, op.values);
          },
          [&](const UpdateOp& op) {
            return BufferUpdate(op.table, op.key, op.columns, op.values);
          },
          [&](const DeleteOp& op) { return BufferDelete(op.table, op.key); },
      },
      op);
}

// Reads keys within the given range and add to StorageIterator. This is done by
// reading from both the base storage & transaction store.
// - Read keys from base storage (and apply merges as mentioned below).
// - Read keys from transaction store and perform the following action
//   for each of OpType as:
//   - insert: add to output storage iterator
//   - update: these updates should be applied over the base storage row.
//   - delete: should remove the key read from the base storage.
absl::Status TransactionStore::Read(
    const Table* table, const KeyRange& key_range,
    absl::Span<const Column* const> columns,
    std::unique_ptr<StorageIterator>* storage_itr,
    bool allow_pending_commit_timestamps_in_read) const {
  // Acquire locks to prevent another transaction to modify this entity.
  ZETASQL_RETURN_IF_ERROR(AcquireReadLock(table, key_range, columns));

  // Read rows buffered within transaction store.
  // Table lookup.
  std::vector<FixedRowStorageIterator::Row> rows;
  auto table_itr = buffered_ops_.find(table);
  if (table_itr != buffered_ops_.end()) {
    auto table = table_itr->second;
    // Key range lookup.
    auto begin_itr = table.lower_bound(key_range.start_key());
    auto end_itr = table.lower_bound(key_range.limit_key());

    for (auto itr = begin_itr; itr != end_itr; ++itr) {
      if (itr->second.first == OpType::kInsert) {
        // Add inserts into the StorageIterator.
        Row row_values = itr->second.second;
        ValueList values;
        values.reserve(columns.size());
        for (const Column* column : columns) {
          if (row_values.find(column) == row_values.end()) {
            values.emplace_back(zetasql::values::Null(column->GetType()));
          } else {
            values.emplace_back(row_values[column]);
          }
        }
        rows.emplace_back(std::make_pair(itr->first, values));
      }
    }
  }

  // Read from the base storage and apply the changes buffered in transaction
  // store.
  std::unique_ptr<StorageIterator> base_itr;
  ZETASQL_RETURN_IF_ERROR(base_storage_->Read(absl::InfiniteFuture(), table->id(),
                                      key_range, GetColumnIDs(columns),
                                      &base_itr));
  while (base_itr->Next()) {
    ValueList values;
    values.reserve(columns.size());

    RowOp row_op;
    // Merge column values from transaction store & base storage.
    if (RowExistsInBuffer(table, base_itr->Key(), &row_op)) {
      if (row_op.first == OpType::kDelete || row_op.first == OpType::kInsert) {
        // Omit the deletes from the output. The buffered inserts have already
        // been added to the output.
        continue;
      }
      if (row_op.first == OpType::kUpdate) {
        // Update the column values to reflect the changes in transaction store.
        for (int i = 0; i < columns.size(); i++) {
          if (row_op.second.find(columns[i]) != row_op.second.end()) {
            values.emplace_back(row_op.second[columns[i]]);
          } else if (base_itr->ColumnValue(i).is_valid()) {
            values.emplace_back(base_itr->ColumnValue(i));
          } else {
            values.emplace_back(zetasql::values::Null(columns[i]->GetType()));
          }
        }
      }
    } else {
      // Copy the base storage column values since this row does not exists in
      // transaction store.
      for (int i = 0; i < columns.size(); i++) {
        if (base_itr->ColumnValue(i).is_valid()) {
          values.emplace_back(base_itr->ColumnValue(i));
        } else {
          values.emplace_back(zetasql::values::Null(columns[i]->GetType()));
        }
      }
    }
    rows.emplace_back(std::make_pair(base_itr->Key(), values));
  }

  // Pending commit timestamp values in buffer cannot be returned to
  // clients.
  if (!allow_pending_commit_timestamps_in_read) {
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
  }

  // Sort the keys to provide iterating in order.
  // Note: this can be optimized by iterating the base_store_iterator and
  // transaction_store_ietrator in parallel and comparing the keys.
  sort(rows.begin(), rows.end(), SortByKey);
  *storage_itr = absl::make_unique<FixedRowStorageIterator>(std::move(rows));
  return absl::OkStatus();
}

bool TransactionStore::RowExistsInBuffer(const Table* table, const Key& key,
                                         RowOp* row_op) const {
  const auto table_itr = buffered_ops_.find(table);
  if (table_itr == buffered_ops_.end()) {
    // Table does not exist. This can happen if the table is empty.
    return false;
  }
  const auto row_op_itr = table_itr->second.find(key);
  if (row_op_itr == table_itr->second.end()) {
    // Key does not exist.
    return false;
  }
  *row_op = row_op_itr->second;
  return true;
}

void TransactionStore::TrackColumnsForCommitTimestamp(
    absl::Span<const Column* const> columns, const ValueList& values) {
  ZETASQL_DCHECK_EQ(columns.size(), values.size());
  for (int i = 0; i < columns.size(); ++i) {
    if (IsPendingCommitTimestamp(columns[i], values[i])) {
      commit_ts_columns_.insert(columns[i]);
    }
  }
}

void TransactionStore::TrackTableForCommitTimestamp(const Table* table,
                                                    const Key& key) {
  if (HasPendingCommitTimestampInKey(table, key)) {
    commit_ts_tables_.insert(table);

    // Any time a table has a pending commit-ts in key, include all its indexes.
    for (const Index* index : table->indexes()) {
      commit_ts_tables_.insert(index->index_data_table());
    }
  }
}

zetasql_base::StatusOr<ValueList> TransactionStore::Lookup(
    const Table* table, const Key& key,
    absl::Span<const Column* const> columns) const {
  ValueList values;

  // Acquire locks to prevent another transaction to modify this entity.
  ZETASQL_RETURN_IF_ERROR(AcquireReadLock(table, KeyRange::Point(key), columns));

  // Check if row exists within the buffer.
  RowOp row_op;
  if (RowExistsInBuffer(table, key, &row_op)) {
    switch (row_op.first) {
      case OpType::kInsert: {
        // Fetch the latest value from the cell.
        for (auto column : columns) {
          auto row_value = row_op.second.find(column);
          if (row_value != row_op.second.end()) {
            values.emplace_back(row_value->second);
          } else {
            values.emplace_back(zetasql::values::Null(column->GetType()));
          }
        }
        break;
      }
      case OpType::kUpdate: {
        // For update, the base storage needs to be checked to retrieve values
        // which might not be included in the update.
        ZETASQL_RETURN_IF_ERROR(base_storage_->Lookup(absl::InfiniteFuture(),
                                              table->id(), key,
                                              GetColumnIDs(columns), &values));
        ResetInvalidValuesToNull(columns, &values);
        for (int i = 0; i < columns.size(); ++i) {
          // Update values retrieved from base storage with new values.
          auto row_value = row_op.second.find(columns[i]);
          if (row_value != row_op.second.end()) {
            values[i] = row_value->second;
          }
        }
        break;
      }
      // Ignore delete operations.
      case OpType::kDelete: {
        return error::RowNotFound(table->id(), key.DebugString());
        break;
      }
    }
    return values;
  }
  ZETASQL_RETURN_IF_ERROR(base_storage_->Lookup(absl::InfiniteFuture(), table->id(),
                                        key, GetColumnIDs(columns), &values));
  ResetInvalidValuesToNull(columns, &values);
  return values;
}

std::vector<WriteOp> TransactionStore::GetBufferedOps() const {
  std::vector<WriteOp> buffered_ops;
  for (const auto& entry : buffered_ops_) {
    const Table* table = entry.first;
    for (const auto& row : entry.second) {
      const Key& key = row.first;
      const RowOp& row_op = row.second;
      std::vector<const Column*> columns;
      ValueList values;
      for (const auto& cell : row_op.second) {
        columns.emplace_back(cell.first);
        values.emplace_back(cell.second);
      }
      switch (row_op.first) {
        case OpType::kInsert: {
          buffered_ops.emplace_back(InsertOp{table, key, columns, values});
          break;
        }
        case OpType::kUpdate: {
          buffered_ops.emplace_back(UpdateOp{table, key, columns, values});
          break;
        }
        case OpType::kDelete: {
          buffered_ops.emplace_back(DeleteOp{table, key});
          break;
        }
      }
    }
  }
  return buffered_ops;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
