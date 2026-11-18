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

#include "backend/storage/in_memory_storage.h"

#include <algorithm>
#include <iterator>
#include <memory>
#include <utility>
#include <vector>

#include "googlesql/public/value.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/time/time.h"
#include "backend/storage/in_memory_iterator.h"
#include "common/errors.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

static constexpr char kExistsColumn[] = "_exists";

}  // namespace

// The fewest due rows a write or delete collects. A call that queues more
// collects twice what it queued, so the queue drains at least as fast as it
// fills, while no call holds the lock for long on its account.
constexpr size_t kMinRowsCollectedPerCall = 256;

bool InMemoryStorage::CollectRow(Row& row, absl::Time horizon) {
  for (auto cell_itr = row.begin(); cell_itr != row.end();) {
    Cell& cell = cell_itr->second;
    // The newest version at or before the horizon is what a read at the horizon
    // sees; every version before it is seen by no read at or after it.
    auto first_after = cell.upper_bound(horizon);
    if (first_after != cell.begin()) {
      cell.erase(cell.begin(), std::prev(first_after));
    }
    // A cell left with only a delete marker reads as a cell never written.
    if (cell_itr->first != kExistsColumn && cell.size() == 1 &&
        cell.begin()->first <= horizon && !cell.begin()->second.is_valid()) {
      row.erase(cell_itr++);
    } else {
      ++cell_itr;
    }
  }
  auto exists_itr = row.find(kExistsColumn);
  if (exists_itr == row.end()) {
    return row.empty();
  }
  const Cell& exists = exists_itr->second;
  return exists.size() == 1 && exists.begin()->first <= horizon &&
         !exists.begin()->second.bool_value();
}

void InMemoryStorage::CollectDueRows(absl::Time now, size_t budget) {
  const absl::Time horizon = now - version_retention_period_;
  bool collected = false;
  while (budget > 0 && !garbage_rows_.empty() &&
         garbage_rows_.front().timestamp <= horizon) {
    const GarbageRow& due = garbage_rows_.front();
    auto table_itr = tables_.find(due.table_id);
    if (table_itr != tables_.end()) {
      Table& table = table_itr->second;
      auto row_itr = table.find(due.key);
      if (row_itr != table.end() && CollectRow(row_itr->second, horizon)) {
        table.erase(row_itr);
      }
    }
    garbage_rows_.pop_front();
    collected = true;
    --budget;
  }
  if (collected && horizon > earliest_version_time_) {
    earliest_version_time_ = horizon;
  }
}

void InMemoryStorage::CollectGarbage(absl::Time now) {
  absl::MutexLock lock(mu_);
  CollectDueRows(now, garbage_rows_.size());
}

size_t InMemoryStorage::PendingGarbageRows() const {
  absl::MutexLock lock(mu_);
  return garbage_rows_.size();
}

absl::Time InMemoryStorage::EarliestVersionTime() const {
  absl::MutexLock lock(mu_);
  return earliest_version_time_;
}

googlesql::Value InMemoryStorage::GetCellValueAtTimestamp(
    const Row& row, const ColumnID& column_id, absl::Time timestamp) const {
  // Perform the lookup for given cell.
  auto cell_itr = row.find(column_id);
  if (cell_itr == row.end()) {
    return googlesql::Value();
  }
  const Cell& cell = cell_itr->second;
  auto val_itr = cell.upper_bound(timestamp);

  // Timestamp is earlier than the time the cell was first written to.
  if (val_itr == cell.begin()) {
    return googlesql::Value();
  }

  // Fetch the value from the column.
  --val_itr;
  return val_itr->second;
}

bool InMemoryStorage::Exists(const Row& row, absl::Time timestamp) const {
  googlesql::Value value =
      GetCellValueAtTimestamp(row, kExistsColumn, timestamp);
  return value.is_valid() && value.bool_value();
}

absl::Status InMemoryStorage::Lookup(
    absl::Time timestamp, const TableID& table_id, const Key& key,
    const std::vector<ColumnID>& column_ids,
    std::vector<googlesql::Value>* values) const {
  absl::MutexLock lock(mu_);

  // Validate the request.
  if (!column_ids.empty() && values == nullptr) {
    return error::Internal(
        "InMemoryStorage::Lookup was passed a nullptr for "
        "values, but had non-empty column_ids.");
  }
  if (values != nullptr) {
    values->clear();
  }

  // Lookup for given table.
  auto table_itr = tables_.find(table_id);
  if (table_itr == tables_.end()) {
    return absl::Status(
        absl::StatusCode::kNotFound,
        absl::StrCat("Key: ", key.DebugString(), " not found for table: ",
                     table_id, " at timestamp: ", absl::FormatTime(timestamp)));
  }
  const Table& table = table_itr->second;

  // Lookup for given key.
  auto row_itr = table.find(key);
  if (row_itr == table.end()) {
    return absl::Status(
        absl::StatusCode::kNotFound,
        absl::StrCat("Key: ", key.DebugString(), " not found for table: ",
                     table_id, " at timestamp: ", absl::FormatTime(timestamp)));
  }
  const Row& row = row_itr->second;

  // Verify if the row exists at the given timestamp.
  if (!Exists(row, timestamp)) {
    return absl::Status(
        absl::StatusCode::kNotFound,
        absl::StrCat(
            "Key: ", key.DebugString(), " does not exist for table: ", table_id,
            " at the given timestamp: " + absl::FormatTime(timestamp)));
  }

  // For request without columns, return ok since the key exist.
  if (column_ids.empty()) {
    return absl::OkStatus();
  }

  // Fetch the value from the cell at the given timestamp.
  for (int i = 0; i < column_ids.size(); ++i) {
    values->emplace_back(
        GetCellValueAtTimestamp(row, column_ids[i], timestamp));
  }

  return absl::OkStatus();
}

absl::Status InMemoryStorage::Read(
    absl::Time timestamp, const TableID& table_id, const KeyRange& key_range,
    const std::vector<ColumnID>& column_ids,
    std::unique_ptr<StorageIterator>* itr) const {
  absl::MutexLock lock(mu_);

  // Validate the request.
  if (!key_range.IsClosedOpen()) {
    return error::Internal(
        absl::StrCat("InMemoryStorage::Read should be called "
                     "with ClosedOpen key range, found: ",
                     key_range.DebugString()));
  }

  std::vector<FixedRowStorageIterator::Row> rows;
  // Return an empty iterator for empty key_range.
  if (key_range.start_key() >= key_range.limit_key()) {
    *itr = std::make_unique<FixedRowStorageIterator>();
    return absl::OkStatus();
  }

  // Lookup for given table.
  auto table_itr = tables_.find(table_id);
  if (table_itr == tables_.end()) {
    *itr = std::make_unique<FixedRowStorageIterator>();
    return absl::OkStatus();
  }
  const Table& table = table_itr->second;

  // Lookup keys from the given key range.
  auto row_start_itr = table.lower_bound(key_range.start_key());
  auto row_end_itr = table.lower_bound(key_range.limit_key());
  for (auto itr = row_start_itr; itr != row_end_itr; ++itr) {
    const InMemoryStorage::Row& row = itr->second;
    if (!Exists(row, timestamp)) {
      continue;
    }

    std::vector<googlesql::Value> values;
    values.reserve(column_ids.size());
    for (const ColumnID& column_id : column_ids) {
      values.emplace_back(GetCellValueAtTimestamp(row, column_id, timestamp));
    }
    rows.emplace_back(std::make_pair(itr->first, values));
  }
  *itr = std::make_unique<FixedRowStorageIterator>(std::move(rows));
  return absl::OkStatus();
}

absl::Status InMemoryStorage::Write(
    absl::Time timestamp, const TableID& table_id, const Key& key,
    const std::vector<ColumnID>& column_ids,
    const std::vector<googlesql::Value>& values) {
  absl::MutexLock lock(mu_);

  // Add the table if it does not exist.
  Table& table = tables_[table_id];

  // Add the row with _exists system column if it does not exist.
  Row& row = table[key];
  // Whether this write gives a cell a second version, the first of which a
  // later collection can remove.
  bool supersedes = false;
  if (!Exists(row, timestamp)) {
    Cell& cell = row[kExistsColumn];
    supersedes |= !cell.empty();
    cell[timestamp] = googlesql::values::Bool(true);
  }

  // Add the values for the given columns.
  for (int i = 0; i < column_ids.size(); ++i) {
    Cell& cell = row[column_ids[i]];
    supersedes |= !cell.empty();
    cell[timestamp] = values[i];
  }

  size_t queued = 0;
  if (supersedes) {
    garbage_rows_.push_back(GarbageRow{timestamp, table_id, key});
    queued = 1;
  }
  CollectDueRows(timestamp, std::max(kMinRowsCollectedPerCall, 2 * queued));
  return absl::OkStatus();
}

absl::Status InMemoryStorage::Delete(absl::Time timestamp,
                                     const TableID& table_id,
                                     const KeyRange& key_range) {
  absl::MutexLock lock(mu_);

  if (!key_range.IsClosedOpen()) {
    return error::Internal(
        absl::StrCat("InMemoryStorage::Delete should be called "
                     "with ClosedOpen key range, found: ",
                     key_range.DebugString()));
  }
  if (key_range.start_key() >= key_range.limit_key()) {
    return absl::OkStatus();
  }

  // Lookup for given table.
  auto table_itr = tables_.find(table_id);
  if (table_itr == tables_.end()) {
    return absl::OkStatus();
  }
  Table& table = table_itr->second;

  // Lookup keys from the given key range.
  auto row_start_itr = table.lower_bound(key_range.start_key());
  if (row_start_itr == table.end()) {
    return absl::OkStatus();
  }
  auto row_end_itr = table.lower_bound(key_range.limit_key());

  // Mark the keys as deleted.
  size_t queued = 0;
  for (auto itr = row_start_itr; itr != row_end_itr; ++itr) {
    if (!Exists(itr->second, timestamp)) {
      continue;
    }

    for (auto& [column_id, cell] : itr->second) {
      if (column_id == kExistsColumn) {
        cell[timestamp] = googlesql::values::Bool(false);
      } else {
        // Column values are marked invalid googlesql::Value to avoid reading
        // the value of the cell before the delete.
        cell[timestamp] = googlesql::Value();
      }
    }
    // Once the delete is older than the retention period, the row is removed.
    garbage_rows_.push_back(GarbageRow{timestamp, table_id, itr->first});
    ++queued;
  }
  CollectDueRows(timestamp, std::max(kMinRowsCollectedPerCall, 2 * queued));
  return absl::OkStatus();
}

void InMemoryStorage::SetVersionRetentionPeriod(
    const absl::Duration version_retention_period) {
  absl::MutexLock lock(mu_);
  version_retention_period_ = version_retention_period;
}

void InMemoryStorage::CleanUpDeletedTables(absl::Time timestamp) {
  absl::MutexLock lock(mu_);
  absl::Time expiration_time = timestamp - version_retention_period_;

  // Every read-only read calls this, which keeps collecting versions while a
  // database is only read.
  CollectDueRows(timestamp, kMinRowsCollectedPerCall);

  // Remove expired dropped tables.
  for (auto it = dropped_tables_.begin();
       it != dropped_tables_.upper_bound(expiration_time);) {
    tables_.erase(it->second);
    it = dropped_tables_.erase(it);
  }
}

void InMemoryStorage::CleanUpDeletedColumns(absl::Time timestamp) {
  absl::MutexLock lock(mu_);
  absl::Time expiration_time = timestamp - version_retention_period_;

  // Remove expired dropped columns.
  for (auto it = dropped_columns_.begin();
       it != dropped_columns_.upper_bound(expiration_time);) {
    auto [table_id, column_id] = it->second;
    for (auto& [_, row] : tables_[table_id]) {
      row.erase(column_id);
    }
    it = dropped_columns_.erase(it);
  }
}

void InMemoryStorage::MarkDroppedTable(absl::Time timestamp,
                                       TableID dropped_table_id) {
  absl::MutexLock lock(mu_);
  dropped_tables_[timestamp] = dropped_table_id;
}

void InMemoryStorage::MarkDroppedColumn(absl::Time timestamp,
                                        TableID dropped_table_id,
                                        ColumnID dropped_column_id) {
  absl::MutexLock lock(mu_);
  dropped_columns_[timestamp] =
      std::make_pair(dropped_table_id, dropped_column_id);
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
