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
#include <cstdint>
#include <iterator>
#include <memory>
#include <optional>
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

// The fewest due rows a write or delete collects. A call that queues more
// collects twice what it queued, so the queue drains at least as fast as it
// fills, while no call holds the lock for long on its account.
constexpr size_t kMinRowsCollectedPerCall = 256;

// The newest version at or before `timestamp`, or `cell.end()`.
template <typename CellT>
auto VersionAt(const CellT& cell, absl::Time timestamp) {
  // Nearly every cell has one or two versions, and a read is nearly always of
  // the newest, so look from the back.
  for (auto itr = cell.end(); itr != cell.begin();) {
    --itr;
    if (itr->timestamp <= timestamp) return itr;
  }
  return cell.end();
}

// Sets the version of `cell` at `timestamp`, replacing one already there.
// Returns true if the cell held a version before, which the new one may
// supersede.
template <typename CellT, typename V>
bool SetVersion(CellT& cell, absl::Time timestamp, V&& value) {
  const bool had_version = !cell.empty();
  if (cell.empty() || cell.back().timestamp < timestamp) {
    cell.push_back({timestamp, std::forward<V>(value)});
    return had_version;
  }
  auto itr = std::lower_bound(
      cell.begin(), cell.end(), timestamp,
      [](const auto& version, absl::Time t) { return version.timestamp < t; });
  if (itr != cell.end() && itr->timestamp == timestamp) {
    itr->value = std::forward<V>(value);
    return had_version;
  }
  cell.insert(itr, {timestamp, std::forward<V>(value)});
  return true;
}

// Removes the versions of `cell` that no read at or after `horizon` sees: all
// before the newest at or before it, and that one too if it is older than
// `floor`. Returns true if it removed any.
template <typename CellT>
bool CollectCell(CellT& cell, absl::Time horizon, absl::Time floor) {
  auto newest_due = VersionAt(cell, horizon);
  if (newest_due == cell.end()) return false;
  auto keep = newest_due->timestamp < floor ? std::next(newest_due) : newest_due;
  if (keep == cell.begin()) return false;
  cell.erase(cell.begin(), keep);
  // An inlined vector that grew to the heap goes back inline only when told.
  cell.shrink_to_fit();
  return true;
}

template <typename ColumnsT>
auto FindColumnIn(ColumnsT& columns, uint32_t ordinal) {
  auto itr = std::lower_bound(
      columns.begin(), columns.end(), ordinal,
      [](const auto& column, uint32_t o) { return column.ordinal < o; });
  return (itr != columns.end() && itr->ordinal == ordinal) ? itr
                                                            : columns.end();
}

}  // namespace

std::optional<absl::Time> InMemoryStorage::CreatedAt(const Row& row,
                                                     absl::Time timestamp) {
  auto itr = VersionAt(row.exists, timestamp);
  if (itr == row.exists.end() || !itr->value) return std::nullopt;
  return itr->timestamp;
}

googlesql::Value InMemoryStorage::CellValueAt(const Row& row,
                                              ColumnOrdinal ordinal,
                                              absl::Time created,
                                              absl::Time timestamp) {
  auto column = FindColumnIn(row.columns, ordinal);
  if (column == row.columns.end()) return googlesql::Value();
  auto version = VersionAt(column->cell, timestamp);
  // A version older than the row's creation belongs to a row since deleted.
  if (version == column->cell.end() || version->timestamp < created) {
    return googlesql::Value();
  }
  return version->value;
}

InMemoryStorage::ColumnOrdinal InMemoryStorage::OrdinalOf(
    const ColumnID& column_id) const {
  auto itr = column_ordinals_.find(column_id);
  return itr == column_ordinals_.end() ? kUnknownColumn : itr->second;
}

InMemoryStorage::ColumnOrdinal InMemoryStorage::InternColumn(
    const ColumnID& column_id) {
  return column_ordinals_
      .try_emplace(column_id,
                   static_cast<ColumnOrdinal>(column_ordinals_.size()))
      .first->second;
}

bool InMemoryStorage::CollectRow(Row& row, absl::Time horizon) {
  // Every read at or after the horizon sees the row created no earlier than
  // the newest version of its existence at or before the horizon, so no such
  // read sees a cell's version older than that.
  auto newest_due = VersionAt(row.exists, horizon);
  const absl::Time floor = newest_due == row.exists.end()
                               ? absl::InfinitePast()
                               : newest_due->timestamp;
  CollectCell(row.exists, horizon, absl::InfinitePast());
  if (row.exists.size() == 1 && row.exists.front().timestamp <= horizon &&
      !row.exists.front().value) {
    return true;
  }
  bool removed_column = false;
  for (auto itr = row.columns.begin(); itr != row.columns.end();) {
    CollectCell(itr->cell, horizon, floor);
    if (itr->cell.empty()) {
      itr = row.columns.erase(itr);
      removed_column = true;
    } else {
      ++itr;
    }
  }
  if (removed_column) row.columns.shrink_to_fit();
  return false;
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
  std::optional<absl::Time> created = CreatedAt(row, timestamp);
  if (!created.has_value()) {
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
  values->reserve(column_ids.size());
  for (const ColumnID& column_id : column_ids) {
    values->emplace_back(
        CellValueAt(row, OrdinalOf(column_id), *created, timestamp));
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

  // Number the columns once for the whole range, not once per row.
  std::vector<ColumnOrdinal> ordinals;
  ordinals.reserve(column_ids.size());
  for (const ColumnID& column_id : column_ids) {
    ordinals.push_back(OrdinalOf(column_id));
  }

  // Lookup keys from the given key range.
  auto row_start_itr = table.lower_bound(key_range.start_key());
  auto row_end_itr = table.lower_bound(key_range.limit_key());
  for (auto itr = row_start_itr; itr != row_end_itr; ++itr) {
    const InMemoryStorage::Row& row = itr->second;
    std::optional<absl::Time> created = CreatedAt(row, timestamp);
    if (!created.has_value()) {
      continue;
    }

    std::vector<googlesql::Value> values;
    values.reserve(ordinals.size());
    for (ColumnOrdinal ordinal : ordinals) {
      values.emplace_back(CellValueAt(row, ordinal, *created, timestamp));
    }
    rows.emplace_back(itr->first, std::move(values));
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

  // Add the row, and a version that says it exists if it does not.
  Row& row = table[key];
  // Whether this write gives a cell a second version, the first of which a
  // later collection can remove.
  bool supersedes = false;
  if (!CreatedAt(row, timestamp).has_value()) {
    supersedes |= SetVersion(row.exists, timestamp, true);
    // Commits write in timestamp order, but a write before a version that
    // already says the row exists makes that version's creation this one.
    auto next = std::upper_bound(
        row.exists.begin(), row.exists.end(), timestamp,
        [](absl::Time t, const ExistsVersion& v) { return t < v.timestamp; });
    if (next != row.exists.end() && next->value) {
      row.exists.erase(next);
    }
  }

  // Add the values for the given columns. Columns are numbered in the order
  // they are first written, so a row written column by column in the same
  // order as the first appends each one.
  if (row.columns.empty()) {
    row.columns.reserve(column_ids.size());
  }
  for (int i = 0; i < column_ids.size(); ++i) {
    const ColumnOrdinal ordinal = InternColumn(column_ids[i]);
    auto column = std::lower_bound(
        row.columns.begin(), row.columns.end(), ordinal,
        [](const Column& c, ColumnOrdinal o) { return c.ordinal < o; });
    if (column == row.columns.end() || column->ordinal != ordinal) {
      column = row.columns.insert(column, Column{ordinal, Cell()});
    }
    supersedes |= SetVersion(column->cell, timestamp, values[i]);
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
    Row& row = itr->second;
    if (!CreatedAt(row, timestamp).has_value()) {
      continue;
    }
    // A value written at this same timestamp, earlier in the same commit, is
    // deleted with the row: a row created again at this timestamp must not
    // read it.
    for (auto column = row.columns.begin(); column != row.columns.end();) {
      if (column->cell.back().timestamp == timestamp) {
        column->cell.pop_back();
      }
      if (column->cell.empty()) {
        column = row.columns.erase(column);
      } else {
        ++column;
      }
    }
    SetVersion(row.exists, timestamp, false);
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
    const ColumnOrdinal ordinal = OrdinalOf(column_id);
    auto table_itr = tables_.find(table_id);
    if (ordinal != kUnknownColumn && table_itr != tables_.end()) {
      for (auto& [_, row] : table_itr->second) {
        auto column = FindColumnIn(row.columns, ordinal);
        if (column != row.columns.end()) row.columns.erase(column);
      }
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
