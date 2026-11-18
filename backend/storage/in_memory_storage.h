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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_IN_MEMORY_STORAGE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_IN_MEMORY_STORAGE_H_

#include <deque>
#include <map>

#include "googlesql/public/value.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "backend/common/ids.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/key_range.h"
#include "backend/storage/iterator.h"
#include "backend/storage/storage.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// InMemoryStorage implements an in-memory multi-version data store.
//
// Keys are stored in sorted order. Value versions for a given column are also
// sorted in order of the timestamp written. A delete marks a key deleted for
// multi-version lookup.
//
// Versions are kept for the version retention period. Once a version has been
// superseded by one at least that old, no read the retention period allows can
// see it, and it is removed; so is a row whose newest such version is its
// delete. Every write that supersedes a version queues its row, and every call
// that writes, deletes or cleans up removes what the queue says is due, a
// bounded number of rows at a time (see CollectDueRows).
//
// Lookup and Read return invalid googlesql::Value(s) for non-existent columns.
//
// This class is thread-safe.
class InMemoryStorage : public Storage {
 public:
  absl::Status Lookup(absl::Time timestamp, const TableID& table_id,
                      const Key& key, const std::vector<ColumnID>& column_ids,
                      std::vector<googlesql::Value>* values) const override
      ABSL_LOCKS_EXCLUDED(mu_);

  absl::Status Read(absl::Time timestamp, const TableID& table_id,
                    const KeyRange& key_range,
                    const std::vector<ColumnID>& column_ids,
                    std::unique_ptr<StorageIterator>* itr) const override
      ABSL_LOCKS_EXCLUDED(mu_);

  absl::Status Write(absl::Time timestamp, const TableID& table_id,
                     const Key& key, const std::vector<ColumnID>& column_ids,
                     const std::vector<googlesql::Value>& values) override
      ABSL_LOCKS_EXCLUDED(mu_);

  absl::Status Delete(absl::Time timestamp, const TableID& table_id,
                      const KeyRange& key_range) override
      ABSL_LOCKS_EXCLUDED(mu_);

  void SetVersionRetentionPeriod(
      absl::Duration version_retention_period) override;

  void CleanUpDeletedTables(absl::Time timestamp) override
      ABSL_LOCKS_EXCLUDED(mu_);

  void CleanUpDeletedColumns(absl::Time timestamp) override
      ABSL_LOCKS_EXCLUDED(mu_);

  void MarkDroppedTable(absl::Time timestamp, TableID dropped_table_id) override
      ABSL_LOCKS_EXCLUDED(mu_);

  void MarkDroppedColumn(absl::Time timestamp, TableID dropped_table_id,
                         ColumnID dropped_column_id) override
      ABSL_LOCKS_EXCLUDED(mu_);

  absl::Time EarliestVersionTime() const override ABSL_LOCKS_EXCLUDED(mu_);

  // Removes every version and row that the queue holds as due by `now`, with
  // no bound on the work done. The write and delete paths do the same work a
  // bounded number of rows at a time.
  void CollectGarbage(absl::Time now) ABSL_LOCKS_EXCLUDED(mu_);

  // The number of rows queued for collection, due or not.
  size_t PendingGarbageRows() const ABSL_LOCKS_EXCLUDED(mu_);

 private:
  using Cell = std::map<absl::Time, googlesql::Value>;
  using Row = absl::flat_hash_map<ColumnID, Cell>;
  using Table = std::map<Key, Row>;
  using Tables = absl::flat_hash_map<TableID, Table>;

  // Returns true if the given row is valid at the specified timestamp.
  bool Exists(const Row& row, absl::Time timestamp) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  // Returns the value for given row and column_id at the specified timestamp.
  googlesql::Value GetCellValueAtTimestamp(const Row& row,
                                           const ColumnID& column_id,
                                           absl::Time timestamp) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  // A row that a write at `timestamp` gave a second version in some cell, or
  // deleted. Once `timestamp` is older than the retention period, the versions
  // it superseded are invisible to every read the retention period allows.
  struct GarbageRow {
    absl::Time timestamp;
    TableID table_id;
    Key key;
  };

  // Removes from `row` every version that a read at or after `horizon` cannot
  // see: in each cell all but the newest version at or before `horizon` and
  // those after it, and a cell left with only a delete marker at or before
  // `horizon`. Returns true if the row itself is deleted at `horizon` and has
  // no later version, so that no such read can see it at all.
  static bool CollectRow(Row& row, absl::Time horizon);

  // Collects the rows queued as due by `now`, at most `budget` of them.
  void CollectDueRows(absl::Time now, size_t budget)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  mutable absl::Mutex mu_;
  Tables tables_ ABSL_GUARDED_BY(mu_);

  // Tracks when tables were dropped so that we can clean up the data.
  std::map<absl::Time, TableID> dropped_tables_ ABSL_GUARDED_BY(mu_);

  // Tracks when columns were dropped so that we can clean up the data.
  std::map<absl::Time, std::pair<TableID, ColumnID>> dropped_columns_
      ABSL_GUARDED_BY(mu_);

  absl::Duration version_retention_period_ ABSL_GUARDED_BY(mu_) =
      absl::Hours(1);

  // Rows to collect, oldest write first.
  std::deque<GarbageRow> garbage_rows_ ABSL_GUARDED_BY(mu_);

  // The newest horizon any version was collected at.
  absl::Time earliest_version_time_ ABSL_GUARDED_BY(mu_) =
      absl::InfinitePast();
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_IN_MEMORY_STORAGE_H_
