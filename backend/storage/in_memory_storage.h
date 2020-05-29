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

#include "zetasql/public/value.h"
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
// sorted in order of the timestamp written. Keys are never deleted, but are
// marked deleted for multi-version lookup.
//
// Lookup and Read return invalid zetasql::Value(s) for non-existent columns.
//
// This class is thread-safe.
class InMemoryStorage : public Storage {
 public:
  absl::Status Lookup(absl::Time timestamp, const TableID& table_id,
                      const Key& key, const std::vector<ColumnID>& column_ids,
                      std::vector<zetasql::Value>* values) const override
      ABSL_LOCKS_EXCLUDED(mu_);

  absl::Status Read(absl::Time timestamp, const TableID& table_id,
                    const KeyRange& key_range,
                    const std::vector<ColumnID>& column_ids,
                    std::unique_ptr<StorageIterator>* itr) const override
      ABSL_LOCKS_EXCLUDED(mu_);

  absl::Status Write(absl::Time timestamp, const TableID& table_id,
                     const Key& key, const std::vector<ColumnID>& column_ids,
                     const std::vector<zetasql::Value>& values) override
      ABSL_LOCKS_EXCLUDED(mu_);

  absl::Status Delete(absl::Time timestamp, const TableID& table_id,
                      const KeyRange& key_range) override
      ABSL_LOCKS_EXCLUDED(mu_);

 private:
  using Cell = std::map<absl::Time, zetasql::Value>;
  using Row = absl::flat_hash_map<ColumnID, Cell>;
  using Table = std::map<Key, Row>;
  using Tables = absl::flat_hash_map<TableID, Table>;

  // Returns true if the given row is valid at the specified timestamp.
  bool Exists(const Row& row, absl::Time timestamp) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  // Returns the value for given row and column_id at the specified timestamp.
  zetasql::Value GetCellValueAtTimestamp(const Row& row,
                                           const ColumnID& column_id,
                                           absl::Time timestamp) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  mutable absl::Mutex mu_;
  Tables tables_ ABSL_GUARDED_BY(mu_);
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_IN_MEMORY_STORAGE_H_
