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

#include <memory>

#include "zetasql/public/value.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "backend/storage/in_memory_iterator.h"
#include "common/errors.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

static constexpr char kExistsColumn[] = "_exists";

}  // namespace

zetasql::Value InMemoryStorage::GetCellValueAtTimestamp(
    const Row& row, const ColumnID& column_id, absl::Time timestamp) const {
  // Perform the lookup for given cell.
  auto cell_itr = row.find(column_id);
  if (cell_itr == row.end()) {
    return zetasql::Value();
  }
  const Cell& cell = cell_itr->second;
  auto val_itr = cell.upper_bound(timestamp);

  // Timestamp is earlier than the time the cell was first written to.
  if (val_itr == cell.begin()) {
    return zetasql::Value();
  }

  // Fetch the value from the column.
  --val_itr;
  return val_itr->second;
}

bool InMemoryStorage::Exists(const Row& row, absl::Time timestamp) const {
  zetasql::Value value =
      GetCellValueAtTimestamp(row, kExistsColumn, timestamp);
  return value.is_valid() && value.bool_value();
}

absl::Status InMemoryStorage::Lookup(
    absl::Time timestamp, const TableID& table_id, const Key& key,
    const std::vector<ColumnID>& column_ids,
    std::vector<zetasql::Value>* values) const {
  absl::MutexLock lock(&mu_);

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
  absl::MutexLock lock(&mu_);

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
    *itr = absl::make_unique<FixedRowStorageIterator>();
    return absl::OkStatus();
  }

  // Lookup for given table.
  auto table_itr = tables_.find(table_id);
  if (table_itr == tables_.end()) {
    *itr = absl::make_unique<FixedRowStorageIterator>();
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

    std::vector<zetasql::Value> values;
    values.reserve(column_ids.size());
    for (const ColumnID& column_id : column_ids) {
      values.emplace_back(GetCellValueAtTimestamp(row, column_id, timestamp));
    }
    rows.emplace_back(std::make_pair(itr->first, values));
  }
  *itr = absl::make_unique<FixedRowStorageIterator>(std::move(rows));
  return absl::OkStatus();
}

absl::Status InMemoryStorage::Write(
    absl::Time timestamp, const TableID& table_id, const Key& key,
    const std::vector<ColumnID>& column_ids,
    const std::vector<zetasql::Value>& values) {
  absl::MutexLock lock(&mu_);

  // Add the table if it does not exist.
  Table& table = tables_[table_id];

  // Add the row with _exists system column if it does not exist.
  Row& row = table[key];
  if (!Exists(row, timestamp)) {
    row[kExistsColumn][timestamp] = zetasql::values::Bool(true);
  }

  // Add the values for the given columns.
  for (int i = 0; i < column_ids.size(); ++i) {
    row[column_ids[i]][timestamp] = values[i];
  }

  return absl::OkStatus();
}

absl::Status InMemoryStorage::Delete(absl::Time timestamp,
                                     const TableID& table_id,
                                     const KeyRange& key_range) {
  absl::MutexLock lock(&mu_);

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
  for (auto itr = row_start_itr; itr != row_end_itr; ++itr) {
    if (!Exists(itr->second, timestamp)) {
      continue;
    }

    for (const auto& columns : itr->second) {
      if (columns.first == kExistsColumn) {
        itr->second[kExistsColumn][timestamp] = zetasql::values::Bool(false);
      } else {
        // Column values are marked invalid zetasql::Value to avoid reading
        // the value of the cell before the delete.
        itr->second[columns.first][timestamp] = zetasql::Value();
      }
    }
  }
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
