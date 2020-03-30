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

#include "backend/transaction/resolve.h"

#include "backend/datamodel/key_set.h"
#include "common/errors.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

KeySet SetSortOrder(const Table* table, const KeySet& key_set) {
  KeySet result_set;
  for (auto& key : key_set.keys()) {
    Key new_key(key);
    // Set ascending and descending values for the keys.
    for (int i = 0; i < new_key.NumColumns(); ++i) {
      new_key.SetColumnDescending(i, table->primary_key()[i]->is_descending());
    }
    result_set.AddKey(new_key);
  }

  for (auto& range : key_set.ranges()) {
    KeyRange new_range(range);
    // Set ascending and descending values for the keys.
    for (int i = 0; i < range.start_key().NumColumns(); ++i) {
      new_range.start_key().SetColumnDescending(
          i, table->primary_key()[i]->is_descending());
    }
    for (int i = 0; i < range.limit_key().NumColumns(); ++i) {
      new_range.limit_key().SetColumnDescending(
          i, table->primary_key()[i]->is_descending());
    }
    result_set.AddRange(new_range);
  }
  return result_set;
}

}  // namespace

zetasql_base::StatusOr<ResolvedReadArg> ResolveReadArg(const ReadArg& read_arg,
                                               const Schema* schema) {
  // Find the table to read from schema.
  auto read_table = schema->FindTable(read_arg.table);
  if (read_table == nullptr) {
    return error::TableNotFound(read_arg.table);
  }

  // Check if index should be read from instead.
  const Index* index = nullptr;
  if (!read_arg.index.empty()) {
    index = schema->FindIndex(read_arg.index);
    if (index == nullptr) {
      return error::IndexNotFound(read_arg.index, read_arg.table);
    }
    if (index->indexed_table() != read_table) {
      return error::IndexTableDoesNotMatchBaseTable(
          read_table->Name(), index->indexed_table()->Name(), index->Name());
    }
    read_table = index->index_data_table();
  }

  // Find the columns to read from schema.
  std::vector<const Column*> columns;
  for (const auto& column_name : read_arg.columns) {
    const Column* column = read_table->FindColumn(column_name);
    if (column == nullptr) {
      if (index == nullptr) {
        return error::ColumnNotFound(read_table->Name(), column_name);
      }
      // Check if column exists in base table and not in index table.
      const Column* base_column =
          index->indexed_table()->FindColumn(column_name);
      if (base_column == nullptr) {
        return error::ColumnNotFound(index->indexed_table()->Name(),
                                     column_name);
      }
      return error::ColumnNotFoundInIndex(
          index->Name(), index->indexed_table()->Name(), column_name);
    }
    columns.push_back(column);
  }

  // Convert key set to canonicalized key ranges.
  std::vector<KeyRange> key_ranges;
  CanonicalizeKeySetForTable(read_arg.key_set, read_table, &key_ranges);

  ResolvedReadArg resolved_read_arg;
  resolved_read_arg.table = read_table;
  resolved_read_arg.key_ranges = std::move(key_ranges);
  resolved_read_arg.columns = columns;
  return resolved_read_arg;
}

void CanonicalizeKeySetForTable(const KeySet& set, const Table* table,
                                std::vector<KeyRange>* ranges) {
  KeySet ordered_set = SetSortOrder(table, set);
  MakeDisjointKeyRanges(ordered_set, ranges);
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
