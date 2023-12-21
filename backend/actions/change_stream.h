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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_CHANGE_STREAM_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_CHANGE_STREAM_H_

#include <cstdint>
#include <memory>

#include "absl/strings/string_view.h"
#include "backend/actions/action.h"
#include "backend/actions/ops.h"
#include "backend/schema/catalog/table.h"
#include "backend/storage/storage.h"
#include "backend/transaction/actions.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Each Mod stores the column and value pairs for keys, new_values, and
// old_values for one WriteOp.
struct Mod {
  const absl::Span<const KeyColumn* const> key_columns;
  std::vector<std::string> non_key_columns;
  const std::vector<zetasql::Value> keys;
  const std::vector<zetasql::Value> new_values;
  const std::vector<zetasql::Value> old_values;
};

// Each ColumnType stores the name, type, is_primary_key, and ordinal_position
// information for a column.
struct ColumnType {
  std::string name;
  const zetasql::Type* type;
  bool is_primary_key;
  int64_t ordinal_position;
};

// Each DataChangeRecord represents one row in change_stream_data_table and will
// be converted to one WriteOp to be written into the change_stream_data_table.
struct DataChangeRecord {
  zetasql::Value partition_token;
  zetasql::Value commit_timestamp;
  std::string server_transaction_id;
  std::string record_sequence;
  bool is_last_record_in_transaction_in_partition;
  std::string tracked_table_name;
  std::vector<ColumnType> column_types;
  std::vector<Mod> mods;
  absl::string_view mod_type;
  std::string value_capture_type;
  int64_t number_of_records_in_transaction;
  int64_t number_of_partitions_in_transaction;
  std::string transaction_tag;
  bool is_system_transaction;
};

// ModGroup stores the column_types and mods collected from WriteOps that should
// be built into the same DataChangeRecord. Specifically, these WriteOps are
// in the same mod_type, for the same change stream, for the same user table,
// and for the same set of tracked non key columns.
struct ModGroup {
  std::string table_name;
  absl::string_view mod_type;
  absl::flat_hash_set<std::string> non_key_column_names;
  // column_types contains ColumnType for all columns including key columns and
  // tracked non key columns
  std::vector<ColumnType> column_types;
  std::vector<Mod> mods;
  zetasql::Value partition_token_str;
};

// Group table mods belonging to the same DataChangeRecord into the same
// ModGroup.
absl::Status LogTableMod(
    WriteOp op, const ChangeStream* change_stream,
    zetasql::Value partition_token,
    absl::flat_hash_map<const ChangeStream*, std::vector<DataChangeRecord>>*
        data_change_records_in_transaction_by_change_stream,
    TransactionID transaction_id,
    absl::flat_hash_map<const ChangeStream*, ModGroup>*
        last_mod_group_by_change_stream,
    ReadOnlyStore* store);

// Build DataChangeRecords with ModGroups, set the
// number_of_records_in_transaction field of DataChangeRecords, and then convert
// DataChangeRecords to write_ops for change_stream_data_table.
std::vector<WriteOp> BuildMutation(
    absl::flat_hash_map<const ChangeStream*, std::vector<DataChangeRecord>>*
        data_change_records_in_transaction_by_change_stream,
    TransactionID transaction_id,
    absl::flat_hash_map<const ChangeStream*, ModGroup>*
        last_mod_group_by_change_stream);

// Build change stream write_ops.
absl::StatusOr<std::vector<WriteOp>> BuildChangeStreamWriteOps(
    const Schema* schema, std::vector<WriteOp> buffered_write_ops,
    ReadOnlyStore* store, TransactionID transaction_id);
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_COMMON_CHANGE_STREAM_H_
