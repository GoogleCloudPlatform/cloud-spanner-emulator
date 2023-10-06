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

#include "tests/common/actions.h"

#include <cstdint>
#include <memory>
#include <queue>
#include <string>
#include <vector>

#include "zetasql/public/json_value.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "backend/actions/ops.h"
#include "backend/common/rows.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/key_range.h"
#include "backend/schema/catalog/change_stream.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
#include "backend/storage/iterator.h"
#include "common/constants.h"
#include "common/errors.h"
#include "common/limits.h"
#include "nlohmann/json_fwd.hpp"
#include "nlohmann/json.hpp"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {
using JSON = ::nlohmann::json;

absl::Status TestReadOnlyStore::Insert(
    const Table* table, const Key& key, absl::Span<const Column* const> columns,
    const std::vector<zetasql::Value>& values) {
  return store_.Write(absl::InfiniteFuture(), table->id(), key,
                      GetColumnIDs(columns), values);
}

absl::StatusOr<bool> TestReadOnlyStore::Exists(const Table* table,
                                               const Key& key) const {
  absl::Status s =
      store_.Lookup(absl::InfiniteFuture(), table->id(), key, {}, {});
  if (s.code() == absl::StatusCode::kNotFound) {
    return false;
  } else if (!s.ok()) {
    return s;
  }
  return true;
}

absl::StatusOr<bool> TestReadOnlyStore::PrefixExists(
    const Table* table, const Key& prefix_key) const {
  std::unique_ptr<StorageIterator> itr;
  ZETASQL_RETURN_IF_ERROR(store_.Read(absl::InfiniteFuture(), table->id(),
                              KeyRange::Point(prefix_key), {}, &itr));
  if (itr->Next() && itr->Status().ok()) {
    return true;
  }
  ZETASQL_RETURN_IF_ERROR(itr->Status());
  return false;
}

absl::StatusOr<std::unique_ptr<StorageIterator>> TestReadOnlyStore::Read(
    const Table* table, const KeyRange& key_range,
    const absl::Span<const Column* const> columns) const {
  std::unique_ptr<StorageIterator> itr;
  ZETASQL_RETURN_IF_ERROR(store_.Read(absl::InfiniteFuture(), table->id(), key_range,
                              GetColumnIDs(columns), &itr));
  return itr;
}

void TestEffectsBuffer::Insert(const Table* table, const Key& key,
                               const absl::Span<const Column* const> columns,
                               const std::vector<zetasql::Value>& values) {
  ops_queue_->push(
      InsertOp{table, key, {columns.begin(), columns.end()}, values});
}

void TestEffectsBuffer::Update(const Table* table, const Key& key,
                               const absl::Span<const Column* const> columns,
                               const std::vector<zetasql::Value>& values) {
  ops_queue_->push(
      UpdateOp{table, key, {columns.begin(), columns.end()}, values});
}

void TestEffectsBuffer::Delete(const Table* table, const Key& key) {
  ops_queue_->push(DeleteOp{table, key});
}

std::string ToFragmentIdString(int64_t record_sequence) {
  return absl::StrFormat("%08d", record_sequence);
}

// Mods inside one DataChangeRecord have the same set of mod type, user table,
// tracked non-key columns, and change stream.
DataChangeRecord TestChangeStreamEffectsBuffer::BuildDataChangeRecord(
    std::string tracked_table_name, std::string value_capture_type,
    const ChangeStream* change_stream) {
  std::vector<ColumnType> column_types =
      last_mod_group_by_change_stream_[change_stream].column_types;
  std::string record_sequence = ToFragmentIdString(
      record_sequence_by_change_stream_[change_stream->Name()]);
  DataChangeRecord record{
      last_mod_group_by_change_stream_[change_stream].partition_token_str,
      zetasql::Value::Timestamp(kCommitTimestampValueSentinel),
      std::to_string(transaction_id_), record_sequence, false,
      tracked_table_name, column_types,
      last_mod_group_by_change_stream_[change_stream].mods,
      last_mod_group_by_change_stream_[change_stream].mod_type,
      value_capture_type,
      -1,  // number_of_records_in_transaction will be reset after processing
           // all mods in one transaction
      1, "", false};
  record_sequence_by_change_stream_[change_stream->Name()]++;
  return record;
}

absl::StatusOr<Key>
TestChangeStreamEffectsBuffer::ComputeChangeStreamDataTableKey(
    zetasql::Value partition_token_str, zetasql::Value commit_timestamp,
    std::string server_transaction_id, std::string record_sequence,
    std::string table_name) {
  Key key;
  key.AddColumn(partition_token_str, false);
  key.AddColumn(commit_timestamp, false);
  key.AddColumn(zetasql::Value::String(server_transaction_id), false);
  key.AddColumn(zetasql::Value::String(record_sequence), false);
  return key;
}

absl::StatusOr<WriteOp>
TestChangeStreamEffectsBuffer::ConvertDataChangeRecordToWriteOp(
    const ChangeStream* change_stream, DataChangeRecord record) {
  // Compute change_stream_data_table key
  ZETASQL_ASSIGN_OR_RETURN(Key change_stream_data_table_key,
                   ComputeChangeStreamDataTableKey(
                       record.partition_token, record.commit_timestamp,
                       record.server_transaction_id, record.record_sequence,
                       change_stream->change_stream_data_table()->Name()));
  std::vector<const Column*> columns;
  for (const Column* column :
       change_stream->change_stream_data_table()->columns()) {
    columns.push_back(column);
  }

  std::vector<zetasql::Value> values;
  values.push_back(record.partition_token);
  values.push_back(record.commit_timestamp);
  values.push_back(zetasql::Value::String(record.server_transaction_id));
  values.push_back(zetasql::Value::String(record.record_sequence));
  values.push_back(zetasql::Value::Bool(
      record.is_last_record_in_transaction_in_partition));
  values.push_back(zetasql::Value::String(record.tracked_table_name));

  std::vector<zetasql::JSONValue> column_types;
  for (const ColumnType& column_type : record.column_types) {
    JSON column_type_json;
    column_type_json["name"] = column_type.name;
    JSON type_json;
    type_json["code"] = column_type.type;
    column_type_json["type"] = type_json.dump();
    column_type_json["is_primary_key"] = column_type.is_primary_key;
    column_type_json["ordinal_position"] = column_type.ordinal_position;
    column_types.push_back(
        zetasql::JSONValue::ParseJSONString(column_type_json.dump()).value());
  }
  values.push_back(zetasql::values::JsonArray(column_types));
  std::vector<zetasql::JSONValue> mods;
  for (const Mod& mod : record.mods) {
    JSON mod_json;
    JSON keys_json;
    for (int i = 0; i < mod.key_columns.size(); ++i) {
      keys_json[mod.key_columns[i]->column()->Name()] =
          mod.keys[i].type_kind() == zetasql::TYPE_STRING
              ? mod.keys[i].string_value()
              : mod.keys[i].GetSQLLiteral();
    }
    JSON new_values_json;
    for (int i = 0; i < mod.new_values.size(); ++i) {
      new_values_json[mod.non_key_columns[i]] =
          mod.new_values[i].type_kind() == zetasql::TYPE_STRING
              ? mod.new_values[i].string_value()
              : mod.new_values[i].GetSQLLiteral();
    }
    mod_json["keys"] = keys_json.dump();
    mod_json["new_values"] = new_values_json.dump();
    if (mod.new_values.empty()) {
      mod_json["new_values"] = kMinimumValidJson;
    }
    // OLD_AND_NEW_VALUES is not supported yet so field old_value is always an
    // empty "{}"
    mod_json["old_values"] = kMinimumValidJson;
    mods.push_back(
        zetasql::JSONValue::ParseJSONString(mod_json.dump()).value());
  }
  values.push_back(zetasql::values::JsonArray(mods));
  values.push_back(zetasql::Value::String(record.mod_type));
  values.push_back(zetasql::Value::String(record.value_capture_type));
  values.push_back(
      zetasql::Value::Int64(record.number_of_records_in_transaction));
  values.push_back(
      zetasql::Value::Int64(record.number_of_partitions_in_transaction));
  values.push_back(zetasql::Value::String(record.transaction_tag));
  values.push_back(zetasql::Value::Bool(record.is_system_transaction));
  return InsertOp{change_stream->change_stream_data_table(),
                  change_stream_data_table_key, columns, values};
}

bool IsPrimaryKey(const Table* table, const Column* column) {
  bool is_primary_key = false;
  for (const KeyColumn* pk : table->primary_key()) {
    if (pk->column()->Name() == column->Name()) {
      is_primary_key = true;
      break;
    }
  }
  return is_primary_key;
}

int64_t GetOrdinalPosition(const Table* table, const Column* column) {
  int64_t ordinal_pos = 1;
  for (const Column* col : table->columns()) {
    if (col->Name() == column->Name()) {
      break;
    }
    ordinal_pos++;
  }
  return ordinal_pos;
}

// TODO: Remove the statement to exclude key columns from op_columns
bool CheckIfNonKeyColumnsRemainSame(std::vector<const Column*> op_columns,
                                    ModGroup last_mod_group, const Table* table,
                                    const ChangeStream* change_stream) {
  std::vector<const Column*> op_non_key_columns_tracked_by_change_stream;
  absl::flat_hash_set<std::string> op_key_cols;
  for (const KeyColumn* pk : table->primary_key()) {
    op_key_cols.insert(pk->column()->Name());
  }
  for (const Column* column : op_columns) {
    if (!op_key_cols.contains(column->Name()) &&
        column->FindChangeStream(change_stream->Name())) {
      op_non_key_columns_tracked_by_change_stream.push_back(column);
    }
  }
  bool same_non_pk_columns = last_mod_group.non_key_column_names.size() ==
                             op_non_key_columns_tracked_by_change_stream.size();
  if (same_non_pk_columns) {
    for (const Column* column : op_non_key_columns_tracked_by_change_stream) {
      if (!last_mod_group.non_key_column_names.contains(column->Name())) {
        same_non_pk_columns = false;
        break;
      }
    }
  }
  return same_non_pk_columns;
}

// Accumulate tracked column types and values for same DataChangeRecord
void TestChangeStreamEffectsBuffer::LogTableMod(
    const Key& key, std::vector<const Column*> columns,
    const std::vector<zetasql::Value>& values, const Table* tracked_table,
    const ChangeStream* change_stream, std::string mod_type,
    zetasql::Value partition_token_str) {
  if (last_mod_group_by_change_stream_.contains(change_stream)) {
    ModGroup last_mod_group = last_mod_group_by_change_stream_[change_stream];
    bool same_non_pk_columns = CheckIfNonKeyColumnsRemainSame(
        columns, last_mod_group, tracked_table, change_stream);
    if (last_mod_group.mod_type != mod_type ||
        last_mod_group.table_name != tracked_table->Name() ||
        !same_non_pk_columns) {
      DataChangeRecord record = BuildDataChangeRecord(
          last_mod_group.table_name,
          change_stream->value_capture_type().value(), change_stream);
      last_mod_group_by_change_stream_.erase(change_stream);
      data_change_records_in_transaction_by_change_stream_[change_stream]
          .push_back(record);
    }
  }

  std::vector<zetasql::Value> new_values_for_tracked_cols;
  std::vector<ColumnType> column_types;
  std::vector<std::string> non_key_cols;
  for (int i = 0; i < columns.size(); ++i) {
    const Column* column = columns[i];
    bool is_primary_key = IsPrimaryKey(tracked_table, column);
    // Add columns (key columns and non key columns) tracked by the change
    // stream to column_types. All key columns are tracked by change stream.
    if (column->FindChangeStream(change_stream->Name()) || is_primary_key) {
      int64_t ordinal_position = GetOrdinalPosition(tracked_table, column);
      ColumnType column_type{
          column->Name(),
          column->GetType()->TypeName(zetasql::PRODUCT_EXTERNAL),
          is_primary_key, ordinal_position};
      column_types.push_back(column_type);
      if (!is_primary_key) {
        new_values_for_tracked_cols.push_back(values[i]);
        non_key_cols.push_back(column->Name());
      }
    }
  }

  if (!new_values_for_tracked_cols.empty() || mod_type != kUpdate) {
    if (!last_mod_group_by_change_stream_.contains(change_stream)) {
      last_mod_group_by_change_stream_[change_stream] =
          ModGroup{.table_name = tracked_table->Name(),
                   .mod_type = mod_type,
                   .non_key_column_names = {},
                   .column_types = {},
                   .mods = {},
                   .partition_token_str = partition_token_str};
    }
    last_mod_group_by_change_stream_[change_stream].table_name =
        tracked_table->Name();
    last_mod_group_by_change_stream_[change_stream].mod_type = mod_type;
    last_mod_group_by_change_stream_[change_stream].non_key_column_names = {
        non_key_cols.begin(), non_key_cols.end()};
    last_mod_group_by_change_stream_[change_stream].column_types = {
        column_types.begin(), column_types.end()};
    Mod mod{tracked_table->primary_key(),
            non_key_cols,
            key.column_values(),
            new_values_for_tracked_cols,
            {}};
    last_mod_group_by_change_stream_[change_stream].mods.push_back(mod);
  }
}

void TestChangeStreamEffectsBuffer::Insert(zetasql::Value partition_token_str,
                                           const ChangeStream* change_stream,
                                           const InsertOp& op) {
  // update ModGroup's column_types, mods, and non-key columns
  LogTableMod(op.key, op.columns, op.values, op.table, change_stream, kInsert,
              partition_token_str);
}

void TestChangeStreamEffectsBuffer::Update(zetasql::Value partition_token_str,
                                           const ChangeStream* change_stream,
                                           const UpdateOp& op) {
  LogTableMod(op.key, op.columns, op.values, op.table, change_stream, kUpdate,
              partition_token_str);
}

void TestChangeStreamEffectsBuffer::Delete(zetasql::Value partition_token_str,
                                           const ChangeStream* change_stream,
                                           const DeleteOp& op) {
  std::vector<const Column*> columns;
  for (const KeyColumn* pk : op.table->primary_key()) {
    columns.push_back(pk->column());
  }
  LogTableMod(op.key, columns, {}, op.table, change_stream, kDelete,
              partition_token_str);
}

// Set number_of_records_in_transaction and build the WriteOp for
// change_stream_data_table
void TestChangeStreamEffectsBuffer::BuildMutation() {
  // After the last user WriteOp passed into this buffer, there may be grouped
  // column types and mods by change streams that haven't been converted to
  // DataChangeRecord. Build them into DataChangeRecords before setting
  // is_last_record_in_transaction_in_partition and
  // number_of_records_in_transaction.
  for (auto& [change_stream, mod_group] : last_mod_group_by_change_stream_) {
    DataChangeRecord record = BuildDataChangeRecord(
        mod_group.table_name, change_stream->value_capture_type().value(),
        change_stream);
    if (!data_change_records_in_transaction_by_change_stream_.contains(
            change_stream)) {
      data_change_records_in_transaction_by_change_stream_[change_stream] =
          std::vector<DataChangeRecord>();
    }
    data_change_records_in_transaction_by_change_stream_[change_stream]
        .push_back(record);
  }
  last_mod_group_by_change_stream_.clear();
  for (auto& [change_stream, records] :
       data_change_records_in_transaction_by_change_stream_) {
    int64_t number_of_records_in_transaction = records.size();
    data_change_records_in_transaction_by_change_stream_
        [change_stream][number_of_records_in_transaction - 1]
            .is_last_record_in_transaction_in_partition = true;
    for (DataChangeRecord record : records) {
      record.number_of_records_in_transaction =
          number_of_records_in_transaction;
      writeops_.push_back(
          ConvertDataChangeRecordToWriteOp(change_stream, record).value());
    }
  }
}

std::vector<WriteOp> TestChangeStreamEffectsBuffer::GetWriteOps() {
  return writeops_;
}

void TestChangeStreamEffectsBuffer::ClearWriteOps() { writeops_.clear(); }

WriteOp ActionsTest::Insert(const Table* table, const Key& key,
                            absl::Span<const Column* const> columns,
                            const std::vector<zetasql::Value> values) {
  return InsertOp{table, key, {columns.begin(), columns.end()}, values};
}

WriteOp ActionsTest::Update(const Table* table, const Key& key,
                            absl::Span<const Column* const> columns,
                            const std::vector<zetasql::Value> values) {
  return UpdateOp{table, key, {columns.begin(), columns.end()}, values};
}

WriteOp ActionsTest::Delete(const Table* table, const Key& key) {
  return DeleteOp{table, key};
}

}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
