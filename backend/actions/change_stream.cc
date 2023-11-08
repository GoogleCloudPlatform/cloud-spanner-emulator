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

#include "backend/actions/change_stream.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "zetasql/public/json_value.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "google/cloud/spanner/bytes.h"
#include "backend/actions/context.h"
#include "backend/actions/ops.h"
#include "backend/common/ids.h"
#include "backend/common/variant.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/key_range.h"
#include "backend/schema/catalog/change_stream.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "backend/storage/iterator.h"
#include "common/constants.h"
#include "common/errors.h"
#include "common/limits.h"
#include "nlohmann/json_fwd.hpp"
#include "nlohmann/json.hpp"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"
namespace google {
namespace spanner {
namespace emulator {
namespace backend {
using ::zetasql::JSONValueRef;
using JSON = ::nlohmann::json;

static constexpr absl::string_view kInsert = "INSERT";
static constexpr absl::string_view kUpdate = "UPDATE";
static constexpr absl::string_view kDelete = "DELETE";
static constexpr absl::string_view kMinimumValidJson = "{}";
static constexpr absl::string_view kArray = "ARRAY";

absl::flat_hash_map<const Table*, std::vector<const ChangeStream*>>
RetrieveTableWithTrackedChangeStreams(const Schema* schema) {
  absl::flat_hash_map<const Table*, std::vector<const ChangeStream*>>
      table_with_tracked_change_streams;
  for (const ChangeStream* change_stream : schema->change_streams()) {
    absl::flat_hash_map<std::string, std::vector<std::string>>
        tracked_tables_columns = change_stream->tracked_tables_columns();
    for (const auto& [table, columns] : tracked_tables_columns) {
      table_with_tracked_change_streams[schema->FindTable(table)].emplace_back(
          change_stream);
    }
  }
  return table_with_tracked_change_streams;
}

absl::StatusOr<zetasql::Value> RetrieveChangeStreamWithPartitionToken(
    ReadOnlyStore* store, const ChangeStream* change_stream) {
  std::vector<const Column*> read_columns = {
      change_stream->change_stream_partition_table()->FindColumn(
          "partition_token"),
      change_stream->change_stream_partition_table()->FindColumn("end_time")};
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<StorageIterator> itr,
                   store->Read(change_stream->change_stream_partition_table(),
                               KeyRange::All(), read_columns));
  std::vector<std::string> active_partition_tokens;
  while (itr->Next()) {
    // Find active partitions by filtering partitions with the end_time equal
    // to the default null end_timestamp. Stale partitions' end_timestamps are
    // set to the transaction commit timestamp.
    if (itr->ColumnValue(1).is_null()) {
      active_partition_tokens.push_back(itr->ColumnValue(0).string_value());
    }
  }
  if (active_partition_tokens.size() > 1) {
    ABSL_LOG(ERROR) << "More than 1 active partition token exists in "
               << change_stream->change_stream_partition_table()->Name();
  }
  std::sort(active_partition_tokens.begin(), active_partition_tokens.end());
  return zetasql::Value::String(active_partition_tokens[0]);
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

// Compare if the list of non-key columns tracked by the change stream in this
// writeOp is the same as that from last mod group.
bool CheckIfNonKeyColumnsRemainSame(std::vector<const Column*> op_columns,
                                    ModGroup last_mod_group, const Table* table,
                                    const ChangeStream* change_stream) {
  std::vector<const Column*> op_non_key_columns_tracked_by_change_stream;
  for (const Column* column : op_columns) {
    if (column->FindChangeStream(change_stream->Name())) {
      op_non_key_columns_tracked_by_change_stream.push_back(column);
    }
  }
  bool same_non_pk_columns =
      op_non_key_columns_tracked_by_change_stream.size() ==
      last_mod_group.non_key_column_names.size();
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

std::string ToFragmentIdString(int64_t record_sequence) {
  return absl::StrFormat("%08d", record_sequence);
}

// Mods inside one DataChangeRecord have the same set of mod type, user table,
// tracked non-key columns, and change stream.
DataChangeRecord BuildDataChangeRecord(
    std::string tracked_table_name, std::string value_capture_type,
    const ChangeStream* change_stream, TransactionID transaction_id,
    int64_t record_sequence_number,
    absl::flat_hash_map<const ChangeStream*, ModGroup>*
        last_mod_group_by_change_stream) {
  std::vector<ColumnType> column_types =
      (*last_mod_group_by_change_stream)[change_stream].column_types;
  std::string record_sequence = ToFragmentIdString(record_sequence_number);
  DataChangeRecord record{
      (*last_mod_group_by_change_stream)[change_stream].partition_token_str,
      zetasql::Value::Timestamp(kCommitTimestampValueSentinel),
      std::to_string(transaction_id), record_sequence, false,
      tracked_table_name, column_types,
      (*last_mod_group_by_change_stream)[change_stream].mods,
      (*last_mod_group_by_change_stream)[change_stream].mod_type,
      value_capture_type,
      -1,  // number_of_records_in_transaction will be reset after processing
           // all mods in one transaction
      1, "", false};
  return record;
}

// Accumulate tracked column types and values for same DataChangeRecord
void LogTableMod(
    const Key& key, std::vector<const Column*> columns,
    std::vector<zetasql::Value> values, const Table* tracked_table,
    const ChangeStream* change_stream, absl::string_view mod_type,
    zetasql::Value partition_token,
    absl::flat_hash_map<const ChangeStream*, std::vector<DataChangeRecord>>*
        data_change_records_in_transaction_by_change_stream,
    TransactionID transaction_id,
    absl::flat_hash_map<const ChangeStream*, ModGroup>*
        last_mod_group_by_change_stream) {
  // For INSERT, column_types should also contain all tracked but not populated
  // columns in the table
  if (mod_type == kInsert) {
    absl::flat_hash_map<const Column*, zetasql::Value> col_to_value;
    for (int i = 0; i < columns.size(); ++i) {
      col_to_value[columns[i]] = values[i];
    }
    std::vector<const Column*> all_columns;
    std::vector<zetasql::Value> all_values;
    for (const Column* col : tracked_table->columns()) {
      all_columns.push_back(col);
      if (col_to_value.contains(col)) {
        all_values.push_back(col_to_value[col]);
      } else {
        all_values.push_back(zetasql::Value::NullString());
      }
    }
    columns = std::move(all_columns);
    values = std::move(all_values);
  }
  if (last_mod_group_by_change_stream->contains(change_stream)) {
    ModGroup last_mod_group = (*last_mod_group_by_change_stream)[change_stream];
    bool same_non_pk_columns = CheckIfNonKeyColumnsRemainSame(
        columns, last_mod_group, tracked_table, change_stream);
    if (last_mod_group.mod_type != mod_type ||
        last_mod_group.table_name != tracked_table->Name() ||
        !same_non_pk_columns) {
      DataChangeRecord record = BuildDataChangeRecord(
          last_mod_group.table_name,
          change_stream->value_capture_type().has_value()
              ? change_stream->value_capture_type().value()
              : std::string(kChangeStreamValueCaptureTypeDefault),
          change_stream, transaction_id,
          (*data_change_records_in_transaction_by_change_stream)[change_stream]
              .size(),
          last_mod_group_by_change_stream);
      last_mod_group_by_change_stream->erase(change_stream);
      (*data_change_records_in_transaction_by_change_stream)[change_stream]
          .push_back(record);
    }
  }

  std::vector<zetasql::Value> new_values_for_tracked_cols;
  std::vector<ColumnType> column_types;
  std::vector<std::string> non_key_cols;
  // For DELETE, column_types should contain all tracked columns in the table
  if (mod_type == kDelete) {
    columns = {tracked_table->columns().begin(),
               tracked_table->columns().end()};
  }
  for (int i = 0; i < columns.size(); ++i) {
    const Column* column = columns[i];
    bool is_primary_key = IsPrimaryKey(tracked_table, column);
    // Add columns (key columns and non key columns) tracked by the change
    // stream to column_types. All key columns are tracked by change stream.
    if (column->FindChangeStream(change_stream->Name()) || is_primary_key) {
      int64_t ordinal_position = GetOrdinalPosition(tracked_table, column);
      ColumnType column_type{column->Name(), column->GetType(), is_primary_key,
                             ordinal_position};
      column_types.push_back(column_type);
      if (!is_primary_key && mod_type != kDelete) {
        new_values_for_tracked_cols.push_back(values[i]);
        non_key_cols.push_back(column->Name());
      }
    }
  }
  std::sort(
      column_types.begin(), column_types.end(),
      [](const ColumnType& col_type_a, const ColumnType& col_type_b) {
        return (col_type_a.ordinal_position < col_type_b.ordinal_position);
      });
  if (!new_values_for_tracked_cols.empty() || mod_type != kUpdate) {
    if (!last_mod_group_by_change_stream->contains(change_stream)) {
      (*last_mod_group_by_change_stream)[change_stream] =
          ModGroup{.table_name = tracked_table->Name(),
                   .mod_type = mod_type,
                   .non_key_column_names = {},
                   .column_types = {},
                   .mods = {},
                   .partition_token_str = partition_token};
    }
    (*last_mod_group_by_change_stream)[change_stream].table_name =
        tracked_table->Name();
    (*last_mod_group_by_change_stream)[change_stream].non_key_column_names = {
        non_key_cols.begin(), non_key_cols.end()};
    (*last_mod_group_by_change_stream)[change_stream].column_types = {
        column_types.begin(), column_types.end()};
    Mod mod{tracked_table->primary_key(),
            non_key_cols,
            key.column_values(),
            new_values_for_tracked_cols,
            {}};
    (*last_mod_group_by_change_stream)[change_stream].mods.push_back(mod);
  }
}

void LogTableMod(
    WriteOp op, const ChangeStream* change_stream,
    zetasql::Value partition_token,
    absl::flat_hash_map<const ChangeStream*, std::vector<DataChangeRecord>>*
        data_change_records_in_transaction_by_change_stream,
    TransactionID transaction_id,
    absl::flat_hash_map<const ChangeStream*, ModGroup>*
        last_mod_group_by_change_stream) {
  std::visit(
      overloaded{
          [&](const InsertOp& op) {
            LogTableMod(op.key, op.columns, op.values, op.table, change_stream,
                        kInsert, partition_token,
                        data_change_records_in_transaction_by_change_stream,
                        transaction_id, last_mod_group_by_change_stream);
          },
          [&](const UpdateOp& op) {
            LogTableMod(op.key, op.columns, op.values, op.table, change_stream,
                        kUpdate, partition_token,
                        data_change_records_in_transaction_by_change_stream,
                        transaction_id, last_mod_group_by_change_stream);
          },
          [&](const DeleteOp& op) {
            std::vector<const Column*> columns;
            for (const KeyColumn* pk : op.table->primary_key()) {
              columns.push_back(pk->column());
            }
            LogTableMod(op.key, columns, {}, op.table, change_stream, kDelete,
                        partition_token,
                        data_change_records_in_transaction_by_change_stream,
                        transaction_id, last_mod_group_by_change_stream);
          },
      },
      op);
}

void CloudValueToJSONValue(const zetasql::Value value, JSONValueRef& ref) {
  switch (value.type_kind()) {
    case zetasql::TYPE_DOUBLE: {
      ref.SetDouble(value.double_value());
      break;
    }
    case zetasql::TYPE_STRING: {
      ref.SetString(value.string_value());
      break;
    }
    case zetasql::TYPE_BOOL: {
      ref.SetBoolean(value.bool_value());
      break;
    }
    case zetasql::TYPE_DATE: {
      ref.SetString(value.Format(false));
      break;
    }
    case zetasql::TYPE_TIMESTAMP: {
      ref.SetString(absl::FormatTime("%Y-%m-%d%ET%H:%M:%SZ", value.ToTime(),
                                     absl::UTCTimeZone()));
      break;
    }
    case zetasql::TYPE_INT64: {
      ref.SetString(value.GetSQLLiteral());
      break;
    }
    case zetasql::TYPE_NUMERIC: {
      ref.SetString(value.Format(false));
      break;
    }
    case zetasql::TYPE_ARRAY: {
      if (value.num_elements() == 0) {
        ref.SetToEmptyArray();
      } else {
        for (int i = 0; i < value.num_elements(); i++) {
          JSONValueRef element_ref = ref.GetArrayElement(i);
          CloudValueToJSONValue(value.element(i), element_ref);
        }
      }
      break;
    }
    case zetasql::TYPE_BYTES: {
      std::string bytes_value = value.bytes_value();
      ref.SetString(google::cloud::spanner_internal::BytesToBase64(
          google::cloud::spanner::Bytes(value.bytes_value())));
      break;
    }
    case zetasql::TYPE_JSON: {
      ref.SetString(value.Format(false));
      break;
    }
    default: {
      break;
    }
  }
}

std::string CloudListValueToJSONString(
    std::vector<std::string> col_names,
    const std::vector<zetasql::Value> mod_new_values) {
  zetasql::JSONValue json_value;
  JSONValueRef ref = json_value.GetRef();
  for (int i = 0; i < col_names.size(); i++) {
    JSONValueRef col_ref = ref.GetMember(col_names[i]);
    if (mod_new_values[i].is_null()) {
      col_ref.SetNull();
    } else {
      CloudValueToJSONValue(mod_new_values[i], col_ref);
    }
  }
  return ref.ToString();
}

absl::StatusOr<Key> ComputeChangeStreamDataTableKey(
    zetasql::Value partition_token_str, zetasql::Value commit_timestamp,
    std::string record_sequence, std::string server_transaction_id,
    std::string table_name) {
  Key key;
  key.AddColumn(partition_token_str, false);
  key.AddColumn(commit_timestamp, false);
  key.AddColumn(zetasql::Value::String(server_transaction_id), false);
  key.AddColumn(zetasql::Value::String(record_sequence), false);
  const int64_t key_size = key.LogicalSizeInBytes();
  if (key_size > limits::kMaxKeySizeBytes) {
    return error::KeyTooLarge(table_name, key_size, limits::kMaxKeySizeBytes);
  }
  return key;
}

absl::StatusOr<WriteOp> ConvertDataChangeRecordToWriteOp(
    const ChangeStream* change_stream, DataChangeRecord record,
    std::vector<const Column*> columns) {
  // Compute change_stream_data_table key
  ZETASQL_ASSIGN_OR_RETURN(Key change_stream_data_table_key,
                   ComputeChangeStreamDataTableKey(
                       record.partition_token, record.commit_timestamp,
                       record.server_transaction_id, record.record_sequence,
                       change_stream->change_stream_data_table()->Name()));
  std::vector<zetasql::Value> values;
  values.push_back(record.partition_token);
  values.push_back(record.commit_timestamp);
  values.push_back(zetasql::Value::String(record.server_transaction_id));
  values.push_back(zetasql::Value::String(record.record_sequence));
  values.push_back(zetasql::Value::Bool(
      record.is_last_record_in_transaction_in_partition));
  values.push_back(zetasql::Value::String(record.tracked_table_name));
  std::vector<zetasql::Value> column_types_name;
  std::vector<zetasql::Value> column_types_type;
  std::vector<zetasql::Value> column_types_is_primary_key;
  std::vector<zetasql::Value> column_types_ordinal_position;
  for (const ColumnType& column_type : record.column_types) {
    column_types_name.push_back(zetasql::Value::String(column_type.name));
    JSON type_json;
    std::string element_type;
    if (column_type.type->IsArray()) {
      type_json["array_element_type"]["code"] =
          column_type.type->AsArray()->element_type()->TypeName(
              zetasql::PRODUCT_EXTERNAL);
      type_json["code"] = kArray;
    } else {
      type_json["code"] =
          column_type.type->TypeName(zetasql::PRODUCT_EXTERNAL);
    }
    column_types_type.push_back(zetasql::Value::String(type_json.dump()));
    column_types_is_primary_key.push_back(
        zetasql::Value::Bool(column_type.is_primary_key));
    column_types_ordinal_position.push_back(
        zetasql::Value::Int64(column_type.ordinal_position));
  }
  values.push_back(zetasql::values::Array(zetasql::types::StringArrayType(),
                                            column_types_name));
  values.push_back(zetasql::values::Array(zetasql::types::StringArrayType(),
                                            column_types_type));
  values.push_back(zetasql::values::Array(zetasql::types::BoolArrayType(),
                                            column_types_is_primary_key));
  values.push_back(zetasql::values::Array(zetasql::types::Int64ArrayType(),
                                            column_types_ordinal_position));
  std::vector<zetasql::Value> mods_keys;
  std::vector<zetasql::Value> mods_new_values;
  std::vector<zetasql::Value> mods_old_values;
  for (const Mod& mod : record.mods) {
    JSON mod_json;
    JSON keys_json;
    for (int i = 0; i < mod.key_columns.size(); ++i) {
      // In theory, key values can't be null but double check to avoid any
      // potential crash
      if (mod.keys[i].is_null()) {
        keys_json[mod.key_columns[i]->column()->Name()] = JSON::value_t::null;
      } else {
        keys_json[mod.key_columns[i]->column()->Name()] =
            mod.keys[i].type_kind() == zetasql::TYPE_STRING
                ? mod.keys[i].string_value()
                : mod.keys[i].GetSQLLiteral();
      }
    }
    std::string new_values_json_str =
        CloudListValueToJSONString(mod.non_key_columns, mod.new_values);
    mods_keys.push_back(zetasql::Value::String(keys_json.dump()));
    if (mod.new_values.empty()) {
      mods_new_values.push_back(zetasql::Value::String(kMinimumValidJson));
    } else {
      mods_new_values.push_back(zetasql::Value::String(new_values_json_str));
    }
    // OLD_AND_NEW_VALUES is not supported yet so field old_value is always an
    // empty "{}"
    mods_old_values.push_back(zetasql::Value::String(kMinimumValidJson));
  }
  values.push_back(
      zetasql::values::Array(zetasql::types::StringArrayType(), mods_keys));
  values.push_back(zetasql::values::Array(zetasql::types::StringArrayType(),
                                            mods_new_values));
  values.push_back(zetasql::values::Array(zetasql::types::StringArrayType(),
                                            mods_old_values));
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

// Set number_of_records_in_transaction and build the WriteOp for
// change_stream_data_table
std::vector<WriteOp> BuildMutation(
    absl::flat_hash_map<const ChangeStream*, std::vector<DataChangeRecord>>*
        data_change_records_in_transaction_by_change_stream,
    TransactionID transaction_id,
    absl::flat_hash_map<const ChangeStream*, ModGroup>*
        last_mod_group_by_change_stream) {
  std::vector<WriteOp> write_ops;
  // After the last user WriteOp passed into this buffer, there may be grouped
  // column types and mods by change streams that haven't been converted to
  // DataChangeRecord. Build them into DataChangeRecords before setting
  // is_last_record_in_transaction_in_partition and
  // number_of_records_in_transaction.
  for (auto& [change_stream, mod_group] : *last_mod_group_by_change_stream) {
    DataChangeRecord record = BuildDataChangeRecord(
        mod_group.table_name,
        change_stream->value_capture_type().has_value()
            ? change_stream->value_capture_type().value()
            : std::string(kChangeStreamValueCaptureTypeDefault),
        change_stream, transaction_id,
        (*data_change_records_in_transaction_by_change_stream)[change_stream]
            .size(),
        last_mod_group_by_change_stream);
    if (!data_change_records_in_transaction_by_change_stream->contains(
            change_stream)) {
      (*data_change_records_in_transaction_by_change_stream)[change_stream] =
          std::vector<DataChangeRecord>();
    }
    (*data_change_records_in_transaction_by_change_stream)[change_stream]
        .push_back(record);
  }
  for (auto& [change_stream, records] :
       *data_change_records_in_transaction_by_change_stream) {
    std::vector<const Column*> columns;
    // Each change_stream has one change_stream_data_table
    for (const Column* column :
         change_stream->change_stream_data_table()->columns()) {
      columns.push_back(column);
    }
    int64_t number_of_records_in_transaction = records.size();
    (*data_change_records_in_transaction_by_change_stream)
        [change_stream][number_of_records_in_transaction - 1]
            .is_last_record_in_transaction_in_partition = true;
    for (DataChangeRecord record : records) {
      record.number_of_records_in_transaction =
          number_of_records_in_transaction;
      write_ops.push_back(
          ConvertDataChangeRecordToWriteOp(change_stream, record, columns)
              .value());
    }
  }
  return write_ops;
}

std::vector<WriteOp> BuildChangeStreamWriteOps(
    const Schema* schema, std::vector<WriteOp> buffered_write_ops,
    ReadOnlyStore* store, TransactionID transaction_id) {
  // Map for change streams and their partition tokens within the transaction.
  absl::flat_hash_map<const ChangeStream*, zetasql::Value>
      change_stream_with_partition_token;
  // Map for tables and the change streams tracking the tables or columns
  // included in the tables.
  absl::flat_hash_map<const Table*, std::vector<const ChangeStream*>>
      table_with_tracked_change_streams =
          RetrieveTableWithTrackedChangeStreams(schema);
  // Map for change streams and their DataChangeRecords
  absl::flat_hash_map<const ChangeStream*, std::vector<DataChangeRecord>>
      data_change_records_in_transaction_by_change_stream;
  // Map for chagne streams and their ModGroups
  absl::flat_hash_map<const ChangeStream*, ModGroup>
      last_mod_group_by_change_stream;
  for (const auto& write_op : buffered_write_ops) {
    const Table* table = TableOf(write_op);
    for (const ChangeStream* change_stream :
         table_with_tracked_change_streams[table]) {
      if (!change_stream_with_partition_token.contains(change_stream)) {
        change_stream_with_partition_token[change_stream] =
            RetrieveChangeStreamWithPartitionToken(store, change_stream)
                .value();
      }
      LogTableMod(write_op, change_stream,
                  change_stream_with_partition_token[change_stream],
                  &data_change_records_in_transaction_by_change_stream,
                  transaction_id, &last_mod_group_by_change_stream);
    }
  }
  std::vector<WriteOp> write_ops =
      BuildMutation(&data_change_records_in_transaction_by_change_stream,
                    transaction_id, &last_mod_group_by_change_stream);
  return write_ops;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
