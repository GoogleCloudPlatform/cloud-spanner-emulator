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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_ACTIONS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_ACTIONS_H_

#include <cstdint>
#include <memory>
#include <queue>
#include <vector>

#include "zetasql/public/value.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/time/time.h"
#include "backend/actions/context.h"
#include "backend/actions/ops.h"
#include "backend/datamodel/key_range.h"
#include "backend/schema/catalog/change_stream.h"
#include "backend/schema/catalog/table.h"
#include "backend/storage/iterator.h"
#include "backend/transaction/transaction_store.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

class ReadWriteTransaction;

// TransactionReadOnlyStore provides a storage for the actions executed within a
// transaction.
//
// The storage is backed by the TransactionStore and always performs the read at
// the latest timestamp.
class TransactionReadOnlyStore : public ReadOnlyStore {
 public:
  explicit TransactionReadOnlyStore(const TransactionStore* txn_store)
      : read_only_store_(txn_store) {}

  absl::StatusOr<bool> Exists(const Table* table,
                              const Key& key) const override;

  absl::StatusOr<bool> PrefixExists(const Table* table,
                                    const Key& prefix_key) const override;

  absl::StatusOr<std::unique_ptr<StorageIterator>> Read(
      const Table* table, const KeyRange& key_range,
      absl::Span<const Column* const> columns) const override;

 private:
  const TransactionStore* read_only_store_;
};

// TransactionEffectsBuffer is the transaction buffer in which the write
// operations live.
class TransactionEffectsBuffer : public EffectsBuffer {
 public:
  explicit TransactionEffectsBuffer(std::queue<WriteOp>* ops_queue)
      : ops_queue_(ops_queue) {}

  void Insert(const Table* table, const Key& key,
              absl::Span<const Column* const> columns,
              const std::vector<zetasql::Value>& values) override;

  void Update(const Table* table, const Key& key,
              absl::Span<const Column* const> columns,
              const std::vector<zetasql::Value>& values) override;

  void Delete(const Table* table, const Key& key) override;

 private:
  std::queue<WriteOp>* ops_queue_;
};

static constexpr char kInsert[] = "INSERT";
static constexpr char kUpdate[] = "UPDATE";
static constexpr char kDelete[] = "DELETE";
static constexpr absl::string_view kMinimumValidJson = "{}";

// Each Mod stores the columns and values for keys, new_values, and old_values
// for one WriteOp.
struct Mod {
  const absl::Span<const KeyColumn* const> key_columns;
  std::vector<std::string> non_key_columns;
  const std::vector<zetasql::Value> keys;
  const std::vector<zetasql::Value> new_values;
  const std::vector<zetasql::Value> old_values;
};

struct ColumnType {
  std::string name;
  std::string type;
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
  std::string mod_type;
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
  std::string mod_type;
  absl::flat_hash_set<std::string> non_key_column_names;
  // column_types contains ColumnType for all columns including key columns and
  // tracked non key columns
  std::vector<ColumnType> column_types;
  std::vector<Mod> mods;
  zetasql::Value partition_token_str;
};

// TODO: Move the logic to backend/actions while skip the
// EffectsBuffer abstraction.
//  ChangeStreamTransactionEffectsBuffer is the transaction buffer in which the
//  change stream write operations live.
class ChangeStreamTransactionEffectsBuffer : public ChangeStreamEffectsBuffer {
 public:
  explicit ChangeStreamTransactionEffectsBuffer(TransactionID transaction_id)
      : transaction_id_(transaction_id) {}

  void Insert(zetasql::Value partition_token_str,
              const ChangeStream* change_stream, const InsertOp& op) override;

  void Update(zetasql::Value partition_token_str,
              const ChangeStream* change_stream, const UpdateOp& op) override;

  void Delete(zetasql::Value partition_token_str,
              const ChangeStream* change_stream, const DeleteOp& op) override;

  void BuildMutation() override;

  std::vector<WriteOp> GetWriteOps() override;

 private:
  DataChangeRecord BuildDataChangeRecord(std::string tracked_table_name,
                                         std::string value_capture_type,
                                         const ChangeStream* change_stream);
  void LogTableMod(const Key& key, std::vector<const Column*> columns,
                   const std::vector<zetasql::Value>& values,
                   const Table* table, const ChangeStream* change_stream,
                   std::string mod_type, zetasql::Value partition_token_str);
  absl::StatusOr<Key> ComputeChangeStreamDataTableKey(
      zetasql::Value partition_token_str, zetasql::Value commit_timestamp,
      std::string record_sequence, std::string server_transaction_id,
      std::string table_name);
  absl::StatusOr<WriteOp> ConvertDataChangeRecordToWriteOp(
      const ChangeStream* change_stream, DataChangeRecord record);
  std::vector<WriteOp> writeops_;
  absl::flat_hash_map<const ChangeStream*, std::vector<DataChangeRecord>>
      data_change_records_in_transaction_by_change_stream_;
  TransactionID transaction_id_;
  // TODO: create a mutation builder class scoped to a single
  // change stream and use a map to store the class objects by change streams.
  absl::flat_hash_map<std::string, int64_t> record_sequence_by_change_stream_;
  absl::flat_hash_map<const ChangeStream*, ModGroup>
      last_mod_group_by_change_stream_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_ACTIONS_H_
