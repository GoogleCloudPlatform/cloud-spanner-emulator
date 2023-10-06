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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_ACTIONS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_ACTIONS_H_

#include <cstdint>
#include <queue>
#include <string>

#include "zetasql/public/value.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/container/flat_hash_map.h"
#include "absl/memory/memory.h"
#include "absl/status/statusor.h"
#include "backend/actions/context.h"
#include "backend/actions/ops.h"
#include "backend/schema/catalog/change_stream.h"
#include "backend/storage/in_memory_storage.h"
#include "common/clock.h"
#include "tests/common/row_reader.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {

// TestReadOnlyStore provides an implementation of the ReadOnlyStore to test
// actions.
class TestReadOnlyStore : public ReadOnlyStore {
 public:
  absl::StatusOr<bool> Exists(const Table* table,
                              const Key& key) const override;

  absl::StatusOr<std::unique_ptr<StorageIterator>> Read(
      const Table* table, const KeyRange& key_range,
      const absl::Span<const Column* const> columns) const override;

  absl::StatusOr<bool> PrefixExists(const Table* table,
                                    const Key& prefix_key) const override;

  absl::Status Insert(const Table* table, const Key& key,
                      absl::Span<const Column* const> columns,
                      const std::vector<zetasql::Value>& values = {});

 private:
  InMemoryStorage store_;
};

// TestEffectsBuffer provides an implementation of EffectsBuffer to test
// actions.
class TestEffectsBuffer : public EffectsBuffer {
 public:
  explicit TestEffectsBuffer(std::queue<WriteOp>* ops_queue)
      : ops_queue_(ops_queue) {}

  void Insert(const Table* table, const Key& key,
              const absl::Span<const Column* const> columns,
              const std::vector<zetasql::Value>& values) override;

  void Update(const Table* table, const Key& key,
              const absl::Span<const Column* const> columns,
              const std::vector<zetasql::Value>& values) override;

  void Delete(const Table* table, const Key& key) override;

  // Accessor.
  std::queue<WriteOp>* ops_queue() const { return ops_queue_; }

 private:
  std::queue<WriteOp>* ops_queue_;
};

static constexpr char kInsert[] = "INSERT";
static constexpr char kUpdate[] = "UPDATE";
static constexpr char kDelete[] = "DELETE";
static constexpr absl::string_view kMinimumValidJson = "{}";

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

struct ModGroup {
  std::string table_name;
  std::string mod_type;
  absl::flat_hash_set<std::string> non_key_column_names;
  std::vector<ColumnType> column_types;
  std::vector<Mod> mods;
  zetasql::Value partition_token_str;
};

// TODO: We won't need TestChangeStreamEffectsBuffer to test the
// group logic after refactoring. TestChangeStreamEffectsBuffer provides an
// implementation of ChangeStreamEffectsBuffer to test actions.
class TestChangeStreamEffectsBuffer : public ChangeStreamEffectsBuffer {
 public:
  explicit TestChangeStreamEffectsBuffer() = default;

  void Insert(zetasql::Value partition_token_str,
              const ChangeStream* change_stream, const InsertOp& op) override;

  void Update(zetasql::Value partition_token_str,
              const ChangeStream* change_stream, const UpdateOp& op) override;

  void Delete(zetasql::Value partition_token_str,
              const ChangeStream* change_stream, const DeleteOp& op) override;

  void BuildMutation() override;

  // Accessor.
  std::vector<WriteOp> GetWriteOps() override;

  void ClearWriteOps() override;

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
  absl::flat_hash_map<std::string, int64_t> record_sequence_by_change_stream_;
  absl::flat_hash_map<const ChangeStream*, ModGroup>
      last_mod_group_by_change_stream_;
};

class ActionsTest : public testing::Test {
 public:
  ActionsTest()
      : ctx_(std::make_unique<ActionContext>(
            std::make_unique<test::TestReadOnlyStore>(),
            std::make_unique<test::TestEffectsBuffer>(&ops_queue_),
            std::make_unique<test::TestChangeStreamEffectsBuffer>(),
            &clock_)) {}

  // Accessors
  test::TestEffectsBuffer* effects_buffer() {
    return dynamic_cast<test::TestEffectsBuffer*>(ctx_->effects());
  }

  test::TestChangeStreamEffectsBuffer* change_stream_effects_buffer() {
    return dynamic_cast<test::TestChangeStreamEffectsBuffer*>(
        ctx_->change_stream_effects());
  }

  test::TestReadOnlyStore* store() {
    return dynamic_cast<test::TestReadOnlyStore*>(ctx_->store());
  }
  ActionContext* ctx() { return ctx_.get(); }

  // Convenience methods.
  WriteOp Insert(const Table* table, const Key& key,
                 absl::Span<const Column* const> columns = {},
                 const std::vector<zetasql::Value> values = {});
  WriteOp Update(const Table* table, const Key& key,
                 absl::Span<const Column* const> columns = {},
                 const std::vector<zetasql::Value> values = {});
  WriteOp Delete(const Table* table, const Key& key);

 private:
  // Test components.
  std::queue<WriteOp> ops_queue_;
  Clock clock_;
  std::unique_ptr<ActionContext> ctx_;
};

}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_ACTIONS_H_
