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

#include <queue>

#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/memory/memory.h"
#include "zetasql/base/statusor.h"
#include "backend/actions/context.h"
#include "backend/actions/ops.h"
#include "backend/storage/in_memory_storage.h"
#include "common/clock.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {

// TestReadOnlyStore provides an implementation of the ReadOnlyStore to test
// actions.
class TestReadOnlyStore : public ReadOnlyStore {
 public:
  zetasql_base::StatusOr<bool> Exists(const Table* table,
                              const Key& key) const override;

  zetasql_base::StatusOr<std::unique_ptr<StorageIterator>> Read(
      const Table* table, const KeyRange& key_range,
      const absl::Span<const Column* const> columns) const override;

  zetasql_base::StatusOr<bool> PrefixExists(const Table* table,
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

class ActionsTest : public testing::Test {
 public:
  ActionsTest()
      : ctx_(absl::make_unique<ActionContext>(
            absl::make_unique<test::TestReadOnlyStore>(),
            absl::make_unique<test::TestEffectsBuffer>(&ops_queue_), &clock_)) {
  }

  // Accessors
  test::TestEffectsBuffer* effects_buffer() {
    return dynamic_cast<test::TestEffectsBuffer*>(ctx_->effects());
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
