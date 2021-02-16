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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_CONTEXT_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_CONTEXT_H_

#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "backend/actions/ops.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/key_range.h"
#include "backend/schema/catalog/table.h"
#include "backend/storage/iterator.h"
#include "common/clock.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// EffectsBuffer abstracts the mutation environment in which an action lives.
//
// Actions don't directly mutate underlying storage (via Write/Delete), but
// instead specify the row operation they would like to apply to storage. These
// row operations get buffered to the list of row operations that the
// transaction needs to apply. Within a statement, for a given row operation,
// all effects resulting from the row operation (including cascading effects)
// are processed before the next row mutation in the statement is processed.
class EffectsBuffer {
 public:
  virtual ~EffectsBuffer() {}

  // Adds an insert operation to the effects buffer.
  virtual void Insert(const Table* table, const Key& key,
                      absl::Span<const Column* const> columns,
                      const std::vector<zetasql::Value>& values) = 0;

  // Adds an update operation to the effects buffer.
  virtual void Update(const Table* table, const Key& key,
                      absl::Span<const Column* const> columns,
                      const std::vector<zetasql::Value>& values) = 0;

  // Adds a delete operation to the effects buffer.
  virtual void Delete(const Table* table, const Key& key) = 0;
};

// ReadOnlyStore abstracts the storage environment in which an action lives.
//
// The contents of the storage presented to the action depends on when the
// action runs. Examples:
//
// - Actions which modify the row operation (like DEFAULT) will run before the
//   row operation has been applied to the storage and will see the contents of
//   the storage without the effect of the row operation.
//
// - Actions which verify statements (like index uniqueness verification) will
//   run after the row operation (in fact after all row operations in the
//   statement) has (have) been applied and will see the effect of the row
//   operation.
class ReadOnlyStore {
 public:
  virtual ~ReadOnlyStore() {}

  // Returns true if the given key exist, false if it does not.
  virtual zetasql_base::StatusOr<bool> Exists(const Table* table,
                                      const Key& key) const = 0;

  // Returns true if a row with the given key prefix exist, false if it does
  // not.
  virtual zetasql_base::StatusOr<bool> PrefixExists(const Table* table,
                                            const Key& prefix_key) const = 0;

  // Reads the given key range from the store.
  virtual zetasql_base::StatusOr<std::unique_ptr<StorageIterator>> Read(
      const Table* table, const KeyRange& key_range,
      absl::Span<const Column* const> columns) const = 0;
};

// ActionContext contains the context in which an action operates.
class ActionContext {
 public:
  ActionContext(std::unique_ptr<ReadOnlyStore> store,
                std::unique_ptr<EffectsBuffer> effects, Clock* clock)
      : store_(std::move(store)), effects_(std::move(effects)), clock_(clock) {}

  // Accessors.
  ReadOnlyStore* store() const { return store_.get(); }
  EffectsBuffer* effects() const { return effects_.get(); }
  Clock* clock() const { return clock_; }

 private:
  std::unique_ptr<ReadOnlyStore> store_;
  std::unique_ptr<EffectsBuffer> effects_;

  // System-wide monotonic clock.
  Clock* clock_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTION_CONTEXT_H_
