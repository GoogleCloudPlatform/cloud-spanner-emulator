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

#include <memory>
#include <queue>
#include <vector>

#include "zetasql/public/value.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "backend/actions/context.h"
#include "backend/datamodel/key_range.h"
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

  zetasql_base::StatusOr<bool> Exists(const Table* table,
                              const Key& key) const override;

  zetasql_base::StatusOr<bool> PrefixExists(const Table* table,
                                    const Key& prefix_key) const override;

  zetasql_base::StatusOr<std::unique_ptr<StorageIterator>> Read(
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

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_ACTIONS_H_
