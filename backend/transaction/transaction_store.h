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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_TRANSACTION_STORE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_TRANSACTION_STORE_H_

#include <memory>

#include "zetasql/public/value.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "backend/actions/ops.h"
#include "backend/common/ids.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/value.h"
#include "backend/locking/handle.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "backend/storage/in_memory_iterator.h"
#include "backend/storage/storage.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// TransactionStore buffers the mutations of a read-write transaction.
//
// Mutations applied to the TransactionStore are stored as an overlay over the
// base database storage and not actually applied to the base database storage.
//
// Multiple writes to the same row are collapsed together. For instance, an
// insert followed by a delete of the same row will clear the row from the
// TransactionStore. A delete followed by an insert followed by multiple updates
// of the same row will be collapsed into a delete and an insert for that row.
//
// Reads from the transaction store combine information from the buffered
// mutations and the base storage to provide a view of the database with the
// mutations applied. This enables read-your-write semantics provided by DML.
//
// TransactionStore is also responsible for acquiring read/write locks for
// rows and columns accessed via its interface.
//
// At commit time, the read-write transaction which owns this store flushes all
// buffered mutations to the underlying database storage in an atomic fashion.
//
// This class is not thread safe.
class TransactionStore {
 public:
  explicit TransactionStore(Storage* base_storage, LockHandle* lock_handle)
      : base_storage_(base_storage), lock_handle_(lock_handle) {}

  // Buffers a write operation. Acquires write locks.
  absl::Status BufferWriteOp(const WriteOp& op);

  // Returns the column values for 'key' by merging information from the
  // buffered mutations and the base storage. Returns NOT_FOUND if 'key'
  // does not exist in the merged view. Acquires read locks.
  zetasql_base::StatusOr<ValueList> Lookup(
      const Table* table, const Key& key,
      absl::Span<const Column* const> columns) const;

  // Returns an iterator for column values of 'key_range' by merging information
  // from the buffered mutations and the base storage. Acquires read locks.
  //
  // Boolean flag allow_pending_commit_timestamps_in_read can be set to false to
  // disallow returning pending_commit_timestamp values to clients.
  absl::Status Read(const Table* table, const KeyRange& key_range,
                    absl::Span<const Column* const> columns,
                    std::unique_ptr<StorageIterator>* storage_itr,
                    bool allow_pending_commit_timestamps_in_read = true) const;

  // Returns the buffered mutations.
  std::vector<WriteOp> GetBufferedOps() const;

  // Clears the buffered mutations.
  void Clear() { buffered_ops_.clear(); }

 private:
  // Types of mutations.
  enum class OpType {
    kInsert,
    kUpdate,
    kDelete,
  };

  using RowOp = std::pair<OpType, Row>;

  // Acquires read locks for the specified column ranges.
  absl::Status AcquireReadLock(const Table* table, const KeyRange& key_range,
                               absl::Span<const Column* const> columns) const;

  // Acquires write locks for the specified column ranges.
  absl::Status AcquireWriteLock(const Table* table, const KeyRange& key_range,
                                absl::Span<const Column* const> columns) const;

  // Buffers an insert mutation. Acquires write locks.
  absl::Status BufferInsert(const Table* table, const Key& key,
                            absl::Span<const Column* const> columns,
                            const ValueList& values);

  // Buffers an update mutation. Acquires write locks.
  absl::Status BufferUpdate(const Table* table, const Key& key,
                            absl::Span<const Column* const> columns,
                            const ValueList& values);

  // Buffers a delete mutation. Acquires write locks.
  absl::Status BufferDelete(const Table* table, const Key& key);

  // Returns true if a mutation has been buffered for 'key' and fills 'row'.
  bool RowExistsInBuffer(const Table* table, const Key& key, RowOp* row) const;

  // Mark a given column non-readable if one or more values being written to it
  // in the mutation contain pending commit timestamp.
  void TrackColumnsForCommitTimestamp(absl::Span<const Column* const> columns,
                                      const ValueList& values);

  // Mark table and it's associated indices as non-readable if key in the
  // mutation contains pending commit timestamp.
  void TrackTableForCommitTimestamp(const Table* table, const Key& key);

  // Underlying storage for the database.
  const Storage* base_storage_;

  // Handle for the lock manager.
  LockHandle* lock_handle_;

  // Map that stores the buffered mutations.
  absl::flat_hash_map<const Table*, std::map<Key, RowOp>> buffered_ops_;

  // Set of non-key columns which have mutation with pending commit timestamp
  // and are thus marked as non-readable in read-your-writes transactions.
  absl::flat_hash_set<const Column*> commit_ts_columns_;

  // Set of tables which have mutation with pending commit timestamp and are
  // thus marked as non-readable in read-your-writes transactions. This also
  // includes backing tables for indices of all such tables.
  absl::flat_hash_set<const Table*> commit_ts_tables_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_TRANSACTION_STORE_H_
