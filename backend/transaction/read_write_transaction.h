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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_READ_WRITE_TRANSACTION_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_READ_WRITE_TRANSACTION_H_

#include <memory>
#include <queue>

#include "absl/base/thread_annotations.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "backend/access/read.h"
#include "backend/access/write.h"
#include "backend/actions/context.h"
#include "backend/actions/manager.h"
#include "backend/common/case.h"
#include "backend/common/ids.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/key_range.h"
#include "backend/locking/handle.h"
#include "backend/locking/manager.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/catalog/versioned_catalog.h"
#include "backend/storage/storage.h"
#include "backend/transaction/actions.h"
#include "backend/transaction/options.h"
#include "backend/transaction/transaction_store.h"
#include "common/clock.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// ReadWriteTransaction is a transaction that can modify the database. All the
// reads and writes in the transaction are logically performed at the same
// timestamp (commit timestamp).
class ReadWriteTransaction : public RowReader, public RowWriter {
 public:
  enum class State {
    // Uninitialized transaction.
    kUninitialized,

    // Active transaction.
    kActive,

    // Committed transaction.
    kCommitted,

    // Rolledback transaction (initiated by user), cannot be retried.
    kRolledback,

    // Transaction has been invalidated due to non-recoverable constraint
    // errors. It cannot be retried. This is not the same as the transaction
    // returning kAborted status error, in that case the transaction can be
    // retried.
    kInvalid,
  };

  ReadWriteTransaction(const ReadWriteOptions& options,
                       const RetryState& retry_state,
                       TransactionID transaction_id, Clock* clock,
                       Storage* storage, LockManager* lock_manager,
                       const VersionedCatalog* const versioned_catalog,
                       ActionManager* action_manager);

  absl::Status Read(const ReadArg& read_arg,
                    std::unique_ptr<RowCursor>* cursor) override
      ABSL_LOCKS_EXCLUDED(mu_);

  absl::Status Write(const Mutation& mutation) override
      ABSL_LOCKS_EXCLUDED(mu_);

  absl::Status Commit() ABSL_LOCKS_EXCLUDED(mu_);

  absl::Status Rollback() ABSL_LOCKS_EXCLUDED(mu_);

  absl::Status Invalidate() ABSL_LOCKS_EXCLUDED(mu_);

  zetasql_base::StatusOr<absl::Time> GetCommitTimestamp() ABSL_LOCKS_EXCLUDED(mu_);

  const State state() const ABSL_LOCKS_EXCLUDED(mu_) {
    absl::MutexLock lock(&mu_);
    return state_;
  }

  // Returns the schema used by this transaction.
  const Schema* schema() const ABSL_LOCKS_EXCLUDED(mu_);

  // Returns the ID of this transaction.
  const TransactionID id() const { return id_; }

  // Returns the options for this transaction.
  const ReadWriteOptions& options() const { return options_; }

  // Returns the retry state for this transaction.
  const RetryState retry_state() const ABSL_LOCKS_EXCLUDED(mu_) {
    absl::MutexLock lock(&mu_);
    return retry_state_;
  }

 private:
  friend class TransactionOpsProcessor;

  enum class OpType {
    kRead,
    kWrite,
    kCommit,
    kRollback,
    kInvalidate,
  };

  absl::Status GuardedCall(OpType op, const std::function<absl::Status()>& fn)
      ABSL_LOCKS_EXCLUDED(mu_);
  absl::Status ProcessWriteOps(const std::vector<WriteOp>& write_ops);

  // Resets the transaction and marks it Active.
  void Reset();

  // Apply the constraint checks and effects to the writes.
  absl::Status ApplyValidators(const WriteOp& op);
  absl::Status ApplyEffectors(const WriteOp& op);
  absl::Status ApplyStatementVerifiers();

  // Returns true if the given key exists within the table.
  bool KeyExists(const Table* table, const Key& key) const;

  // Mutex that guards the state of this transaction.
  mutable absl::Mutex mu_;

  // Options with which the transaction was created.
  ReadWriteOptions options_;

  // Initial state of the transaction between retry attempts.
  RetryState retry_state_ ABSL_GUARDED_BY(mu_);

  // ID for this transaction.
  const TransactionID id_;

  // System-wide monotonic clock.
  Clock* clock_;

  // Underlying storage of the database.
  Storage* base_storage_;

  // Catalog of schemas.
  const VersionedCatalog* const versioned_catalog_;

  // Transaction lock management.
  std::unique_ptr<LockHandle> lock_handle_;

  // The overlay storage layer that handles mutations and read-your-write
  // semantics.
  std::unique_ptr<TransactionStore> transaction_store_;

  // Action Manager for the transaction.
  ActionManager* action_manager_;
  ActionRegistry* action_registry_;
  std::unique_ptr<ActionContext> action_context_;

  // The commit timestamp chosen for this transaction.
  absl::Time commit_timestamp_ ABSL_GUARDED_BY(mu_);

  // Queue of mutations being processed by this transaction.
  std::queue<WriteOp> write_ops_queue_ ABSL_GUARDED_BY(mu_);

  // The state of this transaction.
  State state_ ABSL_GUARDED_BY(mu_) = State::kUninitialized;

  // The schema that is in effect at the timestamp picked for this transaction.
  const Schema* schema_ ABSL_GUARDED_BY(mu_);

  CaseInsensitiveStringMap<std::vector<KeyRange>> deleted_key_ranges_by_table_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_READ_WRITE_TRANSACTION_H_
