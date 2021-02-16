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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_LOCKING_HANDLE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_LOCKING_HANDLE_H_

#include "absl/base/thread_annotations.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "absl/time/time.h"
#include "backend/common/ids.h"
#include "backend/locking/request.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Forward declaration of the LockManager to avoid a circular reference.
class LockManager;

// LockHandle encapsulates a transaction's interface to the lock manager.
//
// A transaction first creates a lock handle via LockManager::CreateHandle().
// All subsequent communication between the transaction and the LockManager
// happens via the LockHandle. This includes incremental acquisition of read
// write locks, waiting on those locks, and unlocking those locks.
//
// EnqueueLock() is non-blocking and only enqueues the lock request. The
// transaction can subsequently query whether the requests have completed by
// checking IsBlocked() or perform a blocking Wait() to find out the final
// state of the lock requests.
//
// Usage (happy path, error handling skipped):
//    // Get a handle.
//    auto handle = lock_manager->CreateHandle(txn_id, txn_priority);
//
//    // Request some locks.
//    handle->EnqueueLock(...)
//    handle->EnqueueLock(...)
//
//    // Wait for those locks to be granted.
//    handle->Wait()
//
//    // Do work based on those locks.
//    ...
//
//    // Unlock all locks held by this transaction.
//    handle->UnlockAll();
class LockHandle {
 public:
  // Returns the ID of the transaction which owns this handle.
  TransactionID tid() { return tid_; }

  // Returns the priority of the transaction which owns this handle.
  TransactionPriority priority() { return priority_; }

  // Enqueues a lock request for this transaction. This method returns
  // immediately. Lock request status can be queried via IsBlocked() and Wait().
  // Many lock requests can be queued up before calling Wait(). Lock requests
  // from aborted handles are ignored by the lock manager.
  void EnqueueLock(const LockRequest& request);

  // Unlocks all locks held by this transaction. The transaction can acquire new
  // locks using the same handle. Frees any waiters waiting on these locks. This
  // method can be called even if the transaction never acquired any locks.
  void UnlockAll();

  // Returns true if this handle is waiting on any lock requests to complete.
  bool IsBlocked() ABSL_LOCKS_EXCLUDED(mu_);

  // Returns true if this handle has been aborted by the lock manager. Previous
  // locks acquired by the handle are not release automatically. The handle must
  // explicitly call UnlockAll().
  bool IsAborted() ABSL_LOCKS_EXCLUDED(mu_);

  // Waits till all locks requested via this handle have either all been granted
  // or have at least one request denied. Lock denials will return ABORTED
  // status, otherwise OK will be returned.
  absl::Status Wait() ABSL_LOCKS_EXCLUDED(mu_);

  // Returns timestamp which can be used by this transaction as a commit
  // timestamp.
  zetasql_base::StatusOr<absl::Time> ReserveCommitTimestamp();

  // Notifies the LockManager that this transaction has committed.
  absl::Status MarkCommitted();

  // Waits for the intended read timestamp to be safe from any in-progress
  // commits.
  void WaitForSafeRead(absl::Time read_time);

 private:
  // Only the LockManager is allowed to create and destroy LockHandles.
  friend class LockManager;
  friend std::unique_ptr<LockHandle>::deleter_type;
  LockHandle(LockManager* manager, TransactionID tid,
             TransactionPriority priority);
  ~LockHandle();

  // Aborts the requests made by this handle (and puts it in a final state).
  void Abort(const absl::Status& status) ABSL_LOCKS_EXCLUDED(mu_);

  // Resets the state of this handle.
  void Reset() ABSL_LOCKS_EXCLUDED(mu_);

  // The LockManager which this LockHandle interacts with.
  LockManager* const manager_;

  // The ID of the transaction which owns this lock handle.
  TransactionID tid_;

  // The priority of the transaction which owns this lock handle.
  TransactionPriority priority_;

  // Mutex to guard state below.
  absl::Mutex mu_;

  // The status of the lock handle requests.
  absl::Status status_ ABSL_GUARDED_BY(mu_);
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_LOCKING_HANDLE_H_
