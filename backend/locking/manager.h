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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_LOCKING_MANAGER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_LOCKING_MANAGER_H_

#include <memory>

#include "absl/base/thread_annotations.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "backend/common/ids.h"
#include "backend/locking/handle.h"
#include "common/clock.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// LockManager represents the lock manager for a database.
//
// Transactions interact with the LockManager via a LockHandle which they obtain
// at initialization time. All subsequent communication with the LockManager
// happens via the LockHandle. See LockHandle methods for more details about
// this interaction.
//
// We currently only implement a whole-database lock. The interface is generic
// to avoid irreversibly baking the single-lock assumption into the rest of the
// system.
class LockManager {
 public:
  explicit LockManager(Clock* clock) : clock_(clock) {}

  // Returns a handle for a single transaction with the given id and priority.
  // Subsequent communication between the transaction and the lock manager
  // happens via the handle. See LockHandle methods for more details.
  std::unique_ptr<LockHandle> CreateHandle(TransactionID id,
                                           TransactionPriority priority);

  // Returns the timestamp at which last schema update or commit completed.
  absl::Time LastCommitTimestamp();

 private:
  // LockHandle simply forwards requests to the LockManager.
  friend class LockHandle;
  void EnqueueLock(LockHandle* handle, const LockRequest& request)
      ABSL_LOCKS_EXCLUDED(mu_);
  void UnlockAll(LockHandle* handle) ABSL_LOCKS_EXCLUDED(mu_);
  zetasql_base::StatusOr<absl::Time> ReserveCommitTimestamp(LockHandle* handle)
      ABSL_LOCKS_EXCLUDED(mu_);
  absl::Status MarkCommitted(LockHandle* handle) ABSL_LOCKS_EXCLUDED(mu_);
  void WaitForSafeRead(absl::Time read_time) ABSL_LOCKS_EXCLUDED(mu_);

  // Mutex to guard state below.
  absl::Mutex mu_;

  // The currently active transaction ID (only one transaction can be active).
  TransactionID active_tid_ ABSL_GUARDED_BY(mu_) = kInvalidTransactionID;

  // System wide monotonic clock used to provide commit and read timestamps.
  Clock* clock_;

  // Timestamp at which last schema update or commit completed.
  absl::Time last_commit_timestamp_ ABSL_GUARDED_BY(mu_) = absl::InfinitePast();

  // Commit timestamp being used by an in-progress commit.
  absl::Time pending_commit_timestamp_ ABSL_GUARDED_BY(mu_) =
      absl::InfiniteFuture();

  // Signals completion of pending commit.
  absl::CondVar pending_commit_cvar_ ABSL_GUARDED_BY(mu_);
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_LOCKING_MANAGER_H_
