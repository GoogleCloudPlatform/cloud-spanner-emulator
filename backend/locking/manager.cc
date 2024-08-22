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

#include "backend/locking/manager.h"

#include <functional>
#include <memory>

#include "absl/memory/memory.h"
#include "absl/random/random.h"
#include "absl/random/uniform_int_distribution.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/substitute.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "backend/common/ids.h"
#include "common/config.h"
#include "common/errors.h"
#include "zetasql/base/ret_check.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

std::unique_ptr<LockHandle> LockManager::CreateHandle(
    TransactionID tid, const std::function<absl::Status()>& abort_fn,
    TransactionPriority priority) {
  return absl::WrapUnique(new LockHandle(this, tid, abort_fn, priority));
}

void LockManager::EnqueueLock(LockHandle* handle, const LockRequest& request) {
  absl::MutexLock lock(&mu_);

  // Don't hand out locks to aborted handles.
  if (handle->IsAborted()) {
    return;
  }

  // If there is no transaction holding the lock, we grant it.
  if (active_handle_ == nullptr) {
    active_handle_ = handle;
    return;
  }

  // If the requesting transaction is already holding the lock, we grant it.
  if (active_handle_->tid() == handle->tid()) {
    return;
  }

  // If we reached here, another transaction is already holding the lock.
  // Randomly abort the current transaction to ensure that starting a new
  // transaction is not blocked by the current transaction if this is waiting
  // for a new transaction to finish.
  absl::BitGen gen;
  if (absl::uniform_int_distribution<int>(1, 100)(gen) <=
      config::abort_current_transaction_probability()) {
    auto could_be_aborted = active_handle_->TryAbortTransaction(
        error::AbortCurrentTransaction(active_handle_->tid(), handle->tid()));
    if (could_be_aborted.ok()) {
      active_handle_ = handle;
      return;
    }
  }

  // Couldn't abort the transaction currently holding the lock, so abort the
  // new transaction.
  handle->Abort(
      error::AbortConcurrentTransaction(handle->tid(), active_handle_->tid()));
}

void LockManager::UnlockAll(LockHandle* handle) {
  absl::MutexLock lock(&mu_);

  // If the transaction does not hold the lock, there is nothing to do.
  if (active_handle_ == nullptr || active_handle_->tid() != handle->tid()) {
    handle->Reset();
    return;
  }

  // Clear the active transaction if it holds the lock.
  active_handle_ = nullptr;
  handle->Reset();
}

absl::StatusOr<absl::Time> LockManager::ReserveCommitTimestamp(
    LockHandle* handle) {
  absl::MutexLock lock(&mu_);

  // If there is no transaction holding the lock, we grant it to the transaction
  // requesting commit timestamp. This can happen if transaction has empty
  // mutations and write locks weren't thus acquired yet.
  if (active_handle_ == nullptr) {
    active_handle_ = handle;
  } else if (active_handle_->tid() != handle->tid()) {
    // There is another active transaction, abort this transaction.
    return error::AbortConcurrentTransaction(handle->tid(),
                                             active_handle_->tid());
  }

  pending_commit_timestamp_ = clock_->Now();
  return pending_commit_timestamp_;
}

absl::Status LockManager::MarkCommitted(LockHandle* handle) {
  absl::MutexLock lock(&mu_);

  // This transaction should have been set as the active transaction.
  ZETASQL_RET_CHECK_EQ(active_handle_->tid(), handle->tid())
      << absl::Substitute("Transaction $0 is not active.", handle->tid());

  last_commit_timestamp_ = pending_commit_timestamp_;
  pending_commit_timestamp_ = absl::InfiniteFuture();
  pending_commit_cvar_.SignalAll();
  return absl::OkStatus();
}

void LockManager::WaitForSafeRead(absl::Time read_time) {
  absl::MutexLock lock(&mu_);

  // Wait for read time to become current if passed a future timestamp  for the
  // case of exact timestamp bound for snapshot read.
  // https://cloud.google.com/spanner/docs/timestamp-bounds#introduction
  bool f = false;
  mu_.AwaitWithDeadline(absl::Condition(&f), read_time);

  while (pending_commit_timestamp_ < read_time) {
    pending_commit_cvar_.Wait(&mu_);
  }
}

absl::Time LockManager::LastCommitTimestamp() {
  absl::ReaderMutexLock lock(&mu_);
  return last_commit_timestamp_;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
