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

#ifndef STORAGE_CLOUD_SPANNER_EMULATOR_FRONTEND_MULTIPLEXED_SESSION_TRANSACTION_MANAGER_H_
#define STORAGE_CLOUD_SPANNER_EMULATOR_FRONTEND_MULTIPLEXED_SESSION_TRANSACTION_MANAGER_H_

#include <memory>

#include "absl/base/thread_annotations.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "backend/common/ids.h"
#include "frontend/entities/transaction.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {
class MultiplexedSessionTransactionManager {
 public:
  explicit MultiplexedSessionTransactionManager();
  explicit MultiplexedSessionTransactionManager(
      absl::Duration old_transaction_staleness_duration,
      absl::Duration staleness_check_duration);

  ~MultiplexedSessionTransactionManager() = default;

  // This type is neither copyable nor movable.
  MultiplexedSessionTransactionManager(
      const MultiplexedSessionTransactionManager&) = delete;
  MultiplexedSessionTransactionManager& operator=(
      const MultiplexedSessionTransactionManager&) = delete;

  // Adds new transactions to the current transactions map.
  absl::Status AddToCurrentTransactions(std::shared_ptr<Transaction> txn,
                                        backend::TransactionID txn_id);

  // Retrieve a transaction from the current transactions map.
  absl::StatusOr<std::shared_ptr<Transaction>>
  GetCurrentTransactionOnMultiplexedSession(backend::TransactionID txn_id);

  // Called occasionally to clear old transactions.
  void ClearOldTransactions();

  // Evaluate if old transactions need to be cleared based on the last time
  // it was checked for clearing.
  void MaybeClearOldTransactions();

  absl::Time GetLastClearTime() const ABSL_LOCKS_EXCLUDED(mu_) {
    absl::MutexLock lock(&mu_);
    return last_clear_time_;
  }

  // Set the last clear time to the current time.
  void SetLastClearTime() ABSL_LOCKS_EXCLUDED(mu_) {
    absl::MutexLock lock(&mu_);
    last_clear_time_ = absl::Now();
  }

  absl::Duration GetStalenessCheckDuration() const ABSL_LOCKS_EXCLUDED(mu_) {
    absl::MutexLock lock(&mu_);
    return staleness_check_duration_;
  }

 private:
  mutable absl::Mutex mu_;
  std::map<backend::TransactionID, std::shared_ptr<Transaction>>
      current_transactions_ ABSL_GUARDED_BY(mu_);
  // The last time the transactions were checked for staleness.
  absl::Time last_clear_time_ ABSL_GUARDED_BY(mu_);
  absl::Duration old_transaction_staleness_duration_;
  absl::Duration staleness_check_duration_;

  // Evict a transaction from the current transactions map.
  void RemoveFromCurrentTransactionsLocked(backend::TransactionID txn_id)
      ABSL_SHARED_LOCKS_REQUIRED(mu_);

  void ClearOldTransactionsLocked() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
};
}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
#endif  // STORAGE_CLOUD_SPANNER_EMULATOR_FRONTEND_MULTIPLEXED_SESSION_TRANSACTION_MANAGER_H_
