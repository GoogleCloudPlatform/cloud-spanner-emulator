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

#include "frontend/collections/multiplexed_session_transaction_manager.h"

#include <memory>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "backend/common/ids.h"
#include "common/errors.h"
#include "frontend/entities/transaction.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

static constexpr absl::Duration kOldTransactionStalenessDuration =
    absl::Minutes(5);
static constexpr absl::Duration kTransactionStalenessDuration =
    absl::Minutes(2);

MultiplexedSessionTransactionManager::MultiplexedSessionTransactionManager()
    : last_clear_time_(absl::Now()),
      old_transaction_staleness_duration_(kOldTransactionStalenessDuration),
      staleness_check_duration_(kTransactionStalenessDuration) {}

MultiplexedSessionTransactionManager::MultiplexedSessionTransactionManager(
    absl::Duration old_transaction_staleness_duration,
    absl::Duration staleness_check_duration)
    : last_clear_time_(absl::Now()),
      old_transaction_staleness_duration_(old_transaction_staleness_duration),
      staleness_check_duration_(staleness_check_duration) {}

absl::Status MultiplexedSessionTransactionManager::AddToCurrentTransactions(
    std::shared_ptr<Transaction> txn, backend::TransactionID txn_id) {
  absl::MutexLock lock(&mu_);
  current_transactions_.emplace(txn_id, txn);
  return absl::OkStatus();
}

absl::StatusOr<std::shared_ptr<Transaction>>
MultiplexedSessionTransactionManager::GetCurrentTransactionOnMultiplexedSession(
    backend::TransactionID txn_id) {
  absl::MutexLock lock(&mu_);
  if (current_transactions_.find(txn_id) != current_transactions_.end()) {
    // Transaction exists
    return current_transactions_[txn_id];
  }
  return error::TransactionNotFound(txn_id);
}

void MultiplexedSessionTransactionManager::RemoveFromCurrentTransactionsLocked(
    backend::TransactionID txn_id) {
  current_transactions_.erase(txn_id);
}

void MultiplexedSessionTransactionManager::ClearOldTransactionsLocked() {
  // Create a temporary list of transactions to be deleted.
  std::vector<backend::TransactionID> transactions_to_delete;
  for (const auto& it : current_transactions_) {
    // If transaction is closed or if the create time is older than
    // old_transaction_staleness_duration_, add it to the list of
    // transactions to be deleted.
    if (it.second->IsClosed() || (absl::Now() - it.second->GetCreateTime()) >
                                     old_transaction_staleness_duration_) {
      transactions_to_delete.push_back(it.first);
    }
  }
  for (auto txn_id : transactions_to_delete) {
    RemoveFromCurrentTransactionsLocked(txn_id);
  }
}

void MultiplexedSessionTransactionManager::ClearOldTransactions() {
  absl::MutexLock lock(&mu_);
  ClearOldTransactionsLocked();
}

void MultiplexedSessionTransactionManager::MaybeClearOldTransactions() {
  absl::MutexLock lock(&mu_);
  if (absl::Now() - last_clear_time_ > staleness_check_duration_) {
    ClearOldTransactionsLocked();
    last_clear_time_ = absl::Now();
  }
}

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
