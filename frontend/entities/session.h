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

#ifndef STORAGE_CLOUD_SPANNER_EMULATOR_FRONTEND_SESSION_H_
#define STORAGE_CLOUD_SPANNER_EMULATOR_FRONTEND_SESSION_H_

#include <map>
#include <memory>
#include <string>

#include "google/spanner/v1/spanner.pb.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "backend/common/ids.h"
#include "frontend/common/labels.h"
#include "frontend/entities/database.h"
#include "frontend/entities/transaction.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

// Session represents a communication channel to a single database.
//
// All reads and writes to a database happen within the context of a session.
// For the emulator, the Session class is mainly concerned with transaction
// management.
//
// A session can only have one transaction outstanding at any time. To reduce
// latency of read/write calls, Cloud Spanner allows many transactions to be
// pre-created (a strategy used by the official client libraries).
//
// Once an api call is made using a transaction, that transaction is marked as
// active, and any transactions created prior to the active transaction are
// marked as invalid. Invalid transactions will be rolled back if necessary.
//
// More information about sessions can be found at:
//     https://cloud.google.com/spanner/docs/sessions
class Session {
 public:
  // Allows to initialize and activate transaction on the current session.
  enum class TransactionActivation {
    // Only initialize a transaction.
    kInitializeOnly,

    // Initialize and make it the current active transaction on this session.
    kInitializeAndActivate,
  };

  Session(const std::string& session_uri, const Labels& labels,
          const absl::Time create_time, std::shared_ptr<Database> database)
      : session_uri_(session_uri),
        labels_(labels),
        create_time_(create_time),
        database_(database) {}

  // Returns the URI for this session.
  const std::string& session_uri() const { return session_uri_; }

  // Returns the labels for this session.
  const Labels& labels() const { return labels_; }

  // Returns the time this session was created.
  absl::Time create_time() const { return create_time_; }

  // Return the time this session was last used.
  absl::Time approximate_last_use_time() const ABSL_LOCKS_EXCLUDED(mu_) {
    absl::MutexLock lock(&mu_);
    return approximate_last_use_time_;
  }

  // Sets the time this session was last used.
  void set_approximate_last_use_time(absl::Time approximate_last_use_time)
      ABSL_LOCKS_EXCLUDED(mu_) {
    absl::MutexLock lock(&mu_);
    approximate_last_use_time_ = approximate_last_use_time;
  }

  // Converts this session to its proto representation.
  absl::Status ToProto(google::spanner::v1::Session* session,
                       bool include_labels = true);

  // Creates a new multi-use transaction.
  zetasql_base::StatusOr<std::shared_ptr<Transaction>> CreateMultiUseTransaction(
      const google::spanner::v1::TransactionOptions& options,
      const TransactionActivation& activation) ABSL_LOCKS_EXCLUDED(mu_);

  // Creates a new single-use transaction.
  zetasql_base::StatusOr<std::unique_ptr<Transaction>> CreateSingleUseTransaction(
      const google::spanner::v1::TransactionOptions& options)
      ABSL_LOCKS_EXCLUDED(mu_);

  // Finds a transaction by id and sets it as the active transaction.
  zetasql_base::StatusOr<std::shared_ptr<Transaction>> FindAndUseTransaction(
      const std::string& bytes) ABSL_LOCKS_EXCLUDED(mu_);

  // Finds or creates a new transaction and sets it as the active transaction.
  zetasql_base::StatusOr<std::shared_ptr<Transaction>> FindOrInitTransaction(
      const google::spanner::v1::TransactionSelector& selector);

 private:
  // Create a transaction based on the provided options.
  zetasql_base::StatusOr<std::unique_ptr<Transaction>> CreateTransaction(
      const spanner_api::TransactionOptions& options,
      const Transaction::Usage& usage, const backend::RetryState& retry_state);

  // Create a read-only transaction.
  zetasql_base::StatusOr<std::unique_ptr<Transaction>> CreateReadOnly(
      const spanner_api::TransactionOptions& options,
      const Transaction::Usage& usage);

  // Create a read-write transaction.
  zetasql_base::StatusOr<std::unique_ptr<Transaction>> CreateReadWrite(
      const spanner_api::TransactionOptions& options,
      const Transaction::Usage& usage, const backend::RetryState& retry_state);

  // Builds the retry state from active transaction.
  backend::RetryState MakeRetryState(
      const spanner_api::TransactionOptions& options, bool is_single_use_txn)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  // The URI for this session.
  const std::string session_uri_;

  // The labels attached to this session.
  const Labels labels_;

  // The time this session was created.
  const absl::Time create_time_;

  // The database to which this session is attached.
  std::shared_ptr<Database> database_;

  // Mutex to guard the state below.
  mutable absl::Mutex mu_;

  // The last time this session was used.
  absl::Time approximate_last_use_time_ ABSL_GUARDED_BY(mu_);

  // Map of transactions that have been pre-created in this session.
  std::map<backend::TransactionID, std::shared_ptr<Transaction>>
      transaction_map_ ABSL_GUARDED_BY(mu_);

  // The currently active transaction.
  std::shared_ptr<Transaction> active_transaction_ ABSL_GUARDED_BY(mu_);

  // The first transaction id which is valid for use within this session.
  backend::TransactionID min_valid_id_ = backend::kInvalidTransactionID + 1;
};

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // STORAGE_CLOUD_SPANNER_EMULATOR_FRONTEND_SESSION_H_
