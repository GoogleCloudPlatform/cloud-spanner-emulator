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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_READ_ONLY_TRANSACTION_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_READ_ONLY_TRANSACTION_H_

#include <memory>

#include "absl/base/thread_annotations.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "backend/access/read.h"
#include "backend/access/write.h"
#include "backend/common/ids.h"
#include "backend/locking/manager.h"
#include "backend/schema/catalog/versioned_catalog.h"
#include "backend/storage/storage.h"
#include "backend/transaction/options.h"
#include "backend/transaction/transaction_store.h"
#include "common/clock.h"
#include "common/errors.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// ReadOnlyTransaction is a read-only transaction that reads from a specific
// timestamp. ReadOnlyTransaction reads the database without needing to acquire
// any locks.
//
// ReadOnlyTransaction cannot be committed, rolled-back, or be used to run DMLs.
class ReadOnlyTransaction : public RowReader {
 public:
  ReadOnlyTransaction(const ReadOnlyOptions& options,
                      TransactionID transaction_id, Clock* clock,
                      Storage* storage, LockManager* lock_manager,
                      const VersionedCatalog* const versioned_catalog);

  absl::Status Read(const ReadArg& read_arg,
                    std::unique_ptr<RowCursor>* cursor) override
      ABSL_LOCKS_EXCLUDED(mu_);

  absl::Time read_timestamp() const { return read_timestamp_; }

  // Returns the schema used by this transaction.
  const Schema* schema() const;

  // Returns the ID of this transaction.
  const TransactionID id() const { return id_; }

  // Returns the options for this transaction.
  const ReadOnlyOptions& options() const { return options_; }

 private:
  // Mutex that guards the Read method.
  absl::Mutex mu_;

  // Picks a read timestamp given transaction type and timestamp bound.
  absl::Time PickReadTimestamp();

  // Options with which the transaction was created.
  ReadOnlyOptions options_;

  // ID for this transaction.
  const TransactionID id_;

  // System-wide monotonic clock.
  Clock* clock_;

  // Underlying storage of the database.
  Storage* base_storage_;

  // VersionedCatalog for the database provided at transaction creation.
  const VersionedCatalog* const versioned_catalog_;

  // Transaction lock management.
  std::unique_ptr<LockHandle> lock_handle_;
  LockManager* lock_manager_;

  // The read timestamp picked by this transaction.
  absl::Time read_timestamp_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_READ_ONLY_TRANSACTION_H_
