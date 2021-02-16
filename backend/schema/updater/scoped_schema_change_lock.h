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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_UPDATER_SCOPED_SCHEMA_CHANGE_LOCK_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_UPDATER_SCOPED_SCHEMA_CHANGE_LOCK_H_

#include <memory>

#include "zetasql/base/statusor.h"
#include "absl/time/time.h"
#include "backend/datamodel/key_range.h"
#include "backend/locking/handle.h"
#include "backend/locking/manager.h"
#include "backend/locking/request.h"
#include "common/errors.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// A class that allows RAII acquisition of database locks for a schema change.
// Locks are held during the lifetime of this object and all locks and holds
// on reserved timestamps are released when going out of scope.
class ScopedSchemaChangeLock {
 public:
  ScopedSchemaChangeLock(TransactionID tid, LockManager* lock_manager) {
    lock_handle_ = lock_manager->CreateHandle(tid, TransactionPriority(1));

    // Use dummy arguments to represent a "database-wide lock".
    LockRequest req{LockMode::kExclusive, /*table_id=*/"", KeyRange::All(),
                    /*column_ids=*/{}};
    lock_handle_->EnqueueLock(req);
  }

  // Will wait to acquire locks on the database or return with a
  // FAILED_PRECONDITION error if a concurrent schema change or read-write
  // transaction was already in progress.
  absl::Status Wait() {
    absl::Status s = lock_handle_->Wait();
    if (!s.ok()) {
      ZETASQL_RET_CHECK_EQ(s.code(), absl::StatusCode::kAborted);
      return error::ConcurrentSchemaChangeOrReadWriteTxnInProgress();
    }
    return absl::OkStatus();
  }

  // Reserves a commit timestamp for the shcema change.
  zetasql_base::StatusOr<absl::Time> ReserveCommitTimestamp() {
    auto status_or = lock_handle_->ReserveCommitTimestamp();
    has_commit_timestamp_ = status_or.status().ok();
    return status_or;
  }

  ~ScopedSchemaChangeLock() {
    if (has_commit_timestamp_) {
      absl::Status s = lock_handle_->MarkCommitted();
    }
    lock_handle_->UnlockAll();
  }

 private:
  std::unique_ptr<LockHandle> lock_handle_;

  bool has_commit_timestamp_ = false;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_UPDATER_SCOPED_SCHEMA_CHANGE_LOCK_H_
