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

#include "backend/transaction/read_only_transaction.h"

#include <memory>

#include "absl/random/random.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/time/time.h"
#include "backend/access/read.h"
#include "backend/common/ids.h"
#include "backend/datamodel/key_set.h"
#include "backend/locking/manager.h"
#include "backend/storage/in_memory_iterator.h"
#include "backend/storage/storage.h"
#include "backend/transaction/options.h"
#include "backend/transaction/resolve.h"
#include "backend/transaction/row_cursor.h"
#include "common/clock.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

absl::Duration kMaxStaleReadDuration = absl::Hours(1);

}  // namespace

ReadOnlyTransaction::ReadOnlyTransaction(
    const ReadOnlyOptions& options, TransactionID transaction_id, Clock* clock,
    Storage* storage, LockManager* lock_manager,
    const VersionedCatalog* const versioned_catalog)
    : options_(options),
      id_(transaction_id),
      clock_(clock),
      base_storage_(storage),
      versioned_catalog_(versioned_catalog),
      lock_manager_(lock_manager) {
  lock_handle_ = lock_manager_->CreateHandle(transaction_id, /*priority=*/1);
  read_timestamp_ = PickReadTimestamp();
}

absl::Status ReadOnlyTransaction::Read(const ReadArg& read_arg,
                                       std::unique_ptr<RowCursor>* cursor) {
  absl::MutexLock lock(&mu_);
  // Wait for any concurrent schema change or read-write transactions to commit
  // before accessing database state to perform a read.
  lock_handle_->WaitForSafeRead(read_timestamp_);
  if (clock_->Now() - read_timestamp_ >= kMaxStaleReadDuration) {
    return error::ReadTimestampPastVersionGCLimit(read_timestamp_);
  }

  ZETASQL_ASSIGN_OR_RETURN(const ResolvedReadArg resolved_read_arg,
                   ResolveReadArg(read_arg, schema()));

  std::vector<std::unique_ptr<StorageIterator>> iterators;
  for (const auto& key_range : resolved_read_arg.key_ranges) {
    std::unique_ptr<StorageIterator> itr;
    ZETASQL_RETURN_IF_ERROR(base_storage_->Read(
        read_timestamp_, resolved_read_arg.table->id(), key_range,
        GetColumnIDs(resolved_read_arg.columns), &itr));
    iterators.push_back(std::move(itr));
  }
  *cursor = absl::make_unique<StorageIteratorRowCursor>(
      std::move(iterators), resolved_read_arg.columns);
  return absl::OkStatus();
}

const Schema* ReadOnlyTransaction::schema() const {
  // Wait for any concurrent schema change or read-write transactions to commit
  // before accessing database state to read schemas in versioned_catalog.
  lock_handle_->WaitForSafeRead(read_timestamp_);
  return versioned_catalog_->GetSchema(read_timestamp_);
}

absl::Time ReadOnlyTransaction::PickReadTimestamp() {
  auto get_random_stale_timestamp =
      [this](absl::Time min_timestamp) -> absl::Time {
    // Any reads performed on or before last_commit_timestamp are guaranteed to
    // see a consistent snapshots of all the commits that have already finished.
    // Thus, picked read timestamp need not be older than last_commit_timestamp.
    absl::Time last_commit_timestamp = lock_manager_->LastCommitTimestamp();
    if (min_timestamp < last_commit_timestamp) {
      min_timestamp = last_commit_timestamp;
    }
    absl::BitGen gen;
    int64_t random_staleness = absl::Uniform<int64_t>(
        gen, 0, absl::ToInt64Microseconds(clock_->Now() - min_timestamp));
    return clock_->Now() - absl::Microseconds(random_staleness);
  };
  switch (options_.bound) {
    case TimestampBound::kStrongRead: {
      read_timestamp_ = clock_->Now();
      break;
    }
    case TimestampBound::kExactTimestamp: {
      read_timestamp_ = options_.timestamp;
      break;
    }
    case TimestampBound::kExactStaleness: {
      read_timestamp_ = clock_->Now() - options_.staleness;
      break;
    }
    case TimestampBound::kMinTimestamp: {
      if (options_.timestamp >= clock_->Now()) {
        // If min timestamp bound is set in future, we want to wait until that
        // time arrives before returning a read result, thus set read_timestamp
        // to be same as the min timestamp provided.
        read_timestamp_ = options_.timestamp;
      } else {
        // Randomly choose staleness to mimic production behavior of reading
        // from potentially lagging replicas.
        read_timestamp_ = get_random_stale_timestamp(options_.timestamp);
      }
      break;
    }
    case TimestampBound::kMaxStaleness: {
      // Randomly choose staleness to mimic production behavior of reading from
      // potentially lagging replicas. Bounded staleness cannot be negative.
      read_timestamp_ =
          get_random_stale_timestamp(clock_->Now() - options_.staleness);
      break;
    }
  }
  return read_timestamp_;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
