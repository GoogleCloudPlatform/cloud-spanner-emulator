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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_DATABASE_CHANGE_STREAM_CHANGE_STREAM_PARTITION_CHURNER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_DATABASE_CHANGE_STREAM_CHANGE_STREAM_PARTITION_CHURNER_H_

#include <functional>
#include <memory>
#include <string>
#include <thread>  // NOLINT
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/flags/declare.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "backend/schema/catalog/schema.h"
#include "backend/transaction/options.h"
#include "backend/transaction/read_write_transaction.h"
#include "common/clock.h"

// How often to terminate currently active change stream partitions.
ABSL_DECLARE_FLAG(absl::Duration, change_stream_churning_interval);

// How often to run the thread with the change stream churning logic.
ABSL_DECLARE_FLAG(absl::Duration, change_stream_churn_thread_sleep_interval);

// How long to sleep before retrying a failed change stream churn transaction.
ABSL_DECLARE_FLAG(absl::Duration,
                  change_stream_churn_thread_retry_sleep_interval);

// Jitter injected when sleeping before retrying a failed change stream
// churn transaction.
ABSL_DECLARE_FLAG(int, change_stream_churn_thread_retry_jitter);

// Whether the change stream churning should be enabled.
ABSL_DECLARE_FLAG(bool, enable_change_stream_churning);

// If set to X seconds, and it's greater than 0, then
// override the default partition token alive seconds from 20-40 seconds to X-2X
// seconds.
ABSL_DECLARE_FLAG(int, override_change_stream_partition_token_alive_seconds);

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// This class churns partitions for each change stream.
//
// Change stream queries in the emulator will run forever unless they are
// churned. Churning means terminating old partitions (start time is more than
// the allowed churn interval in the past) by inserting an end timestamp for
// the old partition into the partition table, and inserting new child
// partitions into the partition table. If two partitions have a parent-child
// relationship, the parent partition will contain the child in the children
// columns, and the child partition should contain the parent in the parent
//  columns. The end timestamp of the parent should be the same as the start
// timestamp of the child. We will run the churning logic for each change
// stream in a per-change stream background thread that will be cached
// in this class. We will add background threads or remove background threads
// from ChangeStreamChurningFactory every time there is a schema change that
// adds or removes change streams.
class ChangeStreamPartitionChurner {
 public:
  using CreateReadWriteTransactionFn =
      std::function<absl::StatusOr<std::unique_ptr<ReadWriteTransaction>>(
          const ReadWriteOptions& options, const RetryState& retry_state)>;

  ChangeStreamPartitionChurner(
      CreateReadWriteTransactionFn create_read_write_transaction_fn,
      Clock* clock);

  ~ChangeStreamPartitionChurner() { ClearAllChurningThreads(); }

  void Update(const Schema* schema);

  int GetNumThreads();

 private:
  std::vector<std::string> GetAllChangeStreamNames() const;

  struct ChurningThread {
    std::thread thread;
    bool stop_thread ABSL_GUARDED_BY(mu) = false;
    absl::Mutex mu;

    ~ChurningThread() {
      {
        absl::MutexLock l(&mu);
        ABSL_LOG(INFO) << "Stopping ChurningThread ";
        stop_thread = true;
      }
      // Join the thread.
      thread.join();
    }
  };

  void CreateChurningThread(absl::string_view change_stream_name);

  void ClearChurningThread(absl::string_view change_stream_name);

  void ClearAllChurningThreads();

  absl::Status ChurnPartitions(absl::string_view change_stream_name);

  void PeriodicChurnPartitions(absl::string_view change_stream_name,
                               ChurningThread* churning_thread);

  absl::Status MovePartition(absl::string_view change_stream_name,
                             absl::string_view partition_token,
                             ReadWriteTransaction* txn);

  absl::Status MergePartition(absl::string_view change_stream_name,
                              absl::string_view first_partition_token,
                              absl::string_view second_partition_token,
                              ReadWriteTransaction* txn);

  absl::Status SplitPartition(absl::string_view change_stream_name,
                              absl::string_view partition_token,
                              ReadWriteTransaction* txn);

  friend class ChangeStreamPartitionChurnerTest;

  CreateReadWriteTransactionFn create_read_write_transaction_fn_;

  // Clock shared across emulator components.
  Clock* clock_;

  mutable absl::Mutex mu_;

  absl::flat_hash_map<std::string, std::unique_ptr<ChurningThread>>
      churn_threads_ ABSL_GUARDED_BY(mu_);
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_DATABASE_CHANGE_STREAM_CHANGE_STREAM_PARTITION_CHURNER_H_
