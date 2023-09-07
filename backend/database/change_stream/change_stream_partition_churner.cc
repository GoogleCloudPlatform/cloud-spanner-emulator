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

#include "backend/database/change_stream/change_stream_partition_churner.h"

#include <memory>  // NOLINT
#include <string>
#include <thread>  // NOLINT
#include <utility>
#include <vector>

#include "zetasql/public/value.h"
#include "absl/container/flat_hash_set.h"
#include "absl/flags/flag.h"
#include "absl/log/log.h"
#include "absl/random/random.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "backend/access/read.h"
#include "backend/access/write.h"
#include "backend/datamodel/key_set.h"
#include "backend/schema/backfills/change_stream_backfill.h"
#include "backend/schema/catalog/change_stream.h"
#include "backend/schema/catalog/schema.h"
#include "backend/transaction/options.h"
#include "backend/transaction/read_write_transaction.h"
#include "common/change_stream.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

ABSL_FLAG(absl::Duration, change_stream_churning_interval, absl::Seconds(20),
          "Change stream churning interval in seconds.");

ABSL_FLAG(absl::Duration, change_stream_churn_thread_sleep_interval,
          absl::Seconds(20), "Change stream thread sleep interval.");

ABSL_FLAG(
    absl::Duration, change_stream_churn_thread_retry_sleep_interval,
    absl::Milliseconds(20),
    "How long to sleep when retrying a failed change stream transaction.");

ABSL_FLAG(
    int, change_stream_churn_thread_retry_jitter, 100,
    "How long to sleep when retrying a failed change stream transaction.");

ABSL_FLAG(bool, enable_change_stream_churning, true,
          "Whether to enable change stream churning.");

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

using zetasql::values::String;
using zetasql::values::StringArray;

void ChangeStreamPartitionChurner::CreateChurningThread(
    absl::string_view change_stream_name) {
  mu_.AssertHeld();
  if (!absl::GetFlag(FLAGS_enable_change_stream_churning)) {
    return;
  }
  auto churning_thread = std::make_unique<ChurningThread>();
  auto thread =
      std::thread(&ChangeStreamPartitionChurner::PeriodicChurnPartitions, this,
                  std::string(change_stream_name), churning_thread.get());
  churning_thread->thread = std::move(thread);
  churn_threads_.try_emplace(change_stream_name, std::move(churning_thread));
}

void ChangeStreamPartitionChurner::ClearChurningThread(
    absl::string_view change_stream_name) {
  mu_.AssertHeld();
  churn_threads_.erase(change_stream_name);
}

void ChangeStreamPartitionChurner::ClearAllChurningThreads() {
  absl::MutexLock l(&mu_);
  churn_threads_.clear();
}

void ChangeStreamPartitionChurner::PeriodicChurnPartitions(
    absl::string_view change_stream_name, ChurningThread* churning_thread) {
  while (true) {
    {
      absl::MutexLock l(&churning_thread->mu);
      churning_thread->mu.AwaitWithTimeout(
          absl::Condition(&churning_thread->stop_thread),
          absl::GetFlag(FLAGS_change_stream_churn_thread_sleep_interval));
      if (churning_thread->stop_thread) {
        return;
      }
    }
    absl::Status s;
    // In the current state, the emulator only allows one ongoing transaction
    // at a time. Thus, churn might fail occasionally due to conflict with
    // another ongoing transaction. We should retry the churn in cases of
    // failure.
    do {
      s = ChurnPartitions(change_stream_name);

      const auto delay =
          absl::GetFlag(FLAGS_change_stream_churn_thread_retry_jitter) *
          absl::Uniform<double>(absl::BitGen(), 0, 1);
      absl::SleepFor(
          absl::GetFlag(FLAGS_change_stream_churn_thread_retry_sleep_interval) +
          absl::Milliseconds(delay));
      if (!s.ok() && !absl::IsAborted(s)) {
        ABSL_LOG(ERROR) << "Failed to churn change stream " << change_stream_name
                   << " with status: " << s;
      }
    } while (!s.ok());
  }
}

// TODO: Change stream churn transactions can potentially cause
// user transactions to abort, since there can only be one concurrent
// transaction at a time in the emulator. We need to either update to
// table-level locking, or we need to implement waiting instead of aborting in
// the transaction lock manager.
absl::Status ChangeStreamPartitionChurner::ChurnPartitions(
    absl::string_view change_stream_name) {
  ZETASQL_ASSIGN_OR_RETURN(auto txn, create_read_write_transaction_fn_(
                                 ReadWriteOptions(), RetryState()));

  const Schema* schema = txn->schema();

  ZETASQL_RET_CHECK(schema != nullptr);

  const ChangeStream* change_stream =
      schema->FindChangeStream(std::string(change_stream_name));

  if (change_stream == nullptr) {
    return absl::OkStatus();
  }

  // Read the change stream partition table.
  backend::ReadArg read_arg;
  read_arg.change_stream_for_partition_table = change_stream->Name();
  read_arg.columns = {"partition_token", "start_time", "end_time", "parents",
                      "children"};
  read_arg.key_set = KeySet::All();
  std::unique_ptr<backend::RowCursor> cursor;
  absl::Status status = txn->Read(read_arg, &cursor);
  ZETASQL_RETURN_IF_ERROR(status);
  std::vector<std::string> churned_partitions;
  while (cursor->Next()) {
    // Only retrieve the active tokens that should be churned.
    if (cursor->ColumnValue(2).is_null()) {
      const std::string& partition_token =
          cursor->ColumnValue(0).string_value();
      const absl::Time start_time = cursor->ColumnValue(1).ToTime();
      const absl::Time expected_end_time =
          start_time + absl::GetFlag(FLAGS_change_stream_churning_interval);
      if (expected_end_time < clock_->Now()) {
        // Only churn the tokens whose start time is more than the specified
        // churn interval in the past.
        churned_partitions.push_back(partition_token);
      }
    }
  }
  for (const auto& churned_partition : churned_partitions) {
    // Churn the tokens retrieved above.
    ZETASQL_RETURN_IF_ERROR(
        ChurnPartition(change_stream_name, churned_partition, txn.get()));
  }
  return txn->Commit();
}

absl::Status ChangeStreamPartitionChurner::ChurnPartition(
    absl::string_view change_stream_name, absl::string_view partition_token,
    ReadWriteTransaction* txn) {
  // Generate a new partition token string
  const std::string new_partition_token = CreatePartitionTokenString();

  Mutation m;
  // Insert a new partition with the start time set to the transaction
  // commit timestamp, and the parents set to the churned partition's token
  // string.
  m.AddWriteOp(
      MutationOpType::kInsert,
      MakeChangeStreamPartitionTableName(change_stream_name),
      {"partition_token", "start_time", "parents"},
      {{String(new_partition_token), String("spanner.commit_timestamp()"),
        StringArray({std::string(partition_token)})}});
  // Modify the existing partition with the end timestamp set to the transaction
  // commit timestamp, and the children set to the new partition token.
  m.AddWriteOp(MutationOpType::kUpdate,
               MakeChangeStreamPartitionTableName(change_stream_name),
               {"partition_token", "end_time", "children"},
               {{String(partition_token), String("spanner.commit_timestamp()"),
                 StringArray({new_partition_token})}});
  return txn->Write(m);
}

void ChangeStreamPartitionChurner::Update(const Schema* schema) {
  // Iterate through the change streams in the schema.
  absl::MutexLock l(&mu_);
  absl::flat_hash_set<std::string> change_stream_names;
  for (const auto& [change_stream_name, churn_thread] : churn_threads_) {
    change_stream_names.insert(change_stream_name);
  }

  for (const auto* change_stream : schema->change_streams()) {
    // If the thread does not exist in the factory.
    if (!change_stream_names.contains(change_stream->Name())) {
      CreateChurningThread(change_stream->Name());
    }
  }

  // Remove all nonexistent change stream threads.
  for (auto& change_stream_name : change_stream_names) {
    if (schema->FindChangeStream(change_stream_name) == nullptr) {
      ClearChurningThread(change_stream_name);
    }
  }
}

int ChangeStreamPartitionChurner::GetNumThreads() {
  absl::MutexLock l(&mu_);
  return churn_threads_.size();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
