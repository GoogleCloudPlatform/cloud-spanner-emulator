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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_OPTIONS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_OPTIONS_H_

#include "absl/time/time.h"
#include "backend/common/ids.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Types of timestamp bounds for snapshot reads.
enum class TimestampBound {
  // Read the latest data.
  kStrongRead,

  // Read a version of the data that's no staler than a bound.
  kMaxStaleness,

  // Read a version of the data that's stale by a specified duration.
  kExactStaleness,

  // Read a version of the data that's no staler than the specified timestamp.
  kMinTimestamp,

  // Read a version of the data at the specified timestamp.
  kExactTimestamp,
};

// Options for creating a read only transaction.
struct ReadOnlyOptions {
  // The timestamp bound for snapshot reads.
  TimestampBound bound = TimestampBound::kStrongRead;

  // The read staleness for kMaxStaleness and kExactStaleness.
  absl::Duration staleness = absl::ZeroDuration();

  // The read timestamp for kMinTimestamp and kExactTimestamp.
  absl::Time timestamp = absl::InfinitePast();
};

// Maintains the lock priority and retry attempts for a transaction such that
// subsequent retries have fewer chances of an ABORT error. Documentation:
// https://cloud.google.com/spanner/docs/reference/rest/v1/TransactionOptions#retrying-aborted-transactions
struct RetryState {
  // Initial abort retry count of the transaction.
  int abort_retry_count = 0;

  // Priority of the transaction.
  TransactionPriority priority = 0;
};

// Options for creating a read write transaction.
struct ReadWriteOptions {};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_TMP_OPTIONS_H_
