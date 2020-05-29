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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_FLUSH_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_FLUSH_H_

#include "backend/actions/ops.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// TODO : Add support to write multiple ops to base storage
// atomically.

// Flushes each of the write ops to base storage at the given timestamp. Note
// that calling this function isn't thread safe and appropriate database locks
// should be acquired.
absl::Status FlushWriteOpsToStorage(const std::vector<WriteOp>& write_ops,
                                    Storage* base_storage,
                                    absl::Time commit_timestamp);

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_FLUSH_H_
