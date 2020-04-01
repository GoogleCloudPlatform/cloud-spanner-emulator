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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_LOCKING_REQUEST_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_LOCKING_REQUEST_H_

#include <vector>

#include "backend/common/ids.h"
#include "backend/datamodel/key_range.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Modes that a lock can be held in.
enum class LockMode {
  kShared,
  kExclusive,
};

// LockRequest encapsulates a single lock request from a transaction.
class LockRequest {
 public:
  LockRequest(LockMode mode, TableID table_id, const KeyRange& key_range,
              const std::vector<ColumnID>& column_ids);

 private:
  // The mode in which we want to acquire the lock.
  LockMode mode_;

  // The table which we want to lock.
  TableID table_id_;

  // The range of keys we want to lock.
  KeyRange key_range_;

  // The columns in this range that we want to lock.
  std::vector<ColumnID> column_ids_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_LOCKING_REQUEST_H_
