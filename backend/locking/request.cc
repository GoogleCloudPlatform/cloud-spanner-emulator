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

#include "backend/locking/request.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

LockRequest::LockRequest(LockMode mode, TableID table_id,
                         const KeyRange& key_range,
                         const std::vector<ColumnID>& column_ids)
    : mode_(mode),
      table_id_(table_id),
      key_range_(key_range),
      column_ids_(column_ids) {}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
