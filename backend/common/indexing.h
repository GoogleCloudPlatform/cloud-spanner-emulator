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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_COMMON_INDEXING_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_COMMON_INDEXING_H_

#include "zetasql/public/value.h"
#include "zetasql/base/statusor.h"
#include "backend/common/rows.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/value.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/catalog/table.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Computes the index key from the given row.
zetasql_base::StatusOr<Key> ComputeIndexKey(const Row& base_row, const Index* index);

// Computes an the ordered list of index row values using the given base row.
ValueList ComputeIndexValues(const Row& base_row, const Index* index);

// Returns true if no index entry should be added for the given base table key.
bool ShouldFilterIndexKey(const Index* index, const Key& key);

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_COMMON_INDEXING_H_
