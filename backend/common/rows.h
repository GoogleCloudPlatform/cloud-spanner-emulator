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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_COMMON_ROWS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_COMMON_ROWS_H_

#include "zetasql/base/statusor.h"
#include "backend/datamodel/value.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Common row abstraction used by various parts of the backend.
using Row = absl::flat_hash_map<const Column* const, zetasql::Value>;

// Returns the value from the given Row for the specified column. Returns Null
// if the specified column was not found.
zetasql::Value GetColumnValueOrNull(const Row& row,
                                      const Column* const column);

// Returns the column/value map for the given column & value list.
Row MakeRow(absl::Span<const Column* const> columns, const ValueList& values);

// Returns a list of ColumnIDs from the given set of columns.
std::vector<ColumnID> GetColumnIDs(absl::Span<const Column* const> columns);

// Returns the Column* from the given set of column names.
zetasql_base::StatusOr<std::vector<const Column*>> GetColumnsByName(
    const Table* table, const std::vector<std::string>& column_names);

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_COMMON_ROWS_H_
