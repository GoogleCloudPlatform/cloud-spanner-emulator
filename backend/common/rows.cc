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

#include "backend/common/rows.h"

#include "zetasql/base/statusor.h"
#include "common/errors.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

zetasql::Value GetColumnValueOrNull(const Row& row,
                                      const Column* const column) {
  const auto entry = row.find(column);
  return (entry != row.end()) ? entry->second
                              : zetasql::Value::Null(column->GetType());
}

Row MakeRow(absl::Span<const Column* const> columns, const ValueList& values) {
  Row row;
  for (int i = 0; i < columns.size(); ++i) {
    row.insert({columns[i], values[i]});
  }
  return row;
}

std::vector<ColumnID> GetColumnIDs(absl::Span<const Column* const> columns) {
  std::vector<ColumnID> column_ids;
  for (const Column* col : columns) {
    column_ids.emplace_back(col->id());
  }
  return column_ids;
}

zetasql_base::StatusOr<std::vector<const Column*>> GetColumnsByName(
    const Table* table, const std::vector<std::string>& column_names) {
  std::vector<const Column*> columns;
  for (const std::string& column_name : column_names) {
    const Column* column = table->FindColumn(column_name);
    if (column == nullptr) {
      return error::ColumnNotFound(table->Name(), column_name);
    }
    columns.emplace_back(column);
  }
  return columns;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
