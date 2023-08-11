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

#include "backend/query/tables_from_metadata.h"

#include <memory>
#include <string>
#include <vector>

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

absl::flat_hash_map<std::string, std::unique_ptr<zetasql::SimpleTable>>
AddTablesFromMetadata(
    const std::vector<ColumnsMetaEntry>& metadata_entries,
    const absl::flat_hash_map<std::string, const zetasql::Type*>&
        spanner_to_gsql_type,
    const absl::flat_hash_set<std::string>& supported_tables) {
  absl::flat_hash_map<std::string, std::unique_ptr<zetasql::SimpleTable>>
      tables_by_name;
  if (metadata_entries.empty()) {
    // No tables to create.
    return tables_by_name;
  }

  std::string table_name = "";
  std::vector<zetasql::SimpleTable::NameAndType> columns;
  for (auto it = metadata_entries.cbegin(); it != metadata_entries.cend();
       ++it) {
    // This is a table we're in the process of creating so add the next
    // column.
    if (!table_name.empty() && it->table_name == table_name) {
      columns.push_back(
          {it->column_name, spanner_to_gsql_type.at(it->spanner_type)});
      continue;
    }

    // It's a new table so if there was an existing table that we were in the
    // process of creating, create the table.
    if (!table_name.empty()) {
      tables_by_name[table_name] =
          std::make_unique<zetasql::SimpleTable>(table_name, columns);

      // Clear the table name and columns for the next table.
      table_name = "";
      columns.clear();
    }

    // We need to check if the new table is one that we support.
    auto supported_tables_it = supported_tables.find(it->table_name);
    if (supported_tables_it != supported_tables.end()) {
      // If we've seen this table before, then the metadata is not ordered by
      // the table name so we have to crash.
      ZETASQL_CHECK(  // crash ok
          tables_by_name.find(it->table_name) == tables_by_name.end())
          << "invalid metadata";
      table_name = it->table_name;
      columns.push_back(
          {it->column_name, spanner_to_gsql_type.at(it->spanner_type)});
    }
  }
  // If the last supported table hasn't been created yet, create it now.
  if (!table_name.empty()) {
    tables_by_name[table_name] =
        std::make_unique<zetasql::SimpleTable>(table_name, columns);
  }

  return tables_by_name;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
