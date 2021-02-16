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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_PRINT_DDL_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_PRINT_DDL_H_

#include "absl/memory/memory.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/foreign_key.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Prints the DDL statement for a column.
std::string PrintColumn(const Column* column);

// Prints the DDL statement for this KeyColumn.
std::string PrintKeyColumn(const KeyColumn* column);

// Prints the DDL statements for an index.
std::string PrintIndex(const Index* index);

// Prints the DDL statements for a table.
std::string PrintTable(const Table* table);

// Prints the DDL statements for a check constraint.
std::string PrintCheckConstraint(const CheckConstraint* check_constraint);

// Prints the DDL statements for a foreign key.
std::string PrintForeignKey(const ForeignKey* foreign_key);

// Converts an OnDeleteAction to its string representation.
std::string OnDeleteActionToString(Table::OnDeleteAction action);

// Converts a Cloud Spanner column type to its string representation.
std::string ColumnTypeToString(const zetasql::Type* type,
                               absl::optional<int64_t> max_length);

// Prints the DDL statements for all tables and indexes within the given schema.
std::vector<std::string> PrintDDLStatements(const Schema* schema);

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_PRINT_DDL_H_
