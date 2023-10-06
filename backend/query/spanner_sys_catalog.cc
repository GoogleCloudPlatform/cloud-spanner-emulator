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

#include "backend/query/spanner_sys_catalog.h"

#include <string>
#include <vector>

#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/value.h"
#include "absl/container/flat_hash_set.h"
#include "backend/query/info_schema_columns_metadata_values.h"
#include "backend/query/tables_from_metadata.h"
#include "zetasql/base/no_destructor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

using ::zetasql::values::Bool;
using ::zetasql::values::Date;
using ::zetasql::values::Int64;

static constexpr char kSupportedOptimizerVersions[] =
    "SUPPORTED_OPTIMIZER_VERSIONS";

static const zetasql_base::NoDestructor<absl::flat_hash_set<std::string>>
    kSupportedTables{{
        kSupportedOptimizerVersions,
    }};
}  // namespace

SpannerSysCatalog::SpannerSysCatalog() : zetasql::SimpleCatalog(kName) {
  // TODO: Use inheritance and pass SpannerSysColumnsMetadata
  // directly to AddTablesFromMetadata.
  std::vector<ColumnsMetaEntry> columns;
  for (const auto& column : SpannerSysColumnsMetadata()) {
    columns.push_back({.table_name = column.table_name,
                       .column_name = column.column_name,
                       .is_nullable = column.is_nullable,
                       .spanner_type = column.spanner_type});
  }

  tables_by_name_ = AddTablesFromMetadata(columns, *kSpannerTypeToGSQLType,
                                          *kSupportedTables);
  for (auto& [name, table] : tables_by_name_) {
    AddTable(table.get());
  }

  FillOptimizerVersionsTable();
}

void SpannerSysCatalog::FillOptimizerVersionsTable() {
  auto table = tables_by_name_.at(kSupportedOptimizerVersions).get();
  std::vector<std::vector<zetasql::Value>> rows;

  rows.push_back({// is_default
                  Bool(true),
                  // release_date: 2023-09-19
                  Date(19619),
                  // version
                  Int64(42)});

  table->SetContents(rows);
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
