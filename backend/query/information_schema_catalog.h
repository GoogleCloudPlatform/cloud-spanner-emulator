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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_INFORMATION_SCHEMA_CATALOG_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_INFORMATION_SCHEMA_CATALOG_H_

#include <memory>
#include <string>
#include <vector>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "zetasql/public/simple_catalog.h"
#include "absl/container/flat_hash_map.h"
#include "backend/schema/catalog/schema.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

struct ColumnsMetaEntry;
struct IndexColumnsMetaEntry;

// InformationSchemaCatalog provides the INFORMATION_SCHEMA tables.
//
// ZetaSQL reference implementation accesses table data via the catalog objects
// themselves. Hence, this class provides both the catalog and the data
// backing the information schema tables.
//
// Cloud Spanner's information schema is documented at:
//   https://cloud.google.com/spanner/docs/information-schema
//
// The emulator only exposes the default (user) schema and INFORMATION_SCHEMA.
// In production, SPANNER_SYS schemas are also exposed which are not available
// in the emulator.
//
// This class is tested via tests/conformance/cases/information_schema.cc
class InformationSchemaCatalog : public zetasql::SimpleCatalog {
 public:
  static constexpr char kName[] = "INFORMATION_SCHEMA";

  explicit InformationSchemaCatalog(const std::string& catalog_name,
                                    const Schema* default_schema);

 private:
  const Schema* default_schema_;
  const ::google::spanner::admin::database::v1::DatabaseDialect dialect_;
  absl::flat_hash_map<std::string, std::unique_ptr<zetasql::SimpleTable>>
      tables_by_name_;

  inline std::string GetNameForDialect(absl::string_view name);
  std::pair<zetasql::Value, zetasql::Value> GetPGDataTypeAndSpannerType(
      const zetasql::Type* type, std::optional<int64_t> length);

  void FillSchemataTable();
  void FillSpannerStatisticsTable();
  void FillDatabaseOptionsTable();

  void FillTablesTable();
  void FillColumnsTable();

  void FillColumnColumnUsageTable();

  zetasql::SimpleTable* AddIndexesTable();
  void FillIndexesTable(zetasql::SimpleTable* indexes);

  zetasql::SimpleTable* AddIndexColumnsTable();
  void FillIndexColumnsTable(zetasql::SimpleTable* index_columns);

  void AddColumnOptionsTable();

  zetasql::SimpleTable* AddTableConstraintsTable();
  void FillTableConstraintsTable(zetasql::SimpleTable* table_constraints);

  zetasql::SimpleTable* AddCheckConstraintsTable();
  void FillCheckConstraintsTable(zetasql::SimpleTable* check_constraints);

  zetasql::SimpleTable* AddConstraintTableUsageTable();
  void FillConstraintTableUsageTable(
      zetasql::SimpleTable* constraint_table_usage);

  zetasql::SimpleTable* AddReferentialConstraintsTable();
  void FillReferentialConstraintsTable(
      zetasql::SimpleTable* referential_constraints);

  zetasql::SimpleTable* AddKeyColumnUsageTable();
  void FillKeyColumnUsageTable(zetasql::SimpleTable* key_column_usage);

  zetasql::SimpleTable* AddConstraintColumnUsageTable();
  void FillConstraintColumnUsageTable(
      zetasql::SimpleTable* constraint_column_usage);

  void FillViewsTable();
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_INFORMATION_SCHEMA_CATALOG_H_
