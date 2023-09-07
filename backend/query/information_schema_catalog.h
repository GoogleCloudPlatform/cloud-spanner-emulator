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
#include "third_party/spanner_pg/ddl/spangres_direct_schema_printer_impl.h"

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
  static constexpr char kPGName[] = "PG_INFORMATION_SCHEMA";

  explicit InformationSchemaCatalog(const std::string& catalog_name,
                                    const Schema* default_schema);

 private:
  const Schema* default_schema_;
  const ::google::spanner::admin::database::v1::DatabaseDialect dialect_;
  absl::flat_hash_map<std::string, std::unique_ptr<zetasql::SimpleTable>>
      tables_by_name_;
  std::unique_ptr<postgres_translator::spangres::SpangresSchemaPrinter>
      pg_schema_printer_;

  inline std::string GetNameForDialect(absl::string_view name);
  std::pair<zetasql::Value, zetasql::Value> GetPGDataTypeAndSpannerType(
      const zetasql::Type* type, std::optional<int64_t> length);

  zetasql::Value GetSpannerType(const Column* column);
  zetasql::Value GetSpannerType(const zetasql::Type* type,
                                  std::optional<int64_t> length);
  zetasql::Value PGDataType(const zetasql::Type* type);
  inline zetasql::Value DialectDefaultSchema();
  inline zetasql::Value DialectBoolValue(bool value);
  zetasql::Value DialectColumnOrdering(const KeyColumn* column);

  void FillSchemataTable();
  void FillSpannerStatisticsTable();
  void FillDatabaseOptionsTable();

  void FillTablesTable();
  void FillColumnsTable();
  void FillColumnColumnUsageTable();
  void FillIndexesTable();
  void FillIndexColumnsTable();

  void AddColumnOptionsTable();

  void FillTableConstraintsTable();
  void FillCheckConstraintsTable();
  void FillConstraintTableUsageTable();
  void FillReferentialConstraintsTable();
  void FillKeyColumnUsageTable();
  void FillConstraintColumnUsageTable();
  void FillViewsTable();
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_INFORMATION_SCHEMA_CATALOG_H_
