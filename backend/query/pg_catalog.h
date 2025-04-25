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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_PG_CATALOG_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_PG_CATALOG_H_

#include <memory>
#include <string>

#include "zetasql/public/simple_catalog.h"
#include "absl/container/flat_hash_map.h"
#include "backend/query/info_schema_columns_metadata_values.h"
#include "backend/schema/catalog/schema.h"
#include "third_party/spanner_pg/catalog/engine_system_catalog.h"

namespace postgres_translator {

// A sub-catalog used for serving queries to the PG catalog by the Cloud Spanner
// Emulator.
class PGCatalog : public zetasql::SimpleCatalog {
 public:
  static constexpr char kName[] = "pg_catalog";

  explicit PGCatalog(
      const EnumerableCatalog* root_catalog,
      const google::spanner::emulator::backend::Schema* default_schema);

 private:
  const EnumerableCatalog* root_catalog_;
  const google::spanner::emulator::backend::Schema* default_schema_;

  const postgres_translator::EngineSystemCatalog* system_catalog_ =
      postgres_translator::EngineSystemCatalog::GetEngineSystemCatalog();

  // Explicitly storing the tables because we are using SimpleCatalog::AddTable
  // which expects that the caller maintains the ownership of the added objects.
  absl::flat_hash_map<std::string, std::unique_ptr<zetasql::SimpleTable>>
      tables_by_name_;

  std::map<std::string,
           std::vector<google::spanner::emulator::backend::ColumnsMetaEntry>>
      info_schema_table_name_to_column_metadata_;
  std::map<std::string,
           std::vector<google::spanner::emulator::backend::ColumnsMetaEntry>>
      pg_catalog_table_name_to_column_metadata_;
  std::map<std::string,
           std::vector<
               google::spanner::emulator::backend::SpannerSysColumnsMetaEntry>>
      spanner_sys_table_name_to_column_metadata_;

  void FillPGAmTable();
  void FillPGAttrdefTable();
  void FillPGAttributeTable();
  void FillPGClassTable();
  void FillPGCollationTable();
  void FillPGConstraintTable();
  void FillPGIndexTable();
  void FillPGIndexesTable();
  void FillPGNamespaceTable();
  void FillPGProcTable();
  void FillPGSequenceTable();
  void FillPGSequencesTable();
  void FillPGSettingsTable();
  void FillPGTablesTable();
  void FillPGTypeTable();
  void FillPGViewsTable();
};

}  // namespace postgres_translator

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_PG_CATALOG_H_
