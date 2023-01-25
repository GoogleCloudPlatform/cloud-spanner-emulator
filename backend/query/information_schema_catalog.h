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

#include <string>
#include <vector>

#include "zetasql/public/simple_catalog.h"
#include "absl/types/optional.h"
#include "absl/types/variant.h"
#include "backend/schema/catalog/schema.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

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

  explicit InformationSchemaCatalog(const Schema* default_schema);

  struct ColumnsMetaEntry {
    std::string table_name;
    std::string column_name;
    std::string is_nullable;
    std::string spanner_type;
  };

  struct IndexColumnsMetaEntry {
    std::string table_name;
    std::string column_name;
    std::string is_nullable;
    std::string column_ordering;
    std::string spanner_type;

    // Not all information schema tables have a primary key on the prefix of
    // table columns. CONSTRAINT_COLUMN_USAGE, for example, has table columns:
    //
    //   TABLE_CATALOG
    //   TABLE_SCHEMA
    //   TABLE_NAME
    //   COLUMN_NAME
    //   CONSTRAINT_CATALOG
    //   CONSTRAINT_SCHEMA
    //   CONSTRAINT_NAME
    //
    // but its primary key is:
    //
    //   CONSTRAINT_CATALOG
    //   CONSTRAINT_SCHEMA
    //   CONSTRAINT_NAME
    //   COLUMN_NAME
    //
    // which corresponds to ordinal positions [1, 2, 3, 4].
    //
    // The default position of 0 indicates that the column is in the same
    // ordinal position in both the table and its primary key.
    int primary_key_ordinal = 0;
  };

  const std::vector<ColumnsMetaEntry>& ColumnsMetadata();
  const std::vector<IndexColumnsMetaEntry>& IndexColumnsMetadata();

 private:
  const Schema* default_schema_;

  const ColumnsMetaEntry& GetColumnMetadata(const zetasql::Table* table,
                                            const zetasql::Column* column);
  const IndexColumnsMetaEntry* FindKeyColumnMetadata(
      const zetasql::Table* table, const zetasql::Column* column);

  std::vector<ColumnsMetaEntry> PopulateInformationSchemaMetadata();
  std::vector<IndexColumnsMetaEntry>
  PopulateInformationSchemaMetadataForIndex();

  void AddSchemataTable();
  void AddSpannerStatisticsTable();
  void AddDatabaseOptionsTable();

  zetasql::SimpleTable* AddTablesTable();
  void FillTablesTable(zetasql::SimpleTable* tables);

  zetasql::SimpleTable* AddColumnsTable();
  void FillColumnsTable(zetasql::SimpleTable* columns);

  zetasql::SimpleTable* AddColumnColumnUsageTable();
  void FillColumnColumnUsageTable(zetasql::SimpleTable* column_column_usage);

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
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_INFORMATION_SCHEMA_CATALOG_H_
