//
// PostgreSQL is released under the PostgreSQL License, a liberal Open Source
// license, similar to the BSD or MIT licenses.
//
// PostgreSQL Database Management System
// (formerly known as Postgres, then as Postgres95)
//
// Portions Copyright © 1996-2020, The PostgreSQL Global Development Group
//
// Portions Copyright © 1994, The Regents of the University of California
//
// Portions Copyright 2023 Google LLC
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
// EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
// FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN
// "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
// MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
//------------------------------------------------------------------------------

#ifndef CATALOG_EMULATOR_PG_CATALOG_H_
#define CATALOG_EMULATOR_PG_CATALOG_H_

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

#endif  // CATALOG_EMULATOR_PG_CATALOG_H_
