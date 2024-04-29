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

#include "third_party/spanner_pg/catalog/pg_catalog.h"

#include <string>
#include <vector>

#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/value.h"
#include "zetasql/base/no_destructor.h"
#include "absl/container/flat_hash_set.h"
#include "absl/strings/str_cat.h"
#include "backend/query/info_schema_columns_metadata_values.h"
#include "backend/query/tables_from_metadata.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/catalog/view.h"

namespace postgres_translator {

namespace {

static constexpr char kDefaultSchema[] = "public";
static constexpr char kPGAvailableExtensionVersions[] =
    "pg_available_extension_versions";
static constexpr char kPGAvailableExtensions[] = "pg_available_extensions";
static constexpr char kPGBackendMemoryContexts[] = "pg_backend_memory_contexts";
static constexpr char kPGConfig[] = "pg_config";
static constexpr char kPGCursors[] = "pg_cursors";
static constexpr char kPGFileSettings[] = "pg_file_settings";
static constexpr char kPGHbaFileRules[] = "pg_hba_file_rules";
static constexpr char kPGIndexes[] = "pg_indexes";
static constexpr char kPGMatviews[] = "pg_matviews";
static constexpr char kPGPolicies[] = "pg_policies";
static constexpr char kPGPreparedXacts[] = "pg_prepared_xacts";
static constexpr char kPGPublicationTables[] = "pg_publication_tables";
static constexpr char kPGRules[] = "pg_rules";
static constexpr char kPGSettings[] = "pg_settings";
static constexpr char kPGShmemAllocations[] = "pg_shmem_allocations";
static constexpr char kPGTables[] = "pg_tables";
static constexpr char kPGViews[] = "pg_views";

using google::spanner::emulator::backend::Index;
using google::spanner::emulator::backend::kSpannerPGTypeToGSQLType;
using google::spanner::emulator::backend::PGCatalogColumnsMetadata;
using google::spanner::emulator::backend::Schema;
using google::spanner::emulator::backend::Table;
using google::spanner::emulator::backend::View;
using ::zetasql::values::Bool;
using ::zetasql::values::NullBool;
using ::zetasql::values::NullInt64;
using ::zetasql::values::NullString;
using ::zetasql::values::String;
using ::zetasql::values::StringArray;

static const zetasql_base::NoDestructor<absl::flat_hash_set<std::string>>
    kSupportedTables{{
        kPGAvailableExtensionVersions,
        kPGAvailableExtensions,
        kPGBackendMemoryContexts,
        kPGConfig,
        kPGCursors,
        kPGFileSettings,
        kPGHbaFileRules,
        kPGIndexes,
        kPGMatviews,
        kPGPolicies,
        kPGPreparedXacts,
        kPGPublicationTables,
        kPGRules,
        kPGSettings,
        kPGShmemAllocations,
        kPGTables,
        kPGViews,
    }};

template <typename T>
std::string PrimaryKeyName(const T* table) {
  return absl::StrCat("PK_", table->Name());
}

}  // namespace

PGCatalog::PGCatalog(const Schema* default_schema)
    : zetasql::SimpleCatalog(kName), default_schema_(default_schema) {
  tables_by_name_ = AddTablesFromMetadata(
      PGCatalogColumnsMetadata(), *kSpannerPGTypeToGSQLType, *kSupportedTables);
  for (auto& [name, table] : tables_by_name_) {
    std::vector<std::vector<zetasql::Value>> empty;
    table.get()->SetContents(empty);
    AddTable(table.get());
  }

  FillPGTablesTable();
  FillPGIndexesTable();
  FillPGSettingsTable();
  FillPGViewsTable();
}

void PGCatalog::FillPGIndexesTable() {
  auto pg_indexes = tables_by_name_.at(kPGIndexes).get();

  std::vector<std::vector<zetasql::Value>> rows;
  for (const Table* table : default_schema_->tables()) {
    // Add normal indexes.
    for (const Index* index : table->indexes()) {
      rows.push_back({
          // schemaname
          String(kDefaultSchema),
          // tablename
          String(table->Name()),
          // indexname
          String(index->Name()),
          // tablespace
          NullString(),
          // indexdef
          NullString(),
      });
    }

    // Add the primary key index.
    rows.push_back({
        // schemaname
        String(kDefaultSchema),
        // tablename
        String(table->Name()),
        // indexname
        String(PrimaryKeyName(table)),
        // tablespace
        NullString(),
        // indexdef
        NullString(),
    });
  }

  pg_indexes->SetContents(rows);
}

void PGCatalog::FillPGTablesTable() {
  auto pg_tables = tables_by_name_.at(kPGTables).get();

  std::vector<std::vector<zetasql::Value>> rows;
  for (const Table* table : default_schema_->tables()) {
    rows.push_back({
        // schemaname
        String(kDefaultSchema),
        // tablename
        String(table->Name()),
        // tableowner
        NullString(),
        // tablespace
        NullString(),
        // hasindexes
        Bool(!table->indexes().empty()),
        // hasrules
        NullBool(),
        // hastriggers
        NullBool(),
        // rowsecurity
        NullBool(),
    });
  }

  pg_tables->SetContents(rows);
}

void PGCatalog::FillPGSettingsTable() {
  auto pg_settings = tables_by_name_.at(kPGSettings).get();

  std::vector<std::vector<zetasql::Value>> rows;
  std::vector<std::string> enumvals;
  rows.push_back({
      // name
      String("max_index_keys"),
      // setting
      String("16"),
      // unit
      NullString(),
      // category
      String("Preset Options"),
      // short_desc
      String("Shows the maximum number of index keys."),
      // extra_desc
      NullString(),
      // context
      String("internal"),
      // vartype
      String("integer"),
      // source
      String("default"),
      // min_val
      String("16"),
      // max_val
      String("16"),
      // enumvals
      StringArray(enumvals),
      // boot_val
      String("16"),
      // reset_val
      String("16"),
      // sourcefile
      NullString(),
      // sourceline
      NullInt64(),
      // pending_restart
      Bool(false),
  });

  pg_settings->SetContents(rows);
}

void PGCatalog::FillPGViewsTable() {
  auto pg_views = tables_by_name_.at(kPGViews).get();

  std::vector<std::vector<zetasql::Value>> rows;
  for (const View* view : default_schema_->views()) {
    rows.push_back({
        // schemaname
        String(kDefaultSchema),
        // viewname
        String(view->Name()),
        // viewowner
        NullString(),
        // definition
        String(view->body_origin().value()),
    });
  }

  pg_views->SetContents(rows);
}

}  // namespace postgres_translator
