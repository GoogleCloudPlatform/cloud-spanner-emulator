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

#include "backend/query/pg_catalog.h"

#include <map>
#include <string>
#include <vector>

#include "zetasql/public/catalog.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "zetasql/base/no_destructor.h"
#include "absl/container/flat_hash_set.h"
#include "absl/strings/str_cat.h"
#include "backend/query/info_schema_columns_metadata_values.h"
#include "backend/query/tables_from_metadata.h"
#include "backend/schema/catalog/change_stream.h"
#include "backend/schema/catalog/check_constraint.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/foreign_key.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/catalog/named_schema.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/sequence.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/catalog/view.h"
#include "third_party/spanner_pg/bootstrap_catalog/bootstrap_catalog.h"
#include "third_party/spanner_pg/catalog/type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_oid_type.h"
#include "third_party/spanner_pg/interface/bootstrap_catalog_accessor.h"

namespace postgres_translator {

namespace {

static constexpr char kDefaultSchema[] = "public";
static constexpr char kPGAm[] = "pg_am";
static constexpr char kPGAttrdef[] = "pg_attrdef";
static constexpr char kPGAttribute[] = "pg_attribute";
static constexpr char kPGAvailableExtensionVersions[] =
    "pg_available_extension_versions";
static constexpr char kPGAvailableExtensions[] = "pg_available_extensions";
static constexpr char kPGBackendMemoryContexts[] = "pg_backend_memory_contexts";
static constexpr char kPGClass[] = "pg_class";
static constexpr char kPGCollation[] = "pg_collation";
static constexpr char kPGConfig[] = "pg_config";
static constexpr char kPGConstraint[] = "pg_constraint";
static constexpr char kPGCursors[] = "pg_cursors";
static constexpr char kPGDescription[] = "pg_description";
static constexpr char kPGEnum[] = "pg_enum";
static constexpr char kPGExtension[] = "pg_extension";
static constexpr char kPGFileSettings[] = "pg_file_settings";
static constexpr char kPGHbaFileRules[] = "pg_hba_file_rules";
static constexpr char kPGIndex[] = "pg_index";
static constexpr char kPGIndexes[] = "pg_indexes";
static constexpr char kPGLanguage[] = "pg_language";
static constexpr char kPGMatviews[] = "pg_matviews";
static constexpr char kPGNamespace[] = "pg_namespace";
static constexpr char kPGPolicies[] = "pg_policies";
static constexpr char kPGPreparedXacts[] = "pg_prepared_xacts";
static constexpr char kPGProc[] = "pg_proc";
static constexpr char kPGPublicationTables[] = "pg_publication_tables";
static constexpr char kPGRange[] = "pg_range";
static constexpr char kPGRoles[] = "pg_roles";
static constexpr char kPGRules[] = "pg_rules";
static constexpr char kPGSequence[] = "pg_sequence";
static constexpr char kPGSequences[] = "pg_sequences";
static constexpr char kPGSettings[] = "pg_settings";
static constexpr char kPGShmemAllocations[] = "pg_shmem_allocations";
static constexpr char kPGTables[] = "pg_tables";
static constexpr char kPGType[] = "pg_type";
static constexpr char kPGViews[] = "pg_views";

using google::spanner::emulator::backend::ChangeStream;
using google::spanner::emulator::backend::CheckConstraint;
using google::spanner::emulator::backend::Column;
using google::spanner::emulator::backend::ForeignKey;
using google::spanner::emulator::backend::Index;
using google::spanner::emulator::backend::KeyColumn;
using google::spanner::emulator::backend::kSpannerPGTypeToGSQLType;
using google::spanner::emulator::backend::NamedSchema;
using google::spanner::emulator::backend::PGCatalogColumnsMetadata;
using google::spanner::emulator::backend::PGColumnsMetadata;
using google::spanner::emulator::backend::Schema;
using google::spanner::emulator::backend::SDLObjectName;
using google::spanner::emulator::backend::Sequence;
using google::spanner::emulator::backend::SpannerSysColumnsMetadata;
using google::spanner::emulator::backend::Table;
using google::spanner::emulator::backend::View;
using ::zetasql::types::Int64ArrayType;
using ::zetasql::types::StringArrayType;
using ::zetasql::values::Bool;
using ::zetasql::values::Int64;
using ::zetasql::values::Int64Array;
using ::zetasql::values::Null;
using ::zetasql::values::NullBool;
using ::zetasql::values::NullDouble;
using ::zetasql::values::NullInt64;
using ::zetasql::values::NullString;
using ::zetasql::values::String;
using ::zetasql::values::StringArray;
using postgres_translator::spangres::datatypes::CreatePgOidValue;
using postgres_translator::spangres::datatypes::GetPgOidArrayType;
using spangres::datatypes::NullPgOid;

struct PgClassSystemTableMetadata {
  std::string table_name;
  int table_oid;
  int namespace_oid;
  int column_count;
  bool is_view;
};
using spangres::datatypes::GetPgOidArrayType;

static const zetasql_base::NoDestructor<absl::flat_hash_set<std::string>>
    kSupportedTables{{
        kPGAm,
        kPGAttrdef,
        kPGAttribute,
        kPGAvailableExtensionVersions,
        kPGAvailableExtensions,
        kPGBackendMemoryContexts,
        kPGClass,
        kPGCollation,
        kPGConfig,
        kPGConstraint,
        kPGCursors,
        kPGDescription,
        kPGEnum,
        kPGExtension,
        kPGFileSettings,
        kPGHbaFileRules,
        kPGIndex,
        kPGIndexes,
        kPGLanguage,
        kPGMatviews,
        kPGNamespace,
        kPGPolicies,
        kPGPreparedXacts,
        kPGProc,
        kPGPublicationTables,
        kPGRange,
        kPGRoles,
        kPGRules,
        kPGSequence,
        kPGSequences,
        kPGSettings,
        kPGShmemAllocations,
        kPGTables,
        kPGType,
        kPGViews,
    }};

const auto kSupportedCollations =
    absl::flat_hash_set<absl::string_view>({"default", "C"});

const auto kHardCodedNamedSchemaOid =
    absl::flat_hash_map<absl::string_view, uint32_t>(
        {{"pg_catalog", 11},
         {"public", 2200},
         {"spanner", 50000},
         {"information_schema", 75003},
         {"spanner_sys", 75004}});

// System table OID mappings.
const auto kHardCodedPgCatalogTableOid =
    absl::flat_hash_map<absl::string_view, uint32_t>({
        {"pg_catalog.pg_aggregate", 2600},
        {"pg_catalog.pg_am", 2601},
        {"pg_catalog.pg_amop", 2602},
        {"pg_catalog.pg_amproc", 2603},
        {"pg_catalog.pg_attrdef", 2604},
        {"pg_catalog.pg_attribute", 1249},
        {"pg_catalog.pg_auth_members", 1261},
        {"pg_catalog.pg_authid", 1260},
        {"pg_catalog.pg_cast", 2605},
        {"pg_catalog.pg_class", 1259},
        {"pg_catalog.pg_collation", 3456},
        {"pg_catalog.pg_constraint", 2606},
        {"pg_catalog.pg_conversion", 2607},
        {"pg_catalog.pg_database", 1262},
        {"pg_catalog.pg_db_role_setting", 2964},
        {"pg_catalog.pg_default_acl", 826},
        {"pg_catalog.pg_depend", 2608},
        {"pg_catalog.pg_description", 2609},
        {"pg_catalog.pg_enum", 3501},
        {"pg_catalog.pg_event_trigger", 3466},
        {"pg_catalog.pg_extension", 3079},
        {"pg_catalog.pg_foreign_data_wrapper", 2328},
        {"pg_catalog.pg_foreign_server", 1417},
        {"pg_catalog.pg_foreign_table", 3118},
        {"pg_catalog.pg_index", 2610},
        {"pg_catalog.pg_inherits", 2611},
        {"pg_catalog.pg_init_privs", 3394},
        {"pg_catalog.pg_language", 2612},
        {"pg_catalog.pg_largeobject", 2613},
        {"pg_catalog.pg_largeobject_metadata", 2995},
        {"pg_catalog.pg_namespace", 2615},
        {"pg_catalog.pg_opclass", 2616},
        {"pg_catalog.pg_operator", 2617},
        {"pg_catalog.pg_opfamily", 2753},
        {"pg_catalog.pg_parameter_acl", 6243},
        {"pg_catalog.pg_partitioned_table", 3350},
        {"pg_catalog.pg_policy", 3256},
        {"pg_catalog.pg_proc", 1255},
        {"pg_catalog.pg_publication", 6104},
        {"pg_catalog.pg_publication_namespace", 6237},
        {"pg_catalog.pg_publication_rel", 6106},
        {"pg_catalog.pg_range", 3541},
        {"pg_catalog.pg_replication_origin", 6000},
        {"pg_catalog.pg_rewrite", 2618},
        {"pg_catalog.pg_seclabel", 3596},
        {"pg_catalog.pg_sequence", 2224},
        {"pg_catalog.pg_shdepend", 1214},
        {"pg_catalog.pg_shdescription", 2396},
        {"pg_catalog.pg_shseclabel", 3592},
        {"pg_catalog.pg_statistic", 2619},
        {"pg_catalog.pg_statistic_ext", 3381},
        {"pg_catalog.pg_statistic_ext_data", 3429},
        {"pg_catalog.pg_subscription", 6100},
        {"pg_catalog.pg_subscription_rel", 6102},
        {"pg_catalog.pg_tablespace", 1213},
        {"pg_catalog.pg_transform", 3576},
        {"pg_catalog.pg_trigger", 2620},
        {"pg_catalog.pg_ts_config", 3602},
        {"pg_catalog.pg_ts_config_map", 3603},
        {"pg_catalog.pg_ts_dict", 3600},
        {"pg_catalog.pg_ts_parser", 3601},
        {"pg_catalog.pg_ts_template", 3764},
        {"pg_catalog.pg_type", 1247},
        {"pg_catalog.pg_user_mapping", 1418},
    });

// System view OID mappings.
const auto kHardCodedSystemViewOid =
    absl::flat_hash_map<absl::string_view, uint32_t>({
        {"pg_catalog.pg_available_extensions", 75008},
        {"pg_catalog.pg_available_extension_versions", 75009},
        {"pg_catalog.pg_backend_memory_contexts", 75010},
        {"pg_catalog.pg_config", 75011},
        {"pg_catalog.pg_cursors", 75012},
        {"pg_catalog.pg_file_settings", 75013},
        {"pg_catalog.pg_group", 75014},
        {"pg_catalog.pg_hba_file_rules", 75015},
        {"pg_catalog.pg_indexes", 75016},
        {"pg_catalog.pg_locks", 75017},
        {"pg_catalog.pg_matviews", 75018},
        {"pg_catalog.pg_policies", 75019},
        {"pg_catalog.pg_prepared_statements", 75020},
        {"pg_catalog.pg_prepared_xacts", 75021},
        {"pg_catalog.pg_publication_tables", 75022},
        {"pg_catalog.pg_replication_origin_status", 75023},
        {"pg_catalog.pg_replication_slots", 75024},
        {"pg_catalog.pg_roles", 75025},
        {"pg_catalog.pg_rules", 75026},
        {"pg_catalog.pg_seclabels", 75027},
        {"pg_catalog.pg_sequences", 75028},
        {"pg_catalog.pg_shadow", 75029},
        {"pg_catalog.pg_shmem_allocations", 75030},
        {"pg_catalog.pg_stats", 75031},
        {"pg_catalog.pg_stats_ext", 75032},
        {"pg_catalog.pg_stats_ext_exprs", 75033},
        {"pg_catalog.pg_tables", 75034},
        {"pg_catalog.pg_timezone_abbrevs", 75035},
        {"pg_catalog.pg_timezone_names", 75036},
        {"pg_catalog.pg_user", 75037},
        {"pg_catalog.pg_user_mappings", 75038},
        {"pg_catalog.pg_views", 75039},
        {"pg_catalog.pg_settings", 75040},

        {"information_schema.information_schema_catalog_name", 75041},
        {"information_schema.check_constraints", 75042},
        {"information_schema.columns", 75043},
        {"information_schema.column_column_usage", 75044},
        {"information_schema.column_options", 75045},
        {"information_schema.constraint_column_usage", 75046},
        {"information_schema.constraint_table_usage", 75047},
        {"information_schema.database_options", 75048},
        {"information_schema.indexes", 75049},
        {"information_schema.index_columns", 75050},
        {"information_schema.key_column_usage", 75051},
        {"information_schema.referential_constraints", 75052},
        {"information_schema.schemata", 75053},
        {"information_schema.spanner_statistics", 75054},
        {"information_schema.table_constraints", 75055},
        {"information_schema.tables", 75056},
        {"information_schema.views", 75057},
        {"information_schema.change_streams", 75058},
        {"information_schema.change_stream_columns", 75059},
        {"information_schema.change_stream_tables", 75060},
        {"information_schema.change_stream_options", 75061},
        {"information_schema.sequences", 75062},
        {"information_schema.parameters", 75063},
        {"information_schema.routines", 75064},
        {"information_schema.routine_options", 75065},
        {"information_schema.applicable_roles", 75066},
        {"information_schema.change_stream_privileges", 75067},
        {"information_schema.column_privileges", 75068},
        {"information_schema.enabled_roles", 75069},
        {"information_schema.routine_privileges", 75070},
        {"information_schema.table_privileges", 75071},
        {"information_schema.role_change_stream_grants", 75072},
        {"information_schema.role_column_grants", 75073},
        {"information_schema.role_routine_grants", 75074},
        {"information_schema.role_table_grants", 75075},
        {"information_schema.placements", 75113},
        {"information_schema.placement_options", 75114},

        {"spanner_sys.active_partitioned_dmls", 75077},
        {"spanner_sys.active_queries_summary", 75078},
        {"spanner_sys.lock_stats_top_10minute", 75079},
        {"spanner_sys.lock_stats_top_hour", 75080},
        {"spanner_sys.lock_stats_top_minute", 75081},
        {"spanner_sys.lock_stats_total_10minute", 75082},
        {"spanner_sys.lock_stats_total_hour", 75083},
        {"spanner_sys.lock_stats_total_minute", 75084},
        {"spanner_sys.oldest_active_queries", 75085},
        {"spanner_sys.query_profiles_top_10minute", 75086},
        {"spanner_sys.query_profiles_top_hour", 75087},
        {"spanner_sys.query_profiles_top_minute", 75088},
        {"spanner_sys.query_stats_top_10minute", 75089},
        {"spanner_sys.query_stats_top_hour", 75090},
        {"spanner_sys.query_stats_top_minute", 75091},
        {"spanner_sys.query_stats_total_10minute", 75092},
        {"spanner_sys.query_stats_total_hour", 75093},
        {"spanner_sys.query_stats_total_minute", 75094},
        {"spanner_sys.read_stats_top_10minute", 75095},
        {"spanner_sys.read_stats_top_hour", 75096},
        {"spanner_sys.read_stats_top_minute", 75097},
        {"spanner_sys.read_stats_total_10minute", 75098},
        {"spanner_sys.read_stats_total_hour", 75099},
        {"spanner_sys.read_stats_total_minute", 75100},
        {"spanner_sys.row_deletion_policies", 75101},
        {"spanner_sys.supported_optimizer_versions", 75102},
        {"spanner_sys.table_operations_stats_10minute", 75103},
        {"spanner_sys.table_operations_stats_hour", 75104},
        {"spanner_sys.table_operations_stats_minute", 75105},
        {"spanner_sys.table_sizes_stats_1hour", 75106},
        {"spanner_sys.txn_stats_top_10minute", 75107},
        {"spanner_sys.txn_stats_top_hour", 75108},
        {"spanner_sys.txn_stats_top_minute", 75109},
        {"spanner_sys.txn_stats_total_10minute", 75110},
        {"spanner_sys.txn_stats_total_hour", 75111},
        {"spanner_sys.txn_stats_total_minute", 75112},
    });

inline std::pair<std::string, std::string>
GetSchemaAndNameForPGCatalog(std::string table_name) {
  const auto& [schema_part, name_part] =
      SDLObjectName::SplitSchemaName(table_name);
  return std::make_pair((schema_part.empty()) ? kDefaultSchema :
                        std::string(schema_part), std::string(name_part));
}

template <typename T>
std::string PrimaryKeyName(const T* table) {
  std::string unqualified_table_name =
      GetSchemaAndNameForPGCatalog(table->Name()).second;
  return absl::StrCat("PK_", unqualified_table_name);
}

}  // namespace

PGCatalog::PGCatalog(const EnumerableCatalog* root_catalog,
                     const Schema* default_schema)
    : zetasql::SimpleCatalog(kName),
      root_catalog_(root_catalog),
      default_schema_(default_schema) {
  tables_by_name_ = AddTablesFromMetadata(
      PGCatalogColumnsMetadata(), *kSpannerPGTypeToGSQLType, *kSupportedTables);
  for (auto& [name, table] : tables_by_name_) {
    AddTable(table.get());
  }

  // Initialize metadata required for building tables.
  for (const auto& column : PGCatalogColumnsMetadata()) {
    pg_catalog_table_name_to_column_metadata_[column.table_name].push_back(
        column);
  }
  for (const auto& column : PGColumnsMetadata()) {
    info_schema_table_name_to_column_metadata_[column.table_name].push_back(
        column);
  }
  for (const auto& column : SpannerSysColumnsMetadata()) {
    spanner_sys_table_name_to_column_metadata_[column.table_name].push_back(
        column);
  }

  FillPGAmTable();
  FillPGAttrdefTable();
  FillPGAttributeTable();
  FillPGClassTable();
  FillPGCollationTable();
  FillPGConstraintTable();
  FillPGIndexTable();
  FillPGIndexesTable();
  FillPGNamespaceTable();
  FillPGProcTable();
  FillPGSequenceTable();
  FillPGSequencesTable();
  FillPGSettingsTable();
  FillPGTablesTable();
  FillPGTypeTable();
  FillPGViewsTable();
}

void PGCatalog::FillPGAmTable() {
  auto pg_am = tables_by_name_.at(kPGAm).get();

  // Mapping for access methods.
  std::map<int, std::string> am_mappings = {
      {75001, "t"},
      {75002, "i"},
  };

  std::vector<std::vector<zetasql::Value>> rows;
  rows.reserve(am_mappings.size());
  for (const auto& [oid, amtype] : am_mappings) {
    rows.push_back({
        // oid
        CreatePgOidValue(oid).value(),
        // amname
        String("spanner_default"),
        // amtype
        String(amtype),
    });
  }
  pg_am->SetContents(rows);
}

void PGCatalog::FillPGAttrdefTable() {
  auto pg_attrdef = tables_by_name_.at(kPGAttrdef).get();

  std::vector<std::vector<zetasql::Value>> rows;
  for (const Table* table : default_schema_->tables()) {
    if (!table->postgresql_oid().has_value()) {
      ZETASQL_VLOG(1) << "Table " << table->Name()
              << " does not have a PostgreSQL OID.";
      continue;
    }
    int ordinal_position = 0;
    for (const Column* column : table->columns()) {
      ++ordinal_position;
      if (!column->postgresql_oid().has_value()) {
        ZETASQL_VLOG(1) << "Column " << column->Name()
                << " does not have a PostgreSQL OID.";
        continue;
      }
      if (!column->is_identity_column() &&
          (column->has_default_value() || column->is_generated())) {
        if (!column->original_expression().has_value()) { continue; }
        rows.push_back({
            // oid
            CreatePgOidValue(column->postgresql_oid().value()).value(),
            // adrelid
            CreatePgOidValue(table->postgresql_oid().value()).value(),
            // adnum
            Int64(ordinal_position),
            // adbin
            String(column->original_expression().value()),
        });
      }
    }
  }
  pg_attrdef->SetContents(rows);
}

void PGCatalog::FillPGAttributeTable() {
  auto pg_attribute = tables_by_name_.at(kPGAttribute).get();

  std::vector<std::vector<zetasql::Value>> rows;
  for (const Table* table : default_schema_->tables()) {
    if (!table->postgresql_oid().has_value()) {
      ZETASQL_VLOG(1) << "Table " << table->Name()
              << " does not have a PostgreSQL OID.";
      continue;
    }
    if (!table->primary_key_index_postgresql_oid().has_value()) {
      ZETASQL_VLOG(1) << "PK for " << table->Name()
              << " does not have a PostgreSQL OID.";
      continue;
    }
    // Add columns.
    int ordinal_position = 1;
    for (const Column* column : table->columns()) {
      const PostgresTypeMapping* pg_type =
          system_catalog_->GetTypeFromReverseMapping(column->GetType());
      auto type = pg_type->PostgresTypeOid();
      type = type == TEXTOID ? VARCHAROID : type;
      type = type == TEXTARRAYOID ? VARCHARARRAYOID : type;
      rows.push_back({
          // attrelid
          CreatePgOidValue(table->postgresql_oid().value()).value(),
          //  attname
          String(column->Name()),
          // atttypid
          CreatePgOidValue(type).value(),
          // attstattarget
          NullInt64(),
          // attlen
          NullInt64(),
          // attnum
          Int64(ordinal_position),
          // attndims
          Int64(column->GetType()->IsArray() ? 1 : 0),
          // attcacheoff
          Int64(-1),
          // atttypmod
          NullInt64(),
          // attbyval
          NullBool(),
          // attalign
          NullString(),
          // attstorage
          NullString(),
          // attcompression
          String(std::string{'\0'}),
          // attnotnull
          Bool(!column->is_nullable()),
          // atthasdef
          Bool(column->has_default_value()),
          // atthasmissing
          Bool(false),
          // attidentity
          String(column->is_identity_column() ? "d" : std::string{'\0'}),
          // attgenerated
          String(column->is_generated() ? "s" : std::string{'\0'}),
          // attisdropped
          Bool(false),
          // attislocal
          Bool(true),
          // attinhcount
          Int64(0),
          // attcollation
          NullPgOid(),
          // attoptions
          NullString(),
          // attfdwoptions
          NullString(),
      });
      ++ordinal_position;
    }
    // Add primary key columns.
    ordinal_position = 1;
    for (const KeyColumn* key_column : table->primary_key()) {
      const PostgresTypeMapping* pg_type =
          system_catalog_->GetTypeFromReverseMapping(
              key_column->column()->GetType());
      auto type = pg_type->PostgresTypeOid();
      type = type == TEXTOID ? VARCHAROID : type;
      type = type == TEXTARRAYOID ? VARCHARARRAYOID : type;
      rows.push_back({
          // attrelid
          CreatePgOidValue(table->primary_key_index_postgresql_oid().value())
              .value(),
          //  attname
          String(key_column->column()->Name()),
          // atttypid
          CreatePgOidValue(type).value(),
          // attstattarget
          NullInt64(),
          // attlen
          NullInt64(),
          // attnum
          Int64(ordinal_position++),
          // attndims
          Int64(key_column->column()->GetType()->IsArray() ? 1 : 0),
          // attcacheoff
          Int64(-1),
          // atttypmod
          NullInt64(),
          // attbyval
          NullBool(),
          // attalign
          NullString(),
          // attstorage
          NullString(),
          // attcompression
          String(std::string{'\0'}),
          // attnotnull
          Bool(!key_column->column()->is_nullable()),
          // atthasdef
          Bool(key_column->column()->has_default_value()),
          // atthasmissing
          Bool(false),
          // attidentity
          String(std::string{'\0'}),
          // attgenerated
          String(key_column->column()->is_generated() ? "s"
                                                      : std::string{'\0'}),
          // attisdropped
          Bool(false),
          // attislocal
          Bool(true),
          // attinhcount
          Int64(0),
          // attcollation
          NullPgOid(),
          // attoptions
          NullString(),
          // attfdwoptions
          NullString(),
      });
    }
    for (const Index* index : table->indexes()) {
      if (!index->postgresql_oid().has_value()) {
        ZETASQL_VLOG(1) << "Index " << index->Name()
                << " does not have a PostgreSQL OID.";
        continue;
      }
      int index_ordinal_position = 1;
      for (const KeyColumn* key_column : index->key_columns()) {
        const PostgresTypeMapping* pg_type =
            system_catalog_->GetTypeFromReverseMapping(
                key_column->column()->GetType());
        auto type = pg_type->PostgresTypeOid();
        type = type == TEXTOID ? VARCHAROID : type;
        type = type == TEXTARRAYOID ? VARCHARARRAYOID : type;
        rows.push_back({
            // attrelid
            CreatePgOidValue(index->postgresql_oid().value()).value(),
            //  attname
            String(key_column->column()->Name()),
            // atttypid
            CreatePgOidValue(type).value(),
            // attstattarget
            NullInt64(),
            // attlen
            NullInt64(),
            // attnum
            Int64(index_ordinal_position++),
            // attndims
            Int64(key_column->column()->GetType()->IsArray() ? 1 : 0),
            // attcacheoff
            Int64(-1),
            // atttypmod
            NullInt64(),
            // attbyval
            NullBool(),
            // attalign
            NullString(),
            // attstorage
            NullString(),
            // attcompression
            String(std::string{'\0'}),
            // attnotnull
            Bool(!key_column->column()->is_nullable()),
            // atthasdef
            Bool(key_column->column()->has_default_value()),
            // atthasmissing
            Bool(false),
            // attidentity
            String(std::string{'\0'}),
            // attgenerated
            String(key_column->column()->is_generated() ? "s"
                                                        : std::string{'\0'}),
            // attisdropped
            Bool(false),
            // attislocal
            Bool(true),
            // attinhcount
            Int64(0),
            // attcollation
            NullPgOid(),
            // attoptions
            NullString(),
            // attfdwoptions
            NullString(),
        });
      }
      index_ordinal_position++;  // Account for inheritted primary key.
      for (const Column* column : index->stored_columns()) {
        const PostgresTypeMapping* pg_type =
            system_catalog_->GetTypeFromReverseMapping(column->GetType());
        auto type = pg_type->PostgresTypeOid();
        type = type == TEXTOID ? VARCHAROID : type;
        type = type == TEXTARRAYOID ? VARCHARARRAYOID : type;
        rows.push_back({
            // attrelid
            CreatePgOidValue(index->postgresql_oid().value()).value(),
            //  attname
            String(column->Name()),
            // atttypid
            CreatePgOidValue(type).value(),
            // attstattarget
            NullInt64(),
            // attlen
            NullInt64(),
            // attnum
            Int64(index_ordinal_position++),
            // attndims
            Int64(column->GetType()->IsArray() ? 1 : 0),
            // attcacheoff
            Int64(-1),
            // atttypmod
            NullInt64(),
            // attbyval
            NullBool(),
            // attalign
            NullString(),
            // attstorage
            NullString(),
            // attcompression
            String(std::string{'\0'}),
            // attnotnull
            Bool(!column->is_nullable()),
            // atthasdef
            Bool(column->has_default_value()),
            // atthasmissing
            Bool(false),
            // attidentity
            String(std::string{'\0'}),
            // attgenerated
            String(column->is_generated() ? "s" : std::string{'\0'}),
            // attisdropped
            Bool(false),
            // attislocal
            Bool(true),
            // attinhcount
            Int64(0),
            // attcollation
            NullPgOid(),
            // attoptions
            NullString(),
            // attfdwoptions
            NullString(),
        });
      }
    }
  }

  for (const View* view : default_schema_->views()) {
    if (!view->postgresql_oid().has_value()) {
      ZETASQL_VLOG(1) << "View " << view->Name() << " does not have a PostgreSQL OID.";
      continue;
    }
    int ordinal_position = 1;
    for (const View::Column& column : view->columns()) {
      const PostgresTypeMapping* pg_type =
          system_catalog_->GetTypeFromReverseMapping(column.type);
      auto type = pg_type->PostgresTypeOid();
      type = type == TEXTOID ? VARCHAROID : type;
      type = type == TEXTARRAYOID ? VARCHARARRAYOID : type;
      rows.push_back({
          // attrelid
          CreatePgOidValue(view->postgresql_oid().value()).value(),
          //  attname
          String(column.name),
          // atttypid
          CreatePgOidValue(type).value(),
          // attstattarget
          NullInt64(),
          // attlen
          NullInt64(),
          // attnum
          Int64(ordinal_position++),
          // attndims
          Int64(column.type->IsArray() ? 1 : 0),
          // attcacheoff
          Int64(-1),
          // atttypmod
          NullInt64(),
          // attbyval
          NullBool(),
          // attalign
          NullString(),
          // attstorage
          NullString(),
          // attcompression
          String(std::string{'\0'}),
          // attnotnull
          Bool(false),
          // atthasdef
          Bool(false),
          // atthasmissing
          Bool(false),
          // attidentity
          String(std::string{'\0'}),
          // attgenerated
          String(std::string{'\0'}),
          // attisdropped
          Bool(false),
          // attislocal
          Bool(true),
          // attinhcount
          Int64(0),
          // attcollation
          NullPgOid(),
          // attoptions
          NullString(),
          // attfdwoptions
          NullString(),
      });
    }
  }

  pg_attribute->SetContents(rows);
}

void PGCatalog::FillPGClassTable() {
  auto pg_class = tables_by_name_.at(kPGClass).get();
  std::vector<std::vector<zetasql::Value>> rows;
  // Add tables.
  for (const Table* table : default_schema_->tables()) {
    const auto& [table_schema_part, table_name_part] =
        GetSchemaAndNameForPGCatalog(table->Name());
    int namespace_oid = 0;
    if (kHardCodedNamedSchemaOid.contains(table_schema_part)) {
      namespace_oid = kHardCodedNamedSchemaOid.at(table_schema_part);
    } else {
      const NamedSchema* named_schema =
          default_schema_->FindNamedSchema(table_schema_part);
      if (!named_schema->postgresql_oid().has_value()) {
        ZETASQL_VLOG(1) << "Named schema " << table_schema_part
                << " does not have a PostgreSQL OID.";
        continue;
      }
      namespace_oid = named_schema->postgresql_oid().value();
    }
    if (!table->postgresql_oid().has_value()) {
      ZETASQL_VLOG(1) << "Table " << table->Name()
              << " does not have a PostgreSQL OID.";
      continue;
    }
    if (!table->primary_key_index_postgresql_oid().has_value()) {
      ZETASQL_VLOG(1) << "PK for " << table->Name()
              << " does not have a PostgreSQL OID.";
      continue;
    }
    rows.push_back({
        // oid
        CreatePgOidValue(table->postgresql_oid().value()).value(),
        // relname
        String(table_name_part),
        // relnamespace
        CreatePgOidValue(namespace_oid).value(),
        // reltype
        NullPgOid(),
        // reloftype
        NullPgOid(),
        // relowner
        NullPgOid(),
        // relam
        CreatePgOidValue(75001).value(),
        // relfilenode
        NullPgOid(),
        // reltablespace
        NullPgOid(),
        // relpages
        NullInt64(),
        // reltuples
        NullDouble(),
        // relallvisible
        NullInt64(),
        // reltoastrelid
        NullPgOid(),
        // relhasindex
        Bool(!table->indexes().empty()),
        // relisshared
        NullBool(),
        // relpersistence
        String("p"),
        // relkind
        String("r"),
        // relnatts
        Int64(table->columns().size()),
        // relchecks
        Int64(table->check_constraints().size()),
        // relhasrules
        NullBool(),
        // relhastriggers
        NullBool(),
        // relhassubclass
        NullBool(),
        // relrowsecurity
        NullBool(),
        // relforcerowsecurity
        NullBool(),
        // relispopulated
        Bool(true),
        // relreplident
        NullString(),
        // relispartition
        NullBool(),
        // relrewrite
        NullPgOid(),
        // relfrozenxid
        NullInt64(),
        // relminmxid
        NullInt64(),
        // reloptions
        NullString(),
        // relpartbound
        NullString(),
    });

    // Add primary key.
    rows.push_back({
        // oid
        CreatePgOidValue(table->primary_key_index_postgresql_oid().value())
            .value(),
        // relname
        String(PrimaryKeyName(table)),
        // relnamespace
        CreatePgOidValue(namespace_oid).value(),
        // reltype
        NullPgOid(),
        // reloftype
        NullPgOid(),
        // relowner
        NullPgOid(),
        // relam
        CreatePgOidValue(75002).value(),
        // relfilenode
        NullPgOid(),
        // reltablespace
        NullPgOid(),
        // relpages
        NullInt64(),
        // reltuples
        NullDouble(),
        // relallvisible
        NullInt64(),
        // reltoastrelid
        NullPgOid(),
        // relhasindex
        Bool(false),
        // relisshared
        NullBool(),
        // relpersistence
        String("p"),
        // relkind
        String("i"),
        // relnatts
        Int64(table->primary_key().size()),
        // relchecks
        Int64(0),
        // relhasrules
        NullBool(),
        // relhastriggers
        NullBool(),
        // relhassubclass
        NullBool(),
        // relrowsecurity
        NullBool(),
        // relforcerowsecurity
        NullBool(),
        // relispopulated
        Bool(true),
        // relreplident
        NullString(),
        // relispartition
        NullBool(),
        // relrewrite
        NullPgOid(),
        // relfrozenxid
        NullInt64(),
        // relminmxid
        NullInt64(),
        // reloptions
        NullString(),
        // relpartbound
        NullString(),
    });

    // Add indexes.
    for (const Index* index : table->indexes()) {
      const auto& [index_schema_part, index_name_part] =
          GetSchemaAndNameForPGCatalog(index->Name());
      if (!index->postgresql_oid().has_value()) {
        ZETASQL_VLOG(1) << "Index " << index->Name()
                << " does not have a PostgreSQL OID.";
        continue;
      }
      rows.push_back({
          // oid
          CreatePgOidValue(index->postgresql_oid().value()).value(),
          // relname
          String(index_name_part),
          // relnamespace
          CreatePgOidValue(namespace_oid).value(),
          // reltype
          NullPgOid(),
          // reloftype
          NullPgOid(),
          // relowner
          NullPgOid(),
          // relam
          CreatePgOidValue(75002).value(),
          // relfilenode
          NullPgOid(),
          // reltablespace
          NullPgOid(),
          // relpages
          NullInt64(),
          // reltuples
          NullDouble(),
          // relallvisible
          NullInt64(),
          // reltoastrelid
          NullPgOid(),
          // relhasindex
          Bool(false),
          // relisshared
          NullBool(),
          // relpersistence
          String("p"),
          // relkind
          String("i"),
          // relnatts
          Int64(index->key_columns().size() + index->stored_columns().size()),
          // relchecks
          Int64(0),
          // relhasrules
          NullBool(),
          // relhastriggers
          NullBool(),
          // relhassubclass
          NullBool(),
          // relrowsecurity
          NullBool(),
          // relforcerowsecurity
          NullBool(),
          // relispopulated
          Bool(true),
          // relreplident
          NullString(),
          // relispartition
          NullBool(),
          // relrewrite
          NullPgOid(),
          // relfrozenxid
          NullInt64(),
          // relminmxid
          NullInt64(),
          // reloptions
          NullString(),
          // relpartbound
          NullString(),
      });
    }
  }
  // Add sequences.
  for (const Sequence* sequence : default_schema_->sequences()) {
    if (sequence->is_internal_use()) {
      // Skip internal sequences.
      continue;
    }
    const auto& [sequence_schema_part, sequence_name_part] =
        GetSchemaAndNameForPGCatalog(sequence->Name());
    int namespace_oid = 0;
    if (kHardCodedNamedSchemaOid.contains(sequence_schema_part)) {
      namespace_oid = kHardCodedNamedSchemaOid.at(sequence_schema_part);
    } else {
      const NamedSchema* named_schema =
          default_schema_->FindNamedSchema(sequence_schema_part);
      if (!named_schema->postgresql_oid().has_value()) {
        ZETASQL_VLOG(1) << "Named schema " << sequence_schema_part
                << " does not have a PostgreSQL OID.";
        continue;
      }
      namespace_oid = named_schema->postgresql_oid().value();
    }
    if (!sequence->postgresql_oid().has_value()) {
      ZETASQL_VLOG(1) << "Sequence " << sequence->Name()
              << " does not have a PostgreSQL OID.";
      continue;
    }
    rows.push_back({
        // oid
        CreatePgOidValue(sequence->postgresql_oid().value()).value(),
        // relname
        String(sequence_name_part),
        // relnamespace
        CreatePgOidValue(namespace_oid).value(),
        // reltype
        NullPgOid(),
        // reloftype
        NullPgOid(),
        // relowner
        NullPgOid(),
        // relam
        CreatePgOidValue(0).value(),
        // relfilenode
        NullPgOid(),
        // reltablespace
        NullPgOid(),
        // relpages
        NullInt64(),
        // reltuples
        NullDouble(),
        // relallvisible
        NullInt64(),
        // reltoastrelid
        NullPgOid(),
        // relhasindex
        Bool(false),
        // relisshared
        NullBool(),
        // relpersistence
        String("p"),
        // relkind
        String("S"),
        // relnatts
        NullInt64(),
        // relchecks
        Int64(0),
        // relhasrules
        NullBool(),
        // relhastriggers
        NullBool(),
        // relhassubclass
        NullBool(),
        // relrowsecurity
        NullBool(),
        // relforcerowsecurity
        NullBool(),
        // relispopulated
        Bool(true),
        // relreplident
        NullString(),
        // relispartition
        NullBool(),
        // relrewrite
        NullPgOid(),
        // relfrozenxid
        NullInt64(),
        // relminmxid
        NullInt64(),
        // reloptions
        NullString(),
        // relpartbound
        NullString(),
    });
  }
  // Add views.
  for (const View* view : default_schema_->views()) {
    const auto& [view_schema_part, view_name_part] =
        GetSchemaAndNameForPGCatalog(view->Name());
    int namespace_oid = 0;
    if (kHardCodedNamedSchemaOid.contains(view_schema_part)) {
      namespace_oid = kHardCodedNamedSchemaOid.at(view_schema_part);
    } else {
      const NamedSchema* named_schema =
          default_schema_->FindNamedSchema(view_schema_part);
      if (!named_schema->postgresql_oid().has_value()) {
        ZETASQL_VLOG(1) << "Named schema " << view_schema_part
                << " does not have a PostgreSQL OID.";
        continue;
      }
      namespace_oid = named_schema->postgresql_oid().value();
    }
    if (!view->postgresql_oid().has_value()) {
      ZETASQL_VLOG(1) << "View " << view->Name() << " does not have a PostgreSQL OID.";
      continue;
    }
    rows.push_back({
        // oid
        CreatePgOidValue(view->postgresql_oid().value()).value(),
        // relname
        String(view_name_part),
        // relnamespace
        CreatePgOidValue(namespace_oid).value(),
        // reltype
        NullPgOid(),
        // reloftype
        NullPgOid(),
        // relowner
        NullPgOid(),
        // relam
        CreatePgOidValue(0).value(),
        // relfilenode
        NullPgOid(),
        // reltablespace
        NullPgOid(),
        // relpages
        NullInt64(),
        // reltuples
        NullDouble(),
        // relallvisible
        NullInt64(),
        // reltoastrelid
        NullPgOid(),
        // relhasindex
        Bool(false),
        // relisshared
        NullBool(),
        // relpersistence
        String("p"),
        // relkind
        String("v"),
        // relnatts
        Int64(view->columns().size()),
        // relchecks
        Int64(0),
        // relhasrules
        NullBool(),
        // relhastriggers
        NullBool(),
        // relhassubclass
        NullBool(),
        // relrowsecurity
        NullBool(),
        // relforcerowsecurity
        NullBool(),
        // relispopulated
        Bool(true),
        // relreplident
        NullString(),
        // relispartition
        NullBool(),
        // relrewrite
        NullPgOid(),
        // relfrozenxid
        NullInt64(),
        // relminmxid
        NullInt64(),
        // reloptions
        NullString(),
        // relpartbound
        NullString(),
    });
  }

  std::vector<PgClassSystemTableMetadata> system_tables_metadata;
  for (const auto& [table_name, metadata] :
       info_schema_table_name_to_column_metadata_) {
    auto full_table_name = absl::StrCat("information_schema.", table_name);
    if (!kHardCodedSystemViewOid.contains(full_table_name)) {
      ZETASQL_VLOG(1) << "Missing oid for " << full_table_name;
      continue;
    }
    int table_oid = kHardCodedSystemViewOid.at(full_table_name);
    int namespace_oid = kHardCodedNamedSchemaOid.at("information_schema");
    system_tables_metadata.push_back(PgClassSystemTableMetadata{
        table_name, table_oid, namespace_oid,
        /*column_count=*/static_cast<int>(metadata.size()), /*is_view=*/true});
  }
  for (const auto& [table_name, metadata] :
       pg_catalog_table_name_to_column_metadata_) {
    int table_oid = 0;
    bool is_view;
    auto full_table_name = absl::StrCat("pg_catalog.", table_name);
    if (kHardCodedSystemViewOid.contains(full_table_name)) {
      table_oid = kHardCodedSystemViewOid.at(full_table_name);
      is_view = true;
    } else if (kHardCodedPgCatalogTableOid.contains(full_table_name)) {
      table_oid = kHardCodedPgCatalogTableOid.at(full_table_name);
      is_view = false;
    } else {
      continue;
    }
    int namespace_oid = kHardCodedNamedSchemaOid.at("pg_catalog");
    system_tables_metadata.push_back(PgClassSystemTableMetadata{
        table_name, table_oid, namespace_oid,
        /*column_count=*/static_cast<int>(metadata.size()), is_view});
  }
  for (const auto& [uppercase_table_name, metadata] :
       spanner_sys_table_name_to_column_metadata_) {
    auto table_name = absl::AsciiStrToLower(uppercase_table_name);
    auto full_table_name = absl::StrCat("spanner_sys.", table_name);
    if (!kHardCodedSystemViewOid.contains(full_table_name)) {
      ZETASQL_VLOG(1) << "Missing oid for " << full_table_name;
      continue;
    }
    int table_oid = kHardCodedSystemViewOid.at(full_table_name);
    int namespace_oid = kHardCodedNamedSchemaOid.at("spanner_sys");
    int column_count = 0;
    for (const auto& column_metadata : metadata) {
      if (absl::StrContains(column_metadata.spanner_type, "STRUCT")) {
        // STRUCT columns are not supported in PG.
        continue;
      }
      ++column_count;
    }
    system_tables_metadata.push_back(PgClassSystemTableMetadata{
        table_name, table_oid, namespace_oid, column_count, /*is_view=*/true});
  }

  for (const auto& metadata : system_tables_metadata) {
    rows.push_back({
        // oid
        CreatePgOidValue(metadata.table_oid).value(),
        // relname
        String(metadata.table_name),
        // relnamespace
        CreatePgOidValue(metadata.namespace_oid).value(),
        // reltype
        NullPgOid(),
        // reloftype
        NullPgOid(),
        // relowner
        NullPgOid(),
        // relam
        CreatePgOidValue(metadata.is_view ? 0 : 75001).value(),
        // relfilenode
        NullPgOid(),
        // reltablespace
        NullPgOid(),
        // relpages
        NullInt64(),
        // reltuples
        NullDouble(),
        // relallvisible
        NullInt64(),
        // reltoastrelid
        NullPgOid(),
        // relhasindex
        Bool(false),
        // relisshared
        NullBool(),
        // relpersistence
        String("p"),
        // relkind
        String(metadata.is_view ? "v" : "r"),
        // relnatts
        Int64(metadata.column_count),
        // relchecks
        Int64(0),
        // relhasrules
        NullBool(),
        // relhastriggers
        NullBool(),
        // relhassubclass
        NullBool(),
        // relrowsecurity
        NullBool(),
        // relforcerowsecurity
        NullBool(),
        // relispopulated
        Bool(true),
        // relreplident
        NullString(),
        // relispartition
        NullBool(),
        // relrewrite
        NullPgOid(),
        // relfrozenxid
        NullInt64(),
        // relminmxid
        NullInt64(),
        // reloptions
        NullString(),
        // relpartbound
        NullString(),
    });
  }

  pg_class->SetContents(rows);
}

void PGCatalog::FillPGCollationTable() {
  auto pg_collation = tables_by_name_.at(kPGCollation).get();
  std::vector<std::vector<zetasql::Value>> rows;
  for (auto collname : kSupportedCollations) {
    auto collation_metadata = GetPgCollationDataFromBootstrap(
        PgBootstrapCatalog::Default(), collname);
    rows.push_back({
        // oid
        CreatePgOidValue(collation_metadata->oid()).value(),
        // collname
        String(collation_metadata->collname()),
        // collnamespace
        CreatePgOidValue(kHardCodedNamedSchemaOid.at("pg_catalog")).value(),
        // collowner
        NullPgOid(),
        // collcollate
        String(collation_metadata->collprovider()),
        // collisdeterministic
        Bool(collation_metadata->collisdeterministic()),
        // collencoding
        Int64(collation_metadata->collencoding()),
        // collcollate
        NullString(),
        // collctype
        NullString(),
        // colliculocale
        NullString(),
        // collversion
        NullString(),
    });
  }

  pg_collation->SetContents(rows);
}

void PGCatalog::FillPGConstraintTable() {
  auto pg_constraint = tables_by_name_.at(kPGConstraint).get();

  std::vector<std::vector<zetasql::Value>> rows;
  for (const Table* table : default_schema_->tables()) {
    if (!table->postgresql_oid().has_value()) {
      ZETASQL_VLOG(1) << "Table " << table->Name()
              << " does not have a PostgreSQL OID.";
      continue;
    }
    const auto& [table_schema, _] = GetSchemaAndNameForPGCatalog(table->Name());
    int namespace_oid = 0;
    if (kHardCodedNamedSchemaOid.contains(table_schema)) {
      namespace_oid = kHardCodedNamedSchemaOid.at(table_schema);
    } else {
      const NamedSchema* named_schema =
          default_schema_->FindNamedSchema(table_schema);
      if (!named_schema->postgresql_oid().has_value()) {
        ZETASQL_VLOG(1) << "Named schema " << table_schema
                << " does not have a PostgreSQL OID.";
        continue;
      }
      namespace_oid = named_schema->postgresql_oid().value();
    }
    int ordinal_position = 1;
    std::map<std::string, int> column_name_to_index;
    for (const Column* column : table->columns()) {
      column_name_to_index[column->Name()] = ordinal_position++;
    }
    for (const CheckConstraint* check_constraint : table->check_constraints()) {
      if (!check_constraint->postgresql_oid().has_value()) {
        ZETASQL_VLOG(1) << "Check constraint " << check_constraint->Name()
                << " does not have a PostgreSQL OID.";
        continue;
      }
      std::vector<int64_t> key_attnums;
      for (const Column* column : check_constraint->dependent_columns()) {
        key_attnums.push_back(column_name_to_index[column->Name()]);
      }
      rows.push_back({
          // oid
          CreatePgOidValue(check_constraint->postgresql_oid().value()).value(),
          // conname
          String(check_constraint->Name()),
          // connamespace
          CreatePgOidValue(namespace_oid).value(),
          // contype
          String("c"),
          // condeferrable
          NullBool(),
          // condeferred
          NullBool(),
          // convalidated
          Bool(true),
          // conrelid
          CreatePgOidValue(table->postgresql_oid().value()).value(),
          // contypid
          NullPgOid(),
          // conindid
          NullPgOid(),
          // conparentid
          NullPgOid(),
          // confrelid
          CreatePgOidValue(0).value(),
          // confupdtype
          String(" "),
          // confdeltype
          String(" "),
          // confmatchtype
          NullString(),
          // conislocal
          NullBool(),
          // coninhcount
          NullInt64(),
          // connoinherit
          NullBool(),
          // conkey
          Int64Array(key_attnums),
          // confkey
          Null(Int64ArrayType()),
          // conpfeqop
          Null(GetPgOidArrayType()),
          // conppeqop
          Null(GetPgOidArrayType()),
          // conffeqop
          Null(GetPgOidArrayType()),
          // conexclop
          Null(GetPgOidArrayType()),
          // conbin
          NullString(),
      });
    }
    for (const ForeignKey* foreign_key : table->foreign_keys()) {
      if (!foreign_key->postgresql_oid().has_value()) {
        ZETASQL_VLOG(1) << "Foreign key " << foreign_key->Name()
                << " does not have a PostgreSQL OID.";
        continue;
      }
      if (!foreign_key->referencing_table()->postgresql_oid().has_value()) {
        ZETASQL_VLOG(1) << "Referencing table " <<
            foreign_key->referencing_table()->Name() << "of foreign key "
            << foreign_key->Name() << " does not have a PostgreSQL OID.";
        continue;
      }
      std::string on_delete_action;
      switch (foreign_key->on_delete_action()) {
        case ForeignKey::Action::kNoAction:
          on_delete_action = "a";
          break;
        case ForeignKey::Action::kCascade:
          on_delete_action = "c";
          break;
        default:
          on_delete_action = " ";
      }
      std::vector<int64_t> key_attnums;
      for (const Column* column : foreign_key->referencing_columns()) {
        key_attnums.push_back(column_name_to_index[column->Name()]);
      }
      int ordinal_position = 1;
      std::map<std::string, int> foreign_column_name_to_index;
      for (const Column* column : foreign_key->referenced_table()->columns()) {
        foreign_column_name_to_index[column->Name()] = ordinal_position++;
      }
      std::vector<int64_t> confkey_attnums;
      for (const Column* column : foreign_key->referenced_columns()) {
        confkey_attnums.push_back(foreign_column_name_to_index[column->Name()]);
      }
      rows.push_back({
          // oid
          CreatePgOidValue(foreign_key->postgresql_oid().value()).value(),
          // conname
          String(foreign_key->Name()),
          // connamespace
          CreatePgOidValue(namespace_oid).value(),
          // contype
          String("f"),
          // condeferrable
          NullBool(),
          // condeferred
          NullBool(),
          // convalidated
          Bool(true),
          // conrelid
          CreatePgOidValue(
              foreign_key->referencing_table()->postgresql_oid().value())
              .value(),
          // contypid
          NullPgOid(),
          // conindid
          NullPgOid(),
          // conparentid
          NullPgOid(),
          // confrelid
          CreatePgOidValue(
              foreign_key->referenced_table()->postgresql_oid().value())
              .value(),
          // confupdtype
          String(" "),  // For update is not yet supported in the emulator.
          // confdeltype
          String(on_delete_action),
          // confmatchtype
          NullString(),
          // conislocal
          NullBool(),
          // coninhcount
          NullInt64(),
          // connoinherit
          NullBool(),
          // conkey
          Int64Array(key_attnums),
          // confkey
          Int64Array(confkey_attnums),
          // conpfeqop
          Null(GetPgOidArrayType()),
          // conppeqop
          Null(GetPgOidArrayType()),
          // conffeqop
          Null(GetPgOidArrayType()),
          // conexclop
          Null(GetPgOidArrayType()),
          // conbin
          NullString(),
      });
    }
    if (!table->primary_key()[0]->postgresql_oid().has_value()) {
      ZETASQL_VLOG(1) << "Primary key constraint for table " << table->Name()
              << " does not have a PostgreSQL OID.";
      continue;
    }
    std::vector<int64_t> key_attnums;
    for (const KeyColumn* key_column : table->primary_key()) {
      key_attnums.push_back(column_name_to_index[key_column->column()->Name()]);
    }
    // Primary key constraint.
    rows.push_back({
        // oid
        CreatePgOidValue(table->primary_key()[0]->postgresql_oid().value())
            .value(),
        // conname
        String(PrimaryKeyName(table)),
        // connamespace
        CreatePgOidValue(namespace_oid).value(),
        // contype
        String("p"),
        // condeferrable
        NullBool(),
        // condeferred
        NullBool(),
        // convalidated
        Bool(true),
        // conrelid
        CreatePgOidValue(table->postgresql_oid().value()).value(),
        // contypid
        NullPgOid(),
        // conindid
        NullPgOid(),
        // conparentid
        NullPgOid(),
        // confrelid
        CreatePgOidValue(0).value(),
        // confupdtype
        String(" "),
        // confdeltype
        String(" "),
        // confmatchtype
        NullString(),
        // conislocal
        NullBool(),
        // coninhcount
        NullInt64(),
        // connoinherit
        NullBool(),
        // conkey
        Int64Array(key_attnums),
        // confkey
        Null(Int64ArrayType()),
        // conpfeqop
        Null(GetPgOidArrayType()),
        // conppeqop
        Null(GetPgOidArrayType()),
        // conffeqop
        Null(GetPgOidArrayType()),
        // conexclop
        Null(GetPgOidArrayType()),
        // conbin
        NullString(),
    });
  }
  pg_constraint->SetContents(rows);
}

void PGCatalog::FillPGIndexTable() {
  auto pg_index = tables_by_name_.at(kPGIndex).get();

  std::vector<std::vector<zetasql::Value>> rows;
  for (const Table* table : default_schema_->tables()) {
    // Columns don't track their index in the table, so we need to build a map
    // to get the index.
    std::map<std::string, int> column_name_to_index;
    for (int i = 0; i < table->columns().size(); ++i) {
      column_name_to_index[table->columns()[i]->Name()] = i + 1;
    }
    for (const Index* index : table->indexes()) {
      std::vector<int64_t> key_columns;
      key_columns.reserve(index->key_columns().size());
      for (const auto& key_column : index->key_columns()) {
        key_columns.push_back(
            column_name_to_index[key_column->column()->Name()]);
      }
      for (const auto& stored_column : index->stored_columns()) {
        key_columns.push_back(
            column_name_to_index[stored_column->Name()]);
      }
      rows.push_back({
          // indexrelid
          CreatePgOidValue(index->postgresql_oid().value()).value(),
          // indrelid
          CreatePgOidValue(table->postgresql_oid().value()).value(),
          // indnatts
          Int64(index->key_columns().size() + index->stored_columns().size()),
          // indnkeyatts
          Int64(index->key_columns().size()),
          // indisunique
          Bool(index->is_unique()),
          // indisprimary
          Bool(false),
          // indisexclusion
          Bool(false),
          // indimmediate
          NullBool(),
          // indisclustered
          Bool(false),
          // indisvalid
          Bool(true),
          // indcheckxmin
          Bool(false),
          // indisready
          Bool(true),
          // indislive
          Bool(true),
          // indisreplident
          Bool(false),
          // indkey
          Int64Array(key_columns),
          // indcollation
          Null(GetPgOidArrayType()),
          // indclass
          Null(GetPgOidArrayType()),
          // indoption
          Null(Int64ArrayType()),
          // indexprs
          NullString(),
          // indpred
          NullString(),
      });
    }
    // Add primary key index.
    std::vector<int64_t> key_columns;
    for (const KeyColumn* key_column : table->primary_key()) {
      key_columns.push_back(column_name_to_index[key_column->column()->Name()]);
    }
    if (!table->primary_key_index_postgresql_oid().has_value()) {
      ZETASQL_VLOG(1) << "Primary key index for table " << table->Name()
              << " does not have a PostgreSQL OID.";
      continue;
    }
    rows.push_back({
        // indexrelid
        CreatePgOidValue(table->primary_key_index_postgresql_oid().value())
            .value(),
        // indrelid
        CreatePgOidValue(table->postgresql_oid().value()).value(),
        // indnatts
        Int64(table->primary_key().size()),
        // indnkeyatts
        Int64(table->primary_key().size()),
        // indisunique
        Bool(true),
        // indisprimary
        Bool(true),
        // indisexclusion
        Bool(false),
        // indimmediate
        NullBool(),
        // indisclustered
        Bool(false),
        // indisvalid
        Bool(true),
        // indcheckxmin
        Bool(false),
        // indisready
        Bool(true),
        // indislive
        Bool(true),
        // indisreplident
        Bool(false),
        // indkey
        Int64Array(key_columns),
        // indcollation
        Null(GetPgOidArrayType()),
        // indclass
        Null(GetPgOidArrayType()),
        // indoption
        Null(Int64ArrayType()),
        // indexprs
        NullString(),
        // indpred
        NullString(),
    });
  }
  pg_index->SetContents(rows);
}

void PGCatalog::FillPGIndexesTable() {
  auto pg_indexes = tables_by_name_.at(kPGIndexes).get();

  std::vector<std::vector<zetasql::Value>> rows;
  for (const Table* table : default_schema_->tables()) {
    const auto& [table_schema, table_name] =
        GetSchemaAndNameForPGCatalog(table->Name());
    // Add normal indexes.
    for (const Index* index : table->indexes()) {
      const auto& [index_schema, index_name] =
          GetSchemaAndNameForPGCatalog(index->Name());
      rows.push_back({
          // schemaname
          String(table_schema),
          // tablename
          String(table_name),
          // indexname
          String(index_name),
          // tablespace
          NullString(),
          // indexdef
          NullString(),
      });
    }

    // Add the primary key index.
    rows.push_back({
        // schemaname
        String(table_schema),
        // tablename
        String(table_name),
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

void PGCatalog::FillPGNamespaceTable() {
  auto pg_namespace = tables_by_name_.at(kPGNamespace).get();

  std::vector<std::vector<zetasql::Value>> rows;
  // Add named schemas.
  for (const NamedSchema* named_schema : default_schema_->named_schemas()) {
    if (!named_schema->postgresql_oid().has_value()) {
      ZETASQL_VLOG(1) << "Named schema " << named_schema->Name()
              << " does not have a PostgreSQL OID.";
      continue;
    }
    rows.push_back({
        // oid
        CreatePgOidValue(named_schema->postgresql_oid().value()).value(),
        // schemaname
        String(named_schema->Name()),
        // namespaceowner
        NullPgOid(),
    });
  }
  for (const auto& [name, oid] : kHardCodedNamedSchemaOid) {
    rows.push_back({
        // oid
        CreatePgOidValue(oid).value(),
        // nspname
        String(name),
        // nspowner
        NullPgOid(),
    });
  }
  pg_namespace->SetContents(rows);
}

void PGCatalog::FillPGProcTable() {
  auto pg_proc = tables_by_name_.at(kPGProc).get();

  std::vector<std::vector<zetasql::Value>> rows;
  absl::flat_hash_set<const PostgresExtendedFunction*> functions;
  auto status = system_catalog_->GetPostgreSQLFunctions(&functions);
  if (!status.ok()) {
    ZETASQL_VLOG(1) << "Failed to get table-valued functions: " << status;
    return;
  }
  for (const PostgresExtendedFunction* function : functions) {
    for (const auto& signature : function->GetPostgresSignatures()) {
      auto proc_metadata = GetPgProcDataFromBootstrap(
          PgBootstrapCatalog::Default(), signature->postgres_proc_oid());
      std::vector<zetasql::Value> proargtypes;
      for (const auto& argtype : proc_metadata->proargtypes()) {
        proargtypes.push_back(CreatePgOidValue(argtype).value());
      }
      rows.push_back({
          // oid
          CreatePgOidValue(proc_metadata->oid()).value(),
          // proname
          String(proc_metadata->proname()),
          // pronamespace
          CreatePgOidValue(proc_metadata->pronamespace()).value(),
          // proowner
          NullPgOid(),
          // prolang
          NullPgOid(),
          // procost
          NullDouble(),
          // prorows
          NullDouble(),
          // provariadic
          CreatePgOidValue(proc_metadata->provariadic()).value(),
          // prokind
          String(proc_metadata->prokind()),
          // prosecdef
          NullBool(),
          // proleakproof
          NullBool(),
          // proisstrict
          NullBool(),
          // proretset
          Bool(proc_metadata->proretset()),
          // provolatile
          NullString(),
          // proparallel
          NullString(),
          // pronargs
          Int64(proc_metadata->pronargs()),
          // pronargdefaults
          Int64(proc_metadata->pronargdefaults()),
          // prorettype
          CreatePgOidValue(proc_metadata->prorettype()).value(),
          // proargtypes
          zetasql::Value::MakeArray(GetPgOidArrayType(), proargtypes).value(),
          // proallargtypes
          Null(GetPgOidArrayType()),
          // proargmodes
          Null(StringArrayType()),
          // proargnames
          Null(StringArrayType()),
          // proargdefaults
          NullString(),
          // protrftypes
          Null(GetPgOidArrayType()),
          // prosrc
          NullString(),
          // probin
          NullString(),
          // prosqlbody
          String(proc_metadata->prosqlbody()),
          // proconfig
          Null(StringArrayType()),
      });
    }
  }

  absl::flat_hash_map<Oid, const zetasql::TableValuedFunction*> tvfs;
  status = system_catalog_->GetTableValuedFunctions(&tvfs);
  if (!status.ok()) {
    ZETASQL_VLOG(1) << "Failed to get table-valued functions: " << status;
    return;
  }
  for (const auto& [tvf_oid, tvf] : tvfs) {
    auto signature = tvf->GetSignature(0);
    std::vector<zetasql::Value> proargtypes;
    int variadic_type_oid = 0;
    if (!signature->result_type().IsRelation()) {
      ZETASQL_VLOG(1) << "Table-valued functions must return a relation type.";
      continue;
    }
    auto& rettype_options = signature->result_type().options();
    if (rettype_options.has_relation_input_schema() &&
        rettype_options.relation_input_schema().num_columns() != 1) {
      ZETASQL_VLOG(1) << "Table-valued functions must return a relation type "
                 << "with exactly one column.";
      continue;
    }
    auto rettype_mapping = system_catalog_->GetTypeFromReverseMapping(
        rettype_options.relation_input_schema().column(0).type);
    if (rettype_mapping == nullptr) {
      ZETASQL_VLOG(1) << "Failed to get type mapping for "
                 << signature->result_type().DebugString();
      continue;
    }
    int rettype_oid = rettype_mapping->PostgresTypeOid();
    for (const auto& arg : signature->arguments()) {
      auto type_mapping =
          system_catalog_->GetTypeFromReverseMapping(arg.type());
      if (type_mapping == nullptr) {
        ZETASQL_VLOG(1) << "Failed to get type mapping for "
                   << arg.type()->DebugString();
        continue;
      }
      if (arg.repeated()) {
        variadic_type_oid = type_mapping->PostgresTypeOid();
      }
      proargtypes.push_back(
          CreatePgOidValue(type_mapping->PostgresTypeOid()).value());
    }
    rows.push_back({
        // oid
        CreatePgOidValue(tvf_oid).value(),
        // proname
        String(tvf->Name()),
        // pronamespace
        CreatePgOidValue(50000).value(),
        // proowner
        NullPgOid(),
        // prolang
        NullPgOid(),
        // procost
        NullDouble(),
        // prorows
        NullDouble(),
        // provariadic
        CreatePgOidValue(variadic_type_oid).value(),
        // prokind
        String("f"),
        // prosecdef
        NullBool(),
        // proleakproof
        NullBool(),
        // proisstrict
        NullBool(),
        // proretset
        Bool(true),
        // provolatile
        NullString(),
        // proparallel
        NullString(),
        // pronargs
        Int64(signature->arguments().size()),
        // pronargdefaults
        Int64(0),
        // prorettype
        CreatePgOidValue(rettype_oid).value(),
        // proargtypes
        zetasql::Value::MakeArray(GetPgOidArrayType(), proargtypes).value(),
        // proallargtypes
        Null(GetPgOidArrayType()),
        // proargmodes
        Null(StringArrayType()),
        // proargnames
        Null(StringArrayType()),
        // proargdefaults
        NullString(),
        // protrftypes
        Null(GetPgOidArrayType()),
        // prosrc
        NullString(),
        // probin
        NullString(),
        // prosqlbody
        NullString(),
        // proconfig
        Null(StringArrayType()),
    });
  }

  absl::flat_hash_set<const zetasql::TableValuedFunction*> changestream_tvfs;
  status = root_catalog_->GetTableValuedFunctions(&changestream_tvfs);
  if (!status.ok()) {
    ZETASQL_VLOG(1) << "Failed to get table-valued functions from root catalog: "
               << status;
    return;
  }
  absl::flat_hash_map<std::string, const zetasql::TableValuedFunction*>
      changestream_tvfs_map;
  for (const zetasql::TableValuedFunction* tvf : changestream_tvfs) {
    changestream_tvfs_map[tvf->Name()] = tvf;
  }
  for (const ChangeStream* change_stream : default_schema_->change_streams()) {
    if (!changestream_tvfs_map.contains(change_stream->tvf_name())) {
      ZETASQL_VLOG(1) << "Change stream " << change_stream->Name()
                 << " does not have a table-valued function.";
      continue;
    }
    if (!change_stream->tvf_postgresql_oid().has_value()) {
        ZETASQL_VLOG(1) << "Change stream TVF " << change_stream->Name()
                << " does not have a PostgreSQL OID.";
        continue;
    }
    const auto& [change_stream_schema, change_stream_name] =
        GetSchemaAndNameForPGCatalog(change_stream->Name());
    int namespace_oid = 0;
    if (kHardCodedNamedSchemaOid.contains(change_stream_schema)) {
      namespace_oid = kHardCodedNamedSchemaOid.at(change_stream_schema);
    } else {
      const NamedSchema* named_schema =
          default_schema_->FindNamedSchema(change_stream_schema);
      if (!named_schema->postgresql_oid().has_value()) {
        ZETASQL_VLOG(1) << "Named schema " << change_stream_schema
                << " does not have a PostgreSQL OID.";
        continue;
      }
      namespace_oid = named_schema->postgresql_oid().value();
    }
    auto signature =
        changestream_tvfs_map[change_stream->tvf_name()]->GetSignature(0);
    std::vector<zetasql::Value> proargtypes;
    int variadic_type_oid = 0;
    if (!signature->result_type().IsRelation()) {
      ZETASQL_VLOG(1) << "Table-valued functions must return a relation type.";
      continue;
    }
    auto& rettype_options = signature->result_type().options();
    if (rettype_options.has_relation_input_schema() &&
        rettype_options.relation_input_schema().num_columns() != 1) {
      ZETASQL_VLOG(1) << "Table-valued functions must return a relation type "
                 << "with exactly one column.";
      continue;
    }
    auto rettype_mapping = system_catalog_->GetTypeFromReverseMapping(
        rettype_options.relation_input_schema().column(0).type);
    if (rettype_mapping == nullptr) {
      ZETASQL_VLOG(1) << "Failed to get type mapping for "
                 << signature->result_type().DebugString();
      continue;
    }
    int rettype_oid = rettype_mapping->PostgresTypeOid();
    for (const auto& arg : signature->arguments()) {
      auto type_mapping =
          system_catalog_->GetTypeFromReverseMapping(arg.type());
      if (type_mapping == nullptr) {
        ZETASQL_VLOG(1) << "Failed to get type mapping for "
                   << arg.type()->DebugString();
        continue;
      } else if (type_mapping->PostgresTypeOid() == TEXTOID) {
        type_mapping = types::PgVarcharMapping();
      } else if (type_mapping->PostgresTypeOid() == TEXTARRAYOID) {
        type_mapping = types::PgVarcharArrayMapping();
      }
      if (arg.repeated()) {
        variadic_type_oid = type_mapping->PostgresTypeOid();
      }
      proargtypes.push_back(
          CreatePgOidValue(type_mapping->PostgresTypeOid()).value());
    }
    rows.push_back({
        // oid
        CreatePgOidValue(change_stream->tvf_postgresql_oid().value()).value(),
        // proname
        String(change_stream->tvf_name()),
        // pronamespace
        CreatePgOidValue(namespace_oid).value(),
        // proowner
        NullPgOid(),
        // prolang
        NullPgOid(),
        // procost
        NullDouble(),
        // prorows
        NullDouble(),
        // provariadic
        CreatePgOidValue(variadic_type_oid).value(),
        // prokind
        String("f"),
        // prosecdef
        NullBool(),
        // proleakproof
        NullBool(),
        // proisstrict
        NullBool(),
        // proretset
        Bool(true),
        // provolatile
        NullString(),
        // proparallel
        NullString(),
        // pronargs
        Int64(signature->arguments().size()),
        // pronargdefaults
        Int64(0),
        // prorettype
        CreatePgOidValue(rettype_oid).value(),
        // proargtypes
        zetasql::Value::MakeArray(GetPgOidArrayType(), proargtypes).value(),
        // proallargtypes
        Null(GetPgOidArrayType()),
        // proargmodes
        Null(StringArrayType()),
        // proargnames
        Null(StringArrayType()),
        // proargdefaults
        NullString(),
        // protrftypes
        Null(GetPgOidArrayType()),
        // prosrc
        NullString(),
        // probin
        NullString(),
        // prosqlbody
        NullString(),
        // proconfig
        Null(StringArrayType()),
    });
  }

  pg_proc->SetContents(rows);
}

void PGCatalog::FillPGSequenceTable() {
  auto pg_sequence = tables_by_name_.at(kPGSequence).get();

  std::vector<std::vector<zetasql::Value>> rows;
  for (const Sequence* sequence : default_schema_->sequences()) {
    if (sequence->is_internal_use()) {
      // Skip internal sequences.
      continue;
    }
    if (!sequence->postgresql_oid().has_value()) {
      ZETASQL_VLOG(1) << "Sequence " << sequence->Name()
              << " does not have a PostgreSQL OID.";
      continue;
    }
    rows.push_back({
        // seqrelid
        CreatePgOidValue(sequence->postgresql_oid().value()).value(),
        // seqtypid
        CreatePgOidValue(20).value(),  // Only bigint is supported.
        // seqstart
        Int64(sequence->start_with_counter().value()),
        // seqincrement
        NullInt64(),
        // seqmax
        NullInt64(),
        // seqmin
        NullInt64(),
        // seqcache
        Int64(1000),
        // seqcycle
        Bool(false),
    });
  }
  pg_sequence->SetContents(rows);
}

void PGCatalog::FillPGSequencesTable() {
  auto pg_sequences = tables_by_name_.at(kPGSequences).get();

  std::vector<std::vector<zetasql::Value>> rows;
  for (const Sequence* sequence : default_schema_->sequences()) {
    if (sequence->is_internal_use()) {
      // Skip internal sequences.
      continue;
    }
    const auto& [sequence_schema_part, sequence_name_part] =
        GetSchemaAndNameForPGCatalog(sequence->Name());
    rows.push_back({
        // schemaname
        String(sequence_schema_part),
        // sequencename
        String(sequence_name_part),
        // sequenceowner
        NullString(),
        // start_value
        Int64(sequence->start_with_counter().value()),
        // min_value
        NullInt64(),
        // max_value
        NullInt64(),
        // increment_by
        NullInt64(),
        // cycle
        Bool(false),
        // cache_size
        Int64(1000),
        // last_value
        NullInt64(),
    });
  }
  pg_sequences->SetContents(rows);
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

void PGCatalog::FillPGTablesTable() {
  auto pg_tables = tables_by_name_.at(kPGTables).get();

  std::vector<std::vector<zetasql::Value>> rows;
  for (const Table* table : default_schema_->tables()) {
    const auto& [table_schema, table_name] =
      GetSchemaAndNameForPGCatalog(table->Name());
    rows.push_back({
        // schemaname
        String(table_schema),
        // tablename
        String(table_name),
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

void PGCatalog::FillPGTypeTable() {
  auto pg_type = tables_by_name_.at(kPGType).get();

  std::vector<std::vector<zetasql::Value>> rows;
  absl::flat_hash_set<const PostgresTypeMapping*> postgres_types;
  auto status = system_catalog_->GetPostgreSQLTypes(&postgres_types);
  for (const PostgresTypeMapping* postgres_type : postgres_types) {
    auto type_metadata = GetPgTypeDataFromBootstrap(
        PgBootstrapCatalog::Default(), postgres_type->PostgresTypeOid());
    rows.push_back({
        // oid
        CreatePgOidValue(postgres_type->PostgresTypeOid()).value(),
        // typname
        String(postgres_type->raw_type_name()),
        // typnamespace
        CreatePgOidValue(11).value(),
        // typowner
        NullPgOid(),
        // typlen
        Int64(type_metadata->typlen()),
        // typbyval
        Bool(type_metadata->typbyval()),
        // typtype
        String(type_metadata->typtype()),
        // typcategory
        String(type_metadata->typcategory()),
        // typispreferred
        Bool(type_metadata->typispreferred()),
        // typisdefined
        Bool(type_metadata->typisdefined()),
        // typdelim
        String(type_metadata->typdelim()),
        // typrelid
        CreatePgOidValue(0).value(),
        // typelem
        CreatePgOidValue(type_metadata->typelem()).value(),
        // typarray
        CreatePgOidValue(type_metadata->typarray()).value(),
        // typalign
        NullString(),
        // typstorage
        NullString(),
        // typnotnull
        NullBool(),
        // typbasetype
        NullPgOid(),
        // typtypmod
        NullInt64(),
        // typndims
        NullInt64(),
        // typcollation
        NullPgOid(),
        // typdefaultbin
        NullString(),
        // typdefault
        NullString(),
    });
  }

  for (uint32_t pseudotype_oid : {
           2276,  // any
           2277,  // anyarray
           2283,  // anyelement
           2776,  // anynonarray
           5078,  // anycompatiblearray
       }) {
    auto type_metadata = GetPgTypeDataFromBootstrap(
        PgBootstrapCatalog::Default(), pseudotype_oid);
    rows.push_back({
        // oid
        CreatePgOidValue(pseudotype_oid).value(),
        // typname
        String(type_metadata->typname()),
        // typnamespace
        CreatePgOidValue(11).value(),
        // typowner
        NullPgOid(),
        // typlen
        Int64(type_metadata->typlen()),
        // typbyval
        Bool(type_metadata->typbyval()),
        // typtype
        String(type_metadata->typtype()),
        // typcategory
        String(type_metadata->typcategory()),
        // typispreferred
        Bool(type_metadata->typispreferred()),
        // typisdefined
        Bool(type_metadata->typisdefined()),
        // typdelim
        String(type_metadata->typdelim()),
        // typrelid
        CreatePgOidValue(0).value(),
        // typelem
        CreatePgOidValue(type_metadata->typelem()).value(),
        // typarray
        CreatePgOidValue(type_metadata->typarray()).value(),
        // typalign
        NullString(),
        // typstorage
        NullString(),
        // typnotnull
        NullBool(),
        // typbasetype
        NullPgOid(),
        // typtypmod
        NullInt64(),
        // typndims
        NullInt64(),
        // typcollation
        NullPgOid(),
        // typdefaultbin
        NullString(),
        // typdefault
        NullString(),
    });
  }

  pg_type->SetContents(rows);
}

void PGCatalog::FillPGViewsTable() {
  auto pg_views = tables_by_name_.at(kPGViews).get();

  std::vector<std::vector<zetasql::Value>> rows;
  for (const View* view : default_schema_->views()) {
    const auto& [view_schema, view_name] =
      GetSchemaAndNameForPGCatalog(view->Name());
    rows.push_back({
        // schemaname
        String(view_schema),
        // viewname
        String(view_name),
        // viewowner
        NullString(),
        // definition
        String(view->body_origin().value()),
    });
  }

  for (const auto& [table_name, metadata] :
       info_schema_table_name_to_column_metadata_) {
    rows.push_back({
        // schemaname
        String("information_schema"),
        // viewname
        String(table_name),
        // viewowner
        NullString(),
        // definition
        NullString(),
    });
  }

  for (const auto& [table_name, metadata] :
       pg_catalog_table_name_to_column_metadata_) {
    rows.push_back({
        // schemaname
        String("pg_catalog"),
        // viewname
        String(table_name),
        // viewowner
        NullString(),
        // definition
        NullString(),
    });
  }

  for (const auto& [table_name, metadata] :
       spanner_sys_table_name_to_column_metadata_) {
    rows.push_back({
        // schemaname
        String("spanner_sys"),
        // viewname
        String(table_name),
        // viewowner
        NullString(),
        // definition
        NullString(),
    });
  }

  pg_views->SetContents(rows);
}

}  // namespace postgres_translator
