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

#include "backend/query/information_schema_catalog.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/value.h"
#include "zetasql/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "backend/query/info_schema_columns_metadata_values.h"
#include "backend/query/spanner_sys_catalog.h"
#include "backend/query/tables_from_metadata.h"
#include "backend/schema/catalog/change_stream.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/model.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/sequence.h"
#include "backend/schema/ddl/operations.pb.h"
#include "backend/schema/printer/print_ddl.h"
#include "backend/schema/updater/ddl_type_conversion.h"
#include "common/limits.h"
#include "third_party/spanner_pg/catalog/spangres_type.h"
#include "third_party/spanner_pg/ddl/spangres_direct_schema_printer_impl.h"
#include "third_party/spanner_pg/ddl/spangres_schema_printer.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

using ::google::spanner::admin::database::v1::DatabaseDialect;
using ::zetasql::Value;
using ::zetasql::types::BoolType;
using ::zetasql::types::Int64Type;
using ::zetasql::types::StringType;
using ::zetasql::values::Bool;
using ::zetasql::values::Int64;
using zetasql::values::NullBytes;
using ::zetasql::values::NullInt64;
using ::zetasql::values::NullString;
using ::zetasql::values::String;
using ::zetasql::values::Timestamp;

static constexpr char kInformationSchema[] = "INFORMATION_SCHEMA";
static constexpr char kTableCatalog[] = "TABLE_CATALOG";
static constexpr char kTableSchema[] = "TABLE_SCHEMA";
static constexpr char kTableName[] = "TABLE_NAME";
static constexpr char kColumnName[] = "COLUMN_NAME";
static constexpr char kOrdinalPosition[] = "ORDINAL_POSITION";
static constexpr char kColumnDefault[] = "COLUMN_DEFAULT";
static constexpr char kDataType[] = "DATA_TYPE";
static constexpr char kIsNullable[] = "IS_NULLABLE";
static constexpr char kSpannerType[] = "SPANNER_TYPE";
static constexpr char kIsGenerated[] = "IS_GENERATED";
static constexpr char kIsStored[] = "IS_STORED";
static constexpr char kGenerationExpression[] = "GENERATION_EXPRESSION";
static constexpr char kIsIdentity[] = "IS_IDENTITY";
static constexpr char kIdentityGeneration[] = "IDENTITY_GENERATION";
static constexpr char kIdentityKind[] = "IDENTITY_KIND";
static constexpr char kBitReversedPositiveSequence[] =
    "BIT_REVERSED_POSITIVE_SEQUENCE";
static constexpr char kIdentityStartWithCounter[] =
    "IDENTITY_START_WITH_COUNTER";
static constexpr char kIdentitySkipRangeMin[] = "IDENTITY_SKIP_RANGE_MIN";
static constexpr char kIdentitySkipRangeMax[] = "IDENTITY_SKIP_RANGE_MAX";
static constexpr char kSpannerState[] = "SPANNER_STATE";
static constexpr char kColumns[] = "COLUMNS";
static constexpr char kSchemaName[] = "SCHEMA_NAME";
static constexpr char kSchemata[] = "SCHEMATA";
static constexpr char kSequences[] = "SEQUENCES";
static constexpr char kSequenceOptions[] = "SEQUENCE_OPTIONS";
static constexpr char kSpannerStatistics[] = "SPANNER_STATISTICS";
static constexpr char kPGCatalog[] = "PG_CATALOG";
static constexpr char kDatabaseOptions[] = "DATABASE_OPTIONS";
static constexpr char kOptionName[] = "OPTION_NAME";
static constexpr char kOptionType[] = "OPTION_TYPE";
static constexpr char kOptionValue[] = "OPTION_VALUE";
static constexpr char kTableType[] = "TABLE_TYPE";
static constexpr char kParentTableName[] = "PARENT_TABLE_NAME";
static constexpr char kOnDeleteAction[] = "ON_DELETE_ACTION";
static constexpr char kRowDeletionPolicyExpression[] =
    "ROW_DELETION_POLICY_EXPRESSION";
static constexpr char kTables[] = "TABLES";
static constexpr char kDatabaseDialect[] = "database_dialect";
static constexpr char kString[] = "STRING";
static constexpr char kCharacterVarying[] = "character varying";
static constexpr char kPublic[] = "public";
static constexpr char kBaseTable[] = "BASE TABLE";
static constexpr char kCommitted[] = "COMMITTED";
static constexpr char kInterleaveType[] = "INTERLEAVE_TYPE";
static constexpr char kInParent[] = "IN PARENT";
static constexpr char kView[] = "VIEW";
static constexpr char kYes[] = "YES";
static constexpr char kNo[] = "NO";
static constexpr char kNone[] = "NONE";
static constexpr char kAlways[] = "ALWAYS";
static constexpr char kByDefault[] = "BY DEFAULT";
static constexpr char kNever[] = "NEVER";
static constexpr char kPrimary_Key[] = "PRIMARY_KEY";
static constexpr char kPrimaryKey[] = "PRIMARY KEY";
static constexpr char kColumnColumnUsage[] = "COLUMN_COLUMN_USAGE";
static constexpr char kDepenentColumn[] = "DEPENDENT_COLUMN";
static constexpr char kIndexes[] = "INDEXES";
static constexpr char kIndex[] = "INDEX";
static constexpr char kIndexName[] = "INDEX_NAME";
static constexpr char kIndexType[] = "INDEX_TYPE";
static constexpr char kIsUnique[] = "IS_UNIQUE";
static constexpr char kIsNullFiltered[] = "IS_NULL_FILTERED";
static constexpr char kIndexState[] = "INDEX_STATE";
static constexpr char kSpannerIsManaged[] = "SPANNER_IS_MANAGED";
static constexpr char kReadWrite[] = "READ_WRITE";
static constexpr char kColumnOrdering[] = "COLUMN_ORDERING";
static constexpr char kConstraintCatalog[] = "CONSTRAINT_CATALOG";
static constexpr char kConstraintSchema[] = "CONSTRAINT_SCHEMA";
static constexpr char kConstraintName[] = "CONSTRAINT_NAME";
static constexpr char kCheckClause[] = "CHECK_CLAUSE";
static constexpr char kDesc[] = "DESC";
static constexpr char kAsc[] = "ASC";
static constexpr char kAscNullsFirst[] = "ASC NULLS FIRST";
static constexpr char kDescNullsLast[] = "DESC NULLS LAST";
static constexpr char kAllowCommitTimestamp[] = "allow_commit_timestamp";
static constexpr char kSpannerCommitTimestamp[] = "spanner.commit_timestamp";
static constexpr char kBool[] = "BOOL";
static constexpr char kTrue[] = "TRUE";
static constexpr char kFalse[] = "FALSE";
static constexpr char kConstraintType[] = "CONSTRAINT_TYPE";
static constexpr char kIsDeferrable[] = "IS_DEFERRABLE";
static constexpr char kInitiallyDeferred[] = "INITIALLY_DEFERRED";
static constexpr char kEnforced[] = "ENFORCED";
static constexpr char kCheck[] = "CHECK";
static constexpr char kColumnOptions[] = "COLUMN_OPTIONS";
static constexpr char kUnique[] = "UNIQUE";
static constexpr char kForeignKey[] = "FOREIGN KEY";
static constexpr char kIndexColumns[] = "INDEX_COLUMNS";
static constexpr char kTableConstraints[] = "TABLE_CONSTRAINTS";
static constexpr char kCheckConstraints[] = "CHECK_CONSTRAINTS";
static constexpr char kConstraintTableUsage[] = "CONSTRAINT_TABLE_USAGE";
static constexpr char kReferentialConstraints[] = "REFERENTIAL_CONSTRAINTS";
static constexpr char kUniqueConstraintCatalog[] = "UNIQUE_CONSTRAINT_CATALOG";
static constexpr char kUniqueConstraintSchema[] = "UNIQUE_CONSTRAINT_SCHEMA";
static constexpr char kUniqueConstraintName[] = "UNIQUE_CONSTRAINT_NAME";
static constexpr char kMatchOption[] = "MATCH_OPTION";
static constexpr char kUpdateRule[] = "UPDATE_RULE";
static constexpr char kDeleteRule[] = "DELETE_RULE";
static constexpr char kSimple[] = "SIMPLE";
static constexpr char kNoAction[] = "NO ACTION";
static constexpr char kKeyColumnUsage[] = "KEY_COLUMN_USAGE";
static constexpr char kConstraintColumnUsage[] = "CONSTRAINT_COLUMN_USAGE";
static constexpr char kPositionInUniqueConstraint[] =
    "POSITION_IN_UNIQUE_CONSTRAINT";
static constexpr char kViews[] = "VIEWS";
static constexpr char kViewDefinition[] = "VIEW_DEFINITION";
static constexpr char kCharacterMaximumLength[] = "CHARACTER_MAXIMUM_LENGTH";
static constexpr char kNumericPrecision[] = "NUMERIC_PRECISION";
static constexpr char kNumericPrecisionRadix[] = "NUMERIC_PRECISION_RADIX";
static constexpr char kNumericScale[] = "NUMERIC_SCALE";
static constexpr char kChangeStreams[] = "CHANGE_STREAMS";
static constexpr char kChangeStreamTables[] = "CHANGE_STREAM_TABLES";
static constexpr char kChangeStreamColumns[] = "CHANGE_STREAM_COLUMNS";
static constexpr char kChangeStreamOptions[] = "CHANGE_STREAM_OPTIONS";
static constexpr char kChangeStreamRetentionPeriodOptionName[] =
    "retention_period";
static constexpr char kChangeStreamValueCaptureTypeOptionName[] =
    "value_capture_type";
static constexpr char kModels[] = "MODELS";
static constexpr char kModelOptions[] = "MODEL_OPTIONS";
static constexpr char kModelColumns[] = "MODEL_COLUMNS";
static constexpr char kModelColumnOptions[] = "MODEL_COLUMN_OPTIONS";

static int kFloatNumericPrecision = 24;
static int kDoubleNumericPrecision = 53;
static int kBigintNumericPrecision = 64;

// The radix for binary or decimal representation of a numeric value.
static int kBinaryRepresentedNumericPrecisionRadix = 2;
static int kDecimalRepresentedNumericPrecisionRadix = 10;

static const zetasql_base::NoDestructor<absl::flat_hash_set<std::string>>
    // For now, this is a set of tables that are created from metadata. Once the
    // migration to auto-create tables is complete, it'll be the tables from
    // https://cloud.google.com/spanner/docs/information-schema.
    kSupportedGSQLTables{{
        kChangeStreams,
        kChangeStreamColumns,
        kChangeStreamOptions,
        kChangeStreamTables,
        kCheckConstraints,
        kColumnColumnUsage,
        kColumnOptions,
        kColumns,
        kConstraintColumnUsage,
        kConstraintTableUsage,
        kDatabaseOptions,
        kIndexes,
        kIndexColumns,
        kKeyColumnUsage,
        kModels,
        kModelOptions,
        kModelColumns,
        kModelColumnOptions,
        kReferentialConstraints,
        kSchemata,
        kSequences,
        kSequenceOptions,
        kSpannerStatistics,
        kTableConstraints,
        kTables,
        kViews,
    }};

static const zetasql_base::NoDestructor<absl::flat_hash_set<std::string>>
    // For now, this is a set of tables that are created from metadata. Once the
    // migration to auto-create tables is complete, it'll be the tables from
    // https://cloud.google.com/spanner/docs/information-schema-pg.
    kSupportedPGTables{{
        absl::AsciiStrToLower(kChangeStreams),
        absl::AsciiStrToLower(kChangeStreamColumns),
        absl::AsciiStrToLower(kChangeStreamOptions),
        absl::AsciiStrToLower(kChangeStreamTables),
        absl::AsciiStrToLower(kCheckConstraints),
        absl::AsciiStrToLower(kColumnColumnUsage),
        absl::AsciiStrToLower(kColumnOptions),
        absl::AsciiStrToLower(kColumns),
        absl::AsciiStrToLower(kConstraintColumnUsage),
        absl::AsciiStrToLower(kConstraintTableUsage),
        absl::AsciiStrToLower(kDatabaseOptions),
        absl::AsciiStrToLower(kIndexes),
        absl::AsciiStrToLower(kIndexColumns),
        absl::AsciiStrToLower(kKeyColumnUsage),
        absl::AsciiStrToLower(kReferentialConstraints),
        absl::AsciiStrToLower(kSchemata),
        absl::AsciiStrToLower(kSequences),
        absl::AsciiStrToLower(kSequenceOptions),
        absl::AsciiStrToLower(kSpannerStatistics),
        absl::AsciiStrToLower(kTableConstraints),
        absl::AsciiStrToLower(kTables),
        absl::AsciiStrToLower(kViews),
    }};

static const zetasql_base::NoDestructor<
    absl::flat_hash_map<zetasql::TypeKind, zetasql::Value>>
    kGSQLTypeKindToDefaultValue{{
        {zetasql::TypeKind::TYPE_STRING, String("")},
        {zetasql::TypeKind::TYPE_INT64, Int64(0)},
        {zetasql::TypeKind::TYPE_BOOL, Bool(false)},
        {zetasql::TypeKind::TYPE_TIMESTAMP, Timestamp(absl::UnixEpoch())},
    }};

bool IsNullable(const ColumnsMetaEntry& column) {
  return std::string(column.is_nullable) == kYes;
}

// Searches for a metadata entry from metadata_entries. Returns the iterator
// position to the entry if found, or the interator end if not.
template <typename T>
typename std::vector<T>::const_iterator FindMetadata(
    const std::vector<T>& metadata_entries, const std::string& table_name,
    const std::string& column_name) {
  for (auto it = metadata_entries.cbegin(); it != metadata_entries.cend();
       ++it) {
    if (table_name == it->table_name && column_name == it->column_name)
      return it;
  }
  return metadata_entries.cend();
}

// Returns a reference to an information schema column's metadata. The column's
// metadata must exist; otherwise, the process crashes with a fatal message.
const ColumnsMetaEntry& GetColumnMetadata(const DatabaseDialect& dialect,
                                          const zetasql::Table* table,
                                          const zetasql::Column* column) {
  std::string error = "Missing metadata for column ";
  if (dialect == DatabaseDialect::POSTGRESQL) {
    std::string table_name = absl::AsciiStrToLower(table->Name());
    std::string column_name = absl::AsciiStrToLower(column->Name());
    auto m = FindMetadata(PGColumnsMetadata(), table_name, column_name);
    if (m == PGColumnsMetadata().end()) {
      ABSL_LOG(FATAL) << error << table_name << "." << column_name;
    }
    return *m;
  }

  auto m = FindMetadata(ColumnsMetadata(), table->Name(), column->Name());
  if (m == ColumnsMetadata().end()) {
    ABSL_LOG(FATAL) << error << table->Name() << "." << column->Name();
  }
  return *m;
}

// Returns a pointer to an information schema key column's metadata. Returns
// nullptr if not found.
const IndexColumnsMetaEntry* FindKeyColumnMetadata(
    const DatabaseDialect& dialect, const zetasql::Table* table,
    const zetasql::Column* column) {
  if (dialect == DatabaseDialect::POSTGRESQL) {
    auto m = FindMetadata(PGIndexColumnsMetadata(),
                          absl::AsciiStrToLower(table->Name()),
                          absl::AsciiStrToLower(column->Name()));
    return m == PGIndexColumnsMetadata().end() ? nullptr : &*m;
  } else {
    auto m =
        FindMetadata(IndexColumnsMetadata(), table->Name(), column->Name());
    return m == IndexColumnsMetadata().end() ? nullptr : &*m;
  }
}

template <typename T>
std::string PrimaryKeyName(const T* table) {
  return absl::StrCat("PK_", table->Name());
}

template <typename T, typename C>
std::string CheckNotNullName(const T* table, const C* column) {
  return absl::StrCat("CK_IS_NOT_NULL_", table->Name(), "_", column->Name());
}

std::string CheckNotNullClause(absl::string_view column_name) {
  return absl::StrCat(column_name, " IS NOT NULL");
}

// If a foreign key uses the primary key for the referenced table as the
// referenced index, referenced_index() will return nullptr. In this case,
// construct the primary key index name from the table name for information
// schema purposes.
std::string ForeignKeyReferencedIndexName(const ForeignKey* foreign_key) {
  return foreign_key->referenced_index()
             ? foreign_key->referenced_index()->Name()
             : PrimaryKeyName(foreign_key->referenced_table());
}

// Returns a table row of default values as key-values where the key is the
// column name and the value is the default value for that column type.
//
// Example: Given the following table schema:
//
// CREATE TABLE users(
//   user_id    INT64,
//   name       STRING(MAX),
//   verified   BOOL,
// ) PRIMARY KEY (user_id);
//
// this function will return the following key-value pairs:
//
// {
//   {"user_id", zetasql::values::Int64(0)},
//   {"name", zetasql::values::String("")},
//   {"verified", zetasql::values::Bool(false)},
// }
absl::flat_hash_map<std::string, zetasql::Value> GetColumnsWithDefault(
    const zetasql::Table* table) {
  absl::flat_hash_map<std::string, zetasql::Value> row;
  for (int i = 0; i < table->NumColumns(); ++i) {
    auto column = table->GetColumn(i);
    row[column->Name()] =
        kGSQLTypeKindToDefaultValue->at(column->GetType()->kind());
  }
  return row;
}

// Returns a row to be inserted into a zetasql::SimpleTable that's constructed
// using the given specific key-value pairs. If a specific value for a column is
// not provided, the default value for that type is assigned.
//
// Example: Given the following table schema:
//
// CREATE TABLE users(
//   user_id    INT64,
//   name       STRING(MAX),
//   verified   BOOL,
// ) PRIMARY KEY (user_id);
//
// and the following key-value pairs of specific values for certain columns:
//
// {
//   {"USER_ID", zetasql::values::Int64(1234)},
//   {"NAME", zetasql::values::String("Spanner User")},
// }
//
// this function will return the following row of values:
//
// {
//   zetasql::values::Int64(1234),
//   zetasql::values::String("Spanner User"),
//   zetasql::values::Bool(false),
// }
//
// where the first two values are taken from row_kvs and the last value is a
// default value.
//
// Note that the keys in row_kvs are expected to be created from the column name
// constants defined in this file and hence must be all upper-case. Otherwise
// this function will crash.
//
// Also note that whenever possible, populate the rows of a table as follows for
// improved readability:
//
// std::vector<std::vector<zetasql::Value>> rows;
// for (const Table* table : default_schema_->tables()) {
//   rows.push_back({
//       // table_catalog
//       String(""),
//       // table_schema
//       DialectDefaultSchema(),
//       // table_name
//       String(table->Name()),
//   });
// }
//
// Only use this function when a table definition for the ZetaSQL dialect
// differs from the PostgreSQL dialect where some of the columns values are
// shared between the two dialects and others are not. See FillTablesTable() for
// an example.
std::vector<zetasql::Value> GetRowFromRowKVs(
    const zetasql::Table* table,
    const absl::flat_hash_map<std::string, zetasql::Value>& row_kvs) {
  auto default_row_kvs = GetColumnsWithDefault(table);
  std::vector<zetasql::Value> row;
  row.reserve(table->NumColumns());
  for (int i = 0; i < table->NumColumns(); ++i) {
    auto column = table->GetColumn(i);
    // Since the row_kvs are constructed using the column name constants defined
    // earlier in the file, all incoming keys in the map must be uppercase so we
    // ensure that if a key is found, it's not the lowercase column name.
    ZETASQL_VLOG(row_kvs.find(  // crash ok
              absl::AsciiStrToLower(column->Name())) == row_kvs.end());
    // We convert the column names to uppercase before looking it up on the map.
    if (auto kv = row_kvs.find(absl::AsciiStrToUpper(column->Name()));
        kv != row_kvs.end()) {
      row.push_back(kv->second);
    } else {
      row.push_back(default_row_kvs.at(column->Name()));
    }
  }
  return row;
}

std::vector<zetasql::Value> GetSchemaRow(const zetasql::Table* table,
                                           zetasql::Value value) {
  absl::flat_hash_map<std::string, zetasql::Value> specific_kvs;
  specific_kvs[kSchemaName] = value;
  return GetRowFromRowKVs(table, specific_kvs);
}

}  // namespace

InformationSchemaCatalog::InformationSchemaCatalog(
    const std::string& catalog_name, const Schema* default_schema,
    const SpannerSysCatalog* spanner_sys_catalog)
    : zetasql::SimpleCatalog(catalog_name),
      default_schema_(default_schema),
      spanner_sys_catalog_(spanner_sys_catalog),
      dialect_(catalog_name == kPGName ? DatabaseDialect::POSTGRESQL
                                       : DatabaseDialect::GOOGLE_STANDARD_SQL) {
  // Create a subset of tables using columns metadata.
  if (dialect_ == DatabaseDialect::POSTGRESQL) {
    absl::StatusOr<
        std::unique_ptr<postgres_translator::spangres::SpangresSchemaPrinter>>
        printer =
            postgres_translator::spangres::CreateSpangresDirectSchemaPrinter();
    ABSL_CHECK_OK(printer.status());  // crash ok
    pg_schema_printer_ = std::move(*printer);

    tables_by_name_ = AddTablesFromMetadata(
        PGColumnsMetadata(), *kSpannerPGTypeToGSQLType, *kSupportedPGTables);
  } else {
    tables_by_name_ = AddTablesFromMetadata(
        ColumnsMetadata(), *kSpannerTypeToGSQLType, *kSupportedGSQLTables);
  }

  for (auto& [name, table] : tables_by_name_) {
    AddTable(table.get());
  }

  FillSchemataTable();
  // kSpannerStatistics currently has no rows in the emulator so we don't call a
  // function to fill the table.
  FillDatabaseOptionsTable();
  FillColumnOptionsTable();

  // These tables are populated only after all tables have been added to the
  // catalog (including meta tables) because they add rows based on the tables
  // in the catalog.
  FillTablesTable();
  FillColumnsTable();
  FillColumnColumnUsageTable();
  FillIndexesTable();
  FillIndexColumnsTable();
  FillCheckConstraintsTable();
  FillTableConstraintsTable();
  FillConstraintTableUsageTable();
  FillReferentialConstraintsTable();
  FillKeyColumnUsageTable();
  FillConstraintColumnUsageTable();
  FillViewsTable();
  FillChangeStreamsTable();
  FillChangeStreamColumnsTable();
  FillChangeStreamOptionsTable();
  FillChangeStreamTablesTable();
  FillSequencesTable();
  FillSequenceOptionsTable();
  FillModelsTable();
  FillModelOptionsTable();
  FillModelColumnsTable();
  FillModelColumnOptionsTable();
}

inline std::string InformationSchemaCatalog::GetNameForDialect(
    absl::string_view name) {
  // The system tables and associated columns are all defined in lowercase for
  // the PG dialect. The constants defined for the InformationSchema are for the
  // ZetaSQL dialect which are all uppercase. So we lowercase them here for
  // PG.
  if (dialect_ == DatabaseDialect::POSTGRESQL) {
    return absl::AsciiStrToLower(name);
  }
  return std::string(name);
}

inline zetasql::Value InformationSchemaCatalog::DialectDefaultSchema() {
  return String(dialect_ == DatabaseDialect::POSTGRESQL ? kPublic : "");
}

void InformationSchemaCatalog::FillSchemataTable() {
  auto table = tables_by_name_.at(GetNameForDialect(kSchemata)).get();
  std::vector<std::vector<zetasql::Value>> rows;

  // Row for the unnamed default schema.
  rows.push_back(GetSchemaRow(table, DialectDefaultSchema()));

  // Row for the information schema.
  rows.push_back(
      GetSchemaRow(table, String(GetNameForDialect(kInformationSchema))));

  // Row for the spanner_sys schema.
  rows.push_back(
      GetSchemaRow(table, String(GetNameForDialect(SpannerSysCatalog::kName))));

  // Row for the pg_catalog schema for PG databases.
  if (dialect_ == DatabaseDialect::POSTGRESQL) {
    rows.push_back(GetSchemaRow(table, String(GetNameForDialect(kPGCatalog))));
  }

  table->SetContents(rows);
}

void InformationSchemaCatalog::FillDatabaseOptionsTable() {
  auto table = tables_by_name_.at(GetNameForDialect(kDatabaseOptions)).get();

  absl::flat_hash_map<std::string, zetasql::Value> specific_kvs;
  specific_kvs[kSchemaName] = DialectDefaultSchema();
  specific_kvs[kOptionType] = String(
      dialect_ == DatabaseDialect::POSTGRESQL ? kCharacterVarying : kString);
  specific_kvs[kOptionName] = String(kDatabaseDialect);
  specific_kvs[kOptionValue] = String(DatabaseDialect_Name(dialect_));

  std::vector<std::vector<zetasql::Value>> rows;
  rows.push_back(GetRowFromRowKVs(table, specific_kvs));

  table->SetContents(rows);
}

// Fills the "information_schema.tables" table based on the specifications
// provided for each dialect:
// ZetaSQL: https://cloud.google.com/spanner/docs/information-schema#tables
// PostgreSQL:
// https://cloud.google.com/spanner/docs/information-schema-pg#tables
//
// Rows are added for each table and view defined in the default schema, as well
// as for tables in the information schema.
void InformationSchemaCatalog::FillTablesTable() {
  auto tables = tables_by_name_.at(GetNameForDialect(kTables)).get();

  // Add table rows.
  std::vector<std::vector<zetasql::Value>> rows;
  for (const Table* table : default_schema_->tables()) {
    absl::flat_hash_map<std::string, zetasql::Value> specific_kvs;
    if (dialect_ == DatabaseDialect::POSTGRESQL) {
      zetasql::Value row_deletion_policy_value = NullString();
      if (table->row_deletion_policy().has_value()) {
        // Use the PG schema printer to get the row deletion policy in a format
        // expected by the PG dialect.
        absl::StatusOr<std::string> row_deletion_policy =
            pg_schema_printer_->PrintRowDeletionPolicyForEmulator(
                table->row_deletion_policy().value());
        ABSL_CHECK_OK(row_deletion_policy.status());  // crash ok
        row_deletion_policy_value = String(*row_deletion_policy);
      }
      specific_kvs[kRowDeletionPolicyExpression] = row_deletion_policy_value;
    } else {
      specific_kvs[kRowDeletionPolicyExpression] =
          table->row_deletion_policy().has_value()
              ? String(RowDeletionPolicyToString(
                    table->row_deletion_policy().value()))
              : NullString();
    }

    specific_kvs[kTableSchema] = DialectDefaultSchema();
    specific_kvs[kTableName] = String(table->Name());
    specific_kvs[kTableType] = String(kBaseTable);
    specific_kvs[kParentTableName] =
        table->parent() ? String(table->parent()->Name()) : NullString();
    specific_kvs[kOnDeleteAction] =
        table->parent()
            ? String(OnDeleteActionToString(table->on_delete_action()))
            : NullString();
    specific_kvs[kSpannerState] = String(kCommitted);
    // The emulator only supports INTERLEAVE IN PARENT.
    specific_kvs[kInterleaveType] = String(kInParent);

    rows.push_back(GetRowFromRowKVs(tables, specific_kvs));
  }

  for (const View* view : default_schema_->views()) {
    absl::flat_hash_map<std::string, zetasql::Value> specific_kvs;
    specific_kvs[kTableSchema] = DialectDefaultSchema();
    specific_kvs[kTableName] = String(view->Name());
    specific_kvs[kTableType] = String(kView);
    specific_kvs[kSpannerState] = NullString();
    specific_kvs[kParentTableName] = NullString();
    specific_kvs[kOnDeleteAction] = NullString();
    specific_kvs[kRowDeletionPolicyExpression] = NullString();

    rows.push_back(GetRowFromRowKVs(tables, specific_kvs));
  }

  for (const auto& table : spanner_sys_catalog_->tables()) {
    absl::flat_hash_map<std::string, zetasql::Value> specific_kvs;
    specific_kvs[kTableSchema] =
        String(GetNameForDialect(SpannerSysCatalog::kName));
    specific_kvs[kTableType] = String(kView);
    specific_kvs[kTableName] = String(table->Name());
    specific_kvs[kParentTableName] = NullString();
    specific_kvs[kOnDeleteAction] = NullString();
    specific_kvs[kSpannerState] = NullString();
    specific_kvs[kRowDeletionPolicyExpression] = NullString();

    rows.push_back(GetRowFromRowKVs(tables, specific_kvs));
  }

  for (const auto& table : this->tables()) {
    absl::flat_hash_map<std::string, zetasql::Value> specific_kvs;
    specific_kvs[kTableSchema] = String(GetNameForDialect(kInformationSchema));
    specific_kvs[kTableName] = String(GetNameForDialect(table->Name()));
    specific_kvs[kTableType] = String(kView);
    specific_kvs[kParentTableName] = NullString();
    specific_kvs[kOnDeleteAction] = NullString();
    specific_kvs[kSpannerState] = NullString();
    specific_kvs[kRowDeletionPolicyExpression] = NullString();

    rows.push_back(GetRowFromRowKVs(tables, specific_kvs));
  }

  tables->SetContents(rows);
}

// Returns the spanner_type based on the dialect.
zetasql::Value InformationSchemaCatalog::GetSpannerType(
    const Column* column) {
  return GetSpannerType(column->GetType(), column->declared_max_length());
}

zetasql::Value InformationSchemaCatalog::GetSpannerType(
    const zetasql::Type* type, std::optional<int64_t> length) {
  if (dialect_ == DatabaseDialect::POSTGRESQL) {
    ddl::ColumnDefinition column_def = GoogleSqlTypeToDDLColumnType(type);
    if (length != std::nullopt) {
      if (type->IsArray()) {
        column_def.mutable_array_subtype()->set_length(length.value());
      } else {
        column_def.set_length(length.value());
      }
    }
    absl::StatusOr<std::string> spanner_type =
        pg_schema_printer_->PrintTypeForEmulator(column_def);
    ABSL_CHECK_OK(spanner_type.status());  // crash ok

    return String(*spanner_type);
  }

  return String(ColumnTypeToString(type, length));
}

// Returns the data_type for the PG dialect. If the type is an array, returns
// "ARRAY". Otherwise returns the spanner_type without the length.
zetasql::Value InformationSchemaCatalog::PGDataType(
    const zetasql::Type* type) {
  return type->IsArray() ? String("ARRAY") : GetSpannerType(type, std::nullopt);
}

// Returns the value to be used by the "numeric_precision" column of the
// "columns" table, based on the given column type.
zetasql::Value GetPGNumericPrecision(const zetasql::Type* type) {
  if (type->IsDouble()) {
    return Int64(kDoubleNumericPrecision);
  } else if (type->IsInt64()) {
    return Int64(kBigintNumericPrecision);
  } else if (type->IsFloat()) {
    return Int64(kFloatNumericPrecision);
  }
  return NullInt64();
}

// Returns the value to be used by the "numeric_precision_radix" column of the
// "columns" table, based on the given column type.
zetasql::Value GetPGNumericPrecisionRadix(const zetasql::Type* type) {
  // Setting the numeric precision radix.
  if (type->IsDouble() || type->IsInt64() || type->IsFloat()) {
    return Int64(kBinaryRepresentedNumericPrecisionRadix);
  } else if (type == postgres_translator::spangres::types::PgNumericMapping()
                         ->mapped_type()) {
    return Int64(kDecimalRepresentedNumericPrecisionRadix);
  }
  return NullInt64();
}

// Fills the "information_schema.columns" table based on the specifications
// provided for each dialect:
// ZetaSQL: https://cloud.google.com/spanner/docs/information-schema#columns
// PostgreSQL:
// https://cloud.google.com/spanner/docs/information-schema-pg#columns
//
// Rows are added for each column in each table and view defined in the default
// schema, as well as for tables in the information schema.
void InformationSchemaCatalog::FillColumnsTable() {
  auto columns = tables_by_name_.at(GetNameForDialect(kColumns)).get();

  // Add table rows.
  std::vector<std::vector<zetasql::Value>> rows;
  absl::flat_hash_map<std::string, zetasql::Value> specific_kvs;
  for (const Table* table : default_schema_->tables()) {
    int pos = 1;
    for (const Column* column : table->columns()) {
      if (dialect_ == DatabaseDialect::POSTGRESQL) {
        specific_kvs[kColumnDefault] =
            column->has_default_value()
                ? String(column->original_expression().value())
                : NullString();

        const zetasql::Type* type = column->GetType();
        if (column->has_allows_commit_timestamp()) {
          specific_kvs[kDataType] = String(kSpannerCommitTimestamp);
          specific_kvs[kSpannerType] = String(kSpannerCommitTimestamp);
        } else {
          specific_kvs[kDataType] = PGDataType(type);
          specific_kvs[kSpannerType] = GetSpannerType(column);
        }

        specific_kvs[kCharacterMaximumLength] =
            (!type->IsArray() && column->declared_max_length() != std::nullopt)
                ? Int64(column->declared_max_length().value())
                : NullInt64();

        specific_kvs[kNumericPrecision] = GetPGNumericPrecision(type);
        specific_kvs[kNumericPrecisionRadix] = GetPGNumericPrecisionRadix(type);
        specific_kvs[kNumericScale] = type->IsInt64() ? Int64(0) : NullInt64();

        specific_kvs[kGenerationExpression] =
            column->is_generated()
                ? String(column->original_expression().value())
                : NullString();
      } else {
        specific_kvs[kGenerationExpression] = NullString();
        if (column->is_generated()) {
          absl::string_view expression = column->expression().value();
          absl::ConsumePrefix(&expression, "(");
          absl::ConsumeSuffix(&expression, ")");
          specific_kvs[kGenerationExpression] = String(expression);
        }

        specific_kvs[kColumnDefault] =
                column->has_default_value()
                ? String(column->expression().value())
                : NullString();

        specific_kvs[kDataType] = NullString();
        specific_kvs[kSpannerType] = GetSpannerType(column);
      }

      specific_kvs[kTableSchema] = DialectDefaultSchema();
      specific_kvs[kTableName] = String(table->Name());
      specific_kvs[kColumnName] = String(column->Name());
      specific_kvs[kOrdinalPosition] = Int64(pos++);
      specific_kvs[kIsNullable] = String(column->is_nullable() ? kYes : kNo);
      specific_kvs[kIsGenerated] =
          String(column->is_generated() ? kAlways : kNever);
      if (column->is_generated()) {
        specific_kvs[kIsStored] =
            column->is_stored() ? String(kYes) : String(kNo);
      } else {
        specific_kvs[kIsStored] = NullString();
      }
      specific_kvs[kSpannerState] = String(kCommitted);
      specific_kvs[kIsIdentity] = String(kNo);
      specific_kvs[kIdentityGeneration] = NullString();
      specific_kvs[kIdentityKind] = NullString();
      specific_kvs[kIdentityStartWithCounter] = NullString();
      specific_kvs[kIdentitySkipRangeMin] = NullString();
      specific_kvs[kIdentitySkipRangeMax] = NullString();

      rows.push_back(GetRowFromRowKVs(columns, specific_kvs));
      specific_kvs.clear();
    }
  }

  // Add columns for views.
  for (const View* view : default_schema_->views()) {
    int pos = 1;
    for (const View::Column& column : view->columns()) {
      if (dialect_ == DatabaseDialect::POSTGRESQL) {
        // Emulator's View::Column doesn't store the length so we pass the
        // length in as a std::nullopt.
        specific_kvs[kDataType] = PGDataType(column.type);
        specific_kvs[kSpannerType] = GetSpannerType(column.type, std::nullopt);

        // Emulator's View::Column doesn't store the length so we assume the
        // length is the max string or byte length.
        // TODO: Update the View::Column to store the actual
        // length.
        specific_kvs[kCharacterMaximumLength] = NullInt64();

        specific_kvs[kNumericPrecision] = GetPGNumericPrecision(column.type);
        specific_kvs[kNumericPrecisionRadix] =
            GetPGNumericPrecisionRadix(column.type);
        specific_kvs[kNumericScale] =
            column.type->IsInt64() ? Int64(0) : NullInt64();
      } else {
        specific_kvs[kDataType] = NullString();
        specific_kvs[kSpannerType] = GetSpannerType(column.type, 0);
      }

      specific_kvs[kTableSchema] = DialectDefaultSchema();
      specific_kvs[kTableName] = String(view->Name());
      specific_kvs[kColumnName] = String(column.name);
      specific_kvs[kOrdinalPosition] = Int64(pos++);
      specific_kvs[kColumnDefault] = NullBytes();
      specific_kvs[kIsNullable] = String(kYes);
      specific_kvs[kIsGenerated] = String(kNever);
      specific_kvs[kGenerationExpression] = NullString();
      specific_kvs[kIsStored] = NullString();
      specific_kvs[kSpannerState] = String(kCommitted);
      specific_kvs[kIsIdentity] = String(kNo);
      specific_kvs[kIdentityGeneration] = NullString();
      specific_kvs[kIdentityKind] = NullString();
      specific_kvs[kIdentityStartWithCounter] = NullString();
      specific_kvs[kIdentitySkipRangeMin] = NullString();
      specific_kvs[kIdentitySkipRangeMax] = NullString();

      rows.push_back(GetRowFromRowKVs(columns, specific_kvs));
      specific_kvs.clear();
    }
  }

  // Add columns for the tables that live inside INFORMATION_SCHEMA.
  for (const auto& table : this->tables()) {
    int pos = 1;
    for (int i = 0; i < table->NumColumns(); ++i) {
      const auto* column = table->GetColumn(i);
      const auto& metadata = GetColumnMetadata(dialect_, table, column);

      if (dialect_ == DatabaseDialect::POSTGRESQL) {
        const zetasql::Type* type = column->GetType();
        // Information schema metadata doesn't store the length of a character
        // varying or bytea type. So we always pass in std::nullopt as the
        // length.
        specific_kvs[kDataType] = PGDataType(type);
        specific_kvs[kSpannerType] = GetSpannerType(type, std::nullopt);

        specific_kvs[kCharacterMaximumLength] = NullInt64();
        specific_kvs[kNumericPrecision] = GetPGNumericPrecision(type);
        specific_kvs[kNumericPrecisionRadix] = GetPGNumericPrecisionRadix(type);
        specific_kvs[kNumericScale] = type->IsInt64() ? Int64(0) : NullInt64();
      } else {
        specific_kvs[kDataType] = NullString();
        specific_kvs[kSpannerType] = String(metadata.spanner_type);
      }

      specific_kvs[kTableSchema] =
          String(GetNameForDialect(kInformationSchema));
      specific_kvs[kTableName] = String(GetNameForDialect(table->Name()));
      specific_kvs[kColumnName] = String(GetNameForDialect(column->Name()));
      specific_kvs[kOrdinalPosition] = Int64(pos++);
      specific_kvs[kColumnDefault] = NullBytes();
      specific_kvs[kIsNullable] = String(metadata.is_nullable);
      specific_kvs[kIsGenerated] = String(kNever);
      specific_kvs[kGenerationExpression] = NullString();
      specific_kvs[kIsStored] = NullString();
      specific_kvs[kSpannerState] = NullString();
      specific_kvs[kIsIdentity] = String(kNo);
      specific_kvs[kIdentityGeneration] = NullString();
      specific_kvs[kIdentityKind] = NullString();
      specific_kvs[kIdentityStartWithCounter] = NullString();
      specific_kvs[kIdentitySkipRangeMin] = NullString();
      specific_kvs[kIdentitySkipRangeMax] = NullString();

      rows.push_back(GetRowFromRowKVs(columns, specific_kvs));
      specific_kvs.clear();
    }
  }

  // Add table to catalog.
  columns->SetContents(rows);
}

void InformationSchemaCatalog::FillColumnColumnUsageTable() {
  auto column_column_usage =
      tables_by_name_.at(GetNameForDialect(kColumnColumnUsage)).get();

  // Add table rows.
  std::vector<std::vector<zetasql::Value>> rows;
  for (const Table* table : default_schema_->tables()) {
    for (const Column* column : table->columns()) {
      if (column->is_generated()) {
        for (const Column* used_column : column->dependent_columns()) {
          rows.push_back({
              // table_catalog
              String(""),
              // table_schema
              DialectDefaultSchema(),
              // table_name
              String(table->Name()),
              // column_name
              String(used_column->Name()),
              // dependent_column
              String(column->Name()),
          });
        }
      }
    }
  }

  // Add table to catalog.
  column_column_usage->SetContents(rows);
}

// Returns a value that represents a boolean based on the dialect. I.e. "YES" or
// "NO" for PG or a true or false for GSQL.
inline zetasql::Value InformationSchemaCatalog::DialectBoolValue(bool value) {
  return dialect_ == DatabaseDialect::POSTGRESQL ? String(value ? kYes : kNo)
                                                 : Bool(value);
}

// Returns the column ordering based on dialect.
//
// For GSQL, it returns ASC or DESC based on whether the key column is set to
// ascending or descending, respectively.
//
// For PG, returns ASC if the order is ASC NULLS LAST, or ASC NULLS FIRST if
// that's the specified order. If the order is descending, returns DESC if the
// order is DESC NULLS FIRST, or DESC NULLS LAST if that's the specified order.
zetasql::Value InformationSchemaCatalog::DialectColumnOrdering(
    const KeyColumn* column) {
  if (dialect_ == DatabaseDialect::POSTGRESQL) {
    if (column->is_descending()) {
      return column->is_nulls_last() ? String(kDescNullsLast) : String(kDesc);
    } else {
      return column->is_nulls_last() ? String(kAsc) : String(kAscNullsFirst);
    }
  }
  return String(column->is_descending() ? kDesc : kAsc);
}

// Fills the "information_schema.indexes" table based on the specifications
// provided for each dialect:
// ZetaSQL: https://cloud.google.com/spanner/docs/information-schema#indexes
// PostgreSQL:
// https://cloud.google.com/spanner/docs/information-schema-pg#indexes
//
// Rows are added for each table defined in the default schema for both
// dialects, as well as for tables in the information schema for the GSQL
// dialect.
void InformationSchemaCatalog::FillIndexesTable() {
  auto indexes = tables_by_name_.at(GetNameForDialect(kIndexes)).get();

  std::vector<std::vector<zetasql::Value>> rows;
  absl::flat_hash_map<std::string, zetasql::Value> specific_kvs;
  for (const Table* table : default_schema_->tables()) {
    // Add normal indexes.
    for (const Index* index : table->indexes()) {
      rows.push_back({
          // table_catalog
          String(""),
          // table_schema
          DialectDefaultSchema(),
          // table_name
          String(table->Name()),
          // index_name
          String(index->Name()),
          // index_type
          String(kIndex),
          // parent_table_name
          String(index->parent() ? index->parent()->Name() : ""),
          // is_unique
          DialectBoolValue(index->is_unique()),
          // is_null_filtered
          DialectBoolValue(index->is_null_filtered()),
          // index_state
          String(kReadWrite),
          // spanner_is_managed
          DialectBoolValue(index->is_managed()),
      });
      if (dialect_ == DatabaseDialect::POSTGRESQL) {
        // PG has one more undocumented column than GSQL called "filter". There
        // is no support in the emulator Index object to print this value so we
        // leave it as a null string. It also means the value is not tested
        // since otherwise the tests fail against production which does have a
        // value for it.
        rows.back().push_back(NullString());
      }
    }

    // Add the primary key index.
    rows.push_back({
        // table_catalog
        String(""),
        // table_schema
        DialectDefaultSchema(),
        // table_name
        String(table->Name()),
        // index_name
        String(kPrimary_Key),
        // index_type
        String(kPrimary_Key),
        // parent_table_name
        String(""),
        // is_unique
        DialectBoolValue(true),
        // is_null_filtered
        DialectBoolValue(false),
        // index_state
        NullString(),
        // spanner_is_managed
        DialectBoolValue(false),
    });
    if (dialect_ == DatabaseDialect::POSTGRESQL) {
      // PG has one more undocumented column than GSQL called "filter". There
      // is no support in the emulator Index object to print this value so we
      // leave it as a null string. It also means the value is not tested
      // since otherwise the tests fail against production which does have a
      // value for it.
      rows.back().push_back(NullString());
    }
  }

  // The primary key index for tables in the INFORMATION_SCHEMA are not added in
  // the PG dialect in production so we also don't add it in the emulator.
  if (dialect_ != DatabaseDialect::POSTGRESQL) {
    // Add the primary key index for tables that live in INFORMATION_SCHEMA.
    for (const auto& table : this->tables()) {
      rows.push_back({
          // table_catalog
          String(""),
          // table_schema
          String(kInformationSchema),
          // table_name
          String(table->Name()),
          // index_name
          String(kPrimary_Key),
          // index_type
          String(kPrimary_Key),
          // parent_table_name
          String(""),
          // is_unique
          Bool(true),
          // is_null_filtered
          Bool(false),
          // index_state
          NullString(),
          // spanner_is_managed
          Bool(false),
      });
    }
  }

  // Add table to catalog.
  indexes->SetContents(rows);
}

// Fills the "information_schema.index_columns" table based on the
// specifications provided for each dialect:
// ZetaSQL:
// https://cloud.google.com/spanner/docs/information-schema#index_columns
// PostgreSQL:
// https://cloud.google.com/spanner/docs/information-schema-pg#index_columns
//
// Rows are added for each table defined in the default schema for both
// dialects, as well as for tables in the information schema for the GSQL
// dialect.
void InformationSchemaCatalog::FillIndexColumnsTable() {
  auto index_columns =
      tables_by_name_.at(GetNameForDialect(kIndexColumns)).get();

  // Add table rows.
  std::vector<std::vector<zetasql::Value>> rows;
  for (const Table* table : default_schema_->tables()) {
    // Add normal indexes.
    for (const Index* index : table->indexes()) {
      int pos = 1;
      // Add key columns.
      for (const KeyColumn* key_column : index->key_columns()) {
        rows.push_back({// table_catalog
                        String(""),
                        // table_schema
                        DialectDefaultSchema(),
                        // table_name
                        String(table->Name()),
                        // index_name
                        String(index->Name()),
                        // index_type
                        String(kIndex),
                        // column_name
                        String(key_column->column()->Name()),
                        // ordinal_position
                        Int64(pos++),
                        // column_ordering
                        DialectColumnOrdering(key_column),
                        // is_nullable
                        String(key_column->column()->is_nullable() &&
                                       !index->is_null_filtered()
                                   ? kYes
                                   : kNo),
                        // spanner_type
                        GetSpannerType(key_column->column())});
      }

      // Add storing columns.
      for (const Column* column : index->stored_columns()) {
        rows.push_back({
            // table_catalog
            String(""),
            // table_schema
            DialectDefaultSchema(),
            // table_name
            String(table->Name()),
            // index_name
            String(index->Name()),
            // index_type
            String(kIndex),
            // column_name
            String(column->Name()),
            // ordinal_position
            NullInt64(),
            // column_ordering
            NullString(),
            // is_nullable
            String(column->is_nullable() ? kYes : kNo),
            // spanner_type
            GetSpannerType(column),
        });
      }
    }

    // Add the primary key columns.
    {
      int pos = 1;
      for (const KeyColumn* key_column : table->primary_key()) {
        rows.push_back({
            // table_catalog
            String(""),
            // table_schema
            DialectDefaultSchema(),
            // table_name
            String(table->Name()),
            // index_name
            String(kPrimary_Key),
            // index_type
            String(kPrimary_Key),
            // column_name
            String(key_column->column()->Name()),
            // ordinal_position
            Int64(pos++),
            // column_ordering
            DialectColumnOrdering(key_column),
            // is_nullable
            String(key_column->column()->is_nullable() ? kYes : kNo),
            // spanner_type
            GetSpannerType(key_column->column()),
        });
      }
    }
  }

  // The primary key columns for tables in the INFORMATION_SCHEMA are not added
  // in the PG dialect in production so we also don't add it in the emulator.
  if (dialect_ != DatabaseDialect::POSTGRESQL) {
    // Add the information schema primary key columns.
    for (const auto& table : this->tables()) {
      int primary_key_ordinal = 1;
      for (int i = 0; i < table->NumColumns(); ++i) {
        const auto* column = table->GetColumn(i);
        const auto* metadata = FindKeyColumnMetadata(dialect_, table, column);
        if (metadata == nullptr) {
          continue;  // Not a primary key column.
        }
        rows.push_back({
            // table_catalog
            String(""),
            // table_schema
            String(kInformationSchema),
            // table_name
            String(table->Name()),
            // index_name
            String(kPrimary_Key),
            // index_type
            String(kPrimary_Key),
            // column_name
            String(column->Name()),
            // ordinal_position
            Int64(metadata->primary_key_ordinal > 0
                      ? metadata->primary_key_ordinal
                      : primary_key_ordinal++),
            // column_ordering
            String(metadata->column_ordering),
            // is_nullable
            String(metadata->is_nullable),
            // spanner_type
            String(metadata->spanner_type),
        });
      }
    }
  }

  index_columns->SetContents(rows);
}

// Fills the "information_schema.column_options" table based on the
// specifications provided for each dialect:
// ZetaSQL:
// https://cloud.google.com/spanner/docs/information-schema#column_options
// PostgreSQL:
// https://cloud.google.com/spanner/docs/information-schema-pg#column_options
//
// Rows are added for each table defined in the default schema for just the GSQL
// dialect.
void InformationSchemaCatalog::FillColumnOptionsTable() {
  auto column_options =
      tables_by_name_.at(GetNameForDialect(kColumnOptions)).get();

  std::vector<std::vector<zetasql::Value>> rows;

  // Only add rows for ZetaSQL dialect as the PostgreSQL dialect doesn't have
  // rows in this table by default.
  if (dialect_ == DatabaseDialect::POSTGRESQL) {
    column_options->SetContents(rows);
    return;
  }

  for (const Table* table : default_schema_->tables()) {
    for (const Column* column : table->columns()) {
      if (column->allows_commit_timestamp()) {
        rows.push_back({// table_catalog
                        String(""),
                        // table_schema
                        String(""),
                        // table_name
                        String(table->Name()),
                        // column_name
                        String(column->Name()),
                        // option_name
                        String(kAllowCommitTimestamp), String(kBool),
                        // option_value
                        String(kTrue)});
      }
    }
  }

  column_options->SetContents(rows);
}

// Fills the "information_schema.table_constraints" table based on the
// specifications provided for each dialect:
// ZetaSQL:
// https://cloud.google.com/spanner/docs/information-schema#table_constraints
// PostgreSQL:
// https://cloud.google.com/spanner/docs/information-schema-pg#table_constraints
//
// Rows are added for each table defined in the default schema for both
// dialects, as well as for tables in the information schema for the GSQL
// dialect.
void InformationSchemaCatalog::FillTableConstraintsTable() {
  auto table_constraints =
      tables_by_name_.at(GetNameForDialect(kTableConstraints)).get();

  std::vector<std::vector<zetasql::Value>> rows;

  // Add the user table constraints.
  for (const auto* table : default_schema_->tables()) {
    // Add the primary key.
    rows.push_back({
        // constraint_catalog
        String(""),
        // constraint_schema
        DialectDefaultSchema(),
        // constraint_name
        String(PrimaryKeyName(table)),
        // table_catalog
        String(""),
        // table_schema
        DialectDefaultSchema(),
        // table_name
        String(table->Name()),
        // constraint_type,
        String(kPrimaryKey),
        // is_deferrable,
        String(kNo),
        // initially_deferred,
        String(kNo),
        // enforced,
        String(kYes),
    });

    // Add the NOT NULL check constraints.
    for (const auto* column : table->columns()) {
      if (column->is_nullable()) {
        continue;
      }
      rows.push_back({
          // constraint_catalog
          String(""),
          // constraint_schema
          DialectDefaultSchema(),
          // constraint_name
          String(CheckNotNullName(table, column)),
          // table_catalog
          String(""),
          // table_schema
          DialectDefaultSchema(),
          // table_name
          String(table->Name()),
          // constraint_type,
          String(kCheck),
          // is_deferrable,
          String(kNo),
          // initially_deferred,
          String(kNo),
          // enforced,
          String(kYes),
      });
    }

    // Add the check constraints defined by the ZETASQL_VLOG keyword.
    for (const auto* check_constraint : table->check_constraints()) {
      rows.push_back({
          // constraint_catalog
          String(""),
          // constraint_schema
          DialectDefaultSchema(),
          // constraint_name
          String(check_constraint->Name()),
          // table_catalog
          String(""),
          // table_schema
          DialectDefaultSchema(),
          // table_name
          String(table->Name()),
          // constraint_type,
          String(kCheck),
          // is_deferrable,
          String(kNo),
          // initially_deferred,
          String(kNo),
          // enforced,
          String(kYes),
      });
    }

    // Add the foreign keys.
    for (const auto* foreign_key : table->foreign_keys()) {
      rows.push_back({
          // constraint_catalog
          String(""),
          // constraint_schema
          DialectDefaultSchema(),
          // constraint_name
          String(foreign_key->Name()),
          // table_catalog
          String(""),
          // table_schema
          DialectDefaultSchema(),
          // table_name
          String(table->Name()),
          // constraint_type,
          String(kForeignKey),
          // is_deferrable,
          String(kNo),
          // initially_deferred,
          String(kNo),
          // enforced,
          String(kYes),
      });

      // Add the foreign key's unique backing index as a unique constraint.
      if (foreign_key->referenced_index()) {
        rows.push_back({
            // constraint_catalog
            String(""),
            // constraint_schema
            DialectDefaultSchema(),
            // constraint_name
            String(foreign_key->referenced_index()->Name()),
            // table_catalog
            String(""),
            // table_schema
            DialectDefaultSchema(),
            // table_name
            String(foreign_key->referenced_table()->Name()),
            // constraint_type,
            String(kUnique),
            // is_deferrable,
            String(kNo),
            // initially_deferred,
            String(kNo),
            // enforced,
            String(kYes),
        });
      }
    }
  }

  // Production doesn't add check constraints for the information schema so we
  // also don't add it in the emulator.
  if (dialect_ != DatabaseDialect::POSTGRESQL) {
    // Add the information schema constraints.
    for (const auto* table : this->tables()) {
      // Add the primary key.
      rows.push_back({
          // constraint_catalog
          String(""),
          // constraint_schema
          String(kInformationSchema),
          // constraint_name
          String(PrimaryKeyName(table)),
          // table_catalog
          String(""),
          // table_schema
          String(kInformationSchema),
          // table_name
          String(table->Name()),
          // constraint_type
          String(kPrimaryKey),
          // is_deferrable,
          String(kNo),
          // initially_deferred
          String(kNo),
          // enforced
          String(kYes),
      });

      // Add the NOT NULL check constraints.
      for (int i = 0; i < table->NumColumns(); ++i) {
        const auto* column = table->GetColumn(i);
        const auto& metadata = GetColumnMetadata(dialect_, table, column);
        if (IsNullable(metadata)) {
          continue;
        }
        rows.push_back({
            // constraint_catalog
            String(""),
            // constraint_schema
            String(kInformationSchema),
            // constraint_name
            String(CheckNotNullName(table, column)),
            // table_catalog
            String(""),
            // table_schema
            String(kInformationSchema),
            // table_name
            String(table->Name()),
            // constraint_type,
            String(kCheck),
            // is_deferrable,
            String(kNo),
            // initially_deferred,
            String(kNo),
            // enforced,
            String(kYes),
        });
      }
    }
  }

  table_constraints->SetContents(rows);
}

// Fills the "information_schema.check_constraints" table based on the
// specifications provided for each dialect:
// ZetaSQL:
// https://cloud.google.com/spanner/docs/information-schema#check_constraints
// PostgreSQL:
// https://cloud.google.com/spanner/docs/information-schema-pg#check_constraints
//
// Rows are added for each table defined in the default schema for both
// dialects, as well as for tables in the information schema for the GSQL
// dialect.
void InformationSchemaCatalog::FillCheckConstraintsTable() {
  auto check_constraints =
      tables_by_name_.at(GetNameForDialect(kCheckConstraints)).get();

  std::vector<std::vector<zetasql::Value>> rows;

  // Add the user table check constraints.
  for (const auto* table : default_schema_->tables()) {
    // Add the NOT NULL check constraints.
    for (const auto* column : table->columns()) {
      if (column->is_nullable()) {
        continue;
      }
      rows.push_back({
          // constraint_catalog
          String(""),
          // constraint_schema
          DialectDefaultSchema(),
          // constraint_name
          String(CheckNotNullName(table, column)),
          // check clause
          String(CheckNotNullClause(column->Name())),
          // spanner state
          String(kCommitted),
      });
    }

    // Add the check constraints defined by the ZETASQL_VLOG keyword.
    for (const auto* check_constraint : table->check_constraints()) {
      rows.push_back({
          // constraint_catalog
          String(""),
          // constraint_schema
          DialectDefaultSchema(),
          // constraint_name
          String(check_constraint->Name()),
          // check clause
          String(dialect_ == DatabaseDialect::POSTGRESQL
                     ? check_constraint->original_expression().value()
                     : check_constraint->expression()),
          // original_expression() is not yet visible externally.
          // spanner state
          String(kCommitted),
      });
    }
  }

  // Production doesn't add check constraints for the information schema so we
  // also don't add it in the emulator.
  if (dialect_ != DatabaseDialect::POSTGRESQL) {
    // Add the information schema constraints.
    for (const auto* table : this->tables()) {
      // Add the NOT NULL check constraints.
      for (int i = 0; i < table->NumColumns(); ++i) {
        const auto* column = table->GetColumn(i);
        const auto& metadata = GetColumnMetadata(dialect_, table, column);
        if (IsNullable(metadata)) {
          continue;
        }
        rows.push_back({
            // constraint_catalog
            String(""),
            // constraint_schema
            String(kInformationSchema),
            // constraint_name
            String(CheckNotNullName(table, column)),
            // check clause
            String(CheckNotNullClause(column->Name())),
            // spanner state
            String(kCommitted),
        });
      }
    }
  }
  check_constraints->SetContents(rows);
}

// Fills the "information_schema.constraint_table_usage" table based on the
// specifications provided for each dialect:
// ZetaSQL:
// https://cloud.google.com/spanner/docs/information-schema#constraint_table_usage
// PostgreSQL:
// https://cloud.google.com/spanner/docs/information-schema-pg#constraint_table_usage
//
// Rows are added for each table defined in the default schema for both
// dialects, as well as for tables in the information schema for the GSQL
// dialect.
void InformationSchemaCatalog::FillConstraintTableUsageTable() {
  auto constraint_table_usage =
      tables_by_name_.at(GetNameForDialect(kConstraintTableUsage)).get();

  std::vector<std::vector<zetasql::Value>> rows;

  // Add the user table constraints.
  for (const auto* table : default_schema_->tables()) {
    // Add the primary key.
    rows.push_back({
        // table_catalog
        String(""),
        // table_schema
        DialectDefaultSchema(),
        // table_name
        String(table->Name()),
        // constraint_catalog
        String(""),
        // constraint_schema
        DialectDefaultSchema(),
        // constraint_name
        String(PrimaryKeyName(table)),
    });

    // Production doesn't add check constraints to the information schema for
    // this table so we also don't add it in the emulator.
    if (dialect_ != DatabaseDialect::POSTGRESQL) {
      // Add the NOT NULL check constraints.
      for (const auto* column : table->columns()) {
        if (column->is_nullable()) {
          continue;
        }
        rows.push_back({
            // table_catalog
            String(""),
            // table_schema
            DialectDefaultSchema(),
            // table_name
            String(table->Name()),
            // constraint_catalog
            String(""),
            // constraint_schema
            DialectDefaultSchema(),
            // constraint_name
            String(CheckNotNullName(table, column)),
        });
      }

      // Add the check constraints defined by the ZETASQL_VLOG keyword.
      for (const auto* check_constraint : table->check_constraints()) {
        rows.push_back({
            // table_catalog
            String(""),
            // table_schema
            DialectDefaultSchema(),
            // table_name
            String(table->Name()),
            // constraint_catalog
            String(""),
            // constraint_schema
            DialectDefaultSchema(),
            // constraint_name
            String(check_constraint->Name()),
        });
      }
    }

    // Add the foreign keys.
    for (const auto* foreign_key : table->foreign_keys()) {
      rows.push_back({
          // table_catalog
          String(""),
          // table_schema
          DialectDefaultSchema(),
          // table_name
          String(foreign_key->referenced_table()->Name()),
          // constraint_catalog
          String(""),
          // constraint_schema
          DialectDefaultSchema(),
          // constraint_name
          String(foreign_key->Name()),
      });

      // Add the foreign key's unique backing index as a unique constraint.
      if (foreign_key->referenced_index()) {
        rows.push_back({
            // table_catalog
            String(""),
            // table_schema
            DialectDefaultSchema(),
            // table_name
            String(foreign_key->referenced_table()->Name()),
            // constraint_catalog
            String(""),
            // constraint_schema
            DialectDefaultSchema(),
            // constraint_name
            String(foreign_key->referenced_index()->Name()),
        });
      }
    }
  }

  // Production doesn't add check constraints for the information schema so we
  // also don't add it in the emulator.
  if (dialect_ != DatabaseDialect::POSTGRESQL) {
    // Add the information schema constraints.
    for (const auto* table : this->tables()) {
      // Add the primary key.
      rows.push_back({
          // table_catalog
          String(""),
          // table_schema
          String(kInformationSchema),
          // table_name
          String(table->Name()),
          // constraint_catalog
          String(""),
          // constraint_schema
          String(kInformationSchema),
          // constraint_name
          String(PrimaryKeyName(table)),
      });

      // Add the NOT NULL check constraints.
      for (int i = 0; i < table->NumColumns(); ++i) {
        const auto* column = table->GetColumn(i);
        const auto& metadata = GetColumnMetadata(dialect_, table, column);
        if (IsNullable(metadata)) {
          continue;
        }
        rows.push_back({
            // table_catalog
            String(""),
            // table_schema
            String(kInformationSchema),
            // table_name
            String(table->Name()),
            // constraint_catalog
            String(""),
            // constraint_schema
            String(kInformationSchema),
            // constraint_name
            String(CheckNotNullName(table, column)),
        });
      }
    }
  }

  constraint_table_usage->SetContents(rows);
}

// Fills the "information_schema.referential_constraints" table based on the
// specifications provided for each dialect:
// ZetaSQL:
// https://cloud.google.com/spanner/docs/information-schema#referential_constraints
// PostgreSQL:
// https://cloud.google.com/spanner/docs/information-schema-pg#referential_constraints
//
// Rows are added for each foreign key defined in the default schema for both
// dialects.
void InformationSchemaCatalog::FillReferentialConstraintsTable() {
  auto referential_constraints =
      tables_by_name_.at(GetNameForDialect(kReferentialConstraints)).get();

  std::vector<std::vector<zetasql::Value>> rows;

  // Add the foreign key constraints.
  for (const auto* table : default_schema_->tables()) {
    for (const auto* foreign_key : table->foreign_keys()) {
      rows.push_back({
          // constraint_catalog
          String(""),
          // constraint_schema
          DialectDefaultSchema(),
          // constraint_name
          String(foreign_key->Name()),
          // unique_constraint_catalog
          String(""),
          // unique_constraint_schema
          DialectDefaultSchema(),
          // unique_constraint_name
          String(ForeignKeyReferencedIndexName(foreign_key)),
          // match_option
          String(dialect_ == DatabaseDialect::POSTGRESQL ? kNone : kSimple),
          // update_rule
          String(kNoAction),
          // delete_rule
          String(kNoAction),
          // spanner_state
          String(kCommitted),
      });
    }
  }

  referential_constraints->SetContents(rows);
}

// Fills the "information_schema.key_column_usage" table based on the
// specifications provided for each dialect:
// ZetaSQL:
// https://cloud.google.com/spanner/docs/information-schema#key_column_usage
// PostgreSQL:
// https://cloud.google.com/spanner/docs/information-schema-pg#key_column_usage
//
// Rows are added for each key column defined in the default schema for both
// dialects, as well as for key columns in the information schema for the GSQL
// dialect.
void InformationSchemaCatalog::FillKeyColumnUsageTable() {
  auto key_column_usage =
      tables_by_name_.at(GetNameForDialect(kKeyColumnUsage)).get();

  std::vector<std::vector<zetasql::Value>> rows;
  for (const auto* table : default_schema_->tables()) {
    // Add the primary key columns.
    int table_ordinal = 1;
    for (const auto* key_column : table->primary_key()) {
      rows.push_back({
          // constraint_catalog
          String(""),
          // constraint_schema
          DialectDefaultSchema(),
          // constraint_name
          String(PrimaryKeyName(table)),
          // table_catalog
          String(""),
          // table_schema
          DialectDefaultSchema(),
          // table_name
          String(table->Name()),
          // column_name
          String(key_column->column()->Name()),
          // ordinal_position
          Int64(table_ordinal++),
          // position_in_unique_constraint
          NullString(),
      });
    }

    // Add the foreign keys.
    for (const auto* foreign_key : table->foreign_keys()) {
      // Add the foreign key referencing columns.
      int foreign_key_ordinal = 1;
      for (const auto* column : foreign_key->referencing_columns()) {
        rows.push_back({
            // constraint_catalog
            String(""),
            // constraint_schema
            DialectDefaultSchema(),
            // constraint_name
            String(foreign_key->Name()),
            // table_catalog
            String(""),
            // table_schema
            DialectDefaultSchema(),
            // table_name
            String(table->Name()),
            // column_name
            String(column->Name()),
            // ordinal_position
            Int64(foreign_key_ordinal),
            // position_in_unique_constraint
            Int64(foreign_key_ordinal),
        });
        ++foreign_key_ordinal;
      }

      // Add the foreign key's unique backing index columns.
      if (foreign_key->referenced_index()) {
        int index_ordinal = 1;
        for (const auto* key_column :
             foreign_key->referenced_index()->key_columns()) {
          rows.push_back({
              // constraint_catalog
              String(""),
              // constraint_schema
              DialectDefaultSchema(),
              // constraint_name
              String(foreign_key->referenced_index()->Name()),
              // table_catalog
              String(""),
              // table_schema
              DialectDefaultSchema(),
              // table_name
              String(foreign_key->referenced_table()->Name()),
              // column_name
              String(key_column->column()->Name()),
              // ordinal_position
              Int64(index_ordinal++),
              // position_in_unique_constraint
              NullInt64(),
          });
        }
      }
    }
  }

  // Production doesn't add check constraints for the information schema so we
  // also don't add it in the emulator.
  if (dialect_ != DatabaseDialect::POSTGRESQL) {
    // Add the information schema primary key columns.
    for (const auto& table : this->tables()) {
      int primary_key_ordinal = 1;
      for (int i = 0; i < table->NumColumns(); ++i) {
        const auto* column = table->GetColumn(i);
        const auto* metadata = FindKeyColumnMetadata(dialect_, table, column);
        if (metadata == nullptr) {
          continue;  // Not a primary key column.
        }
        rows.push_back({
            // constraint_catalog
            String(""),
            // constraint_schema
            String(kInformationSchema),
            // constraint_name
            String(PrimaryKeyName(table)),
            // table_catalog
            String(""),
            // table_schema
            String(kInformationSchema),
            // table_name
            String(table->Name()),
            // column_name
            String(metadata->column_name),
            // ordinal_position
            Int64(metadata->primary_key_ordinal > 0
                      ? metadata->primary_key_ordinal
                      : primary_key_ordinal++),
            // position_in_unique_constraint
            NullString(),
        });
      }
    }
  }

  key_column_usage->SetContents(rows);
}

// Fills the "information_schema.constraints_column_usage" table based on the
// specifications provided for each dialect:
// ZetaSQL:
// https://cloud.google.com/spanner/docs/information-schema#constraints_column_usage
// PostgreSQL:
// https://cloud.google.com/spanner/docs/information-schema-pg#constraints_column_usage
//
// Rows are added for each column defined in the default schema for both
// dialects, as well as for columns in the information schema for the GSQL
// dialect.
void InformationSchemaCatalog::FillConstraintColumnUsageTable() {
  auto constraint_column_usage =
      tables_by_name_.at(GetNameForDialect(kConstraintColumnUsage)).get();

  std::vector<std::vector<zetasql::Value>> rows;
  for (const auto* table : default_schema_->tables()) {
    // Add the primary key columns.
    for (const auto* key_column : table->primary_key()) {
      rows.push_back({
          // table_catalog
          String(""),
          // table_schema
          DialectDefaultSchema(),
          // table_name
          String(table->Name()),
          // column_name
          String(key_column->column()->Name()),
          // constraint_catalog
          String(""),
          // constraint_schema
          DialectDefaultSchema(),
          // constraint_name
          String(PrimaryKeyName(table)),
      });
    }

    // Add the NOT NULL check constraints.
    for (const auto* column : table->columns()) {
      if (column->is_nullable()) {
        continue;
      }
      rows.push_back({
          // table_catalog
          String(""),
          // table_schema
          DialectDefaultSchema(),
          // table_name
          String(table->Name()),
          // column_name
          String(column->Name()),
          // constraint_catalog
          String(""),
          // constraint_schema
          DialectDefaultSchema(),
          // constraint_name
          String(CheckNotNullName(table, column)),
      });
    }

    // Add the check constraints defined by the ZETASQL_VLOG keyword.
    for (const auto* check_constraint : table->check_constraints()) {
      for (const auto* dep_column : check_constraint->dependent_columns()) {
        rows.push_back({
            // table_catalog
            String(""),
            // table_schema
            DialectDefaultSchema(),
            // table_name
            String(table->Name()),
            // column_name
            String(dep_column->Name()),
            // constraint_catalog
            String(""),
            // constraint_schema
            DialectDefaultSchema(),
            // constraint_name
            String(check_constraint->Name()),
        });
      }
    }

    // Add the foreign keys.
    for (const auto* foreign_key : table->foreign_keys()) {
      // Add the foreign key referenced columns.
      for (const auto* column : foreign_key->referenced_columns()) {
        rows.push_back({
            // table_catalog
            String(""),
            // table_schema
            DialectDefaultSchema(),
            // table_name
            String(foreign_key->referenced_table()->Name()),
            // column_name
            String(column->Name()),
            // constraint_catalog
            String(""),
            // constraint_schema
            DialectDefaultSchema(),
            // constraint_name
            String(foreign_key->Name()),
        });
      }

      // Add the foreign key's unique backing index columns.
      if (foreign_key->referenced_index()) {
        for (const auto* key_column :
             foreign_key->referenced_index()->key_columns()) {
          rows.push_back({
              // table_catalog
              String(""),
              // table_schema
              DialectDefaultSchema(),
              // table_name
              String(foreign_key->referenced_table()->Name()),
              // column_name
              String(key_column->column()->Name()),
              // constraint_catalog
              String(""),
              // constraint_schema
              DialectDefaultSchema(),
              // constraint_name
              String(foreign_key->referenced_index()->Name()),
          });
        }
      }
    }
  }

  // Production doesn't add constraint column usage for the information schema
  // so we also don't add it in the emulator.
  if (dialect_ != DatabaseDialect::POSTGRESQL) {
    // Add the information schema primary key columns.
    for (const auto& table : this->tables()) {
      for (int i = 0; i < table->NumColumns(); ++i) {
        const auto* column = table->GetColumn(i);
        const auto* metadata = FindKeyColumnMetadata(dialect_, table, column);
        if (metadata == nullptr) {
          continue;  // Not a primary key column.
        }
        rows.push_back({
            // table_catalog
            String(""),
            // table_schema
            String(kInformationSchema),
            // table_name
            String(table->Name()),
            // column_name
            String(metadata->column_name),
            // constraint_catalog
            String(""),
            // constraint_schema
            String(kInformationSchema),
            // constraint_name
            String(PrimaryKeyName(table)),
        });
      }
    }

    // Add the information schema NOT NULL check constraints.
    for (const auto& table : this->tables()) {
      for (int i = 0; i < table->NumColumns(); ++i) {
        const auto* column = table->GetColumn(i);
        const auto& metadata = GetColumnMetadata(dialect_, table, column);
        if (IsNullable(metadata)) {
          continue;
        }
        rows.push_back({
            // table_catalog
            String(""),
            // table_schema
            String(kInformationSchema),
            // table_name
            String(table->Name()),
            // column_name
            String(metadata.column_name),
            // constraint_catalog
            String(""),
            // constraint_schema
            String(kInformationSchema),
            // constraint_name
            String(CheckNotNullName(table, column)),
        });
      }
    }
  }

  constraint_column_usage->SetContents(rows);
}

// Fills the "information_schema.views" table based on the specifications
// provided for each dialect:
// ZetaSQL: https://cloud.google.com/spanner/docs/information-schema#views
// PostgreSQL:
// https://cloud.google.com/spanner/docs/information-schema-pg#views
//
// Rows are added for each view defined in the default schema.
void InformationSchemaCatalog::FillViewsTable() {
  auto views = tables_by_name_.at(GetNameForDialect(kViews)).get();

  std::vector<std::vector<zetasql::Value>> rows;
  absl::flat_hash_map<std::string, zetasql::Value> specific_kvs;
  for (const View* view : default_schema_->views()) {
    specific_kvs[kTableSchema] = DialectDefaultSchema();
    specific_kvs[kTableName] = String(view->Name());
    if (dialect_ == DatabaseDialect::POSTGRESQL) {
      specific_kvs[kViewDefinition] = String(view->body_origin().value());
    } else {
      specific_kvs[kViewDefinition] = String(view->body());
    }
    rows.push_back(GetRowFromRowKVs(views, specific_kvs));
    specific_kvs.clear();
  }

  views->SetContents(rows);
}

// Fills the "information_schema.change_treams" table based on the
// specifications provided for each dialect: ZetaSQL:
// https://cloud.google.com/spanner/docs/information-schema#change-streams
// PostgreSQL:
// https://cloud.google.com/spanner/docs/information-schema-pg#change-streams
//
// Rows are added for each change stream defined in the default schema.
void InformationSchemaCatalog::FillChangeStreamsTable() {
  auto change_streams =
      tables_by_name_.at(GetNameForDialect(kChangeStreams)).get();
  std::vector<std::vector<zetasql::Value>> rows;
  absl::flat_hash_map<std::string, zetasql::Value> specific_kvs;
  for (const ChangeStream* change_stream : default_schema_->change_streams()) {
    rows.push_back({// change_stream_catalog
                    String(""),
                    // change_stream_schema
                    DialectDefaultSchema(),
                    // change_stream_name
                    String(change_stream->Name()),
                    // all
                    DialectBoolValue(change_stream->track_all())});
  }
  change_streams->SetContents(rows);
}

// Fills the "information_schema.change_tream_columns" table based on the
// specifications provided for each dialect: ZetaSQL:
// https://cloud.google.com/spanner/docs/information-schema#change-stream-columns
// PostgreSQL:
// https://cloud.google.com/spanner/docs/information-schema-pg#change-strea-columns
//
// Rows are added for each column tracked explicitly by each change stream
// defined in the default schema.
void InformationSchemaCatalog::FillChangeStreamColumnsTable() {
  auto change_stream_columns =
      tables_by_name_.at(GetNameForDialect(kChangeStreamColumns)).get();
  std::vector<std::vector<zetasql::Value>> rows;
  for (const ChangeStream* change_stream : default_schema_->change_streams()) {
    // Skip change streams tracking the entire database.
    if (change_stream->track_all()) {
      continue;
    }
    for (const auto& table_to_columns :
         change_stream->tracked_tables_columns()) {
      const std::string table_name = table_to_columns.first;
      const Table* tracking_table =
          default_schema_->FindTableCaseSensitive(table_name);
      const bool is_tracking_all_cols =
          tracking_table->FindChangeStream(change_stream->Name());
      // Skip change streams tracking the entire table.
      if (is_tracking_all_cols) {
        continue;
      }
      for (const auto& column : table_to_columns.second) {
        rows.push_back({
            // change_stream_catalog
            String(""),
            // change_stream_schema
            DialectDefaultSchema(),
            // change_stream_name
            String(change_stream->Name()),
            // table_catalog
            String(""),
            // table_schema
            DialectDefaultSchema(),
            // table_name
            String(table_name),
            // column_name
            String(column),
        });
      }
    }
  }
  change_stream_columns->SetContents(rows);
}

// Fills the "information_schema.change_tream_options" table based on the
// specifications provided for each dialect: ZetaSQL:
// https://cloud.google.com/spanner/docs/information-schema#change-stream-options
// PostgreSQL:
// https://cloud.google.com/spanner/docs/information-schema-pg#change-streams-options
//
// Rows are added for each explicitly specified option for each change stream
// defined in the default schema.
void InformationSchemaCatalog::FillChangeStreamOptionsTable() {
  auto change_stream_options =
      tables_by_name_.at(GetNameForDialect(kChangeStreamOptions)).get();
  std::vector<std::vector<zetasql::Value>> rows;
  std::string string_type = (dialect_ == DatabaseDialect::POSTGRESQL)
                                ? "character varying"
                                : "STRING";
  for (const ChangeStream* change_stream : default_schema_->change_streams()) {
    if (change_stream->retention_period().has_value()) {
      rows.push_back({
          // change_stream_catalog
          String(""),
          // change_stream_schema
          DialectDefaultSchema(),
          // change_stream_name
          String(change_stream->Name()),
          // option_name
          String(kChangeStreamRetentionPeriodOptionName),
          // option_type
          String(string_type),
          // option_value
          String(change_stream->retention_period().value()),
      });
    }
    if (change_stream->value_capture_type().has_value()) {
      rows.push_back({
          // change_stream_catalog
          String(""),
          // change_stream_schema
          DialectDefaultSchema(),
          // change_stream_name
          String(change_stream->Name()),
          // option_name
          String(kChangeStreamValueCaptureTypeOptionName),
          // option_type
          String(string_type),
          // option_value
          String(change_stream->value_capture_type().value()),
      });
    }
  }
  change_stream_options->SetContents(rows);
}

// Fills the "information_schema.change_tream_tables" table based on the
// specifications provided for each dialect: ZetaSQL:
// https://cloud.google.com/spanner/docs/information-schema#change-stream-tables
// PostgreSQL:
// https://cloud.google.com/spanner/docs/information-schema-pg#change-strea-tables
//
// Rows are added for each table tracked explicitly by each change stream
// defined in the default schema.
void InformationSchemaCatalog::FillChangeStreamTablesTable() {
  auto change_stream_tables =
      tables_by_name_.at(GetNameForDialect(kChangeStreamTables)).get();
  std::vector<std::vector<zetasql::Value>> rows;
  for (const ChangeStream* change_stream : default_schema_->change_streams()) {
    // Skip change streams tracking the entire database.
    if (change_stream->track_all()) {
      continue;
    }
    for (const auto& table_to_columns :
         change_stream->tracked_tables_columns()) {
      const std::string table_name = table_to_columns.first;
      // A change stream is tracking entire table if it is created for the table
      // without explicit columns.
      const bool is_tracking_all_cols =
          default_schema_->FindTableCaseSensitive(table_name)
              ->FindChangeStream(change_stream->Name());
      rows.push_back({
          // change_stream_catalog
          String(""),
          // change_stream_schema
          DialectDefaultSchema(),
          // change_stream_name
          String(change_stream->Name()),
          // table_catalog
          String(""),
          // table_schema
          DialectDefaultSchema(),
          // table_name
          String(table_name),
          // all_columns
          DialectBoolValue(is_tracking_all_cols),
      });
    }
  }
  change_stream_tables->SetContents(rows);
}

// Fills the "information_schema.sequences" table based on the
// specifications provided for each dialect: ZetaSQL:
// https://cloud.google.com/spanner/docs/information-schema#sequences
// PostgreSQL:
// https://cloud.google.com/spanner/docs/information-schema-pg#sequences
//
// Rows are added for each sequence defined in the default schema.
void InformationSchemaCatalog::FillSequencesTable() {
  auto sequences = tables_by_name_.at(GetNameForDialect(kSequences)).get();
  std::vector<std::vector<zetasql::Value>> rows;
  for (const Sequence* sequence : default_schema_->sequences()) {
    if (absl::StartsWith(sequence->Name(), "_")) {
      // Skip internal sequences.
      continue;
    }
    if (dialect_ == DatabaseDialect::POSTGRESQL) {
      rows.push_back({// sequence_catalog
                      String(""),
                      // sequence_schema
                      DialectDefaultSchema(),
                      // sequence_name
                      String(sequence->Name()),
                      // data_type
                      String("INT64"),
                      // numeric_precision
                      NullInt64(),
                      // numeric_precision_radix
                      NullInt64(),
                      // numeric_scale
                      NullInt64(),
                      // start_value
                      NullInt64(),
                      // minimum_value
                      NullInt64(),
                      // maximum_value
                      NullInt64(),
                      // increment
                      NullInt64(),
                      // cycle_option
                      String(kNo),
                      // sequence_kind
                      String(GetNameForDialect(sequence->sequence_kind_name())),
                      // counter_start_value
                      (sequence->start_with_counter().has_value()
                           ? Int64(sequence->start_with_counter().value())
                           : NullInt64()),
                      // skip_range_min
                      (sequence->skip_range_min().has_value()
                           ? Int64(sequence->skip_range_min().value())
                           : NullInt64()),
                      // skip_range_max
                      (sequence->skip_range_max().has_value()
                           ? Int64(sequence->skip_range_max().value())
                           : NullInt64())});
    } else {
      rows.push_back({// catalog
                      String(""),
                      // schema
                      DialectDefaultSchema(),
                      // name
                      String(sequence->Name()),
                      // data_type
                      String("INT64")});
    }
  }
  sequences->SetContents(rows);
}

// Fills the "information_schema.sequences" table based on the
// specifications provided for ZetaSQL:
// https://cloud.google.com/spanner/docs/information-schema#sequence_options //
// NOLINT
void InformationSchemaCatalog::FillSequenceOptionsTable() {
  if (dialect_ == DatabaseDialect::POSTGRESQL) {
    // PostgreSQL dialect does not have the SEQUENCE_OPTIONS table.
    return;
  }
  auto sequences =
      tables_by_name_.at(GetNameForDialect(kSequenceOptions)).get();
  std::vector<std::vector<zetasql::Value>> rows;
  for (const Sequence* sequence : default_schema_->sequences()) {
    rows.push_back(
        {// catalog
         String(""),
         // schema
         DialectDefaultSchema(),
         // name
         String(sequence->Name()),
         // option_name
         String("sequence_kind"),
         // option_type
         String("STRING"),
         // option_value
         String(absl::AsciiStrToLower(sequence->sequence_kind_name()))});
    if (sequence->skip_range_min().has_value()) {
      rows.push_back({// catalog
                      String(""),
                      // schema
                      DialectDefaultSchema(),
                      // name
                      String(sequence->Name()),
                      // option_name
                      String("skip_range_min"),
                      // option_type
                      String("INT64"),
                      // option_value
                      Int64(sequence->skip_range_min().value())});
    }
    if (sequence->skip_range_max().has_value()) {
      rows.push_back({// catalog
                      String(""),
                      // schema
                      DialectDefaultSchema(),
                      // name
                      String(sequence->Name()),
                      // option_name
                      String("skip_range_max"),
                      // option_type
                      String("INT64"),
                      // option_value
                      Int64(sequence->skip_range_max().value())});
    }
    if (sequence->start_with_counter().has_value()) {
      rows.push_back({// catalog
                      String(""),
                      // schema
                      DialectDefaultSchema(),
                      // name
                      String(sequence->Name()),
                      // option_name
                      String("start_with_counter"),
                      // option_type
                      String("INT64"),
                      // option_value
                      Int64(sequence->start_with_counter().value())});
    }
  }
  sequences->SetContents(rows);
}

void InformationSchemaCatalog::FillModelsTable() {
  if (dialect_ == DatabaseDialect::POSTGRESQL) {
    return;
  }

  std::vector<std::vector<zetasql::Value>> rows;
  for (const Model* model : default_schema_->models()) {
    rows.push_back({
        // model_catalog
        String(""),
        // model_schema
        DialectDefaultSchema(),
        // model_name
        String(model->Name()),
        // is_remote
        Bool(model->is_remote()),
    });
  }

  tables_by_name_.at(GetNameForDialect(kModels))->SetContents(rows);
}

void InformationSchemaCatalog::FillModelOptionsTable() {
  if (dialect_ == DatabaseDialect::POSTGRESQL) {
    return;
  }

  std::vector<std::vector<zetasql::Value>> rows;
  for (const Model* model : default_schema_->models()) {
    if (model->default_batch_size().has_value()) {
      rows.push_back({
          // model_catalog
          String(""),
          // model_schema
          DialectDefaultSchema(),
          // model_name
          String(model->Name()),
          // option_name
          String("default_batch_size"),
          // option_type
          String("INT64"),
          // option_value
          Int64(*model->default_batch_size()),
      });
    }
    if (model->endpoint().has_value()) {
      rows.push_back({
          // model_catalog
          String(""),
          // model_schema
          DialectDefaultSchema(),
          // model_name
          String(model->Name()),
          // option_name
          String("endpoint"),
          // option_type
          String("STRING"),
          // option_value
          String(*model->endpoint()),
      });
    }
    if (!model->endpoints().empty()) {
      rows.push_back({
          // model_catalog
          String(""),
          // model_schema
          DialectDefaultSchema(),
          // model_name
          String(model->Name()),
          // option_name
          String("endpoints"),
          // option_type
          String("ARRAY<STRING>"),
          // option_value
          String(absl::StrCat(
              "[",
              absl::StrJoin(model->endpoints(), ", ",
                            [](std::string* out, const std::string& endpoint) {
                              absl::StrAppend(out, "'", endpoint, "'");
                            }),
              "]")),
      });
    }
  }

  tables_by_name_.at(GetNameForDialect(kModelOptions))->SetContents(rows);
}

void InformationSchemaCatalog::FillModelColumnsTable() {
  if (dialect_ == DatabaseDialect::POSTGRESQL) {
    return;
  }

  std::vector<std::vector<zetasql::Value>> rows;
  for (const Model* model : default_schema_->models()) {
    for (int i = 0; i < model->input().size(); ++i) {
      FillModelColumnsTable(*model, model->input().at(i), "INPUT", i + 1,
                            &rows);
    }
    for (int i = 0; i < model->output().size(); ++i) {
      FillModelColumnsTable(*model, model->output().at(i), "OUTPUT", i + 1,
                            &rows);
    }
  }

  tables_by_name_.at(GetNameForDialect(kModelColumns))->SetContents(rows);
}

void InformationSchemaCatalog::FillModelColumnsTable(
    const Model& model, const Model::ModelColumn& column,
    absl::string_view column_kind, int64_t ordinal_position,
    std::vector<std::vector<zetasql::Value>>* rows) {
  rows->push_back({
      // model_catalog
      String(""),
      // model_schema
      DialectDefaultSchema(),
      // model_name
      String(model.Name()),
      // column_kind
      String(column_kind),
      // column_name
      String(column.name),
      // ordinal_position
      Int64(ordinal_position),
      // data_type
      GetSpannerType(column.type, std::nullopt),
      // is_explicit
      Bool(column.is_explicit),
  });
}

void InformationSchemaCatalog::FillModelColumnOptionsTable() {
  if (dialect_ == DatabaseDialect::POSTGRESQL) {
    return;
  }

  std::vector<std::vector<zetasql::Value>> rows;
  for (const Model* model : default_schema_->models()) {
    for (int i = 0; i < model->input().size(); ++i) {
      FillModelColumnOptionsTable(*model, model->input().at(i), "INPUT", &rows);
    }
    for (int i = 0; i < model->output().size(); ++i) {
      FillModelColumnOptionsTable(*model, model->output().at(i), "OUTPUT",
                                  &rows);
    }
  }

  tables_by_name_.at(GetNameForDialect(kModelColumnOptions))->SetContents(rows);
}

void InformationSchemaCatalog::FillModelColumnOptionsTable(
    const Model& model, const Model::ModelColumn& column,
    absl::string_view column_kind,
    std::vector<std::vector<zetasql::Value>>* rows) {
  rows->push_back({
      // model_catalog
      String(""),
      // model_schema
      DialectDefaultSchema(),
      // model_name
      String(model.Name()),
      // column_kind
      String(column_kind),
      // column_name
      String(column.name),
      // option_name
      String("required"),
      // option_type
      GetSpannerType(BoolType(), std::nullopt),
      // option_value
      String(column.is_required.value_or(true) ? kTrue : kFalse),
  });
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
