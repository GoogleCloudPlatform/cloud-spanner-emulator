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

#include <memory>
#include <string>
#include <vector>

#include "backend/query/info_schema_columns_metadata_values.h"
#include "backend/schema/printer/print_ddl.h"
#include "zetasql/base/no_destructor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

using ::google::spanner::admin::database::v1::DatabaseDialect;
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
static constexpr char kSpannerState[] = "SPANNER_STATE";
static constexpr char kColumns[] = "COLUMNS";
static constexpr char kSchemaName[] = "SCHEMA_NAME";
static constexpr char kSchemata[] = "SCHEMATA";
static constexpr char kSpannerStatistics[] = "SPANNER_STATISTICS";
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
static constexpr char kView[] = "VIEW";
static constexpr char kYes[] = "YES";
static constexpr char kNo[] = "NO";
static constexpr char kAlways[] = "ALWAYS";
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
static constexpr char kAllowCommitTimestamp[] = "allow_commit_timestamp";
static constexpr char kBool[] = "BOOL";
static constexpr char kTrue[] = "TRUE";
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

static const zetasql_base::NoDestructor<absl::flat_hash_set<std::string>>
    // For now, this is a set of tables that are created from metadata. Once the
    // migration to auto-create tables is complete, it'll be the tables from
    // https://cloud.google.com/spanner/docs/information-schema.
    kSupportedGSQLTables{{
        kDatabaseOptions,
        kSchemata,
        kSpannerStatistics,
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
  auto m = FindMetadata(ColumnsMetadata(), table->Name(), column->Name());
  if (m == ColumnsMetadata().end()) {
    ZETASQL_LOG(DFATAL) << error << table->Name() << "." << column->Name();
  }
  return *m;
}

// Returns a pointer to an information schema key column's metadata. Returns
// nullptr if not found.
const IndexColumnsMetaEntry* FindKeyColumnMetadata(
    const DatabaseDialect& dialect, const zetasql::Table* table,
    const zetasql::Column* column) {
  auto m = FindMetadata(IndexColumnsMetadata(), table->Name(), column->Name());
  return m == IndexColumnsMetadata().end() ? nullptr : &*m;
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
//   {"user_id", zetasql::values::Int64(1234)},
//   {"name", zetasql::values::String("Spanner User")},
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
std::vector<zetasql::Value> GetRowFromRowKVs(
    const zetasql::Table* table,
    const absl::flat_hash_map<std::string, zetasql::Value>& row_kvs) {
  auto default_row_kvs = GetColumnsWithDefault(table);
  std::vector<zetasql::Value> row;
  row.reserve(table->NumColumns());
  for (int i = 0; i < table->NumColumns(); ++i) {
    auto column = table->GetColumn(i);
    if (auto kv = row_kvs.find(column->Name()); kv != row_kvs.end()) {
      row.push_back(kv->second);
    } else {
      row.push_back(default_row_kvs.at(column->Name()));
    }
  }
  return row;
}

}  // namespace

absl::flat_hash_map<std::string, std::unique_ptr<zetasql::SimpleTable>>
AddTablesFromMetadata(
    const std::vector<ColumnsMetaEntry>& metadata_entries,
    const absl::flat_hash_map<std::string, const zetasql::Type*>&
        spanner_to_gsql_type,
    const absl::flat_hash_set<std::string>& supported_tables) {
  absl::flat_hash_map<std::string, std::unique_ptr<zetasql::SimpleTable>>
      tables_by_name;
  if (metadata_entries.empty()) {
    // No tables to create.
    return tables_by_name;
  }

  std::string table_name = "";
  std::vector<zetasql::SimpleTable::NameAndType> columns;
  for (auto it = metadata_entries.cbegin(); it != metadata_entries.cend();
       ++it) {
    // This is a table we're in the process of creating so add the next
    // column.
    if (!table_name.empty() && it->table_name == table_name) {
      columns.push_back(
          {it->column_name, spanner_to_gsql_type.at(it->spanner_type)});
      continue;
    }

    // It's a new table so if there was an existing table that we were in the
    // process of creating, create the table.
    if (!table_name.empty()) {
      tables_by_name[table_name] =
          std::make_unique<zetasql::SimpleTable>(table_name, columns);

      // Clear the table name and columns for the next table.
      table_name = "";
      columns.clear();
    }

    // We need to check if the new table is one that we support.
    auto supported_tables_it = supported_tables.find(it->table_name);
    if (supported_tables_it != supported_tables.end()) {
      // If we've seen this table before, then the metadata is not ordered by
      // the table name so we have to crash.
      ZETASQL_CHECK(  // crash ok
          tables_by_name.find(it->table_name) == tables_by_name.end())
          << "invalid metadata";
      table_name = it->table_name;
      columns.push_back(
          {it->column_name, spanner_to_gsql_type.at(it->spanner_type)});
    }
  }
  // If the last supported table hasn't been created yet, create it now.
  if (!table_name.empty()) {
    tables_by_name[table_name] =
        std::make_unique<zetasql::SimpleTable>(table_name, columns);
  }

  return tables_by_name;
}

InformationSchemaCatalog::InformationSchemaCatalog(
    const std::string& catalog_name, const Schema* default_schema)
    : zetasql::SimpleCatalog(catalog_name),
      default_schema_(default_schema),
      // clang-format off
      dialect_(DatabaseDialect::GOOGLE_STANDARD_SQL) {
  // clang-format on

  // Create a subset of tables using columns metadata.
    tables_by_name_ = AddTablesFromMetadata(
        ColumnsMetadata(), *kSpannerTypeToGSQLType, *kSupportedGSQLTables);

  for (auto& [name, table] : tables_by_name_) {
    AddTable(table.get());
  }

  FillSchemataTable();
  // kSpannerStatistics currently has no rows in the emulator so we don't call a
  // function to fill the table.
  FillDatabaseOptionsTable();

  auto* tables = AddTablesTable();
  auto* columns = AddColumnsTable();
  auto* column_column_usage = AddColumnColumnUsageTable();
  auto* indexes = AddIndexesTable();
  auto* index_columns = AddIndexColumnsTable();
  AddColumnOptionsTable();
  auto* check_constraints = AddCheckConstraintsTable();
  auto* table_constraints = AddTableConstraintsTable();
  auto* constraint_table_usage = AddConstraintTableUsageTable();
  auto* referential_constraints = AddReferentialConstraintsTable();
  auto* key_column_usage = AddKeyColumnUsageTable();
  auto* constraint_column_usage = AddConstraintColumnUsageTable();
  auto* views = AddViewsTable();

  // These tables are populated only after all tables have been added to the
  // catalog (including meta tables) because they add rows based on the tables
  // in the catalog.
  FillTablesTable(tables);
  FillColumnsTable(columns);
  FillColumnColumnUsageTable(column_column_usage);
  FillIndexesTable(indexes);
  FillIndexColumnsTable(index_columns);
  FillCheckConstraintsTable(check_constraints);
  FillTableConstraintsTable(table_constraints);
  FillConstraintTableUsageTable(constraint_table_usage);
  FillReferentialConstraintsTable(referential_constraints);
  FillKeyColumnUsageTable(key_column_usage);
  FillConstraintColumnUsageTable(constraint_column_usage);
  FillViewsTable(views);
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

void InformationSchemaCatalog::FillSchemataTable() {
  auto table = tables_by_name_.at(GetNameForDialect(kSchemata)).get();
  std::vector<std::vector<zetasql::Value>> rows;

  // Row for the unnamed default schema. This is an empty string in GSQL and
  // kPublic in PG.
  absl::flat_hash_map<std::string, zetasql::Value> specific_kvs;
  std::string schema_name = GetNameForDialect(kSchemaName);
  if (dialect_ == DatabaseDialect::POSTGRESQL) {
    specific_kvs[schema_name] = String(kPublic);
  }
  rows.push_back(GetRowFromRowKVs(table, specific_kvs));

  // Row for the information schema.
  specific_kvs.clear();
  specific_kvs[schema_name] = String(GetNameForDialect(kInformationSchema));
  rows.push_back(GetRowFromRowKVs(table, specific_kvs));

  table->SetContents(rows);
}

void InformationSchemaCatalog::FillDatabaseOptionsTable() {
  auto table = tables_by_name_.at(GetNameForDialect(kDatabaseOptions)).get();

  absl::flat_hash_map<std::string, zetasql::Value> specific_kvs;
  if (dialect_ == DatabaseDialect::POSTGRESQL) {
    specific_kvs[GetNameForDialect(kSchemaName)] = String(kPublic);
    specific_kvs[GetNameForDialect(kOptionType)] = String(kCharacterVarying);
  } else {
    specific_kvs[GetNameForDialect(kOptionType)] = String(kString);
  }
  specific_kvs[GetNameForDialect(kOptionName)] = String(kDatabaseDialect);
  specific_kvs[GetNameForDialect(kOptionValue)] =
      String(DatabaseDialect_Name(dialect_));

  std::vector<std::vector<zetasql::Value>> rows;
  rows.push_back(GetRowFromRowKVs(table, specific_kvs));

  table->SetContents(rows);
}

zetasql::SimpleTable* InformationSchemaCatalog::AddTablesTable() {
  // Setup table schema.
  auto tables = new zetasql::SimpleTable(
      kTables, {{kTableCatalog, StringType()},
                {kTableSchema, StringType()},
                {kTableType, StringType()},
                {kTableName, StringType()},
                {kParentTableName, StringType()},
                {kOnDeleteAction, StringType()},
                {kSpannerState, StringType()},
                {kRowDeletionPolicyExpression, StringType()}});
  // Add table to catalog so it is included in rows.
  AddOwnedTable(tables);
  return tables;
}

void InformationSchemaCatalog::FillTablesTable(zetasql::SimpleTable* tables) {
  // Add table rows.
  std::vector<std::vector<zetasql::Value>> rows;
  for (const Table* table : default_schema_->tables()) {
    rows.push_back({
        // table_catalog
        String(""),
        // table_schema
        String(""),
        // table_type
        String(kBaseTable),
        // table_name
        String(table->Name()),
        // parent_table_name
        table->parent() ? String(table->parent()->Name()) : NullString(),
        // on_delete_action
        table->parent()
            ? String(OnDeleteActionToString(table->on_delete_action()))
            : NullString(),
        // spanner_state,
        String(kCommitted),
        // row_deletion_policy_expression
        table->row_deletion_policy().has_value()
            ? String(RowDeletionPolicyToString(
                  table->row_deletion_policy().value()))
            : NullString(),
    });
  }

  for (const View* view : default_schema_->views()) {
    rows.push_back({
        // table_catalog
        String(""),
        // table_schema
        String(""),
        // table_type
        String("VIEW"),
        // table_name
        String(view->Name()),
        // parent_table_name
        NullString(),
        // on_delete_action
        NullString(),
        // spanner_state,
        String(kCommitted),
        // row_deletion_policy_expression
        NullString(),
    });
  }

  for (const auto& table : this->tables()) {
    rows.push_back({
        // table_catalog
        String(""),
        // table_schema
        String(kInformationSchema),
        // table_type
        String(kView),
        // table_name
        String(table->Name()),
        // parent_table_name
        NullString(),
        // on_delete_action
        NullString(),
        // spanner_state,
        NullString(),
        // row_deletion_policy_expression
        NullString(),
    });
  }

  tables->SetContents(rows);
}

zetasql::SimpleTable* InformationSchemaCatalog::AddColumnsTable() {
  // Setup table schema.
  auto columns = new zetasql::SimpleTable(
      kColumns, {{kTableCatalog, StringType()},
                 {kTableSchema, StringType()},
                 {kTableName, StringType()},
                 {kColumnName, StringType()},
                 {kOrdinalPosition, Int64Type()},
                 {kColumnDefault, StringType()},
                 {kDataType, StringType()},
                 {kIsNullable, StringType()},
                 {kSpannerType, StringType()},
                 {kIsGenerated, StringType()},
                 {kGenerationExpression, StringType()},
                 {kIsStored, StringType()},
                 {kSpannerState, StringType()}});
  AddOwnedTable(columns);
  return columns;
}

void InformationSchemaCatalog::FillColumnsTable(
    zetasql::SimpleTable* columns) {
  // Add table rows.
  std::vector<std::vector<zetasql::Value>> rows;
  for (const Table* table : default_schema_->tables()) {
    int pos = 1;
    for (const Column* column : table->columns()) {
      absl::string_view expression;
      if (column->is_generated()) {
        expression = column->expression().value();
        absl::ConsumePrefix(&expression, "(");
        absl::ConsumeSuffix(&expression, ")");
      }
      rows.push_back({
          // table_catalog
          String(""),
          // table_schema
          String(""),
          // table_name
          String(table->Name()),
          // column_name
          String(column->Name()),
          // ordinal_position
          Int64(pos++),
          // column_default,
          column->has_default_value() ? String(column->expression().value())
                                      : NullString(),
          // data_type,
          NullString(),
          // is_nullable
          String(column->is_nullable() ? kYes : kNo),
          // spanner_type
          String(ColumnTypeToString(column->GetType(),
                                    column->declared_max_length())),
          // is_generated
          String(column->is_generated() ? kAlways : kNever),
          // generation_expression
          column->is_generated() ? String(expression) : NullString(),
          // is_stored
          column->is_generated() ? String(kYes) : NullString(),
          // spanner_state
          String(kCommitted),
      });
    }
  }

  // Add columns for views.
  for (const View* view : default_schema_->views()) {
    int pos = 1;
    for (const View::Column& column : view->columns()) {
      rows.push_back({
          // table_catalog
          String(""),
          // table_schema
          String(""),
          // table_name
          String(view->Name()),
          // column_name
          String(column.name),
          // ordinal_position
          Int64(pos++),
          // column_default,
          NullBytes(),
          // data_type,
          NullString(),
          // is_nullable
          String(kYes),
          // spanner_type
          String(ColumnTypeToString(column.type, 0)),
          // is_generated
          String(kNever),
          // generation_expression
          NullString(),
          // is_stored
          NullString(),
          // spanner_state
          String(kCommitted),
      });
    }
  }

  // Add columns for the tables that live inside INFORMATION_SCHEMA.
  for (const auto& table : this->tables()) {
    int pos = 1;
    for (int i = 0; i < table->NumColumns(); ++i) {
      const auto* column = table->GetColumn(i);
      const auto& metadata = GetColumnMetadata(dialect_, table, column);
      rows.push_back({
          // table_catalog
          String(""),
          // table_schema
          String(kInformationSchema),
          // table_name
          String(table->Name()),
          // column_name
          String(column->Name()),
          // ordinal_position
          Int64(pos++),
          // column_default,
          NullString(),
          // data_type,
          NullString(),
          // is_nullable
          String(metadata.is_nullable),
          // spanner_type
          String(metadata.spanner_type),
          // is_generated
          String(kNever),
          // generation_expression
          NullString(),
          // is_stored
          NullString(),
          // spanner_state
          NullString(),
      });
    }
  }

  // Add table to catalog.
  columns->SetContents(rows);
}

zetasql::SimpleTable* InformationSchemaCatalog::AddColumnColumnUsageTable() {
  // Setup table schema.
  auto column_column_usage = new zetasql::SimpleTable(
      kColumnColumnUsage, {{kTableCatalog, StringType()},
                           {kTableSchema, StringType()},
                           {kTableName, StringType()},
                           {kColumnName, StringType()},
                           {kDepenentColumn, StringType()}});
  AddOwnedTable(column_column_usage);
  return column_column_usage;
}

void InformationSchemaCatalog::FillColumnColumnUsageTable(
    zetasql::SimpleTable* column_column_usage) {
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
              String(""),
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

zetasql::SimpleTable* InformationSchemaCatalog::AddIndexesTable() {
  // Setup table schema.
  auto indexes =
      new zetasql::SimpleTable(kIndexes, {
                                               {kTableCatalog, StringType()},
                                               {kTableSchema, StringType()},
                                               {kTableName, StringType()},
                                               {kIndexName, StringType()},
                                               {kIndexType, StringType()},
                                               {kParentTableName, StringType()},
                                               {kIsUnique, BoolType()},
                                               {kIsNullFiltered, BoolType()},
                                               {kIndexState, StringType()},
                                               {kSpannerIsManaged, BoolType()},
                                           });
  AddOwnedTable(indexes);
  return indexes;
}

void InformationSchemaCatalog::FillIndexesTable(
    zetasql::SimpleTable* indexes) {
  // Add table rows.
  std::vector<std::vector<zetasql::Value>> rows;
  for (const Table* table : default_schema_->tables()) {
    // Add normal indexes.
    for (const Index* index : table->indexes()) {
      rows.push_back({
          // table_catalog
          String(""),
          // table_schema
          String(""),
          // table_name
          String(table->Name()),
          // index_name
          String(index->Name()),
          // index_type
          String(kIndex),
          // parent_table_name
          String(index->parent() ? index->parent()->Name() : ""),
          // is_unique
          Bool(index->is_unique()),
          // is_null_filtered
          Bool(index->is_null_filtered()),
          // index_state
          String(kReadWrite),
          // spanner_is_managed
          Bool(index->is_managed()),
      });
    }

    // Add the primary key index.
    rows.push_back({
        // table_catalog
        String(""),
        // table_schema
        String(""),
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

  // Add table to catalog.
  indexes->SetContents(rows);
}

zetasql::SimpleTable* InformationSchemaCatalog::AddIndexColumnsTable() {
  // Setup table schema.
  auto index_columns = new zetasql::SimpleTable(
      kIndexColumns, {
                         {kTableCatalog, StringType()},
                         {kTableSchema, StringType()},
                         {kTableName, StringType()},
                         {kIndexName, StringType()},
                         {kIndexType, StringType()},
                         {kColumnName, StringType()},
                         {kOrdinalPosition, Int64Type()},
                         {kColumnOrdering, StringType()},
                         {kIsNullable, StringType()},
                         {kSpannerType, StringType()},
                     });

  // Add table to catalog.
  AddOwnedTable(index_columns);
  return index_columns;
}

void InformationSchemaCatalog::FillIndexColumnsTable(
    zetasql::SimpleTable* index_columns) {
  // Add table rows.
  std::vector<std::vector<zetasql::Value>> rows;
  for (const Table* table : default_schema_->tables()) {
    // Add normal indexes.
    for (const Index* index : table->indexes()) {
      int pos = 1;
      // Add key columns.
      for (const KeyColumn* key_column : index->key_columns()) {
        rows.push_back({
            // table_catalog
            String(""),
            // table_schema
            String(""),
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
            String(key_column->is_descending() ? kDesc : kAsc),
            // is_nullable
            String(key_column->column()->is_nullable() &&
                           !index->is_null_filtered()
                       ? kYes
                       : kNo),
            // spanner_type
            String(ColumnTypeToString(
                key_column->column()->GetType(),
                key_column->column()->declared_max_length())),
        });
      }

      // Add storing columns.
      for (const Column* column : index->stored_columns()) {
        rows.push_back({
            // table_catalog
            String(""),
            // table_schema
            String(""),
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
            String(ColumnTypeToString(column->GetType(),
                                      column->declared_max_length())),
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
            String(""),
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
            String(key_column->is_descending() ? kDesc : kAsc),
            // is_nullable
            String(key_column->column()->is_nullable() ? kYes : kNo),
            // spanner_type
            String(ColumnTypeToString(
                key_column->column()->GetType(),
                key_column->column()->declared_max_length())),
        });
      }
    }
  }

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

  index_columns->SetContents(rows);
}

void InformationSchemaCatalog::AddColumnOptionsTable() {
  // Setup table schema.
  auto columns = new zetasql::SimpleTable(kColumnOptions,
                                            {{kTableCatalog, StringType()},
                                             {kTableSchema, StringType()},
                                             {kTableName, StringType()},
                                             {kColumnName, StringType()},
                                             {kOptionName, StringType()},
                                             {kOptionType, StringType()},
                                             {kOptionValue, StringType()}});

  // Add table rows.
  std::vector<std::vector<zetasql::Value>> rows;
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

  // Add table to catalog.
  columns->SetContents(rows);
  AddOwnedTable(columns);
}

zetasql::SimpleTable* InformationSchemaCatalog::AddTableConstraintsTable() {
  // Setup table schema.
  auto table_constraints = new zetasql::SimpleTable(
      kTableConstraints, {
                             {kConstraintCatalog, StringType()},
                             {kConstraintSchema, StringType()},
                             {kConstraintName, StringType()},
                             {kTableCatalog, StringType()},
                             {kTableSchema, StringType()},
                             {kTableName, StringType()},
                             {kConstraintType, StringType()},
                             {kIsDeferrable, StringType()},
                             {kInitiallyDeferred, StringType()},
                             {kEnforced, StringType()},
                         });

  // Add table to catalog.
  AddOwnedTable(table_constraints);
  return table_constraints;
}

void InformationSchemaCatalog::FillTableConstraintsTable(
    zetasql::SimpleTable* table_constraints) {
  std::vector<std::vector<zetasql::Value>> rows;

  // Add the user table constraints.
  for (const auto* table : default_schema_->tables()) {
    // Add the primary key.
    rows.push_back({
        // constraint_catalog
        String(""),
        // constraint_schema
        String(""),
        // constraint_name
        String(PrimaryKeyName(table)),
        // table_catalog
        String(""),
        // table_schema
        String(""),
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
          String(""),
          // constraint_name
          String(CheckNotNullName(table, column)),
          // table_catalog
          String(""),
          // table_schema
          String(""),
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

    // Add the check constraints defined by the ZETASQL_CHECK keyword.
    for (const auto* check_constraint : table->check_constraints()) {
      rows.push_back({
          // constraint_catalog
          String(""),
          // constraint_schema
          String(""),
          // constraint_name
          String(check_constraint->Name()),
          // table_catalog
          String(""),
          // table_schema
          String(""),
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
          String(""),
          // constraint_name
          String(foreign_key->Name()),
          // table_catalog
          String(""),
          // table_schema
          String(""),
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
            String(""),
            // constraint_name
            String(foreign_key->referenced_index()->Name()),
            // table_catalog
            String(""),
            // table_schema
            String(""),
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

  table_constraints->SetContents(rows);
}

zetasql::SimpleTable* InformationSchemaCatalog::AddCheckConstraintsTable() {
  // Setup table schema.
  auto check_constraints = new zetasql::SimpleTable(
      kCheckConstraints, {
                             {kConstraintCatalog, StringType()},
                             {kConstraintSchema, StringType()},
                             {kConstraintName, StringType()},
                             {kCheckClause, StringType()},
                             {kSpannerState, StringType()},
                         });

  // Add table to catalog.
  AddOwnedTable(check_constraints);
  return check_constraints;
}

void InformationSchemaCatalog::FillCheckConstraintsTable(
    zetasql::SimpleTable* check_constraints) {
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
          String(""),
          // constraint_name
          String(CheckNotNullName(table, column)),
          // check clause
          String(CheckNotNullClause(column->Name())),
          // spanner state
          String(kCommitted),
      });
    }

    // Add the check constraints defined by the ZETASQL_CHECK keyword.
    for (const auto* check_constraint : table->check_constraints()) {
      rows.push_back({
          // constraint_catalog
          String(""),
          // constraint_schema
          String(""),
          // constraint_name
          String(check_constraint->Name()),
          // check clasue
          String(check_constraint->expression()),
          // spanner state
          String(kCommitted),
      });
    }
  }

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
  check_constraints->SetContents(rows);
}

zetasql::SimpleTable*
InformationSchemaCatalog::AddConstraintTableUsageTable() {
  // Setup table schema.
  auto constraint_table_usage = new zetasql::SimpleTable(
      kConstraintTableUsage, {
                                 {kTableCatalog, StringType()},
                                 {kTableSchema, StringType()},
                                 {kTableName, StringType()},
                                 {kConstraintCatalog, StringType()},
                                 {kConstraintSchema, StringType()},
                                 {kConstraintName, StringType()},
                             });

  // Add table to catalog.
  AddOwnedTable(constraint_table_usage);
  return constraint_table_usage;
}

void InformationSchemaCatalog::FillConstraintTableUsageTable(
    zetasql::SimpleTable* constraint_table_usage) {
  std::vector<std::vector<zetasql::Value>> rows;

  // Add the user table constraints.
  for (const auto* table : default_schema_->tables()) {
    // Add the primary key.
    rows.push_back({
        // table_catalog
        String(""),
        // table_schema
        String(""),
        // table_name
        String(table->Name()),
        // constraint_catalog
        String(""),
        // constraint_schema
        String(""),
        // constraint_name
        String(PrimaryKeyName(table)),
    });

    // Add the NOT NULL check constraints.
    for (const auto* column : table->columns()) {
      if (column->is_nullable()) {
        continue;
      }
      rows.push_back({
          // table_catalog
          String(""),
          // table_schema
          String(""),
          // table_name
          String(table->Name()),
          // constraint_catalog
          String(""),
          // constraint_schema
          String(""),
          // constraint_name
          String(CheckNotNullName(table, column)),
      });
    }

    // Add the check constraints defined by the ZETASQL_CHECK keyword.
    for (const auto* check_constraint : table->check_constraints()) {
      rows.push_back({
          // table_catalog
          String(""),
          // table_schema
          String(""),
          // table_name
          String(table->Name()),
          // constraint_catalog
          String(""),
          // constraint_schema
          String(""),
          // constraint_name
          String(check_constraint->Name()),
      });
    }

    // Add the foreign keys.
    for (const auto* foreign_key : table->foreign_keys()) {
      rows.push_back({
          // table_catalog
          String(""),
          // table_schema
          String(""),
          // table_name
          String(foreign_key->referenced_table()->Name()),
          // constraint_catalog
          String(""),
          // constraint_schema
          String(""),
          // constraint_name
          String(foreign_key->Name()),
      });

      // Add the foreign key's unique backing index as a unique constraint.
      if (foreign_key->referenced_index()) {
        rows.push_back({
            // table_catalog
            String(""),
            // table_schema
            String(""),
            // table_name
            String(foreign_key->referenced_table()->Name()),
            // constraint_catalog
            String(""),
            // constraint_schema
            String(""),
            // constraint_name
            String(foreign_key->referenced_index()->Name()),
        });
      }
    }
  }

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

  constraint_table_usage->SetContents(rows);
}

zetasql::SimpleTable*
InformationSchemaCatalog::AddReferentialConstraintsTable() {
  // Setup table schema.
  auto referential_constraints = new zetasql::SimpleTable(
      kReferentialConstraints, {
                                   {kConstraintCatalog, StringType()},
                                   {kConstraintSchema, StringType()},
                                   {kConstraintName, StringType()},
                                   {kUniqueConstraintCatalog, StringType()},
                                   {kUniqueConstraintSchema, StringType()},
                                   {kUniqueConstraintName, StringType()},
                                   {kMatchOption, StringType()},
                                   {kUpdateRule, StringType()},
                                   {kDeleteRule, StringType()},
                                   {kSpannerState, StringType()},
                               });

  // Add table to catalog.
  AddOwnedTable(referential_constraints);
  return referential_constraints;
}

void InformationSchemaCatalog::FillReferentialConstraintsTable(
    zetasql::SimpleTable* referential_constraints) {
  std::vector<std::vector<zetasql::Value>> rows;

  // Add the foreign key constraints.
  for (const auto* table : default_schema_->tables()) {
    for (const auto* foreign_key : table->foreign_keys()) {
      rows.push_back({
          // constraint_catalog
          String(""),
          // constraint_schema
          String(""),
          // constraint_name
          String(foreign_key->Name()),
          // unique_constraint_catalog
          String(""),
          // unique_constraint_schema
          String(""),
          // unique_constraint_name
          String(ForeignKeyReferencedIndexName(foreign_key)),
          // match_option
          String(kSimple),
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

zetasql::SimpleTable* InformationSchemaCatalog::AddKeyColumnUsageTable() {
  // Setup table schema.
  auto key_column_usage = new zetasql::SimpleTable(
      kKeyColumnUsage, {
                           {kConstraintCatalog, StringType()},
                           {kConstraintSchema, StringType()},
                           {kConstraintName, StringType()},
                           {kTableCatalog, StringType()},
                           {kTableSchema, StringType()},
                           {kTableName, StringType()},
                           {kColumnName, StringType()},
                           {kOrdinalPosition, Int64Type()},
                           {kPositionInUniqueConstraint, Int64Type()},
                       });

  // Add table to catalog.
  AddOwnedTable(key_column_usage);
  return key_column_usage;
}

void InformationSchemaCatalog::FillKeyColumnUsageTable(
    zetasql::SimpleTable* key_column_usage) {
  std::vector<std::vector<zetasql::Value>> rows;

  for (const auto* table : default_schema_->tables()) {
    // Add the primary key columns.
    int table_ordinal = 1;
    for (const auto* key_column : table->primary_key()) {
      rows.push_back({
          // constraint_catalog
          String(""),
          // constraint_schema
          String(""),
          // constraint_name
          String(PrimaryKeyName(table)),
          // table_catalog
          String(""),
          // table_schema
          String(""),
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
            String(""),
            // constraint_name
            String(foreign_key->Name()),
            // table_catalog
            String(""),
            // table_schema
            String(""),
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
              String(""),
              // constraint_name
              String(foreign_key->referenced_index()->Name()),
              // table_catalog
              String(""),
              // table_schema
              String(""),
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

  key_column_usage->SetContents(rows);
}

zetasql::SimpleTable*
InformationSchemaCatalog::AddConstraintColumnUsageTable() {
  // Setup table schema.
  auto constraint_column_usage = new zetasql::SimpleTable(
      kConstraintColumnUsage, {
                                  {kTableCatalog, StringType()},
                                  {kTableSchema, StringType()},
                                  {kTableName, StringType()},
                                  {kColumnName, StringType()},
                                  {kConstraintCatalog, StringType()},
                                  {kConstraintSchema, StringType()},
                                  {kConstraintName, StringType()},
                              });

  // Add table to catalog.
  AddOwnedTable(constraint_column_usage);
  return constraint_column_usage;
}

void InformationSchemaCatalog::FillConstraintColumnUsageTable(
    zetasql::SimpleTable* constraint_column_usage) {
  std::vector<std::vector<zetasql::Value>> rows;

  for (const auto* table : default_schema_->tables()) {
    // Add the primary key columns.
    for (const auto* key_column : table->primary_key()) {
      rows.push_back({
          // table_catalog
          String(""),
          // table_schema
          String(""),
          // table_name
          String(table->Name()),
          // column_name
          String(key_column->column()->Name()),
          // constraint_catalog
          String(""),
          // constraint_schema
          String(""),
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
          String(""),
          // table_name
          String(table->Name()),
          // column_name
          String(column->Name()),
          // constraint_catalog
          String(""),
          // constraint_schema
          String(""),
          // constraint_name
          String(CheckNotNullName(table, column)),
      });
    }

    // Add the check constraints defined by the ZETASQL_CHECK keyword.
    for (const auto* check_constraint : table->check_constraints()) {
      for (const auto* dep_column : check_constraint->dependent_columns()) {
        rows.push_back({
            // table_catalog
            String(""),
            // table_schema
            String(""),
            // table_name
            String(table->Name()),
            // column_name
            String(dep_column->Name()),
            // constraint_catalog
            String(""),
            // constraint_schema
            String(""),
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
            String(""),
            // table_name
            String(foreign_key->referenced_table()->Name()),
            // column_name
            String(column->Name()),
            // constraint_catalog
            String(""),
            // constraint_schema
            String(""),
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
              String(""),
              // table_name
              String(foreign_key->referenced_table()->Name()),
              // column_name
              String(key_column->column()->Name()),
              // constraint_catalog
              String(""),
              // constraint_schema
              String(""),
              // constraint_name
              String(foreign_key->referenced_index()->Name()),
          });
        }
      }
    }
  }

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

  constraint_column_usage->SetContents(rows);
}

zetasql::SimpleTable* InformationSchemaCatalog::AddViewsTable() {
  // Setup table schema.
  auto views =
      new zetasql::SimpleTable(kViews, {
                                             {kTableCatalog, StringType()},
                                             {kTableSchema, StringType()},
                                             {kTableName, StringType()},
                                             {kViewDefinition, StringType()},
                                         });

  // Add table to catalog.
  AddOwnedTable(views);
  return views;
}

void InformationSchemaCatalog::FillViewsTable(zetasql::SimpleTable* views) {
  std::vector<std::vector<zetasql::Value>> rows;

  for (const View* view : default_schema_->views()) {
    rows.push_back({
        // table_catalog
        String(""),
        // table_schema
        String(""),
        // table_name
        String(view->Name()),
        // view_definition
        String(view->body()),
    });
  }

  views->SetContents(rows);
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
