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

#include "backend/schema/printer/print_ddl.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

using zetasql::types::BoolType;
using zetasql::types::Int64Type;
using zetasql::types::StringType;
using zetasql::values::Bool;
using zetasql::values::Int64;
using zetasql::values::NullInt64;
using zetasql::values::NullString;
using zetasql::values::String;

}  // namespace

InformationSchemaCatalog::InformationSchemaCatalog(const Schema* default_schema)
    : zetasql::SimpleCatalog(kName), default_schema_(default_schema) {
  AddSchemataTable();
  AddTablesTable();
  AddColumnsTable();
  AddIndexesTable();
  AddIndexColumnsTable();
  AddColumnOptionsTable();
}

void InformationSchemaCatalog::AddSchemataTable() {
  // Setup table schema.
  auto schemata = new zetasql::SimpleTable(
      "SCHEMATA",
      {{"CATALOG_NAME", StringType()}, {"SCHEMA_NAME", StringType()}});

  // Add table rows.
  std::vector<std::vector<zetasql::Value>> rows;
  rows.push_back({String(""), String("")});
  rows.push_back({String(""), String("INFORMATION_SCHEMA")});

  // Add table to catalog.
  schemata->SetContents(rows);
  AddOwnedTable(schemata);
}

void InformationSchemaCatalog::AddTablesTable() {
  // Setup table schema.
  auto tables = new zetasql::SimpleTable(
      "TABLES", {{"TABLE_CATALOG", StringType()},
                 {"TABLE_SCHEMA", StringType()},
                 {"TABLE_NAME", StringType()},
                 {"PARENT_TABLE_NAME", StringType()},
                 {"ON_DELETE_ACTION", StringType()}});

  // Add table rows.
  std::vector<std::vector<zetasql::Value>> rows;
  for (const Table* table : default_schema_->tables()) {
    rows.push_back(
        {// table_catalog
         String(""),
         // table_schema
         String(""),
         // table_name
         String(table->Name()),
         // parent_table_name
         table->parent() ? String(table->parent()->Name()) : NullString(),
         // on_delete_action
         table->parent()
             ? String(OnDeleteActionToString(table->on_delete_action()))
             : NullString()});
  }

  // Add table to catalog.
  tables->SetContents(rows);
  AddOwnedTable(tables);
}

void InformationSchemaCatalog::AddColumnsTable() {
  // Setup table schema.
  auto columns =
      new zetasql::SimpleTable("COLUMNS", {{"TABLE_CATALOG", StringType()},
                                             {"TABLE_SCHEMA", StringType()},
                                             {"TABLE_NAME", StringType()},
                                             {"COLUMN_NAME", StringType()},
                                             {"ORDINAL_POSITION", Int64Type()},
                                             {"IS_NULLABLE", StringType()},
                                             {"SPANNER_TYPE", StringType()}});

  // Add table rows.
  std::vector<std::vector<zetasql::Value>> rows;
  for (const Table* table : default_schema_->tables()) {
    int pos = 1;
    for (const Column* column : table->columns()) {
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
          // is_nullable
          String(column->is_nullable() ? "YES" : "NO"),
          // spanner_type
          String(ColumnTypeToString(column->GetType(),
                                    column->declared_max_length())),
      });
    }
  }

  // Add table to catalog.
  columns->SetContents(rows);
  AddOwnedTable(columns);
}

void InformationSchemaCatalog::AddIndexesTable() {
  // Setup table schema.
  auto indexes = new zetasql::SimpleTable(
      "INDEXES", {
                     {"TABLE_CATALOG", StringType()},
                     {"TABLE_SCHEMA", StringType()},
                     {"TABLE_NAME", StringType()},
                     {"INDEX_NAME", StringType()},
                     {"INDEX_TYPE", StringType()},
                     {"PARENT_TABLE_NAME", StringType()},
                     {"IS_UNIQUE", BoolType()},
                     {"IS_NULL_FILTERED", BoolType()},
                     {"INDEX_STATE", StringType()},
                 });

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
          String("INDEX"),
          // parent_table_name
          String(index->parent() ? index->parent()->Name() : ""),
          // is_unique
          Bool(index->is_unique()),
          // is_null_filtered
          Bool(index->is_null_filtered()),
          // index_state
          String("READ_WRITE"),
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
        String("PRIMARY_KEY"),
        // index_type
        String("PRIMARY_KEY"),
        // parent_table_name
        String(""),
        // is_unique
        Bool(true),
        // is_null_filtered
        Bool(false),
        // index_state
        NullString(),
    });
  }

  // Add table to catalog.
  indexes->SetContents(rows);
  AddOwnedTable(indexes);
}

void InformationSchemaCatalog::AddIndexColumnsTable() {
  // Setup table schema.
  auto index_columns = new zetasql::SimpleTable(
      "INDEX_COLUMNS", {
                           {"TABLE_CATALOG", StringType()},
                           {"TABLE_SCHEMA", StringType()},
                           {"TABLE_NAME", StringType()},
                           {"INDEX_NAME", StringType()},
                           {"COLUMN_NAME", StringType()},
                           {"ORDINAL_POSITION", Int64Type()},
                           {"COLUMN_ORDERING", StringType()},
                           {"IS_NULLABLE", StringType()},
                           {"SPANNER_TYPE", StringType()},
                       });

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
            // column_name
            String(key_column->column()->Name()),
            // ordinal_position
            Int64(pos++),
            // column_ordering
            String(key_column->is_descending() ? "DESC" : "ASC"),
            // is_nullable
            String(key_column->column()->is_nullable() &&
                           !index->is_null_filtered()
                       ? "YES"
                       : "NO"),
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
            // column_name
            String(column->Name()),
            // ordinal_position
            NullInt64(),
            // column_ordering
            NullString(),
            // is_nullable
            String(column->is_nullable() ? "YES" : "NO"),
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
            String("PRIMARY_KEY"),
            // column_name
            String(key_column->column()->Name()),
            // ordinal_position
            Int64(pos++),
            // column_ordering
            String(key_column->is_descending() ? "DESC" : "ASC"),
            // is_nullable
            String(key_column->column()->is_nullable() ? "YES" : "NO"),
            // spanner_type
            String(ColumnTypeToString(
                key_column->column()->GetType(),
                key_column->column()->declared_max_length())),
        });
      }
    }
  }

  // Add table to catalog.
  index_columns->SetContents(rows);
  AddOwnedTable(index_columns);
}

void InformationSchemaCatalog::AddColumnOptionsTable() {
  // Setup table schema.
  auto columns = new zetasql::SimpleTable("COLUMN_OPTIONS",
                                            {{"TABLE_CATALOG", StringType()},
                                             {"TABLE_SCHEMA", StringType()},
                                             {"TABLE_NAME", StringType()},
                                             {"COLUMN_NAME", StringType()},
                                             {"OPTION_NAME", StringType()},
                                             {"OPTION_TYPE", StringType()},
                                             {"OPTION_VALUE", StringType()}});

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
                        String("allow_commit_timestamp"), String("BOOL"),
                        // option_value
                        String("TRUE")});
      }
    }
  }

  // Add table to catalog.
  columns->SetContents(rows);
  AddOwnedTable(columns);
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
