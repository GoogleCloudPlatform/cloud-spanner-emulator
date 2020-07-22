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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using Bytes = google::cloud::spanner::Bytes;

class InformationSchemaTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    return SetSchema({R"(
      CREATE TABLE Base (
        Key1 INT64,
        Key2 STRING(256),
        BoolValue BOOL,
        IntValue INT64 NOT NULL,
        DoubleValue FLOAT64,
        StrValue STRING(MAX),
        ByteValue BYTES(256),
        TimestampValue TIMESTAMP options (allow_commit_timestamp = true),
        DateValue DATE,
        BoolArray ARRAY<BOOL> NOT NULL,
        IntArray ARRAY<INT64>,
        DoubleArray ARRAY<FLOAT64>,
        StrArray ARRAY<STRING(256)>,
        ByteArray ARRAY<BYTES(MAX)>,
        TimestampArray ARRAY<TIMESTAMP>,
        DateArray ARRAY<DATE>
      ) PRIMARY KEY (Key1, Key2 DESC)
    )",
                      R"(
      CREATE TABLE CascadeChild (
        Key1 INT64,
        Key2 STRING(256),
        ChildKey BOOL,
        Value1 STRING(MAX) NOT NULL,
        Value2 BOOL
      ) PRIMARY KEY (Key1, Key2 DESC, ChildKey ASC),
        INTERLEAVE IN PARENT Base ON DELETE CASCADE
    )",
                      R"(
      CREATE TABLE NoActionChild (
        Key1 INT64,
        Key2 STRING(256),
        ChildKey BOOL,
        Value  STRING(MAX)
      ) PRIMARY KEY (Key1, Key2 DESC, ChildKey ASC),
        INTERLEAVE IN PARENT Base ON DELETE NO ACTION
    )",
                      R"(
      CREATE UNIQUE NULL_FILTERED INDEX CascadeChildByValue
      ON CascadeChild(Key1, Key2 DESC, Value2 ASC)
      STORING(Value1), INTERLEAVE IN Base
    )",
                      R"(
      CREATE INDEX NoActionChildByValue ON NoActionChild(Value ASC)
    )"});
  }

  // INFORMATION_SCHEMA tables that the emulator does not yet support.
  // Production will return column metadata values for these tables and the
  // emulator will not, so the column tests will ignore any column within any of
  // these tables.
  static google::cloud::spanner::SqlStatement::ParamType
  BuildParamsForUnsupportedTables() {
    return {
        {"unsupported_tables", cloud::spanner::Value(std::vector<std::string>{
                                   "TABLE_CONSTRAINTS",
                                   "CONSTRAINT_TABLE_USAGE",
                                   "REFERENTIAL_CONSTRAINTS",
                                   "KEY_COLUMN_USAGE",
                                   "CONSTRAINT_COLUMN_USAGE",
                                   "CHECK_CONSTRAINTS",
                                   "DATABASE_OPTIONS",
                               })}};
  }

  // Aliases so test expectations read more clearly.
  cloud::spanner::Value Nb() { return Null<Bytes>(); }
  cloud::spanner::Value Ns() { return Null<std::string>(); }
};

TEST_F(InformationSchemaTest, QueriesSchemataTable) {
  EXPECT_THAT(Query(R"(
                select
                  s.catalog_name,
                  s.schema_name
                from
                  information_schema.schemata AS s
                order by
                  s.catalog_name,
                  s.schema_name
                limit 2
              )"),
              IsOkAndHoldsRows({{"", ""}, {"", "INFORMATION_SCHEMA"}}));
}

TEST_F(InformationSchemaTest, QueriesTablesFromInformationSchema) {
  // The documented set of tables that should be returned is at:
  // https://cloud.google.com/spanner/docs/information-schema#information_schemadatabase_options.
  //
  // The tables filtered out by the WHERE clause are not currently available in
  // the emulator. This test should not need to filter on table_name.

  EXPECT_THAT(
      QueryWithParams(R"(
                select
                  t.table_catalog,
                  t.table_schema,
                  t.table_name,
                  t.parent_table_name,
                  t.on_delete_action,
                  t.spanner_state
                from
                  information_schema.tables AS t
                where
                      t.table_schema = 'INFORMATION_SCHEMA'
                  and t.table_name not in unnest(@unsupported_tables)
              )",
                      BuildParamsForUnsupportedTables()),
      IsOkAndHoldsUnorderedRows(
          {{"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", Ns(), Ns(), Ns()},
           {"", "INFORMATION_SCHEMA", "COLUMNS", Ns(), Ns(), Ns()},
           {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", Ns(), Ns(), Ns()},
           {"", "INFORMATION_SCHEMA", "INDEXES", Ns(), Ns(), Ns()},
           {"", "INFORMATION_SCHEMA", "SCHEMATA", Ns(), Ns(), Ns()},
           {"", "INFORMATION_SCHEMA", "TABLES", Ns(), Ns(), Ns()}}));
}

TEST_F(InformationSchemaTest, QueryColumns) {
  // The emulator is missing these columns:
  //    - IS_GENERATED STRING
  //    - GENERATION_EXPRESSION STRING
  //    - IS_STORED STRING
  //
  // The tables filtered out by the WHERE clause are not currently available in
  // the emulator. This test should not need to filter on table_name.
  //
  // This test currently ignores the ORDINAL_POSITION column as the emulator
  // reports a different value because production has additional columns that
  // the emulator does not yet support.

  EXPECT_THAT(QueryWithParams(R"(
                select
                  t.table_catalog,
                  t.table_schema,
                  t.table_name,
                  t.column_name,
                  t.column_default,
                  t.data_type,
                  t.is_nullable,
                  t.spanner_type,
                  t.spanner_state
                from
                  information_schema.columns as t
                where
                      t.table_schema = 'INFORMATION_SCHEMA'
                  and t.table_name not in unnest(@unsupported_tables)

                  and not (t.table_name = 'COLUMNS' and t.column_name = 'GENERATION_EXPRESSION')
                  and not (t.table_name = 'COLUMNS' and t.column_name = 'IS_GENERATED')
                  and not (t.table_name = 'COLUMNS' and t.column_name = 'IS_STORED')
                  and not (t.table_name = 'INDEXES' and t.column_name = 'SPANNER_IS_MANAGED')
                  and not (t.table_name = 'SCHEMATA' and t.column_name = 'EFFECTIVE_TIMESTAMP')
              )",
                              BuildParamsForUnsupportedTables()),

              IsOkAndHoldsUnorderedRows({
                  {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "COLUMN_NAME",
                   Nb(), Ns(), "NO", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "OPTION_NAME",
                   Nb(), Ns(), "NO", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "OPTION_TYPE",
                   Nb(), Ns(), "NO", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "OPTION_VALUE",
                   Nb(), Ns(), "NO", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "TABLE_CATALOG",
                   Nb(), Ns(), "NO", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "TABLE_NAME",
                   Nb(), Ns(), "NO", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "TABLE_SCHEMA",
                   Nb(), Ns(), "NO", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "COLUMNS", "COLUMN_DEFAULT", Nb(),
                   Ns(), "YES", "BYTES(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "COLUMNS", "COLUMN_NAME", Nb(),
                   Ns(), "NO", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "COLUMNS", "DATA_TYPE", Nb(), Ns(),
                   "YES", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "COLUMNS", "IS_NULLABLE", Nb(),
                   Ns(), "YES", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "COLUMNS", "ORDINAL_POSITION",
                   Nb(), Ns(), "NO", "INT64", Ns()},
                  {"", "INFORMATION_SCHEMA", "COLUMNS", "SPANNER_STATE", Nb(),
                   Ns(), "YES", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "COLUMNS", "SPANNER_TYPE", Nb(),
                   Ns(), "YES", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "COLUMNS", "TABLE_CATALOG", Nb(),
                   Ns(), "NO", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "COLUMNS", "TABLE_NAME", Nb(),
                   Ns(), "NO", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "COLUMNS", "TABLE_SCHEMA", Nb(),
                   Ns(), "NO", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "COLUMN_NAME",
                   Nb(), Ns(), "NO", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "COLUMN_ORDERING",
                   Nb(), Ns(), "YES", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "INDEX_NAME",
                   Nb(), Ns(), "NO", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "INDEX_TYPE",
                   Nb(), Ns(), "NO", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "IS_NULLABLE",
                   Nb(), Ns(), "YES", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS",
                   "ORDINAL_POSITION", Nb(), Ns(), "YES", "INT64", Ns()},
                  {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "SPANNER_TYPE",
                   Nb(), Ns(), "YES", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "TABLE_CATALOG",
                   Nb(), Ns(), "NO", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "TABLE_NAME",
                   Nb(), Ns(), "NO", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "TABLE_SCHEMA",
                   Nb(), Ns(), "NO", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "INDEXES", "INDEX_NAME", Nb(),
                   Ns(), "NO", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "INDEXES", "INDEX_STATE", Nb(),
                   Ns(), "NO", "STRING(100)", Ns()},
                  {"", "INFORMATION_SCHEMA", "INDEXES", "INDEX_TYPE", Nb(),
                   Ns(), "NO", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "INDEXES", "IS_NULL_FILTERED",
                   Nb(), Ns(), "NO", "BOOL", Ns()},
                  {"", "INFORMATION_SCHEMA", "INDEXES", "IS_UNIQUE", Nb(), Ns(),
                   "NO", "BOOL", Ns()},
                  {"", "INFORMATION_SCHEMA", "INDEXES", "PARENT_TABLE_NAME",
                   Nb(), Ns(), "NO", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "INDEXES", "TABLE_CATALOG", Nb(),
                   Ns(), "NO", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "INDEXES", "TABLE_NAME", Nb(),
                   Ns(), "NO", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "INDEXES", "TABLE_SCHEMA", Nb(),
                   Ns(), "NO", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "SCHEMATA", "CATALOG_NAME", Nb(),
                   Ns(), "NO", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "SCHEMATA", "SCHEMA_NAME", Nb(),
                   Ns(), "NO", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "TABLES", "ON_DELETE_ACTION", Nb(),
                   Ns(), "YES", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "TABLES", "PARENT_TABLE_NAME",
                   Nb(), Ns(), "YES", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "TABLES", "SPANNER_STATE", Nb(),
                   Ns(), "YES", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "TABLES", "TABLE_CATALOG", Nb(),
                   Ns(), "NO", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "TABLES", "TABLE_NAME", Nb(), Ns(),
                   "NO", "STRING(MAX)", Ns()},
                  {"", "INFORMATION_SCHEMA", "TABLES", "TABLE_SCHEMA", Nb(),
                   Ns(), "NO", "STRING(MAX)", Ns()},
              }));
}

TEST_F(InformationSchemaTest, QueryIndexes) {
  EXPECT_THAT(QueryWithParams(R"(
                select
                  t.table_catalog,
                  t.table_schema,
                  t.table_name,
                  t.index_name,
                  t.index_type,
                  t.parent_table_name,
                  t.is_unique,
                  t.is_null_filtered,
                  t.index_state
                from
                  information_schema.indexes as t
                where
                      t.table_schema = 'INFORMATION_SCHEMA'
                  and t.table_name not in unnest(@unsupported_tables)
              )",
                              BuildParamsForUnsupportedTables()),
              IsOkAndHoldsUnorderedRows({
                  {"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "PRIMARY_KEY",
                   "PRIMARY_KEY", "", true, false, Ns()},
                  {"", "INFORMATION_SCHEMA", "COLUMNS", "PRIMARY_KEY",
                   "PRIMARY_KEY", "", true, false, Ns()},
                  {"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "PRIMARY_KEY",
                   "PRIMARY_KEY", "", true, false, Ns()},
                  {"", "INFORMATION_SCHEMA", "INDEXES", "PRIMARY_KEY",
                   "PRIMARY_KEY", "", true, false, Ns()},
                  {"", "INFORMATION_SCHEMA", "SCHEMATA", "PRIMARY_KEY",
                   "PRIMARY_KEY", "", true, false, Ns()},
                  {"", "INFORMATION_SCHEMA", "TABLES", "PRIMARY_KEY",
                   "PRIMARY_KEY", "", true, false, Ns()},
              }));
}

TEST_F(InformationSchemaTest, QueryIndexColumns) {
  // ORDINAL_POSITION is ignored because the test will break if production adds
  // additional columns.
  EXPECT_THAT(QueryWithParams(R"(
                select
                  t.table_schema,
                  t.table_name,
                  t.index_name,
                  t.index_type,
                  t.column_name,
                  t.column_ordering,
                  t.is_nullable,
                  t.spanner_type
                from
                  information_schema.index_columns as t
                where
                      t.table_schema = 'INFORMATION_SCHEMA'
                  and t.table_name not in unnest(@unsupported_tables)
              )",
                              BuildParamsForUnsupportedTables()),

              IsOkAndHoldsUnorderedRows({
                  {"INFORMATION_SCHEMA", "COLUMN_OPTIONS", "PRIMARY_KEY",
                   "PRIMARY_KEY", "TABLE_CATALOG", "ASC", "NO", "STRING(MAX)"},
                  {"INFORMATION_SCHEMA", "COLUMN_OPTIONS", "PRIMARY_KEY",
                   "PRIMARY_KEY", "TABLE_SCHEMA", "ASC", "NO", "STRING(MAX)"},
                  {"INFORMATION_SCHEMA", "COLUMN_OPTIONS", "PRIMARY_KEY",
                   "PRIMARY_KEY", "TABLE_NAME", "ASC", "NO", "STRING(MAX)"},
                  {"INFORMATION_SCHEMA", "COLUMN_OPTIONS", "PRIMARY_KEY",
                   "PRIMARY_KEY", "COLUMN_NAME", "ASC", "NO", "STRING(MAX)"},
                  {"INFORMATION_SCHEMA", "COLUMN_OPTIONS", "PRIMARY_KEY",
                   "PRIMARY_KEY", "OPTION_NAME", "ASC", "NO", "STRING(MAX)"},
                  {"INFORMATION_SCHEMA", "COLUMNS", "PRIMARY_KEY",
                   "PRIMARY_KEY", "TABLE_CATALOG", "ASC", "NO", "STRING(MAX)"},
                  {"INFORMATION_SCHEMA", "COLUMNS", "PRIMARY_KEY",
                   "PRIMARY_KEY", "TABLE_SCHEMA", "ASC", "NO", "STRING(MAX)"},
                  {"INFORMATION_SCHEMA", "COLUMNS", "PRIMARY_KEY",
                   "PRIMARY_KEY", "TABLE_NAME", "ASC", "NO", "STRING(MAX)"},
                  {"INFORMATION_SCHEMA", "COLUMNS", "PRIMARY_KEY",
                   "PRIMARY_KEY", "COLUMN_NAME", "ASC", "NO", "STRING(MAX)"},
                  {"INFORMATION_SCHEMA", "INDEX_COLUMNS", "PRIMARY_KEY",
                   "PRIMARY_KEY", "TABLE_CATALOG", "ASC", "NO", "STRING(MAX)"},
                  {"INFORMATION_SCHEMA", "INDEX_COLUMNS", "PRIMARY_KEY",
                   "PRIMARY_KEY", "TABLE_SCHEMA", "ASC", "NO", "STRING(MAX)"},
                  {"INFORMATION_SCHEMA", "INDEX_COLUMNS", "PRIMARY_KEY",
                   "PRIMARY_KEY", "TABLE_NAME", "ASC", "NO", "STRING(MAX)"},
                  {"INFORMATION_SCHEMA", "INDEX_COLUMNS", "PRIMARY_KEY",
                   "PRIMARY_KEY", "INDEX_NAME", "ASC", "NO", "STRING(MAX)"},
                  {"INFORMATION_SCHEMA", "INDEX_COLUMNS", "PRIMARY_KEY",
                   "PRIMARY_KEY", "COLUMN_NAME", "ASC", "NO", "STRING(MAX)"},
                  {"INFORMATION_SCHEMA", "INDEX_COLUMNS", "PRIMARY_KEY",
                   "PRIMARY_KEY", "INDEX_TYPE", "ASC", "NO", "STRING(MAX)"},
                  {"INFORMATION_SCHEMA", "INDEXES", "PRIMARY_KEY",
                   "PRIMARY_KEY", "TABLE_CATALOG", "ASC", "NO", "STRING(MAX)"},
                  {"INFORMATION_SCHEMA", "INDEXES", "PRIMARY_KEY",
                   "PRIMARY_KEY", "TABLE_SCHEMA", "ASC", "NO", "STRING(MAX)"},
                  {"INFORMATION_SCHEMA", "INDEXES", "PRIMARY_KEY",
                   "PRIMARY_KEY", "TABLE_NAME", "ASC", "NO", "STRING(MAX)"},
                  {"INFORMATION_SCHEMA", "INDEXES", "PRIMARY_KEY",
                   "PRIMARY_KEY", "INDEX_NAME", "ASC", "NO", "STRING(MAX)"},
                  {"INFORMATION_SCHEMA", "INDEXES", "PRIMARY_KEY",
                   "PRIMARY_KEY", "INDEX_TYPE", "ASC", "NO", "STRING(MAX)"},
                  {"INFORMATION_SCHEMA", "SCHEMATA", "PRIMARY_KEY",
                   "PRIMARY_KEY", "CATALOG_NAME", "ASC", "NO", "STRING(MAX)"},
                  {"INFORMATION_SCHEMA", "SCHEMATA", "PRIMARY_KEY",
                   "PRIMARY_KEY", "SCHEMA_NAME", "ASC", "NO", "STRING(MAX)"},
                  {"INFORMATION_SCHEMA", "TABLES", "PRIMARY_KEY", "PRIMARY_KEY",
                   "TABLE_CATALOG", "ASC", "NO", "STRING(MAX)"},
                  {"INFORMATION_SCHEMA", "TABLES", "PRIMARY_KEY", "PRIMARY_KEY",
                   "TABLE_SCHEMA", "ASC", "NO", "STRING(MAX)"},
                  {"INFORMATION_SCHEMA", "TABLES", "PRIMARY_KEY", "PRIMARY_KEY",
                   "TABLE_NAME", "ASC", "NO", "STRING(MAX)"},
              }));
}

TEST_F(InformationSchemaTest, QueriesTablesTable) {
  EXPECT_THAT(Query(R"(
                select
                  t.table_name,
                  t.parent_table_name,
                  t.on_delete_action
                from
                  information_schema.tables AS t
                where
                      t.table_catalog = ''
                  and t.table_schema = ''
                order by
                  t.table_catalog,
                  t.table_schema,
                  t.table_name
              )"),
              IsOkAndHoldsRows({{"Base", Ns(), Ns()},
                                {"CascadeChild", "Base", "CASCADE"},
                                {"NoActionChild", "Base", "NO ACTION"}}));
}

TEST_F(InformationSchemaTest, QueriesColumnsTable) {
  EXPECT_THAT(
      Query(R"(
                select
                  c.table_catalog,
                  c.table_schema,
                  c.table_name,
                  c.column_name,
                  c.ordinal_position,
                  c.column_default,
                  c.data_type,
                  c.is_nullable,
                  c.spanner_type
                from
                  information_schema.columns AS c
                where
                      c.table_catalog = ''
                  and c.table_schema = ''
                order by
                  c.table_catalog,
                  c.table_schema,
                  c.table_name,
                  c.ordinal_position
              )"),
      IsOkAndHoldsRows(
          {{"", "", "Base", "Key1", 1, Nb(), Ns(), "YES", "INT64"},
           {"", "", "Base", "Key2", 2, Nb(), Ns(), "YES", "STRING(256)"},
           {"", "", "Base", "BoolValue", 3, Nb(), Ns(), "YES", "BOOL"},
           {"", "", "Base", "IntValue", 4, Nb(), Ns(), "NO", "INT64"},
           {"", "", "Base", "DoubleValue", 5, Nb(), Ns(), "YES", "FLOAT64"},
           {"", "", "Base", "StrValue", 6, Nb(), Ns(), "YES", "STRING(MAX)"},
           {"", "", "Base", "ByteValue", 7, Nb(), Ns(), "YES", "BYTES(256)"},
           {"", "", "Base", "TimestampValue", 8, Nb(), Ns(), "YES",
            "TIMESTAMP"},
           {"", "", "Base", "DateValue", 9, Nb(), Ns(), "YES", "DATE"},
           {"", "", "Base", "BoolArray", 10, Nb(), Ns(), "NO", "ARRAY<BOOL>"},
           {"", "", "Base", "IntArray", 11, Nb(), Ns(), "YES", "ARRAY<INT64>"},
           {"", "", "Base", "DoubleArray", 12, Nb(), Ns(), "YES",
            "ARRAY<FLOAT64>"},
           {"", "", "Base", "StrArray", 13, Nb(), Ns(), "YES",
            "ARRAY<STRING(256)>"},
           {"", "", "Base", "ByteArray", 14, Nb(), Ns(), "YES",
            "ARRAY<BYTES(MAX)>"},
           {"", "", "Base", "TimestampArray", 15, Nb(), Ns(), "YES",
            "ARRAY<TIMESTAMP>"},
           {"", "", "Base", "DateArray", 16, Nb(), Ns(), "YES", "ARRAY<DATE>"},
           {"", "", "CascadeChild", "Key1", 1, Nb(), Ns(), "YES", "INT64"},
           {"", "", "CascadeChild", "Key2", 2, Nb(), Ns(), "YES",
            "STRING(256)"},
           {"", "", "CascadeChild", "ChildKey", 3, Nb(), Ns(), "YES", "BOOL"},
           {"", "", "CascadeChild", "Value1", 4, Nb(), Ns(), "NO",
            "STRING(MAX)"},
           {"", "", "CascadeChild", "Value2", 5, Nb(), Ns(), "YES", "BOOL"},
           {"", "", "NoActionChild", "Key1", 1, Nb(), Ns(), "YES", "INT64"},
           {"", "", "NoActionChild", "Key2", 2, Nb(), Ns(), "YES",
            "STRING(256)"},
           {"", "", "NoActionChild", "ChildKey", 3, Nb(), Ns(), "YES", "BOOL"},
           {"", "", "NoActionChild", "Value", 4, Nb(), Ns(), "YES",
            "STRING(MAX)"}}));
}

TEST_F(InformationSchemaTest, QueriesIndexesTable) {
  EXPECT_THAT(
      Query(R"(
                select
                  i.table_catalog,
                  i.table_schema,
                  i.table_name,
                  i.index_name,
                  i.index_type,
                  i.parent_table_name,
                  i.is_unique,
                  i.is_null_filtered,
                  i.index_state
                from
                  information_schema.indexes AS i
                where
                      i.table_catalog = ''
                  and i.table_schema = ''
                order by
                  i.table_catalog,
                  i.table_schema,
                  i.table_name,
                  i.index_name
              )"),
      IsOkAndHoldsRows({{"", "", "Base", "PRIMARY_KEY", "PRIMARY_KEY", "", true,
                         false, Ns()},
                        {"", "", "CascadeChild", "CascadeChildByValue", "INDEX",
                         "Base", true, true, "READ_WRITE"},
                        {"", "", "CascadeChild", "PRIMARY_KEY", "PRIMARY_KEY",
                         "", true, false, Ns()},
                        {"", "", "NoActionChild", "NoActionChildByValue",
                         "INDEX", "", false, false, "READ_WRITE"},
                        {"", "", "NoActionChild", "PRIMARY_KEY", "PRIMARY_KEY",
                         "", true, false, Ns()}}));
}

TEST_F(InformationSchemaTest, QueriesIndexColumnsTable) {
  EXPECT_THAT(
      Query(R"(
                select
                  ic.table_catalog,
                  ic.table_schema,
                  ic.table_name,
                  ic.index_name,
                  ic.column_name,
                  ic.ordinal_position,
                  ic.column_ordering,
                  ic.is_nullable,
                  ic.spanner_type
                from
                  information_schema.index_columns AS ic
                where
                      ic.table_catalog = ''
                  and ic.table_schema = ''
                order by
                  ic.table_catalog,
                  ic.table_schema,
                  ic.table_name,
                  ic.index_name,
                  ic.ordinal_position
              )"),
      IsOkAndHoldsRows({
          {"", "", "Base", "PRIMARY_KEY", "Key1", 1, "ASC", "YES", "INT64"},
          {"", "", "Base", "PRIMARY_KEY", "Key2", 2, "DESC", "YES",
           "STRING(256)"},
          {"", "", "CascadeChild", "CascadeChildByValue", "Value1",
           Null<std::int64_t>(), Ns(), "NO", "STRING(MAX)"},
          {"", "", "CascadeChild", "CascadeChildByValue", "Key1", 1, "ASC",
           "NO", "INT64"},
          {"", "", "CascadeChild", "CascadeChildByValue", "Key2", 2, "DESC",
           "NO", "STRING(256)"},
          {"", "", "CascadeChild", "CascadeChildByValue", "Value2", 3, "ASC",
           "NO", "BOOL"},
          {"", "", "CascadeChild", "PRIMARY_KEY", "Key1", 1, "ASC", "YES",
           "INT64"},
          {"", "", "CascadeChild", "PRIMARY_KEY", "Key2", 2, "DESC", "YES",
           "STRING(256)"},
          {"", "", "CascadeChild", "PRIMARY_KEY", "ChildKey", 3, "ASC", "YES",
           "BOOL"},
          {"", "", "NoActionChild", "NoActionChildByValue", "Value", 1, "ASC",
           "YES", "STRING(MAX)"},
          {"", "", "NoActionChild", "PRIMARY_KEY", "Key1", 1, "ASC", "YES",
           "INT64"},
          {"", "", "NoActionChild", "PRIMARY_KEY", "Key2", 2, "DESC", "YES",
           "STRING(256)"},
          {"", "", "NoActionChild", "PRIMARY_KEY", "ChildKey", 3, "ASC", "YES",
           "BOOL"},
      }));
}

TEST_F(InformationSchemaTest, QueriesColumnOptionsTable) {
  EXPECT_THAT(Query(R"(
                select
                  co.table_catalog,
                  co.table_schema,
                  co.table_name,
                  co.column_name,
                  co.option_name,
                  co.option_type,
                  co.option_value
                from
                  information_schema.column_options AS co
                where
                      co.table_catalog = ''
                  and co.table_schema = ''
                order by
                  co.table_catalog,
                  co.table_schema,
                  co.table_name,
                  co.column_name,
                  co.option_name
              )"),
              IsOkAndHoldsRows({
                  {"", "", "Base", "TimestampValue", "allow_commit_timestamp",
                   "BOOL", "TRUE"},
              }));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
