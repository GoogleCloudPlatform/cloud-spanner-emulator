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

class InformationSchemaTest : public DatabaseTest {
 public:
  zetasql_base::Status SetUpDatabase() override {
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

TEST_F(InformationSchemaTest, QueriesTablesTable) {
  EXPECT_THAT(
      Query(R"(
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
      IsOkAndHoldsRows({{"Base", Null<std::string>(), Null<std::string>()},
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
          {{"", "", "Base", "Key1", 1, "YES", "INT64"},
           {"", "", "Base", "Key2", 2, "YES", "STRING(256)"},
           {"", "", "Base", "BoolValue", 3, "YES", "BOOL"},
           {"", "", "Base", "IntValue", 4, "NO", "INT64"},
           {"", "", "Base", "DoubleValue", 5, "YES", "FLOAT64"},
           {"", "", "Base", "StrValue", 6, "YES", "STRING(MAX)"},
           {"", "", "Base", "ByteValue", 7, "YES", "BYTES(256)"},
           {"", "", "Base", "TimestampValue", 8, "YES", "TIMESTAMP"},
           {"", "", "Base", "DateValue", 9, "YES", "DATE"},
           {"", "", "Base", "BoolArray", 10, "NO", "ARRAY<BOOL>"},
           {"", "", "Base", "IntArray", 11, "YES", "ARRAY<INT64>"},
           {"", "", "Base", "DoubleArray", 12, "YES", "ARRAY<FLOAT64>"},
           {"", "", "Base", "StrArray", 13, "YES", "ARRAY<STRING(256)>"},
           {"", "", "Base", "ByteArray", 14, "YES", "ARRAY<BYTES(MAX)>"},
           {"", "", "Base", "TimestampArray", 15, "YES", "ARRAY<TIMESTAMP>"},
           {"", "", "Base", "DateArray", 16, "YES", "ARRAY<DATE>"},
           {"", "", "CascadeChild", "Key1", 1, "YES", "INT64"},
           {"", "", "CascadeChild", "Key2", 2, "YES", "STRING(256)"},
           {"", "", "CascadeChild", "ChildKey", 3, "YES", "BOOL"},
           {"", "", "CascadeChild", "Value1", 4, "NO", "STRING(MAX)"},
           {"", "", "CascadeChild", "Value2", 5, "YES", "BOOL"},
           {"", "", "NoActionChild", "Key1", 1, "YES", "INT64"},
           {"", "", "NoActionChild", "Key2", 2, "YES", "STRING(256)"},
           {"", "", "NoActionChild", "ChildKey", 3, "YES", "BOOL"},
           {"", "", "NoActionChild", "Value", 4, "YES", "STRING(MAX)"}}));
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
                         false, Null<std::string>()},
                        {"", "", "CascadeChild", "CascadeChildByValue", "INDEX",
                         "Base", true, true, "READ_WRITE"},
                        {"", "", "CascadeChild", "PRIMARY_KEY", "PRIMARY_KEY",
                         "", true, false, Null<std::string>()},
                        {"", "", "NoActionChild", "NoActionChildByValue",
                         "INDEX", "", false, false, "READ_WRITE"},
                        {"", "", "NoActionChild", "PRIMARY_KEY", "PRIMARY_KEY",
                         "", true, false, Null<std::string>()}}));
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
           Null<std::int64_t>(), Null<std::string>(), "NO", "STRING(MAX)"},
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
