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

#include "backend/schema/catalog/change_stream.h"

#include <algorithm>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/time/clock.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/updater/schema_updater_tests/base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {

namespace {

TEST_P(SchemaUpdaterTest, CreateChangeStream) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
        c1 STRING(100),
      ) PRIMARY KEY (k1)
    )",
                                                  R"(
    CREATE CHANGE STREAM C FOR ALL)"}));
  const ChangeStream* change_stream = schema->FindChangeStream("C");
  EXPECT_NE(change_stream, nullptr);
  EXPECT_EQ(schema->FindTable("T")->FindChangeStream(change_stream->Name()),
            change_stream);
  EXPECT_EQ(schema->FindTable("T")->FindColumn("k1")->FindChangeStream(
                change_stream->Name()),
            nullptr);
  EXPECT_EQ(schema->FindTable("T")->FindColumn("c1")->FindChangeStream(
                change_stream->Name()),
            change_stream);

  const Table* change_stream_partition_table =
      change_stream->change_stream_partition_table();
  EXPECT_NE(change_stream_partition_table, nullptr);
  EXPECT_NE(change_stream_partition_table->FindColumn("partition_token"),
            nullptr);
  EXPECT_NE(change_stream_partition_table->FindColumn("start_time"), nullptr);
  EXPECT_NE(change_stream_partition_table->FindColumn("end_time"), nullptr);
  EXPECT_NE(change_stream_partition_table->FindColumn("parents"), nullptr);
  EXPECT_NE(change_stream_partition_table->FindColumn("children"), nullptr);

  auto t = schema->FindChangeStream("C");
  EXPECT_NE(t, nullptr);
  EXPECT_EQ(t->tvf_name(), "READ_C");
  ASSERT_TRUE((t->creation_time() < absl::Now()));
  EXPECT_EQ(t->parsed_retention_period(), 60 * 60 * 24);

  const Table* change_stream_data_table =
      change_stream->change_stream_data_table();
  EXPECT_NE(change_stream_data_table, nullptr);
  EXPECT_NE(change_stream_data_table->FindColumn("partition_token"), nullptr);
  EXPECT_NE(change_stream_data_table->FindColumn("commit_timestamp"), nullptr);
  EXPECT_NE(change_stream_data_table->FindColumn("record_sequence"), nullptr);
  EXPECT_NE(change_stream_data_table->FindColumn("server_transaction_id"),
            nullptr);
  EXPECT_NE(change_stream_data_table->FindColumn(
                "is_last_record_in_transaction_in_partition"),
            nullptr);
  EXPECT_NE(change_stream_data_table->FindColumn("table_name"), nullptr);
  EXPECT_NE(change_stream_data_table->FindColumn("column_types"), nullptr);
  EXPECT_NE(change_stream_data_table->FindColumn("mods"), nullptr);
  EXPECT_NE(change_stream_data_table->FindColumn("mod_type"), nullptr);
  EXPECT_NE(change_stream_data_table->FindColumn("value_capture_type"),
            nullptr);
  EXPECT_NE(
      change_stream_data_table->FindColumn("number_of_records_in_transaction"),
      nullptr);
  EXPECT_NE(change_stream_data_table->FindColumn(
                "number_of_partitions_in_transaction"),
            nullptr);
  EXPECT_NE(change_stream_data_table->FindColumn("transaction_tag"), nullptr);
  EXPECT_NE(change_stream_data_table->FindColumn("is_system_transaction"),
            nullptr);
}

TEST_P(SchemaUpdaterTest, CreateChangeStream_TrackInterleavedTables) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema({R"(
          CREATE TABLE Users(
            UserId     INT64 NOT NULL,
            Name       STRING(MAX),
            Age        INT64,
          ) PRIMARY KEY (UserId)
        )",
                                     R"(
          CREATE TABLE Threads (
            UserId     INT64 NOT NULL,
            ThreadId   INT64 NOT NULL,
            Starred    BOOL,
          ) PRIMARY KEY (UserId, ThreadId),
          INTERLEAVE IN PARENT Users ON DELETE CASCADE
        )",
                                     R"(CREATE CHANGE STREAM C FOR ALL)"}));

  const Table* parent = schema->FindTable("Users");
  const Table* child = schema->FindTable("Threads");
  EXPECT_EQ(parent->change_streams().size(), 1);
  EXPECT_EQ(child->change_streams().size(), 1);
  EXPECT_EQ(parent->FindColumn("UserId")->change_streams().size(), 0);
  EXPECT_EQ(child->FindColumn("UserId")->change_streams().size(), 0);
  EXPECT_EQ(child->FindColumn("ThreadId")->change_streams().size(), 0);
  EXPECT_EQ(parent->FindColumn("Name")->change_streams().size(), 1);
  EXPECT_EQ(parent->FindColumn("Age")->change_streams().size(), 1);
  EXPECT_EQ(child->FindColumn("Starred")->change_streams().size(), 1);
}

TEST_P(SchemaUpdaterTest, CreateChangeStream_AlreadyExists) {
  EXPECT_THAT(CreateSchema({
                  R"(
      CREATE CHANGE STREAM C FOR ALL)",
                  R"(
      CREATE CHANGE STREAM C FOR ALL)"}),
              StatusIs(error::SchemaObjectAlreadyExists("Change Stream", "C")));
}

TEST_P(SchemaUpdaterTest, CreateChangeStream_ObjectsTrackedSuccessfully) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
        c1 STRING(100),
      ) PRIMARY KEY (k1)
    )",
                                     R"(CREATE CHANGE STREAM C FOR T)"}));
  absl::flat_hash_map<std::string, std::vector<std::string>>
      tracked_tables_columns =
          schema->FindChangeStream("C")->tracked_tables_columns();
  auto it = tracked_tables_columns.find("T");
  EXPECT_NE(it, tracked_tables_columns.end());
  std::vector<std::string> tracked_columns = tracked_tables_columns["T"];
  EXPECT_THAT(tracked_columns, testing::UnorderedElementsAre("k1", "c1"));
  ASSERT_TRUE(schema->FindTable("T")->FindChangeStream("C"));
  ASSERT_FALSE(schema->FindTable("T")->FindColumn("k1")->FindChangeStream("C"));
  ASSERT_TRUE(schema->FindTable("T")->FindColumn("c1")->FindChangeStream("C"));
}

TEST_P(SchemaUpdaterTest, CreateChangeStream_ExplicitlyTrackColumns) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
              CREATE TABLE test_table (
                int64_col INT64 NOT NULL,
                string_col STRING(MAX)
              ) PRIMARY KEY (int64_col)
            )",
                                                  R"(
              CREATE CHANGE STREAM change_stream_test_table FOR test_table(string_col)
            )"}));
  const ChangeStream* change_stream =
      schema->FindChangeStream("change_stream_test_table");
  EXPECT_EQ(change_stream->tracked_tables_columns()["test_table"].size(), 1);
  ASSERT_TRUE(
      schema->FindTable("test_table")->FindChangeStream(change_stream->Name()));
  ASSERT_TRUE(schema->FindTable("test_table")
                  ->FindColumn("string_col")
                  ->FindChangeStream(change_stream->Name()));
  ASSERT_FALSE(schema->FindTable("test_table")
                   ->FindColumn("int64_col")
                   ->FindChangeStream(change_stream->Name()));
}

TEST_P(SchemaUpdaterTest,
       CreateChangeStream_TrackOnlyPKWithOverLimitChangeStreams) {
  EXPECT_THAT(CreateSchema({R"(
              CREATE TABLE test_table (
                int64_col INT64 NOT NULL,
                string_col STRING(MAX)
              ) PRIMARY KEY (int64_col)
            )",
                            R"(
              CREATE CHANGE STREAM c1 FOR test_table()
            )",
                            R"(CREATE CHANGE STREAM c2 FOR test_table())",
                            R"(CREATE CHANGE STREAM c3 FOR test_table())",
                            R"(CREATE CHANGE STREAM c4 FOR test_table())"}),
              StatusIs(error::TooManyChangeStreamsTrackingSameObject(
                  "c4", 3, "test_table")));
}

TEST_P(SchemaUpdaterTest,
       CreateChangeStream_TrackNoObjectsOverLimitChangeStreams) {
  EXPECT_THAT(CreateSchema({R"(
              CREATE TABLE test_table (
                int64_col INT64 NOT NULL,
                string_col STRING(MAX)
              ) PRIMARY KEY (int64_col)
            )",
                            R"(
              CREATE CHANGE STREAM c1
            )",
                            R"(CREATE CHANGE STREAM c2 FOR test_table)",
                            R"(CREATE CHANGE STREAM c3 FOR test_table)",
                            R"(CREATE CHANGE STREAM c4 FOR test_table)",
                            R"(CREATE CHANGE STREAM c5 FOR test_table)"}),
              StatusIs(error::TooManyChangeStreamsTrackingSameObject(
                  "c5", 3, "test_table")));
}

TEST_P(SchemaUpdaterTest, AlterChangeStreamSetOptions_TrackSameObjects) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto schema,
      CreateSchema(
          {R"(
      CREATE TABLE T (
        k1 INT64,
        c1 STRING(100),
      ) PRIMARY KEY (k1)
    )",
           R"(CREATE CHANGE STREAM C FOR T)",
           R"(ALTER CHANGE STREAM C SET OPTIONS ( retention_period = '36h' ))"}));
  absl::flat_hash_map<std::string, std::vector<std::string>>
      tracked_tables_columns =
          schema->FindChangeStream("C")->tracked_tables_columns();
  auto it = tracked_tables_columns.find("T");
  EXPECT_NE(it, tracked_tables_columns.end());
  std::vector<std::string> tracked_columns =
      schema->FindChangeStream("C")->tracked_tables_columns()["T"];
  auto itr = std::find(tracked_columns.begin(), tracked_columns.end(), "c1");
  EXPECT_NE(itr, tracked_columns.end());
}

TEST_P(SchemaUpdaterTest, AlterChangeStreamSetForClause_TrackDifferentObjects) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
        c1 STRING(100),
      ) PRIMARY KEY (k1)
    )",
                                     R"(
      CREATE TABLE T1 (
        k1 INT64,
        c1 STRING(100),
      ) PRIMARY KEY (k1)
    )",
                                     R"(CREATE CHANGE STREAM C FOR T)",
                                     R"(ALTER CHANGE STREAM C SET FOR T1)"}));
  absl::flat_hash_map<std::string, std::vector<std::string>>
      tracked_tables_columns =
          schema->FindChangeStream("C")->tracked_tables_columns();
  auto it = tracked_tables_columns.find("T");
  EXPECT_EQ(it, tracked_tables_columns.end());
  it = tracked_tables_columns.find("T1");
  EXPECT_NE(it, tracked_tables_columns.end());
  ASSERT_TRUE(schema->FindTable("T1")->FindChangeStream("C"));
  ASSERT_FALSE(schema->FindTable("T")->FindChangeStream("C"));
}

TEST_P(SchemaUpdaterTest, AlterChangeStreamSetForClause_TrackOnlyPK) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
              CREATE TABLE test_table (
                int64_col INT64 NOT NULL,
                string_col STRING(MAX),
                string_col2 STRING(MAX)
              ) PRIMARY KEY (int64_col)
            )",
                                                  R"(
              CREATE CHANGE STREAM change_stream_test_table FOR test_table
            )",
                                                  R"(
              ALTER CHANGE STREAM change_stream_test_table SET FOR test_table()
            )"}));
  const Table* table = schema->FindTable("test_table");
  const Column* pk_col = table->FindColumn("int64_col");
  const Column* col1 = table->FindColumn("string_col");
  const Column* col2 = table->FindColumn("string_col2");
  EXPECT_NE(col2, nullptr);
  EXPECT_EQ(col1->change_streams().size(), 0);
  EXPECT_EQ(col2->change_streams().size(), 0);
  EXPECT_EQ(pk_col->change_streams().size(), 0);
  EXPECT_EQ(table->change_streams().size(), 1);
  const ChangeStream* change_stream =
      schema->FindChangeStream("change_stream_test_table");
  ASSERT_TRUE(!col1->FindChangeStream(change_stream->Name()));
  ASSERT_TRUE(!col2->FindChangeStream(change_stream->Name()));
  ASSERT_TRUE(table->FindChangeStream(change_stream->Name()));
}

TEST_P(SchemaUpdaterTest, AlterChangeStreamDropForClause) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
        c1 STRING(100),
      ) PRIMARY KEY (k1)
    )",
                                     R"(CREATE CHANGE STREAM C FOR T)",
                                     R"(ALTER CHANGE STREAM C DROP FOR ALL)"}));
  const Table* table = schema->FindTable("T");
  EXPECT_EQ(table->FindChangeStream("C"), nullptr);
  EXPECT_EQ(table->change_streams().size(), 0);
  EXPECT_EQ(schema->FindChangeStream("C")->tracked_tables_columns().size(), 0);
  ASSERT_FALSE(table->FindColumn("k1")->FindChangeStream("C"));
  ASSERT_FALSE(table->FindColumn("c1")->FindChangeStream("C"));
}

TEST_P(SchemaUpdaterTest, SetOptions_RetentionPeriod) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"(
      CREATE TABLE T (
        k1 INT64,
        c1 STRING(100),
      ) PRIMARY KEY (k1)
    )",
                                        R"(
      CREATE CHANGE STREAM C FOR ALL OPTIONS ( retention_period = '36h' ))"}));
  EXPECT_EQ(schema->FindChangeStream("C")->retention_period(), "36h");
  EXPECT_EQ(schema->FindChangeStream("C")->parsed_retention_period(),
            36 * 60 * 60);
  EXPECT_THAT(
      UpdateSchema(
          schema.get(),
          {R"(ALTER CHANGE STREAM C SET OPTIONS ( retention_period = '360h'))"}),
      StatusIs(error::InvalidDataRetentionPeriod("360h")));
  EXPECT_THAT(
      UpdateSchema(
          schema.get(),
          {R"(ALTER CHANGE STREAM C SET OPTIONS ( retention_period = '360'))"}),
      StatusIs(error::InvalidTimeDurationFormat("360")));
}

TEST_P(SchemaUpdaterTest, SetOptions_ValueCaptureType) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto schema,
      CreateSchema({
          R"(
      CREATE TABLE T (
        k1 INT64,
        c1 STRING(100),
      ) PRIMARY KEY (k1)
    )",
          R"(
      CREATE CHANGE STREAM C FOR ALL OPTIONS ( value_capture_type = 'NEW_ROW' ))",
          R"(ALTER CHANGE STREAM C SET OPTIONS ( value_capture_type = 'NEW_VALUES' ))"}));
  EXPECT_EQ(schema->FindChangeStream("C")->value_capture_type(), "NEW_VALUES");
  EXPECT_THAT(
      UpdateSchema(
          schema.get(),
          {R"(ALTER CHANGE STREAM C SET OPTIONS ( value_capture_type = 'OLD_VALUES'))"}),
      StatusIs(error::InvalidValueCaptureType("OLD_VALUES")));
}

TEST_P(SchemaUpdaterTest, DropChangeStream) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
              CREATE TABLE test_table (
                int64_col INT64 NOT NULL
              ) PRIMARY KEY (int64_col)
            )",
                                                  R"(
              CREATE CHANGE STREAM C FOR ALL
            )"}));
  EXPECT_NE(schema->FindChangeStream("C"), nullptr);
  auto table = schema->FindTable("test_table");
  EXPECT_EQ(table->change_streams().size(), 1);
  ASSERT_TRUE(table->FindChangeStream("C"));
  ASSERT_FALSE(table->FindColumn("int64_col")->FindChangeStream("C"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema,
                       UpdateSchema(schema.get(), {R"(DROP CHANGE STREAM C)"}));
  EXPECT_EQ(new_schema->FindTable("_change_stream_data_C"), nullptr);
  EXPECT_EQ(new_schema->FindTable("_change_stream_partition_C"), nullptr);
  auto new_table = new_schema->FindTable("test_table");
  ASSERT_FALSE(new_table->FindChangeStream("C"));
  EXPECT_EQ(new_table->change_streams().size(), 0);
  ASSERT_FALSE(new_table->FindColumn("int64_col")->FindChangeStream("C"));
  EXPECT_EQ(new_table->FindColumn("int64_col")->change_streams().size(), 0);
}

TEST_P(SchemaUpdaterTest, DropAndRecreateChangeStream) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
        c1 STRING(100),
      ) PRIMARY KEY (k1)
    )",
                                                  R"(
    CREATE CHANGE STREAM C FOR ALL)"}));
  const ChangeStream* change_stream_c = schema->FindChangeStream("C");
  const Table* change_stream_data_table_c =
      change_stream_c->change_stream_data_table();
  std::string change_stream_data_table_id_0 = change_stream_data_table_c->id();
  EXPECT_NE(change_stream_data_table_c, nullptr);
  const Table* change_stream_partition_table_c =
      change_stream_c->change_stream_partition_table();
  std::string change_stream_partition_table_id_0 =
      change_stream_partition_table_c->id();
  EXPECT_NE(change_stream_partition_table_c, nullptr);
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema_1,
                       UpdateSchema(schema.get(), {R"(DROP CHANGE STREAM C)"}));
  EXPECT_EQ(new_schema_1->FindTable("_change_stream_data_C"), nullptr);
  EXPECT_EQ(new_schema_1->FindTable("_change_stream_partition_C"), nullptr);
  ASSERT_FALSE(new_schema_1->FindTable("T")->FindChangeStream("C"));
  ASSERT_FALSE(
      new_schema_1->FindTable("T")->FindColumn("k1")->FindChangeStream("C"));
  ASSERT_FALSE(
      new_schema_1->FindTable("T")->FindColumn("c1")->FindChangeStream("C"));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto new_schema_2,
      UpdateSchema(new_schema_1.get(), {R"(CREATE CHANGE STREAM C FOR ALL)"}));
  change_stream_c = new_schema_2->FindChangeStream("C");
  EXPECT_NE(change_stream_c, nullptr);
  change_stream_data_table_c = change_stream_c->change_stream_data_table();
  EXPECT_NE(change_stream_data_table_c, nullptr);
  EXPECT_NE(change_stream_data_table_c->id(), change_stream_data_table_id_0);
  change_stream_partition_table_c =
      change_stream_c->change_stream_partition_table();
  EXPECT_NE(change_stream_partition_table_c, nullptr);
  EXPECT_NE(change_stream_partition_table_c->id(),
            change_stream_partition_table_id_0);
  ASSERT_TRUE(new_schema_2->FindTable("T")->FindChangeStream("C"));
  ASSERT_FALSE(
      new_schema_2->FindTable("T")->FindColumn("k1")->FindChangeStream("C"));
  ASSERT_TRUE(
      new_schema_2->FindTable("T")->FindColumn("c1")->FindChangeStream("C"));
}

TEST_P(SchemaUpdaterTest, CreateChangeStreamTrackAll_TracksNewlyAddedColumns) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
              CREATE TABLE test_table (
                int64_col INT64 NOT NULL,
                string_col STRING(MAX)
              ) PRIMARY KEY (int64_col)
            )",
                                                  R"(
              CREATE CHANGE STREAM change_stream_test_table FOR ALL
            )",
                                                  R"(
              ALTER TABLE test_table ADD COLUMN string_col2 STRING(MAX)
            )"}));

  const Table* table = schema->FindTable("test_table");
  const Column* col1 = table->FindColumn("string_col");
  const Column* col2 = table->FindColumn("string_col2");
  EXPECT_NE(col2, nullptr);
  EXPECT_EQ(col1->change_streams().size(), 1);
  EXPECT_EQ(col2->change_streams().size(), 1);
  const ChangeStream* change_stream =
      schema->FindChangeStream("change_stream_test_table");
  std::vector<std::string> column_list =
      change_stream->tracked_tables_columns()["test_table"];
  auto itr = std::find(column_list.begin(), column_list.end(), "string_col");
  EXPECT_NE(itr, column_list.end());
  ASSERT_TRUE(schema->FindTable("test_table")
                  ->FindColumn("string_col2")
                  ->FindChangeStream("change_stream_test_table"));
}

TEST_P(SchemaUpdaterTest,
       ChangeStreamTrackEntireTable_TracksNewlyAddedColumns) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
              CREATE TABLE test_table (
                int64_col INT64 NOT NULL,
                string_col STRING(MAX)
              ) PRIMARY KEY (int64_col)
            )",
                                                  R"(
              CREATE CHANGE STREAM change_stream_test_table FOR test_table
            )",
                                                  R"(
              ALTER TABLE test_table ADD COLUMN string_col2 STRING(MAX)
            )"}));

  const Table* table = schema->FindTable("test_table");
  const Column* col1 = table->FindColumn("string_col");
  const Column* col2 = table->FindColumn("string_col2");
  EXPECT_NE(col2, nullptr);
  EXPECT_EQ(col1->change_streams().size(), 1);
  EXPECT_EQ(col2->change_streams().size(), 1);
  const ChangeStream* change_stream =
      schema->FindChangeStream("change_stream_test_table");
  std::vector<std::string> column_list =
      change_stream->tracked_tables_columns()["test_table"];
  auto itr = std::find(column_list.begin(), column_list.end(), "string_col2");
  EXPECT_NE(itr, column_list.end());
  ASSERT_TRUE(schema->FindTable("test_table")
                  ->FindColumn("string_col2")
                  ->FindChangeStream("change_stream_test_table"));
}

TEST_P(SchemaUpdaterTest, ChangeStreamTrackAll_TracksNewlyAddedTable) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
              CREATE TABLE test_table (
                int64_col INT64 NOT NULL,
                string_col STRING(MAX)
              ) PRIMARY KEY (int64_col)
            )",
                                                  R"(
              CREATE CHANGE STREAM change_stream_test_table FOR ALL
            )",
                                                  R"(
              CREATE TABLE test_table_2 (
                int64_col INT64 NOT NULL,
                string_col STRING(MAX)
              ) PRIMARY KEY (int64_col)
            )"}));

  const Table* table = schema->FindTable("test_table_2");
  const Column* pk_col = table->FindColumn("int64_col");
  const Column* str_col = table->FindColumn("string_col");
  EXPECT_NE(table, nullptr);
  EXPECT_NE(pk_col, nullptr);
  EXPECT_NE(str_col, nullptr);
  EXPECT_EQ(table->change_streams().size(), 1);
  EXPECT_EQ(pk_col->change_streams().size(), 0);
  EXPECT_EQ(str_col->change_streams().size(), 1);
  const ChangeStream* change_stream =
      schema->FindChangeStream("change_stream_test_table");
  std::vector<std::string> column_list =
      change_stream->tracked_tables_columns()["test_table_2"];
  auto itr = std::find(column_list.begin(), column_list.end(), str_col->Name());
  EXPECT_NE(itr, column_list.end());
  ASSERT_TRUE(schema->FindTable("test_table_2")
                  ->FindChangeStream("change_stream_test_table"));
}

}  // namespace

}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
