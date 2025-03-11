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

#include "backend/schema/catalog/locality_group.h"

#include <memory>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/updater/schema_updater_tests/base.h"
#include "common/errors.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {
namespace {
using database_api::DatabaseDialect::POSTGRESQL;

TEST_P(SchemaUpdaterTest, CreateLocalityGroupBasic) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema({R"( CREATE LOCALITY GROUP lg )"}));
  const LocalityGroup* locality_group = schema->FindLocalityGroup("lg");
  ASSERT_NE(locality_group, nullptr);
  EXPECT_EQ(locality_group->Name(), "lg");
}

TEST_P(SchemaUpdaterTest, CreateLocalityGroupIfNotExists) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema({R"( CREATE LOCALITY GROUP lg )"}));
  ZETASQL_EXPECT_OK(UpdateSchema(schema.get(),
                         {R"( CREATE LOCALITY GROUP IF NOT EXISTS lg )"}));
}

TEST_P(SchemaUpdaterTest, CreateLocalityGroupWithInvalidName) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  EXPECT_THAT(CreateSchema({R"(
      CREATE LOCALITY GROUP _test
      OPTIONS (storage = 'ssd', ssd_to_hdd_spill_timespan = '10m')
    )"}),
              StatusIs(error::InvalidLocalityGroupName("_test")));
}

TEST_P(SchemaUpdaterTest, CreateDefaultLocalityGroup) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  EXPECT_THAT(CreateSchema({R"(
      CREATE LOCALITY GROUP default
      OPTIONS (storage = 'ssd', ssd_to_hdd_spill_timespan = '10m')
    )"}),
              StatusIs(error::CreatingDefaultLocalityGroup()));
}

TEST_P(SchemaUpdaterTest, CreateLocalityGroupWithOptions) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE LOCALITY GROUP lg
      OPTIONS (storage = 'ssd', ssd_to_hdd_spill_timespan = '10m')
    )"}));
  const LocalityGroup* locality_group = schema->FindLocalityGroup("lg");
  EXPECT_NE(locality_group, nullptr);
  EXPECT_EQ(locality_group->Name(), "lg");
  EXPECT_TRUE(locality_group->inflash());
  for (const auto& timespan : locality_group->ssd_to_hdd_spill_timespans()) {
    EXPECT_EQ(timespan, "disk:10m");
  }
}

TEST_P(SchemaUpdaterTest, CreateLocalityGroupWithInvalidOptions) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  EXPECT_THAT(CreateSchema({R"(
      CREATE LOCALITY GROUP lg
      OPTIONS (storage = 'ddd', ssd_to_hdd_spill_timespan = '10m')
    )"}),
              ::zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  testing::HasSubstr("Unexpected value for option: storage.")));
  EXPECT_THAT(CreateSchema({R"(
      CREATE LOCALITY GROUP lg
      OPTIONS (storage = 'ssd', ssd_to_hdd_spill_timespan = '10')
    )"}),
              ::zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  testing::HasSubstr("Cannot parse 10 as a valid timestamp")));

  EXPECT_THAT(CreateSchema({R"(
      CREATE LOCALITY GROUP lg OPTIONS (type = 'ssd')
    )"}),
              ::zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  testing::HasSubstr("Option: type is unknown.")));
}

TEST_P(SchemaUpdaterTest, AlterLocalityGroupBasic) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE LOCALITY GROUP lg
      OPTIONS (storage = 'ssd', ssd_to_hdd_spill_timespan = '10m')
    )"}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto updated_schema, UpdateSchema(schema.get(), {R"(
      ALTER LOCALITY GROUP lg SET OPTIONS (storage = 'hdd', ssd_to_hdd_spill_timespan = '1h')
    )"}));
  const LocalityGroup* locality_group = updated_schema->FindLocalityGroup("lg");
  EXPECT_NE(locality_group, nullptr);
  EXPECT_EQ(locality_group->Name(), "lg");
  EXPECT_FALSE(locality_group->inflash().value());
  for (const auto& timespan : locality_group->ssd_to_hdd_spill_timespans()) {
    EXPECT_EQ(timespan, "disk:1h");
  }
}

TEST_P(SchemaUpdaterTest, AlterDefaultLocalityGroup) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      ALTER LOCALITY GROUP default
      SET OPTIONS (storage = 'ssd', ssd_to_hdd_spill_timespan = '10m')
    )"}));
  const LocalityGroup* locality_group = schema->FindLocalityGroup("default");
  EXPECT_NE(locality_group, nullptr);
  EXPECT_EQ(locality_group->Name(), "default");
  EXPECT_TRUE(locality_group->inflash().value());
  for (const auto& timespan : locality_group->ssd_to_hdd_spill_timespans()) {
    EXPECT_EQ(timespan, "disk:10m");
  }
  // Update the default locality group again.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto updated_schema, UpdateSchema(schema.get(), {R"(
      ALTER LOCALITY GROUP default SET OPTIONS (storage = 'hdd', ssd_to_hdd_spill_timespan = '1h')
    )"}));
  const LocalityGroup* updated_locality_group =
      updated_schema->FindLocalityGroup("default");
  EXPECT_NE(updated_locality_group, nullptr);
  EXPECT_EQ(updated_locality_group->Name(), "default");
  EXPECT_FALSE(updated_locality_group->inflash().value());
  for (const auto& timespan :
       updated_locality_group->ssd_to_hdd_spill_timespans()) {
    EXPECT_EQ(timespan, "disk:1h");
  }
}

TEST_P(SchemaUpdaterTest, AlterLocalityGroupWithIF_EXISTS) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE LOCALITY GROUP lg
      OPTIONS (storage = 'ssd', ssd_to_hdd_spill_timespan = '10m')
    )"}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto updated_schema, UpdateSchema(schema.get(), {R"(
      ALTER LOCALITY GROUP IF EXISTS alter_lg SET OPTIONS (storage = 'hdd', ssd_to_hdd_spill_timespan = '1h')
    )"}));
  const LocalityGroup* locality_group = updated_schema->FindLocalityGroup("lg");
  EXPECT_NE(locality_group, nullptr);
  EXPECT_EQ(locality_group->Name(), "lg");
  const LocalityGroup* alter_locality_group =
      updated_schema->FindLocalityGroup("alter_lg");
  EXPECT_EQ(alter_locality_group, nullptr);
}

TEST_P(SchemaUpdaterTest, AlterLocalityGroupNotFound) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE LOCALITY GROUP lg
      OPTIONS (storage = 'ssd', ssd_to_hdd_spill_timespan = '10m')
    )"}));
  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      ALTER LOCALITY GROUP alter_lg SET OPTIONS (storage = 'hdd', ssd_to_hdd_spill_timespan = '1h')
    )"}),
              StatusIs(error::LocalityGroupNotFound("alter_lg")));
}

TEST_P(SchemaUpdaterTest, DropLocalityGroupBasic) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE LOCALITY GROUP lg
      OPTIONS (storage = 'ssd', ssd_to_hdd_spill_timespan = '10m')
    )"}));
  const LocalityGroup* locality_group = schema->FindLocalityGroup("lg");
  EXPECT_NE(locality_group, nullptr);
  EXPECT_EQ(locality_group->Name(), "lg");

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto updated_schema, UpdateSchema(schema.get(), {R"(
      DROP LOCALITY GROUP lg
    )"}));

  EXPECT_EQ(updated_schema->FindLocalityGroup("lg"), nullptr);

  // Test drop locality group if exists.
  ZETASQL_EXPECT_OK(UpdateSchema(schema.get(), {R"(
      DROP LOCALITY GROUP IF EXISTS lg
    )"}));
}

TEST_P(SchemaUpdaterTest, DropUsedLocalityGroup) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE LOCALITY GROUP lg
      OPTIONS (storage = 'ssd', ssd_to_hdd_spill_timespan = '10m')
    )",
                                                  R"(
      CREATE TABLE T (
        k1 INT64,
        c1 STRING(100),
      ) PRIMARY KEY (k1),
        OPTIONS (locality_group = 'lg')
    )"}));
  const LocalityGroup* locality_group = schema->FindLocalityGroup("lg");
  EXPECT_NE(locality_group, nullptr);
  EXPECT_EQ(locality_group->Name(), "lg");
  EXPECT_EQ(locality_group->use_count(), 1);

  // Make sure it's used by table T.
  const Table* table = schema->FindTable("T");
  EXPECT_NE(table, nullptr);
  EXPECT_NE(table->locality_group(), nullptr);

  EXPECT_THAT(
      UpdateSchema(schema.get(), {R"(
      DROP LOCALITY GROUP lg
    )"}),
      StatusIs(error::DroppingLocalityGroupWithAssignedTableColumn("lg")));
}

TEST_P(SchemaUpdaterTest, DropDefaultLocalityGroup) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  EXPECT_THAT(CreateSchema({R"(
      DROP LOCALITY GROUP default
    )"}),
              StatusIs(error::DroppingDefaultLocalityGroup()));
}

TEST_P(SchemaUpdaterTest, AssignLocalityGroupToColumn) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE LOCALITY GROUP lg
      OPTIONS (storage = 'ssd', ssd_to_hdd_spill_timespan = '10m')
    )",
                                                  R"(
      CREATE TABLE T (
        k1 INT64,
        c1 STRING(100) OPTIONS (locality_group = 'lg'),
      ) PRIMARY KEY (k1)
    )"}));
  const LocalityGroup* locality_group = schema->FindLocalityGroup("lg");
  EXPECT_NE(locality_group, nullptr);
  EXPECT_EQ(locality_group->Name(), "lg");
  EXPECT_EQ(locality_group->use_count(), 1);

  const Column* column = schema->FindTable("T")->FindColumn("c1");
  EXPECT_NE(column, nullptr);
  EXPECT_NE(column->locality_group(), nullptr);
}

TEST_P(SchemaUpdaterTest, AlterColumnScopedLocalityGroup) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE LOCALITY GROUP old_lg
      OPTIONS (storage = 'ssd', ssd_to_hdd_spill_timespan = '10m')
    )",
                                                  R"(
      CREATE LOCALITY GROUP new_lg
      OPTIONS (storage = 'hdd', ssd_to_hdd_spill_timespan = '1h')
    )",
                                                  R"(
      CREATE TABLE T (
        k1 INT64,
        c1 STRING(100) OPTIONS (locality_group = 'old_lg'),
      ) PRIMARY KEY (k1)
    )"}));
  const LocalityGroup* locality_group = schema->FindLocalityGroup("old_lg");
  EXPECT_NE(locality_group, nullptr);
  EXPECT_EQ(locality_group->Name(), "old_lg");
  EXPECT_EQ(locality_group->use_count(), 1);

  const Column* column = schema->FindTable("T")->FindColumn("c1");
  EXPECT_NE(column, nullptr);
  EXPECT_NE(column->locality_group(), nullptr);
  EXPECT_EQ(column->locality_group()->Name(), "old_lg");

  // Change the locality group of the c1 column.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto updated_schema, UpdateSchema(schema.get(), {R"(
      ALTER TABLE T ALTER COLUMN c1 SET OPTIONS (locality_group = 'new_lg')
    )"}));

  const LocalityGroup* new_locality_group =
      updated_schema->FindLocalityGroup("new_lg");
  EXPECT_NE(new_locality_group, nullptr);
  EXPECT_EQ(new_locality_group->Name(), "new_lg");
  EXPECT_EQ(new_locality_group->use_count(), 1);

  const LocalityGroup* old_locality_group =
      updated_schema->FindLocalityGroup("old_lg");
  EXPECT_NE(old_locality_group, nullptr);
  EXPECT_EQ(old_locality_group->Name(), "old_lg");
  EXPECT_EQ(old_locality_group->use_count(), 0);

  const Column* updated_column =
      updated_schema->FindTable("T")->FindColumn("c1");
  EXPECT_NE(updated_column, nullptr);
  EXPECT_NE(updated_column->locality_group(), nullptr);
  EXPECT_EQ(updated_column->locality_group()->Name(), "new_lg");
}

TEST_P(SchemaUpdaterTest, AlterTableScopedLocalityGroup) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE LOCALITY GROUP old_lg
      OPTIONS (storage = 'ssd', ssd_to_hdd_spill_timespan = '10m')
    )",
                                                  R"(
      CREATE LOCALITY GROUP new_lg
      OPTIONS (storage = 'hdd', ssd_to_hdd_spill_timespan = '1h')
    )",
                                                  R"(
      CREATE TABLE T (
        k1 INT64,
        c1 STRING(100),
      ) PRIMARY KEY (k1),
      OPTIONS (locality_group = 'old_lg')
    )"}));
  const LocalityGroup* locality_group = schema->FindLocalityGroup("old_lg");
  EXPECT_NE(locality_group, nullptr);
  EXPECT_EQ(locality_group->Name(), "old_lg");
  EXPECT_EQ(locality_group->use_count(), 1);

  const Table* table = schema->FindTable("T");
  EXPECT_NE(table, nullptr);
  EXPECT_NE(table->locality_group(), nullptr);
  EXPECT_EQ(table->locality_group()->Name(), "old_lg");

  // Change the locality group of the table T.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto updated_schema, UpdateSchema(schema.get(), {R"(
      ALTER TABLE T SET OPTIONS (locality_group = 'new_lg')
    )"}));

  const LocalityGroup* new_locality_group =
      updated_schema->FindLocalityGroup("new_lg");
  EXPECT_NE(new_locality_group, nullptr);
  EXPECT_EQ(new_locality_group->Name(), "new_lg");
  EXPECT_EQ(new_locality_group->use_count(), 1);

  const LocalityGroup* old_locality_group =
      updated_schema->FindLocalityGroup("old_lg");
  EXPECT_NE(old_locality_group, nullptr);
  EXPECT_EQ(old_locality_group->Name(), "old_lg");
  EXPECT_EQ(old_locality_group->use_count(), 0);

  const Table* updated_table = updated_schema->FindTable("T");
  EXPECT_NE(updated_table, nullptr);
  EXPECT_NE(updated_table->locality_group(), nullptr);
  EXPECT_EQ(updated_table->locality_group()->Name(), "new_lg");
}

TEST_P(SchemaUpdaterTest, LocalityGroupNotFound) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  // Create a table with undefined locality groups.
  EXPECT_THAT(CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
        c1 STRING(100),
      ) PRIMARY KEY (k1),
      OPTIONS (locality_group = 'lg')
    )"}),
              StatusIs(error::LocalityGroupNotFound("lg")));

  // Set a column with undefined locality groups.
  EXPECT_THAT(CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
        c1 STRING(100) OPTIONS (locality_group = 'lg'),
      ) PRIMARY KEY (k1)
    )"}),
              StatusIs(error::LocalityGroupNotFound("lg")));

  // Alter a table with undefined locality groups.
  EXPECT_THAT(
      CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
        c1 STRING(100),
      ) PRIMARY KEY (k1)
    )",
                    R"(ALTER TABLE T SET OPTIONS (locality_group = 'lg'))"}),
      StatusIs(error::LocalityGroupNotFound("lg")));

  // Alter a column with undefined locality groups.
  EXPECT_THAT(
      CreateSchema(
          {R"(
      CREATE TABLE T (
        k1 INT64,
        c1 STRING(100),
      ) PRIMARY KEY (k1)
    )",
           R"(ALTER TABLE T ALTER COLUMN c1 SET OPTIONS (locality_group = 'lg'))"}),
      StatusIs(error::LocalityGroupNotFound("lg")));
}

}  // namespace
}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner

}  // namespace google
