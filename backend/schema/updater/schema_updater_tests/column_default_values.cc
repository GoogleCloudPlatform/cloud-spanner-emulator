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

#include <memory>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "backend/schema/updater/schema_updater_tests/base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {

TEST_P(SchemaUpdaterTest, NonKeyColumns) {
  std::unique_ptr<const Schema> schema;
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        schema,
        CreateSchema({R"(
          CREATE TABLE T (
            K INT64 NOT NULL,
            V STRING(10),
            D1 INT64 NOT NULL DEFAULT (1),
          ) PRIMARY KEY (K)
        )",
                      "ALTER TABLE T ADD COLUMN D2 INT64 DEFAULT (2)"}));

  const Table* table = schema->FindTable("T");
  ASSERT_NE(table, nullptr);
  const Column* col = table->FindColumn("V");
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Name(), "V");
  EXPECT_FALSE(col->is_generated());
  EXPECT_FALSE(col->has_default_value());
  EXPECT_FALSE(col->expression().has_value());

  col = table->FindColumn("D1");
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Name(), "D1");
  EXPECT_EQ(col->GetType()->kind(), zetasql::TYPE_INT64);
  EXPECT_FALSE(col->is_nullable());
  EXPECT_FALSE(col->is_generated());
  EXPECT_TRUE(col->has_default_value());
  EXPECT_TRUE(col->expression().has_value());
  EXPECT_EQ(col->expression().value(), "1");
  EXPECT_EQ(col->dependent_columns().size(), 0);

  col = table->FindColumn("D2");
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Name(), "D2");
  EXPECT_EQ(col->GetType()->kind(), zetasql::TYPE_INT64);
  EXPECT_TRUE(col->is_nullable());
  EXPECT_FALSE(col->is_generated());
  EXPECT_TRUE(col->has_default_value());
  EXPECT_TRUE(col->expression().has_value());
  EXPECT_EQ(col->expression().value(), "2");
  EXPECT_EQ(col->dependent_columns().size(), 0);
}

TEST_P(SchemaUpdaterTest, FunctionAsDefault) {
  std::unique_ptr<const Schema> schema;
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
          CREATE TABLE T (
            K INT64 NOT NULL,
            V TIMESTAMP DEFAULT (CURRENT_TIMESTAMP())
          ) PRIMARY KEY(K)
        )"}));

  const Table* table = schema->FindTable("T");
  ASSERT_NE(table, nullptr);
  const Column* col = table->FindColumn("V");
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Name(), "V");
  EXPECT_FALSE(col->is_generated());
  EXPECT_TRUE(col->has_default_value());
  EXPECT_TRUE(col->expression().has_value());
  EXPECT_EQ(col->expression().value(), "CURRENT_TIMESTAMP()");
}

TEST_P(SchemaUpdaterTest, KeyColumn) {
  std::unique_ptr<const Schema> schema;
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        schema,
        CreateSchema({R"(
        CREATE TABLE T (
          K1 INT64 NOT NULL,
          K2 INT64 DEFAULT (20),
          K3 INT64,
          V STRING(10),
        ) PRIMARY KEY (K1, K2, K3)
      )",
                      "ALTER TABLE T ALTER COLUMN K3 INT64 DEFAULT (30)"}));

  const Table* table = schema->FindTable("T");
  ASSERT_NE(table, nullptr);
  const Column* col = table->FindColumn("K1");
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Name(), "K1");
  EXPECT_FALSE(col->is_generated());
  EXPECT_FALSE(col->has_default_value());
  EXPECT_FALSE(col->expression().has_value());

  col = table->FindColumn("K2");
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Name(), "K2");
  EXPECT_EQ(col->GetType()->kind(), zetasql::TYPE_INT64);
  EXPECT_FALSE(col->is_generated());
  EXPECT_TRUE(col->has_default_value());
  EXPECT_TRUE(col->expression().has_value());
  EXPECT_EQ(col->expression().value(), "20");
  EXPECT_EQ(col->dependent_columns().size(), 0);

  col = table->FindColumn("K3");
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Name(), "K3");
  EXPECT_EQ(col->GetType()->kind(), zetasql::TYPE_INT64);
  EXPECT_FALSE(col->is_generated());
  EXPECT_TRUE(col->has_default_value());
  EXPECT_TRUE(col->expression().has_value());
  EXPECT_EQ(col->expression().value(), "30");
  EXPECT_EQ(col->dependent_columns().size(), 0);
}

TEST_P(SchemaUpdaterTest, SetDropDefault) {
  std::unique_ptr<const Schema> schema;
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        schema, CreateSchema({R"(
        CREATE TABLE T (
          K1 INT64 NOT NULL,
          K2 INT64 DEFAULT (20),
          K3 INT64,
          V STRING(10) DEFAULT ("Hello"),
        ) PRIMARY KEY (K1, K2, K3)
      )",
                              "ALTER TABLE T ALTER COLUMN K3 SET DEFAULT (30)",
                              "ALTER TABLE T ALTER COLUMN K2 SET DEFAULT (2)",
                              "ALTER TABLE T ALTER V DROP DEFAULT"}));

  const Table* table = schema->FindTable("T");
  ASSERT_NE(table, nullptr);
  const Column* col = table->FindColumn("K1");
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Name(), "K1");
  EXPECT_FALSE(col->is_generated());
  EXPECT_FALSE(col->has_default_value());
  EXPECT_FALSE(col->expression().has_value());

  col = table->FindColumn("K2");
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Name(), "K2");
  EXPECT_EQ(col->GetType()->kind(), zetasql::TYPE_INT64);
  EXPECT_FALSE(col->is_generated());
  EXPECT_TRUE(col->has_default_value());
  EXPECT_TRUE(col->expression().has_value());
  EXPECT_EQ(col->expression().value(), "2");
  EXPECT_EQ(col->dependent_columns().size(), 0);

  col = table->FindColumn("K3");
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Name(), "K3");
  EXPECT_EQ(col->GetType()->kind(), zetasql::TYPE_INT64);
  EXPECT_FALSE(col->is_generated());
  EXPECT_TRUE(col->has_default_value());
  EXPECT_TRUE(col->expression().has_value());
  EXPECT_EQ(col->expression().value(), "30");
  EXPECT_EQ(col->dependent_columns().size(), 0);

  col = table->FindColumn("V");
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Name(), "V");
  EXPECT_EQ(col->GetType()->kind(), zetasql::TYPE_STRING);
  EXPECT_FALSE(col->is_generated());
  EXPECT_FALSE(col->has_default_value());
  EXPECT_FALSE(col->expression().has_value());
}

}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
