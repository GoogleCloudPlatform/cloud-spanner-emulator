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

#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "backend/schema/updater/schema_updater_tests/base.h"
#include "common/errors.h"
#include "tests/common/scoped_feature_flags_setter.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {

using google::spanner::emulator::test::ScopedEmulatorFeatureFlagsSetter;
using ::testing::HasSubstr;
using ::zetasql_base::testing::StatusIs;

class GeneratedColumnSchemaUpdaterTest : public SchemaUpdaterTest {
 public:
  GeneratedColumnSchemaUpdaterTest()
      : feature_flags_({.enable_stored_generated_columns = true}) {}

 private:
  ScopedEmulatorFeatureFlagsSetter feature_flags_;
};

TEST_F(GeneratedColumnSchemaUpdaterTest, Basic) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T (
        K INT64 NOT NULL,
        V STRING(10),
        G1 INT64 NOT NULL AS (k + LENGTH(v)) STORED,
      ) PRIMARY KEY (K)
    )",
                                                  R"(
      ALTER TABLE T ADD COLUMN G2 INT64 AS (G1 + G1) STORED
    )"}));

  const Table* table = schema->FindTable("T");
  ASSERT_NE(table, nullptr);
  const Column* col = table->FindColumn("V");
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Name(), "V");
  EXPECT_FALSE(col->is_generated());
  EXPECT_FALSE(col->expression().has_value());

  col = table->FindColumn("G1");
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Name(), "G1");
  EXPECT_EQ(col->GetType()->kind(), zetasql::TYPE_INT64);
  EXPECT_FALSE(col->is_nullable());
  EXPECT_TRUE(col->is_generated());
  EXPECT_FALSE(col->has_default_value());
  EXPECT_TRUE(col->expression().has_value());
  EXPECT_EQ(col->expression().value(), "(k + LENGTH(v))");

  auto get_column_names = [](absl::Span<const Column* const> columns,
                             std::vector<std::string>* column_names) {
    column_names->clear();
    column_names->reserve(columns.size());
    for (const Column* col : columns) {
      column_names->push_back(col->Name());
    }
  };
  std::vector<std::string> dependent_column_names;
  get_column_names(col->dependent_columns(), &dependent_column_names);
  EXPECT_THAT(dependent_column_names,
              testing::UnorderedElementsAreArray({"K", "V"}));
  col = table->FindColumn("G2");
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Name(), "G2");
  EXPECT_EQ(col->GetType()->kind(), zetasql::TYPE_INT64);
  EXPECT_TRUE(col->is_nullable());
  EXPECT_TRUE(col->is_generated());
  EXPECT_FALSE(col->has_default_value());
  EXPECT_TRUE(col->expression().has_value());
  EXPECT_EQ(col->expression().value(), "(G1 + G1)");
  get_column_names(col->dependent_columns(), &dependent_column_names);
  EXPECT_THAT(dependent_column_names,
              testing::UnorderedElementsAreArray({"G1"}));
}

TEST_F(GeneratedColumnSchemaUpdaterTest,
       CannotCreateTableAddNonStoredGeneratedColumn) {
  EXPECT_THAT(
      CreateSchema({R"(
      CREATE TABLE T (
        K INT64 NOT NULL,
        V INT64,
        G INT64 AS (K + V),
      ) PRIMARY KEY (K)
    )"}),
      StatusIs(
          absl::StatusCode::kUnimplemented,
          HasSubstr("Generated column `G` without the STORED attribute is not "
                    "supported.")));
}

TEST_F(GeneratedColumnSchemaUpdaterTest,
       CannotAlterTableAddNonStoredGeneratedColumn) {
  EXPECT_THAT(
      CreateSchema({R"(
      CREATE TABLE T (
        K INT64 NOT NULL,
        V INT64,
      ) PRIMARY KEY (K)
    )",
                    R"(
      ALTER TABLE T ADD COLUMN G INT64 AS (K + V)
    )"}),
      StatusIs(
          absl::StatusCode::kUnimplemented,
          HasSubstr("Generated column `G` without the STORED attribute is not "
                    "supported.")));
}

TEST_F(GeneratedColumnSchemaUpdaterTest,
       CannotAlterTableAlterColumnToNonStoredGenerated) {
  EXPECT_THAT(
      CreateSchema({R"(
      CREATE TABLE T (
        K INT64 NOT NULL,
        V INT64,
        G INT64,
      ) PRIMARY KEY (K)
    )",
                    R"(
      ALTER TABLE T ALTER COLUMN G INT64 AS (K + V)
    )"}),
      StatusIs(
          absl::StatusCode::kUnimplemented,
          HasSubstr("Generated column `G` without the STORED attribute is not "
                    "supported.")));
}

std::vector<std::string> SchemaForCaseSensitivityTests() {
  return {
      R"sql(
                CREATE TABLE T (
                  K INT64 NOT NULL,
                  V INT64,
                ) PRIMARY KEY (K)
            )sql",
  };
}

TEST_F(GeneratedColumnSchemaUpdaterTest,
       StoredColumnExpressionIsCaseInsensitive) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema(SchemaForCaseSensitivityTests()));

  ZETASQL_EXPECT_OK(UpdateSchema(schema.get(), {R"(
      ALTER TABLE T ADD COLUMN G INT64 AS (k + v) STORED
    )"}));
}

TEST_F(GeneratedColumnSchemaUpdaterTest, StoredColumnNameIsCaseSensitive) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema(SchemaForCaseSensitivityTests()));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      ALTER TABLE T DROP COLUMN g
    )"}),
              StatusIs(absl::StatusCode::kNotFound,
                       HasSubstr("Column not found in table T: g")));
}

}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
