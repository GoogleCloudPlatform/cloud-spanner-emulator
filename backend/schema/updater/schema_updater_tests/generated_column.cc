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

#include "backend/schema/updater/schema_updater_tests/base.h"
#include "tests/common/scoped_feature_flags_setter.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {

using google::spanner::emulator::test::ScopedEmulatorFeatureFlagsSetter;

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
  EXPECT_TRUE(col->expression().has_value());
  EXPECT_EQ(col->expression().value(), "(G1 + G1)");
  get_column_names(col->dependent_columns(), &dependent_column_names);
  EXPECT_THAT(dependent_column_names,
              testing::UnorderedElementsAreArray({"G1"}));
}

}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
