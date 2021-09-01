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

namespace {

using ::google::spanner::emulator::test::ScopedEmulatorFeatureFlagsSetter;

class CheckConstraintSchemaUpdaterTest : public SchemaUpdaterTest {
 public:
  CheckConstraintSchemaUpdaterTest()
      : feature_flags_({.enable_stored_generated_columns = true,
                        .enable_check_constraint = true}) {}

 private:
  ScopedEmulatorFeatureFlagsSetter feature_flags_;
};

TEST_F(CheckConstraintSchemaUpdaterTest, Basic) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto schema,
      CreateSchema({"CREATE TABLE T ("
                    "  K INT64,"
                    "  V INT64,"
                    "  CONSTRAINT C1 CHECK(K > 0)"
                    ") PRIMARY KEY (K)",
                    "ALTER TABLE T ADD CONSTRAINT C2 CHECK(K + V > 0)"}));

  const Table* table = ASSERT_NOT_NULL(schema->FindTable("T"));
  const CheckConstraint* check1 =
      ASSERT_NOT_NULL(table->FindCheckConstraint("C1"));
  const CheckConstraint* check2 =
      ASSERT_NOT_NULL(table->FindCheckConstraint("C2"));
  EXPECT_EQ(check1->Name(), "C1");
  EXPECT_EQ(check1->table()->Name(), "T");
  EXPECT_EQ(check1->expression(), "K > 0");

  EXPECT_EQ(check2->Name(), "C2");
  EXPECT_EQ(check2->table()->Name(), "T");
  EXPECT_EQ(check2->expression(), "K + V > 0");

  auto get_column_names = [](absl::Span<const Column* const> columns,
                             std::vector<std::string>* column_names) {
    column_names->clear();
    column_names->reserve(columns.size());
    for (const Column* col : columns) {
      column_names->push_back(col->Name());
    }
  };

  std::vector<std::string> dependent_column_names;
  get_column_names(check1->dependent_columns(), &dependent_column_names);
  EXPECT_THAT(dependent_column_names,
              testing::UnorderedElementsAreArray({"K"}));

  get_column_names(check2->dependent_columns(), &dependent_column_names);
  EXPECT_THAT(dependent_column_names,
              testing::UnorderedElementsAreArray({"K", "V"}));
}

}  // namespace

}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
