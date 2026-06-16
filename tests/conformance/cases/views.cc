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
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "common/feature_flags.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"
#include "googlesql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using ::testing::HasSubstr;
using ::googlesql_base::testing::StatusIs;

class ViewsTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
  absl::Status SetUpDatabase() override {
    feature_flags_setter_ = std::make_unique<ScopedEmulatorFeatureFlagsSetter>(
        EmulatorFeatureFlags::Flags{
            .enable_views = true,
        });
    GOOGLESQL_RETURN_IF_ERROR(SetSchemaFromFile("views.test"));

    // Populate the table with some data.
    std::vector<ValueRow> rows = {{1, 2}, {3, 4}, {5, 6}, {7, 8}};
    for (const auto& row : rows) {
      GOOGLESQL_ASSIGN_OR_RETURN(auto _, Insert("t", {"k", "v"}, row));
    }
    return absl::OkStatus();
  }

  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

 protected:
  std::unique_ptr<ScopedEmulatorFeatureFlagsSetter> feature_flags_setter_;
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectViewsTests, ViewsTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<ViewsTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(ViewsTest, SampleSomeRows) {
  EXPECT_THAT(Query("SELECT * FROM v_mixed"),
              IsOkAndHoldsUnorderedRows({{true, 33}}));
  EXPECT_THAT(Query("SELECT * FROM v_depend"),
              IsOkAndHoldsUnorderedRows({{"HI"}}));
}

TEST_P(ViewsTest, SimpleQuery) { GOOGLESQL_EXPECT_OK(Query("SELECT * FROM v_bool")); }

TEST_P(ViewsTest, SimpleQueryFromTable) {
  EXPECT_THAT(Query("SELECT * FROM v_table"),
              IsOkAndHoldsUnorderedRows({{3}, {7}, {11}, {15}}));
}

TEST_P(ViewsTest, Aliases) {
  // Aliased table name to an aliased output column name.
  EXPECT_THAT(Query("SELECT output1 FROM v_aliastable"),
              IsOkAndHoldsUnorderedRows({
                  {1},
                  {3},
                  {5},
                  {7},
              }));
  EXPECT_THAT(Query("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                    "WHERE TABLE_NAME = 'v_aliastable'"),
              IsOkAndHoldsUnorderedRows({
                  {"output1"},
              }));

  // Aliased column name from an aliased input column name.
  EXPECT_THAT(Query("SELECT kprime2 FROM v_aliascolumn"),
              IsOkAndHoldsUnorderedRows({
                  {3},
                  {5},
                  {7},
                  {9},
              }));
  EXPECT_THAT(Query("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                    "WHERE TABLE_NAME = 'v_aliascolumn'"),
              IsOkAndHoldsUnorderedRows({
                  {"kprime2"},
              }));
}

TEST_P(ViewsTest, InformationSchemaVisibility) {
  std::string sql =
      "SELECT TABLE_NAME, VIEW_DEFINITION "
      "FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'v'";

  GOOGLESQL_ASSERT_OK(UpdateSchema({
      R"(CREATE VIEW v SQL SECURITY INVOKER AS SELECT true AS t, false AS f)",
  }));
  // User-created views are visible in the information schema.
  EXPECT_THAT(Query(sql), IsOkAndHoldsUnorderedRows(
                              {{"v", "SELECT true AS t, false AS f"}}));
  GOOGLESQL_ASSERT_OK(UpdateSchema({
      R"(CREATE OR REPLACE VIEW v SQL SECURITY INVOKER AS SELECT t.k FROM t)",
  }));
  // Replaced views are updated in the information schema.
  auto expected_definition =
      dialect_ == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL
          ? "SELECT t.k FROM t"
          : "SELECT k FROM t";
  EXPECT_THAT(Query(sql),
              IsOkAndHoldsUnorderedRows({{"v", expected_definition}}));

  GOOGLESQL_ASSERT_OK(UpdateSchema({R"(DROP VIEW v)"}));
  // User-created views are no longer visible in the information schema.
  EXPECT_THAT(Query(sql), IsOkAndHoldsUnorderedRows({}));
}

class ViewsDisabledTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
  absl::Status SetUpDatabase() override {
    feature_flags_setter_ = std::make_unique<ScopedEmulatorFeatureFlagsSetter>(
        EmulatorFeatureFlags::Flags{
            .enable_views = false,
        });
    return absl::OkStatus();
  }

  void SetUp() override {
    if (GetConformanceTestGlobals().in_prod_env) {
      GTEST_SKIP() << "Test not applicable to the real Spanner backend "
                      "(Emulator flags test)";
    }
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }
  void TearDown() override {
    if (GetConformanceTestGlobals().in_prod_env) {
      return;
    }
    DatabaseTest::TearDown();
  }

 protected:
  std::unique_ptr<ScopedEmulatorFeatureFlagsSetter> feature_flags_setter_;
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectViewsDisabledTests, ViewsDisabledTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<ViewsDisabledTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(ViewsDisabledTest, CreateDropViewFailsIfDisabled) {
  EXPECT_THAT(
      UpdateSchema({R"(
      CREATE VIEW v SQL SECURITY INVOKER AS SELECT true AS t, false AS f)"}),
      StatusIs(
          absl::StatusCode::kUnimplemented,
          HasSubstr("`CREATE` for INVOKER RIGHTS views is not supported")));
  EXPECT_THAT(
      UpdateSchema({R"(
      DROP VIEW v)"}),
      StatusIs(absl::StatusCode::kUnimplemented,
               HasSubstr("`DROP` for INVOKER RIGHTS views is not supported")));
}

}  // namespace
}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
