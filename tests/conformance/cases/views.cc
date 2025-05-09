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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "common/feature_flags.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

class ViewsTest : public DatabaseTest {
  absl::Status SetUpDatabase() override {
    feature_flags_setter_ = std::make_unique<ScopedEmulatorFeatureFlagsSetter>(
        EmulatorFeatureFlags::Flags{
            .enable_views = true,
        });
    return absl::OkStatus();
  }

 protected:
  std::unique_ptr<ScopedEmulatorFeatureFlagsSetter> feature_flags_setter_;
};

TEST_F(ViewsTest, SampleSomeRows) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"(CREATE VIEW R SQL SECURITY INVOKER AS SELECT "HI" AS OUTPUT)",
      R"(CREATE VIEW R1 SQL SECURITY INVOKER AS SELECT R.OUTPUT FROM R)",
      R"(CREATE VIEW V SQL SECURITY INVOKER AS SELECT TRUE AS C1, 33 AS C2)",
  }));

  EXPECT_THAT(Query("SELECT * FROM V"),
              IsOkAndHoldsUnorderedRows({{true, 33}}));
  EXPECT_THAT(Query("SELECT * FROM R1"), IsOkAndHoldsUnorderedRows({{"HI"}}));
}

TEST_F(ViewsTest, Basic) {
  ZETASQL_ASSERT_OK(UpdateSchema({R"(
        CREATE TABLE T (
          K INT64,
          V INT64,
        ) PRIMARY KEY (K)
      )"}));

  // Create, replace, then drop a view. When replacing the view change the
  // and query some columns from a table.
  ZETASQL_EXPECT_OK(UpdateSchema({
      "CREATE VIEW V SQL SECURITY INVOKER AS SELECT TRUE AS T",
  }));
  EXPECT_THAT(Query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS"),
              IsOkAndHoldsUnorderedRows({{"V"}}));

  ZETASQL_EXPECT_OK(UpdateSchema({
      "CREATE OR REPLACE VIEW V SQL SECURITY INVOKER AS SELECT T.K FROM T",
  }));
  EXPECT_THAT(Query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS"),
              IsOkAndHoldsUnorderedRows({{"V"}}));

  ZETASQL_EXPECT_OK(UpdateSchema({
      "DROP VIEW V",
  }));
  EXPECT_THAT(Query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS"),
              IsOkAndHoldsUnorderedRows({}));

  // Create a new quoted view using `CREATE OR REPLACE`, then drop it.
  ZETASQL_EXPECT_OK(UpdateSchema({
      "CREATE OR REPLACE VIEW `TABLE` SQL SECURITY INVOKER AS SELECT 1 AS A",
  }));
  EXPECT_THAT(Query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS"),
              IsOkAndHoldsUnorderedRows({{"TABLE"}}));
  ZETASQL_EXPECT_OK(UpdateSchema({
      "DROP VIEW `TABLE`",
  }));
  EXPECT_THAT(Query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS"),
              IsOkAndHoldsUnorderedRows({}));
}

TEST_F(ViewsTest, NoSecurityType) {
  ASSERT_THAT(
      UpdateSchema({"CREATE VIEW V AS SELECT TRUE AS T"}),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kInvalidArgument,
          testing::EndsWith("`V` is missing the SQL SECURITY clause.")));
}

TEST_F(ViewsTest, UnknownSecurityType) {
  EXPECT_THAT(
      UpdateSchema({
          "CREATE VIEW S1 SQL SECURITY FOO AS SELECT T.K FROM T",
      }),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kInvalidArgument,
          in_prod_env()
              ? testing::EndsWith(
                    "Encountered 'FOO' while parsing: sql_security")
              : testing::EndsWith("Expecting 'INVOKER' but found 'FOO'")));
}

TEST_F(ViewsTest, SimpleQuery) {
  ZETASQL_ASSERT_OK(UpdateSchema({
      R"(
        CREATE TABLE T (
          K INT64,
          V INT64,
        ) PRIMARY KEY (K)
      )",
  }));

  ZETASQL_ASSERT_OK(UpdateSchema({
      "CREATE VIEW V SQL SECURITY INVOKER AS SELECT TRUE AS T",
  }));

  ZETASQL_EXPECT_OK(Query("SELECT * FROM V"));
}

TEST_F(ViewsTest, SimpleQueryFromTable) {
  ZETASQL_ASSERT_OK(UpdateSchema({
      R"(
        CREATE TABLE T (
          K INT64,
          V INT64,
        ) PRIMARY KEY (K)
      )",
  }));
  ZETASQL_ASSERT_OK(Insert("T", {"K", "V"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("T", {"K", "V"}, {3, 4}));
  ZETASQL_ASSERT_OK(Insert("T", {"K", "V"}, {5, 6}));
  ZETASQL_ASSERT_OK(Insert("T", {"K", "V"}, {7, 8}));

  ZETASQL_ASSERT_OK(UpdateSchema({
      "CREATE VIEW V SQL SECURITY INVOKER AS SELECT T.K + T.V AS OUT FROM T",
  }));

  EXPECT_THAT(Query("SELECT * FROM V"), IsOkAndHoldsUnorderedRows({
                                            {3},
                                            {7},
                                            {11},
                                            {15},
                                        }));
}

TEST_F(ViewsTest, Aliases) {
  ZETASQL_ASSERT_OK(UpdateSchema({
      R"(
        CREATE TABLE T (
          K INT64,
          V INT64,
        ) PRIMARY KEY (K)
      )",
  }));
  ZETASQL_ASSERT_OK(Insert("T", {"K", "V"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("T", {"K", "V"}, {3, 4}));
  ZETASQL_ASSERT_OK(Insert("T", {"K", "V"}, {5, 6}));
  ZETASQL_ASSERT_OK(Insert("T", {"K", "V"}, {7, 8}));

  // Aliased table name to an aliased output column name.
  ZETASQL_ASSERT_OK(UpdateSchema({
      "CREATE VIEW V1 SQL SECURITY INVOKER AS "
      "    SELECT TPRIME.K AS OUTPUT1 FROM T AS TPRIME",
  }));
  EXPECT_THAT(Query("SELECT OUTPUT1 FROM V1"), IsOkAndHoldsUnorderedRows({
                                                   {1},
                                                   {3},
                                                   {5},
                                                   {7},
                                               }));
  EXPECT_THAT(Query("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                    "WHERE TABLE_NAME = 'V1'"),
              IsOkAndHoldsUnorderedRows({
                  {"OUTPUT1"},
              }));

  // Aliased column name from an aliased input column name.
  ZETASQL_ASSERT_OK(UpdateSchema({
      "CREATE VIEW V2 SQL SECURITY INVOKER AS"
      "    SELECT KPRIME1 + 1 AS KPRIME2 FROM"
      "        (SELECT T.K + 1 AS KPRIME1 FROM T)",
  }));
  EXPECT_THAT(Query("SELECT KPRIME2 FROM V2"), IsOkAndHoldsUnorderedRows({
                                                   {3},
                                                   {5},
                                                   {7},
                                                   {9},
                                               }));
  EXPECT_THAT(Query("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                    "WHERE TABLE_NAME = 'V2'"),
              IsOkAndHoldsUnorderedRows({
                  {"KPRIME2"},
              }));
}

TEST_F(ViewsTest, StrictNameResolution) {
  ZETASQL_ASSERT_OK(UpdateSchema({
      R"(
        CREATE TABLE T (
          K INT64,
          V INT64,
        ) PRIMARY KEY (K)
      )",
  }));

  EXPECT_THAT(
      UpdateSchema(
          {"CREATE VIEW Invalid SQL SECURITY INVOKER AS SELECT K FROM T"}),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kInvalidArgument,
          testing::HasSubstr("K cannot be used without a qualifier "
                             "in strict name resolution mode")));
}

TEST_F(ViewsTest, ErrorCreateViewWithZetaSQLAnalysisError) {
  EXPECT_THAT(
      UpdateSchema({"CREATE VIEW Invalid SQL SECURITY INVOKER AS SELECT TRUE"}),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kInvalidArgument,
          testing::ContainsRegex(
              "Error (analyzing|parsing) the definition of view `Invalid`: "
              "CREATE VIEW columns must be named, but column 1 has no name")));
}

TEST_F(ViewsTest, ErrorCreateViewReferencingMissingTable) {
  EXPECT_THAT(
      UpdateSchema({"CREATE VIEW Invalid SQL SECURITY INVOKER AS SELECT ONE "
                    "FROM MISSING"}),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kInvalidArgument,
          testing::ContainsRegex("Error (analyzing|parsing) the definition of "
                                 "view `Invalid`: Table not found: MISSING")));
}

TEST_F(ViewsTest, ErrorReplacingViewHasAnalysisError) {
  // Try to replace an existing view with one that has a ZetaSQL analysis
  // error.
  ZETASQL_ASSERT_OK(
      UpdateSchema({"CREATE VIEW V SQL SECURITY INVOKER AS SELECT TRUE AS T"}));
  EXPECT_THAT(
      UpdateSchema({
          "CREATE OR REPLACE VIEW V SQL SECURITY INVOKER AS SELECT ERROR",
      }),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kFailedPrecondition,
          // Use two HasSubstr checks instead of a regex because .* doesn't
          // match newlines.
          testing::AllOf(
              testing::HasSubstr(
                  "Cannot replace VIEW `V` because new definition is invalid"),
              testing::HasSubstr("Unrecognized name: ERROR"))));
}

TEST_F(ViewsTest, ErrorReplacingViewCausesRecursion) {
  ZETASQL_ASSERT_OK(
      UpdateSchema({"CREATE VIEW V SQL SECURITY INVOKER AS SELECT TRUE AS T"}));

  // Create a recursive view.
  EXPECT_THAT(
      UpdateSchema({
          "CREATE OR REPLACE VIEW V SQL SECURITY INVOKER AS SELECT V.T FROM V",
      }),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kFailedPrecondition,
          testing::HasSubstr(
              "Cannot replace VIEW `V` because new definition is recursive.")));
}

TEST_F(ViewsTest, ReplacingViewWithDependentView) {
  // Change the type of a column that another view depends on.
  ZETASQL_ASSERT_OK(UpdateSchema(
      {"CREATE VIEW V SQL SECURITY INVOKER AS SELECT TRUE AS T",
       "CREATE VIEW D SQL SECURITY INVOKER AS SELECT V.T = TRUE AS C FROM V"}));
  ZETASQL_EXPECT_OK(
      UpdateSchema({"CREATE OR REPLACE VIEW V SQL SECURITY INVOKER AS "
                    "(SELECT CAST(1 AS BOOL) AS T)"}));
}

TEST_F(ViewsTest, ErrorReplacingViewBreaksDependentView) {
  // Change the type of a column that another view depends on.
  ZETASQL_ASSERT_OK(UpdateSchema(
      {"CREATE VIEW V SQL SECURITY INVOKER AS SELECT TRUE AS T",
       "CREATE VIEW D SQL SECURITY INVOKER AS SELECT V.T = TRUE AS C FROM V"}));
  EXPECT_THAT(
      UpdateSchema({
          "CREATE OR REPLACE VIEW V SQL SECURITY INVOKER AS SELECT 1 AS T",
      }),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kFailedPrecondition,
          testing::AllOf(
              testing::HasSubstr(
                  "The new definition causes the definition of VIEW `D` to "
                  "become "
                  "invalid with the following diagnostic message:"),
              testing::HasSubstr("No matching signature"))));
}

TEST_F(ViewsTest, ReplacingViewChangesDependentViewSignature) {
  ZETASQL_ASSERT_OK(
      UpdateSchema({"CREATE VIEW V SQL SECURITY INVOKER AS SELECT TRUE AS T",
                    "CREATE VIEW FORWARD SQL SECURITY INVOKER AS "
                    "SELECT CAST(V.T AS BOOL) AS T FROM V"}));
  // Test replacing a view that changes a column type, while another view
  // implicitly forwards that column type.
  ZETASQL_EXPECT_OK(UpdateSchema(
      {"CREATE OR REPLACE VIEW V SQL SECURITY INVOKER AS SELECT 1 AS T"}));
}

TEST_F(ViewsTest, ErrorReplacingViewChangesDependentViewSignature) {
  ZETASQL_ASSERT_OK(UpdateSchema(
      {"CREATE VIEW V SQL SECURITY INVOKER AS SELECT TRUE AS T",
       "CREATE VIEW FORWARD SQL SECURITY INVOKER AS SELECT V.T FROM V"}));
  // Test replacing a view that changes a column type, while another view
  // implicitly forwards that column type. Without an explicit cast, this should
  // be disallowed.
  EXPECT_THAT(
      UpdateSchema({
          "CREATE OR REPLACE VIEW V SQL SECURITY INVOKER AS SELECT 1 AS T",
      }),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kFailedPrecondition,
          testing::ContainsRegex(
              "Cannot alter|replace VIEW `V`. Action would implicitly change "
              "the type of an output column for VIEW `FORWARD` from `BOOL` to "
              "`INT64`")));
  ZETASQL_EXPECT_OK(UpdateSchema({
      "DROP VIEW FORWARD",
  }));
}

TEST_F(ViewsTest, DropViewWithDependents) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      "CREATE TABLE T (K INT64, C INT64) PRIMARY KEY (K)",
      "CREATE VIEW DEP_ON_COL SQL SECURITY INVOKER AS SELECT T.C FROM T",
      "CREATE VIEW DEP_ON_VIEW SQL SECURITY INVOKER AS"
      "  SELECT DEP_ON_COL.C FROM DEP_ON_COL",
  }));

  // Try to drop a table that a view depends on.
  EXPECT_THAT(UpdateSchema({
                  "DROP TABLE T",
              }),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kFailedPrecondition,
                  testing::ContainsRegex("Cannot drop TABLE `T` on which there "
                                         "are dependent views: .*")));

  // Try to drop a view that another view depends on.
  EXPECT_THAT(
      UpdateSchema({
          "DROP VIEW DEP_ON_COL",
      }),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kFailedPrecondition,
          testing::ContainsRegex("Cannot drop VIEW `DEP_ON_COL` on which there "
                                 "are dependent views: .*")));

  // Try to drop table column that a view depends on.
  EXPECT_THAT(
      UpdateSchema({
          "ALTER TABLE T DROP COLUMN C",
      }),
      zetasql_base::testing::StatusIs(absl::StatusCode::kFailedPrecondition,
                                testing::HasSubstr("Cannot drop column `C`")));

  // Can drop dependencies after dropping the depending view.
  ZETASQL_EXPECT_OK(UpdateSchema({
      "DROP VIEW DEP_ON_VIEW",
      "DROP VIEW DEP_ON_COL",
      "DROP TABLE T",
  }));
}

TEST_F(ViewsTest, ErrorViewDefinitionContainsInvalidType) {
  EXPECT_THAT(
      UpdateSchema({"CREATE VIEW V SQL SECURITY INVOKER AS "
                    "SELECT CAST(1 AS INT32) AS OUT"}),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kInvalidArgument,
          testing::ContainsRegex("Error (analyzing|parsing) the definition of "
                                 "view `V`: Type not found: INT32")));
}

TEST_F(ViewsTest, DropUnknownView) {
  // Drop a view that does not exist.
  EXPECT_THAT(UpdateSchema({
                  "DROP VIEW UNKNOWN",
              }),
              zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(ViewsTest, InformationSchema) {
  ZETASQL_ASSERT_OK(UpdateSchema({
      R"(CREATE TABLE T (
          K INT64,
          V INT64,
         ) PRIMARY KEY (K))",
      R"(CREATE VIEW V SQL SECURITY INVOKER AS SELECT TRUE AS T, FALSE AS F)",
  }));

  // User-created views are visible in the information schema.
  EXPECT_THAT(
      Query(R"(SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, VIEW_DEFINITION
               FROM INFORMATION_SCHEMA.VIEWS
               WHERE TABLE_SCHEMA = '')"),
      IsOkAndHoldsUnorderedRows(
          {{"", "", "V", "SELECT TRUE AS T, FALSE AS F"}}));

  // The TABLE_TYPE column is visible in INFORMATION_SCHEMA.TABLES.
  EXPECT_THAT(Query(R"(SELECT TABLE_NAME, TABLE_TYPE
                       FROM INFORMATION_SCHEMA.TABLES
                       WHERE TABLE_NAME = 'T' OR TABLE_NAME = 'V')"),
              IsOkAndHoldsUnorderedRows({{"T", "BASE TABLE"}, {"V", "VIEW"}}));

  // View columns are added to INFORMATION_SCHEMA.COLUMNS.
  EXPECT_THAT(Query("SELECT TABLE_NAME, COLUMN_NAME, SPANNER_TYPE FROM "
                    "INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ''"),
              IsOkAndHoldsUnorderedRows({{"T", "K", "INT64"},
                                         {"T", "V", "INT64"},
                                         {"V", "T", "BOOL"},
                                         {"V", "F", "BOOL"}}));
}

}  // namespace
}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
