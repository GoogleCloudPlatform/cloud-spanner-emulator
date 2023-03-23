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

#include <algorithm>
#include <iterator>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/types/type_factory.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/strings/ascii.h"
#include "backend/schema/printer/print_ddl.h"
#include "backend/schema/updater/schema_updater_tests/base.h"
#include "common/errors.h"
#include "common/feature_flags.h"
#include "tests/common/scoped_feature_flags_setter.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {

using ::google::spanner::emulator::test::ScopedEmulatorFeatureFlagsSetter;

namespace {

class ViewsTest : public SchemaUpdaterTest {
 public:
  ViewsTest()
      : flag_setter_({
            .enable_views = true,
        }) {}
  const ScopedEmulatorFeatureFlagsSetter flag_setter_;
};

TEST_F(ViewsTest, Basic) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
    CREATE TABLE T(
      col1 INT64,
      col2 STRING(MAX)
    ) PRIMARY KEY(col1)
  )",
                                                  R"(
    CREATE OR REPLACE VIEW `MyView` SQL SECURITY INVOKER AS
    SELECT T.col1, T.col2 FROM T
  )"}));
  auto t = schema->FindTable("T");
  ASSERT_NE(t, nullptr);

  auto v = schema->FindView("Myview");
  EXPECT_NE(v, nullptr);
  EXPECT_EQ(schema->FindViewCaseSensitive("MyView"), v);
  EXPECT_THAT(schema->views(), testing::ElementsAreArray({v}));
  EXPECT_EQ(v->Name(), "MyView");
  EXPECT_EQ(v->columns().size(), 2);
  EXPECT_THAT(absl::StripAsciiWhitespace(v->body()),
              testing::StrEq("SELECT T.col1, T.col2 FROM T"));
  EXPECT_THAT(v->dependencies(), testing::ElementsAreArray({t}));
  EXPECT_THAT(v->security(), testing::Eq(View::SqlSecurity::INVOKER));
}

TEST_F(ViewsTest, MultipleTableDependencies) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
    CREATE TABLE T1(
      col1 INT64,
      col2 STRING(MAX)
    ) PRIMARY KEY(col1)
  )",
                                                  R"(
    CREATE TABLE T2(
      k1 INT64,
      k2 STRING(MAX)
    ) PRIMARY KEY(k1)
  )",
                                                  R"(
    CREATE OR REPLACE VIEW V SQL SECURITY INVOKER AS
    SELECT T1.col1, T2.k1 FROM T1, T2
  )"}));
  auto v = schema->FindView("V");
  auto t1 = schema->FindTable("T1");
  auto t2 = schema->FindTable("T2");
  EXPECT_THAT(v->dependencies(), testing::UnorderedElementsAreArray({t1, t2}));
}

TEST_F(ViewsTest, ViewDependsOnView) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
    CREATE TABLE T1(
      col1 INT64,
      col2 STRING(MAX)
    ) PRIMARY KEY(col1)
  )",
                                                  R"(
    CREATE OR REPLACE VIEW V1 SQL SECURITY INVOKER AS
    SELECT T1.col1 AS k1 FROM T1
  )",
                                                  R"(
    CREATE OR REPLACE VIEW V2 SQL SECURITY INVOKER AS
    SELECT V1.k1 AS k2 FROM V1
  )"}));
  auto v1 = schema->FindView("V1");
  EXPECT_NE(v1, nullptr);
  auto v2 = schema->FindView("V2");
  EXPECT_NE(v2, nullptr);
  EXPECT_EQ(v2->dependencies().size(), 1);
  EXPECT_EQ(v2->dependencies()[0], v1);
  EXPECT_EQ(v2->security(), View::SqlSecurity::INVOKER);
  EXPECT_EQ(v2->columns()[0].type, zetasql::types::Int64Type());
  EXPECT_EQ(v2->columns()[0].name, "k2");
  EXPECT_THAT(absl::StripAsciiWhitespace(v2->body()),
              testing::StrEq("SELECT V1.k1 AS k2 FROM V1"));
}

TEST_F(ViewsTest, ViewRequiresInvokerSecurity) {
  EXPECT_THAT(CreateSchema({"CREATE VIEW V AS SELECT 1"}),
              StatusIs(error::ViewRequiresInvokerSecurity("V")));
}

TEST_F(ViewsTest, MissingDependency) {
  EXPECT_THAT(
      CreateSchema({"CREATE VIEW V SQL SECURITY INVOKER AS SELECT * FROM T"}),
      ::zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument,
                                  testing::HasSubstr("Table not found: T")));
}

TEST_F(ViewsTest, ViewAnalysisError_UnsupportedFunction) {
  EXPECT_THAT(CreateSchema({R"(
        CREATE TABLE T(
          k1 INT64,
          k2 STRING(MAX)
        ) PRIMARY KEY(k1)
      )",
                            R"(
      CREATE VIEW MYVIEW SQL SECURITY INVOKER AS SELECT
      APPROX_COUNT_DISTINCT(T.k1) AS C1 FROM T GROUP BY T.k1
      )"}),
              ::zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  testing::HasSubstr(
                      "Unsupported built-in function: APPROX_COUNT_DISTINCT")));
}

TEST_F(ViewsTest, ViewAnalysisError_UnsupportedFunction_PrunedColumn) {
  EXPECT_THAT(CreateSchema({R"(
        CREATE TABLE T(
          k1 INT64,
          k2 STRING(MAX)
        ) PRIMARY KEY(k1)
      )",
                            R"(
      CREATE VIEW MYVIEW SQL SECURITY INVOKER AS (
        SELECT S.k1 FROM
        (SELECT T.k1, APPROX_COUNT_DISTINCT(T.k2) FROM T GROUP BY T.k1) S
      ))"}),
              ::zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  testing::HasSubstr(
                      "Unsupported built-in function: APPROX_COUNT_DISTINCT")));
}

TEST_F(ViewsTest, ViewReplaceError) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
    CREATE TABLE T1(
      col1 INT64,
      col2 STRING(MAX)
    ) PRIMARY KEY(col1)
  )",
                                                  R"(
    CREATE VIEW V SQL SECURITY INVOKER AS
    SELECT T1.col1, T1.col2 FROM T1
  )"}));

  EXPECT_THAT(
      UpdateSchema(schema.get(),
                   {
                       "CREATE OR REPLACE VIEW V SQL SECURITY INVOKER AS "
                       "SELECT T1.col2 FROM T1",
                   }),
      ::zetasql_base::testing::StatusIs(
          absl::StatusCode::kUnimplemented,
          testing::HasSubstr(
              "`REPLACE` for INVOKER RIGHTS views is not supported")));
}

TEST_F(ViewsTest, StrictNameResolutionMode) {
  EXPECT_THAT(CreateSchema({R"(
    CREATE TABLE T1(
      col1 INT64,
      col2 STRING(MAX)
    ) PRIMARY KEY(col1)
  )",
                            R"(
    CREATE VIEW V SQL SECURITY INVOKER AS
    SELECT * FROM T1
  )"}),
              ::zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  testing::HasSubstr("SELECT * is not allowed")));
}

TEST_F(ViewsTest, DropViewBasic) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
    CREATE TABLE T1(
      col1 INT64,
      col2 STRING(MAX)
    ) PRIMARY KEY(col1)
  )",
                                                  R"(
    CREATE VIEW V SQL SECURITY INVOKER AS
    SELECT T1.col1, T1.col2 FROM T1
  )"}));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_chema,
                       UpdateSchema(schema.get(), {"DROP VIEW V"}));
  EXPECT_THAT(new_chema->views(), testing::IsEmpty());
  EXPECT_EQ(new_chema->FindView("V"), nullptr);
  EXPECT_NE(new_chema->FindTable("T1"), nullptr);
}

TEST_F(ViewsTest, ViewNotFound) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto schema, CreateSchema({
                       "CREATE VIEW V SQL SECURITY INVOKER AS SELECT 1 AS c",
                       "DROP VIEW V",
                   }));

  EXPECT_THAT(
      UpdateSchema(schema.get(), {"DROP VIEW V"}),
      ::zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound,
                                  testing::HasSubstr("View not found: V")));
}

TEST_F(ViewsTest, PrintViewBasic) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
    CREATE TABLE T1(
      col1 INT64,
    ) PRIMARY KEY(col1)
  )",
                                                  R"(
    CREATE VIEW V SQL SECURITY INVOKER AS
    SELECT T1.col1 FROM T1
  )"}));

  EXPECT_THAT(
      PrintDDLStatements(schema.get()),
      testing::ElementsAreArray(
          {"CREATE TABLE T1 (\n  col1 INT64,\n) PRIMARY KEY(col1)",
           "CREATE VIEW V SQL SECURITY INVOKER AS SELECT T1.col1 FROM T1"}));
}

}  // namespace

}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
