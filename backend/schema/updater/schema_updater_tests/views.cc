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
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "google/spanner/admin/database/v1/common.pb.h"
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
using ::testing::StrEq;

namespace {

// For the following tests, a custom PG DDL statement is required as translating
// queries from GSQL to PG is not supported in tests.
using database_api::DatabaseDialect::POSTGRESQL;

class ViewsTest : public SchemaUpdaterTest {
 public:
  ViewsTest()
      : flag_setter_({
            .enable_views = true,
        }) {}
  const ScopedEmulatorFeatureFlagsSetter flag_setter_;
};

INSTANTIATE_TEST_SUITE_P(
    SchemaUpdaterPerDialectTests, ViewsTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<ViewsTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(ViewsTest, Basic) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
    CREATE TABLE t(
      col1 bigint primary key,
      col2 varchar
    )
  )",
                                       R"(
    CREATE OR REPLACE VIEW "MyView" SQL SECURITY INVOKER AS SELECT T.col1, T.col2 FROM T
  )"},
                                      database_api::DatabaseDialect::POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
      CREATE TABLE T(
        col1 INT64,
        col2 STRING(MAX)
      ) PRIMARY KEY(col1)
    )",
                                               R"(
      CREATE OR REPLACE VIEW `MyView` SQL SECURITY INVOKER AS
      SELECT T.col1, T.col2 FROM T
    )"}));
  }

  auto t = schema->FindTable("T");
  ASSERT_NE(t, nullptr);

  auto v = schema->FindView("Myview");
  EXPECT_NE(v, nullptr);
  EXPECT_EQ(schema->FindViewCaseSensitive("MyView"), v);
  EXPECT_THAT(schema->views(), testing::ElementsAreArray({v}));
  EXPECT_EQ(v->Name(), "MyView");
  EXPECT_EQ(v->columns().size(), 2);
  if (GetParam() == POSTGRESQL) {
    ASSERT_TRUE(v->body_origin().has_value());
    EXPECT_THAT(absl::StripAsciiWhitespace(*v->body_origin()),
                StrEq("SELECT col1, col2 FROM t"));
    EXPECT_THAT(absl::StripAsciiWhitespace(v->body()),
                StrEq("SELECT t_3.a_1 AS col1, t_3.a_2 AS col2 FROM (SELECT "
                      "t.col1 AS a_1, t.col2 AS a_2 FROM t) AS t_3"));
  } else {
    EXPECT_THAT(absl::StripAsciiWhitespace(v->body()),
                StrEq("SELECT T.col1, T.col2 FROM T"));
  }
  EXPECT_THAT(
      v->dependencies(),
      testing::UnorderedElementsAreArray((std::vector<const SchemaNode*>{
          t, t->FindColumn("col1"), t->FindColumn("col2")})));
  EXPECT_THAT(v->security(), testing::Eq(View::SqlSecurity::INVOKER));
}

TEST_P(ViewsTest, OrderBy) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
    CREATE TABLE t(
      col1 bigint primary key,
      col2 varchar
    )
  )",
                                       R"(
    CREATE OR REPLACE VIEW "MyView" SQL SECURITY INVOKER AS SELECT T.col1, T.col2 FROM T ORDER BY T.col2
  )"},
                                      database_api::DatabaseDialect::POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
      CREATE TABLE T(
        col1 INT64,
        col2 STRING(MAX)
      ) PRIMARY KEY(col1)
    )",
                                               R"(
      CREATE OR REPLACE VIEW `MyView` SQL SECURITY INVOKER AS
      SELECT T.col1, T.col2 FROM T ORDER BY T.col2
    )"}));
  }

  auto t = schema->FindTable("T");
  ASSERT_NE(t, nullptr);

  auto v = schema->FindView("Myview");
  EXPECT_NE(v, nullptr);
  if (GetParam() == POSTGRESQL) {
    ASSERT_TRUE(v->body_origin().has_value());
    EXPECT_THAT(absl::StripAsciiWhitespace(*v->body_origin()),
                StrEq("SELECT col1, col2 FROM t ORDER BY col2"));
    EXPECT_THAT(absl::StripAsciiWhitespace(v->body()),
                StrEq("SELECT t_3.a_1 AS col1, t_3.a_2 AS col2 FROM (SELECT "
                      "t.col1 AS a_1, t.col2 AS a_2 FROM t) AS t_3 ORDER BY "
                      "t_3.a_2 NULLS LAST"));
  } else {
    EXPECT_THAT(absl::StripAsciiWhitespace(v->body()),
                StrEq("SELECT T.col1, T.col2 FROM T ORDER BY T.col2"));
  }
}

TEST_P(ViewsTest, IndexDependency) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
    CREATE TABLE T(
      col1 INT64,
      col2 STRING(MAX)
    ) PRIMARY KEY(col1)
  )",
                                                  R"(
    CREATE INDEX Idx ON T(col2)
  )",
                                                  R"(
    CREATE OR REPLACE VIEW `MyView` SQL SECURITY INVOKER AS
    SELECT T.col1, T.col2 FROM T@{force_index=Idx}
  )"}));
  auto t = schema->FindTable("T");
  ASSERT_NE(t, nullptr);
  auto idx = t->FindIndex("Idx");
  ASSERT_NE(idx, nullptr);

  auto v = schema->FindView("Myview");
  EXPECT_NE(v, nullptr);
  EXPECT_EQ(v->Name(), "MyView");
  EXPECT_EQ(v->columns().size(), 2);
  EXPECT_THAT(absl::StripAsciiWhitespace(v->body()),
              testing::StrEq("SELECT T.col1, T.col2 FROM T@{force_index=Idx}"));
  EXPECT_THAT(
      v->dependencies(),
      testing::UnorderedElementsAreArray((std::vector<const SchemaNode*>{
          t, idx, t->FindColumn("col1"), t->FindColumn("col2")})));
  EXPECT_THAT(v->security(), testing::Eq(View::SqlSecurity::INVOKER));
}

TEST_P(ViewsTest, MultipleTableDependencies) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
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
  EXPECT_THAT(v->dependencies(),
              testing::UnorderedElementsAreArray(std::vector<const SchemaNode*>{
                  t1,
                  t1->FindColumn("col1"),
                  t2,
                  t2->FindColumn("k1"),
              }));
}

TEST_P(ViewsTest, ViewDependsOnView) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
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

TEST_P(ViewsTest, ViewRequiresInvokerSecurity) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  EXPECT_THAT(CreateSchema({"CREATE VIEW V AS SELECT 1"}),
              StatusIs(error::ViewRequiresInvokerSecurity("V")));
}

TEST_P(ViewsTest, MissingDependency) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  EXPECT_THAT(
      CreateSchema({"CREATE VIEW V SQL SECURITY INVOKER AS SELECT * FROM T"}),
      ::zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument,
                                  testing::HasSubstr("Table not found: T")));
}

TEST_P(ViewsTest, ViewAnalysisError_UnsupportedFunction) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
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

TEST_P(ViewsTest, ViewAnalysisError_UnsupportedFunction_PrunedColumn) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
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

TEST_P(ViewsTest, ViewReplace) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
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
  auto view = schema->FindView("V");
  EXPECT_EQ(view->columns().size(), 2);
  EXPECT_EQ(view->columns()[0].name, "col1");
  EXPECT_EQ(view->columns()[0].type, zetasql::types::Int64Type());

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto new_schema,
      UpdateSchema(schema.get(),
                   {
                       "CREATE OR REPLACE VIEW V SQL SECURITY INVOKER AS "
                       "SELECT T1.col2 FROM T1",
                   }));
  EXPECT_EQ(new_schema->views().size(), 1);
  auto new_view = new_schema->FindView("V");
  EXPECT_NE(new_view, nullptr);
  EXPECT_EQ(new_view->columns().size(), 1);
  EXPECT_EQ(new_view->columns()[0].name, "col2");
  EXPECT_EQ(new_view->columns()[0].type, zetasql::types::StringType());
}

TEST_P(ViewsTest, ViewReplace_NoCircularDependency) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
    CREATE VIEW v1 SQL SECURITY INVOKER AS
    SELECT 1 AS k1
  )",
                                                  R"(
    CREATE VIEW V2 SQL SECURITY INVOKER AS
    SELECT V1.k1 AS k2 FROM V1
  )"}));

  EXPECT_THAT(
      UpdateSchema(schema.get(),
                   {
                       "CREATE OR REPLACE VIEW V1 SQL SECURITY INVOKER AS "
                       "SELECT V2.k2 FROM V2",
                   }),
      ::zetasql_base::testing::StatusIs(
          absl::StatusCode::kFailedPrecondition,
          testing::HasSubstr("Cannot replace VIEW `v1` because new definition "
                             "is recursive.")));
}

TEST_P(ViewsTest, ViewReplaceDependentViewInvalidDefinition) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
    CREATE VIEW v1 SQL SECURITY INVOKER AS
    SELECT 1 AS k1, 2 AS k2
  )",
                                                  R"(
    CREATE VIEW V2 SQL SECURITY INVOKER AS
    SELECT V1.k2 AS k2 FROM V1
  )"}));

  EXPECT_THAT(
      UpdateSchema(schema.get(),
                   {
                       "CREATE OR REPLACE VIEW V1 SQL SECURITY INVOKER AS "
                       "SELECT 1 AS k1",
                   }),
      ::zetasql_base::testing::StatusIs(
          absl::StatusCode::kFailedPrecondition,
          testing::ContainsRegex(
              "Cannot alter VIEW .* The new definition causes "
              "the definition of VIEW .* to become invalid with the "
              "following diagnostic message: Name k2 not found inside V1")));
}

TEST_P(ViewsTest, ViewReplaceDependentViewIncompatibleTypeChange) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
    CREATE VIEW v1 SQL SECURITY INVOKER AS
    SELECT 1 AS k1, 2 AS k2
  )",
                                                  R"(
    CREATE TABLE T1 (col1 STRING(MAX)) PRIMARY KEY()
  )",
                                                  R"(
    CREATE VIEW V2 SQL SECURITY INVOKER AS
    SELECT V1.k2 AS a1, T1.col1 AS a2 FROM V1, T1
  )"}));

  EXPECT_THAT(
      UpdateSchema(schema.get(),
                   {
                       "CREATE OR REPLACE VIEW V1 SQL SECURITY INVOKER AS "
                       "SELECT 1 AS k1, CAST('2' AS STRING) AS k2",
                   }),
      ::zetasql_base::testing::StatusIs(
          absl::StatusCode::kFailedPrecondition,
          testing::ContainsRegex(
              "Cannot alter VIEW `V1`. Action would implicitly change the "
              "type of an output column for VIEW `V2` from `INT64` to "
              "`STRING`")));

  EXPECT_THAT(UpdateSchema(schema.get(),
                           {
                               "ALTER TABLE T1 ALTER COLUMN col1 BYTES(MAX)",
                           }),
              ::zetasql_base::testing::StatusIs(
                  absl::StatusCode::kFailedPrecondition,
                  testing::ContainsRegex(
                      "Cannot alter column `T1.col1`. Action would implicitly "
                      "change the "
                      "type of an output column for VIEW `V2` from `STRING` to "
                      "`BYTES`")));
}

TEST_P(ViewsTest, ViewReplace_DependentViewCompatibleTypeChange) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
    CREATE VIEW v1 SQL SECURITY INVOKER AS
    SELECT 'a' AS k1
  )",
                                                  R"(
    CREATE TABLE T1 (col1 STRING(MAX)) PRIMARY KEY()
  )",
                                                  R"(
    CREATE VIEW V2 SQL SECURITY INVOKER AS
    SELECT CAST(V1.k1 AS STRING) AS c1, CAST(T1.col1 AS STRING) AS col1
    FROM V1, T1
  )"}));

  // Explicit cast on the output of V1 ensures that the dependent view V2
  // is still valid after the output type of V1 is changed.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto new_schema,
      UpdateSchema(schema.get(),
                   {
                       "CREATE OR REPLACE VIEW V1 SQL SECURITY INVOKER AS "
                       "SELECT 1 AS k1",
                       "ALTER TABLE T1 ALTER COLUMN col1 BYTES(MAX)",
                   }));
  auto v2 = new_schema->FindView("V2");
  EXPECT_NE(v2, nullptr);
  EXPECT_EQ(v2->columns().size(), 2);
  EXPECT_EQ(v2->columns()[0].type, zetasql::types::StringType());
  EXPECT_EQ(v2->columns()[1].type, zetasql::types::StringType());
}

TEST_P(ViewsTest, StrictNameResolutionMode) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
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

TEST_P(ViewsTest, DropViewBasic) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
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

TEST_P(ViewsTest, UnnamedColumnView) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto schema, CreateSchema({
                       "CREATE VIEW V SQL SECURITY INVOKER AS SELECT 1 AS c",
                   }));
  auto v = schema->FindView("V");
  ASSERT_NE(v, nullptr);
  EXPECT_EQ(v->columns().size(), 1);
  EXPECT_EQ(v->columns()[0].name, "c");
}

TEST_P(ViewsTest, ViewNotFound) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
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

TEST_P(ViewsTest, ViewIfExistsNotFound) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto schema, CreateSchema({
                       "CREATE VIEW V SQL SECURITY INVOKER AS SELECT 1 as c",
                       "DROP VIEW V",
                   }));

  ZETASQL_ASSERT_OK(UpdateSchema(schema.get(), {"DROP VIEW IF EXISTS V"}));
}

TEST_P(ViewsTest, ViewIfExistsNotFoundPG) {
  if (GetParam() != POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto schema,
      CreateSchema(
          {R"(CREATE VIEW users_view SQL SECURITY INVOKER AS SELECT 1)",
           R"(DROP VIEW users_view)"},
          database_api::DatabaseDialect::POSTGRESQL,
          /*use_gsql_to_pg_translation=*/false));

  ZETASQL_ASSERT_OK(UpdateSchema(schema.get(), {"DROP VIEW IF EXISTS users_view"},
                         database_api::DatabaseDialect::POSTGRESQL,
                         /*use_gsql_to_pg_translation=*/false));
}

TEST_P(ViewsTest, PrintViewBasic) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
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
      zetasql_base::testing::IsOkAndHolds(testing::ElementsAreArray(
          {"CREATE TABLE T1 (\n  col1 INT64,\n) PRIMARY KEY(col1)",
           "CREATE VIEW V SQL SECURITY INVOKER AS SELECT T1.col1 FROM T1"})));
}

TEST_P(ViewsTest, DropView_Dependencies) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"(
    CREATE TABLE T1(
      col1 INT64,
      col2 INT64
    ) PRIMARY KEY(col1)
  )",
                                        R"(
    CREATE VIEW V1 SQL SECURITY INVOKER AS
    SELECT T1.col2 AS k1 FROM T1
  )",
                                        R"(
    CREATE OR REPLACE VIEW V2 SQL SECURITY INVOKER AS
    SELECT V1.k1 AS k2 FROM V1)",
                                    }));

  EXPECT_THAT(UpdateSchema(schema.get(), {"DROP VIEW V1"}),
              ::zetasql_base::testing::StatusIs(
                  absl::StatusCode::kFailedPrecondition,
                  testing::HasSubstr("Cannot drop VIEW `V1` on which there "
                                     "are dependent views: V2.")));

  EXPECT_THAT(UpdateSchema(schema.get(), {"DROP TABLE T1"}),
              ::zetasql_base::testing::StatusIs(
                  absl::StatusCode::kFailedPrecondition,
                  testing::HasSubstr("Cannot drop TABLE `T1` on which there "
                                     "are dependent views: V1.")));

  EXPECT_THAT(UpdateSchema(schema.get(), {"ALTER TABLE T1 DROP COLUMN col2"}),
              ::zetasql_base::testing::StatusIs(
                  absl::StatusCode::kFailedPrecondition,
                  testing::HasSubstr("Cannot drop column `col2` on which there "
                                     "are dependent views: V1.")));
}

TEST_P(ViewsTest, DropViewIsCaseSensitive) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
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

  EXPECT_THAT(UpdateSchema(schema.get(), {"DROP VIEW v"}),
              StatusIs(error::ViewNotFound("v")));
}
}  // namespace

}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
