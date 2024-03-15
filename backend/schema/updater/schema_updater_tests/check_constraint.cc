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

#include "backend/schema/catalog/check_constraint.h"

#include <iterator>
#include <memory>
#include <string>
#include <vector>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/types/span.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/updater/schema_updater_tests/base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {

namespace {

// For the following tests, a custom PG DDL statement is required as translating
// expressions from GSQL to PG is not supported in tests.
using database_api::DatabaseDialect::GOOGLE_STANDARD_SQL;
using database_api::DatabaseDialect::POSTGRESQL;

TEST_P(SchemaUpdaterTest, CheckConstraintBasic) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        schema,
        CreateSchema(
            {R"(
            CREATE TABLE "T" (
              "K" bigint PRIMARY KEY,
              "V" bigint,
              CONSTRAINT "C1" CHECK("K" > 0)
            ))",
             R"(ALTER TABLE "T" ADD CONSTRAINT "C2" CHECK("K" + "V" > 0))"},
            /*proto_descriptor_bytes=*/"",
            POSTGRESQL,
            /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        schema,
        CreateSchema({
            "CREATE TABLE T ("
            "  K INT64,"
            "  V INT64,"
            "  CONSTRAINT C1 CHECK(K > 0)"  // Crash OK
            ") PRIMARY KEY (K)",
            "ALTER TABLE T ADD CONSTRAINT C2 CHECK(K + V > 0)"  // Crash OK
        }));
  }

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
  if (GetParam() == POSTGRESQL) {
    EXPECT_EQ(check2->expression(), "(K + V) > 0");
  } else {
    EXPECT_EQ(check2->expression(), "K + V > 0");
  }

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

std::vector<std::string> SchemaForCaseSensitivityTests(
    database_api::DatabaseDialect dialect) {
  if (dialect == POSTGRESQL) {
    return {
        "CREATE TABLE T ("
        "  K bigint primary key,"
        "  V bigint,"
        "  CONSTRAINT C1 CHECK(K > 0)"  // Crash OK
        ")",
    };
  }
  return {
      "CREATE TABLE T ("
      "  K INT64,"
      "  V INT64,"
      "  CONSTRAINT C1 CHECK(K > 0)"  // Crash OK
      ") PRIMARY KEY (K)",
  };
}

TEST_P(SchemaUpdaterTest, CheckConstraintColumnNameIsCaseInsensitive) {
  std::string add_constraint_ddl =
      "ALTER TABLE T ADD CONSTRAINT C2 CHECK(v > 0)";  // Crash OK
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                         CreateSchema(SchemaForCaseSensitivityTests(POSTGRESQL),
                                      /*proto_descriptor_bytes=*/"",
                                      POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
    ZETASQL_EXPECT_OK(UpdateSchema(schema.get(), {add_constraint_ddl},
                           /*proto_descriptor_bytes=*/"",
                           POSTGRESQL,
                           /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        auto schema,
        CreateSchema(SchemaForCaseSensitivityTests(GOOGLE_STANDARD_SQL)));
    ZETASQL_EXPECT_OK(UpdateSchema(schema.get(), {add_constraint_ddl}));
  }
}

TEST_P(SchemaUpdaterTest, CheckConstraintConstraintNameIsCaseInsensitive) {
  std::string drop_constraint_ddl = "ALTER TABLE T DROP CONSTRAINT c1";
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                         CreateSchema(SchemaForCaseSensitivityTests(POSTGRESQL),
                                      /*proto_descriptor_bytes=*/"",
                                      POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
    ZETASQL_EXPECT_OK(UpdateSchema(schema.get(), {drop_constraint_ddl},
                           /*proto_descriptor_bytes=*/"",
                           POSTGRESQL,
                           /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        auto schema,
        CreateSchema(SchemaForCaseSensitivityTests(GOOGLE_STANDARD_SQL)));
    ZETASQL_EXPECT_OK(UpdateSchema(schema.get(), {drop_constraint_ddl}));
  }
}

}  // namespace

}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
