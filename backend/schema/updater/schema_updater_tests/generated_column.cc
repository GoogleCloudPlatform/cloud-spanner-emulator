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

#include "google/spanner/admin/database/v1/common.pb.h"
#include "zetasql/public/type.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/types/span.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/updater/schema_updater_tests/base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {

using ::testing::HasSubstr;
using ::zetasql_base::testing::StatusIs;
// For the following tests, a custom PG DDL statement is required as translating
// expressions from GSQL to PG is not supported in tests.
using database_api::DatabaseDialect::GOOGLE_STANDARD_SQL;
using database_api::DatabaseDialect::POSTGRESQL;

TEST_P(SchemaUpdaterTest, GeneratedColumnBasic) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
      create table "T" (
        "K" bigint primary key,
        "V" varchar(10),
        "G1" bigint not null generated always as ("K" + LENGTH("V")) stored
      )
    )"},
                                      /*proto_descriptor_bytes=*/"",
                                      database_api::DatabaseDialect::POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
        CREATE TABLE T (
          K INT64 NOT NULL,
          V STRING(10),
          G1 INT64 NOT NULL AS (k + LENGTH(v)) STORED,
        ) PRIMARY KEY (K)
      )"}));
  }

  const Table* table = schema->FindTable("T");
  ASSERT_NE(table, nullptr);
  const Column* col = table->FindColumn("V");
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Name(), "V");
  EXPECT_FALSE(col->is_generated());
  EXPECT_FALSE(col->expression().has_value());
  EXPECT_FALSE(col->original_expression().has_value());

  col = table->FindColumn("G1");
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Name(), "G1");
  EXPECT_EQ(col->GetType()->kind(), zetasql::TYPE_INT64);
  EXPECT_FALSE(col->is_nullable());
  EXPECT_TRUE(col->is_generated());
  EXPECT_FALSE(col->has_default_value());
  EXPECT_TRUE(col->expression().has_value());
  if (GetParam() == POSTGRESQL) {
    EXPECT_EQ(col->expression().value(), "K + (LENGTH(V))");
    EXPECT_EQ(col->original_expression(), "(\"K\" + length(\"V\"))");
  } else {
    EXPECT_EQ(col->expression().value(), "(k + LENGTH(v))");
    EXPECT_FALSE(col->original_expression().has_value());
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
  get_column_names(col->dependent_columns(), &dependent_column_names);
  EXPECT_THAT(dependent_column_names,
              testing::UnorderedElementsAreArray({"K", "V"}));
}

TEST_P(SchemaUpdaterTest, AlterTableReferenceAnotherGeneratedColumn) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    EXPECT_THAT(CreateSchema({R"(
      create table "T" (
        "K" bigint primary key,
        "V" varchar(10),
        "G1" bigint not null generated always as ("K" + LENGTH("V")) stored
      )
    )",
                              R"(
      alter table "T" add column "G2" bigint generated always as ("G1" + "G1") STORED
    )"},
                             /*proto_descriptor_bytes=*/"",
                             database_api::DatabaseDialect::POSTGRESQL,
                             /*use_gsql_to_pg_translation=*/false),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("A generated column \"G2\" cannot reference "
                                   "another generated column \"G1\".")));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
        CREATE TABLE T (
          K INT64 NOT NULL,
          V STRING(10),
          G1 INT64 NOT NULL AS (k + LENGTH(v)) STORED,
        ) PRIMARY KEY (K)
      )",
                                               R"(
        ALTER TABLE T ADD COLUMN G2 INT64 AS (G1 + G1) STORED
      )"}));

    auto get_column_names = [](absl::Span<const Column* const> columns,
                               std::vector<std::string>* column_names) {
      column_names->clear();
      column_names->reserve(columns.size());
      for (const Column* col : columns) {
        column_names->push_back(col->Name());
      }
    };
    const Table* table = schema->FindTable("T");
    ASSERT_NE(table, nullptr);
    const Column* col = table->FindColumn("G2");
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->Name(), "G2");
    EXPECT_EQ(col->GetType()->kind(), zetasql::TYPE_INT64);
    EXPECT_TRUE(col->is_nullable());
    EXPECT_TRUE(col->is_generated());
    EXPECT_FALSE(col->has_default_value());
    EXPECT_TRUE(col->expression().has_value());
    if (GetParam() == POSTGRESQL) {
      EXPECT_EQ(col->expression().value(), "G1 + G1");
      EXPECT_EQ(col->original_expression(), "(\"G1\" + \"G1\")");
    } else {
      EXPECT_EQ(col->expression().value(), "(G1 + G1)");
      EXPECT_FALSE(col->original_expression().has_value());
    }
    std::vector<std::string> dependent_column_names;
    get_column_names(col->dependent_columns(), &dependent_column_names);
    EXPECT_THAT(dependent_column_names,
                testing::UnorderedElementsAreArray({"G1"}));
  }
}

TEST_P(SchemaUpdaterTest, CreateTableReferenceAnotherGeneratedColumn) {
  if (GetParam() == POSTGRESQL) {
    EXPECT_THAT(CreateSchema({R"(
      CREATE TABLE T (
        K bigint primary key,
        V bigint,
        G bigint generated always as (K + V) stored,
        H bigint generated always as (G + 1) stored
      )
    )"},
                             /*proto_descriptor_bytes=*/"",
                             database_api::DatabaseDialect::POSTGRESQL,
                             /*use_gsql_to_pg_translation=*/false),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("A generated column \"h\" cannot reference "
                                   "another generated column \"g\".")));
  } else {
    ZETASQL_EXPECT_OK(CreateSchema({R"(
        CREATE TABLE T (
          K INT64 NOT NULL,
          V INT64,
          G INT64 AS (K + V) STORED,
          H INT64 AS (G + 1) STORED,
        ) PRIMARY KEY (K)
      )"}));
  }
}

TEST_P(SchemaUpdaterTest, CannotCreateTableAddNonStoredGeneratedColumn) {
  if (GetParam() == POSTGRESQL) {
    EXPECT_THAT(CreateSchema({R"(
      CREATE TABLE T (
        K bigint primary key,
        V bigint,
        G bigint generated always as (K + V)
      )
    )"},
                             /*proto_descriptor_bytes=*/"",
                             database_api::DatabaseDialect::POSTGRESQL,
                             /*use_gsql_to_pg_translation=*/false),
                zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
  } else {
    ZETASQL_EXPECT_OK(CreateSchema({R"(
        CREATE TABLE T (
          K INT64 NOT NULL,
          V INT64,
          G INT64 AS (K + V),
        ) PRIMARY KEY (K)
      )"}));
  }
}

TEST_P(SchemaUpdaterTest, CannotAlterTableAddNonStoredGeneratedColumn) {
  if (GetParam() == POSTGRESQL) {
    EXPECT_THAT(CreateSchema({R"(
        CREATE TABLE T (
          K bigint primary key,
          V bigint
        )
      )",
                              R"(
        ALTER TABLE T ADD COLUMN G bigint generated always as (K + V)
      )"},
                             /*proto_descriptor_bytes=*/"",
                             database_api::DatabaseDialect::POSTGRESQL,
                             /*use_gsql_to_pg_translation=*/false),
                zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
  } else {
    ZETASQL_EXPECT_OK(CreateSchema({R"(
        CREATE TABLE T (
          K INT64 NOT NULL,
          V INT64,
        ) PRIMARY KEY (K)
      )",
                            R"(
        ALTER TABLE T ADD COLUMN G INT64 AS (K + V)
      )"}));
  }
}

TEST_P(SchemaUpdaterTest, CannotAlterTableAlterColumnToNonStoredGenerated) {
  if (GetParam() == POSTGRESQL) {
    EXPECT_THAT(CreateSchema({R"(
        CREATE TABLE T (
          K bigint primary key,
          V bigint,
          G bigint
        )
      )",
                              R"(
        ALTER TABLE T ALTER COLUMN G bigint generated always as (K + V)
      )"},
                             /*proto_descriptor_bytes=*/"",
                             database_api::DatabaseDialect::POSTGRESQL,
                             /*use_gsql_to_pg_translation=*/false),
                zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
  } else {
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
            absl::StatusCode::kInvalidArgument,
            HasSubstr("Cannot convert column `T.G` to a generated column.")));
  }
}

std::vector<std::string> SchemaForCaseSensitivityTests(
    database_api::DatabaseDialect dialect) {
  if (dialect == POSTGRESQL) {
    return {
        R"sql(
                CREATE TABLE T (
                  K bigint primary key,
                  V bigint
                )
            )sql",
    };
  }
  return {
      R"sql(
                CREATE TABLE T (
                  K INT64 NOT NULL,
                  V INT64,
                ) PRIMARY KEY (K)
            )sql",
  };
}

TEST_P(SchemaUpdaterTest, StoredColumnExpressionIsCaseInsensitive) {
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                         CreateSchema(SchemaForCaseSensitivityTests(POSTGRESQL),
                                      /*proto_descriptor_bytes=*/"",
                                      POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));

    ZETASQL_EXPECT_OK(UpdateSchema(schema.get(), {R"(
        ALTER TABLE T ADD COLUMN G bigint generated always as (k + v) STORED
      )"},
                           /*proto_descriptor_bytes=*/"",
                           POSTGRESQL,
                           /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        auto schema,
        CreateSchema(SchemaForCaseSensitivityTests(GOOGLE_STANDARD_SQL)));

    ZETASQL_EXPECT_OK(UpdateSchema(schema.get(), {R"(
        ALTER TABLE T ADD COLUMN G INT64 AS (k + v) STORED
      )"}));
  }
}

TEST_P(SchemaUpdaterTest, StoredColumnNameIsCaseSensitive) {
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                         CreateSchema(SchemaForCaseSensitivityTests(POSTGRESQL),
                                      /*proto_descriptor_bytes=*/"",
                                      POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
    ZETASQL_EXPECT_OK(UpdateSchema(schema.get(), {R"(
        ALTER TABLE T ADD COLUMN G bigint generated always as (k + v) STORED
      )"},
                           /*proto_descriptor_bytes=*/"",
                           POSTGRESQL,
                           /*use_gsql_to_pg_translation=*/false));
    // TODO: find out why using T gets the "Table not found: T"
    // error.
    EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      ALTER TABLE t DROP COLUMN g
    )"}),
                StatusIs(absl::StatusCode::kNotFound,
                         HasSubstr("Column not found in table t: g")));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        auto schema,
        CreateSchema(SchemaForCaseSensitivityTests(GOOGLE_STANDARD_SQL)));
    ZETASQL_EXPECT_OK(UpdateSchema(schema.get(), {R"(
        ALTER TABLE T ADD COLUMN G INT64 AS (k + v) STORED
      )"}));
    EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      ALTER TABLE T DROP COLUMN g
    )"}),
                StatusIs(absl::StatusCode::kNotFound,
                         HasSubstr("Column not found in table T: g")));
  }
}

}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
