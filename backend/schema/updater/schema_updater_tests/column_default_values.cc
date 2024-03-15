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
#include "zetasql/public/options.pb.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "backend/schema/updater/schema_updater_tests/base.h"
#include "third_party/spanner_pg/datatypes/extended/pg_numeric_type.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {

// For the following tests, a custom PG DDL statement is required as translating
// expressions from GSQL to PG is not supported in tests.
using database_api::DatabaseDialect::POSTGRESQL;

TEST_P(SchemaUpdaterTest, NonKeyColumns) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        schema,
        CreateSchema(
            {
                R"(
          create table "T" (
            "K" bigint primary key,
            "V" varchar(10),
            "D1" bigint not null default (1)
          )
        )",
                "alter table \"T\" add column \"D2\" bigint default (2)"},
            /*proto_descriptor_bytes=*/"",
            database_api::DatabaseDialect::POSTGRESQL,
            /*use_gsql_to_pg_translation=*/false));
  } else {
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
  }

  const Table* table = schema->FindTable("T");
  ASSERT_NE(table, nullptr);
  const Column* col = table->FindColumn("V");
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Name(), "V");
  EXPECT_FALSE(col->is_generated());
  EXPECT_FALSE(col->has_default_value());
  EXPECT_FALSE(col->expression().has_value());
  EXPECT_FALSE(col->original_expression().has_value());

  col = table->FindColumn("D1");
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Name(), "D1");
  EXPECT_EQ(col->GetType()->kind(), zetasql::TYPE_INT64);
  EXPECT_FALSE(col->is_nullable());
  EXPECT_FALSE(col->is_generated());
  EXPECT_TRUE(col->has_default_value());
  EXPECT_TRUE(col->expression().has_value());
  EXPECT_EQ(col->expression().value(), "1");
  if (GetParam() == POSTGRESQL) {
    EXPECT_EQ(col->original_expression(), "'1'::bigint");
  } else {
    EXPECT_FALSE(col->original_expression().has_value());
  }
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
  if (GetParam() == POSTGRESQL) {
    EXPECT_EQ(col->original_expression(), "'2'::bigint");
  } else {
    EXPECT_FALSE(col->original_expression().has_value());
  }
  EXPECT_EQ(col->dependent_columns().size(), 0);
}

TEST_P(SchemaUpdaterTest, FunctionAsDefault) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
        CREATE TABLE "T" (
          "K" bigint primary key,
          "V" timestamptz DEFAULT (NOW())
        )
      )"},
                                      /*proto_descriptor_bytes=*/"",
                                      database_api::DatabaseDialect::POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
          CREATE TABLE T (
            K INT64 NOT NULL,
            V TIMESTAMP DEFAULT (CURRENT_TIMESTAMP())
          ) PRIMARY KEY(K)
        )"}));
  }

  const Table* table = schema->FindTable("T");
  ASSERT_NE(table, nullptr);
  const Column* col = table->FindColumn("V");
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Name(), "V");
  EXPECT_FALSE(col->is_generated());
  EXPECT_TRUE(col->has_default_value());
  EXPECT_TRUE(col->expression().has_value());
  if (GetParam() == POSTGRESQL) {
    EXPECT_EQ(col->original_expression(), "now()");
  } else {
    EXPECT_FALSE(col->original_expression().has_value());
  }
  EXPECT_EQ(col->expression().value(), "CURRENT_TIMESTAMP()");
}

TEST_P(SchemaUpdaterTest, KeyColumn) {
  std::unique_ptr<const Schema> schema;
  // The DDL translation from GSQL to PG does not support expressions. Skip it
  // for now and a hand-written PG DDL will be added.
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        schema,
        CreateSchema({R"(
        create table "T" (
          "K1" bigint not null,
          "K2" bigint default (20),
          "K3" bigint,
          "V" varchar(10),
          primary key ("K1", "K2", "K3")
        )
      )",
                      "alter table \"T\" alter column \"K3\" set default (30)"},
                     /*proto_descriptor_bytes=*/"",
                     database_api::DatabaseDialect::POSTGRESQL,
                     /*use_gsql_to_pg_translation=*/false));
  } else {
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
  }

  const Table* table = schema->FindTable("T");
  ASSERT_NE(table, nullptr);
  const Column* col = table->FindColumn("K1");
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Name(), "K1");
  EXPECT_FALSE(col->is_generated());
  EXPECT_FALSE(col->has_default_value());
  EXPECT_FALSE(col->expression().has_value());
  EXPECT_FALSE(col->original_expression().has_value());

  col = table->FindColumn("K2");
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Name(), "K2");
  EXPECT_EQ(col->GetType()->kind(), zetasql::TYPE_INT64);
  EXPECT_FALSE(col->is_generated());
  EXPECT_TRUE(col->has_default_value());
  EXPECT_TRUE(col->expression().has_value());
  EXPECT_EQ(col->expression().value(), "20");
  if (GetParam() == POSTGRESQL) {
    EXPECT_EQ(col->original_expression(), "'20'::bigint");
  } else {
    EXPECT_FALSE(col->original_expression().has_value());
  }
  EXPECT_EQ(col->dependent_columns().size(), 0);

  col = table->FindColumn("K3");
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Name(), "K3");
  EXPECT_EQ(col->GetType()->kind(), zetasql::TYPE_INT64);
  EXPECT_FALSE(col->is_generated());
  EXPECT_TRUE(col->has_default_value());
  EXPECT_TRUE(col->expression().has_value());
  EXPECT_EQ(col->expression().value(), "30");
  if (GetParam() == POSTGRESQL) {
    EXPECT_EQ(col->original_expression(), "'30'::bigint");
  } else {
    EXPECT_FALSE(col->original_expression().has_value());
  }
  EXPECT_EQ(col->dependent_columns().size(), 0);
}

TEST_P(SchemaUpdaterTest, SetDropDefault) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        schema,
        CreateSchema(
            {
                R"(
        create table "T" (
          "K1" bigint not null,
          "K2" bigint default (20),
          "K3" bigint,
          "V" varchar(10) default ('Hello'),
          primary key ("K1", "K2", "K3")
        )
      )",
                "alter table \"T\" alter column \"K3\" set default (30)",
                "alter table \"T\" alter column \"K2\" set default (2)",
                "alter table \"T\" alter column \"V\" drop default"},
            /*proto_descriptor_bytes=*/"",
            database_api::DatabaseDialect::POSTGRESQL,
            /*use_gsql_to_pg_translation=*/false));
  } else {
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
  }

  const Table* table = schema->FindTable("T");
  ASSERT_NE(table, nullptr);
  const Column* col = table->FindColumn("K1");
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Name(), "K1");
  EXPECT_FALSE(col->is_generated());
  EXPECT_FALSE(col->has_default_value());
  EXPECT_FALSE(col->expression().has_value());
  EXPECT_FALSE(col->original_expression().has_value());

  col = table->FindColumn("K2");
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Name(), "K2");
  EXPECT_EQ(col->GetType()->kind(), zetasql::TYPE_INT64);
  EXPECT_FALSE(col->is_generated());
  EXPECT_TRUE(col->has_default_value());
  EXPECT_TRUE(col->expression().has_value());
  EXPECT_EQ(col->expression().value(), "2");
  if (GetParam() == POSTGRESQL) {
    EXPECT_EQ(col->original_expression(), "'2'::bigint");
  } else {
    EXPECT_FALSE(col->original_expression().has_value());
  }
  EXPECT_EQ(col->dependent_columns().size(), 0);

  col = table->FindColumn("K3");
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Name(), "K3");
  EXPECT_EQ(col->GetType()->kind(), zetasql::TYPE_INT64);
  EXPECT_FALSE(col->is_generated());
  EXPECT_TRUE(col->has_default_value());
  EXPECT_TRUE(col->expression().has_value());
  EXPECT_EQ(col->expression().value(), "30");
  if (GetParam() == POSTGRESQL) {
    EXPECT_EQ(col->original_expression(), "'30'::bigint");
  } else {
    EXPECT_FALSE(col->original_expression().has_value());
  }
  EXPECT_EQ(col->dependent_columns().size(), 0);

  col = table->FindColumn("V");
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Name(), "V");
  EXPECT_EQ(col->GetType()->kind(), zetasql::TYPE_STRING);
  EXPECT_FALSE(col->is_generated());
  EXPECT_FALSE(col->has_default_value());
  EXPECT_FALSE(col->expression().has_value());
  EXPECT_FALSE(col->original_expression().has_value());
}

TEST_P(SchemaUpdaterTest, NumericColumnDefault) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema(
                                     {
                                         R"(
        CREATE TABLE t (
          key bigint primary key,
          value numeric DEFAULT 0.0 NOT NULL
        )
      )"},
                                     /*proto_descriptor_bytes=*/"",
                                     database_api::DatabaseDialect::POSTGRESQL,
                                     /*use_gsql_to_pg_translation=*/false));
    EXPECT_EQ(schema->dialect(), database_api::DatabaseDialect::POSTGRESQL);
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
        CREATE TABLE t (
          key INT64,
          value NUMERIC NOT NULL DEFAULT (0.0)
        ) PRIMARY KEY (key)
      )"}));
    EXPECT_EQ(schema->dialect(),
              database_api::DatabaseDialect::GOOGLE_STANDARD_SQL);
  }
  const Table* table = schema->FindTable("t");
  ASSERT_NE(table, nullptr);
  const Column* col = table->FindColumn("value");
  if (GetParam() == POSTGRESQL) {
    EXPECT_EQ(
        col->GetType()->TypeName(zetasql::PRODUCT_EXTERNAL),
        postgres_translator::spangres::datatypes::GetPgNumericType()->TypeName(
            zetasql::PRODUCT_EXTERNAL));
  } else {
    EXPECT_EQ(col->GetType()->kind(), zetasql::TYPE_NUMERIC);
  }
  EXPECT_TRUE(col->has_default_value());
  EXPECT_TRUE(col->expression().has_value());
  if (GetParam() == POSTGRESQL) {
    EXPECT_EQ(col->expression().value(), "pg.cast_to_numeric('0.0')");
    EXPECT_EQ(col->original_expression(), "0.0");
  } else {
    EXPECT_EQ(col->expression().value(), "0.0");
    EXPECT_FALSE(col->original_expression().has_value());
  }
}

}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
