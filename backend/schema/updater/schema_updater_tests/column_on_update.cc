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
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/updater/schema_updater_tests/base.h"
#include "common/errors.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {

// For the following tests, a custom PG DDL statement is required as translating
// expressions from GSQL to PG is not supported in tests.
using database_api::DatabaseDialect::POSTGRESQL;

TEST_P(SchemaUpdaterTest, OnUpdate) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
        CREATE TABLE T (
          id bigint NOT NULL PRIMARY KEY,
          a bigint,
          def_val SPANNER.COMMIT_TIMESTAMP
            DEFAULT (SPANNER.PENDING_COMMIT_TIMESTAMP()),
          update_val SPANNER.COMMIT_TIMESTAMP
            DEFAULT (SPANNER.PENDING_COMMIT_TIMESTAMP())
            ON UPDATE (SPANNER.PENDING_COMMIT_TIMESTAMP())
        )
      )"},
                                      /*proto_descriptor_bytes=*/"",
                                      database_api::DatabaseDialect::POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
        CREATE TABLE T (
          id INT64 NOT NULL,
          a INT64,
          def_val TIMESTAMP DEFAULT (PENDING_COMMIT_TIMESTAMP())
            OPTIONS (allow_commit_timestamp = true),
          update_val TIMESTAMP DEFAULT (PENDING_COMMIT_TIMESTAMP())
            ON UPDATE (PENDING_COMMIT_TIMESTAMP())
            OPTIONS (allow_commit_timestamp = true),
        ) PRIMARY KEY (id)
      )"}));
  }

  const Table* table = schema->FindTable("T");
  ASSERT_NE(table, nullptr);
  const Column* col = table->FindColumn("a");
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Name(), "a");
  EXPECT_FALSE(col->is_generated());
  EXPECT_FALSE(col->has_default_value());
  EXPECT_FALSE(col->has_on_update());
  EXPECT_FALSE(col->expression().has_value());
  EXPECT_FALSE(col->original_expression().has_value());
  EXPECT_EQ(col->dependent_columns().size(), 0);

  col = table->FindColumn("def_val");
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Name(), "def_val");
  EXPECT_FALSE(col->is_generated());
  EXPECT_TRUE(col->has_default_value());
  EXPECT_FALSE(col->has_on_update());
  EXPECT_TRUE(col->expression().has_value());
  if (GetParam() == POSTGRESQL) {
    EXPECT_EQ(col->original_expression(), "spanner.pending_commit_timestamp()");
  } else {
    EXPECT_FALSE(col->original_expression().has_value());
  }
  EXPECT_EQ(col->dependent_columns().size(), 0);

  col = table->FindColumn("update_val");
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Name(), "update_val");
  EXPECT_FALSE(col->is_generated());
  EXPECT_TRUE(col->has_default_value());
  EXPECT_TRUE(col->has_on_update());
  EXPECT_TRUE(col->expression().has_value());
  if (GetParam() == POSTGRESQL) {
    EXPECT_EQ(col->original_expression(), "spanner.pending_commit_timestamp()");
  } else {
    EXPECT_FALSE(col->original_expression().has_value());
  }
  EXPECT_EQ(col->dependent_columns().size(), 0);
}

TEST_P(SchemaUpdaterTest, OnUpdateError) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    EXPECT_THAT(
        CreateSchema({R"(
          CREATE TABLE T (
            key BIGINT NOT NULL PRIMARY KEY,
            def SPANNER.COMMIT_TIMESTAMP DEFAULT (NOW())
              ON UPDATE (NOW())
          )
      )"},
                     /*proto_descriptor_bytes=*/"",
                     database_api::DatabaseDialect::POSTGRESQL,
                     /*use_gsql_to_pg_translation=*/false),
        StatusIs(error::OnUpdateExpressionMustBePendingCommitTimestamp()));
  } else {
    EXPECT_THAT(
        CreateSchema({R"(
          CREATE TABLE T (
            key INT64 NOT NULL,
            def TIMESTAMP DEFAULT (CURRENT_TIMESTAMP())
              ON UPDATE (CURRENT_TIMESTAMP())
          ) PRIMARY KEY(key)
      )"}),
        StatusIs(error::OnUpdateExpressionMustBePendingCommitTimestamp()));
  }

  if (GetParam() == POSTGRESQL) {
    EXPECT_THAT(CreateSchema({R"(
        CREATE TABLE T (
          key BIGINT NOT NULL PRIMARY KEY,
          def SPANNER.COMMIT_TIMESTAMP
            DEFAULT (SPANNER.PENDING_COMMIT_TIMESTAMP())
            ON UPDATE (SPANNER.PENDING_COMMIT_TIMESTAMP())
        )
      )",
                              "ALTER TABLE T ALTER COLUMN def DROP DEFAULT"},
                             /*proto_descriptor_bytes=*/"",
                             database_api::DatabaseDialect::POSTGRESQL,
                             /*use_gsql_to_pg_translation=*/false),
                StatusIs(error::OnUpdateWithoutDefaultValue("def")));
  } else {
    EXPECT_THAT(CreateSchema({R"(
        CREATE TABLE T (
          key INT64 NOT NULL,
          def TIMESTAMP DEFAULT (PENDING_COMMIT_TIMESTAMP())
            ON UPDATE (PENDING_COMMIT_TIMESTAMP())
            OPTIONS ( allow_commit_timestamp = true ),
        ) PRIMARY KEY(key)
      )",
                              "ALTER TABLE T ALTER COLUMN def DROP DEFAULT"}),
                StatusIs(error::OnUpdateWithoutDefaultValue("def")));
  }

  if (GetParam() == POSTGRESQL) {
    EXPECT_THAT(CreateSchema({R"(
        CREATE TABLE T (
          key BIGINT NOT NULL PRIMARY KEY,
          def SPANNER.COMMIT_TIMESTAMP DEFAULT (NOW())
            ON UPDATE (spanner.pending_commit_timestamp())
        )
      )"},
                             /*proto_descriptor_bytes=*/"",
                             database_api::DatabaseDialect::POSTGRESQL,
                             /*use_gsql_to_pg_translation=*/false),
                StatusIs(error::OnUpdateDefaultValueMismatch("def")));
  } else {
    EXPECT_THAT(CreateSchema({R"(
        CREATE TABLE T (
          key INT64 NOT NULL,
          def TIMESTAMP DEFAULT (current_timestamp())
            ON UPDATE (PENDING_COMMIT_TIMESTAMP())
            OPTIONS ( allow_commit_timestamp = true ),
        ) PRIMARY KEY(key)
      )"}),
                StatusIs(error::OnUpdateDefaultValueMismatch("def")));
  }

  if (GetParam() == POSTGRESQL) {
    EXPECT_THAT(
        CreateSchema({R"(
        CREATE TABLE T (
          key BIGINT NOT NULL,
          def SPANNER.COMMIT_TIMESTAMP DEFAULT (spanner.pending_commit_timestamp())
            ON UPDATE (spanner.pending_commit_timestamp()),
          PRIMARY KEY (key, def)
        )
      )"},
                     /*proto_descriptor_bytes=*/"",
                     database_api::DatabaseDialect::POSTGRESQL,
                     /*use_gsql_to_pg_translation=*/false),
        StatusIs(error::ColumnWithOnUpdateUsedInPrimaryKey("t", "def")));
  } else {
    EXPECT_THAT(
        CreateSchema({R"(
        CREATE TABLE T (
          key INT64 NOT NULL,
          def TIMESTAMP ON UPDATE (PENDING_COMMIT_TIMESTAMP())
            DEFAULT (PENDING_COMMIT_TIMESTAMP())
            OPTIONS ( allow_commit_timestamp = true ),
        ) PRIMARY KEY(key, def)
      )"}),
        StatusIs(error::ColumnWithOnUpdateUsedInPrimaryKey("T", "def")));
  }
}

}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
