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

#include "tests/common/schema_constructor.h"

#include <memory>
#include <string>
#include <utility>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "zetasql/public/type.h"
#include "absl/log/log.h"
#include "absl/status/statusor.h"
#include "absl/log/check.h"
#include "absl/types/span.h"
#include "backend/common/ids.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/updater/schema_updater.h"
#include "tests/common/scoped_feature_flags_setter.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace database_api = ::google::spanner::admin::database::v1;

absl::StatusOr<std::unique_ptr<const backend::Schema>> CreateSchemaFromDDL(
    absl::Span<const std::string> statements,
    zetasql::TypeFactory* type_factory
    ,
    database_api::DatabaseDialect dialect) {
  backend::TableIDGenerator table_id_gen;
  backend::ColumnIDGenerator column_id_gen;
  backend::SchemaChangeContext context{
      .type_factory = type_factory,
      .table_id_generator = &table_id_gen,
      .column_id_generator = &column_id_gen,
  };
  backend::SchemaUpdater updater;
  return updater.ValidateSchemaFromDDL(
      backend::SchemaChangeOperation{
          .statements = statements
          ,
          .database_dialect = dialect},
      context);
}

std::unique_ptr<const backend::Schema> CreateSchemaWithOneTable(
    zetasql::TypeFactory* type_factory,
    database_api::DatabaseDialect dialect) {
  std::string test_table =
      R"(
          CREATE TABLE test_table (
            int64_col INT64 NOT NULL,
            string_col STRING(MAX)
          ) PRIMARY KEY (int64_col)
      )";
  if (dialect == database_api::DatabaseDialect::POSTGRESQL) {
    test_table =
        R"(
            CREATE TABLE test_table (
              int64_col bigint NOT NULL PRIMARY KEY,
              string_col varchar
            )
        )";
  }
  auto maybe_schema = CreateSchemaFromDDL(
      {
          test_table,
          R"(
              CREATE UNIQUE INDEX test_index ON test_table(string_col DESC)
            )",
      },
      type_factory
      ,
      dialect);
  ABSL_CHECK_OK(maybe_schema.status());
  return std::move(maybe_schema.value());
}

std::unique_ptr<const backend::Schema>
CreateSchemaWithOneTableAndOneChangeStream(
    zetasql::TypeFactory* type_factory,
    database_api::DatabaseDialect dialect) {
  absl::StatusOr<std::unique_ptr<const backend::Schema>> maybe_schema;

  std::string test_table =
      R"(
          CREATE TABLE test_table (
            int64_col INT64 NOT NULL,
            string_col STRING(MAX)
          ) PRIMARY KEY (int64_col)
      )";
  if (dialect == database_api::DatabaseDialect::POSTGRESQL) {
    test_table = R"(
              CREATE TABLE test_table (
                int64_col bigint NOT NULL,
                string_col varchar,
                PRIMARY KEY (int64_col)
              )
            )";
  }
  maybe_schema = CreateSchemaFromDDL(
      {
          test_table,
          R"(
              CREATE CHANGE STREAM change_stream_test_table FOR ALL
            )",
      },
      type_factory
      ,
      dialect);

  ABSL_CHECK_OK(maybe_schema.status());
  return std::move(maybe_schema.value());
}

std::unique_ptr<const backend::Schema> CreateSimpleDefaultValuesSchema(
    zetasql::TypeFactory* type_factory) {
  auto maybe_schema = CreateSchemaFromDDL(
      {
          R"sql(
            CREATE TABLE players (
              player_id INT64 NOT NULL,
              account_balance NUMERIC DEFAULT (0.0),
            ) PRIMARY KEY(player_id)
          )sql",
      },
      type_factory);
  ABSL_DCHECK_OK(maybe_schema.status());
  return std::move(maybe_schema.value());
}

std::unique_ptr<const backend::Schema> CreateSchemaWithInterleaving(
    zetasql::TypeFactory* const type_factory,
    database_api::DatabaseDialect dialect) {
  std::string parent_table =
      R"(
          CREATE TABLE Parent (
            k1 INT64 NOT NULL,
            c1 STRING(MAX)
          ) PRIMARY KEY (k1)
        )";
  std::string cascade_delete_child_table =
      R"(
          CREATE TABLE CascadeDeleteChild (
            k1 INT64 NOT NULL,
            k2 INT64 NOT NULL,
            c1 STRING(MAX)
          ) PRIMARY KEY (k1, k2),
                INTERLEAVE IN PARENT Parent ON DELETE CASCADE
        )";
  std::string no_action_delete_child_table =
      R"(
          CREATE TABLE NoActionDeleteChild (
            k1 INT64 NOT NULL,
            k2 INT64 NOT NULL,
            c1 STRING(MAX)
          ) PRIMARY KEY (k1, k2),
                INTERLEAVE IN PARENT Parent ON DELETE NO ACTION
        )";
  if (dialect == database_api::DatabaseDialect::POSTGRESQL) {
    parent_table =
        R"(
            CREATE TABLE Parent (
              k1 bigint NOT NULL PRIMARY KEY,
              c1 varchar
            )
          )";
    cascade_delete_child_table =
        R"(
            CREATE TABLE CascadeDeleteChild (
              k1 bigint NOT NULL,
              k2 bigint NOT NULL,
              c1 varchar,
              PRIMARY KEY (k1, k2)
            ) INTERLEAVE IN PARENT Parent ON DELETE CASCADE
        )";
    no_action_delete_child_table =
        R"(
            CREATE TABLE NoActionDeleteChild (
              k1 bigint NOT NULL,
              k2 bigint NOT NULL,
              c1 varchar,
              PRIMARY KEY (k1, k2)
            ) INTERLEAVE IN PARENT Parent ON DELETE NO ACTION
          )";
  }
  auto maybe_schema = CreateSchemaFromDDL(
      {
          parent_table,
          cascade_delete_child_table,
          no_action_delete_child_table,
      },
      type_factory
      ,
      dialect);
  ABSL_CHECK_OK(maybe_schema.status());
  return std::move(maybe_schema.value());
}

std::unique_ptr<const backend::Schema> CreateSchemaWithMultiTables(
    zetasql::TypeFactory* type_factory,
    database_api::DatabaseDialect dialect) {
  std::string test_table =
      R"(
          CREATE TABLE test_table (
            int64_col INT64 NOT NULL,
            string_col STRING(MAX)
          ) PRIMARY KEY (int64_col)
        )";
  std::string child_table =
      R"(
          CREATE TABLE child_table (
            int64_col INT64 NOT NULL,
            child_key INT64 NOT NULL,
          ) PRIMARY KEY (int64_col, child_key),
          INTERLEAVE IN PARENT test_table ON DELETE CASCADE
        )";
  std::string test_table2 =
      R"(
          CREATE TABLE test_table2 (
            int64_col INT64 NOT NULL,
            string_col STRING(MAX)
          ) PRIMARY KEY (int64_col)
        )";
  if (dialect == database_api::DatabaseDialect::POSTGRESQL) {
    test_table =
        R"(
            CREATE TABLE test_table (
              int64_col bigint NOT NULL PRIMARY KEY,
              string_col varchar
            )
        )";
    child_table =
        R"(
            CREATE TABLE child_table (
              int64_col bigint NOT NULL,
              child_key bigint NOT NULL,
              PRIMARY KEY (int64_col, child_key)
            ) INTERLEAVE IN PARENT test_table ON DELETE CASCADE
        )";
    test_table2 =
        R"(
            CREATE TABLE test_table2 (
              int64_col bigint NOT NULL PRIMARY KEY,
              string_col varchar
            )
        )";
  }
  auto maybe_schema = CreateSchemaFromDDL(
      {
          test_table,
          child_table,
          test_table2,
      },
      type_factory
      ,
      dialect);
  ABSL_CHECK_OK(maybe_schema.status());
  return std::move(maybe_schema.value());
}

std::unique_ptr<const backend::Schema> CreateSchemaWithForeignKey(
    zetasql::TypeFactory* type_factory,
    database_api::DatabaseDialect dialect) {
  std::string referenced_table =
      R"(
          CREATE TABLE referenced_table (
            k1 INT64 NOT NULL,
            c1 STRING(20),
            c2 STRING(20),
          ) PRIMARY KEY (k1)
        )";
  std::string referencing_table =
      R"(
          CREATE TABLE referencing_table (
            k1 INT64 NOT NULL,
            c1 STRING(20),
            c2 STRING(20),
            CONSTRAINT C FOREIGN KEY (k1, c1)
              REFERENCES referenced_table (k1, c1),
            FOREIGN KEY (c2) REFERENCES referenced_table (c2),
          ) PRIMARY KEY (k1)
        )";
  if (dialect == database_api::DatabaseDialect::POSTGRESQL) {
    referenced_table =
        R"(
            CREATE TABLE referenced_table (
              k1 bigint NOT NULL PRIMARY KEY,
              c1 varchar(20),
              c2 varchar(20)
            )
        )";
    referencing_table =
        R"(
            CREATE TABLE referencing_table (
              k1 bigint NOT NULL,
              c1 varchar(20),
              c2 varchar(20),
              CONSTRAINT C FOREIGN KEY (k1, c1)
                REFERENCES referenced_table (k1, c1),
              FOREIGN KEY (c2) REFERENCES referenced_table (c2),
              PRIMARY KEY (k1)
            )
        )";
  }
  absl::StatusOr<std::unique_ptr<const backend::Schema>> schema =
      CreateSchemaFromDDL(
          {
              referenced_table,
              referencing_table,
          },
          type_factory
          ,
          dialect);
  if (!schema.ok()) {
    ABSL_LOG(ERROR) << "Failed to create the schema: " << schema.status();
    return nullptr;
  }
  return *std::move(schema);
}

std::unique_ptr<const backend::Schema> CreateSchemaWithForeignKeyOnDelete(
    zetasql::TypeFactory* type_factory,
    database_api::DatabaseDialect dialect) {
  std::string referenced_table =
      R"(
          CREATE TABLE referenced_table (
            k1 INT64 NOT NULL,
            c1 STRING(20),
            c2 STRING(20),
          ) PRIMARY KEY (k1)
        )";
  std::string referencing_table =
      R"(
          CREATE TABLE referencing_table (
            k1 INT64 NOT NULL,
            c1 STRING(20),
            c2 STRING(20),
            CONSTRAINT C1 FOREIGN KEY (k1)
              REFERENCES referenced_table (k1) ON DELETE CASCADE,
            CONSTRAINT C2 FOREIGN KEY (k1, c1)
              REFERENCES referenced_table (k1, c1) ON DELETE CASCADE,
          ) PRIMARY KEY (k1)
        )";
  if (dialect == database_api::DatabaseDialect::POSTGRESQL) {
    referenced_table =
        R"(
            CREATE TABLE referenced_table (
              k1 bigint NOT NULL PRIMARY KEY,
              c1 varchar(20),
              c2 varchar(20)
            )
        )";
    referencing_table =
        R"(
            CREATE TABLE referencing_table (
              k1 bigint NOT NULL,
              c1 varchar(20),
              c2 varchar(20),
              CONSTRAINT C1 FOREIGN KEY (k1)
                REFERENCES referenced_table (k1) ON DELETE CASCADE,
              CONSTRAINT C2 FOREIGN KEY (k1, c1)
                REFERENCES referenced_table (k1, c1) ON DELETE CASCADE,
              PRIMARY KEY (k1)
            )
        )";
  }
  absl::StatusOr<std::unique_ptr<const backend::Schema>> schema =
      CreateSchemaFromDDL(
          {
              referenced_table,
              referencing_table,
          },
          type_factory
          ,
          dialect);
  if (!schema.ok()) {
    ABSL_LOG(ERROR) << "Failed to create the schema: " << schema.status();
    return nullptr;
  }
  return *std::move(schema);
}

std::unique_ptr<const backend::Schema> CreateSchemaWithView(
    zetasql::TypeFactory* type_factory) {
  test::ScopedEmulatorFeatureFlagsSetter setter({.enable_views = true});
  auto maybe_schema = CreateSchemaFromDDL(
      {
          R"(
              CREATE TABLE test_table (
                int64_col INT64 NOT NULL,
                string_col STRING(MAX)
              ) PRIMARY KEY (int64_col)
            )",
          R"(
              CREATE VIEW test_view SQL SECURITY INVOKER
              AS (SELECT 2*t.int64_col+1 AS vcol, 'a' || t.string_col AS col
              FROM test_table t)
            )",
      },
      type_factory);
  ABSL_CHECK_OK(maybe_schema.status());
  return std::move(maybe_schema.value());
}

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
