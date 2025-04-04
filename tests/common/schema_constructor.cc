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
#include <string_view>
#include <utility>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "zetasql/public/type.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "backend/common/ids.h"
#include "backend/database/pg_oid_assigner/pg_oid_assigner.h"
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
    zetasql::TypeFactory* type_factory, std::string proto_descriptor_bytes,
    database_api::DatabaseDialect dialect, std::string_view database_id) {
  backend::TableIDGenerator table_id_gen;
  backend::ColumnIDGenerator column_id_gen;
  backend::PgOidAssigner pg_oid_assigner(
      dialect == database_api::DatabaseDialect::POSTGRESQL);
  backend::SchemaChangeContext context{
      .type_factory = type_factory,
      .table_id_generator = &table_id_gen,
      .column_id_generator = &column_id_gen,
      .pg_oid_assigner = &pg_oid_assigner,
      .database_id = std::string(database_id),
  };
  backend::SchemaUpdater updater;
  return updater.ValidateSchemaFromDDL(
      backend::SchemaChangeOperation{
          .statements = statements
          ,
          .proto_descriptor_bytes = proto_descriptor_bytes
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
      "" /*proto_descriptor_bytes*/
      ,
      dialect);
  ABSL_CHECK_OK(maybe_schema.status());
  return std::move(maybe_schema.value());
}

std::unique_ptr<const backend::Schema> CreateSchemaWithTimestampDateTable(
    zetasql::TypeFactory* type_factory,
    database_api::DatabaseDialect dialect) {
  std::string timestamp_date_table =
      R"(
          CREATE TABLE timestamp_date_table (
            int64_col INT64 NOT NULL,
            timestamp_col TIMESTAMP,
            date_col DATE
          ) PRIMARY KEY (int64_col)
      )";
  if (dialect == database_api::DatabaseDialect::POSTGRESQL) {
    timestamp_date_table =
        R"(
            CREATE TABLE timestamp_date_table (
              int64_col bigint NOT NULL PRIMARY KEY,
              timestamp_col timestamptz,
              date_col date
            )
        )";
  }
  auto maybe_schema = CreateSchemaFromDDL(
      {
          timestamp_date_table,
      },
      type_factory, "" /*proto_descriptor_bytes*/
      ,
      dialect);
  ABSL_CHECK_OK(maybe_schema.status());  // Crash OK
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
      "" /*proto_descriptor_bytes*/
      ,
      dialect);

  ABSL_CHECK_OK(maybe_schema.status());
  return std::move(maybe_schema.value());
}

absl::StatusOr<std::unique_ptr<const backend::Schema>>
CreateSchemaWithOneSequence(zetasql::TypeFactory* type_factory,
                            database_api::DatabaseDialect dialect) {
  test::ScopedEmulatorFeatureFlagsSetter setter(
      {.enable_bit_reversed_positive_sequences = true,
       .enable_bit_reversed_positive_sequences_postgresql = true,
       .enable_identity_columns = true});

  if (dialect == database_api::DatabaseDialect::POSTGRESQL) {
    return CreateSchemaFromDDL(
        {
            R"(
                CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE SKIP RANGE 1 1000
              )",
            R"(
              CREATE TABLE test_table (
                int64_col bigint DEFAULT nextval('myseq'),
                string_col varchar,
                PRIMARY KEY (int64_col)
              )
            )",
            R"(
              CREATE TABLE test_id_table (
                int64_col bigint NOT NULL
                  GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE),
                string_col varchar,
                PRIMARY KEY (int64_col)
              )
            )",
        },
        type_factory, "" /*proto_descriptor_bytes*/
        ,
        dialect);
  } else {
    return CreateSchemaFromDDL(
        {
            R"(
                CREATE SEQUENCE myseq OPTIONS (
                  sequence_kind = "bit_reversed_positive",
                  skip_range_min = 1,
                  skip_range_max = 1000
                )
              )",
            R"(
                CREATE TABLE test_table (
                  int64_col INT64 NOT NULL
                      DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE myseq)),
                  string_col STRING(MAX)
                ) PRIMARY KEY (int64_col)
              )",
            R"(
                CREATE TABLE test_id_table (
                  int64_col INT64 NOT NULL
                    GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE),
                  string_col STRING(MAX)
                ) PRIMARY KEY (int64_col)
              )",
        },
        type_factory);
  }
}

std::unique_ptr<const backend::Schema> CreateSchemaWithOneModel(
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
  maybe_schema = CreateSchemaFromDDL(
      {
          test_table,
          R"(
              CREATE MODEL test_model
              INPUT (string_col STRING(MAX))
              OUTPUT (outcome BOOL)
              REMOTE OPTIONS (endpoint = 'test')
            )",
      },
      type_factory
      ,
      "" /*proto_descriptor_bytes*/
      ,
      dialect);

  ABSL_CHECK_OK(maybe_schema.status());  // Crash OK
  return std::move(maybe_schema.value());
}

std::unique_ptr<const backend::Schema> CreateSchemaWithOnePropertyGraph(
    zetasql::TypeFactory* type_factory,
    database_api::DatabaseDialect dialect) {
  std::string node_table =
      R"(
          CREATE TABLE node_table (
            id INT64 NOT NULL,
          ) PRIMARY KEY (id)
      )";
  std::string edge_table =
      R"(
        CREATE TABLE edge_table (
          from_id INT64 NOT NULL,
          to_id INT64 NOT NULL,
        ) PRIMARY KEY(from_id, to_id)
      )";
  std::string property_graph =
      R"(
          CREATE PROPERTY GRAPH test_graph
              NODE TABLES(
                node_table KEY(id)
                  LABEL Test PROPERTIES(id))
              EDGE TABLES(
                edge_table
                  KEY(from_id, to_id)
                  SOURCE KEY(from_id) REFERENCES node_table(id)
                  DESTINATION KEY(to_id) REFERENCES node_table(id)
                  DEFAULT LABEL PROPERTIES ALL COLUMNS)
          )";
  auto maybe_schema = CreateSchemaFromDDL(
      {
          node_table,
          edge_table,
          property_graph,
      },
      type_factory, "" /*proto_descriptor_bytes*/
      ,
      dialect);
  ABSL_CHECK_OK(maybe_schema.status());  // Crash OK
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

std::unique_ptr<const backend::Schema> CreateSimpleDefaultKeySchema(
    zetasql::TypeFactory* type_factory,
    database_api::DatabaseDialect dialect) {
  std::string table_ddl = R"sql(
            CREATE TABLE players_default_key (
              prefix INT64 NOT NULL DEFAULT(100),
              player_id INT64 NOT NULL,
              balance INT64 DEFAULT(1),
            ) PRIMARY KEY(prefix, player_id)
          )sql";
  if (dialect == database_api::DatabaseDialect::POSTGRESQL) {
    table_ddl = R"sql(
            CREATE TABLE players_default_key (
              prefix bigint DEFAULT(100),
              player_id bigint NOT NULL,
              balance bigint DEFAULT(1),
              PRIMARY KEY(prefix, player_id)
            )
          )sql";
  }
  auto maybe_schema = CreateSchemaFromDDL(
      {
          table_ddl,
      },
      type_factory
      // copybara:protos_strip_begin
      ,
      "" /*proto_descriptor_bytes*/
      // copybara:protos_strip_end
      ,
      dialect);
  ABSL_CHECK_OK(maybe_schema.status());  // Crash OK
  return std::move(maybe_schema.value());
}

std::unique_ptr<const backend::Schema> CreateSimpleTimestampKeySchema(
    zetasql::TypeFactory* type_factory,
    database_api::DatabaseDialect dialect) {
  std::string table_ddl =
      R"sql(
            CREATE TABLE timestamp_key_table (
              k INT64 NOT NULL,
              ts TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
              val INT64,
            ) PRIMARY KEY(k, ts)
          )sql";

  if (dialect == database_api::DatabaseDialect::POSTGRESQL) {
    table_ddl = R"sql(
            CREATE TABLE timestamp_key_table (
              k bigint NOT NULL,
              ts spanner.commit_timestamp,
              val bigint,
              PRIMARY KEY(k, ts)
            )
          )sql";
  }
  auto maybe_schema = CreateSchemaFromDDL(
      {
          table_ddl,
      },
      type_factory
      // copybara:protos_strip_begin
      ,
      "" /*proto_descriptor_bytes*/
      // copybara:protos_strip_end
      ,
      dialect);
  ABSL_CHECK_OK(maybe_schema.status());  // Crash OK
  return std::move(maybe_schema.value());
}

std::unique_ptr<const backend::Schema> CreateSchemaWithOneTableWithSynonym(
    zetasql::TypeFactory* type_factory,
    database_api::DatabaseDialect dialect) {
  std::string test_table =
      R"(
          CREATE TABLE test_table (
            int64_col INT64 NOT NULL,
            string_col STRING(MAX),
            SYNONYM(test_synonym)
          ) PRIMARY KEY (int64_col)
      )";
  if (dialect == database_api::DatabaseDialect::POSTGRESQL) {
    test_table =
        R"(
            CREATE TABLE test_table (
              int64_col bigint NOT NULL PRIMARY KEY,
              string_col varchar,
              SYNONYM(test_synonym)
            )
        )";
  }
  auto maybe_schema = CreateSchemaFromDDL({test_table},
                                          type_factory
                                          ,
                                          "" /*proto_descriptor_bytes*/
                                          ,
                                          dialect);
  ABSL_CHECK_OK(maybe_schema.status());  // Crash OK
  return std::move(maybe_schema.value());
}

std::unique_ptr<const backend::Schema> CreateSchemaWithProtoEnumColumn(
    zetasql::TypeFactory* type_factory, std::string proto_descriptors) {
  auto maybe_schema = test::CreateSchemaFromDDL(
      {
          R"sql(
            CREATE PROTO BUNDLE (
              emulator.tests.common.Simple,
              emulator.tests.common.TestEnum,
            )

          )sql",
          R"sql(
              CREATE TABLE test_table (
                int64_col INT64 NOT NULL,
                proto_col emulator.tests.common.Simple,
                enum_col emulator.tests.common.TestEnum,
                array_proto_col ARRAY<emulator.tests.common.Simple>,
                array_enum_col ARRAY<emulator.tests.common.TestEnum>,
              ) PRIMARY KEY (int64_col)
            )sql",
      },
      type_factory, proto_descriptors);
  ABSL_CHECK_OK(maybe_schema.status());  // Crash OK
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
      "" /*proto_descriptor_bytes*/
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
      "" /*proto_descriptor_bytes*/
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
            CONSTRAINT C2 FOREIGN KEY (k1, c2)
              REFERENCES referenced_table (k1, c2) NOT ENFORCED,
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
          "" /*proto_descriptor_bytes*/
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
          "" /*proto_descriptor_bytes*/
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

std::unique_ptr<const backend::Schema> CreateSchemaWithNamedSchema(
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
              CREATE SCHEMA Foo
            )",
      },
      type_factory);
  if (!maybe_schema.ok()) {
    ABSL_LOG(ERROR) << "Failed to create the schema: " << maybe_schema.status();
    return nullptr;
  }
  return std::move(maybe_schema.value());
}
}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
