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
#include "backend/schema/catalog/schema.h"
#include "backend/schema/updater/schema_updater_tests/base.h"
#include "common/errors.h"
#include "common/feature_flags.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "third_party/spanner_pg/datatypes/extended/spanner_extended_type.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {

namespace types = zetasql::types;

namespace {

using database_api::DatabaseDialect::POSTGRESQL;

TEST_P(SchemaUpdaterTest, CreateTable_SingleKey) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
    CREATE TABLE T(
      col1 INT64,
      col2 STRING(MAX)
    ) PRIMARY KEY(col1))"}));

  auto t = schema->FindTable("T");
  EXPECT_NE(t, nullptr);
  EXPECT_EQ(t->columns().size(), 2);
  EXPECT_EQ(t->primary_key().size(), 1);

  auto col1 = t->columns()[0];
  EXPECT_THAT(col1, ColumnIs("col1", types::Int64Type()));
  EXPECT_THAT(col1, IsKeyColumnOf(t, "ASC"));

  auto col2 = t->columns()[1];
  EXPECT_THAT(col2, ColumnIs("col2", types::StringType()));
  EXPECT_THAT(col2, testing::Not(IsKeyColumnOf(t, "ASC")));
}

TEST_P(SchemaUpdaterTest, CreateTable_MultiKey) {
  std::unique_ptr<const Schema> schema;
  // Changing the ordering for key columns is unsupported in PG.
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
    CREATE TABLE T (
      col1 bigint,
      col2 varchar,
      PRIMARY KEY(col1, col2)
    ))"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
      CREATE TABLE T(
        col1 INT64,
        col2 STRING(MAX)
      ) PRIMARY KEY(col1 DESC, col2))"}));
  }

  auto t = schema->FindTable("T");
  EXPECT_NE(t, nullptr);
  EXPECT_EQ(t->columns().size(), 2);
  EXPECT_EQ(t->primary_key().size(), 2);

  auto col1 = t->columns()[0];
  EXPECT_THAT(col1, ColumnIs("col1", types::Int64Type()));
  if (GetParam() == POSTGRESQL) {
    EXPECT_THAT(col1, IsKeyColumnOf(t, "ASC"));
  } else {
    EXPECT_THAT(col1, IsKeyColumnOf(t, "DESC"));
  }

  auto col2 = t->columns()[1];
  EXPECT_THAT(col2, ColumnIs("col2", types::StringType()));
  EXPECT_THAT(col2, IsKeyColumnOf(t, "ASC"));
}

TEST_P(SchemaUpdaterTest, CreateTable_NoKey) {
  // Empty key columns are unsupported in PG.
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
    CREATE TABLE T(
      col1 INT64
    ) PRIMARY KEY())"}));

  auto t = schema->FindTable("T");
  EXPECT_NE(t, nullptr);
  EXPECT_EQ(t->columns().size(), 1);
  EXPECT_EQ(t->primary_key().size(), 0);

  auto col1 = t->columns()[0];
  EXPECT_THAT(col1, ColumnIs("col1", types::Int64Type()));
  EXPECT_THAT(col1, testing::Not(IsKeyColumnOf(t, "ASC")));
}

TEST_P(SchemaUpdaterTest, CreateTable_NoColumns) {
  // Empty key columns are unsupported in PG.
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
    CREATE TABLE T(
    ) PRIMARY KEY())"}));

  auto t = schema->FindTable("T");
  EXPECT_NE(t, nullptr);
  EXPECT_EQ(t->columns().size(), 0);
  EXPECT_EQ(t->primary_key().size(), 0);
}

TEST_P(SchemaUpdaterTest, CreateTable_ColumnLength) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  // Empty key columns are unsupported in PG.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
    CREATE TABLE T(
      col1 BYTES(10)
    ) PRIMARY KEY())"}));

  auto t = schema->FindTable("T");
  EXPECT_NE(t, nullptr);
  EXPECT_EQ(t->columns().size(), 1);

  auto col1 = t->columns()[0];
  EXPECT_THAT(col1, ColumnIs("col1", types::BytesType()));
  EXPECT_EQ(col1->declared_max_length(), 10);

  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
    CREATE TABLE T(
      col1 BYTES(MAX)
    ) PRIMARY KEY())"}));

  t = schema->FindTable("T");
  col1 = t->columns()[0];
  EXPECT_THAT(col1, ColumnIs("col1", types::BytesType()));
  EXPECT_FALSE(col1->declared_max_length().has_value());
}

TEST_P(SchemaUpdaterTest, CreateTable_AllowCommitTimestamp) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"(
      CREATE TABLE T(
        col1 bigint PRIMARY KEY,
        col2 spanner.commit_timestamp
      ))"},
                                      /*proto_descriptor_bytes=*/"",
                                      /*dialect=*/POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
      CREATE TABLE T(
        col1 INT64,
        col2 TIMESTAMP OPTIONS(
          allow_commit_timestamp = true
        )
      ) PRIMARY KEY(col1))"}));
  }

  auto t = schema->FindTable("T");
  auto col2 = t->columns()[1];
  EXPECT_THAT(col2, ColumnIs("col2", types::TimestampType()));
  EXPECT_TRUE(col2->allows_commit_timestamp());

  // The following tests are skipped because column options are unsupported in
  // PG.
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
    CREATE TABLE T(
      col1 INT64,
      col2 TIMESTAMP OPTIONS(
        allow_commit_timestamp = false
      )
    ) PRIMARY KEY(col1))"}));

  t = schema->FindTable("T");
  col2 = t->columns()[1];
  EXPECT_FALSE(col2->allows_commit_timestamp());

  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
    CREATE TABLE T(
      col1 INT64,
      col2 TIMESTAMP OPTIONS(
        allow_commit_timestamp = true,
        allow_commit_timestamp = null
      )
    ) PRIMARY KEY(col1))"}));

  t = schema->FindTable("T");
  col2 = t->columns()[1];
  EXPECT_FALSE(col2->allows_commit_timestamp());
}

TEST_P(SchemaUpdaterTest, CreateTable_InvalidColumnLength) {
  // Empty key columns are unsupported in PG.
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  EXPECT_THAT(CreateSchema({R"(
    CREATE TABLE T(
      col1 BYTES(1000000000)
    ) PRIMARY KEY())"}),
              StatusIs(error::InvalidColumnLength(
                  "T.col1", 1000000000, 1, limits::kMaxBytesColumnLength)));
}

TEST_P(SchemaUpdaterTest, CreateTable_DuplicateKeys) {
  // Only BYTES(MAX) is supported in PG.
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  EXPECT_THAT(CreateSchema({R"(
    CREATE TABLE T(
      col1 INT64,
      col2 BYTES(10)
    ) PRIMARY KEY(col1,col1))"}),
              StatusIs(error::MultipleRefsToKeyColumn("Table", "T", "col1")));
}

TEST_P(SchemaUpdaterTest, CreateTable_DuplicateColumns) {
  EXPECT_THAT(CreateSchema({R"(
    CREATE TABLE T(
      col1 INT64,
      col1 STRING(MAX)
    ) PRIMARY KEY(col1,col1))"}),
              StatusIs(error::DuplicateColumnName("T.col1")));
}

TEST_P(SchemaUpdaterTest, CreateTable_ColumnNullability) {
  // Empty key columns are unsupported in PG.
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
    CREATE TABLE T(
      col1 INT64 NOT NULL,
      col2 INT64
    ) PRIMARY KEY())"}));

  auto t = schema->FindTable("T");
  EXPECT_NE(t, nullptr);
  EXPECT_EQ(t->columns().size(), 2);

  auto col1 = t->columns()[0];
  EXPECT_FALSE(col1->is_nullable());

  auto col2 = t->columns()[0];
  EXPECT_FALSE(col2->is_nullable());
}

TEST_P(SchemaUpdaterTest, CreateTable_ColumnNotFound) {
  EXPECT_THAT(CreateSchema({R"(
        CREATE TABLE T(
          col1 INT64
        ) PRIMARY KEY(col2))"}),
              StatusIs(error::NonExistentKeyColumn("Table", "T", "col2")));
}

TEST_P(SchemaUpdaterTest, CreateTable_AlreadyExists) {
  EXPECT_THAT(CreateSchema({
                  R"(
      CREATE TABLE T(
        col1 INT64
      ) PRIMARY KEY(col1)
    )",
                  R"(
      DROP TABLE T
    )",
                  R"(
      CREATE TABLE T(
        col1 INT64
      ) PRIMARY KEY(col1)
    )",
                  R"(
      CREATE TABLE T(
        col2 INT64
      ) PRIMARY KEY(col2)
    )"}),
              StatusIs(error::SchemaObjectAlreadyExists("Table", "T")));
}

TEST_P(SchemaUpdaterTest, CreateTable_Interleave) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE `Parent` (
        k1 INT64 NOT NULL,
        c1 STRING(MAX)
      ) PRIMARY KEY (k1)
    )",
                                                  R"(
      CREATE TABLE Child (
        k1 INT64 NOT NULL,
        k2 INT64 NOT NULL,
        c1 STRING(MAX)
      ) PRIMARY KEY (k1, k2),
        INTERLEAVE IN PARENT `Parent` ON DELETE CASCADE
    )"}));

  auto p = schema->FindTable("Parent");
  EXPECT_NE(p, nullptr);
  auto c = schema->FindTable("Child");
  EXPECT_NE(c, nullptr);
  EXPECT_THAT(c, IsInterleavedIn(p, Table::OnDeleteAction::kCascade));
}

TEST_P(SchemaUpdaterTest, CreateTable_InterleaveMismatch) {
  // Changing the ordering for key columns is unsupported in PG.
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE `Parent` (
        k1 INT64 NOT NULL,
        c1 STRING(MAX)
      ) PRIMARY KEY (k1))"}));

  EXPECT_THAT(
      UpdateSchema(schema.get(), {R"(
      CREATE TABLE Child (
        k1 INT64 NOT NULL,
        k2 INT64 NOT NULL,
        c1 STRING(MAX)
      ) PRIMARY KEY (k1 DESC, k2),
        INTERLEAVE IN PARENT `Parent` ON DELETE CASCADE
    )"}),
      StatusIs(error::IncorrectParentKeyOrder("Table", "Child", "k1", "ASC")));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      CREATE TABLE Child (
        k1 INT64,
        k2 INT64 NOT NULL,
        c1 STRING(MAX)
      ) PRIMARY KEY (k1, k2),
        INTERLEAVE IN PARENT `Parent` ON DELETE CASCADE
    )"}),
              StatusIs(error::IncorrectParentKeyNullability(
                  "Table", "Child", "k1", "not null", "nullable")));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      CREATE TABLE Child (
        k1 STRING(10),
        k2 INT64 NOT NULL,
        c1 STRING(MAX)
      ) PRIMARY KEY (k1, k2),
        INTERLEAVE IN PARENT `Parent` ON DELETE CASCADE
    )"}),
              StatusIs(error::IncorrectParentKeyType("Table", "Child", "k1",
                                                     "STRING", "INT64")));
}

TEST_P(SchemaUpdaterTest, CreateTable_InterleaveDepth) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T0 (
        k0 INT64,
      ) PRIMARY KEY (k0)
    )"}));

  std::vector<std::string> statements;
  std::vector<int> key_idx;
  for (int i = 0; i < limits::kMaxInterleavingDepth; ++i) {
    key_idx.push_back(i);
    std::vector<std::string> key_names;
    std::transform(key_idx.begin(), key_idx.end(),
                   std::back_inserter(key_names),
                   [](int k) { return absl::StrCat("k", k); });
    std::vector<std::string> key_defs;
    std::transform(key_idx.begin(), key_idx.end(), std::back_inserter(key_defs),
                   [](int k) { return absl::StrCat("k", k, " INT64"); });
    std::string create_table = absl::Substitute(
        "CREATE TABLE T$0 ( $1 ) PRIMARY KEY($2), INTERLEAVE IN PARENT T$3",
        i + 1, absl::StrJoin(key_defs, ","), absl::StrJoin(key_names, ","), i);
    statements.emplace_back(std::move(create_table));
  }

  EXPECT_THAT(UpdateSchema(schema.get(), statements),
              StatusIs(error::DeepNesting("Table", "T7",
                                          limits::kMaxInterleavingDepth)));
}

TEST_P(SchemaUpdaterTest, CreateTable_ParentNotFound) {
  EXPECT_THAT(CreateSchema({
                  R"(
      CREATE TABLE `Parent` (
        k1 INT64 NOT NULL,
        c1 STRING(MAX)
      ) PRIMARY KEY (k1)
    )",
                  R"(
      CREATE TABLE Child (
        k1 INT64 NOT NULL,
        k2 INT64 NOT NULL,
        c1 STRING(MAX)
      ) PRIMARY KEY (k1, k2),
        INTERLEAVE IN PARENT `NoParent` ON DELETE CASCADE
    )"}),
              StatusIs(error::TableNotFound("NoParent")));
}

TEST_P(SchemaUpdaterTest, CreateTable_ChildTableMissingPrimaryKey) {
  EXPECT_THAT(
      CreateSchema({
          R"(
      CREATE TABLE `Parent` (
        k1 INT64 NOT NULL,
        k2 INT64 NOT NULL,
        c1 STRING(MAX)
      ) PRIMARY KEY (k1, k2)
    )",
          R"(
      CREATE TABLE Child (
        k1 INT64 NOT NULL,
        k2 INT64 NOT NULL,
        k3 INT64 NOT NULL,
        c1 STRING(MAX)
      ) PRIMARY KEY (k1, k3),
        INTERLEAVE IN PARENT `Parent` ON DELETE CASCADE
    )"}),
      StatusIs(error::MustReferenceParentKeyColumn("Table", "Child", "k2")));
}

TEST_P(SchemaUpdaterTest,
       CreateTable_ChildTableCaseSensitiveMissingPrimaryKey) {
  EXPECT_THAT(
      CreateSchema({
          R"(
      CREATE TABLE `Parent` (
        k1 INT64 NOT NULL,
        c1 STRING(MAX)
      ) PRIMARY KEY (k1)
    )",
          R"(
      CREATE TABLE Child (
        K1 INT64 NOT NULL,
        k2 INT64 NOT NULL,
        c1 STRING(MAX)
      ) PRIMARY KEY (K1, k2),
        INTERLEAVE IN PARENT `Parent` ON DELETE CASCADE
    )"}),
      StatusIs(error::MustReferenceParentKeyColumn("Table", "Child", "k1")));
}

TEST_P(SchemaUpdaterTest, CreateTable_ChildTablePrimaryKeyInWrongOrder) {
  EXPECT_THAT(
      CreateSchema({
          R"(
      CREATE TABLE `Parent` (
        k1 INT64 NOT NULL,
        c1 STRING(MAX)
      ) PRIMARY KEY (k1)
    )",
          R"(
      CREATE TABLE Child (
        k1 INT64 NOT NULL,
        k2 INT64 NOT NULL,
        c1 STRING(MAX)
      ) PRIMARY KEY (k2, k1),
        INTERLEAVE IN PARENT `Parent` ON DELETE CASCADE
    )"}),
      StatusIs(error::IncorrectParentKeyPosition("Table", "Child", "k1", 1)));
}

TEST_P(SchemaUpdaterTest, CreateTable_CreateChildTable) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"(
      CREATE TABLE `Parent` (
        k1 INT64 NOT NULL,
        c1 STRING(MAX)
      ) PRIMARY KEY (k1)
    )",
                                        R"(
      CREATE TABLE Child (
        k1 INT64 NOT NULL,
        k2 INT64 NOT NULL,
        c1 STRING(MAX)
      ) PRIMARY KEY (k1, k2),
        INTERLEAVE IN PARENT `Parent` ON DELETE CASCADE
    )"}));
  auto parent_table = schema->FindTable("Parent");
  EXPECT_NE(parent_table, nullptr);
  auto child_table = schema->FindTable("Child");
  EXPECT_NE(child_table, nullptr);
}

TEST_P(SchemaUpdaterTest, CreateTable_WithSynonym) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
    CREATE TABLE T(
      col1 INT64,
      col2 STRING(MAX),
      SYNONYM(S),
    ) PRIMARY KEY(col1))"}));

  auto t = schema->FindTable("T");
  EXPECT_NE(t, nullptr);
  auto s = schema->FindTable("S");
  EXPECT_NE(s, nullptr);
  EXPECT_EQ(t, s);

  auto t1 = schema->FindTableUsingSynonym("T");
  EXPECT_EQ(t1, nullptr);
  auto s1 = schema->FindTableUsingSynonym("S");
  EXPECT_NE(s1, nullptr);

  t1 = schema->FindTableCaseSensitive("T");
  EXPECT_NE(t1, nullptr);
  t1 = schema->FindTableCaseSensitive("t");
  EXPECT_EQ(t1, nullptr);
  s1 = schema->FindTableUsingSynonymCaseSensitive("S");
  EXPECT_NE(s1, nullptr);
  s1 = schema->FindTableUsingSynonymCaseSensitive("s");
  EXPECT_EQ(s1, nullptr);
}

TEST_P(SchemaUpdaterTest, CreateTable_TableNameConflictsWithSynonym) {
  EXPECT_THAT(CreateSchema({
                  R"(
      CREATE TABLE T(
        col1 INT64,
        SYNONYM(S),
      ) PRIMARY KEY(col1)
    )",
                  R"(
      CREATE TABLE S(
        col1 INT64
      ) PRIMARY KEY(col1)
    )"}),
              StatusIs(error::SchemaObjectAlreadyExists("Table", "S")));
}

TEST_P(SchemaUpdaterTest, CreateTable_SynonymConflictsWithTableName) {
  EXPECT_THAT(CreateSchema({
                  R"(
      CREATE TABLE T(
        col1 INT64,
      ) PRIMARY KEY(col1)
    )",
                  R"(
      CREATE TABLE T2(
        col1 INT64,
        SYNONYM(T),
      ) PRIMARY KEY(col1)
    )"}),
              StatusIs(error::SchemaObjectAlreadyExists("Table", "T")));
}

TEST_P(SchemaUpdaterTest, CreateTable_SynonymConflictsWithSynonym) {
  EXPECT_THAT(CreateSchema({
                  R"(
      CREATE TABLE T1(
        col1 INT64,
        SYNONYM(S),
      ) PRIMARY KEY(col1)
    )",
                  R"(
      CREATE TABLE T2(
        col1 INT64,
        SYNONYM(S),
      ) PRIMARY KEY(col1)
    )"}),
              StatusIs(error::SchemaObjectAlreadyExists("Table", "S")));
}

TEST_P(SchemaUpdaterTest, AlterTable_AddColumn) {
  // Only BYTES(MAX) is supported in PG.
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64,
      ) PRIMARY KEY (k1)
    )"}));

  auto t_old = schema->FindTable("T");
  EXPECT_EQ(t_old->FindColumn("c2"), nullptr);

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema, UpdateSchema(schema.get(), {R"(
      ALTER TABLE T ADD COLUMN c2 BYTES(100)
    )"}));

  auto t_new = new_schema->FindTable("T");
  EXPECT_NE(t_old, t_new);
  auto c2 = t_new->FindColumn("c2");
  EXPECT_NE(c2, nullptr);
  EXPECT_THAT(c2, ColumnIs("c2", types::BytesType()));
  EXPECT_TRUE(c2->is_nullable());
  EXPECT_EQ(c2->declared_max_length(), 100);
}

TEST_P(SchemaUpdaterTest, AlterTableAddColumnIfNotExists) {
  // IF NOT EXISTS isn't yet supported on the PG side of the emulator
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64,
      ) PRIMARY KEY (k1)
    )"}));

  auto t_old = schema->FindTable("T");
  EXPECT_EQ(t_old->FindColumn("c2"), nullptr);

  // Add a column, make sure it goes in right.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema, UpdateSchema(schema.get(), {R"(
      ALTER TABLE T ADD COLUMN c2 BYTES(100)
    )"}));

  auto t_new = new_schema->FindTable("T");
  EXPECT_NE(t_old, t_new);
  auto c2 = t_new->FindColumn("c2");
  EXPECT_NE(c2, nullptr);
  EXPECT_THAT(c2, ColumnIs("c2", types::BytesType()));
  EXPECT_TRUE(c2->is_nullable());
  EXPECT_EQ(c2->declared_max_length(), 100);

  // Add the same column again and make sure we didn't change anything.
  ZETASQL_ASSERT_OK(UpdateSchema(new_schema.get(), {R"(
      ALTER TABLE T ADD COLUMN IF NOT EXISTS c2 INT64
    )"}));

  t_new = new_schema->FindTable("T");
  EXPECT_NE(t_old, t_new);
  c2 = t_new->FindColumn("c2");
  EXPECT_NE(c2, nullptr);
  EXPECT_THAT(c2, ColumnIs("c2", types::BytesType()));
  EXPECT_TRUE(c2->is_nullable());
  EXPECT_EQ(c2->declared_max_length(), 100);
}

TEST_P(SchemaUpdaterTest, AlterTable_AddColumnAlreadyExists) {
  // Only BYTES(MAX) is supported in PG.
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64,
      ) PRIMARY KEY (k1)
    )"}));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      ALTER TABLE T ADD COLUMN c1 BYTES(100)
    )"}),
              StatusIs(error::DuplicateColumnName("T.c1")));
}

TEST_P(SchemaUpdaterTest, AlterColumn_ChangeColumnType_StaticCheckValid) {
  // Only BYTES(MAX) is supported in PG.
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
        c1 STRING(100),
      ) PRIMARY KEY (k1)
    )"}));

  auto t = schema->FindTable("T");
  auto c1 = t->FindColumn("c1");
  EXPECT_THAT(c1, ColumnIs("c1", types::StringType()));
  EXPECT_EQ(c1->declared_max_length(), 100);

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema, UpdateSchema(schema.get(), {R"(
      ALTER TABLE T ALTER COLUMN c1 BYTES(400)
    )"}));

  t = new_schema->FindTable("T");
  c1 = t->FindColumn("c1");
  EXPECT_THAT(c1, ColumnIs("c1", types::BytesType()));
  EXPECT_EQ(c1->declared_max_length(), 400);
}

TEST_P(SchemaUpdaterTest, AlterColumn_ChangeColumnType_Invalid) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
        c1 STRING(100),
      ) PRIMARY KEY (k1)
    )"}));

  auto t = schema->FindTable("T");
  auto c1 = t->FindColumn("c1");
  EXPECT_THAT(c1, ColumnIs("c1", types::StringType()));
  EXPECT_EQ(c1->declared_max_length(), 100);

  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      ALTER TABLE T ALTER COLUMN c1 INT64
    )"}),
              StatusIs(error::CannotChangeColumnType("c1", "STRING", "INT64")));
}

TEST_P(SchemaUpdaterTest, AlterColumn_ChangeNonArrayToArray) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
        c1 STRING(100),
      ) PRIMARY KEY (k1)
    )"}));

  EXPECT_THAT(
      UpdateSchema(schema.get(), {R"(
      ALTER TABLE T ALTER COLUMN c1 ARRAY<STRING(MAX)>
    )"}),
      StatusIs(error::CannotChangeColumnType("c1", "STRING", "ARRAY<STRING>")));

  EXPECT_THAT(
      UpdateSchema(schema.get(), {R"(
      ALTER TABLE T ALTER COLUMN c1 ARRAY<BYTES(MAX)>
    )"}),
      StatusIs(error::CannotChangeColumnType("c1", "STRING", "ARRAY<BYTES>")));
}

TEST_P(SchemaUpdaterTest, AlterColumn_NotNullToNullable) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"(
      CREATE TABLE T (
        k1 INT64 NOT NULL,
        c1 STRING(MAX),
        c2 INT64 NOT NULL,
      ) PRIMARY KEY (k1)
    )"}));

  auto t = schema->FindTable("T");
  auto c2 = t->FindColumn("c2");
  EXPECT_FALSE(c2->is_nullable());

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema, UpdateSchema(schema.get(), {R"(
      ALTER TABLE T ALTER COLUMN c2 INT64
    )"}));

  t = new_schema->FindTable("T");
  c2 = t->FindColumn("c2");
  EXPECT_TRUE(c2->is_nullable());
}

TEST_P(SchemaUpdaterTest, AlterColumn_ChangeIndexedColumnType) {
  // Only BYTES(MAX) is supported in PG.
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"(
      CREATE TABLE T (
        k1 INT64 NOT NULL,
        c1 STRING(10),
      ) PRIMARY KEY (k1)
    )",
                                        R"(
      CREATE INDEX Idx1 ON T(c1)
    )"}));

  auto t = schema->FindTable("T");
  auto c1 = t->FindColumn("c1");
  EXPECT_THAT(c1, ColumnIs("c1", types::StringType()));

  auto idx = schema->FindIndex("Idx1");
  auto c1_idx = idx->key_columns()[0];
  EXPECT_THAT(c1_idx->column(), SourceColumnIs(c1));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema, UpdateSchema(schema.get(), {R"(
      ALTER TABLE T ALTER COLUMN c1 BYTES(40)
    )"}));

  auto t_new = new_schema->FindTable("T");
  auto c1_new = t_new->FindColumn("c1");
  EXPECT_THAT(c1_new, ColumnIs("c1", types::BytesType()));

  auto idx_new = new_schema->FindIndex("Idx1");
  auto c1_idx_new = idx_new->key_columns()[0];
  EXPECT_THAT(c1_idx_new->column(), SourceColumnIs(c1_new));
}

TEST_P(SchemaUpdaterTest, AlterColumn_ChangeIndexedColumnNullability) {
  // Only BYTES(MAX) is supported in PG.
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"(
      CREATE TABLE T (
        k1 INT64,
        c1 STRING(10) NOT NULL,
        c2 STRING(10),
      ) PRIMARY KEY (k1)
    )",
                                        R"(
      CREATE INDEX Idx1 ON T(c1) STORING(c2)
    )",
                                        R"(
      CREATE NULL_FILTERED INDEX Idx2 ON T(c2) STORING(c1)
    )"}));

  // Changing the nullability of indexed columns for non-null-filtered indexes
  // is not allowed.
  EXPECT_THAT(
      UpdateSchema(schema.get(), {R"(
      ALTER TABLE T ALTER COLUMN c1 STRING(10)
    )"}),
      StatusIs(error::ChangingNullConstraintOnIndexedColumn("c1", "Idx1")));

  // Changing nullability of stored columns and indexed columns
  // in null filtered indexes is allowed
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema, UpdateSchema(schema.get(), {R"(
      ALTER TABLE T ALTER COLUMN c2 BYTES(40)
    )"}));

  auto c2 = new_schema->FindTable("T")->FindColumn("c2");
  EXPECT_THAT(c2, ColumnIs("c2", types::BytesType()));
  EXPECT_EQ(c2->declared_max_length(), 40);
  auto idx1 = new_schema->FindIndex("Idx1");
  auto idx2 = new_schema->FindIndex("Idx2");
  EXPECT_THAT(idx1->stored_columns()[0], SourceColumnIs(c2));
  EXPECT_THAT(idx2->key_columns()[0]->column(), SourceColumnIs(c2));
}

TEST_P(SchemaUpdaterTest, AlterColumn_KeyColumnType) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T (
        k1 STRING(100) NOT NULL,
      ) PRIMARY KEY (k1)
    )"}));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema, UpdateSchema(schema.get(), {R"(
      ALTER TABLE T ALTER COLUMN k1 BYTES(MAX) NOT NULL
    )"}));

  auto t = new_schema->FindTable("T");
  auto c1 = t->FindColumn("k1");
  EXPECT_THAT(c1, ColumnIs("k1", types::BytesType()));
  EXPECT_FALSE(c1->declared_max_length().has_value());
}

TEST_P(SchemaUpdaterTest, AlterColumn_KeyColumnNullability) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64 NOT NULL,
      ) PRIMARY KEY (k1)
    )"}));

  EXPECT_THAT(
      UpdateSchema(schema.get(), {R"(
      ALTER TABLE T ALTER COLUMN k1 INT64
    )"}),
      StatusIs(error::CannotChangeKeyColumn("T.k1", "from NOT NULL to NULL")));
}

TEST_P(SchemaUpdaterTest, AlterTable_UnsetAllowCommitTimestamp) {
  // Assigning column options is not supported in PG.
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
        c1 TIMESTAMP OPTIONS (
          allow_commit_timestamp=true
        ),
      ) PRIMARY KEY (k1)
    )"}));

  auto c1 = schema->FindTable("T")->FindColumn("c1");
  EXPECT_TRUE(c1->allows_commit_timestamp());

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema, UpdateSchema(schema.get(), {R"(
      ALTER TABLE T ALTER COLUMN c1 SET OPTIONS (
        allow_commit_timestamp = false
      )
    )"}));

  auto c1_new = new_schema->FindTable("T")->FindColumn("c1");
  EXPECT_FALSE(c1_new->allows_commit_timestamp());
}

TEST_P(SchemaUpdaterTest, AlterTable_DropColumn) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
        c1 STRING(MAX),
      ) PRIMARY KEY (k1)
    )"}));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema, UpdateSchema(schema.get(), {R"(
      ALTER TABLE T DROP COLUMN c1
    )"}));

  auto t = new_schema->FindTable("T");
  auto c1 = t->FindColumn("c1");
  EXPECT_EQ(c1, nullptr);
}

TEST_P(SchemaUpdaterTest, AlterTable_InvalidDropKeyColumn) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
        c1 STRING(MAX),
      ) PRIMARY KEY (k1)
    )"}));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      ALTER TABLE T DROP COLUMN k1
    )"}),
              StatusIs(error::InvalidDropKeyColumn("k1", "T")));
}

TEST_P(SchemaUpdaterTest, AlterTable_InvalidDropIndexedColumn) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"(
      CREATE TABLE T (
        k1 INT64,
        c1 STRING(10) NOT NULL,
        c2 STRING(10),
      ) PRIMARY KEY (k1)
    )",
                                        R"(
      CREATE INDEX Idx1 ON T(c1) STORING(c2)
    )"}));

  EXPECT_THAT(
      UpdateSchema(schema.get(), {R"(
      ALTER TABLE T DROP COLUMN c1
    )"}),
      StatusIs(error::InvalidDropColumnWithDependency("c1", "T", "Idx1")));

  EXPECT_THAT(
      UpdateSchema(schema.get(), {R"(
      ALTER TABLE T DROP COLUMN c2
    )"}),
      StatusIs(error::InvalidDropColumnWithDependency("c2", "T", "Idx1")));
}

TEST_P(SchemaUpdaterTest, AlterTable_ChangeOnDelete) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"(
      CREATE TABLE T1 (
        k1 INT64,
        c1 STRING(10),
      ) PRIMARY KEY (k1)
    )",
                                        R"(
      CREATE TABLE T2 (
        k1 INT64,
        c1 STRING(10),
        c2 BOOL,
      ) PRIMARY KEY (k1, c1), INTERLEAVE IN PARENT T1
    )"}));

  auto t2 = schema->FindTable("T2");
  EXPECT_NE(t2, nullptr);
  EXPECT_EQ(t2->on_delete_action(), Table::OnDeleteAction::kNoAction);

  // Change from NO ACTION to CASCADE.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema, UpdateSchema(schema.get(), {R"(
      ALTER TABLE T2 SET ON DELETE CASCADE
    )"}));

  t2 = new_schema->FindTable("T2");
  EXPECT_NE(t2, nullptr);
  EXPECT_EQ(t2->on_delete_action(), Table::OnDeleteAction::kCascade);

  // Change from CASCADE to NO ACTION
  ZETASQL_ASSERT_OK_AND_ASSIGN(new_schema, UpdateSchema(new_schema.get(), {R"(
      ALTER TABLE T2 SET ON DELETE NO ACTION
    )"}));

  t2 = new_schema->FindTable("T2");
  EXPECT_NE(t2, nullptr);
  EXPECT_EQ(t2->on_delete_action(), Table::OnDeleteAction::kNoAction);
}

TEST_P(SchemaUpdaterTest, AlterTable_AddSynonym) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
      ) PRIMARY KEY (k1)
    )"}));

  auto t_old = schema->FindTable("T");
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema, UpdateSchema(schema.get(), {R"(
      ALTER TABLE T ADD SYNONYM S
    )"}));

  auto t_new = new_schema->FindTable("T");
  EXPECT_NE(t_old, t_new);

  auto s_new = new_schema->FindTable("S");
  EXPECT_EQ(t_new, s_new);
}

TEST_P(SchemaUpdaterTest, AlterTable_CannotAddDuplicateSynonym) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
      ) PRIMARY KEY (k1)
    )",
                                                  R"(
      CREATE TABLE S (
        k1 INT64,
      ) PRIMARY KEY (k1)
    )"}));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      ALTER TABLE T ADD SYNONYM S
    )"}),
              StatusIs(error::SchemaObjectAlreadyExists("Table", "S")));
}

TEST_P(SchemaUpdaterTest, AlterTable_CannotAddTwoSynonyms) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
        SYNONYM(S),
      ) PRIMARY KEY (k1)
    )"}));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      ALTER TABLE T ADD SYNONYM S2
    )"}),
              StatusIs(error::SynonymAlreadyExists("S", "T")));
}

TEST_P(SchemaUpdaterTest, AlterTable_DropSynonym) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
        SYNONYM(S),
      ) PRIMARY KEY (k1)
    )"}));

  auto t_old = schema->FindTable("T");
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema, UpdateSchema(schema.get(), {R"(
      ALTER TABLE T DROP SYNONYM S
    )"}));

  auto t_new = new_schema->FindTable("T");
  EXPECT_NE(t_old, t_new);

  auto s_new = new_schema->FindTable("S");
  EXPECT_EQ(s_new, nullptr);

  // S is now available for reuse.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema1, UpdateSchema(new_schema.get(), {R"(
      CREATE TABLE S (
        k1 INT64,
      ) PRIMARY KEY (k1)
    )"}));
  s_new = new_schema1->FindTable("S");
  EXPECT_NE(s_new, nullptr);
}

TEST_P(SchemaUpdaterTest, AlterTable_CannotDropNonExistentSynonym) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
      ) PRIMARY KEY (k1)
    )"}));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      ALTER TABLE T DROP SYNONYM S
    )"}),
              StatusIs(error::SynonymDoesNotExist("S", "T")));
}

TEST_P(SchemaUpdaterTest, AlterTable_CannotDropInvalidSynonym) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
        SYNONYM(S),
      ) PRIMARY KEY (k1)
    )"}));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      ALTER TABLE T DROP SYNONYM S2
    )"}),
              StatusIs(error::SynonymDoesNotExist("S2", "T")));
}

TEST_P(SchemaUpdaterTest, DropTableNonexistentIfExists) {
  // IF NOT EXISTS isn't yet supported on the PG side of the emulator
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T1 (
        k1 INT64,
        c1 STRING(10),
      ) PRIMARY KEY (k1)
    )",
                                                  R"(
      CREATE TABLE T2 (
        k2 INT64,
        c2 STRING(MAX),
      ) PRIMARY KEY (k2)
    )"}));

  auto t1 = schema->FindTable("T1");
  EXPECT_NE(t1, nullptr);

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema, UpdateSchema(schema.get(), {R"(
      DROP TABLE T1
    )"}));

  // Dropped table and its dependent nodes like columns, key columns etc.
  // are deleted.
  t1 = new_schema->FindTable("T1");
  EXPECT_EQ(t1, nullptr);

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema2, UpdateSchema(schema.get(), {R"(
      DROP TABLE IF EXISTS T1
    )"}));

  // Make sure it's still gone
  t1 = new_schema2->FindTable("T1");
  EXPECT_EQ(t1, nullptr);

  // Make sure the other table is still there.
  auto t2 = new_schema2->FindTable("T2");
  EXPECT_NE(t2, nullptr);
}

TEST_P(SchemaUpdaterTest, DropTableIfExists) {
  // IF NOT EXISTS isn't yet supported on the PG side of the emulator
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T1 (
        k1 INT64,
        c1 STRING(10),
      ) PRIMARY KEY (k1)
    )",
                                                  R"(
      CREATE TABLE T2 (
        k2 INT64,
        c2 STRING(MAX),
      ) PRIMARY KEY (k2)
    )"}));

  auto t1 = schema->FindTable("T1");
  EXPECT_NE(t1, nullptr);

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema, UpdateSchema(schema.get(), {R"(
      DROP TABLE IF EXISTS T1
    )"}));

  // Dropped table and its dependent nodes like columns, key columns etc.
  // are deleted.
  t1 = new_schema->FindTable("T1");
  EXPECT_EQ(t1, nullptr);
}

TEST_P(SchemaUpdaterTest, DropTable) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T1 (
        k1 INT64,
        c1 STRING(10),
      ) PRIMARY KEY (k1)
    )",
                                                  R"(
      CREATE TABLE T2 (
        k2 INT64,
        c2 STRING(MAX),
      ) PRIMARY KEY (k2)
    )"}));

  auto t1 = schema->FindTable("T1");
  EXPECT_NE(t1, nullptr);
  EXPECT_EQ(schema->GetSchemaGraph()->GetSchemaNodes().size(), 8);

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema, UpdateSchema(schema.get(), {R"(
      DROP TABLE T1
    )"}));

  // Dropped table and its dependent nodes like columns, key columns etc.
  // are deleted.
  t1 = new_schema->FindTable("T1");
  EXPECT_EQ(t1, nullptr);
  EXPECT_EQ(new_schema->GetSchemaGraph()->GetSchemaNodes().size(), 4);

  // The other table is still there.
  auto t2 = new_schema->FindTable("T2");
  EXPECT_NE(t2, nullptr);
}

TEST_P(SchemaUpdaterTest, DropTable_CanDropChildTable) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T1 (
        k1 INT64,
        c1 STRING(10),
      ) PRIMARY KEY (k1)
    )",
                                                  R"(
      CREATE TABLE T2 (
        k1 INT64,
        k2 INT64,
        c2 STRING(MAX),
      ) PRIMARY KEY (k1, k2), INTERLEAVE IN PARENT T1
    )"}));

  // Cannot drop parent table.
  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      DROP TABLE T1
    )"}),
              StatusIs(error::DropTableWithInterleavedTables("T1", "T2")));

  // Can drop child table.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema, UpdateSchema(schema.get(), {R"(
      DROP TABLE T2
    )"}));

  auto t1 = new_schema->FindTable("T1");
  EXPECT_EQ(t1->children().size(), 0);
  EXPECT_EQ(new_schema->FindTable("T2"), nullptr);
}

TEST_P(SchemaUpdaterTest, DropTable_CanDropChildAndParentTogether) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T1 (
        k1 INT64,
        c1 STRING(10),
      ) PRIMARY KEY (k1)
    )",
                                                  R"(
      CREATE TABLE T2 (
        k1 INT64,
        k2 INT64,
        c2 STRING(MAX),
      ) PRIMARY KEY (k1, k2), INTERLEAVE IN PARENT T1
    )"}));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema, UpdateSchema(schema.get(), {R"(
      DROP TABLE T2)",
                                                                    R"(
      DROP TABLE T1)"}));

  EXPECT_TRUE(new_schema->tables().empty());
}

TEST_P(SchemaUpdaterTest, DropTable_Recreate) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T1 (
        k1 INT64,
        c1 STRING(10),
      ) PRIMARY KEY (k1)
    )"}));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema, UpdateSchema(schema.get(), {R"(
      DROP TABLE T1
    )"}));

  EXPECT_TRUE(new_schema->tables().empty());

  ZETASQL_ASSERT_OK_AND_ASSIGN(new_schema, UpdateSchema(new_schema.get(), {R"(
      CREATE TABLE T1 (
        k1 INT64,
        c1 STRING(10),
      ) PRIMARY KEY (k1)
    )"}));

  auto t1 = new_schema->FindTable("T1");
  EXPECT_NE(t1, nullptr);
}

TEST_P(SchemaUpdaterTest, DropTableWithSynonym) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T1 (
        k1 INT64,
        SYNONYM(S1)
      ) PRIMARY KEY (k1)
    )"}));

  auto t1 = schema->FindTable("T1");
  EXPECT_NE(t1, nullptr);
  auto s1 = schema->FindTable("S1");
  EXPECT_NE(s1, nullptr);
  EXPECT_EQ(t1, s1);

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema, UpdateSchema(schema.get(), {R"(
      DROP TABLE T1
    )"}));

  // Dropped table and its dependent nodes like columns, key columns etc.
  // are deleted.
  t1 = new_schema->FindTable("T1");
  EXPECT_EQ(t1, nullptr);
  s1 = new_schema->FindTable("S1");
  EXPECT_EQ(s1, nullptr);
}

TEST_P(SchemaUpdaterTest, ChangeKeyColumn) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"(
      CREATE TABLE T1 (
        k1 STRING(30),
        k2 INT64
      ) PRIMARY KEY (k1)
    )",
                                        R"(
      CREATE TABLE T2 (
        k1 STRING(30),
        k2 INT64,
        c1 BYTES(MAX)
      ) PRIMARY KEY (k1,k2), INTERLEAVE IN PARENT T1
    )"}));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      ALTER TABLE T2 ALTER COLUMN k1 STRING(30)
    )"}),
              StatusIs(error::AlteringParentColumn("T2.k1")));
}

TEST_P(SchemaUpdaterTest, CreateTable_NumericColumns) {
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T(
        col1 INT64,
        col2 PG.NUMERIC,
        col3 ARRAY<PG.NUMERIC>
      ) PRIMARY KEY(col1))"}));
    auto t = schema->FindTable("T");
    EXPECT_NE(t, nullptr);
    EXPECT_EQ(t->columns().size(), 3);
    EXPECT_EQ(t->primary_key().size(), 1);

    auto col1 = t->columns()[0];
    EXPECT_THAT(col1, ColumnIs("col1", types::Int64Type()));
    EXPECT_THAT(col1, IsKeyColumnOf(t, "ASC"));

    auto col2 = t->columns()[1];
    EXPECT_TRUE(col2->GetType()->IsExtendedType());
    EXPECT_EQ(static_cast<const postgres_translator::spangres::datatypes::
                              SpannerExtendedType*>(col2->GetType())
                  ->code(),
              google::spanner::v1::TypeAnnotationCode::PG_NUMERIC);

    auto col3 = t->columns()[2];
    EXPECT_TRUE(col3->GetType()->IsArray());
    EXPECT_TRUE(col3->GetType()->AsArray()->element_type()->IsExtendedType());
    EXPECT_EQ(static_cast<const postgres_translator::spangres::datatypes::
                              SpannerExtendedType*>(
                  col3->GetType()->AsArray()->element_type())
                  ->code(),
              google::spanner::v1::TypeAnnotationCode::PG_NUMERIC);
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T(
        col1 INT64,
        col2 NUMERIC,
        col3 ARRAY<NUMERIC>
      ) PRIMARY KEY(col1))"}));
    auto t = schema->FindTable("T");
    EXPECT_NE(t, nullptr);
    EXPECT_EQ(t->columns().size(), 3);
    EXPECT_EQ(t->primary_key().size(), 1);

    auto col1 = t->columns()[0];
    EXPECT_THAT(col1, ColumnIs("col1", types::Int64Type()));
    EXPECT_THAT(col1, IsKeyColumnOf(t, "ASC"));

    auto col2 = t->columns()[1];
    EXPECT_THAT(col2, ColumnIs("col2", types::NumericType()));

    auto col3 = t->columns()[2];
    EXPECT_THAT(col3, ColumnIs("col3", types::NumericArrayType()));
  }
}

TEST_P(SchemaUpdaterTest, CreateTable_NumericAsPK) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  EmulatorFeatureFlags::Flags flags;
  emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T (
        k1 NUMERIC
      ) PRIMARY KEY (k1)
    )"}));

  auto t = schema->FindTable("T");
  EXPECT_NE(t, nullptr);
  EXPECT_EQ(t->columns().size(), 1);
  EXPECT_EQ(t->primary_key().size(), 1);
  EXPECT_THAT(t->columns()[0], ColumnIs("k1", types::NumericType()));
}

TEST_P(SchemaUpdaterTest, CreateTable_JsonColumns) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
    CREATE TABLE T(
      col1 INT64,
      col2 PG.JSONB,
      col3 ARRAY<PG.JSONB>
    ) PRIMARY KEY(col1))"}));

    auto t = schema->FindTable("T");
    EXPECT_NE(t, nullptr);
    EXPECT_EQ(t->columns().size(), 3);
    EXPECT_EQ(t->primary_key().size(), 1);

    auto col1 = t->columns()[0];
    EXPECT_THAT(col1, ColumnIs("col1", types::Int64Type()));
    EXPECT_THAT(col1, IsKeyColumnOf(t, "ASC"));

    auto col2 = t->columns()[1];
    EXPECT_TRUE(col2->GetType()->IsExtendedType());
    EXPECT_EQ(static_cast<const postgres_translator::spangres::datatypes::
                              SpannerExtendedType*>(col2->GetType())
                  ->code(),
              google::spanner::v1::TypeAnnotationCode::PG_JSONB);

    auto col3 = t->columns()[2];
    EXPECT_TRUE(col3->GetType()->IsArray());
    EXPECT_TRUE(col3->GetType()->AsArray()->element_type()->IsExtendedType());
    EXPECT_EQ(static_cast<const postgres_translator::spangres::datatypes::
                              SpannerExtendedType*>(
                  col3->GetType()->AsArray()->element_type())
                  ->code(),
              google::spanner::v1::TypeAnnotationCode::PG_JSONB);
  } else {
    EmulatorFeatureFlags::Flags flags;
    emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
    CREATE TABLE T(
      col1 INT64,
      col2 JSON,
      col3 ARRAY<JSON>
    ) PRIMARY KEY(col1))"}));

    auto t = schema->FindTable("T");
    EXPECT_NE(t, nullptr);
    EXPECT_EQ(t->columns().size(), 3);
    EXPECT_EQ(t->primary_key().size(), 1);

    auto col1 = t->columns()[0];
    EXPECT_THAT(col1, ColumnIs("col1", types::Int64Type()));
    EXPECT_THAT(col1, IsKeyColumnOf(t, "ASC"));

    auto col2 = t->columns()[1];
    EXPECT_THAT(col2, ColumnIs("col2", types::JsonType()));

    auto col3 = t->columns()[2];
    EXPECT_THAT(col3, ColumnIs("col3", types::JsonArrayType()));
  }
}

TEST_P(SchemaUpdaterTest, CreateTable_JsonAsPK) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  EmulatorFeatureFlags::Flags flags;
  emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);
  EXPECT_THAT(CreateSchema({R"(
      CREATE TABLE T (
        k1 JSON
      ) PRIMARY KEY (k1)
    )"}),
              StatusIs(error::InvalidPrimaryKeyColumnType("T.k1", "JSON")));
}

std::vector<std::string> SchemaForCaseSensitivityTests() {
  return {
      R"sql(
                CREATE TABLE T (
                k1 INT64 NOT NULL,
                c1 STRING(MAX)
              ) PRIMARY KEY (k1)
            )sql",
      R"sql(
                CREATE TABLE T1 (
                  a INT64 NOT NULL,
                ) PRIMARY KEY (a)
            )sql",
  };
}

TEST_P(SchemaUpdaterTest, PrimaryKeyIsCaseSensitive) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema(SchemaForCaseSensitivityTests()));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      CREATE TABLE T2 (
        k1 INT64 NOT NULL,
        k2 INT64 NOT NULL,
      ) PRIMARY KEY (K1)
    )"}),
              StatusIs(error::NonExistentKeyColumn("Table", "T2", "K1")));
}

TEST_P(SchemaUpdaterTest, TableNameIsCaseSensitive) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema(SchemaForCaseSensitivityTests()));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      CREATE TABLE t (
        k1 INT64 NOT NULL,
      ) PRIMARY KEY (K1)
    )"}),
              StatusIs(error::SchemaObjectAlreadyExists("T", "t")));
}

TEST_P(SchemaUpdaterTest, InterleaveTableNameIsCaseSensitive) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema(SchemaForCaseSensitivityTests()));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      CREATE TABLE Child (
        k1 INT64 NOT NULL,
        k2 INT64 NOT NULL,
        c1 STRING(MAX)
      ) PRIMARY KEY (k1, k2),
        INTERLEAVE IN PARENT `t` ON DELETE CASCADE
    )"}),
              StatusIs(error::TableNotFound("t")));
}

TEST_P(SchemaUpdaterTest, AlterTableNameIsCaseSensitive) {
  // Only BYTES(MAX) is supported in PG.
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema(SchemaForCaseSensitivityTests()));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      ALTER TABLE t ADD COLUMN k2 BYTES(100)
    )"}),
              StatusIs(error::TableNotFound("t")));
}

TEST_P(SchemaUpdaterTest, AlterTableSetOptionsIsCaseSensitive) {
  // Assigning column options is not supported in PG.
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema(SchemaForCaseSensitivityTests()));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      ALTER TABLE t ALTER COLUMN c1 SET OPTIONS (
        allow_commit_timestamp = false )
    )"}),
              StatusIs(error::TableNotFound("t")));
}

TEST_P(SchemaUpdaterTest, DropTableIsCaseSensitive) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema(SchemaForCaseSensitivityTests()));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      DROP TABLE t)"}),
              StatusIs(error::TableNotFound("t")));
}

TEST_P(SchemaUpdaterTest, AlterColumnIsCaseSensitive) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema(SchemaForCaseSensitivityTests()));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      ALTER TABLE T ALTER COLUMN K2 INT64 DEFAULT(0)
    )"}),
              StatusIs(error::ColumnNotFound("T", "K2")));
}

TEST_P(SchemaUpdaterTest, DropColumnIsCaseSensitive) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema(SchemaForCaseSensitivityTests()));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      ALTER TABLE T DROP COLUMN K2
    )"}),
              StatusIs(error::ColumnNotFound("T", "K2")));
}

TEST_P(SchemaUpdaterTest, CreateTableIfNotExists) {
  // IF NOT EXISTS isn't yet supported on the PG side of the emulator
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
    CREATE TABLE T(
      col1 INT64,
      col2 STRING(MAX)
    ) PRIMARY KEY(col1 DESC, col2))"}));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema, UpdateSchema(schema.get(), {R"(
    CREATE TABLE IF NOT EXISTS T(
      col1 STRING(MAX),
      col3 INT64
    ) PRIMARY KEY(col3))"}));
  auto t = new_schema->FindTable("T");
  // If the new table was *not* created (it shouldn't be) then this will be
  // null.
  ASSERT_EQ(t->FindColumn("col3"), nullptr);
  // If the new table wasn't created (it shouldn't be) then this *won't* be
  // null.
  ASSERT_NE(t->FindColumn("col2"), nullptr);
}
}  // namespace

}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
