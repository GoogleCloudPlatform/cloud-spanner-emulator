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
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/updater/schema_updater_tests/base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {
namespace {

using database_api::DatabaseDialect::POSTGRESQL;
using testing::HasSubstr;

TEST_P(SchemaUpdaterTest, AlterTableIfExists_TableNotFound) {
  std::string ddl;
  if (GetParam() == POSTGRESQL) {
    ddl = "ALTER TABLE IF EXISTS NonExistent ADD COLUMN c2 bigint";
  } else {
    ddl = "ALTER TABLE IF EXISTS NonExistent ADD COLUMN c2 INT64";
  }
  GOOGLESQL_EXPECT_OK(UpdateSchema(nullptr, {ddl}, "", GetParam(),
                         /*use_gsql_to_pg_translation=*/false));
}

TEST_P(SchemaUpdaterTest, AlterTableIfExists_AddColumn) {
  std::string create_ddl;
  std::string alter_ddl;
  if (GetParam() == POSTGRESQL) {
    create_ddl = "CREATE TABLE T(col1 bigint PRIMARY KEY)";
    alter_ddl = "ALTER TABLE IF EXISTS T ADD COLUMN c2 bigint";
  } else {
    create_ddl = "CREATE TABLE T(col1 INT64) PRIMARY KEY(col1)";
    alter_ddl = "ALTER TABLE IF EXISTS T ADD COLUMN c2 INT64";
  }
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema({create_ddl}, "", GetParam(),
                                    /*use_gsql_to_pg_translation=*/false));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto new_schema,
                       UpdateSchema(schema.get(), {alter_ddl}, "", GetParam(),
                                    /*use_gsql_to_pg_translation=*/false));
  const Table* t = new_schema->FindTable("T");
  ASSERT_NE(t, nullptr);
  EXPECT_NE(t->FindColumn("c2"), nullptr);
}

TEST_P(SchemaUpdaterTest, AlterTableIfExists_DropColumn) {
  std::string create_ddl;
  std::string alter_ddl;
  if (GetParam() == POSTGRESQL) {
    create_ddl = "CREATE TABLE T(col1 bigint PRIMARY KEY, col2 bigint)";
    alter_ddl = "ALTER TABLE IF EXISTS T DROP COLUMN col2";
  } else {
    create_ddl = "CREATE TABLE T(col1 INT64, col2 INT64) PRIMARY KEY(col1)";
    alter_ddl = "ALTER TABLE IF EXISTS T DROP COLUMN col2";
  }
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema({create_ddl}, "", GetParam(),
                                    /*use_gsql_to_pg_translation=*/false));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto new_schema,
                       UpdateSchema(schema.get(), {alter_ddl}, "", GetParam(),
                                    /*use_gsql_to_pg_translation=*/false));
  const Table* t = new_schema->FindTable("T");
  ASSERT_NE(t, nullptr);
  EXPECT_EQ(t->FindColumn("col2"), nullptr);
}

TEST_P(SchemaUpdaterTest, AlterTableIfExists_DropColumn_TableNotFound) {
  std::string ddl;
  if (GetParam() == POSTGRESQL) {
    ddl = "ALTER TABLE IF EXISTS NonExistent DROP COLUMN col2";
  } else {
    ddl = "ALTER TABLE IF EXISTS NonExistent DROP COLUMN col2";
  }
  GOOGLESQL_EXPECT_OK(UpdateSchema(nullptr, {ddl}, "", GetParam(),
                         /*use_gsql_to_pg_translation=*/false));
}

TEST_P(SchemaUpdaterTest, AlterTableIfExists_RenameTo) {
  std::string create_ddl;
  std::string alter_ddl;
  if (GetParam() == POSTGRESQL) {
    create_ddl = "CREATE TABLE T(col1 bigint PRIMARY KEY)";
    alter_ddl = "ALTER TABLE IF EXISTS T RENAME TO S";
  } else {
    create_ddl = "CREATE TABLE T(col1 INT64) PRIMARY KEY(col1)";
    alter_ddl = "ALTER TABLE IF EXISTS T RENAME TO S";
  }
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema({create_ddl}, "", GetParam(),
                                    /*use_gsql_to_pg_translation=*/false));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto new_schema,
                       UpdateSchema(schema.get(), {alter_ddl}, "", GetParam(),
                                    /*use_gsql_to_pg_translation=*/false));
  EXPECT_EQ(new_schema->FindTable("T"), nullptr);
  EXPECT_NE(new_schema->FindTable("S"), nullptr);
}

TEST_P(SchemaUpdaterTest, AlterTableIfExists_RenameTo_TableNotFound) {
  std::string ddl;
  if (GetParam() == POSTGRESQL) {
    ddl = "ALTER TABLE IF EXISTS NonExistent RENAME TO S";
  } else {
    ddl = "ALTER TABLE IF EXISTS NonExistent RENAME TO S";
  }
  GOOGLESQL_EXPECT_OK(UpdateSchema(nullptr, {ddl}, "", GetParam(),
                         /*use_gsql_to_pg_translation=*/false));
}

TEST_P(SchemaUpdaterTest, AlterTableIfExists_AddConstraint) {
  std::string create_ddl;
  std::string alter_ddl;
  if (GetParam() == POSTGRESQL) {
    create_ddl = "CREATE TABLE T(col1 bigint PRIMARY KEY)";
    alter_ddl =
        "ALTER TABLE IF EXISTS T ADD CONSTRAINT c_gt_0 check (col1 > 0)";
  } else {
    create_ddl = "CREATE TABLE T(col1 INT64) PRIMARY KEY(col1)";
    alter_ddl =
        "ALTER TABLE IF EXISTS T ADD CONSTRAINT c_gt_0 check (col1 > 0)";
  }
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema({create_ddl}, "", GetParam(),
                                    /*use_gsql_to_pg_translation=*/false));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto new_schema,
                       UpdateSchema(schema.get(), {alter_ddl}, "", GetParam(),
                                    /*use_gsql_to_pg_translation=*/false));
  const Table* t = new_schema->FindTable("T");
  ASSERT_NE(t, nullptr);
}

TEST_P(SchemaUpdaterTest, AlterTableIfExists_AddConstraint_TableNotFound) {
  std::string ddl;
  if (GetParam() == POSTGRESQL) {
    ddl =
        "ALTER TABLE IF EXISTS NonExistent ADD CONSTRAINT c_gt_0 check (col1 > "
        "0)";
  } else {
    ddl =
        "ALTER TABLE IF EXISTS NonExistent ADD CONSTRAINT c_gt_0 check (col1 > "
        "0)";
  }
  GOOGLESQL_EXPECT_OK(UpdateSchema(nullptr, {ddl}, "", GetParam(),
                         /*use_gsql_to_pg_translation=*/false));
}

TEST_P(SchemaUpdaterTest, AlterTableIfExists_DropConstraint) {
  std::string create_ddl;
  std::string alter_ddl;
  if (GetParam() == POSTGRESQL) {
    create_ddl = R"(CREATE TABLE T(
                      col1 bigint PRIMARY KEY,
                      CONSTRAINT c_gt_0 check (col1 > 0)
                    ))";
    alter_ddl = "ALTER TABLE IF EXISTS T DROP CONSTRAINT c_gt_0";
  } else {
    create_ddl = R"(CREATE TABLE T(
                      col1 INT64,
                      CONSTRAINT c_gt_0 check (col1 > 0)
                    ) PRIMARY KEY(col1))";
    alter_ddl = "ALTER TABLE IF EXISTS T DROP CONSTRAINT c_gt_0";
  }
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema({create_ddl}, "", GetParam(),
                                    /*use_gsql_to_pg_translation=*/false));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto new_schema,
                       UpdateSchema(schema.get(), {alter_ddl}, "", GetParam(),
                                    /*use_gsql_to_pg_translation=*/false));
}

TEST_P(SchemaUpdaterTest, AlterTableIfExists_DropConstraint_TableNotFound) {
  std::string ddl;
  if (GetParam() == POSTGRESQL) {
    ddl = "ALTER TABLE IF EXISTS NonExistent DROP CONSTRAINT c_gt_0";
  } else {
    ddl = "ALTER TABLE IF EXISTS NonExistent DROP CONSTRAINT c_gt_0";
  }
  GOOGLESQL_EXPECT_OK(UpdateSchema(nullptr, {ddl}, "", GetParam(),
                         /*use_gsql_to_pg_translation=*/false));
}

TEST_P(SchemaUpdaterTest, AlterTableIfExists_AddForeignKey) {
  std::string create_t1;
  std::string create_t2;
  std::string alter_ddl;
  if (GetParam() == POSTGRESQL) {
    create_t1 = "CREATE TABLE \"T1\"(id bigint PRIMARY KEY)";
    create_t2 = "CREATE TABLE \"T2\"(id bigint PRIMARY KEY, ref_id bigint)";
    alter_ddl =
        "ALTER TABLE IF EXISTS \"T2\" ADD FOREIGN KEY (ref_id) REFERENCES "
        "\"T1\"(id)";
  } else {
    create_t1 = "CREATE TABLE T1(id INT64) PRIMARY KEY(id)";
    create_t2 = "CREATE TABLE T2(id INT64, ref_id INT64) PRIMARY KEY(id)";
    alter_ddl =
        "ALTER TABLE IF EXISTS T2 ADD FOREIGN KEY (ref_id) REFERENCES T1(id)";
  }
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema({create_t1, create_t2}, "", GetParam(),
                                    /*use_gsql_to_pg_translation=*/false));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto new_schema,
                       UpdateSchema(schema.get(), {alter_ddl}, "", GetParam(),
                                    /*use_gsql_to_pg_translation=*/false));

  const Table* t2 = new_schema->FindTable("T2");
  ASSERT_NE(t2, nullptr);
  bool found_fk = false;
  for (const auto* fk : t2->foreign_keys()) {
    if (fk->referenced_table()->Name() == "T1") {
      found_fk = true;
      break;
    }
  }
  EXPECT_TRUE(found_fk);
}

TEST_P(SchemaUpdaterTest, AlterTableIfExists_AddForeignKey_TableNotFound) {
  std::string ddl;
  if (GetParam() == POSTGRESQL) {
    ddl =
        "ALTER TABLE IF EXISTS NonExistent ADD FOREIGN KEY (ref_id) REFERENCES "
        "T1(id)";
  } else {
    ddl =
        "ALTER TABLE IF EXISTS NonExistent ADD FOREIGN KEY (ref_id) REFERENCES "
        "T1(id)";
  }
  GOOGLESQL_EXPECT_OK(UpdateSchema(nullptr, {ddl}, "", GetParam(),
                         /*use_gsql_to_pg_translation=*/false));
}

TEST_P(SchemaUpdaterTest, AlterTableIfExists_SetOnDelete) {
  if (GetParam() == POSTGRESQL) {
    return;
  }

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema({R"(
    CREATE TABLE T1(
      id INT64
    ) PRIMARY KEY(id))",
                                     R"(
    CREATE TABLE T2(
      id INT64,
      ref_id INT64
    ) PRIMARY KEY(id), INTERLEAVE IN PARENT T1 ON DELETE NO ACTION)"},
                                    "", GetParam(),
                                    /*use_gsql_to_pg_translation=*/false));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto new_schema, UpdateSchema(schema.get(), {R"(
    ALTER TABLE IF EXISTS T2 SET ON DELETE CASCADE
  )"}));
  const Table* t2 = new_schema->FindTable("T2");
  ASSERT_NE(t2, nullptr);
  EXPECT_EQ(t2->on_delete_action(), Table::OnDeleteAction::kCascade);
}

TEST_P(SchemaUpdaterTest, AlterTableIfExists_SetOnDelete_TableNotFound) {
  if (GetParam() == POSTGRESQL) return;

  GOOGLESQL_EXPECT_OK(UpdateSchema(nullptr, {R"(
    ALTER TABLE IF EXISTS NonExistent SET ON DELETE CASCADE
  )"},
                         "", GetParam(),
                         /*use_gsql_to_pg_translation=*/false));
}

TEST_P(SchemaUpdaterTest, AlterTableIfExists_RowDeletionPolicy) {
  std::string create_ddl;
  std::string add_ddl;
  std::string replace_ddl;
  std::string drop_ddl;
  if (GetParam() == POSTGRESQL) {
    create_ddl = R"(
      CREATE TABLE T(
        id bigint PRIMARY KEY,
        ts timestamp with time zone
      ))";
    add_ddl = "ALTER TABLE IF EXISTS T ADD TTL INTERVAL '1 days' ON ts";
    replace_ddl = "ALTER TABLE IF EXISTS T ALTER TTL INTERVAL '2 days' ON ts";
    drop_ddl = "ALTER TABLE IF EXISTS T DROP TTL";
  } else {
    create_ddl = R"(
      CREATE TABLE T(
        id INT64,
        ts TIMESTAMP
      ) PRIMARY KEY(id))";
    add_ddl =
        "ALTER TABLE IF EXISTS T ADD ROW DELETION POLICY (OLDER_THAN(ts, "
        "INTERVAL 1 DAY))";
    replace_ddl =
        "ALTER TABLE IF EXISTS T REPLACE ROW DELETION POLICY (OLDER_THAN(ts, "
        "INTERVAL 2 DAY))";
    drop_ddl = "ALTER TABLE IF EXISTS T DROP ROW DELETION POLICY";
  }

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema({create_ddl}, "", GetParam(),
                                    /*use_gsql_to_pg_translation=*/false));
  // Verify that adding a row deletion policy to an existing table works.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto schema_add,
                       UpdateSchema(schema.get(), {add_ddl}, "", GetParam(),
                                    /*use_gsql_to_pg_translation=*/false));
  const Table* t = schema_add->FindTable("T");
  ASSERT_NE(t, nullptr);
  EXPECT_TRUE(t->row_deletion_policy().has_value());

  // Verify that replacing a row deletion policy on an existing table works.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto schema_replace,
      UpdateSchema(schema_add.get(), {replace_ddl}, "", GetParam(),
                   /*use_gsql_to_pg_translation=*/false));
  t = schema_replace->FindTable("T");
  ASSERT_NE(t, nullptr);
  EXPECT_TRUE(t->row_deletion_policy().has_value());

  // Verify that dropping a row deletion policy from an existing table works.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto schema_drop,
      UpdateSchema(schema_replace.get(), {drop_ddl}, "", GetParam(),
                   /*use_gsql_to_pg_translation=*/false));
  t = schema_drop->FindTable("T");
  EXPECT_FALSE(t->row_deletion_policy().has_value());
}

TEST_P(SchemaUpdaterTest, AlterTableIfExists_RowDeletionPolicy_TableNotFound) {
  std::string add_ddl;
  std::string replace_ddl;
  std::string drop_ddl;
  if (GetParam() == POSTGRESQL) {
    add_ddl =
        "ALTER TABLE IF EXISTS NonExistent ADD TTL INTERVAL '1 days' ON ts";
    replace_ddl =
        "ALTER TABLE IF EXISTS NonExistent ALTER TTL INTERVAL '2 days' ON ts";
    drop_ddl = "ALTER TABLE IF EXISTS NonExistent DROP TTL";
  } else {
    add_ddl =
        "ALTER TABLE IF EXISTS NonExistent ADD ROW DELETION POLICY "
        "(OLDER_THAN(ts, INTERVAL 1 DAY))";
    replace_ddl =
        "ALTER TABLE IF EXISTS NonExistent REPLACE ROW DELETION POLICY "
        "(OLDER_THAN(ts, INTERVAL 2 DAY))";
    drop_ddl = "ALTER TABLE IF EXISTS NonExistent DROP ROW DELETION POLICY";
  }

  GOOGLESQL_EXPECT_OK(UpdateSchema(nullptr, {add_ddl}, "", GetParam(),
                         /*use_gsql_to_pg_translation=*/false));
  GOOGLESQL_EXPECT_OK(UpdateSchema(nullptr, {replace_ddl}, "", GetParam(),
                         /*use_gsql_to_pg_translation=*/false));
  GOOGLESQL_EXPECT_OK(UpdateSchema(nullptr, {drop_ddl}, "", GetParam(),
                         /*use_gsql_to_pg_translation=*/false));
}

TEST_P(SchemaUpdaterTest, AlterTableIfExists_Synonym) {
  if (GetParam() == POSTGRESQL) return;

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema({R"(
    CREATE TABLE T(
      id INT64
    ) PRIMARY KEY(id))"},
                                    "", GetParam(),
                                    /*use_gsql_to_pg_translation=*/false));
  // Verify that adding a synonym to an existing table works.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto schema_add,
                       UpdateSchema(schema.get(), {R"(
    ALTER TABLE IF EXISTS T ADD SYNONYM S
  )"},
                                    "", GetParam(),
                                    /*use_gsql_to_pg_translation=*/false));
  EXPECT_NE(schema_add->FindTableUsingSynonym("S"), nullptr);
  EXPECT_EQ(schema_add->FindTableUsingSynonym("S")->Name(), "T");
  // Verify that dropping a synonym from an existing table works.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto schema_drop,
                       UpdateSchema(schema_add.get(), {R"(
    ALTER TABLE IF EXISTS T DROP SYNONYM S
  )"},
                                    "", GetParam(),
                                    /*use_gsql_to_pg_translation=*/false));
  EXPECT_EQ(schema_drop->FindTableUsingSynonym("S"), nullptr);
}

TEST_P(SchemaUpdaterTest, AlterTableIfExists_Synonym_TableNotFound) {
  if (GetParam() == POSTGRESQL) return;

  GOOGLESQL_EXPECT_OK(UpdateSchema(nullptr, {R"(
    ALTER TABLE IF EXISTS NonExistent ADD SYNONYM S
  )"},
                         "", GetParam(),
                         /*use_gsql_to_pg_translation=*/false));
  GOOGLESQL_EXPECT_OK(UpdateSchema(nullptr, {R"(
    ALTER TABLE IF EXISTS NonExistent DROP SYNONYM S
  )"},
                         "", GetParam(),
                         /*use_gsql_to_pg_translation=*/false));
}

TEST_P(SchemaUpdaterTest, AlterTableIfExists_SetOptions_TableNotFound) {
  if (GetParam() == POSTGRESQL) return;

  GOOGLESQL_EXPECT_OK(UpdateSchema(nullptr, {R"(
    ALTER TABLE IF EXISTS NonExistent SET OPTIONS (locality_group = 'default')
  )"},
                         "", GetParam(),
                         /*use_gsql_to_pg_translation=*/false));
}

TEST_P(SchemaUpdaterTest, AlterTableIfExists_AlterColumn_TableNotFound) {
  std::string ddl;
  if (GetParam() == POSTGRESQL) {
    ddl = "ALTER TABLE IF EXISTS NonExistent ALTER COLUMN c TYPE bigint";
  } else {
    ddl = "ALTER TABLE IF EXISTS NonExistent ALTER COLUMN c INT64";
  }
  GOOGLESQL_EXPECT_OK(UpdateSchema(nullptr, {ddl}, "", GetParam(),
                         /*use_gsql_to_pg_translation=*/false));
}

TEST_P(SchemaUpdaterTest, AlterTableIfExists_SetColumnOptions_Fail) {
  if (GetParam() == POSTGRESQL) return;  // GSQL only syntax for now

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema({R"(
    CREATE TABLE T(
      col1 INT64
    ) PRIMARY KEY(col1))"},
                                    "", GetParam(),
                                    /*use_gsql_to_pg_translation=*/false));

  EXPECT_THAT(
      UpdateSchema(schema.get(), {R"(
    ALTER TABLE IF EXISTS T ALTER COLUMN col1
    SET OPTIONS (allow_commit_timestamp = true)
  )"},
                   "", GetParam(),
                   /*use_gsql_to_pg_translation=*/false),
      googlesql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument,
                                HasSubstr("IF EXISTS is not supported")));
}

}  // namespace
}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
