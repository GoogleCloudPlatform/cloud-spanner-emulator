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

#include "backend/schema/updater/schema_updater.h"

#include <iterator>
#include <memory>

#include "zetasql/public/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/memory/memory.h"
#include "zetasql/base/status.h"
#include "absl/strings/substitute.h"
#include "absl/time/time.h"
#include "backend/common/ids.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/graph/schema_graph.h"
#include "backend/schema/graph/schema_graph_editor.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/storage/in_memory_storage.h"
#include "common/errors.h"
#include "common/limits.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

namespace types = zetasql::types;

// Matcher for matching a zetasql_base::StatusOr with an expected status.
MATCHER_P(StatusIs, status, "") { return arg.status() == status; }

MATCHER_P2(ColumnIs, name, type, "") {
  bool match = true;
  match &= ::testing::ExplainMatchResult(::testing::Eq(name), arg->Name(),
                                         result_listener);
  match &= arg->GetType()->Equals(type);
  if (!match) {
    (*result_listener) << "("
                       << absl::StrJoin(
                              {arg->Name(), arg->GetType()->DebugString()}, ",")
                       << ")";
  }
  return match;
}

MATCHER_P2(IsKeyColumnOf, table, order, "") {
  auto pk = table->FindKeyColumn(arg->Name());
  if (pk == nullptr) {
    return ::testing::ExplainMatchResult(::testing::Eq(arg), nullptr,
                                         result_listener);
  }
  return ::testing::ExplainMatchResult(::testing::Eq(pk->is_descending()),
                                       std::string(order) == "DESC",
                                       result_listener);
}

MATCHER_P2(IsInterleavedIn, parent, on_delete, "") {
  // Check parent-child relationship.
  EXPECT_EQ(arg->parent(), parent);
  auto children = parent->children();
  auto it = std::find(children.begin(), children.end(), arg);
  EXPECT_NE(it, children.end());
  EXPECT_EQ(arg->on_delete_action(), on_delete);

  // Check primary keys.
  auto parent_pk = parent->primary_key();
  auto child_pk = arg->primary_key();
  EXPECT_LE(parent_pk.size(), child_pk.size());

  // Nullability of interleaved null-filtered index key columns can be
  // different from parent key columns.
  bool ignore_nullability =
      arg->owner_index() != nullptr && arg->owner_index()->is_null_filtered();

  for (int i = 0; i < parent_pk.size(); ++i) {
    EXPECT_THAT(child_pk[i]->column(),
                ColumnIs(parent_pk[i]->column()->Name(),
                         parent_pk[i]->column()->GetType()));
    EXPECT_EQ(child_pk[i]->is_descending(), parent_pk[i]->is_descending());
    EXPECT_EQ(child_pk[i]->column()->declared_max_length(),
              parent_pk[i]->column()->declared_max_length());
    if (!ignore_nullability) {
      EXPECT_EQ(child_pk[i]->column()->is_nullable(),
                parent_pk[i]->column()->is_nullable());
    }
  }
  return true;
}

MATCHER_P(SourceColumnIs, source, "") {
  EXPECT_EQ(arg->source_column(), source);
  EXPECT_EQ(arg->Name(), source->Name());
  EXPECT_TRUE(arg->GetType()->Equals(source->GetType()));
  EXPECT_EQ(arg->declared_max_length(), source->declared_max_length());
  return true;
}

class SchemaUpdaterTest : public testing::Test {
 public:
  SchemaUpdaterTest() {}

  zetasql_base::StatusOr<std::unique_ptr<const Schema>> CreateSchema(
      absl::Span<const std::string> statements) {
    return UpdateSchema(/*base_schema=*/nullptr, statements);
  }

  zetasql_base::StatusOr<std::unique_ptr<const Schema>> UpdateSchema(
      const Schema* base_schema, absl::Span<const std::string> statements) {
    SchemaUpdater updater;
    SchemaChangeContext context{.type_factory = &type_factory_,
                                .table_id_generator = &table_id_generator_,
                                .column_id_generator = &column_id_generator_};
    return updater.ValidateSchemaFromDDL(statements, context, base_schema);
  }

 protected:
  zetasql::TypeFactory type_factory_;

  TableIDGenerator table_id_generator_;

  ColumnIDGenerator column_id_generator_;
};

TEST_F(SchemaUpdaterTest, CreationOrder) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"(
      CREATE TABLE T1 (
        k1 INT64,
        k2 INT64
      ) PRIMARY KEY (k1)
    )",
                                        R"(
      CREATE TABLE T2 (
        k1 INT64,
        k2 INT64,
        c1 BYTES(MAX)
      ) PRIMARY KEY (k1,k2), INTERLEAVE IN PARENT T1
    )",
                                        R"(
      CREATE INDEX Idx1 ON T1(k1) STORING(k2)
    )",
                                        R"(
      CREATE INDEX Idx2 ON T2(k1,c1), INTERLEAVE IN T1
    )"}));

  auto t1 = schema->FindTable("T1");
  auto t2 = schema->FindTable("T2");
  auto idx1 = schema->FindIndex("Idx1");
  auto idx2 = schema->FindIndex("Idx2");
  auto idx1_dt = idx1->index_data_table();
  auto idx2_dt = idx2->index_data_table();

  // Check that the nodes are added in topological order so that they
  // are cloned and validated in topological order.
  std::vector<const SchemaNode*> expected = {
      t1->FindColumn("k1"),
      t1->FindColumn("k2"),
      t1->FindKeyColumn("k1"),
      t1,
      t2->FindColumn("k1"),
      t2->FindColumn("k2"),
      t2->FindColumn("c1"),
      t2->FindKeyColumn("k1"),
      t2->FindKeyColumn("k2"),
      t2,
      idx1_dt->FindColumn("k1"),
      idx1_dt->FindKeyColumn("k1"),
      idx1_dt->FindColumn("k2"),
      idx1,
      idx1_dt,
      idx2_dt->FindColumn("k1"),
      idx2_dt->FindColumn("c1"),
      idx2_dt->FindColumn("k2"),
      idx2_dt->FindKeyColumn("k1"),
      idx2_dt->FindKeyColumn("c1"),
      idx2_dt->FindKeyColumn("k2"),
      idx2,
      idx2_dt,
  };

  EXPECT_THAT(schema->GetSchemaGraph()->GetSchemaNodes(),
              testing::ElementsAreArray(expected));
}

TEST_F(SchemaUpdaterTest, CreateTable_SingleKey) {
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

TEST_F(SchemaUpdaterTest, CreateTable_MultiKey) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
    CREATE TABLE T(
      col1 INT64,
      col2 STRING(MAX)
    ) PRIMARY KEY(col1 DESC, col2))"}));

  auto t = schema->FindTable("T");
  EXPECT_NE(t, nullptr);
  EXPECT_EQ(t->columns().size(), 2);
  EXPECT_EQ(t->primary_key().size(), 2);

  auto col1 = t->columns()[0];
  EXPECT_THAT(col1, ColumnIs("col1", types::Int64Type()));
  EXPECT_THAT(col1, IsKeyColumnOf(t, "DESC"));

  auto col2 = t->columns()[1];
  EXPECT_THAT(col2, ColumnIs("col2", types::StringType()));
  EXPECT_THAT(col2, IsKeyColumnOf(t, "ASC"));
}

TEST_F(SchemaUpdaterTest, CreateTable_NoKey) {
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

TEST_F(SchemaUpdaterTest, CreateTable_NoColumns) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
    CREATE TABLE T(
    ) PRIMARY KEY())"}));

  auto t = schema->FindTable("T");
  EXPECT_NE(t, nullptr);
  EXPECT_EQ(t->columns().size(), 0);
  EXPECT_EQ(t->primary_key().size(), 0);
}

TEST_F(SchemaUpdaterTest, CreateTable_ColumnLength) {
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

TEST_F(SchemaUpdaterTest, CreateTable_AllowCommitTimestamp) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
    CREATE TABLE T(
      col1 INT64,
      col2 TIMESTAMP OPTIONS(
        allow_commit_timestamp = true
      )
    ) PRIMARY KEY(col1))"}));

  auto t = schema->FindTable("T");
  auto col2 = t->columns()[1];
  EXPECT_THAT(col2, ColumnIs("col2", types::TimestampType()));
  EXPECT_TRUE(col2->allows_commit_timestamp());

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

TEST_F(SchemaUpdaterTest, CreateTable_InvalidColumnLength) {
  EXPECT_THAT(CreateSchema({R"(
    CREATE TABLE T(
      col1 BYTES(1000000000)
    ) PRIMARY KEY())"}),
              StatusIs(error::InvalidColumnLength(
                  "T.col1", 1000000000, 1, limits::kMaxBytesColumnLength)));
}

TEST_F(SchemaUpdaterTest, CreateTable_DuplicateKeys) {
  EXPECT_THAT(CreateSchema({R"(
    CREATE TABLE T(
      col1 INT64,
      col2 BYTES(10)
    ) PRIMARY KEY(col1,col1))"}),
              StatusIs(error::MultipleRefsToKeyColumn("Table", "T", "col1")));
}

TEST_F(SchemaUpdaterTest, CreateTable_DuplicateColumns) {
  EXPECT_THAT(CreateSchema({R"(
    CREATE TABLE T(
      col1 INT64,
      col1 STRING(MAX)
    ) PRIMARY KEY(col1,col1))"}),
              StatusIs(error::DuplicateColumnName("T.col1")));
}

TEST_F(SchemaUpdaterTest, CreateTable_ColumnNullability) {
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

TEST_F(SchemaUpdaterTest, CreateTable_ColumnNotFound) {
  EXPECT_THAT(CreateSchema({R"(
        CREATE TABLE T(
          col1 INT64
        ) PRIMARY KEY(col2))"}),
              StatusIs(error::NonExistentKeyColumn("Table", "T", "col2")));
}

TEST_F(SchemaUpdaterTest, CreateTable_AlreadyExists) {
  EXPECT_THAT(CreateSchema({
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

TEST_F(SchemaUpdaterTest, CreateTable_Interleave) {
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

TEST_F(SchemaUpdaterTest, CreateTable_InterleaveMismatch) {
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

TEST_F(SchemaUpdaterTest, CreateTable_InterleaveDepth) {
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
              StatusIs(error::DeepNesting("Table", "T6",
                                          limits::kMaxInterleavingDepth)));
}

TEST_F(SchemaUpdaterTest, CreateTable_ParentNotFound) {
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

TEST_F(SchemaUpdaterTest, CreateIndex) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"(
      CREATE TABLE T (
        k1 INT64 NOT NULL,
        c1 STRING(10),
        c2 STRING(MAX)
      ) PRIMARY KEY (k1)
    )",
                                        R"(
      CREATE INDEX Idx1 ON T(c1)
    )",
                                        R"(
      CREATE INDEX Idx2 ON T(c1) STORING(c2))",
                                    }));

  auto idx = schema->FindIndex("Idx1");
  EXPECT_NE(idx, nullptr);

  auto t = schema->FindTable("T");
  EXPECT_EQ(idx->indexed_table(), t);
  EXPECT_FALSE(idx->is_null_filtered());
  EXPECT_FALSE(idx->is_unique());
  EXPECT_EQ(idx->key_columns().size(), 1);
  EXPECT_EQ(idx->stored_columns().size(), 0);

  // The data table is not discoverable in the Schema.
  EXPECT_EQ(schema->FindTable(absl::StrCat(kIndexDataTablePrefix, "Idx1")),
            nullptr);
  auto idx_data = idx->index_data_table();
  EXPECT_NE(idx_data, nullptr);
  EXPECT_TRUE(idx_data->indexes().empty());

  EXPECT_EQ(idx_data->primary_key().size(), 2);
  auto data_pk = idx_data->primary_key();

  auto t_c1 = t->FindColumn("c1");
  EXPECT_THAT(data_pk[0]->column(), ColumnIs("c1", type_factory_.get_string()));
  EXPECT_THAT(data_pk[0]->column(), SourceColumnIs(t_c1));
  EXPECT_EQ(data_pk[0], idx->key_columns()[0]);

  auto t_k1 = t->FindColumn("k1");
  EXPECT_THAT(data_pk[1]->column(), ColumnIs("k1", type_factory_.get_int64()));
  EXPECT_THAT(data_pk[1]->column(), SourceColumnIs(t_k1));

  // For non-null-filtered indexes, the nullability of column matches
  // the nullability of source column.
  EXPECT_EQ(data_pk[0]->column()->is_nullable(), t_c1->is_nullable());
  EXPECT_EQ(data_pk[1]->column()->is_nullable(), t_k1->is_nullable());

  auto idx2 = schema->FindIndex("Idx2");
  EXPECT_NE(idx2, nullptr);
  EXPECT_EQ(idx2->stored_columns().size(), 1);
  auto t_c2 = t->FindColumn("c2");
  auto idx2_c2 = idx2->stored_columns()[0];
  EXPECT_THAT(idx2_c2, ColumnIs("c2", type_factory_.get_string()));
  EXPECT_THAT(idx2_c2, SourceColumnIs(t_c2));
}

TEST_F(SchemaUpdaterTest, CreateIndex_NoKeys) {
  EXPECT_THAT(CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64
      ) PRIMARY KEY (k1)
    )",
                            R"(
      CREATE INDEX Idx ON T()
    )"}),
              StatusIs(error::IndexWithNoKeys("Idx")));
}

TEST_F(SchemaUpdaterTest, CreateIndex_DescKeys) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64
      ) PRIMARY KEY (k1 ASC)
    )",
                                                  R"(
      CREATE INDEX Idx ON T(c1 DESC, k1 DESC)
    )"}));

  auto idx = schema->FindIndex("Idx");
  EXPECT_NE(idx, nullptr);
  EXPECT_EQ(idx->key_columns().size(), 2);
  EXPECT_TRUE(idx->key_columns()[0]->is_descending());
  EXPECT_TRUE(idx->key_columns()[1]->is_descending());
}

TEST_F(SchemaUpdaterTest, CreateIndex_SharedPK) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"(
      CREATE TABLE T (
        k1 INT64 NOT NULL,
        c1 STRING(MAX),
        c2 STRING(MAX)
      ) PRIMARY KEY (k1)
    )",
                                        R"(
      CREATE NULL_FILTERED INDEX Idx ON T(k1) STORING(c2)
    )"}));

  auto t = schema->FindTable("T");
  auto k1 = t->FindColumn("k1");

  auto idx = schema->FindIndex("Idx");
  EXPECT_NE(idx, nullptr);
  EXPECT_EQ(idx->stored_columns().size(), 1);
  EXPECT_EQ(idx->key_columns().size(), 1);

  auto idx_data = idx->index_data_table();
  EXPECT_EQ(idx_data->primary_key().size(), 1);
  EXPECT_THAT(idx_data->primary_key()[0]->column(), SourceColumnIs(k1));
}

TEST_F(SchemaUpdaterTest, CreateIndex_NullFiltered_Unique) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"(
      CREATE TABLE T (
        k1 INT64,
        c1 STRING(MAX),
        c2 STRING(MAX),
        c3 STRING(MAX) NOT NULL,
      ) PRIMARY KEY (k1)
    )",
                                        R"(
      CREATE UNIQUE NULL_FILTERED INDEX Idx ON T(c1) STORING(c2,c3)
    )"}));

  auto idx = schema->FindIndex("Idx");
  EXPECT_TRUE(idx->is_null_filtered());
  EXPECT_TRUE(idx->is_unique());

  auto idx_data = idx->index_data_table();
  auto data_columns = idx_data->columns();
  EXPECT_EQ(data_columns.size(), 4);

  // Indexed column is not nullable.
  EXPECT_THAT(data_columns[0], ColumnIs("c1", types::StringType()));
  EXPECT_FALSE(data_columns[0]->is_nullable());

  // Table PK nullability is retained.
  EXPECT_THAT(data_columns[1], ColumnIs("k1", types::Int64Type()));
  EXPECT_TRUE(data_columns[1]->is_nullable());

  // Stored columns nullability is retained.
  EXPECT_THAT(data_columns[2], ColumnIs("c2", types::StringType()));
  EXPECT_TRUE(data_columns[2]->is_nullable());

  EXPECT_THAT(data_columns[3], ColumnIs("c3", types::StringType()));
  EXPECT_FALSE(data_columns[3]->is_nullable());
}

TEST_F(SchemaUpdaterTest, CreateIndex_Interleave) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"(
      CREATE TABLE T1 (
        k1 INT64,
        k2 INT64
      ) PRIMARY KEY (k1)
    )",
                                        R"(
      CREATE TABLE T2 (
        k1 INT64,
        k2 INT64,
        c1 BYTES(MAX)
      ) PRIMARY KEY (k1,k2), INTERLEAVE IN PARENT T1
    )",
                                        R"(
      CREATE INDEX Idx ON T2(k1,c1), INTERLEAVE IN T1
    )"}));

  auto t1 = schema->FindTable("T1");
  EXPECT_NE(t1, nullptr);

  auto idx = schema->FindIndex("Idx");
  EXPECT_EQ(idx->parent(), t1);
  EXPECT_NE(idx, nullptr);
  auto idx_data = idx->index_data_table();
  EXPECT_EQ(idx_data->parent(), t1);
  EXPECT_THAT(idx_data, IsInterleavedIn(t1, Table::OnDeleteAction::kCascade));
}

TEST_F(SchemaUpdaterTest, CreateIndex_NullFilteredInterleave) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"(
      CREATE TABLE T1 (
        k1 INT64,
        k2 INT64
      ) PRIMARY KEY (k1)
    )",
                                        R"(
      CREATE TABLE T2 (
        k1 INT64,
        k2 INT64,
        c1 BYTES(MAX)
      ) PRIMARY KEY (k1,k2), INTERLEAVE IN PARENT T1
    )",
                                        R"(
      CREATE NULL_FILTERED INDEX Idx ON T2(k1,c1), INTERLEAVE IN T1
    )"}));

  auto t1 = schema->FindTable("T1");
  EXPECT_NE(t1, nullptr);

  auto idx = schema->FindIndex("Idx");
  EXPECT_EQ(idx->parent(), t1);
  EXPECT_NE(idx, nullptr);
  auto idx_data = idx->index_data_table();
  EXPECT_EQ(idx_data->parent(), t1);
  EXPECT_THAT(idx_data, IsInterleavedIn(t1, Table::OnDeleteAction::kCascade));

  EXPECT_TRUE(t1->FindColumn("k1")->is_nullable());
  EXPECT_FALSE(idx_data->FindColumn("k1")->is_nullable());
}

TEST_F(SchemaUpdaterTest, CreateIndex_InvalidInterleaved) {
  EXPECT_THAT(
      CreateSchema({R"(
      CREATE TABLE T1 (
        k1 INT64,
        k2 INT64
      ) PRIMARY KEY (k1)
    )",
                    R"(
      CREATE TABLE T2 (
        k1 INT64,
        k2 INT64,
        c1 BYTES(MAX)
      ) PRIMARY KEY (k1,k2)
    )",
                    R"(
      CREATE INDEX Idx ON T2(k1,c1), INTERLEAVE IN T1
    )"}),
      StatusIs(error::IndexInterleaveTableUnacceptable("Idx", "T2", "T1")));
}

TEST_F(SchemaUpdaterTest, CreateIndex_TableNotFound) {
  EXPECT_THAT(CreateSchema({"CREATE INDEX Idx ON T2(k1)"}),
              StatusIs(error::TableNotFound("T2")));
}

TEST_F(SchemaUpdaterTest, CreateIndex_ColumnNotFound) {
  EXPECT_THAT(CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64
      ) PRIMARY KEY (k1)
    )",
                            R"(
      CREATE INDEX Idx ON T(c2)
    )"}),
              StatusIs(error::IndexRefsNonExistentColumn("Idx", "c2")));
}

TEST_F(SchemaUpdaterTest, CreateIndex_DuplicateColumn) {
  EXPECT_THAT(CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64
      ) PRIMARY KEY (k1)
    )",
                            R"(
      CREATE INDEX Idx ON T(c1,c1)
    )"}),
              StatusIs(error::IndexRefsColumnTwice("Idx", "c1")));
}

TEST_F(SchemaUpdaterTest, CreateIndex_StoredRefsIndexKey) {
  EXPECT_THAT(CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64
      ) PRIMARY KEY (k1)
    )",
                            R"(
      CREATE INDEX Idx ON T(c1) STORING(c1)
    )"}),
              StatusIs(error::IndexRefsKeyAsStoredColumn("Idx", "c1")));
}

TEST_F(SchemaUpdaterTest, CreateIndex_UnsupportedArrayTypeKeyColumn) {
  EXPECT_THAT(CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
        c1 ARRAY<INT64>
      ) PRIMARY KEY (k1)
    )",
                            R"(
      CREATE INDEX Idx ON T(c1)
    )"}),
              StatusIs(error::CannotCreateIndexOnArrayColumns("Idx", "c1")));
}

TEST_F(SchemaUpdaterTest, CreateIndex_ArrayStoredColumn) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64,
        c2 ARRAY<INT64>
      ) PRIMARY KEY (k1)
    )",
                                                  R"(
      CREATE INDEX Idx ON T(c1) STORING(c2)
    )"}));

  auto idx = schema->FindIndex("Idx");
  EXPECT_NE(idx, nullptr);
  EXPECT_EQ(idx->stored_columns().size(), 1);
  auto c2 = idx->stored_columns()[0];

  const zetasql::ArrayType* array_type;
  ZETASQL_ASSERT_OK(type_factory_.MakeArrayType(types::Int64Type(), &array_type));

  EXPECT_THAT(c2, ColumnIs("c2", array_type));
}

TEST_F(SchemaUpdaterTest, AlterTable_AddColumn) {
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

TEST_F(SchemaUpdaterTest, AlterTable_AddColumn_AlreadyExists) {
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

TEST_F(SchemaUpdaterTest, AlterColumn_ChangeColumnType_StaticCheckValid) {
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

TEST_F(SchemaUpdaterTest, AlterColumn_ChangeColumnType_Invalid) {
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

TEST_F(SchemaUpdaterTest, AlterColumn_ChangeNonArrayToArray) {
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

TEST_F(SchemaUpdaterTest, AlterColumn_NotNullToNullable) {
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

TEST_F(SchemaUpdaterTest, AlterColumn_ChangeIndexedColumnType) {
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

TEST_F(SchemaUpdaterTest, AlterColumn_ChangeIndexedColumnNullability) {
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

TEST_F(SchemaUpdaterTest, AlterColumn_KeyColumnType) {
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

TEST_F(SchemaUpdaterTest, AlterColumn_KeyColumnNullability) {
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

TEST_F(SchemaUpdaterTest, AlterTable_UnsetAllowCommitTimestamp) {
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

TEST_F(SchemaUpdaterTest, AlterTable_DropColumn) {
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

TEST_F(SchemaUpdaterTest, AlterTable_InvalidDropKeyColumn) {
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

TEST_F(SchemaUpdaterTest, AlterTable_InvalidDropIndexedColumn) {
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

TEST_F(SchemaUpdaterTest, AlterTable_ChangeOnDelete) {
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

TEST_F(SchemaUpdaterTest, DropTable) {
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

TEST_F(SchemaUpdaterTest, DropTable_CanDropChildTable) {
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

TEST_F(SchemaUpdaterTest, DropTable_CanDropChildAndParentTogether) {
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

TEST_F(SchemaUpdaterTest, DropTable_Recreate) {
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

TEST_F(SchemaUpdaterTest, DropTable_WithIndex) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64
      ) PRIMARY KEY (k1 ASC)
    )",
                                                  R"(
      CREATE INDEX Idx1 ON T(c1 DESC, k1 DESC)
    )"}));

  // Global index.
  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      DROP TABLE T
    )"}),
              StatusIs(error::DropTableWithDependentIndices("T", "Idx1")));

  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64
      ) PRIMARY KEY (k1 ASC)
    )",
                                             R"(
      CREATE INDEX Idx2 ON T(k1), INTERLEAVE IN T
    )"}));

  // Interleaved index.
  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      DROP TABLE T
    )"}),
              StatusIs(error::DropTableWithDependentIndices("T", "Idx2")));
}

TEST_F(SchemaUpdaterTest, DropIndex) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64
      ) PRIMARY KEY (k1 ASC)
    )",
                                                  R"(
      CREATE INDEX Idx ON T(c1 DESC, k1 DESC)
    )"}));

  EXPECT_EQ(schema->GetSchemaGraph()->GetSchemaNodes().size(), 10);

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema, UpdateSchema(schema.get(), {R"(
      DROP INDEX Idx
    )"}));

  EXPECT_EQ(new_schema->FindIndex("Idx"), nullptr);

  // Check that the index data table (and other dependent nodes) are
  // also deleted.
  EXPECT_EQ(new_schema->GetSchemaGraph()->GetSchemaNodes().size(), 4);
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
