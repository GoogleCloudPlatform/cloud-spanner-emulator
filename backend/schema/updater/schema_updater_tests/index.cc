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

#include "backend/schema/catalog/index.h"

#include <memory>
#include <string>
#include <vector>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/type_factory.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/updater/schema_updater.h"
#include "backend/schema/updater/schema_updater_tests/base.h"
#include "common/errors.h"
#include "third_party/spanner_pg/datatypes/extended/spanner_extended_type.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {

namespace types = zetasql::types;

namespace {

using database_api::DatabaseDialect::POSTGRESQL;
using google::spanner::v1::TypeAnnotationCode::PG_JSONB;
using google::spanner::v1::TypeAnnotationCode::PG_NUMERIC;
using postgres_translator::spangres::datatypes::SpannerExtendedType;
using testing::IsEmpty;
using testing::UnorderedElementsAre;

TEST_P(SchemaUpdaterTest, CreateIndex) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema(
                                     {
                                         R"sql(
        CREATE TABLE T (
          k1 bigint primary key,
          c1 varchar(10),
          c2 varchar,
          c3 numeric,
          c4 jsonb
        )
      )sql",
                                         R"sql(
        CREATE INDEX Idx1 ON T(c1)
      )sql",
                                         R"sql(
        CREATE INDEX Idx2 ON T(c1) INCLUDE (c2, c3, c4))sql"},
                                     /*proto_descriptor_bytes=*/"",
                                     /*dialect=*/POSTGRESQL,
                                     /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({
                                     R"sql(
        CREATE TABLE T (
          k1 INT64 NOT NULL,
          c1 STRING(10),
          c2 STRING(MAX),
          c3 NUMERIC,
          c4 JSON
        ) PRIMARY KEY (k1)
      )sql",
                                     R"sql(
        CREATE INDEX Idx1 ON T(c1)
      )sql",
                                     R"sql(
        CREATE INDEX Idx2 ON T(c1) STORING(c2, c3, c4))sql",
                                 }));
  }

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
  EXPECT_EQ(idx2->stored_columns().size(), 3);
  auto t_c2 = t->FindColumn("c2");
  auto idx2_c2 = idx2->stored_columns()[0];
  EXPECT_THAT(idx2_c2, ColumnIs("c2", type_factory_.get_string()));
  EXPECT_THAT(idx2_c2, SourceColumnIs(t_c2));
  auto t_c3 = t->FindColumn("c3");
  auto idx2_c3 = idx2->stored_columns()[1];
  if (GetParam() == POSTGRESQL) {
    EXPECT_TRUE(idx2_c3->GetType()->IsExtendedType());
    EXPECT_EQ(
        static_cast<const SpannerExtendedType*>(idx2_c3->GetType())->code(),
        PG_NUMERIC);
  } else {
    EXPECT_THAT(idx2_c3, ColumnIs("c3", type_factory_.get_numeric()));
  }
  EXPECT_THAT(idx2_c3, SourceColumnIs(t_c3));
  auto t_c4 = t->FindColumn("c4");
  auto idx2_c4 = idx2->stored_columns()[2];
  if (GetParam() == POSTGRESQL) {
    EXPECT_TRUE(idx2_c4->GetType()->IsExtendedType());
    EXPECT_EQ(
        static_cast<const SpannerExtendedType*>(idx2_c4->GetType())->code(),
        PG_JSONB);
  } else {
    EXPECT_THAT(idx2_c4, ColumnIs("c4", type_factory_.get_json()));
  }
  EXPECT_THAT(idx2_c4, SourceColumnIs(t_c4));
}

TEST_P(SchemaUpdaterTest, CreateIndex_NoKeys) {
  // Creating an index with no key columns is not supported in PG.
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  EXPECT_THAT(CreateSchema({R"sql(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64
      ) PRIMARY KEY (k1)
    )sql",
                            R"sql(
      CREATE INDEX Idx ON T()
    )sql"}),
              StatusIs(error::IndexWithNoKeys("Idx")));
}

TEST_P(SchemaUpdaterTest, CreateIndexIfNotExists) {
  // IF NOT EXISTS isn't yet supported on the PG side of the emulator
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  EXPECT_THAT(CreateSchema({R"sql(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64
      ) PRIMARY KEY (k1)
    )sql",
                            R"sql(
      CREATE INDEX IF NOT EXISTS Idx ON T(c1)
    )sql"}),
              StatusIs(absl::OkStatus()));
}

TEST_P(SchemaUpdaterTest, CreateIndexWhereIsNotNull) {
  std::unique_ptr<const Schema> schema;
  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({
                                   R"sql(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64,
        s1 STRING(MAX),
      ) PRIMARY KEY (k1)
    )sql",
                                   R"sql(
      CREATE INDEX Idx ON T(c1, s1) WHERE c1 IS NOT NULL AND s1 IS NOT NULL
    )sql"}));
  const Index* idx = schema->FindIndex("Idx");
  EXPECT_NE(idx, nullptr);

  const Table* base_table = schema->FindTable("T");
  EXPECT_EQ(idx->indexed_table(), base_table);
  EXPECT_FALSE(idx->is_null_filtered());
  EXPECT_FALSE(idx->is_unique());
  EXPECT_EQ(idx->key_columns().size(), 2);
  EXPECT_EQ(idx->stored_columns().size(), 0);

  // The data table is not discoverable in the Schema.
  EXPECT_EQ(schema->FindTable(absl::StrCat(kIndexDataTablePrefix, "Idx")),
            nullptr);
  const Table* idx_data = idx->index_data_table();
  EXPECT_NE(idx_data, nullptr);
  EXPECT_TRUE(idx_data->indexes().empty());

  EXPECT_EQ(idx_data->primary_key().size(), 3);
  auto data_pk = idx_data->primary_key();

  auto t_c1 = base_table->FindColumn("c1");
  EXPECT_THAT(data_pk[0]->column(), ColumnIs("c1", type_factory_.get_int64()));
  EXPECT_THAT(data_pk[0]->column(), SourceColumnIs(t_c1));
  EXPECT_EQ(data_pk[0], idx->key_columns()[0]);
  EXPECT_FALSE(data_pk[0]->column()->is_nullable());

  auto t_s1 = base_table->FindColumn("s1");
  EXPECT_THAT(data_pk[1]->column(), ColumnIs("s1", type_factory_.get_string()));
  EXPECT_THAT(data_pk[1]->column(), SourceColumnIs(t_s1));
  EXPECT_FALSE(data_pk[1]->column()->is_nullable());

  auto t_k1 = base_table->FindColumn("k1");
  EXPECT_THAT(data_pk[2]->column(), ColumnIs("k1", type_factory_.get_int64()));
  EXPECT_THAT(data_pk[2]->column(), SourceColumnIs(t_k1));
  EXPECT_EQ(data_pk[2]->column()->is_nullable(), t_k1->is_nullable());
}

TEST_P(SchemaUpdaterTest, CreateIndexIfNotExistsOnExistingIndex) {
  // IF NOT EXISTS isn't yet supported on the PG side of the emulator
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  EXPECT_THAT(CreateSchema({R"sql(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64
      ) PRIMARY KEY (k1)
    )sql",
                            R"sql(
      CREATE INDEX Idx ON T(c1)
    )sql",
                            R"sql(
      CREATE INDEX IF NOT EXISTS Idx ON T(c1)
    )sql"}),
              StatusIs(absl::OkStatus()));
}

TEST_P(SchemaUpdaterTest, CreateIndex_DescKeys) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"sql(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64
      ) PRIMARY KEY (k1 ASC)
    )sql",
                                                  R"sql(
      CREATE INDEX Idx ON T(c1 DESC, k1 DESC)
    )sql"}));

  auto idx = schema->FindIndex("Idx");
  EXPECT_NE(idx, nullptr);
  EXPECT_EQ(idx->key_columns().size(), 2);
  EXPECT_TRUE(idx->key_columns()[0]->is_descending());
  EXPECT_TRUE(idx->key_columns()[1]->is_descending());
  EXPECT_TRUE(idx->key_columns()[0]->is_nulls_last());
  EXPECT_TRUE(idx->key_columns()[1]->is_nulls_last());
}

TEST_P(SchemaUpdaterTest, CreateIndex_AscKeys) {
  std::unique_ptr<const Schema> schema;
  if (GetParam() == POSTGRESQL) {
    // Custom DDL statements are required because the original Spanner DDL would
    // generate an ASC ordering by default. After the translation from Spanner
    // to PG, the ordering of the PG DDL is also ASC instead of ASC_NULLS_LAST.
    // If the ordering is not specified, the default ordering should be
    // ASC_NULLS_LAST in PG.
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                         CreateSchema({R"sql(
        CREATE TABLE T (
          k1 bigint primary key,
          c1 bigint
        )
      )sql",
                                       R"sql(
        CREATE INDEX Idx ON T(c1, k1)
      )sql"},
                                      /*proto_descriptor_bytes=*/"", POSTGRESQL,
                                      /*use_gsql_to_pg_translation=*/false));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"sql(
        CREATE TABLE T (
          k1 INT64,
          c1 INT64
        ) PRIMARY KEY (k1 ASC)
      )sql",
                                               R"sql(
        CREATE INDEX Idx ON T(c1, k1)
      )sql"}));
  }

  auto idx = schema->FindIndex("Idx");
  EXPECT_NE(idx, nullptr);
  EXPECT_EQ(idx->key_columns().size(), 2);
  EXPECT_FALSE(idx->key_columns()[0]->is_descending());
  EXPECT_FALSE(idx->key_columns()[1]->is_descending());
  if (GetParam() == POSTGRESQL) {
    // Sorted NULLs last
    EXPECT_TRUE(idx->key_columns()[0]->is_nulls_last());
    EXPECT_TRUE(idx->key_columns()[1]->is_nulls_last());
  } else {
    // Sorted NULLs first
    EXPECT_FALSE(idx->key_columns()[0]->is_nulls_last());
    EXPECT_FALSE(idx->key_columns()[1]->is_nulls_last());
  }
}

TEST_P(SchemaUpdaterTest, CreateIndex_SharedPK) {
  // Null filtered indexes are not supported in PG.
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"sql(
      CREATE TABLE T (
        k1 INT64 NOT NULL,
        c1 STRING(MAX),
        c2 STRING(MAX)
      ) PRIMARY KEY (k1)
    )sql",
                                        R"sql(
      CREATE NULL_FILTERED INDEX Idx ON T(k1) STORING(c2)
    )sql"}));

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

TEST_P(SchemaUpdaterTest, CreateIndex_NullFiltered_Unique) {
  // Null filtered indexes are not supported in PG.
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"sql(
      CREATE TABLE T (
        k1 INT64,
        c1 STRING(MAX),
        c2 STRING(MAX),
        c3 STRING(MAX) NOT NULL,
      ) PRIMARY KEY (k1)
    )sql",
                                        R"sql(
      CREATE UNIQUE NULL_FILTERED INDEX Idx ON T(c1) STORING(c2,c3)
    )sql"}));

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

TEST_P(SchemaUpdaterTest, CreateIndex_Interleave) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"sql(
      CREATE TABLE T1 (
        k1 INT64,
        k2 INT64
      ) PRIMARY KEY (k1)
    )sql",
                                        R"sql(
      CREATE TABLE T2 (
        k1 INT64,
        k2 INT64,
        c1 BYTES(MAX)
      ) PRIMARY KEY (k1,k2), INTERLEAVE IN PARENT T1
    )sql",
                                        R"sql(
      CREATE INDEX Idx ON T2(k1,c1), INTERLEAVE IN T1
    )sql"}));

  auto t1 = schema->FindTable("T1");
  EXPECT_NE(t1, nullptr);

  auto idx = schema->FindIndex("Idx");
  EXPECT_EQ(idx->parent(), t1);
  EXPECT_NE(idx, nullptr);
  auto idx_data = idx->index_data_table();
  EXPECT_EQ(idx_data->parent(), t1);
  EXPECT_THAT(idx_data, IsInterleavedIn(t1, Table::OnDeleteAction::kCascade));
}

TEST_P(SchemaUpdaterTest, CreateIndex_NullFilteredInterleave) {
  // Null filtered indexes are not supported in PG.
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"sql(
      CREATE TABLE T1 (
        k1 INT64,
        k2 INT64
      ) PRIMARY KEY (k1)
    )sql",
                                        R"sql(
      CREATE TABLE T2 (
        k1 INT64,
        k2 INT64,
        c1 BYTES(MAX)
      ) PRIMARY KEY (k1,k2), INTERLEAVE IN PARENT T1
    )sql",
                                        R"sql(
      CREATE NULL_FILTERED INDEX Idx ON T2(k1,c1), INTERLEAVE IN T1
    )sql"}));

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

TEST_P(SchemaUpdaterTest, CreateIndex_InvalidInterleaved) {
  EXPECT_THAT(
      CreateSchema({R"sql(
      CREATE TABLE T1 (
        k1 INT64,
        k2 INT64
      ) PRIMARY KEY (k1)
    )sql",
                    R"sql(
      CREATE TABLE T2 (
        k1 INT64,
        k2 INT64,
        c1 BYTES(MAX)
      ) PRIMARY KEY (k1,k2)
    )sql",
                    R"sql(
      CREATE INDEX Idx ON T2(k1,c1), INTERLEAVE IN T1
    )sql"}),
      StatusIs(error::IndexInterleaveTableUnacceptable("Idx", "T2", "T1")));
}

TEST_P(SchemaUpdaterTest, CreateIndex_TableNotFound) {
  EXPECT_THAT(CreateSchema({"CREATE INDEX Idx ON T2(k1)"}),
              StatusIs(error::TableNotFound("T2")));
}

TEST_P(SchemaUpdaterTest, CreateIndex_ColumnNotFound) {
  EXPECT_THAT(CreateSchema({
                  R"sql(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64
      ) PRIMARY KEY (k1)
    )sql",
                  R"sql(
      CREATE INDEX Idx ON T(c2)
    )sql"}),
              StatusIs(error::IndexRefsNonExistentColumn("Idx", "c2")));
}

TEST_P(SchemaUpdaterTest, CreateIndex_DuplicateColumn) {
  EXPECT_THAT(CreateSchema({R"sql(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64
      ) PRIMARY KEY (k1)
    )sql",
                            R"sql(
      CREATE INDEX Idx ON T(c1,c1)
    )sql"}),
              StatusIs(error::IndexRefsColumnTwice("Idx", "c1")));
}

TEST_P(SchemaUpdaterTest, CreateIndex_StoredRefsIndexKey) {
  EXPECT_THAT(CreateSchema({R"sql(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64
      ) PRIMARY KEY (k1)
    )sql",
                            R"sql(
      CREATE INDEX Idx ON T(c1) STORING(c1)
    )sql"}),
              StatusIs(error::IndexRefsKeyAsStoredColumn("Idx", "c1")));
}

TEST_P(SchemaUpdaterTest, CreateIndex_UnsupportedArrayTypeKeyColumn) {
  EXPECT_THAT(CreateSchema({R"sql(
      CREATE TABLE T (
        k1 INT64,
        c1 ARRAY<INT64>
      ) PRIMARY KEY (k1)
    )sql",
                            R"sql(
      CREATE INDEX Idx ON T(c1)
    )sql"}),
              StatusIs(error::CannotCreateIndexOnColumn("Idx", "c1", "ARRAY")));
}

TEST_P(SchemaUpdaterTest, CreateIndex_ArrayStoredColumn) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"sql(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64,
        c2 ARRAY<INT64>
      ) PRIMARY KEY (k1)
    )sql",
                                                  R"sql(
      CREATE INDEX Idx ON T(c1) STORING(c2)
    )sql"}));

  auto idx = schema->FindIndex("Idx");
  EXPECT_NE(idx, nullptr);
  EXPECT_EQ(idx->stored_columns().size(), 1);
  auto c2 = idx->stored_columns()[0];

  const zetasql::ArrayType* array_type;
  ZETASQL_ASSERT_OK(type_factory_.MakeArrayType(types::Int64Type(), &array_type));

  EXPECT_THAT(c2, ColumnIs("c2", array_type));
}

TEST_P(SchemaUpdaterTest, AlterIndex_AddColumn) {
  const zetasql::ArrayType* int_array_type;
  ZETASQL_ASSERT_OK(type_factory_.MakeArrayType(types::Int64Type(), &int_array_type));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"sql(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64,
        c2 ARRAY<INT64>,
        c3 STRING(123),
      ) PRIMARY KEY (k1)
    )sql",
                                                  R"sql(
      CREATE INDEX Idx ON T(c1) STORING(c2)
    )sql"}));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto new_schema,
      UpdateSchema(schema.get(),
                   {R"sql(ALTER INDEX Idx ADD STORED COLUMN c3)sql"}));
  auto idx = new_schema->FindIndex("Idx");
  ASSERT_NOT_NULL(idx);

  EXPECT_THAT(idx->stored_columns(),
              UnorderedElementsAre(ColumnIs("c2", int_array_type),
                                   ColumnIs("c3", types::StringType())));
}

TEST_P(SchemaUpdaterTest, AlterIndex_StoreNotNullColumn) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"sql(
      CREATE TABLE T (
        col1 INT64 NOT NULL,
        col2 STRING(MAX) NOT NULL,
        col3 INT64 NOT NULL,
      ) PRIMARY KEY (col1)
    )sql",
                                        R"sql(
      CREATE INDEX Idx ON T(col2)
    )sql"}));

  ZETASQL_EXPECT_OK(UpdateSchema(schema.get(), {R"sql(
      ALTER INDEX Idx ADD STORED COLUMN col3
    )sql"}));
}

TEST_P(SchemaUpdaterTest, AlterIndex_StoreNotNullColumnWithNullFilteredIndex) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"sql(
      CREATE TABLE T (
        col1 INT64 NOT NULL,
        col2 STRING(MAX) NOT NULL,
        col3 INT64 NOT NULL,
      ) PRIMARY KEY (col1)
    )sql",
                                        R"sql(
      CREATE NULL_FILTERED INDEX Idx ON T(col2)
    )sql"}));

  ZETASQL_EXPECT_OK(UpdateSchema(schema.get(), {R"sql(
      ALTER INDEX Idx ADD STORED COLUMN col3
    )sql"}));
}

TEST_P(SchemaUpdaterTest, AlterIndex_AddColumnNotFound) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"sql(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64
      ) PRIMARY KEY (k1)
    )sql",
                                                  R"sql(
      CREATE INDEX Idx ON T(c1)
    )sql"}));

  EXPECT_THAT(
      UpdateSchema(schema.get(),
                   {R"sql(ALTER INDEX Idx ADD STORED COLUMN not_existed)sql"}),
      StatusIs(error::ColumnNotFound("T", "not_existed")));
}

TEST_P(SchemaUpdaterTest, AlterIndex_DropColumn) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"sql(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64,
        c2 ARRAY<INT64>
      ) PRIMARY KEY (k1)
    )sql",
                                                  R"sql(
      CREATE INDEX Idx ON T(c1) STORING(c2)
    )sql"}));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto new_schema,
      UpdateSchema(schema.get(),
                   {R"sql(ALTER INDEX Idx DROP STORED COLUMN c2)sql"}));

  auto idx = new_schema->FindIndex("Idx");
  ASSERT_NOT_NULL(idx);
  EXPECT_THAT(idx->stored_columns(), IsEmpty());
}

TEST_P(SchemaUpdaterTest, AlterIndex_DropColumnNotFound) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"sql(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64
      ) PRIMARY KEY (k1)
    )sql",
                                                  R"sql(
      CREATE INDEX Idx ON T(c1)
    )sql"}));

  EXPECT_THAT(
      UpdateSchema(schema.get(),
                   {R"sql(ALTER INDEX Idx DROP STORED COLUMN not_existed)sql"}),
      StatusIs(error::ColumnNotFoundInIndex("Idx", "not_existed")));
}

TEST_P(SchemaUpdaterTest, DropTable_WithIndex) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"sql(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64
      ) PRIMARY KEY (k1 ASC)
    )sql",
                                                  R"sql(
      CREATE INDEX Idx1 ON T(c1 DESC, k1 DESC)
    )sql"}));

  // Global index.
  EXPECT_THAT(UpdateSchema(schema.get(), {R"sql(
      DROP TABLE T
    )sql"}),
              StatusIs(error::DropTableWithDependentIndices("T", "Idx1")));

  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"sql(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64
      ) PRIMARY KEY (k1 ASC)
    )sql",
                                             R"sql(
      CREATE INDEX Idx2 ON T(k1), INTERLEAVE IN T
    )sql"}));

  // Interleaved index.
  EXPECT_THAT(UpdateSchema(schema.get(), {R"sql(
      DROP TABLE T
    )sql"}),
              StatusIs(error::DropTableWithDependentIndices("T", "Idx2")));
}

TEST_P(SchemaUpdaterTest, DropIndex) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"sql(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64
      ) PRIMARY KEY (k1 ASC)
    )sql",
                                                  R"sql(
      CREATE INDEX Idx ON T(c1 DESC, k1 DESC)
    )sql"}));

  EXPECT_NE(schema->FindIndex("Idx"), nullptr);

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema, UpdateSchema(schema.get(), {R"sql(
      DROP INDEX Idx
    )sql"}));

  EXPECT_EQ(new_schema->FindIndex("Idx"), nullptr);

  // Check that the index data table (and other dependent nodes) are
  // also deleted.
  EXPECT_EQ(new_schema->GetSchemaGraph()->GetSchemaNodes().size(), 4);
}

TEST_P(SchemaUpdaterTest, DropIndexIfExists) {
  // IF NOT EXISTS isn't yet supported on the PG side of the emulator
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"sql(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64
      ) PRIMARY KEY (k1 ASC)
    )sql",
                                                  R"sql(
      CREATE INDEX Idx ON T(c1 DESC, k1 DESC)
    )sql"}));

  EXPECT_EQ(schema->GetSchemaGraph()->GetSchemaNodes().size(), 10);

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema, UpdateSchema(schema.get(), {R"sql(
      DROP INDEX Idx
    )sql"}));

  EXPECT_EQ(new_schema->FindIndex("Idx"), nullptr);

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema2, UpdateSchema(new_schema.get(), {R"sql(
      DROP INDEX IF EXISTS Idx
    )sql"}));

  EXPECT_EQ(new_schema2->FindIndex("Idx"), nullptr);
}

TEST_P(SchemaUpdaterTest, DropIndexIfExistsTwice) {
  // IF NOT EXISTS isn't yet supported on the PG side of the emulator
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"sql(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64
      ) PRIMARY KEY (k1 ASC)
    )sql",
                                                  R"sql(
      CREATE INDEX Idx ON T(c1 DESC, k1 DESC)
    )sql"}));

  EXPECT_EQ(schema->GetSchemaGraph()->GetSchemaNodes().size(), 10);

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema, UpdateSchema(schema.get(), {R"sql(
      DROP INDEX IF EXISTS Idx
    )sql"}));

  EXPECT_EQ(new_schema->FindIndex("Idx"), nullptr);

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema2, UpdateSchema(new_schema.get(), {R"sql(
      DROP INDEX IF EXISTS Idx
    )sql"}));

  EXPECT_EQ(new_schema2->FindIndex("Idx"), nullptr);
}

TEST_P(SchemaUpdaterTest, DropIndexIfExistsButIndexDoesNotExist) {
  // IF NOT EXISTS isn't yet supported on the PG side of the emulator
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({R"sql(
      CREATE TABLE T (
        k1 INT64,
        c1 INT64
      ) PRIMARY KEY (k1 ASC)
    )sql"}));

  EXPECT_EQ(schema->FindIndex("Idx"), nullptr);

  // Make sure dropping an index that doesn't exist is fine.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto new_schema, UpdateSchema(schema.get(), {R"sql(
      DROP INDEX IF EXISTS Idx
    )sql"}));

  EXPECT_EQ(new_schema->FindIndex("Idx"), nullptr);
}

TEST_P(SchemaUpdaterTest, CreateIndexOnTableWithNoPK) {
  // Table with no key columns is not supported in PG.
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"sql(
      CREATE TABLE T ( col1 INT64 ) PRIMARY KEY ()
    )sql",
                                        R"sql(
      CREATE INDEX Idx ON T(col1)
    )sql"}));
  auto t = schema->FindTable("T");
  ASSERT_NE(t, nullptr);
  auto col1 = t->FindColumn("col1");
  ASSERT_NE(col1, nullptr);
  auto idx = schema->FindIndex("Idx");
  ASSERT_NE(idx, nullptr);

  EXPECT_EQ(idx->key_columns().size(), 1);

  auto idx_data = idx->index_data_table();
  auto data_columns = idx_data->columns();
  EXPECT_EQ(data_columns.size(), 1);
  EXPECT_THAT(data_columns[0], ColumnIs("col1", types::Int64Type()));

  EXPECT_THAT(idx_data->primary_key()[0]->column(), SourceColumnIs(col1));
}

TEST_P(SchemaUpdaterTest, CreateIndex_NumericColumn) {
  if (GetParam() == POSTGRESQL) {
    // PG.NUMERIC does not support for indexing yet.
    EXPECT_THAT(CreateSchema(
                    {
                        R"sql(
        CREATE TABLE T (
          col1 bigint primary key,
          col2 numeric
        )
      )sql",
                        R"sql(
        CREATE INDEX Idx ON T(col2)
      )sql"},
                    /*proto_descriptor_bytes=*/"",
                    /*dialect=*/POSTGRESQL,
                    /*use_gsql_to_pg_translation=*/false),
                StatusIs(error::CannotCreateIndexOnColumn("idx", "col2",
                                                          "PG.NUMERIC")));
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                          R"sql(
        CREATE TABLE T (
          col1 INT64 NOT NULL,
          col2 NUMERIC
        ) PRIMARY KEY (col1)
      )sql",
                                          R"sql(
        CREATE INDEX Idx ON T(col2)
      )sql"}));

    auto t = schema->FindTable("T");
    auto col2 = t->FindColumn("col2");
    EXPECT_TRUE(col2->GetType()->IsNumericType());

    auto idx = schema->FindIndex("Idx");
    EXPECT_NE(idx, nullptr);
    EXPECT_EQ(idx->key_columns().size(), 1);

    auto idx_data = idx->index_data_table();
    EXPECT_THAT(idx_data->primary_key()[0]->column(), SourceColumnIs(col2));
  }
}

TEST_P(SchemaUpdaterTest, CreateIndex_JsonColumn) {
  if (GetParam() == POSTGRESQL) {
    EXPECT_THAT(
        CreateSchema(
            {
                R"sql(
        CREATE TABLE T (
          col1 bigint primary key,
          col2 jsonb
        )
      )sql",
                R"sql(
        CREATE INDEX idx ON T(col2)
      )sql"},
            /*proto_descriptor_bytes=*/"",
            /*dialect=*/POSTGRESQL,
            /*use_gsql_to_pg_translation=*/false),
        StatusIs(error::CannotCreateIndexOnColumn("idx", "col2", "PG.JSONB")));
  } else {
    EXPECT_THAT(
        CreateSchema({
            R"sql(
        CREATE TABLE T (
          col1 INT64 NOT NULL,
          col2 JSON
        ) PRIMARY KEY (col1)
      )sql",
            R"sql(
        CREATE INDEX Idx ON T(col2)
      )sql"}),
        StatusIs(error::CannotCreateIndexOnColumn("Idx", "col2", "JSON")));
  }
}

std::vector<std::string> SchemaForCaseSensitivityTests() {
  return {
      R"sql(
                CREATE TABLE T (
                  k1 INT64 NOT NULL,
                  k2 INT64 NOT NULL,
                  c1 STRING(10),
                ) PRIMARY KEY (k1)
            )sql",
      R"sql(
                CREATE INDEX Idx1 ON T(c1))sql",
  };
}

TEST_P(SchemaUpdaterTest, TableNameIsCaseSensitive) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema(SchemaForCaseSensitivityTests()));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"sql(
      CREATE INDEX Idx1 ON t(c1)
    )sql"}),
              StatusIs(error::TableNotFound("t")));
}

TEST_P(SchemaUpdaterTest, ColumnNameIsCaseSensitive) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema(SchemaForCaseSensitivityTests()));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"sql(
      CREATE INDEX Idx2 ON T(K2))sql"}),
              StatusIs(error::IndexRefsNonExistentColumn("Idx2", "K2")));
}

TEST_P(SchemaUpdaterTest, StoringColumnNameIsCaseSensitive) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema(SchemaForCaseSensitivityTests()));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"sql(
      CREATE INDEX Idx2 ON T(k2) STORING(C1))sql"}),
              StatusIs(error::IndexRefsNonExistentColumn("Idx2", "C1")));
}

TEST_P(SchemaUpdaterTest, DropIndexIsCaseSensitive) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema(SchemaForCaseSensitivityTests()));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"sql(
      DROP INDEX idx1)sql"}),
              StatusIs(error::IndexNotFound("idx1")));
}
// For the following tests, a custom PG DDL statement is required as the schema
// has a generated column. Translating expressions from GSQL to PG is not
// supported in tests.

TEST_P(SchemaUpdaterTest, CannotCreateIndexOnTokenListColumn) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  EXPECT_THAT(
      CreateSchema({
          R"sql(
      CREATE TABLE T (
        col1 INT64 NOT NULL,
        col2 STRING(MAX),
        col3 TOKENLIST AS(TOKENIZE_FULLTEXT(col2)) STORED HIDDEN
      ) PRIMARY KEY (col1)
    )sql",
          R"sql(
      CREATE INDEX Idx ON T(col3)
    )sql"}),
      StatusIs(error::CannotCreateIndexOnColumn("Idx", "col3", "TOKENLIST")));
}

TEST_P(SchemaUpdaterTest, BasicCreateSearchIndex) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"sql(
      CREATE TABLE T (
        col1 INT64 NOT NULL,
        col2 STRING(MAX),
        col3 TOKENLIST AS(TOKENIZE_FULLTEXT(col2)) STORED HIDDEN
      ) PRIMARY KEY (col1)
    )sql",
                                        R"sql(
      CREATE SEARCH INDEX Idx ON T(col3)
    )sql"}));

  auto t = schema->FindTable("T");
  auto col3 = t->FindColumn("col3");
  EXPECT_TRUE(col3->GetType()->IsTokenListType());

  auto idx = schema->FindIndex("Idx");
  EXPECT_NE(idx, nullptr);
  EXPECT_EQ(idx->key_columns().size(), 1);

  auto idx_data = idx->index_data_table();
  EXPECT_THAT(idx_data->primary_key()[0]->column(), SourceColumnIs(col3));
}

TEST_P(SchemaUpdaterTest, ComplexCreateSearchIndex) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"sql(
      CREATE TABLE T (
        col1 INT64 NOT NULL,
        col2 STRING(MAX),
        col3 TOKENLIST AS(TOKENIZE_FULLTEXT(col2)) STORED HIDDEN,
        col4 TOKENLIST AS(TOKENIZE_SUBSTRING(col2)) STORED HIDDEN,
        col5 INT64,
        col6 FLOAT64,
        col7 INT64 NOT NULL
      ) PRIMARY KEY (col1)
    )sql",
                                        R"sql(
      CREATE SEARCH INDEX Idx
      ON T(col3, col4)
      STORING (col2, col5)
      PARTITION BY col1, col6
      ORDER BY col7
    )sql"}));

  auto t = schema->FindTable("T");
  auto col3 = t->FindColumn("col3");
  EXPECT_TRUE(col3->GetType()->IsTokenListType());
  auto col4 = t->FindColumn("col4");
  EXPECT_TRUE(col4->GetType()->IsTokenListType());

  auto idx = schema->FindIndex("Idx");
  EXPECT_NE(idx, nullptr);
  EXPECT_EQ(idx->key_columns().size(), 2);

  auto idx_data = idx->index_data_table();
  EXPECT_EQ(idx_data->primary_key().size(), 3);
  EXPECT_THAT(idx_data->primary_key()[0]->column(), SourceColumnIs(col3));
  EXPECT_THAT(idx_data->primary_key()[1]->column(), SourceColumnIs(col4));
  auto col1 = t->FindColumn("col1");
  EXPECT_THAT(idx_data->primary_key()[2]->column(), SourceColumnIs(col1));

  EXPECT_EQ(idx->stored_columns().size(), 2);
  auto col2 = t->FindColumn("col2");
  EXPECT_THAT(idx->stored_columns()[0], SourceColumnIs(col2));
  auto col5 = t->FindColumn("col5");
  EXPECT_THAT(idx->stored_columns()[1], SourceColumnIs(col5));

  EXPECT_EQ(idx->partition_by().size(), 2);
  EXPECT_THAT(idx->partition_by()[0], SourceColumnIs(col1));
  auto col6 = t->FindColumn("col6");
  EXPECT_THAT(idx->partition_by()[1], SourceColumnIs(col6));
  EXPECT_NE(idx_data->FindColumn("col6"), nullptr);

  EXPECT_EQ(idx->order_by().size(), 1);
  auto col7 = t->FindColumn("col7");
  EXPECT_THAT(idx->order_by()[0], SourceColumnIs(col7));
  EXPECT_NE(idx_data->FindColumn("col7"), nullptr);
}

TEST_P(SchemaUpdaterTest, UnableToCreateSearchIndexOnJsonColumn) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  EXPECT_THAT(CreateSchema({
                  R"sql(
      CREATE TABLE T (
        col1 INT64 NOT NULL,
        col2 STRING(MAX),
        col3 TOKENLIST AS(TOKENIZE_FULLTEXT(col2)) STORED HIDDEN,
        col4 JSON,
      ) PRIMARY KEY (col1)
    )sql",
                  R"sql(
      CREATE SEARCH INDEX SearchIndex ON T(col3, col4)
    )sql"}),
              StatusIs(error::CannotCreateIndexOnColumn("SearchIndex", "col4",
                                                        "JSON")));
}

TEST_P(SchemaUpdaterTest, CreateSearchIndexShouldNotStoreKeyColumn) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  EXPECT_THAT(
      CreateSchema({
          R"sql(
      CREATE TABLE T (
        col1 INT64 NOT NULL,
        col2 STRING(MAX),
        col3 TOKENLIST AS(TOKENIZE_FULLTEXT(col2)) STORED HIDDEN
      ) PRIMARY KEY (col1)
    )sql",
          R"sql(
      CREATE SEARCH INDEX SearchIndex ON T(col3) STORING(col3)
    )sql"}),
      StatusIs(error::IndexRefsKeyAsStoredColumn("SearchIndex", "col3")));
}

TEST_P(SchemaUpdaterTest, CreateSearchIndexPartitionByColumnMustExist) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  EXPECT_THAT(
      CreateSchema({
          R"sql(
      CREATE TABLE T (
        col1 INT64 NOT NULL,
        col2 STRING(MAX),
        col3 TOKENLIST AS(TOKENIZE_FULLTEXT(col2)) STORED HIDDEN
      ) PRIMARY KEY (col1)
    )sql",
          R"sql(
      CREATE SEARCH INDEX SearchIndex ON T(col3) PARTITION BY col4
    )sql"}),
      StatusIs(error::IndexRefsNonExistentColumn("SearchIndex", "col4")));
}

TEST_P(SchemaUpdaterTest, CreateSearchIndexOrderByColumnMustExist) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  EXPECT_THAT(
      CreateSchema({
          R"sql(
      CREATE TABLE T (
        col1 INT64 NOT NULL,
        col2 STRING(MAX),
        col3 TOKENLIST AS(TOKENIZE_FULLTEXT(col2)) STORED HIDDEN
      ) PRIMARY KEY (col1)
    )sql",
          R"sql(
      CREATE SEARCH INDEX SearchIndex ON T(col3) ORDER BY col4
    )sql"}),
      StatusIs(error::IndexRefsNonExistentColumn("SearchIndex", "col4")));
}

TEST_P(SchemaUpdaterTest, CreateSearchIndexPartitionByNotTokenListType) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  EXPECT_THAT(CreateSchema({
                  R"sql(
      CREATE TABLE T (
        col1 INT64 NOT NULL,
        col2 STRING(MAX),
        col3 TOKENLIST AS(TOKENIZE_FULLTEXT(col2)) STORED HIDDEN,
        col4 INT64
      ) PRIMARY KEY (col1)
    )sql",
                  R"sql(
      CREATE SEARCH INDEX SearchIndex ON T(col3) PARTITION BY col3
    )sql"}),
              StatusIs(error::SearchIndexNotPartitionByokenListType(
                  "SearchIndex", "col3")));
}

TEST_P(SchemaUpdaterTest, CreateSearchIndexOrderByMustNotNull) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  EXPECT_THAT(CreateSchema({
                  R"sql(
      CREATE TABLE T (
        col1 INT64 NOT NULL,
        col2 STRING(MAX),
        col3 TOKENLIST AS(TOKENIZE_FULLTEXT(col2)) STORED HIDDEN,
        col4 INT64
      ) PRIMARY KEY (col1)
    )sql",
                  R"sql(
      CREATE SEARCH INDEX SearchIndex ON T(col3) ORDER BY col4
    )sql"}),
              StatusIs(error::SearchIndexSortMustBeNotNullError(
                  "col4", "SearchIndex")));
}

TEST_P(SchemaUpdaterTest, CreateSearchIndexNullFilteredOrderBy) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_EXPECT_OK(CreateSchema({
      R"sql(
      CREATE TABLE T (
        col1 INT64 NOT NULL,
        col2 STRING(MAX),
        col3 TOKENLIST AS(TOKENIZE_FULLTEXT(col2)) STORED HIDDEN,
        col4 INT64
      ) PRIMARY KEY (col1)
    )sql",
      R"sql(
      CREATE SEARCH INDEX SearchIndex ON T(col3)
      ORDER BY col4
      WHERE col4 IS NOT NULL
    )sql"}));
}

TEST_P(SchemaUpdaterTest, CreateSearchIndexOrderByMustBeIntegerType) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  EXPECT_THAT(CreateSchema({
                  R"sql(
      CREATE TABLE T (
        col1 INT64 NOT NULL,
        col2 STRING(MAX),
        col3 TOKENLIST AS(TOKENIZE_FULLTEXT(col2)) STORED HIDDEN,
        col4 JSON NOT NULL
      ) PRIMARY KEY (col1)
    )sql",
                  R"sql(
      CREATE SEARCH INDEX SearchIndex ON T(col3) ORDER BY col4
    )sql"}),
              StatusIs(error::SearchIndexOrderByMustBeIntegerType(
                  "SearchIndex", "col4", "JSON")));
}

TEST_P(SchemaUpdaterTest, CreateSearchIndexTokenColumnOrderNotAllowed) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"sql(
      CREATE TABLE T (
        col1 INT64 NOT NULL,
        col2 STRING(MAX),
        col3 TOKENLIST AS(TOKENIZE_FULLTEXT(col2)) STORED HIDDEN,
        col4 INT64 NOT NULL,
      ) PRIMARY KEY (col1)
    )sql"}));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"sql(
      CREATE SEARCH INDEX SearchIndex ON T(col3 ASC) ORDER BY col4
    )sql"}),
              StatusIs(error::SearchIndexTokenlistKeyOrderUnsupported(
                  "col3", "SearchIndex")));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"sql(
      CREATE SEARCH INDEX SearchIndex ON T(col3 DESC) ORDER BY col4
    )sql"}),
              StatusIs(error::SearchIndexTokenlistKeyOrderUnsupported(
                  "col3", "SearchIndex")));
}

}  // namespace

}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
