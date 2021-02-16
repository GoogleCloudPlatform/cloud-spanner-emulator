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

#include "backend/schema/catalog/schema.h"

#include <memory>

#include "zetasql/public/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/time/time.h"
#include "backend/schema/builders/column_builder.h"
#include "backend/schema/builders/index_builder.h"
#include "backend/schema/builders/table_builder.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/printer/print_ddl.h"
#include "common/errors.h"
#include "common/limits.h"
#include "tests/common/schema_constructor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

class SchemaTest : public testing::Test {
 public:
  SchemaTest()
      : context_(/*storage =*/nullptr, /*global_names =*/nullptr,
                 /*type_factory =*/nullptr,
                 /*pending_commit_timestamp =*/absl::Now()),
        type_factory_(absl::make_unique<zetasql::TypeFactory>()),
        base_schema_(test::CreateSchemaWithOneTable(type_factory_.get())) {}

 protected:
  Table::Builder table_builder(const std::string& name) {
    Table::Builder b;
    b.set_name(name).set_id(name);
    return b;
  }

  Column::Builder column_builder(const std::string& name, const Table* table) {
    Column::Builder c;
    c.set_name(name)
        .set_id(name)
        .set_type(type_factory_->get_string())
        .set_table(table);
    return c;
  }

  // Dummy validation context.
  SchemaValidationContext context_;

  // The type factory must outlive the type objects that it has made.
  std::unique_ptr<zetasql::TypeFactory> type_factory_;

  // Base schema for use in tests.
  std::unique_ptr<const Schema> base_schema_;
};

// TODO: Move this test to the unit test for
// SchemaConstructor/SchemaUpdater.
TEST_F(SchemaTest, Basic) {
  EXPECT_EQ(base_schema_->tables().size(), 1);

  // Verify the table properties.
  const Table* table = base_schema_->FindTable("test_table");
  ASSERT_NE(table, nullptr);
  EXPECT_EQ(table->columns().size(), 2);

  // Verify the two columns created by the TestSchemaConstructor
  const Column* col1 = table->FindColumn("int64_col");
  ASSERT_NE(col1, nullptr);
  EXPECT_EQ(col1->Name(), "int64_col");
  EXPECT_EQ(col1->GetType()->kind(), zetasql::TYPE_INT64);
  EXPECT_FALSE(col1->is_nullable());
  EXPECT_NE(table->FindKeyColumn("int64_col"), nullptr);

  const Column* col2 = table->FindColumn("string_col");
  ASSERT_NE(col2, nullptr);
  EXPECT_EQ(col2->Name(), "string_col");
  EXPECT_EQ(col2->GetType()->kind(), zetasql::TYPE_STRING);
  EXPECT_TRUE(col2->is_nullable());
  EXPECT_FALSE(col2->declared_max_length().has_value());
  EXPECT_EQ(table->FindKeyColumn("string_col"), nullptr);

  // Verify primary key.
  EXPECT_EQ(table->primary_key().size(), 1);
  EXPECT_EQ(table->primary_key()[0]->column(), col1);
  EXPECT_FALSE(table->primary_key()[0]->is_descending());

  const Index* index = base_schema_->FindIndex("test_index");
  ASSERT_NE(index, nullptr);
  EXPECT_EQ(table->FindIndex("test_index"), index);
  EXPECT_EQ(table->indexes().size(), 1);
  EXPECT_EQ(index->index_data_table()->owner_index(), index);
  EXPECT_EQ(index->indexed_table(), table);
  EXPECT_EQ(index->key_columns().size(), 1);
  EXPECT_TRUE(index->is_unique());

  const Table* index_data_table = index->index_data_table();
  ASSERT_NE(index_data_table, nullptr);
  EXPECT_EQ(index_data_table->columns().size(), 2);

  const Column* index_col1 = index_data_table->FindColumn("string_col");
  ASSERT_NE(index_col1, nullptr);
  EXPECT_EQ(index_col1->Name(), "string_col");
  EXPECT_EQ(index_col1->GetType()->kind(), zetasql::TYPE_STRING);
  EXPECT_TRUE(index_col1->is_nullable());
  EXPECT_FALSE(index_col1->declared_max_length().has_value());

  const Column* index_col2 = index_data_table->FindColumn("int64_col");
  ASSERT_NE(index_col2, nullptr);
  EXPECT_EQ(index_col2->Name(), "int64_col");
  EXPECT_EQ(index_col2->GetType()->kind(), zetasql::TYPE_INT64);
  EXPECT_FALSE(index_col2->is_nullable());

  EXPECT_EQ(index_data_table->primary_key().size(), 2);
  EXPECT_EQ(index_data_table->primary_key()[0]->column(), index_col1);
  EXPECT_TRUE(index_data_table->primary_key()[0]->is_descending());
  EXPECT_EQ(index_data_table->primary_key()[1]->column(), index_col2);
  EXPECT_FALSE(index_data_table->primary_key()[1]->is_descending());
}

// TODO: Move this test to the unit test for
// SchemaConstructor/SchemaUpdater.
TEST_F(SchemaTest, InterleaveTest) {
  base_schema_ = test::CreateSchemaWithInterleaving(type_factory_.get());
  EXPECT_EQ(base_schema_->tables().size(), 3);

  // Verify parent table.
  const Table* parent_table = base_schema_->FindTable("Parent");
  ASSERT_NE(parent_table, nullptr);
  EXPECT_EQ(parent_table->children().size(), 2);

  // Verify the interleaving table properties.
  auto table = base_schema_->FindTable("CascadeDeleteChild");
  ASSERT_NE(table, nullptr);
  EXPECT_EQ(table->on_delete_action(), Table::OnDeleteAction::kCascade);
  EXPECT_EQ(table->parent(), parent_table);

  table = base_schema_->FindTable("NoActionDeleteChild");
  ASSERT_NE(table, nullptr);
  EXPECT_EQ(table->on_delete_action(), Table::OnDeleteAction::kNoAction);
  EXPECT_EQ(table->parent(), parent_table);
}

// Column tests.
TEST_F(SchemaTest, ColumnBuilder) {
  const auto* table = base_schema_->FindTable("test_table");
  Column::Builder b1;
  b1.set_name("C1")
      .set_id("C1")
      .set_table(table)
      .set_type(type_factory_->get_string())
      .set_declared_max_length(23);
  auto c1 = b1.build();

  EXPECT_EQ(c1->Name(), "C1");
  EXPECT_EQ(c1->FullName(), "test_table.C1");
  EXPECT_EQ(c1->table(), table);
  EXPECT_TRUE(c1->GetType()->IsString());
  EXPECT_EQ(c1->id(), "C1");
  EXPECT_EQ(c1->source_column(), nullptr);
  EXPECT_TRUE(c1->is_nullable());
  EXPECT_EQ(c1->declared_max_length(), 23);
  EXPECT_EQ(c1->effective_max_length(), 23);
  ZETASQL_EXPECT_OK(c1->Validate(&context_));

  Column::Builder b2;
  b2.set_name("C2")
      .set_id("C2")
      .set_table(table)
      .set_type(type_factory_->get_string())
      .set_nullable(false)
      .set_source_column(c1.get());
  auto c2 = b2.build();
  EXPECT_FALSE(c2->is_nullable());
  EXPECT_EQ(c2->source_column(), c1.get());
  EXPECT_TRUE(c2->GetType()->Equals(c1->GetType()));
  EXPECT_EQ(c2->declared_max_length(), 23);
  EXPECT_EQ(c2->effective_max_length(), 23);
  ZETASQL_EXPECT_OK(c2->Validate(&context_));

  Column::Builder b3;
  b3.set_name("C3")
      .set_id("C3")
      .set_table(table)
      .set_type(type_factory_->get_int64())
      .set_nullable(false)
      .set_allow_commit_timestamp(true);
  auto c3 = b3.build();
  EXPECT_EQ(c3->Validate(&context_),
            error::UnallowedCommitTimestampOption("test_table.C3"));

  // User-specified length exceeds max limit.
  Column::Builder b4;
  const int64_t invalid_length = 1000000000000;
  b4.set_name("C4")
      .set_id("C4")
      .set_table(table)
      .set_type(type_factory_->get_string())
      .set_declared_max_length(invalid_length);
  auto c4 = b4.build();
  EXPECT_EQ(c4->Validate(&context_),
            error::InvalidColumnLength("test_table.C4", invalid_length, 1,
                                       limits::kMaxStringColumnLength));

  // Column name length exceeds limit.
  Column::Builder b5;
  const std::string column_name(130, 'c');
  b5.set_name(column_name)
      .set_id("C1")
      .set_table(table)
      .set_type(type_factory_->get_int64());
  auto c5 = b5.build();
  EXPECT_EQ(c5->Validate(&context_),
            error::InvalidSchemaName("Column", column_name));

  Column::Builder b6;
  b6.set_name("c6").set_id("C1").set_table(table).set_type(
      type_factory_->get_bytes());
  auto c6 = b6.build();
  ZETASQL_EXPECT_OK(c6->Validate(&context_));
  EXPECT_FALSE(c6->declared_max_length().has_value());
  EXPECT_EQ(c6->effective_max_length(), limits::kMaxBytesColumnLength);
}

TEST_F(SchemaTest, KeyColumnBuilder) {
  const auto* table = base_schema_->FindTable("test_table");

  Column::Builder c1;
  c1.set_name("C1").set_id("C1").set_table(table).set_type(
      type_factory_->get_string());

  KeyColumn::Builder kb1;
  kb1.set_column(c1.get()).set_descending(true);
  auto k1 = kb1.build();
  EXPECT_EQ(k1->column(), c1.get());
  EXPECT_EQ(k1->is_descending(), true);
  ZETASQL_EXPECT_OK(k1->Validate(&context_));

  const zetasql::ArrayType* array_type;
  ZETASQL_ASSERT_OK(
      type_factory_->MakeArrayType(type_factory_->get_int64(), &array_type));
  Column::Builder c2;
  c2.set_name("C2").set_id("C2").set_table(table).set_type(array_type);

  KeyColumn::Builder kb2;
  kb2.set_column(c2.get());
  auto k2 = kb2.build();
  EXPECT_THAT(k2->Validate(&context_),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  testing::HasSubstr("is part of the primary key")));
}

TEST_F(SchemaTest, TableBuilder) {
  auto t = table_builder("T").build();
  ZETASQL_EXPECT_OK(t->Validate(&context_));

  Table::Builder tb = table_builder("T1");
  auto c1 = column_builder("C1", tb.get()).build();
  auto k1 = KeyColumn::Builder().set_column(c1.get()).build();
  auto t1 = tb.add_column(c1.get()).add_key_column(k1.get()).build();
  ZETASQL_EXPECT_OK(t1->Validate(&context_));

  tb = table_builder("T2");
  auto c2 = column_builder("C1", tb.get()).build();
  auto k2 = KeyColumn::Builder().set_column(c2.get()).build();
  auto t2 = tb.add_column(c2.get())
                .add_key_column(k2.get())
                .set_parent_table(t1.get())
                .set_on_delete(Table::OnDeleteAction::kNoAction)
                .build();
  ZETASQL_EXPECT_OK(t2->Validate(&context_));

  tb = table_builder("T3");
  auto c3 = column_builder("C3", tb.get()).build();
  auto k3 = KeyColumn::Builder().set_column(c3.get()).build();
  auto t3 = tb.add_column(c3.get()).add_column(c3.get()).build();
  EXPECT_EQ(t3->Validate(&context_), error::DuplicateColumnName("T3.C3"));

  tb = table_builder("T4");
  auto c4 = column_builder("C4", tb.get()).build();
  auto k4 = KeyColumn::Builder().set_column(c4.get()).build();
  auto t4 = tb.add_column(c4.get())
                .add_key_column(k4.get())
                .add_key_column(k4.get())
                .build();
  EXPECT_EQ(t4->Validate(&context_),
            error::MultipleRefsToKeyColumn("Table", "T4", "C4"));

  tb = table_builder("T5");
  auto c5 = column_builder("C1", tb.get())
                .set_type(type_factory_->get_int64())
                .build();
  auto k5 = KeyColumn::Builder().set_column(c5.get()).build();
  auto t5 = tb.add_column(c5.get())
                .add_key_column(k5.get())
                .set_parent_table(t2.get())
                .build();
  EXPECT_EQ(
      t5->Validate(&context_),
      error::IncorrectParentKeyType("Table", "T5", "C1", "INT64", "STRING"));

  const std::string table_name(130, 'T');
  tb = table_builder(table_name);
  auto tinvalid = tb.build();
  EXPECT_EQ(tinvalid->Validate(&context_),
            error::InvalidSchemaName("Table", table_name));
}

TEST_F(SchemaTest, IndexBuilder) {
  // Indexed table.
  Table::Builder tb = table_builder("T1");
  auto c1 = column_builder("c1", tb.get()).build();
  auto c2 = column_builder("c2", tb.get()).build();
  const zetasql::ArrayType* array_type;
  ZETASQL_ASSERT_OK(
      type_factory_->MakeArrayType(type_factory_->get_int64(), &array_type));
  auto c3 = Column::Builder()
                .set_name("c3")
                .set_id("c3")
                .set_type(array_type)
                .set_table(tb.get())
                .build();
  tb.add_column(c1.get());
  tb.add_column(c2.get());
  tb.add_column(c3.get());

  Index::Builder ib;
  ib.set_name("I1");

  // Index data table.
  Table::Builder idtb = table_builder("_data_T1");
  auto c1_dt = column_builder("c1", idtb.get())
                   .set_source_column(c1.get())
                   .set_nullable(false)
                   .build();
  auto c2_dt = column_builder("c2", idtb.get())
                   .set_source_column(c2.get())
                   .set_nullable(false)
                   .build();
  auto k1_dt = KeyColumn::Builder().set_column(c1_dt.get()).build();
  auto k2_dt = KeyColumn::Builder().set_column(c2_dt.get()).build();
  idtb.add_column(c2_dt.get()).add_column(c1_dt.get());
  idtb.add_key_column(k2_dt.get()).add_key_column(k1_dt.get());
  idtb.set_owner_index(ib.get());

  auto idt = idtb.build();
  auto i1 = ib.set_name("I1")
                .set_indexed_table(tb.get())
                .set_index_data_table(idt.get())
                .add_key_column(k2_dt.get())
                .add_stored_column(c1_dt.get())
                .set_unique(true)
                .set_null_filtered(true)
                .build();

  // Basic null-filtered unique index.
  ZETASQL_EXPECT_OK(idt->Validate(&context_));
  ZETASQL_EXPECT_OK(i1->Validate(&context_));

  // Index with array column as key column.
  ib = Index::Builder();
  idtb = table_builder("_data_T2");
  c1_dt = column_builder("c1", idtb.get()).set_source_column(c1.get()).build();
  auto c3_dt =
      column_builder("c3", idtb.get()).set_source_column(c3.get()).build();
  auto k3_dt = KeyColumn::Builder().set_column(c3_dt.get()).build();
  idtb.add_column(c3_dt.get()).add_column(c1_dt.get());
  idtb.add_key_column(k3_dt.get());
  idtb.set_owner_index(ib.get());

  auto i2 = ib.set_name("I2")
                .set_indexed_table(tb.get())
                .set_index_data_table(idtb.get())
                .add_key_column(k3_dt.get())
                .add_stored_column(c1_dt.get())
                .build();
  EXPECT_EQ(i2->Validate(&context_),
            error::IndexRefsUnsupportedColumn("I2", "ARRAY<INT64>"));

  // Index with array column as stored.
  ib = Index::Builder();
  idtb = table_builder("_data_T3");
  c1_dt = column_builder("c1", idtb.get()).set_source_column(c1.get()).build();
  k1_dt = KeyColumn::Builder().set_column(c1_dt.get()).build();
  c3_dt = column_builder("c3", idtb.get()).set_source_column(c3.get()).build();
  idtb.add_column(c1_dt.get()).add_column(c3_dt.get());
  idtb.add_key_column(k1_dt.get());
  idtb.set_owner_index(ib.get());

  auto i3 = ib.set_name("I3")
                .set_indexed_table(tb.get())
                .set_index_data_table(idtb.get())
                .add_key_column(k1_dt.get())
                .add_stored_column(c3_dt.get())
                .build();
  ZETASQL_EXPECT_OK(i3->Validate(&context_));

  // Key and stored column same.
  ib = Index::Builder();
  auto i4 = ib.set_name("I4")
                .set_indexed_table(tb.get())
                .set_index_data_table(idtb.get())
                .add_key_column(k1_dt.get())
                .add_stored_column(c1_dt.get())
                .add_stored_column(c3_dt.get())
                .build();
  EXPECT_EQ(i4->Validate(&context_),
            error::IndexRefsKeyAsStoredColumn("I4", "c1"));

  // Duplicate columns.
  ib = Index::Builder();
  auto i5 = ib.set_name("I5")
                .set_indexed_table(tb.get())
                .set_index_data_table(idtb.get())
                .add_key_column(k1_dt.get())
                .add_key_column(k1_dt.get())
                .add_stored_column(c3_dt.get())
                .build();
  EXPECT_EQ(i5->Validate(&context_), error::IndexRefsColumnTwice("I5", "c1"));

  const std::string index_name(130, 'I');
  auto invalid_idx = Index::Builder()
                         .set_name(index_name)
                         .set_indexed_table(tb.get())
                         .set_index_data_table(idtb.get())
                         .build();
  EXPECT_EQ(invalid_idx->Validate(&context_),
            error::InvalidSchemaName("Index", index_name));
}

// TODO: GetDatabaseDDL needs more robust testing with
// RTG/golden files.
TEST_F(SchemaTest, PrintDDLStatementsTestOneTable) {
  std::unique_ptr<const Schema> schema =
      test::CreateSchemaWithOneTable(type_factory_.get());
  std::vector<std::string> statements = PrintDDLStatements(schema.get());

  EXPECT_EQ(statements[0],
            R"(CREATE TABLE test_table (
  int64_col INT64 NOT NULL,
  string_col STRING(MAX),
) PRIMARY KEY(int64_col))");
  EXPECT_EQ(statements[1],
            R"(CREATE UNIQUE INDEX test_index ON test_table(string_col DESC))");
}

TEST_F(SchemaTest, PrintDDLStatementsTestInterleaving) {
  std::unique_ptr<const Schema> schema =
      test::CreateSchemaWithInterleaving(type_factory_.get());
  std::vector<std::string> statements = PrintDDLStatements(schema.get());

  EXPECT_EQ(statements[0],
            R"(CREATE TABLE Parent (
  k1 INT64 NOT NULL,
  c1 STRING(MAX),
) PRIMARY KEY(k1))");
  EXPECT_EQ(statements[1],
            R"(CREATE TABLE CascadeDeleteChild (
  k1 INT64 NOT NULL,
  k2 INT64 NOT NULL,
  c1 STRING(MAX),
) PRIMARY KEY(k1, k2),
  INTERLEAVE IN PARENT Parent ON DELETE CASCADE)");
  EXPECT_EQ(statements[2],
            R"(CREATE TABLE NoActionDeleteChild (
  k1 INT64 NOT NULL,
  k2 INT64 NOT NULL,
  c1 STRING(MAX),
) PRIMARY KEY(k1, k2),
  INTERLEAVE IN PARENT Parent ON DELETE NO ACTION)");
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
