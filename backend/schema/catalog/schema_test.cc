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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "zetasql/public/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/time/clock.h"
#include "backend/schema/builders/change_stream_builder.h"
#include "backend/schema/builders/column_builder.h"
#include "backend/schema/builders/database_options_builder.h"
#include "backend/schema/builders/index_builder.h"
#include "backend/schema/builders/locality_group_builder.h"
#include "backend/schema/builders/named_schema_builder.h"
#include "backend/schema/builders/sequence_builder.h"
#include "backend/schema/builders/table_builder.h"
#include "backend/schema/builders/udf_builder.h"
#include "backend/schema/builders/view_builder.h"
#include "backend/schema/catalog/change_stream.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/database_options.h"
#include "backend/schema/catalog/foreign_key.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/catalog/locality_group.h"
#include "backend/schema/catalog/named_schema.h"
#include "backend/schema/catalog/sequence.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/catalog/udf.h"
#include "backend/schema/catalog/view.h"
#include "backend/schema/printer/print_ddl.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "common/errors.h"
#include "common/limits.h"
#include "tests/common/schema_constructor.h"
#include "tests/common/scoped_feature_flags_setter.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

using absl::StatusCode;
using test::ScopedEmulatorFeatureFlagsSetter;
using ::testing::ElementsAre;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

class SchemaTest : public testing::Test {
 public:
  SchemaTest()
      : type_factory_(std::make_unique<zetasql::TypeFactory>()),
        base_schema_(test::CreateSchemaWithOneTable(type_factory_.get())),
        flag_setter_({
            .enable_fk_delete_cascade_action = true,
            .enable_identity_columns = true,
            .enable_user_defined_functions = true,
            .enable_fk_enforcement_option = true,
        }) {}

  void SetUp() override {
    // Use GOOGLE_STANDARD_SQL dialect by default.
    context_ = SchemaValidationContext(
        /*storage =*/nullptr, /*global_names =*/nullptr,
        /*type_factory =*/nullptr,
        /*pending_commit_timestamp =*/absl::Now(),
        /*dialect=*/admin::database::v1::GOOGLE_STANDARD_SQL);
  }

 protected:
  Table::Builder table_builder(const std::string& name,
                               const std::string& synonym = "") {
    Table::Builder b;
    b.set_name(name).set_id(name);
    if (!synonym.empty()) {
      b.set_synonym(synonym);
    }
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

  ChangeStream::Builder change_stream_builder(const std::string& name) {
    ChangeStream::Builder c;
    c.set_name(name).set_id(name);
    return c;
  }

  Sequence::Builder sequence_builder(const std::string& name) {
    Sequence::Builder c;
    c.set_name(name).set_id(name);
    return c;
  }

  LocalityGroup::Builder locality_group_builder(const std::string& name) {
    LocalityGroup::Builder c;
    c.set_name(name);
    return c;
  }

  DatabaseOptions::Builder database_options_builder(const std::string& name) {
    DatabaseOptions::Builder c;
    c.set_db_name(name);
    return c;
  }

  void SetPostgresqlDialect() {
    context_ = SchemaValidationContext(
        /*storage =*/nullptr, /*global_names =*/nullptr,
        /*type_factory =*/nullptr,
        /*pending_commit_timestamp =*/absl::Now(),
        /*dialect=*/admin::database::v1::POSTGRESQL);
  }

  // Dummy validation context.
  SchemaValidationContext context_;

  // The type factory must outlive the type objects that it has made.
  std::unique_ptr<zetasql::TypeFactory> type_factory_;

  // Base schema for use in tests.
  std::unique_ptr<const Schema> base_schema_;

  const ScopedEmulatorFeatureFlagsSetter flag_setter_;
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

TEST_F(SchemaTest, ChangeStreamBasic) {
  EXPECT_EQ(base_schema_->change_streams().size(), 0);
  base_schema_ =
      test::CreateSchemaWithOneTableAndOneChangeStream(type_factory_.get());
  EXPECT_EQ(base_schema_->change_streams().size(), 1);
  const ChangeStream* change_stream =
      base_schema_->FindChangeStream("change_stream_test_table");
  const Table* table = base_schema_->FindTable("test_table");
  const Column* col = table->FindColumn("string_col");
  ASSERT_NE(col->FindChangeStream("change_stream_test_table"), nullptr);
  ASSERT_TRUE(col->FindChangeStream("change_stream_test") == nullptr);
  EXPECT_EQ(col->change_streams().size(), 1);
  ASSERT_TRUE(col->is_trackable_by_change_stream());
  ASSERT_NE(table->FindChangeStream("change_stream_test_table"), nullptr);
  ASSERT_TRUE(table->FindChangeStream("change_stream_test") == nullptr);
  ASSERT_TRUE(table->is_trackable_by_change_stream());
  EXPECT_EQ(table->trackable_columns().size(), 2);
  ASSERT_NE(change_stream, nullptr);
  ASSERT_TRUE(change_stream->track_all());
  EXPECT_EQ(change_stream->tracked_tables_columns().size(), 1);
  EXPECT_EQ(change_stream->tracked_tables_columns()[table->Name()].size(), 2);
  EXPECT_EQ(table->FindChangeStream("change_stream_test_table"), change_stream);
  EXPECT_EQ(table->change_streams().size(), 1);
  EXPECT_EQ(col->FindChangeStream("change_stream_test_table"), change_stream);
  EXPECT_EQ(col->change_streams().size(), 1);
}

TEST_F(SchemaTest, SequenceBasic) {
  EXPECT_EQ(base_schema_->sequences().size(), 0);
  ZETASQL_ASSERT_OK_AND_ASSIGN(base_schema_,
                       test::CreateSchemaWithOneSequence(type_factory_.get()));
  EXPECT_EQ(base_schema_->sequences().size(), 2);
  const Sequence* sequence = base_schema_->FindSequence("myseq");
  ASSERT_NE(sequence, nullptr);
  EXPECT_FALSE(sequence->start_with_counter().has_value());
  EXPECT_EQ(sequence->skip_range_min().value(), 1);
  EXPECT_EQ(sequence->skip_range_max().value(), 1000);

  const Table* table = base_schema_->FindTable("test_table");
  const Column* col = table->FindColumn("int64_col");
  EXPECT_TRUE(col->has_default_value());
  EXPECT_EQ(col->expression(), "GET_NEXT_SEQUENCE_VALUE(SEQUENCE myseq)");
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
              StatusIs(StatusCode::kInvalidArgument,
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

TEST_F(SchemaTest, TableSynonymBuilder) {
  auto t = table_builder("T", "S").build();
  ZETASQL_EXPECT_OK(t->Validate(&context_));

  Table::Builder tb = table_builder("T1", "S1");
  auto c1 = column_builder("C1", tb.get()).build();
  auto k1 = KeyColumn::Builder().set_column(c1.get()).build();
  auto t1 = tb.add_column(c1.get()).add_key_column(k1.get()).build();
  ZETASQL_EXPECT_OK(t1->Validate(&context_));

  const std::string synonym(130, 'S');
  auto t2 = table_builder("T1", synonym).build();
  EXPECT_EQ(t2->Validate(&context_),
            error::InvalidSchemaName("Synonym", synonym));
}

TEST_F(SchemaTest, ChangeStreamBuilderValid) {
  auto c = change_stream_builder("CS").build();
  ZETASQL_EXPECT_OK(c->Validate(&context_));
}

TEST_F(SchemaTest, ChangeStreamBuilderInvalid) {
  const std::string change_stream_name(130, 'C');
  auto invalid_cs = change_stream_builder(change_stream_name).build();
  EXPECT_EQ(invalid_cs->Validate(&context_),
            error::InvalidSchemaName("Change Stream", change_stream_name));
}

TEST_F(SchemaTest, SequenceBuilderValid) {
  auto c = sequence_builder("Sequence").build();
  ZETASQL_EXPECT_OK(c->Validate(&context_));
}

TEST_F(SchemaTest, SequenceBuilderInvalid) {
  const std::string sequence_name(130, 'S');
  auto invalid_cs = sequence_builder(sequence_name).build();
  EXPECT_EQ(invalid_cs->Validate(&context_),
            error::InvalidSchemaName("Sequence", sequence_name));
}

TEST_F(SchemaTest, DatabaseOptionsBuilderValid) {
  auto c = database_options_builder("C").build();
  ZETASQL_EXPECT_OK(c->Validate(&context_));
}

TEST_F(SchemaTest, DatabaseOptionsBuilderInvalid) {
  DatabaseOptions::Builder cs = database_options_builder("C1");
  const std::string database_name(130, 'C');
  auto invalid_cs = database_options_builder(database_name).build();
  EXPECT_EQ(invalid_cs->Validate(&context_),
            error::InvalidSchemaName("Database Options", database_name));
}

TEST_F(SchemaTest, LocalityGroupBuilderValid) {
  auto lg = locality_group_builder("LG").build();
  ZETASQL_EXPECT_OK(lg->Validate(&context_));
}

TEST_F(SchemaTest, LocalityGroupBuilderInvalid) {
  const std::string locality_group_name(130, 'L');
  auto invalid_lg = locality_group_builder(locality_group_name).build();
  EXPECT_EQ(invalid_lg->Validate(&context_),
            error::InvalidSchemaName("Locality Group", locality_group_name));
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

TEST_F(SchemaTest, ViewBuilder) {
  View::Builder vb;
  vb.set_name("V1");
  vb.set_sql_security(View::SqlSecurity::INVOKER);
  vb.set_sql_body("SELECT t.int64_col AS c1 FROM test_table t");
  vb.add_column(View::Column{"c1", type_factory_->get_int64()});
  auto test_table = base_schema_->FindTable("test_table");
  ASSERT_TRUE(test_table != nullptr);
  vb.add_dependency(test_table);

  auto view = vb.build();

  ZETASQL_EXPECT_OK(view->Validate(&context_));
  EXPECT_EQ(view->Name(), "V1");
  EXPECT_EQ(view->body(), "SELECT t.int64_col AS c1 FROM test_table t");
  EXPECT_THAT(view->dependencies(), testing::ElementsAreArray({test_table}));
  EXPECT_THAT(view->columns(), testing::ElementsAreArray({testing::FieldsAre(
                                   "c1", type_factory_->get_int64())}));
}

TEST_F(SchemaTest, NamedSchemaBuilder) {
  NamedSchema::Builder nsb;
  nsb.set_name("ns1");
  nsb.set_id("ns1");

  Table::Builder table_builder;
  auto c1 = column_builder("c1", table_builder.get()).build();
  auto c2 = column_builder("c2", table_builder.get()).build();
  auto k1 = KeyColumn::Builder().set_column(c1.get()).build();
  auto k2 = KeyColumn::Builder().set_column(c2.get()).build();
  auto t1 = table_builder.add_column(c1.get())
                .add_column(c2.get())
                .add_key_column(k1.get())
                .add_key_column(k2.get())
                .build();
  nsb.add_table(t1.get());

  View::Builder view_builder;
  view_builder.set_name("V1");
  view_builder.set_sql_security(View::SqlSecurity::INVOKER);
  view_builder.set_sql_body("SELECT t.int64_col AS c1 FROM test_table t");
  view_builder.add_column(View::Column{"c1", type_factory_->get_int64()});
  auto test_table = base_schema_->FindTable("test_table");
  ASSERT_TRUE(test_table != nullptr);
  view_builder.add_dependency(test_table);
  auto view = view_builder.build();
  nsb.add_view(view.get());

  Sequence::Builder sequence_builder;
  sequence_builder.set_name("S1");
  sequence_builder.set_id("S1");
  auto sequence = sequence_builder.build();
  nsb.add_sequence(sequence.get());

  Index::Builder index_builder;
  index_builder.set_name("I1");
  auto index = index_builder.build();
  nsb.add_index(index.get());

  auto named_schema = nsb.build();
  ZETASQL_EXPECT_OK(named_schema->Validate(&context_));
  EXPECT_EQ(named_schema->Name(), "ns1");
  EXPECT_EQ(named_schema->id(), "ns1");
  EXPECT_THAT(named_schema->tables(), testing::ElementsAreArray({t1.get()}));
  EXPECT_THAT(named_schema->views(), testing::ElementsAreArray({view.get()}));
  EXPECT_THAT(named_schema->sequences(),
              testing::ElementsAreArray({sequence.get()}));
  EXPECT_THAT(named_schema->indexes(),
              testing::ElementsAreArray({index.get()}));
}

// TODO Once dependency tracking is added, test here.
TEST_F(SchemaTest, UdfBuilder) {
  // Valid UDF with simple addition
  Udf::Builder udf_builder_simple_add;
  udf_builder_simple_add.set_name("udf_simple_add");
  udf_builder_simple_add.set_sql_security(Udf::SqlSecurity::INVOKER);
  udf_builder_simple_add.set_sql_body(
      "CREATE FUNCTION udf_simple_add(a INT64, b INT64) RETURNS INT64 AS (a + "
      "b)");
  auto udf_simple_add = udf_builder_simple_add.build();
  ZETASQL_EXPECT_OK(udf_simple_add->Validate(&context_));
  EXPECT_EQ(udf_simple_add->Name(), "udf_simple_add");
  EXPECT_EQ(udf_simple_add->body(),
            "CREATE FUNCTION udf_simple_add(a INT64, b INT64) RETURNS INT64 AS "
            "(a + b)");
  EXPECT_EQ(udf_simple_add->security(), Udf::SqlSecurity::INVOKER);

  // UDF with missing parameters (should fail validation due to missing name and
  // body)
  Udf::Builder udf_builder_missing_params;
  auto udf_missing_params = udf_builder_missing_params.build();
  EXPECT_THAT(
      udf_missing_params->Validate(&context_),
      StatusIs(StatusCode::kInternal, testing::HasSubstr("RET_CHECK failure")));

  // UDF with invalid name (starts with underscore)
  Udf::Builder udf_builder_invalid_name;
  udf_builder_invalid_name.set_name("_udf_invalid");
  udf_builder_invalid_name.set_sql_security(Udf::SqlSecurity::INVOKER);
  udf_builder_invalid_name.set_sql_body(
      "CREATE FUNCTION _udf_invalid(a INT64, b INT64) RETURNS INT64 AS (a + "
      "b)");
  auto udf_invalid_name = udf_builder_invalid_name.build();
  EXPECT_THAT(udf_invalid_name->Validate(&context_),
              StatusIs(StatusCode::kInvalidArgument,
                       testing::HasSubstr("Udf name not valid: _udf_invalid")));

  // UDF with no name
  Udf::Builder udf_builder_no_name;
  udf_builder_no_name.set_sql_security(Udf::SqlSecurity::INVOKER);
  udf_builder_no_name.set_sql_body(
      "CREATE FUNCTION udf_no_name(a INT64, b INT64) RETURNS INT64 AS (a + b)");
  auto udf_no_name = udf_builder_no_name.build();
  // Handled in the parsers so a ZETASQL_RET_CHECK failure is expected.
  EXPECT_THAT(
      udf_no_name->Validate(&context_),
      StatusIs(StatusCode::kInternal, testing::HasSubstr("RET_CHECK failure")));

  // UDF with no body
  Udf::Builder udf_builder_no_body;
  udf_builder_no_body.set_name("udf_no_body");
  udf_builder_no_body.set_sql_security(Udf::SqlSecurity::INVOKER);
  auto udf_no_body = udf_builder_no_body.build();
  // Handled in the parsers so a ZETASQL_RET_CHECK failure is expected.
  EXPECT_THAT(
      udf_no_body->Validate(&context_),
      StatusIs(StatusCode::kInternal, testing::HasSubstr("RET_CHECK failure")));

  // UDF with reserved keyword as name, needs to be quoted
  Udf::Builder udf_builder_reserved_keyword;
  udf_builder_reserved_keyword.set_name("function");
  udf_builder_reserved_keyword.set_sql_security(Udf::SqlSecurity::INVOKER);
  udf_builder_reserved_keyword.set_sql_body(
      "CREATE FUNCTION `function`(a INT64) RETURNS INT64 AS (a * 2)");
  auto udf_reserved_keyword = udf_builder_reserved_keyword.build();
  ZETASQL_EXPECT_OK(udf_reserved_keyword->Validate(&context_));
  EXPECT_EQ(udf_reserved_keyword->Name(), "function");

  // UDF with name starting with a number, needs to be quoted
  Udf::Builder udf_builder_number_start;
  udf_builder_number_start.set_name("1udf");
  udf_builder_number_start.set_sql_security(Udf::SqlSecurity::INVOKER);
  udf_builder_number_start.set_sql_body(
      "CREATE FUNCTION `1udf`(a INT64) RETURNS INT64 AS (a + 1)");
  auto udf_number_start = udf_builder_number_start.build();
  ZETASQL_EXPECT_OK(udf_number_start->Validate(&context_));
  EXPECT_EQ(udf_number_start->Name(), "1udf");

  // UDF with uppercase name
  Udf::Builder udf_builder_uppercase;
  udf_builder_uppercase.set_name("UDF_UPPERCASE");
  udf_builder_uppercase.set_sql_security(Udf::SqlSecurity::INVOKER);
  udf_builder_uppercase.set_sql_body(
      "CREATE FUNCTION UDF_UPPERCASE(a INT64) RETURNS INT64 AS (a - 1)");
  auto udf_uppercase = udf_builder_uppercase.build();
  ZETASQL_EXPECT_OK(udf_uppercase->Validate(&context_));
  EXPECT_EQ(udf_uppercase->Name(), "UDF_UPPERCASE");

  // UDF calling another UDF
  // First, define the helper UDF
  Udf::Builder udf_builder_helper;
  udf_builder_helper.set_name("helper_udf");
  udf_builder_helper.set_sql_security(Udf::SqlSecurity::INVOKER);
  udf_builder_helper.set_sql_body(
      "CREATE FUNCTION helper_udf(a INT64) RETURNS INT64 AS (a * a)");
  auto udf_helper = udf_builder_helper.build();
  ZETASQL_EXPECT_OK(udf_helper->Validate(&context_));
  EXPECT_EQ(udf_helper->Name(), "helper_udf");

  // Now, define a UDF that calls the helper UDF
  Udf::Builder udf_builder_caller;
  udf_builder_caller.set_name("caller_udf");
  udf_builder_caller.set_sql_security(Udf::SqlSecurity::INVOKER);
  udf_builder_caller.set_sql_body(
      "CREATE FUNCTION caller_udf(a INT64) RETURNS INT64 AS (helper_udf(a) + "
      "1)");
  udf_builder_caller.add_dependency(udf_helper.get());
  auto udf_caller = udf_builder_caller.build();
  ZETASQL_EXPECT_OK(udf_caller->Validate(&context_));
  EXPECT_EQ(udf_caller->Name(), "caller_udf");
}

// TODO: GetDatabaseDDL needs more robust testing with
// RTG/golden files.
TEST_F(SchemaTest, PrintDDLStatementsTestOneTable) {
  std::unique_ptr<const Schema> schema =
      test::CreateSchemaWithOneTable(type_factory_.get());
  absl::StatusOr<std::vector<std::string>> statements =
      PrintDDLStatements(schema.get());

  EXPECT_THAT(
      statements,
      IsOkAndHolds(ElementsAre(
          R"(CREATE TABLE test_table (
  int64_col INT64 NOT NULL,
  string_col STRING(MAX),
) PRIMARY KEY(int64_col))",
          "CREATE UNIQUE INDEX test_index ON test_table(string_col DESC)")));
}

TEST_F(SchemaTest, PrintDDLStatementsTestOneTableWithSynonym) {
  std::unique_ptr<const Schema> schema =
      test::CreateSchemaWithOneTableWithSynonym(type_factory_.get());
  absl::StatusOr<std::vector<std::string>> statements =
      PrintDDLStatements(schema.get());

  EXPECT_THAT(statements, IsOkAndHolds(ElementsAre(
                              R"(CREATE TABLE test_table (
  int64_col INT64 NOT NULL,
  string_col STRING(MAX),
  SYNONYM(test_synonym),
) PRIMARY KEY(int64_col))")));
}

TEST_F(SchemaTest, PrintDDLStatementsTestOneTableDrop) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const backend::Schema> schema,
                       test::CreateSchemaFromDDL(
                           {
                               R"(CREATE TABLE T (
  col1 INT64,
  col2 STRING(MAX),
) PRIMARY KEY(col1))",
                               R"(CREATE TABLE T1 (
  col1 INT64,
  col2 STRING(MAX),
) PRIMARY KEY(col1))",
                               R"(DROP TABLE T)"},
                           type_factory_.get()));
  absl::StatusOr<std::vector<std::string>> statements =
      PrintDDLStatements(schema.get());

  EXPECT_THAT(statements, IsOkAndHolds(ElementsAre(
                              R"(CREATE TABLE T1 (
  col1 INT64,
  col2 STRING(MAX),
) PRIMARY KEY(col1))")));
}

TEST_F(SchemaTest, PostgreSQLPrintDDLStatementsTestOneTable) {
  SetPostgresqlDialect();
  std::unique_ptr<const Schema> schema = test::CreateSchemaWithOneTable(
      type_factory_.get(), database_api::DatabaseDialect::POSTGRESQL);
  absl::StatusOr<std::vector<std::string>> statements =
      PrintDDLStatements(schema.get());

  EXPECT_THAT(statements, IsOkAndHolds(ElementsAre(
                              R"(CREATE TABLE test_table (
  int64_col bigint NOT NULL,
  string_col character varying,
  PRIMARY KEY(int64_col)
))",
                              "CREATE UNIQUE INDEX test_index ON test_table "
                              "(string_col DESC)")));
}

TEST_F(SchemaTest, PrintDDLStatementsTestInterleaving) {
  std::unique_ptr<const Schema> schema =
      test::CreateSchemaWithInterleaving(type_factory_.get());
  absl::StatusOr<std::vector<std::string>> statements =
      PrintDDLStatements(schema.get());

  EXPECT_THAT(statements, IsOkAndHolds(ElementsAre(
                              R"(CREATE TABLE Parent (
  k1 INT64 NOT NULL,
  c1 STRING(MAX),
) PRIMARY KEY(k1))",
                              R"(CREATE TABLE CascadeDeleteChild (
  k1 INT64 NOT NULL,
  k2 INT64 NOT NULL,
  c1 STRING(MAX),
) PRIMARY KEY(k1, k2),
  INTERLEAVE IN PARENT Parent ON DELETE CASCADE)",
                              R"(CREATE TABLE NoActionDeleteChild (
  k1 INT64 NOT NULL,
  k2 INT64 NOT NULL,
  c1 STRING(MAX),
) PRIMARY KEY(k1, k2),
  INTERLEAVE IN PARENT Parent ON DELETE NO ACTION)")));
}

TEST_F(SchemaTest, PostgreSQLPrintDDLStatementsTestInterleaving) {
  SetPostgresqlDialect();
  std::unique_ptr<const Schema> schema = test::CreateSchemaWithInterleaving(
      type_factory_.get(), database_api::DatabaseDialect::POSTGRESQL);
  absl::StatusOr<std::vector<std::string>> statements =
      PrintDDLStatements(schema.get());

  EXPECT_THAT(statements, IsOkAndHolds(ElementsAre(
                              R"(CREATE TABLE parent (
  k1 bigint NOT NULL,
  c1 character varying,
  PRIMARY KEY(k1)
))",
                              R"(CREATE TABLE cascadedeletechild (
  k1 bigint NOT NULL,
  k2 bigint NOT NULL,
  c1 character varying,
  PRIMARY KEY(k1, k2)
) INTERLEAVE IN PARENT parent ON DELETE CASCADE)",
                              R"(CREATE TABLE noactiondeletechild (
  k1 bigint NOT NULL,
  k2 bigint NOT NULL,
  c1 character varying,
  PRIMARY KEY(k1, k2)
) INTERLEAVE IN PARENT parent ON DELETE NO ACTION)")));
}

TEST_F(SchemaTest, PrintDDLStatementsTestForeignKey) {
  std::unique_ptr<const Schema> schema =
      test::CreateSchemaWithForeignKey(type_factory_.get());
  absl::StatusOr<std::vector<std::string>> statements =
      PrintDDLStatements(schema.get());

  EXPECT_THAT(statements,
              IsOkAndHolds(ElementsAre(R"(CREATE TABLE referenced_table (
  k1 INT64 NOT NULL,
  c1 STRING(20),
  c2 STRING(20),
) PRIMARY KEY(k1))",
                                       R"(CREATE TABLE referencing_table (
  k1 INT64 NOT NULL,
  c1 STRING(20),
  c2 STRING(20),
  CONSTRAINT C FOREIGN KEY(k1, c1) REFERENCES referenced_table(k1, c1),
  FOREIGN KEY(c2) REFERENCES referenced_table(c2),
  CONSTRAINT C2 FOREIGN KEY(k1, c2) REFERENCES referenced_table(k1, c2) NOT ENFORCED,
) PRIMARY KEY(k1))")));
}

TEST_F(SchemaTest, PostgreSQLPrintDDLStatementsTestForeignKey) {
  SetPostgresqlDialect();
  std::unique_ptr<const Schema> schema = test::CreateSchemaWithForeignKey(
      type_factory_.get(), database_api::DatabaseDialect::POSTGRESQL);
  absl::StatusOr<std::vector<std::string>> statements =
      PrintDDLStatements(schema.get());

  EXPECT_THAT(statements, IsOkAndHolds(ElementsAre(
                              R"(CREATE TABLE referenced_table (
  k1 bigint NOT NULL,
  c1 character varying(20),
  c2 character varying(20),
  PRIMARY KEY(k1)
))",
                              R"(CREATE TABLE referencing_table (
  k1 bigint NOT NULL,
  c1 character varying(20),
  c2 character varying(20),
  PRIMARY KEY(k1),
  CONSTRAINT c FOREIGN KEY (k1, c1) REFERENCES referenced_table(k1, c1),
  FOREIGN KEY (c2) REFERENCES referenced_table(c2)
))")));
}

TEST_F(SchemaTest, PrintDDLStatementsTestForeignKeyOnDeleteAction) {
  std::unique_ptr<const Schema> schema =
      test::CreateSchemaWithForeignKeyOnDelete(type_factory_.get());
  absl::StatusOr<std::vector<std::string>> statements =
      PrintDDLStatements(schema.get());

  EXPECT_THAT(statements,
              IsOkAndHolds(ElementsAre(R"(CREATE TABLE referenced_table (
  k1 INT64 NOT NULL,
  c1 STRING(20),
  c2 STRING(20),
) PRIMARY KEY(k1))",
                                       R"(CREATE TABLE referencing_table (
  k1 INT64 NOT NULL,
  c1 STRING(20),
  c2 STRING(20),
  CONSTRAINT C1 FOREIGN KEY(k1) REFERENCES referenced_table(k1) ON DELETE CASCADE,
  CONSTRAINT C2 FOREIGN KEY(k1, c1) REFERENCES referenced_table(k1, c1) ON DELETE CASCADE,
) PRIMARY KEY(k1))")));
}

TEST_F(SchemaTest, PostgreSQLPrintDDLStatementsTestForeignKeyOnDeleteAction) {
  SetPostgresqlDialect();
  std::unique_ptr<const Schema> schema =
      test::CreateSchemaWithForeignKeyOnDelete(
          type_factory_.get(), database_api::DatabaseDialect::POSTGRESQL);
  absl::StatusOr<std::vector<std::string>> statements =
      PrintDDLStatements(schema.get());

  EXPECT_THAT(statements, IsOkAndHolds(ElementsAre(
                              R"(CREATE TABLE referenced_table (
  k1 bigint NOT NULL,
  c1 character varying(20),
  c2 character varying(20),
  PRIMARY KEY(k1)
))",
                              R"(CREATE TABLE referencing_table (
  k1 bigint NOT NULL,
  c1 character varying(20),
  c2 character varying(20),
  PRIMARY KEY(k1),
  CONSTRAINT c1 FOREIGN KEY (k1) REFERENCES referenced_table(k1) ON DELETE CASCADE,
  CONSTRAINT c2 FOREIGN KEY (k1, c1) REFERENCES referenced_table(k1, c1) ON DELETE CASCADE
))")));
}

TEST_F(SchemaTest, TestForeignKeyDebugString) {
  std::unique_ptr<const Schema> schema =
      test::CreateSchemaWithForeignKey(type_factory_.get());
  // Verify the foreign key debug info.
  const Table* table = schema->FindTable("referencing_table");
  ASSERT_NE(table, nullptr);
  const ForeignKey* fk1 = table->FindForeignKey("C");
  std::string fk_debug_info1 = fk1->DebugString();
  EXPECT_THAT(fk_debug_info1,
              "FK:C:referencing_table(k1,c1)[IDX_referencing_table_k1_c1_N_"
              "0384119846ECD68B]:referenced_table(k1,c1)[IDX_referenced_table_"
              "k1_c1_U_E4AC4992000E770F][][]");
  const ForeignKey* fk2 = table->FindForeignKey("C2");
  std::string fk_debug_info2 = fk2->DebugString();
  EXPECT_THAT(fk_debug_info2,
              "FK:C2:referencing_table(k1,c2)[]:referenced_table(k1,c2)[IDX_"
              "referenced_table_k1_c2_U_8FC9C72A12156B74][][NOT ENFORCED]");
}

TEST_F(SchemaTest, TestForeignKeyActionDebugString) {
  std::unique_ptr<const Schema> schema =
      test::CreateSchemaWithForeignKeyOnDelete(type_factory_.get());
  // Verify the foreign key debug info.
  const Table* table = schema->FindTable("referencing_table");
  ASSERT_NE(table, nullptr);
  const ForeignKey* fk1 = table->FindForeignKey("C1");
  std::string fk_debug_info1 = fk1->DebugString();
  EXPECT_THAT(fk_debug_info1,
              "FK:C1:referencing_table(k1)[PK]:referenced_table(k1)[PK][ON "
              "DELETE CASCADE][]");
  const ForeignKey* fk2 = table->FindForeignKey("C2");
  std::string fk_debug_info2 = fk2->DebugString();
  EXPECT_THAT(fk_debug_info2,
              "FK:C2:referencing_table(k1,c1)[IDX_referencing_table_k1_c1_N_"
              "0384119846ECD68B]:referenced_table(k1,c1)[IDX_referenced_table_"
              "k1_c1_U_E4AC4992000E770F][ON DELETE CASCADE][]");
}

TEST_F(SchemaTest, PrintDDLStatementsTestColumnExpressions) {
  std::string test_table =
      R"(
          CREATE TABLE test_table (
            int64_col INT64 NOT NULL,
            string_col STRING(10),
            default_int64_col INT64 DEFAULT (10),
            default_timestamp_col TIMESTAMP DEFAULT (CURRENT_TIMESTAMP()),
            gen_col INT64 NOT NULL AS (int64_col + LENGTH(string_col)) STORED,
          ) PRIMARY KEY (int64_col)
      )";
  absl::StatusOr<std::unique_ptr<const backend::Schema>> schema =
      test::CreateSchemaFromDDL(
          {
              test_table,
          },
          type_factory_.get());
  ZETASQL_ASSERT_OK(schema);
  absl::StatusOr<std::vector<std::string>> statements =
      PrintDDLStatements(schema.value().get());

  EXPECT_THAT(statements, IsOkAndHolds(ElementsAre(R"(CREATE TABLE test_table (
  int64_col INT64 NOT NULL,
  string_col STRING(10),
  default_int64_col INT64 DEFAULT (10),
  default_timestamp_col TIMESTAMP DEFAULT (CURRENT_TIMESTAMP()),
  gen_col INT64 NOT NULL AS (int64_col + LENGTH(string_col)) STORED,
) PRIMARY KEY(int64_col))")));
}

TEST_F(SchemaTest, PostgreSQLPrintDDLStatementsTestColumnExpressions) {
  SetPostgresqlDialect();
  std::string test_table =
      R"(
          CREATE TABLE test_table (
            int64_col bigint not null primary key,
            string_col varchar(10),
            default_int64_col bigint DEFAULT (10),
            default_timestamp_col timestamptz DEFAULT (NOW()),
            gen_col bigint not null generated always as ("int64_col" + LENGTH("string_col")) stored
          )
      )";
  absl::StatusOr<std::unique_ptr<const backend::Schema>> schema =
      test::CreateSchemaFromDDL(
          {
              test_table,
          },
          type_factory_.get(),
          /*proto_descriptor_bytes=*/"",
          database_api::DatabaseDialect::POSTGRESQL);
  ZETASQL_ASSERT_OK(schema);
  absl::StatusOr<std::vector<std::string>> statements =
      PrintDDLStatements(schema.value().get());

  EXPECT_THAT(statements, zetasql_base::testing::IsOkAndHolds(
                              testing::ElementsAre(R"(CREATE TABLE test_table (
  int64_col bigint NOT NULL,
  string_col character varying(10),
  default_int64_col bigint DEFAULT '10'::bigint,
  default_timestamp_col timestamp with time zone DEFAULT now(),
  gen_col bigint GENERATED ALWAYS AS ((int64_col + length(string_col))) STORED NOT NULL,
  PRIMARY KEY(int64_col)
))")));
}

TEST_F(SchemaTest, PrintDDLStatementsTestAllowCommitTimestamp) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const backend::Schema> schema,
                       test::CreateSchemaFromDDL(
                           {
                               R"(
    CREATE TABLE T(
      col1 INT64,
      col2 TIMESTAMP OPTIONS(
        allow_commit_timestamp = true
      )
    ) PRIMARY KEY(col1))",
                           },
                           type_factory_.get()));

  EXPECT_THAT(PrintDDLStatements(schema.get()),
              IsOkAndHolds(ElementsAre(R"(CREATE TABLE T (
  col1 INT64,
  col2 TIMESTAMP OPTIONS (
    allow_commit_timestamp = true
  ),
) PRIMARY KEY(col1))")));
}

TEST_F(SchemaTest, PostgreSQLPrintDDLStatementsTestAllowCommitTimestamp) {
  SetPostgresqlDialect();
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const backend::Schema> schema,
                       test::CreateSchemaFromDDL(
                           {
                               R"(
    CREATE TABLE T(
      col1 bigint primary key,
      col2 spanner.commit_timestamp
    ))",
                           },
                           type_factory_.get(),
                           /*proto_descriptor_bytes=*/"",
                           database_api::DatabaseDialect::POSTGRESQL));

  EXPECT_THAT(PrintDDLStatements(schema.get()),
              IsOkAndHolds(ElementsAre(R"(CREATE TABLE t (
  col1 bigint NOT NULL,
  col2 spanner.commit_timestamp,
  PRIMARY KEY(col1)
))")));
}

TEST_F(SchemaTest, PrintDDLStatementsTestCheckConstraints) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const backend::Schema> schema,
                       test::CreateSchemaFromDDL(
                           {R"(CREATE TABLE T (
             K INT64,
             V INT64,
             CONSTRAINT C1 CHECK(K > 0)
           ) PRIMARY KEY (K))",
                            "ALTER TABLE T ADD CONSTRAINT C2 CHECK(K + V > 0)"},
                           type_factory_.get()));

  EXPECT_THAT(PrintDDLStatements(schema.get()),
              IsOkAndHolds(ElementsAre(R"(CREATE TABLE T (
  K INT64,
  V INT64,
  CONSTRAINT C1 CHECK(K > 0),
  CONSTRAINT C2 CHECK(K + V > 0),
) PRIMARY KEY(K))")));
}

TEST_F(SchemaTest, PostgreSQLPrintDDLStatementsTestCheckConstraints) {
  SetPostgresqlDialect();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const backend::Schema> schema,
      test::CreateSchemaFromDDL(
          {R"(
          CREATE TABLE "T" (
             "K" bigint PRIMARY KEY,
             "V" bigint,
             CONSTRAINT "C1" CHECK("K" > 0)
           ))",
           R"(ALTER TABLE "T" ADD CONSTRAINT "C2" CHECK("K" + "V" > 0))"},
          type_factory_.get(),
          /*proto_descriptor_bytes=*/"",
          database_api::DatabaseDialect::POSTGRESQL));
  EXPECT_THAT(PrintDDLStatements(schema.get()),
              IsOkAndHolds(ElementsAre(R"(CREATE TABLE "T" (
  "K" bigint NOT NULL,
  "V" bigint,
  PRIMARY KEY("K"),
  CONSTRAINT "C1" CHECK(("K" > '0'::bigint)),
  CONSTRAINT "C2" CHECK((("K" + "V") > '0'::bigint))
))")));
}

TEST_F(SchemaTest, PrintDDLStatementsTestNoNameCheckConstraints) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const backend::Schema> schema,
                       test::CreateSchemaFromDDL({R"(CREATE TABLE T (
             K INT64,
             V INT64,
             CHECK(K > 0)
           ) PRIMARY KEY (K))"},
                                                 type_factory_.get()));

  EXPECT_THAT(PrintDDLStatements(schema.get()),
              IsOkAndHolds(ElementsAre(R"(CREATE TABLE T (
  K INT64,
  V INT64,
  CHECK(K > 0),
) PRIMARY KEY(K))")));
}

TEST_F(SchemaTest, PostgreSQLPrintDDLStatementsTestNoNameCheckConstraints) {
  SetPostgresqlDialect();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const backend::Schema> schema,
      test::CreateSchemaFromDDL({R"(
          CREATE TABLE "T" (
             "K" bigint PRIMARY KEY,
             "V" bigint,
             CHECK("K" > 0)
           ))"},
                                type_factory_.get(),
                                /*proto_descriptor_bytes=*/"",
                                database_api::DatabaseDialect::POSTGRESQL));
  EXPECT_THAT(PrintDDLStatements(schema.get()),
              IsOkAndHolds(ElementsAre(R"(CREATE TABLE "T" (
  "K" bigint NOT NULL,
  "V" bigint,
  PRIMARY KEY("K"),
  CHECK(("K" > '0'::bigint))
))")));
}

TEST_F(SchemaTest, PostgreSQLPrintDDLStatementsTestCheckConstraintsOrdering) {
  SetPostgresqlDialect();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const backend::Schema> schema,
      test::CreateSchemaFromDDL({R"(
  CREATE TABLE Users(
    id bigint PRIMARY KEY,
    reg varchar(255),
    named varchar(255),
    inline_named varchar(255) CONSTRAINT con_inline_named CHECK(inline_named IS NOT NULL),
    inline varchar(255) CHECK(inline IS NOT NULL),
    CHECK(reg IS NOT NULL),
    CONSTRAINT con_named CHECK(named IS NOT NULL)
  )
           )"},
                                type_factory_.get(),
                                /*proto_descriptor_bytes=*/"",
                                database_api::DatabaseDialect::POSTGRESQL));
  EXPECT_THAT(PrintDDLStatements(schema.get()),
              IsOkAndHolds(ElementsAre(R"(CREATE TABLE users (
  id bigint NOT NULL,
  reg character varying(255),
  named character varying(255),
  inline_named character varying(255),
  inline character varying(255),
  PRIMARY KEY(id),
  CHECK((inline IS NOT NULL)),
  CHECK((reg IS NOT NULL)),
  CONSTRAINT con_inline_named CHECK((inline_named IS NOT NULL)),
  CONSTRAINT con_named CHECK((named IS NOT NULL))
))")));
}

TEST_F(SchemaTest, PrintDDLStatementsTestViews) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const backend::Schema> schema,
                       test::CreateSchemaFromDDL({R"(
    CREATE TABLE T(
      col1 INT64,
      col2 STRING(MAX)
    ) PRIMARY KEY(col1)
  )",
                                                  R"(
    CREATE OR REPLACE VIEW `MyView` SQL SECURITY INVOKER AS
    SELECT T.col1, T.col2 FROM T
  )"},
                                                 type_factory_.get()));

  EXPECT_THAT(
      PrintDDLStatements(schema.get()),
      IsOkAndHolds(ElementsAre(R"(CREATE TABLE T (
  col1 INT64,
  col2 STRING(MAX),
) PRIMARY KEY(col1))",
                               "CREATE VIEW MyView SQL SECURITY INVOKER AS "
                               "SELECT T.col1, T.col2 FROM T")));
}

TEST_F(SchemaTest, PostgreSQLPrintDDLStatementsTestViews) {
  SetPostgresqlDialect();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const backend::Schema> schema,
      test::CreateSchemaFromDDL({R"(
    CREATE TABLE t(
      col1 bigint primary key,
      col2 varchar
    )
  )",
                                 R"(
    CREATE OR REPLACE VIEW "MyView" SQL SECURITY INVOKER AS SELECT T.col1, T.col2 FROM T
  )"},
                                type_factory_.get(),
                                /*proto_descriptor_bytes=*/"",
                                database_api::DatabaseDialect::POSTGRESQL));

  EXPECT_THAT(
      PrintDDLStatements(schema.get()),
      IsOkAndHolds(ElementsAre(
          R"(CREATE TABLE t (
  col1 bigint NOT NULL,
  col2 character varying,
  PRIMARY KEY(col1)
))",
          R"(CREATE VIEW "MyView" SQL SECURITY INVOKER AS SELECT col1, col2 FROM t)")));
}

TEST_F(SchemaTest, PrintDDLStatementsTestStoredIndex) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const backend::Schema> schema,
      test::CreateSchemaFromDDL(
          {R"(
    CREATE TABLE T(
      col1 INT64,
      col2 STRING(MAX),
      col3 STRING(MAX),
      col4 INT64,
    ) PRIMARY KEY(col1)
  )",
           "CREATE INDEX col2_idx ON T(col2) STORING (col3, col4)"},
          type_factory_.get()));

  EXPECT_THAT(PrintDDLStatements(schema.get()),
              IsOkAndHolds(ElementsAre(
                  R"(CREATE TABLE T (
  col1 INT64,
  col2 STRING(MAX),
  col3 STRING(MAX),
  col4 INT64,
) PRIMARY KEY(col1))",
                  "CREATE INDEX col2_idx ON T(col2) STORING (col3, col4)")));
}

TEST_F(SchemaTest, PostgreSQLPrintDDLStatementsTestStoredIndex) {
  SetPostgresqlDialect();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const backend::Schema> schema,
      test::CreateSchemaFromDDL(
          {R"(
    CREATE TABLE t(
      col1 bigint primary key,
      col2 varchar,
      col3 varchar,
      col4 bigint
    )
  )",
           "CREATE INDEX col2_idx ON t(col2) INCLUDE (col3, col4)"},
          type_factory_.get(),
          /*proto_descriptor_bytes=*/"",
          database_api::DatabaseDialect::POSTGRESQL));

  EXPECT_THAT(PrintDDLStatements(schema.get()),
              IsOkAndHolds(ElementsAre(
                  R"(CREATE TABLE t (
  col1 bigint NOT NULL,
  col2 character varying,
  col3 character varying,
  col4 bigint,
  PRIMARY KEY(col1)
))",
                  "CREATE INDEX col2_idx ON t (col2) INCLUDE "
                  "(col3, col4)")));
}

TEST_F(SchemaTest, ZetaSQLPrintDDLStatementsTestCreateChangeStreams) {
  // Test CREATE CHANGE STREAM statements tracking nothing, pk columns
  // implicitly, one entire table, and ALL.
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const backend::Schema> schema,
                       test::CreateSchemaFromDDL(
                           {
                               R"(CREATE TABLE T1 (
      col1 INT64,
      col2 STRING(MAX),
      col3 STRING(MAX)
    ) PRIMARY KEY(col1))",
                               R"(CREATE TABLE T2 (
      col1 INT64,
      col2 STRING(MAX),
      col3 STRING(MAX)
    ) PRIMARY KEY(col1))",
                               R"(CREATE CHANGE STREAM cs)",
                               R"(CREATE CHANGE STREAM cs_T1_col1 FOR T1())",
                               R"(CREATE CHANGE STREAM cs_T1 FOR T1)",
                               R"(CREATE CHANGE STREAM cs_ALL FOR ALL)",
                               R"(CREATE CHANGE STREAM cs_T1_T2 FOR T1, T2)"},
                           type_factory_.get()));
  absl::StatusOr<std::vector<std::string>> statements =
      PrintDDLStatements(schema.get());

  EXPECT_THAT(statements, IsOkAndHolds(ElementsAre(
                              R"(CREATE TABLE T1 (
  col1 INT64,
  col2 STRING(MAX),
  col3 STRING(MAX),
) PRIMARY KEY(col1))",
                              R"(CREATE TABLE T2 (
  col1 INT64,
  col2 STRING(MAX),
  col3 STRING(MAX),
) PRIMARY KEY(col1))",
                              R"(CREATE CHANGE STREAM cs)",
                              R"(CREATE CHANGE STREAM cs_T1_col1 FOR T1())",
                              R"(CREATE CHANGE STREAM cs_T1 FOR T1)",
                              R"(CREATE CHANGE STREAM cs_ALL FOR ALL)",
                              R"(CREATE CHANGE STREAM cs_T1_T2 FOR T1, T2)")));

  // Test CREATE CHANGE STREAM statements tracking columns specifically.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      schema,
      test::CreateSchemaFromDDL(
          {
              R"(CREATE TABLE T1 (
      col1 INT64,
      col2 STRING(MAX),
      col3 STRING(MAX)
    ) PRIMARY KEY(col1))",
              R"(CREATE TABLE T2 (
      col1 INT64,
      col2 STRING(MAX),
      col3 STRING(MAX)
    ) PRIMARY KEY(col1))",
              R"(CREATE CHANGE STREAM cs_T1_col2 FOR T1(col2))",
              R"(CREATE CHANGE STREAM cs_T1_col2_col3 FOR T1(col2, col3))",
              R"(CREATE CHANGE STREAM cs_T1_col2_T2_col2 FOR T1(col2), T2(col2))"},
          type_factory_.get()));
  statements = PrintDDLStatements(schema.get());

  EXPECT_THAT(
      statements,
      IsOkAndHolds(ElementsAre(
          R"(CREATE TABLE T1 (
  col1 INT64,
  col2 STRING(MAX),
  col3 STRING(MAX),
) PRIMARY KEY(col1))",
          R"(CREATE TABLE T2 (
  col1 INT64,
  col2 STRING(MAX),
  col3 STRING(MAX),
) PRIMARY KEY(col1))",
          R"(CREATE CHANGE STREAM cs_T1_col2 FOR T1(col2))",
          R"(CREATE CHANGE STREAM cs_T1_col2_col3 FOR T1(col2, col3))",
          R"(CREATE CHANGE STREAM cs_T1_col2_T2_col2 FOR T1(col2), T2(col2))")));
}

TEST_F(
    SchemaTest,
    ZetaSQLPrintDDLStatementsTestCreateChangeStreamsWithSpecialTableNames) {
  // Test CREATE CHANGE STREAM statements tracking nothing, pk columns
  // implicitly, one entire table, and ALL.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const backend::Schema> schema,
      test::CreateSchemaFromDDL(
          {R"(CREATE TABLE `ALL` (Id STRING(20) NOT NULL,`All` STRING(20),) PRIMARY KEY(Id))",
           R"(CREATE CHANGE STREAM change_stream FOR `ALL`(`All`))"},
          type_factory_.get()));
  absl::StatusOr<std::vector<std::string>> statements =
      PrintDDLStatements(schema.get());

  EXPECT_THAT(statements,
              IsOkAndHolds(ElementsAre(
                  R"(CREATE TABLE `ALL` (
  Id STRING(20) NOT NULL,
  `All` STRING(20),
) PRIMARY KEY(Id))",
                  R"(CREATE CHANGE STREAM change_stream FOR `ALL`(`All`))")));
}

TEST_F(SchemaTest,
       ZetaSQLPrintDDLStatementsTestCreateChangeStreamsValueCaptureTypes) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const backend::Schema> schema,
      test::CreateSchemaFromDDL(
          {R"(
    CREATE TABLE T (
      col1 INT64,
      col2 STRING(MAX),
      col3 STRING(MAX)
    ) PRIMARY KEY(col1))",
           R"(CREATE CHANGE STREAM cs_T_old_and_new_values FOR T OPTIONS ( value_capture_type = 'OLD_AND_NEW_VALUES' ))",
           R"(CREATE CHANGE STREAM cs_T_new_row FOR T OPTIONS ( value_capture_type = 'NEW_ROW' ))",
           R"(CREATE CHANGE STREAM cs_T_new_values FOR T OPTIONS ( value_capture_type = 'NEW_VALUES' ))"},
          type_factory_.get()));
  absl::StatusOr<std::vector<std::string>> statements =
      PrintDDLStatements(schema.get());

  EXPECT_THAT(
      statements,
      IsOkAndHolds(ElementsAre(
          R"(CREATE TABLE T (
  col1 INT64,
  col2 STRING(MAX),
  col3 STRING(MAX),
) PRIMARY KEY(col1))",
          R"(CREATE CHANGE STREAM cs_T_old_and_new_values FOR T OPTIONS ( value_capture_type = 'OLD_AND_NEW_VALUES' ))",
          R"(CREATE CHANGE STREAM cs_T_new_row FOR T OPTIONS ( value_capture_type = 'NEW_ROW' ))",
          R"(CREATE CHANGE STREAM cs_T_new_values FOR T OPTIONS ( value_capture_type = 'NEW_VALUES' ))")));
}

TEST_F(SchemaTest,
       ZetaSQLPrintDDLStatementsTestCreateChangeStreamsTwoOptions) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const backend::Schema> schema,
      test::CreateSchemaFromDDL(
          {R"(
    CREATE TABLE T (
      col1 INT64,
      col2 STRING(MAX),
      col3 STRING(MAX)
    ) PRIMARY KEY(col1))",
           R"(CREATE CHANGE STREAM cs_T_new_row_36h FOR T OPTIONS ( value_capture_type = 'NEW_ROW', retention_period = '36h' ))",
           R"(CREATE CHANGE STREAM cs_T_36h_new_row FOR T OPTIONS ( retention_period = '36h', value_capture_type = 'NEW_ROW' ))"},
          type_factory_.get()));
  absl::StatusOr<std::vector<std::string>> statements =
      PrintDDLStatements(schema.get());

  EXPECT_THAT(
      statements,
      IsOkAndHolds(ElementsAre(
          R"(CREATE TABLE T (
  col1 INT64,
  col2 STRING(MAX),
  col3 STRING(MAX),
) PRIMARY KEY(col1))",
          R"(CREATE CHANGE STREAM cs_T_new_row_36h FOR T OPTIONS ( value_capture_type = 'NEW_ROW', retention_period = '36h' ))",
          R"(CREATE CHANGE STREAM cs_T_36h_new_row FOR T OPTIONS ( retention_period = '36h', value_capture_type = 'NEW_ROW' ))")));
}

TEST_F(SchemaTest,
       ZetaSQLPrintDDLStatementsTestAlterChangeStreamsSetOptions) {
  // ALTER retention_period
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const backend::Schema> schema,
      test::CreateSchemaFromDDL(
          {R"(
    CREATE TABLE T (
      col1 INT64,
      col2 STRING(MAX)
    ) PRIMARY KEY(col1))",
           R"(CREATE CHANGE STREAM cs_T FOR T)",
           R"(ALTER CHANGE STREAM cs_T SET OPTIONS ( retention_period = '36h' ))"},
          type_factory_.get()));
  absl::StatusOr<std::vector<std::string>> statements =
      PrintDDLStatements(schema.get());

  EXPECT_THAT(
      statements,
      IsOkAndHolds(ElementsAre(
          R"(CREATE TABLE T (
  col1 INT64,
  col2 STRING(MAX),
) PRIMARY KEY(col1))",
          R"(CREATE CHANGE STREAM cs_T FOR T OPTIONS ( retention_period = '36h' ))")));

  // ALTER value_capture_type
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      schema,
      test::CreateSchemaFromDDL(
          {R"(
    CREATE TABLE T (
      col1 INT64,
      col2 STRING(MAX)
    ) PRIMARY KEY(col1))",
           R"(CREATE CHANGE STREAM cs_T FOR T)",
           R"(ALTER CHANGE STREAM cs_T SET OPTIONS ( value_capture_type = 'NEW_VALUES' ))"},
          type_factory_.get()));
  statements = PrintDDLStatements(schema.get());

  EXPECT_THAT(
      statements,
      IsOkAndHolds(ElementsAre(
          R"(CREATE TABLE T (
  col1 INT64,
  col2 STRING(MAX),
) PRIMARY KEY(col1))",
          R"(CREATE CHANGE STREAM cs_T FOR T OPTIONS ( value_capture_type = 'NEW_VALUES' ))")));
}

TEST_F(SchemaTest, ZetaSQLPrintDDLStatementsTestAlterChangeStreamsSetFor) {
  // ALTER tracked object
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const backend::Schema> schema,
      test::CreateSchemaFromDDL({R"(
    CREATE TABLE T (
      col1 INT64,
      col2 STRING(MAX)
    ) PRIMARY KEY(col1))",
                                 R"(CREATE CHANGE STREAM cs_T FOR T)",
                                 R"(ALTER CHANGE STREAM cs_T SET FOR ALL)"},
                                type_factory_.get()));
  absl::StatusOr<std::vector<std::string>> statements =
      PrintDDLStatements(schema.get());

  EXPECT_THAT(statements, IsOkAndHolds(ElementsAre(
                              R"(CREATE TABLE T (
  col1 INT64,
  col2 STRING(MAX),
) PRIMARY KEY(col1))",
                              R"(CREATE CHANGE STREAM cs_T FOR ALL)")));
}

TEST_F(SchemaTest,
       ZetaSQLPrintDDLStatementsTestAlterChangeStreamsDropForAll) {
  // ALTER drop for all
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const backend::Schema> schema,
      test::CreateSchemaFromDDL(
          {R"(
    CREATE TABLE T (
      col1 INT64,
      col2 STRING(MAX)
    ) PRIMARY KEY(col1))",
           R"(CREATE CHANGE STREAM cs_T FOR T OPTIONS ( value_capture_type = 'NEW_VALUES' ))",
           R"(ALTER CHANGE STREAM cs_T DROP FOR ALL)"},
          type_factory_.get()));
  absl::StatusOr<std::vector<std::string>> statements =
      PrintDDLStatements(schema.get());
  EXPECT_THAT(
      statements,
      IsOkAndHolds(ElementsAre(
          R"(CREATE TABLE T (
  col1 INT64,
  col2 STRING(MAX),
) PRIMARY KEY(col1))",
          R"(CREATE CHANGE STREAM cs_T OPTIONS ( value_capture_type = 'NEW_VALUES' ))")));
}

TEST_F(SchemaTest, ZetaSQLPrintDDLStatementsTestDropChangeStreams) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const backend::Schema> schema,
      test::CreateSchemaFromDDL(
          {R"(
    CREATE TABLE T (
      col1 INT64,
      col2 STRING(MAX)
    ) PRIMARY KEY(col1))",
           R"(CREATE CHANGE STREAM cs_T FOR T)", R"(DROP CHANGE STREAM cs_T)"},
          type_factory_.get()));
  absl::StatusOr<std::vector<std::string>> statements =
      PrintDDLStatements(schema.get());

  EXPECT_THAT(statements, IsOkAndHolds(ElementsAre(
                              R"(CREATE TABLE T (
  col1 INT64,
  col2 STRING(MAX),
) PRIMARY KEY(col1))")));
}

TEST_F(SchemaTest, PostgreSQLPrintDDLStatementsTestChangeStreams) {
  SetPostgresqlDialect();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const backend::Schema> schema,
      test::CreateSchemaFromDDL({R"(
    CREATE TABLE t(
      col1 bigint primary key,
      col2 varchar
    )
  )",
                                 R"(
    CREATE CHANGE STREAM change_stream FOR t
  )"},
                                type_factory_.get(),
                                /*proto_descriptor_bytes=*/"",
                                database_api::DatabaseDialect::POSTGRESQL));

  EXPECT_THAT(PrintDDLStatements(schema.get()),
              IsOkAndHolds(ElementsAre(
                  R"(CREATE TABLE t (
  col1 bigint NOT NULL,
  col2 character varying,
  PRIMARY KEY(col1)
))",
                  R"(CREATE CHANGE STREAM change_stream
FOR t)")));
}

TEST_F(SchemaTest, PrintDDLStatementsTestSequences) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const backend::Schema> schema,
                       test::CreateSchemaWithOneSequence(type_factory_.get()));

  EXPECT_THAT(PrintDDLStatements(schema.get()),
              IsOkAndHolds(ElementsAre(
                  R"(CREATE SEQUENCE myseq OPTIONS (
  sequence_kind = 'bit_reversed_positive',
  skip_range_min = 1,
  skip_range_max = 1000))",
                  R"(CREATE TABLE test_table (
  int64_col INT64 NOT NULL DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE myseq)),
  string_col STRING(MAX),
) PRIMARY KEY(int64_col))",
                  R"(CREATE TABLE test_id_table (
  int64_col INT64 NOT NULL GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE),
  string_col STRING(MAX),
) PRIMARY KEY(int64_col))")));
}

TEST_F(SchemaTest, PostgreSQLPrintDDLStatementsTestSequences) {
  SetPostgresqlDialect();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const backend::Schema> schema,
      test::CreateSchemaWithOneSequence(
          type_factory_.get(), database_api::DatabaseDialect::POSTGRESQL));

  EXPECT_THAT(
      PrintDDLStatements(schema.get()),
      IsOkAndHolds(ElementsAre("CREATE SEQUENCE myseq BIT_REVERSED_POSITIVE "
                               "START COUNTER WITH 1 SKIP RANGE 1 1000",
                               R"(CREATE TABLE test_table (
  int64_col bigint DEFAULT nextval('myseq'::text) NOT NULL,
  string_col character varying,
  PRIMARY KEY(int64_col)
))",
                               "CREATE TABLE test_id_table (\n"
                               "  int64_col bigint GENERATED BY DEFAULT AS "
                               "IDENTITY (BIT_REVERSED_POSITIVE) NOT NULL,\n"
                               "  string_col character varying,\n"
                               "  PRIMARY KEY(int64_col)\n)")));
}

TEST_F(SchemaTest, PostgreSQLPrintDDLStatementsTestArrays) {
  SetPostgresqlDialect();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const backend::Schema> schema,
      test::CreateSchemaFromDDL({R"(
CREATE TABLE base (
  key bigint,
  bool_array bool[] NOT NULL,
  int_array bigint[4],
  double_array float8[],
  str_array varchar(256)[],
  byte_array bytea[],
  timestamp_array timestamptz[],
  date_array date[],
  PRIMARY KEY (key)
);
  )"},
                                type_factory_.get(),
                                /*proto_descriptor_bytes=*/"",
                                database_api::DatabaseDialect::POSTGRESQL));

  EXPECT_THAT(PrintDDLStatements(schema.get()),
              IsOkAndHolds(ElementsAre(R"(CREATE TABLE base (
  key bigint NOT NULL,
  bool_array boolean[] NOT NULL,
  int_array bigint[],
  double_array double precision[],
  str_array character varying(256)[],
  byte_array bytea[],
  timestamp_array timestamp with time zone[],
  date_array date[],
  PRIMARY KEY(key)
))")));
}

TEST_F(SchemaTest, PostgreSQLPrintDDLStatementsTestTTL) {
  SetPostgresqlDialect();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const backend::Schema> schema,
      test::CreateSchemaFromDDL({R"(
CREATE TABLE vanishing_data (
  id bigint PRIMARY KEY,
  shadow_date timestamptz
) TTL INTERVAL '5 DAYS' ON shadow_date
  )",
                                 R"(
ALTER TABLE vanishing_data ALTER TTL INTERVAL '3 WEEKS 2 DAYS' ON shadow_date
                            )"},
                                type_factory_.get(),
                                /*proto_descriptor_bytes=*/"",
                                database_api::DatabaseDialect::POSTGRESQL));

  EXPECT_THAT(PrintDDLStatements(schema.get()),
              IsOkAndHolds(ElementsAre(R"(CREATE TABLE vanishing_data (
  id bigint NOT NULL,
  shadow_date timestamp with time zone,
  PRIMARY KEY(id)
) TTL INTERVAL '3 WEEKS 2 DAYS' ON shadow_date)")));
}

TEST_F(SchemaTest, ZetaSQLPrintDDLStatementsIdentityColumns) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const backend::Schema> schema,
                       test::CreateSchemaFromDDL(
                           {"ALTER DATABASE db SET OPTIONS "
                            "(default_sequence_kind = 'bit_reversed_positive')",
                            R"(
              CREATE TABLE T (
                id INT64 GENERATED BY DEFAULT AS IDENTITY (
                  SKIP RANGE 4000, 5000 START COUNTER WITH 3000),
                non_key_col INT64 GENERATED BY DEFAULT AS IDENTITY (
                  BIT_REVERSED_POSITIVE SKIP RANGE 2000, 3000
                  START COUNTER WITH 1000),
                value INT64,
              ) PRIMARY KEY(id)
            )"},
                           type_factory_.get()));
  absl::StatusOr<std::vector<std::string>> statements =
      PrintDDLStatements(schema.get());

  EXPECT_THAT(
      statements,
      IsOkAndHolds(ElementsAre(
          R"(ALTER DATABASE db SET OPTIONS (default_sequence_kind = 'bit_reversed_positive'))",
          R"(CREATE TABLE T (
  id INT64 GENERATED BY DEFAULT AS IDENTITY (SKIP RANGE 4000, 5000 START COUNTER WITH 3000),
  non_key_col INT64 GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE SKIP RANGE 2000, 3000 START COUNTER WITH 1000),
  value INT64,
) PRIMARY KEY(id))")));
}

TEST_F(SchemaTest, ZetaSQLPrintDDLStatementsSequenceClause) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const backend::Schema> schema,
                       test::CreateSchemaFromDDL(
                           {"ALTER DATABASE db SET OPTIONS "
                            "(default_sequence_kind = 'bit_reversed_positive')",
                            R"(
              CREATE SEQUENCE myseq1
            )",
                            R"(
              CREATE SEQUENCE myseq2 BIT_REVERSED_POSITIVE SKIP RANGE 2000, 3000 START COUNTER WITH 1000
            )",
                            R"(
              CREATE SEQUENCE myseq3 SKIP RANGE 2000, 3000 START COUNTER WITH 1000
            )",
                            R"(
              CREATE SEQUENCE myseq4 OPTIONS (
                skip_range_min = 1,
                skip_range_max = 1000,
                start_with_counter = 2000
              ))"},
                           type_factory_.get()));
  absl::StatusOr<std::vector<std::string>> statements =
      PrintDDLStatements(schema.get());

  EXPECT_THAT(
      statements,
      IsOkAndHolds(ElementsAre(
          "ALTER DATABASE db SET OPTIONS "
          "(default_sequence_kind = 'bit_reversed_positive')",
          R"(CREATE SEQUENCE myseq1)",
          R"(CREATE SEQUENCE myseq2 BIT_REVERSED_POSITIVE SKIP RANGE 2000, 3000 START COUNTER WITH 1000)",
          R"(CREATE SEQUENCE myseq3 SKIP RANGE 2000, 3000 START COUNTER WITH 1000)",
          R"(CREATE SEQUENCE myseq4 OPTIONS (
  skip_range_min = 1,
  skip_range_max = 1000,
  start_with_counter = 2000))")));
}

TEST_F(SchemaTest, ZetaSQLPrintDDLStatementsTestCreateLocalityGroup) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const backend::Schema> schema,
      test::CreateSchemaFromDDL(
          {R"(CREATE LOCALITY GROUP lg OPTIONS ( storage = 'ssd' ))"},
          type_factory_.get()));
  absl::StatusOr<std::vector<std::string>> statements =
      PrintDDLStatements(schema.get());

  EXPECT_THAT(statements,
              IsOkAndHolds(ElementsAre(
                  R"(CREATE LOCALITY GROUP lg OPTIONS ( storage = 'ssd' ))")));
}

TEST_F(SchemaTest, ZetaSQLPrintDDLStatementsTestAlterLocalityGroup) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const backend::Schema> schema,
      test::CreateSchemaFromDDL(
          {R"(CREATE LOCALITY GROUP lg)",
           R"(AlTER LOCALITY GROUP lg SET OPTIONS ( storage = 'ssd', ssd_to_hdd_spill_timespan = '10m' ))"},
          type_factory_.get()));
  absl::StatusOr<std::vector<std::string>> statements =
      PrintDDLStatements(schema.get());

  EXPECT_THAT(
      statements,
      IsOkAndHolds(ElementsAre(
          R"(CREATE LOCALITY GROUP lg OPTIONS ( storage = 'ssd', ssd_to_hdd_spill_timespan = '10m' ))")));
}

TEST_F(SchemaTest, ZetaSQLPrintDDLStatementsTestDropLocalityGroup) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const backend::Schema> schema,
      test::CreateSchemaFromDDL(
          {R"(CREATE LOCALITY GROUP lg)", R"(CREATE LOCALITY GROUP lg2)",
           R"(DROP LOCALITY GROUP lg)"},
          type_factory_.get()));
  absl::StatusOr<std::vector<std::string>> statements =
      PrintDDLStatements(schema.get());

  EXPECT_THAT(statements,
              IsOkAndHolds(ElementsAre(R"(CREATE LOCALITY GROUP lg2)")));
}

TEST_F(SchemaTest, PrintDDLStatementsTestUDFsWithDependencies) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const backend::Schema> schema,
      test::CreateSchemaFromDDL(
          {R"(CREATE FUNCTION udf_base(x INT64 DEFAULT 1) RETURNS INT64 SQL SECURITY INVOKER AS (x + 1))",
           R"(CREATE FUNCTION udf_increment(x INT64) RETURNS INT64 SQL SECURITY INVOKER AS (udf_base(x) * 2))",
           R"(CREATE TABLE T1 (id INT64 NOT NULL, val INT64, val_generated INT64 AS (udf_increment(val)) STORED) PRIMARY KEY(id))",
           R"(CREATE FUNCTION udf_complex(x INT64 DEFAULT NULL) RETURNS INT64 SQL SECURITY INVOKER AS (udf_increment(x) + 3))",
           R"(CREATE TABLE T2 (id INT64 NOT NULL, val INT64 DEFAULT (udf_complex(5))) PRIMARY KEY(id))",
           R"(CREATE VIEW V1 SQL SECURITY INVOKER AS SELECT T1.id, udf_base(T1.val) AS val_plus_one FROM T1)",
           R"(CREATE VIEW V2 SQL SECURITY INVOKER AS SELECT V1.id, V1.val_plus_one, udf_complex(V1.val_plus_one) AS complex_val FROM V1)"},
          type_factory_.get()));

  EXPECT_THAT(
      PrintDDLStatements(schema.get()),
      IsOkAndHolds(ElementsAre(
          "CREATE FUNCTION udf_base(x INT64 DEFAULT 1) RETURNS INT64 SQL "
          "SECURITY INVOKER AS (x + 1)",
          "CREATE FUNCTION udf_increment(x INT64) RETURNS INT64 SQL SECURITY "
          "INVOKER AS (udf_base(x) * 2)",
          "CREATE TABLE T1 (\n  id INT64 NOT NULL,\n  val INT64,\n  "
          "val_generated INT64 AS (udf_increment(val)) STORED,\n) PRIMARY "
          "KEY(id)",
          "CREATE FUNCTION udf_complex(x INT64 DEFAULT NULL) RETURNS INT64 SQL "
          "SECURITY INVOKER AS (udf_increment(x) + 3)",
          "CREATE TABLE T2 (\n  id INT64 NOT NULL,\n  val INT64 DEFAULT "
          "(udf_complex(5)),\n) PRIMARY KEY(id)",
          "CREATE VIEW V1 SQL SECURITY INVOKER AS SELECT T1.id, "
          "udf_base(T1.val) AS val_plus_one FROM T1",
          "CREATE VIEW V2 SQL SECURITY INVOKER AS SELECT V1.id, "
          "V1.val_plus_one, udf_complex(V1.val_plus_one) AS complex_val FROM "
          "V1")));
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
