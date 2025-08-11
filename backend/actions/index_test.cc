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

#include "backend/actions/index.h"

#include <memory>
#include <queue>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/memory/memory.h"
#include "absl/types/variant.h"
#include "backend/actions/context.h"
#include "backend/actions/ops.h"
#include "tests/common/actions.h"
#include "tests/common/schema_constructor.h"
#include "tests/common/scoped_feature_flags_setter.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

using ::google::spanner::emulator::test::ScopedEmulatorFeatureFlagsSetter;
using zetasql::types::StringType;
using zetasql::values::Int64;
using zetasql::values::Null;
using zetasql::values::String;

class IndexTest : public test::ActionsTest {
 public:
  IndexTest()
      : schema_(emulator::test::CreateSchemaFromDDL(
                    {
                        R"(
                            CREATE TABLE TestTable (
                              int64_col INT64 NOT NULL,
                              string_col STRING(MAX),
                              another_string_col STRING(MAX)
                            ) PRIMARY KEY (int64_col)
                          )",
                        R"(
                            CREATE UNIQUE NULL_FILTERED INDEX TestIndex ON
                            TestTable(string_col DESC)
                            STORING(another_string_col)
                    )"},
                    &type_factory_)
                    .value()),
        table_(schema_->FindTable("TestTable")),
        base_columns_(table_->columns()),
        index_(schema_->FindIndex("TestIndex")),
        index_columns_(index_->index_data_table()->columns()),
        effector_(std::make_unique<IndexEffector>(index_)) {}

 protected:
  // Test components.
  zetasql::TypeFactory type_factory_;
  std::unique_ptr<const Schema> schema_;

  // Test variables.
  const Table* table_;
  absl::Span<const Column* const> base_columns_;
  const Index* index_;
  absl::Span<const Column* const> index_columns_;
  std::unique_ptr<Effector> effector_;
};

TEST_F(IndexTest, DeleteCascadesToIndexEntry) {
  // Add row in base table & index.
  ZETASQL_EXPECT_OK(store()->Insert(table_, Key({Int64(1)}), base_columns_,
                            {Int64(1), String("value"), String("value2")}));
  ZETASQL_EXPECT_OK(store()->Insert(index_->index_data_table(),
                            Key({String("value"), Int64(1)}), index_columns_,
                            {Int64(1), String("value"), String("value2")}));

  // Delete base table entry.
  ZETASQL_EXPECT_OK(effector_->Effect(ctx(), Delete(table_, Key({Int64(1)}))));
  // Verify index delete is added to the transaction buffer.
  ASSERT_EQ(effects_buffer()->ops_queue()->size(), 1);
  EXPECT_THAT(
      effects_buffer()->ops_queue()->front(),
      testing::VariantWith<DeleteOp>(DeleteOp{
          index_->index_data_table(), Key({String("value"), Int64(1)})}));
}

TEST_F(IndexTest, InsertCascadesToIndexEntry) {
  // Insert base table entry.
  ZETASQL_EXPECT_OK(effector_->Effect(
      ctx(), Insert(table_, Key({Int64(1)}), base_columns_,
                    {Int64(1), String("value"), String("value2")})));

  // Verify index entry is added to the transaction buffer.
  ASSERT_EQ(effects_buffer()->ops_queue()->size(), 1);
  EXPECT_THAT(effects_buffer()->ops_queue()->front(),
              testing::VariantWith<InsertOp>(
                  InsertOp{index_->index_data_table(),
                           Key({String("value"), Int64(1)}),
                           {index_columns_.begin(), index_columns_.end()},
                           {String("value"), Int64(1), String("value2")}}));
}

TEST_F(IndexTest, InsertNullDoesNotCascadeToIndexEntry) {
  // Insert base table entry.
  ZETASQL_EXPECT_OK(effector_->Effect(
      ctx(), Insert(table_, Key({Int64(1)}), {table_->FindColumn("int64_col")},
                    {Int64(1)})));

  // Verify index entry is not added to the transaction buffer.
  ASSERT_EQ(effects_buffer()->ops_queue()->size(), 0);
}

TEST_F(IndexTest, UpdateColumnOnlyInsertsNewEntry) {
  // Add row in base table. This is NULL_FILTERED from the index since
  // string_col is NULL value.
  ZETASQL_EXPECT_OK(store()->Insert(table_, Key({Int64(1)}), base_columns_,
                            {Int64(1), Null(StringType()), String("value2")}));

  // Update base table entry.
  ZETASQL_EXPECT_OK(effector_->Effect(
      ctx(), Update(table_, Key({Int64(1)}), {table_->FindColumn("string_col")},
                    {String("new-value")})));

  // Verify new entry is added to the transaction buffer.
  ASSERT_EQ(effects_buffer()->ops_queue()->size(), 1);
  EXPECT_THAT(effects_buffer()->ops_queue()->front(),
              testing::VariantWith<InsertOp>(
                  InsertOp{index_->index_data_table(),
                           Key({String("new-value"), Int64(1)}),
                           {index_columns_.begin(), index_columns_.end()},
                           {String("new-value"), Int64(1), String("value2")}}));
}

TEST_F(IndexTest, UpdateColumnOnlyDeletesOldEntry) {
  // Add row in base table and index.
  ZETASQL_EXPECT_OK(store()->Insert(table_, Key({Int64(1)}), base_columns_,
                            {Int64(1), String("value"), String("value2")}));
  ZETASQL_EXPECT_OK(store()->Insert(index_->index_data_table(),
                            Key({String("value"), Int64(1)}), index_columns_,
                            {Int64(1), String("value"), String("value2")}));

  // Update base table entry.
  ZETASQL_EXPECT_OK(effector_->Effect(
      ctx(), Update(table_, Key({Int64(1)}), {table_->FindColumn("string_col")},
                    {Null(StringType())})));

  // Verify old entry is deleted, though new entry will be NULL_FILTERED and not
  // in the transaction buffer.
  ASSERT_EQ(effects_buffer()->ops_queue()->size(), 1);
  EXPECT_THAT(
      effects_buffer()->ops_queue()->front(),
      testing::VariantWith<DeleteOp>(DeleteOp{
          index_->index_data_table(), Key({String("value"), Int64(1)})}));
}

TEST_F(IndexTest, UpdateCascadesToIndexEntry) {
  // Add row in base table & index.
  ZETASQL_EXPECT_OK(store()->Insert(table_, Key({Int64(1)}), base_columns_,
                            {Int64(1), String("value"), String("value2")}));
  ZETASQL_EXPECT_OK(store()->Insert(index_->index_data_table(),
                            Key({String("value"), Int64(1)}), index_columns_,
                            {Int64(1), String("value"), String("value2")}));

  // Update base table entry.
  ZETASQL_EXPECT_OK(effector_->Effect(
      ctx(), Update(table_, Key({Int64(1)}), {table_->FindColumn("string_col")},
                    {String("new-value")})));

  // Verify old index entry is deleted and new entry is added to the transaction
  // buffer.
  ASSERT_EQ(effects_buffer()->ops_queue()->size(), 2);
  EXPECT_THAT(
      effects_buffer()->ops_queue()->front(),
      testing::VariantWith<DeleteOp>(DeleteOp{
          index_->index_data_table(), Key({String("value"), Int64(1)})}));
  effects_buffer()->ops_queue()->pop();

  EXPECT_THAT(effects_buffer()->ops_queue()->front(),
              testing::VariantWith<InsertOp>(
                  InsertOp{index_->index_data_table(),
                           Key({String("new-value"), Int64(1)}),
                           {index_columns_.begin(), index_columns_.end()},
                           {String("new-value"), Int64(1), String("value2")}}));
}

class RemoteIndexTest : public test::ActionsTest {
 public:
  RemoteIndexTest()
      : flag_setter_({.enable_interleave_in = true}),
        schema_(emulator::test::CreateSchemaFromDDL(
                    {
                        R"(
                            CREATE TABLE ParentTable (
                              pk STRING(MAX),
                              value INT64
                            ) PRIMARY KEY (pk)
                          )",
                        R"(
                            CREATE TABLE TestTable (
                              int64_col INT64 NOT NULL,
                              string_col STRING(MAX),
                              another_string_col STRING(MAX)
                            ) PRIMARY KEY (int64_col)
                          )",
                        R"(
                            CREATE INDEX RemoteIndex ON TestTable(string_col)
                            STORING(another_string_col),
                            INTERLEAVE IN ParentTable
                    )"},
                    &type_factory_)
                    .value()),
        parent_table_(schema_->FindTable("ParentTable")),
        parent_columns_(parent_table_->columns()),
        table_(schema_->FindTable("TestTable")),
        base_columns_(table_->columns()),
        index_(schema_->FindIndex("RemoteIndex")),
        index_columns_(index_->index_data_table()->columns()),
        effector_(std::make_unique<IndexEffector>(index_)) {}

 protected:
  // Test components.
  zetasql::TypeFactory type_factory_;
  const ScopedEmulatorFeatureFlagsSetter flag_setter_;
  std::unique_ptr<const Schema> schema_;

  // Test variables.
  const Table* parent_table_;
  absl::Span<const Column* const> parent_columns_;
  const Table* table_;
  absl::Span<const Column* const> base_columns_;
  const Index* index_;
  absl::Span<const Column* const> index_columns_;
  std::unique_ptr<Effector> effector_;
};

TEST_F(RemoteIndexTest, BaseTableDeleteCascadesToIndexEntry) {
  // Add row in base table & index.
  ZETASQL_EXPECT_OK(store()->Insert(parent_table_, Key({String("value")}),
                            parent_columns_, {String("value"), Int64(11)}));
  ZETASQL_EXPECT_OK(store()->Insert(table_, Key({Int64(1)}), base_columns_,
                            {Int64(1), String("value"), String("value2")}));
  ZETASQL_EXPECT_OK(store()->Insert(index_->index_data_table(),
                            Key({String("value"), Int64(1)}), index_columns_,
                            {Int64(1), String("value"), String("value2")}));
  // Delete base table entry.
  ZETASQL_EXPECT_OK(effector_->Effect(ctx(), Delete(table_, Key({Int64(1)}))));
  // Verify index delete is added to the transaction buffer.
  ASSERT_EQ(effects_buffer()->ops_queue()->size(), 1);
  EXPECT_THAT(
      effects_buffer()->ops_queue()->front(),
      testing::VariantWith<DeleteOp>(DeleteOp{
          index_->index_data_table(), Key({String("value"), Int64(1)})}));
}

TEST_F(RemoteIndexTest, BaseTableInsertCascadesToIndexEntry) {
  // Add row in parent table.
  ZETASQL_EXPECT_OK(store()->Insert(parent_table_, Key({String("value")}),
                            parent_columns_, {String("value"), Int64(11)}));

  // Insert base table entry.
  ZETASQL_EXPECT_OK(effector_->Effect(
      ctx(), Insert(table_, Key({Int64(1)}), base_columns_,
                    {Int64(1), String("value"), String("value2")})));

  // Verify index entry is added to the transaction buffer.
  ASSERT_EQ(effects_buffer()->ops_queue()->size(), 1);
  EXPECT_THAT(effects_buffer()->ops_queue()->front(),
              testing::VariantWith<InsertOp>(
                  InsertOp{index_->index_data_table(),
                           Key({String("value"), Int64(1)}),
                           {index_columns_.begin(), index_columns_.end()},
                           {String("value"), Int64(1), String("value2")}}));
}

TEST_F(RemoteIndexTest, BaseTableInsertCascadeToIndexEntryWithoutParentRow) {
  // Insert base table entry.
  ZETASQL_EXPECT_OK(effector_->Effect(
      ctx(), Insert(table_, Key({Int64(1)}), base_columns_,
                    {Int64(1), String("value"), String("value2")})));

  // Verify index entry is added to the transaction buffer.
  ASSERT_EQ(effects_buffer()->ops_queue()->size(), 1);
  EXPECT_THAT(effects_buffer()->ops_queue()->front(),
              testing::VariantWith<InsertOp>(
                  InsertOp{index_->index_data_table(),
                           Key({String("value"), Int64(1)}),
                           {index_columns_.begin(), index_columns_.end()},
                           {String("value"), Int64(1), String("value2")}}));
}

TEST_F(RemoteIndexTest, BaseTableUpdateCascadesToIndexEntry) {
  // Add row in parent table.
  ZETASQL_EXPECT_OK(store()->Insert(parent_table_, Key({String("value")}),
                            parent_columns_, {String("value"), Int64(11)}));
  // Add row in base table & index.
  ZETASQL_EXPECT_OK(store()->Insert(table_, Key({Int64(1)}), base_columns_,
                            {Int64(1), String("value"), String("value2")}));
  ZETASQL_EXPECT_OK(store()->Insert(index_->index_data_table(),
                            Key({String("value"), Int64(1)}), index_columns_,
                            {Int64(1), String("value"), String("value2")}));

  // Update base table entry.
  ZETASQL_EXPECT_OK(effector_->Effect(
      ctx(), Update(table_, Key({Int64(1)}), {table_->FindColumn("string_col")},
                    {String("new-value")})));

  // Verify old index entry is deleted and new entry is added to the transaction
  // buffer.
  ASSERT_EQ(effects_buffer()->ops_queue()->size(), 2);
  EXPECT_THAT(
      effects_buffer()->ops_queue()->front(),
      testing::VariantWith<DeleteOp>(DeleteOp{
          index_->index_data_table(), Key({String("value"), Int64(1)})}));
  effects_buffer()->ops_queue()->pop();

  EXPECT_THAT(effects_buffer()->ops_queue()->front(),
              testing::VariantWith<InsertOp>(
                  InsertOp{index_->index_data_table(),
                           Key({String("new-value"), Int64(1)}),
                           {index_columns_.begin(), index_columns_.end()},
                           {String("new-value"), Int64(1), String("value2")}}));
}

TEST_F(RemoteIndexTest, CanDeleteParentRowIfIndexEntryExists) {
  // Add row in parent table.
  ZETASQL_EXPECT_OK(store()->Insert(parent_table_, Key({String("value")}),
                            parent_columns_, {String("value"), Int64(11)}));
  // Add row in base table and index.
  ZETASQL_EXPECT_OK(store()->Insert(table_, Key({Int64(1)}), base_columns_,
                            {Int64(1), String("value"), String("value2")}));
  ZETASQL_EXPECT_OK(store()->Insert(index_->index_data_table(),
                            Key({String("value"), Int64(1)}), index_columns_,
                            {Int64(1), String("value"), String("value2")}));

  // Delete parent table entry.
  ZETASQL_EXPECT_OK(store()->Delete(parent_table_, Key({String("value")})));

  // Verify the delete is not cascaded to the index.
  ASSERT_EQ(effects_buffer()->ops_queue()->size(), 0);
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
