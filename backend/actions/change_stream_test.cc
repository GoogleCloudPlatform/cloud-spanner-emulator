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

#include "backend/actions/change_stream.h"

#include <cstdint>
#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "zetasql/public/json_value.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/log/log.h"
#include "absl/types/span.h"
#include "absl/types/variant.h"
#include "backend/actions/action.h"
#include "backend/actions/context.h"
#include "backend/actions/ops.h"
#include "backend/datamodel/key.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "tests/common/actions.h"
#include "tests/common/schema_constructor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

using zetasql::values::Bool;
using zetasql::values::Int64;
using zetasql::values::String;

class ChangeStreamTest : public test::ActionsTest {
 public:
  ChangeStreamTest()
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
                            CREATE TABLE TestTable2 (
                              int64_col INT64 NOT NULL,
                              string_col STRING(MAX),
                              another_string_col STRING(MAX)
                            ) PRIMARY KEY (int64_col)
                          )",
                        R"(
                            CREATE CHANGE STREAM ChangeStream_TestTable FOR ALL OPTIONS ( value_capture_type = 'NEW_VALUES' )
                    )",
                        R"(
                            CREATE CHANGE STREAM ChangeStream_TestTable2 FOR TestTable2(string_col) OPTIONS ( value_capture_type = 'NEW_VALUES' )
                    )",
                        R"(
                            CREATE CHANGE STREAM ChangeStream_TestTable3 FOR TestTable2() OPTIONS ( value_capture_type = 'NEW_VALUES' )
                    )"},
                    &type_factory_)
                    .value()),
        table_(schema_->FindTable("TestTable")),
        table2_(schema_->FindTable("TestTable2")),
        base_columns_(table_->columns()),
        base_columns2_(table2_->columns()),
        change_stream_(schema_->FindChangeStream("ChangeStream_TestTable")),
        change_stream2_(schema_->FindChangeStream("ChangeStream_TestTable2")),
        change_stream3_(schema_->FindChangeStream("ChangeStream_TestTable3")),
        effector_(std::make_unique<ChangeStreamEffector>(change_stream_)),
        effector2_(std::make_unique<ChangeStreamEffector>(change_stream2_)),
        effector3_(std::make_unique<ChangeStreamEffector>(change_stream3_)) {}

 protected:
  // Test components.
  zetasql::TypeFactory type_factory_;
  std::unique_ptr<const Schema> schema_;

  // Test variables.
  const Table* table_;
  const Table* table2_;
  const Table* table3_;
  absl::Span<const Column* const> base_columns_;
  absl::Span<const Column* const> base_columns2_;
  const ChangeStream* change_stream_;
  const ChangeStream* change_stream2_;
  const ChangeStream* change_stream3_;
  std::unique_ptr<Effector> effector_;
  std::unique_ptr<Effector> effector2_;
  std::unique_ptr<Effector> effector3_;
  std::vector<const Column*> another_string_col_ = {
      table2_->FindColumn("int64_col"),
      table2_->FindColumn("another_string_col")};
  std::vector<const Column*> string_col_ = {table2_->FindColumn("int64_col"),
                                            table2_->FindColumn("string_col")};
};

TEST_F(ChangeStreamTest, AddOneInsertOpAndCheckResultWriteOpContent) {
  // Populate partition table with the initial partition token
  std::vector<const Column*> columns;
  columns.push_back(change_stream_->change_stream_partition_table()
                        ->FindKeyColumn("partition_token")
                        ->column());
  columns.push_back(
      change_stream_->change_stream_partition_table()->FindColumn("end_time"));
  const std::vector<zetasql::Value> values = {
      zetasql::Value::String("11111"), zetasql::Value::NullTimestamp()};
  // Insert 1st partition
  ZETASQL_EXPECT_OK(store()->Insert(change_stream_->change_stream_partition_table(),
                            Key({String("11111")}), columns, values));
  // Insert 2nd partition
  const std::vector<zetasql::Value> values2 = {
      zetasql::Value::String("22222"), zetasql::Value::NullTimestamp()};
  ZETASQL_EXPECT_OK(store()->Insert(change_stream_->change_stream_partition_table(),
                            Key({String("22222")}), columns, values2));
  // Insert base table entry.
  ZETASQL_EXPECT_OK(effector_->Effect(
      ctx(), Insert(table_, Key({Int64(1)}), base_columns_,
                    {Int64(1), String("value"), String("value2")})));
  // Set number_of_records_in_transaction in each DataChangeRecord after
  // finishing processing all operations
  ctx()->change_stream_effects()->BuildMutation();
  // Verify change stream entry is added to the transaction buffer.
  ASSERT_EQ(change_stream_effects_buffer()->GetWriteOps().size(), 1);
  WriteOp op = change_stream_effects_buffer()->GetWriteOps()[0];
  // Verify the table of the received WriteOp
  ASSERT_EQ(TableOf(op), change_stream_->change_stream_data_table());
  // Verify the received WriteOp is InsertOp
  auto* operation = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation, nullptr);
  // Verify columns in the rebuilt InsertOp corresponds to columns in
  // change_stream_data_table
  ASSERT_EQ(operation->columns,
            change_stream_->change_stream_data_table()->columns());
  ASSERT_EQ(operation->columns.size(), 14);
  ASSERT_EQ(operation->values.size(), 14);
  // Verify values in the rebuilt InsertOp are correct
  // Verify partition_token
  ASSERT_EQ(operation->values[0], zetasql::Value::String("11111"));
  // Verify record_sequence
  ASSERT_EQ(operation->values[3], zetasql::Value(String("00000000")));
  // Verify is_last_record_in_transaction_in_partition
  ASSERT_EQ(operation->values[4], zetasql::Value(Bool(true)));
  // Verify table_name
  ASSERT_EQ(operation->values[5], zetasql::Value(String("TestTable")));
  // Verify column_types
  zetasql::Value col_types = operation->values[6];
  ASSERT_EQ(col_types.num_elements(), 3);
  for (int i = 0; i < col_types.num_elements(); ++i) {
    zetasql::JSONValueConstRef curr_col = col_types.element(i).json_value();
    ASSERT_EQ(curr_col.GetMembers().size(), 4);
    ASSERT_EQ(curr_col.HasMember("name"), true);
    ASSERT_EQ(curr_col.HasMember("is_primary_key"), true);
    ASSERT_EQ(curr_col.HasMember("ordinal_position"), true);
    ASSERT_EQ(curr_col.HasMember("type"), true);
  }
  ASSERT_EQ(col_types.element(0).json_value().GetMembers()[0].first,
            "is_primary_key");
  ASSERT_EQ(col_types.element(0).json_value().GetMembers()[0].second.ToString(),
            "true");
  ASSERT_EQ(col_types.element(0).json_value().GetMembers()[1].first, "name");
  ASSERT_EQ(col_types.element(0).json_value().GetMembers()[1].second.ToString(),
            "\"int64_col\"");
  ASSERT_EQ(col_types.element(0).json_value().GetMembers()[2].first,
            "ordinal_position");
  ASSERT_EQ(col_types.element(0).json_value().GetMembers()[2].second.ToString(),
            "1");
  ASSERT_EQ(col_types.element(0).json_value().GetMembers()[3].first, "type");
  ASSERT_EQ(
      col_types.element(0).json_value().GetMembers()[3].second.GetString(),
      "{\"code\":\"INT64\"}");
  // Verify mods
  zetasql::Value mods = operation->values[7];
  ASSERT_EQ(mods.num_elements(), 1);
  for (int i = 0; i < mods.num_elements(); ++i) {
    zetasql::JSONValueConstRef curr_mod = mods.element(i).json_value();
    ASSERT_EQ(curr_mod.GetMembers().size(), 3);
    ASSERT_EQ(curr_mod.GetMembers()[0].first, "keys");
    ASSERT_EQ(curr_mod.GetMembers()[1].first, "new_values");
    ASSERT_EQ(curr_mod.GetMembers()[2].first, "old_values");
  }

  ASSERT_EQ(mods.element(0).json_value().GetMembers()[0].second.GetString(),
            "{\"int64_col\":\"1\"}");
  ASSERT_EQ(mods.element(0).json_value().GetMembers()[1].second.GetString(),
            "{\"another_string_col\":\"value2\",\"string_col\":"
            "\"value\"}");
  ASSERT_EQ(mods.element(0).json_value().GetMembers()[2].second.GetString(),
            "{}");
  // Verify mod_type
  ASSERT_EQ(operation->values[8], zetasql::Value(String("INSERT")));
  // Verify value_capture_type
  ASSERT_EQ(operation->values[9], zetasql::Value(String("NEW_VALUES")));
  // Verify number_of_records_in_transaction
  ASSERT_EQ(operation->values[10], zetasql::Value(Int64(1)));
  // Verify number_of_partitions_in_transaction
  ASSERT_EQ(operation->values[11], zetasql::Value(Int64(1)));
  // Verify transaction_tag
  ASSERT_EQ(operation->values[12], zetasql::Value(String("")));
  // Verify is_system_transaction
  ASSERT_EQ(operation->values[13], zetasql::Value(Bool(false)));
}

TEST_F(ChangeStreamTest, AddTwoInsertForDiffSetCols) {
  // Populate partition table with the initial partition token
  std::vector<const Column*> columns;
  columns.push_back(change_stream2_->change_stream_partition_table()
                        ->FindKeyColumn("partition_token")
                        ->column());
  columns.push_back(
      change_stream2_->change_stream_partition_table()->FindColumn("end_time"));
  const std::vector<zetasql::Value> values = {
      zetasql::Value::String("11111"), zetasql::Value::NullTimestamp()};
  // Insert 1st partition
  ZETASQL_EXPECT_OK(store()->Insert(change_stream2_->change_stream_partition_table(),
                            Key({String("11111")}), columns, values));
  // Insert 1st base table entry. base_columns1 only contains the first two
  // columns of TestTable2.
  std::vector<const Column*> base_columns1 = {
      table2_->FindColumn("int64_col"), table2_->FindColumn("string_col")};
  ZETASQL_EXPECT_OK(
      effector2_->Effect(ctx(), Insert(table2_, Key({Int64(1)}), base_columns1,
                                       {Int64(1), String("value")})));
  // Insert 2nd base table entry. base_columns2_ contains all columns of
  // TestTable2.
  ZETASQL_EXPECT_OK(effector2_->Effect(
      ctx(), Insert(table2_, Key({Int64(2)}), base_columns2_,
                    {Int64(2), String("value"), String("value2")})));
  ctx()->change_stream_effects()->BuildMutation();
  // Verify change stream entry is added to the transaction buffer.
  ASSERT_EQ(change_stream_effects_buffer()->GetWriteOps().size(), 1);
}

TEST_F(ChangeStreamTest, AddTwoInsertForDiffSetNonKeyTrackedCols) {
  // Populate partition table with the initial partition token
  std::vector<const Column*> columns;
  columns.push_back(change_stream2_->change_stream_partition_table()
                        ->FindKeyColumn("partition_token")
                        ->column());
  columns.push_back(
      change_stream2_->change_stream_partition_table()->FindColumn("end_time"));
  const std::vector<zetasql::Value> values = {
      zetasql::Value::String("11111"), zetasql::Value::NullTimestamp()};
  // Insert 1st partition
  ZETASQL_EXPECT_OK(store()->Insert(change_stream2_->change_stream_partition_table(),
                            Key({String("11111")}), columns, values));
  // Insert 1st base table entry.
  std::vector<const Column*> base_columns1 = {table2_->FindColumn("int64_col")};
  ZETASQL_EXPECT_OK(effector2_->Effect(
      ctx(), Insert(table2_, Key({Int64(1)}), base_columns1, {Int64(1)})));
  // Insert 2nd base table entry
  std::vector<const Column*> base_columns2 = {
      table2_->FindColumn("int64_col"), table2_->FindColumn("string_col")};
  ZETASQL_EXPECT_OK(
      effector2_->Effect(ctx(), Insert(table2_, Key({Int64(2)}), base_columns2,
                                       {Int64(2), String("value")})));
  ctx()->change_stream_effects()->BuildMutation();
  // Verify change stream entry is added to the transaction buffer.
  ASSERT_EQ(change_stream_effects_buffer()->GetWriteOps().size(), 2);
}

// Add operations with different mod_types to the buffer and check if distinct
// DataChangeRecords are generated once mod_type changed.
// Insert, Insert, Update, Update, Insert, Delete, Delete -> 4 WriteOps
TEST_F(ChangeStreamTest, AddMultipleDataChangeRecordsToChangeStreamDataTable) {
  // Populate partition table with the initial partition token
  std::vector<const Column*> columns;
  columns.push_back(change_stream_->change_stream_partition_table()
                        ->FindKeyColumn("partition_token")
                        ->column());
  columns.push_back(
      change_stream_->change_stream_partition_table()->FindColumn("end_time"));
  const std::vector<zetasql::Value> values = {
      zetasql::Value::String("11111"), zetasql::Value::NullTimestamp()};
  ZETASQL_EXPECT_OK(store()->Insert(change_stream_->change_stream_partition_table(),
                            Key({String("11111")}), columns, values));
  // Insert base table entry.
  ZETASQL_EXPECT_OK(effector_->Effect(
      ctx(), Insert(table_, Key({Int64(1)}), base_columns_,
                    {Int64(1), String("value"), String("value2")})));
  ZETASQL_EXPECT_OK(effector_->Effect(
      ctx(), Insert(table_, Key({Int64(2)}), base_columns_,
                    {Int64(2), String("value_row2"), String("value2_row2")})));
  // Update base table entry.
  ZETASQL_EXPECT_OK(effector_->Effect(
      ctx(),
      Update(table_, Key({Int64(1)}), base_columns_,
             {Int64(1), String("updated_value"), String("updated_value2")})));
  ZETASQL_EXPECT_OK(
      effector_->Effect(ctx(), Update(table_, Key({Int64(2)}), base_columns_,
                                      {Int64(2), String("updated_value_row2"),
                                       String("updated_value2_row2")})));
  // Insert base table entry.
  ZETASQL_EXPECT_OK(effector_->Effect(
      ctx(), Insert(table_, Key({Int64(3)}), base_columns_,
                    {Int64(3), String("value_row3"), String("value2_row3")})));
  // Delete base table entry.
  ZETASQL_EXPECT_OK(effector_->Effect(ctx(), Delete(table_, Key({Int64(1)}))));
  ZETASQL_EXPECT_OK(effector_->Effect(ctx(), Delete(table_, Key({Int64(2)}))));
  // Set number_of_records_in_transaction in each DataChangeRecord after
  // finishing processing all operations
  ctx()->change_stream_effects()->BuildMutation();
  // Verify the number of change stream entries is added to the transaction
  // buffer.
  // Insert, Insert, Update, Update, Insert, Delete, Delete -> 4 WriteOps
  ASSERT_EQ(change_stream_effects_buffer()->GetWriteOps().size(), 4);

  WriteOp op = change_stream_effects_buffer()->GetWriteOps()[0];
  // Verify the first received WriteOp is InsertOp
  auto* operation = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation, nullptr);
  ASSERT_EQ(operation->values[8], zetasql::Value(String("INSERT")));
  ASSERT_EQ(operation->values[3], zetasql::Value(String("00000000")));
  ASSERT_EQ(operation->values[4],
            zetasql::Value(
                Bool(false /* is_last_record_in_transaction_in_partition */)));
  // Verify number_of_records_in_transaction
  ASSERT_EQ(operation->values[10], zetasql::Value(Int64(4)));
  // Verify the column_types of the 1st WriteOp (INSERT mod_type)
  zetasql::Value col_types = operation->values[6];
  ASSERT_EQ(col_types.num_elements(), 3);
  for (int i = 0; i < col_types.num_elements(); ++i) {
    zetasql::JSONValueConstRef curr_col = col_types.element(i).json_value();
    ASSERT_EQ(curr_col.GetMembers().size(), 4);
    ASSERT_EQ(curr_col.HasMember("name"), true);
    ASSERT_EQ(curr_col.HasMember("is_primary_key"), true);
    ASSERT_EQ(curr_col.HasMember("ordinal_position"), true);
    ASSERT_EQ(curr_col.HasMember("type"), true);
  }
  // Verify the mods of the 1st WriteOp (INSERT mod_type)
  zetasql::Value mods = operation->values[7];
  ASSERT_EQ(mods.num_elements(), 2);
  for (int i = 0; i < mods.num_elements(); ++i) {
    zetasql::JSONValueConstRef curr_mod = mods.element(i).json_value();
    ASSERT_EQ(curr_mod.GetMembers().size(), 3);
    ASSERT_EQ(curr_mod.GetMembers()[0].first, "keys");
    ASSERT_EQ(curr_mod.GetMembers()[1].first, "new_values");
    ASSERT_EQ(curr_mod.GetMembers()[2].first, "old_values");
  }
  ASSERT_EQ(mods.element(0).json_value().GetMembers()[0].second.GetString(),
            "{\"int64_col\":\"1\"}");
  ASSERT_EQ(mods.element(0).json_value().GetMembers()[1].second.GetString(),
            "{\"another_string_col\":\"value2\",\"string_"
            "col\":\"value\"}");
  ASSERT_EQ(mods.element(0).json_value().GetMembers()[2].second.GetString(),
            "{}");
  ASSERT_EQ(mods.element(1).json_value().GetMembers()[0].second.GetString(),
            "{\"int64_col\":\"2\"}");
  ASSERT_EQ(mods.element(1).json_value().GetMembers()[1].second.GetString(),
            "{\"another_string_col\":\"value2_row2\","
            "\"string_col\":\"value_row2\"}");
  ASSERT_EQ(mods.element(1).json_value().GetMembers()[2].second.GetString(),
            "{}");
  // Verify the second received WriteOp is UpdateOp
  op = change_stream_effects_buffer()->GetWriteOps()[1];
  auto* operation2 = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation2, nullptr);
  // Verify mod_type
  ASSERT_EQ(operation2->values[8], zetasql::Value(String("UPDATE")));
  ASSERT_EQ(operation->values[3], zetasql::Value(String("00000001")));
  ASSERT_EQ(operation2->values[4],
            zetasql::Value(
                Bool(false /* is_last_record_in_transaction_in_partition */)));
  // Verify number_of_records_in_transaction
  ASSERT_EQ(operation2->values[10], zetasql::Value(Int64(4)));
  // Verify the column_types of the 2nd WriteOp (UPDATE mod_type)
  col_types = operation2->values[6];
  ASSERT_EQ(col_types.num_elements(), 3);
  for (int i = 0; i < col_types.num_elements(); ++i) {
    zetasql::JSONValueConstRef curr_col = col_types.element(i).json_value();
    ASSERT_EQ(curr_col.GetMembers().size(), 4);
    ASSERT_EQ(curr_col.HasMember("name"), true);
    ASSERT_EQ(curr_col.HasMember("is_primary_key"), true);
    ASSERT_EQ(curr_col.HasMember("ordinal_position"), true);
    ASSERT_EQ(curr_col.HasMember("type"), true);
  }
  // Verify the mods of the 2nd WriteOp (UPDATE mod_type)
  mods = operation2->values[7];
  ASSERT_EQ(mods.num_elements(), 2);
  for (int i = 0; i < mods.num_elements(); ++i) {
    zetasql::JSONValueConstRef curr_mod = mods.element(i).json_value();
    ASSERT_EQ(curr_mod.GetMembers().size(), 3);
    ASSERT_EQ(curr_mod.GetMembers()[0].first, "keys");
    ASSERT_EQ(curr_mod.GetMembers()[1].first, "new_values");
    ASSERT_EQ(curr_mod.GetMembers()[2].first, "old_values");
  }
  ASSERT_EQ(mods.element(0).json_value().GetMembers()[0].second.GetString(),
            "{\"int64_col\":\"1\"}");
  ASSERT_EQ(mods.element(0).json_value().GetMembers()[1].second.GetString(),
            "{\"another_string_col\":\"updated_value2\",\"string_"
            "col\":\"updated_value\"}");
  ASSERT_EQ(mods.element(0).json_value().GetMembers()[2].second.GetString(),
            "{}");
  ASSERT_EQ(mods.element(1).json_value().GetMembers()[0].second.GetString(),
            "{\"int64_col\":\"2\"}");
  ASSERT_EQ(mods.element(1).json_value().GetMembers()[1].second.GetString(),
            "{\"another_string_col\":\"updated_value2_row2\","
            "\"string_col\":\"updated_value_row2\"}");
  ASSERT_EQ(mods.element(1).json_value().GetMembers()[2].second.GetString(),
            "{}");
  // Verify the 3rd received WriteOp is InsertOp
  op = change_stream_effects_buffer()->GetWriteOps()[2];
  auto* operation3 = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation3, nullptr);
  ASSERT_EQ(operation3->values[8], zetasql::Value(String("INSERT")));
  ASSERT_EQ(operation->values[3], zetasql::Value(String("00000002")));
  ASSERT_EQ(operation3->values[4],
            zetasql::Value(
                Bool(false /* is_last_record_in_transaction_in_partition */)));
  // Verify number_of_records_in_transaction
  ASSERT_EQ(operation3->values[10], zetasql::Value(Int64(4)));

  // Verify the 4th(last) received WriteOp is DeleteOp
  op = change_stream_effects_buffer()->GetWriteOps()[3];
  auto operation4 = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation4, nullptr);
  ASSERT_EQ(operation->values[3], zetasql::Value(String("00000003")));
  // Verify is_last_record_in_transaction_in_partition
  ASSERT_EQ(operation4->values[4], zetasql::Value(Bool(true)));
  ASSERT_EQ(operation4->values[4],
            zetasql::Value(
                Bool(true /* is_last_record_in_transaction_in_partition */)));
  // Verify number_of_records_in_transaction
  ASSERT_EQ(operation4->values[10], zetasql::Value(Int64(4)));

  // Verify the column_types of the 4th WriteOp (DELETE mod_type)
  col_types = operation4->values[6];
  ASSERT_EQ(col_types.num_elements(), 1);
  // Verify the mods of the 4th WriteOp (DELETE mod_type)
  mods = operation->values[7];
  ASSERT_EQ(mods.num_elements(), 2);
  for (int i = 0; i < mods.num_elements(); ++i) {
    zetasql::JSONValueConstRef curr_mod = mods.element(i).json_value();
    ASSERT_EQ(curr_mod.GetMembers().size(), 3);
    ASSERT_EQ(curr_mod.GetMembers()[0].first, "keys");
    ASSERT_EQ(curr_mod.GetMembers()[1].first, "new_values");
    ASSERT_EQ(curr_mod.GetMembers()[2].first, "old_values");
  }
  ASSERT_EQ(mods.element(0).json_value().GetMembers()[0].second.GetString(),
            "{\"int64_col\":\"1\"}");
  ASSERT_EQ(mods.element(0).json_value().GetMembers()[1].second.GetString(),
            "{}");
  ASSERT_EQ(mods.element(0).json_value().GetMembers()[2].second.GetString(),
            "{}");
  ASSERT_EQ(mods.element(1).json_value().GetMembers()[0].second.GetString(),
            "{\"int64_col\":\"2\"}");
  ASSERT_EQ(mods.element(1).json_value().GetMembers()[1].second.GetString(),
            "{}");
  ASSERT_EQ(mods.element(1).json_value().GetMembers()[2].second.GetString(),
            "{}");
}

// Insert to table1, Insert to table2, Insert to table1 -> 3 DataChangeRecords
TEST_F(ChangeStreamTest, AddWriteOpForDiffUserTablesForSameChangeStream) {
  // Populate partition table with the initial partition token
  std::vector<const Column*> columns;
  columns.push_back(change_stream_->change_stream_partition_table()
                        ->FindKeyColumn("partition_token")
                        ->column());
  columns.push_back(
      change_stream_->change_stream_partition_table()->FindColumn("end_time"));
  const std::vector<zetasql::Value> values = {
      zetasql::Value::String("11111"), zetasql::Value::NullTimestamp()};
  ZETASQL_EXPECT_OK(store()->Insert(change_stream_->change_stream_partition_table(),
                            Key({String("11111")}), columns, values));

  // Insert base table entry to TestTable.
  ZETASQL_EXPECT_OK(effector_->Effect(
      ctx(), Insert(table_, Key({Int64(1)}), base_columns_,
                    {Int64(1), String("value"), String("value2")})));
  // Insert base table entry to TestTable2.
  ZETASQL_EXPECT_OK(effector_->Effect(
      ctx(), Insert(table2_, Key({Int64(1)}), base_columns2_,
                    {Int64(1), String("value"), String("value2")})));
  // Insert base table entry to TestTable.
  ZETASQL_EXPECT_OK(effector_->Effect(
      ctx(), Insert(table_, Key({Int64(2)}), base_columns_,
                    {Int64(2), String("value_row2"), String("value2_row2")})));

  // Set number_of_records_in_transaction in each DataChangeRecord after
  // finishing processing all operations
  ctx()->change_stream_effects()->BuildMutation();
  // Verify the number rebuilt WriteOps added to the transaction
  // buffer.
  ASSERT_EQ(change_stream_effects_buffer()->GetWriteOps().size(), 3);
  WriteOp op = change_stream_effects_buffer()->GetWriteOps()[0];
  InsertOp* insert_op = std::get_if<InsertOp>(&op);
  ASSERT_NE(insert_op, nullptr);
  EXPECT_EQ(insert_op->values[5], zetasql::Value(String("TestTable")));
  op = change_stream_effects_buffer()->GetWriteOps()[1];
  insert_op = std::get_if<InsertOp>(&op);
  ASSERT_NE(insert_op, nullptr);
  EXPECT_EQ(insert_op->values[5], zetasql::Value(String("TestTable2")));
  op = change_stream_effects_buffer()->GetWriteOps()[2];
  insert_op = std::get_if<InsertOp>(&op);
  ASSERT_NE(insert_op, nullptr);
  EXPECT_EQ(insert_op->values[5], zetasql::Value(String("TestTable")));
}

// Update table1(another_string_col), Update table1(string_col), Update
// table1(another_string_col) -> 3 DataChangeRecords
TEST_F(ChangeStreamTest, AddWriteOpForDiffNonKeyColsForSameChangeStream) {
  // Populate partition table with the initial partition token
  std::vector<const Column*> columns;
  columns.push_back(change_stream_->change_stream_partition_table()
                        ->FindKeyColumn("partition_token")
                        ->column());
  columns.push_back(
      change_stream_->change_stream_partition_table()->FindColumn("end_time"));
  const std::vector<zetasql::Value> values = {
      zetasql::Value::String("11111"), zetasql::Value::NullTimestamp()};
  ZETASQL_EXPECT_OK(store()->Insert(change_stream_->change_stream_partition_table(),
                            Key({String("11111")}), columns, values));
  // Insert base table entry to TestTable.
  ZETASQL_EXPECT_OK(effector_->Effect(
      ctx(), Update(table2_, Key({Int64(1)}), another_string_col_,
                    {Int64(1), String("another_string_value1")})));
  // Insert base table entry to TestTable2.
  ZETASQL_EXPECT_OK(
      effector_->Effect(ctx(), Update(table2_, Key({Int64(1)}), string_col_,
                                      {Int64(1), String("string_value1")})));
  // Insert base table entry to TestTable.
  ZETASQL_EXPECT_OK(effector_->Effect(
      ctx(), Update(table2_, Key({Int64(2)}), another_string_col_,
                    {Int64(2), String("another_string_value2")})));
  // Set number_of_records_in_transaction in each DataChangeRecord after
  // finishing processing all operations
  ctx()->change_stream_effects()->BuildMutation();
  // Verify the number of rebuilt WriteOps added to the transaction
  // buffer.
  ASSERT_EQ(change_stream_effects_buffer()->GetWriteOps().size(), 3);
}

TEST_F(ChangeStreamTest, AddWriteOpForDifferentChangeStreams) {
  // Populate ChangeStream_TestTable_partition_table with the initial partition
  // token
  std::vector<const Column*> columns_cs1 = {
      change_stream_->change_stream_partition_table()
          ->FindKeyColumn("partition_token")
          ->column(),
      change_stream_->change_stream_partition_table()->FindColumn("end_time")};
  const std::vector<zetasql::Value> values_cs1 = {
      zetasql::Value::String("11111"), zetasql::Value::NullTimestamp()};
  ZETASQL_EXPECT_OK(store()->Insert(change_stream_->change_stream_partition_table(),
                            Key({String("11111")}), columns_cs1, values_cs1));
  // Populate ChangeStream_TestTable2_partition_table with the initial partition
  // token
  std::vector<const Column*> columns_cs2 = {
      change_stream2_->change_stream_partition_table()
          ->FindKeyColumn("partition_token")
          ->column(),
      change_stream2_->change_stream_partition_table()->FindColumn("end_time")};
  const std::vector<zetasql::Value> values_cs2 = {
      zetasql::Value::String("22222"), zetasql::Value::NullTimestamp()};
  ZETASQL_EXPECT_OK(store()->Insert(change_stream2_->change_stream_partition_table(),
                            Key({String("22222")}), columns_cs2, values_cs2));

  // Insert to base table
  ZETASQL_EXPECT_OK(
      effector_->Effect(ctx(), Insert(table2_, Key({Int64(1)}), string_col_,
                                      {Int64(1), String("string_value1")})));
  ZETASQL_EXPECT_OK(
      effector2_->Effect(ctx(), Insert(table2_, Key({Int64(2)}), string_col_,
                                       {Int64(2), String("string_value2")})));
  ZETASQL_EXPECT_OK(
      effector_->Effect(ctx(), Insert(table2_, Key({Int64(1)}), string_col_,
                                      {Int64(3), String("string_value3")})));
  ZETASQL_EXPECT_OK(effector_->Effect(
      ctx(), Insert(table2_, Key({Int64(1)}), another_string_col_,
                    {Int64(4), String("another_string_value4")})));
  // Set number_of_records_in_transaction in each DataChangeRecord after
  // finishing processing all operations
  ctx()->change_stream_effects()->BuildMutation();
  // Insert to table2(string_col) tracked by cs1, Insert to table2(string_col)
  // tracked by cs2, Insert to table2(string_col) tracked by cs1, Insert to
  // table2(another_string_col) tracked by cs1 -> 3 DataChangeRecords
  ASSERT_EQ(change_stream_effects_buffer()->GetWriteOps().size(), 3);
  int count_cs_test_table = 0;
  int count_cs_test_table2 = 0;
  for (int64_t i = 0; i < change_stream_effects_buffer()->GetWriteOps().size();
       ++i) {
    WriteOp op = change_stream_effects_buffer()->GetWriteOps()[i];
    auto* insert_operation = std::get_if<InsertOp>(&op);
    if (insert_operation->table->Name() ==
        "_change_stream_data_ChangeStream_TestTable") {
      count_cs_test_table++;
    } else if (insert_operation->table->Name() ==
               "_change_stream_data_ChangeStream_TestTable2") {
      count_cs_test_table2++;
    }
  }
  ASSERT_EQ(count_cs_test_table, 2);
  ASSERT_EQ(count_cs_test_table2, 1);
}

TEST_F(ChangeStreamTest,
       InsertUpdateDeleteUntrackedColumnsForChangeStreamTrackingKeyColsOnly) {
  // Populate ChangeStream_TestTable3_partition_table with the initial partition
  // token
  std::vector<const Column*> columns_cs3 = {
      change_stream3_->change_stream_partition_table()
          ->FindKeyColumn("partition_token")
          ->column(),
      change_stream3_->change_stream_partition_table()->FindColumn("end_time")};
  const std::vector<zetasql::Value> values_cs3 = {
      zetasql::Value::String("33333"), zetasql::Value::NullTimestamp()};
  ZETASQL_EXPECT_OK(store()->Insert(change_stream3_->change_stream_partition_table(),
                            Key({String("33333")}), columns_cs3, values_cs3));
  // Insert to an untracked column.
  ZETASQL_EXPECT_OK(effector3_->Effect(
      ctx(), Insert(table2_, Key({Int64(1)}), another_string_col_,
                    {Int64(1), String("another_string_value1")})));
  // Update to an untracked column.
  ZETASQL_EXPECT_OK(effector3_->Effect(
      ctx(), Update(table2_, Key({Int64(1)}), another_string_col_,
                    {Int64(1), String("another_string_value_update")})));
  // Delete the row.
  ZETASQL_EXPECT_OK(effector3_->Effect(ctx(), Delete(table2_, Key({Int64(1)}))));
  // Set number_of_records_in_transaction in each DataChangeRecord after
  // finishing processing all operations
  ctx()->change_stream_effects()->BuildMutation();
  // Verify the number of rebuilt WriteOps added to the transaction
  // buffer.
  ASSERT_EQ(change_stream_effects_buffer()->GetWriteOps().size(), 2);
  // Verify the first received WriteOp is for INSERT mod_type
  WriteOp op = change_stream_effects_buffer()->GetWriteOps()[0];
  auto* operation = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation, nullptr);
  ASSERT_EQ(operation->values[8], zetasql::Value(String("INSERT")));
  // Verify column_types
  zetasql::Value col_types = operation->values[6];
  // Since column_types only include column types tracked by the change stream,
  // verify only the key column is included in column_types
  ASSERT_EQ(col_types.num_elements(), 1);
  ASSERT_EQ(col_types.element(0).json_value().GetMembers()[0].first,
            "is_primary_key");
  ASSERT_EQ(col_types.element(0).json_value().GetMembers()[0].second.ToString(),
            "true");
  // Since new_values field in mods field only contains non_key_col values,
  // new_values should be empty.
  zetasql::Value mods = operation->values[7];
  ASSERT_EQ(mods.num_elements(), 1);
  zetasql::JSONValueConstRef curr_mod = mods.element(0).json_value();
  ASSERT_EQ(curr_mod.GetMembers().size(), 3);
  ASSERT_EQ(curr_mod.GetMembers()[0].first, "keys");
  ASSERT_EQ(mods.element(0).json_value().GetMembers()[0].second.GetString(),
            "{\"int64_col\":\"1\"}");
  ASSERT_EQ(curr_mod.GetMembers()[1].first, "new_values");
  ASSERT_EQ(mods.element(0).json_value().GetMembers()[1].second.GetString(),
            "{}");
  ASSERT_EQ(curr_mod.GetMembers()[2].first, "old_values");
  ASSERT_EQ(mods.element(0).json_value().GetMembers()[2].second.GetString(),
            "{}");

  // Verify the second received WriteOp is for DELETE mod_type
  op = change_stream_effects_buffer()->GetWriteOps()[1];
  auto* operation2 = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation, nullptr);
  ASSERT_EQ(operation2->values[8], zetasql::Value(String("DELETE")));
  // Verify column_types to be empty
  col_types = operation2->values[6];
  // Since column_types only include column types tracked by the change stream,
  // verify only the key column is included in column_types
  ASSERT_EQ(col_types.num_elements(), 1);
  ASSERT_EQ(col_types.element(0).json_value().GetMembers()[0].first,
            "is_primary_key");
  ASSERT_EQ(col_types.element(0).json_value().GetMembers()[0].second.ToString(),
            "true");
  // Verify mods to be empty
  mods = operation2->values[7];
  ASSERT_EQ(mods.num_elements(), 1);
  curr_mod = mods.element(0).json_value();
  ASSERT_EQ(curr_mod.GetMembers().size(), 3);
  ASSERT_EQ(curr_mod.GetMembers()[0].first, "keys");
  ASSERT_EQ(mods.element(0).json_value().GetMembers()[0].second.GetString(),
            "{\"int64_col\":\"1\"}");
  ASSERT_EQ(curr_mod.GetMembers()[1].first, "new_values");
  ASSERT_EQ(mods.element(0).json_value().GetMembers()[1].second.GetString(),
            "{}");
  ASSERT_EQ(curr_mod.GetMembers()[2].first, "old_values");
  ASSERT_EQ(mods.element(0).json_value().GetMembers()[2].second.GetString(),
            "{}");
}

TEST_F(ChangeStreamTest, InsertUpdateDeleteUntrackedColumnsSameRow) {
  // Populate ChangeStream_TestTable2_partition_table with the initial partition
  // token
  std::vector<const Column*> columns_cs2 = {
      change_stream2_->change_stream_partition_table()
          ->FindKeyColumn("partition_token")
          ->column(),
      change_stream2_->change_stream_partition_table()->FindColumn("end_time")};
  const std::vector<zetasql::Value> values_cs2 = {
      zetasql::Value::String("22222"), zetasql::Value::NullTimestamp()};
  ZETASQL_EXPECT_OK(store()->Insert(change_stream2_->change_stream_partition_table(),
                            Key({String("22222")}), columns_cs2, values_cs2));
  // Insert to an untracked column.
  ZETASQL_EXPECT_OK(effector2_->Effect(
      ctx(), Insert(table2_, Key({Int64(1)}), another_string_col_,
                    {Int64(1), String("another_string_value1")})));
  // Update to an untracked column.
  ZETASQL_EXPECT_OK(effector2_->Effect(
      ctx(), Update(table2_, Key({Int64(1)}), another_string_col_,
                    {Int64(1), String("another_string_value_update")})));
  // Delete the row.
  ZETASQL_EXPECT_OK(effector2_->Effect(ctx(), Delete(table2_, Key({Int64(1)}))));
  // Set number_of_records_in_transaction in each DataChangeRecord after
  // finishing processing all operations
  ctx()->change_stream_effects()->BuildMutation();
  // Verify the number of rebuilt WriteOps added to the transaction
  // buffer.
  ASSERT_EQ(change_stream_effects_buffer()->GetWriteOps().size(), 2);
  // Verify the first received WriteOp is for INSERT mod_type
  WriteOp op = change_stream_effects_buffer()->GetWriteOps()[0];
  auto* operation = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation, nullptr);
  ASSERT_EQ(operation->values[8], zetasql::Value(String("INSERT")));
  ASSERT_EQ(operation->values[4],
            zetasql::Value(
                Bool(false /* is_last_record_in_transaction_in_partition */)));
  // Verify number_of_records_in_transaction
  ASSERT_EQ(operation->values[10], zetasql::Value(Int64(2)));
  // Verify column_types
  zetasql::Value col_types = operation->values[6];
  // Since column_types only include column types tracked by the change stream,
  // verify only the key column is included in column_types
  ASSERT_EQ(col_types.num_elements(), 1);
  ASSERT_EQ(col_types.element(0).json_value().GetMembers()[0].first,
            "is_primary_key");
  ASSERT_EQ(col_types.element(0).json_value().GetMembers()[0].second.ToString(),
            "true");
  // Since new_values field in mods field only contains non_key_col values,
  // new_values should be empty.
  zetasql::Value mods = operation->values[7];
  ASSERT_EQ(mods.num_elements(), 1);
  zetasql::JSONValueConstRef curr_mod = mods.element(0).json_value();
  ASSERT_EQ(curr_mod.GetMembers().size(), 3);
  ASSERT_EQ(curr_mod.GetMembers()[0].first, "keys");
  ASSERT_EQ(mods.element(0).json_value().GetMembers()[0].second.GetString(),
            "{\"int64_col\":\"1\"}");
  ASSERT_EQ(curr_mod.GetMembers()[1].first, "new_values");
  ASSERT_EQ(mods.element(0).json_value().GetMembers()[1].second.GetString(),
            "{}");
  ASSERT_EQ(curr_mod.GetMembers()[2].first, "old_values");
  ASSERT_EQ(mods.element(0).json_value().GetMembers()[2].second.GetString(),
            "{}");

  // Verify the second received WriteOp is for DELETE mod_type
  op = change_stream_effects_buffer()->GetWriteOps()[1];
  auto* operation2 = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation, nullptr);
  ASSERT_EQ(operation2->values[8], zetasql::Value(String("DELETE")));
  // Verify column_types to be empty
  col_types = operation2->values[6];
  // Since column_types only include column types tracked by the change stream,
  // verify only the key column is included in column_types
  ASSERT_EQ(col_types.num_elements(), 1);
  ASSERT_EQ(col_types.element(0).json_value().GetMembers()[0].first,
            "is_primary_key");
  ASSERT_EQ(col_types.element(0).json_value().GetMembers()[0].second.ToString(),
            "true");
  // Verify mods to be empty
  mods = operation2->values[7];
  ASSERT_EQ(mods.num_elements(), 1);
  curr_mod = mods.element(0).json_value();
  ASSERT_EQ(curr_mod.GetMembers().size(), 3);
  ASSERT_EQ(curr_mod.GetMembers()[0].first, "keys");
  ASSERT_EQ(mods.element(0).json_value().GetMembers()[0].second.GetString(),
            "{\"int64_col\":\"1\"}");
  ASSERT_EQ(curr_mod.GetMembers()[1].first, "new_values");
  ASSERT_EQ(mods.element(0).json_value().GetMembers()[1].second.GetString(),
            "{}");
  ASSERT_EQ(curr_mod.GetMembers()[2].first, "old_values");
  ASSERT_EQ(mods.element(0).json_value().GetMembers()[2].second.GetString(),
            "{}");
}

TEST_F(ChangeStreamTest, MultipleInsertToSeparateSubsetsColumnsSameTable) {
  // Populate ChangeStream_TestTable_partition_table with the initial partition
  // token
  std::vector<const Column*> columns_cs1 = {
      change_stream_->change_stream_partition_table()
          ->FindKeyColumn("partition_token")
          ->column(),
      change_stream_->change_stream_partition_table()->FindColumn("end_time")};
  const std::vector<zetasql::Value> values_cs1 = {
      zetasql::Value::String("11111"), zetasql::Value::NullTimestamp()};
  ZETASQL_EXPECT_OK(store()->Insert(change_stream_->change_stream_partition_table(),
                            Key({String("11111")}), columns_cs1, values_cs1));
  // Insert base table entry to TestTable(string_col).
  ZETASQL_EXPECT_OK(
      effector_->Effect(ctx(), Insert(table_, Key({Int64(1)}), string_col_,
                                      {Int64(1), String("string_value1")})));
  // Insert base table entry to TestTable(another_string_col).
  ZETASQL_EXPECT_OK(effector_->Effect(
      ctx(), Insert(table2_, Key({Int64(1)}), another_string_col_,
                    {Int64(1), String("another_string_value1")})));
  // Set number_of_records_in_transaction in each DataChangeRecord after
  // finishing processing all operations
  ctx()->change_stream_effects()->BuildMutation();
  // Verify the number of rebuilt WriteOps added to the transaction
  // buffer.
  ASSERT_EQ(change_stream_effects_buffer()->GetWriteOps().size(), 2);

  // Verify the first received WriteOp is for INSERT mod_type
  WriteOp op = change_stream_effects_buffer()->GetWriteOps()[0];
  auto* operation = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation, nullptr);
  // Verify FIRST DataChangeRecord's column_types
  zetasql::Value col_types = operation->values[6];
  // Since column_types include column types tracked by the change stream and
  // change_stream_ tracks all, verify both the key column and the tracked
  // non_key column (string_col_) are included in column_types.
  ASSERT_EQ(col_types.num_elements(), 2);
  ASSERT_EQ(col_types.element(0).json_value().GetMembers()[0].first,
            "is_primary_key");
  ASSERT_EQ(col_types.element(0).json_value().GetMembers()[0].second.ToString(),
            "true");
  ASSERT_EQ(col_types.element(1).json_value().GetMembers()[0].first,
            "is_primary_key");
  ASSERT_EQ(col_types.element(1).json_value().GetMembers()[0].second.ToString(),
            "false");
  // Verify mods to be empty
  zetasql::Value mods = operation->values[7];
  ASSERT_EQ(mods.num_elements(), 1);
  zetasql::JSONValueConstRef curr_mod = mods.element(0).json_value();
  ASSERT_EQ(curr_mod.GetMembers().size(), 3);
  ASSERT_EQ(curr_mod.GetMembers()[0].first, "keys");
  ASSERT_EQ(mods.element(0).json_value().GetMembers()[0].second.GetString(),
            "{\"int64_col\":\"1\"}");
  ASSERT_EQ(curr_mod.GetMembers()[1].first, "new_values");
  ASSERT_EQ(mods.element(0).json_value().GetMembers()[1].second.GetString(),
            "{\"string_col\":\"string_value1\"}");
  ASSERT_EQ(curr_mod.GetMembers()[2].first, "old_values");
  ASSERT_EQ(mods.element(0).json_value().GetMembers()[2].second.GetString(),
            "{}");

  op = change_stream_effects_buffer()->GetWriteOps()[1];
  operation = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation, nullptr);
  // Verify SECOND DataChangeRecord's column_types
  zetasql::Value col_types2 = operation->values[6];
  // Since column_types include column types tracked by the change stream and
  // change_stream_ tracks all, verify both the key column and the tracked
  // non_key column (another_string_col_) are included in column_types.
  ASSERT_EQ(col_types2.num_elements(), 2);
  ASSERT_EQ(col_types2.element(0).json_value().GetMembers()[0].first,
            "is_primary_key");
  ASSERT_EQ(
      col_types2.element(0).json_value().GetMembers()[0].second.ToString(),
      "true");
  ASSERT_EQ(col_types2.element(1).json_value().GetMembers()[0].first,
            "is_primary_key");
  ASSERT_EQ(
      col_types2.element(1).json_value().GetMembers()[0].second.ToString(),
      "false");
  // Verify mods to be empty
  zetasql::Value mods2 = operation->values[7];
  ASSERT_EQ(mods2.num_elements(), 1);
  zetasql::JSONValueConstRef curr_mod2 = mods2.element(0).json_value();
  ASSERT_EQ(curr_mod2.GetMembers().size(), 3);
  ASSERT_EQ(curr_mod2.GetMembers()[0].first, "keys");
  ASSERT_EQ(mods2.element(0).json_value().GetMembers()[0].second.GetString(),
            "{\"int64_col\":\"1\"}");
  ASSERT_EQ(curr_mod2.GetMembers()[1].first, "new_values");
  ASSERT_EQ(mods2.element(0).json_value().GetMembers()[1].second.GetString(),
            "{\"another_string_col\":\"another_string_value1\"}");
  ASSERT_EQ(curr_mod2.GetMembers()[2].first, "old_values");
  ASSERT_EQ(mods2.element(0).json_value().GetMembers()[2].second.GetString(),
            "{}");
}
}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
