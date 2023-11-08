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

#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/log.h"
#include "absl/types/span.h"
#include "absl/types/variant.h"
#include "backend/actions/context.h"
#include "backend/actions/ops.h"
#include "backend/datamodel/key.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "tests/common/actions.h"
#include "tests/common/schema_constructor.h"
#include "nlohmann/json_fwd.hpp"
#include "nlohmann/json.hpp"
namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {
using JSON = ::nlohmann::json;
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
                            CREATE CHANGE STREAM ChangeStream_All FOR ALL OPTIONS ( value_capture_type = 'NEW_VALUES' )
                        )",
                        R"(
                            CREATE CHANGE STREAM ChangeStream_TestTable2StrCol FOR TestTable2(string_col) OPTIONS ( value_capture_type = 'NEW_VALUES' )
                        )",
                        R"(
                            CREATE CHANGE STREAM ChangeStream_TestTable2KeyOnly FOR TestTable2() OPTIONS ( value_capture_type = 'NEW_VALUES' )
                        )",
                        R"(
                            CREATE CHANGE STREAM ChangeStream_TestTable2 FOR TestTable2 OPTIONS ( value_capture_type = 'NEW_VALUES' )
                        )"},
                    &type_factory_)
                    .value()),
        table_(schema_->FindTable("TestTable")),
        table2_(schema_->FindTable("TestTable2")),
        base_columns_(table_->columns()),
        base_columns_table_2_all_col_(table2_->columns()),
        change_stream_(schema_->FindChangeStream("ChangeStream_All")),
        change_stream2_(
            schema_->FindChangeStream("ChangeStream_TestTable2StrCol")),
        change_stream3_(
            schema_->FindChangeStream("ChangeStream_TestTable2KeyOnly")),
        change_stream4_(schema_->FindChangeStream("ChangeStream_TestTable2")) {}

 protected:
  // Test components.
  zetasql::TypeFactory type_factory_;
  std::unique_ptr<const Schema> schema_;

  // Test variables.
  const Table* table_;
  const Table* table2_;
  const Table* table3_;
  absl::Span<const Column* const> base_columns_;
  absl::Span<const Column* const> base_columns_table_2_all_col_;
  const ChangeStream* change_stream_;
  const ChangeStream* change_stream2_;
  const ChangeStream* change_stream3_;
  const ChangeStream* change_stream4_;
  std::vector<const Column*> key_and_another_string_col_table_1_ = {
      table_->FindColumn("int64_col"),
      table_->FindColumn("another_string_col")};
  std::vector<const Column*> key_and_string_col_table_1_ = {
      table_->FindColumn("int64_col"), table_->FindColumn("string_col")};
  std::vector<const Column*> key_and_another_string_col_table_2_ = {
      table2_->FindColumn("int64_col"),
      table2_->FindColumn("another_string_col")};
  std::vector<const Column*> key_and_string_col_table_2_ = {
      table2_->FindColumn("int64_col"), table2_->FindColumn("string_col")};
};

void set_up_partition_token_for_change_stream_partition_table(
    const ChangeStream* change_stream, test::TestReadOnlyStore* store) {
  // Populate partition table with the initial partition token
  std::vector<const Column*> columns;
  columns.push_back(change_stream->change_stream_partition_table()
                        ->FindKeyColumn("partition_token")
                        ->column());
  columns.push_back(
      change_stream->change_stream_partition_table()->FindColumn("end_time"));
  const std::vector<zetasql::Value> values = {
      zetasql::Value::String("11111"), zetasql::Value::NullTimestamp()};
  // Insert 1st partition to change_stream2_'s partition table
  ZETASQL_EXPECT_OK(store->Insert(change_stream->change_stream_partition_table(),
                          Key({String("11111")}), columns, values));
}

TEST_F(ChangeStreamTest, AddOneInsertOpAndCheckResultWriteOpContent) {
  set_up_partition_token_for_change_stream_partition_table(change_stream_,
                                                           store());
  // Insert base table entry.
  std::vector<WriteOp> buffered_write_ops;
  buffered_write_ops.push_back(
      Insert(table_, Key({Int64(1)}), base_columns_,
             {Int64(1), String("value"), String("value2")}));
  std::vector<WriteOp> change_stream_write_ops =
      BuildChangeStreamWriteOps(schema_.get(), buffered_write_ops, store(), 1);
  // Verify change stream entry is added to the transaction buffer.
  ASSERT_EQ(change_stream_write_ops.size(), 1);
  WriteOp op = change_stream_write_ops[0];
  // Verify the table of the received WriteOp
  ASSERT_EQ(TableOf(op), change_stream_->change_stream_data_table());
  // Verify the received WriteOp is InsertOp
  auto* operation = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation, nullptr);
  // Verify columns in the rebuilt InsertOp corresponds to columns in
  // change_stream_data_table
  ASSERT_EQ(operation->columns,
            change_stream_->change_stream_data_table()->columns());
  ASSERT_EQ(operation->columns.size(), 19);
  ASSERT_EQ(operation->values.size(), 19);
  // Verify values in the rebuilt InsertOp are correct
  // Verify partition_token
  ASSERT_EQ(operation->values[0], zetasql::Value::String("11111"));
  // Verify record_sequence
  ASSERT_EQ(operation->values[3], zetasql::Value(String("00000000")));
  // Verify is_last_record_in_transaction_in_partition
  ASSERT_EQ(operation->values[4], zetasql::Value(Bool(true)));
  // Verify table_name
  ASSERT_EQ(operation->values[5], zetasql::Value(String("TestTable")));
  // Verify column_types_name
  ASSERT_EQ(operation->values[6],
            zetasql::values::Array(
                zetasql::types::StringArrayType(),
                {zetasql::Value(String("int64_col")),
                 zetasql::Value(String("string_col")),
                 zetasql::Value(String("another_string_col"))}));
  // Verify column_types_type
  JSON col_1_type;
  col_1_type["code"] = "INT64";
  JSON col_2_type;
  col_2_type["code"] = "STRING";
  JSON col_3_type;
  col_3_type["code"] = "STRING";
  ASSERT_EQ(
      operation->values[7],
      zetasql::values::Array(zetasql::types::StringArrayType(),
                               {zetasql::Value(String(col_1_type.dump())),
                                zetasql::Value(String(col_2_type.dump())),
                                zetasql::Value(String(col_3_type.dump()))}));
  // ASSERT_EQ(operation->values[7], zetasql::Value(String("int64_col")));
  // Verify column_types_is_primary_key
  ASSERT_EQ(operation->values[8],
            zetasql::values::Array(
                zetasql::types::BoolArrayType(),
                {zetasql::Value(Bool(true)), zetasql::Value(Bool(false)),
                 zetasql::Value(Bool(false))}));
  // Verify column_types_ordinal_position
  ASSERT_EQ(operation->values[9],
            zetasql::values::Array(
                zetasql::types::Int64ArrayType(),
                {zetasql::Value(Int64(1)), zetasql::Value(Int64(2)),
                 zetasql::Value(Int64(3))}));
  // Verify mods
  zetasql::Value mod_keys = operation->values[10];
  ASSERT_EQ(mod_keys.element(0),
            zetasql::Value(String("{\"int64_col\":\"1\"}")));
  zetasql::Value mod_new_values = operation->values[11];
  ASSERT_EQ(mod_new_values.element(0),
            zetasql::Value(
                String("{\"another_string_col\":\"value2\",\"string_col\":"
                       "\"value\"}")));
  zetasql::Value mod_old_values = operation->values[12];
  ASSERT_EQ(mod_old_values.element(0), zetasql::Value(String("{}")));
  // Verify mod_type
  ASSERT_EQ(operation->values[13], zetasql::Value(String("INSERT")));
  // Verify value_capture_type
  ASSERT_EQ(operation->values[14], zetasql::Value(String("NEW_VALUES")));
  // Verify number_of_records_in_transaction
  ASSERT_EQ(operation->values[15], zetasql::Value(Int64(1)));
  // Verify number_of_partitions_in_transaction
  ASSERT_EQ(operation->values[16], zetasql::Value(Int64(1)));
  // Verify transaction_tag
  ASSERT_EQ(operation->values[17], zetasql::Value(String("")));
  // Verify is_system_transaction
  ASSERT_EQ(operation->values[18], zetasql::Value(Bool(false)));
}

TEST_F(ChangeStreamTest, AddTwoInsertForDiffSetCols) {
  set_up_partition_token_for_change_stream_partition_table(change_stream_,
                                                           store());
  // Insert base table entry.
  std::vector<WriteOp> buffered_write_ops;
  // Insert 1st base table entry. base_columns1 only contains the first two
  // columns of TestTable2.
  std::vector<const Column*> insert_columns1 = {
      table_->FindColumn("int64_col"), table_->FindColumn("string_col")};
  buffered_write_ops.push_back(Insert(table_, Key({Int64(1)}), insert_columns1,
                                      {Int64(1), String("value")}));
  // Insert 2nd base table entry. base_columns_table_2_all_col_ contains all
  // columns of TestTable2.
  buffered_write_ops.push_back(
      Insert(table_, Key({Int64(2)}), base_columns_,
             {Int64(2), String("value"), String("value2")}));
  std::vector<WriteOp> change_stream_write_ops =
      BuildChangeStreamWriteOps(schema_.get(), buffered_write_ops, store(), 1);
  // Verify change stream entry is added to the transaction buffer.
  ASSERT_EQ(change_stream_write_ops.size(), 1);
}

TEST_F(ChangeStreamTest, AddTwoInsertDiffSetsNonKeyTrackedCols) {
  // Populate partition table with the initial partition token
  set_up_partition_token_for_change_stream_partition_table(change_stream_,
                                                           store());
  std::vector<WriteOp> buffered_write_ops;
  // Insert 1st base table entry.
  std::vector<const Column*> base_columns1 = {table_->FindColumn("int64_col")};
  buffered_write_ops.push_back(
      Insert(table_, Key({Int64(1)}), base_columns1, {Int64(1)}));
  // Insert 2nd base table entry
  std::vector<const Column*> base_columns2 = {table_->FindColumn("int64_col"),
                                              table_->FindColumn("string_col")};
  buffered_write_ops.push_back(Insert(table_, Key({Int64(2)}), base_columns2,
                                      {Int64(2), String("value")}));
  std::vector<WriteOp> change_stream_write_ops =
      BuildChangeStreamWriteOps(schema_.get(), buffered_write_ops, store(), 1);
  // Verify change stream entry is added to the transaction buffer.
  ASSERT_EQ(change_stream_write_ops.size(), 1);
}

// Add operations with different mod_types to the buffer and check if distinct
// DataChangeRecords are generated once mod_type changed.
// Insert, Insert, Update, Update, Insert, Delete, Delete -> 4 WriteOps
TEST_F(ChangeStreamTest, AddMultipleDataChangeRecordsToChangeStreamDataTable) {
  // Populate partition table with the initial partition token
  set_up_partition_token_for_change_stream_partition_table(change_stream_,
                                                           store());
  std::vector<WriteOp> buffered_write_ops;
  buffered_write_ops.push_back(
      Insert(table_, Key({Int64(1)}), base_columns_,
             {Int64(1), String("value"), String("value2")}));
  buffered_write_ops.push_back(
      Insert(table_, Key({Int64(2)}), base_columns_,
             {Int64(2), String("value_row2"), String("value2_row2")}));
  buffered_write_ops.push_back(
      Update(table_, Key({Int64(1)}), base_columns_,
             {Int64(1), String("updated_value"), String("updated_value2")}));
  buffered_write_ops.push_back(Update(
      table_, Key({Int64(2)}), base_columns_,
      {Int64(2), String("updated_value_row2"), String("updated_value2_row2")}));
  buffered_write_ops.push_back(
      Insert(table_, Key({Int64(3)}), base_columns_,
             {Int64(3), String("value_row3"), String("value2_row3")}));
  buffered_write_ops.push_back(Delete(table_, Key({Int64(1)})));
  buffered_write_ops.push_back(Delete(table_, Key({Int64(2)})));
  std::vector<WriteOp> change_stream_write_ops =
      BuildChangeStreamWriteOps(schema_.get(), buffered_write_ops, store(), 1);
  // Verify the number of change stream entries is added to the transaction
  // buffer.
  // Insert, Insert, Update, Update, Insert, Delete, Delete -> 4 WriteOps
  ASSERT_EQ(change_stream_write_ops.size(), 4);

  WriteOp op = change_stream_write_ops[0];
  // Verify the first received WriteOp is InsertOp
  auto* operation = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation, nullptr);
  // Verify mod_type
  ASSERT_EQ(operation->values[13], zetasql::Value(String("INSERT")));
  // column_type_names
  ASSERT_EQ(operation->values[3], zetasql::Value(String("00000000")));
  // Verify is_last_record_in_transaction_in_partition
  ASSERT_EQ(operation->values[4], zetasql::Value(Bool(false)));
  // Verify number_of_records_in_transaction
  ASSERT_EQ(operation->values[15], zetasql::Value(Int64(4)));
  // Verify the column_types of the 1st WriteOp (INSERT mod_type)
  ASSERT_EQ(operation->values[6],
            zetasql::values::Array(
                zetasql::types::StringArrayType(),
                {zetasql::Value(String("int64_col")),
                 zetasql::Value(String("string_col")),
                 zetasql::Value(String("another_string_col"))}));
  // Verify column_types_type
  JSON col_1_type;
  col_1_type["code"] = "INT64";
  JSON col_2_type;
  col_2_type["code"] = "STRING";
  JSON col_3_type;
  col_3_type["code"] = "STRING";
  ASSERT_EQ(
      operation->values[7],
      zetasql::values::Array(zetasql::types::StringArrayType(),
                               {zetasql::Value(String(col_1_type.dump())),
                                zetasql::Value(String(col_2_type.dump())),
                                zetasql::Value(String(col_3_type.dump()))}));
  ASSERT_EQ(operation->values[8],
            zetasql::values::Array(
                zetasql::types::BoolArrayType(),
                {zetasql::Value(Bool(true)), zetasql::Value(Bool(false)),
                 zetasql::Value(Bool(false))}));
  // Verify the mods of the 1st WriteOp (INSERT mod_type)
  zetasql::Value mod_keys = operation->values[10];
  ASSERT_EQ(mod_keys.num_elements(), 2);
  ASSERT_EQ(mod_keys.element(0),
            zetasql::Value(String("{\"int64_col\":\"1\"}")));
  ASSERT_EQ(mod_keys.element(1),
            zetasql::Value(String("{\"int64_col\":\"2\"}")));
  zetasql::Value mod_new_values = operation->values[11];
  ASSERT_EQ(mod_new_values.element(0),
            zetasql::Value(
                String("{\"another_string_col\":\"value2\",\"string_col\":"
                       "\"value\"}")));
  ASSERT_EQ(mod_new_values.element(1),
            zetasql::Value(String("{\"another_string_col\":\"value2_row2\","
                                    "\"string_col\":\"value_row2\"}")));
  zetasql::Value mod_old_values = operation->values[12];
  ASSERT_EQ(mod_old_values.element(0), zetasql::Value(String("{}")));
  ASSERT_EQ(mod_old_values.element(1), zetasql::Value(String("{}")));

  // Verify the 2nd received WriteOp (UPDATE mod_type)
  op = change_stream_write_ops[1];
  auto* operation2 = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation2, nullptr);
  ASSERT_EQ(operation2->values[3], zetasql::Value(String("00000001")));
  // Verify is_last_record_in_transaction_in_partition
  ASSERT_EQ(operation2->values[4], zetasql::Value(Bool(false)));
  // Verify mod_type
  ASSERT_EQ(operation2->values[13], zetasql::Value(String("UPDATE")));
  // Verify number_of_records_in_transaction
  ASSERT_EQ(operation2->values[15], zetasql::Value(Int64(4)));
  // Verify the column_types_name of the 2nd WriteOp (UPDATE mod_type)
  ASSERT_EQ(operation2->values[6],
            zetasql::values::Array(
                zetasql::types::StringArrayType(),
                {zetasql::Value(String("int64_col")),
                 zetasql::Value(String("string_col")),
                 zetasql::Value(String("another_string_col"))}));
  // Verify column_types_type
  ASSERT_EQ(
      operation2->values[7],
      zetasql::values::Array(zetasql::types::StringArrayType(),
                               {zetasql::Value(String(col_1_type.dump())),
                                zetasql::Value(String(col_2_type.dump())),
                                zetasql::Value(String(col_3_type.dump()))}));
  // Verify column_types_is_primary_key
  ASSERT_EQ(operation2->values[8],
            zetasql::values::Array(
                zetasql::types::BoolArrayType(),
                {zetasql::Value(Bool(true)), zetasql::Value(Bool(false)),
                 zetasql::Value(Bool(false))}));
  // Verify column_types_ordinal_position
  ASSERT_EQ(operation2->values[9],
            zetasql::values::Array(
                zetasql::types::Int64ArrayType(),
                {zetasql::Value(Int64(1)), zetasql::Value(Int64(2)),
                 zetasql::Value(Int64(3))}));
  // Verify the mods of the 2nd WriteOp (UPDATE mod_type)
  zetasql::Value mod_2_keys = operation->values[10];
  ASSERT_EQ(mod_2_keys.num_elements(), 2);
  ASSERT_EQ(mod_2_keys.element(0),
            zetasql::Value(String("{\"int64_col\":\"1\"}")));
  ASSERT_EQ(mod_2_keys.element(1),
            zetasql::Value(String("{\"int64_col\":\"2\"}")));
  zetasql::Value mod_2_new_values = operation->values[11];
  ASSERT_EQ(mod_2_new_values.element(0),
            zetasql::Value(
                String("{\"another_string_col\":\"updated_value2\",\"string_"
                       "col\":\"updated_value\"}")));
  ASSERT_EQ(
      mod_2_new_values.element(1),
      zetasql::Value(String("{\"another_string_col\":\"updated_value2_row2\","
                              "\"string_col\":\"updated_value_row2\"}")));
  zetasql::Value mod_2_old_values = operation->values[12];
  ASSERT_EQ(mod_2_old_values.element(0), zetasql::Value(String("{}")));
  ASSERT_EQ(mod_2_old_values.element(1), zetasql::Value(String("{}")));

  // Verify the 3rd received WriteOp (INSERT mod_type)
  op = change_stream_write_ops[2];
  auto* operation3 = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation3, nullptr);
  ASSERT_EQ(operation3->values[13], zetasql::Value(String("INSERT")));
  ASSERT_EQ(operation->values[3], zetasql::Value(String("00000002")));
  // Verify is_last_record_in_transaction_in_partition
  ASSERT_EQ(operation3->values[4], zetasql::Value(Bool(false)));
  // Verify number_of_records_in_transaction
  ASSERT_EQ(operation3->values[15], zetasql::Value(Int64(4)));

  // Verify the 4th(last) received WriteOp is DeleteOp
  op = change_stream_write_ops[3];
  auto operation4 = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation4, nullptr);
  ASSERT_EQ(operation4->values[3], zetasql::Value(String("00000003")));
  // Verify is_last_record_in_transaction_in_partition
  ASSERT_EQ(operation4->values[4], zetasql::Value(Bool(true)));
  // Verify mod_type
  ASSERT_EQ(operation4->values[13], zetasql::Value(String("DELETE")));
  // Verify number_of_records_in_transaction
  ASSERT_EQ(operation4->values[15], zetasql::Value(Int64(4)));

  // Verify the column_types of the 4th WriteOp (DELETE mod_type)
  ASSERT_EQ(operation4->values[6],
            zetasql::values::Array(
                zetasql::types::StringArrayType(),
                {zetasql::Value(String("int64_col")),
                 zetasql::Value(String("string_col")),
                 zetasql::Value(String("another_string_col"))}));
  // Verify column_types_type
  ASSERT_EQ(
      operation4->values[7],
      zetasql::values::Array(zetasql::types::StringArrayType(),
                               {zetasql::Value(String(col_1_type.dump())),
                                zetasql::Value(String(col_2_type.dump())),
                                zetasql::Value(String(col_3_type.dump()))}));
  // Verify column_types_is_primary_key
  ASSERT_EQ(operation4->values[8],
            zetasql::values::Array(
                zetasql::types::BoolArrayType(),
                {zetasql::Value(Bool(true)), zetasql::Value(Bool(false)),
                 zetasql::Value(Bool(false))}));
  // Verify column_types_ordinal_position
  ASSERT_EQ(operation4->values[9],
            zetasql::values::Array(
                zetasql::types::Int64ArrayType(),
                {zetasql::Value(Int64(1)), zetasql::Value(Int64(2)),
                 zetasql::Value(Int64(3))}));
  // Verify the mods of the 4th WriteOp (DELETE mod_type)
  zetasql::Value mod_4_keys = operation4->values[10];
  ASSERT_EQ(mod_4_keys.num_elements(), 2);
  ASSERT_EQ(mod_4_keys.element(0),
            zetasql::Value(String("{\"int64_col\":\"1\"}")));
  ASSERT_EQ(mod_4_keys.element(1),
            zetasql::Value(String("{\"int64_col\":\"2\"}")));
  zetasql::Value mod_4_new_values = operation4->values[11];
  ASSERT_EQ(mod_4_new_values.element(0), zetasql::Value(String("{}")));
  ASSERT_EQ(mod_4_new_values.element(1), zetasql::Value(String("{}")));
  zetasql::Value mod_4_old_values = operation4->values[12];
  ASSERT_EQ(mod_4_old_values.element(0), zetasql::Value(String("{}")));
  ASSERT_EQ(mod_4_old_values.element(1), zetasql::Value(String("{}")));
}

// Insert to table1, Insert to table2, Insert to table1 -> 3 DataChangeRecords
TEST_F(ChangeStreamTest, AddWriteOpForDiffUserTablesForSameChangeStream) {
  // Populate partition table with the initial partition token
  set_up_partition_token_for_change_stream_partition_table(change_stream_,
                                                           store());
  absl::flat_hash_map<const ChangeStream*, std::vector<DataChangeRecord>>
      data_change_records_in_transaction_by_change_stream;
  absl::flat_hash_map<const ChangeStream*, ModGroup>
      last_mod_group_by_change_stream;
  // Insert base table entry to TestTable.
  LogTableMod(Insert(table_, Key({Int64(1)}), base_columns_,
                     {Int64(1), String("value"), String("value2")}),
              change_stream_, zetasql::Value::String("11111"),
              &data_change_records_in_transaction_by_change_stream, 1,
              &last_mod_group_by_change_stream);
  // Insert base table entry to TestTable2.
  LogTableMod(Insert(table2_, Key({Int64(1)}), base_columns_table_2_all_col_,
                     {Int64(1), String("value"), String("value2")}),
              change_stream_, zetasql::Value::String("11111"),
              &data_change_records_in_transaction_by_change_stream, 1,
              &last_mod_group_by_change_stream);
  // Insert base table entry to TestTable.
  LogTableMod(Insert(table_, Key({Int64(2)}), base_columns_,
                     {Int64(2), String("value_row2"), String("value2_row2")}),
              change_stream_, zetasql::Value::String("11111"),
              &data_change_records_in_transaction_by_change_stream, 1,
              &last_mod_group_by_change_stream);

  // Set number_of_records_in_transaction in each DataChangeRecord after
  // finishing processing all operations
  std::vector<WriteOp> write_ops =
      BuildMutation(&data_change_records_in_transaction_by_change_stream, 1,
                    &last_mod_group_by_change_stream);
  // Verify the number rebuilt WriteOps added to the transaction
  // buffer.
  ASSERT_EQ(write_ops.size(), 3);
  WriteOp op = write_ops[0];
  ABSL_LOG(INFO) << "1st op: " << TableOf(op);
  InsertOp* insert_op = std::get_if<InsertOp>(&op);
  ASSERT_NE(insert_op, nullptr);
  EXPECT_EQ(insert_op->values[5], zetasql::Value(String("TestTable")));
  op = write_ops[1];
  ABSL_LOG(INFO) << "2nd op: " << TableOf(op);
  insert_op = std::get_if<InsertOp>(&op);
  ASSERT_NE(insert_op, nullptr);
  EXPECT_EQ(insert_op->values[5], zetasql::Value(String("TestTable2")));
  op = write_ops[2];
  insert_op = std::get_if<InsertOp>(&op);
  ASSERT_NE(insert_op, nullptr);
  EXPECT_EQ(insert_op->values[5], zetasql::Value(String("TestTable")));
}

// Update table1(another_string_col), Update table1(string_col), Update
// table1(another_string_col) -> 3 DataChangeRecords
TEST_F(ChangeStreamTest, AddWriteOpForDiffNonKeyColsForSameChangeStream) {
  // Populate partition table with the initial partition token
  set_up_partition_token_for_change_stream_partition_table(change_stream_,
                                                           store());
  absl::flat_hash_map<const ChangeStream*, std::vector<DataChangeRecord>>
      data_change_records_in_transaction_by_change_stream;
  absl::flat_hash_map<const ChangeStream*, ModGroup>
      last_mod_group_by_change_stream;
  // Insert base table entry to TestTable.
  LogTableMod(
      Update(table_, Key({Int64(1)}), key_and_another_string_col_table_1_,
             {Int64(1), String("another_string_value1")}),
      change_stream_, zetasql::Value::String("11111"),
      &data_change_records_in_transaction_by_change_stream, 1,
      &last_mod_group_by_change_stream);
  // Insert base table entry to TestTable2.
  LogTableMod(Update(table_, Key({Int64(1)}), key_and_string_col_table_1_,
                     {Int64(1), String("string_value1")}),
              change_stream_, zetasql::Value::String("11111"),
              &data_change_records_in_transaction_by_change_stream, 1,
              &last_mod_group_by_change_stream);
  // Insert base table entry to TestTable.
  LogTableMod(
      Update(table_, Key({Int64(2)}), key_and_another_string_col_table_1_,
             {Int64(2), String("another_string_value2")}),
      change_stream_, zetasql::Value::String("11111"),
      &data_change_records_in_transaction_by_change_stream, 1,
      &last_mod_group_by_change_stream);
  // Set number_of_records_in_transaction in each DataChangeRecord after
  // finishing processing all operations
  std::vector<WriteOp> write_ops =
      BuildMutation(&data_change_records_in_transaction_by_change_stream, 1,
                    &last_mod_group_by_change_stream);
  // Verify the number of rebuilt WriteOps added to the transaction
  // buffer.
  EXPECT_EQ(write_ops.size(), 3);
}

TEST_F(ChangeStreamTest, AddWriteOpForDifferentChangeStreams) {
  // Populate ChangeStream_All_partition_table with the initial partition
  // token
  set_up_partition_token_for_change_stream_partition_table(change_stream_,
                                                           store());
  // Populate ChangeStream_TestTable2StrCol_partition_table with the initial
  // partition token
  set_up_partition_token_for_change_stream_partition_table(change_stream2_,
                                                           store());
  absl::flat_hash_map<const ChangeStream*, std::vector<DataChangeRecord>>
      data_change_records_in_transaction_by_change_stream;
  absl::flat_hash_map<const ChangeStream*, ModGroup>
      last_mod_group_by_change_stream;
  // Insert base table entry to TestTable.
  LogTableMod(Insert(table2_, Key({Int64(1)}), key_and_string_col_table_2_,
                     {Int64(1), String("string_value1")}),
              change_stream_, zetasql::Value::String("11111"),
              &data_change_records_in_transaction_by_change_stream, 1,
              &last_mod_group_by_change_stream);
  LogTableMod(Insert(table2_, Key({Int64(2)}), key_and_string_col_table_2_,
                     {Int64(2), String("string_value2")}),
              change_stream2_, zetasql::Value::String("11111"),
              &data_change_records_in_transaction_by_change_stream, 1,
              &last_mod_group_by_change_stream);
  LogTableMod(Insert(table2_, Key({Int64(1)}), key_and_string_col_table_2_,
                     {Int64(3), String("string_value3")}),
              change_stream_, zetasql::Value::String("11111"),
              &data_change_records_in_transaction_by_change_stream, 1,
              &last_mod_group_by_change_stream);
  LogTableMod(
      Insert(table2_, Key({Int64(1)}), key_and_another_string_col_table_2_,
             {Int64(4), String("another_string_value4")}),
      change_stream_, zetasql::Value::String("11111"),
      &data_change_records_in_transaction_by_change_stream, 1,
      &last_mod_group_by_change_stream);
  // Set number_of_records_in_transaction in each DataChangeRecord after
  // finishing processing all operations
  std::vector<WriteOp> write_ops =
      BuildMutation(&data_change_records_in_transaction_by_change_stream, 1,
                    &last_mod_group_by_change_stream);
  // Insert to table2(string_col) tracked by cs1, Insert to table2(string_col)
  // tracked by cs2, Insert to table2(string_col) tracked by cs1, Insert to
  // table2(another_string_col) tracked by cs1 -> 3 DataChangeRecords
  ASSERT_EQ(write_ops.size(), 2);
  int count_cs_test_table = 0;
  int count_cs_test_table2 = 0;
  for (int64_t i = 0; i < write_ops.size(); ++i) {
    WriteOp op = write_ops[i];
    auto* insert_operation = std::get_if<InsertOp>(&op);
    if (insert_operation->table->Name() ==
        "_change_stream_data_ChangeStream_All") {
      count_cs_test_table++;
    } else if (insert_operation->table->Name() ==
               "_change_stream_data_ChangeStream_TestTable2StrCol") {
      count_cs_test_table2++;
    }
  }
  ASSERT_EQ(count_cs_test_table, 1);
  ASSERT_EQ(count_cs_test_table2, 1);
}

TEST_F(ChangeStreamTest,
       InsertUpdateDeleteUntrackedColumnsForChangeStreamTrackingKeyColsOnly) {
  // Populate ChangeStream_TestTable2KeyOnly_partition_table with the initial
  // partition token
  set_up_partition_token_for_change_stream_partition_table(change_stream3_,
                                                           store());
  absl::flat_hash_map<const ChangeStream*, std::vector<DataChangeRecord>>
      data_change_records_in_transaction_by_change_stream;
  absl::flat_hash_map<const ChangeStream*, ModGroup>
      last_mod_group_by_change_stream;
  // Insert base table entry to TestTable.
  LogTableMod(
      Insert(table2_, Key({Int64(1)}), key_and_another_string_col_table_2_,
             {Int64(1), String("another_string_value1")}),
      change_stream3_, zetasql::Value::String("11111"),
      &data_change_records_in_transaction_by_change_stream, 1,
      &last_mod_group_by_change_stream);
  // Update to an untracked column.
  LogTableMod(
      Update(table2_, Key({Int64(1)}), key_and_another_string_col_table_2_,
             {Int64(1), String("another_string_value_update")}),
      change_stream3_, zetasql::Value::String("11111"),
      &data_change_records_in_transaction_by_change_stream, 1,
      &last_mod_group_by_change_stream);
  // Delete the row.
  LogTableMod(Delete(table2_, Key({Int64(1)})), change_stream3_,
              zetasql::Value::String("11111"),
              &data_change_records_in_transaction_by_change_stream, 1,
              &last_mod_group_by_change_stream);
  // Set number_of_records_in_transaction in each DataChangeRecord after
  // finishing processing all operations
  std::vector<WriteOp> write_ops =
      BuildMutation(&data_change_records_in_transaction_by_change_stream, 1,
                    &last_mod_group_by_change_stream);
  // Verify the number of rebuilt WriteOps added to the transaction
  // buffer.
  ASSERT_EQ(write_ops.size(), 2);
  // Verify the first received WriteOp is for INSERT mod_type
  WriteOp op = write_ops[0];
  auto* operation = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation, nullptr);
  ASSERT_EQ(operation->values[13], zetasql::Value(String("INSERT")));
  // Verify column_types_name
  ASSERT_EQ(operation->values[6],
            zetasql::values::Array(zetasql::types::StringArrayType(),
                                     {zetasql::Value(String("int64_col"))}));
  // Verify column_types_type
  JSON col_1_type;
  col_1_type["code"] = "INT64";
  ASSERT_EQ(
      operation->values[7],
      zetasql::values::Array(zetasql::types::StringArrayType(),
                               {zetasql::Value(String(col_1_type.dump()))}));
  // Verify column_types_is_primary_key
  ASSERT_EQ(operation->values[8],
            zetasql::values::Array(zetasql::types::BoolArrayType(),
                                     {zetasql::Value(Bool(true))}));
  // Verify column_types_ordinal_position
  ASSERT_EQ(operation->values[9],
            zetasql::values::Array(zetasql::types::Int64ArrayType(),
                                     {zetasql::Value(Int64(1))}));

  // Since new_values field in mods field only contains non_key_col values,
  // new_values should be empty.
  zetasql::Value mod_keys = operation->values[10];
  ASSERT_EQ(mod_keys.num_elements(), 1);
  ASSERT_EQ(mod_keys.element(0),
            zetasql::Value(String("{\"int64_col\":\"1\"}")));
  zetasql::Value mod_new_values = operation->values[11];
  ASSERT_EQ(mod_new_values.element(0), zetasql::Value(String("{}")));
  zetasql::Value mod_old_values = operation->values[12];
  ASSERT_EQ(mod_old_values.element(0), zetasql::Value(String("{}")));

  // Verify the second received WriteOp is for DELETE mod_type
  op = write_ops[1];
  auto* operation2 = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation, nullptr);
  ASSERT_EQ(operation2->values[13], zetasql::Value(String("DELETE")));
  // Verify column_types_name
  ASSERT_EQ(operation->values[6],
            zetasql::values::Array(zetasql::types::StringArrayType(),
                                     {zetasql::Value(String("int64_col"))}));
  // Verify column_types_type
  ASSERT_EQ(
      operation->values[7],
      zetasql::values::Array(zetasql::types::StringArrayType(),
                               {zetasql::Value(String(col_1_type.dump()))}));
  // ASSERT_EQ(operation->values[7], zetasql::Value(String("int64_col")));
  // Verify column_types_is_primary_key
  ASSERT_EQ(operation->values[8],
            zetasql::values::Array(zetasql::types::BoolArrayType(),
                                     {zetasql::Value(Bool(true))}));
  // Verify column_types_ordinal_position
  ASSERT_EQ(operation->values[9],
            zetasql::values::Array(zetasql::types::Int64ArrayType(),
                                     {zetasql::Value(Int64(1))}));
  // Verify mods to be empty
  mod_keys = operation->values[10];
  ASSERT_EQ(mod_keys.num_elements(), 1);
  ASSERT_EQ(mod_keys.element(0),
            zetasql::Value(String("{\"int64_col\":\"1\"}")));
  mod_new_values = operation->values[11];
  ASSERT_EQ(mod_new_values.element(0), zetasql::Value(String("{}")));
  mod_old_values = operation->values[12];
  ASSERT_EQ(mod_old_values.element(0), zetasql::Value(String("{}")));
}

TEST_F(ChangeStreamTest, InsertUpdateDeleteUntrackedColumnsSameRow) {
  // Populate ChangeStream_TestTable2StrCol_partition_table with the initial
  // partition token
  set_up_partition_token_for_change_stream_partition_table(change_stream2_,
                                                           store());
  absl::flat_hash_map<const ChangeStream*, std::vector<DataChangeRecord>>
      data_change_records_in_transaction_by_change_stream;
  absl::flat_hash_map<const ChangeStream*, ModGroup>
      last_mod_group_by_change_stream;
  // Insert base table entry to TestTable.
  LogTableMod(
      Insert(table2_, Key({Int64(1)}), key_and_another_string_col_table_2_,
             {Int64(1), String("another_string_value1")}),
      change_stream2_, zetasql::Value::String("11111"),
      &data_change_records_in_transaction_by_change_stream, 1,
      &last_mod_group_by_change_stream);
  // Update to an untracked column.
  LogTableMod(
      Update(table2_, Key({Int64(1)}), key_and_another_string_col_table_2_,
             {Int64(1), String("another_string_value_update")}),
      change_stream2_, zetasql::Value::String("11111"),
      &data_change_records_in_transaction_by_change_stream, 1,
      &last_mod_group_by_change_stream);
  // Delete the row.
  LogTableMod(Delete(table2_, Key({Int64(1)})), change_stream2_,
              zetasql::Value::String("11111"),
              &data_change_records_in_transaction_by_change_stream, 1,
              &last_mod_group_by_change_stream);
  // Set number_of_records_in_transaction in each DataChangeRecord after
  // finishing processing all operations
  std::vector<WriteOp> write_ops =
      BuildMutation(&data_change_records_in_transaction_by_change_stream, 1,
                    &last_mod_group_by_change_stream);
  // Verify the number of rebuilt WriteOps added to the transaction
  // buffer.
  ASSERT_EQ(write_ops.size(), 2);
  // Verify the first received WriteOp is for INSERT mod_type
  WriteOp op = write_ops[0];
  auto* operation = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation, nullptr);
  ASSERT_EQ(operation->values[13], zetasql::Value(String("INSERT")));
  // Verify is_last_record_in_transaction_in_partition
  ASSERT_EQ(operation->values[4], zetasql::Value(Bool(false)));
  // Verify number_of_records_in_transaction
  ASSERT_EQ(operation->values[15], zetasql::Value(Int64(2)));
  // Verify column_types_name
  ASSERT_EQ(operation->values[6],
            zetasql::values::Array(zetasql::types::StringArrayType(),
                                     {zetasql::Value(String("int64_col")),
                                      zetasql::Value(String("string_col"))}));
  // Verify column_types_type
  JSON col_1_type;
  col_1_type["code"] = "INT64";
  JSON col_2_type;
  col_2_type["code"] = "STRING";
  ASSERT_EQ(
      operation->values[7],
      zetasql::values::Array(zetasql::types::StringArrayType(),
                               {zetasql::Value(String(col_1_type.dump())),
                                zetasql::Value(String(col_2_type.dump()))}));
  // Verify column_types_is_primary_key
  ASSERT_EQ(operation->values[8],
            zetasql::values::Array(
                zetasql::types::BoolArrayType(),
                {zetasql::Value(Bool(true)), zetasql::Value(Bool(false))}));
  // Verify column_types_ordinal_position
  ASSERT_EQ(operation->values[9],
            zetasql::values::Array(
                zetasql::types::Int64ArrayType(),
                {zetasql::Value(Int64(1)), zetasql::Value(Int64(2))}));
  // Since new_values field in mods field only contains non_key_col values,
  // new_values should be empty.
  zetasql::Value mod_keys = operation->values[10];
  ASSERT_EQ(mod_keys.num_elements(), 1);
  ASSERT_EQ(mod_keys.element(0),
            zetasql::Value(String("{\"int64_col\":\"1\"}")));
  zetasql::Value mod_new_values = operation->values[11];
  ASSERT_EQ(mod_new_values.element(0),
            zetasql::Value(String("{\"string_col\":null}")));
  zetasql::Value mod_old_values = operation->values[12];
  ASSERT_EQ(mod_old_values.element(0), zetasql::Value(String("{}")));

  // Verify the second received WriteOp is for DELETE mod_type
  op = write_ops[1];
  auto* operation2 = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation2, nullptr);
  ASSERT_EQ(operation2->values[13], zetasql::Value(String("DELETE")));
  // Verify column_types_name
  ASSERT_EQ(operation2->values[6],
            zetasql::values::Array(zetasql::types::StringArrayType(),
                                     {zetasql::Value(String("int64_col")),
                                      zetasql::Value(String("string_col"))}));
  // Verify column_types_type
  ASSERT_EQ(
      operation2->values[7],
      zetasql::values::Array(zetasql::types::StringArrayType(),
                               {zetasql::Value(String(col_1_type.dump())),
                                zetasql::Value(String(col_2_type.dump()))}));
  // Verify column_types_is_primary_key
  ASSERT_EQ(operation2->values[8],
            zetasql::values::Array(
                zetasql::types::BoolArrayType(),
                {zetasql::Value(Bool(true)), zetasql::Value(Bool(false))}));
  // Verify column_types_ordinal_position
  ASSERT_EQ(operation2->values[9],
            zetasql::values::Array(
                zetasql::types::Int64ArrayType(),
                {zetasql::Value(Int64(1)), zetasql::Value(Int64(2))}));
  // Verify mods to be empty
  mod_keys = operation->values[10];
  ASSERT_EQ(mod_keys.num_elements(), 1);
  ASSERT_EQ(mod_keys.element(0),
            zetasql::Value(String("{\"int64_col\":\"1\"}")));
  mod_new_values = operation->values[11];
  ASSERT_EQ(mod_new_values.element(0), zetasql::Value(String("{}")));
  mod_old_values = operation->values[12];
  ASSERT_EQ(mod_old_values.element(0), zetasql::Value(String("{}")));
}

TEST_F(ChangeStreamTest, MultipleInsertToSeparateSubsetsColumnsSameTable) {
  // Populate ChangeStream_All_partition_table with the initial partition
  // token
  set_up_partition_token_for_change_stream_partition_table(change_stream_,
                                                           store());
  std::vector<WriteOp> buffered_write_ops;
  buffered_write_ops.push_back(Insert(table_, Key({Int64(1)}),
                                      key_and_string_col_table_1_,
                                      {Int64(1), String("string_value1")}));
  buffered_write_ops.push_back(
      Insert(table_, Key({Int64(2)}), key_and_another_string_col_table_1_,
             {Int64(2), String("another_string_value2")}));
  std::vector<WriteOp> change_stream_write_ops =
      BuildChangeStreamWriteOps(schema_.get(), buffered_write_ops, store(), 1);
  // Verify the number of rebuilt WriteOps added to the transaction
  // buffer.
  ASSERT_EQ(change_stream_write_ops.size(), 1);

  // Verify the first received WriteOp is for INSERT mod_type
  WriteOp op = change_stream_write_ops[0];
  auto* operation = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation, nullptr);
  // Verify column_types. Since column_types include column types tracked by the
  // change_stream_ and the change_stream_ tracks all, verify both the key
  // column and the tracked non_key column (string_col_) are included in
  // column_types.
  ASSERT_EQ(operation->values[6],
            zetasql::values::Array(
                zetasql::types::StringArrayType(),
                {zetasql::Value(String("int64_col")),
                 zetasql::Value(String("string_col")),
                 zetasql::Value(String("another_string_col"))}));
  JSON col_1_type;
  col_1_type["code"] = "INT64";
  JSON col_2_type;
  col_2_type["code"] = "STRING";
  JSON col_3_type;
  col_3_type["code"] = "STRING";
  ASSERT_EQ(
      operation->values[7],
      zetasql::values::Array(zetasql::types::StringArrayType(),
                               {zetasql::Value(String(col_1_type.dump())),
                                zetasql::Value(String(col_2_type.dump())),
                                zetasql::Value(String(col_3_type.dump()))}));
  ASSERT_EQ(operation->values[8],
            zetasql::values::Array(
                zetasql::types::BoolArrayType(),
                {zetasql::Value(Bool(true)), zetasql::Value(Bool(false)),
                 zetasql::Value(Bool(false))}));
  ASSERT_EQ(operation->values[9],
            zetasql::values::Array(
                zetasql::types::Int64ArrayType(),
                {zetasql::Value(Int64(1)), zetasql::Value(Int64(2)),
                 zetasql::Value(Int64(3))}));
  // Verify mods
  zetasql::Value mod_keys = operation->values[10];
  ASSERT_EQ(mod_keys.num_elements(), 2);
  ASSERT_EQ(mod_keys.element(0),
            zetasql::Value(String("{\"int64_col\":\"1\"}")));
  ASSERT_EQ(mod_keys.element(1),
            zetasql::Value(String("{\"int64_col\":\"2\"}")));
  zetasql::Value mod_new_values = operation->values[11];
  ASSERT_EQ(
      mod_new_values.element(0),
      zetasql::Value(String(
          "{\"another_string_col\":null,\"string_col\":\"string_value1\"}")));
  ASSERT_EQ(mod_new_values.element(1),
            zetasql::Value(String("{\"another_string_col\":\"another_string_"
                                    "value2\",\"string_col\":null}")));
  zetasql::Value mod_old_values = operation->values[12];
  ASSERT_EQ(mod_old_values.element(0), zetasql::Value(String("{}")));
}
}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
