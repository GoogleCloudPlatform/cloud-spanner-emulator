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

#include "google/protobuf/descriptor.pb.h"
#include "googlesql/public/json_value.h"
#include "googlesql/public/numeric_value.h"
#include "googlesql/public/type.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/log.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "absl/types/variant.h"
#include "backend/actions/ops.h"
#include "backend/datamodel/key.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "tests/common/actions.h"
#include "tests/common/schema_constructor.h"
#include "tests/common/test.pb.h"
#include "nlohmann/json_fwd.hpp"
#include "nlohmann/json.hpp"
namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {
using JSON = ::nlohmann::json;
using googlesql::JSONValue;
using googlesql::NumericValue;
using googlesql::values::Bool;
using googlesql::values::Double;
using googlesql::values::DoubleArray;
using googlesql::values::Float;
using googlesql::values::FloatArray;
using googlesql::values::Int64;
using googlesql::values::Json;
using googlesql::values::JsonArray;
using googlesql::values::Numeric;
using googlesql::values::NumericArray;
using googlesql::values::Proto;
using googlesql::values::String;

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
                            CREATE TABLE TestTable3 (
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
                        )",
                        R"(
                            CREATE CHANGE STREAM ChangeStream_ExcludeTxn FOR TestTable3 OPTIONS ( allow_txn_exclusion = true )
                        )"},
                    &type_factory_)
                    .value()),
        float_schema_(emulator::test::CreateSchemaFromDDL(
                          {
                              R"(
                            CREATE TABLE FloatTable (
                              int64_col INT64 NOT NULL,
                              float_col FLOAT32,
                              double_col FLOAT64,
                              float_arr ARRAY<FLOAT32>,
                              double_arr ARRAY<FLOAT64>
                            ) PRIMARY KEY (int64_col)
                          )",
                              R"(
                            CREATE CHANGE STREAM ChangeStream_FloatTable FOR FloatTable OPTIONS ( value_capture_type = 'NEW_VALUES' )
                        )"},
                          &type_factory_)
                          .value()),
        pg_schema_(
            emulator::test::CreateSchemaFromDDL(
                {
                    R"(
                          CREATE TABLE entended_pg_datatypes (
                            int_col bigint NOT NULL PRIMARY KEY,
                            jsonb_col jsonb,
                            jsonb_arr jsonb[],
                            numeric_col numeric,
                            numeric_arr numeric[]
                          )
                        )",
                    R"(CREATE CHANGE STREAM pg_stream FOR ALL WITH ( value_capture_type = 'NEW_VALUES' ))",
                },
                &type_factory_, "", /*proto_descriptor_bytes*/
                database_api::DatabaseDialect::POSTGRESQL)
                .value()),
        table_(schema_->FindTable("TestTable")),
        table2_(schema_->FindTable("TestTable2")),
        table3_(schema_->FindTable("TestTable3")),
        float_table_(float_schema_->FindTable("FloatTable")),
        pg_table_(pg_schema_->FindTable("entended_pg_datatypes")),
        base_columns_(table_->columns()),
        base_columns_table_2_all_col_(table2_->columns()),
        float_columns_(float_table_->columns()),
        pg_columns_(pg_table_->columns()),
        change_stream_(schema_->FindChangeStream("ChangeStream_All")),
        change_stream2_(
            schema_->FindChangeStream("ChangeStream_TestTable2StrCol")),
        change_stream3_(
            schema_->FindChangeStream("ChangeStream_TestTable2KeyOnly")),
        change_stream4_(schema_->FindChangeStream("ChangeStream_TestTable2")),
        change_stream_exclude_txn_(
            schema_->FindChangeStream("ChangeStream_ExcludeTxn")),
        float_change_stream_(
            float_schema_->FindChangeStream("ChangeStream_FloatTable")),
        pg_change_stream_(pg_schema_->FindChangeStream("pg_stream")) {}

 protected:
  // Test components.
  googlesql::TypeFactory type_factory_;
  std::unique_ptr<const Schema> schema_;
  std::unique_ptr<const Schema> float_schema_;
  std::unique_ptr<const Schema> pg_schema_;

  // Test variables.
  const Table* table_;
  const Table* table2_;
  const Table* table3_;
  const Table* float_table_;
  const Table* pg_table_;
  absl::Span<const Column* const> base_columns_;
  absl::Span<const Column* const> base_columns_table_2_all_col_;
  absl::Span<const Column* const> float_columns_;
  absl::Span<const Column* const> pg_columns_;
  const ChangeStream* change_stream_;
  const ChangeStream* change_stream2_;
  const ChangeStream* change_stream3_;
  const ChangeStream* change_stream4_;
  const ChangeStream* change_stream_exclude_txn_;
  const ChangeStream* float_change_stream_;
  const ChangeStream* pg_change_stream_;
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
  const std::vector<googlesql::Value> values = {
      googlesql::Value::String("11111"), googlesql::Value::NullTimestamp()};
  // Insert 1st partition to change_stream2_'s partition table
  GOOGLESQL_EXPECT_OK(store->Insert(change_stream->change_stream_partition_table(),
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
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::vector<WriteOp> change_stream_write_ops,
      BuildChangeStreamWriteOps(schema_.get(), buffered_write_ops, store(), 1,
                                /*exclude_txn_from_change_streams=*/false));
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
  ASSERT_EQ(operation->values[0], googlesql::Value::String("11111"));
  // Verify record_sequence
  ASSERT_EQ(operation->values[3], googlesql::Value(String("00000000")));
  // Verify is_last_record_in_transaction_in_partition
  ASSERT_EQ(operation->values[4], googlesql::Value(Bool(true)));
  // Verify table_name
  ASSERT_EQ(operation->values[5], googlesql::Value(String("TestTable")));
  // Verify column_types_name
  ASSERT_EQ(operation->values[6],
            googlesql::values::Array(
                googlesql::types::StringArrayType(),
                {googlesql::Value(String("int64_col")),
                 googlesql::Value(String("string_col")),
                 googlesql::Value(String("another_string_col"))}));
  // Verify column_types_type
  JSON col_1_type;
  col_1_type["code"] = "INT64";
  JSON col_2_type;
  col_2_type["code"] = "STRING";
  JSON col_3_type;
  col_3_type["code"] = "STRING";
  ASSERT_EQ(
      operation->values[7],
      googlesql::values::Array(googlesql::types::StringArrayType(),
                               {googlesql::Value(String(col_1_type.dump())),
                                googlesql::Value(String(col_2_type.dump())),
                                googlesql::Value(String(col_3_type.dump()))}));
  // Verify column_types_is_primary_key
  ASSERT_EQ(operation->values[8],
            googlesql::values::Array(
                googlesql::types::BoolArrayType(),
                {googlesql::Value(Bool(true)), googlesql::Value(Bool(false)),
                 googlesql::Value(Bool(false))}));
  // Verify column_types_ordinal_position
  ASSERT_EQ(operation->values[9],
            googlesql::values::Array(
                googlesql::types::Int64ArrayType(),
                {googlesql::Value(Int64(1)), googlesql::Value(Int64(2)),
                 googlesql::Value(Int64(3))}));
  // Verify mods
  googlesql::Value mod_keys = operation->values[10];
  ASSERT_EQ(mod_keys.element(0),
            googlesql::Value(String("{\"int64_col\":\"1\"}")));
  googlesql::Value mod_new_values = operation->values[11];
  ASSERT_EQ(mod_new_values.element(0),
            googlesql::Value(
                String("{\"another_string_col\":\"value2\",\"string_col\":"
                       "\"value\"}")));
  googlesql::Value mod_old_values = operation->values[12];
  ASSERT_EQ(mod_old_values.element(0), googlesql::Value(String("{}")));
  // Verify mod_type
  ASSERT_EQ(operation->values[13], googlesql::Value(String("INSERT")));
  // Verify value_capture_type
  ASSERT_EQ(operation->values[14], googlesql::Value(String("NEW_VALUES")));
  // Verify number_of_records_in_transaction
  ASSERT_EQ(operation->values[15], googlesql::Value(Int64(1)));
  // Verify number_of_partitions_in_transaction
  ASSERT_EQ(operation->values[16], googlesql::Value(Int64(1)));
  // Verify transaction_tag
  ASSERT_EQ(operation->values[17], googlesql::Value(String("")));
  // Verify is_system_transaction
  ASSERT_EQ(operation->values[18], googlesql::Value(Bool(false)));
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
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::vector<WriteOp> change_stream_write_ops,
      BuildChangeStreamWriteOps(schema_.get(), buffered_write_ops, store(), 1,
                                /*exclude_txn_from_change_streams=*/false));
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
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::vector<WriteOp> change_stream_write_ops,
      BuildChangeStreamWriteOps(schema_.get(), buffered_write_ops, store(), 1,
                                /*exclude_txn_from_change_streams=*/false));
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
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::vector<WriteOp> change_stream_write_ops,
      BuildChangeStreamWriteOps(schema_.get(), buffered_write_ops, store(), 1,
                                /*exclude_txn_from_change_streams=*/false));
  // Verify the number of change stream entries is added to the transaction
  // buffer.
  // Insert, Insert, Update, Update, Insert, Delete, Delete -> 4 WriteOps
  ASSERT_EQ(change_stream_write_ops.size(), 4);

  WriteOp op = change_stream_write_ops[0];
  // Verify the first received WriteOp is InsertOp
  auto* operation = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation, nullptr);
  // Verify mod_type
  ASSERT_EQ(operation->values[13], googlesql::Value(String("INSERT")));
  // column_type_names
  ASSERT_EQ(operation->values[3], googlesql::Value(String("00000000")));
  // Verify is_last_record_in_transaction_in_partition
  ASSERT_EQ(operation->values[4], googlesql::Value(Bool(false)));
  // Verify number_of_records_in_transaction
  ASSERT_EQ(operation->values[15], googlesql::Value(Int64(4)));
  // Verify the column_types of the 1st WriteOp (INSERT mod_type)
  ASSERT_EQ(operation->values[6],
            googlesql::values::Array(
                googlesql::types::StringArrayType(),
                {googlesql::Value(String("int64_col")),
                 googlesql::Value(String("string_col")),
                 googlesql::Value(String("another_string_col"))}));
  // Verify column_types_type
  JSON col_1_type;
  col_1_type["code"] = "INT64";
  JSON col_2_type;
  col_2_type["code"] = "STRING";
  JSON col_3_type;
  col_3_type["code"] = "STRING";
  ASSERT_EQ(
      operation->values[7],
      googlesql::values::Array(googlesql::types::StringArrayType(),
                               {googlesql::Value(String(col_1_type.dump())),
                                googlesql::Value(String(col_2_type.dump())),
                                googlesql::Value(String(col_3_type.dump()))}));
  ASSERT_EQ(operation->values[8],
            googlesql::values::Array(
                googlesql::types::BoolArrayType(),
                {googlesql::Value(Bool(true)), googlesql::Value(Bool(false)),
                 googlesql::Value(Bool(false))}));
  // Verify the mods of the 1st WriteOp (INSERT mod_type)
  googlesql::Value mod_keys = operation->values[10];
  ASSERT_EQ(mod_keys.num_elements(), 2);
  ASSERT_EQ(mod_keys.element(0),
            googlesql::Value(String("{\"int64_col\":\"1\"}")));
  ASSERT_EQ(mod_keys.element(1),
            googlesql::Value(String("{\"int64_col\":\"2\"}")));
  googlesql::Value mod_new_values = operation->values[11];
  ASSERT_EQ(mod_new_values.element(0),
            googlesql::Value(
                String("{\"another_string_col\":\"value2\",\"string_col\":"
                       "\"value\"}")));
  ASSERT_EQ(mod_new_values.element(1),
            googlesql::Value(String("{\"another_string_col\":\"value2_row2\","
                                    "\"string_col\":\"value_row2\"}")));
  googlesql::Value mod_old_values = operation->values[12];
  ASSERT_EQ(mod_old_values.element(0), googlesql::Value(String("{}")));
  ASSERT_EQ(mod_old_values.element(1), googlesql::Value(String("{}")));

  // Verify the 2nd received WriteOp (UPDATE mod_type)
  op = change_stream_write_ops[1];
  auto* operation2 = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation2, nullptr);
  ASSERT_EQ(operation2->values[3], googlesql::Value(String("00000001")));
  // Verify is_last_record_in_transaction_in_partition
  ASSERT_EQ(operation2->values[4], googlesql::Value(Bool(false)));
  // Verify mod_type
  ASSERT_EQ(operation2->values[13], googlesql::Value(String("UPDATE")));
  // Verify number_of_records_in_transaction
  ASSERT_EQ(operation2->values[15], googlesql::Value(Int64(4)));
  // Verify the column_types_name of the 2nd WriteOp (UPDATE mod_type)
  ASSERT_EQ(operation2->values[6],
            googlesql::values::Array(
                googlesql::types::StringArrayType(),
                {googlesql::Value(String("int64_col")),
                 googlesql::Value(String("string_col")),
                 googlesql::Value(String("another_string_col"))}));
  // Verify column_types_type
  ASSERT_EQ(
      operation2->values[7],
      googlesql::values::Array(googlesql::types::StringArrayType(),
                               {googlesql::Value(String(col_1_type.dump())),
                                googlesql::Value(String(col_2_type.dump())),
                                googlesql::Value(String(col_3_type.dump()))}));
  // Verify column_types_is_primary_key
  ASSERT_EQ(operation2->values[8],
            googlesql::values::Array(
                googlesql::types::BoolArrayType(),
                {googlesql::Value(Bool(true)), googlesql::Value(Bool(false)),
                 googlesql::Value(Bool(false))}));
  // Verify column_types_ordinal_position
  ASSERT_EQ(operation2->values[9],
            googlesql::values::Array(
                googlesql::types::Int64ArrayType(),
                {googlesql::Value(Int64(1)), googlesql::Value(Int64(2)),
                 googlesql::Value(Int64(3))}));
  // Verify the mods of the 2nd WriteOp (UPDATE mod_type)
  googlesql::Value mod_2_keys = operation->values[10];
  ASSERT_EQ(mod_2_keys.num_elements(), 2);
  ASSERT_EQ(mod_2_keys.element(0),
            googlesql::Value(String("{\"int64_col\":\"1\"}")));
  ASSERT_EQ(mod_2_keys.element(1),
            googlesql::Value(String("{\"int64_col\":\"2\"}")));
  googlesql::Value mod_2_new_values = operation->values[11];
  ASSERT_EQ(mod_2_new_values.element(0),
            googlesql::Value(
                String("{\"another_string_col\":\"updated_value2\",\"string_"
                       "col\":\"updated_value\"}")));
  ASSERT_EQ(
      mod_2_new_values.element(1),
      googlesql::Value(String("{\"another_string_col\":\"updated_value2_row2\","
                              "\"string_col\":\"updated_value_row2\"}")));
  googlesql::Value mod_2_old_values = operation->values[12];
  ASSERT_EQ(mod_2_old_values.element(0), googlesql::Value(String("{}")));
  ASSERT_EQ(mod_2_old_values.element(1), googlesql::Value(String("{}")));

  // Verify the 3rd received WriteOp (INSERT mod_type)
  op = change_stream_write_ops[2];
  auto* operation3 = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation3, nullptr);
  ASSERT_EQ(operation3->values[13], googlesql::Value(String("INSERT")));
  ASSERT_EQ(operation->values[3], googlesql::Value(String("00000002")));
  // Verify is_last_record_in_transaction_in_partition
  ASSERT_EQ(operation3->values[4], googlesql::Value(Bool(false)));
  // Verify number_of_records_in_transaction
  ASSERT_EQ(operation3->values[15], googlesql::Value(Int64(4)));

  // Verify the 4th(last) received WriteOp is DeleteOp
  op = change_stream_write_ops[3];
  auto operation4 = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation4, nullptr);
  ASSERT_EQ(operation4->values[3], googlesql::Value(String("00000003")));
  // Verify is_last_record_in_transaction_in_partition
  ASSERT_EQ(operation4->values[4], googlesql::Value(Bool(true)));
  // Verify mod_type
  ASSERT_EQ(operation4->values[13], googlesql::Value(String("DELETE")));
  // Verify number_of_records_in_transaction
  ASSERT_EQ(operation4->values[15], googlesql::Value(Int64(4)));

  // Verify the column_types of the 4th WriteOp (DELETE mod_type)
  ASSERT_EQ(operation4->values[6],
            googlesql::values::Array(
                googlesql::types::StringArrayType(),
                {googlesql::Value(String("int64_col")),
                 googlesql::Value(String("string_col")),
                 googlesql::Value(String("another_string_col"))}));
  // Verify column_types_type
  ASSERT_EQ(
      operation4->values[7],
      googlesql::values::Array(googlesql::types::StringArrayType(),
                               {googlesql::Value(String(col_1_type.dump())),
                                googlesql::Value(String(col_2_type.dump())),
                                googlesql::Value(String(col_3_type.dump()))}));
  // Verify column_types_is_primary_key
  ASSERT_EQ(operation4->values[8],
            googlesql::values::Array(
                googlesql::types::BoolArrayType(),
                {googlesql::Value(Bool(true)), googlesql::Value(Bool(false)),
                 googlesql::Value(Bool(false))}));
  // Verify column_types_ordinal_position
  ASSERT_EQ(operation4->values[9],
            googlesql::values::Array(
                googlesql::types::Int64ArrayType(),
                {googlesql::Value(Int64(1)), googlesql::Value(Int64(2)),
                 googlesql::Value(Int64(3))}));
  // Verify the mods of the 4th WriteOp (DELETE mod_type)
  googlesql::Value mod_4_keys = operation4->values[10];
  ASSERT_EQ(mod_4_keys.num_elements(), 2);
  ASSERT_EQ(mod_4_keys.element(0),
            googlesql::Value(String("{\"int64_col\":\"1\"}")));
  ASSERT_EQ(mod_4_keys.element(1),
            googlesql::Value(String("{\"int64_col\":\"2\"}")));
  googlesql::Value mod_4_new_values = operation4->values[11];
  ASSERT_EQ(mod_4_new_values.element(0), googlesql::Value(String("{}")));
  ASSERT_EQ(mod_4_new_values.element(1), googlesql::Value(String("{}")));
  googlesql::Value mod_4_old_values = operation4->values[12];
  ASSERT_EQ(mod_4_old_values.element(0), googlesql::Value(String("{}")));
  ASSERT_EQ(mod_4_old_values.element(1), googlesql::Value(String("{}")));
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
  ASSERT_THAT(LogTableMod(Insert(table_, Key({Int64(1)}), base_columns_,
                                 {Int64(1), String("value"), String("value2")}),
                          change_stream_, googlesql::Value::String("11111"),
                          &data_change_records_in_transaction_by_change_stream,
                          1, &last_mod_group_by_change_stream, store()),
              ::googlesql_base::testing::IsOk());
  // Insert base table entry to TestTable2.
  ASSERT_THAT(LogTableMod(Insert(table2_, Key({Int64(1)}),
                                 base_columns_table_2_all_col_,
                                 {Int64(1), String("value"), String("value2")}),
                          change_stream_, googlesql::Value::String("11111"),
                          &data_change_records_in_transaction_by_change_stream,
                          1, &last_mod_group_by_change_stream, store()),
              ::googlesql_base::testing::IsOk());
  // Insert base table entry to TestTable.
  ASSERT_THAT(LogTableMod(Insert(table_, Key({Int64(2)}), base_columns_,
                                 {Int64(2), String("value_row2"),
                                  String("value2_row2")}),
                          change_stream_, googlesql::Value::String("11111"),
                          &data_change_records_in_transaction_by_change_stream,
                          1, &last_mod_group_by_change_stream, store()),
              ::googlesql_base::testing::IsOk());

  // Set number_of_records_in_transaction in each DataChangeRecord after
  // finishing processing all operations
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::vector<WriteOp> write_ops,
      BuildMutation(&data_change_records_in_transaction_by_change_stream, 1,
                    &last_mod_group_by_change_stream));
  // Verify the number rebuilt WriteOps added to the transaction
  // buffer.
  ASSERT_EQ(write_ops.size(), 3);
  WriteOp op = write_ops[0];
  InsertOp* insert_op = std::get_if<InsertOp>(&op);
  ASSERT_NE(insert_op, nullptr);
  EXPECT_EQ(insert_op->values[5], googlesql::Value(String("TestTable")));
  op = write_ops[1];
  insert_op = std::get_if<InsertOp>(&op);
  ASSERT_NE(insert_op, nullptr);
  EXPECT_EQ(insert_op->values[5], googlesql::Value(String("TestTable2")));
  op = write_ops[2];
  insert_op = std::get_if<InsertOp>(&op);
  ASSERT_NE(insert_op, nullptr);
  EXPECT_EQ(insert_op->values[5], googlesql::Value(String("TestTable")));
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
  ASSERT_THAT(LogTableMod(Update(table_, Key({Int64(1)}),
                                 key_and_another_string_col_table_1_,
                                 {Int64(1), String("another_string_value1")}),
                          change_stream_, googlesql::Value::String("11111"),
                          &data_change_records_in_transaction_by_change_stream,
                          1, &last_mod_group_by_change_stream, store()),
              ::googlesql_base::testing::IsOk());
  // Insert base table entry to TestTable2.
  ASSERT_THAT(
      LogTableMod(Update(table_, Key({Int64(1)}), key_and_string_col_table_1_,
                         {Int64(1), String("string_value1")}),
                  change_stream_, googlesql::Value::String("11111"),
                  &data_change_records_in_transaction_by_change_stream, 1,
                  &last_mod_group_by_change_stream, store()),
      ::googlesql_base::testing::IsOk());
  // Insert base table entry to TestTable.
  ASSERT_THAT(LogTableMod(Update(table_, Key({Int64(2)}),
                                 key_and_another_string_col_table_1_,
                                 {Int64(2), String("another_string_value2")}),
                          change_stream_, googlesql::Value::String("11111"),
                          &data_change_records_in_transaction_by_change_stream,
                          1, &last_mod_group_by_change_stream, store()),
              ::googlesql_base::testing::IsOk());
  // Set number_of_records_in_transaction in each DataChangeRecord after
  // finishing processing all operations
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::vector<WriteOp> write_ops,
      BuildMutation(&data_change_records_in_transaction_by_change_stream, 1,
                    &last_mod_group_by_change_stream));
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
  ASSERT_THAT(
      LogTableMod(Insert(table2_, Key({Int64(1)}), key_and_string_col_table_2_,
                         {Int64(1), String("string_value1")}),
                  change_stream_, googlesql::Value::String("11111"),
                  &data_change_records_in_transaction_by_change_stream, 1,
                  &last_mod_group_by_change_stream, store()),
      ::googlesql_base::testing::IsOk());
  ASSERT_THAT(
      LogTableMod(Insert(table2_, Key({Int64(2)}), key_and_string_col_table_2_,
                         {Int64(2), String("string_value2")}),
                  change_stream2_, googlesql::Value::String("11111"),
                  &data_change_records_in_transaction_by_change_stream, 1,
                  &last_mod_group_by_change_stream, store()),
      ::googlesql_base::testing::IsOk());
  ASSERT_THAT(
      LogTableMod(Insert(table2_, Key({Int64(1)}), key_and_string_col_table_2_,
                         {Int64(3), String("string_value3")}),
                  change_stream_, googlesql::Value::String("11111"),
                  &data_change_records_in_transaction_by_change_stream, 1,
                  &last_mod_group_by_change_stream, store()),
      ::googlesql_base::testing::IsOk());
  ASSERT_THAT(LogTableMod(Insert(table2_, Key({Int64(1)}),
                                 key_and_another_string_col_table_2_,
                                 {Int64(4), String("another_string_value4")}),
                          change_stream_, googlesql::Value::String("11111"),
                          &data_change_records_in_transaction_by_change_stream,
                          1, &last_mod_group_by_change_stream, store()),
              ::googlesql_base::testing::IsOk());
  // Set number_of_records_in_transaction in each DataChangeRecord after
  // finishing processing all operations
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::vector<WriteOp> write_ops,
      BuildMutation(&data_change_records_in_transaction_by_change_stream, 1,
                    &last_mod_group_by_change_stream));
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
  ASSERT_THAT(LogTableMod(Insert(table2_, Key({Int64(1)}),
                                 key_and_another_string_col_table_2_,
                                 {Int64(1), String("another_string_value1")}),
                          change_stream3_, googlesql::Value::String("11111"),
                          &data_change_records_in_transaction_by_change_stream,
                          1, &last_mod_group_by_change_stream, store()),
              ::googlesql_base::testing::IsOk());
  // Update to an untracked column.
  ASSERT_THAT(
      LogTableMod(
          Update(table2_, Key({Int64(1)}), key_and_another_string_col_table_2_,
                 {Int64(1), String("another_string_value_update")}),
          change_stream3_, googlesql::Value::String("11111"),
          &data_change_records_in_transaction_by_change_stream, 1,
          &last_mod_group_by_change_stream, store()),
      ::googlesql_base::testing::IsOk());
  // Delete the row.
  ASSERT_THAT(LogTableMod(Delete(table2_, Key({Int64(1)})), change_stream3_,
                          googlesql::Value::String("11111"),
                          &data_change_records_in_transaction_by_change_stream,
                          1, &last_mod_group_by_change_stream, store()),
              ::googlesql_base::testing::IsOk());
  // Set number_of_records_in_transaction in each DataChangeRecord after
  // finishing processing all operations
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::vector<WriteOp> write_ops,
      BuildMutation(&data_change_records_in_transaction_by_change_stream, 1,
                    &last_mod_group_by_change_stream));
  // Verify the number of rebuilt WriteOps added to the transaction
  // buffer.
  ASSERT_EQ(write_ops.size(), 2);
  // Verify the first received WriteOp is for INSERT mod_type
  WriteOp op = write_ops[0];
  auto* operation = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation, nullptr);
  ASSERT_EQ(operation->values[13], googlesql::Value(String("INSERT")));
  // Verify column_types_name
  ASSERT_EQ(operation->values[6],
            googlesql::values::Array(googlesql::types::StringArrayType(),
                                     {googlesql::Value(String("int64_col"))}));
  // Verify column_types_type
  JSON col_1_type;
  col_1_type["code"] = "INT64";
  ASSERT_EQ(
      operation->values[7],
      googlesql::values::Array(googlesql::types::StringArrayType(),
                               {googlesql::Value(String(col_1_type.dump()))}));
  // Verify column_types_is_primary_key
  ASSERT_EQ(operation->values[8],
            googlesql::values::Array(googlesql::types::BoolArrayType(),
                                     {googlesql::Value(Bool(true))}));
  // Verify column_types_ordinal_position
  ASSERT_EQ(operation->values[9],
            googlesql::values::Array(googlesql::types::Int64ArrayType(),
                                     {googlesql::Value(Int64(1))}));

  // Since new_values field in mods field only contains non_key_col values,
  // new_values should be empty.
  googlesql::Value mod_keys = operation->values[10];
  ASSERT_EQ(mod_keys.num_elements(), 1);
  ASSERT_EQ(mod_keys.element(0),
            googlesql::Value(String("{\"int64_col\":\"1\"}")));
  googlesql::Value mod_new_values = operation->values[11];
  ASSERT_EQ(mod_new_values.element(0), googlesql::Value(String("{}")));
  googlesql::Value mod_old_values = operation->values[12];
  ASSERT_EQ(mod_old_values.element(0), googlesql::Value(String("{}")));

  // Verify the second received WriteOp is for DELETE mod_type
  op = write_ops[1];
  auto* operation2 = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation, nullptr);
  ASSERT_EQ(operation2->values[13], googlesql::Value(String("DELETE")));
  // Verify column_types_name
  ASSERT_EQ(operation->values[6],
            googlesql::values::Array(googlesql::types::StringArrayType(),
                                     {googlesql::Value(String("int64_col"))}));
  // Verify column_types_type
  ASSERT_EQ(
      operation->values[7],
      googlesql::values::Array(googlesql::types::StringArrayType(),
                               {googlesql::Value(String(col_1_type.dump()))}));
  // ASSERT_EQ(operation->values[7], googlesql::Value(String("int64_col")));
  // Verify column_types_is_primary_key
  ASSERT_EQ(operation->values[8],
            googlesql::values::Array(googlesql::types::BoolArrayType(),
                                     {googlesql::Value(Bool(true))}));
  // Verify column_types_ordinal_position
  ASSERT_EQ(operation->values[9],
            googlesql::values::Array(googlesql::types::Int64ArrayType(),
                                     {googlesql::Value(Int64(1))}));
  // Verify mods to be empty
  mod_keys = operation->values[10];
  ASSERT_EQ(mod_keys.num_elements(), 1);
  ASSERT_EQ(mod_keys.element(0),
            googlesql::Value(String("{\"int64_col\":\"1\"}")));
  mod_new_values = operation->values[11];
  ASSERT_EQ(mod_new_values.element(0), googlesql::Value(String("{}")));
  mod_old_values = operation->values[12];
  ASSERT_EQ(mod_old_values.element(0), googlesql::Value(String("{}")));
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
  ASSERT_THAT(LogTableMod(Insert(table2_, Key({Int64(1)}),
                                 key_and_another_string_col_table_2_,
                                 {Int64(1), String("another_string_value1")}),
                          change_stream2_, googlesql::Value::String("11111"),
                          &data_change_records_in_transaction_by_change_stream,
                          1, &last_mod_group_by_change_stream, store()),
              ::googlesql_base::testing::IsOk());
  // Update to an untracked column.
  ASSERT_THAT(
      LogTableMod(
          Update(table2_, Key({Int64(1)}), key_and_another_string_col_table_2_,
                 {Int64(1), String("another_string_value_update")}),
          change_stream2_, googlesql::Value::String("11111"),
          &data_change_records_in_transaction_by_change_stream, 1,
          &last_mod_group_by_change_stream, store()),
      ::googlesql_base::testing::IsOk());
  // Delete the row.
  ASSERT_THAT(LogTableMod(Delete(table2_, Key({Int64(1)})), change_stream2_,
                          googlesql::Value::String("11111"),
                          &data_change_records_in_transaction_by_change_stream,
                          1, &last_mod_group_by_change_stream, store()),
              ::googlesql_base::testing::IsOk());
  // Set number_of_records_in_transaction in each DataChangeRecord after
  // finishing processing all operations
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::vector<WriteOp> write_ops,
      BuildMutation(&data_change_records_in_transaction_by_change_stream, 1,
                    &last_mod_group_by_change_stream));
  // Verify the number of rebuilt WriteOps added to the transaction
  // buffer.
  ASSERT_EQ(write_ops.size(), 2);
  // Verify the first received WriteOp is for INSERT mod_type
  WriteOp op = write_ops[0];
  auto* operation = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation, nullptr);
  ASSERT_EQ(operation->values[13], googlesql::Value(String("INSERT")));
  // Verify is_last_record_in_transaction_in_partition
  ASSERT_EQ(operation->values[4], googlesql::Value(Bool(false)));
  // Verify number_of_records_in_transaction
  ASSERT_EQ(operation->values[15], googlesql::Value(Int64(2)));
  // Verify column_types_name
  ASSERT_EQ(operation->values[6],
            googlesql::values::Array(googlesql::types::StringArrayType(),
                                     {googlesql::Value(String("int64_col")),
                                      googlesql::Value(String("string_col"))}));
  // Verify column_types_type
  JSON col_1_type;
  col_1_type["code"] = "INT64";
  JSON col_2_type;
  col_2_type["code"] = "STRING";
  ASSERT_EQ(
      operation->values[7],
      googlesql::values::Array(googlesql::types::StringArrayType(),
                               {googlesql::Value(String(col_1_type.dump())),
                                googlesql::Value(String(col_2_type.dump()))}));
  // Verify column_types_is_primary_key
  ASSERT_EQ(operation->values[8],
            googlesql::values::Array(
                googlesql::types::BoolArrayType(),
                {googlesql::Value(Bool(true)), googlesql::Value(Bool(false))}));
  // Verify column_types_ordinal_position
  ASSERT_EQ(operation->values[9],
            googlesql::values::Array(
                googlesql::types::Int64ArrayType(),
                {googlesql::Value(Int64(1)), googlesql::Value(Int64(2))}));
  // Since new_values field in mods field only contains non_key_col values,
  // new_values should be empty.
  googlesql::Value mod_keys = operation->values[10];
  ASSERT_EQ(mod_keys.num_elements(), 1);
  ASSERT_EQ(mod_keys.element(0),
            googlesql::Value(String("{\"int64_col\":\"1\"}")));
  googlesql::Value mod_new_values = operation->values[11];
  ASSERT_EQ(mod_new_values.element(0),
            googlesql::Value(String("{\"string_col\":null}")));
  googlesql::Value mod_old_values = operation->values[12];
  ASSERT_EQ(mod_old_values.element(0), googlesql::Value(String("{}")));

  // Verify the second received WriteOp is for DELETE mod_type
  op = write_ops[1];
  auto* operation2 = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation2, nullptr);
  ASSERT_EQ(operation2->values[13], googlesql::Value(String("DELETE")));
  // Verify column_types_name
  ASSERT_EQ(operation2->values[6],
            googlesql::values::Array(googlesql::types::StringArrayType(),
                                     {googlesql::Value(String("int64_col")),
                                      googlesql::Value(String("string_col"))}));
  // Verify column_types_type
  ASSERT_EQ(
      operation2->values[7],
      googlesql::values::Array(googlesql::types::StringArrayType(),
                               {googlesql::Value(String(col_1_type.dump())),
                                googlesql::Value(String(col_2_type.dump()))}));
  // Verify column_types_is_primary_key
  ASSERT_EQ(operation2->values[8],
            googlesql::values::Array(
                googlesql::types::BoolArrayType(),
                {googlesql::Value(Bool(true)), googlesql::Value(Bool(false))}));
  // Verify column_types_ordinal_position
  ASSERT_EQ(operation2->values[9],
            googlesql::values::Array(
                googlesql::types::Int64ArrayType(),
                {googlesql::Value(Int64(1)), googlesql::Value(Int64(2))}));
  // Verify mods to be empty
  mod_keys = operation->values[10];
  ASSERT_EQ(mod_keys.num_elements(), 1);
  ASSERT_EQ(mod_keys.element(0),
            googlesql::Value(String("{\"int64_col\":\"1\"}")));
  mod_new_values = operation->values[11];
  ASSERT_EQ(mod_new_values.element(0), googlesql::Value(String("{}")));
  mod_old_values = operation->values[12];
  ASSERT_EQ(mod_old_values.element(0), googlesql::Value(String("{}")));
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
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::vector<WriteOp> change_stream_write_ops,
      BuildChangeStreamWriteOps(schema_.get(), buffered_write_ops, store(), 1,
                                /*exclude_txn_from_change_streams=*/false));
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
            googlesql::values::Array(
                googlesql::types::StringArrayType(),
                {googlesql::Value(String("int64_col")),
                 googlesql::Value(String("string_col")),
                 googlesql::Value(String("another_string_col"))}));
  JSON col_1_type;
  col_1_type["code"] = "INT64";
  JSON col_2_type;
  col_2_type["code"] = "STRING";
  JSON col_3_type;
  col_3_type["code"] = "STRING";
  ASSERT_EQ(
      operation->values[7],
      googlesql::values::Array(googlesql::types::StringArrayType(),
                               {googlesql::Value(String(col_1_type.dump())),
                                googlesql::Value(String(col_2_type.dump())),
                                googlesql::Value(String(col_3_type.dump()))}));
  ASSERT_EQ(operation->values[8],
            googlesql::values::Array(
                googlesql::types::BoolArrayType(),
                {googlesql::Value(Bool(true)), googlesql::Value(Bool(false)),
                 googlesql::Value(Bool(false))}));
  ASSERT_EQ(operation->values[9],
            googlesql::values::Array(
                googlesql::types::Int64ArrayType(),
                {googlesql::Value(Int64(1)), googlesql::Value(Int64(2)),
                 googlesql::Value(Int64(3))}));
  // Verify mods
  googlesql::Value mod_keys = operation->values[10];
  ASSERT_EQ(mod_keys.num_elements(), 2);
  ASSERT_EQ(mod_keys.element(0),
            googlesql::Value(String("{\"int64_col\":\"1\"}")));
  ASSERT_EQ(mod_keys.element(1),
            googlesql::Value(String("{\"int64_col\":\"2\"}")));
  googlesql::Value mod_new_values = operation->values[11];
  ASSERT_EQ(
      mod_new_values.element(0),
      googlesql::Value(String(
          "{\"another_string_col\":null,\"string_col\":\"string_value1\"}")));
  ASSERT_EQ(mod_new_values.element(1),
            googlesql::Value(String("{\"another_string_col\":\"another_string_"
                                    "value2\",\"string_col\":null}")));
  googlesql::Value mod_old_values = operation->values[12];
  ASSERT_EQ(mod_old_values.element(0), googlesql::Value(String("{}")));
}

TEST_F(ChangeStreamTest, VerifyTxnExclusion) {
  // Populate ChangeStream_All_partition_table and
  // ChangeStream_ExcludeTxn_partition_table with the initial partition
  // token
  set_up_partition_token_for_change_stream_partition_table(change_stream_,
                                                           store());
  set_up_partition_token_for_change_stream_partition_table(
      change_stream_exclude_txn_, store());
  std::vector<WriteOp> buffered_write_ops;
  buffered_write_ops.push_back(Insert(
      table3_, Key({Int64(1)}),
      {table3_->FindColumn("int64_col"), table3_->FindColumn("string_col")},
      {Int64(1), String("string_value1")}));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::vector<WriteOp> change_stream_write_exclude_txn_ops,
      BuildChangeStreamWriteOps(schema_.get(), buffered_write_ops, store(), 1,
                                /*exclude_txn_from_change_streams=*/true));
  // Verify the number of rebuilt WriteOps added to the transaction
  // buffer. Only ChangeStream_All that does not exclude the
  // transaction should have a WriteOp added to the transaction buffer.
  ASSERT_EQ(change_stream_write_exclude_txn_ops.size(), 1);

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::vector<WriteOp> change_stream_write_include_txn_ops,
      BuildChangeStreamWriteOps(schema_.get(), buffered_write_ops, store(), 1,
                                /*exclude_txn_from_change_streams=*/false));
  // Verify the number of rebuilt WriteOps added to the transaction
  // buffer. ChangeStream_ExcludeTxn and ChangeStream_All should both have a
  // WriteOp added to the transaction buffer.
  ASSERT_EQ(change_stream_write_include_txn_ops.size(), 2);
}

TEST_F(ChangeStreamTest, PgVerifyExtendedDatatypesValueAndType) {
  set_up_partition_token_for_change_stream_partition_table(pg_change_stream_,
                                                           store());
  // Insert base table entry.
  std::vector<WriteOp> buffered_write_ops;
  buffered_write_ops.push_back(Insert(
      pg_table_, Key({Int64(1)}), pg_columns_,
      {Int64(1), Json(JSONValue(static_cast<int64_t>(2024))),
       JsonArray({JSONValue(static_cast<int64_t>(1)),
                  JSONValue(static_cast<int64_t>(2))}),
       Numeric(11), NumericArray({NumericValue(22), NumericValue(33)})}));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::vector<WriteOp> change_stream_write_ops,
      BuildChangeStreamWriteOps(pg_schema_.get(), buffered_write_ops, store(),
                                1, /*exclude_txn_from_change_streams=*/false));
  // Verify change stream entry is added to the transaction buffer.
  ASSERT_EQ(change_stream_write_ops.size(), 1);
  WriteOp op = change_stream_write_ops[0];
  // Verify the table of the received WriteOp
  ASSERT_EQ(TableOf(op), pg_change_stream_->change_stream_data_table());
  // Verify the received WriteOp is InsertOp
  auto* operation = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation, nullptr);
  // Verify columns in the rebuilt InsertOp corresponds to columns in
  // change_stream_data_table
  ASSERT_EQ(operation->columns,
            pg_change_stream_->change_stream_data_table()->columns());
  ASSERT_EQ(operation->columns.size(), 19);
  ASSERT_EQ(operation->values.size(), 19);
  // Verify values in the rebuilt InsertOp are correct
  // Verify partition_token
  ASSERT_EQ(operation->values[0], googlesql::Value::String("11111"));
  // Verify record_sequence
  ASSERT_EQ(operation->values[3], googlesql::Value(String("00000000")));
  // Verify is_last_record_in_transaction_in_partition
  ASSERT_EQ(operation->values[4], googlesql::Value(Bool(true)));
  // Verify table_name
  ASSERT_EQ(operation->values[5],
            googlesql::Value(String("entended_pg_datatypes")));
  // Verify column_types_name
  ASSERT_EQ(
      operation->values[6],
      googlesql::values::Array(googlesql::types::StringArrayType(),
                               {googlesql::Value(String("int_col")),
                                googlesql::Value(String("jsonb_col")),
                                googlesql::Value(String("jsonb_arr")),
                                googlesql::Value(String("numeric_col")),
                                googlesql::Value(String("numeric_arr"))}));
  // Verify column_types_type
  JSON int_type;
  int_type["code"] = "INT64";
  JSON jsonb_type;
  jsonb_type["code"] = "JSON";
  jsonb_type["type_annotation"] = "PG_JSONB";
  JSON json_arr_type;
  json_arr_type["code"] = "ARRAY";
  json_arr_type["array_element_type"]["code"] = "JSON";
  json_arr_type["array_element_type"]["type_annotation"] = "PG_JSONB";
  JSON numeric_type;
  numeric_type["code"] = "NUMERIC";
  numeric_type["type_annotation"] = "PG_NUMERIC";
  JSON numeric_arr_type;
  numeric_arr_type["code"] = "ARRAY";
  numeric_arr_type["array_element_type"]["code"] = "NUMERIC";
  numeric_arr_type["array_element_type"]["type_annotation"] = "PG_NUMERIC";
  ASSERT_EQ(operation->values[7],
            googlesql::values::Array(
                googlesql::types::StringArrayType(),
                {googlesql::Value(String(int_type.dump())),
                 googlesql::Value(String(jsonb_type.dump())),
                 googlesql::Value(String(json_arr_type.dump())),
                 googlesql::Value(String(numeric_type.dump())),
                 googlesql::Value(String(numeric_arr_type.dump()))}));
  // Verify column_types_is_primary_key
  ASSERT_EQ(operation->values[8],
            googlesql::values::Array(
                googlesql::types::BoolArrayType(),
                {googlesql::Value(Bool(true)), googlesql::Value(Bool(false)),
                 googlesql::Value(Bool(false)), googlesql::Value(Bool(false)),
                 googlesql::Value(Bool(false))}));
  // Verify column_types_ordinal_position
  ASSERT_EQ(operation->values[9],
            googlesql::values::Array(
                googlesql::types::Int64ArrayType(),
                {googlesql::Value(Int64(1)), googlesql::Value(Int64(2)),
                 googlesql::Value(Int64(3)), googlesql::Value(Int64(4)),
                 googlesql::Value(Int64(5))}));
  // Verify mods
  googlesql::Value mod_keys = operation->values[10];
  ASSERT_EQ(mod_keys.element(0),
            googlesql::Value(String("{\"int_col\":\"1\"}")));
  googlesql::Value mod_new_values = operation->values[11];
  ASSERT_EQ(
      mod_new_values.element(0),
      googlesql::Value(String(
          R"({"jsonb_arr":["1","2"],"jsonb_col":"2024","numeric_arr":["22","33"],"numeric_col":"11"})")));
  googlesql::Value mod_old_values = operation->values[12];
  ASSERT_EQ(mod_old_values.element(0), googlesql::Value(String("{}")));
  // Verify mod_type
  ASSERT_EQ(operation->values[13], googlesql::Value(String("INSERT")));
  // Verify value_capture_type
  ASSERT_EQ(operation->values[14], googlesql::Value(String("NEW_VALUES")));
  // Verify number_of_records_in_transaction
  ASSERT_EQ(operation->values[15], googlesql::Value(Int64(1)));
  // Verify number_of_partitions_in_transaction
  ASSERT_EQ(operation->values[16], googlesql::Value(Int64(1)));
  // Verify transaction_tag
  ASSERT_EQ(operation->values[17], googlesql::Value(String("")));
  // Verify is_system_transaction
  ASSERT_EQ(operation->values[18], googlesql::Value(Bool(false)));
}

TEST_F(ChangeStreamTest, FloatValueAndTypes) {
  set_up_partition_token_for_change_stream_partition_table(float_change_stream_,
                                                           store());
  // Insert base table entry.
  std::vector<WriteOp> buffered_write_ops;
  buffered_write_ops.push_back(
      Insert(float_table_, Key({Int64(1)}), float_columns_,
             {Int64(1), Float(1.1f), Double(2.2), FloatArray({1.1f, 3.14f}),
              DoubleArray({2.2, 2.71})}));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::vector<WriteOp> change_stream_write_ops,
                       BuildChangeStreamWriteOps(
                           float_schema_.get(), buffered_write_ops, store(), 1,
                           /*exclude_txn_from_change_streams=*/false));

  // Verify change stream entry is added to the transaction buffer.
  ASSERT_EQ(change_stream_write_ops.size(), 1);
  WriteOp op = change_stream_write_ops[0];
  // Verify the table of the received WriteOp
  ASSERT_EQ(TableOf(op), float_change_stream_->change_stream_data_table());
  // Verify the received WriteOp is InsertOp
  auto* operation = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation, nullptr);
  // Verify columns in the rebuilt InsertOp corresponds to columns in
  // change_stream_data_table
  ASSERT_EQ(operation->columns,
            float_change_stream_->change_stream_data_table()->columns());

  // Verify values in the rebuilt InsertOp are correct
  // Verify partition_token
  ASSERT_EQ(operation->values[0], googlesql::Value::String("11111"));
  // Verify record_sequence
  ASSERT_EQ(operation->values[3], googlesql::Value(String("00000000")));
  // Verify is_last_record_in_transaction_in_partition
  ASSERT_EQ(operation->values[4], googlesql::Value(Bool(true)));
  // Verify table_name
  ASSERT_EQ(operation->values[5], googlesql::Value(String("FloatTable")));
  // Verify column_types_name
  ASSERT_EQ(operation->values[6],
            googlesql::values::Array(googlesql::types::StringArrayType(),
                                     {googlesql::Value(String("int64_col")),
                                      googlesql::Value(String("float_col")),
                                      googlesql::Value(String("double_col")),
                                      googlesql::Value(String("float_arr")),
                                      googlesql::Value(String("double_arr"))}));
  // Verify column_types_type
  JSON int_type;
  int_type["code"] = "INT64";
  JSON float32_type;
  float32_type["code"] = "FLOAT32";
  JSON float32_arr_type;
  float32_arr_type["code"] = "ARRAY";
  float32_arr_type["array_element_type"]["code"] = "FLOAT32";
  JSON float64_type;
  float64_type["code"] = "FLOAT64";
  JSON float64_arr_type;
  float64_arr_type["code"] = "ARRAY";
  float64_arr_type["array_element_type"]["code"] = "FLOAT64";
  ASSERT_EQ(operation->values[7],
            googlesql::values::Array(
                googlesql::types::StringArrayType(),
                {googlesql::Value(String(int_type.dump())),
                 googlesql::Value(String(float32_type.dump())),
                 googlesql::Value(String(float64_type.dump())),
                 googlesql::Value(String(float32_arr_type.dump())),
                 googlesql::Value(String(float64_arr_type.dump()))}));
  // Verify column_types_is_primary_key
  ASSERT_EQ(operation->values[8],
            googlesql::values::Array(
                googlesql::types::BoolArrayType(),
                {googlesql::Value(Bool(true)), googlesql::Value(Bool(false)),
                 googlesql::Value(Bool(false)), googlesql::Value(Bool(false)),
                 googlesql::Value(Bool(false))}));
  // Verify column_types_ordinal_position
  ASSERT_EQ(operation->values[9],
            googlesql::values::Array(
                googlesql::types::Int64ArrayType(),
                {googlesql::Value(Int64(1)), googlesql::Value(Int64(2)),
                 googlesql::Value(Int64(3)), googlesql::Value(Int64(4)),
                 googlesql::Value(Int64(5))}));
  // Verify mods
  googlesql::Value mod_keys = operation->values[10];
  ASSERT_EQ(mod_keys.element(0),
            googlesql::Value(String("{\"int64_col\":\"1\"}")));
  googlesql::Value mod_new_values = operation->values[11];
  ASSERT_EQ(
      mod_new_values.element(0),
      googlesql::Value(String(
          R"({"double_arr":[2.2,2.71],"double_col":2.2,"float_arr":[1.100000023841858,3.140000104904175],"float_col":1.100000023841858})")));
  googlesql::Value mod_old_values = operation->values[12];
  ASSERT_EQ(mod_old_values.element(0), googlesql::Value(String("{}")));
  // Verify mod_type
  ASSERT_EQ(operation->values[13], googlesql::Value(String("INSERT")));
  // Verify value_capture_type
  ASSERT_EQ(operation->values[14], googlesql::Value(String("NEW_VALUES")));
  // Verify number_of_records_in_transaction
  ASSERT_EQ(operation->values[15], googlesql::Value(Int64(1)));
  // Verify number_of_partitions_in_transaction
  ASSERT_EQ(operation->values[16], googlesql::Value(Int64(1)));
  // Verify transaction_tag
  ASSERT_EQ(operation->values[17], googlesql::Value(String("")));
  // Verify is_system_transaction
  ASSERT_EQ(operation->values[18], googlesql::Value(Bool(false)));
}

TEST_F(ChangeStreamTest, ProtoValueAndTypes) {
  // Setup proto schema
  google::protobuf::FileDescriptorSet file_descriptor_set;
  ::emulator::tests::common::Simple::descriptor()->file()->CopyTo(
      file_descriptor_set.add_file());
  std::string proto_descriptors = file_descriptor_set.SerializeAsString();

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto proto_schema,
                       emulator::test::CreateSchemaFromDDL(
                           {
                               R"(
              CREATE PROTO BUNDLE (
                emulator.tests.common.Simple,
                emulator.tests.common.TestEnum,
              )
          )",
                               R"(
              CREATE TABLE ProtoTable (
                int64_col INT64 NOT NULL,
                proto_col emulator.tests.common.Simple,
                proto_arr ARRAY<emulator.tests.common.Simple>
              ) PRIMARY KEY (int64_col)
          )",
                               R"(
              CREATE CHANGE STREAM ChangeStream_ProtoTable FOR ProtoTable OPTIONS ( value_capture_type = 'NEW_VALUES' )
          )"},
                           &type_factory_, proto_descriptors));

  const Table* proto_table = proto_schema->FindTable("ProtoTable");
  const ChangeStream* proto_stream =
      proto_schema->FindChangeStream("ChangeStream_ProtoTable");

  set_up_partition_token_for_change_stream_partition_table(proto_stream,
                                                           store());

  const googlesql::Type* proto_type =
      proto_table->FindColumn("proto_col")->GetType();

  // Insert base table entry.
  ::emulator::tests::common::Simple simple_proto;
  simple_proto.set_field("test_field");
  std::string encoded_proto =
      absl::Base64Escape(simple_proto.SerializeAsString());

  const googlesql::Type* proto_arr_type;
  GOOGLESQL_ASSERT_OK(type_factory_.MakeArrayType(proto_type, &proto_arr_type));

  std::vector<WriteOp> buffered_write_ops;
  buffered_write_ops.push_back(
      Insert(proto_table, Key({Int64(1)}), proto_table->columns(),
             {Int64(1), Proto(proto_type->AsProto(), simple_proto),
              googlesql::values::Array(
                  proto_arr_type->AsArray(),
                  {Proto(proto_type->AsProto(), simple_proto)})}));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::vector<WriteOp> change_stream_write_ops,
                       BuildChangeStreamWriteOps(
                           proto_schema.get(), buffered_write_ops, store(), 1,
                           /*exclude_txn_from_change_streams=*/false));

  // Verify change stream entry is added to the transaction buffer.
  ASSERT_EQ(change_stream_write_ops.size(), 1);
  WriteOp op = change_stream_write_ops[0];
  auto* operation = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation, nullptr);

  // Verify column_types_type tags PROTO properly
  JSON int_type;
  int_type["code"] = "INT64";
  JSON proto_json_type;
  proto_json_type["code"] = "PROTO";
  JSON proto_arr_json_type;
  proto_arr_json_type["code"] = "ARRAY";
  proto_arr_json_type["array_element_type"]["code"] = "PROTO";
  ASSERT_EQ(operation->values[7],
            googlesql::values::Array(
                googlesql::types::StringArrayType(),
                {googlesql::Value(String(int_type.dump())),
                 googlesql::Value(String(proto_json_type.dump())),
                 googlesql::Value(String(proto_arr_json_type.dump()))}));

  // Verify mods includes Base64 representations
  googlesql::Value mod_new_values = operation->values[11];
  ASSERT_EQ(mod_new_values.element(0),
            googlesql::Value(String(
                absl::StrCat("{\"proto_arr\":[\"", encoded_proto,
                             "\"],\"proto_col\":\"", encoded_proto, "\"}"))));
}

TEST_F(ChangeStreamTest, TimestampValueAndTypes) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto t_schema, emulator::test::CreateSchemaFromDDL(
                                          {
                                              R"(
              CREATE TABLE TimestampTable (
                commit_ts TIMESTAMP NOT NULL,
                k INT64
              ) PRIMARY KEY (commit_ts, k)
          )",
                                              R"(
              CREATE CHANGE STREAM ChangeStream_TimestampTable FOR TimestampTable OPTIONS ( value_capture_type = 'NEW_VALUES' )
          )"},
                                          &type_factory_));

  const Table* t_table = t_schema->FindTable("TimestampTable");
  const ChangeStream* t_stream =
      t_schema->FindChangeStream("ChangeStream_TimestampTable");

  set_up_partition_token_for_change_stream_partition_table(t_stream, store());

  // Insert base table entries with fractional timestamp.
  absl::Time time1;
  std::string err;
  ASSERT_TRUE(absl::ParseTime("%Y-%m-%dT%H:%M:%E*SZ",
                              "1970-01-21T14:09:51.123456789Z", &time1, &err))
      << err;
  googlesql::Value ts_val1 = googlesql::Value::Timestamp(time1);

  absl::Time time2;
  ASSERT_TRUE(absl::ParseTime("%Y-%m-%dT%H:%M:%E*SZ",
                              "1970-01-21T14:09:51.123000000Z", &time2, &err))
      << err;
  googlesql::Value ts_val2 = googlesql::Value::Timestamp(time2);

  std::vector<WriteOp> buffered_write_ops;
  buffered_write_ops.push_back(Insert(t_table, Key({ts_val1, Int64(42)}),
                                      t_table->columns(),
                                      {ts_val1, Int64(42)}));
  buffered_write_ops.push_back(Insert(t_table, Key({ts_val2, Int64(43)}),
                                      t_table->columns(),
                                      {ts_val2, Int64(43)}));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::vector<WriteOp> change_stream_write_ops,
      BuildChangeStreamWriteOps(t_schema.get(), buffered_write_ops, store(), 1,
                                /*exclude_txn_from_change_streams=*/false));

  // Verify change stream entry is added to the transaction buffer.
  ASSERT_EQ(change_stream_write_ops.size(), 1);
  WriteOp op = change_stream_write_ops[0];
  auto* operation = std::get_if<InsertOp>(&op);
  ASSERT_NE(operation, nullptr);

  // Verify elements in the primary key show correct formatted timestamp
  // preserving fractional digits and trailing Z.
  googlesql::Value mod_keys = operation->values[10];
  ASSERT_EQ(mod_keys.num_elements(), 2);
  ASSERT_EQ(
      mod_keys.element(0),
      googlesql::Value(String(
          "{\"commit_ts\":\"1970-01-21T14:09:51.123456789Z\",\"k\":\"42\"}")));
  ASSERT_EQ(mod_keys.element(1),
            googlesql::Value(String(
                "{\"commit_ts\":\"1970-01-21T14:09:51.123Z\",\"k\":\"43\"}")));
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
