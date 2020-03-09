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

#include "backend/transaction/row_cursor.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "backend/storage/in_memory_iterator.h"
#include "tests/common/schema_constructor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

using zetasql::Value;
using zetasql::values::Int64;
using zetasql::values::String;

class StorageIteratorRowCursorTest : public testing::Test {
 public:
  StorageIteratorRowCursorTest()
      : type_factory_(absl::make_unique<zetasql::TypeFactory>()),
        schema_(test::CreateSchemaWithOneTable(type_factory_.get())) {
    std::vector<std::string> columns = {"int64_col", "string_col"};
    auto table = schema_->FindTable("test_table");
    for (auto column_name : columns) {
      const Column* column = table->FindColumn(column_name);
      columns_.push_back(column);
    }
  }

 protected:
  // The type factory must outlive the type objects that it has made.
  std::unique_ptr<zetasql::TypeFactory> type_factory_;
  std::unique_ptr<const Schema> schema_;
  std::vector<std::unique_ptr<StorageIterator>> iterators_;
  std::vector<const Column*> columns_;
};

TEST_F(StorageIteratorRowCursorTest, CreateWithNoIterators) {
  StorageIteratorRowCursor rowc(std::move(iterators_), std::move(columns_));

  EXPECT_EQ(2, rowc.NumColumns());
  EXPECT_EQ("int64_col", rowc.ColumnName(0));
  EXPECT_EQ("string_col", rowc.ColumnName(1));
  EXPECT_TRUE(rowc.ColumnType(0)->IsInt64());
  EXPECT_TRUE(rowc.ColumnType(1)->IsString());

  EXPECT_FALSE(rowc.Next());
  ZETASQL_EXPECT_OK(rowc.Status());
}

TEST_F(StorageIteratorRowCursorTest, CreateWithEmptyIterator) {
  iterators_.push_back(absl::make_unique<FixedRowStorageIterator>());

  StorageIteratorRowCursor rowc(std::move(iterators_), std::move(columns_));

  EXPECT_EQ(2, rowc.NumColumns());
  EXPECT_EQ("int64_col", rowc.ColumnName(0));
  EXPECT_EQ("string_col", rowc.ColumnName(1));
  EXPECT_TRUE(rowc.ColumnType(0)->IsInt64());
  EXPECT_TRUE(rowc.ColumnType(1)->IsString());

  EXPECT_FALSE(rowc.Next());
  ZETASQL_EXPECT_OK(rowc.Status());
}

TEST_F(StorageIteratorRowCursorTest, CreateWithUnaryLengthIterator) {
  std::vector<std::pair<Key, std::vector<Value>>> row_values = {
      {Key({Int64(2)}), {Int64(20), String("test_string1")}}};
  iterators_.push_back(
      absl::make_unique<FixedRowStorageIterator>(std::move(row_values)));

  StorageIteratorRowCursor rowc(std::move(iterators_), std::move(columns_));

  EXPECT_EQ(2, rowc.NumColumns());
  EXPECT_EQ("int64_col", rowc.ColumnName(0));
  EXPECT_EQ("string_col", rowc.ColumnName(1));
  EXPECT_TRUE(rowc.ColumnType(0)->IsInt64());
  EXPECT_TRUE(rowc.ColumnType(1)->IsString());

  while (rowc.Next()) {
    EXPECT_EQ(Int64(20), rowc.ColumnValue(0));
    EXPECT_EQ(String("test_string1"), rowc.ColumnValue(1));
  }
  EXPECT_FALSE(rowc.Next());
  ZETASQL_EXPECT_OK(rowc.Status());
}

TEST_F(StorageIteratorRowCursorTest, ConvertsInvalidValuesToNulls) {
  std::vector<std::pair<Key, std::vector<Value>>> row_values = {
      {Key({Int64(2)}), {Int64(20), zetasql::Value()}}};
  iterators_.push_back(
      absl::make_unique<FixedRowStorageIterator>(std::move(row_values)));

  StorageIteratorRowCursor rowc(std::move(iterators_), std::move(columns_));

  EXPECT_EQ(2, rowc.NumColumns());
  EXPECT_EQ("int64_col", rowc.ColumnName(0));
  EXPECT_EQ("string_col", rowc.ColumnName(1));
  EXPECT_TRUE(rowc.ColumnType(0)->IsInt64());
  EXPECT_TRUE(rowc.ColumnType(1)->IsString());

  while (rowc.Next()) {
    EXPECT_EQ(Int64(20), rowc.ColumnValue(0));
    EXPECT_EQ(zetasql::values::NullString(), rowc.ColumnValue(1));
  }
  EXPECT_FALSE(rowc.Next());
  ZETASQL_EXPECT_OK(rowc.Status());
}

TEST_F(StorageIteratorRowCursorTest, CreateWithEmptyAndNonEmptyIterators) {
  iterators_.push_back(absl::make_unique<FixedRowStorageIterator>());
  std::vector<std::pair<Key, std::vector<Value>>> row_values = {
      {Key({Int64(2)}), {Int64(20), String("test_string1")}}};
  iterators_.push_back(
      absl::make_unique<FixedRowStorageIterator>(std::move(row_values)));

  StorageIteratorRowCursor rowc(std::move(iterators_), std::move(columns_));

  EXPECT_EQ(2, rowc.NumColumns());
  EXPECT_EQ("int64_col", rowc.ColumnName(0));
  EXPECT_EQ("string_col", rowc.ColumnName(1));
  EXPECT_TRUE(rowc.ColumnType(0)->IsInt64());
  EXPECT_TRUE(rowc.ColumnType(1)->IsString());

  while (rowc.Next()) {
    EXPECT_EQ(Int64(20), rowc.ColumnValue(0));
    EXPECT_EQ(String("test_string1"), rowc.ColumnValue(1));
  }
  EXPECT_FALSE(rowc.Next());
  ZETASQL_EXPECT_OK(rowc.Status());
}

TEST_F(StorageIteratorRowCursorTest, CreateWithMultiRowIterators) {
  std::vector<std::pair<Key, std::vector<Value>>> row_values = {
      {Key({Int64(1)}), {Int64(10), String("test_string1")}},
      {Key({Int64(2)}), {Int64(20), String("test_string2")}},
      {Key({Int64(3)}), {Int64(30), String("test_string3")}}};
  iterators_.push_back(
      absl::make_unique<FixedRowStorageIterator>(std::move(row_values)));

  StorageIteratorRowCursor rowc(std::move(iterators_), std::move(columns_));

  EXPECT_EQ(2, rowc.NumColumns());
  EXPECT_EQ("int64_col", rowc.ColumnName(0));
  EXPECT_EQ("string_col", rowc.ColumnName(1));
  EXPECT_TRUE(rowc.ColumnType(0)->IsInt64());
  EXPECT_TRUE(rowc.ColumnType(1)->IsString());

  int row_count = 0;
  while (rowc.Next()) {
    row_count += 1;
    EXPECT_EQ(Int64(10 * row_count), rowc.ColumnValue(0));
    EXPECT_EQ(String("test_string" + std::to_string(row_count)),
              rowc.ColumnValue(1));
  }
  EXPECT_EQ(3, row_count);
  EXPECT_FALSE(rowc.Next());
  ZETASQL_EXPECT_OK(rowc.Status());
}

TEST_F(StorageIteratorRowCursorTest, CreateRowCursorMultipleMultiRowIterators) {
  std::vector<std::pair<Key, std::vector<Value>>> row_values = {
      {Key({Int64(1)}), {Int64(10), String("test_string1")}},
      {Key({Int64(2)}), {Int64(20), String("test_string2")}},
      {Key({Int64(3)}), {Int64(30), String("test_string3")}}};
  iterators_.push_back(
      absl::make_unique<FixedRowStorageIterator>(std::move(row_values)));
  // insert empty iterator in middle.
  iterators_.push_back(absl::make_unique<FixedRowStorageIterator>());
  row_values = {{Key({Int64(4)}), {Int64(40), String("test_string4")}},
                {Key({Int64(5)}), {Int64(50), String("test_string5")}}},
  iterators_.push_back(
      absl::make_unique<FixedRowStorageIterator>(std::move(row_values)));
  // insert empty iterator at end.
  iterators_.push_back(absl::make_unique<FixedRowStorageIterator>());

  StorageIteratorRowCursor rowc(std::move(iterators_), std::move(columns_));

  EXPECT_EQ(2, rowc.NumColumns());
  EXPECT_EQ("int64_col", rowc.ColumnName(0));
  EXPECT_EQ("string_col", rowc.ColumnName(1));
  EXPECT_TRUE(rowc.ColumnType(0)->IsInt64());
  EXPECT_TRUE(rowc.ColumnType(1)->IsString());

  int row_count = 0;
  while (rowc.Next()) {
    row_count += 1;
    EXPECT_EQ(Int64(10 * row_count), rowc.ColumnValue(0));
    EXPECT_EQ(String("test_string" + std::to_string(row_count)),
              rowc.ColumnValue(1));
  }
  EXPECT_EQ(5, row_count);
  EXPECT_FALSE(rowc.Next());
  ZETASQL_EXPECT_OK(rowc.Status());
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
