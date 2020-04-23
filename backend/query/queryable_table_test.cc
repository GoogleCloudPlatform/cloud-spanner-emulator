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

#include "backend/query/queryable_table.h"

#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "backend/access/read.h"
#include "backend/query/catalog.h"
#include "backend/query/queryable_column.h"
#include "tests/common/row_cursor.h"
#include "tests/common/row_reader.h"
#include "tests/common/schema_constructor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

using testing::ElementsAre;

class QueryableTableTest : public testing::Test {
 public:
  const Schema* schema() { return schema_.get(); }
  RowReader* reader() { return &reader_; }

 private:
  zetasql::TypeFactory type_factory_;
  std::unique_ptr<const Schema> schema_ =
      test::CreateSchemaWithOneTable(&type_factory_);
  test::TestRowReader reader_{
      {{"test_table",
        {{"int64_col", "string_col"},
         {zetasql::types::Int64Type(), zetasql::types::StringType()},
         {{zetasql::values::Int64(42), zetasql::values::String("foo")}}}}}};
};

TEST_F(QueryableTableTest, FindColumnByName) {
  const auto* schema_table = schema()->FindTable("test_table");
  QueryableTable table{schema_table, reader()};
  const auto* column = table.FindColumnByName("string_col");
  EXPECT_NE(column, nullptr);
  const QueryableColumn* queryable_column =
      dynamic_cast<const QueryableColumn*>(column);
  EXPECT_NE(queryable_column, nullptr);
  EXPECT_EQ(queryable_column->wrapped_column(),
            schema_table->FindColumn("string_col"));
}

TEST_F(QueryableTableTest, PrimaryKey) {
  const auto* schema_table = schema()->FindTable("test_table");
  QueryableTable table{schema_table, reader()};
  ASSERT_TRUE(table.PrimaryKey().has_value());
  EXPECT_THAT(table.PrimaryKey().value(),
              ElementsAre(0 /* index of int64_col*/));
}

TEST_F(QueryableTableTest, CreateEvaluatorTableIteratorWithZeroColumns) {
  QueryableTable table{schema()->FindTable("test_table"), reader()};
  auto iterator =
      table.CreateEvaluatorTableIterator(/*column_idxs=*/{}).value();
  ASSERT_EQ(iterator->NumColumns(), 0);
  ASSERT_TRUE(iterator->NextRow());
  ZETASQL_ASSERT_OK(iterator->Status());
  ASSERT_FALSE(iterator->NextRow());
}

TEST_F(QueryableTableTest, CreateEvaluatorTableIteratorWithTheSecondColumn) {
  QueryableTable table{schema()->FindTable("test_table"), reader()};
  auto iterator =
      table.CreateEvaluatorTableIterator(/*column_idxs=*/{1}).value();
  ASSERT_EQ(iterator->NumColumns(), 1);
  EXPECT_EQ(iterator->GetColumnName(0), "string_col");
  EXPECT_TRUE(iterator->GetColumnType(0)->IsString());
  ASSERT_TRUE(iterator->NextRow());
  ZETASQL_ASSERT_OK(iterator->Status());
  EXPECT_EQ(iterator->GetValue(0).string_value(), "foo");
  ASSERT_FALSE(iterator->NextRow());
}

TEST_F(QueryableTableTest, CreateEvaluatorTableIteratorWithAllColumns) {
  QueryableTable table{schema()->FindTable("test_table"), reader()};
  auto iterator =
      table.CreateEvaluatorTableIterator(/*column_idxs=*/{0, 1}).value();
  ASSERT_EQ(iterator->NumColumns(), 2);
  EXPECT_EQ(iterator->GetColumnName(0), "int64_col");
  EXPECT_EQ(iterator->GetColumnName(1), "string_col");
  EXPECT_TRUE(iterator->GetColumnType(0)->IsInt64());
  EXPECT_TRUE(iterator->GetColumnType(1)->IsString());
  ASSERT_TRUE(iterator->NextRow());
  ZETASQL_ASSERT_OK(iterator->Status());
  EXPECT_EQ(iterator->GetValue(0).int64_value(), 42);
  EXPECT_EQ(iterator->GetValue(1).string_value(), "foo");
  ASSERT_FALSE(iterator->NextRow());
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
