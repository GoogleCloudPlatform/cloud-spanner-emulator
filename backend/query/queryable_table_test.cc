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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "googlesql/public/evaluator_table_iterator.h"
#include "googlesql/public/type.h"
#include "googlesql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "absl/container/flat_hash_map.h"
#include "backend/access/read.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/key_range.h"
#include "backend/datamodel/key_set.h"
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
  googlesql::TypeFactory type_factory_;
  std::unique_ptr<const Schema> schema_ =
      test::CreateSchemaWithOneTable(&type_factory_);
  test::TestRowReader reader_{
      {{"test_table",
        {{"int64_col", "string_col"},
         {googlesql::types::Int64Type(), googlesql::types::StringType()},
         {{googlesql::values::Int64(42), googlesql::values::String("foo")}}}}}};
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
  GOOGLESQL_ASSERT_OK(iterator->Status());
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
  GOOGLESQL_ASSERT_OK(iterator->Status());
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
  GOOGLESQL_ASSERT_OK(iterator->Status());
  EXPECT_EQ(iterator->GetValue(0).int64_value(), 42);
  EXPECT_EQ(iterator->GetValue(1).string_value(), "foo");
  ASSERT_FALSE(iterator->NextRow());
}

// Answers every read with no rows and keeps the key set it was asked for.
class KeySetRecordingRowReader : public RowReader {
 public:
  absl::Status Read(const ReadArg& read_arg,
                    std::unique_ptr<RowCursor>* cursor) override {
    key_set_read_ = read_arg.key_set.DebugString();
    *cursor = std::make_unique<test::TestRowCursor>(
        std::vector<std::string>{}, std::vector<const googlesql::Type*>{},
        std::vector<std::vector<googlesql::Value>>{});
    return absl::OkStatus();
  }

  const std::string& key_set_read() const { return key_set_read_; }

 private:
  std::string key_set_read_;
};

using ColumnFilters =
    absl::flat_hash_map<int, std::unique_ptr<googlesql::ColumnFilter>>;

// A table keyed by (tenant, id): columns tenant, id and note, in that order.
class QueryableTableKeyFilterTest : public testing::Test {
 protected:
  static constexpr int kTenant = 0;
  static constexpr int kId = 1;
  static constexpr int kNote = 2;

  // The key set the table is read with by a scan of 'column_idxs' the
  // evaluator has offered 'filters' to. Filters are keyed by position in the
  // scan, not in the table.
  std::string KeySetRead(std::vector<int> column_idxs, ColumnFilters filters) {
    QueryableTable table{schema_->FindTable("tenant_rows"), &reader_};
    auto iterator = table.CreateEvaluatorTableIterator(column_idxs).value();
    GOOGLESQL_EXPECT_OK(iterator->SetColumnFilterMap(std::move(filters)));
    EXPECT_FALSE(iterator->NextRow());
    GOOGLESQL_EXPECT_OK(iterator->Status());
    return reader_.key_set_read();
  }

  static std::unique_ptr<googlesql::ColumnFilter> In(
      std::vector<googlesql::Value> values) {
    return std::make_unique<googlesql::ColumnFilter>(values);
  }

  static std::string Prefixes(std::vector<std::vector<googlesql::Value>> keys) {
    KeySet key_set;
    for (const auto& key : keys) {
      key_set.AddRange(KeyRange::Prefix(Key(key)));
    }
    return key_set.DebugString();
  }

 private:
  googlesql::TypeFactory type_factory_;
  std::unique_ptr<const Schema> schema_ =
      test::CreateSchemaFromDDL(
          std::vector<std::string>{R"(CREATE TABLE tenant_rows (
                tenant STRING(36) NOT NULL,
                id INT64 NOT NULL,
                note STRING(MAX),
              ) PRIMARY KEY (tenant, id))"},
          &type_factory_)
          .value();
  KeySetRecordingRowReader reader_;
};

TEST_F(QueryableTableKeyFilterTest, NoFilterReadsTheWholeTable) {
  EXPECT_EQ(KeySetRead({kTenant, kId, kNote}, {}), KeySet::All().DebugString());
}

TEST_F(QueryableTableKeyFilterTest, EqualityOnTheLeadingKeyColumnReadsItsRows) {
  ColumnFilters filters;
  filters[0] = In({googlesql::values::String("acme")});

  EXPECT_EQ(KeySetRead({kTenant, kId, kNote}, std::move(filters)),
            Prefixes({{googlesql::values::String("acme")}}));
}

TEST_F(QueryableTableKeyFilterTest, ListsOnEveryKeyColumnReadEachKey) {
  ColumnFilters filters;
  filters[0] = In({googlesql::values::String("acme")});
  filters[1] = In({googlesql::values::Int64(1), googlesql::values::Int64(2)});

  EXPECT_EQ(
      KeySetRead({kTenant, kId, kNote}, std::move(filters)),
      Prefixes(
          {{googlesql::values::String("acme"), googlesql::values::Int64(1)},
           {googlesql::values::String("acme"), googlesql::values::Int64(2)}}));
}

TEST_F(QueryableTableKeyFilterTest, FiltersAreFoundByPositionInTheScan) {
  // The scan reads note then tenant, so the tenant filter is at position 1.
  ColumnFilters filters;
  filters[1] = In({googlesql::values::String("acme")});

  EXPECT_EQ(KeySetRead({kNote, kTenant}, std::move(filters)),
            Prefixes({{googlesql::values::String("acme")}}));
}

TEST_F(QueryableTableKeyFilterTest, AFilterPastAnUnfilteredKeyColumnIsNotUsed) {
  ColumnFilters filters;
  filters[1] = In({googlesql::values::Int64(1)});

  EXPECT_EQ(KeySetRead({kTenant, kId, kNote}, std::move(filters)),
            KeySet::All().DebugString());
}

TEST_F(QueryableTableKeyFilterTest, AFilterOnAColumnOutsideTheKeyIsNotUsed) {
  ColumnFilters filters;
  filters[2] = In({googlesql::values::String("a note")});

  EXPECT_EQ(KeySetRead({kTenant, kId, kNote}, std::move(filters)),
            KeySet::All().DebugString());
}

TEST_F(QueryableTableKeyFilterTest, ARangeFilterIsNotUsed) {
  ColumnFilters filters;
  filters[0] = std::make_unique<googlesql::ColumnFilter>(
      googlesql::values::String("a"), googlesql::values::String("b"));

  EXPECT_EQ(KeySetRead({kTenant, kId, kNote}, std::move(filters)),
            KeySet::All().DebugString());
}

TEST_F(QueryableTableKeyFilterTest, AValueOfAnotherTypeEndsTheNarrowing) {
  ColumnFilters filters;
  filters[0] = In({googlesql::values::String("acme")});
  filters[1] = In({googlesql::values::Uint64(1)});

  EXPECT_EQ(KeySetRead({kTenant, kId, kNote}, std::move(filters)),
            Prefixes({{googlesql::values::String("acme")}}));
}

TEST_F(QueryableTableKeyFilterTest, AListTooLongToExpandEndsTheNarrowing) {
  std::vector<googlesql::Value> ids;
  for (int i = 0; i < 10001; ++i) {
    ids.push_back(googlesql::values::Int64(i));
  }
  ColumnFilters filters;
  filters[0] = In({googlesql::values::String("acme")});
  filters[1] = In(ids);

  EXPECT_EQ(KeySetRead({kTenant, kId, kNote}, std::move(filters)),
            Prefixes({{googlesql::values::String("acme")}}));
}

TEST_F(QueryableTableKeyFilterTest, AnEmptyListReadsNothing) {
  ColumnFilters filters;
  filters[0] = In({});

  EXPECT_EQ(KeySetRead({kTenant, kId, kNote}, std::move(filters)),
            KeySet().DebugString());
}

TEST_F(QueryableTableKeyFilterTest, AScanWithoutTheLeadingKeyColumnReadsAll) {
  ColumnFilters filters;
  filters[0] = In({googlesql::values::Int64(1)});

  EXPECT_EQ(KeySetRead({kId, kNote}, std::move(filters)),
            KeySet::All().DebugString());
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
