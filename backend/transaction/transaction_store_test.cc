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

#include "backend/transaction/transaction_store.h"

#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/time/time.h"
#include "backend/datamodel/key_range.h"
#include "backend/datamodel/value.h"
#include "backend/locking/manager.h"
#include "backend/storage/in_memory_storage.h"
#include "common/clock.h"
#include "tests/common/schema_constructor.h"

using zetasql::types::StringType;
using zetasql::values::Int64;
using zetasql::values::Null;
using zetasql::values::String;
using testing::ElementsAre;
using zetasql_base::testing::StatusIs;

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

class TransactionStoreTest : public testing::Test {
 public:
  TransactionStoreTest()
      : lock_manager_(LockManager(&clock_)),
        lock_handle_(lock_manager_.CreateHandle(TransactionID(1),
                                                TransactionPriority(1))),
        transaction_store_(&base_storage_, lock_handle_.get()),
        type_factory_(absl::make_unique<zetasql::TypeFactory>()),
        schema_(test::CreateSchemaWithOneTable(type_factory_.get())),
        table_(schema_->FindTable("test_table")),
        int64_col_(table_->FindColumn("int64_col")),
        string_col_(table_->FindColumn("string_col")) {}

 protected:
  // Components
  Clock clock_;
  LockManager lock_manager_;
  std::unique_ptr<LockHandle> lock_handle_;
  InMemoryStorage base_storage_;
  TransactionStore transaction_store_;
  // The type factory must outlive the type objects that it has made.
  std::unique_ptr<zetasql::TypeFactory> type_factory_;
  std::unique_ptr<const Schema> schema_;

  // Constants
  const Table* table_;
  const Column* int64_col_;
  const Column* string_col_;
};

TEST_F(TransactionStoreTest, CanReadBufferedWrites) {
  // CREATE TABLE test_table (
  //   int64_col INT64 NOT NULL,
  //   string_col STRING(MAX)
  // ) PRIMARY_KEY(int64_col);
  //
  // Populate the table with some data.
  absl::Time t0 = absl::Now();
  ZETASQL_EXPECT_OK(base_storage_.Write(t0, table_->id(), Key({Int64(1)}),
                                {int64_col_->id(), string_col_->id()},
                                {Int64(1), String("value")}));
  ZETASQL_EXPECT_OK(base_storage_.Write(t0, table_->id(), Key({Int64(2)}),
                                {int64_col_->id(), string_col_->id()},
                                {Int64(2), String("value")}));

  // Insert a new row.
  ZETASQL_EXPECT_OK(transaction_store_.BufferInsert(table_, Key({Int64(3)}),
                                            {int64_col_, string_col_},
                                            {Int64(3), String("value")}));

  // Update an existing row.
  ZETASQL_EXPECT_OK(transaction_store_.BufferUpdate(
      table_, Key({Int64(1)}), {string_col_}, {String("new-value")}));

  // Delete another existing row.
  ZETASQL_EXPECT_OK(transaction_store_.BufferDelete(table_, Key({Int64(2)})));

  // Read your writes.
  std::unique_ptr<StorageIterator> itr;
  ZETASQL_EXPECT_OK(transaction_store_.Read(table_, KeyRange::All(),
                                    {int64_col_, string_col_}, &itr));

  // row key = 1
  EXPECT_TRUE(itr->Next());
  EXPECT_EQ(itr->Key(), Key({Int64(1)}));
  EXPECT_EQ(itr->NumColumns(), 2);
  EXPECT_EQ(itr->ColumnValue(0), Int64(1));
  EXPECT_EQ(itr->ColumnValue(1), String("new-value"));
  // row key = 3
  EXPECT_TRUE(itr->Next());
  EXPECT_EQ(itr->Key(), Key({Int64(3)}));
  EXPECT_EQ(itr->NumColumns(), 2);
  EXPECT_EQ(itr->ColumnValue(0), Int64(3));
  EXPECT_EQ(itr->ColumnValue(1), String("value"));
  // No other rows to read.
  EXPECT_FALSE(itr->Next());
}

TEST_F(TransactionStoreTest, CanBufferInsertAfterDelete) {
  // CREATE TABLE test_table (
  //   int64_col INT64 NOT NULL,
  //   string_col STRING(MAX)
  // ) PRIMARY_KEY(int64_col);
  //
  // Populate the table with some data.
  absl::Time t0 = absl::Now();
  ZETASQL_EXPECT_OK(base_storage_.Write(t0, table_->id(), Key({Int64(1)}),
                                {int64_col_->id(), string_col_->id()},
                                {Int64(1), String("value")}));

  // Read your writes.
  std::unique_ptr<StorageIterator> itr;
  ZETASQL_EXPECT_OK(transaction_store_.Read(table_, KeyRange::Point(Key({Int64(1)})),
                                    {int64_col_, string_col_}, &itr));
  EXPECT_TRUE(itr->Next());
  EXPECT_EQ(itr->Key(), Key({Int64(1)}));
  EXPECT_EQ(itr->NumColumns(), 2);
  EXPECT_EQ(itr->ColumnValue(0), Int64(1));
  EXPECT_EQ(itr->ColumnValue(1), String("value"));
  // No other rows to read.
  EXPECT_FALSE(itr->Next());

  ZETASQL_EXPECT_OK(transaction_store_.BufferDelete(table_, Key({Int64(1)})));

  // Read your writes.
  ZETASQL_EXPECT_OK(transaction_store_.Read(table_, KeyRange::Point(Key({Int64(1)})),
                                    {int64_col_, string_col_}, &itr));
  // No rows to read.
  EXPECT_FALSE(itr->Next());

  ZETASQL_EXPECT_OK(transaction_store_.BufferInsert(table_, Key({Int64(1)}),
                                            {int64_col_, string_col_},
                                            {Int64(1), String("new-value")}));

  // Read your writes.
  // This tests the case where a Delete & Insert are buffered over an existing
  // base storage row.
  ZETASQL_EXPECT_OK(transaction_store_.Read(table_, KeyRange::Point(Key({Int64(1)})),
                                    {int64_col_, string_col_}, &itr));
  EXPECT_TRUE(itr->Next());
  EXPECT_EQ(itr->Key(), Key({Int64(1)}));
  EXPECT_EQ(itr->NumColumns(), 2);
  EXPECT_EQ(itr->ColumnValue(0), Int64(1));
  EXPECT_EQ(itr->ColumnValue(1), String("new-value"));
  // No other rows to read.
  EXPECT_FALSE(itr->Next());
}

TEST_F(TransactionStoreTest, CanBufferUpdateAfterInsert) {
  // CREATE TABLE test_table (
  //   int64_col INT64 NOT NULL,
  //   string_col STRING(MAX)
  // ) PRIMARY_KEY(int64_col);

  ZETASQL_EXPECT_OK(transaction_store_.BufferInsert(table_, Key({Int64(1)}),
                                            {int64_col_, string_col_},
                                            {Int64(1), String("old-value")}));

  // Read your writes.
  std::unique_ptr<StorageIterator> itr;
  ZETASQL_EXPECT_OK(transaction_store_.Read(table_, KeyRange::Point(Key({Int64(1)})),
                                    {int64_col_, string_col_}, &itr));
  EXPECT_TRUE(itr->Next());
  EXPECT_EQ(itr->Key(), Key({Int64(1)}));
  EXPECT_EQ(itr->NumColumns(), 2);
  EXPECT_EQ(itr->ColumnValue(0), Int64(1));
  EXPECT_EQ(itr->ColumnValue(1), String("old-value"));
  // No other rows to read.
  EXPECT_FALSE(itr->Next());

  ZETASQL_EXPECT_OK(transaction_store_.BufferUpdate(table_, Key({Int64(1)}),
                                            {int64_col_, string_col_},
                                            {Int64(1), String("new-value")}));

  // Read your writes.
  ZETASQL_EXPECT_OK(transaction_store_.Read(table_, KeyRange::Point(Key({Int64(1)})),
                                    {int64_col_, string_col_}, &itr));
  EXPECT_TRUE(itr->Next());
  EXPECT_EQ(itr->ColumnValue(1), String("new-value"));
}

TEST_F(TransactionStoreTest, CanBufferDeleteAfterInsert) {
  // CREATE TABLE test_table (
  //   int64_col INT64 NOT NULL,
  //   string_col STRING(MAX)
  // ) PRIMARY_KEY(int64_col);

  ZETASQL_EXPECT_OK(transaction_store_.BufferInsert(table_, Key({Int64(1)}),
                                            {int64_col_, string_col_},
                                            {Int64(1), String("old-value")}));

  ZETASQL_EXPECT_OK(transaction_store_.BufferDelete(table_, Key({Int64(1)})));

  std::unique_ptr<StorageIterator> itr;
  ZETASQL_EXPECT_OK(transaction_store_.Read(table_, KeyRange::Point(Key({Int64(1)})),
                                    {int64_col_, string_col_}, &itr));
  // No rows to read.
  EXPECT_FALSE(itr->Next());
}

TEST_F(TransactionStoreTest, CanBufferMultipleUpdates) {
  // CREATE TABLE test_table (
  //   int64_col INT64 NOT NULL,
  //   string_col STRING(MAX)
  // ) PRIMARY_KEY(int64_col);
  absl::Time t0 = absl::Now();
  ZETASQL_EXPECT_OK(base_storage_.Write(t0, table_->id(), Key({Int64(1)}),
                                {int64_col_->id(), string_col_->id()},
                                {Int64(1), String("value-1")}));

  ZETASQL_EXPECT_OK(transaction_store_.BufferUpdate(
      table_, Key({Int64(1)}), {string_col_}, {String("value-2")}));

  // Read
  std::unique_ptr<StorageIterator> itr;
  ZETASQL_EXPECT_OK(transaction_store_.Read(table_, KeyRange::Point(Key({Int64(1)})),
                                    {int64_col_, string_col_}, &itr));
  EXPECT_TRUE(itr->Next());
  EXPECT_EQ(itr->Key(), Key({Int64(1)}));
  EXPECT_EQ(itr->NumColumns(), 2);
  EXPECT_EQ(itr->ColumnValue(0), Int64(1));
  EXPECT_EQ(itr->ColumnValue(1), String("value-2"));
  // No other rows to read.
  EXPECT_FALSE(itr->Next());

  // Buffer update.
  ZETASQL_EXPECT_OK(transaction_store_.BufferUpdate(
      table_, Key({Int64(1)}), {string_col_}, {String("value-3")}));

  // Read
  ZETASQL_EXPECT_OK(transaction_store_.Read(table_, KeyRange::Point(Key({Int64(1)})),
                                    {string_col_}, &itr));
  EXPECT_TRUE(itr->Next());
  EXPECT_EQ(itr->NumColumns(), 1);
  EXPECT_EQ(itr->ColumnValue(0), String("value-3"));
  // No other rows to read.
  EXPECT_FALSE(itr->Next());
}

TEST_F(TransactionStoreTest, CanBufferReplaceWithNullValues) {
  // CREATE TABLE test_table (
  //   int64_col INT64 NOT NULL,
  //   string_col STRING(MAX)
  // ) PRIMARY_KEY(int64_col);
  absl::Time t0 = absl::Now();
  ZETASQL_EXPECT_OK(base_storage_.Write(t0, table_->id(), Key({Int64(1)}),
                                {int64_col_->id(), string_col_->id()},
                                {Int64(1), String("value-1")}));

  // Delete row and only insert key column value.
  ZETASQL_EXPECT_OK(transaction_store_.BufferDelete(table_, Key({Int64(1)})));
  ZETASQL_EXPECT_OK(transaction_store_.BufferInsert(table_, Key({Int64(1)}),
                                            {int64_col_}, {Int64(1)}));

  // Lookup should return null value for string_col.
  std::vector<zetasql::Value> values;
  ZETASQL_EXPECT_OK(transaction_store_.Lookup(table_, Key({Int64(1)}), {string_col_},
                                      &values));
  EXPECT_THAT(values, testing::ElementsAre(Null(StringType())));
}

TEST_F(TransactionStoreTest, CanBufferDeleteInsertUpdate) {
  // CREATE TABLE test_table (
  //   int64_col INT64 NOT NULL,
  //   string_col STRING(MAX)
  // ) PRIMARY_KEY(int64_col);
  absl::Time t0 = absl::Now();
  ZETASQL_EXPECT_OK(base_storage_.Write(t0, table_->id(), Key({Int64(1)}),
                                {int64_col_->id(), string_col_->id()},
                                {Int64(1), String("value-1")}));

  ZETASQL_EXPECT_OK(transaction_store_.BufferDelete(table_, Key({Int64(1)})));
  ZETASQL_EXPECT_OK(transaction_store_.BufferInsert(table_, Key({Int64(1)}),
                                            {int64_col_, string_col_},
                                            {Int64(1), String("value-2")}));
  ZETASQL_EXPECT_OK(transaction_store_.BufferUpdate(
      table_, Key({Int64(1)}), {string_col_}, {String("value-3")}));

  std::vector<zetasql::Value> values;
  ZETASQL_EXPECT_OK(transaction_store_.Lookup(table_, Key({Int64(1)}),
                                      {int64_col_, string_col_}, &values));
  EXPECT_THAT(values, testing::ElementsAre(Int64(1), String("value-3")));
}

TEST_F(TransactionStoreTest, ReadValueNotFound) {
  // CREATE TABLE test_table (
  //   int64_col INT64 NOT NULL,
  //   string_col STRING(MAX)
  // ) PRIMARY_KEY(int64_col);

  // Read on empty table.
  std::unique_ptr<StorageIterator> itr;
  ZETASQL_EXPECT_OK(transaction_store_.Read(table_, KeyRange::Point(Key({Int64(1)})),
                                    {string_col_}, &itr));
  EXPECT_FALSE(itr->Next());
}

TEST_F(TransactionStoreTest, ReadValueOnlyInBaseStorage) {
  // CREATE TABLE test_table (
  //   int64_col INT64 NOT NULL,
  //   string_col STRING(MAX)
  // ) PRIMARY_KEY(int64_col);

  // Write into base storage.
  absl::Time t0 = absl::Now();
  ZETASQL_EXPECT_OK(base_storage_.Write(t0, table_->id(), Key({Int64(1)}),
                                {int64_col_->id(), string_col_->id()},
                                {Int64(1), String("value-1")}));

  // Read
  std::unique_ptr<StorageIterator> itr;
  ZETASQL_EXPECT_OK(transaction_store_.Read(table_, KeyRange::All(),
                                    {int64_col_, string_col_}, &itr));
  EXPECT_TRUE(itr->Next());
  EXPECT_EQ(itr->Key(), Key({Int64(1)}));
  EXPECT_EQ(itr->NumColumns(), 2);
  EXPECT_EQ(itr->ColumnValue(0), Int64(1));
  EXPECT_EQ(itr->ColumnValue(1), String("value-1"));
  // No other rows to read.
  EXPECT_FALSE(itr->Next());
}

TEST_F(TransactionStoreTest, Lookup) {
  // CREATE TABLE test_table (
  //   int64_col INT64 NOT NULL,
  //   string_col STRING(MAX)
  // ) PRIMARY_KEY(int64_col);

  // Populate transaction store
  ZETASQL_EXPECT_OK(transaction_store_.BufferInsert(table_, Key({Int64(1)}),
                                            {int64_col_, string_col_},
                                            {Int64(1), String("value")}));
  ZETASQL_EXPECT_OK(transaction_store_.BufferInsert(table_, Key({Int64(3)}),
                                            {int64_col_, string_col_},
                                            {Int64(3), String("value")}));
  ZETASQL_EXPECT_OK(transaction_store_.BufferUpdate(
      table_, Key({Int64(1)}), {string_col_}, {String("new-value")}));

  // Delete another existing row.
  ZETASQL_EXPECT_OK(transaction_store_.BufferDelete(table_, Key({Int64(2)})));

  // Now verify the values in the transaction store.
  std::vector<zetasql::Value> values;
  // Row Int64(1) is updated.
  ZETASQL_EXPECT_OK(transaction_store_.Lookup(table_, Key({Int64(1)}),
                                      {int64_col_, string_col_}, &values));
  EXPECT_EQ(values[0], Int64(1));
  EXPECT_EQ(values[1], String("new-value"));

  // Row Int64(2) does not exist.
  EXPECT_THAT(transaction_store_.Lookup(table_, Key({Int64(2)}),
                                        {int64_col_, string_col_}, &values),
              StatusIs(zetasql_base::StatusCode::kNotFound));

  // Row Int64(3) is inserted.
  ZETASQL_EXPECT_OK(transaction_store_.Lookup(table_, Key({Int64(3)}),
                                      {int64_col_, string_col_}, &values));
  EXPECT_EQ(values[0], Int64(3));
  EXPECT_EQ(values[1], String("value"));

  ZETASQL_EXPECT_OK(transaction_store_.BufferDelete(table_, Key({Int64(3)})));

  // Row Int64(3) has been deleted so it should return not found.
  EXPECT_THAT(transaction_store_.Lookup(table_, Key({Int64(3)}),
                                        {int64_col_, string_col_}, &values),
              StatusIs(zetasql_base::StatusCode::kNotFound));
}

TEST_F(TransactionStoreTest, LookupDelete) {
  // CREATE TABLE test_table (
  //   int64_col INT64 NOT NULL,
  //   string_col STRING(MAX)
  // ) PRIMARY_KEY(int64_col);

  std::vector<zetasql::Value> values;
  EXPECT_THAT(transaction_store_.Lookup(table_, Key({Int64(1)}),
                                        {int64_col_, string_col_}, &values),
              StatusIs(zetasql_base::StatusCode::kNotFound));

  ZETASQL_EXPECT_OK(transaction_store_.BufferInsert(table_, Key({Int64(1)}),
                                            {int64_col_, string_col_},
                                            {Int64(1), String("value")}));
  ZETASQL_EXPECT_OK(transaction_store_.Lookup(table_, Key({Int64(1)}),
                                      {int64_col_, string_col_}, &values));

  ZETASQL_EXPECT_OK(transaction_store_.BufferDelete(table_, Key({Int64(1)})));
  EXPECT_THAT(transaction_store_.Lookup(table_, Key({Int64(1)}),
                                        {int64_col_, string_col_}, &values),
              StatusIs(zetasql_base::StatusCode::kNotFound));
}

TEST_F(TransactionStoreTest, LookupBaseStorage) {
  // CREATE TABLE test_table (
  //   int64_col INT64 NOT NULL,
  //   string_col STRING(MAX)
  // ) PRIMARY_KEY(int64_col);

  absl::Time t0 = absl::Now();
  ZETASQL_EXPECT_OK(base_storage_.Write(t0, table_->id(), Key({Int64(1)}),
                                {int64_col_->id(), string_col_->id()},
                                {Int64(1), String("value")}));

  std::vector<zetasql::Value> values;
  ZETASQL_EXPECT_OK(transaction_store_.Lookup(table_, Key({Int64(1)}),
                                      {int64_col_, string_col_}, &values));
  EXPECT_EQ(values[0], Int64(1));
  EXPECT_EQ(values[1], String("value"));

  ZETASQL_EXPECT_OK(transaction_store_.BufferUpdate(
      table_, Key({Int64(1)}), {string_col_}, {String("new-value")}));
  ZETASQL_EXPECT_OK(transaction_store_.Lookup(table_, Key({Int64(1)}),
                                      {int64_col_, string_col_}, &values));
  EXPECT_EQ(values[0], Int64(1));
  EXPECT_EQ(values[1], String("new-value"));
}

TEST_F(TransactionStoreTest, LookupNullValues) {
  // CREATE TABLE test_table (
  //   int64_col INT64 NOT NULL,
  //   string_col STRING(MAX)
  // ) PRIMARY_KEY(int64_col);

  absl::Time t0 = absl::Now();
  ZETASQL_EXPECT_OK(base_storage_.Write(t0, table_->id(), Key({Int64(1)}),
                                {int64_col_->id(), string_col_->id()},
                                {Int64(1), String("value")}));
  ZETASQL_EXPECT_OK(transaction_store_.BufferInsert(table_, Key({Int64(2)}),
                                            {int64_col_, string_col_},
                                            {Int64(2), String("value")}));

  ZETASQL_EXPECT_OK(transaction_store_.Lookup(table_, Key({Int64(1)}), {}, nullptr));
  ZETASQL_EXPECT_OK(transaction_store_.Lookup(table_, Key({Int64(2)}), {}, nullptr));

  ZETASQL_EXPECT_OK(transaction_store_.BufferUpdate(table_, Key({Int64(1)}),
                                            {int64_col_, string_col_},
                                            {Int64(2), String("new-value")}));

  ZETASQL_EXPECT_OK(transaction_store_.Lookup(table_, Key({Int64(1)}), {}, nullptr));
}

TEST_F(TransactionStoreTest, ReturnsNullValuesForUnpopulatedColumns) {
  // CREATE TABLE test_table (
  //   int64_col INT64 NOT NULL,
  //   string_col STRING(MAX)
  // ) PRIMARY_KEY(int64_col);

  // Write three rows
  // - Key(1) which only exists in base storage
  // - Key(2) which only exists in transaction storage
  // - Key(3) which exists in both base and transaction storage.
  // In each case, we do not populate the string column, and expect that all
  // reads for the string column return NULL.
  absl::Time t0 = absl::Now();
  ZETASQL_EXPECT_OK(base_storage_.Write(t0, table_->id(), Key({Int64(1)}),
                                {int64_col_->id()}, {Int64(1)}));
  ZETASQL_EXPECT_OK(transaction_store_.BufferInsert(table_, Key({Int64(2)}),
                                            {int64_col_}, {Int64(2)}));
  ZETASQL_EXPECT_OK(base_storage_.Write(t0, table_->id(), Key({Int64(3)}),
                                {int64_col_->id()}, {Int64(3)}));
  ZETASQL_EXPECT_OK(transaction_store_.BufferUpdate(table_, Key({Int64(3)}),
                                            {int64_col_}, {Int64(3)}));

  for (const int key : {1, 2, 3}) {
    std::vector<zetasql::Value> values;
    ZETASQL_EXPECT_OK(transaction_store_.Lookup(table_, Key({Int64(key)}),
                                        {int64_col_, string_col_}, &values));
    EXPECT_THAT(values, ElementsAre(Int64(key), Null(StringType())));
  }

  for (const int key : {1, 2, 3}) {
    std::unique_ptr<StorageIterator> itr;
    ZETASQL_EXPECT_OK(transaction_store_.Read(table_,
                                      KeyRange::Point(Key({Int64(key)})),
                                      {int64_col_, string_col_}, &itr));
    EXPECT_TRUE(itr->Next());
    EXPECT_EQ(itr->Key(), Key({Int64(key)}));
    EXPECT_EQ(itr->NumColumns(), 2);
    EXPECT_EQ(itr->ColumnValue(0), Int64(key));
    EXPECT_EQ(itr->ColumnValue(1), Null(StringType()));
    EXPECT_FALSE(itr->Next());
  }
}

TEST_F(TransactionStoreTest, ReadsClosedOpenRange) {
  // We insert key {1}, then read range [0, 1) which should exclude the key.
  ZETASQL_EXPECT_OK(transaction_store_.BufferInsert(table_, Key({Int64(1)}),
                                            {int64_col_}, {Int64(1)}));
  std::unique_ptr<StorageIterator> itr;
  ZETASQL_EXPECT_OK(transaction_store_.Read(
      table_, KeyRange::ClosedOpen(Key({Int64(0)}), Key({Int64(1)})),
      {int64_col_, string_col_}, &itr));
  EXPECT_FALSE(itr->Next());
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
