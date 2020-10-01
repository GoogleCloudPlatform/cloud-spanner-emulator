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
#include "zetasql/base/statusor.h"
#include "absl/time/time.h"
#include "backend/actions/ops.h"
#include "backend/common/rows.h"
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
        base_storage_(absl::make_unique<InMemoryStorage>()),
        transaction_store_(base_storage_.get(), lock_handle_.get()),
        type_factory_(absl::make_unique<zetasql::TypeFactory>()),
        schema_(test::CreateSchemaFromDDL(
                    {
                        R"(
                          CREATE TABLE TestTable (
                            Int64Col    INT64 NOT NULL,
                            StringCol   STRING(MAX),
                          ) PRIMARY KEY (Int64Col)
                        )"},
                    type_factory_.get())
                    .ValueOrDie()),
        table_(schema_->FindTable("TestTable")),
        int64_col_(table_->FindColumn("Int64Col")),
        string_col_(table_->FindColumn("StringCol")) {}

 protected:
  // Components
  Clock clock_;
  LockManager lock_manager_;
  std::unique_ptr<LockHandle> lock_handle_;
  std::unique_ptr<InMemoryStorage> base_storage_;
  TransactionStore transaction_store_;

  // The type factory must outlive the type objects that it has made.
  std::unique_ptr<zetasql::TypeFactory> type_factory_;
  std::unique_ptr<const Schema> schema_;

  // Constants
  const Table* table_;
  const Column* int64_col_;
  const Column* string_col_;

  // Helper functions to use in tests.
  absl::Status Write(absl::Time timestamp, const Key& key,
                     const ValueList& values) {
    return base_storage_->Write(timestamp, table_->id(), key,
                                {int64_col_->id(), string_col_->id()}, values);
  }

  absl::Status BufferInsert(const Key& key, std::vector<const Column*> columns,
                            const ValueList& values) {
    return transaction_store_.BufferWriteOp(
        InsertOp{table_, key, columns, values});
  }

  absl::Status BufferUpdate(const Key& key, std::vector<const Column*> columns,
                            const ValueList& values) {
    return transaction_store_.BufferWriteOp(
        UpdateOp{table_, key, columns, values});
  }

  absl::Status BufferDelete(const Key& key) {
    return transaction_store_.BufferWriteOp(DeleteOp{table_, key});
  }

  zetasql_base::StatusOr<ValueList> Lookup(const Key& key) {
    return transaction_store_.Lookup(table_, key, {int64_col_, string_col_});
  }

  zetasql_base::StatusOr<std::vector<ValueList>> ReadAll() {
    return Read(KeyRange::All());
  }

  zetasql_base::StatusOr<std::vector<ValueList>> Read(const KeyRange& key_range) {
    std::unique_ptr<StorageIterator> itr;
    ZETASQL_RETURN_IF_ERROR(transaction_store_.Read(table_, key_range,
                                            {int64_col_, string_col_}, &itr));

    std::vector<ValueList> rows;
    while (itr->Next()) {
      rows.emplace_back();
      for (int i = 0; i < itr->NumColumns(); i++) {
        rows.back().push_back(itr->ColumnValue(i));
      }
    }
    return rows;
  }

  auto IsOkAndHoldsRow(const ValueList& row) {
    return zetasql_base::testing::IsOkAndHolds(row);
  }

  auto IsOkAndHoldsRows(const std::vector<ValueList>& rows) {
    return zetasql_base::testing::IsOkAndHolds(testing::ElementsAreArray(rows));
  }
};

TEST_F(TransactionStoreTest, CanReadBufferedWrites) {
  // Populate the table with some data.
  absl::Time t0 = absl::Now();
  ZETASQL_EXPECT_OK(Write(t0, Key({Int64(1)}), {Int64(1), String("value")}));
  ZETASQL_EXPECT_OK(Write(t0, Key({Int64(2)}), {Int64(2), String("value")}));

  // Insert a new row.
  ZETASQL_EXPECT_OK(BufferInsert(Key({Int64(3)}), {int64_col_, string_col_},
                         {Int64(3), String("value")}));

  // Update an existing row.
  ZETASQL_EXPECT_OK(
      BufferUpdate(Key({Int64(1)}), {string_col_}, {String("new-value")}));

  // Delete another existing row.
  ZETASQL_EXPECT_OK(BufferDelete(Key({Int64(2)})));

  // Read your writes.
  EXPECT_THAT(ReadAll(), IsOkAndHoldsRows({{Int64(1), String("new-value")},
                                           {Int64(3), String("value")}}));
}

TEST_F(TransactionStoreTest, CanBufferInsertAfterDelete) {
  // Populate the table with some data.
  absl::Time t0 = absl::Now();
  ZETASQL_EXPECT_OK(Write(t0, Key({Int64(1)}), {Int64(1), String("value")}));

  // Read your writes.
  EXPECT_THAT(ReadAll(), IsOkAndHoldsRows({{Int64(1), String("value")}}));

  ZETASQL_EXPECT_OK(BufferDelete(Key({Int64(1)})));

  // Read your writes, no rows to read.
  EXPECT_THAT(ReadAll(), IsOkAndHoldsRows({}));

  ZETASQL_EXPECT_OK(BufferInsert(Key({Int64(1)}), {int64_col_, string_col_},
                         {Int64(1), String("new-value")}));

  // Read your writes.
  // This tests the case where a Delete & Insert are buffered over an existing
  // base storage row.
  EXPECT_THAT(ReadAll(), IsOkAndHoldsRows({{Int64(1), String("new-value")}}));
}

TEST_F(TransactionStoreTest, CanBufferUpdateAfterInsert) {
  ZETASQL_EXPECT_OK(BufferInsert(Key({Int64(1)}), {int64_col_, string_col_},
                         {Int64(1), String("old-value")}));

  // Read your writes.
  EXPECT_THAT(ReadAll(), IsOkAndHoldsRows({{Int64(1), String("old-value")}}));

  ZETASQL_EXPECT_OK(BufferInsert(Key({Int64(1)}), {int64_col_, string_col_},
                         {Int64(1), String("new-value")}));

  // Read your writes.
  EXPECT_THAT(ReadAll(), IsOkAndHoldsRows({{Int64(1), String("new-value")}}));
}

TEST_F(TransactionStoreTest, CanBufferDeleteAfterInsert) {
  ZETASQL_EXPECT_OK(BufferInsert(Key({Int64(1)}), {int64_col_, string_col_},
                         {Int64(1), String("old-value")}));

  ZETASQL_EXPECT_OK(BufferDelete(Key({Int64(1)})));

  // No rows to read.
  EXPECT_THAT(ReadAll(), IsOkAndHoldsRows({}));
}

TEST_F(TransactionStoreTest, CanBufferMultipleUpdates) {
  absl::Time t0 = absl::Now();
  ZETASQL_EXPECT_OK(Write(t0, Key({Int64(1)}), {Int64(1), String("value-1")}));

  ZETASQL_EXPECT_OK(BufferUpdate(Key({Int64(1)}), {string_col_}, {String("value-2")}));

  // Read
  EXPECT_THAT(ReadAll(), IsOkAndHoldsRows({{Int64(1), String("value-2")}}));

  // Buffer update.
  ZETASQL_EXPECT_OK(BufferUpdate(Key({Int64(1)}), {string_col_}, {String("value-3")}));

  // Read
  EXPECT_THAT(ReadAll(), IsOkAndHoldsRows({{Int64(1), String("value-3")}}));
}

TEST_F(TransactionStoreTest, CanBufferReplaceWithNullValues) {
  absl::Time t0 = absl::Now();
  ZETASQL_EXPECT_OK(Write(t0, Key({Int64(1)}), {Int64(1), String("value-1")}));

  // Delete row and only insert key column value.
  ZETASQL_EXPECT_OK(BufferDelete(Key({Int64(1)})));
  ZETASQL_EXPECT_OK(BufferInsert(Key({Int64(1)}), {int64_col_}, {Int64(1)}));

  // Read should return null value for string_col.
  EXPECT_THAT(ReadAll(), IsOkAndHoldsRows({{Int64(1), Null(StringType())}}));
}

TEST_F(TransactionStoreTest, CanBufferDeleteInsertUpdate) {
  absl::Time t0 = absl::Now();
  ZETASQL_EXPECT_OK(Write(t0, Key({Int64(1)}), {Int64(1), String("value-1")}));

  ZETASQL_EXPECT_OK(BufferDelete(Key({Int64(1)})));
  ZETASQL_EXPECT_OK(BufferInsert(Key({Int64(1)}), {int64_col_, string_col_},
                         {Int64(1), String("value-2")}));
  ZETASQL_EXPECT_OK(BufferUpdate(Key({Int64(1)}), {string_col_}, {String("value-3")}));

  EXPECT_THAT(ReadAll(), IsOkAndHoldsRows({{Int64(1), String("value-3")}}));
}

TEST_F(TransactionStoreTest, ReadValueNotFound) {
  // Read on empty table.
  EXPECT_THAT(ReadAll(), IsOkAndHoldsRows({}));
}

TEST_F(TransactionStoreTest, ReadValueOnlyInBaseStorage) {
  // Write into base storage.
  absl::Time t0 = absl::Now();
  ZETASQL_EXPECT_OK(Write(t0, Key({Int64(1)}), {Int64(1), String("value-1")}));

  // Read
  EXPECT_THAT(ReadAll(), IsOkAndHoldsRows({{Int64(1), String("value-1")}}));
}

TEST_F(TransactionStoreTest, Lookup) {
  // Populate transaction store
  ZETASQL_EXPECT_OK(BufferInsert(Key({Int64(1)}), {int64_col_, string_col_},
                         {Int64(1), String("value")}));
  ZETASQL_EXPECT_OK(BufferInsert(Key({Int64(3)}), {int64_col_, string_col_},
                         {Int64(3), String("value")}));
  ZETASQL_EXPECT_OK(
      BufferUpdate(Key({Int64(1)}), {string_col_}, {String("new-value")}));

  // Delete another existing row.
  ZETASQL_EXPECT_OK(BufferDelete(Key({Int64(2)})));

  // Now verify the values in the transaction store.
  // Row Int64(1) is updated.
  EXPECT_THAT(Lookup(Key({Int64(1)})),
              IsOkAndHoldsRow({Int64(1), String("new-value")}));

  // Row Int64(2) does not exist.
  EXPECT_THAT(Lookup(Key({Int64(2)})), StatusIs(absl::StatusCode::kNotFound));

  // Row Int64(3) is inserted.
  EXPECT_THAT(Lookup(Key({Int64(3)})),
              IsOkAndHoldsRow({Int64(3), String("value")}));

  ZETASQL_EXPECT_OK(BufferDelete(Key({Int64(3)})));

  // Row Int64(3) has been deleted so it should return not found.
  EXPECT_THAT(Lookup(Key({Int64(3)})), StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(TransactionStoreTest, LookupDelete) {
  EXPECT_THAT(Lookup(Key({Int64(1)})), StatusIs(absl::StatusCode::kNotFound));

  ZETASQL_EXPECT_OK(BufferInsert(Key({Int64(1)}), {int64_col_, string_col_},
                         {Int64(1), String("value")}));
  EXPECT_THAT(Lookup(Key({Int64(1)})),
              IsOkAndHoldsRow({Int64(1), String("value")}));

  ZETASQL_EXPECT_OK(BufferDelete(Key({Int64(1)})));
  EXPECT_THAT(Lookup(Key({Int64(1)})), StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(TransactionStoreTest, LookupBaseStorage) {
  absl::Time t0 = absl::Now();
  ZETASQL_EXPECT_OK(Write(t0, Key({Int64(1)}), {Int64(1), String("value")}));
  EXPECT_THAT(Lookup(Key({Int64(1)})),
              IsOkAndHoldsRow({Int64(1), String("value")}));

  ZETASQL_EXPECT_OK(
      BufferUpdate(Key({Int64(1)}), {string_col_}, {String("new-value")}));
  EXPECT_THAT(Lookup(Key({Int64(1)})),
              IsOkAndHoldsRow({Int64(1), String("new-value")}));
}

TEST_F(TransactionStoreTest, LookupEmptyColumns) {
  absl::Time t0 = absl::Now();
  ZETASQL_EXPECT_OK(Write(t0, Key({Int64(1)}), {Int64(1), String("value")}));
  ZETASQL_EXPECT_OK(BufferInsert(Key({Int64(2)}), {int64_col_, string_col_},
                         {Int64(2), String("value")}));

  ZETASQL_EXPECT_OK(transaction_store_.Lookup(table_, Key({Int64(1)}), {}));
  ZETASQL_EXPECT_OK(transaction_store_.Lookup(table_, Key({Int64(2)}), {}));

  ZETASQL_EXPECT_OK(BufferUpdate(Key({Int64(1)}), {int64_col_, string_col_},
                         {Int64(2), String("new-value")}));

  ZETASQL_EXPECT_OK(transaction_store_.Lookup(table_, Key({Int64(1)}), {}));
}

TEST_F(TransactionStoreTest, ReturnsNullValuesForUnpopulatedColumns) {
  // Write three rows
  // - Key(1) which only exists in base storage
  // - Key(2) which only exists in transaction storage
  // - Key(3) which exists in both base and transaction storage.
  // In each case, we do not populate the string column, and expect that all
  // reads for the string column return NULL.
  absl::Time t0 = absl::Now();
  ZETASQL_EXPECT_OK(base_storage_->Write(t0, table_->id(), Key({Int64(1)}),
                                 {int64_col_->id()}, {Int64(1)}));
  ZETASQL_EXPECT_OK(BufferInsert(Key({Int64(2)}), {int64_col_}, {Int64(2)}));

  ZETASQL_EXPECT_OK(base_storage_->Write(t0, table_->id(), Key({Int64(3)}),
                                 {int64_col_->id()}, {Int64(3)}));
  ZETASQL_EXPECT_OK(BufferUpdate(Key({Int64(3)}), {int64_col_}, {Int64(3)}));

  for (const int key : {1, 2, 3}) {
    EXPECT_THAT(Lookup(Key({Int64(key)})),
                IsOkAndHoldsRow({Int64(key), Null(StringType())}));
  }

  EXPECT_THAT(ReadAll(), IsOkAndHoldsRows({
                             {Int64(1), Null(StringType())},
                             {Int64(2), Null(StringType())},
                             {Int64(3), Null(StringType())},
                         }));
}

TEST_F(TransactionStoreTest, ReadsClosedOpenRange) {
  // We insert key {1}, then read range [0, 1) which should exclude the key.
  ZETASQL_EXPECT_OK(BufferInsert(Key({Int64(1)}), {int64_col_}, {Int64(1)}));

  EXPECT_THAT(Read(KeyRange::ClosedOpen(Key({Int64(0)}), Key({Int64(1)}))),
              IsOkAndHoldsRows({}));
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
