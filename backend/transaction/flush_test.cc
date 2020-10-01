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

#include "backend/transaction/flush.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "zetasql/base/statusor.h"
#include "absl/time/time.h"
#include "backend/actions/ops.h"
#include "backend/storage/in_memory_storage.h"
#include "tests/common/schema_constructor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

using zetasql::values::Int64;
using zetasql::values::String;

class FlushTest : public testing::Test {
 public:
  FlushTest()
      : storage_(absl::make_unique<InMemoryStorage>()),
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
  std::unique_ptr<InMemoryStorage> storage_;

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
    return storage_->Write(timestamp, table_->id(), key,
                           {int64_col_->id(), string_col_->id()}, values);
  }

  zetasql_base::StatusOr<std::vector<ValueList>> ReadAll(absl::Time timestamp) {
    std::unique_ptr<StorageIterator> itr;
    ZETASQL_RETURN_IF_ERROR(storage_->Read(timestamp, table_->id(), KeyRange::All(),
                                   {int64_col_->id(), string_col_->id()},
                                   &itr));

    std::vector<ValueList> rows;
    while (itr->Next()) {
      rows.emplace_back();
      for (int i = 0; i < itr->NumColumns(); i++) {
        rows.back().push_back(itr->ColumnValue(i));
      }
    }
    return rows;
  }

  auto IsOkAndHoldsRows(const std::vector<ValueList>& rows) {
    return zetasql_base::testing::IsOkAndHolds(testing::ElementsAreArray(rows));
  }
};

TEST_F(FlushTest, CanFlushWriteOpsToStorage) {
  absl::Time t0 = absl::Now();

  // Insert - {1, "value"}, {2, "value"}
  ZETASQL_ASSERT_OK(Write(t0, Key({Int64(1)}), {Int64(1), String("value")}));
  ZETASQL_ASSERT_OK(Write(t0, Key({Int64(2)}), {Int64(2), String("value")}));

  // Make sure we can read back the rows written to base storage.
  EXPECT_THAT(ReadAll(t0), IsOkAndHoldsRows({{Int64(1), String("value")},
                                             {Int64(2), String("value")}}));

  // Update base storage by flushing write ops at a later timestamp.
  absl::Time t1 = t0 + absl::Seconds(1);

  // Insert - {3, "value"}
  InsertOp insert_op{table_,
                     Key({Int64(3)}),
                     {int64_col_, string_col_},
                     {Int64(3), String("value")}};

  // Update - {1, "value"} -> {1, "new-value"}
  UpdateOp update_op{
      table_, Key({Int64(1)}), {string_col_}, {String("new-value")}};

  // Delete - {2, "value"}
  DeleteOp delete_op{table_, Key({Int64(2)})};

  ZETASQL_ASSERT_OK(FlushWriteOpsToStorage({insert_op, update_op, delete_op},
                                   storage_.get(), t1));

  // Make sure reads on base storage at t1 reflect the flushed write ops.
  EXPECT_THAT(ReadAll(t1), IsOkAndHoldsRows({{Int64(1), String("new-value")},
                                             {Int64(3), String("value")}}));
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
