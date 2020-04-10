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
      : type_factory_(absl::make_unique<zetasql::TypeFactory>()),
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
  InMemoryStorage base_storage_;
  std::unique_ptr<zetasql::TypeFactory> type_factory_;
  std::unique_ptr<const Schema> schema_;
  const Table* table_;
  const Column* int64_col_;
  const Column* string_col_;
};

TEST_F(FlushTest, CanFlushWriteOpsToStorage) {
  absl::Time t0 = absl::Now();

  // Insert - {1, "value"}, {2, "value"}
  ZETASQL_ASSERT_OK(base_storage_.Write(t0, table_->id(), Key({Int64(1)}),
                                {int64_col_->id(), string_col_->id()},
                                {Int64(1), String("value")}));
  ZETASQL_ASSERT_OK(base_storage_.Write(t0, table_->id(), Key({Int64(2)}),
                                {int64_col_->id(), string_col_->id()},
                                {Int64(2), String("value")}));

  // Make sure we can read back the rows written to base storage.
  {
    std::unique_ptr<StorageIterator> itr;
    ZETASQL_ASSERT_OK(base_storage_.Read(t0, table_->id(), KeyRange::All(),
                                 {int64_col_->id(), string_col_->id()}, &itr));
    std::vector<std::pair<int64_t, std::string>> rows_read;
    while (itr->Next()) {
      ASSERT_EQ(itr->NumColumns(), 2);
      rows_read.push_back({itr->ColumnValue(0).int64_value(),
                           itr->ColumnValue(1).string_value()});
    }
    std::vector<std::pair<int64_t, std::string>> rows_expected{{1, "value"},
                                                             {2, "value"}};
    EXPECT_EQ(rows_read, rows_expected);
  }

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
                                   &base_storage_, t1));

  // Make sure reads on base storage reflect the flushed write ops.
  {
    std::unique_ptr<StorageIterator> itr;
    ZETASQL_ASSERT_OK(base_storage_.Read(t1, table_->id(), KeyRange::All(),
                                 {int64_col_->id(), string_col_->id()}, &itr));
    std::vector<std::pair<int64_t, std::string>> rows_read;
    while (itr->Next()) {
      ASSERT_EQ(itr->NumColumns(), 2);
      rows_read.push_back({itr->ColumnValue(0).int64_value(),
                           itr->ColumnValue(1).string_value()});
    }
    std::vector<std::pair<int64_t, std::string>> rows_expected{{1, "new-value"},
                                                             {3, "value"}};
    EXPECT_EQ(rows_read, rows_expected);
  }
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
