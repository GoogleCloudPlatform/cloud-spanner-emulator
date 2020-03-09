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

#include "backend/transaction/write_util.h"

#include <queue>
#include <sstream>
#include <variant>

#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/time/time.h"
#include "backend/actions/ops.h"
#include "backend/datamodel/key_range.h"
#include "backend/datamodel/key_set.h"
#include "backend/locking/manager.h"
#include "backend/schema/catalog/schema.h"
#include "backend/storage/in_memory_storage.h"
#include "common/clock.h"
#include "common/errors.h"
#include "tests/common/schema_constructor.h"
#include "zetasql/base/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

using zetasql_base::testing::StatusIs;

class WritesTest : public testing::Test {
 public:
  // CREATE TABLE test_table (
  //   int64_col INT64 NOT NULL,
  //   string_col STRING(MAX)
  // ) PRIMARY_KEY(int64_col);
  //
  WritesTest()
      : transaction_store_(&base_storage_, lock_handle_.get()),
        schema_(test::CreateSchemaWithOneTable(&type_factory_)),
        test_table_(schema_->FindTable("test_table")),
        int_col_(test_table_->FindColumn("int64_col")),
        string_col_(test_table_->FindColumn("string_col")) {}

 protected:
  // Components
  Clock clock_;
  LockManager lock_manager_ = LockManager(&clock_);
  std::unique_ptr<LockHandle> lock_handle_ =
      lock_manager_.CreateHandle(TransactionID(1), TransactionPriority(1));
  InMemoryStorage base_storage_;
  TransactionStore transaction_store_;
  std::queue<WriteOp> write_ops_queue_;
  zetasql::TypeFactory type_factory_;
  std::unique_ptr<const Schema> schema_;

  // Constants
  const Table* test_table_;
  const Column* int_col_;
  const Column* string_col_;
};

TEST(UtilTest, ExtractPrimaryKeyIndicesBasic) {
  zetasql::TypeFactory type_factory;
  auto schema = test::CreateSchemaWithOneTable(&type_factory);
  auto table = schema->FindTable("test_table");
  std::vector<std::string> columns = {"col1", "int64_col"};
  std::vector<absl::optional<int>> key_column_indices;
  ZETASQL_ASSERT_OK_AND_ASSIGN(key_column_indices,
                       ExtractPrimaryKeyIndices(columns, table->primary_key()));
  EXPECT_THAT(key_column_indices.size(), 1);
  EXPECT_THAT(key_column_indices[0], 1);
}

TEST(UtilTest, ExtractPrimaryKeyIndicesEmptyColumns) {
  zetasql::TypeFactory type_factory;
  auto schema = test::CreateSchemaWithOneTable(&type_factory);
  auto table = schema->FindTable("test_table");
  // Empty columns.
  std::vector<std::string> columns = {};
  EXPECT_EQ(ExtractPrimaryKeyIndices(columns, table->primary_key()).status(),
            error::NullValueForNotNullColumn("test_table", "int64_col"));
}

TEST(UtilTest, ExtractPrimaryKeyIndicesMissingKey) {
  zetasql::TypeFactory type_factory;
  auto schema = test::CreateSchemaWithOneTable(&type_factory);
  auto table = schema->FindTable("test_table");
  // Key column is missing.
  std::vector<std::string> columns = {"col1", "col2"};
  EXPECT_EQ(ExtractPrimaryKeyIndices(columns, table->primary_key()).status(),
            error::NullValueForNotNullColumn("test_table", "int64_col"));
}

TEST(UtilTest, ExtractPrimaryKeyIndicesColumnCaseInsensitive) {
  zetasql::TypeFactory type_factory;
  auto schema = test::CreateSchemaWithOneTable(&type_factory);
  auto table = schema->FindTable("test_table");
  // Column names are case-insensitive.
  std::vector<std::string> columns = {"INT64_COL", "col1", "col2"};
  std::vector<absl::optional<int>> key_column_indices;
  ZETASQL_ASSERT_OK_AND_ASSIGN(key_column_indices,
                       ExtractPrimaryKeyIndices(columns, table->primary_key()));
  EXPECT_THAT(key_column_indices.size(), 1);
  EXPECT_THAT(key_column_indices[0], 0);
}

TEST(UtilTest, ExtractPrimaryKeyIndicesDuplicateColumns) {
  zetasql::TypeFactory type_factory;
  auto schema = test::CreateSchemaWithOneTable(&type_factory);
  auto table = schema->FindTable("test_table");

  // Duplicate key columns
  std::vector<std::string> columns = {"int64_col", "int64_col", "col2"};
  std::vector<absl::optional<int>> key_column_indices;
  ZETASQL_ASSERT_OK_AND_ASSIGN(key_column_indices,
                       ExtractPrimaryKeyIndices(columns, table->primary_key()));
  EXPECT_THAT(key_column_indices.size(), 1);
  EXPECT_THAT(key_column_indices[0], 0);
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
