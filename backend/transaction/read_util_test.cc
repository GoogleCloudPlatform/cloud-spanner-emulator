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

#include "backend/transaction/read_util.h"

#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/time/time.h"
#include "backend/datamodel/key_range.h"
#include "backend/datamodel/key_set.h"
#include "backend/schema/catalog/schema.h"
#include "backend/storage/in_memory_storage.h"
#include "tests/common/schema_constructor.h"
#include "zetasql/base/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

using zetasql::values::Int64;
using zetasql::values::String;

class ReadTest : public testing::Test {
 public:
  // CREATE TABLE test_table (
  //   int64_col INT64 NOT NULL,
  //   string_col STRING(MAX)
  // ) PRIMARY_KEY(int64_col);
  //
  // CREATE INDEX test_index ON test_table(string_col);
  //
  ReadTest()
      : schema_(test::CreateSchemaWithOneTable(&type_factory_)),
        test_table_(schema_->FindTable("test_table")),
        index_(schema_->FindIndex("test_index")),
        index_data_table_(index_->index_data_table()),
        int_col_(test_table_->FindColumn("int64_col")),
        string_col_(test_table_->FindColumn("string_col")),
        index_string_col_(index_data_table_->FindColumn("string_col")) {}

 protected:
  zetasql::TypeFactory type_factory_;
  std::unique_ptr<const Schema> schema_;
  const Table* test_table_;
  const Index* index_;
  const Table* index_data_table_;
  const Column* int_col_;
  const Column* string_col_;
  const Column* index_string_col_;
};

TEST_F(ReadTest, ExtractTableAndColumnsFromReadArgBasic) {
  backend::ReadArg read_arg;
  read_arg.table = "test_table";
  read_arg.columns = {"int64_col", "string_col"};
  read_arg.key_set = KeySet(Key({Int64(1)}));

  const Table* table;
  std::vector<const Column*> columns;
  ZETASQL_EXPECT_OK(ExtractTableAndColumnsFromReadArg(read_arg, schema_.get(), &table,
                                              &columns));

  EXPECT_EQ(table, test_table_);
  EXPECT_THAT(columns, testing::ElementsAre(int_col_, string_col_));
}

TEST_F(ReadTest, ExtractTableAndColumnsFromReadArgEmptyReadArg) {
  backend::ReadArg read_arg;

  const Table* table;
  std::vector<const Column*> columns;
  EXPECT_THAT(ExtractTableAndColumnsFromReadArg(read_arg, schema_.get(), &table,
                                                &columns),
              zetasql_base::testing::StatusIs(zetasql_base::StatusCode::kNotFound));

  EXPECT_EQ(table, nullptr);
  EXPECT_THAT(columns, testing::ElementsAre());
}

TEST_F(ReadTest, ExtractTableAndColumnsFromReadArgInvalidTable) {
  backend::ReadArg read_arg;
  read_arg.table = "invalid_table";
  read_arg.columns = {"int64_col", "string_col"};
  read_arg.key_set = KeySet(Key({Int64(1)}));

  const Table* table;
  std::vector<const Column*> columns;
  EXPECT_THAT(ExtractTableAndColumnsFromReadArg(read_arg, schema_.get(), &table,
                                                &columns),
              zetasql_base::testing::StatusIs(zetasql_base::StatusCode::kNotFound));
}

TEST_F(ReadTest, ExtractTableAndColumnsFromReadArgInvalidColumns) {
  backend::ReadArg read_arg;
  read_arg.table = "test_table";
  read_arg.columns = {"invalid_col"};
  read_arg.key_set = KeySet(Key({Int64(1)}));

  const Table* table;
  std::vector<const Column*> columns;
  EXPECT_THAT(ExtractTableAndColumnsFromReadArg(read_arg, schema_.get(), &table,
                                                &columns),
              zetasql_base::testing::StatusIs(zetasql_base::StatusCode::kNotFound));
}

TEST_F(ReadTest, ExtractTableAndColumnsFromReadArgWithIndex) {
  backend::ReadArg read_arg;
  read_arg.table = "test_table";
  read_arg.index = "test_index";
  read_arg.columns = {"string_col"};
  read_arg.key_set = KeySet(Key({String("value")}));

  const Table* table;
  std::vector<const Column*> columns;
  ZETASQL_EXPECT_OK(ExtractTableAndColumnsFromReadArg(read_arg, schema_.get(), &table,
                                              &columns));

  EXPECT_EQ(table, index_->index_data_table());
  EXPECT_THAT(columns, testing::ElementsAre(index_string_col_));
}

TEST_F(ReadTest, ExtractTableAndColumnsFromReadArgWithInvalidIndex) {
  backend::ReadArg read_arg;
  read_arg.table = "test_table";
  read_arg.index = "invalid_index";
  read_arg.columns = {"string_col"};
  read_arg.key_set = KeySet(Key({String("value")}));

  const Table* table;
  std::vector<const Column*> columns;
  EXPECT_THAT(ExtractTableAndColumnsFromReadArg(read_arg, schema_.get(), &table,
                                                &columns),
              zetasql_base::testing::StatusIs(zetasql_base::StatusCode::kNotFound));
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
