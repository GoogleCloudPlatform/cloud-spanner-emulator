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

#include "backend/transaction/resolve.h"

#include <memory>

#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "backend/access/write.h"
#include "backend/datamodel/key_range.h"
#include "backend/datamodel/key_set.h"
#include "backend/schema/catalog/schema.h"
#include "backend/storage/in_memory_storage.h"
#include "common/clock.h"
#include "common/errors.h"
#include "tests/common/schema_constructor.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

using zetasql::values::Int64;
using zetasql::values::String;
using zetasql_base::testing::StatusIs;

class ResolveTest : public testing::Test {
 public:
  ResolveTest()
      : type_factory_(std::make_unique<zetasql::TypeFactory>()),
        schema_(test::CreateSchemaFromDDL(
                    {
                        R"(
                          CREATE TABLE TestTable (
                            Int64Col    INT64 NOT NULL,
                            StringCol   STRING(MAX),
                            Int64ValCol INT64
                          ) PRIMARY KEY (Int64Col)
                        )",
                        R"(
                          CREATE UNIQUE INDEX TestIndex ON TestTable(StringCol DESC)
                        )",
                        R"(
                          CREATE CHANGE STREAM ChangeStream_TestTable FOR ALL
                          )"},
                    type_factory_.get())
                    .value()),
        test_table_(schema_->FindTable("TestTable")),
        index_(schema_->FindIndex("TestIndex")),
        index_data_table_(index_->index_data_table()),
        int_col_(test_table_->FindColumn("Int64Col")),
        string_col_(test_table_->FindColumn("StringCol")),
        index_string_col_(index_data_table_->FindColumn("StringCol")),
        change_stream_(schema_->FindChangeStream("ChangeStream_TestTable")),
        change_stream_partition_table_(
            change_stream_->change_stream_partition_table()),
        change_stream_data_table_(change_stream_->change_stream_data_table()) {}

 protected:
  Clock clock_;
  std::unique_ptr<zetasql::TypeFactory> type_factory_;
  std::unique_ptr<const Schema> schema_;
  const Table* test_table_;
  const Index* index_;
  const Table* index_data_table_;
  const Column* int_col_;
  const Column* string_col_;
  const Column* index_string_col_;
  const ChangeStream* change_stream_;
  const Table* change_stream_partition_table_;
  const Table* change_stream_data_table_;
};

TEST_F(ResolveTest, CanResolveTableAndColumnsFromReadArg) {
  backend::ReadArg read_arg;
  read_arg.table = "TestTable";
  read_arg.columns = {"Int64Col", "StringCol"};
  read_arg.key_set = KeySet(Key({Int64(1)}));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto resolved_read_arg,
                       ResolveReadArg(read_arg, schema_.get()));

  EXPECT_EQ(resolved_read_arg.table, test_table_);
  EXPECT_THAT(resolved_read_arg.columns,
              testing::ElementsAre(int_col_, string_col_));
}

TEST_F(ResolveTest, CanResolveChangeStreamInternalPartitionTableFromReadArg) {
  backend::ReadArg read_arg;
  read_arg.change_stream_for_partition_table = "ChangeStream_TestTable";

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto resolved_read_arg,
                       ResolveReadArg(read_arg, schema_.get()));

  EXPECT_EQ(resolved_read_arg.table, change_stream_partition_table_);
}
TEST_F(ResolveTest,
       CannotResolveUnexistedChangeStreamInternalPartitionTableFromReadArg) {
  backend::ReadArg read_arg;
  read_arg.change_stream_for_partition_table = "ChangeStream_FooBar";

  EXPECT_THAT(ResolveReadArg(read_arg, schema_.get()),
              StatusIs(absl::StatusCode::kNotFound));
}
TEST_F(ResolveTest, CanResolveChangeStreamInternalDataTableFromReadArg) {
  backend::ReadArg read_arg;
  read_arg.change_stream_for_data_table = "ChangeStream_TestTable";

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto resolved_read_arg,
                       ResolveReadArg(read_arg, schema_.get()));

  EXPECT_EQ(resolved_read_arg.table, change_stream_data_table_);
}
TEST_F(ResolveTest,
       CannotResolveUnexistedChangeStreamInternalDataTableFromReadArg) {
  backend::ReadArg read_arg;
  read_arg.change_stream_for_data_table = "ChangeStream_FooBar";

  EXPECT_THAT(ResolveReadArg(read_arg, schema_.get()),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(ResolveTest, CannotResolveEmptyReadArg) {
  backend::ReadArg read_arg;
  EXPECT_THAT(ResolveReadArg(read_arg, schema_.get()),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(ResolveTest, CannotResolveReadArgWithInvalidTable) {
  backend::ReadArg read_arg;
  read_arg.table = "InvalidTable";
  read_arg.columns = {"Int64Col", "StringCol"};
  read_arg.key_set = KeySet(Key({Int64(1)}));

  EXPECT_THAT(ResolveReadArg(read_arg, schema_.get()),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(ResolveTest, CannotResolveReadArgWithInvalidColumns) {
  backend::ReadArg read_arg;
  read_arg.table = "TestTable";
  read_arg.columns = {"InvalidCol"};
  read_arg.key_set = KeySet(Key({Int64(1)}));

  EXPECT_THAT(ResolveReadArg(read_arg, schema_.get()),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(ResolveTest, CanResolveTableAndColumnsFromReadArgWithIndex) {
  backend::ReadArg read_arg;
  read_arg.table = "TestTable";
  read_arg.index = "TestIndex";
  read_arg.columns = {"StringCol"};
  read_arg.key_set = KeySet(Key({String("value")}));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto resolved_read_arg,
                       ResolveReadArg(read_arg, schema_.get()));

  EXPECT_EQ(resolved_read_arg.table, index_->index_data_table());
  EXPECT_THAT(resolved_read_arg.columns,
              testing::ElementsAre(index_string_col_));
}

TEST_F(ResolveTest, CannotResolveReadArgWithInvalidIndex) {
  backend::ReadArg read_arg;
  read_arg.table = "TestTable";
  read_arg.index = "InvalidIndex";
  read_arg.columns = {"StringCol"};
  read_arg.key_set = KeySet(Key({String("value")}));

  EXPECT_THAT(ResolveReadArg(read_arg, schema_.get()),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(ResolveTest, CanResolveDeleteMutationOp) {
  backend::MutationOp mutation_op;
  mutation_op.type = MutationOpType::kDelete;
  mutation_op.table = "TestTable";
  Key k1({Int64(1)});
  KeySet ks;
  ks.AddKey(k1);

  mutation_op.key_set = ks;

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const ResolvedMutationOp& resolved_mutation_op,
      ResolveDeleteMutationOp(mutation_op, schema_.get(), clock_.Now()));

  EXPECT_EQ(resolved_mutation_op.table, test_table_);
  EXPECT_EQ(resolved_mutation_op.type, MutationOpType::kDelete);
  EXPECT_THAT(resolved_mutation_op.key_ranges,
              testing::ElementsAre(KeyRange::Point(k1)));
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
