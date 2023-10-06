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

#include "backend/transaction/actions.h"

#include <cstdint>
#include <memory>
#include <vector>

#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/log.h"
#include "absl/types/span.h"
#include "backend/actions/context.h"
#include "backend/actions/ops.h"
#include "backend/datamodel/key.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "tests/common/schema_constructor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

class ActionsTest : public testing::Test {
 public:
  ActionsTest()
      : type_factory_(std::make_unique<zetasql::TypeFactory>()),
        schema_(::google::spanner::emulator::test::CreateSchemaFromDDL(
                    {
                        R"(
                          CREATE TABLE TestTable (
                            Int64Col    INT64 NOT NULL,
                            StringCol   STRING(MAX),
                            Int64ValCol INT64
                          ) PRIMARY KEY (Int64Col)
                        )",
                        R"(
                          CREATE TABLE TestTable2 (
                            Int64Col    INT64 NOT NULL,
                            StringCol   STRING(MAX),
                            Int64ValCol INT64
                          ) PRIMARY KEY (Int64Col)
                        )",
                        R"(
                          CREATE CHANGE STREAM ChangeStream_TestTable FOR ALL
                          )"},
                    type_factory_.get())
                    .value()),
        change_stream_effects_buffer_(
            std::make_unique<ChangeStreamTransactionEffectsBuffer>(5)),
        test_table_(schema_->FindTable("TestTable")),
        test_table2_(schema_->FindTable("TestTable2")),
        int_col_(test_table_->FindColumn("Int64Col")),
        string_col_(test_table_->FindColumn("StringCol")),
        change_stream_(schema_->FindChangeStream("ChangeStream_TestTable")),
        change_stream_partition_table_(
            change_stream_->change_stream_partition_table()),
        change_stream_data_table_(change_stream_->change_stream_data_table()) {}

 protected:
  std::unique_ptr<zetasql::TypeFactory> type_factory_;
  std::unique_ptr<const Schema> schema_;
  std::unique_ptr<ChangeStreamEffectsBuffer> change_stream_effects_buffer_;
  const Table* test_table_;
  const Table* test_table2_;
  const Column* int_col_;
  const Column* string_col_;
  const ChangeStream* change_stream_;
  const Table* change_stream_partition_table_;
  const Table* change_stream_data_table_;
  absl::flat_hash_map<const ChangeStream*, ModGroup>
      last_mod_group_by_change_stream_;
};

TEST_F(ActionsTest, TestSingleInsert) {
  zetasql::Value partition_token_str = zetasql::Value::String(
      "cnJLWVo1MFBhc2liWUtoT1RaUFBHeEZPRUdadzlqeDFhSFo3TllYdlFEV3d"
      "ZQkt6a2hPeWZiTHRxc3F4Ymx5QzJXSFA0bUtTMG9QWTBoWGlQdGg0VDJ4SH"
      "VsRVVXQmVaaHZ3amlGOWkzV0UxRWprZGFQaDFQY0lIT1hsc2dQUE5MTTQxW"
      "kRoY2t2ZXdjTVpBbEVDQXFVUEN6QjVkWVUwdG8yc0xFWnk");
  std::vector<const Column*> columns = {test_table_->FindColumn("Int64Col")};
  std::vector<zetasql::Value> values = {zetasql::Value::Int64(1)};
  const InsertOp& insert_op_1 = {.table = test_table_,
                                 .key = Key(values),
                                 .columns = {columns},
                                 .values = {values}};
  change_stream_effects_buffer_->Insert(partition_token_str, change_stream_,
                                        insert_op_1);
  const InsertOp& insert_op_2 = {.table = test_table2_,
                                 .key = Key(values),
                                 .columns = {columns},
                                 .values = {values}};
  change_stream_effects_buffer_->Insert(partition_token_str, change_stream_,
                                        insert_op_2);
  std::vector<const Column*> update_columns = {
      test_table_->FindColumn("StringCol")};
  std::vector<zetasql::Value> update_values = {
      zetasql::Value::String("update_value")};
  const UpdateOp& update_op = {.table = test_table_,
                               .key = Key(values),
                               .columns = {update_columns},
                               .values = {update_values}};
  change_stream_effects_buffer_->Update(partition_token_str, change_stream_,
                                        update_op);
  const DeleteOp& delete_op = {.table = test_table_, .key = Key(values)};
  change_stream_effects_buffer_->Delete(partition_token_str, change_stream_,
                                        delete_op);
  change_stream_effects_buffer_->BuildMutation();
  EXPECT_EQ(change_stream_effects_buffer_->GetWriteOps().size(), 4);
  change_stream_effects_buffer_->ClearWriteOps();
  EXPECT_EQ(change_stream_effects_buffer_->GetWriteOps().size(), 0);
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
