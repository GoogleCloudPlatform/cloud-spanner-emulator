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

#include "backend/schema/backfills/change_stream_backfill.h"

#include <memory>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/container/flat_hash_set.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "backend/database/database.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/updater/schema_updater.h"
#include "backend/transaction/options.h"
#include "common/errors.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

constexpr char kDatabaseId[] = "test-db";

class ChangeStreamBackfillTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::vector<std::string> create_statements = {R"(
                              CREATE CHANGE STREAM C FOR ALL)"};
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        database_, Database::Create(
                       &clock_, kDatabaseId,
                       SchemaChangeOperation{.statements = create_statements}));
  }
  // Test components.
  Clock clock_;
  std::unique_ptr<Database> database_;
  const Schema* schema_;
};

TEST_F(ChangeStreamBackfillTest, BackfillChangeStreamPartitionTable) {
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadWriteTransaction> txn,
                         database_->CreateReadWriteTransaction(
                             ReadWriteOptions(), RetryState()));
    // Check that change stream partition table and change stream data table
    // exist
    schema_ = txn->schema();
    EXPECT_NE(schema_->FindChangeStream("C"), nullptr);
    ASSERT_NE(schema_->FindChangeStream("C")->change_stream_partition_table(),
              nullptr);
    ASSERT_NE(schema_->FindChangeStream("C")->change_stream_data_table(),
              nullptr);
  }
  // Verify current values in table.
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<ReadOnlyTransaction> txn,
        database_->CreateReadOnlyTransaction(ReadOnlyOptions()));
    std::unique_ptr<backend::RowCursor> cursor;
    backend::ReadArg read_arg;
    read_arg.change_stream_for_partition_table = "C";
    read_arg.columns = {"partition_token", "start_time", "end_time", "parents",
                        "children"};
    read_arg.key_set = KeySet::All();

    ZETASQL_EXPECT_OK(txn->Read(read_arg, &cursor));
    std::vector<zetasql::Value> values;
    while (cursor->Next()) {
      values.push_back(cursor->ColumnValue(0));
      values.push_back(cursor->ColumnValue(1));
      values.push_back(cursor->ColumnValue(2));
      values.push_back(cursor->ColumnValue(3));
      values.push_back(cursor->ColumnValue(4));
    }
    EXPECT_THAT(values.size(), 10);
  }
}

TEST_F(ChangeStreamBackfillTest, TestNoDuplicateTokenStringsAreGenerated) {
  absl::flat_hash_set<std::string> token_strings;
  absl::Time end_time = absl::Now() + absl::Milliseconds(900);
  while (absl::Now() < end_time) {
    ASSERT_TRUE(token_strings.insert(CreatePartitionTokenString()).second);
  }
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
