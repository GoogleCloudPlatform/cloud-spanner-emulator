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

#include "backend/schema/verifiers/column_value_verifiers.h"

#include <memory>

#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/memory/memory.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "backend/database/database.h"
#include "backend/schema/builders/column_builder.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "backend/storage/in_memory_storage.h"
#include "common/clock.h"
#include "common/errors.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

using zetasql::values::BytesArray;
using zetasql::values::Int64;
using zetasql::values::NullString;
using zetasql::values::String;
using zetasql::values::StringArray;
using zetasql::values::Timestamp;

class ColumnValueVerifiersTest : public ::testing::Test {
 public:
  ColumnValueVerifiersTest() {}

  absl::Status UpdateSchema(absl::Span<const std::string> update_statements) {
    int num_succesful;
    absl::Status backfill_status;
    absl::Time update_time;
    ZETASQL_RETURN_IF_ERROR(database_->UpdateSchema(update_statements, &num_succesful,
                                            &update_time, &backfill_status));
    return backfill_status;
  }

  void SetUp() override {
    ZETASQL_ASSERT_OK_AND_ASSIGN(database_, Database::Create(&clock_, {R"(
                            CREATE TABLE TestTable (
                              int64_col INT64,
                              string_col STRING(30),
                              string_array_col ARRAY<STRING(30)>,
                              bytes_array_col ARRAY<BYTES(30)>,
                              timestamp_col TIMESTAMP
                            ) PRIMARY KEY (int64_col)
                          )"}));

    ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadWriteTransaction> txn,
                         database_->CreateReadWriteTransaction(
                             ReadWriteOptions(), RetryState()));

    commit_ts_value_ = absl::Now() + absl::Minutes(15);
    Mutation m;
    m.AddWriteOp(MutationOpType::kInsert, "TestTable",
                 {"int64_col", "string_col"}, {{Int64(1), NullString()}});
    m.AddWriteOp(MutationOpType::kInsert, "TestTable",
                 {"int64_col", "string_col"},
                 {{Int64(2), String("test-long-value")}});
    m.AddWriteOp(MutationOpType::kInsert, "TestTable",
                 {"int64_col", "timestamp_col"},
                 {{Int64(3), Timestamp(commit_ts_value_)}});
    m.AddWriteOp(MutationOpType::kInsert, "TestTable",
                 {"int64_col", "string_array_col"},
                 {{Int64(4), StringArray({"abcdefghijklmnopqrstuvwxyz"})}});
    m.AddWriteOp(MutationOpType::kInsert, "TestTable",
                 {"int64_col", "bytes_array_col"},
                 {{Int64(5), BytesArray({"1234567890!@#$%^&*()"})}});
    ZETASQL_ASSERT_OK(txn->Write(m));
    ZETASQL_ASSERT_OK(txn->Commit());
  }

 protected:
  Clock clock_;

  std::unique_ptr<Database> database_;

  absl::Time commit_ts_value_;
};

TEST_F(ColumnValueVerifiersTest, VerifyNotNullValue) {
  EXPECT_EQ(UpdateSchema({R"(
    ALTER TABLE TestTable ALTER COLUMN string_col STRING(30) NOT NULL
  )"}),
            error::NullValueForNotNullColumn("TestTable", "string_col",
                                             "{Int64(1)}"));
}

TEST_F(ColumnValueVerifiersTest, VerifyColumnLength) {
  EXPECT_EQ(
      UpdateSchema({R"(
    ALTER TABLE TestTable ALTER COLUMN string_col STRING(10)
  )"}),
      error::InvalidColumnSizeReduction("string_col", 10, 15, "{Int64(2)}"));
}

TEST_F(ColumnValueVerifiersTest, VerifyStringArrayColumnLength) {
  EXPECT_EQ(UpdateSchema({R"(
    ALTER TABLE TestTable ALTER COLUMN string_array_col ARRAY<STRING(25)>
  )"}),
            error::InvalidColumnSizeReduction("string_array_col", 25, 26,
                                              "{Int64(4)}"));
}

TEST_F(ColumnValueVerifiersTest, VerifyBytesArrayColumnLength) {
  EXPECT_EQ(UpdateSchema({R"(
    ALTER TABLE TestTable ALTER COLUMN bytes_array_col ARRAY<BYTES(15)>
  )"}),
            error::InvalidColumnSizeReduction("bytes_array_col", 15, 20,
                                              "{Int64(5)}"));
}

TEST_F(ColumnValueVerifiersTest, VerifyBytesToStringArrayChange) {
  EXPECT_EQ(UpdateSchema({R"(
    ALTER TABLE TestTable ALTER COLUMN bytes_array_col ARRAY<STRING(15)>
  )"}),
            error::InvalidColumnSizeReduction("bytes_array_col", 15, 20,
                                              "{Int64(5)}"));
}

TEST_F(ColumnValueVerifiersTest, VerifyStringToBytesArrayChange) {
  EXPECT_EQ(UpdateSchema({R"(
    ALTER TABLE TestTable ALTER COLUMN string_array_col ARRAY<BYTES(15)>
  )"}),
            error::InvalidColumnSizeReduction("string_array_col", 15, 26,
                                              "{Int64(4)}"));
}

TEST_F(ColumnValueVerifiersTest, VerifyColumnCommitTimestamp) {
  EXPECT_EQ(UpdateSchema({R"(
    ALTER TABLE TestTable ALTER COLUMN timestamp_col SET OPTIONS (
      allow_commit_timestamp = true
    )
  )"}),
            error::CommitTimestampNotInFuture("timestamp_col", "{Int64(3)}",
                                              commit_ts_value_));
}

TEST_F(ColumnValueVerifiersTest, VerifyColumnTypeChange) {
  EXPECT_EQ(
      UpdateSchema({R"(
    ALTER TABLE TestTable ALTER COLUMN string_col BYTES(10)
  )"}),
      error::InvalidColumnSizeReduction("string_col", 10, 15, "{Int64(2)}"));
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
