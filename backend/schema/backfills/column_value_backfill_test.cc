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

#include "backend/schema/backfills/column_value_backfill.h"

#include <memory>
#include <vector>

#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "zetasql/base/statusor.h"
#include "absl/types/span.h"
#include "backend/database/database.h"
#include "backend/datamodel/key_set.h"
#include "backend/schema/catalog/schema.h"
#include "backend/transaction/options.h"
#include "common/errors.h"
#include "tests/common/actions.h"
#include "tests/common/scoped_feature_flags_setter.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

using zetasql::values::Array;
using zetasql::values::Bytes;
using zetasql::values::Int64;
using zetasql::values::Null;
using zetasql::values::NullBytes;
using zetasql::values::NullString;
using zetasql::values::String;

class ColumnValueBackfillTest : public ::testing::Test {
 public:
  ColumnValueBackfillTest()
      : emulator_feature_flags_({.enable_stored_generated_columns = true}) {}

 protected:
  void SetUp() override {
    std::vector<std::string> create_statements;
    ZETASQL_ASSERT_OK_AND_ASSIGN(database_, Database::Create(&clock_, {R"(
                            CREATE TABLE TestTable (
                              int64_col INT64,
                              string_col STRING(10),
                              string_array_col ARRAY<STRING(MAX)>,
                              bytes_array_col ARRAY<BYTES(MAX)>
                            ) PRIMARY KEY (int64_col)
                         )"}));

    ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadWriteTransaction> txn,
                         database_->CreateReadWriteTransaction(
                             ReadWriteOptions(), RetryState()));

    const auto* test_table = txn->schema()->FindTable("TestTable");
    ASSERT_NE(test_table, nullptr);

    string_array_type_ =
        test_table->FindColumn("string_array_col")->GetType()->AsArray();
    bytes_array_type_ =
        test_table->FindColumn("bytes_array_col")->GetType()->AsArray();

    Mutation m;
    m.AddWriteOp(
        MutationOpType::kInsert, "TestTable",
        {"int64_col", "string_col", "string_array_col"},
        {{Int64(1), String("ФдΣβaA"),
          Array(string_array_type_, {String("ФдΣβaA"), NullString()})}});

    // Add a row with null values as well.
    m.AddWriteOp(MutationOpType::kInsert, "TestTable", {"int64_col"},
                 {{Int64(2)}});

    ZETASQL_EXPECT_OK(txn->Write(m));
    ZETASQL_EXPECT_OK(txn->Commit());
  }

  absl::Status UpdateSchema(absl::Span<const std::string> update_statements) {
    int num_succesful;
    absl::Status backfill_status;
    absl::Time update_time;
    ZETASQL_RETURN_IF_ERROR(database_->UpdateSchema(update_statements, &num_succesful,
                                            &update_time, &backfill_status));
    return backfill_status;
  }

  std::vector<zetasql::Value> ColumnValues(const std::string& column_name) {
    zetasql_base::StatusOr<std::unique_ptr<ReadOnlyTransaction>> status_or =
        database_->CreateReadOnlyTransaction(ReadOnlyOptions());
    ZETASQL_EXPECT_OK(status_or.status());

    auto txn = std::move(status_or).ValueOrDie();
    std::unique_ptr<backend::RowCursor> cursor;
    ReadArg args;
    args.table = "TestTable";
    args.key_set = KeySet::All();
    args.columns = std::vector<std::string>{column_name};
    ZETASQL_EXPECT_OK(txn->Read(args, &cursor));

    std::vector<zetasql::Value> values;
    while (cursor->Next()) {
      values.push_back(cursor->ColumnValue(0));
    }
    return values;
  }

  Clock clock_;
  std::unique_ptr<Database> database_;
  const zetasql::ArrayType* string_array_type_ = nullptr;
  const zetasql::ArrayType* bytes_array_type_ = nullptr;
  emulator::test::ScopedEmulatorFeatureFlagsSetter emulator_feature_flags_;
};

TEST_F(ColumnValueBackfillTest, FailedBackfillHasNoEffect) {
  // Failed column type change.
  EXPECT_EQ(
      UpdateSchema({R"(
    ALTER TABLE TestTable ALTER COLUMN string_col BYTES(7)
    )"}),
      error::InvalidColumnSizeReduction("string_col", 7, 10, "{Int64(1)}"));

  // Check that the column values are unchanged.
  EXPECT_THAT(ColumnValues("string_col"), testing::ElementsAreArray({
                                              String("ФдΣβaA"),
                                              NullString(),
                                          }));
}

TEST_F(ColumnValueBackfillTest, BackfillAtomicType) {
  ZETASQL_EXPECT_OK(UpdateSchema({R"(
    ALTER TABLE TestTable ALTER COLUMN string_col BYTES(10)
    )"}));

  EXPECT_THAT(ColumnValues("string_col"), testing::ElementsAreArray({
                                              Bytes("ФдΣβaA"),
                                              NullBytes(),
                                          }));
}

TEST_F(ColumnValueBackfillTest, BackfillArrayType) {
  ZETASQL_EXPECT_OK(UpdateSchema({R"(
    ALTER TABLE TestTable ALTER COLUMN string_array_col ARRAY<BYTES(10)>
    )"}));

  EXPECT_THAT(ColumnValues("string_array_col"),
              testing::ElementsAreArray({
                  Array(bytes_array_type_, {Bytes("ФдΣβaA"), NullBytes()}),
                  Null(bytes_array_type_),
              }));
}

TEST_F(ColumnValueBackfillTest, BackfillGeneratedColumn) {
  ZETASQL_ASSERT_OK(UpdateSchema({R"(
    ALTER TABLE TestTable
      ADD COLUMN gen_col_1 STRING(MAX) AS (string_col) STORED
    )"}));

  EXPECT_THAT(ColumnValues("gen_col_1"), testing::ElementsAreArray({
                                             String("ФдΣβaA"),
                                             NullString(),
                                         }));
  ZETASQL_ASSERT_OK(UpdateSchema({R"(
    ALTER TABLE TestTable
      ADD COLUMN gen_col_2 STRING(MAX) AS (gen_col_1) STORED
    )"}));
  EXPECT_THAT(ColumnValues("gen_col_2"), testing::ElementsAreArray({
                                             String("ФдΣβaA"),
                                             NullString(),
                                         }));
}

TEST_F(ColumnValueBackfillTest, BackfillGeneratedColumnNotNullFail) {
  EXPECT_THAT(
      UpdateSchema({R"(
    ALTER TABLE TestTable
      ADD COLUMN gen_col STRING(MAX) NOT NULL AS (string_col) STORED
    )"}),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kFailedPrecondition,
          testing::HasSubstr("Cannot specify a null value for column")));
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
