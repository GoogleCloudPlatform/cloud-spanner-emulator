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

#include "backend/schema/verifiers/check_constraint_verifiers.h"

#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "backend/database/database.h"
#include "backend/transaction/options.h"
#include "backend/transaction/read_write_transaction.h"
#include "common/clock.h"
#include "tests/common/scoped_feature_flags_setter.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

using google::spanner::emulator::test::ScopedEmulatorFeatureFlagsSetter;
using zetasql::values::Int64;
using zetasql_base::testing::StatusIs;

class CheckConstraintVerifiersTest : public ::testing::Test {
 public:
  CheckConstraintVerifiersTest()
      : feature_flags_({.enable_stored_generated_columns = true,
                        .enable_check_constraint = true}) {}

 protected:
  void SetUp() override {
    ZETASQL_ASSERT_OK_AND_ASSIGN(database_, Database::Create(&clock_, {R"(
          CREATE TABLE T (
          A INT64,
          B INT64,
          C INT64 AS (A + B) STORED,
        ) PRIMARY KEY(A))"}));

    ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadWriteTransaction> txn,
                         database_->CreateReadWriteTransaction(
                             ReadWriteOptions(), RetryState()));

    commit_ts_value_ = absl::Now() + absl::Minutes(15);
    Mutation m;
    m.AddWriteOp(MutationOpType::kInsert, "T", {"A", "B"},
                 {{Int64(1), Int64(1)}});
    m.AddWriteOp(MutationOpType::kInsert, "T", {"A", "B"},
                 {{Int64(2), Int64(-1)}});
    m.AddWriteOp(MutationOpType::kInsert, "T", {"A", "B"},
                 {{Int64(3), Int64(1)}});
    m.AddWriteOp(MutationOpType::kInsert, "T", {"A", "B"},
                 {{Int64(4), zetasql::values::NullInt64()}});
    // Value of B is not present. This is to verify that it will be converted to
    // NULL in check_constraint_verifiers.
    m.AddWriteOp(MutationOpType::kInsert, "T", {"A"}, {{Int64(5)}});
    ZETASQL_ASSERT_OK(txn->Write(m));
    ZETASQL_ASSERT_OK(txn->Commit());
  }

  absl::Status UpdateSchema(absl::Span<const std::string> statements) {
    int succesful;
    absl::Status status;
    absl::Time timestamp;
    ZETASQL_RETURN_IF_ERROR(
        database_->UpdateSchema(statements, &succesful, &timestamp, &status));
    return status;
  }

  Clock clock_;

  std::unique_ptr<Database> database_;

  absl::Time commit_ts_value_;

 private:
  ScopedEmulatorFeatureFlagsSetter feature_flags_;
};

TEST_F(CheckConstraintVerifiersTest, ValidExistingData) {
  ZETASQL_ASSERT_OK(
      UpdateSchema({"ALTER TABLE T"
                    " ADD CONSTRAINT a_gt_zero CHECK(A > 0)"}));

  ZETASQL_ASSERT_OK(
      UpdateSchema({"ALTER TABLE T"
                    " ADD CONSTRAINT c_gt_zero CHECK(C > 0)"}));
}

TEST_F(CheckConstraintVerifiersTest, InalidExistingData) {
  EXPECT_THAT(UpdateSchema({"ALTER TABLE T"
                            " ADD CONSTRAINT b_gt_zero CHECK(B > 0)"}),
              StatusIs(absl::StatusCode::kOutOfRange));

  EXPECT_THAT(UpdateSchema({"ALTER TABLE T"
                            " ADD CONSTRAINT c_gt_zero CHECK(C > 5)"}),
              StatusIs(absl::StatusCode::kOutOfRange));
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
