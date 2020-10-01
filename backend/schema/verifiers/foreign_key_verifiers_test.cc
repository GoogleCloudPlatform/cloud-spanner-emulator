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

#include "backend/schema/verifiers/foreign_key_verifiers.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "backend/database/database.h"
#include "backend/transaction/options.h"
#include "backend/transaction/read_write_transaction.h"
#include "common/clock.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

using zetasql::values::Int64;
using zetasql::values::NullInt64;
using zetasql_base::testing::StatusIs;

// Unit tests for the foreign key verifier. These include branch coverage of the
// verifier code. Separate conformance tests cover more detailed end-to-end
// tests for different foreign key shapes and initial data states.
class ForeignKeyVerifiersTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ZETASQL_ASSERT_OK(CreateDatabase({R"(
        CREATE TABLE T (
          A INT64,
          B INT64,
          C INT64,
        ) PRIMARY KEY(A))",
                              R"(
        CREATE TABLE U (
          X INT64,
          Y INT64,
          Z INT64,
        ) PRIMARY KEY(X))"}));
  }

  absl::Status AddForeignKey() {
    return UpdateSchema({R"(
        ALTER TABLE U
          ADD CONSTRAINT C
            FOREIGN KEY(Z, Y)
            REFERENCES T(B, C))"});
  }

  absl::Status CreateDatabase(const std::vector<std::string>& statements) {
    ZETASQL_ASSIGN_OR_RETURN(database_, Database::Create(&clock_, statements));
    return absl::OkStatus();
  }

  absl::Status UpdateSchema(absl::Span<const std::string> statements) {
    int succesful;
    absl::Status status;
    absl::Time timestamp;
    ZETASQL_RETURN_IF_ERROR(
        database_->UpdateSchema(statements, &succesful, &timestamp, &status));
    return status;
  }

  void Insert(const std::string& table, const std::vector<std::string>& columns,
              const std::vector<int>& values) {
    Mutation m;
    m.AddWriteOp(MutationOpType::kInsert, table, columns, {AsList(values)});
    ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadWriteTransaction> txn,
                         database_->CreateReadWriteTransaction(
                             ReadWriteOptions(), RetryState()));
    ZETASQL_ASSERT_OK(txn->Write(m));
    ZETASQL_ASSERT_OK(txn->Commit());
  }

  ValueList AsList(const std::vector<int>& values) {
    ValueList value_list;
    std::transform(
        values.begin(), values.end(), std::back_inserter(value_list),
        [](int value) { return value == 0 ? NullInt64() : Int64(value); });
    return value_list;
  }

  Clock clock_;
  std::unique_ptr<Database> database_;
};

TEST_F(ForeignKeyVerifiersTest, NoExistingData) { ZETASQL_EXPECT_OK(AddForeignKey()); }

TEST_F(ForeignKeyVerifiersTest, ValidExistingData) {
  Insert("T", {"A", "B", "C"}, {1, 2, 3});
  Insert("U", {"X", "Y", "Z"}, {4, 3, 2});
  ZETASQL_EXPECT_OK(AddForeignKey());
}

TEST_F(ForeignKeyVerifiersTest, InvalidExistingData) {
  Insert("T", {"A", "B", "C"}, {1, 2, 3});
  Insert("U", {"X", "Y", "Z"}, {4, 2, 3});
  EXPECT_THAT(AddForeignKey(), StatusIs(absl::StatusCode::kFailedPrecondition));
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
