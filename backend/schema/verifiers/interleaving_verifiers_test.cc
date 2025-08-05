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

#include <memory>
#include <string>
#include <vector>

#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "backend/access/write.h"
#include "backend/database/database.h"
#include "backend/schema/updater/schema_updater.h"
#include "backend/transaction/options.h"
#include "backend/transaction/read_write_transaction.h"
#include "common/clock.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

using google::spanner::emulator::test::ScopedEmulatorFeatureFlagsSetter;
using zetasql::values::Int64;
using zetasql::values::NullInt64;
using zetasql_base::testing::StatusIs;

constexpr char kDatabaseId[] = "test-db";

class InterleavingVerifiersTest : public ::testing::Test {
 public:
  InterleavingVerifiersTest()
      : feature_flags_({.enable_interleave_in = true}) {}

 protected:
  void SetUp() override {
    std::vector<std::string> statements = {R"(
          CREATE TABLE T (
          K1 INT64,
          V INT64,
        ) PRIMARY KEY(K1))",
                                           R"(
          CREATE TABLE C (
          K1 INT64,
          K2 INT64,
          V INT64,
        ) PRIMARY KEY(K1,K2), INTERLEAVE IN T)"};
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        database_,
        Database::Create(&clock_, kDatabaseId,
                         SchemaChangeOperation{.statements = statements}));
  }

  absl::Status UpdateSchema(absl::Span<const std::string> statements) {
    int successful;
    absl::Status status;
    absl::Time timestamp;
    ZETASQL_RETURN_IF_ERROR(
        database_->UpdateSchema(SchemaChangeOperation{.statements = statements},
                                &successful, &timestamp, &status));
    return status;
  }

  Clock clock_;

  std::unique_ptr<Database> database_;

  absl::Time commit_ts_value_;

 private:
  ScopedEmulatorFeatureFlagsSetter feature_flags_;
};

TEST_F(InterleavingVerifiersTest, VerifierRunsOnEmptyTables) {
  ZETASQL_ASSERT_OK(UpdateSchema({"ALTER TABLE C SET INTERLEAVE IN PARENT T"}));

  ZETASQL_ASSERT_OK(UpdateSchema({"ALTER TABLE C SET INTERLEAVE IN T"}));
}

TEST_F(InterleavingVerifiersTest, VerifierRunsOnNullAndNonNullKeys) {
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadWriteTransaction> txn,
                         database_->CreateReadWriteTransaction(
                             ReadWriteOptions(), RetryState()));
    // Add a row with a NULL key to T.
    // Add 2 rows to C: (NULL, 1) and (2, 2).
    Mutation m;
    m.AddWriteOp(MutationOpType::kInsert, "T", {"K1"}, {{NullInt64()}});
    m.AddWriteOp(MutationOpType::kInsert, "C", {"K1", "K2"},
                 {{NullInt64(), Int64(1)}});
    m.AddWriteOp(MutationOpType::kInsert, "C", {"K1", "K2"},
                 {{Int64(2), Int64(2)}});
    ZETASQL_ASSERT_OK(txn->Write(m));
    ZETASQL_ASSERT_OK(txn->Commit());
  }

  // Cannot migrate to INTERLEAVE IN PARENT because the parent row does not
  // exist for key (2).
  ASSERT_THAT(UpdateSchema({"ALTER TABLE C SET INTERLEAVE IN PARENT T"}),
              StatusIs(absl::StatusCode::kFailedPrecondition));

  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadWriteTransaction> txn,
                         database_->CreateReadWriteTransaction(
                             ReadWriteOptions(), RetryState()));
    // Add a row with key 2 to T.
    Mutation m;
    m.AddWriteOp(MutationOpType::kInsert, "T", {"K1"}, {{Int64(2)}});
    ZETASQL_ASSERT_OK(txn->Write(m));
    ZETASQL_ASSERT_OK(txn->Commit());
  }

  // Migrate to INTERLEAVE IN PARENT should now succeed.
  ZETASQL_ASSERT_OK(UpdateSchema({"ALTER TABLE C SET INTERLEAVE IN PARENT T"}));

  ZETASQL_ASSERT_OK(UpdateSchema({"ALTER TABLE C SET INTERLEAVE IN T"}));

  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadWriteTransaction> txn,
                         database_->CreateReadWriteTransaction(
                             ReadWriteOptions(), RetryState()));
    // Delete the row with key (2) from T & C
    Mutation m;
    m.AddWriteOp(MutationOpType::kDelete, "C", {"K1", "K2"},
                 {{Int64(2), Int64(2)}});
    m.AddWriteOp(MutationOpType::kDelete, "T", {"K1"}, {{Int64(2)}});
    ZETASQL_ASSERT_OK(txn->Write(m));
    ZETASQL_ASSERT_OK(txn->Commit());
  }

  // Migration should still succeed.
  ZETASQL_ASSERT_OK(UpdateSchema({"ALTER TABLE C SET INTERLEAVE IN PARENT T"}));
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
