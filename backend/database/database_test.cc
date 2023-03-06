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

#include "backend/database/database.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "backend/access/read.h"
#include "backend/datamodel/key_set.h"
#include "backend/schema/updater/schema_updater.h"
#include "backend/transaction/options.h"
#include "common/clock.h"
#include "common/errors.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

using zetasql::values::Int64;
using zetasql_base::testing::StatusIs;

class DatabaseTest : public ::testing::Test {
 public:
  DatabaseTest() = default;

  ReadArg read_column(std::string table_name, std::string column_name) {
    ReadArg args;
    args.table = table_name;
    args.key_set = KeySet::All();
    args.columns = std::vector<std::string>{column_name};
    return args;
  }

 protected:
  Clock clock_;
};

TEST_F(DatabaseTest, CreateSuccessful) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Database> database,
                       Database::Create(&clock_, SchemaChangeOperation{}));

  std::vector<std::string> create_statements = {R"(
    CREATE TABLE T(
      k1 INT64,
      k2 INT64,
    ) PRIMARY KEY(k1)
  )",
                                                R"(
    CREATE INDEX I on T(k1))"};

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      database,
      Database::Create(&clock_,
                       SchemaChangeOperation{.statements = create_statements}));
}

TEST_F(DatabaseTest, UpdateSchemaSuccessful) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto db,
                       Database::Create(&clock_, SchemaChangeOperation{}));

  std::vector<std::string> update_statements = {R"(
    CREATE TABLE T(
      k1 INT64,
      k2 INT64,
    ) PRIMARY KEY(k1)
  )",
                                                R"(
    CREATE INDEX I on T(k1)
  )"};

  absl::Status backfill_status;
  int completed_statements;
  absl::Time commit_ts;
  ZETASQL_EXPECT_OK(
      db->UpdateSchema(SchemaChangeOperation{.statements = update_statements},
                       &completed_statements, &commit_ts, &backfill_status));
  ZETASQL_EXPECT_OK(backfill_status);
}

TEST_F(DatabaseTest, UpdateSchemaPartialSuccess) {
  std::vector<std::string> create_statements = {R"(
    CREATE TABLE T(
      k1 INT64,
      k2 INT64,
    ) PRIMARY KEY(k1)
  )"};
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto db, Database::Create(&clock_, SchemaChangeOperation{
                                             .statements = create_statements}));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ReadWriteTransaction> txn,
      db->CreateReadWriteTransaction(ReadWriteOptions(), RetryState()));

  Mutation m;
  m.AddWriteOp(MutationOpType::kInsert, "T", {"k1", "k2"},
               {{Int64(1), Int64(2)}});
  m.AddWriteOp(MutationOpType::kInsert, "T", {"k1", "k2"},
               {{Int64(2), Int64(2)}});
  m.AddWriteOp(MutationOpType::kInsert, "T", {"k1", "k2"},
               {{Int64(3), Int64(2)}});
  ZETASQL_ASSERT_OK(txn->Write(m));
  ZETASQL_ASSERT_OK(txn->Commit());

  std::vector<std::string> update_statements = {R"(
    CREATE TABLE T1(
      a INT64,
    ) PRIMARY KEY(a)
  )",
                                                R"(
    CREATE UNIQUE INDEX Idx on T(k2)
  )",
                                                R"(
    CREATE TABLE T2(
      b INT64,
    ) PRIMARY KEY(b)
  )"};

  absl::Status backfill_status;
  int completed_statements;
  absl::Time commit_ts;

  // The statements are semantically valid, indicated by an OK return status.
  ZETASQL_EXPECT_OK(
      db->UpdateSchema(SchemaChangeOperation{.statements = update_statements},
                       &completed_statements, &commit_ts, &backfill_status));

  // But the backfill statements fail.
  EXPECT_EQ(backfill_status,
            error::UniqueIndexViolationOnIndexCreation("Idx", "{Int64(2)}"));

  // Only the first statement in the batch is successfuly applied.
  EXPECT_EQ(completed_statements, 1);
}

TEST_F(DatabaseTest, ConcurrentSchemaChangeIsAborted) {
  std::vector<std::string> create_statements = {R"(
    CREATE TABLE T(
      k1 INT64,
      k2 INT64,
    ) PRIMARY KEY(k1)
  )"};
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto db, Database::Create(&clock_, SchemaChangeOperation{
                                             .statements = create_statements}));

  // Initiate a Read inside a read-write transaction to acquire locks.
  std::unique_ptr<RowCursor> row_cursor;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ReadWriteTransaction> txn,
      db->CreateReadWriteTransaction(ReadWriteOptions(), RetryState()));
  ZETASQL_EXPECT_OK(txn->Read(read_column("T", "k1"), &row_cursor));

  std::vector<std::string> update_statements = {R"(
    CREATE TABLE T(
      k1 INT64,
      k2 INT64,
    ) PRIMARY KEY(k1)
  )"};
  absl::Status backfill_status;
  int completed_statements;
  absl::Time commit_ts;
  EXPECT_EQ(
      db->UpdateSchema(SchemaChangeOperation{.statements = update_statements},
                       &completed_statements, &commit_ts, &backfill_status),
      error::ConcurrentSchemaChangeOrReadWriteTxnInProgress());
}

TEST_F(DatabaseTest, SchemaChangeLocksSuccesfullyReleased) {
  std::vector<std::string> create_statements = {R"(
    CREATE TABLE T(
      k1 INT64,
      k2 INT64,
    ) PRIMARY KEY(k1)
  )"};
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto db, Database::Create(&clock_, SchemaChangeOperation{
                                             .statements = create_statements}));

  // Schema update will fail.
  std::vector<std::string> update_statements = {R"(
    CREATE TABLE T(
      k1 INT64,
      k2 INT64,
    ) PRIMARY KEY(k1)
  )"};
  absl::Status backfill_status;
  int completed_statements;
  absl::Time commit_ts;
  EXPECT_FALSE(
      db->UpdateSchema(SchemaChangeOperation{.statements = update_statements},
                       &completed_statements, &commit_ts, &backfill_status)
          .ok());

  // Can still run transactions as locks would have been released.
  std::unique_ptr<RowCursor> row_cursor;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ReadWriteTransaction> txn,
      db->CreateReadWriteTransaction(ReadWriteOptions(), RetryState()));
  ZETASQL_EXPECT_OK(txn->Read(read_column("T", "k1"), &row_cursor));
  ZETASQL_EXPECT_OK(txn->Commit());
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
