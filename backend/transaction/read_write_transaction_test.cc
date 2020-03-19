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

#include "backend/transaction/read_write_transaction.h"

#include <functional>
#include <thread>  // NOLINT
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "zetasql/base/status.h"
#include "zetasql/base/case.h"
#include "absl/time/time.h"
#include "backend/access/write.h"
#include "backend/actions/manager.h"
#include "backend/actions/ops.h"
#include "backend/common/ids.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/value.h"
#include "backend/schema/catalog/versioned_catalog.h"
#include "backend/storage/in_memory_storage.h"
#include "backend/transaction/actions.h"
#include "backend/transaction/options.h"
#include "backend/transaction/write_util.h"
#include "common/clock.h"
#include "tests/common/schema_constructor.h"
#include "zetasql/base/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

using zetasql::values::Int64;
using zetasql::values::String;

ValueList GetIndexValues(const Key& key,
                         std::unique_ptr<ReadWriteTransaction>* txn) {
  ValueList values;
  std::unique_ptr<backend::RowCursor> cursor;
  backend::ReadArg read_arg;
  // We specify the read using the user-visible table on which the index
  // is present and not by directly using the index data table.
  read_arg.table = "test_table";
  read_arg.columns = {"string_col"};
  read_arg.index = "test_index";
  read_arg.key_set = KeySet(key);

  ZETASQL_EXPECT_OK(txn->get()->Read(read_arg, &cursor));
  while (cursor->Next()) {
    values.push_back(cursor->ColumnValue(0));
  }
  return values;
}

class ReadWriteTransactionTest : public testing::Test {
 public:
  ReadWriteTransactionTest()
      : lock_manager_(absl::make_unique<LockManager>(&clock_)),
        type_factory_(absl::make_unique<zetasql::TypeFactory>()),
        schema_(test::CreateSchemaFromDDL(
                    {
                        R"(
              CREATE TABLE test_table (
                int64_col INT64 NOT NULL,
                string_col STRING(MAX),
                int64_val_col INT64
              ) PRIMARY KEY (int64_col)
            )",
                        R"(
              CREATE UNIQUE INDEX test_index ON test_table(string_col DESC)
            )",
                    },
                    type_factory_.get())
                    .ValueOrDie()),
        versioned_catalog_(VersionedCatalog(std::move(schema_))),
        action_manager_(absl::make_unique<ActionManager>()) {
    action_manager_->AddActionsForSchema(
        versioned_catalog_.GetSchema(absl::InfiniteFuture()));
    primary_transaction_ = absl::make_unique<ReadWriteTransaction>(
        opts_, txn_id_, &clock_, &storage_, lock_manager_.get(),
        &versioned_catalog_, action_manager_.get());
    secondary_transaction_ = absl::make_unique<ReadWriteTransaction>(
        opts_, txn_id_ + 1, &clock_, &storage_, lock_manager_.get(),
        &versioned_catalog_, action_manager_.get());
  }

 protected:
  Clock clock_;
  // Components.
  std::unique_ptr<LockManager> lock_manager_;
  // The type factory must outlive the type objects that it has made.
  std::unique_ptr<zetasql::TypeFactory> type_factory_;
  std::unique_ptr<const Schema> schema_;
  VersionedCatalog versioned_catalog_;
  InMemoryStorage storage_;
  std::unique_ptr<ActionManager> action_manager_;

  // Constants.
  TransactionID txn_id_ = 1;
  absl::Time t0_ = clock_.Now();

  // Test State.
  ReadWriteOptions opts_;
  absl::Time read_timestamp_;
  absl::Time commit_timestamp_;
  std::unique_ptr<ReadWriteTransaction> primary_transaction_;
  std::unique_ptr<ReadWriteTransaction> secondary_transaction_;
};

TEST_F(ReadWriteTransactionTest, Read) {
  // Buffer mutations.
  Mutation m;
  m.AddWriteOp(MutationOpType::kInsert, "test_table",
               {"int64_col", "string_col"}, {{Int64(1), String("value")}});
  ZETASQL_EXPECT_OK(primary_transaction_->Write(m));

  // Verify the values.
  std::unique_ptr<backend::RowCursor> cursor;
  backend::ReadArg read_arg;
  read_arg.table = "test_table";
  read_arg.columns = {"int64_col", "string_col"};
  read_arg.key_set = KeySet(Key({Int64(1)}));

  ZETASQL_EXPECT_OK(primary_transaction_->Read(read_arg, &cursor));
  std::vector<zetasql::Value> values;
  while (cursor->Next()) {
    values.push_back(cursor->ColumnValue(0));
    values.push_back(cursor->ColumnValue(1));
  }
  EXPECT_THAT(values, testing::ElementsAre(Int64(1), String("value")));

  // Commit the transaction.
  ZETASQL_EXPECT_OK(primary_transaction_->Commit());
}

TEST_F(ReadWriteTransactionTest, ReadEmptyDatabase) {
  std::unique_ptr<backend::RowCursor> cursor;
  backend::ReadArg read_arg;
  read_arg.table = "test_table";
  read_arg.columns = {"int64_col", "string_col"};
  read_arg.key_set = KeySet(Key({Int64(1)}));

  ZETASQL_EXPECT_OK(primary_transaction_->Read(read_arg, &cursor));
  std::vector<zetasql::Value> values;
  while (cursor->Next()) {
    values.push_back(cursor->ColumnValue(0));
    values.push_back(cursor->ColumnValue(1));
  }

  EXPECT_THAT(values, testing::ElementsAre());
}

TEST_F(ReadWriteTransactionTest, ReadTableNotFound) {
  std::unique_ptr<backend::RowCursor> cursor;
  backend::ReadArg read_arg;
  read_arg.table = "non-existend-table";
  read_arg.columns = {"int64_col", "string_col"};
  read_arg.key_set = KeySet(Key({Int64(1)}));

  EXPECT_THAT(primary_transaction_->Read(read_arg, &cursor),
              zetasql_base::testing::StatusIs(zetasql_base::StatusCode::kNotFound));
}

TEST_F(ReadWriteTransactionTest, ReadColumnNotFound) {
  std::unique_ptr<backend::RowCursor> cursor;
  backend::ReadArg read_arg;
  read_arg.table = "test_table";
  read_arg.columns = {"non-existent-column", "string_col"};
  read_arg.key_set = KeySet(Key({Int64(1)}));

  EXPECT_THAT(primary_transaction_->Read(read_arg, &cursor),
              zetasql_base::testing::StatusIs(zetasql_base::StatusCode::kNotFound));
}

TEST_F(ReadWriteTransactionTest, Commit) {
  // Buffer mutations.
  Mutation m;
  m.AddWriteOp(MutationOpType::kInsert, "test_table",
               {"int64_col", "string_col"}, {{Int64(3), String("value")}});
  ZETASQL_EXPECT_OK(primary_transaction_->Write(m));

  // Commit the transaction.
  absl::Time before_commit_timestamp_ = clock_.Now();
  ZETASQL_EXPECT_OK(primary_transaction_->Commit());
  // Verify Commit Timestamp.
  ZETASQL_ASSERT_OK_AND_ASSIGN(commit_timestamp_,
                       primary_transaction_->GetCommitTimestamp());
  EXPECT_GT(commit_timestamp_, before_commit_timestamp_);
  EXPECT_EQ(primary_transaction_->state(),
            ReadWriteTransaction::State::kCommitted);

  // Start new transaction and verify the values.
  std::unique_ptr<backend::RowCursor> cursor;
  backend::ReadArg read_arg;
  read_arg.table = "test_table";
  read_arg.columns = {"int64_col", "string_col"};
  read_arg.key_set = KeySet(Key({Int64(3)}));

  ZETASQL_EXPECT_OK(secondary_transaction_->Read(read_arg, &cursor));
  std::vector<zetasql::Value> values;
  while (cursor->Next()) {
    values.push_back(cursor->ColumnValue(0));
    values.push_back(cursor->ColumnValue(1));
  }

  EXPECT_THAT(values, testing::ElementsAre(Int64(3), String("value")));

  // Verify index values.
  std::vector<zetasql::Value> index_values;
  Key index_key = Key();
  index_values = GetIndexValues(index_key, &secondary_transaction_);
  EXPECT_THAT(index_values, testing::ElementsAre(String("value")));
}

TEST_F(ReadWriteTransactionTest, CommitWithNoBufferedMutation) {
  // Commit the transaction.
  absl::Time before_commit_timestamp_ = clock_.Now();
  ZETASQL_EXPECT_OK(primary_transaction_->Commit());

  // Verify Commit Timestamp.
  ZETASQL_ASSERT_OK_AND_ASSIGN(commit_timestamp_,
                       primary_transaction_->GetCommitTimestamp());
  EXPECT_GT(commit_timestamp_, before_commit_timestamp_);
  EXPECT_EQ(primary_transaction_->state(),
            ReadWriteTransaction::State::kCommitted);
}

TEST_F(ReadWriteTransactionTest, CommitWithMultipleChangesToDatabase) {
  // Buffer mutations.
  Mutation m;
  m.AddWriteOp(MutationOpType::kInsert, "test_table",
               {"int64_col", "string_col"}, {{Int64(3), String("value")}});
  m.AddWriteOp(MutationOpType::kInsert, "test_table",
               {"int64_col", "string_col"}, {{Int64(6), String("value-2")}});
  ZETASQL_EXPECT_OK(primary_transaction_->Write(m));

  // Commit the transaction.
  absl::Time before_commit_timestamp_ = clock_.Now();
  ZETASQL_EXPECT_OK(primary_transaction_->Commit());
  // Verify Commit Timestamp.
  ZETASQL_ASSERT_OK_AND_ASSIGN(commit_timestamp_,
                       primary_transaction_->GetCommitTimestamp());
  EXPECT_GT(commit_timestamp_, before_commit_timestamp_);
  EXPECT_EQ(primary_transaction_->state(),
            ReadWriteTransaction::State::kCommitted);

  // Verify the values.
  std::unique_ptr<backend::RowCursor> cursor;
  backend::ReadArg read_arg;
  read_arg.table = "test_table";
  read_arg.columns = {"int64_col", "string_col"};
  read_arg.key_set = KeySet(Key({Int64(3)}));
  read_arg.key_set.AddKey(Key({Int64(6)}));

  ZETASQL_EXPECT_OK(secondary_transaction_->Read(read_arg, &cursor));
  std::vector<zetasql::Value> values;
  while (cursor->Next()) {
    values.push_back(cursor->ColumnValue(0));
    values.push_back(cursor->ColumnValue(1));
  }

  EXPECT_THAT(values, testing::ElementsAre(Int64(3), String("value"), Int64(6),
                                           String("value-2")));

  // Verify index values.
  std::vector<zetasql::Value> index_values;
  Key index_key = Key();
  index_values = GetIndexValues(index_key, &secondary_transaction_);
  EXPECT_THAT(index_values,
              testing::ElementsAre(String("value-2"), String("value")));
}

TEST_F(ReadWriteTransactionTest,
       ConcurrentReadWriteTransactionsReturnsAborted) {
  // Started "writes" on first transaction.
  Mutation m1;
  m1.AddWriteOp(MutationOpType::kInsert, "test_table",
                {"int64_col", "string_col"}, {{Int64(1), String("value-1")}});
  ZETASQL_EXPECT_OK(primary_transaction_->Write(m1));

  // Before commiting first transaction, starting another transaction is in
  // progress. Write for second transaction should consistently ABORT.
  Mutation m2;
  m2.AddWriteOp(MutationOpType::kInsert, "test_table",
                {"int64_col", "string_col"}, {{Int64(2), String("value-2")}});
  for (int i = 0; i < 5; i++) {
    EXPECT_THAT(secondary_transaction_->Write(m2),
                zetasql_base::testing::StatusIs(zetasql_base::StatusCode::kAborted));
  }

  // Commit the first transaction.
  ZETASQL_EXPECT_OK(primary_transaction_->Commit());
  EXPECT_EQ(primary_transaction_->state(),
            ReadWriteTransaction::State::kCommitted);

  // Now, secondary transaction can write / commit.
  ZETASQL_EXPECT_OK(secondary_transaction_->Write(m2));
  ZETASQL_EXPECT_OK(secondary_transaction_->Commit());
  EXPECT_EQ(secondary_transaction_->state(),
            ReadWriteTransaction::State::kCommitted);
}

TEST_F(ReadWriteTransactionTest, ConcurrentTransactionsEventuallySucceed) {
  // Start n threads each doing a transactional increment k times.
  int n = 20;
  int k = 10;

  // Seed value that will now be updated n*k times.
  Mutation seed_m;
  seed_m.AddWriteOp(MutationOpType::kInsert, "test_table",
                    {"int64_col", "int64_val_col"}, {{Int64(1), Int64(0)}});
  ZETASQL_EXPECT_OK(primary_transaction_->Write(seed_m));
  ZETASQL_EXPECT_OK(primary_transaction_->Commit());
  EXPECT_EQ(primary_transaction_->state(),
            ReadWriteTransaction::State::kCommitted);

  // Read args.
  backend::ReadArg read_arg;
  read_arg.table = "test_table";
  read_arg.columns = {"int64_val_col"};
  read_arg.key_set = KeySet(Key({Int64(1)}));

  std::vector<std::thread> threads;
  std::atomic<int> id_counter(0);
  for (int i = 0; i < n; ++i) {
    threads.emplace_back([&]() {
      for (int j = 0; j < k; ++j) {
        // Start a  ReadWriteTransaction that will read and increment
        // this value.
        auto cur_txn = absl::make_unique<ReadWriteTransaction>(
            opts_, TransactionID(++id_counter), &clock_, &storage_,
            lock_manager_.get(), &versioned_catalog_, action_manager_.get());
        while (true) {
          std::unique_ptr<backend::RowCursor> cursor;
          zetasql_base::Status status = cur_txn->Read(read_arg, &cursor);
          if (status.ok()) {
            // Increment and save this value to the database.
            int cur_val = 0;
            while (cursor->Next()) {
              cur_val = cursor->ColumnValue(0).int64_value();
            }
            ZETASQL_ASSERT_OK(cursor->Status());
            Mutation m;
            m.AddWriteOp(MutationOpType::kUpdate, "test_table",
                         {"int64_col", "int64_val_col"},
                         {{Int64(1), Int64(cur_val + 1)}});

            status.Update(cur_txn->Write(m));
            if (status.ok()) {
              status.Update(cur_txn->Commit());
              ZETASQL_ASSERT_OK(status);

              EXPECT_EQ(cur_txn->state(),
                        ReadWriteTransaction::State::kCommitted);

              break;
            }
          }

          // Retry on abort.
          if (status.code() == zetasql_base::StatusCode::kAborted) {
            continue;
          }
        }
      }
    });
  }

  // Wait for all threads to complete.
  for (std::thread& thread : threads) {
    thread.join();
  }

  // Verify the value.
  std::unique_ptr<backend::RowCursor> cursor;
  ZETASQL_EXPECT_OK(secondary_transaction_->Read(read_arg, &cursor));
  int final_val;
  while (cursor->Next()) {
    final_val = cursor->ColumnValue(0).int64_value();
  }
  EXPECT_THAT(final_val, n * k);
}

TEST_F(ReadWriteTransactionTest, ConcurrentSchemaUpdatesWithTransactions) {
  // Start a ReadWrite transaction. This holds default schema at start of the
  // database creation.
  auto cur_txn = absl::make_unique<ReadWriteTransaction>(
      opts_, txn_id_, &clock_, &storage_, lock_manager_.get(),
      &versioned_catalog_, action_manager_.get());

  // Update the schema with "new_table".
  auto schema = test::CreateSchemaFromDDL(
                    {
                        R"(
              CREATE TABLE new_table (
                int64_col INT64 NOT NULL,
                string_col STRING(MAX)
              ) PRIMARY KEY (int64_col)
            )",
                    },
                    type_factory_.get())
                    .ValueOrDie();
  ZETASQL_ASSERT_OK(versioned_catalog_.AddSchema(clock_.Now(), std::move(schema)));
  action_manager_->AddActionsForSchema(versioned_catalog_.GetLatestSchema());

  // Transaction should return latest schema unless an operation is performed.
  ASSERT_NE(cur_txn->schema()->FindTable("new_table"), nullptr);

  Mutation m;
  m.AddWriteOp(MutationOpType::kInsert, "new_table",
               {"int64_col", "string_col"}, {{Int64(1), String("value")}});
  ZETASQL_EXPECT_OK(cur_txn->Write(m));

  // Schema change to verify the transaction is aborted in subsequent requests.
  schema = test::CreateSchemaFromDDL(
               {
                   R"(
              CREATE TABLE another_new_table (
                int64_col INT64 NOT NULL,
                string_col STRING(MAX)
              ) PRIMARY KEY (int64_col)
            )",
               },
               type_factory_.get())
               .ValueOrDie();
  ZETASQL_ASSERT_OK(versioned_catalog_.AddSchema(clock_.Now(), std::move(schema)));
  action_manager_->AddActionsForSchema(versioned_catalog_.GetLatestSchema());

  // Transaction is aborted.
  EXPECT_THAT(cur_txn->Write(m),
              zetasql_base::testing::StatusIs(zetasql_base::StatusCode::kAborted));
}

TEST_F(ReadWriteTransactionTest, CommitWithNoEffectiveChangesToDatabase) {
  // Buffer mutations.
  Mutation m;
  m.AddWriteOp(MutationOpType::kInsert, "test_table",
               {"int64_col", "string_col"}, {{Int64(3), String("value")}});
  m.AddDeleteOp("test_table", KeySet(Key({Int64(3)})));
  ZETASQL_EXPECT_OK(primary_transaction_->Write(m));

  // Commit the transaction.
  absl::Time before_commit_timestamp_ = clock_.Now();
  ZETASQL_EXPECT_OK(primary_transaction_->Commit());
  // Verify Commit Timestamp.
  ZETASQL_ASSERT_OK_AND_ASSIGN(commit_timestamp_,
                       primary_transaction_->GetCommitTimestamp());
  EXPECT_GT(commit_timestamp_, before_commit_timestamp_);
  EXPECT_EQ(primary_transaction_->state(),
            ReadWriteTransaction::State::kCommitted);

  // Verify the values.
  std::unique_ptr<backend::RowCursor> cursor;
  backend::ReadArg read_arg;
  read_arg.table = "test_table";
  read_arg.columns = {"int64_col", "string_col"};
  read_arg.key_set = KeySet(Key({Int64(3)}));

  ZETASQL_EXPECT_OK(secondary_transaction_->Read(read_arg, &cursor));
  std::vector<zetasql::Value> values;
  while (cursor->Next()) {
    values.push_back(cursor->ColumnValue(0));
    values.push_back(cursor->ColumnValue(1));
  }
  EXPECT_THAT(values, testing::ElementsAre());
}

TEST_F(ReadWriteTransactionTest, DuplicateCommitFails) {
  // Commit the transaction.
  ZETASQL_EXPECT_OK(primary_transaction_->Commit());

  // Verify Commit State.
  EXPECT_EQ(primary_transaction_->state(),
            ReadWriteTransaction::State::kCommitted);

  // Call commit on a committed transaction should fail.
  EXPECT_THAT(primary_transaction_->Commit(),
              zetasql_base::testing::StatusIs(zetasql_base::StatusCode::kInternal));
}

TEST_F(ReadWriteTransactionTest, CommitAfterRollbackFails) {
  // Rollback transaction.
  ZETASQL_EXPECT_OK(primary_transaction_->Rollback());
  EXPECT_EQ(primary_transaction_->state(),
            ReadWriteTransaction::State::kRolledback);

  // Call commit on a rolled-back transaction should fail.
  EXPECT_THAT(primary_transaction_->Commit(),
              zetasql_base::testing::StatusIs(zetasql_base::StatusCode::kInternal));
}

TEST_F(ReadWriteTransactionTest, GetCommitTimestampWithoutTransactionCommit) {
  EXPECT_THAT(primary_transaction_->GetCommitTimestamp(),
              zetasql_base::testing::StatusIs(zetasql_base::StatusCode::kInternal));
}

TEST_F(ReadWriteTransactionTest, FailsReadWithInvalidIndex) {
  // Buffer mutations.
  Mutation m;
  m.AddWriteOp(MutationOpType::kInsert, "test_table",
               {"int64_col", "string_col"}, {{Int64(3), String("value")}});
  ZETASQL_EXPECT_OK(primary_transaction_->Write(m));

  // Commit the transaction.
  ZETASQL_EXPECT_OK(primary_transaction_->Commit());

  // Verify that reading from an invalid index fails.
  std::unique_ptr<backend::RowCursor> cursor;
  backend::ReadArg read_arg;
  read_arg.table = "test_table";
  read_arg.index = "invalid_index";
  read_arg.columns = {"string_col", "int64_col"};
  read_arg.key_set = KeySet(Key({String("value")}));

  EXPECT_THAT(secondary_transaction_->Read(read_arg, &cursor),
              zetasql_base::testing::StatusIs(zetasql_base::StatusCode::kNotFound));
}

TEST_F(ReadWriteTransactionTest, CanReadUsingIndex) {
  // Buffer mutations.
  Mutation m;
  m.AddWriteOp(MutationOpType::kInsert, "test_table",
               {"int64_col", "string_col"}, {{Int64(3), String("value")}});
  ZETASQL_EXPECT_OK(primary_transaction_->Write(m));

  // Commit the transaction.
  ZETASQL_EXPECT_OK(primary_transaction_->Commit());

  // Verify the values.
  std::unique_ptr<backend::RowCursor> cursor;
  backend::ReadArg read_arg;
  read_arg.table = "test_table";
  read_arg.index = "test_index";
  read_arg.columns = {"string_col", "int64_col"};
  read_arg.key_set = KeySet(Key({String("value")}));

  ZETASQL_EXPECT_OK(secondary_transaction_->Read(read_arg, &cursor));
  std::vector<zetasql::Value> values;
  while (cursor->Next()) {
    values.push_back(cursor->ColumnValue(0));
    values.push_back(cursor->ColumnValue(1));
  }
  EXPECT_THAT(values, testing::ElementsAre(String("value"), Int64(3)));
}

TEST_F(ReadWriteTransactionTest, IndexInsertTest) {
  // Buffer mutations.
  Mutation m;
  m.AddWriteOp(MutationOpType::kInsert, "test_table",
               {"int64_col", "string_col"}, {{Int64(3), String("value")}});
  ZETASQL_EXPECT_OK(primary_transaction_->Write(m));

  // Commit the transaction.
  ZETASQL_EXPECT_OK(primary_transaction_->Commit());

  // Verify the values.
  std::vector<zetasql::Value> values;
  Key index_key = Key({String("value")});
  values = GetIndexValues(index_key, &secondary_transaction_);
  EXPECT_THAT(values, testing::ElementsAre(String("value")));

  // Searching for empty string should result in no elements.
  values.clear();
  index_key = Key({String("")});
  values = GetIndexValues(index_key, &secondary_transaction_);
  EXPECT_THAT(values, testing::ElementsAre());
}

TEST_F(ReadWriteTransactionTest, IndexUpdateTest) {
  // Buffer mutations.
  Mutation m;
  m.AddWriteOp(MutationOpType::kInsert, "test_table",
               {"int64_col", "string_col"}, {{Int64(3), String("value")}});
  m.AddWriteOp(MutationOpType::kUpdate, "test_table",
               {"int64_col", "string_col"}, {{Int64(3), String("new-value")}});
  ZETASQL_EXPECT_OK(primary_transaction_->Write(m));

  // Commit the transaction.
  ZETASQL_EXPECT_OK(primary_transaction_->Commit());

  // Verify the values.
  std::vector<zetasql::Value> values;
  Key index_key = Key({String("value")});
  values = GetIndexValues(index_key, &secondary_transaction_);
  EXPECT_THAT(values, testing::ElementsAre());

  // Searching for empty string should result in no elements.
  values.clear();
  index_key = Key({String("new-value")});
  values = GetIndexValues(index_key, &secondary_transaction_);
  EXPECT_THAT(values, testing::ElementsAre(String("new-value")));
}

TEST_F(ReadWriteTransactionTest, IndexDeleteTest) {
  // Buffer mutations.
  Mutation m;
  m.AddWriteOp(MutationOpType::kInsert, "test_table",
               {"int64_col", "string_col"}, {{Int64(3), String("value")}});
  m.AddDeleteOp("test_table", KeySet(Key({Int64(3)})));
  ZETASQL_EXPECT_OK(primary_transaction_->Write(m));

  // Commit the transaction.
  ZETASQL_EXPECT_OK(primary_transaction_->Commit());

  // Verify the values.
  std::vector<zetasql::Value> values;
  Key index_key = Key({String("value")});
  values = GetIndexValues(index_key, &secondary_transaction_);
  EXPECT_THAT(values, testing::ElementsAre());
}

TEST_F(ReadWriteTransactionTest, IndexDeleteAreIdempotentTest) {
  // Buffer mutations.
  Mutation m;
  m.AddWriteOp(MutationOpType::kInsert, "test_table",
               {"int64_col", "string_col"}, {{Int64(3), String("value")}});
  m.AddDeleteOp("test_table", KeySet(Key({Int64(3)})));

  // Replace mutation op is translated to a Delete WriteOp which in turn
  // triggers a index delete op.
  m.AddWriteOp(MutationOpType::kReplace, "test_table",
               {"int64_col", "string_col"}, {{Int64(3), String("value2")}});
  ZETASQL_EXPECT_OK(primary_transaction_->Write(m));

  // Commit the transaction.
  ZETASQL_EXPECT_OK(primary_transaction_->Commit());

  // Verify the values.
  std::vector<zetasql::Value> values;
  Key index_key = Key({String("value")});
  values = GetIndexValues(index_key, &secondary_transaction_);
  EXPECT_THAT(values, testing::ElementsAre());
}

TEST_F(ReadWriteTransactionTest, IndexUniquenessFailTest) {
  // Buffer two mutations that should violate index uniqueness constraint.
  Mutation m;
  m.AddWriteOp(MutationOpType::kInsert, "test_table",
               {"int64_col", "string_col"}, {{Int64(3), String("value")}});
  m.AddWriteOp(MutationOpType::kInsert, "test_table",
               {"int64_col", "string_col"}, {{Int64(4), String("value")}});
  EXPECT_THAT(primary_transaction_->Write(m),
              zetasql_base::testing::StatusIs(zetasql_base::StatusCode::kAlreadyExists));
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
