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
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "absl/time/time.h"
#include "backend/access/write.h"
#include "backend/actions/manager.h"
#include "backend/actions/ops.h"
#include "backend/common/ids.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/key_range.h"
#include "backend/datamodel/key_set.h"
#include "backend/datamodel/value.h"
#include "backend/schema/catalog/versioned_catalog.h"
#include "backend/storage/in_memory_storage.h"
#include "backend/transaction/actions.h"
#include "backend/transaction/options.h"
#include "common/clock.h"
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

class ReadWriteTransactionTest : public testing::Test {
 public:
  ReadWriteTransactionTest()
      : type_factory_(absl::make_unique<zetasql::TypeFactory>()),
        lock_manager_(absl::make_unique<LockManager>(&clock_)),
        storage_(absl::make_unique<InMemoryStorage>()),
        versioned_catalog_(absl::make_unique<VersionedCatalog>(
            std::move(test::CreateSchemaFromDDL(
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
                )"},
                          type_factory_.get())
                          .ValueOrDie()))),
        action_manager_(absl::make_unique<ActionManager>()) {
    action_manager_->AddActionsForSchema(
        versioned_catalog_->GetSchema(absl::InfiniteFuture()),
        /*function_catalog=*/nullptr);
  }

 protected:
  Clock clock_;

  // The type factory must outlive the type objects that it has made.
  std::unique_ptr<zetasql::TypeFactory> type_factory_;

  // Internal state of database exposed for the purpose of testing.
  std::unique_ptr<LockManager> lock_manager_;
  std::unique_ptr<InMemoryStorage> storage_;
  std::unique_ptr<VersionedCatalog> versioned_catalog_;
  std::unique_ptr<ActionManager> action_manager_;

  // Counter to generate TransactionID.
  std::atomic<int> id_counter_ = 0;

  std::unique_ptr<ReadWriteTransaction> CreateReadWriteTransaction() {
    return absl::make_unique<ReadWriteTransaction>(
        ReadWriteOptions(), RetryState(), ++id_counter_, &clock_,
        storage_.get(), lock_manager_.get(), versioned_catalog_.get(),
        action_manager_.get());
  }

  zetasql_base::StatusOr<std::vector<ValueList>> ReadAll(
      ReadWriteTransaction* txn, std::vector<std::string> columns) {
    return ReadAllUsingIndex(txn, /*index =*/"", columns);
  }

  zetasql_base::StatusOr<std::vector<ValueList>> ReadAllUsingIndex(
      ReadWriteTransaction* txn, std::string index,
      std::vector<std::string> columns) {
    return ReadUsingIndex(txn, KeySet(KeyRange::All()), index, columns);
  }

  zetasql_base::StatusOr<std::vector<ValueList>> ReadUsingIndex(
      ReadWriteTransaction* txn, KeySet key_set, std::string index,
      std::vector<std::string> columns) {
    backend::ReadArg read_arg{.table = "test_table",
                              .index = index,
                              .key_set = key_set,
                              .columns = columns};

    std::unique_ptr<backend::RowCursor> cursor;
    ZETASQL_RETURN_IF_ERROR(txn->Read(read_arg, &cursor));

    std::vector<ValueList> rows;
    while (cursor->Next()) {
      rows.emplace_back();
      for (int i = 0; i < cursor->NumColumns(); i++) {
        rows.back().push_back(cursor->ColumnValue(i));
      }
    }
    return rows;
  }

  auto IsOkAndHoldsRows(const std::vector<ValueList>& rows) {
    return zetasql_base::testing::IsOkAndHolds(testing::ElementsAreArray(rows));
  }
};

TEST_F(ReadWriteTransactionTest, CanReadAfterFlush) {
  // Buffer mutations.
  Mutation m;
  m.AddWriteOp(MutationOpType::kInsert, "test_table",
               {"int64_col", "string_col"}, {{Int64(1), String("value1")}});
  m.AddWriteOp(MutationOpType::kInsert, "test_table",
               {"int64_col", "string_col"}, {{Int64(2), String("value2")}});

  // Commit the transaction.
  auto txn1 = CreateReadWriteTransaction();
  ZETASQL_EXPECT_OK(txn1->Write(m));
  ZETASQL_EXPECT_OK(txn1->Commit());

  // Verify the values.
  auto txn2 = CreateReadWriteTransaction();
  EXPECT_THAT(ReadAll(txn2.get(), {"int64_col", "string_col"}),
              IsOkAndHoldsRows({{Int64(1), String("value1")},
                                {Int64(2), String("value2")}}));

  // Update some of the existing rows and insert some new rows.
  Mutation m2;
  m2.AddWriteOp(MutationOpType::kInsert, "test_table",
                {"int64_col", "string_col"}, {{Int64(3), String("value3")}});
  m2.AddWriteOp(MutationOpType::kUpdate, "test_table",
                {"int64_col", "string_col"},
                {{Int64(1), String("new-value1")}});
  m2.AddDeleteOp("test_table", KeySet(Key({Int64(2)})));
  ZETASQL_EXPECT_OK(txn2->Write(m2));
  ZETASQL_EXPECT_OK(txn2->Commit());

  // Verify that updates are flushed to underlying storage.
  auto txn3 = CreateReadWriteTransaction();
  EXPECT_THAT(ReadAll(txn3.get(), {"int64_col", "string_col"}),
              IsOkAndHoldsRows({{Int64(1), String("new-value1")},
                                {Int64(3), String("value3")}}));
}

TEST_F(ReadWriteTransactionTest, ReadEmptyDatabase) {
  auto txn1 = CreateReadWriteTransaction();
  EXPECT_THAT(ReadAll(txn1.get(), {"int64_col", "string_col"}),
              IsOkAndHoldsRows({}));
}

TEST_F(ReadWriteTransactionTest, ReadTableNotFound) {
  std::unique_ptr<backend::RowCursor> cursor;
  backend::ReadArg read_arg;
  read_arg.table = "non-existend-table";
  read_arg.columns = {"int64_col", "string_col"};
  read_arg.key_set = KeySet(Key({Int64(1)}));

  auto txn = CreateReadWriteTransaction();
  EXPECT_THAT(txn->Read(read_arg, &cursor),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(ReadWriteTransactionTest, ReadColumnNotFound) {
  std::unique_ptr<backend::RowCursor> cursor;
  backend::ReadArg read_arg;
  read_arg.table = "test_table";
  read_arg.columns = {"non-existent-column", "string_col"};
  read_arg.key_set = KeySet(Key({Int64(1)}));

  auto txn = CreateReadWriteTransaction();
  EXPECT_THAT(txn->Read(read_arg, &cursor),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(ReadWriteTransactionTest, Commit) {
  // Buffer mutations.
  Mutation m;
  m.AddWriteOp(MutationOpType::kInsert, "test_table",
               {"int64_col", "string_col"}, {{Int64(3), String("value")}});

  auto txn1 = CreateReadWriteTransaction();
  ZETASQL_EXPECT_OK(txn1->Write(m));

  // Commit the transaction.
  absl::Time before_commit_timestamp_ = clock_.Now();
  ZETASQL_EXPECT_OK(txn1->Commit());

  // Verify Commit Timestamp.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_timestamp_, txn1->GetCommitTimestamp());
  EXPECT_GT(commit_timestamp_, before_commit_timestamp_);
  EXPECT_EQ(txn1->state(), ReadWriteTransaction::State::kCommitted);

  // Start new transaction and verify the values.
  auto txn2 = CreateReadWriteTransaction();
  EXPECT_THAT(ReadAll(txn2.get(), {"int64_col", "string_col"}),
              IsOkAndHoldsRows({{Int64(3), String("value")}}));

  // Verify read using index.
  EXPECT_THAT(ReadAllUsingIndex(txn2.get(), "test_index", {"string_col"}),
              IsOkAndHoldsRows({{String("value")}}));
}

TEST_F(ReadWriteTransactionTest, CommitWithNoBufferedMutation) {
  absl::Time before_commit_timestamp_ = clock_.Now();

  // Commit the transaction.
  auto txn = CreateReadWriteTransaction();
  ZETASQL_EXPECT_OK(txn->Commit());

  // Verify Commit Timestamp.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_timestamp_, txn->GetCommitTimestamp());
  EXPECT_GT(commit_timestamp_, before_commit_timestamp_);
  EXPECT_EQ(txn->state(), ReadWriteTransaction::State::kCommitted);
}

TEST_F(ReadWriteTransactionTest, CommitWithMultipleChangesToDatabase) {
  // Buffer mutations.
  Mutation m;
  m.AddWriteOp(MutationOpType::kInsert, "test_table",
               {"int64_col", "string_col"}, {{Int64(3), String("value")}});
  m.AddWriteOp(MutationOpType::kInsert, "test_table",
               {"int64_col", "string_col"}, {{Int64(6), String("value-2")}});

  absl::Time before_commit_timestamp_ = clock_.Now();

  // Commit the transaction.
  auto txn1 = CreateReadWriteTransaction();
  ZETASQL_EXPECT_OK(txn1->Write(m));
  ZETASQL_EXPECT_OK(txn1->Commit());

  // Verify Commit Timestamp.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_timestamp_, txn1->GetCommitTimestamp());
  EXPECT_GT(commit_timestamp_, before_commit_timestamp_);
  EXPECT_EQ(txn1->state(), ReadWriteTransaction::State::kCommitted);

  // Verify the values.
  auto txn2 = CreateReadWriteTransaction();
  EXPECT_THAT(ReadAll(txn2.get(), {"int64_col", "string_col"}),
              IsOkAndHoldsRows({{Int64(3), String("value")},
                                {Int64(6), String("value-2")}}));

  // Verify that read using index results in descending order for string_col.
  EXPECT_THAT(ReadAllUsingIndex(txn2.get(), "test_index", {"string_col"}),
              IsOkAndHoldsRows({{String("value-2")}, {String("value")}}));
}

TEST_F(ReadWriteTransactionTest,
       ConcurrentReadWriteTransactionsReturnsAborted) {
  // Started "writes" on first transaction.
  Mutation m1;
  m1.AddWriteOp(MutationOpType::kInsert, "test_table",
                {"int64_col", "string_col"}, {{Int64(1), String("value-1")}});

  auto txn1 = CreateReadWriteTransaction();
  ZETASQL_EXPECT_OK(txn1->Write(m1));

  // Before commiting first transaction, starting another transaction is in
  // progress. Write for second transaction should consistently ABORT.
  auto txn2 = CreateReadWriteTransaction();
  Mutation m2;
  m2.AddWriteOp(MutationOpType::kInsert, "test_table",
                {"int64_col", "string_col"}, {{Int64(2), String("value-2")}});
  for (int i = 0; i < 5; i++) {
    EXPECT_THAT(txn2->Write(m2), StatusIs(absl::StatusCode::kAborted));
  }

  // Commit the first transaction.
  ZETASQL_EXPECT_OK(txn1->Commit());
  EXPECT_EQ(txn1->state(), ReadWriteTransaction::State::kCommitted);

  // Now, secondary transaction can write / commit.
  ZETASQL_EXPECT_OK(txn2->Write(m2));
  ZETASQL_EXPECT_OK(txn2->Commit());
  EXPECT_EQ(txn2->state(), ReadWriteTransaction::State::kCommitted);
}

TEST_F(ReadWriteTransactionTest, ConcurrentTransactionsEventuallySucceed) {
  // Start n threads each doing a transactional increment k times.
  int n = 20;
  int k = 10;

  // Seed value that will now be updated n*k times.
  Mutation seed_m;
  seed_m.AddWriteOp(MutationOpType::kInsert, "test_table",
                    {"int64_col", "int64_val_col"}, {{Int64(1), Int64(0)}});

  auto txn1 = CreateReadWriteTransaction();
  ZETASQL_EXPECT_OK(txn1->Write(seed_m));
  ZETASQL_EXPECT_OK(txn1->Commit());
  EXPECT_EQ(txn1->state(), ReadWriteTransaction::State::kCommitted);

  // Read args.
  backend::ReadArg read_arg;
  read_arg.table = "test_table";
  read_arg.columns = {"int64_val_col"};
  read_arg.key_set = KeySet(Key({Int64(1)}));

  std::vector<std::thread> threads;
  for (int i = 0; i < n; ++i) {
    threads.emplace_back([&]() {
      for (int j = 0; j < k; ++j) {
        // Start a  ReadWriteTransaction that will read and increment
        // this value.
        auto cur_txn = CreateReadWriteTransaction();
        while (true) {
          std::unique_ptr<backend::RowCursor> cursor;
          absl::Status status = cur_txn->Read(read_arg, &cursor);
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
          if (status.code() == absl::StatusCode::kAborted) {
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
  auto txn2 = CreateReadWriteTransaction();
  std::unique_ptr<backend::RowCursor> cursor;
  ZETASQL_EXPECT_OK(txn2->Read(read_arg, &cursor));
  int final_val;
  while (cursor->Next()) {
    final_val = cursor->ColumnValue(0).int64_value();
  }
  EXPECT_THAT(final_val, n * k);
}

TEST_F(ReadWriteTransactionTest, ConcurrentSchemaUpdatesWithTransactions) {
  // Start a ReadWrite transaction. This holds default schema at start of the
  // database creation.
  auto txn = CreateReadWriteTransaction();

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
  ZETASQL_ASSERT_OK(versioned_catalog_->AddSchema(clock_.Now(), std::move(schema)));
  action_manager_->AddActionsForSchema(versioned_catalog_->GetLatestSchema(),
                                       /*function_catalog=*/nullptr);

  // Transaction should return latest schema unless an operation is performed.
  ASSERT_NE(txn->schema()->FindTable("new_table"), nullptr);

  Mutation m;
  m.AddWriteOp(MutationOpType::kInsert, "new_table",
               {"int64_col", "string_col"}, {{Int64(1), String("value")}});
  ZETASQL_EXPECT_OK(txn->Write(m));

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
  ZETASQL_ASSERT_OK(versioned_catalog_->AddSchema(clock_.Now(), std::move(schema)));
  action_manager_->AddActionsForSchema(versioned_catalog_->GetLatestSchema(),
                                       /*function_catalog=*/nullptr);

  // Transaction is aborted.
  EXPECT_THAT(txn->Write(m), StatusIs(absl::StatusCode::kAborted));
}

TEST_F(ReadWriteTransactionTest, CommitWithNoEffectiveChangesToDatabase) {
  // Buffer mutations.
  Mutation m;
  m.AddWriteOp(MutationOpType::kInsert, "test_table",
               {"int64_col", "string_col"}, {{Int64(3), String("value")}});
  m.AddDeleteOp("test_table", KeySet(Key({Int64(3)})));

  absl::Time before_commit_timestamp_ = clock_.Now();

  // Commit the transaction.
  auto txn1 = CreateReadWriteTransaction();
  ZETASQL_EXPECT_OK(txn1->Write(m));
  ZETASQL_EXPECT_OK(txn1->Commit());

  // Verify Commit Timestamp.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto commit_timestamp_, txn1->GetCommitTimestamp());
  EXPECT_GT(commit_timestamp_, before_commit_timestamp_);
  EXPECT_EQ(txn1->state(), ReadWriteTransaction::State::kCommitted);

  // Verify the values.
  auto txn2 = CreateReadWriteTransaction();
  EXPECT_THAT(ReadAll(txn2.get(), {"int64_col", "string_col"}),
              IsOkAndHoldsRows({}));
}

TEST_F(ReadWriteTransactionTest, DuplicateCommitFails) {
  // Commit the transaction.
  auto txn = CreateReadWriteTransaction();
  ZETASQL_EXPECT_OK(txn->Commit());

  // Verify Commit State.
  EXPECT_EQ(txn->state(), ReadWriteTransaction::State::kCommitted);

  // Call commit on a committed transaction should fail.
  EXPECT_THAT(txn->Commit(), StatusIs(absl::StatusCode::kInternal));
}

TEST_F(ReadWriteTransactionTest, CommitAfterRollbackFails) {
  // Rollback transaction.
  auto txn = CreateReadWriteTransaction();
  ZETASQL_EXPECT_OK(txn->Rollback());
  EXPECT_EQ(txn->state(), ReadWriteTransaction::State::kRolledback);

  // Call commit on a rolled-back transaction should fail.
  EXPECT_THAT(txn->Commit(), StatusIs(absl::StatusCode::kInternal));
}

TEST_F(ReadWriteTransactionTest, GetCommitTimestampWithoutTransactionCommit) {
  auto txn = CreateReadWriteTransaction();
  EXPECT_THAT(txn->GetCommitTimestamp(), StatusIs(absl::StatusCode::kInternal));
}

TEST_F(ReadWriteTransactionTest, FailsReadWithInvalidIndex) {
  // Buffer mutations.
  Mutation m;
  m.AddWriteOp(MutationOpType::kInsert, "test_table",
               {"int64_col", "string_col"}, {{Int64(3), String("value")}});

  // Commit the transaction.
  auto txn1 = CreateReadWriteTransaction();
  ZETASQL_EXPECT_OK(txn1->Write(m));
  ZETASQL_EXPECT_OK(txn1->Commit());

  // Verify that reading from an invalid index fails.
  auto txn2 = CreateReadWriteTransaction();
  EXPECT_THAT(ReadAllUsingIndex(txn2.get(), "invalid_index",
                                {"int64_col", "string_col"}),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(ReadWriteTransactionTest, CanReadUsingIndex) {
  // Buffer mutations.
  Mutation m;
  m.AddWriteOp(MutationOpType::kInsert, "test_table",
               {"int64_col", "string_col"}, {{Int64(3), String("value")}});

  // Commit the transaction.
  auto txn1 = CreateReadWriteTransaction();
  ZETASQL_EXPECT_OK(txn1->Write(m));
  ZETASQL_EXPECT_OK(txn1->Commit());

  // Verify the values.
  auto txn2 = CreateReadWriteTransaction();
  EXPECT_THAT(ReadUsingIndex(txn2.get(), KeySet(Key({String("value")})),
                             "test_index", {"string_col", "int64_col"}),
              IsOkAndHoldsRows({{String("value"), Int64(3)}}));
}

TEST_F(ReadWriteTransactionTest, IndexInsertTest) {
  // Buffer mutations.
  Mutation m;
  m.AddWriteOp(MutationOpType::kInsert, "test_table",
               {"int64_col", "string_col"}, {{Int64(3), String("value")}});

  // Commit the transaction.
  auto txn1 = CreateReadWriteTransaction();
  ZETASQL_EXPECT_OK(txn1->Write(m));
  ZETASQL_EXPECT_OK(txn1->Commit());

  // Verify the values.
  auto txn2 = CreateReadWriteTransaction();
  EXPECT_THAT(
      ReadAllUsingIndex(txn2.get(), "test_index", {"string_col", "int64_col"}),
      IsOkAndHoldsRows({{String("value"), Int64(3)}}));

  EXPECT_THAT(ReadUsingIndex(txn2.get(), KeySet(Key({String("value")})),
                             "test_index", {"string_col"}),
              IsOkAndHoldsRows({{String("value")}}));

  // Searching for empty string should result in no elements.
  EXPECT_THAT(ReadUsingIndex(txn2.get(), KeySet(Key({String("")})),
                             "test_index", {"string_col"}),
              IsOkAndHoldsRows({}));
}

TEST_F(ReadWriteTransactionTest, IndexUpdateTest) {
  // Buffer mutations.
  Mutation m;
  m.AddWriteOp(MutationOpType::kInsert, "test_table",
               {"int64_col", "string_col"}, {{Int64(3), String("value")}});
  m.AddWriteOp(MutationOpType::kUpdate, "test_table",
               {"int64_col", "string_col"}, {{Int64(3), String("new-value")}});

  // Commit the transaction.
  auto txn1 = CreateReadWriteTransaction();
  ZETASQL_EXPECT_OK(txn1->Write(m));
  ZETASQL_EXPECT_OK(txn1->Commit());

  // Verify the index contains value "new-value" and not "value"
  auto txn2 = CreateReadWriteTransaction();
  EXPECT_THAT(ReadUsingIndex(txn2.get(), KeySet(Key({String("value")})),
                             "test_index", {"string_col"}),
              IsOkAndHoldsRows({}));

  EXPECT_THAT(ReadUsingIndex(txn2.get(), KeySet(Key({String("new-value")})),
                             "test_index", {"string_col"}),
              IsOkAndHoldsRows({{String("new-value")}}));
}

TEST_F(ReadWriteTransactionTest, IndexDeleteTest) {
  // Buffer mutations.
  Mutation m;
  m.AddWriteOp(MutationOpType::kInsert, "test_table",
               {"int64_col", "string_col"}, {{Int64(3), String("value")}});
  m.AddDeleteOp("test_table", KeySet(Key({Int64(3)})));

  // Commit the transaction.
  auto txn1 = CreateReadWriteTransaction();
  ZETASQL_EXPECT_OK(txn1->Write(m));
  ZETASQL_EXPECT_OK(txn1->Commit());

  // Verify the values.
  auto txn2 = CreateReadWriteTransaction();
  EXPECT_THAT(ReadUsingIndex(txn2.get(), KeySet(Key({String("value")})),
                             "test_index", {"string_col"}),
              IsOkAndHoldsRows({}));
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

  // Commit the transaction.
  auto txn1 = CreateReadWriteTransaction();
  ZETASQL_EXPECT_OK(txn1->Write(m));
  ZETASQL_EXPECT_OK(txn1->Commit());

  // Verify the values.
  auto txn2 = CreateReadWriteTransaction();
  EXPECT_THAT(ReadUsingIndex(txn2.get(), KeySet(Key({String("value")})),
                             "test_index", {"string_col"}),
              IsOkAndHoldsRows({}));
}

TEST_F(ReadWriteTransactionTest, IndexUniquenessFailTest) {
  // Buffer two mutations that should violate index uniqueness constraint.
  Mutation m;
  m.AddWriteOp(MutationOpType::kInsert, "test_table",
               {"int64_col", "string_col"}, {{Int64(3), String("value")}});
  m.AddWriteOp(MutationOpType::kInsert, "test_table",
               {"int64_col", "string_col"}, {{Int64(4), String("value")}});

  auto txn = CreateReadWriteTransaction();
  EXPECT_THAT(txn->Write(m), StatusIs(absl::StatusCode::kAlreadyExists));
}

TEST_F(ReadWriteTransactionTest, UpdateAfterDeleteFails) {
  Mutation m;
  m.AddDeleteOp("test_table", KeySet{Key{{Int64(4)}}});
  m.AddWriteOp(MutationOpType::kUpdate, "test_table",
               {"int64_col", "string_col"}, {{Int64(4), String("value")}});

  auto txn = CreateReadWriteTransaction();
  EXPECT_THAT(txn->Write(m), StatusIs(absl::StatusCode::kInvalidArgument));
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
