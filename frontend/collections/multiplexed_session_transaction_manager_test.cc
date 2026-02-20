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

#include "frontend/collections/multiplexed_session_transaction_manager.h"

#include <memory>
#include <utility>

#include "zetasql/public/types/type_factory.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "backend/actions/manager.h"
#include "backend/locking/manager.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/versioned_catalog.h"
#include "backend/storage/in_memory_storage.h"
#include "backend/transaction/options.h"
#include "backend/transaction/read_write_transaction.h"
#include "common/clock.h"
#include "frontend/entities/transaction.h"
#include "tests/common/schema_constructor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {
namespace {

using ::zetasql_base::testing::StatusIs;

constexpr char kDatabaseUri[] =
    "projects/test-project/instances/test-instance/databases/test-database";
constexpr char kDatabaseUri2[] =
    "projects/test-project/instances/test-instance/databases/test-database-2";

namespace spanner_api = ::google::spanner::v1;

class MultiplexedSessionTransactionManagerTest : public testing::Test {
 protected:
  void SetUp() override {
    type_factory_ = std::make_unique<zetasql::TypeFactory>();
    lock_manager_ = std::make_unique<backend::LockManager>(&clock_);
    storage_ = std::make_unique<backend::InMemoryStorage>();
    versioned_catalog_ = std::make_unique<backend::VersionedCatalog>(
        std::move(GetSchema()).value());
    action_manager_ = std::make_unique<backend::ActionManager>();
    action_manager_->AddActionsForSchema(
        versioned_catalog_->GetSchema(absl::InfiniteFuture()),
        /*function_catalog=*/nullptr, type_factory_.get());
  }

  virtual absl::StatusOr<std::unique_ptr<const backend::Schema>> GetSchema() {
    return test::CreateSchemaFromDDL(
        {
            R"sql(
                  CREATE TABLE test_table (
                    int64_col INT64 NOT NULL,
                    string_col STRING(MAX),
                    int64_val_col INT64
                  ) PRIMARY KEY (int64_col)
                )sql",
            R"sql(
                  CREATE UNIQUE INDEX test_index ON test_table(string_col DESC)
                )sql"},
        type_factory_.get());
  }

  Clock clock_;

  // The type factory must outlive the type objects that it has made.
  std::unique_ptr<zetasql::TypeFactory> type_factory_;

  // Internal state of database exposed for the purpose of testing.
  std::unique_ptr<backend::LockManager> lock_manager_;
  std::unique_ptr<backend::InMemoryStorage> storage_;
  std::unique_ptr<backend::VersionedCatalog> versioned_catalog_;
  std::unique_ptr<backend::ActionManager> action_manager_;

  std::unique_ptr<backend::ReadWriteTransaction> CreateReadWriteTransaction(
      int id) {
    return std::make_unique<backend::ReadWriteTransaction>(
        backend::ReadWriteOptions(), backend::RetryState(), id, &clock_,
        storage_.get(), lock_manager_.get(), versioned_catalog_.get(),
        action_manager_.get());
  }
};

TEST_F(MultiplexedSessionTransactionManagerTest, ValidateTransactionAdded) {
  MultiplexedSessionTransactionManager mux_txn_manager;

  std::unique_ptr<backend::ReadWriteTransaction> backend_txn =
      CreateReadWriteTransaction(1);
  spanner_api::TransactionOptions options;
  options.mutable_read_write();
  std::shared_ptr<Transaction> txn_to_add = std::make_shared<Transaction>(
      std::move(backend_txn), nullptr, options, Transaction::Usage::kMultiUse);

  ZETASQL_ASSERT_OK(
      mux_txn_manager.AddToCurrentTransactions(txn_to_add, kDatabaseUri, 1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<Transaction> txn_from_manager,
      mux_txn_manager.GetCurrentTransactionOnMultiplexedSession(kDatabaseUri,
                                                                1));
  ASSERT_EQ(txn_from_manager->id(), 1);
  // Check that the transaction is not cleared here since its neither closed nor
  // has become stale
  mux_txn_manager.ClearOldTransactions();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<Transaction> txn_from_manager_2,
      mux_txn_manager.GetCurrentTransactionOnMultiplexedSession(kDatabaseUri,
                                                                1));
}

TEST_F(MultiplexedSessionTransactionManagerTest, ClearStaleTransactions) {
  // Create a mux transaction manager that runs a check if its been more than 1
  // second since the last check.
  // Also set the old transaction staleness duration to 3 seconds.
  MultiplexedSessionTransactionManager mux_txn_manager(
      /*old_transaction_staleness_duration=*/absl::Seconds(3),
      /*staleness_check_duration=*/absl::Seconds(1));
  // Add a transaction to the manager.
  std::unique_ptr<backend::ReadWriteTransaction> backend_txn =
      CreateReadWriteTransaction(1);
  spanner_api::TransactionOptions options;
  options.mutable_read_write();
  std::shared_ptr<Transaction> txn_to_add = std::make_shared<Transaction>(
      std::move(backend_txn), nullptr, spanner_api::TransactionOptions(),
      Transaction::Usage::kMultiUse);
  ZETASQL_ASSERT_OK(
      mux_txn_manager.AddToCurrentTransactions(txn_to_add, kDatabaseUri, 1));
  // Sleep for 4 seconds to make this transaction stale.
  absl::SleepFor(absl::Seconds(4));
  // Run the clear the old transactions method.
  mux_txn_manager.ClearOldTransactions();
  absl::StatusOr<std::shared_ptr<Transaction>> txn_from_manager =
      mux_txn_manager.GetCurrentTransactionOnMultiplexedSession(kDatabaseUri,
                                                                1);
  ASSERT_THAT(txn_from_manager, StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(MultiplexedSessionTransactionManagerTest, ClearClosedTransactions) {
  // A default mux transaction manager is fine in this test since we explicitly
  // close the transaction and validate if it gets cleared out.
  MultiplexedSessionTransactionManager mux_txn_manager;
  // Add a transaction to the manager.
  std::unique_ptr<backend::ReadWriteTransaction> backend_txn =
      CreateReadWriteTransaction(1);
  spanner_api::TransactionOptions options;
  options.mutable_read_write();
  std::shared_ptr<Transaction> txn_to_add = std::make_shared<Transaction>(
      std::move(backend_txn), nullptr, spanner_api::TransactionOptions(),
      Transaction::Usage::kMultiUse);
  ZETASQL_ASSERT_OK(
      mux_txn_manager.AddToCurrentTransactions(txn_to_add, kDatabaseUri, 1));
  // Close the transaction.
  txn_to_add->Close();
  // Run the clear the old transactions method and it should clear it out.
  mux_txn_manager.ClearOldTransactions();
  absl::StatusOr<std::shared_ptr<Transaction>> txn_from_manager =
      mux_txn_manager.GetCurrentTransactionOnMultiplexedSession(kDatabaseUri,
                                                                1);
  ASSERT_THAT(txn_from_manager, StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(MultiplexedSessionTransactionManagerTest, TransactionCollision) {
  MultiplexedSessionTransactionManager mux_txn_manager;

  // Create two transactions with the same ID but for different databases.
  std::unique_ptr<backend::ReadWriteTransaction> backend_txn1 =
      CreateReadWriteTransaction(1);
  std::unique_ptr<backend::ReadWriteTransaction> backend_txn2 =
      CreateReadWriteTransaction(1);

  spanner_api::TransactionOptions options;
  options.mutable_read_write();

  std::shared_ptr<Transaction> txn1 = std::make_shared<Transaction>(
      std::move(backend_txn1), nullptr, options, Transaction::Usage::kMultiUse);
  std::shared_ptr<Transaction> txn2 = std::make_shared<Transaction>(
      std::move(backend_txn2), nullptr, options, Transaction::Usage::kMultiUse);

  // Add both transactions to the manager.
  ZETASQL_ASSERT_OK(mux_txn_manager.AddToCurrentTransactions(txn1, kDatabaseUri, 1));
  ZETASQL_ASSERT_OK(mux_txn_manager.AddToCurrentTransactions(txn2, kDatabaseUri2, 1));

  // Verify we can retrieve them correctly.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<Transaction> txn_from_manager1,
      mux_txn_manager.GetCurrentTransactionOnMultiplexedSession(kDatabaseUri,
                                                                1));
  ASSERT_EQ(txn_from_manager1->id(), 1);
  ASSERT_EQ(txn_from_manager1, txn1);

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<Transaction> txn_from_manager2,
      mux_txn_manager.GetCurrentTransactionOnMultiplexedSession(kDatabaseUri2,
                                                                1));
  ASSERT_EQ(txn_from_manager2->id(), 1);
  ASSERT_EQ(txn_from_manager2, txn2);

  // Verify that they are distinct.
  ASSERT_NE(txn_from_manager1, txn_from_manager2);
}

}  // namespace

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
