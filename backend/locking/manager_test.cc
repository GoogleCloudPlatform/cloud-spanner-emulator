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

#include "backend/locking/manager.h"

#include <atomic>
#include <thread>  // NOLINT

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/time/clock.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

class LockManagerTest : public testing::Test {
 public:
  LockManagerTest()
      : request_(LockMode::kExclusive, "table", KeyRange::All(), {}) {}

  Clock* clock() { return &clock_; }
  LockManager* manager() { return &manager_; }
  const LockRequest& request() { return request_; }

 private:
  Clock clock_;
  LockManager manager_ = LockManager(&clock_);
  LockRequest request_;
};

TEST_F(LockManagerTest, SingleTransactionAcquiresLock) {
  std::unique_ptr<LockHandle> lh =
      manager()->CreateHandle(TransactionID(1), TransactionPriority(1));
  lh->EnqueueLock(request());
  EXPECT_FALSE(lh->IsBlocked());
  ZETASQL_EXPECT_OK(lh->Wait());
}

TEST_F(LockManagerTest, ConcurrentTransactionIsAborted) {
  std::unique_ptr<LockHandle> lh1 =
      manager()->CreateHandle(TransactionID(1), TransactionPriority(1));
  std::unique_ptr<LockHandle> lh2 =
      manager()->CreateHandle(TransactionID(2), TransactionPriority(1));

  // First transaction gets the lock.
  lh1->EnqueueLock(request());
  EXPECT_FALSE(lh1->IsBlocked());
  ZETASQL_EXPECT_OK(lh1->Wait());
  EXPECT_FALSE(lh1->IsBlocked());
  EXPECT_FALSE(lh1->IsAborted());

  // Second transaction does not get the lock.
  lh2->EnqueueLock(request());
  EXPECT_FALSE(lh2->IsBlocked());
  EXPECT_TRUE(lh2->IsAborted());
  EXPECT_THAT(lh2->Wait(),
              zetasql_base::testing::StatusIs(absl::StatusCode::kAborted));
  EXPECT_TRUE(lh2->IsAborted());
}

TEST_F(LockManagerTest, SequentialTransactionAcquiresLock) {
  std::unique_ptr<LockHandle> lh1 =
      manager()->CreateHandle(TransactionID(1), TransactionPriority(1));
  std::unique_ptr<LockHandle> lh2 =
      manager()->CreateHandle(TransactionID(2), TransactionPriority(1));
  std::unique_ptr<LockHandle> lh3 =
      manager()->CreateHandle(TransactionID(3), TransactionPriority(1));

  // First transaction gets the lock.
  lh1->EnqueueLock(request());
  ZETASQL_EXPECT_OK(lh1->Wait());

  // Second transaction does not get the lock yet.
  lh2->EnqueueLock(request());
  EXPECT_THAT(lh2->Wait(),
              zetasql_base::testing::StatusIs(absl::StatusCode::kAborted));

  // First transaction unlocks.
  lh1->UnlockAll();

  // Second transaction is still in a final aborted state.
  EXPECT_THAT(lh2->Wait(),
              zetasql_base::testing::StatusIs(absl::StatusCode::kAborted));

  // Now another transaction can get the lock.
  lh3->EnqueueLock(request());
  ZETASQL_EXPECT_OK(lh3->Wait());
}

TEST_F(LockManagerTest, TransactionsThatDidNotAcquireLockCanReleaseIt) {
  std::unique_ptr<LockHandle> lh1 =
      manager()->CreateHandle(TransactionID(1), TransactionPriority(1));
  lh1->UnlockAll();

  std::unique_ptr<LockHandle> lh2 =
      manager()->CreateHandle(TransactionID(1), TransactionPriority(1));
  lh2->EnqueueLock(request());
  EXPECT_FALSE(lh2->IsBlocked());
  ZETASQL_EXPECT_OK(lh2->Wait());
}

TEST_F(LockManagerTest, EnsuresSerializationWithParallelTransactions) {
  // Simulate a thread-safe mvcc store with a single key. Even though multiple
  // threads access this store, they are synchronized by the lock manager.
  // Concurrent access issues are expected to be caught by tsan.
  std::map<absl::Time, int> value{{absl::InfinitePast(), 0}};
  auto SetValue = [&value](absl::Time t, int i) { value[t] = i; };
  auto GetValue = [&value](absl::Time t) {
    auto itr = value.upper_bound(t);
    --itr;
    return itr->second;
  };

  // Start n threads each doing a transactional increment k times.
  int n = 20;
  int k = 10;
  std::vector<std::thread> threads;
  std::atomic<int> id_counter(0);
  for (int i = 0; i < n; ++i) {
    threads.emplace_back(
        [&](int i) {
          for (int j = 0; j < k; ++j) {
            while (true) {
              // Get a lock.
              std::unique_ptr<LockHandle> lh = manager()->CreateHandle(
                  TransactionID(++id_counter), TransactionPriority(1));
              lh->EnqueueLock(request());
              absl::Status status = lh->Wait();

              // Retry on aborts.
              if (status.code() == absl::StatusCode::kAborted) {
                continue;
              } else {
                ZETASQL_ASSERT_OK(status);
              }

              // We got the lock, increment the counter.
              int cur_value = GetValue(absl::InfiniteFuture());
              int new_value = cur_value + 1;
              SetValue(clock()->Now(), new_value);

              // Unlock the lock.
              lh->UnlockAll();
              break;
            }
          }
        },
        i);
  }

  // Wait for all threads to complete.
  for (std::thread& thread : threads) {
    thread.join();
  }

  // Expect that the counter was incremented n*k times.
  EXPECT_EQ(n * k, GetValue(absl::InfiniteFuture()));
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
