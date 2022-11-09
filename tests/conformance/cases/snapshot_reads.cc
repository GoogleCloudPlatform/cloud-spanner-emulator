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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/time/time.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using zetasql_base::testing::StatusIs;

class SnapshotReadsTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    return SetSchema({R"(
      CREATE TABLE Users(
        ID   INT64 NOT NULL,
        Name STRING(MAX),
        Age  INT64
      ) PRIMARY KEY (ID)
    )"});
  }
};

TEST_F(SnapshotReadsTest, CanReadWithMinTimestampBound) {
  // Insert a few rows.
  ZETASQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 23}));
  ZETASQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

  // Read using a min_timestamp bounded staleness.
  auto result = Read(Transaction::SingleUseOptions(
                         MakePastTimestamp(std::chrono::minutes(10))),
                     "Users", {"ID", "Name", "Age"}, KeySet::All());
  EXPECT_THAT(result, zetasql_base::testing::IsOk());
  // Bounded staleness reads can return an empty set or a subset of the rows in
  // their committed order. With a bounded staleness of 10 mins, the reads can
  // return an empty set. When a non-empty set is returned, we want to ensure
  // that the commit order is still respected, and the first row always exists
  // in the results.
  if (!result.value().empty()) {
    EXPECT_THAT(result.value(),
                testing::IsSupersetOf({ValueRow{1, "John", 23}}));
  }
  // Ensures that the bounded staleness does not return any other data, and only
  // returns these two rows.
  EXPECT_THAT(result.value(), testing::IsSubsetOf({ValueRow{1, "John", 23},
                                                   ValueRow{2, "Peter", 41}}));
}

TEST_F(SnapshotReadsTest, CanReadWithMaxStalenessBound) {
  // Insert a few rows.
  ZETASQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 23}));
  ZETASQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

  // Read using a max staleness bound.
  auto result = Read(Transaction::SingleUseOptions(std::chrono::minutes(10)),
                     "Users", {"ID", "Name", "Age"}, KeySet::All());
  EXPECT_THAT(result, zetasql_base::testing::IsOk());
  // Bounded staleness reads can return an empty set or a subset of the rows in
  // their committed order. With a bounded staleness of 10 mins, the reads can
  // return an empty set. When a non-empty set is returned, we want to ensure
  // that the commit order is still respected, and the first row always exists
  // in the results.
  if (!result.value().empty()) {
    EXPECT_THAT(result.value(),
                testing::IsSupersetOf({ValueRow{1, "John", 23}}));
  }
  // Ensures that the bounded staleness does not return any other data, and only
  // returns these two rows.
  EXPECT_THAT(result.value(), testing::IsSubsetOf({ValueRow{1, "John", 23},
                                                   ValueRow{2, "Peter", 41}}));
}

TEST_F(SnapshotReadsTest, CanReadWithExactTimestamp) {
  // Insert a row.
  ZETASQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 23}));

  // Sleep for 2s, and then insert another row.
  absl::SleepFor(absl::Seconds(2));
  ZETASQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

  // Read using an exact timestamp option set at 1s in the past. Only row 1
  // is visible at that timestamp.
  EXPECT_THAT(Read(Transaction::SingleUseOptions(Transaction::ReadOnlyOptions(
                       MakePastTimestamp(std::chrono::seconds(1)))),
                   "Users", {"ID", "Name", "Age"}, KeySet::All()),
              IsOkAndHoldsRows({ValueRow{1, "John", 23}}));
}

TEST_F(SnapshotReadsTest, CanReadWithExactStaleness) {
  // Insert a row.
  ZETASQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 23}));

  // Sleep for 2s, and then insert another row.
  absl::SleepFor(absl::Seconds(2));
  ZETASQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

  // Read using an exact staleness option set to 1s in the past. Only
  // row 1 is visible at that timestamp.
  EXPECT_THAT(Read(Transaction::SingleUseOptions(
                       Transaction::ReadOnlyOptions(std::chrono::seconds(1))),
                   "Users", {"ID", "Name", "Age"}, KeySet::All()),
              IsOkAndHoldsRows({ValueRow{1, "John", 23}}));
}

TEST_F(SnapshotReadsTest, CanReadWithExactTimestampInFuture) {
  // Insert a row.
  ZETASQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 23}));
  ZETASQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

  // Read using an exact timestamp option set to 100 ms in the future. Able to
  // read all the rows, but will wait for ~100 ms to pass before returning. Use
  // a larger time of 2000 ms for prod to reduce flakiness.
  int64_t future_time_ms = in_prod_env() ? 2000 : 100;
  absl::Time start_time = absl::Now();
  EXPECT_THAT(
      Read(Transaction::SingleUseOptions(Transaction::ReadOnlyOptions(
               MakeFutureTimestamp(std::chrono::milliseconds(future_time_ms)))),
           "Users", {"ID", "Name", "Age"}, KeySet::All()),
      IsOkAndHoldsRows({ValueRow{1, "John", 23}, ValueRow{2, "Peter", 41}}));

  EXPECT_GE(absl::Now() - absl::Milliseconds(future_time_ms * 0.9), start_time);
}

TEST_F(SnapshotReadsTest, CanReadWithMinTimestampBoundInFuture) {
  // Insert a few rows.
  ZETASQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", "23"}));
  ZETASQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

  // Read using a min_timestamp bound set to 100 ms in future. Able to read all
  // rows, but will wait for ~100 ms to pass before returning. Use a larger time
  // of 1000 ms for prod to reduce flakiness.
  int64_t future_time_ms = in_prod_env() ? 1000 : 100;
  absl::Time start_time = absl::Now();
  EXPECT_THAT(
      Read(Transaction::SingleUseOptions(
               MakeFutureTimestamp(std::chrono::milliseconds(future_time_ms))),
           "Users", {"ID", "Name", "Age"}, KeySet::All()),
      IsOkAndHoldsRows({ValueRow{1, "John", 23}, ValueRow{2, "Peter", 41}}));

  EXPECT_GE(absl::Now() - absl::Milliseconds(future_time_ms * 0.9), start_time);
}

TEST_F(SnapshotReadsTest, CannnotReadWithExactTimestampTooFarInFuture) {
  // Insert a row.
  ZETASQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 23}));
  ZETASQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

  if (!in_prod_env()) {
    EXPECT_THAT(Read(Transaction::SingleUseOptions(Transaction::ReadOnlyOptions(
                         MakeFutureTimestamp(std::chrono::hours(2)))),
                     "Users", {"ID", "Name", "Age"}, KeySet::All()),
                StatusIs(absl::StatusCode::kDeadlineExceeded));
  }
}

TEST_F(SnapshotReadsTest, CannnotQueryWithExactTimestampTooFarInFuture) {
  // Insert a row.
  ZETASQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 23}));
  ZETASQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

  if (!in_prod_env()) {
    EXPECT_THAT(QuerySingleUseTransaction(
                    Transaction::SingleUseOptions(Transaction::ReadOnlyOptions(
                        MakeFutureTimestamp(std::chrono::hours(2)))),
                    SqlStatement{"SELECT ID, Name, Age FROM Users"}),
                StatusIs(absl::StatusCode::kDeadlineExceeded));
  }
}

TEST_F(SnapshotReadsTest, CannnotReadWithMinTimestampBoundTooFarInFuture) {
  // Insert a row.
  ZETASQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 23}));
  ZETASQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

  if (!in_prod_env()) {
    EXPECT_THAT(Read(Transaction::SingleUseOptions(
                         MakeFutureTimestamp(std::chrono::hours(2))),
                     "Users", {"ID", "Name", "Age"}, KeySet::All()),
                StatusIs(absl::StatusCode::kDeadlineExceeded));
  }
}

TEST_F(SnapshotReadsTest, CannnotQueryWithMinTimestampBoundTooFarInFuture) {
  // Insert a row.
  ZETASQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 23}));
  ZETASQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

  if (!in_prod_env()) {
    EXPECT_THAT(QuerySingleUseTransaction(
                    Transaction::SingleUseOptions(Transaction::ReadOnlyOptions(
                        MakeFutureTimestamp(std::chrono::hours(2)))),
                    SqlStatement{"SELECT ID, Name, Age FROM Users"}),
                StatusIs(absl::StatusCode::kDeadlineExceeded));
  }
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
