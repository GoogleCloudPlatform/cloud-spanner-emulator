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

using zetasql_base::testing::IsOk;
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
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", "23"}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

  // Read using a min_timestamp bounded staleness.
  EXPECT_THAT(
      Read(Transaction::SingleUseOptions(
               MakePastTimestamp(std::chrono::minutes(10))),
           "Users", {"ID", "Name", "Age"}, KeySet::All()),
      IsOkAndHoldsRows({ValueRow{1, "John", 23}, ValueRow{2, "Peter", 41}}));
}

TEST_F(SnapshotReadsTest, CanReadWithMaxStalenessBound) {
  // Insert a few rows.
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 23}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

  // Read using a max statleness bound.
  EXPECT_THAT(
      Read(Transaction::SingleUseOptions(std::chrono::minutes(10)),
           "Users", {"ID", "Name", "Age"}, KeySet::All()),
      IsOkAndHoldsRows({ValueRow{1, "John", 23}, ValueRow{2, "Peter", 41}}));
}

TEST_F(SnapshotReadsTest, CanReadWithExactTimestamp) {
  // Insert a row.
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 23}));

  // Sleep for 200 ms, and then insert another row.
  absl::SleepFor(absl::Milliseconds(200));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

  // Read using an exact timestamp option set at 100 ms in the past. Only row 1
  // is visible at that timestamp.
  EXPECT_THAT(Read(Transaction::SingleUseOptions(Transaction::ReadOnlyOptions(
                       MakePastTimestamp(std::chrono::milliseconds(100)))),
                   "Users", {"ID", "Name", "Age"}, KeySet::All()),
              IsOkAndHoldsRows({ValueRow{1, "John", 23}}));
}

TEST_F(SnapshotReadsTest, CanReadWithExactStaleness) {
  // Insert a row.
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 23}));

  // Sleep for 200 ms, and then insert another row.
  absl::SleepFor(absl::Milliseconds(200));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

  // Read using an exact staleness option set to 100 ms in the past. Only
  // row 1 is visible at that timestamp.
  EXPECT_THAT(Read(Transaction::SingleUseOptions(Transaction::ReadOnlyOptions(
                       std::chrono::milliseconds(100))),
                   "Users", {"ID", "Name", "Age"}, KeySet::All()),
              IsOkAndHoldsRows({ValueRow{1, "John", 23}}));
}

TEST_F(SnapshotReadsTest, CanReadWithExactTimestampInFuture) {
  // Insert a row.
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 23}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

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
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", "23"}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

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
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 23}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

  if (!in_prod_env()) {
    EXPECT_THAT(Read(Transaction::SingleUseOptions(Transaction::ReadOnlyOptions(
                         MakeFutureTimestamp(std::chrono::hours(2)))),
                     "Users", {"ID", "Name", "Age"}, KeySet::All()),
                StatusIs(absl::StatusCode::kDeadlineExceeded));
  }
}

TEST_F(SnapshotReadsTest, CannnotQueryWithExactTimestampTooFarInFuture) {
  // Insert a row.
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 23}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

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
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 23}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

  if (!in_prod_env()) {
    EXPECT_THAT(Read(Transaction::SingleUseOptions(
                         MakeFutureTimestamp(std::chrono::hours(2))),
                     "Users", {"ID", "Name", "Age"}, KeySet::All()),
                StatusIs(absl::StatusCode::kDeadlineExceeded));
  }
}

TEST_F(SnapshotReadsTest, CannnotQueryWithMinTimestampBoundTooFarInFuture) {
  // Insert a row.
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 23}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

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
