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

#include "google/spanner/admin/database/v1/common.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/time/time.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using googlesql_base::testing::StatusIs;

class SnapshotReadsTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

 public:
  absl::Status SetUpDatabase() override {
    return SetSchemaFromFile("snapshot_reads.test");
  }
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectSnapshotReadsTest, SnapshotReadsTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<SnapshotReadsTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(SnapshotReadsTest, CanReadWithMinTimestampBound) {
  // Insert a few rows.
  GOOGLESQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 23}));
  GOOGLESQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

  // Read using a min_timestamp bounded staleness.
  auto result = Read(Transaction::SingleUseOptions(
                         MakePastTimestamp(std::chrono::minutes(10))),
                     "Users", {"ID", "Name", "Age"}, KeySet::All());
  EXPECT_THAT(result, googlesql_base::testing::IsOk());
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

TEST_P(SnapshotReadsTest, CanReadWithMaxStalenessBound) {
  // Insert a few rows.
  GOOGLESQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 23}));
  GOOGLESQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

  // Read using a max staleness bound.
  auto result = Read(Transaction::SingleUseOptions(std::chrono::minutes(10)),
                     "Users", {"ID", "Name", "Age"}, KeySet::All());
  EXPECT_THAT(result, googlesql_base::testing::IsOk());
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

TEST_P(SnapshotReadsTest, CanReadWithExactTimestamp) {
  // Insert a row.
  GOOGLESQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 23}));

  // Sleep for 2s, and then insert another row.
  absl::SleepFor(absl::Seconds(2));
  GOOGLESQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

  // Read using an exact timestamp option set at 1s in the past. Only row 1
  // is visible at that timestamp.
  EXPECT_THAT(Read(Transaction::SingleUseOptions(Transaction::ReadOnlyOptions(
                       MakePastTimestamp(std::chrono::seconds(1)))),
                   "Users", {"ID", "Name", "Age"}, KeySet::All()),
              IsOkAndHoldsRows({ValueRow{1, "John", 23}}));
}

TEST_P(SnapshotReadsTest, CanReadWithExactStaleness) {
  // Insert a row.
  GOOGLESQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 23}));

  // Sleep for 2s, and then insert another row.
  absl::SleepFor(absl::Seconds(2));
  GOOGLESQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

  // Read using an exact staleness option set to 1s in the past. Only
  // row 1 is visible at that timestamp.
  EXPECT_THAT(Read(Transaction::SingleUseOptions(
                       Transaction::ReadOnlyOptions(std::chrono::seconds(1))),
                   "Users", {"ID", "Name", "Age"}, KeySet::All()),
              IsOkAndHoldsRows({ValueRow{1, "John", 23}}));
}

TEST_P(SnapshotReadsTest, CanReadWithExactTimestampInFuture) {
  // Insert a row.
  GOOGLESQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 23}));
  GOOGLESQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

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

TEST_P(SnapshotReadsTest, CanReadWithMinTimestampBoundInFuture) {
  // Insert a few rows.
  GOOGLESQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", "23"}));
  GOOGLESQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

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

TEST_P(SnapshotReadsTest, CannnotReadWithExactTimestampTooFarInFuture) {
  // Insert a row.
  GOOGLESQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 23}));
  GOOGLESQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

  if (!in_prod_env()) {
    EXPECT_THAT(Read(Transaction::SingleUseOptions(Transaction::ReadOnlyOptions(
                         MakeFutureTimestamp(std::chrono::hours(2)))),
                     "Users", {"ID", "Name", "Age"}, KeySet::All()),
                StatusIs(absl::StatusCode::kDeadlineExceeded));
  }
}

TEST_P(SnapshotReadsTest, CannnotQueryWithExactTimestampTooFarInFuture) {
  // Insert a row.
  GOOGLESQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 23}));
  GOOGLESQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

  if (!in_prod_env()) {
    EXPECT_THAT(QuerySingleUseTransaction(
                    Transaction::SingleUseOptions(Transaction::ReadOnlyOptions(
                        MakeFutureTimestamp(std::chrono::hours(2)))),
                    SqlStatement{"SELECT ID, Name, Age FROM Users"}),
                StatusIs(absl::StatusCode::kDeadlineExceeded));
  }
}

TEST_P(SnapshotReadsTest, CannnotReadWithMinTimestampBoundTooFarInFuture) {
  // Insert a row.
  GOOGLESQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 23}));
  GOOGLESQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

  if (!in_prod_env()) {
    EXPECT_THAT(Read(Transaction::SingleUseOptions(
                         MakeFutureTimestamp(std::chrono::hours(2))),
                     "Users", {"ID", "Name", "Age"}, KeySet::All()),
                StatusIs(absl::StatusCode::kDeadlineExceeded));
  }
}

TEST_P(SnapshotReadsTest, CannnotQueryWithMinTimestampBoundTooFarInFuture) {
  // Insert a row.
  GOOGLESQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 23}));
  GOOGLESQL_ASSERT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

  if (!in_prod_env()) {
    EXPECT_THAT(QuerySingleUseTransaction(
                    Transaction::SingleUseOptions(Transaction::ReadOnlyOptions(
                        MakeFutureTimestamp(std::chrono::hours(2)))),
                    SqlStatement{"SELECT ID, Name, Age FROM Users"}),
                StatusIs(absl::StatusCode::kDeadlineExceeded));
  }
}

class VersionRetentionPeriodTest : public DatabaseTest {
  absl::Status SetUpDatabase() override { return absl::OkStatus(); }
};

TEST_F(VersionRetentionPeriodTest,
       CannotReadDroppedTableWithExactTimestampAfterTableDeletion) {
  if (in_prod_env()) {
    GTEST_SKIP() << "Skipping this test in prod environment because the "
                    "minimum version retention period is 1 hour";
  }

  GOOGLESQL_ASSERT_OK(SetSchema({R"(
    CREATE TABLE TestTable(
      ID   INT64 NOT NULL,
      Name STRING(MAX),
      Age  INT64
    ) PRIMARY KEY (ID)
  )"}));

  // Sleep for 1s to ensure that the timestamp is different from the commit
  // timestamp of the table creation.
  absl::SleepFor(absl::Seconds(1));
  Timestamp before_table_deletion_empty_read = MakeNowTimestamp();

  // Insert a row.
  GOOGLESQL_ASSERT_OK(Insert("TestTable", {"ID", "Name", "Age"}, {1, "John", 23}));

  // Create a timestamp after confirming that the data is readable.
  EXPECT_THAT(Read(Transaction::SingleUseOptions(std::chrono::seconds(1)),
                   "TestTable", {"ID", "Name", "Age"}, KeySet::All()),
              IsOkAndHoldsRows({ValueRow{1, "John", 23}}));

  // Sleep for 1s to ensure that the timestamp is different from the commit
  // timestamp of the row insertion.
  absl::SleepFor(absl::Seconds(1));
  Timestamp before_table_deletion_read_with_data = MakeNowTimestamp();

  GOOGLESQL_ASSERT_OK(UpdateSchema({"DROP TABLE TestTable"}));

  // Reads with a timestamp before the table deletion should succeed.
  EXPECT_THAT(Read(Transaction::SingleUseOptions(Transaction::ReadOnlyOptions(
                       before_table_deletion_empty_read)),
                   "TestTable", {"ID", "Name", "Age"}, KeySet::All()),
              IsOkAndHoldsRows({}));
  EXPECT_THAT(Read(Transaction::SingleUseOptions(Transaction::ReadOnlyOptions(
                       before_table_deletion_read_with_data)),
                   "TestTable", {"ID", "Name", "Age"}, KeySet::All()),
              IsOkAndHoldsRows({ValueRow{1, "John", 23}}));

  // Sleep for 2s to ensure that the next version retention period update
  // triggers the cleanup of the table and the schema with the definition.
  absl::SleepFor(absl::Seconds(2));

  // Set the version_retention_period to 1s. This will trigger the cleanup of
  // the schema with the table definition, and the table's data.
  GOOGLESQL_ASSERT_OK(UpdateSchema(
      {"ALTER DATABASE db SET OPTIONS (version_retention_period = '1s')"}));

  // Set the version_retention_period back to 1 hour so that reads with
  // timestamp before the table deletion are not restricted.
  GOOGLESQL_ASSERT_OK(UpdateSchema(
      {"ALTER DATABASE db SET OPTIONS (version_retention_period = '1h')"}));

  // The reads should fail with NotFound error because the table was dropped
  // and the schema with the table no longer exists.
  EXPECT_THAT(Read(Transaction::SingleUseOptions(Transaction::ReadOnlyOptions(
                       before_table_deletion_empty_read)),
                   "TestTable", {"ID", "Name", "Age"}, KeySet::All()),
              StatusIs(absl::StatusCode::kNotFound));
  EXPECT_THAT(Read(Transaction::SingleUseOptions(Transaction::ReadOnlyOptions(
                       before_table_deletion_read_with_data)),
                   "TestTable", {"ID", "Name", "Age"}, KeySet::All()),
              StatusIs(absl::StatusCode::kNotFound));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
