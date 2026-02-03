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

#include <vector>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/strings/substitute.h"
#include "absl/time/time.h"
#include "google/cloud/spanner/timestamp.h"
#include "common/feature_flags.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using zetasql_base::testing::StatusIs;

class ColumnOnUpdateReadWriteTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  absl::Status SetUpDatabase() override {
    return SetSchemaFromFile("column_on_update.test");
  }

  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }
};

MATCHER_P(WhenLowercased, matcher, "") {
  return ::testing::ExplainMatchResult(matcher, absl::AsciiStrToLower(arg),
                                       result_listener);
}

INSTANTIATE_TEST_SUITE_P(
    PerDialectColumnOnUpdateTests, ColumnOnUpdateReadWriteTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<ColumnOnUpdateReadWriteTest::ParamType>&
           info) { return database_api::DatabaseDialect_Name(info.param); });

TEST_P(ColumnOnUpdateReadWriteTest, MutationPCT) {
  // Insert two rows with explicit values.
  ZETASQL_EXPECT_OK(Insert(
      "on_update", {"k", "ts_on_update"},
      {1, cloud::spanner::MakeTimestamp(absl::FromUnixSeconds(10)).value()}));
  ZETASQL_EXPECT_OK(Insert(
      "on_update", {"k", "ts_on_update"},
      {2, cloud::spanner::MakeTimestamp(absl::FromUnixSeconds(20)).value()}));
  EXPECT_THAT(
      Query("SELECT k, ts_on_update from on_update ORDER BY k ASC"),
      IsOkAndHoldsRows(
          {{1,
            cloud::spanner::MakeTimestamp(absl::FromUnixSeconds(10)).value()},
           {2, cloud::spanner::MakeTimestamp(absl::FromUnixSeconds(20))
                   .value()}}));

  // Only touch the primary key - should not trigger ON UPDATE.
  ZETASQL_EXPECT_OK(Update("on_update", {"k"}, {1}));
  EXPECT_THAT(
      Query("SELECT k, ts_on_update from on_update ORDER BY k ASC"),
      IsOkAndHoldsRows(
          {{1,
            cloud::spanner::MakeTimestamp(absl::FromUnixSeconds(10)).value()},
           {2, cloud::spanner::MakeTimestamp(absl::FromUnixSeconds(20))
                   .value()}}));

  // Set an explicit value for the ON UPDATE column.
  ZETASQL_EXPECT_OK(Update(
      "on_update", {"k", "ts_on_update"},
      {2, cloud::spanner::MakeTimestamp(absl::FromUnixSeconds(30)).value()}));
  EXPECT_THAT(
      Query("SELECT k, ts_on_update from on_update ORDER BY k ASC"),
      IsOkAndHoldsRows(
          {{1,
            cloud::spanner::MakeTimestamp(absl::FromUnixSeconds(10)).value()},
           {2, cloud::spanner::MakeTimestamp(absl::FromUnixSeconds(30))
                   .value()}}));

  // Trigger ON UPDATE by touching a non-key column.
  ZETASQL_EXPECT_OK(
      Update("on_update", {"k", "ts"}, {1, "spanner.commit_timestamp()"}));
  EXPECT_THAT(Query("SELECT k from on_update WHERE ts = ts_on_update"),
              IsOkAndHoldsRows({{1}}));
}

TEST_P(ColumnOnUpdateReadWriteTest, DMLsWithPCT) {
  std::string pct_function =
      GetParam() == database_api::DatabaseDialect::POSTGRESQL
          ? "SPANNER.PENDING_COMMIT_TIMESTAMP"
          : "PENDING_COMMIT_TIMESTAMP";
  std::string timestamp_function =
      GetParam() == database_api::DatabaseDialect::POSTGRESQL
          ? "TO_TIMESTAMP"
          : "TIMESTAMP_SECONDS";

  // Insert two rows with explicit values.
  ZETASQL_ASSERT_OK(CommitDml({SqlStatement(
      absl::Substitute("INSERT INTO on_update(k, ts_on_update) VALUES "
                       "(1, $0(10)), (2, $0(20))",
                       timestamp_function))}));
  EXPECT_THAT(
      Query("SELECT k, ts_on_update from on_update ORDER BY k ASC"),
      IsOkAndHoldsRows(
          {{1,
            cloud::spanner::MakeTimestamp(absl::FromUnixSeconds(10)).value()},
           {2, cloud::spanner::MakeTimestamp(absl::FromUnixSeconds(20))
                   .value()}}));

  // Only touch the primary key - should not trigger ON UPDATE.
  std::string sql = "INSERT $0 INTO on_update(k) VALUES (1) $1";
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    sql = absl::Substitute(sql, "",
                           "ON CONFLICT(k) DO UPDATE SET k = excluded.k");
  } else {
    sql = absl::Substitute(sql, "OR UPDATE", "");
  }
  ZETASQL_ASSERT_OK(CommitDml({SqlStatement(sql)}));
  EXPECT_THAT(
      Query("SELECT k, ts_on_update from on_update ORDER BY k ASC"),
      IsOkAndHoldsRows(
          {{1,
            cloud::spanner::MakeTimestamp(absl::FromUnixSeconds(10)).value()},
           {2, cloud::spanner::MakeTimestamp(absl::FromUnixSeconds(20))
                   .value()}}));

  // Set an explicit value for the ON UPDATE column.
  ZETASQL_ASSERT_OK(CommitDml(
      {SqlStatement(absl::Substitute("UPDATE on_update SET ts_on_update = "
                                     "$0(30) WHERE k = 2",
                                     timestamp_function))}));
  EXPECT_THAT(
      Query("SELECT k, ts_on_update from on_update ORDER BY k ASC"),
      IsOkAndHoldsRows(
          {{1,
            cloud::spanner::MakeTimestamp(absl::FromUnixSeconds(10)).value()},
           {2, cloud::spanner::MakeTimestamp(absl::FromUnixSeconds(30))
                   .value()}}));

  // Trigger ON UPDATE by touching a non-key column.
  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement(absl::Substitute("UPDATE on_update SET ts = "
                                               "$0() WHERE k = 1",
                                               pct_function))}));
  EXPECT_THAT(Query("SELECT k from on_update WHERE ts = ts_on_update"),
              IsOkAndHoldsRows({{1}}));
}

TEST_P(ColumnOnUpdateReadWriteTest, ReturningPCT) {
  EmulatorFeatureFlags::Flags flags;
  flags.enable_dml_returning = true;
  emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

  std::vector<ValueRow> result;
  std::string timestamp_function =
      GetParam() == database_api::DatabaseDialect::POSTGRESQL
          ? "TO_TIMESTAMP"
          : "TIMESTAMP_SECONDS";
  std::string returning =
      GetParam() == database_api::DatabaseDialect::POSTGRESQL ? "RETURNING"
                                                              : "THEN RETURN";
  std::string sql = absl::Substitute(
      "UPDATE on_update SET ts = $0(10) WHERE k = 1 $1 ts_on_update",
      timestamp_function, returning);
  ;
  EXPECT_THAT(CommitDmlReturning({SqlStatement(sql)}, result),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr(
                           "The PENDING_COMMIT_TIMESTAMP() function may only "
                           "be used as a value for INSERT or UPDATE")));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
