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
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using googlesql_base::testing::StatusIs;

class TablesampleTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

  absl::Status SetUpDatabase() override {
    GOOGLESQL_EXPECT_OK(SetSchemaFromFile("tablesample.test"));
    GOOGLESQL_EXPECT_OK(MultiInsert("Entries", {"Id"},
                          {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}}));
    return absl::OkStatus();
  }
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectTablesampleTests, TablesampleTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<TablesampleTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(TablesampleTest, SampleSomeRows) {
  if (dialect_ == database_api::DatabaseDialect::POSTGRESQL) {
    EXPECT_THAT(Query(R"(SELECT COUNT(*) > 0 FROM Entries
                        TABLESAMPLE BERNOULLI(99.99))"),
                IsOkAndHoldsRows({{true}}));
    EXPECT_THAT(Query(R"(SELECT COUNT(*) = 1 FROM Entries
                        TABLESAMPLE SPANNER.RESERVOIR(1))"),
                IsOkAndHoldsRows({{true}}));
  } else {
    EXPECT_THAT(Query(R"(SELECT COUNT(*) > 0 FROM Entries
                        TABLESAMPLE BERNOULLI(99.99 PERCENT))"),
                IsOkAndHoldsRows({{true}}));
    EXPECT_THAT(Query(R"(SELECT COUNT(*) = 1 FROM Entries
                        TABLESAMPLE RESERVOIR(1 ROWS))"),
                IsOkAndHoldsRows({{true}}));
  }
}

TEST_P(TablesampleTest, RepeatableIsNotSupported) {
  if (dialect_ == database_api::DatabaseDialect::POSTGRESQL) {
    EXPECT_THAT(Query(R"(SELECT * FROM Entries
                       TABLESAMPLE BERNOULLI(50) REPEATABLE(5))"),
                StatusIs(absl::StatusCode::kUnimplemented));
    EXPECT_THAT(Query(R"(SELECT * FROM Entries
                       TABLESAMPLE SPANNER.RESERVOIR(10) REPEATABLE(6))"),
                StatusIs(absl::StatusCode::kUnimplemented));
    EXPECT_THAT(Query(R"(SELECT * FROM Entries
                       TABLESAMPLE SYSTEM(20) REPEATABLE(7))"),
                StatusIs(absl::StatusCode::kUnimplemented));
    EXPECT_THAT(Query(R"(SELECT * FROM Entries
                       TABLESAMPLE SYSTEM(10) REPEATABLE(8))"),
                StatusIs(absl::StatusCode::kUnimplemented));
  } else {
    EXPECT_THAT(Query(R"(SELECT * FROM Entries
                       TABLESAMPLE BERNOULLI(50 PERCENT) REPEATABLE(5))"),
                StatusIs(absl::StatusCode::kInvalidArgument));
    EXPECT_THAT(Query(R"(SELECT * FROM Entries
                       TABLESAMPLE RESERVOIR(10 ROWS) REPEATABLE(6))"),
                StatusIs(absl::StatusCode::kInvalidArgument));
    EXPECT_THAT(Query(R"(SELECT * FROM Entries
                       TABLESAMPLE SYSTEM(20 PERCENT) REPEATABLE(7))"),
                StatusIs(absl::StatusCode::kInvalidArgument));
    EXPECT_THAT(Query(R"(SELECT * FROM Entries
                       TABLESAMPLE SYSTEM(10 ROWS) REPEATABLE(8))"),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }
}

TEST_P(TablesampleTest, SystemSampingIsNotSupported) {
  if (dialect_ == database_api::DatabaseDialect::POSTGRESQL) {
    EXPECT_THAT(Query(R"(SELECT * FROM Entries
                       TABLESAMPLE SYSTEM(50))"),
                StatusIs(absl::StatusCode::kUnimplemented));
  } else {
    EXPECT_THAT(Query(R"(SELECT * FROM Entries
                        TABLESAMPLE SYSTEM(50 PERCENT))"),
                StatusIs(absl::StatusCode::kInvalidArgument));
    EXPECT_THAT(Query(R"(SELECT * FROM Entries
                        TABLESAMPLE SYSTEM(50 ROWS))"),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }
}

}  // namespace
}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
