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
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

constexpr int64_t kNumRows = 20;
constexpr int64_t kStringSize = 409600;

class LargeReadsTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

 public:
  absl::Status SetUpDatabase() override {
    return SetSchemaFromFile("large_reads.test");
  }

 protected:
  void PopulateDatabase() {
    // Populate the database with more than 4MB worth of data.
    for (int i = 0; i < kNumRows; ++i) {
      GOOGLESQL_EXPECT_OK(Insert("Users", {"ID", "Name"},
                       {i, std::string(kStringSize, 'a' + (i % 26))}));
    }
  }
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectLargeReadsTest, LargeReadsTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<LargeReadsTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(LargeReadsTest, CanPerformLargeReadWithRange) {
  PopulateDatabase();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::vector<ValueRow> rows,
      Read("Users", {"ID", "Name"}, ClosedOpen(Key(0), Key(12))));
  EXPECT_THAT(rows.size(), 12);
  for (int i = 0; i < rows.size(); ++i) {
    std::vector<google::cloud::spanner::Value> values = rows[i];
    EXPECT_THAT(values,
                testing::ElementsAre(
                    Value(i), Value(std::string(kStringSize, 'a' + (i % 26)))));
  }
}

TEST_P(LargeReadsTest, CanPerformLargeReadWithAllRows) {
  PopulateDatabase();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::vector<ValueRow> rows,
                       Read("Users", {"ID", "Name"}, KeySet::All()));
  EXPECT_EQ(rows.size(), kNumRows);
  for (int i = 0; i < rows.size(); ++i) {
    std::vector<google::cloud::spanner::Value> values = rows[i];
    EXPECT_THAT(values,
                testing::ElementsAre(
                    Value(i), Value(std::string(kStringSize, 'a' + (i % 26)))));
  }
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
