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
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using zetasql_base::testing::IsOk;

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

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
