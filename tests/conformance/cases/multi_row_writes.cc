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
#include "absl/status/status_matchers.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using googlesql_base::testing::StatusIs;

class MultiRowWritesTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

 public:
  absl::Status SetUpDatabase() override {
    return SetSchemaFromFile("multi_row_writes.test");
  }
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectMultiRowWritesTest, MultiRowWritesTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<MultiRowWritesTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(MultiRowWritesTest, CanCommitAnEmptyMutation) { GOOGLESQL_EXPECT_OK(Commit({})); }

TEST_P(MultiRowWritesTest, InsertSameKeyErrorWithAlreadyExists) {
  EXPECT_THAT(MultiInsert("Users", {"ID"}, {{1}, {1}}),
              in_prod_env() ? StatusIs(absl::StatusCode::kInvalidArgument)
                            : StatusIs(absl::StatusCode::kAlreadyExists));
}

TEST_P(MultiRowWritesTest, InsertOrUpdateSameKeySucceeds) {
  GOOGLESQL_EXPECT_OK(MultiInsertOrUpdate("Users", {"ID"}, {{1}, {1}}));

  // Read to verify correct values.
  EXPECT_THAT(ReadAll("Users", {"ID"}), IsOkAndHoldsRows({{1}}));
}

TEST_P(MultiRowWritesTest, UpdateSameKeySucceeds) {
  GOOGLESQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "Mark", 25}));
  GOOGLESQL_EXPECT_OK(MultiUpdate("Users", {"ID", "Age"}, {{1, 26}, {1, 27}}));

  // Read to verify correct values.
  EXPECT_THAT(ReadAll("Users", {"ID", "Name", "Age"}),
              IsOkAndHoldsRows({{1, "Mark", 27}}));
}

TEST_P(MultiRowWritesTest, ReplaceSameKeySucceeds) {
  GOOGLESQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "Mark", 25}));
  GOOGLESQL_EXPECT_OK(MultiReplace("Users", {"ID", "Name", "Age"},
                         {{1, "Mark", 26}, {1, "Mark", 27}}));

  // Read to verify correct values.
  EXPECT_THAT(ReadAll("Users", {"ID", "Name", "Age"}),
              IsOkAndHoldsRows({{1, "Mark", 27}}));
}

TEST_P(MultiRowWritesTest, DeleteSameKeySucceeds) {
  GOOGLESQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "Mark", 25}));
  GOOGLESQL_EXPECT_OK(Delete("Users", {Key(1), Key(1)}));

  // Read to verify row does not exist.
  EXPECT_THAT(ReadAll("Users", {"ID", "Name", "Age"}), IsOkAndHoldsRows({}));
}

TEST_P(MultiRowWritesTest, MultipleModsWithErrorsFails) {
  EXPECT_THAT(Commit({
                  MakeInsert("Users", {"ID", "Name"}, 1, "Mark"),
                  MakeInsert("NonExistentTable", {"Column"}, 1),
              }),
              googlesql_base::testing::StatusIs(absl::StatusCode::kNotFound));
}

TEST_P(MultiRowWritesTest, DeleteNotAppliedWithFailingMods) {
  GOOGLESQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "Mark", 25}));

  EXPECT_THAT(Commit({
                  MakeDelete("Users", KeySet::All()),
                  MakeInsert("NonExistentTable", {"Column"}, 1),
              }),
              googlesql_base::testing::StatusIs(absl::StatusCode::kNotFound));

  EXPECT_THAT(ReadAll("Users", {"ID", "Name", "Age"}),
              IsOkAndHoldsRow({1, "Mark", 25}));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
