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

#include "absl/status/status.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using zetasql_base::testing::StatusIs;

class MultiRowWritesTest : public DatabaseTest {
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

TEST_F(MultiRowWritesTest, CanCommitAnEmptyMutation) { ZETASQL_EXPECT_OK(Commit({})); }

TEST_F(MultiRowWritesTest, InsertSameKeyErrorWithAlreadyExists) {
  EXPECT_THAT(MultiInsert("Users", {"ID"}, {{1}, {1}}),
              in_prod_env() ? StatusIs(absl::StatusCode::kInvalidArgument)
                            : StatusIs(absl::StatusCode::kAlreadyExists));
}

TEST_F(MultiRowWritesTest, InsertOrUpdateSameKeySucceeds) {
  ZETASQL_EXPECT_OK(MultiInsertOrUpdate("Users", {"ID"}, {{1}, {1}}));

  // Read to verify correct values.
  EXPECT_THAT(ReadAll("Users", {"ID"}), IsOkAndHoldsRows({{1}}));
}

TEST_F(MultiRowWritesTest, UpdateSameKeySucceeds) {
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "Mark", 25}));
  ZETASQL_EXPECT_OK(MultiUpdate("Users", {"ID", "Age"}, {{1, 26}, {1, 27}}));

  // Read to verify correct values.
  EXPECT_THAT(ReadAll("Users", {"ID", "Name", "Age"}),
              IsOkAndHoldsRows({{1, "Mark", 27}}));
}

TEST_F(MultiRowWritesTest, ReplaceSameKeySucceeds) {
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "Mark", 25}));
  ZETASQL_EXPECT_OK(MultiReplace("Users", {"ID", "Name", "Age"},
                         {{1, "Mark", 26}, {1, "Mark", 27}}));

  // Read to verify correct values.
  EXPECT_THAT(ReadAll("Users", {"ID", "Name", "Age"}),
              IsOkAndHoldsRows({{1, "Mark", 27}}));
}

TEST_F(MultiRowWritesTest, DeleteSameKeySucceeds) {
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "Mark", 25}));
  ZETASQL_EXPECT_OK(Delete("Users", {Key(1), Key(1)}));

  // Read to verify row does not exist.
  EXPECT_THAT(ReadAll("Users", {"ID", "Name", "Age"}), IsOkAndHoldsRows({}));
}

TEST_F(MultiRowWritesTest, MultipleModsWithErrorsFails) {
  EXPECT_THAT(Commit({
                  MakeInsert("Users", {"ID", "Name"}, 1, "Mark"),
                  MakeInsert("NonExistentTable", {"Column"}, 1),
              }),
              zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(MultiRowWritesTest, DeleteNotAppliedWithFailingMods) {
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "Mark", 25}));

  EXPECT_THAT(Commit({
                  MakeDelete("Users", KeySet::All()),
                  MakeInsert("NonExistentTable", {"Column"}, 1),
              }),
              zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));

  EXPECT_THAT(ReadAll("Users", {"ID", "Name", "Age"}),
              IsOkAndHoldsRow({1, "Mark", 25}));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
