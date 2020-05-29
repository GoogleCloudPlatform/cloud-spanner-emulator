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

#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using zetasql_base::testing::StatusIs;

class DeleteRangeTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    return SetSchema({R"(
      CREATE TABLE Users(
        ShardID  INT64 NOT NULL,
        UserID   INT64,
        ClassID  STRING(MAX),
        Name     STRING(MAX),
        Age      INT64
      ) PRIMARY KEY (ShardID, UserID DESC, ClassID)
    )"});
  }
};

TEST_F(DeleteRangeTest, CannotDeleteRangeWithMultipleDifferingKeyParts) {
  // Insert a few rows.
  ZETASQL_EXPECT_OK(Insert("Users", {"ShardID", "UserID", "ClassID", "Name", "Age"},
                   {1, 2, "New", "John", 21}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ShardID", "UserID", "ClassID", "Name", "Age"},
                   {2, 3, "Cancelled", "Peter", 41}));

  // DeleteRange only allows keys to differ in last part.
  EXPECT_THAT(Delete("Users", ClosedOpen(Key(1, 2, "a"), Key(1, 3, "b"))),
              StatusIs(absl::StatusCode::kUnimplemented));
  EXPECT_THAT(Delete("Users", ClosedOpen(Key(1), Key(1, 3, "b"))),
              StatusIs(absl::StatusCode::kUnimplemented));
  EXPECT_THAT(Delete("Users", ClosedClosed(Key(1, 2, "a"), Key(2))),
              StatusIs(absl::StatusCode::kUnimplemented));
  EXPECT_THAT(Delete("Users", ClosedClosed(Key(), Key(2, 3))),
              StatusIs(absl::StatusCode::kUnimplemented));
  EXPECT_THAT(Delete("Users", ClosedClosed(Key(2, 3), Key())),
              StatusIs(absl::StatusCode::kUnimplemented));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
