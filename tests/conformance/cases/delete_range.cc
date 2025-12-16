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
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using zetasql_base::testing::StatusIs;

class DeleteRangeTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

  absl::Status SetUpDatabase() override {
    return SetSchemaFromFile("delete_range.test");
  }
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectDeleteRangeTest, DeleteRangeTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<DeleteRangeTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(DeleteRangeTest, CannotDeleteRangeWithMultipleDifferingKeyParts) {
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
