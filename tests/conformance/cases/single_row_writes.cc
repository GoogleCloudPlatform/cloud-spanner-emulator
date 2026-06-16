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
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using googlesql_base::testing::StatusIs;

class SingleRowWritesTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

  absl::Status SetUpDatabase() override {
    return SetSchemaFromFile("single_row_writes.test");
  }
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectSingleRowWritesTest, SingleRowWritesTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<SingleRowWritesTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(SingleRowWritesTest, CanReadInsertedRows) {
  // Insert a few rows (some with null columns).
  GOOGLESQL_EXPECT_OK(Insert("Users", {"ID", "Name"}, {1, "John"}));
  GOOGLESQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

  // Read back all rows.
  EXPECT_THAT(
      ReadAll("Users", {"ID", "Name", "Age"}),
      IsOkAndHoldsRows({{1, "John", Null<std::int64_t>()}, {2, "Peter", 41}}));
}

TEST_P(SingleRowWritesTest, CannotInsertARowTwice) {
  // Insert a row.
  GOOGLESQL_EXPECT_OK(Insert("Users", {"ID", "Name"}, {1, "John"}));

  // Check that we cannot do a double-insert.
  EXPECT_THAT(Insert("Users", {"ID", "Name"}, {1, "Peter"}),
              StatusIs(absl::StatusCode::kAlreadyExists));
}

TEST_P(SingleRowWritesTest, CannotUpdateWithoutInsert) {
  // Check that we cannot update a non-existent row.
  EXPECT_THAT(Update("Users", {"ID", "Name"}, {1, "Peter"}),
              StatusIs(absl::StatusCode::kNotFound));

  // Check that we can update a row that exists.
  GOOGLESQL_EXPECT_OK(Insert("Users", {"ID", "Name"}, {1, "John"}));
  GOOGLESQL_EXPECT_OK(Update("Users", {"ID", "Name"}, {1, "Peter"}));
  EXPECT_THAT(ReadAll("Users", {"ID", "Name"}), IsOkAndHoldsRow({1, "Peter"}));
}

TEST_P(SingleRowWritesTest, CanUpdateWithoutNonKeyColumns) {
  // Insert a row.
  GOOGLESQL_EXPECT_OK(Insert("Users", {"ID", "Name"}, {1, "John"}));

  // Update the row, but only specify the primary key (no-op).
  GOOGLESQL_EXPECT_OK(Update("Users", {"ID"}, {1}));
  EXPECT_THAT(ReadAll("Users", {"ID", "Name"}), IsOkAndHoldsRow({1, "John"}));
}

TEST_P(SingleRowWritesTest, ReplaceClearsOldColumnValues) {
  // Insert a fully-specified row.
  GOOGLESQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 41}));

  // Check that replace clears previous value of a column.
  GOOGLESQL_EXPECT_OK(Replace("Users", {"ID", "Name"}, {1, "Peter"}));
  EXPECT_THAT(ReadAll("Users", {"ID", "Name", "Age"}),
              IsOkAndHoldsRow({1, "Peter", Null<std::int64_t>()}));
}

TEST_P(SingleRowWritesTest, DeleteClearsRow) {
  // Insert a few rows.
  GOOGLESQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 25}));
  GOOGLESQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

  // Read back all rows.
  EXPECT_THAT(ReadAll("Users", {"ID", "Name", "Age"}),
              IsOkAndHoldsRows({{1, "John", 25}, {2, "Peter", 41}}));

  // Delete one of the rows.
  GOOGLESQL_EXPECT_OK(Delete("Users", Key(1)));

  // Read back all rows.
  EXPECT_THAT(ReadAll("Users", {"ID", "Name", "Age"}),
              IsOkAndHoldsRow({2, "Peter", 41}));
}

TEST_P(SingleRowWritesTest, CanDeleteNonExistentRow) {
  // Deletes are idempotent - we do not require the row to exist.
  GOOGLESQL_EXPECT_OK(Delete("Users", Key(1)));

  // Check that no row was introduced in the process.
  EXPECT_THAT(ReadAll("Users", {"ID", "Name", "Age"}),
              googlesql_base::testing::IsOkAndHolds(testing::IsEmpty()));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
