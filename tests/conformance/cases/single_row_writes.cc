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
#include "absl/status/status.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using zetasql_base::testing::StatusIs;

class SingleRowWritesTest : public DatabaseTest {
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

TEST_F(SingleRowWritesTest, CanReadInsertedRows) {
  // Insert a few rows (some with null columns).
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name"}, {1, "John"}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

  // Read back all rows.
  EXPECT_THAT(
      ReadAll("Users", {"ID", "Name", "Age"}),
      IsOkAndHoldsRows({{1, "John", Null<std::int64_t>()}, {2, "Peter", 41}}));
}

TEST_F(SingleRowWritesTest, CannotInsertARowTwice) {
  // Insert a row.
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name"}, {1, "John"}));

  // Check that we cannot do a double-insert.
  EXPECT_THAT(Insert("Users", {"ID", "Name"}, {1, "Peter"}),
              StatusIs(absl::StatusCode::kAlreadyExists));
}

TEST_F(SingleRowWritesTest, CannotUpdateWithoutInsert) {
  // Check that we cannot update a non-existent row.
  EXPECT_THAT(Update("Users", {"ID", "Name"}, {1, "Peter"}),
              StatusIs(absl::StatusCode::kNotFound));

  // Check that we can update a row that exists.
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name"}, {1, "John"}));
  ZETASQL_EXPECT_OK(Update("Users", {"ID", "Name"}, {1, "Peter"}));
  EXPECT_THAT(ReadAll("Users", {"ID", "Name"}), IsOkAndHoldsRow({1, "Peter"}));
}

TEST_F(SingleRowWritesTest, CanUpdateWithoutNonKeyColumns) {
  // Insert a row.
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name"}, {1, "John"}));

  // Update the row, but only specify the primary key (no-op).
  ZETASQL_EXPECT_OK(Update("Users", {"ID"}, {1}));
  EXPECT_THAT(ReadAll("Users", {"ID", "Name"}), IsOkAndHoldsRow({1, "John"}));
}

TEST_F(SingleRowWritesTest, ReplaceClearsOldColumnValues) {
  // Insert a fully-specified row.
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 41}));

  // Check that replace clears previous value of a column.
  ZETASQL_EXPECT_OK(Replace("Users", {"ID", "Name"}, {1, "Peter"}));
  EXPECT_THAT(ReadAll("Users", {"ID", "Name", "Age"}),
              IsOkAndHoldsRow({1, "Peter", Null<std::int64_t>()}));
}

TEST_F(SingleRowWritesTest, DeleteClearsRow) {
  // Insert a few rows.
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 25}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));

  // Read back all rows.
  EXPECT_THAT(ReadAll("Users", {"ID", "Name", "Age"}),
              IsOkAndHoldsRows({{1, "John", 25}, {2, "Peter", 41}}));

  // Delete one of the rows.
  ZETASQL_EXPECT_OK(Delete("Users", Key(1)));

  // Read back all rows.
  EXPECT_THAT(ReadAll("Users", {"ID", "Name", "Age"}),
              IsOkAndHoldsRow({2, "Peter", 41}));
}

TEST_F(SingleRowWritesTest, CanDeleteNonExistentRow) {
  // Deletes are idempotent - we do not require the row to exist.
  ZETASQL_EXPECT_OK(Delete("Users", Key(1)));

  // Check that no row was introduced in the process.
  EXPECT_THAT(ReadAll("Users", {"ID", "Name", "Age"}),
              zetasql_base::testing::IsOkAndHolds(testing::IsEmpty()));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
