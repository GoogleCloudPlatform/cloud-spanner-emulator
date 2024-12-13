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

#include <array>
#include <string>

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

class ColumnConstraintsTest
    : public DatabaseTest,
      public ::testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  absl::Status SetUpDatabase() override {
    return SetSchemaFromFile("column_constraints.test");
  }

  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectColumnConstraintsTests, ColumnConstraintsTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<ColumnConstraintsTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(ColumnConstraintsTest, CannotInsertNullValueIntoTableWithNonNullColumn) {
  // stringcol is missing, so a Null value will be inserted instead.
  EXPECT_THAT(Insert("testtable", {"id1"}, {1}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
  EXPECT_THAT(
      Insert("testtable", {"id1", "stringcol"}, {2, Null<std::string>()}),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_P(ColumnConstraintsTest, CannotInsertOrUpdateRowInTableWithNonNullColumn) {
  ZETASQL_EXPECT_OK(Insert("testtable", {"id1", "stringcol"}, {1, "value"}));
  // NOT NULL stringcol is missing from InsertOrUpdate, and this will error.
  EXPECT_THAT(
      InsertOrUpdate("testtable", {"id1", "bytescol"}, {1, Bytes("1234")}),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_P(ColumnConstraintsTest, CannotReplaceRowInTableWithNonNullColumn) {
  ZETASQL_EXPECT_OK(Insert("testtable", {"id1", "stringcol"}, {1, "value"}));
  // NOT NULL stringcol is missing from Replace, and this will error.
  EXPECT_THAT(Replace("testtable", {"id1", "bytescol"}, {1, Bytes("1234")}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_P(ColumnConstraintsTest, CanUpdateRowInTableWithNonNullColumn) {
  ZETASQL_EXPECT_OK(Insert("testtable", {"id1", "stringcol"}, {1, "value"}));
  // NOT NULL stringcol is missing from update, but it already exists so this
  // should succeed.
  ZETASQL_EXPECT_OK(Update("testtable", {"id1", "bytescol"}, {1, Bytes("1234")}));
}

TEST_P(ColumnConstraintsTest, SizeEnforcementHappensOnUTFCharactersForStrings) {
  // This is 5 UTF characters that are 4 bytes each.
  std::array<unsigned char, 20> utf_chars = {
      0xF0, 0x9F, 0x80, 0xA1, 0xF0, 0x9F, 0x81, 0xA2, 0xF0, 0x9F,
      0x82, 0xA3, 0xF0, 0x9F, 0x83, 0xA4, 0xF0, 0x9F, 0x84, 0xA5};
  ZETASQL_EXPECT_OK(Insert("testtable", {"id1", "stringcol"},
                   {1, std::string(utf_chars.begin(), utf_chars.end())}));
}

TEST_P(ColumnConstraintsTest, CannotInsertDuplicateColumns) {
  EXPECT_THAT(Insert("testtable", {"id1", "id1", "stringcol"}, {1, 1, "value"}),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(Insert("testtable", {"id1", "stringcol", "stringcol"},
                     {1, "value", "new-value"}),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
