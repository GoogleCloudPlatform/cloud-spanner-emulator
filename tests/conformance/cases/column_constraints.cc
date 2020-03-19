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
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using zetasql_base::testing::StatusIs;

class ColumnConstraintsTest : public DatabaseTest {
 public:
  zetasql_base::Status SetUpDatabase() override {
    return SetSchema({R"(
      CREATE TABLE TestTable(
        ID1         INT64 NOT NULL,
        StringCol   STRING(5) NOT NULL,
        BytesCol    BYTES(30),
      ) PRIMARY KEY (ID1)
    )"});
  }
};

TEST_F(ColumnConstraintsTest, CannotInsertNullValueIntoTableWithNonNullColumn) {
  // StringCol is missing, so a Null value will be inserted instead.
  EXPECT_THAT(Insert("TestTable", {"ID1"}, {1}),
              StatusIs(zetasql_base::StatusCode::kFailedPrecondition));
  EXPECT_THAT(
      Insert("TestTable", {"ID1", "StringCol"}, {2, Null<std::string>()}),
      StatusIs(zetasql_base::StatusCode::kFailedPrecondition));
}

TEST_F(ColumnConstraintsTest, CanUpdateRowInTableWithNonNullColumn) {
  ZETASQL_EXPECT_OK(Insert("TestTable", {"ID1", "StringCol"}, {1, "value"}));
  // Non-null StringCol is missing from update, but it already exists so this
  // should succeed.
  ZETASQL_EXPECT_OK(Update("TestTable", {"ID1", "BytesCol"}, {1, Bytes("1234")}));
}

TEST_F(ColumnConstraintsTest, SizeEnforcementHappensOnUTFCharactersForStrings) {
  // This is 5 UTF characters that are 4 bytes each.
  std::array<unsigned char, 20> utf_chars = {
      0xF0, 0x9F, 0x80, 0xA1, 0xF0, 0x9F, 0x81, 0xA2, 0xF0, 0x9F,
      0x82, 0xA3, 0xF0, 0x9F, 0x83, 0xA4, 0xF0, 0x9F, 0x84, 0xA5};
  ZETASQL_EXPECT_OK(Insert("TestTable", {"ID1", "StringCol"},
                   {1, std::string(utf_chars.begin(), utf_chars.end())}));
}

TEST_F(ColumnConstraintsTest, CannotInsertDuplicateColumns) {
  EXPECT_THAT(Insert("TestTable", {"ID1", "ID1", "StringCol"}, {1, 1, "value"}),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
  EXPECT_THAT(Insert("TestTable", {"ID1", "StringCol", "StringCol"},
                     {1, "value", "new-value"}),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
