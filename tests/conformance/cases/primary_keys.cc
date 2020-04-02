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

class PrimaryKeysTest : public DatabaseTest {
 public:
  zetasql_base::Status SetUpDatabase() override {
    return SetSchema({R"(
      CREATE TABLE TableWithNullableKey(
        key1 STRING(MAX) NOT NULL,
        key2 STRING(MAX),
        col1 STRING(MAX)
      ) PRIMARY KEY (key1, key2)
    )"});
  }
};

TEST_F(PrimaryKeysTest, CanInsertRowWithMultiPartKey) {
  // Insert a row with a fully-specified key.
  ZETASQL_ASSERT_OK(Insert("TableWithNullableKey", {"key1", "key2", "col1"},
                   {"key1_val", "key2_val", "col1_val"}));

  // Verify that it exists.
  EXPECT_THAT(ReadAll("TableWithNullableKey", {"key1", "key2", "col1"}),
              IsOkAndHoldsRows({{"key1_val", "key2_val", "col1_val"}}));
}

TEST_F(PrimaryKeysTest, CannotInsertWithoutRequiredKeyColumn) {
  // Check that we cannot do an insert if we skip key1 which is required.
  EXPECT_THAT(Insert("TableWithNullableKey", {"key2"}, {"key2_val"}),
              StatusIs(zetasql_base::StatusCode::kFailedPrecondition));
}

TEST_F(PrimaryKeysTest, CanInsertWithNullableKeyColumn) {
  // Insert a row without specifying key2, it should be seen as a NULL.
  ZETASQL_ASSERT_OK(Insert("TableWithNullableKey", {"key1", "col1"},
                   {"key1_val", "col1_val"}));

  // Verify that the row exists with NULL as the value for key2.
  EXPECT_THAT(
      ReadAll("TableWithNullableKey", {"key1", "key2", "col1"}),
      IsOkAndHoldsRows({{"key1_val", Null<std::string>(), "col1_val"}}));
}

TEST_F(PrimaryKeysTest, CanInsertRowWithExplicitNullKeyColumn) {
  // Insert a row with key2 explicitly specified as NULL.
  ZETASQL_ASSERT_OK(Insert("TableWithNullableKey", {"key1", "key2", "col1"},
                   {"key1_val", Null<std::string>(), "col1_val"}));

  // Verify that the row exists with NULL as the value for key2.
  EXPECT_THAT(
      ReadAll("TableWithNullableKey", {"key1", "key2", "col1"}),
      IsOkAndHoldsRows({{"key1_val", Null<std::string>(), "col1_val"}}));
}

TEST_F(PrimaryKeysTest, CannotInsertNullForNotNullKeyColumn) {
  // Try to insert a row with key1 explicitly specified as NULL.
  EXPECT_THAT(Insert("TableWithNullableKey", {"key1", "key2", "col1"},
                     {Null<std::string>(), "key2_val", "col1_val"}),
              StatusIs(zetasql_base::StatusCode::kFailedPrecondition));
}

TEST_F(PrimaryKeysTest, CannotInsertKeyTooLarge) {
  std::string long_str(8192, 'a');
  EXPECT_THAT(
      Insert("TableWithNullableKey", {"key1", "key2"}, {long_str, "abc"}),
      StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
