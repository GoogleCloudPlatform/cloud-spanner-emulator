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

class SingletonTableTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    return SetSchema({R"(
      CREATE TABLE Singleton(
        col1 STRING(MAX),
        col2 STRING(MAX)
      ) PRIMARY KEY ()
    )"});
  }
};

TEST_F(SingletonTableTest, CanCreateRowWithEmptyKeysAndValues) {
  // It is ok not to specify any keys or values for a nullable singleton table.
  ZETASQL_EXPECT_OK(Insert("Singleton", {}, {}));
}

TEST_F(SingletonTableTest, CanCreateSingletonRow) {
  // Insert a row in the singleton table (no key is specified).
  ZETASQL_EXPECT_OK(Insert("Singleton", {"col1", "col2"}, {"val1", "val2"}));

  // Check that the row exists.
  EXPECT_THAT(Read("Singleton", {"col1", "col2"}, KeySet::All()),
              IsOkAndHoldsRows({{"val1", "val2"}}));
}

TEST_F(SingletonTableTest, CannotInsertMultipleRows) {
  // Check that we cannot do a double insert in a singleton table.
  ZETASQL_EXPECT_OK(Insert("Singleton", {"col1", "col2"}, {"val1", "val2"}));
  EXPECT_THAT(Insert("Singleton", {"col1", "col2"}, {"val3", "val4"}),
              StatusIs(absl::StatusCode::kAlreadyExists));
}

TEST_F(SingletonTableTest, CanUpdateMultipleRows) {
  // Check that we can do an update in a singleton table.
  ZETASQL_EXPECT_OK(Insert("Singleton", {"col1", "col2"}, {"val1", "val2"}));
  ZETASQL_EXPECT_OK(Update("Singleton", {"col1", "col2"}, {"val3", "val4"}));
  ZETASQL_EXPECT_OK(Update("Singleton", {"col1", "col2"}, {"val5", "val6"}));

  // Check that the row exists.
  EXPECT_THAT(ReadAll("Singleton", {"col1", "col2"}),
              IsOkAndHoldsRows({{"val5", "val6"}}));
}

TEST_F(SingletonTableTest, CanInsertOrUpdateMultipleRows) {
  // Check that we can do an insert_or_update in a singleton table.
  ZETASQL_EXPECT_OK(Insert("Singleton", {"col1", "col2"}, {"val1", "val2"}));
  ZETASQL_EXPECT_OK(InsertOrUpdate("Singleton", {"col1", "col2"}, {"val3", "val4"}));
  ZETASQL_EXPECT_OK(InsertOrUpdate("Singleton", {"col1", "col2"}, {"val5", "val6"}));

  // Check that the row exists.
  EXPECT_THAT(ReadAll("Singleton", {"col1", "col2"}),
              IsOkAndHoldsRows({{"val5", "val6"}}));
}

TEST_F(SingletonTableTest, CanReplaceRows) {
  // Check that we can do a replace in a singleton table.
  ZETASQL_EXPECT_OK(Insert("Singleton", {"col1", "col2"}, {"val1", "val2"}));
  ZETASQL_EXPECT_OK(Replace("Singleton", {"col1", "col2"}, {"val3", "val4"}));

  // Check that the row exists.
  EXPECT_THAT(ReadAll("Singleton", {"col1", "col2"}),
              IsOkAndHoldsRows({{"val3", "val4"}}));
}

TEST_F(SingletonTableTest, CanDeleteWithEmptyKey) {
  // Check that we can delete the singleton table row.
  ZETASQL_EXPECT_OK(Insert("Singleton", {"col1", "col2"}, {"val1", "val2"}));
  ZETASQL_EXPECT_OK(Delete("Singleton", Key()));
  EXPECT_THAT(Read("Singleton", {"col1", "col2"}, KeySet::All()),
              zetasql_base::testing::IsOkAndHolds(testing::IsEmpty()));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
