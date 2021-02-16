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

class CaseSensitivityTest : public DatabaseTest {
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

TEST_F(CaseSensitivityTest, NamesAreCaseInsensitive) {
  // Insert a few rows, with random case changes in table and column names.
  ZETASQL_EXPECT_OK(Insert("UserS", {"Id", "NamE"}, {1, "John"}));
  ZETASQL_EXPECT_OK(Insert("UsErs", {"iD", "NAme"}, {2, "Peter"}));

  // Read back all rows, with random case changes in table and column names.
  EXPECT_THAT(ReadAll("users", {"id", "NaMe"}),
              IsOkAndHoldsRows({{1, "John"}, {2, "Peter"}}));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
