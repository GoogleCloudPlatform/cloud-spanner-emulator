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

#include <string>
#include <tuple>
#include <utility>
#include <vector>

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

class MalformedWritesTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

 public:
  absl::Status SetUpDatabase() override {
    return SetSchemaFromFile("malformed_writes.test");
  }
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectMalformedWritesTest, MalformedWritesTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<MalformedWritesTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(MalformedWritesTest, CannotSpecifySameColumnMultipleTimes) {
  // Cannot specify a key column multiple times in a write.
  EXPECT_THAT(Insert("Users", {"UserId", "UserId", "Name"},
                     {"1", "2", "Suzanne Collins"}),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(Insert("Users", {"UserId", "UserId"}, {"1", "2"}),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Cannot specify a key column multiple times even if set to the same value.
  EXPECT_THAT(Insert("Users", {"UserId", "UserId"}, {"1", "1"}),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Key column names are case-insensitive and thus cannot specify same column
  // specified with different case either.
  EXPECT_THAT(Insert("Users", {"uSERiD", "UserId"}, {"1", "2"}),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Cannot specify a non-key column multiple times in a write.
  EXPECT_THAT(Insert("Users", {"UserId", "Name", "Name"},
                     {"1", "Douglas Adams", "Suzanne Collins"}),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Cannot specify a column multiple times even if set to the same value.
  EXPECT_THAT(Insert("Users", {"UserId", "Name", "Name"},
                     {"1", "Douglas Adams", "Douglas Adams"}),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Column names are case-insensitive and thus cannot specify same column
  // specified with different case either.
  EXPECT_THAT(Insert("Users", {"UserId", "Name", "nAME"},
                     {"1", "Douglas Adams", "Douglas Adams"}),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_P(MalformedWritesTest, CannotInsertValueWithMismatchingType) {
  // Insert with all types match is successful.
  GOOGLESQL_EXPECT_OK(Insert("Users", {"UserId", "Name", "Age", "Updated"},
                   {"1", "Douglas Adams", 27, MakeNowTimestamp()}));

  // Type for int key does not match.
  EXPECT_THAT(Insert("Users", {"UserId", "Name", "Age"},
                     {MakeNowTimestamp(), "Suzanne Collins", 27}),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Type for int column does not match.
  EXPECT_THAT(Insert("Users", {"UserId", "Name", "Age"},
                     {"1", "Suzanne Collins", MakeNowTimestamp()}),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Type for timestamp column does not match.
  EXPECT_THAT(Insert("Users", {"UserId", "Name", "Updated"},
                     {"1", "Suzanne Collins", 27}),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_P(MalformedWritesTest, CannotHaveDifferentNumberOfColumnsAndValues) {
  EXPECT_THAT(
      Insert("Users", {"UserId", "Name", "Updated"}, {"1", "Douglas Adams"}),
      StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_P(MalformedWritesTest, CannotSpecifyPartialKey) {
  GOOGLESQL_EXPECT_OK(Insert("Users", {"UserId", "Name"}, {1, "Douglas Adams"}));
  GOOGLESQL_EXPECT_OK(Insert("Threads", {"UserId", "ThreadId", "Starred"}, {1, 1, true}));

  // Can perform a successful read by specifying the full primary key.
  EXPECT_THAT(Read("Threads", {"UserId", "ThreadId", "Starred"}, Key(1, 1)),
              IsOkAndHoldsRow({1, 1, true}));

  // Cannot however read by specifying a prefix of the primary key.
  EXPECT_THAT(Read("Threads", {"UserId", "ThreadId", "Starred"}, Key(1)),
              StatusIs(absl::StatusCode::kFailedPrecondition));

  // Partial key cannot be used to perform writes either.
  EXPECT_THAT(Update("Threads", {"UserId", "Starred"}, {1, false}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_P(MalformedWritesTest, CannotWriteToNonExistentColumn) {
  EXPECT_THAT(Insert("Users", {"bad-key", "Name"}, {1, "Douglas Adams"}),
              StatusIs(absl::StatusCode::kNotFound));

  EXPECT_THAT(Insert("Users", {"UserId", "bad-column"}, {1, "Douglas Adams"}),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_P(MalformedWritesTest, CannotWriteToEmptyTableName) {
  // Check that empty table name cannot be specified for all mutation types:
  // insert, update, delete and replace.
  EXPECT_THAT(Insert("", {"UserId", "Name"}, {1, "Douglas Adams"}),
              StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(InsertOrUpdate("", {"UserId", "Name"}, {1, "Douglas Adams"}),
              StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(Update("", {"UserId", "Name"}, {1, "Douglas Adams"}),
              StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(Replace("", {"UserId", "Name"}, {1, "Douglas Adams"}),
              StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(Delete("", Key(1, "Douglas Adams")),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_P(MalformedWritesTest, CannotWriteToNonExistentTable) {
  EXPECT_THAT(Insert("bad-table", {"UserId", "Name"}, {1, "Douglas Adams"}),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_P(MalformedWritesTest, CannotWriteStructToStringColumn) {
  // Check that struct type cannot be passed to a string column for all mutation
  // types: insert, update, delete and replace.
  using StructType = std::tuple<std::pair<std::string, std::string>>;
  StructType struct_val{StructType{{"Name", "Douglas Adams"}}};

  EXPECT_THAT(Insert("", {"UserId", "Name"}, {1, struct_val}),
              StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(InsertOrUpdate("", {"UserId", "Name"}, {1, struct_val}),
              StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(Update("", {"UserId", "Name"}, {1, struct_val}),
              StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(Replace("", {"UserId", "Name"}, {1, struct_val}),
              StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(Delete("", Key(1, struct_val)),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_P(MalformedWritesTest, CannotWriteListStructToStringColumn) {
  // Check that list of struct cannot be passed to a string column for all
  // mutation types: insert, update, delete and replace.
  using StructType = std::tuple<std::pair<std::string, std::string>>;
  std::vector<StructType> struct_arr{StructType{{"Name", "Douglas Adams"}},
                                     StructType{{"Name", "Suzanne Collins"}}};

  EXPECT_THAT(Insert("", {"UserId", "Name"}, {1, struct_arr}),
              StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(InsertOrUpdate("", {"UserId", "Name"}, {1, struct_arr}),
              StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(Update("", {"UserId", "Name"}, {1, struct_arr}),
              StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(Replace("", {"UserId", "Name"}, {1, struct_arr}),
              StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(Delete("", Key(1, struct_arr)),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
