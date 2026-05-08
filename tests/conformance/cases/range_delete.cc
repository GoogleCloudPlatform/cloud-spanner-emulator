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

#include <unistd.h>

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

class RangeDeleteTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

 public:
  absl::Status SetUpDatabase() override {
    return SetSchemaFromFile("range_delete.test");
  }

 protected:
  void PopulateDatabase() {
    // Write fixure data to use in deletes.
    GOOGLESQL_EXPECT_OK(MultiInsert(
        "Users", {"UserId", "Name"},
        {{1, "Douglas Adams"}, {2, "Suzanne Collins"}, {3, "J.R.R. Tolkien"}}));

    GOOGLESQL_EXPECT_OK(MultiInsert("Threads", {"UserId", "ThreadId", "Starred"},
                          {{1, 1, true},
                           {1, 2, true},
                           {1, 3, true},
                           {1, 4, false},
                           {2, 1, false},
                           {2, 2, true},
                           {3, 1, false}}));
  }
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectRangeDeleteTest, RangeDeleteTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<RangeDeleteTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(RangeDeleteTest, CanPointDelete) {
  PopulateDatabase();

  GOOGLESQL_EXPECT_OK(Delete("Threads", Key(2, 2)));
  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows({{1, 1, true},
                                {1, 2, true},
                                {1, 3, true},
                                {1, 4, false},
                                {2, 1, false},
                                {3, 1, false}}));
}

TEST_P(RangeDeleteTest, CanDeleteKeysInSpecifiedRange) {
  PopulateDatabase();

  GOOGLESQL_EXPECT_OK(Delete("Threads", ClosedOpen(Key(1, 1), Key(1, 4))));
  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows(
                  {{1, 4, false}, {2, 1, false}, {2, 2, true}, {3, 1, false}}));
}

TEST_P(RangeDeleteTest, DeleteRangeOnEmptyTableIsNoOp) {
  GOOGLESQL_EXPECT_OK(Delete("Threads", OpenOpen(Key(1, 2), Key(1, 1))));
}

TEST_P(RangeDeleteTest, DeleteEmptyRangeIsNoOp) {
  PopulateDatabase();

  GOOGLESQL_EXPECT_OK(Delete("Threads", OpenOpen(Key(1, 2), Key(1, 1))));
  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows({{1, 1, true},
                                {1, 2, true},
                                {1, 3, true},
                                {1, 4, false},
                                {2, 1, false},
                                {2, 2, true},
                                {3, 1, false}}));
}

TEST_P(RangeDeleteTest, CanDeleteClosedOpenRange) {
  PopulateDatabase();

  GOOGLESQL_EXPECT_OK(Delete("Threads", ClosedOpen(Key(1), Key(2))));
  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows({{2, 1, false}, {2, 2, true}, {3, 1, false}}));
}

TEST_P(RangeDeleteTest, CanDeleteClosedClosedRange) {
  PopulateDatabase();

  GOOGLESQL_EXPECT_OK(Delete("Threads", ClosedClosed(Key(1), Key(2))));
  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows({{3, 1, false}}));
}

TEST_P(RangeDeleteTest, CanDeleteOpenOpenRange) {
  PopulateDatabase();

  GOOGLESQL_EXPECT_OK(Delete("Threads", OpenOpen(Key(1), Key(3))));
  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows({{1, 1, true},
                                {1, 2, true},
                                {1, 3, true},
                                {1, 4, false},
                                {3, 1, false}}));
}

TEST_P(RangeDeleteTest, CanDeleteOpenClosedRange) {
  PopulateDatabase();

  GOOGLESQL_EXPECT_OK(Delete("Threads", OpenClosed(Key(1), Key(2))));
  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows({{1, 1, true},
                                {1, 2, true},
                                {1, 3, true},
                                {1, 4, false},
                                {3, 1, false}}));
}

TEST_P(RangeDeleteTest, CanDeletePrefixRange) {
  PopulateDatabase();

  GOOGLESQL_EXPECT_OK(Delete("Threads", ClosedClosed(Key(2), Key(2))));
  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows({{1, 1, true},
                                {1, 2, true},
                                {1, 3, true},
                                {1, 4, false},
                                {3, 1, false}}));
}

TEST_P(RangeDeleteTest, DeleteOpenPrefixRangeIsNoOp) {
  PopulateDatabase();

  GOOGLESQL_EXPECT_OK(Delete("Threads", OpenOpen(Key(1), Key(1))));
  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows({{1, 1, true},
                                {1, 2, true},
                                {1, 3, true},
                                {1, 4, false},
                                {2, 1, false},
                                {2, 2, true},
                                {3, 1, false}}));
}

TEST_P(RangeDeleteTest, CanDeletePrefixRangeWithDifferentKeySizes) {
  PopulateDatabase();

  GOOGLESQL_EXPECT_OK(Delete("Threads", ClosedClosed(Key(1, 1), Key(1))));
  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows({{2, 1, false}, {2, 2, true}, {3, 1, false}}));
}

TEST_P(RangeDeleteTest, CannotDeleteInvalidPrefixRange) {
  PopulateDatabase();

  EXPECT_THAT(Delete("Threads", ClosedClosed(Key(1, 3), Key(2, 2))),
              StatusIs(absl::StatusCode::kUnimplemented));

  EXPECT_THAT(Delete("Threads", ClosedClosed(Key(1, 2), Key(2, 1))),
              StatusIs(absl::StatusCode::kUnimplemented));

  EXPECT_THAT(Delete("Threads", ClosedClosed(Key(1, 2), Key(2, 1))),
              StatusIs(absl::StatusCode::kUnimplemented));

  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows({{1, 1, true},
                                {1, 2, true},
                                {1, 3, true},
                                {1, 4, false},
                                {2, 1, false},
                                {2, 2, true},
                                {3, 1, false}}));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
