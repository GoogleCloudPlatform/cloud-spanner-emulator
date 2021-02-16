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

class RangeDeleteTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    ZETASQL_RETURN_IF_ERROR(SetSchema({
        R"(
        CREATE TABLE Users (
          UserId    INT64 NOT NULL,
          Name      STRING(MAX),
        ) PRIMARY KEY (UserId)
      )",
        R"(
        CREATE TABLE Threads (
          UserId    INT64 NOT NULL,
          ThreadId  INT64 NOT NULL,
          Starred   BOOL
        ) PRIMARY KEY (UserId, ThreadId),
        INTERLEAVE IN PARENT Users ON DELETE CASCADE
      )"}));
    return absl::OkStatus();
  }

 protected:
  void PopulateDatabase() {
    // Write fixure data to use in deletes.
    ZETASQL_EXPECT_OK(MultiInsert(
        "Users", {"UserId", "Name"},
        {{1, "Douglas Adams"}, {2, "Suzanne Collins"}, {3, "J.R.R. Tolkien"}}));

    ZETASQL_EXPECT_OK(MultiInsert("Threads", {"UserId", "ThreadId", "Starred"},
                          {{1, 1, true},
                           {1, 2, true},
                           {1, 3, true},
                           {1, 4, false},
                           {2, 1, false},
                           {2, 2, true},
                           {3, 1, false}}));
  }
};

TEST_F(RangeDeleteTest, CanPointDelete) {
  PopulateDatabase();

  ZETASQL_EXPECT_OK(Delete("Threads", Key(2, 2)));
  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows({{1, 1, true},
                                {1, 2, true},
                                {1, 3, true},
                                {1, 4, false},
                                {2, 1, false},
                                {3, 1, false}}));
}

TEST_F(RangeDeleteTest, CanDeleteKeysInSpecifiedRange) {
  PopulateDatabase();

  ZETASQL_EXPECT_OK(Delete("Threads", ClosedOpen(Key(1, 1), Key(1, 4))));
  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows(
                  {{1, 4, false}, {2, 1, false}, {2, 2, true}, {3, 1, false}}));
}

TEST_F(RangeDeleteTest, DeleteRangeOnEmptyTableIsNoOp) {
  ZETASQL_EXPECT_OK(Delete("Threads", OpenOpen(Key(1, 2), Key(1, 1))));
}

TEST_F(RangeDeleteTest, DeleteEmptyRangeIsNoOp) {
  PopulateDatabase();

  ZETASQL_EXPECT_OK(Delete("Threads", OpenOpen(Key(1, 2), Key(1, 1))));
  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows({{1, 1, true},
                                {1, 2, true},
                                {1, 3, true},
                                {1, 4, false},
                                {2, 1, false},
                                {2, 2, true},
                                {3, 1, false}}));
}

TEST_F(RangeDeleteTest, CanDeleteClosedOpenRange) {
  PopulateDatabase();

  ZETASQL_EXPECT_OK(Delete("Threads", ClosedOpen(Key(1), Key(2))));
  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows({{2, 1, false}, {2, 2, true}, {3, 1, false}}));
}

TEST_F(RangeDeleteTest, CanDeleteClosedClosedRange) {
  PopulateDatabase();

  ZETASQL_EXPECT_OK(Delete("Threads", ClosedClosed(Key(1), Key(2))));
  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows({{3, 1, false}}));
}

TEST_F(RangeDeleteTest, CanDeleteOpenOpenRange) {
  PopulateDatabase();

  ZETASQL_EXPECT_OK(Delete("Threads", OpenOpen(Key(1), Key(3))));
  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows({{1, 1, true},
                                {1, 2, true},
                                {1, 3, true},
                                {1, 4, false},
                                {3, 1, false}}));
}

TEST_F(RangeDeleteTest, CanDeleteOpenClosedRange) {
  PopulateDatabase();

  ZETASQL_EXPECT_OK(Delete("Threads", OpenClosed(Key(1), Key(2))));
  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows({{1, 1, true},
                                {1, 2, true},
                                {1, 3, true},
                                {1, 4, false},
                                {3, 1, false}}));
}

TEST_F(RangeDeleteTest, CanDeletePrefixRange) {
  PopulateDatabase();

  ZETASQL_EXPECT_OK(Delete("Threads", ClosedClosed(Key(2), Key(2))));
  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows({{1, 1, true},
                                {1, 2, true},
                                {1, 3, true},
                                {1, 4, false},
                                {3, 1, false}}));
}

TEST_F(RangeDeleteTest, DeleteOpenPrefixRangeIsNoOp) {
  PopulateDatabase();

  ZETASQL_EXPECT_OK(Delete("Threads", OpenOpen(Key(1), Key(1))));
  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows({{1, 1, true},
                                {1, 2, true},
                                {1, 3, true},
                                {1, 4, false},
                                {2, 1, false},
                                {2, 2, true},
                                {3, 1, false}}));
}

TEST_F(RangeDeleteTest, CanDeletePrefixRangeWithDifferentKeySizes) {
  PopulateDatabase();

  ZETASQL_EXPECT_OK(Delete("Threads", ClosedClosed(Key(1, 1), Key(1))));
  EXPECT_THAT(ReadAll("Threads", {"UserId", "ThreadId", "Starred"}),
              IsOkAndHoldsRows({{2, 1, false}, {2, 2, true}, {3, 1, false}}));
}

TEST_F(RangeDeleteTest, CannotDeleteInvalidPrefixRange) {
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
