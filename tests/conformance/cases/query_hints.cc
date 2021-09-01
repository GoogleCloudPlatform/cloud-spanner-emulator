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

class QueryHintsTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    return SetSchema({
        R"(
          CREATE TABLE Users(
            ID   INT64 NOT NULL,
            Name STRING(MAX),
            Age  INT64
          ) PRIMARY KEY (ID)
        )",
        "CREATE INDEX UsersByName ON Users(Name)",
        R"(
          CREATE TABLE Messages (
            UserId     INT64 NOT NULL,
            ThreadId   INT64 NOT NULL,
            MessageId  INT64 NOT NULL,
            Subject    STRING(MAX),
          ) PRIMARY KEY (UserId, ThreadId, MessageId)
        )",
        "CREATE INDEX MessagesBySubject ON Messages(Subject)",
        "CREATE NULL_FILTERED INDEX UsersByAge ON Users(Age)",
        "CREATE NULL_FILTERED INDEX MessagesByIdAndThread ON "
        "Messages(MessageId, ThreadId)",
        "CREATE NULL_FILTERED INDEX MessagesByIdAndSubject ON "
        "Messages(MessageId, Subject)",
    });
  }
};

TEST_F(QueryHintsTest, QueryWithStatementHints) {
  ZETASQL_EXPECT_OK(Query("SELECT ID, Name, Age FROM Users@{force_index=UsersByName}"));
  EXPECT_THAT(
      Query("@{force_index=UsersByName} SELECT ID, Name, Age FROM Users"),
      StatusIs(absl::StatusCode::kInvalidArgument));
  ZETASQL_EXPECT_OK(
      Query("@{force_index=_BASE_TABLE} SELECT ID, Name, Age FROM Users"));
  ZETASQL_EXPECT_OK(Query("@{join_method=merge_join} SELECT ID, Name, Age FROM Users"));
  ZETASQL_EXPECT_OK(Query("@{join_method=hash_join} SELECT ID, Name, Age FROM Users"));
  ZETASQL_EXPECT_OK(Query("@{join_method=apply_join} SELECT ID, Name, Age FROM Users"));
  ZETASQL_EXPECT_OK(Query("@{join_method=loop_join} SELECT ID, Name, Age FROM Users"));
  ZETASQL_EXPECT_OK(Query("@{join_type=hash_join} SELECT ID, Name, Age FROM Users"));
  ZETASQL_EXPECT_OK(Query("@{join_type=apply_join} SELECT ID, Name, Age FROM Users"));
  ZETASQL_EXPECT_OK(Query("@{join_type=loop_join} SELECT ID, Name, Age FROM Users"));
  EXPECT_THAT(
      Query(
          "@{hash_join_build_side=build_left} SELECT ID, Name, Age FROM Users"),
      StatusIs(absl::StatusCode::kInvalidArgument));
  ZETASQL_EXPECT_OK(
      Query("@{join_type=hash_join,hash_join_build_side=build_right} SELECT "
            "ID, Name, Age FROM Users"));
  ZETASQL_EXPECT_OK(
      Query("SELECT a.ID, b.Name FROM Users a HASH JOIN "
            "@{hash_join_build_side=build_right} Users b "
            "ON false"));
}

TEST_F(QueryHintsTest, QueryWithGroupHints) {
  ZETASQL_EXPECT_OK(
      Query("SELECT 1 FROM Users GROUP @{group_method=hash_group} BY Name"));
  ZETASQL_EXPECT_OK(
      Query("SELECT 1 FROM Users GROUP @{group_method=stream_group} BY Name"));
  ZETASQL_EXPECT_OK(
      Query("SELECT 1 FROM Users GROUP @{group_type=hash_group} BY Name"));
  ZETASQL_EXPECT_OK(
      Query("SELECT 1 FROM Users GROUP @{group_type=stream_group} BY Name"));
}

// Hints for engines other than spanner are ignored.
TEST_F(QueryHintsTest, QueryWithOtherEngineHints) {
  ZETASQL_EXPECT_OK(
      Query("SELECT 1 FROM Users GROUP @{otherengine.something=1} BY Name"));
}

TEST_F(QueryHintsTest, InvalidIndexHintInQueryReturnsError) {
  EXPECT_THAT(Query("SELECT Name FROM Users"
                    "@{force_index=MessagesBySubject}"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(QueryHintsTest, InvalidIndexHintInDMLReturnsError) {
  EXPECT_THAT(Query("INSERT INTO Users(ID, Name) SELECT "
                    "UserId,Subject FROM Messages@{force_index=UsersByName} "),
              StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(
      CommitDml({SqlStatement(
          "UPDATE Users SET Name = 'Peter' "
          "WHERE ID IN "
          "(SELECT MessageId FROM Messages@{force_index=UsersByName})")}),
      StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(QueryHintsTest, InvalidNullFilteredIndexHint) {
  EXPECT_THAT(Query("SELECT ID FROM Users@{force_index=UsersByAge}"),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Null filtered index on (nullable, non-nullable) column.
  EXPECT_THAT(Query("SELECT MessageId FROM Messages"
                    "@{force_index=MessagesByIdAndSubject}"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(QueryHintsTest, ValidNullFilteredIndexHint) {
  ZETASQL_EXPECT_OK(
      Query("SELECT ThreadId FROM "
            "Messages@{force_index=MessagesByIdAndThread}"));
}

TEST_F(QueryHintsTest, DisableNullFilteredIndexCheckForValidQuery) {
  const std::string valid_query =
      "SELECT ID FROM Users@{force_index=UsersByAge} "
      "WHERE Age IS NOT NULL";

  // The query is accepted in prod but rejected by the Emulator as it currently
  // cannot determine nullability from the predicate."
  if (in_prod_env()) {
    ZETASQL_EXPECT_OK(Query(valid_query));
  } else {
    EXPECT_THAT(Query(valid_query),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }

  // With the hint to disable null-filtered checks, the emulator is pacified.
  const std::string modified_valid_query =
      "SELECT ID FROM Users@{force_index=UsersByAge, "
      "spanner_emulator.disable_query_null_filtered_index_check=true} "
      "WHERE Age IS NOT NULL";

  ZETASQL_EXPECT_OK(Query(modified_valid_query));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
