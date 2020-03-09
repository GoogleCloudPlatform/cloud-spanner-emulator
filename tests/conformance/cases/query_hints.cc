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

#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

class QueryHintsTest : public DatabaseTest {
 public:
  zetasql_base::Status SetUpDatabase() override {
    return SetSchema({
        R"(
          CREATE TABLE Users(
            ID   INT64 NOT NULL,
            Name STRING(MAX),
            Age  INT64
          ) PRIMARY KEY (ID)
        )",
        "CREATE INDEX UsersByName ON Users(Name)"});
  }
};

TEST_F(QueryHintsTest, QueryWithStatementHints) {
  ZETASQL_EXPECT_OK(Query("SELECT ID, Name, Age FROM Users@{force_index=UsersByName}"));
  EXPECT_THAT(
      Query("@{force_index=UsersByName} SELECT ID, Name, Age FROM Users"),
      ::zetasql_base::testing::StatusIs(zetasql_base::StatusCode::kInvalidArgument));
  ZETASQL_EXPECT_OK(
      Query("@{force_index=_BASE_TABLE} SELECT ID, Name, Age FROM Users"));
  ZETASQL_EXPECT_OK(Query("@{join_method=hash_join} SELECT ID, Name, Age FROM Users"));
  ZETASQL_EXPECT_OK(Query("@{join_method=apply_join} SELECT ID, Name, Age FROM Users"));
  ZETASQL_EXPECT_OK(Query("@{join_method=loop_join} SELECT ID, Name, Age FROM Users"));
  ZETASQL_EXPECT_OK(Query("@{join_type=hash_join} SELECT ID, Name, Age FROM Users"));
  ZETASQL_EXPECT_OK(Query("@{join_type=apply_join} SELECT ID, Name, Age FROM Users"));
  ZETASQL_EXPECT_OK(Query("@{join_type=loop_join} SELECT ID, Name, Age FROM Users"));
  EXPECT_THAT(
      Query(
          "@{hash_join_build_side=build_left} SELECT ID, Name, Age FROM Users"),
      ::zetasql_base::testing::StatusIs(zetasql_base::StatusCode::kInvalidArgument));
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

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
