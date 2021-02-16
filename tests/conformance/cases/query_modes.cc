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

#include "google/spanner/v1/query_plan.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "tests/conformance/common/database_test_base.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using zetasql_base::testing::StatusIs;

class QueryModesTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    ZETASQL_RETURN_IF_ERROR(SetSchema({R"(
      CREATE TABLE Users(
        ID   INT64 NOT NULL,
        Name STRING(MAX),
        Age  INT64
      ) PRIMARY KEY (ID)
    )"}));

    ZETASQL_RETURN_IF_ERROR(Insert("Users", {"ID", "Name"}, {1, "John"}).status());
    ZETASQL_RETURN_IF_ERROR(Insert("Users", {"ID", "Name"}, {2, "Peter"}).status());

    return absl::OkStatus();
  }
};

TEST_F(QueryModesTest, RejectsQueriesInPlanMode) {
  // The emulator does not support query plans, but prod does.
  EXPECT_THAT(ToUtilStatusOr(client().AnalyzeSql(
                  Transaction(Transaction::ReadOnlyOptions()),
                  SqlStatement("select * from Users"))),
              StatusIs(in_prod_env() ? absl::StatusCode::kOk
                                     : absl::StatusCode::kUnimplemented));
}

TEST_F(QueryModesTest, ProvidesBasicStatsInProfileMode) {
  // The emulator supports basic profile stats to allow some sql shells to work.
  auto stats = client()
                   .ProfileQuery(SqlStatement("select * from Users"))
                   .ExecutionStats();
  ASSERT_TRUE(stats.has_value());
  EXPECT_EQ("2", stats.value()["rows_returned"]);
  EXPECT_EQ(1, stats.value().count("elapsed_time"));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
