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

#include "google/spanner/admin/database/v1/common.pb.h"
#include "google/spanner/v1/query_plan.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "google/cloud/spanner/results.h"
#include "tests/conformance/common/database_test_base.h"
#include "googlesql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using googlesql_base::testing::StatusIs;

class QueryModesTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

 public:
  absl::Status SetUpDatabase() override {
    GOOGLESQL_RETURN_IF_ERROR(SetSchemaFromFile("query_modes.test"));

    GOOGLESQL_RETURN_IF_ERROR(Insert("Users", {"ID", "Name"}, {1, "John"}).status());
    GOOGLESQL_RETURN_IF_ERROR(Insert("Users", {"ID", "Name"}, {2, "Peter"}).status());

    return absl::OkStatus();
  }
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectQueryModesTest, QueryModesTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<QueryModesTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(QueryModesTest, AcceptsQueriesInPlanMode) {
  // The emulator does return the same query plans as prod, but it does support
  // PLAN mode in order to allow clients to execute AnalyzeSql to get the query
  // metadata without having to actually execute the statement. This also allows
  // clients to let the backend infer the query parameters in a statement.
  auto plan = client().AnalyzeSql(Transaction(Transaction::ReadOnlyOptions()),
                                  SqlStatement("select * from Users"));
  ASSERT_TRUE(plan.ok());
  if (!in_prod_env()) {
    EXPECT_EQ(1, plan.value().plan_nodes_size());
  }
}

TEST_P(QueryModesTest, ProvidesBasicStatsInProfileMode) {
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
