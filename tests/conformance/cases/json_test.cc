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

#include "google/cloud/spanner/json.h"

#include <string>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "google/cloud/spanner/value.h"
#include "tests/conformance/common/database_test_base.h"

namespace google::spanner::emulator::test {

namespace {

class JsonTest : public DatabaseTest {
 public:
  void SetUp() override {
    dialect_ = database_api::DatabaseDialect::GOOGLE_STANDARD_SQL;
    DatabaseTest::SetUp();
  }

  absl::Status SetUpDatabase() override {
    return SetSchema(
        {"CREATE TABLE JsonTable ("
         "  id   INT64 NOT NULL,"
         "  col  JSON"
         ") PRIMARY KEY (id)"});
  }
};

TEST_F(JsonTest, JsonComparisons) {
  if (in_prod_env()) {
    GTEST_SKIP() << "JSON comparison not supported in production Spanner yet";
  }

  GOOGLESQL_EXPECT_OK(Insert("JsonTable", {"id", "col"},
                   {1, cloud::spanner::Json("{\"a\": 1}")}));
  GOOGLESQL_EXPECT_OK(Insert("JsonTable", {"id", "col"},
                   {2, cloud::spanner::Json("{\"b\": 2}")}));
  GOOGLESQL_EXPECT_OK(
      Insert("JsonTable", {"id", "col"}, {3, cloud::spanner::Json("null")}));
  GOOGLESQL_EXPECT_OK(Insert("JsonTable", {"id"}, {4}));

  // Equality
  EXPECT_THAT(Query("SELECT id FROM JsonTable WHERE col = JSON '{\"a\": 1}'"),
              IsOkAndHoldsRow({1}));

  // Inequality
  EXPECT_THAT(Query("SELECT id FROM JsonTable WHERE col != JSON '{\"a\": 1}' "
                    "ORDER BY id"),
              IsOkAndHoldsRows({{2}, {3}}));

  // Less than
  EXPECT_THAT(
      Query(
          "SELECT id FROM JsonTable WHERE col < JSON '{\"b\": 2}' ORDER BY id"),
      IsOkAndHoldsRows({{1}, {3}}));

  // Less than or equal to
  EXPECT_THAT(Query("SELECT id FROM JsonTable WHERE col <= JSON '{\"b\": 2}' "
                    "ORDER BY id"),
              IsOkAndHoldsRows({{1}, {2}, {3}}));

  // Greater than
  EXPECT_THAT(
      Query(
          "SELECT id FROM JsonTable WHERE col > JSON '{\"a\": 1}' ORDER BY id"),
      IsOkAndHoldsRows({{2}}));

  // Greater than or equal to
  EXPECT_THAT(Query("SELECT id FROM JsonTable WHERE col >= JSON '{\"b\": 2}' "
                    "ORDER BY id"),
              IsOkAndHoldsRows({{2}}));

  // Ordering / Min Max
  EXPECT_THAT(Query("SELECT MIN(col) FROM JsonTable"),
              IsOkAndHoldsRow({cloud::spanner::Json("null")}));
  EXPECT_THAT(Query("SELECT MAX(col) FROM JsonTable"),
              IsOkAndHoldsRow({cloud::spanner::Json("{\"b\":2}")}));
  EXPECT_THAT(Query("SELECT id FROM JsonTable ORDER BY col"),
              IsOkAndHoldsRows({{4}, {3}, {1}, {2}}));

  // Group by
  EXPECT_THAT(
      Query("SELECT col, count(*) FROM JsonTable GROUP BY col ORDER BY col"),
      IsOkAndHoldsRows(
          {{cloud::spanner::MakeNullValue<cloud::spanner::Json>(), 1},
           {cloud::spanner::Json("null"), 1},
           {cloud::spanner::Json("{\"a\":1}"), 1},
           {cloud::spanner::Json("{\"b\":2}"), 1}}));

  // Join
  EXPECT_THAT(Query("SELECT t1.id FROM JsonTable t1 JOIN JsonTable t2 ON "
                    "t1.col = t2.col WHERE t1.id = 1"),
              IsOkAndHoldsRow({1}));

  // Cast
  EXPECT_THAT(Query("SELECT CAST('{\"c\": 3}' AS JSON)"),
              IsOkAndHoldsRow({cloud::spanner::Json("\"{\\\"c\\\": 3}\"")}));
  EXPECT_THAT(Query("SELECT JSON '{\"c\": 3}' = '{\"c\": 3}'"),
              IsOkAndHoldsRow({false}));

  // Comparison with non-json (e.g., int)
  EXPECT_THAT(Query("SELECT id FROM JsonTable WHERE col = 3"),
              IsOkAndHoldsRows({}));
}

}  // namespace
}  // namespace google::spanner::emulator::test
