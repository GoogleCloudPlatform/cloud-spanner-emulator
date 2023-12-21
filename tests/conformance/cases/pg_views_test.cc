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

#include "google/spanner/admin/database/v1/common.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

class PGViewsTest : public DatabaseTest {
 public:
  PGViewsTest() : feature_flags_({.enable_postgresql_interface = true}) {}

  void SetUp() override {
    dialect_ = database_api::DatabaseDialect::POSTGRESQL;
    DatabaseTest::SetUp();
  }

  absl::Status SetUpDatabase() override {
    return SetSchemaFromFile("query.test");
  }

 private:
  test::ScopedEmulatorFeatureFlagsSetter feature_flags_;
};

TEST_F(PGViewsTest, PGOrderBy) {
  ZETASQL_ASSERT_OK(UpdateSchema({
      R"(
        CREATE TABLE T (
          K bigint primary key,
          V bigint
        )
      )",
  }));
  ZETASQL_ASSERT_OK(Insert("T", {"K"}, {1}));
  ZETASQL_ASSERT_OK(Insert("T", {"K", "V"}, {3, 4}));
  ZETASQL_ASSERT_OK(Insert("T", {"K", "V"}, {5, 6}));
  ZETASQL_ASSERT_OK(Insert("T", {"K", "V"}, {7, 8}));

  ZETASQL_ASSERT_OK(UpdateSchema({
      "CREATE VIEW v_asc SQL SECURITY INVOKER AS SELECT T.K FROM T ORDER BY "
      "T.V",
  }));
  ZETASQL_ASSERT_OK(UpdateSchema({
      "CREATE VIEW v_desc SQL SECURITY INVOKER AS SELECT T.K FROM T ORDER BY "
      "T.V DESC",
  }));

  EXPECT_THAT(Query("SELECT T.K FROM T ORDER BY T.V"),
              IsOkAndHoldsRows({{3}, {5}, {7}, {1}}));
  EXPECT_THAT(Query("SELECT T.K FROM T ORDER BY T.V DESC"),
              IsOkAndHoldsRows({{1}, {7}, {5}, {3}}));

  // We can only check the rows but not the ordering since the returned ordering
  // is not guaranteed by selecting all rows from a view.
  EXPECT_THAT(Query("SELECT * FROM v_asc"), IsOkAndHoldsUnorderedRows({
                                                {3},
                                                {5},
                                                {7},
                                                {1},
                                            }));
  EXPECT_THAT(Query("SELECT * FROM v_desc"), IsOkAndHoldsUnorderedRows({
                                                 {1},
                                                 {7},
                                                 {5},
                                                 {3},
                                             }));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
