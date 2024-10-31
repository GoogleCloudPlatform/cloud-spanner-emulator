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
#include <vector>

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

class PGSynonymsTest : public DatabaseTest {
 public:
  PGSynonymsTest()
      : feature_flags_({
            .enable_postgresql_interface = true,
        }) {}
  void SetUp() override {
    dialect_ = database_api::DatabaseDialect::POSTGRESQL;
    DatabaseTest::SetUp();
  }

  absl::Status SetUpDatabase() override { return absl::OkStatus(); }

 private:
  test::ScopedEmulatorFeatureFlagsSetter feature_flags_;
};

TEST_F(PGSynonymsTest, CreateTableWithSynonym) {
  ZETASQL_ASSERT_OK(UpdateSchema(
      {R"(CREATE TABLE t (col1 BIGINT PRIMARY KEY, col2 BIGINT, SYNONYM (syn)))"}));
  ZETASQL_ASSERT_OK(Insert("t", {"col1", "col2"}, {1, 2}));
  EXPECT_THAT(Query("SELECT * FROM syn"), IsOkAndHoldsUnorderedRows({{1, 2}}));
}

TEST_F(PGSynonymsTest, AddSynonym) {
  ZETASQL_ASSERT_OK(
      UpdateSchema({R"(CREATE TABLE t (col1 BIGINT PRIMARY KEY, col2 BIGINT))",
                    R"(ALTER TABLE t ADD SYNONYM syn)"}));
  ZETASQL_ASSERT_OK(Insert("t", {"col1", "col2"}, {1, 2}));
  EXPECT_THAT(Query("SELECT * FROM syn"), IsOkAndHoldsUnorderedRows({{1, 2}}));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
