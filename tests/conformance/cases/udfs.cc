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

#include <cstdint>
#include <memory>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "google/cloud/spanner/value.h"
#include "common/feature_flags.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

class UDFsTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
  absl::Status SetUpDatabase() override {
    feature_flags_setter_ = std::make_unique<ScopedEmulatorFeatureFlagsSetter>(
        EmulatorFeatureFlags::Flags{
            .enable_views = true,
            .enable_user_defined_functions = true,
        });
    return absl::OkStatus();
  }

 public:
  // Aliases so test expectations read more clearly.
  cloud::spanner::Value Nb() { return Null<Bytes>(); }
  cloud::spanner::Value Ns() { return Null<std::string>(); }
  cloud::spanner::Value Ni() { return Null<std::int64_t>(); }

 protected:
  std::unique_ptr<ScopedEmulatorFeatureFlagsSetter> feature_flags_setter_;
};

INSTANTIATE_TEST_SUITE_P(
    UDFsTest, UDFsTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<UDFsTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(UDFsTest, SimpleUdf) {
  ZETASQL_ASSERT_OK(UpdateSchema({
      R"SQL(
      CREATE FUNCTION inc(x INT64) RETURNS INT64 SQL SECURITY INVOKER
      AS (x + 1)
    )SQL"}));
  EXPECT_THAT(Query("SELECT inc(1)"), IsOkAndHoldsUnorderedRows({{2}}));
}

}  // namespace
}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
