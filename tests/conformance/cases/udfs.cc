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
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
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

using ::testing::HasSubstr;
using ::googlesql_base::testing::StatusIs;

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
  GOOGLESQL_ASSERT_OK(UpdateSchema({
      R"SQL(
      CREATE FUNCTION inc_udf(x INT64) RETURNS INT64
      AS (x + 1)
    )SQL"}));
  EXPECT_THAT(Query("SELECT inc_udf(1)"), IsOkAndHoldsUnorderedRows({{2}}));
}

TEST_P(UDFsTest, UdfWithInvokerSecurity) {
  GOOGLESQL_ASSERT_OK(UpdateSchema({
      R"SQL(
      CREATE FUNCTION get_table_name() RETURNS STRING SQL SECURITY INVOKER
      AS ("test")
    )SQL"}));
  EXPECT_THAT(Query("SELECT get_table_name()"),
              IsOkAndHoldsUnorderedRows({{"test"}}));
}

TEST_P(UDFsTest, UdfNullSemantics) {
  GOOGLESQL_ASSERT_OK(UpdateSchema({
      R"SQL(
      CREATE FUNCTION inc_udf(x INT64) RETURNS INT64 AS (x + 1)
    )SQL"}));
  EXPECT_THAT(Query("SELECT inc_udf(NULL)"),
              IsOkAndHoldsUnorderedRows({{Ni()}}));
}

TEST_P(UDFsTest, UdfVariousTypes) {
  GOOGLESQL_ASSERT_OK(UpdateSchema({
      R"SQL(
      CREATE FUNCTION my_lower(s STRING) RETURNS STRING AS (LOWER(s))
    )SQL",
      R"SQL(
      CREATE FUNCTION my_not(b BOOL) RETURNS BOOL AS (NOT b)
    )SQL"}));
  EXPECT_THAT(Query("SELECT my_lower('HI'), my_not(TRUE)"),
              IsOkAndHoldsUnorderedRows({{"hi", false}}));
  EXPECT_THAT(Query("SELECT my_lower(NULL), my_not(NULL)"),
              IsOkAndHoldsUnorderedRows({{Ns(), Null<bool>()}}));
}

TEST_P(UDFsTest, UdfMultipleDefinitions) {
  GOOGLESQL_ASSERT_OK(UpdateSchema({
      R"SQL(
      CREATE FUNCTION inc_udf(x INT64) RETURNS INT64 AS (x + 1)
    )SQL",
      R"SQL(
      CREATE FUNCTION dec_udf(x INT64) RETURNS INT64 AS (x - 1)
    )SQL"}));
  EXPECT_THAT(Query("SELECT inc_udf(1), dec_udf(1)"),
              IsOkAndHoldsUnorderedRows({{2, 0}}));
}

TEST_P(UDFsTest, UdfCallingOtherUdf) {
  GOOGLESQL_ASSERT_OK(UpdateSchema({
      R"SQL(
      CREATE FUNCTION inc_udf(x INT64) RETURNS INT64 AS (x + 1)
    )SQL",
      R"SQL(
      CREATE FUNCTION inc_udf2(x INT64) RETURNS INT64 AS (inc_udf(inc_udf(x)))
    )SQL"}));
  EXPECT_THAT(Query("SELECT inc_udf2(1)"), IsOkAndHoldsUnorderedRows({{3}}));
}

TEST_P(UDFsTest, UdfWithQueryExpression) {
  GOOGLESQL_ASSERT_OK(UpdateSchema({R"SQL(
      CREATE FUNCTION get_v() RETURNS STRING AS (
        (SELECT "v")
      )
    )SQL"}));
  EXPECT_THAT(Query("SELECT get_v()"), IsOkAndHoldsUnorderedRows({{"v"}}));
}

TEST_P(UDFsTest, ArrayDefaultParam) {
  GOOGLESQL_ASSERT_OK(UpdateSchema({
      R"SQL(
      CREATE FUNCTION test_udf(x ARRAY<INT64> DEFAULT [1]) RETURNS ARRAY<INT64> AS (x)
    )SQL"}));
  EXPECT_THAT(Query("SELECT test_udf()"),
              IsOkAndHoldsUnorderedRows(
                  {{cloud::spanner::Value(std::vector<std::int64_t>{1})}}));
}

TEST_P(UDFsTest, IntervalDefaultParam) {
  EXPECT_THAT(UpdateSchema({
                  R"SQL(
      CREATE FUNCTION test_udf(x INTERVAL DEFAULT INTERVAL 1 DAY) RETURNS INTERVAL AS (x)
    )SQL"}),
              googlesql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  testing::HasSubstr(
                      "Function parameter default value must be a literal")));
}

class UDFsDisabledTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
  absl::Status SetUpDatabase() override {
    feature_flags_setter_ = std::make_unique<ScopedEmulatorFeatureFlagsSetter>(
        EmulatorFeatureFlags::Flags{
            .enable_user_defined_functions = false,
        });
    return absl::OkStatus();
  }

  void SetUp() override {
    if (GetConformanceTestGlobals().in_prod_env) {
      GTEST_SKIP() << "Test not applicable to the real Spanner backend "
                      "(Emulator flags test)";
    }
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }
  void TearDown() override {
    if (GetConformanceTestGlobals().in_prod_env) {
      return;
    }
    DatabaseTest::TearDown();
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
    UDFsDisabledTest, UDFsDisabledTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<UDFsDisabledTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(UDFsDisabledTest, CreateDropFunctionFailsIfDisabled) {
  GTEST_SKIP() << "Temporarily disabled";
  if (dialect_ == database_api::DatabaseDialect::POSTGRESQL) {
    EXPECT_THAT(
        UpdateSchema({R"sql(
      CREATE FUNCTION inc_udf(x bigint) RETURNS bigint AS 'SELECT x + 1' LANGUAGE SQL
    )sql"}),
        StatusIs(absl::StatusCode::kFailedPrecondition,
                 HasSubstr("<CREATE FUNCTION> statement is not supported")));
    EXPECT_THAT(
        UpdateSchema({R"sql(
      DROP FUNCTION inc_udf
    )sql"}),
        StatusIs(absl::StatusCode::kFailedPrecondition,
                 HasSubstr("<DROP FUNCTION> statement is not supported")));
  } else {
    EXPECT_THAT(
        UpdateSchema({
            R"SQL(
      CREATE FUNCTION inc_udf(x INT64) RETURNS INT64 AS (x + 1)
    )SQL"}),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("User defined functions are not supported")));
    EXPECT_THAT(
        UpdateSchema({
            R"SQL(
      DROP FUNCTION inc_udf
    )SQL"}),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("User defined functions are not supported")));
  }
}

}  // namespace
}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
