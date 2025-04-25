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
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_format.h"
#include "absl/time/civil_time.h"
#include "absl/time/time.h"
#include "google/cloud/spanner/timestamp.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using ::testing::HasSubstr;
using ::zetasql_base::testing::StatusIs;

// The microseconds since epoch for "1978-02-14 01:48:23.673001+00" is
// 256268903673001.
constexpr int64_t kTestTimestampInMicrosecond = 256268903673001;

struct DefaultTimezoneTestCase {
  std::string test_query;
  std::string expected_result_type;
  std::vector<std::pair<std::string, std::string>> timezone_and_expected_result;

  static std::vector<DefaultTimezoneTestCase> TestCases() {
    return {
        {"SELECT TIMESTAMP('2008-12-25 15:30:00')",
         "TIMESTAMP",
         // An empty string means the default timezone, which is
         // America/Los_Angeles.
         {
             {"", "2008-12-25T23:30:00Z"},
             {"UTC", "2008-12-25T15:30:00Z"},
             // Australia/Sydney is +11:00.
             {"Australia/Sydney", "2008-12-25T04:30:00Z"},
             // America/Los_Angeles is -08:00.
             {"America/Los_Angeles", "2008-12-25T23:30:00Z"},
             // America/Denver is -07:00.
             {"America/Denver", "2008-12-25T22:30:00Z"},
             // America/Chicago is -06:00.
             {"America/Chicago", "2008-12-25T21:30:00Z"},
             // America/New_York is -05:00.
             {"America/New_York", "2008-12-25T20:30:00Z"},
             // Europe/London is +00:00.
             {"Europe/London", "2008-12-25T15:30:00Z"},
             // Europe/Berlin is +01:00.
             {"Europe/Berlin", "2008-12-25T14:30:00Z"},
             // Europe/Bucharest is +02:00.
             {"Europe/Bucharest", "2008-12-25T13:30:00Z"},
             // Asia/Kolkata is +05:30.
             {"Asia/Kolkata", "2008-12-25T10:00:00Z"},
             // Asia/Tokyo is +09:00.
             {"Asia/Tokyo", "2008-12-25T06:30:00Z"},
             // Asia/Shanghai is +08:00.
             {"Asia/Shanghai", "2008-12-25T07:30:00Z"},
         }},
        {"SELECT EXTRACT(HOUR FROM TIMESTAMP('2008-12-25 15:30:00+00'))",
         "INT64",
         {{"", "7"}, {"UTC", "15"}}},
        {"SELECT FORMAT_TIMESTAMP('%c', TIMESTAMP '2008-12-25 15:30:00+00')",
         "STRING",
         {{"", "Thu Dec 25 07:30:00 2008"},
          {"UTC", "Thu Dec 25 15:30:00 2008"}}},
        {"SELECT PARSE_TIMESTAMP('%c', 'Thu Dec 25 07:30:00 2008')",
         "TIMESTAMP",
         {{"", "2008-12-25T15:30:00Z"}, {"UTC", "2008-12-25T07:30:00Z"}}},
        {"SELECT STRING(TIMESTAMP '2008-12-25 15:30:00+00')",
         "STRING",
         {{"", "2008-12-25 07:30:00-08"}, {"UTC", "2008-12-25 15:30:00+00"}}},
        {"SELECT TIMESTAMP_TRUNC(TIMESTAMP '2008-12-25 15:30:00+00', DAY)",
         "TIMESTAMP",
         {{"", "2008-12-25T08:00:00Z"}, {"UTC", "2008-12-25T00:00:00Z"}}},
        {"SELECT TIMESTAMP_ADD('2014-12-01', interval 100 SECOND)",
         "TIMESTAMP",
         {{"", "2014-12-01T08:01:40Z"}, {"UTC", "2014-12-01T00:01:40Z"}}},
        {"SELECT DATE(TIMESTAMP '2024-11-27 15:30:00+11')",
         "DATE",
         {{"", "2024-11-26"}, {"UTC", "2024-11-27"}}},
        {"SELECT CAST('2008-12-25 15:30:00' AS TIMESTAMP)",
         "TIMESTAMP",
         {{"", "2008-12-25T23:30:00Z"}, {"UTC", "2008-12-25T15:30:00Z"}}},
        {"SELECT CAST(TIMESTAMP '2024-11-27 15:30:00+11' AS DATE)",
         "DATE",
         {{"", "2024-11-26"}, {"UTC", "2024-11-27"}}},
        {"SELECT EXTRACT(DATE FROM TIMESTAMP('1678-06-01 06:18:52.530455+00'))",
         "DATE",
         {{"", "1678-05-31"}, {"UTC", "1678-06-01"}}},
    };
  }
};

class DefaultTimeZoneTest
    : public DatabaseTest,
      public testing::WithParamInterface<DefaultTimezoneTestCase> {
 public:
  DefaultTimeZoneTest() : feature_flags_({.enable_default_time_zone = true}) {}

  void SetUp() override { DatabaseTest::SetUp(); }

  absl::Status SetUpDatabase() override { return absl::OkStatus(); }

 private:
  test::ScopedEmulatorFeatureFlagsSetter feature_flags_;
};

INSTANTIATE_TEST_SUITE_P(
    RunDefaultTimezoneTests, DefaultTimeZoneTest,
    testing::ValuesIn(DefaultTimezoneTestCase::TestCases()));

TEST_P(DefaultTimeZoneTest, TestDefaultTimezone) {
  const DefaultTimezoneTestCase& testcase = GetParam();

  for (const auto& [timezone, expected_result] :
       testcase.timezone_and_expected_result) {
    if (timezone.empty()) {
      // Reset to the default timezone.
      ZETASQL_ASSERT_OK(UpdateSchema({R"(
        ALTER DATABASE db SET OPTIONS (default_time_zone = NULL)
      )"}));
    } else {
      ZETASQL_ASSERT_OK(UpdateSchema(
          {absl::StrFormat(
               "ALTER DATABASE db SET OPTIONS (default_time_zone = '%s')",
               timezone)
               .c_str()}));
    }
    auto query = Query(testcase.test_query);
    if (testcase.expected_result_type == "TIMESTAMP") {
      ZETASQL_ASSERT_OK_AND_ASSIGN(auto expected_result_timestamp,
                           ParseRFC3339TimeSeconds(expected_result));
      EXPECT_THAT(query, IsOkAndHoldsRows({{expected_result_timestamp}}));
    } else if (testcase.expected_result_type == "DATE") {
      absl::Time t;
      EXPECT_TRUE(absl::ParseTime("%Y-%m-%d", expected_result, &t, nullptr));
      absl::CivilDay expected_result_civil_day =
          absl::ToCivilDay(t, absl::UTCTimeZone());
      EXPECT_THAT(query,
                  IsOkAndHoldsRows({{Value(expected_result_civil_day)}}));
    } else if (testcase.expected_result_type == "INT64") {
      int64_t expected_result_int64;
      ASSERT_TRUE(absl::SimpleAtoi(expected_result, &expected_result_int64));
      EXPECT_THAT(query, IsOkAndHoldsRows({{expected_result_int64}}));
    } else {
      EXPECT_THAT(query, IsOkAndHoldsRows({{expected_result}}));
    }
  }
}

TEST_F(DefaultTimeZoneTest, SetDefaultTimezone) {
  auto query = "SELECT TIMESTAMP('2008-12-25 15:30:00')";

  ZETASQL_ASSERT_OK(UpdateSchema({R"(ALTER DATABASE db SET OPTIONS (
        default_time_zone = 'UTC'))"}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto expected_result_timestamp,
                       ParseRFC3339TimeSeconds("2008-12-25T15:30:00Z"));
  EXPECT_THAT(Query(query), IsOkAndHoldsRows({{expected_result_timestamp}}));

  ZETASQL_ASSERT_OK(UpdateSchema({R"(ALTER DATABASE db SET OPTIONS (
        default_time_zone = 'Australia/Sydney'))"}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(expected_result_timestamp,
                       ParseRFC3339TimeSeconds("2008-12-25T04:30:00Z"));
  EXPECT_THAT(Query(query), IsOkAndHoldsRows({{expected_result_timestamp}}));

  ZETASQL_ASSERT_OK(UpdateSchema({R"(ALTER DATABASE db SET OPTIONS (
        default_time_zone = NULL))"}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(expected_result_timestamp,
                       ParseRFC3339TimeSeconds("2008-12-25T23:30:00Z"));
  EXPECT_THAT(Query(query), IsOkAndHoldsRows({{expected_result_timestamp}}));
}

TEST_F(DefaultTimeZoneTest, InvalidTimezoneName) {
  EXPECT_THAT(UpdateSchema({R"(ALTER DATABASE db SET OPTIONS (
        default_time_zone = 'invalid_name'))"}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("Invalid time zone name")));
}

TEST_F(DefaultTimeZoneTest, DisallowChangingDefaultTimezoneAfterTableCreation) {
  EXPECT_THAT(
      UpdateSchema({R"(CREATE TABLE test (id INT64 PRIMARY KEY))",
                    R"(ALTER DATABASE db SET OPTIONS (
        default_time_zone = 'UTC'))"}),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               HasSubstr("The 'default_time_zone' database option cannot be "
                         "changed on a database with user defined tables.")));
}

TEST_F(DefaultTimeZoneTest,
       DisallowSwitchingDefaultTimezoneAfterTableCreation) {
  ZETASQL_ASSERT_OK(UpdateSchema({R"(ALTER DATABASE db SET OPTIONS (
        default_time_zone = 'UTC'))"}));
  EXPECT_THAT(
      UpdateSchema({R"(CREATE TABLE test (id INT64 PRIMARY KEY))",
                    R"(ALTER DATABASE db SET OPTIONS (
        default_time_zone = 'Australia/Sydney'))"}),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               HasSubstr("The 'default_time_zone' database option cannot be "
                         "changed on a database with user defined tables.")));
}

TEST_F(DefaultTimeZoneTest, GeneratedColumnDefaultTimezone) {
  ZETASQL_ASSERT_OK(UpdateSchema({R"(CREATE TABLE test (
        id INT64,
        value TIMESTAMP AS ('2008-12-25 15:30:00') STORED,
      ) PRIMARY KEY(id))"}));
  ZETASQL_ASSERT_OK(CommitDml({SqlStatement("INSERT INTO test (id) VALUES (1)")}));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto expected_result_timestamp,
                       ParseRFC3339TimeSeconds("2008-12-25T23:30:00Z"));
  EXPECT_THAT(Query("SELECT value FROM test"),
              IsOkAndHoldsRows({{expected_result_timestamp}}));
}

TEST_F(DefaultTimeZoneTest, GeneratedColumnUTC) {
  ZETASQL_ASSERT_OK(UpdateSchema({R"(ALTER DATABASE db SET OPTIONS (
        default_time_zone = 'UTC'))"}));
  ZETASQL_ASSERT_OK(UpdateSchema({R"(CREATE TABLE test (
        id INT64,
        value TIMESTAMP AS ('2008-12-25 15:30:00') STORED,
      ) PRIMARY KEY(id))"}));
  ZETASQL_ASSERT_OK(CommitDml({SqlStatement("INSERT INTO test (id) VALUES (1)")}));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto expected_result_timestamp,
                       ParseRFC3339TimeSeconds("2008-12-25T15:30:00Z"));
  EXPECT_THAT(Query("SELECT value FROM test"),
              IsOkAndHoldsRows({{expected_result_timestamp}}));
}

TEST_F(DefaultTimeZoneTest, CheckConstraintDefaultTimezone) {
  ZETASQL_ASSERT_OK(UpdateSchema(
      {"CREATE TABLE test (id INT64, value TIMESTAMP, CONSTRAINT test_check "
       "CHECK(value = '2008-12-25 15:30:00')) PRIMARY KEY(id)"}));  // Crash OK

  EXPECT_THAT(
      CommitDml({SqlStatement(
          "INSERT INTO test (id, value) VALUES (1, '2008-12-25 15:30:00Z')")}),
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr("Check constraint `test`.`test_check` is violated")));
  ZETASQL_ASSERT_OK(CommitDml({SqlStatement(
      "INSERT INTO test (id, value) VALUES (1, '2008-12-25 23:30:00Z')")}));
}

TEST_F(DefaultTimeZoneTest, CheckConstraintUTC) {
  ZETASQL_ASSERT_OK(UpdateSchema({R"(ALTER DATABASE db SET OPTIONS (
        default_time_zone = 'UTC'))"}));
  ZETASQL_ASSERT_OK(UpdateSchema(
      {"CREATE TABLE test (id INT64, value TIMESTAMP, CONSTRAINT test_check "
       "CHECK(value = '2008-12-25 15:30:00')) PRIMARY KEY(id)"}));  // Crash OK

  EXPECT_THAT(
      CommitDml({SqlStatement(
          "INSERT INTO test (id, value) VALUES (1, '2008-12-25 23:30:00Z')")}),
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr("Check constraint `test`.`test_check` is violated")));
  ZETASQL_ASSERT_OK(CommitDml({SqlStatement(
      "INSERT INTO test (id, value) VALUES (1, '2008-12-25 15:30:00Z')")}));
}

TEST_F(DefaultTimeZoneTest, ViewDefaultTimezone) {
  ZETASQL_ASSERT_OK(UpdateSchema({R"(
    CREATE VIEW test_view
    SQL SECURITY INVOKER
    AS SELECT TIMESTAMP('2008-12-25 15:30:00') AS time)"}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto expected_result_timestamp,
                       ParseRFC3339TimeSeconds("2008-12-25T23:30:00Z"));
  EXPECT_THAT(Query("SELECT * FROM test_view"),
              IsOkAndHoldsRows({{expected_result_timestamp}}));
}

TEST_F(DefaultTimeZoneTest, ViewUTC) {
  ZETASQL_ASSERT_OK(UpdateSchema({R"(ALTER DATABASE db SET OPTIONS (
        default_time_zone = 'UTC'))"}));
  ZETASQL_ASSERT_OK(UpdateSchema({R"(
    CREATE VIEW test_view
    SQL SECURITY INVOKER
    AS SELECT TIMESTAMP('2008-12-25 15:30:00') AS time)"}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto expected_result_timestamp,
                       ParseRFC3339TimeSeconds("2008-12-25T15:30:00Z"));
  EXPECT_THAT(Query("SELECT * FROM test_view"),
              IsOkAndHoldsRows({{expected_result_timestamp}}));
}

TEST_F(DefaultTimeZoneTest, QueryParameterTimestampToString) {
  EXPECT_THAT(QueryWithParams(
                  "SELECT STRING(@param)",
                  {{"param",
                    Value(google::cloud::spanner::MakeTimestamp(
                              absl::FromUnixMicros(kTestTimestampInMicrosecond))
                              .value())}}),
              IsOkAndHoldsRow({"1978-02-13 17:48:23.673001-08"}));
}

TEST_F(DefaultTimeZoneTest, QueryParameterTimestampToStringUTC) {
  ZETASQL_ASSERT_OK(UpdateSchema({R"(ALTER DATABASE db SET OPTIONS (
        default_time_zone = 'UTC'))"}));
  Timestamp timestamp = google::cloud::spanner::MakeTimestamp(
                            absl::FromUnixMicros(kTestTimestampInMicrosecond))
                            .value();
  EXPECT_THAT(
      QueryWithParams("SELECT STRING(@param)", {{"param", Value(timestamp)}}),
      IsOkAndHoldsRow({"1978-02-14 01:48:23.673001+00"}));
}

TEST_F(DefaultTimeZoneTest, InsertDMLWithTimestampParameter) {
  ZETASQL_ASSERT_OK(UpdateSchema({R"(CREATE TABLE test (
        id INT64,
        value TIMESTAMP,
      ) PRIMARY KEY(id))"}));

  Timestamp timestamp = google::cloud::spanner::MakeTimestamp(
                            absl::FromUnixMicros(kTestTimestampInMicrosecond))
                            .value();
  ZETASQL_EXPECT_OK(CommitDml(
      {SqlStatement("INSERT INTO test (id, value) VALUES (1, @param)",
                    SqlStatement::ParamType{{"param", Value(timestamp)}})}));

  EXPECT_THAT(Query("SELECT value FROM test WHERE id = 1"),
              IsOkAndHoldsRows({{timestamp}}));
}

TEST_F(DefaultTimeZoneTest, InsertDMLWithTimestampParameterUTC) {
  // Since we provide a timestamp value explicitly, no timezone-related
  // functions are needed and changing the default timezone has no impact.
  ZETASQL_ASSERT_OK(UpdateSchema({R"(ALTER DATABASE db SET OPTIONS (
        default_time_zone = 'UTC'))"}));
  ZETASQL_ASSERT_OK(UpdateSchema({R"(CREATE TABLE test (
        id INT64,
        value TIMESTAMP,
      ) PRIMARY KEY(id))"}));

  Timestamp timestamp = google::cloud::spanner::MakeTimestamp(
                            absl::FromUnixMicros(kTestTimestampInMicrosecond))
                            .value();
  ZETASQL_EXPECT_OK(CommitDml(
      {SqlStatement("INSERT INTO test (id, value) VALUES (1, @param)",
                    SqlStatement::ParamType{{"param", Value(timestamp)}})}));

  EXPECT_THAT(Query("SELECT value FROM test WHERE id = 1"),
              IsOkAndHoldsRows({{timestamp}}));
}

TEST_F(DefaultTimeZoneTest, QueryParameterExtractDate) {
  std::string err;
  absl::Time time;
  absl::ParseTime(absl::RFC3339_full, "1678-06-01T06:18:52.530455Z", &time,
                  &err);
  Timestamp timestamp = google::cloud::spanner::MakeTimestamp(time).value();

  absl::Time date;
  EXPECT_TRUE(absl::ParseTime("%Y-%m-%d", "1678-05-31", &date, nullptr));
  absl::CivilDay expected_date = absl::ToCivilDay(date, absl::UTCTimeZone());
  EXPECT_THAT(QueryWithParams("SELECT EXTRACT(DATE FROM @param)",
                              {{"param", Value(timestamp)}}),
              IsOkAndHoldsRow({expected_date}));
}

TEST_F(DefaultTimeZoneTest, QueryParameterExtractDateUTC) {
  ZETASQL_ASSERT_OK(UpdateSchema({R"(ALTER DATABASE db SET OPTIONS (
        default_time_zone = 'UTC'))"}));
  std::string err;
  absl::Time time;
  absl::ParseTime(absl::RFC3339_full, "1678-06-01T06:18:52.530455Z", &time,
                  &err);
  Timestamp timestamp = google::cloud::spanner::MakeTimestamp(time).value();

  absl::Time date;
  EXPECT_TRUE(absl::ParseTime("%Y-%m-%d", "1678-06-01", &date, nullptr));
  absl::CivilDay expected_date = absl::ToCivilDay(date, absl::UTCTimeZone());
  EXPECT_THAT(QueryWithParams("SELECT EXTRACT(DATE FROM @param)",
                              {{"param", Value(timestamp)}}),
              IsOkAndHoldsRow({expected_date}));
}

class PgDefaultTimeZoneTest
    : public DatabaseTest,
      public testing::WithParamInterface<DefaultTimezoneTestCase> {
 public:
  PgDefaultTimeZoneTest()
      : feature_flags_({.enable_default_time_zone = true}) {}

  void SetUp() override {
    dialect_ = database_api::DatabaseDialect::POSTGRESQL;
    DatabaseTest::SetUp();
  }

  absl::Status SetUpDatabase() override { return absl::OkStatus(); }

 private:
  test::ScopedEmulatorFeatureFlagsSetter feature_flags_;
};

TEST_F(PgDefaultTimeZoneTest, StringLiteral) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto expected_result_timestamp,
                       ParseRFC3339TimeSeconds("2008-12-25T23:30:00Z"));
  EXPECT_THAT(Query("select '2008-12-25 15:30:00'::timestamptz"),
              IsOkAndHoldsRows({{expected_result_timestamp}}));

  ZETASQL_ASSERT_OK(UpdateSchema({R"(
      ALTER DATABASE db SET spanner.default_time_zone = 'UTC')"}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(expected_result_timestamp,
                       ParseRFC3339TimeSeconds("2008-12-25T15:30:00Z"));
  EXPECT_THAT(Query("select '2008-12-25 15:30:00'::timestamptz"),
              IsOkAndHoldsRows({{expected_result_timestamp}}));
}

TEST_F(PgDefaultTimeZoneTest, CastTimestamptzToText) {
  EXPECT_THAT(
      Query("select cast('2008-12-25 15:30:00+00'::timestamptz as text)"),
      IsOkAndHoldsRows({{"2008-12-25 07:30:00-08"}}));

  ZETASQL_ASSERT_OK(UpdateSchema({R"(
      ALTER DATABASE db SET spanner.default_time_zone = 'UTC')"}));
  EXPECT_THAT(
      Query("select cast('2008-12-25 15:30:00+00'::timestamptz as text)"),
      IsOkAndHoldsRows({{"2008-12-25 15:30:00+00"}}));
}

TEST_F(PgDefaultTimeZoneTest, DateTruncation) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto expected_result_timestamp,
                       ParseRFC3339TimeSeconds("2008-12-25T08:00:00Z"));
  EXPECT_THAT(
      Query("select date_trunc('day', timestamptz '2008-12-25 15:30:00+00')"),
      IsOkAndHoldsRows({{expected_result_timestamp}}));

  ZETASQL_ASSERT_OK(UpdateSchema({R"(
      ALTER DATABASE db SET spanner.default_time_zone = 'UTC')"}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(expected_result_timestamp,
                       ParseRFC3339TimeSeconds("2008-12-25T00:00:00Z"));
}

TEST_F(PgDefaultTimeZoneTest, Extract) {
  EXPECT_THAT(Query("select extract(hour from timestamptz '2008-12-25 "
                    "15:30:00+00')::text"),
              IsOkAndHoldsRows({{"7"}}));

  ZETASQL_ASSERT_OK(UpdateSchema({R"(
      ALTER DATABASE db SET spanner.default_time_zone = 'UTC')"}));
  EXPECT_THAT(Query("select extract(hour from timestamptz '2008-12-25 "
                    "15:30:00+00')::text"),
              IsOkAndHoldsRows({{"15"}}));
}

TEST_F(PgDefaultTimeZoneTest, ToChar) {
  EXPECT_THAT(Query("select to_char('1902-07-21T06:07:15.42172Z'::timestamptz, "
                    "'YYYY-MM-DD HH24:MI:SS.USOF')"),
              IsOkAndHoldsRows({{"1902-07-20 22:07:15.421720-08"}}));

  ZETASQL_ASSERT_OK(UpdateSchema({R"(
      ALTER DATABASE db SET spanner.default_time_zone = 'UTC')"}));
  EXPECT_THAT(Query("select to_char('1902-07-21T06:07:15.42172Z'::timestamptz, "
                    "'YYYY-MM-DD HH24:MI:SS.USOF')"),
              IsOkAndHoldsRows({{"1902-07-21 06:07:15.421720+00"}}));
}

TEST_F(PgDefaultTimeZoneTest, ToCharAnotherCase) {
  EXPECT_THAT(Query("SELECT to_char('1970-01-01 02:03:04'::timestamptz, "
                    "'YYYY-MM-DD HH24 MI SS')"),
              IsOkAndHoldsRows({{"1970-01-01 02 03 04"}}));

  ZETASQL_ASSERT_OK(UpdateSchema({R"(
      ALTER DATABASE db SET spanner.default_time_zone = 'UTC')"}));
  EXPECT_THAT(Query("SELECT to_char('1970-01-01 02:03:04'::timestamptz, "
                    "'YYYY-MM-DD HH24 MI SS')"),
              IsOkAndHoldsRows({{"1970-01-01 02 03 04"}}));
}

TEST_F(PgDefaultTimeZoneTest, QueryParameterTimestampToString) {
  EXPECT_THAT(
      QueryWithParams(
          "SELECT cast($1 as text)",
          {{"p1", Value(google::cloud::spanner::MakeTimestamp(
                            absl::FromUnixMicros(kTestTimestampInMicrosecond))
                            .value())}}),
      IsOkAndHoldsRow({"1978-02-13 17:48:23.673001-08"}));

  ZETASQL_ASSERT_OK(UpdateSchema({R"(
      ALTER DATABASE db SET spanner.default_time_zone = 'UTC')"}));
  EXPECT_THAT(
      QueryWithParams(
          "SELECT cast($1 as text)",
          {{"p1", Value(google::cloud::spanner::MakeTimestamp(
                            absl::FromUnixMicros(kTestTimestampInMicrosecond))
                            .value())}}),
      IsOkAndHoldsRow({"1978-02-14 01:48:23.673001+00"}));
}

TEST_F(PgDefaultTimeZoneTest, InsertDMLWithTimestampParameter) {
  ZETASQL_ASSERT_OK(UpdateSchema({R"(CREATE TABLE test (
        id bigint primary key,
        value timestamptz
      ))"}));

  Timestamp timestamp = google::cloud::spanner::MakeTimestamp(
                            absl::FromUnixMicros(kTestTimestampInMicrosecond))
                            .value();
  ZETASQL_EXPECT_OK(CommitDml(
      {SqlStatement("INSERT INTO test (id, value) VALUES (1, $1)",
                    SqlStatement::ParamType{{"p1", Value(timestamp)}})}));

  EXPECT_THAT(Query("SELECT value FROM test WHERE id = 1"),
              IsOkAndHoldsRows({{timestamp}}));
}

TEST_F(PgDefaultTimeZoneTest, InsertDMLWithTimestampParameterUTC) {
  // Since we provide a timestamp value explicitly, no timezone-related
  // functions are needed and changing the default timezone has no impact.
  ZETASQL_ASSERT_OK(UpdateSchema({R"(
      ALTER DATABASE db SET spanner.default_time_zone = 'UTC')"}));
  ZETASQL_ASSERT_OK(UpdateSchema({R"(CREATE TABLE test (
        id bigint primary key,
        value timestamptz
      ))"}));

  Timestamp timestamp = google::cloud::spanner::MakeTimestamp(
                            absl::FromUnixMicros(kTestTimestampInMicrosecond))
                            .value();
  ZETASQL_EXPECT_OK(CommitDml(
      {SqlStatement("INSERT INTO test (id, value) VALUES (1, $1)",
                    SqlStatement::ParamType{{"p1", Value(timestamp)}})}));

  EXPECT_THAT(Query("SELECT value FROM test WHERE id = 1"),
              IsOkAndHoldsRows({{timestamp}}));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
