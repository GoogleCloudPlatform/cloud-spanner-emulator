//
// PostgreSQL is released under the PostgreSQL License, a liberal Open Source
// license, similar to the BSD or MIT licenses.
//
// PostgreSQL Database Management System
// (formerly known as Postgres, then as Postgres95)
//
// Portions Copyright © 1996-2020, The PostgreSQL Global Development Group
//
// Portions Copyright © 1994, The Regents of the University of California
//
// Portions Copyright 2023 Google LLC
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
// EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
// FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN
// "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
// MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
//------------------------------------------------------------------------------

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/time/civil_time.h"
#include "absl/time/time.h"
#include "third_party/spanner_pg/function_evaluators/tests/test_base.h"
#include "third_party/spanner_pg/interface/datetime_evaluators.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/timezone_helper.h"

namespace postgres_translator::function_evaluators {
namespace {

using ::testing::IsNull;
using ::testing::Pointee;
using ::testing::StrEq;
using ::zetasql_base::testing::IsOkAndHolds;

constexpr char kDefaultTimezone[] = "UTC";

class TimestampTzTest : public PgEvaluatorTest {
 protected:
  void SetUp() override {
    PgEvaluatorTest::SetUp();
    ZETASQL_ASSERT_OK(InitTimezone(kDefaultTimezone));
  }
  void TearDown() override {
    PgEvaluatorTest::TearDown();
    CleanupPostgresDateTimeCache();
    CleanupTimezone();
  }
};

TEST_F(TimestampTzTest, ReturnsFormattedTime) {
  absl::CivilSecond civil_second(2000, 1, 2, 3, 4, 5);
  absl::TimeZone timezone = absl::UTCTimeZone();
  absl::Duration subseconds = absl::Microseconds(123456);
  absl::Time time = timezone.At(civil_second).pre + subseconds;

  EXPECT_THAT(PgTimestampTzToChar(time, "YYYY-MM-DD HH:MI:SS.FF6 TZ"),
              IsOkAndHolds(Pointee(StrEq("2000-01-02 03:04:05.123456 GMT"))));
}

TEST_F(TimestampTzTest, ReturnsEmptyStringWhenFormatIsEmpty) {
  absl::CivilSecond civil_second(2000, 1, 2, 3, 4, 5);
  absl::TimeZone timezone = absl::UTCTimeZone();
  absl::Duration subseconds = absl::Microseconds(123456);
  absl::Time time = timezone.At(civil_second).pre + subseconds;

  EXPECT_THAT(PgTimestampTzToChar(time, ""), IsOkAndHolds(IsNull()));
}

TEST_F(PgEvaluatorTest, SupportsOlsonTimezones) {
  absl::CivilSecond civil_second(2000, 1, 2, 3, 4, 5);
  absl::TimeZone timezone = absl::UTCTimeZone();
  absl::Duration subseconds = absl::Microseconds(123456);
  absl::Time time = timezone.At(civil_second).pre + subseconds;

  ZETASQL_ASSERT_OK(InitTimezone("Antarctica/Palmer"));
  EXPECT_THAT(PgTimestampTzToChar(time, "YYYY-MM-DD HH24:MI:SS.US TZ"),
              IsOkAndHolds(Pointee(StrEq("2000-01-02 00:04:05.123456 -03"))));
  CleanupTimezone();

  ZETASQL_ASSERT_OK(InitTimezone("America/Caracas"));
  EXPECT_THAT(PgTimestampTzToChar(time, "YYYY-MM-DD HH24:MI:SS.US TZ"),
              IsOkAndHolds(Pointee(StrEq("2000-01-01 23:04:05.123456 -04"))));
  CleanupTimezone();

  ZETASQL_ASSERT_OK(InitTimezone("Australia/Adelaide"));  // UTC+10:30
  EXPECT_THAT(PgTimestampTzToChar(time, "YYYY-MM-DD HH24:MI:SS.US TZ"),
              IsOkAndHolds(Pointee(StrEq("2000-01-02 13:34:05.123456 ACDT"))));
  CleanupTimezone();

  ZETASQL_ASSERT_OK(InitTimezone("Pacific/Chatham"));
  EXPECT_THAT(PgTimestampTzToChar(time, "YYYY-MM-DD HH24:MI:SS.US TZ"),
              IsOkAndHolds(Pointee(StrEq("2000-01-02 16:49:05.123456 +1345"))));
  CleanupTimezone();

  ZETASQL_ASSERT_OK(InitTimezone("Universal"));
  EXPECT_THAT(PgTimestampTzToChar(time, "YYYY-MM-DD HH24:MI:SS.US TZ"),
              IsOkAndHolds(Pointee(StrEq("2000-01-02 03:04:05.123456 UTC"))));
  CleanupTimezone();

  ZETASQL_ASSERT_OK(InitTimezone("Asia/Amman"));  // UTC+02
  EXPECT_THAT(PgTimestampTzToChar(time, "YYYY-MM-DD HH24:MI:SS.US TZ"),
              IsOkAndHolds(Pointee(StrEq("2000-01-02 05:04:05.123456 EET"))));
  CleanupTimezone();

  CleanupPostgresDateTimeCache();
}

TEST_F(PgEvaluatorTest, SupportsPosixTimezones) {
  absl::CivilSecond civil_second(2000, 1, 2, 3, 4, 5);
  absl::TimeZone timezone = absl::UTCTimeZone();
  absl::Duration subseconds = absl::Microseconds(123456);
  absl::Time time = timezone.At(civil_second).pre + subseconds;

  // Positive values in posix format mean WEST of Greenwich while negatives are
  // EAST, but the TZ value is printed in ISO format, where positive values are
  // EAST of Greenwich and negative values are WEST. This is why the sign is
  // inverted in the result (and the time is back 12 hours).
  ZETASQL_ASSERT_OK(InitTimezone("Etc/GMT+12"));
  EXPECT_THAT(PgTimestampTzToChar(time, "YYYY-MM-DD HH24:MI:SS.US TZ"),
              IsOkAndHolds(Pointee(StrEq("2000-01-01 15:04:05.123456 -12"))));
  CleanupTimezone();

  CleanupPostgresDateTimeCache();
}

// ---- Parameterized tests
// These tests exercise regression scenarios in PostgreSQL

struct RegressionTestCase {
  absl::CivilSecond input_civil_second;
  absl::Duration input_subseconds;
  absl::TimeZone input_timezone;
  std::string input_format;
  std::string expected_formatted_timestamp;
};

class RegressionTest : public PgEvaluatorTestWithParam<RegressionTestCase> {
 protected:
  void SetUp() override {
    PgEvaluatorTestWithParam<RegressionTestCase>::SetUp();
    ZETASQL_ASSERT_OK(InitTimezone(kDefaultTimezone));
  }

  void TearDown() override {
    PgEvaluatorTestWithParam<RegressionTestCase>::TearDown();
    CleanupPostgresDateTimeCache();
    CleanupTimezone();
  }
};

TEST_P(RegressionTest, PostgreSQLRegressionTests) {
  const RegressionTestCase& test_case = GetParam();

  absl::Time input_timestamp =
      test_case.input_timezone.At(test_case.input_civil_second).pre +
      test_case.input_subseconds;
  EXPECT_THAT(
      PgTimestampTzToChar(input_timestamp, test_case.input_format),
      IsOkAndHolds(Pointee(StrEq(test_case.expected_formatted_timestamp))));
}
INSTANTIATE_TEST_SUITE_P(
    RegressionTestSuite, RegressionTest,
    testing::Values(
        RegressionTestCase(
            {absl::CivilSecond(1997, 6, 10, 18, 32, 1),
             absl::Microseconds(123456), absl::FixedTimeZone(-7 * 60 * 60),
             "DAY Day day DY Dy dy MONTH Month month RM MON Mon mon",
             "WEDNESDAY Wednesday wednesday WED Wed wed JUNE      June      "
             "june      VI   JUN Jun jun"}),
        RegressionTestCase({absl::CivilSecond(1997, 6, 10, 18, 32, 1),
                            absl::Microseconds(123456),
                            absl::FixedTimeZone(-7 * 60 * 60),
                            "FMDAY FMDay FMday FMMONTH FMMonth FMmonth FMRM",
                            "WEDNESDAY Wednesday wednesday JUNE June june VI"}),
        RegressionTestCase({absl::CivilSecond(1997, 6, 10, 18, 32, 1),
                            absl::Microseconds(123456),
                            absl::FixedTimeZone(-7 * 60 * 60),
                            "Y,YYY YYYY YYY YY Y CC Q MM WW DDD DD D J",
                            "1,997 1997 997 97 7 20 2 06 24 162 11 4 2450611"}),
        RegressionTestCase({absl::CivilSecond(1997, 6, 10, 18, 32, 1),
                            absl::Microseconds(123456),
                            absl::FixedTimeZone(-7 * 60 * 60),
                            "FMY,YYY FMYYYY FMYYY FMYY FMY FMCC FMQ FMMM FMWW "
                            "FMDDD FMDD FMD FMJ",
                            "1,997 1997 997 97 7 20 2 6 24 162 11 4 2450611"}),
        RegressionTestCase({absl::CivilSecond(1997, 6, 10, 18, 32, 1),
                            absl::Microseconds(123456),
                            absl::FixedTimeZone(-7 * 60 * 60),
                            "HH HH12 HH24 MI SS SSSS", "01 01 01 32 01 5521"}),
        RegressionTestCase(
            {absl::CivilSecond(1997, 6, 10, 18, 32, 1),
             absl::Microseconds(123456), absl::FixedTimeZone(-7 * 60 * 60),
             "\"HH:MI:SS is\" HH:MI:SS \"\\\"text between quote marks\\\"\"",
             "HH:MI:SS is 01:32:01 \"text between quote marks\""}),
        RegressionTestCase(
            {absl::CivilSecond(1997, 6, 10, 18, 32, 1),
             absl::Microseconds(123456), absl::FixedTimeZone(-7 * 60 * 60),
             "HH24--text--MI--text--SS", "01--text--32--text--01"}),
        RegressionTestCase({absl::CivilSecond(1997, 6, 10, 18, 32, 1),
                            absl::Microseconds(123456),
                            absl::FixedTimeZone(-7 * 60 * 60),
                            "YYYYTH YYYYth Jth", "1997TH 1997th 2450611th"}),
        RegressionTestCase({absl::CivilSecond(1997, 6, 10, 18, 32, 1),
                            absl::Microseconds(123456),
                            absl::FixedTimeZone(-7 * 60 * 60),
                            "YYYY A.D. YYYY a.d. YYYY bc HH:MI:SS P.M. "
                            "HH:MI:SS p.m. HH:MI:SS pm",
                            "1997 A.D. 1997 a.d. 1997 ad 01:32:01 A.M. "
                            "01:32:01 a.m. 01:32:01 am"}),
        RegressionTestCase(
            {absl::CivilSecond(1997, 6, 10, 18, 32, 1),
             absl::Microseconds(123456), absl::FixedTimeZone(-7 * 60 * 60),
             "IYYY IYY IY I IW IDDD ID", "1997 997 97 7 24 164 3"}),
        RegressionTestCase({absl::CivilSecond(1997, 6, 10, 18, 32, 1),
                            absl::Microseconds(123456),
                            absl::FixedTimeZone(-7 * 60 * 60),
                            "FMIYYY FMIYY FMIY FMI FMIW FMIDDD FMID",
                            "1997 997 97 7 24 164 3"}),
        RegressionTestCase(
            {absl::CivilSecond(1997, 6, 10, 18, 32, 1),
             absl::Microseconds(123456), absl::UTCTimeZone(),
             "FF1 FF2 FF3 FF4 FF5 FF6  ff1 ff2 ff3 ff4 ff5 ff6  MS US",
             "1 12 123 1234 12345 123456  1 12 123 1234 12345 123456  123 "
             "123456"})));

TEST_F(PgEvaluatorTest, RegressionFixedGMTTimeOffset) {
  // Timezone offset, in seconds, 1.5 hours WEST of Greenwich
  int32_t iso_timezone_offset_seconds = -(3600 + 1800);
  // absl takes ISO timezone offsets (negative values are WEST of Greenwich)
  absl::Time input_timestamp =
      absl::FromCivil(absl::CivilSecond(2012, 12, 12, 12, 0, 0),
                      absl::FixedTimeZone(iso_timezone_offset_seconds));

  // PostgreSQL takes POSIX timezone offsets (positive values are WEST of
  // Greenwich, the opposite of the ISO format). Thus, we need to invert the
  // sign.
  ZETASQL_ASSERT_OK(InitTimezoneOffset(-iso_timezone_offset_seconds));

  // Although PostgreSQL takes POSIX timezone offsets they are represented in
  // ISO in the to_char method, thus the sign is inverted in the formatting
  // process.
  EXPECT_THAT(PgTimestampTzToChar(input_timestamp, "YYYY-MM-DD HH:MI:SS TZ"),
              IsOkAndHolds(Pointee(StrEq("2012-12-12 12:00:00 -01:30"))));

  EXPECT_THAT(PgTimestampTzToChar(input_timestamp, "YYYY-MM-DD HH:MI SSSS"),
              IsOkAndHolds(Pointee(StrEq("2012-12-12 12:00 43200"))));
  EXPECT_THAT(PgTimestampTzToChar(input_timestamp, "YYYY-MM-DD HH:MI SSSSS"),
              IsOkAndHolds(Pointee(StrEq("2012-12-12 12:00 43200"))));

  CleanupPostgresDateTimeCache();
  CleanupTimezone();
}
}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
