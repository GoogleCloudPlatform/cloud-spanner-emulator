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

#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "absl/time/civil_time.h"
#include "absl/time/time.h"
#include "third_party/spanner_pg/function_evaluators/tests/test_base.h"
#include "third_party/spanner_pg/interface/datetime_evaluators.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/timezone_helper.h"

namespace postgres_translator::function_evaluators {
namespace {

using ::postgres_translator::CleanupTimezone;
using ::postgres_translator::InitTimezone;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

inline constexpr char kDefaultTimezone[] = "UTC";

// ---- Parameterized tests
// These tests exercise regression scenarios in PostgreSQL

struct RegressionTestCase {
  std::string input_string;
  std::string input_format;
  absl::CivilSecond expected_civil_second;
  absl::TimeZone expected_timezone = absl::UTCTimeZone();
  absl::Duration expected_subseconds = absl::ZeroDuration();

  absl::Time expected_time() const {
    return expected_timezone.At(expected_civil_second).pre +
           expected_subseconds;
  }
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
  EXPECT_THAT(ToTimestamp(test_case.input_string, test_case.input_format),
              IsOkAndHolds(Eq(test_case.expected_time())));
}

INSTANTIATE_TEST_SUITE_P(
    RegressionTestSuite, RegressionTest,
    testing::Values(
        RegressionTestCase({"0097/Feb/16 --> 08:14:30",
                            "YYYY/Mon/DD --> HH:MI:SS",
                            absl::CivilSecond(97, 2, 16, 8, 14, 30)}),
        RegressionTestCase({"97/2/16 8:14:30",
                            "FMYYYY/FMMM/FMDD FMHH:FMMI:FMSS",
                            absl::CivilSecond(97, 2, 16, 8, 14, 30)}),
        RegressionTestCase({"2011$03!18 23_38_15", "YYYY-MM-DD HH24:MI:SS",
                            absl::CivilSecond(2011, 3, 18, 23, 38, 15)}),
        RegressionTestCase({"1985 January 12", "YYYY FMMonth DD",
                            absl::CivilSecond(1985, 1, 12, 0, 0, 0)}),
        RegressionTestCase({"1985 FMMonth 12", "YYYY \"FMMonth\" DD",
                            absl::CivilSecond(1985, 1, 12, 0, 0, 0)}),
        RegressionTestCase({"1985 \\ 12", "YYYY \\ DD",
                            absl::CivilSecond(1985, 1, 12, 0, 0, 0)}),
        RegressionTestCase(
            {"My birthday-> Year: 1976, Month: May, Day: 16",
             "\"My birthday-> Year:\" YYYY, \"Month:\" FMMonth, \"Day:\" DD",
             absl::CivilSecond(1976, 5, 16, 0, 0, 0)}),
        RegressionTestCase({"1,582nd VIII 21", "Y,YYYth FMRM DD",
                            absl::CivilSecond(1582, 8, 21, 0, 0, 0)}),
        RegressionTestCase(
            {"15 \"text between quote marks\" 98 54 45",
             "HH24 \"\\\"text between quote marks\\\"\" YY MI SS",
             absl::CivilSecond(1998, 1, 1, 15, 54, 45)}),
        RegressionTestCase({"05121445482000", "MMDDHH24MISSYYYY",
                            absl::CivilSecond(2000, 5, 12, 14, 45, 48)}),
        RegressionTestCase({"2000January09Sunday", "YYYYFMMonthDDFMDay",
                            absl::CivilSecond(2000, 1, 9, 0, 0, 0)}),
        RegressionTestCase({"97/Feb/16", "YY:Mon:DD",
                            absl::CivilSecond(1997, 2, 16, 0, 0, 0)}),
        RegressionTestCase({"97/Feb/16", "FXYY:Mon:DD",
                            absl::CivilSecond(1997, 2, 16, 0, 0, 0)}),
        RegressionTestCase({"97/Feb/16", "FXYY/Mon/DD",
                            absl::CivilSecond(1997, 2, 16, 0, 0, 0)}),
        RegressionTestCase({"19971116", "YYYYMMDD",
                            absl::CivilSecond(1997, 11, 16, 0, 0, 0)}),
        RegressionTestCase({"1997 AD 11 16", "YYYY BC MM DD",
                            absl::CivilSecond(1997, 11, 16, 0, 0, 0)}),
        RegressionTestCase({"1997 A.D. 11 16", "YYYY B.C. MM DD",
                            absl::CivilSecond(1997, 11, 16, 0, 0, 0)}),
        RegressionTestCase({"9-1116", "Y-MMDD",
                            absl::CivilSecond(2009, 11, 16, 0, 0, 0)}),
        RegressionTestCase({"95-1116", "YY-MMDD",
                            absl::CivilSecond(1995, 11, 16, 0, 0, 0)}),
        RegressionTestCase({"995-1116", "YYY-MMDD",
                            absl::CivilSecond(1995, 11, 16, 0, 0, 0)}),
        RegressionTestCase({"2005426", "YYYYWWD",
                            absl::CivilSecond(2005, 10, 15, 0, 0, 0)}),
        RegressionTestCase({"2005300", "YYYYDDD",
                            absl::CivilSecond(2005, 10, 27, 0, 0, 0)}),
        RegressionTestCase({"2005527", "IYYYIWID",
                            absl::CivilSecond(2006, 1, 1, 0, 0, 0)}),
        RegressionTestCase({"005527", "IYYIWID",
                            absl::CivilSecond(2006, 1, 1, 0, 0, 0)}),
        RegressionTestCase({"05527", "IYIWID",
                            absl::CivilSecond(2006, 1, 1, 0, 0, 0)}),
        RegressionTestCase({"5527", "IIWID",
                            absl::CivilSecond(2006, 1, 1, 0, 0, 0)}),
        RegressionTestCase({"2005364", "IYYYIDDD",
                            absl::CivilSecond(2006, 1, 1, 0, 0, 0)}),
        RegressionTestCase({"20050302", "YYYYMMDD",
                            absl::CivilSecond(2005, 3, 2, 0, 0, 0)}),
        RegressionTestCase({"2005 03 02", "YYYYMMDD",
                            absl::CivilSecond(2005, 3, 2, 0, 0, 0)}),
        RegressionTestCase({" 2005 03 02", "YYYYMMDD",
                            absl::CivilSecond(2005, 3, 2, 0, 0, 0)}),
        RegressionTestCase({"  20050302", "YYYYMMDD",
                            absl::CivilSecond(2005, 3, 2, 0, 0, 0)}),
        RegressionTestCase({"2011-12-18 11:38 AM", "YYYY-MM-DD HH12:MI PM",
                            absl::CivilSecond(2011, 12, 18, 11, 38, 0)}),
        RegressionTestCase({"2011-12-18 11:38 PM", "YYYY-MM-DD HH12:MI PM",
                            absl::CivilSecond(2011, 12, 18, 23, 38, 0)}),
        RegressionTestCase({"2011-12-18 11:38 A.M.", "YYYY-MM-DD HH12:MI P.M.",
                            absl::CivilSecond(2011, 12, 18, 11, 38, 0)}),
        RegressionTestCase({"2011-12-18 11:38 P.M.", "YYYY-MM-DD HH12:MI P.M.",
                            absl::CivilSecond(2011, 12, 18, 23, 38, 0)}),
        RegressionTestCase({"2011-12-18 11:38 +05", "YYYY-MM-DD HH12:MI TZH",
                            absl::CivilSecond(2011, 12, 18, 11, 38, 0),
                            absl::FixedTimeZone(5 * 60 * 60)}),
        RegressionTestCase({"2011-12-18 11:38 -05", "YYYY-MM-DD HH12:MI TZH",
                            absl::CivilSecond(2011, 12, 18, 11, 38, 0),
                            absl::FixedTimeZone(-5 * 60 * 60)}),
        RegressionTestCase({"2011-12-18 11:38 +05:20",
                            "YYYY-MM-DD HH12:MI TZH:TZM",
                            absl::CivilSecond(2011, 12, 18, 11, 38, 0),
                            absl::FixedTimeZone(5 * 60 * 60 + 20 * 60)}),
        RegressionTestCase({"2011-12-18 11:38 -05:20",
                            "YYYY-MM-DD HH12:MI TZH:TZM",
                            absl::CivilSecond(2011, 12, 18, 11, 38, 0),
                            absl::FixedTimeZone(-5 * 60 * 60 - 20 * 60)}),
        RegressionTestCase({"2011-12-18 11:38 20", "YYYY-MM-DD HH12:MI TZM",
                            absl::CivilSecond(2011, 12, 18, 11, 38, 0),
                            absl::FixedTimeZone(20 * 60)}),
        RegressionTestCase({"2018-11-02 12:34:56.025",
                            "YYYY-MM-DD HH24:MI:SS.MS",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Milliseconds(25)}),
        RegressionTestCase({"2018-11-02 12:34:56", "YYYY-MM-DD HH24:MI:SS.FF1",
                            absl::CivilSecond(2018, 11, 02, 12, 34, 56)}),
        RegressionTestCase({"2018-11-02 12:34:56", "YYYY-MM-DD HH24:MI:SS.FF2",
                            absl::CivilSecond(2018, 11, 02, 12, 34, 56)}),
        RegressionTestCase({"2018-11-02 12:34:56", "YYYY-MM-DD HH24:MI:SS.FF3",
                            absl::CivilSecond(2018, 11, 02, 12, 34, 56)}),
        RegressionTestCase({"2018-11-02 12:34:56", "YYYY-MM-DD HH24:MI:SS.FF4",
                            absl::CivilSecond(2018, 11, 02, 12, 34, 56)}),
        RegressionTestCase({"2018-11-02 12:34:56", "YYYY-MM-DD HH24:MI:SS.FF5",
                            absl::CivilSecond(2018, 11, 02, 12, 34, 56)}),
        RegressionTestCase({"2018-11-02 12:34:56", "YYYY-MM-DD HH24:MI:SS.FF6",
                            absl::CivilSecond(2018, 11, 02, 12, 34, 56)}),
        RegressionTestCase({"2018-11-02 12:34:56.1",
                            "YYYY-MM-DD HH24:MI:SS.FF1",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Milliseconds(100)}),
        RegressionTestCase({"2018-11-02 12:34:56.1",
                            "YYYY-MM-DD HH24:MI:SS.FF1",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Milliseconds(100)}),
        RegressionTestCase({"2018-11-02 12:34:56.1",
                            "YYYY-MM-DD HH24:MI:SS.FF2",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Milliseconds(100)}),
        RegressionTestCase({"2018-11-02 12:34:56.1",
                            "YYYY-MM-DD HH24:MI:SS.FF3",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Milliseconds(100)}),
        RegressionTestCase({"2018-11-02 12:34:56.1",
                            "YYYY-MM-DD HH24:MI:SS.FF4",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Milliseconds(100)}),
        RegressionTestCase({"2018-11-02 12:34:56.1",
                            "YYYY-MM-DD HH24:MI:SS.FF5",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Milliseconds(100)}),
        RegressionTestCase({"2018-11-02 12:34:56.1",
                            "YYYY-MM-DD HH24:MI:SS.FF6",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Milliseconds(100)}),
        RegressionTestCase({"2018-11-02 12:34:56.12",
                            "YYYY-MM-DD HH24:MI:SS.FF1",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Milliseconds(100)}),
        RegressionTestCase({"2018-11-02 12:34:56.12",
                            "YYYY-MM-DD HH24:MI:SS.FF2",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Milliseconds(120)}),
        RegressionTestCase({"2018-11-02 12:34:56.12",
                            "YYYY-MM-DD HH24:MI:SS.FF3",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Milliseconds(120)}),
        RegressionTestCase({"2018-11-02 12:34:56.12",
                            "YYYY-MM-DD HH24:MI:SS.FF4",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Milliseconds(120)}),
        RegressionTestCase({"2018-11-02 12:34:56.12",
                            "YYYY-MM-DD HH24:MI:SS.FF5",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Milliseconds(120)}),
        RegressionTestCase({"2018-11-02 12:34:56.12",
                            "YYYY-MM-DD HH24:MI:SS.FF6",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Milliseconds(120)}),
        RegressionTestCase({"2018-11-02 12:34:56.123",
                            "YYYY-MM-DD HH24:MI:SS.FF1",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Milliseconds(100)}),
        RegressionTestCase({"2018-11-02 12:34:56.123",
                            "YYYY-MM-DD HH24:MI:SS.FF2",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Milliseconds(120)}),
        RegressionTestCase({"2018-11-02 12:34:56.123",
                            "YYYY-MM-DD HH24:MI:SS.FF3",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Milliseconds(123)}),
        RegressionTestCase({"2018-11-02 12:34:56.123",
                            "YYYY-MM-DD HH24:MI:SS.FF4",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Milliseconds(123)}),
        RegressionTestCase({"2018-11-02 12:34:56.123",
                            "YYYY-MM-DD HH24:MI:SS.FF5",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Milliseconds(123)}),
        RegressionTestCase({"2018-11-02 12:34:56.123",
                            "YYYY-MM-DD HH24:MI:SS.FF6",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Milliseconds(123)}),
        RegressionTestCase({"2018-11-02 12:34:56.1234",
                            "YYYY-MM-DD HH24:MI:SS.FF1",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Microseconds(100000)}),
        RegressionTestCase({"2018-11-02 12:34:56.1234",
                            "YYYY-MM-DD HH24:MI:SS.FF2",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Microseconds(120000)}),
        RegressionTestCase({"2018-11-02 12:34:56.1234",
                            "YYYY-MM-DD HH24:MI:SS.FF3",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Microseconds(123000)}),
        RegressionTestCase({"2018-11-02 12:34:56.1234",
                            "YYYY-MM-DD HH24:MI:SS.FF4",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Microseconds(123400)}),
        RegressionTestCase({"2018-11-02 12:34:56.1234",
                            "YYYY-MM-DD HH24:MI:SS.FF5",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Microseconds(123400)}),
        RegressionTestCase({"2018-11-02 12:34:56.1234",
                            "YYYY-MM-DD HH24:MI:SS.FF6",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Microseconds(123400)}),
        RegressionTestCase({"2018-11-02 12:34:56.12345",
                            "YYYY-MM-DD HH24:MI:SS.FF1",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Microseconds(100000)}),
        RegressionTestCase({"2018-11-02 12:34:56.12345",
                            "YYYY-MM-DD HH24:MI:SS.FF2",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Microseconds(120000)}),
        RegressionTestCase({"2018-11-02 12:34:56.12345",
                            "YYYY-MM-DD HH24:MI:SS.FF3",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Microseconds(123000)}),
        RegressionTestCase({"2018-11-02 12:34:56.12345",
                            "YYYY-MM-DD HH24:MI:SS.FF4",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Microseconds(123500)}),
        RegressionTestCase({"2018-11-02 12:34:56.12345",
                            "YYYY-MM-DD HH24:MI:SS.FF5",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Microseconds(123450)}),
        RegressionTestCase({"2018-11-02 12:34:56.12345",
                            "YYYY-MM-DD HH24:MI:SS.FF6",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Microseconds(123450)}),
        RegressionTestCase({"2018-11-02 12:34:56.123456",
                            "YYYY-MM-DD HH24:MI:SS.FF1",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Microseconds(100000)}),
        RegressionTestCase({"2018-11-02 12:34:56.123456",
                            "YYYY-MM-DD HH24:MI:SS.FF2",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Microseconds(120000)}),
        RegressionTestCase({"2018-11-02 12:34:56.123456",
                            "YYYY-MM-DD HH24:MI:SS.FF3",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Microseconds(123000)}),
        RegressionTestCase({"2018-11-02 12:34:56.123456",
                            "YYYY-MM-DD HH24:MI:SS.FF4",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Microseconds(123500)}),
        RegressionTestCase({"2018-11-02 12:34:56.123456",
                            "YYYY-MM-DD HH24:MI:SS.FF5",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Microseconds(123460)}),
        RegressionTestCase({"2018-11-02 12:34:56.123456",
                            "YYYY-MM-DD HH24:MI:SS.FF6",
                            absl::CivilSecond(2018, 11, 2, 12, 34, 56),
                            absl::UTCTimeZone(), absl::Microseconds(123456)})));

TEST_F(RegressionTest, ReturnsInvalidArgumentWhenDateIsOutOfBounds) {
  EXPECT_THAT(ToTimestamp("20000-1116", "YYYY-MMDD"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Timestamp is out of supported range")));
  EXPECT_THAT(ToTimestamp("1997 BC 11 16", "YYYY BC MM DD"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Timestamp is out of supported range")));
  EXPECT_THAT(ToTimestamp("1997 B.C. 11 16", "YYYY B.C. MM DD"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Timestamp is out of supported range")));
  EXPECT_THAT(
      ToTimestamp("2018-11-02 12:34:56.123456789", "YYYY-MM-DD HH24:MI:SS.FF1"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Timestamp is out of supported range")));
  // Empty format returns 0001-01-01 00:00:00+00 BC timestamp
  EXPECT_THAT(ToTimestamp("2000-01-02 03:04:05.678", ""),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Timestamp is out of supported range")));
}

TEST_F(RegressionTest, ReturnsInvalidArgumentWhenFormatIsInvalid) {
  EXPECT_THAT(ToTimestamp("97/Feb/16", "YYMonDD"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid value \"/Feb/16\" for \"Mon\"")));
}

TEST_F(RegressionTest, ReturnsUnimplementedForTZPattern) {
  EXPECT_THAT(
      ToTimestamp("2011-12-18 11:38 PST", "YYYY-MM-DD HH12:MI TZ"),
      StatusIs(
          absl::StatusCode::kUnimplemented,
          HasSubstr("formatting field \"TZ\" is only supported in to_char")));
}

// --config=asan
TEST_F(RegressionTest, ASANViolation) {
  EXPECT_THAT(ToTimestamp("\013 \010\25411111111\010",
                          std::string("\232\361UC--------tym---.\000----", 24)),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(
      ToTimestamp(
          "\274p\034\03488883888888\034\034h8\364",
          "%\036\036\223!\014\tG\024\213\213^cc\227\227cs\335\355\217c+"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("timestamp out of range")));
  EXPECT_THAT(
      ToTimestamp(
          "\32677DDDD777577777\3207777\3247877777HH7777777777\022\02277777\327"
          "\370777nnnnnnnn\302nnnnnnlllllllnnnnnnnn7777\3648777w7",
          "\202W\202\337_\230\024WW\024\024\322\337"),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("cannot calculate day of year without year information")));
  EXPECT_THAT(
      ToTimestamp(
          "\255]]]]]]]\\]]]]]]555555558?5555558555555]]]]]\316\\230Yb4]]]]]]]]]"
          "]]]]]]]]]]]]]]]]Z]]]]"
          "\213\213\213\213\213\213\213\213\213\213\213\213\213\213\213\213\213"
          "\213\213\213\213\213\213\213\213\213\213\213\213\213\213\213\213\213"
          "\213\213\213\213\213\213\213\213\213\213\213\213\213\213]"
          "\213\213\213\213\213\213\2132\213\213\213\213\213\213\213\213\213"
          "\213\213\213\213\213\213\213\213\213\213\213\213\213\213\213\213\213"
          "\213\213\213\213\213\213\213\213\213\213\213\213\213\213\213\213\213"
          "\213\213\213\213\213\213\213\213\213\213\213\213\213\213\213\213\213"
          "\213\213\213\213\213\213\213\213\213\213\213\213\213\214\236\213\213"
          "\213\213\243\213\213\213\213\213\213\213\213\213\213\213\213\213\213"
          "\213\030\030\030\030\030\030\030\030\213\213]]gk\017",
          "\257a\327#\257\257\257\257\351\257\254\360\261\221\331msnn"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Timestamp is out of supported range")));
  EXPECT_THAT(ToTimestamp("\336V\036\232\250\030{"
                          "Ulqu\3613333333333\370\005\265K\347\373\357\37395",
                          "\237A\237\254\233X`\023\315\315\315\315\n\006IW"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("timestamp out of range")));
  EXPECT_THAT(
      ToTimestamp(
          "\007\3411111\021111111\34211111111111-1111111111\251-"
          "5Q\024\3101111U1111111\251\311\255A%%%%\351%\234",
          "\346R\360\366\362\362:\034Qr\303CCC\324r\252\037\275<\365Y,>"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("timestamp out of range")));

  EXPECT_THAT(
      ToTimestamp(
          "0000000400000000000,004-\031R400 "
          "0000000000000010000000000000000000000000000000000000 "
          "\213284D12R\214000000000000000000000000",
          "\271y,yyyTHyy_WW\347dx\204\264\324yth\373A\271&TMMONHZZZONT"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("invalid value \"R4\" for \"yy\"")));
}
}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
