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
#include "third_party/spanner_pg/function_evaluators/tests/test_base.h"
#include "third_party/spanner_pg/interface/datetime_evaluators.h"
#include "third_party/spanner_pg/shims/timezone_helper.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator::function_evaluators {
namespace {

constexpr char kDefaultTimezone[] = "America/Los_Angeles";

using ::testing::HasSubstr;
using ::zetasql_base::testing::StatusIs;

struct TimestamptzMathTestCase {
  TimestamptzMathTestCase(std::string timestamptz_input_in,
                          double interval_value_in,
                          std::string interval_type_in,
                          std::string expected_timestamptz_in)
      : timestamptz_input_str(timestamptz_input_in),
        interval_value(interval_value_in),
        interval_type(interval_type_in),
        expected_timestamptz_str(expected_timestamptz_in) {}

  std::string timestamptz_input_str;
  double interval_value;
  std::string interval_type;
  std::string expected_timestamptz_str;
};

class TimestamptzMathTest
    : public PgEvaluatorTestWithParam<TimestamptzMathTestCase> {
 protected:
  void SetUp() override {
    PgEvaluatorTestWithParam<TimestamptzMathTestCase>::SetUp();
    ZETASQL_ASSERT_OK(InitTimezone(kDefaultTimezone));
  }

  void TearDown() override {
    PgEvaluatorTestWithParam<TimestamptzMathTestCase>::TearDown();
    CleanupPostgresDateTimeCache();
    CleanupTimezone();
  }
};

TEST_P(TimestamptzMathTest, TimestamptzAdd) {
  const TimestamptzMathTestCase& test_case = GetParam();
  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Time timestamptz_input,
                       PgTimestamptzIn(test_case.timestamptz_input_str));
  std::string interval =
      absl::StrCat(test_case.interval_value, " ", test_case.interval_type);
  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Time computed_time,
                       PgTimestamptzAdd(timestamptz_input, interval));

  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Time expected_timestamptz,
                       PgTimestamptzIn(test_case.expected_timestamptz_str));
  EXPECT_EQ(expected_timestamptz, computed_time);
}

TEST_P(TimestamptzMathTest, TimestamptzSubtract) {
  const TimestamptzMathTestCase& test_case = GetParam();
  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Time timestamptz_input,
                       PgTimestamptzIn(test_case.timestamptz_input_str));

  // Turn each add case into a subtract case by multiplying the interval
  // value by -1.
  double interval_value = -1 * test_case.interval_value;
  std::string interval =
      absl::StrCat(interval_value, " ", test_case.interval_type);
  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Time computed_time,
                       PgTimestamptzSubtract(timestamptz_input, interval));

  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Time expected_timestamptz,
                       PgTimestamptzIn(test_case.expected_timestamptz_str));
  EXPECT_EQ(expected_timestamptz, computed_time);
}

INSTANTIATE_TEST_SUITE_P(
    TimestamptzMathTestSuite, TimestamptzMathTest,
    testing::Values(
        // Interval crossing time shift for Europe/Warsaw timezone (with DST)
        // From the PG regress test suite
        TimestamptzMathTestCase({"2022-10-30 00:00:00+01", 1, "DAY",
                                 "2022-10-30 16:00:00"}),
        TimestamptzMathTestCase({"2021-10-31 00:00:00+02", 1, "DAY",
                                 "2021-10-31 15:00:00"}),
        TimestamptzMathTestCase({"2022-10-30 00:00:00+01", -1, "DAY",
                                 "2022-10-28 16:00:00"}),
        TimestamptzMathTestCase({"2021-10-31 00:00:00+02", -1, "DAY",
                                 "2021-10-29 15:00:00"}),
        // From the ZetaSQL timestamp test suite.
        // Microseconds
        TimestamptzMathTestCase({"2000-01-01 00:11:22.345678", 0,
                                 "microseconds", "2000-01-01 0:11:22.345678"}),
        TimestamptzMathTestCase({"2000-01-01 00:11:22.345678", 1,
                                 "microseconds", "2000-01-01 0:11:22.345679"}),
        TimestamptzMathTestCase({"2000-01-01 00:11:22.345678", -1,
                                 "microseconds", "2000-01-01 0:11:22.345677"}),
        TimestamptzMathTestCase({"2000-01-01 00:11:22.345678", 322,
                                 "microseconds", "2000-01-01 00:11:22.346"}),
        TimestamptzMathTestCase({"2000-01-01 00:11:22.345678", -678,
                                 "microseconds", "2000-01-01 00:11:22.345"}),
        TimestamptzMathTestCase({"2015-05-01", 0, "microseconds",
                                 "2015-05-01 00:00:00"}),
        TimestamptzMathTestCase({"2015-05-01", 1, "microseconds",
                                 "2015-05-01 00:00:00.000001"}),
        TimestamptzMathTestCase({"2015-05-01", -1, "microseconds",
                                 "2015-04-30 23:59:59.999999"}),
        TimestamptzMathTestCase({"2015-05-01", 456, "microseconds",
                                 "2015-05-01 00:00:00.000456"}),
        TimestamptzMathTestCase({"2015-12-31 23:59:59.899980", 100020,
                                 "microseconds", "2016-01-01 00:00:00"}),
        // Milliseconds
        TimestamptzMathTestCase({"2000-01-01 00:11:22.345678", 0,
                                 "milliseconds", "2000-01-01 00:11:22.345678"}),
        TimestamptzMathTestCase({"2000-01-01 00:11:22.345678", 1,
                                 "milliseconds", "2000-01-01 00:11:22.346678"}),
        TimestamptzMathTestCase({"2000-01-01 00:11:22.345678", -1,
                                 "milliseconds", "2000-01-01 00:11:22.344678"}),
        TimestamptzMathTestCase({"2000-01-01 00:11:22.345678", 655,
                                 "milliseconds", "2000-01-01 00:11:23.000678"}),
        TimestamptzMathTestCase({"2000-01-01 00:11:22.345678", -345,
                                 "milliseconds", "2000-01-01 00:11:22.000678"}),
        // Seconds
        TimestamptzMathTestCase({"2000-01-01 00:11:22.345678", 0, "SECONDS",
                                 "2000-01-01 00:11:22.345678"}),
        TimestamptzMathTestCase({"2000-01-01 00:11:22.345678", 1, "SECONDS",
                                 "2000-01-01 00:11:23.345678"}),
        TimestamptzMathTestCase({"2000-01-01 00:11:21.345678", -1, "SECONDS",
                                 "2000-01-01 00:11:20.345678"}),
        TimestamptzMathTestCase({"2000-01-01 00:11:22.345678", 38, "SECONDS",
                                 "2000-01-01 00:12:00.345678"}),
        TimestamptzMathTestCase({"2000-01-01 00:11:22.345678", -22, "SECONDS",
                                 "2000-01-01 00:11:00.345678"}),
        TimestamptzMathTestCase({"9999-12-31 15:59:55", 4, "SECONDS",
                                 "9999-12-31 15:59:59"}),
        TimestamptzMathTestCase({"0001-01-01 00:00:09", -9, "SECONDS",
                                 "0001-01-01 00:00:00"}),
        // Minutes
        TimestamptzMathTestCase({"2000-01-01 00:11:22.345678", 0, "MINUTES",
                                 "2000-01-01 00:11:22.345678"}),
        TimestamptzMathTestCase({"2000-01-01 00:11:22.345678", 1, "MINUTE",
                                 "2000-01-01 00:12:22.345678"}),
        TimestamptzMathTestCase({"2000-01-01 00:11:21.345678", -1, "MINUTE",
                                 "2000-01-01 00:10:21.345678"}),
        TimestamptzMathTestCase({"2000-01-01 00:11:22.345678", 49, "MINUTES",
                                 "2000-01-01 1:00:22.345678"}),
        TimestamptzMathTestCase({"2000-01-01 00:11:22.345678", -11, "MINUTES",
                                 "2000-01-01 00:00:22.345678"}),
        TimestamptzMathTestCase({"9999-12-31 15:55:00", 4, "MINUTES",
                                 "9999-12-31 15:59:00"}),
        TimestamptzMathTestCase({"0001-01-01 00:09:00", -9, "MINUTES",
                                 "0001-01-01 00:00:00"}),
        // Hours
        TimestamptzMathTestCase({"2000-01-01 00:11:22.345678", 0, "HOURS",
                                 "2000-01-01 00:11:22.345678"}),
        TimestamptzMathTestCase({"2000-01-01 00:11:22.345678", 1, "HOUR",
                                 "2000-01-01 01:11:22.345678"}),
        TimestamptzMathTestCase({"2000-01-01 00:11:21.345678", -1, "HOUR",
                                 "1999-12-31 23:11:21.345678"}),
        TimestamptzMathTestCase({"2000-01-01 05:11:22.345678", 49, "HOURS",
                                 "2000-01-03 06:11:22.345678"}),
        TimestamptzMathTestCase({"2000-01-01 06:11:22.345678", -11, "HOURS",
                                 "1999-12-31 19:11:22.345678"}),
        TimestamptzMathTestCase({"9999-12-31 11:00:00", 4, "HOURS",
                                 "9999-12-31 15:00:00"}),
        TimestamptzMathTestCase({"0001-01-01 09:00:00", -9, "HOURS",
                                 "0001-01-01 00:00:00"}),
        // Days
        TimestamptzMathTestCase({"2000-01-01 00:11:22.345678", 0, "DAYS",
                                 "2000-01-01 00:11:22.345678"}),
        TimestamptzMathTestCase({"2000-01-01 00:11:22.345678", 1, "DAY",
                                 "2000-01-02 00:11:22.345678"}),
        TimestamptzMathTestCase({"2000-01-01 00:11:21.345678", -1, "DAY",
                                 "1999-12-31 00:11:21.345678"}),
        TimestamptzMathTestCase({"2000-01-01 05:11:22.345678", 15, "DAYS",
                                 "2000-01-16 05:11:22.345678"}),
        TimestamptzMathTestCase({"2000-01-01 05:11:22.345678", 49, "DAYS",
                                 "2000-02-19 05:11:22.345678"}),
        TimestamptzMathTestCase({"2000-01-05 06:11:22.345678", -11, "DAYS",
                                 "1999-12-25 06:11:22.345678"}),
        TimestamptzMathTestCase({"9999-12-27 15:59:00", 4, "DAYS",
                                 "9999-12-31 15:59:00"}),
        TimestamptzMathTestCase({"0001-01-09 00:00:00", -8, "DAYS",
                                 "0001-01-01 00:00:00"}),
        // Some tests around the DST boundaries.
        // Before 'fall back' boundary for America/Los_Angeles, this corresponds
        // to the 'first' 1:01am.
        TimestamptzMathTestCase({"2016-11-06 08:01:01.123400+00", 1, "DAY",
                                 "2016-11-07 01:01:01.123400"}),
        TimestamptzMathTestCase({"2016-11-06 08:01:01.123400+00", -1, "DAY",
                                 "2016-11-05 01:01:01.123400"}),
        // Inside 'fall back' boundary for America/Los_Angeles, this corresponds
        // to the 'second' 1:01am.
        TimestamptzMathTestCase({"2016-11-06 09:01:01.123400+00", 1, "DAY",
                                 "2016-11-07 01:01:01.123400"}),
        TimestamptzMathTestCase({"2016-11-06 09:01:01.123400+00", -1, "DAY",
                                 "2016-11-05 01:01:01.123400"}),
        // Inside 'fall back' boundary for America/Los_Angeles, this corresponds
        // to 2:01am.
        TimestamptzMathTestCase({"2016-11-06 10:01:01.123400+00", 1, "DAY",
                                 "2016-11-07 02:01:01.123400"}),
        TimestamptzMathTestCase({"2016-11-06 10:01:01.123400+00", -1, "DAY",
                                 "2016-11-05 02:01:01.123400"}),
        // Before 'spring forward' boundary for America/Los_Angeles, this
        // corresponds to 1:01am.
        TimestamptzMathTestCase({"2016-03-13 09:01:01.123400+00", 1, "DAY",
                                 "2016-03-14 01:01:01.123400"}),
        TimestamptzMathTestCase({"2016-03-13 09:01:01.123400+00", -1, "DAY",
                                 "2016-03-12 01:01:01.123400"}),
        // After 'spring forward' boundary for America/Los_Angeles, this
        // corresponds to 3:01am
        TimestamptzMathTestCase({"2016-03-13 10:01:01.123400+00", 1, "DAY",
                                 "2016-03-14 03:01:01.123400"}),
        TimestamptzMathTestCase({"2016-03-13 10:01:01.123400+00", -1, "DAY",
                                 "2016-03-12 03:01:01.123400"})));

struct TimestamptzMathErrorTestCase {
  TimestamptzMathErrorTestCase(std::string timestamptz_input,
                               double interval_val, std::string interval_type)
      : timestamptz(timestamptz_input),
        interval_value(interval_val),
        interval_part(interval_type) {}

  std::string timestamptz;
  double interval_value;
  std::string interval_part;
};

class TimestamptzMathErrorTest
    : public PgEvaluatorTestWithParam<TimestamptzMathErrorTestCase> {
 protected:
  void SetUp() override {
    PgEvaluatorTestWithParam<TimestamptzMathErrorTestCase>::SetUp();
    ZETASQL_ASSERT_OK(InitTimezone(kDefaultTimezone));
  }

  void TearDown() override {
    PgEvaluatorTestWithParam<TimestamptzMathErrorTestCase>::TearDown();
    CleanupPostgresDateTimeCache();
    CleanupTimezone();
  }
};

TEST_P(TimestamptzMathErrorTest, TimestamptzMathError) {
  const TimestamptzMathErrorTestCase& test_case = GetParam();
  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Time time_input,
                       PgTimestamptzIn(test_case.timestamptz));
  std::string interval =
      absl::StrCat(test_case.interval_value, " ", test_case.interval_part);
  EXPECT_THAT(PgTimestamptzAdd(time_input, interval),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Timestamp is out of supported range")));

  // Turn each add case into a subtract case by multiplying the interval
  // value by -1.
  double flipped_interval_value = -1 * test_case.interval_value;
  interval = absl::StrCat(flipped_interval_value, " ", test_case.interval_part);
  EXPECT_THAT(PgTimestamptzSubtract(time_input, interval),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Timestamp is out of supported range")));
}

INSTANTIATE_TEST_SUITE_P(
    TimestamptzMathErrorTestSuite, TimestamptzMathErrorTest,
    testing::Values(
        TimestamptzMathErrorTestCase({"9999-12-31 23:59:59.999999+00", 1,
                                      "MICROSECOND"}),
        TimestamptzMathErrorTestCase({"0001-01-01+00", -1, "MICROSECOND"}),
        TimestamptzMathErrorTestCase({"9999-12-31 23:59:59.998999+00", 2,
                                      "MILLISECOND"}),
        TimestamptzMathErrorTestCase({"0001-01-01+00", -2, "MILLISECOND"}),
        TimestamptzMathErrorTestCase({"9999-12-31 23:59:55+00", 5, "SECOND"}),
        TimestamptzMathErrorTestCase({"9999-12-31 23:59:55+00", 6, "SECOND"}),
        TimestamptzMathErrorTestCase({"0001-01-01+00", -1, "SECOND"}),
        TimestamptzMathErrorTestCase({"0001-01-01 00:00:09+00", -10, "SECOND"}),
        TimestamptzMathErrorTestCase({"9999-12-31 23:55:00+00", 5, "MINUTE"}),
        TimestamptzMathErrorTestCase({"9999-12-31 23:55:00+00", 6, "MINUTE"}),
        TimestamptzMathErrorTestCase({"0001-01-01+00", -1, "MINUTE"}),
        TimestamptzMathErrorTestCase({"0001-01-01 00:09:00+00", -10, "MINUTE"}),
        TimestamptzMathErrorTestCase({"9999-12-31 19:00:00+00", 5, "HOUR"}),
        TimestamptzMathErrorTestCase({"9999-12-31 19:00:00+00", 6, "HOUR"}),
        TimestamptzMathErrorTestCase({"0001-01-01+00", -1, "HOUR"}),
        TimestamptzMathErrorTestCase({"0001-01-01 09:00:00+00", -10, "HOUR"}),
        TimestamptzMathErrorTestCase({"9999-12-28 00:00:00+00", 5, "DAY"}),
        TimestamptzMathErrorTestCase({"0001-01-01+00", -1, "DAY"})));

}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
