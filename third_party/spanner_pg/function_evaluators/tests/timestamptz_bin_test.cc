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

struct TimestamptzBinTestCase {
  TimestamptzBinTestCase(std::string interval_in, std::string source_in,
                         std::string origin_in, std::string expected_result_in)
      : interval(interval_in),
        source(source_in),
        origin(origin_in),
        expected_result(expected_result_in) {}

  std::string interval;
  std::string source;
  std::string origin;
  std::string expected_result;
};

class TimestamptzBinTest
    : public PgEvaluatorTestWithParam<TimestamptzBinTestCase> {
 protected:
  void SetUp() override {
    PgEvaluatorTestWithParam<TimestamptzBinTestCase>::SetUp();
    ZETASQL_ASSERT_OK(InitTimezone(kDefaultTimezone));
  }

  void TearDown() override {
    PgEvaluatorTestWithParam<TimestamptzBinTestCase>::TearDown();
    CleanupPostgresDateTimeCache();
    CleanupTimezone();
  }
};

TEST_P(TimestamptzBinTest, TimestamptzBin) {
  const TimestamptzBinTestCase& test_case = GetParam();
  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Time source_input,
                       PgTimestamptzIn(test_case.source));
  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Time origin_input,
                       PgTimestamptzIn(test_case.origin));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      absl::Time computed_time,
      PgTimestamptzBin(test_case.interval, source_input, origin_input));

  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Time expected_timestamptz,
                       PgTimestamptzIn(test_case.expected_result));
  EXPECT_EQ(expected_timestamptz, computed_time);
}

INSTANTIATE_TEST_SUITE_P(
    TimestamptzBinTestSuite, TimestamptzBinTest,
    testing::Values(
        // Bin timestamps into arbitrary intervals
        TimestamptzBinTestCase({
            "15 days",
            "Feb 11 15:44:17.71393 2020",
            "Jan 01 00:00:00 2001",
            "Feb 06 00:00:00 2020",
        }),
        TimestamptzBinTestCase({"2 hours", "Feb 11 15:44:17.71393 2020",
                                "Jan 01 00:00:00 2001",
                                "Feb 11 14:00:00 2020"}),
        TimestamptzBinTestCase({"1 hour 30 min", "Feb 11 15:44:17.71393 2020",
                                "Jan 01 00:00:00 2001",
                                "Feb 11 15:00:00 2020"}),
        TimestamptzBinTestCase({"15 minutes", "Feb 11 15:44:17.71393 2020",
                                "Jan 01 00:00:00 2001",
                                "Feb 11 15:30:00 2020"}),
        TimestamptzBinTestCase({"10 seconds", "Feb 11 15:44:17.71393 2020",
                                "Jan 01 00:00:00 2001",
                                "Feb 11 15:44:10 2020"}),
        TimestamptzBinTestCase(
            {"100 milliseconds", "Feb 11 15:44:17.71393 2020",
             "Jan 01 00:00:00 2001", "Feb 11 15:44:17.7 2020"}),
        TimestamptzBinTestCase(
            {"250 microseconds", "Feb 11 15:44:17.71393 2020",
             "Jan 01 00:00:00 2001", "Feb 11 15:44:17.71375 2020"}),
        // Shift bins using the origin parameter.
        TimestamptzBinTestCase({"5 min", "2020-02-01 01:01:01+00",
                                "2020-02-01 00:02:30+00",
                                "Jan 31 16:57:30 2020"})));

struct TimestamptzBinErrorTestCase {
  TimestamptzBinErrorTestCase(std::string interval_in, std::string source_in,
                              std::string origin_in,
                              absl::StatusCode status_code_in,
                              std::string expected_error_in)
      : interval(interval_in),
        source(source_in),
        origin(origin_in),
        status_code(status_code_in),
        expected_error(expected_error_in) {}

  std::string interval;
  std::string source;
  std::string origin;
  absl::StatusCode status_code;
  std::string expected_error;
};

class TimestamptzBinErrorTest
    : public PgEvaluatorTestWithParam<TimestamptzBinErrorTestCase> {
 protected:
  void SetUp() override {
    PgEvaluatorTestWithParam<TimestamptzBinErrorTestCase>::SetUp();
    ZETASQL_ASSERT_OK(InitTimezone(kDefaultTimezone));
  }

  void TearDown() override {
    PgEvaluatorTestWithParam<TimestamptzBinErrorTestCase>::TearDown();
    CleanupPostgresDateTimeCache();
    CleanupTimezone();
  }
};

TEST_P(TimestamptzBinErrorTest, TimestamptzBin) {
  const TimestamptzBinErrorTestCase& test_case = GetParam();
  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Time source_input,
                       PgTimestamptzIn(test_case.source));
  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Time origin_input,
                       PgTimestamptzIn(test_case.origin));
  EXPECT_THAT(
      PgTimestamptzBin(test_case.interval, source_input, origin_input),
      StatusIs(test_case.status_code, HasSubstr(test_case.expected_error)));
}

INSTANTIATE_TEST_SUITE_P(
    TimestamptzBinErrorTestSuite, TimestamptzBinErrorTest,
    testing::Values(
        // Disallow intervals with months or years.
        TimestamptzBinErrorTestCase({"5 months", "2020-02-01 01:01:01+00",
                                     "2001-01-01+00",
                                     absl::StatusCode::kUnimplemented,
                                     "timestamps cannot be binned into "
                                     "intervals containing months or years"}),
        TimestamptzBinErrorTestCase({"5 years", "2020-02-01 01:01:01+00",
                                     "2001-01-01+00",
                                     absl::StatusCode::kUnimplemented,
                                     "timestamps cannot be binned into "
                                     "intervals containing months or years"}),
        // Disallow zero intervals.
        TimestamptzBinErrorTestCase({"0 days", "1970-01-01 01:00:00+00",
                                     "1970-01-01 00:00:00+00",
                                     absl::StatusCode::kInvalidArgument,
                                     "stride must be greater than zero"}),
        // Disallow negative intervals.
        TimestamptzBinErrorTestCase({"-2 days", "1970-01-01 01:00:00+00",
                                     "1970-01-01 00:00:00+00",
                                     absl::StatusCode::kInvalidArgument,
                                     "stride must be greater than zero"})));

}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
