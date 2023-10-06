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

struct TimestamptzTruncTestCase {
  TimestamptzTruncTestCase(std::string field_in, std::string source_in,
                           std::string timezone_in,
                           std::string expected_result_in)
      : field(field_in),
        source(source_in),
        timezone(timezone_in),
        expected_result(expected_result_in) {}

  std::string field;
  std::string source;
  std::string timezone;
  std::string expected_result;
};

class TimestamptzTruncTest
    : public PgEvaluatorTestWithParam<TimestamptzTruncTestCase> {
 protected:
  void SetUp() override {
    PgEvaluatorTestWithParam<TimestamptzTruncTestCase>::SetUp();
    ZETASQL_ASSERT_OK(InitTimezone(kDefaultTimezone));
  }

  void TearDown() override {
    PgEvaluatorTestWithParam<TimestamptzTruncTestCase>::TearDown();
    CleanupPostgresDateTimeCache();
    CleanupTimezone();
  }
};

TEST_P(TimestamptzTruncTest, TimestamptzTrunc) {
  const TimestamptzTruncTestCase& test_case = GetParam();
  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Time source_input,
                       PgTimestamptzIn(test_case.source));
  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Time expected_timestamptz,
                       PgTimestamptzIn(test_case.expected_result));

  if (test_case.timezone.empty()) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Time computed_time,
                         PgTimestamptzTrunc(test_case.field, source_input));
    EXPECT_EQ(expected_timestamptz, computed_time);
  } else {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        absl::Time computed_time,
        PgTimestamptzTrunc(test_case.field, source_input, test_case.timezone));
    EXPECT_EQ(expected_timestamptz, computed_time);
  }
}

// All test cases are from pg_regress.
INSTANTIATE_TEST_SUITE_P(
    TimestamptzTruncTestSuite, TimestamptzTruncTest,
    testing::Values(
        TimestamptzTruncTestCase({"week", "2004-02-29 15:44:17.71393",
                                  /*timezone_in=*/"", "Feb 23 00:00:00 2004"}),
        TimestamptzTruncTestCase({"day", "2001-02-16 20:38:40+00",
                                  "Australia/Sydney", "Feb 16 05:00:00 2001"}),
        TimestamptzTruncTestCase({"century", "1970-03-20 04:30:00.00000",
                                  /*timezone_in=*/"", "Jan 01 00:00:00 1901"}),
        TimestamptzTruncTestCase({"CENTURY", "2004-08-10",
                                  /*timezone_in=*/"", "Jan 01 00:00:00 2001"}),
        TimestamptzTruncTestCase({"decade", "1993-12-25",
                                  /*timezone_in=*/"", "Jan 01 00:00:00 1990"})));

}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
