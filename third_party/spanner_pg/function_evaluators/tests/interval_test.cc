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

#include <cstdint>
#include <limits>
#include <memory>
#include <string>

#include "zetasql/public/interval_value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/function_evaluators/tests/test_base.h"
#include "third_party/spanner_pg/interface/datetime_evaluators.h"
#include "third_party/spanner_pg/shims/timezone_helper.h"

namespace postgres_translator::function_evaluators {
namespace {

constexpr char kDefaultTimezone[] = "America/Los_Angeles";

zetasql::IntervalValue GsqlInterval(absl::string_view interval) {
  return *zetasql::IntervalValue::ParseFromString(interval,
                                                    /*allow_nanos=*/true);
}

template <typename T>
class IntervalTestBase : public PgEvaluatorTestWithParam<T> {
 protected:
  void SetUp() override {
    PgEvaluatorTestWithParam<T>::SetUp();
    ZETASQL_ASSERT_OK(InitTimezone(kDefaultTimezone));
  }

  void TearDown() override {
    PgEvaluatorTestWithParam<T>::TearDown();
    CleanupPostgresDateTimeCache();
    CleanupTimezone();
  }
};

struct MakeIntervalTestCase {
  int64_t years;
  int64_t months;
  int64_t weeks;
  int64_t days;
  int64_t hours;
  int64_t minutes;
  double seconds;
  std::string expected_interval;
  absl::StatusCode expected_status = absl::StatusCode::kOk;
};

class MakeIntervalTest : public IntervalTestBase<MakeIntervalTestCase> {};

TEST_P(MakeIntervalTest, MakeInterval) {
  const MakeIntervalTestCase& testcase = GetParam();

  absl::StatusOr<zetasql::IntervalValue> actual_interval = PgMakeInterval(
      testcase.years, testcase.months, testcase.weeks, testcase.days,
      testcase.hours, testcase.minutes, testcase.seconds);
  if (testcase.expected_status != absl::StatusCode::kOk) {
    EXPECT_THAT(actual_interval,
                zetasql_base::testing::StatusIs(testcase.expected_status));
    return;
  }

  ZETASQL_ASSERT_OK(actual_interval);
  ZETASQL_ASSERT_OK_AND_ASSIGN(zetasql::IntervalValue expected_interval,
                       PgIntervalIn(testcase.expected_interval));
  EXPECT_EQ(*actual_interval, expected_interval);
}

INSTANTIATE_TEST_SUITE_P(
    MakeIntervalTestSuite, MakeIntervalTest,
    testing::Values(
        MakeIntervalTestCase{0, 0, 0, 0, 0, 0, 0, "0-0 0 0:0:0"},
        MakeIntervalTestCase{0, 120000, 0, 3660000, 0, 0, 316224000000.000000,
                             "10000-0 3660000 87840000:0:0"},
        MakeIntervalTestCase{0, -120000, 0, -3660000, 0, 0,
                             -316224000000.000000,
                             "-10000-0 -3660000 -87840000:0:0"},
        MakeIntervalTestCase{1, 1, 1, 1, 1, 1, 1.123456789,
                             "1-1 8 1:1:1.123457"},
        MakeIntervalTestCase{1, 1, 1, 1, 1, 1, 1.1234565, "1-1 8 1:1:1.123456"},
        MakeIntervalTestCase{-1, -1, -1, -1, -1, -1, -1.123456,
                             "-1-1 -8 -1:1:1.123456"},
        MakeIntervalTestCase{1, -2, 3, -4, 5, -6, 7.5678765,
                             "0-10 17 4:54:7.567876"},
        MakeIntervalTestCase{-1, 2, -3, 4, -5, 6, -7.5678765,
                             "-0-10 -17 -4:54:7.567876"},
        MakeIntervalTestCase{2, 38, 53, 378, 100, 94, -7200.5678765,
                             "5-2 749 99:33:59.432124"},
        MakeIntervalTestCase{0, 120001, 0, 3660000, 0, 0, 316224000000.000000,
                             "", absl::StatusCode::kInvalidArgument},
        MakeIntervalTestCase{0, -120001, 0, 3660000, 0, 0, 316224000000.000000,
                             "", absl::StatusCode::kInvalidArgument},
        MakeIntervalTestCase{-1, -1, -1, -1, -1, -1,
                             std::numeric_limits<double>::infinity(), "",
                             absl::StatusCode::kInvalidArgument},
        MakeIntervalTestCase{-1, -1, -1, -1, -1, -1,
                             std::numeric_limits<double>::quiet_NaN(), "",
                             absl::StatusCode::kInvalidArgument},
        MakeIntervalTestCase{std::numeric_limits<int32_t>::max(), 1, 1, 1, 1, 1,
                             1.123456789, "",
                             absl::StatusCode::kInvalidArgument},
        MakeIntervalTestCase{std::numeric_limits<int32_t>::min(), 1, 1, 1, 1, 1,
                             1.123456789, "",
                             absl::StatusCode::kInvalidArgument},
        MakeIntervalTestCase{1, 1, 1, 1, std::numeric_limits<int32_t>::max(),
                             std::numeric_limits<int32_t>::max(),
                             std::numeric_limits<double>::max(), "",
                             absl::StatusCode::kInvalidArgument},
        MakeIntervalTestCase{1, 1, 1, 1, std::numeric_limits<int64_t>::max(), 1,
                             std::numeric_limits<double>::max(), "",
                             absl::StatusCode::kInvalidArgument},
        MakeIntervalTestCase{1, 1, 1, std::numeric_limits<int64_t>::min(), 1, 1,
                             std::numeric_limits<double>::max(), "",
                             absl::StatusCode::kInvalidArgument}));

struct IntervalMultiplyTestCase {
  zetasql::IntervalValue input_interval;
  double scale;
  zetasql::IntervalValue expected_interval;
  absl::StatusCode expected_status = absl::StatusCode::kOk;
};

class IntervalMultiplyTest : public IntervalTestBase<IntervalMultiplyTestCase> {
};

TEST_P(IntervalMultiplyTest, IntervalMultiply) {
  const IntervalMultiplyTestCase& testcase = GetParam();

  absl::StatusOr<zetasql::IntervalValue> actual_interval =
      PgIntervalMultiply(testcase.input_interval, testcase.scale);
  if (testcase.expected_status != absl::StatusCode::kOk) {
    EXPECT_THAT(actual_interval,
                zetasql_base::testing::StatusIs(testcase.expected_status));
    return;
  }

  ZETASQL_ASSERT_OK(actual_interval);
  EXPECT_EQ(*actual_interval, testcase.expected_interval);
}

INSTANTIATE_TEST_SUITE_P(
    IntervalMultiplyTestSuite, IntervalMultiplyTest,
    testing::Values(
        IntervalMultiplyTestCase{zetasql::IntervalValue::MaxValue(), 1.0,
                                 zetasql::IntervalValue::MaxValue()},
        IntervalMultiplyTestCase{zetasql::IntervalValue::MaxValue(), -1.0,
                                 zetasql::IntervalValue::MinValue()},
        IntervalMultiplyTestCase{zetasql::IntervalValue::MinValue(), 1.0,
                                 zetasql::IntervalValue::MinValue()},
        IntervalMultiplyTestCase{zetasql::IntervalValue::MinValue(), -1.0,
                                 zetasql::IntervalValue::MaxValue()},
        IntervalMultiplyTestCase{zetasql::IntervalValue::MinValue(), 0.0,
                                 zetasql::IntervalValue()},
        IntervalMultiplyTestCase{GsqlInterval("2-2 2 2:2:2.23456789"), 2.5,
                                 GsqlInterval("5-5 5 5:5:5.58642")},
        IntervalMultiplyTestCase{GsqlInterval("2-2 2 2:2:2.23456789"), -2.5,
                                 GsqlInterval("-5-5 -5 -5:5:5.58642")},
        IntervalMultiplyTestCase{GsqlInterval("2-2 2 2:2:2.23456"), -3.1456789,
                                 GsqlInterval("-6-9 -29 -28:29:59.141476")},
        IntervalMultiplyTestCase{GsqlInterval("2-2 2 2:2:2.23456"), 3.1456789,
                                 GsqlInterval("6-9 29 28:29:59.141476")},
        IntervalMultiplyTestCase{GsqlInterval("-1-1 8 -1:1:1.123456"), 1000.0,
                                 GsqlInterval("-1083-4 8000 -1016:58:43.456")},
        IntervalMultiplyTestCase{zetasql::IntervalValue::MaxValue(), 2.0,
                                 zetasql::IntervalValue(),
                                 absl::StatusCode::kInvalidArgument},
        IntervalMultiplyTestCase{zetasql::IntervalValue::MaxValue(), -2.0,
                                 zetasql::IntervalValue(),
                                 absl::StatusCode::kInvalidArgument},
        IntervalMultiplyTestCase{GsqlInterval("2-2 2 2:2:2.23456"),
                                 std::numeric_limits<double>::infinity(),
                                 zetasql::IntervalValue(),
                                 absl::StatusCode::kInvalidArgument},
        IntervalMultiplyTestCase{GsqlInterval("2-2 2 2:2:2.23456"),
                                 std::numeric_limits<double>::quiet_NaN(),
                                 zetasql::IntervalValue(),
                                 absl::StatusCode::kInvalidArgument}));

struct IntervalDivideTestCase {
  zetasql::IntervalValue input_interval;
  double scale;
  zetasql::IntervalValue expected_interval;
  absl::StatusCode expected_status = absl::StatusCode::kOk;
};

class IntervalDivideTest : public IntervalTestBase<IntervalDivideTestCase> {};

TEST_P(IntervalDivideTest, IntervalDivide) {
  const IntervalDivideTestCase& testcase = GetParam();
  absl::StatusOr<zetasql::IntervalValue> actual_interval =
      PgIntervalDivide(testcase.input_interval, testcase.scale);
  if (testcase.expected_status != absl::StatusCode::kOk) {
    EXPECT_THAT(actual_interval,
                zetasql_base::testing::StatusIs(testcase.expected_status));
    return;
  }
  ZETASQL_ASSERT_OK(actual_interval);
  EXPECT_EQ(*actual_interval, testcase.expected_interval);
}

INSTANTIATE_TEST_SUITE_P(
    IntervalDivideTestSuite, IntervalDivideTest,
    testing::Values(
        IntervalDivideTestCase{zetasql::IntervalValue::MaxValue(), 1.0,
                               zetasql::IntervalValue::MaxValue()},
        IntervalDivideTestCase{zetasql::IntervalValue::MaxValue(), -1.0,
                               zetasql::IntervalValue::MinValue()},
        IntervalDivideTestCase{zetasql::IntervalValue::MinValue(), 1.0,
                               zetasql::IntervalValue::MinValue()},
        IntervalDivideTestCase{zetasql::IntervalValue::MinValue(), -1.0,
                               zetasql::IntervalValue::MaxValue()},
        IntervalDivideTestCase{GsqlInterval("2-2 2 2:2:2.23456789"), 2.5,
                               GsqlInterval("0-10 12 20:00:48.893827")},
        IntervalDivideTestCase{GsqlInterval("2-2 2 2:2:2.23456789"), -2.5,
                               GsqlInterval("-0-10 -12 -20:00:48.893827")},
        IntervalDivideTestCase{GsqlInterval("2-2 2 2:2:2.23456"), -3.1456789,
                               GsqlInterval("-0-8 -8 -14:55:34.485249")},
        IntervalDivideTestCase{GsqlInterval("2-2 2 2:2:2.23456"), 3.1456789,
                               GsqlInterval("0-8 8 14:55:34.485249")},
        IntervalDivideTestCase{GsqlInterval("-1-1 8 -1:1:1.123456"), 1000.0,
                               GsqlInterval("-09:10:08.461123")},
        IntervalDivideTestCase{GsqlInterval("2-2 2 2:2:2.23456"),
                               std::numeric_limits<double>::infinity(),
                               zetasql::IntervalValue()},
        IntervalDivideTestCase{GsqlInterval("2-2 2 2:2:2.23456"),
                               -std::numeric_limits<double>::infinity(),
                               zetasql::IntervalValue()},
        IntervalDivideTestCase{zetasql::IntervalValue::MaxValue(), 0.0,
                               zetasql::IntervalValue(),
                               absl::StatusCode::kOutOfRange},
        IntervalDivideTestCase{GsqlInterval("2-2 2 2:2:2.23456"),
                               std::numeric_limits<double>::quiet_NaN(),
                               zetasql::IntervalValue(),
                               absl::StatusCode::kInvalidArgument}));

struct IntervalFromStringTestCase {
  std::string input_interval;
  zetasql::IntervalValue expected_interval;
  absl::StatusCode expected_status = absl::StatusCode::kOk;
};

class IntervalFromStringTest
    : public IntervalTestBase<IntervalFromStringTestCase> {};

TEST_P(IntervalFromStringTest, IntervalFromString) {
  const IntervalFromStringTestCase& testcase = GetParam();
  absl::StatusOr<zetasql::IntervalValue> actual_interval =
      PgIntervalIn(testcase.input_interval);
  if (testcase.expected_status != absl::StatusCode::kOk) {
    EXPECT_THAT(actual_interval,
                zetasql_base::testing::StatusIs(testcase.expected_status));
    return;
  }
  ZETASQL_ASSERT_OK(actual_interval);
  EXPECT_EQ(*actual_interval, testcase.expected_interval);
}

INSTANTIATE_TEST_SUITE_P(
    IntervalFromStringTestSuite, IntervalFromStringTest,
    testing::Values(
        IntervalFromStringTestCase{"0-0 0 0:0:0", zetasql::IntervalValue()},
        IntervalFromStringTestCase{"1-1 8 1:1:1.123456789",
                                   GsqlInterval("1-1 8 1:1:1.123457")},
        IntervalFromStringTestCase{"-1-1 -8 -1:1:1.123456789",
                                   GsqlInterval("-1-1 -8 -1:1:1.123457")},
        IntervalFromStringTestCase{
            "1 year 2 mons 3 weeks 4 days 5 hours 6 mins 7.567876589 seconds",
            GsqlInterval("1-2 25 5:6:7.567877")},
        IntervalFromStringTestCase{"P1.5Y2M3.8W4DT5H6M7.5678765S",
                                   GsqlInterval("1-8 30 19:30:07.567876")},
        IntervalFromStringTestCase{
            "@ 1 year 2 mons -3 days 4 hours 5 mins 6 secs ago",
            GsqlInterval("-1-2 3 -4:5:6")},
        IntervalFromStringTestCase{"-1-2 +3 -4:05:06",
                                   GsqlInterval("-1-2 3 -4:5:6")},
        IntervalFromStringTestCase{"test_string", zetasql::IntervalValue(),
                                   absl::StatusCode::kInvalidArgument},
        IntervalFromStringTestCase{"120000 years", zetasql::IntervalValue(),
                                   absl::StatusCode::kInvalidArgument}));

struct IntervalToStringTestCase {
  zetasql::IntervalValue input_interval;
  std::string expected_interval;
};

class IntervalToStringTest : public IntervalTestBase<IntervalToStringTestCase> {
};

TEST_P(IntervalToStringTest, IntervalToString) {
  const IntervalToStringTestCase& testcase = GetParam();
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::string actual_interval_str,
                       PgIntervalOut(testcase.input_interval));
  EXPECT_THAT(actual_interval_str, testcase.expected_interval);
}

INSTANTIATE_TEST_SUITE_P(
    IntervalToStringTestSuite, IntervalToStringTest,
    testing::Values(
        IntervalToStringTestCase{zetasql::IntervalValue(), "00:00:00"},
        IntervalToStringTestCase{zetasql::IntervalValue::MaxValue(),
                                 "10000 years 3660000 days 87840000:00:00"},
        IntervalToStringTestCase{zetasql::IntervalValue::MinValue(),
                                 "-10000 years -3660000 days -87840000:00:00"},
        IntervalToStringTestCase{GsqlInterval("1-1 8 1:1:1.123456789"),
                                 "1 year 1 mon 8 days 01:01:01.123457"},
        IntervalToStringTestCase{GsqlInterval("1-1 -8 1:1:1.123456789"),
                                 "1 year 1 mon -8 days +01:01:01.123457"},
        IntervalToStringTestCase{GsqlInterval("-2-2 16 1:1:1.123456789"),
                                 "-2 years -2 mons +16 days 01:01:01.123457"},
        IntervalToStringTestCase{GsqlInterval("0-12 30 0:75:89.456756674"),
                                 "1 year 30 days 01:16:29.456757"},
        IntervalToStringTestCase{GsqlInterval("0-0 0 0:32:32.456756674"),
                                 "00:32:32.456757"}));

struct IntervalToCharTestCase {
  zetasql::IntervalValue input_interval;
  std::string format;
  std::string expected_interval;
  absl::StatusCode expected_status = absl::StatusCode::kOk;
};

class IntervalToCharTest : public IntervalTestBase<IntervalToCharTestCase> {};

TEST_P(IntervalToCharTest, IntervalToChar) {
  const IntervalToCharTestCase& testcase = GetParam();
  absl::StatusOr<std::unique_ptr<std::string>> result =
      PgIntervalToChar(testcase.input_interval, testcase.format);
  if (testcase.expected_status != absl::StatusCode::kOk) {
    EXPECT_THAT(result, zetasql_base::testing::StatusIs(testcase.expected_status));
    return;
  }
  EXPECT_THAT(result, zetasql_base::testing::IsOkAndHolds(
                          Pointee(testing::StrEq(testcase.expected_interval))));
}

INSTANTIATE_TEST_SUITE_P(
    IntervalToCharTestSuite, IntervalToCharTest,
    testing::Values(
        IntervalToCharTestCase{zetasql::IntervalValue(),
                               "YYYY-MM-DD HH:MI:SS", "0000-00-00 12:00:00"},
        IntervalToCharTestCase{zetasql::IntervalValue::MaxValue(),
                               "YYYY-MM-DD HH12:MI:SS",
                               "10000-00-3660000 12:00:00"},
        IntervalToCharTestCase{zetasql::IntervalValue::MinValue(),
                               "YYYY-MM-DD HH24:MI:SS",
                               "-10000-00--3660000 -87840000:00:00"},
        IntervalToCharTestCase{GsqlInterval("1-1 8 1:1:1.123456789"),
                               "YYYY-MM-DD HH12:MI SSSS",
                               "0001-01-08 01:01 3661"},
        IntervalToCharTestCase{GsqlInterval("1-1 -8 1:1:1.123456789"),
                               "YYYY-MM-DD HH12:MI US",
                               "0001-01--8 01:01 123457"},
        // Error scenarios - Interval is not tied to calendar date, so any
        // format which requires calendar date should fail.
        IntervalToCharTestCase{GsqlInterval("-2-2 16 1:1:1.123456789"),
                               "Mon-DD-YYYY", "",
                               absl::StatusCode::kInvalidArgument},
        IntervalToCharTestCase{GsqlInterval("0-12 30 0:75:89.456756674"),
                               "D-YYYY", "",
                               absl::StatusCode::kInvalidArgument},
        IntervalToCharTestCase{GsqlInterval("1-7 9 0:32:32.456756674"),
                               "Dy-YY", "",
                               absl::StatusCode::kInvalidArgument}));

}  // namespace

}  // namespace postgres_translator::function_evaluators
