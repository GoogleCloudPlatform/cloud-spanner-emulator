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

#include "spanner/public/type.h"
#include "zetasql/public/interval_value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/cord.h"
#include "third_party/spanner_pg/datatypes/extended/pg_numeric_type.h"
#include "third_party/spanner_pg/function_evaluators/tests/test_base.h"
#include "third_party/spanner_pg/interface/datetime_evaluators.h"
#include "third_party/spanner_pg/numeric_arithmetic/round.h"
#include "third_party/spanner_pg/shims/timezone_helper.h"
#include "util/math/mathutil.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator::function_evaluators {
namespace {

constexpr char kDefaultTimezone[] = "America/Los_Angeles";

using ::spangres::numeric_arithmetic::RoundToInteger;
using ::spanner::datatypes::common::DecodeNumericValueToDecimalString;
using ::testing::HasSubstr;
using ::zetasql_base::testing::StatusIs;

struct TimestamptzPartTestCase {
  TimestamptzPartTestCase(
      std::string source_in,
      absl::flat_hash_map<std::string, double> expected_values_in,
      int julian_in)
      : source(source_in),
        expected_values(expected_values_in),
        julian(julian_in) {}

  std::string source;
  absl::flat_hash_map<std::string, double> expected_values;
  int julian;
};

class TimestamptzPartTest
    : public PgEvaluatorTestWithParam<TimestamptzPartTestCase> {
 protected:
  void SetUp() override {
    PgEvaluatorTestWithParam<TimestamptzPartTestCase>::SetUp();
    ZETASQL_ASSERT_OK(InitTimezone(kDefaultTimezone));
  }

  void TearDown() override {
    PgEvaluatorTestWithParam<TimestamptzPartTestCase>::TearDown();
    CleanupPostgresDateTimeCache();
    CleanupTimezone();
  }
};

TEST_P(TimestamptzPartTest, TimestamptzPart) {
  const TimestamptzPartTestCase& test_case = GetParam();
  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Time source_input,
                       PgTimestamptzIn(test_case.source));

  for (const auto& test_pair : test_case.expected_values) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(double computed_result,
                         PgTimestamptzPart(test_pair.first, source_input));
    EXPECT_EQ(test_pair.second, computed_result);
  }

  // Round for julian.
  ZETASQL_ASSERT_OK_AND_ASSIGN(double computed_result,
                       PgTimestamptzPart("julian", source_input));
  EXPECT_EQ(test_case.julian, MathUtil::Round<int>(computed_result));
}

// A few test cases from pg_regress timestamptz.out .
INSTANTIATE_TEST_SUITE_P(
    TimestamptzPartTestSuite, TimestamptzPartTest,
    testing::Values(TimestamptzPartTestCase({"Dec 31 16:00:00 1969",
                                             {{"year", 1969},
                                              {"month", 12},
                                              {"day", 31},
                                              {"hour", 16},
                                              {"minute", 0},
                                              {"second", 0},
                                              {"quarter", 4},
                                              {"msec", 0},
                                              {"usec", 0},
                                              {"isoyear", 1970},
                                              {"week", 1},
                                              {"isodow", 3},
                                              {"dow", 3},
                                              {"doy", 365},
                                              {"decade", 196},
                                              {"century", 20},
                                              {"millennium", 2},
                                              {"epoch", 0},
                                              {"timezone", -28800},
                                              {"timezone_hour", -8},
                                              {"timezone_minute", 0}},
                                             2440588}),
                    TimestamptzPartTestCase({"Feb 10 17:32:01.6 1997",
                                             {{"year", 1997},
                                              {"month", 2},
                                              {"day", 10},
                                              {"hour", 17},
                                              {"minute", 32},
                                              {"second", 1.6},
                                              {"quarter", 1},
                                              {"msec", 1600},
                                              {"usec", 1600000},
                                              {"isoyear", 1997},
                                              {"week", 7},
                                              {"isodow", 1},
                                              {"dow", 1},
                                              {"doy", 41},
                                              {"decade", 199},
                                              {"century", 20},
                                              {"millennium", 2},
                                              {"epoch", 855624721.6},
                                              {"timezone", -28800},
                                              {"timezone_hour", -8},
                                              {"timezone_minute", 0}},
                                             2450491}),
                    TimestamptzPartTestCase({"Feb 16 17:32:01 1897",
                                             {{"year", 1897},
                                              {"month", 2},
                                              {"day", 16},
                                              {"hour", 17},
                                              {"minute", 32},
                                              {"second", 1},
                                              {"quarter", 1},
                                              {"msec", 1000},
                                              {"usec", 1000000},
                                              {"isoyear", 1897},
                                              {"week", 7},
                                              {"isodow", 2},
                                              {"dow", 2},
                                              {"doy", 47},
                                              {"decade", 189},
                                              {"century", 19},
                                              {"millennium", 2},
                                              {"epoch", -2299530479},
                                              {"timezone", -28800},
                                              {"timezone_hour", -8},
                                              {"timezone_minute", 0}},
                                             2413973})));

// The extract implementation is mostly the same as date_part, so pg_regress
// only has a few cases for additional coverage.
struct TimestamptzExtractTestCase {
  TimestamptzExtractTestCase(std::string source_in, std::string microseconds,
                             std::string milliseconds, std::string seconds,
                             std::string epoch, int julian_in)
      : source(source_in), julian(julian_in) {
    expected_values["microseconds"] = microseconds;
    expected_values["milliseconds"] = milliseconds;
    expected_values["seconds"] = seconds;
    expected_values["epoch"] = epoch;
  }

  std::string source;
  int julian;
  absl::flat_hash_map<std::string, std::string> expected_values;
};

class TimestamptzExtractTest
    : public PgEvaluatorTestWithParam<TimestamptzExtractTestCase> {
 protected:
  void SetUp() override {
    PgEvaluatorTestWithParam<TimestamptzExtractTestCase>::SetUp();
    ZETASQL_ASSERT_OK(InitTimezone(kDefaultTimezone));
  }

  void TearDown() override {
    PgEvaluatorTestWithParam<TimestamptzExtractTestCase>::TearDown();
    CleanupPostgresDateTimeCache();
    CleanupTimezone();
  }
};

TEST_P(TimestamptzExtractTest, TimestamptzExtract) {
  const TimestamptzExtractTestCase& test_case = GetParam();
  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Time source_input,
                       PgTimestamptzIn(test_case.source));
  for (const auto& test_pair : test_case.expected_values) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Cord computed_result,
                         PgTimestamptzExtract(test_pair.first, source_input));
    std::string readable_numeric;

    readable_numeric.reserve(computed_result.size());
    absl::CopyCordToString(computed_result, &readable_numeric);
    EXPECT_EQ(readable_numeric, test_pair.second);
  }

  // Round for julian.
  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Cord computed_result,
                       PgTimestamptzExtract("julian", source_input));
  int32_t computed_julian;
  ZETASQL_ASSERT_OK(RoundToInteger(computed_result.Flatten(), &computed_julian));
  EXPECT_EQ(computed_julian, test_case.julian);
}

// Test cases from pg_regress timestamptz.out
INSTANTIATE_TEST_SUITE_P(
    TimestamptzExtractTestSuite, TimestamptzExtractTest,
    testing::Values(
        TimestamptzExtractTestCase({"Feb 10 17:32:01.4 1997", "1400000",
                                    "1400.000", "1.400000", "855624721.400000",
                                    2450491}),
        TimestamptzExtractTestCase({"Sep 22 18:19:20 2001", "20000000",
                                    "20000.000", "20.000000",
                                    "1001207960.000000", 2452176}),
        TimestamptzExtractTestCase({"Feb 16 17:32:01 1897", "1000000",
                                    "1000.000", "1.000000",
                                    "-2299530479.000000", 2413973})));

struct DateExtractTestCase {
  DateExtractTestCase(
      std::string source_in,
      absl::flat_hash_map<std::string, std::string> expected_values_in)
      : source(source_in), expected_values(expected_values_in) {}

  std::string source;
  absl::flat_hash_map<std::string, std::string> expected_values;
};

class DateExtractTest : public PgEvaluatorTestWithParam<DateExtractTestCase> {
 protected:
  void SetUp() override {
    PgEvaluatorTestWithParam<DateExtractTestCase>::SetUp();
    ZETASQL_ASSERT_OK(InitTimezone(kDefaultTimezone));
  }

  void TearDown() override {
    PgEvaluatorTestWithParam<DateExtractTestCase>::TearDown();
    CleanupPostgresDateTimeCache();
    CleanupTimezone();
  }
};

TEST_P(DateExtractTest, DateExtract) {
  const DateExtractTestCase& test_case = GetParam();
  ZETASQL_ASSERT_OK_AND_ASSIGN(int32_t source_input, PgDateIn(test_case.source));
  for (const auto& test_pair : test_case.expected_values) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Cord computed_result,
                         PgDateExtract(test_pair.first, source_input));
    std::string readable_numeric;

    readable_numeric.reserve(computed_result.size());
    absl::CopyCordToString(computed_result, &readable_numeric);
    EXPECT_EQ(readable_numeric, test_pair.second);
  }
}

// Test cases from pg_regress timestamptz.out
INSTANTIATE_TEST_SUITE_P(
    DateExtractTestSuite, DateExtractTest,
    testing::Values(
        DateExtractTestCase({"1970-01-01", {{"epoch", "0"}}}),
        DateExtractTestCase({"1900-12-31", {{"century", "19"}}}),
        DateExtractTestCase({"1901-01-01", {{"century", "20"}}}),
        DateExtractTestCase({"2000-12-31",
                             {{"century", "20"}, {"MILLENNIUM", "2"}}}),
        DateExtractTestCase({"2001-01-01",
                             {{"century", "21"}, {"MILLENNIUM", "3"}}}),
        DateExtractTestCase({"1994-12-25", {{"decade", "199"}}}),
        DateExtractTestCase({"2020-08-11",
                             {{"month", "8"},
                              {"year", "2020"},
                              {"decade", "202"},
                              {"century", "21"},
                              {"millennium", "3"},
                              {"isoyear", "2020"},
                              {"quarter", "3"},
                              {"week", "33"},
                              {"dow", "2"},
                              {"isodow", "2"},
                              {"doy", "224"},
                              {"epoch", "1597104000"},
                              {"julian", "2459073"}}})));

struct DateExtractErrorTestCase {
  DateExtractErrorTestCase(
      std::string source_in,
      absl::flat_hash_set<std::string> unsupported_fields_in)
      : source(source_in), unsupported_fields(unsupported_fields_in) {}

  std::string source;
  absl::flat_hash_set<std::string> unsupported_fields;
};

class DateExtractErrorTest
    : public PgEvaluatorTestWithParam<DateExtractErrorTestCase> {
 protected:
  void SetUp() override {
    PgEvaluatorTestWithParam<DateExtractErrorTestCase>::SetUp();
    ZETASQL_ASSERT_OK(InitTimezone(kDefaultTimezone));
  }

  void TearDown() override {
    PgEvaluatorTestWithParam<DateExtractErrorTestCase>::TearDown();
    CleanupPostgresDateTimeCache();
    CleanupTimezone();
  }
};

TEST_P(DateExtractErrorTest, DateExtractError) {
  const DateExtractErrorTestCase& test_case = GetParam();
  ZETASQL_ASSERT_OK_AND_ASSIGN(int32_t source_input, PgDateIn(test_case.source));
  for (const auto& unsupported_field : test_case.unsupported_fields) {
    EXPECT_THAT(PgDateExtract(unsupported_field, source_input),
                StatusIs(absl::StatusCode::kUnimplemented,
                         HasSubstr(absl::StrFormat(
                             "unit \"%s\" not supported for type date",
                             unsupported_field))));
  }
}

// Test cases from pg_regress timestamptz.out
INSTANTIATE_TEST_SUITE_P(DateExtractErrorTestSuite, DateExtractErrorTest,
                         testing::Values(DateExtractErrorTestCase(
                             {"1970-01-01",
                              {"timezone", "timezone_m", "timezone_h",
                               "microseconds", "milliseconds", "second",
                               "minute", "hour"}})));
}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
