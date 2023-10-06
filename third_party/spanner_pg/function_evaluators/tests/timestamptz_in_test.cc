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

#include "zetasql/public/types/timestamp_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "third_party/spanner_pg/function_evaluators/tests/test_base.h"
#include "third_party/spanner_pg/interface/datetime_evaluators.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/timezone_helper.h"

namespace postgres_translator::function_evaluators {
namespace {

inline constexpr char kDefaultTimezone[] = "UTC";

using ::zetasql::types::kTimestampMax;
using ::zetasql::types::kTimestampMin;
using ::postgres_translator::CleanupTimezone;
using ::postgres_translator::InitTimezone;
using ::testing::HasSubstr;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

struct ExpectedTime {
  ExpectedTime(absl::CivilSecond civil_second, double timezone_offset,
               std::string named_timezone = "",
               absl::Duration subseconds_duration = absl::ZeroDuration())
      : cs(civil_second), subseconds(subseconds_duration) {
    if (named_timezone.empty()) {
      timezone = absl::FixedTimeZone(timezone_offset * 60 * 60);
    } else {
      ABSL_CHECK(absl::LoadTimeZone(named_timezone, &timezone));
    }
  }

  // 6 fields are YYYY-MM-DD hh:mm:ss
  absl::CivilSecond cs;
  absl::TimeZone timezone;
  absl::Duration subseconds;
};

class TimestamptzInTest : public PgEvaluatorTest {
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

TEST_F(TimestamptzInTest, ReturnsParsedTimestamptzWhenWithinBounds) {
  EXPECT_THAT(PgTimestamptzIn("1970-01-01"),
              IsOkAndHolds(absl::FromUnixMicros(0)));
  EXPECT_THAT(PgTimestamptzIn("0001-01-01 00:00:00+0"),
              IsOkAndHolds(absl::FromUnixMicros(kTimestampMin)));
  EXPECT_THAT(PgTimestamptzIn("9999-12-31 23:59:59.999999+0"),
              IsOkAndHolds(absl::FromUnixMicros(kTimestampMax)));
}

TEST_F(TimestamptzInTest, ReturnsErrorForUnsupportedTimestamptz) {
  EXPECT_THAT(PgTimestamptzIn("`~-_=+[]{}|"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid input syntax for type timestamp ")));
  // Invalid timezone.
  EXPECT_THAT(
      PgTimestamptzIn("jan 1, 2000 01:02:03 america/boston"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("time zone \"america/boston\" not recognized")));
  // Timezone is a directory
  EXPECT_THAT(
      PgTimestamptzIn("January 8, 1999 04:05:06 America/Kentucky"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("time zone \"america/kentucky\" not recognized")));
  // Timezone abbreviations are not supported yet.
  EXPECT_THAT(PgTimestamptzIn("2003-04-12 04:05:06 PST"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid input syntax for type timestamp")));
  EXPECT_THAT(PgTimestamptzIn("infinity"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("infinity and -infinity are not supported")));
  EXPECT_THAT(PgTimestamptzIn("-infinity"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("infinity and -infinity are not supported")));
  EXPECT_THAT(PgTimestamptzIn("epoch"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("epoch is not supported")));
  EXPECT_THAT(PgTimestamptzIn("2000-01-01 25:01:01+00"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Timestamp is out of supported range")));
  EXPECT_THAT(PgTimestamptzIn("2000-01-01 05:60:00+00"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Timestamp is out of supported range")));
  EXPECT_THAT(PgTimestamptzIn("2000-01-01 01:00:70+00"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Timestamp is out of supported range")));
  EXPECT_THAT(PgTimestamptzIn("2000-01-01 13:00:00 AM"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Timestamp is out of supported range")));
  EXPECT_THAT(PgTimestamptzIn("2000-01-01 13:00:00 PM"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Timestamp is out of supported range")));
  EXPECT_THAT(PgTimestamptzIn("2000-01-01 -01:00:00+00"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid input syntax for type timestamp")));
  EXPECT_THAT(PgTimestamptzIn("10000-01-01T00:00:00 +00"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Timestamp is out of supported range")));
}

const std::map<std::string, ExpectedTime> kTimestamptzSupportedTestCases = {
    {"January 1, 2000 04:05:06.789+00",
     ExpectedTime(absl::CivilSecond(2000, 1, 1, 4, 5, 6), 0,
                  /*named_timezone=*/"", absl::Milliseconds(789))},
    {"January 1, 2000 04:05:06+00",
     ExpectedTime(absl::CivilSecond(2000, 1, 1, 4, 5, 6), 0)},
    {"January 1, 2000 04:05+00",
     ExpectedTime(absl::CivilSecond(2000, 1, 1, 4, 5, 0), 0)},
    {"January 1, 2000 040506+00",
     ExpectedTime(absl::CivilSecond(2000, 1, 1, 4, 5, 6), 0)},
    {"January 1, 2000 04:05 AM +00",
     ExpectedTime(absl::CivilSecond(2000, 1, 1, 4, 5, 0), 0)},
    {"January 1, 2000 04:05 PM +00",
     ExpectedTime(absl::CivilSecond(2000, 1, 1, 16, 5, 0), 0)},
    {"January 1, 2000 04:05:06.789-8",
     ExpectedTime(absl::CivilSecond(2000, 1, 1, 4, 5, 6), -8,
                  /*named_timezone=*/"", absl::Milliseconds(789))},
    {"January 1, 2000 04:05:06-08:00",
     ExpectedTime(absl::CivilSecond(2000, 1, 1, 4, 5, 6), -8)},
    {"January 1, 2000 04:05-08:00",
     ExpectedTime(absl::CivilSecond(2000, 1, 1, 4, 5, 0), -8)},
    {"January 1, 2000 040506-08",
     ExpectedTime(absl::CivilSecond(2000, 1, 1, 4, 5, 6), -8)},
    {"January 1, 2000 040506+0730",
     ExpectedTime(absl::CivilSecond(2000, 1, 1, 4, 5, 6), 7.5)},
    {"January 1, 2000 040506+07:30:00",
     ExpectedTime(absl::CivilSecond(2000, 1, 1, 4, 5, 6), 7.5)},
    {"January 8, 1999 04:05:06 America/Los_Angeles",
     ExpectedTime(absl::CivilSecond(1999, 1, 8, 4, 5, 6), 0,
                  "America/Los_Angeles")},
    {"January 8, 1999 04:05:06 America/Kentucky/Monticello",
     ExpectedTime(absl::CivilSecond(1999, 1, 8, 4, 5, 6), 0,
                  "America/Kentucky/Monticello")},
    {"January 8, 1999 04:05:06pm Egypt",
     ExpectedTime(absl::CivilSecond(1999, 1, 8, 16, 5, 6), 0, "Egypt")},
    {"January 8, 1999 04:05:06pm Europe/Lisbon",
     ExpectedTime(absl::CivilSecond(1999, 1, 8, 16, 5, 6), 0, "Europe/Lisbon")},
    // Default timezone
    {"oct 31, 2022", ExpectedTime(absl::CivilSecond(2022, 10, 31, 0, 0, 0), 0,
                                  kDefaultTimezone)},
    {"2000-01-01 23:59:59",
     ExpectedTime(absl::CivilSecond(2000, 1, 1, 23, 59, 59), 0,
                  kDefaultTimezone)},
    {"9999-12-31 23:59:59.999999 +00",
     ExpectedTime(absl::CivilSecond(9999, 12, 31, 23, 59, 59), 0,
                  /*named_timezone=*/"", absl::Microseconds(999999))},
    {"2003-04-12 04:05:06 zulu",
     ExpectedTime(absl::CivilSecond(2003, 4, 12, 4, 5, 6), 0)},
    // "z" is the only supported timezone abbrevation, which is short for zulu.
    {"2003-04-12 04:05:06 z",
     ExpectedTime(absl::CivilSecond(2003, 4, 12, 4, 5, 6), 0)},
};

TEST_F(TimestamptzInTest, RegressionSpecialTimestamptzs) {
  for (const auto& test_case : kTimestamptzSupportedTestCases) {
    absl::Time expected_time =
        test_case.second.timezone.At(test_case.second.cs).pre +
        test_case.second.subseconds;
    ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Time parsed_time,
                         PgTimestamptzIn(test_case.first));
    EXPECT_EQ(expected_time, parsed_time);
  }
}
}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
