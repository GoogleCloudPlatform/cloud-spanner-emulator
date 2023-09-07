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

#include "third_party/spanner_pg/function_evaluators/function_evaluators.h"

#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "third_party/spanner_pg/datetime_parsing/datetime_constants.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator::function_evaluators {
namespace {

using ::testing::HasSubstr;
using ::zetasql_base::testing::StatusIs;

constexpr char kDefaultTimezone[] = "America/Los_Angeles";

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

const std::map<std::string, int32_t> kDateSupportedTestCases = {
    {"January 1, 1970", 0},
    {"January 1, 2000", 10957},
    {"1975-08-05", 2042},
    {"04-12-2021", 18729},
    {"December 7, 1910", -21575},
    {"1999-01-08", 10599},
    {"January 8, 1999", 10599},
    {"1/8/1999", 10599},
    {"1/18/1999", 10609},
    {"01/02/03", 12054},
    {"1999-Jan-08", 10599},
    {"Jan-08-1999", 10599},
    {"08-Jan-1999", 10599},
    {"08-Jan-99", 10599},
    {"Jan-08-99", 10599},
    {"19990108", 10599},
    {"990108", 10599},
    {"1999.008", 10599},
    {"J2451187", 10599},
    {"January 1, 1", 11323}};

const std::map<std::string, std::string> kDateUnsupportedTestCases = {
    {"infinity", "Infinity and -infinity are not supported"},
    {"-infinity", "Infinity and -infinity are not supported"},
    {"January 1, 00 BC", "date/time field value out of range"},
    {"Sept 40, 2500", "date/time field value out of range"},
    {"99-Jan-08", "date/time field value out of range"},
    {"January 8, 99 BC", "Date is out of supported range"},
    {"5874897-12-31", "Date is out of supported range"}};

// Date and Timestamp use the same functions. The DateIn test cases should cover
// all variations of date input. These test cases will focus on time inputs.
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

std::map<std::string, std::string> kTimestamptzUnsupportedTestCases = {
    {"`~-_=+[]{}|", "invalid input syntax for type timestamp "},
    // Invalid timezone.
    {"jan 1, 2000 01:02:03 america/boston",
     "time zone \"america/boston\" not recognized"},
    // Timezone is a directory
    {"January 8, 1999 04:05:06 America/Kentucky",
     "time zone \"america/kentucky\" not recognized"},
    // Timezone abbreviations are not supported yet.
    {"2003-04-12 04:05:06 PST", "invalid input syntax for type timestamp"},
    {"infinity", "Infinity and -infinity are not supported"},
    {"-infinity", "Infinity and -infinity are not supported"},
    {"epoch", "Epoch is not supported"},
    {"2000-01-01 25:01:01+00", "date/time field value out of range"},
    {"2000-01-01 05:60:00+00", "date/time field value out of range"},
    {"2000-01-01 01:00:70+00", "date/time field value out of range"},
    {"2000-01-01 13:00:00 AM", "date/time field value out of range"},
    {"2000-01-01 13:00:00 PM", "date/time field value out of range"},
    {"2000-01-01 -01:00:00+00", "invalid input syntax for type timestamp"},
    {"10000-01-01T00:00:00 +00", "Timestamp is out of supported range"},
};

TEST(PGFunctionEvaluatorsTest, UnsupportedFunction) {
  EXPECT_THAT(UnsupportedFunctionEvaluator("UnknownFunction"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(PGFunctionEvaluatorsTest, DateIn) {
  for (const auto& test_case : kDateSupportedTestCases) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(int32_t date_offset,
                         DateIn(test_case.first, kDefaultTimezone));
    EXPECT_EQ(test_case.second, date_offset);
  }
}

TEST(PGFunctionEvaluatorsTest, DateTimeKeywordsDateIn) {
  for (int i = 0; i < szdatetktbl; ++i) {
    const datetkn token = datetktbl[i];
    if (token.type == MONTH) {
      // Check that all month keywords and abbreviations are supported and match
      // their integer counterparts
      std::string string_date = absl::StrFormat("%s 10, 2005", token.token);
      std::string int_date = absl::StrFormat("2005-%d-10", token.value);
      ZETASQL_ASSERT_OK_AND_ASSIGN(int32_t string_date_offset,
                           DateIn(string_date, kDefaultTimezone));
      ZETASQL_ASSERT_OK_AND_ASSIGN(int32_t int_date_offset,
                           DateIn(int_date, kDefaultTimezone));
      EXPECT_EQ(string_date_offset, int_date_offset);
    } else if (token.type == ADBC) {
      // AD and BC are both supported in PostgreSQL but BC is unsupported in
      // ZetaSQL so it is also unsupported in Spangres.
      std::string date = absl::StrFormat("January 8, 99 %s", token.token);
      if (token.value == AD) {
        ZETASQL_EXPECT_OK(DateIn(date, kDefaultTimezone));
      } else {
        ASSERT_EQ(token.value, BC);
        EXPECT_THAT(DateIn(date, kDefaultTimezone),
                    StatusIs(absl::StatusCode::kInvalidArgument,
                             HasSubstr("Date is out of supported range")));
      }
    } else if (token.type == RESERV) {
      if (token.value == DTK_EARLY || token.value == DTK_LATE) {
        EXPECT_THAT(
            DateIn(token.token, kDefaultTimezone),
            StatusIs(absl::StatusCode::kInvalidArgument,
                     HasSubstr("Infinity and -infinity are not supported")));
      } else if (token.value == DTK_ZULU) {
        // allballs is supported but has no impact on date
        std::string allballs = absl::StrFormat("2000-01-01 %s", token.token);
        std::string zeros = "2000-01-01 00:00:00";
        ZETASQL_ASSERT_OK_AND_ASSIGN(int32_t allballs_date,
                             DateIn(allballs, kDefaultTimezone));
        ZETASQL_ASSERT_OK_AND_ASSIGN(int32_t zeros_date,
                             DateIn(zeros, kDefaultTimezone));
        EXPECT_EQ(allballs_date, zeros_date);
      } else if (token.value == DTK_EPOCH) {
        EXPECT_THAT(DateIn(token.token, kDefaultTimezone),
                    StatusIs(absl::StatusCode::kInvalidArgument,
                             HasSubstr("Epoch is not supported")));
      } else {
        // now, today, tomorrow, and yesterday are not supported.
        EXPECT_THAT(DateIn(token.token, kDefaultTimezone),
                    StatusIs(absl::StatusCode::kInvalidArgument,
                             HasSubstr("Current/relative datetimes")));
      }
    }
  }
}

TEST(PGFunctionEvaluatorsTest, UnsupportedDateIn) {
  for (const auto& test_case : kDateUnsupportedTestCases) {
    EXPECT_THAT(DateIn(test_case.first, kDefaultTimezone),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr(test_case.second)));
  }
}

TEST(PGFunctionEvaluatorsTest, TimestamptzIn) {
  for (const auto& test_case : kTimestamptzSupportedTestCases) {
    absl::Time expected_time =
        test_case.second.timezone.At(test_case.second.cs).pre +
        test_case.second.subseconds;
    ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Time parsed_time,
                         TimestamptzIn(test_case.first, kDefaultTimezone));
    EXPECT_EQ(expected_time, parsed_time);
  }
}

TEST(PGFunctionEvaluatorsTest, DateTimeKeywordsTimestamptzIn) {
  for (int i = 0; i < szdatetktbl; ++i) {
    const datetkn token = datetktbl[i];
    if (token.type == MONTH) {
      // Check that all month keywords and abbreviations are supported and match
      // their integer counterparts
      std::string string_month =
          absl::StrFormat("%s 10, 2005 01:02:03+01", token.token);
      std::string int_month =
          absl::StrFormat("2005-%d-10 01:02:03+01", token.value);
      ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Time string_timestamp,
                           TimestamptzIn(string_month, kDefaultTimezone));
      ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Time int_timestamp,
                           TimestamptzIn(int_month, kDefaultTimezone));
      EXPECT_EQ(string_timestamp, int_timestamp);
    } else if (token.type == ADBC) {
      // AD and BC are both supported in PostgreSQL but BC is unsupported in
      // ZetaSQL so it is also unsupported in Spangres.
      std::string timestamp =
          absl::StrFormat("January 8, 99 %s 01:02:03-07", token.token);
      if (token.value == AD) {
        ZETASQL_EXPECT_OK(TimestamptzIn(timestamp, kDefaultTimezone));
      } else {
        ASSERT_EQ(token.value, BC);
        EXPECT_THAT(TimestamptzIn(timestamp, kDefaultTimezone),
                    StatusIs(absl::StatusCode::kInvalidArgument,
                             HasSubstr("Timestamp is out of supported range")));
      }
    } else if (token.type == RESERV) {
      if (token.value == DTK_EARLY || token.value == DTK_LATE) {
        EXPECT_THAT(
            TimestamptzIn(token.token, kDefaultTimezone),
            StatusIs(absl::StatusCode::kInvalidArgument,
                     HasSubstr("Infinity and -infinity are not supported")));
      } else if (token.value == DTK_EPOCH) {
        EXPECT_THAT(TimestamptzIn(token.token, kDefaultTimezone),
                    StatusIs(absl::StatusCode::kInvalidArgument,
                             HasSubstr("Epoch is not supported")));
      } else if (token.value == DTK_ZULU) {
        // allballs is supported and uses UTC as the timezone.
        std::string allballs = absl::StrFormat("2000-01-01 %s", token.token);
        std::string zeros = "2000-01-01 00:00:00+00";
        ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Time allballs_timestamp,
                             TimestamptzIn(allballs, kDefaultTimezone));
        ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Time zeros_timestamp,
                             TimestamptzIn(zeros, kDefaultTimezone));
        EXPECT_EQ(allballs_timestamp, zeros_timestamp);
      } else {
        // now, today, tomorrow, and yesterday are not supported.
        EXPECT_THAT(TimestamptzIn(token.token, kDefaultTimezone),
                    StatusIs(absl::StatusCode::kInvalidArgument,
                             HasSubstr("Current/relative datetimes")));
      }
    }
  }
}

TEST(PGFunctionEvaluatorsTest, UnsupportedTimestamptzIn) {
  for (const auto& test_case : kTimestamptzUnsupportedTestCases) {
    EXPECT_THAT(TimestamptzIn(test_case.first, kDefaultTimezone),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr(test_case.second)));
  }
}

TEST(PGFunctionEvaluatorsTest, InvalidDefaultTimezone) {
  // If the string has a timezone, invalid default timezones are ignored.
  ZETASQL_EXPECT_OK(TimestamptzIn("Jan 1, 2000 01:02:03 +08",
                          /*default_timezone=*/"bananas"));
  ZETASQL_EXPECT_OK(TimestamptzIn("Jan 1, 2000 01:02:03 Australia/Sydney",
                          /*default_timezone=*/"bananas"));

  // If the string does not have a timezone, the default timezone must be valid.
  EXPECT_THAT(
      TimestamptzIn("Jan 1, 2000 01:02:03", /*default_timezone=*/"bananas"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("default timezone \"bananas\" not recognized")));
}

}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
