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

#include "third_party/spanner_pg/util/interval_helpers.h"
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"

namespace {

void Validate(const PGInterval& lhs, const PGInterval& rhs) {
  EXPECT_EQ(lhs.months, rhs.months);
  EXPECT_EQ(lhs.days, rhs.days);
  EXPECT_EQ(lhs.micros, rhs.micros);
}

TEST(Intervals, IntervalToSecs) {
  // Make sure that we get errors for bad intervals, and no errors for good
  // ones.
  EXPECT_THAT(IntervalToSecs("1 day"), zetasql_base::testing::IsOk());
  EXPECT_THAT(IntervalToSecs("P1Y2M3DT4H5M6S"), zetasql_base::testing::IsOk());
  EXPECT_THAT(IntervalToSecs("P-1Y2M3DT4H5M6S"), zetasql_base::testing::IsOk());
  EXPECT_THAT(IntervalToSecs("greeble"), testing::Not(zetasql_base::testing::IsOk()));
  EXPECT_THAT(IntervalToSecs("Pasdf"), testing::Not(zetasql_base::testing::IsOk()));
  EXPECT_THAT(IntervalToSecs("November 17, 1858"),
              testing::Not(zetasql_base::testing::IsOk()));

  // Test that some conversions work
  EXPECT_EQ(86400, IntervalToSecs("1 day").value());
  EXPECT_EQ(86340, IntervalToSecs("1 day - 1 minute").value());
  EXPECT_EQ(5443215, IntervalToSecs("2 months 3 days 15 seconds").value());
  EXPECT_EQ(4838415,
            IntervalToSecs("2 months 3 days 15 seconds - 1 week").value());

  // The following three intervals are identical, but in different formats.
  EXPECT_EQ(36561906,
            IntervalToSecs("1 year 2 months 3 days 4 hours 5 minutes 6 seconds")
                .value());
  EXPECT_EQ(36561906, IntervalToSecs("P1Y2M3DT4H5M6S").value());
  EXPECT_EQ(36561906, IntervalToSecs("P0001-02-03T04:05:06").value());
}

TEST(Intervals, ParseInterval) {
  // Make sure that we get errors for bad intervals, and no errors for good
  // ones.
  EXPECT_THAT(ParseInterval("1 day"), zetasql_base::testing::IsOk());
  EXPECT_THAT(ParseInterval("P1Y2M3DT4H5M6S"), zetasql_base::testing::IsOk());
  EXPECT_THAT(ParseInterval("P-1Y2M3DT4H5M6S"), zetasql_base::testing::IsOk());
  EXPECT_THAT(ParseInterval("greeble"), testing::Not(zetasql_base::testing::IsOk()));
  EXPECT_THAT(ParseInterval("Pasdf"), testing::Not(zetasql_base::testing::IsOk()));
  EXPECT_THAT(ParseInterval("November 17, 1858"),
              testing::Not(zetasql_base::testing::IsOk()));

  // Test that some conversions work
  Validate(ParseInterval("1 day").value(),
           PGInterval{.months = 0, .days = 1, .micros = 0});
  Validate(ParseInterval("1 day - 1 minute").value(),
           PGInterval{.months = 0, .days = 1, .micros = -60000000});
  Validate(ParseInterval("2 months 3 days 15 seconds").value(),
           PGInterval{.months = 2, .days = 3, .micros = 15000000});
  Validate(ParseInterval("2 months 3 days 15 seconds - 1 week").value(),
           PGInterval{.months = 2, .days = -4, .micros = 15000000});

  // The following three intervals are identical, but in different formats.
  Validate(ParseInterval("1 year 2 months 3 days 4 hours 5 minutes 6 seconds")
               .value(),
           PGInterval{.months = 14, .days = 3, .micros = 14706000000});
  Validate(ParseInterval("P1Y2M3DT4H5M6S").value(),
           PGInterval{.months = 14, .days = 3, .micros = 14706000000});
  Validate(ParseInterval("P0001-02-03T04:05:06").value(),
           PGInterval{.months = 14, .days = 3, .micros = 14706000000});

  // Test fractional values
  Validate(ParseInterval("1.5 days").value(),
           PGInterval{.months = 0, .days = 1, .micros = 43200000000});
  Validate(ParseInterval("1.5 months").value(),
           PGInterval{.months = 1, .days = 15, .micros = 0});
  Validate(ParseInterval("2.5 hours").value(),
           PGInterval{.months = 0, .days = 0, .micros = 9000000000});
}

struct IntervalToStringTestParams {
  PGInterval interval;
  std::string expected;
};

class IntervalToStringTest
    : public testing::TestWithParam<IntervalToStringTestParams> {};

TEST_P(IntervalToStringTest, Basic) {
  IntervalToStringTestParams params = GetParam();
  EXPECT_EQ(IntervalToString(params.interval), params.expected);
}

INSTANTIATE_TEST_SUITE_P(
    IntervalToStringTest, IntervalToStringTest,
    testing::ValuesIn<IntervalToStringTestParams>({
        // zero-interval
        {.interval =
             PGInterval{
                 .months = 0, .days = 0, .micros = 0, .nano_fraction = 0},
         .expected = "00:00:00"},
        // max-value
        {.interval = PGInterval{.months = 120000,
                                .days = 3660000,
                                .micros = 31622400000000000,
                                .nano_fraction = 0},
         .expected = "10000 years 3660000 days 8784000:00:00"},
        // min-value
        {.interval = PGInterval{.months = -120000,
                                .days = -3660000,
                                .micros = -31622400000000000,
                                .nano_fraction = 0},
         .expected = "-10000 years -3660000 days -8784000:00:00"},
        // year-month part
        {.interval =
             PGInterval{
                 .months = 12, .days = 0, .micros = 0, .nano_fraction = 0},
         .expected = "1 year 00:00:00"},
        {.interval =
             PGInterval{
                 .months = -12, .days = 0, .micros = 0, .nano_fraction = 0},
         .expected = "-1 years +00:00:00"},
        {.interval =
             PGInterval{
                 .months = 1, .days = 0, .micros = 0, .nano_fraction = 0},
         .expected = "1 mon 00:00:00"},
        {.interval =
             PGInterval{
                 .months = -1, .days = 0, .micros = 0, .nano_fraction = 0},
         .expected = "-1 mons +00:00:00"},
        {.interval =
             PGInterval{
                 .months = 25, .days = 0, .micros = 0, .nano_fraction = 0},
         .expected = "2 years 1 mon 00:00:00"},
        {.interval =
             PGInterval{
                 .months = -25, .days = 0, .micros = 0, .nano_fraction = 0},
         .expected = "-2 years -1 mons +00:00:00"},
        // day part
        {.interval =
             PGInterval{
                 .months = 0, .days = 1, .micros = 0, .nano_fraction = 0},
         .expected = "1 day 00:00:00"},
        {.interval =
             PGInterval{
                 .months = 0, .days = -1, .micros = 0, .nano_fraction = 0},
         .expected = "-1 days +00:00:00"},
        {.interval =
             PGInterval{
                 .months = 0, .days = 31, .micros = 0, .nano_fraction = 0},
         .expected = "31 days 00:00:00"},
        {.interval =
             PGInterval{
                 .months = 0, .days = -31, .micros = 0, .nano_fraction = 0},
         .expected = "-31 days +00:00:00"},
        // Time part
        {.interval =
             PGInterval{
                 .months = 0, .days = 0, .micros = 0, .nano_fraction = 1},
         .expected = "00:00:00.000000001"},
        {.interval =
             PGInterval{
                 .months = 0, .days = 0, .micros = 0, .nano_fraction = -1},
         .expected = "-00:00:00.000000001"},
        {.interval =
             PGInterval{
                 .months = 0, .days = 0, .micros = 1, .nano_fraction = 0},
         .expected = "00:00:00.000001"},
        {.interval =
             PGInterval{
                 .months = 0, .days = 0, .micros = -1, .nano_fraction = 0},
         .expected = "-00:00:00.000001"},
        {.interval = PGInterval{.months = 0,
                                .days = 0,
                                .micros = -100,
                                .nano_fraction = 0},
         .expected = "-00:00:00.0001"},
        {.interval = PGInterval{.months = 0,
                                .days = 0,
                                .micros = 100000,
                                .nano_fraction = 0},
         .expected = "00:00:00.1"},
        {.interval = PGInterval{.months = 0,
                                .days = 0,
                                .micros = 100000000,
                                .nano_fraction = 0},
         .expected = "00:01:40"},
        // mixed signs
        {.interval = PGInterval{.months = -34,
                                .days = 12,
                                .micros = 18956478,
                                .nano_fraction = 978},
         .expected = "-2 years -10 mons +12 days 00:00:18.956478978"},
        {.interval = PGInterval{.months = -34,
                                .days = -12,
                                .micros = 18956478,
                                .nano_fraction = 978},
         .expected = "-2 years -10 mons -12 days +00:00:18.956478978"},
        {.interval = PGInterval{.months = -34,
                                .micros = 18956478,
                                .nano_fraction = 978},
         .expected = "-2 years -10 mons +00:00:18.956478978"},
        {.interval =
             PGInterval{.months = 34, .micros = 18956478, .nano_fraction = 978},
         .expected = "2 years 10 mons 00:00:18.956478978"},
        {.interval = PGInterval{.months = 789,
                                .days = -56784,
                                .micros = -320,
                                .nano_fraction = 7},
         .expected = "65 years 9 mons -56784 days -00:00:00.000319993"},
        {.interval = PGInterval{.months = 56946,
                                .days = -4809,
                                .micros = -1978,
                                .nano_fraction = 5678},
         .expected = "4745 years 6 mons -4809 days -00:00:00.001972322"},
        {.interval = PGInterval{.months = 78654,
                                .days = -2301,
                                .micros = 1378,
                                .nano_fraction = 52},
         .expected = "6554 years 6 mons -2301 days +00:00:00.001378052"},
    }));
}  // namespace
