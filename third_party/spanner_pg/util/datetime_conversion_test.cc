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

#include "third_party/spanner_pg/util/datetime_conversion.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/time/time.h"

namespace {

using ::testing::Eq;
using ::testing::HasSubstr;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

// Represents the offset that needs to be added to a pg date to convert it to a
// gsql date
inline constexpr int32_t kPgOffsetToGsqlDate = 10957;

TEST(DatetimeConversionTest, TimestampConversion) {
  TimestampTz pg_timestamptz = 1640030937311000;
  absl::Time gsql_timestamp =
      absl::TimeFromTimeval({.tv_sec = 2586715737, .tv_usec = 311000});
  EXPECT_EQ(PgTimestamptzToAbslTime(pg_timestamptz), gsql_timestamp);
  EXPECT_EQ(AbslTimeToPgTimestamptz(gsql_timestamp), pg_timestamptz);
}

TEST(DatetimeConversionTest, DateOffsetConversion) {
  int32_t pg_date_offset = 10000;
  int32_t gsql_date_offset = 20957;
  EXPECT_EQ(PgDateOffsetToGsqlDateOffset(pg_date_offset), gsql_date_offset);
  EXPECT_EQ(GsqlDateOffsetToPgDateOffset(gsql_date_offset), pg_date_offset);
}

TEST(SafeDatetimeConversionTest, SucceedsWhenDateWithinMaxBounds) {
  int32_t pg_date_offset = INT32_MAX - kPgOffsetToGsqlDate;
  int32_t gsql_date_offset = INT32_MAX;
  EXPECT_THAT(SafePgDateOffsetToGsqlDateOffset(pg_date_offset),
              IsOkAndHolds(Eq(gsql_date_offset)));
  EXPECT_THAT(SafeGsqlDateOffsetToPgDateOffset(gsql_date_offset),
              IsOkAndHolds(Eq(pg_date_offset)));
}

TEST(SafeDatetimeConversionTest, SucceedsWhenDateWithinMinBounds) {
  int32_t pg_date_offset = INT32_MIN;
  int32_t gsql_date_offset = INT32_MIN + kPgOffsetToGsqlDate;
  EXPECT_THAT(SafePgDateOffsetToGsqlDateOffset(pg_date_offset),
              IsOkAndHolds(Eq(gsql_date_offset)));
  EXPECT_THAT(SafeGsqlDateOffsetToPgDateOffset(gsql_date_offset),
              IsOkAndHolds(Eq(pg_date_offset)));
}

TEST(SafeDatetimeConversionTest, FailsWhenDateIsOutOfBounds) {
  // We will try to add 10957 to convert from a pg date to a gsql date and it
  // will overflow
  int32_t pg_date_offset = INT32_MAX - (kPgOffsetToGsqlDate - 1);
  EXPECT_THAT(SafePgDateOffsetToGsqlDateOffset(pg_date_offset),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Date is out of supported range")));

  // We will try to subtract 10957 to convert from a gsql date to a pg date and
  // it will underflow
  int32_t gsql_date_offset = INT32_MIN + (kPgOffsetToGsqlDate - 1);
  EXPECT_THAT(SafeGsqlDateOffsetToPgDateOffset(gsql_date_offset),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Date is out of supported range")));
}
}  // namespace
