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

using ::zetasql::types::kDateMax;
using ::zetasql::types::kDateMin;
using ::postgres_translator::CleanupTimezone;
using ::postgres_translator::InitTimezone;
using ::testing::HasSubstr;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

class ToDateTest : public PgEvaluatorTest {
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

TEST_F(ToDateTest, ReturnsParsedDateWhenWithinBounds) {
  EXPECT_THAT(PgToDate("1970-01-01", "YYYY-MM-DD"), IsOkAndHolds(0));
  EXPECT_THAT(PgToDate("0001-01-01", "YYYY-MM-DD"), IsOkAndHolds(kDateMin));
  EXPECT_THAT(PgToDate("9999-12-31", "YYYY-MM-DD"), IsOkAndHolds(kDateMax));
}

TEST_F(ToDateTest, ReturnsErrorWhenDateIsOutOfBounds) {
  EXPECT_THAT(PgToDate("0000-01-01", "YYYY-MM-DD"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Date is out of supported range")));
  EXPECT_THAT(PgToDate("10000-01-01", "YYYY-MM-DD"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Date is out of supported range")));
  // Empty format returns 0001-01-01 BC date in PostgreSQL
  EXPECT_THAT(PgToDate("1999-01-02", ""),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Date is out of supported range")));
}

TEST_F(ToDateTest, RegressionMultipleSpaces) {
  // postgres=> select to_date('2011-12-18', 'YYYY-MM-DD') - '1970-01-01'::date;
  DateADT expected_date = 15326;
  EXPECT_THAT(PgToDate("2011 12  18", "YYYY MM DD"),
              IsOkAndHolds(expected_date));
  EXPECT_THAT(PgToDate("2011 12  18", "YYYY MM  DD"),
              IsOkAndHolds(expected_date));
  EXPECT_THAT(PgToDate("2011 12  18", "YYYY MM   DD"),
              IsOkAndHolds(expected_date));

  EXPECT_THAT(PgToDate("2011 12 18", "YYYY  MM DD"),
              IsOkAndHolds(expected_date));
  EXPECT_THAT(PgToDate("2011  12 18", "YYYY  MM DD"),
              IsOkAndHolds(expected_date));
  EXPECT_THAT(PgToDate("2011   12 18", "YYYY  MM DD"),
              IsOkAndHolds(expected_date));

  EXPECT_THAT(PgToDate("2011 12 18", "YYYYxMMxDD"),
              IsOkAndHolds(expected_date));
  EXPECT_THAT(PgToDate("2011x 12x 18", "YYYYxMMxDD"),
              IsOkAndHolds(expected_date));

  EXPECT_THAT(
      PgToDate("2011 x12 x18", "YYYYxMMxDD"),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("invalid value")));
}

TEST_F(ToDateTest, RegressionSpecialDates) {
  // postgres=> select to_date('2016-02-29', 'YYYY-MM-DD') - '1970-01-01'::date;
  DateADT leap_year1 = 16860;
  EXPECT_THAT(PgToDate("2016-02-29", "YYYY-MM-DD"), IsOkAndHolds(leap_year1));

  // postgres=> select to_date('2016 366', 'YYYY DDD') - '1970-01-01'::date;
  DateADT leap_year2 = 17166;
  EXPECT_THAT(PgToDate("2016 366", "YYYY DDD"), IsOkAndHolds(leap_year2));

  // postgres=> select to_date('2016 365', 'YYYY DDD') - '1970-01-01'::date;
  DateADT leap_year3 = 17165;
  EXPECT_THAT(PgToDate("2016 365", "YYYY DDD"), IsOkAndHolds(leap_year3));

  // postgres=> select to_date('2015 365', 'YYYY DDD') - '1970-01-01'::date;
  DateADT expected_date1 = 16800;
  EXPECT_THAT(PgToDate("2015 365", "YYYY DDD"), IsOkAndHolds(expected_date1));

  DateADT expected_date2 = -703426;
  EXPECT_THAT(PgToDate("44-02-01", "YYYY-MM-DD"), IsOkAndHolds(expected_date2));
  DateADT expected_date3 = -703426;
  EXPECT_THAT(PgToDate("-44-02-01 BC", "YYYY-MM-DD BC"),
              IsOkAndHolds(expected_date3));
}

TEST_F(ToDateTest, RegressionOutOfRangeError) {
  EXPECT_THAT(PgToDate("0000-02-01", "YYYY-MM-DD"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Date is out of supported range")));
  EXPECT_THAT(PgToDate("2016-13-10", "YYYY-MM-DD"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Date is out of supported range")));
  EXPECT_THAT(PgToDate("2016-02-30", "YYYY-MM-DD"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Date is out of supported range")));
  EXPECT_THAT(PgToDate("2015-02-29", "YYYY-MM-DD"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Date is out of supported range")));
  EXPECT_THAT(PgToDate("2015 366", "YYYY DDD"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Date is out of supported range")));
  EXPECT_THAT(PgToDate("2016 367", "YYYY DDD"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Date is out of supported range")));

  EXPECT_THAT(PgToDate("44-02-01 BC", "YYYY-MM-DD BC"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Date is out of supported range")));
  EXPECT_THAT(PgToDate("-44-02-01", "YYYY-MM-DD"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Date is out of supported range")));
}

TEST_F(ToDateTest, RegressionUncommonDatePatterns) {
  // select to_date('1 4 1902', 'Q MM YYYY') - '1970-01-01'::date;
  DateADT date1 = -24747;
  EXPECT_THAT(PgToDate("1 4 1902", "Q MM YYYY"), IsOkAndHolds(date1));

  // select to_date('3 4 21 01', 'W MM CC YY') - '1970-01-01'::date;
  DateADT date2 = 11427;
  EXPECT_THAT(PgToDate("3 4 21 01", "W MM CC YY"), IsOkAndHolds(date2));

  // select to_date('2458872', 'J') - '1970-01-01'::date;
  DateADT date3 = 18284;
  EXPECT_THAT(PgToDate("2458872", "J"), IsOkAndHolds(date3));
}
}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
