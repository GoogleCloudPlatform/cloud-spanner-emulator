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

class DateInTest : public PgEvaluatorTest {
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

TEST_F(DateInTest, ReturnsParsedDateWhenWithinBounds) {
  EXPECT_THAT(PgDateIn("1970-01-01"), IsOkAndHolds(0));
  EXPECT_THAT(PgDateIn("0001-01-01"), IsOkAndHolds(kDateMin));
  EXPECT_THAT(PgDateIn("9999-12-31"), IsOkAndHolds(kDateMax));
}

TEST_F(DateInTest, ReturnsErrorWhenDateIsOutOfBounds) {
  EXPECT_THAT(PgDateIn("0000-01-01"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Date is out of supported range")));
  EXPECT_THAT(PgDateIn("10000-01-01"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Date is out of supported range")));
  EXPECT_THAT(PgDateIn("infinity"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("infinity and -infinity are not supported")));
  EXPECT_THAT(PgDateIn("-infinity"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("infinity and -infinity are not supported")));
  EXPECT_THAT(PgDateIn("January 1, 00 BC"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Date is out of supported range")));
  EXPECT_THAT(PgDateIn("Sept 40, 25000"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Date is out of supported range")));
  EXPECT_THAT(PgDateIn("99-Jan-08"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Date is out of supported range")));
  EXPECT_THAT(PgDateIn("January 8, 99 BC"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Date is out of supported range")));
  EXPECT_THAT(PgDateIn("5874897-12-31"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Date is out of supported range")));
}

TEST_F(DateInTest, RegressionSpecialDates) {
  EXPECT_THAT(PgDateIn("January 1, 1970"), IsOkAndHolds(0));
  EXPECT_THAT(PgDateIn("January 1, 2000"), IsOkAndHolds(10957));
  EXPECT_THAT(PgDateIn("1975-08-05"), IsOkAndHolds(2042));
  EXPECT_THAT(PgDateIn("04-12-2021"), IsOkAndHolds(18729));
  EXPECT_THAT(PgDateIn("December 7, 1910"), IsOkAndHolds(-21575));
  EXPECT_THAT(PgDateIn("1999-01-08"), IsOkAndHolds(10599));
  EXPECT_THAT(PgDateIn("January 8, 1999"), IsOkAndHolds(10599));
  EXPECT_THAT(PgDateIn("1/8/1999"), IsOkAndHolds(10599));
  EXPECT_THAT(PgDateIn("1/18/1999"), IsOkAndHolds(10609));
  EXPECT_THAT(PgDateIn("01/02/03"), IsOkAndHolds(12054));
  EXPECT_THAT(PgDateIn("1999-Jan-08"), IsOkAndHolds(10599));
  EXPECT_THAT(PgDateIn("Jan-08-1999"), IsOkAndHolds(10599));
  EXPECT_THAT(PgDateIn("08-Jan-1999"), IsOkAndHolds(10599));
  EXPECT_THAT(PgDateIn("08-Jan-99"), IsOkAndHolds(10599));
  EXPECT_THAT(PgDateIn("Jan-08-99"), IsOkAndHolds(10599));
  EXPECT_THAT(PgDateIn("19990108"), IsOkAndHolds(10599));
  EXPECT_THAT(PgDateIn("990108"), IsOkAndHolds(10599));
  EXPECT_THAT(PgDateIn("1999.008"), IsOkAndHolds(10599));
  EXPECT_THAT(PgDateIn("J2451187"), IsOkAndHolds(10599));
  EXPECT_THAT(PgDateIn("January 1, 1"), IsOkAndHolds(11323));
}
}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
