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

#include "zetasql/public/types/timestamp_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "third_party/spanner_pg/function_evaluators/tests/test_base.h"
#include "third_party/spanner_pg/interface/datetime_evaluators.h"

namespace postgres_translator::function_evaluators {
namespace {

using ::zetasql::types::kDateMax;
using ::zetasql::types::kDateMin;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

inline constexpr int32_t kDaysUntilMaxDate = kDateMax;

TEST(DatePliTest, ReturnsValidDateWhenResultWithinRange) {
  // 1970-01-01 + 1 = 1970-01-02
  EXPECT_THAT(DatePli(0, 1), IsOkAndHolds(Eq(1)));

  // 1970-01-01 + DAYS UNTIL MAX DATE = 1970-01-02
  EXPECT_THAT(DatePli(0, kDaysUntilMaxDate), IsOkAndHolds(Eq(kDateMax)));

  // 9999-12-31 + 0 = 9999-12-31
  EXPECT_THAT(DatePli(kDaysUntilMaxDate, 0), IsOkAndHolds(Eq(kDateMax)));
}

TEST(DatePliTest, ReturnsErrorWhenResultOutOfRange) {
  // 9999-12-31 = out of range (10000-01-01)
  EXPECT_THAT(DatePli(kDateMax, 1),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Date is out of supported range")));

  // 0001-01-01 + INT64_MAX = out of range (dates are represented as int32_t)
  EXPECT_THAT(DatePli(kDateMin, INT64_MAX),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Date is out of supported range")));
}
}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
