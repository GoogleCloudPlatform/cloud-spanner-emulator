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

using ::zetasql::types::kDateMin;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

inline constexpr int32_t kDaysUntilEpoch = -kDateMin;

TEST(DateMiiTest, ReturnsValidDateWhenResultWithinRange) {
  // 1970-01-01 - 1 = 1969-12-30
  EXPECT_THAT(DateMii(0, 1), IsOkAndHolds(Eq(-1)));

  // 1970-01-01 - DAYS UNTIL EPOCH = 0001-01-01
  EXPECT_THAT(DateMii(0, kDaysUntilEpoch), IsOkAndHolds(Eq(kDateMin)));

  // 0001-01-01 - 0 = 0001-01-01
  EXPECT_THAT(DateMii(kDateMin, 0), IsOkAndHolds(Eq(kDateMin)));
}

TEST(DateMiiTest, ReturnsErrorWhenResultOutOfRange) {
  // 0001-01-01 - 1 = out of range (0001-12-31 BC)
  EXPECT_THAT(DateMii(kDateMin, 1),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Date is out of supported range")));

  // 0001-01-01 - INT64_MIN = out of range (dates are represented as int32_t)
  EXPECT_THAT(DateMii(kDateMin, INT64_MIN),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Date is out of supported range")));
}
}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
