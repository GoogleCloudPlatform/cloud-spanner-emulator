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
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "third_party/spanner_pg/datatypes/common/numeric_core.h"
#include "third_party/spanner_pg/function_evaluators/tests/test_base.h"
#include "third_party/spanner_pg/interface/mathematical_evaluators.h"
#include "third_party/spanner_pg/postgres_includes/all.h"

namespace postgres_translator::function_evaluators {
namespace {

using ::postgres_translator::spangres::datatypes::common::kPGNumericNaN;
using ::postgres_translator::spangres::datatypes::common::
    kPGNumericNegativeInfinity;
using ::postgres_translator::spangres::datatypes::common::
    kPGNumericPositiveInfinity;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

class TruncTest : public PgEvaluatorTest {};

TEST_F(TruncTest, TruncateNumericsToSpecifiedScale) {
  EXPECT_THAT(Trunc("123.456", 1), IsOkAndHolds("123.4"));
  EXPECT_THAT(Trunc("123.456", 2), IsOkAndHolds("123.45"));
  EXPECT_THAT(Trunc("123.456", 3), IsOkAndHolds("123.456"));
  EXPECT_THAT(Trunc("123.456", 4), IsOkAndHolds("123.4560"));
  EXPECT_THAT(Trunc("123.456", 10), IsOkAndHolds("123.4560000000"));
  EXPECT_THAT(Trunc("-123.456", 1), IsOkAndHolds("-123.4"));
  EXPECT_THAT(Trunc("0", 1), IsOkAndHolds("0.0"));
}

TEST_F(TruncTest, DoesNothingOnZeroScale) {
  EXPECT_THAT(Trunc("1", 0), IsOkAndHolds("1"));
  EXPECT_THAT(Trunc("12", 0), IsOkAndHolds("12"));
  EXPECT_THAT(Trunc("123", 0), IsOkAndHolds("123"));
  EXPECT_THAT(Trunc("0", 0), IsOkAndHolds("0"));
  EXPECT_THAT(Trunc("-1", 0), IsOkAndHolds("-1"));
  EXPECT_THAT(Trunc("-12", 0), IsOkAndHolds("-12"));
  EXPECT_THAT(Trunc("-123", 0), IsOkAndHolds("-123"));
}

TEST_F(TruncTest, NegativeScaleZerosDecimalPositions) {
  EXPECT_THAT(Trunc("1", -1), IsOkAndHolds("0"));
  EXPECT_THAT(Trunc("12", -1), IsOkAndHolds("10"));
  EXPECT_THAT(Trunc("123", -1), IsOkAndHolds("120"));
  EXPECT_THAT(Trunc("0", -1), IsOkAndHolds("0"));
  EXPECT_THAT(Trunc("123.456", -1), IsOkAndHolds("120"));
}

TEST_F(TruncTest, CapsToMaxNumericResultScaleWhenScaleIsOverIt) {
  const int16_t max_precision = std::numeric_limits<int16_t>::max() / 2;
  EXPECT_THAT(
      Trunc("1", max_precision + 1),
      IsOkAndHolds(absl::StrCat("1.", std::string(max_precision, '0'))));
}

TEST_F(TruncTest, ReturnsErrorWhenScaleDoesNotFitIntoInt32) {
  EXPECT_THAT(
      Trunc("1", static_cast<int64_t>(std::numeric_limits<int32_t>::min()) - 1),
      StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(
      Trunc("1", static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1),
      StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(TruncTest, DoesNothingForSpecialCaseValues) {
  EXPECT_THAT(Trunc(kPGNumericPositiveInfinity, 1),
              IsOkAndHolds(kPGNumericPositiveInfinity));
  EXPECT_THAT(Trunc(kPGNumericNegativeInfinity, 1),
              IsOkAndHolds(kPGNumericNegativeInfinity));
  EXPECT_THAT(Trunc(kPGNumericNaN, 1), IsOkAndHolds(kPGNumericNaN));
}

TEST_F(TruncTest, ReturnsErrorForInvalidInput) {
  EXPECT_THAT(Trunc("abcd", 1), StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(Trunc("", 1), StatusIs(absl::StatusCode::kInvalidArgument));
}

}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
