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

#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "third_party/spanner_pg/datatypes/common/numeric_core.h"
#include "third_party/spanner_pg/function_evaluators/tests/test_base.h"
#include "third_party/spanner_pg/interface/mathematical_evaluators.h"

namespace postgres_translator::function_evaluators {
namespace {

using ::postgres_translator::spangres::datatypes::common::
    kMaxPGNumericFractionalDigits;
using ::postgres_translator::spangres::datatypes::common::kPGNumericNaN;
using ::postgres_translator::spangres::datatypes::common::
    kPGNumericNegativeInfinity;
using ::postgres_translator::spangres::datatypes::common::
    kPGNumericPositiveInfinity;
using ::postgres_translator::spangres::datatypes::common::MaxNumericString;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

class ModTest : public PgEvaluatorTest {};

TEST_F(ModTest, ReturnsModOfGivenNumbers) {
  EXPECT_THAT(Mod("123", "10"), IsOkAndHolds("3"));
  EXPECT_THAT(Mod("10", "20"), IsOkAndHolds("10"));
  EXPECT_THAT(Mod("123", "123"), IsOkAndHolds("0"));
  EXPECT_THAT(Mod("-123", "10"), IsOkAndHolds("-3"));
  EXPECT_THAT(Mod("-10", "20"), IsOkAndHolds("-10"));
  EXPECT_THAT(Mod("-123", "123"), IsOkAndHolds("0"));
  EXPECT_THAT(Mod("123", "-10"), IsOkAndHolds("3"));
}

TEST_F(ModTest, ReturnsModWithDividendDecimalCases) {
  EXPECT_THAT(Mod("123", "200.123456789"), IsOkAndHolds("123.000000000"));
  EXPECT_THAT(Mod("-123.1", "200.123456789"), IsOkAndHolds("-123.100000000"));
  EXPECT_THAT(Mod("123", "123.000000000"), IsOkAndHolds("0.000000000"));
  EXPECT_THAT(Mod("1", MaxNumericString()),
              IsOkAndHolds(absl::StrCat(
                  "1.", std::string(kMaxPGNumericFractionalDigits, '0'))));
}

TEST_F(ModTest, ReturnsNaNForInfinityDivisors) {
  EXPECT_THAT(Mod(kPGNumericPositiveInfinity, "1"),
              IsOkAndHolds(kPGNumericNaN));
  EXPECT_THAT(Mod(kPGNumericNegativeInfinity, "1"),
              IsOkAndHolds(kPGNumericNaN));
}

TEST_F(ModTest, ReturnsDivisorForInfinityDividends) {
  EXPECT_THAT(Mod("1", kPGNumericPositiveInfinity), IsOkAndHolds("1"));
  EXPECT_THAT(Mod("1", kPGNumericNegativeInfinity), IsOkAndHolds("1"));
}

TEST_F(ModTest, ReturnsNaNForNaNDivisorOrDividend) {
  EXPECT_THAT(Mod(kPGNumericNaN, "1"), IsOkAndHolds(kPGNumericNaN));
  EXPECT_THAT(Mod("1", kPGNumericNaN), IsOkAndHolds(kPGNumericNaN));
}

TEST_F(ModTest, ReturnsErrorForInvalidInput) {
  EXPECT_THAT(Mod("abcd", "123"), StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(Mod("123", ""), StatusIs(absl::StatusCode::kInvalidArgument));
}

}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
