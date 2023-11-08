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
    kMaxPGNumericWholeDigits;
using ::postgres_translator::spangres::datatypes::common::kPGNumericNaN;
using ::postgres_translator::spangres::datatypes::common::
    kPGNumericNegativeInfinity;
using ::postgres_translator::spangres::datatypes::common::
    kPGNumericPositiveInfinity;
using ::postgres_translator::spangres::datatypes::common::MaxNumericString;
using ::postgres_translator::spangres::datatypes::common::MinNumericString;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

class FloorTest : public PgEvaluatorTest {};

TEST_F(FloorTest, ReturnsFloorOfGivenNumber) {
  EXPECT_THAT(Floor("123"), IsOkAndHolds("123"));
  EXPECT_THAT(Floor("0"), IsOkAndHolds("0"));
  EXPECT_THAT(Floor("0.1"), IsOkAndHolds("0"));
  EXPECT_THAT(Floor("0.5"), IsOkAndHolds("0"));
  EXPECT_THAT(Floor("0.9"), IsOkAndHolds("0"));
  EXPECT_THAT(Floor("0.000000000000000000000000001"), IsOkAndHolds("0"));
  EXPECT_THAT(Floor("1.9"), IsOkAndHolds("1"));
  EXPECT_THAT(Floor(MaxNumericString()),
              IsOkAndHolds(std::string(kMaxPGNumericWholeDigits, '9')));
}

TEST_F(FloorTest, ReturnsFloorOfGivenNegativeNumber) {
  EXPECT_THAT(Floor("-123"), IsOkAndHolds("-123"));
  EXPECT_THAT(Floor("-123.1"), IsOkAndHolds("-124"));
  EXPECT_THAT(Floor("-0.1"), IsOkAndHolds("-1"));
  EXPECT_THAT(Floor("-0.5"), IsOkAndHolds("-1"));
  EXPECT_THAT(Floor("-0.9"), IsOkAndHolds("-1"));
}

TEST_F(FloorTest, DoesNothingForSpecialValues) {
  EXPECT_THAT(Floor(kPGNumericPositiveInfinity),
              IsOkAndHolds(kPGNumericPositiveInfinity));
  EXPECT_THAT(Floor(kPGNumericNegativeInfinity),
              IsOkAndHolds(kPGNumericNegativeInfinity));
  EXPECT_THAT(Floor(kPGNumericNaN), IsOkAndHolds(kPGNumericNaN));
}

TEST_F(FloorTest, ReturnsErrorForUnderflow) {
  EXPECT_THAT(Floor(MinNumericString()),
              StatusIs(absl::StatusCode::kOutOfRange));
}

TEST_F(FloorTest, ReturnsErrorForInvalidInput) {
  EXPECT_THAT(Floor("abcd"), StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(Floor(""), StatusIs(absl::StatusCode::kInvalidArgument));
}

}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
