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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "third_party/spanner_pg/function_evaluators/tests/test_base.h"
#include "third_party/spanner_pg/interface/formatting_evaluators.h"

namespace postgres_translator::function_evaluators {
namespace {

using ::testing::HasSubstr;
using ::testing::IsNull;
using ::testing::Pointee;
using ::testing::StrEq;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

class NumericToNumberTest : public PgEvaluatorTest {
 protected:
  void TearDown() override {
    PgEvaluatorTest::TearDown();
    CleanupPostgresNumberCache();
  }
};

TEST_F(NumericToNumberTest, ReturnsNullWhenFormatIsEmpty) {
  EXPECT_THAT(NumericToNumber("123", ""), IsOkAndHolds(IsNull()));
}

TEST_F(NumericToNumberTest, ParsesNumberWithDigit9) {
  EXPECT_THAT(NumericToNumber("123", "9999"),
              IsOkAndHolds(Pointee(StrEq(("123")))));
  EXPECT_THAT(NumericToNumber("123", "999"),
              IsOkAndHolds(Pointee(StrEq(("123")))));
  EXPECT_THAT(NumericToNumber("123", "99"),
              IsOkAndHolds(Pointee(StrEq(("12")))));
  EXPECT_THAT(NumericToNumber("123", "9"), IsOkAndHolds(Pointee(StrEq(("1")))));
}

TEST_F(NumericToNumberTest, ParsesNumberWithDigit0) {
  EXPECT_THAT(NumericToNumber("123", "0000"),
              IsOkAndHolds(Pointee(StrEq(("123")))));
  EXPECT_THAT(NumericToNumber("123", "000"),
              IsOkAndHolds(Pointee(StrEq(("123")))));
  EXPECT_THAT(NumericToNumber("123", "00"),
              IsOkAndHolds(Pointee(StrEq(("12")))));
  EXPECT_THAT(NumericToNumber("123", "0"), IsOkAndHolds(Pointee(StrEq(("1")))));
  EXPECT_THAT(NumericToNumber("00123", "00000"),
              IsOkAndHolds(Pointee(StrEq(("123")))));
}

TEST_F(NumericToNumberTest, ParseNumberWithDecimalPoint) {
  EXPECT_THAT(NumericToNumber("123.456", "999.999"),
              IsOkAndHolds(Pointee(StrEq(("123.456")))));
  EXPECT_THAT(NumericToNumber("123.456", "999.99"),
              IsOkAndHolds(Pointee(StrEq(("123.45")))));
  EXPECT_THAT(NumericToNumber("123.456", "999.9"),
              IsOkAndHolds(Pointee(StrEq(("123.4")))));
  EXPECT_THAT(NumericToNumber("123.456", "999."),
              IsOkAndHolds(Pointee(StrEq(("123")))));
  EXPECT_THAT(NumericToNumber(".456", ".999"),
              IsOkAndHolds(Pointee(StrEq(("0.456")))));
  EXPECT_THAT(NumericToNumber(".456", ".99"),
              IsOkAndHolds(Pointee(StrEq(("0.45")))));
  EXPECT_THAT(NumericToNumber(".456", ".9"),
              IsOkAndHolds(Pointee(StrEq(("0.4")))));
}

TEST_F(NumericToNumberTest, ParseNumberWithThosandsSeparator) {
  EXPECT_THAT(NumericToNumber("123,456", "999,999"),
              IsOkAndHolds(Pointee(StrEq(("123456")))));
  EXPECT_THAT(NumericToNumber("123,456", "999,99"),
              IsOkAndHolds(Pointee(StrEq(("12345")))));
  EXPECT_THAT(NumericToNumber("123,456", "999,9"),
              IsOkAndHolds(Pointee(StrEq(("1234")))));
  EXPECT_THAT(NumericToNumber("123,456", "999,"),
              IsOkAndHolds(Pointee(StrEq(("123")))));
  EXPECT_THAT(NumericToNumber("123,456", "99,9"),
              IsOkAndHolds(Pointee(StrEq(("123")))));
  EXPECT_THAT(NumericToNumber("123,456", "9,9"),
              IsOkAndHolds(Pointee(StrEq(("12")))));
  EXPECT_THAT(NumericToNumber("123,456", "9,"),
              IsOkAndHolds(Pointee(StrEq(("1")))));
  EXPECT_THAT(NumericToNumber("123,456", ",9"),
              IsOkAndHolds(Pointee(StrEq(("1")))));
}

TEST_F(NumericToNumberTest, ParseNumberWithAngleBrackets) {
  EXPECT_THAT(NumericToNumber("<123>", "999PR"),
              IsOkAndHolds(Pointee(StrEq(("-123")))));
  EXPECT_THAT(NumericToNumber("<123>", "99PR"),
              IsOkAndHolds(Pointee(StrEq(("-12")))));
  EXPECT_THAT(NumericToNumber("<123>", "9PR"),
              IsOkAndHolds(Pointee(StrEq(("-1")))));
  EXPECT_THAT(NumericToNumber("123", "999PR"),
              IsOkAndHolds(Pointee(StrEq(("123")))));
  EXPECT_THAT(NumericToNumber("123", "99PR"),
              IsOkAndHolds(Pointee(StrEq(("12")))));
  EXPECT_THAT(NumericToNumber("123", "9PR"),
              IsOkAndHolds(Pointee(StrEq(("1")))));
}

TEST_F(NumericToNumberTest, ParseNumberWithLocalePatterns) {
  // Currency symbol
  EXPECT_THAT(NumericToNumber("$123.45", "L999.99"),
              IsOkAndHolds(Pointee(StrEq(("123.45")))));
  // Decimal point
  EXPECT_THAT(NumericToNumber("123.45", "999D99"),
              IsOkAndHolds(Pointee(StrEq(("123.45")))));
  // Group separator
  EXPECT_THAT(NumericToNumber("123,456", "999G999"),
              IsOkAndHolds(Pointee(StrEq(("123456")))));
  // Minus sign
  EXPECT_THAT(NumericToNumber("-123", "MI999"),
              IsOkAndHolds(Pointee(StrEq(("-123")))));
  // Plus sign
  EXPECT_THAT(NumericToNumber("+123", "PL999"),
              IsOkAndHolds(Pointee(StrEq(("123")))));
  // Plus/minus sign
  EXPECT_THAT(NumericToNumber("-123", "SG999"),
              IsOkAndHolds(Pointee(StrEq(("-123")))));
  EXPECT_THAT(NumericToNumber("+123", "SG999"),
              IsOkAndHolds(Pointee(StrEq(("123")))));
  // Upper case ordinal number suffix
  EXPECT_THAT(NumericToNumber("1ST", "9TH"),
              IsOkAndHolds(Pointee(StrEq(("1")))));
  // Lower case ordinal number suffix
  EXPECT_THAT(NumericToNumber("3rd", "9th"),
              IsOkAndHolds(Pointee(StrEq(("3")))));
}

TEST_F(NumericToNumberTest, DividesNumberByPowersOfTen) {
  EXPECT_THAT(NumericToNumber("99", "9V9"),
              IsOkAndHolds(Pointee(StrEq(("9.9000000000000000")))));
  EXPECT_THAT(NumericToNumber("999", "9V99"),
              IsOkAndHolds(Pointee(StrEq(("9.9900000000000000")))));
  EXPECT_THAT(NumericToNumber("9999", "9V999"),
              IsOkAndHolds(Pointee(StrEq(("9.9990000000000000")))));
}

TEST_F(NumericToNumberTest, ParsesNumberWithStringLiterals) {
  EXPECT_THAT(NumericToNumber("Good number: 123", "\"Good number:\" 999"),
              IsOkAndHolds(Pointee(StrEq(("123")))));
}

TEST_F(NumericToNumberTest, ParsesNumberWithSpaces) {
  EXPECT_THAT(NumericToNumber("1  2 3 ", "9  9 9"),
              IsOkAndHolds(Pointee(StrEq(("123")))));
}

TEST_F(NumericToNumberTest, ReturnsErrorWhenFormatIsInvalid) {
  EXPECT_THAT(NumericToNumber("1.23+e05", "9.99EEEE"),
              StatusIs(absl::StatusCode::kUnimplemented,
                       HasSubstr("\"EEEE\" not supported for input")));
  EXPECT_THAT(NumericToNumber("III", "RN"),
              StatusIs(absl::StatusCode::kUnimplemented,
                       HasSubstr("\"RN\" not supported for input")));
  EXPECT_THAT(NumericToNumber("NaN", "999"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid input syntax for type numeric")));
  EXPECT_THAT(NumericToNumber("123.45", "9.99"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("numeric field overflow")));
}

struct RegressionTestCase {
  std::string input_value;
  std::string input_format;
  std::string expected_output;
};

class RegressionTest : public PgEvaluatorTestWithParam<RegressionTestCase> {
 protected:
  void TearDown() override {
    PgEvaluatorTestWithParam<RegressionTestCase>::TearDown();
    CleanupPostgresNumberCache();
  }
};

// ---- Parameterized tests
// These tests exercise regression scenarios in PostgreSQL

TEST_P(RegressionTest, PostgreSQLRegressionTests) {
  const RegressionTestCase& test_case = GetParam();
  EXPECT_THAT(NumericToNumber(test_case.input_value, test_case.input_format),
              IsOkAndHolds(Pointee(StrEq(test_case.expected_output))));
}
INSTANTIATE_TEST_SUITE_P(
    RegressionTestSuite, RegressionTest,
    testing::Values(
        RegressionTestCase({"-34,338,492", "99G999G999", "-34338492"}),
        RegressionTestCase({"-34,338,492.654,878", "99G999G999D999G999",
                            "-34338492.654878"}),
        RegressionTestCase({"<564646.654564>", "999999.999999PR",
                            "-564646.654564"}),
        RegressionTestCase({"0.00001-", "9.999999S", "-0.00001"}),
        RegressionTestCase({"5.01-", "FM9.999999S", "-5.01"}),
        RegressionTestCase({"5.01-", "FM9.999999MI", "-5.01"}),
        RegressionTestCase({"5 4 4 4 4 8 . 7 8", "9 9 9 9 9 9 . 9 9",
                            "544448.78"}),
        RegressionTestCase({".01", "FM9.99", "0.01"}),
        RegressionTestCase({".0", "99999999.99999999", "0.0"}),
        RegressionTestCase({"0", "99.99", "0"}),
        RegressionTestCase({".-01", "S99.99", "-0.01"}),
        RegressionTestCase({".01-", "99.99S", "-0.01"}),
        RegressionTestCase({" . 0 1-", " 9 9 . 9 9 S", "-0.01"}),
        RegressionTestCase({"34,50", "999,99", "3450"}),
        RegressionTestCase({"123,000", "999G", "123"}),
        RegressionTestCase({"123456", "999G999", "123456"}),
        RegressionTestCase({"$1234.56", "L9,999.99", "1234.56"}),
        RegressionTestCase({"$1234.56", "L99,999.99", "1234.56"}),
        RegressionTestCase({"$1,234.56", "L99,999.99", "1234.56"}),
        RegressionTestCase({"1234.56", "L99,999.99", "1234.56"}),
        RegressionTestCase({"1,234.56", "L99,999.99", "1234.56"}),
        RegressionTestCase({"42nd", "99th", "42"})));
}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
