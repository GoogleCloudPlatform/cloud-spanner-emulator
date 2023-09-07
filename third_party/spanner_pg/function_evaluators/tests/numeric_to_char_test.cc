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
#include "third_party/spanner_pg/function_evaluators/tests/test_base.h"
#include "third_party/spanner_pg/interface/formatting_evaluators.h"

namespace postgres_translator::function_evaluators {
namespace {

using ::testing::HasSubstr;
using ::testing::StrEq;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

class NumericToCharTest : public PgEvaluatorTest {
 protected:
  void TearDown() override {
    PgEvaluatorTest::TearDown();
    CleanupPostgresNumberCache();
  }
};

TEST_F(NumericToCharTest, FormatNumberWithDigit9) {
  EXPECT_THAT(NumericToChar("123.45", "999"), IsOkAndHolds(StrEq(" 123")));
  EXPECT_THAT(NumericToChar("-123.45", "999"), IsOkAndHolds(StrEq("-123")));
  EXPECT_THAT(NumericToChar("123.45", "9999"), IsOkAndHolds(StrEq("  123")));
  EXPECT_THAT(NumericToChar("NaN", "999"), IsOkAndHolds(StrEq(" NaN")));
  EXPECT_THAT(NumericToChar("NaN", "9999"), IsOkAndHolds(StrEq("  NaN")));
}

TEST_F(NumericToCharTest, FormatNumberWithDigit0) {
  EXPECT_THAT(NumericToChar("123.45", "000"), IsOkAndHolds(StrEq(" 123")));
  EXPECT_THAT(NumericToChar("123.45", "0000"), IsOkAndHolds(StrEq(" 0123")));
  EXPECT_THAT(NumericToChar("NaN", "000"), IsOkAndHolds(StrEq(" NaN")));
  EXPECT_THAT(NumericToChar("NaN", "0000"), IsOkAndHolds(StrEq(" 0NaN")));
}

TEST_F(NumericToCharTest, FormatNumberWithDecimalPoint) {
  EXPECT_THAT(NumericToChar("123.45", "999.99"),
              IsOkAndHolds(StrEq(" 123.45")));
  EXPECT_THAT(NumericToChar("123.45", "999.00"),
              IsOkAndHolds(StrEq(" 123.45")));
  EXPECT_THAT(NumericToChar("NaN", "999.00"), IsOkAndHolds(StrEq(" NaN")));
  // Rounds up
  EXPECT_THAT(NumericToChar("123.45", "999.9"), IsOkAndHolds(StrEq(" 123.5")));
  EXPECT_THAT(NumericToChar("123.45", "999.0"), IsOkAndHolds(StrEq(" 123.5")));
  EXPECT_THAT(NumericToChar("123.45", "9999.999"),
              IsOkAndHolds(StrEq("  123.450")));
  EXPECT_THAT(NumericToChar("123.45", "0000.000"),
              IsOkAndHolds(StrEq(" 0123.450")));
}

TEST_F(NumericToCharTest, FormatNumberRoundsNumber) {
  // Rounds down
  EXPECT_THAT(NumericToChar("0.1", "9"), IsOkAndHolds(StrEq(" 0")));
  EXPECT_THAT(NumericToChar("0.4", "9"), IsOkAndHolds(StrEq(" 0")));
  EXPECT_THAT(NumericToChar("1.2", "9"), IsOkAndHolds(StrEq(" 1")));
  EXPECT_THAT(NumericToChar("1.4", "9"), IsOkAndHolds(StrEq(" 1")));
  EXPECT_THAT(NumericToChar("1.49", "9"), IsOkAndHolds(StrEq(" 1")));
  EXPECT_THAT(NumericToChar("1.44", "9.9"), IsOkAndHolds(StrEq(" 1.4")));

  // Rounds up
  EXPECT_THAT(NumericToChar("0.5", "9"), IsOkAndHolds(StrEq(" 1")));
  EXPECT_THAT(NumericToChar("1.5", "9"), IsOkAndHolds(StrEq(" 2")));
  EXPECT_THAT(NumericToChar("1.9", "9"), IsOkAndHolds(StrEq(" 2")));
  EXPECT_THAT(NumericToChar("1.45", "9.9"), IsOkAndHolds(StrEq(" 1.5")));
  EXPECT_THAT(NumericToChar("1.451", "9.9"), IsOkAndHolds(StrEq(" 1.5")));
  EXPECT_THAT(NumericToChar("1.46", "9.9"), IsOkAndHolds(StrEq(" 1.5")));
  EXPECT_THAT(NumericToChar("0.51", "9"), IsOkAndHolds(StrEq(" 1")));

  // Negative numbers
  EXPECT_THAT(NumericToChar("-0.1", "9"), IsOkAndHolds(StrEq(" 0")));
  EXPECT_THAT(NumericToChar("-0.5", "9"), IsOkAndHolds(StrEq("-1")));
  EXPECT_THAT(NumericToChar("-0.51", "9"), IsOkAndHolds(StrEq("-1")));
  EXPECT_THAT(NumericToChar("-0.9", "9"), IsOkAndHolds(StrEq("-1")));
  EXPECT_THAT(NumericToChar("-1.49", "9"), IsOkAndHolds(StrEq("-1")));
  EXPECT_THAT(NumericToChar("-1.5", "9"), IsOkAndHolds(StrEq("-2")));
  EXPECT_THAT(NumericToChar("-1.9", "9"), IsOkAndHolds(StrEq("-2")));
}

TEST_F(NumericToCharTest, FormatNumberWithThousandsSeparator) {
  EXPECT_THAT(NumericToChar("123456.789", "999,999.999"),
              IsOkAndHolds(StrEq(" 123,456.789")));
  EXPECT_THAT(NumericToChar("NaN", "999,999.999"),
              IsOkAndHolds(StrEq("     NaN")));
}

TEST_F(NumericToCharTest, FormatNumberWithAngleBrackets) {
  EXPECT_THAT(NumericToChar("123.45", "999.99PR"),
              IsOkAndHolds(StrEq(" 123.45 ")));
  EXPECT_THAT(NumericToChar("-123.45", "999.99PR"),
              IsOkAndHolds(StrEq("<123.45>")));
  EXPECT_THAT(NumericToChar("NaN", "999PR"), IsOkAndHolds(StrEq(" NaN ")));
}

TEST_F(NumericToCharTest, FormatNumberWithAnchoredSign) {
  EXPECT_THAT(NumericToChar("123.45", "999.99S"),
              IsOkAndHolds(StrEq("123.45+")));
  EXPECT_THAT(NumericToChar("123.45", "S999.99"),
              IsOkAndHolds(StrEq("+123.45")));
  EXPECT_THAT(NumericToChar("123.45", "9S99.99"),
              IsOkAndHolds(StrEq("+123.45")));
  EXPECT_THAT(NumericToChar("123.45", "99S9.99"),
              IsOkAndHolds(StrEq("+123.45")));
  EXPECT_THAT(NumericToChar("NaN", "S999"), IsOkAndHolds(StrEq("+NaN")));
}

// Locale is hardcoded to en_US in Spangres
TEST_F(NumericToCharTest, FormatNumberWithLocalePatterns) {
  // Currency symbol
  EXPECT_THAT(NumericToChar("123.45", "L999.99"),
              IsOkAndHolds(StrEq("$ 123.45")));
  // Decimal point
  EXPECT_THAT(NumericToChar("123.45", "999D99"),
              IsOkAndHolds(StrEq(" 123.45")));
  // Group separator
  EXPECT_THAT(NumericToChar("123456.789", "999G999.999"),
              IsOkAndHolds(StrEq(" 123,456.789")));
  // Minus sign
  EXPECT_THAT(NumericToChar("-12.34", "9MI9D99"),
              IsOkAndHolds(StrEq("1-2.34")));
  EXPECT_THAT(NumericToChar("NaN", "999MI"), IsOkAndHolds(StrEq("NaN ")));
  // Plus sign
  EXPECT_THAT(NumericToChar("12.34", "9PL9.99"),
              IsOkAndHolds(StrEq(" 1+2.34")));
  EXPECT_THAT(NumericToChar("NaN", "999PL"), IsOkAndHolds(StrEq(" NaN+")));
  // Plus/minus sign
  EXPECT_THAT(NumericToChar("-12.34", "9SG9.99"),
              IsOkAndHolds(StrEq("1-2.34")));
  EXPECT_THAT(NumericToChar("12.34", "9SG9.99"), IsOkAndHolds(StrEq("1+2.34")));
  EXPECT_THAT(NumericToChar("NaN", "999S"), IsOkAndHolds(StrEq("NaN+")));
  // Upper case ordinal number suffix (rounds down)
  EXPECT_THAT(NumericToChar("0.49", "9TH"), IsOkAndHolds(StrEq(" 0TH")));
  EXPECT_THAT(NumericToChar("1.49", "9TH"), IsOkAndHolds(StrEq(" 1ST")));
  EXPECT_THAT(NumericToChar("2.49", "9TH"), IsOkAndHolds(StrEq(" 2ND")));
  EXPECT_THAT(NumericToChar("3.49", "9TH"), IsOkAndHolds(StrEq(" 3RD")));
  EXPECT_THAT(NumericToChar("4.49", "9TH"), IsOkAndHolds(StrEq(" 4TH")));
  // Upper case ordinal number suffix (rounds up)
  EXPECT_THAT(NumericToChar("1.5", "9TH"), IsOkAndHolds(StrEq(" 2ND")));

  // Lower case ordinal number suffix (rounds down)
  EXPECT_THAT(NumericToChar("0.49", "9th"), IsOkAndHolds(StrEq(" 0th")));
  EXPECT_THAT(NumericToChar("1.49", "9th"), IsOkAndHolds(StrEq(" 1st")));
  EXPECT_THAT(NumericToChar("2.49", "9th"), IsOkAndHolds(StrEq(" 2nd")));
  EXPECT_THAT(NumericToChar("3.49", "9th"), IsOkAndHolds(StrEq(" 3rd")));
  EXPECT_THAT(NumericToChar("4.49", "9th"), IsOkAndHolds(StrEq(" 4th")));
  // Lower case ordinal number suffix (rounds up)
  EXPECT_THAT(NumericToChar("1.5", "9th"), IsOkAndHolds(StrEq(" 2nd")));
}

TEST_F(NumericToCharTest, FormatNumberAsRomanNumerals) {
  // Rounds down
  EXPECT_THAT(NumericToChar("1.4", "9RN"),
              IsOkAndHolds(StrEq("              I")));
  EXPECT_THAT(NumericToChar("2.4", "9RN"),
              IsOkAndHolds(StrEq("             II")));
  EXPECT_THAT(NumericToChar("3.4", "9RN"),
              IsOkAndHolds(StrEq("            III")));
  EXPECT_THAT(NumericToChar("4.4", "9RN"),
              IsOkAndHolds(StrEq("             IV")));
  EXPECT_THAT(NumericToChar("5.4", "9RN"),
              IsOkAndHolds(StrEq("              V")));
  EXPECT_THAT(NumericToChar("10.4", "9RN"),
              IsOkAndHolds(StrEq("              X")));
  EXPECT_THAT(NumericToChar("50.4", "9RN"),
              IsOkAndHolds(StrEq("              L")));
  EXPECT_THAT(NumericToChar("100.4", "9RN"),
              IsOkAndHolds(StrEq("              C")));
  EXPECT_THAT(NumericToChar("500.4", "9RN"),
              IsOkAndHolds(StrEq("              D")));
  EXPECT_THAT(NumericToChar("1000.4", "9RN"),
              IsOkAndHolds(StrEq("              M")));
  EXPECT_THAT(NumericToChar("3999.4", "9RN"),
              IsOkAndHolds(StrEq("      MMMCMXCIX")));

  // Rounds up
  EXPECT_THAT(NumericToChar("5.5", "9RN"),
              IsOkAndHolds(StrEq("             VI")));
}

TEST_F(NumericToCharTest, ShiftNumberByPowersOfTen) {
  EXPECT_THAT(NumericToChar("9.9", "9V9"), IsOkAndHolds(StrEq(" 99")));
  EXPECT_THAT(NumericToChar("9.9", "9V99999"), IsOkAndHolds(StrEq(" 990000")));
  EXPECT_THAT(NumericToChar("NaN", "999V9"), IsOkAndHolds(StrEq("  NaN")));
  EXPECT_THAT(NumericToChar("NaN", "999V99"), IsOkAndHolds(StrEq("   NaN")));
}

TEST_F(NumericToCharTest, FormatNumberWithScientificNotation) {
  EXPECT_THAT(NumericToChar("123456.789", "9.99EEEE"),
              IsOkAndHolds(StrEq(" 1.23e+05")));
}

TEST_F(NumericToCharTest, FormatNumberWithFillMode) {
  EXPECT_THAT(NumericToChar("1.2", "9RNFM"), IsOkAndHolds(StrEq("I")));
  EXPECT_THAT(NumericToChar("123.45", "9999.999FM"),
              IsOkAndHolds(StrEq("123.45")));
  EXPECT_THAT(NumericToChar("NaN", "999999FM"), IsOkAndHolds(StrEq("NaN")));
}

TEST_F(NumericToCharTest, FormatNumberWithStringLiterals) {
  EXPECT_THAT(NumericToChar("485.67", "\"Good number:\"999.99"),
              IsOkAndHolds(StrEq("Good number: 485.67")));
  EXPECT_THAT(NumericToChar("NaN", "\"Not a number:\"999"),
              IsOkAndHolds(StrEq("Not a number: NaN")));
}

TEST_F(NumericToCharTest, FormatNumberEdgeCases) {
  // Maintains digits after 0
  EXPECT_THAT(NumericToChar("12.34", "0999.9"), IsOkAndHolds(StrEq(" 0012.3")));

  // Angle brackets format along with decimal / thousands separator
  EXPECT_THAT(NumericToChar("-123.5", "0000.00PR"),
              IsOkAndHolds(StrEq("<0123.50>")));
  EXPECT_THAT(NumericToChar("-123.5", "0000PR,"),
              IsOkAndHolds(StrEq("<0124>,")));
  EXPECT_THAT(NumericToChar("-123.5", "0000PR."), IsOkAndHolds(StrEq("<0124")));
  EXPECT_THAT(NumericToChar("-123.5", "0000PR,."),
              IsOkAndHolds(StrEq("<0124,")));

  // Minus sign is replaced by space on positive numbers
  EXPECT_THAT(NumericToChar("12.49", "0MI0.0"), IsOkAndHolds(StrEq("1 2.5")));
  // Plus sign is replaced by space on negative numbers
  EXPECT_THAT(NumericToChar("-12.46", "0PL0.0"), IsOkAndHolds(StrEq("-1 2.5")));

  // Ordinal number suffixes are not applied to values < 0
  EXPECT_THAT(NumericToChar("-123.1", "000TH"), IsOkAndHolds(StrEq("-123")));
  EXPECT_THAT(NumericToChar("-123.1", "000th"), IsOkAndHolds(StrEq("-123")));
}

TEST_F(NumericToCharTest,
       ReturnsPlaceholderWhenNumberCanNotBeFormattedWithValidPattern) {
  // Less significant digits in format then in given number
  EXPECT_THAT(NumericToChar("123.45", "00.00"), IsOkAndHolds(StrEq(" ##.##")));
  EXPECT_THAT(NumericToChar("123.45", "9PL9"), IsOkAndHolds(StrEq(" #+#")));
  EXPECT_THAT(NumericToChar("123.45", "9MI9"), IsOkAndHolds(StrEq("# #")));
  // Only 1-3999 roman numbers are supported (0.4 is rounded down / 3999.5 is
  // rounded up)
  EXPECT_THAT(NumericToChar("0.4", "9RN"),
              IsOkAndHolds(StrEq("###############")));
  EXPECT_THAT(NumericToChar("3999.5", "9RN"),
              IsOkAndHolds(StrEq("###############")));
  // NaN and scientfic notation
  EXPECT_THAT(NumericToChar("NaN", "9EEEE"), IsOkAndHolds(StrEq(" #.####")));
  EXPECT_THAT(NumericToChar("NaN", "999EEEE"),
              IsOkAndHolds(StrEq(" ###.####")));
}

TEST_F(NumericToCharTest, ReturnsEmptyStringWhenFormatIsEmpty) {
  EXPECT_THAT(NumericToChar("123.45", ""), IsOkAndHolds(StrEq("")));
}

TEST_F(NumericToCharTest, ReturnsErrorWhenFormatIsInvalid) {
  EXPECT_THAT(NumericToChar("-9.9", "9PR.9"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("\"9\" must be ahead of \"PR\"")));
  EXPECT_THAT(
      NumericToChar("9.9", "9.9V9"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("cannot use \"V\" and decimal point together")));
  EXPECT_THAT(NumericToChar("NaN", "999TH"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("\"NaN\" is not a number")));
}

TEST_F(NumericToCharTest, ReturnsErrorForInvalidNaNFormatting) {
  EXPECT_THAT(NumericToChar("NaN", "999th"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("\"NaN\" is not a number")));
  EXPECT_THAT(NumericToChar("NaN", "RN"),
              StatusIs(absl::StatusCode::kUnimplemented,
                       HasSubstr("cannot convert NaN to integer")));
}

TEST_F(NumericToCharTest, ReturnsErrorWhenGivenNumberIsNotANumeric) {
  EXPECT_THAT(
      NumericToChar("abc", "999"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("invalid input syntax for type numeric: \"abc\"")));
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
  EXPECT_THAT(NumericToChar(test_case.input_value, test_case.input_format),
              IsOkAndHolds(StrEq(test_case.expected_output)));
}
INSTANTIATE_TEST_SUITE_P(
    RegressionTestSuite, RegressionTest,
    testing::Values(
        RegressionTestCase({"0", "9G999G999G999G999G999",
                            "                     0"}),
        RegressionTestCase({"0", "9G999G999G999G999G999D999G999G999G999G999",
                            "                      .000,000,000,000,000"}),
        RegressionTestCase({"0", "9999999999999999.999999999999999PR",
                            "                 .000000000000000 "}),
        RegressionTestCase({"0", "9999999999999999.999999999999999S",
                            "                .000000000000000+"}),
        RegressionTestCase({"0", "MI9999999999999999.999999999999999",
                            "                 .000000000000000"}),
        RegressionTestCase({"0", "FMS9999999999999999.999999999999999", "+0."}),
        RegressionTestCase({"0", "FM9999999999999999.999999999999999THPR",
                            "0."}),
        RegressionTestCase({"0", "SG9999999999999999.999999999999999th",
                            "+                .000000000000000"}),
        RegressionTestCase({"0", "0999999999999999.999999999999999",
                            " 0000000000000000.000000000000000"}),
        RegressionTestCase({"0", "S0999999999999999.999999999999999",
                            "+0000000000000000.000000000000000"}),
        RegressionTestCase({"0", "FM0999999999999999.999999999999999",
                            "0000000000000000."}),
        RegressionTestCase({"0", "FM9999999999999999.099999999999999", ".0"}),
        RegressionTestCase({"0", "FM9999999999990999.990999999999999",
                            "0000.000"}),
        RegressionTestCase({"0", "FM0999999999999999.999909999999999",
                            "0000000000000000.00000"}),
        RegressionTestCase({"0", "FM9999999990999999.099999999999999",
                            "0000000.0"}),
        RegressionTestCase({"0", "L9999999999999999.099999999999999",
                            "$                 .000000000000000"}),
        RegressionTestCase({"0", "FM9999999999999999.99999999999999", "0."}),
        RegressionTestCase({"0",
                            "S 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 9 "
                            "9 9 9 9 9 9 9 9 9 9",
                            "                                 +. 0 0 0 0 0 0 0 "
                            "0 0 0 0 0 0 0 0 0 0"}),
        RegressionTestCase({"0",
                            "FMS 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 "
                            "9 9 9 9 9 9 9 9 9 9 9",
                            "                +0 .                 "}),
        RegressionTestCase(
            {"0",
             "99999 \"text\" 9999 \"9999\" 999 \"\\\"text between quote "
             "marks\\\"\" 9999",
             "      text      9999     \"text between quote marks\"     0"}),
        RegressionTestCase({"0", "999999SG9999999999", "      +         0"}),
        RegressionTestCase({"0", "FM9999999999999999.999999999999999", "0."}),
        RegressionTestCase({"0", "9.999EEEE", " 0.000e+00"}),

        RegressionTestCase({"93901.57763026", "9G999G999G999G999G999",
                            "                93,902"}),
        RegressionTestCase({"93901.57763026",
                            "9G999G999G999G999G999D999G999G999G999G999",
                            "                93,901.577,630,260,000,000"}),
        RegressionTestCase({"93901.57763026",
                            "9999999999999999.999999999999999PR",
                            "            93901.577630260000000 "}),
        RegressionTestCase({"93901.57763026",
                            "9999999999999999.999999999999999S",
                            "           93901.577630260000000+"}),
        RegressionTestCase({"93901.57763026",
                            "MI9999999999999999.999999999999999",
                            "            93901.577630260000000"}),
        RegressionTestCase({"93901.57763026",
                            "FMS9999999999999999.999999999999999",
                            "+93901.57763026"}),
        RegressionTestCase({"93901.57763026",
                            "FM9999999999999999.999999999999999THPR",
                            "93901.57763026"}),
        RegressionTestCase({"93901.57763026",
                            "SG9999999999999999.999999999999999th",
                            "+           93901.577630260000000"}),
        RegressionTestCase({"93901.57763026",
                            "0999999999999999.999999999999999",
                            " 0000000000093901.577630260000000"}),
        RegressionTestCase({"93901.57763026",
                            "S0999999999999999.999999999999999",
                            "+0000000000093901.577630260000000"}),
        RegressionTestCase({"93901.57763026",
                            "FM0999999999999999.999999999999999",
                            "0000000000093901.57763026"}),
        RegressionTestCase({"93901.57763026",
                            "FM9999999999999999.099999999999999",
                            "93901.57763026"}),
        RegressionTestCase({"93901.57763026",
                            "FM9999999999990999.990999999999999",
                            "93901.57763026"}),
        RegressionTestCase({"93901.57763026",
                            "FM0999999999999999.999909999999999",
                            "0000000000093901.57763026"}),
        RegressionTestCase({"93901.57763026",
                            "FM9999999990999999.099999999999999",
                            "0093901.57763026"}),
        RegressionTestCase({"93901.57763026",
                            "L9999999999999999.099999999999999",
                            "$            93901.577630260000000"}),
        RegressionTestCase({"93901.57763026",
                            "FM9999999999999999.99999999999999",
                            "93901.57763026"}),
        RegressionTestCase({"93901.57763026",
                            "S 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 9 "
                            "9 9 9 9 9 9 9 9 9 9",
                            "                       +9 3 9 0 1 . 5 7 7 6 3 0 2 "
                            "6 0 0 0 0 0 0 0 0 0"}),
        RegressionTestCase(
            {"93901.57763026",
             "FMS 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 9 9 9 9 9 9 9 "
             "9 9 9 9",
             "            +9 3 9 0 1 . 5 7 7 6 3 0 2 6         "}),
        RegressionTestCase(
            {"93901.57763026",
             "99999 \"text\" 9999 \"9999\" 999 \"\\\"text between quote "
             "marks\\\"\" 9999",
             "      text      9999    9 \"text between quote marks\" 3902"}),
        RegressionTestCase({"93901.57763026", "999999SG9999999999",
                            "      +     93902"}),
        RegressionTestCase({"93901.57763026",
                            "FM9999999999999999.999999999999999",
                            "93901.57763026"}),
        RegressionTestCase({"93901.57763026", "9.999EEEE", " 9.390e+04"}),

        RegressionTestCase({"-34338492.215397047", "9G999G999G999G999G999",
                            "           -34,338,492"}),
        RegressionTestCase({"-34338492.215397047",
                            "9G999G999G999G999G999D999G999G999G999G999",
                            "           -34,338,492.215,397,047,000,000"}),
        RegressionTestCase({"-34338492.215397047",
                            "9999999999999999.999999999999999PR",
                            "        <34338492.215397047000000>"}),
        RegressionTestCase({"-34338492.215397047",
                            "9999999999999999.999999999999999S",
                            "        34338492.215397047000000-"}),
        RegressionTestCase({"-34338492.215397047",
                            "MI9999999999999999.999999999999999",
                            "-        34338492.215397047000000"}),
        RegressionTestCase({"-34338492.215397047",
                            "FMS9999999999999999.999999999999999",
                            "-34338492.215397047"}),
        RegressionTestCase({"-34338492.215397047",
                            "FM9999999999999999.999999999999999THPR",
                            "<34338492.215397047>"}),
        RegressionTestCase({"-34338492.215397047",
                            "SG9999999999999999.999999999999999th",
                            "-        34338492.215397047000000"}),
        RegressionTestCase({"-34338492.215397047",
                            "0999999999999999.999999999999999",
                            "-0000000034338492.215397047000000"}),
        RegressionTestCase({"-34338492.215397047",
                            "S0999999999999999.999999999999999",
                            "-0000000034338492.215397047000000"}),
        RegressionTestCase({"-34338492.215397047",
                            "FM0999999999999999.999999999999999",
                            "-0000000034338492.215397047"}),
        RegressionTestCase({"-34338492.215397047",
                            "FM9999999999999999.099999999999999",
                            "-34338492.215397047"}),
        RegressionTestCase({"-34338492.215397047",
                            "FM9999999999990999.990999999999999",
                            "-34338492.215397047"}),
        RegressionTestCase({"-34338492.215397047",
                            "FM0999999999999999.999909999999999",
                            "-0000000034338492.215397047"}),
        RegressionTestCase({"-34338492.215397047",
                            "FM9999999990999999.099999999999999",
                            "-34338492.215397047"}),
        RegressionTestCase({"-34338492.215397047",
                            "L9999999999999999.099999999999999",
                            "$        -34338492.215397047000000"}),
        RegressionTestCase({"-34338492.215397047",
                            "FM9999999999999999.99999999999999",
                            "-34338492.215397047"}),
        RegressionTestCase({"-34338492.215397047",
                            "S 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 9 "
                            "9 9 9 9 9 9 9 9 9 9",
                            "                 -3 4 3 3 8 4 9 2 . 2 1 5 3 9 7 0 "
                            "4 7 0 0 0 0 0 0 0 0"}),
        RegressionTestCase(
            {"-34338492.215397047",
             "FMS 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 9 9 9 9 9 9 9 "
             "9 9 9 9",
             "         -3 4 3 3 8 4 9 2 . 2 1 5 3 9 7 0 4 7        "}),
        RegressionTestCase(
            {"-34338492.215397047",
             "99999 \"text\" 9999 \"9999\" 999 \"\\\"text between quote "
             "marks\\\"\" 9999",
             "      text    -3 9999 433 \"text between quote marks\" 8492"}),
        RegressionTestCase({"-34338492.215397047", "999999SG9999999999",
                            "      -  34338492"}),
        RegressionTestCase({"-34338492.215397047",
                            "FM9999999999999999.999999999999999",
                            "-34338492.215397047"}),
        RegressionTestCase({"-34338492.215397047", "9.999EEEE", "-3.434e+07"}),

        RegressionTestCase({"100", "foo999", "foo 100"}),
        RegressionTestCase({"100", "f\\oo999", "f\\oo 100"}),
        RegressionTestCase({"100", "f\\\\oo999", "f\\\\oo 100"}),
        RegressionTestCase({"100", "f\\\"oo999", "f\"oo 100"}),
        RegressionTestCase({"100", "f\\\\\"oo999", "f\\\"oo 100"}),
        RegressionTestCase({"100", "f\"ool\"999", "fool 100"}),
        RegressionTestCase({"100", "f\"\\ool\"999", "fool 100"}),
        RegressionTestCase({"100", "f\"\\\\ool\"999", "f\\ool 100"}),
        RegressionTestCase({"100", "f\"ool\\\"999", "fool\"999"}),
        RegressionTestCase({"100", "f\"ool\\\\\"999", "fool\\ 100"})));
}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
