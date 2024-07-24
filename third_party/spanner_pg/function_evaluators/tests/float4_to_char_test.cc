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

#include <cmath>
#include <limits>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "third_party/spanner_pg/function_evaluators/tests/test_base.h"
#include "third_party/spanner_pg/interface/formatting_evaluators.h"

namespace postgres_translator::function_evaluators {
namespace {

using ::testing::HasSubstr;
using ::testing::StrEq;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

class Float4ToCharTest : public PgEvaluatorTest {
 protected:
  void TearDown() override {
    PgEvaluatorTest::TearDown();
    CleanupPostgresNumberCache();
  }
};

TEST_F(Float4ToCharTest, FormatNumberWithDigit9) {
  EXPECT_THAT(Float4ToChar(123.45f, "999"), IsOkAndHolds(StrEq(" 123")));
  EXPECT_THAT(Float4ToChar(-123.45f, "999"), IsOkAndHolds(StrEq("-123")));
  EXPECT_THAT(Float4ToChar(123.45f, "9999"), IsOkAndHolds(StrEq("  123")));
}

TEST_F(Float4ToCharTest, FormatNumberRoundsNumber) {
  // Rounds down
  EXPECT_THAT(Float4ToChar(0.1f, "9"), IsOkAndHolds(StrEq(" 0")));
  EXPECT_THAT(Float4ToChar(0.4f, "9"), IsOkAndHolds(StrEq(" 0")));
  EXPECT_THAT(Float4ToChar(1.2f, "9"), IsOkAndHolds(StrEq(" 1")));
  EXPECT_THAT(Float4ToChar(1.4f, "9"), IsOkAndHolds(StrEq(" 1")));
  EXPECT_THAT(Float4ToChar(1.49f, "9"), IsOkAndHolds(StrEq(" 1")));
  EXPECT_THAT(Float4ToChar(1.44f, "9.9"), IsOkAndHolds(StrEq(" 1.4")));
  EXPECT_THAT(Float4ToChar(1.449f, "9.9"), IsOkAndHolds(StrEq(" 1.4")));
  // Edge case, 0.5 is rounded down
  EXPECT_THAT(Float4ToChar(0.5f, "9"), IsOkAndHolds(StrEq(" 0")));

  // Rounds up
  EXPECT_THAT(Float4ToChar(1.5f, "9"), IsOkAndHolds(StrEq(" 2")));
  EXPECT_THAT(Float4ToChar(1.9f, "9"), IsOkAndHolds(StrEq(" 2")));
  EXPECT_THAT(Float4ToChar(1.45f, "9.9"), IsOkAndHolds(StrEq(" 1.5")));
  EXPECT_THAT(Float4ToChar(1.451f, "9.9"), IsOkAndHolds(StrEq(" 1.5")));
  EXPECT_THAT(Float4ToChar(1.46f, "9.9"), IsOkAndHolds(StrEq(" 1.5")));
  EXPECT_THAT(Float4ToChar(0.51f, "9"), IsOkAndHolds(StrEq(" 1")));

  // Negative numbers
  EXPECT_THAT(Float4ToChar(-0.1f, "9"), IsOkAndHolds(StrEq("-0")));
  EXPECT_THAT(Float4ToChar(-0.5f, "9"), IsOkAndHolds(StrEq("-0")));
  EXPECT_THAT(Float4ToChar(-0.51f, "9"), IsOkAndHolds(StrEq("-1")));
  EXPECT_THAT(Float4ToChar(-0.9f, "9"), IsOkAndHolds(StrEq("-1")));
  EXPECT_THAT(Float4ToChar(-1.49f, "9"), IsOkAndHolds(StrEq("-1")));
  EXPECT_THAT(Float4ToChar(-1.5f, "9"), IsOkAndHolds(StrEq("-2")));
  EXPECT_THAT(Float4ToChar(-1.9f, "9"), IsOkAndHolds(StrEq("-2")));
}

TEST_F(Float4ToCharTest, FormatNumberWithDigit0) {
  EXPECT_THAT(Float4ToChar(123.45f, "000"), IsOkAndHolds(StrEq(" 123")));
  EXPECT_THAT(Float4ToChar(123.45f, "0000"), IsOkAndHolds(StrEq(" 0123")));
}

TEST_F(Float4ToCharTest, FormatNumberWithDecimalPoint) {
  EXPECT_THAT(Float4ToChar(123.45f, "999.99"), IsOkAndHolds(StrEq(" 123.45")));
  EXPECT_THAT(Float4ToChar(123.45f, "999.00"), IsOkAndHolds(StrEq(" 123.45")));
  // Rounds down
  EXPECT_THAT(Float4ToChar(123.45f, "999.9"), IsOkAndHolds(StrEq(" 123.4")));
  EXPECT_THAT(Float4ToChar(123.45f, "999.0"), IsOkAndHolds(StrEq(" 123.4")));
  EXPECT_THAT(Float4ToChar(123.45f, "9999.999"),
              IsOkAndHolds(StrEq("  123.450")));
  EXPECT_THAT(Float4ToChar(123.45f, "0000.000"),
              IsOkAndHolds(StrEq(" 0123.450")));
}

TEST_F(Float4ToCharTest, FormatNumberWithThousandsSeparator) {
  EXPECT_THAT(Float4ToChar(123456.789f, "999,999.999"),
              IsOkAndHolds(StrEq(" 123,457")));
  EXPECT_THAT(Float4ToChar(12345.67f, "999,999.999"),
              IsOkAndHolds(StrEq("  12,345.7")));
  EXPECT_THAT(Float4ToChar(1234567, "999,999,999.999"),
              IsOkAndHolds(StrEq("   1,234,567")));
}

TEST_F(Float4ToCharTest, FormatNumberWithAngleBrackets) {
  EXPECT_THAT(Float4ToChar(123.45f, "999.99PR"),
              IsOkAndHolds(StrEq(" 123.45 ")));
  EXPECT_THAT(Float4ToChar(-123.45f, "999.99PR"),
              IsOkAndHolds(StrEq("<123.45>")));
}

TEST_F(Float4ToCharTest, FormatNumberWithAnchoredSign) {
  EXPECT_THAT(Float4ToChar(123.45f, "999.99S"), IsOkAndHolds(StrEq("123.45+")));
  EXPECT_THAT(Float4ToChar(123.45f, "S999.99"), IsOkAndHolds(StrEq("+123.45")));
  EXPECT_THAT(Float4ToChar(123.45f, "9S99.99"), IsOkAndHolds(StrEq("+123.45")));
  EXPECT_THAT(Float4ToChar(123.45f, "99S9.99"), IsOkAndHolds(StrEq("+123.45")));
}

// Locale is hardcoded to en_US in Spangres
TEST_F(Float4ToCharTest, FormatNumberWithLocalePatterns) {
  // Currency symbol
  EXPECT_THAT(Float4ToChar(123.45f, "L999.99"),
              IsOkAndHolds(StrEq("$ 123.45")));
  // Decimal point
  EXPECT_THAT(Float4ToChar(123.45f, "999D99"), IsOkAndHolds(StrEq(" 123.45")));
  // Group separator
  EXPECT_THAT(Float4ToChar(12345.6f, "999G999.999"),
              IsOkAndHolds(StrEq("  12,345.6")));
  // Minus sign
  EXPECT_THAT(Float4ToChar(-12.34f, "9MI9D99"), IsOkAndHolds(StrEq("1-2.34")));
  // Plus sign
  EXPECT_THAT(Float4ToChar(12.34f, "9PL9.99"), IsOkAndHolds(StrEq(" 1+2.34")));
  // Plus/minus sign
  EXPECT_THAT(Float4ToChar(-12.34f, "9SG9.99"), IsOkAndHolds(StrEq("1-2.34")));
  EXPECT_THAT(Float4ToChar(12.34f, "9SG9.99"), IsOkAndHolds(StrEq("1+2.34")));
  // Upper case ordinal number suffix (rounds down)
  EXPECT_THAT(Float4ToChar(0.49f, "9TH"), IsOkAndHolds(StrEq(" 0TH")));
  EXPECT_THAT(Float4ToChar(1.49f, "9TH"), IsOkAndHolds(StrEq(" 1ST")));
  EXPECT_THAT(Float4ToChar(2.49f, "9TH"), IsOkAndHolds(StrEq(" 2ND")));
  EXPECT_THAT(Float4ToChar(3.49f, "9TH"), IsOkAndHolds(StrEq(" 3RD")));
  EXPECT_THAT(Float4ToChar(4.49f, "9TH"), IsOkAndHolds(StrEq(" 4TH")));
  // Upper case ordinal number suffix (rounds up)
  EXPECT_THAT(Float4ToChar(1.5f, "9TH"), IsOkAndHolds(StrEq(" 2ND")));

  // Lower case ordinal number suffix (rounds down)
  EXPECT_THAT(Float4ToChar(0.49f, "9th"), IsOkAndHolds(StrEq(" 0th")));
  EXPECT_THAT(Float4ToChar(1.49f, "9th"), IsOkAndHolds(StrEq(" 1st")));
  EXPECT_THAT(Float4ToChar(2.49f, "9th"), IsOkAndHolds(StrEq(" 2nd")));
  EXPECT_THAT(Float4ToChar(3.49f, "9th"), IsOkAndHolds(StrEq(" 3rd")));
  EXPECT_THAT(Float4ToChar(4.49f, "9th"), IsOkAndHolds(StrEq(" 4th")));
  // Lower case ordinal number suffix (rounds up)
  EXPECT_THAT(Float4ToChar(1.5f, "9th"), IsOkAndHolds(StrEq(" 2nd")));
}

TEST_F(Float4ToCharTest, FormatNumberAsRomanNumerals) {
  // Rounds down
  EXPECT_THAT(Float4ToChar(1.4f, "9RN"),
              IsOkAndHolds(StrEq("              I")));
  EXPECT_THAT(Float4ToChar(2.4f, "9RN"),
              IsOkAndHolds(StrEq("             II")));
  EXPECT_THAT(Float4ToChar(3.4f, "9RN"),
              IsOkAndHolds(StrEq("            III")));
  EXPECT_THAT(Float4ToChar(4.4f, "9RN"),
              IsOkAndHolds(StrEq("             IV")));
  EXPECT_THAT(Float4ToChar(5.4f, "9RN"),
              IsOkAndHolds(StrEq("              V")));
  EXPECT_THAT(Float4ToChar(10.4f, "9RN"),
              IsOkAndHolds(StrEq("              X")));
  EXPECT_THAT(Float4ToChar(50.4f, "9RN"),
              IsOkAndHolds(StrEq("              L")));
  EXPECT_THAT(Float4ToChar(100.4f, "9RN"),
              IsOkAndHolds(StrEq("              C")));
  EXPECT_THAT(Float4ToChar(500.4f, "9RN"),
              IsOkAndHolds(StrEq("              D")));
  EXPECT_THAT(Float4ToChar(1000.4f, "9RN"),
              IsOkAndHolds(StrEq("              M")));
  EXPECT_THAT(Float4ToChar(3999.4f, "9RN"),
              IsOkAndHolds(StrEq("      MMMCMXCIX")));

  // Rounds up
  EXPECT_THAT(Float4ToChar(5.5f, "9RN"),
              IsOkAndHolds(StrEq("             VI")));
}

TEST_F(Float4ToCharTest, ShiftNumberByPowersOfTen) {
  EXPECT_THAT(Float4ToChar(9.9f, "9V9"), IsOkAndHolds(StrEq(" 99")));
  EXPECT_THAT(Float4ToChar(9.9f, "9V99999"), IsOkAndHolds(StrEq(" 990000")));
}

TEST_F(Float4ToCharTest, FormatNumberWithScientificNotation) {
  EXPECT_THAT(Float4ToChar(123456.789f, "9.99EEEE"),
              IsOkAndHolds(StrEq(" 1.23e+05")));
}

TEST_F(Float4ToCharTest, FormatNumberWithFillMode) {
  EXPECT_THAT(Float4ToChar(1.2f, "9RNFM"), IsOkAndHolds(StrEq("I")));
  EXPECT_THAT(Float4ToChar(123.45f, "9999.999FM"),
              IsOkAndHolds(StrEq("123.45")));
}

TEST_F(Float4ToCharTest, FormatNumberWithStringLiterals) {
  EXPECT_THAT(Float4ToChar(485.67f, "\"Good number:\"999.99"),
              IsOkAndHolds(StrEq("Good number: 485.67")));
}

TEST_F(Float4ToCharTest, FormatNumberEdgeCases) {
  // Maintains digits after 0
  EXPECT_THAT(Float4ToChar(12.34f, "0999.9"), IsOkAndHolds(StrEq(" 0012.3")));

  // Angle brackets format along with decimal / thousands separator
  EXPECT_THAT(Float4ToChar(-123.5f, "0000.00PR"),
              IsOkAndHolds(StrEq("<0123.50>")));
  EXPECT_THAT(Float4ToChar(-123.5f, "0000PR,"), IsOkAndHolds(StrEq("<0124>,")));
  EXPECT_THAT(Float4ToChar(-123.5f, "0000PR."), IsOkAndHolds(StrEq("<0124")));
  EXPECT_THAT(Float4ToChar(-123.5f, "0000PR,."),
              IsOkAndHolds(StrEq("<0124,")));

  // Minus sign is replaced by space on positive numbers
  EXPECT_THAT(Float4ToChar(12.49f, "0MI0.0"), IsOkAndHolds(StrEq("1 2.5")));
  // Plus sign is replaced by space on negative numbers
  EXPECT_THAT(Float4ToChar(-12.46f, "0PL0.0"), IsOkAndHolds(StrEq("-1 2.5")));

  // Ordinal number suffixes are not applied to values < 0
  EXPECT_THAT(Float4ToChar(-123.1f, "000TH"), IsOkAndHolds(StrEq("-123")));
  EXPECT_THAT(Float4ToChar(-123.1f, "000th"), IsOkAndHolds(StrEq("-123")));
}

TEST_F(Float4ToCharTest,
       ReturnsPlaceholderWhenNumberCanNotBeFormattedWithValidPattern) {
  // Less significant digits in format then in given number
  EXPECT_THAT(Float4ToChar(123.45f, "00.00"), IsOkAndHolds(StrEq(" ##.##")));
  EXPECT_THAT(Float4ToChar(123.45f, "9PL9"), IsOkAndHolds(StrEq(" #+#")));
  EXPECT_THAT(Float4ToChar(123.45f, "9MI9"), IsOkAndHolds(StrEq("# #")));
  // Only 1-3999 roman numbers are supported (0.5 is rounded down / 3999.5 is
  // rounded up)
  EXPECT_THAT(Float4ToChar(0.5f, "9RN"),
              IsOkAndHolds(StrEq("###############")));
  EXPECT_THAT(Float4ToChar(3999.5f, "9RN"),
              IsOkAndHolds(StrEq("###############")));
}

TEST_F(Float4ToCharTest, ReturnsEmptyStringWhenFormatIsEmpty) {
  EXPECT_THAT(Float4ToChar(123.45f, ""), IsOkAndHolds(StrEq("")));
}

TEST_F(Float4ToCharTest, MsanViolationTest) {
  // Because the format goes beyond the allowed number of double digits the
  // output template (with '#'s) is shrunk. This originally caused a pointer
  // to overflow to an invalid position. We can run this test with msan to
  // guarantee such bug is fixed. This is also tested in the corresponding fuzz
  // tests.
  EXPECT_THAT(Float4ToChar(12345678901, "FM9999999999D999990"),
              IsOkAndHolds(StrEq("##########.")));
  EXPECT_THAT(Float4ToChar(12345678901, "FM9999999999D9999900000000000000000"),
              IsOkAndHolds(StrEq("##########.")));
}

TEST_F(Float4ToCharTest, AsanViolationTest) {
  EXPECT_THAT(Float4ToChar(std::numeric_limits<float>::quiet_NaN(), "rn"),
              IsOkAndHolds(StrEq("###############")));
}

TEST_F(Float4ToCharTest, ReturnsErrorWhenFormatIsInvalid) {
  EXPECT_THAT(Float4ToChar(-9.9f, "9PR.9"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("\"9\" must be ahead of \"PR\"")));
  EXPECT_THAT(
      Float4ToChar(9.9f, "9.9V9"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("cannot use \"V\" and decimal point together")));
  EXPECT_THAT(Float4ToChar(9.9f, "9..9"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("multiple decimal points")));
  EXPECT_THAT(Float4ToChar(9.9f, "EEEE9.9"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("\"EEEE\" must be the last pattern used")));
  EXPECT_THAT(Float4ToChar(9.9f, "9EEEE EEEE"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("cannot use \"EEEE\" twice")));
  EXPECT_THAT(
      Float4ToChar(9.9f, "S9.9EEEE"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("\"EEEE\" is incompatible with other formats")));
}

struct RegressionTestCase {
  double input_value;
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
  EXPECT_THAT(Float4ToChar(test_case.input_value, test_case.input_format),
              IsOkAndHolds(StrEq(test_case.expected_output)));
}
INSTANTIATE_TEST_SUITE_P(
    RegressionTestSuite, RegressionTest,
    testing::Values(
        RegressionTestCase({0, "9G999G999G999G999G999",
                            "                     0"}),
        RegressionTestCase({0, "9G999G999D999G999", "          .000,00"}),
        RegressionTestCase({0, "9999999999999999.999999999999999PR",
                            "                 .00000 "}),
        RegressionTestCase({0, "9999999999999999.99999S",
                               "                .00000+"}),
        RegressionTestCase({0, "MI9999999999999999.99999",
                               "                 .00000"}),
        RegressionTestCase({0, "FMS9999999999999999.999999999999999", "+0."}),
        RegressionTestCase({0, "FM9999999999999999.999999999999999THPR", "0."}),
        RegressionTestCase({0, "SG9999999999999999.999999th",
                            "+                .00000"}),
        RegressionTestCase({0, "0999999999999999.999999",
                            " 0000000000000000.00000"}),
        RegressionTestCase({0, "S0999999999999999.999999",
                            "+0000000000000000.00000"}),
        RegressionTestCase({0, "FM0999999999999999.999999999999999",
                            "0000000000000000."}),
        RegressionTestCase({0, "FM9999999999999999.099999999999999", ".0"}),
        RegressionTestCase({0, "FM9999999999990999.990999999999999",
                            "0000.000"}),
        RegressionTestCase({0, "FM0999999999999999.999909999999999",
                            "0000000000000000.00000"}),
        RegressionTestCase({0, "FM9999999990999999.099999999999999",
                            "0000000.0"}),
        RegressionTestCase({0, "L9999999999999999.099999999999999",
                            "$                 .00000"}),
        RegressionTestCase({0, "FM9999999999999999.99999999999999", "0."}),
        RegressionTestCase({0,
                            "S 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 9 "
                            "9 9 9 9 9 9 9 9 9 9",
                            "                                 +. 0 0 0 0 0 "}),
        RegressionTestCase({0,
                            "FMS 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 "
                            "9 9 9 9 9 9 9 9 9 9 9",
                            "                +0 .                 "}),
        RegressionTestCase(
            {0,
             "99999 \"text\" 9999 \"9999\" 999 \"\\\"text between quote "
             "marks\\\"\" 9999",
             "      text      9999     \"text between quote marks\"     0"}),
        RegressionTestCase({0, "999999SG9999999999", "      +         0"}),
        RegressionTestCase({0, "FM9999999999999999.999999999999999", "0."}),
        RegressionTestCase({0, "9.999EEEE", " 0.000e+00"}),

        RegressionTestCase({-1234.354f, "9G999G999G999G999G999",
                            "                -1,234"}),
        RegressionTestCase({-1234.354f,
                            "9G999G999G999G999G999D999G999G999G999G999",
                            "                -1,234.35"}),
        RegressionTestCase({-1234.354f,
                            "9999999999999999.999999999999999PR",
                            "            <1234.35>"}),
        RegressionTestCase({-1234.354f,
                            "9999999999999999.999999999999999S",
                            "            1234.35-"}),
        RegressionTestCase({-1234.354f,
                            "MI9999999999999999.999999999999999",
                            "-            1234.35"}),
        RegressionTestCase({-1234.354f,
                            "FMS9999999999999999.999999999999999",
                            "-1234.35"}),
        RegressionTestCase({-1234.354f,
                            "FM9999999999999999.999999999999999THPR",
                            "<1234.35>"}),
        RegressionTestCase({-1234.354f,
                            "SG9999999999999999.999999999999999th",
                            "-            1234.35"}),
        RegressionTestCase({-1234.354f,
                            "0999999999999999.999999999999999",
                            "-0000000000001234.35"}),
        RegressionTestCase({-1234.354f,
                            "S0999999999999999.999999999999999",
                            "-0000000000001234.35"}),
        RegressionTestCase({-1234.354f,
                            "FM0999999999999999.999999999999999",
                            "-0000000000001234.35"}),
        RegressionTestCase({-1234.354f,
                            "FM9999999999999999.099999999999999",
                            "-1234.35"}),
        RegressionTestCase({-1234.354f,
                            "FM9999999999990999.990999999999999",
                            "-1234.35"}),
        RegressionTestCase({-1234.354f,
                            "FM0999999999999999.999909999999999",
                             "-0000000000001234.35"}),
        RegressionTestCase({-1234.354f,
                            "FM9999999990999999.099999999999999",
                            "-0001234.35"}),
        RegressionTestCase({-1234.354f,
                            "L9999999999999999.099999999999999",
                            "$            -1234.35"}),
        RegressionTestCase({-1234.354f,
                            "FM9999999999999999.99999999999999",
                            "-1234.35"}),
        RegressionTestCase(
            {-1234.354f,
             "S 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 9 9 9 9 9 9 9 9 "
             "9 9 9",
             "                         -1 2 3 4 . 3 5 "}),
        RegressionTestCase(
            {-1234.354f,
             "FMS 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 9 9 9 9 9 9 9 "
             "9 9 9 9",
             "             -1 2 3 4 . 3 5               "}),
        RegressionTestCase(
            {-1234.354f,
             "99999 \"text\" 9999 \"9999\" 999 \"\\\"text between quote "
             "marks\\\"\" 9999",
             "      text      9999     \"text between quote marks\" -1234"}),
        RegressionTestCase({-1234.354f, "999999SG9999999999",
                            "      -      1234"}),
        RegressionTestCase({-1234.354f,
                            "FM9999999999999999.999999999999999",
                            "-1234.35"}),
        RegressionTestCase({-1234.354f, "9.999EEEE", "-1.234e+03"}),

        RegressionTestCase({9461.411f, "9G999G999G999G999G999",
                            "                 9,461"}),
        RegressionTestCase({9461.411f,
                            "9G999G999G999G999G999D999G999G999G999G999",
                            "                 9,461.41"}),
        RegressionTestCase({9461.411f, "9999999999999999.999999999999999PR",
                            "             9461.41 "}),
        RegressionTestCase({9461.411f, "9999999999999999.999999999999999S",
                            "            9461.41+"}),
        RegressionTestCase({9461.411f, "MI9999999999999999.999999999999999",
                            "             9461.41"}),
        RegressionTestCase({9461.411f,
                            "FMS9999999999999999.999999999999999",
                            "+9461.41"}),
        RegressionTestCase({9461.411f,
                            "FM9999999999999999.999999999999999THPR",
                            "9461.41"}),
        RegressionTestCase({9461.411f,
                            "SG9999999999999999.999999999999999th",
                            "+            9461.41"}),
        RegressionTestCase({9461.411f, "0999999999999999.999999999999999",
                            " 0000000000009461.41"}),
        RegressionTestCase({9461.411f, "S0999999999999999.999999999999999",
                            "+0000000000009461.41"}),
        RegressionTestCase({9461.411f, "FM0999999999999999.999999999999999",
                            "0000000000009461.41"}),
        RegressionTestCase({9461.411f, "FM9999999999999999.099999999999999",
                            "9461.41"}),
        RegressionTestCase({9461.411f, "FM9999999999990999.990999999999999",
                            "9461.41"}),
        RegressionTestCase({9461.411f, "FM0999999999999999.999909999999999",
                            "0000000000009461.41"}),
        RegressionTestCase({9461.411f, "FM9999999990999999.099999999999999",
                            "0009461.41"}),
        RegressionTestCase({9461.411f, "L9999999999999999.099999999999999",
                            "$             9461.41"}),
        RegressionTestCase({9461.411f, "FM9999999999999999.99999999999999",
                            "9461.41"}),
        RegressionTestCase(
            {9461.411f,
             "S 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 9 9 9 9 9 9 9 9 "
             "9 9 9",
             "                         +9 4 6 1 . 4 1 "}),
        RegressionTestCase({9461.41f,
                            "FMS 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 "
                            "9 9 9 9 9 9 9 9 9 9 9",
                            "             +9 4 6 1 . 4 1               "}),
        RegressionTestCase(
            {9461.41f,
             "99999 \"text\" 9999 \"9999\" 999 \"\\\"text between quote "
             "marks\\\"\" 9999",
             "      text      9999     \"text between quote marks\"  9461"}),
        RegressionTestCase({9461.41f, "999999SG9999999999",
                            "      +      9461"}),
        RegressionTestCase({9461.41f, "FM9999999999999999.999999999999999",
                            "9461.41"}),
        RegressionTestCase({9461.41f, "9.999EEEE", " 9.461e+03"})));
}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
