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

class Float8ToCharTest : public PgEvaluatorTest {
 protected:
  void TearDown() override {
    PgEvaluatorTest::TearDown();
    CleanupPostgresNumberCache();
  }
};

TEST_F(Float8ToCharTest, FormatNumberWithDigit9) {
  EXPECT_THAT(Float8ToChar(123.45, "999"), IsOkAndHolds(StrEq(" 123")));
  EXPECT_THAT(Float8ToChar(-123.45, "999"), IsOkAndHolds(StrEq("-123")));
  EXPECT_THAT(Float8ToChar(123.45, "9999"), IsOkAndHolds(StrEq("  123")));
}

TEST_F(Float8ToCharTest, FormatNumberRoundsNumber) {
  // Rounds down
  EXPECT_THAT(Float8ToChar(0.1, "9"), IsOkAndHolds(StrEq(" 0")));
  EXPECT_THAT(Float8ToChar(0.4, "9"), IsOkAndHolds(StrEq(" 0")));
  EXPECT_THAT(Float8ToChar(1.2, "9"), IsOkAndHolds(StrEq(" 1")));
  EXPECT_THAT(Float8ToChar(1.4, "9"), IsOkAndHolds(StrEq(" 1")));
  EXPECT_THAT(Float8ToChar(1.49, "9"), IsOkAndHolds(StrEq(" 1")));
  EXPECT_THAT(Float8ToChar(1.44, "9.9"), IsOkAndHolds(StrEq(" 1.4")));
  EXPECT_THAT(Float8ToChar(1.45, "9.9"), IsOkAndHolds(StrEq(" 1.4")));
  // Edge case, 0.5 is rounded down
  EXPECT_THAT(Float8ToChar(0.5, "9"), IsOkAndHolds(StrEq(" 0")));

  // Rounds up
  EXPECT_THAT(Float8ToChar(1.5, "9"), IsOkAndHolds(StrEq(" 2")));
  EXPECT_THAT(Float8ToChar(1.9, "9"), IsOkAndHolds(StrEq(" 2")));
  EXPECT_THAT(Float8ToChar(1.451, "9.9"), IsOkAndHolds(StrEq(" 1.5")));
  EXPECT_THAT(Float8ToChar(1.46, "9.9"), IsOkAndHolds(StrEq(" 1.5")));
  EXPECT_THAT(Float8ToChar(0.51, "9"), IsOkAndHolds(StrEq(" 1")));

  // Negative numbers
  EXPECT_THAT(Float8ToChar(-0.1, "9"), IsOkAndHolds(StrEq("-0")));
  EXPECT_THAT(Float8ToChar(-0.5, "9"), IsOkAndHolds(StrEq("-0")));
  EXPECT_THAT(Float8ToChar(-0.51, "9"), IsOkAndHolds(StrEq("-1")));
  EXPECT_THAT(Float8ToChar(-0.9, "9"), IsOkAndHolds(StrEq("-1")));
  EXPECT_THAT(Float8ToChar(-1.49, "9"), IsOkAndHolds(StrEq("-1")));
  EXPECT_THAT(Float8ToChar(-1.5, "9"), IsOkAndHolds(StrEq("-2")));
  EXPECT_THAT(Float8ToChar(-1.9, "9"), IsOkAndHolds(StrEq("-2")));
}

TEST_F(Float8ToCharTest, FormatNumberWithDigit0) {
  EXPECT_THAT(Float8ToChar(123.45, "000"), IsOkAndHolds(StrEq(" 123")));
  EXPECT_THAT(Float8ToChar(123.45, "0000"), IsOkAndHolds(StrEq(" 0123")));
}

TEST_F(Float8ToCharTest, FormatNumberWithDecimalPoint) {
  EXPECT_THAT(Float8ToChar(123.45, "999.99"), IsOkAndHolds(StrEq(" 123.45")));
  EXPECT_THAT(Float8ToChar(123.45, "999.00"), IsOkAndHolds(StrEq(" 123.45")));
  // Rounds up
  EXPECT_THAT(Float8ToChar(123.45, "999.9"), IsOkAndHolds(StrEq(" 123.5")));
  EXPECT_THAT(Float8ToChar(123.45, "999.0"), IsOkAndHolds(StrEq(" 123.5")));
  EXPECT_THAT(Float8ToChar(123.45, "9999.999"),
              IsOkAndHolds(StrEq("  123.450")));
  EXPECT_THAT(Float8ToChar(123.45, "0000.000"),
              IsOkAndHolds(StrEq(" 0123.450")));
}

TEST_F(Float8ToCharTest, FormatNumberWithThousandsSeparator) {
  EXPECT_THAT(Float8ToChar(123456.789, "999,999.999"),
              IsOkAndHolds(StrEq(" 123,456.789")));
}

TEST_F(Float8ToCharTest, FormatNumberWithAngleBrackets) {
  EXPECT_THAT(Float8ToChar(123.45, "999.99PR"),
              IsOkAndHolds(StrEq(" 123.45 ")));
  EXPECT_THAT(Float8ToChar(-123.45, "999.99PR"),
              IsOkAndHolds(StrEq("<123.45>")));
}

TEST_F(Float8ToCharTest, FormatNumberWithAnchoredSign) {
  EXPECT_THAT(Float8ToChar(123.45, "999.99S"), IsOkAndHolds(StrEq("123.45+")));
  EXPECT_THAT(Float8ToChar(123.45, "S999.99"), IsOkAndHolds(StrEq("+123.45")));
  EXPECT_THAT(Float8ToChar(123.45, "9S99.99"), IsOkAndHolds(StrEq("+123.45")));
  EXPECT_THAT(Float8ToChar(123.45, "99S9.99"), IsOkAndHolds(StrEq("+123.45")));
}

// Locale is hardcoded to en_US in Spangres
TEST_F(Float8ToCharTest, FormatNumberWithLocalePatterns) {
  // Currency symbol
  EXPECT_THAT(Float8ToChar(123.45, "L999.99"), IsOkAndHolds(StrEq("$ 123.45")));
  // Decimal point
  EXPECT_THAT(Float8ToChar(123.45, "999D99"), IsOkAndHolds(StrEq(" 123.45")));
  // Group separator
  EXPECT_THAT(Float8ToChar(123456.789, "999G999.999"),
              IsOkAndHolds(StrEq(" 123,456.789")));
  // Minus sign
  EXPECT_THAT(Float8ToChar(-12.34, "9MI9D99"), IsOkAndHolds(StrEq("1-2.34")));
  // Plus sign
  EXPECT_THAT(Float8ToChar(12.34, "9PL9.99"), IsOkAndHolds(StrEq(" 1+2.34")));
  // Plus/minus sign
  EXPECT_THAT(Float8ToChar(-12.34, "9SG9.99"), IsOkAndHolds(StrEq("1-2.34")));
  EXPECT_THAT(Float8ToChar(12.34, "9SG9.99"), IsOkAndHolds(StrEq("1+2.34")));
  // Upper case ordinal number suffix (rounds down)
  EXPECT_THAT(Float8ToChar(0.49, "9TH"), IsOkAndHolds(StrEq(" 0TH")));
  EXPECT_THAT(Float8ToChar(1.49, "9TH"), IsOkAndHolds(StrEq(" 1ST")));
  EXPECT_THAT(Float8ToChar(2.49, "9TH"), IsOkAndHolds(StrEq(" 2ND")));
  EXPECT_THAT(Float8ToChar(3.49, "9TH"), IsOkAndHolds(StrEq(" 3RD")));
  EXPECT_THAT(Float8ToChar(4.49, "9TH"), IsOkAndHolds(StrEq(" 4TH")));
  // Upper case ordinal number suffix (rounds up)
  EXPECT_THAT(Float8ToChar(1.5, "9TH"), IsOkAndHolds(StrEq(" 2ND")));

  // Lower case ordinal number suffix (rounds down)
  EXPECT_THAT(Float8ToChar(0.49, "9th"), IsOkAndHolds(StrEq(" 0th")));
  EXPECT_THAT(Float8ToChar(1.49, "9th"), IsOkAndHolds(StrEq(" 1st")));
  EXPECT_THAT(Float8ToChar(2.49, "9th"), IsOkAndHolds(StrEq(" 2nd")));
  EXPECT_THAT(Float8ToChar(3.49, "9th"), IsOkAndHolds(StrEq(" 3rd")));
  EXPECT_THAT(Float8ToChar(4.49, "9th"), IsOkAndHolds(StrEq(" 4th")));
  // Lower case ordinal number suffix (rounds up)
  EXPECT_THAT(Float8ToChar(1.5, "9th"), IsOkAndHolds(StrEq(" 2nd")));
}

TEST_F(Float8ToCharTest, FormatNumberAsRomanNumerals) {
  // Rounds down
  EXPECT_THAT(Float8ToChar(1.4, "9RN"), IsOkAndHolds(StrEq("              I")));
  EXPECT_THAT(Float8ToChar(2.4, "9RN"), IsOkAndHolds(StrEq("             II")));
  EXPECT_THAT(Float8ToChar(3.4, "9RN"), IsOkAndHolds(StrEq("            III")));
  EXPECT_THAT(Float8ToChar(4.4, "9RN"), IsOkAndHolds(StrEq("             IV")));
  EXPECT_THAT(Float8ToChar(5.4, "9RN"), IsOkAndHolds(StrEq("              V")));
  EXPECT_THAT(Float8ToChar(10.4, "9RN"),
              IsOkAndHolds(StrEq("              X")));
  EXPECT_THAT(Float8ToChar(50.4, "9RN"),
              IsOkAndHolds(StrEq("              L")));
  EXPECT_THAT(Float8ToChar(100.4, "9RN"),
              IsOkAndHolds(StrEq("              C")));
  EXPECT_THAT(Float8ToChar(500.4, "9RN"),
              IsOkAndHolds(StrEq("              D")));
  EXPECT_THAT(Float8ToChar(1000.4, "9RN"),
              IsOkAndHolds(StrEq("              M")));
  EXPECT_THAT(Float8ToChar(3999.4, "9RN"),
              IsOkAndHolds(StrEq("      MMMCMXCIX")));

  // Rounds up
  EXPECT_THAT(Float8ToChar(5.5, "9RN"), IsOkAndHolds(StrEq("             VI")));
}

TEST_F(Float8ToCharTest, ShiftNumberByPowersOfTen) {
  EXPECT_THAT(Float8ToChar(9.9, "9V9"), IsOkAndHolds(StrEq(" 99")));
  EXPECT_THAT(Float8ToChar(9.9, "9V99999"), IsOkAndHolds(StrEq(" 990000")));
}

TEST_F(Float8ToCharTest, FormatNumberWithScientificNotation) {
  EXPECT_THAT(Float8ToChar(123456.789, "9.99EEEE"),
              IsOkAndHolds(StrEq(" 1.23e+05")));
}

TEST_F(Float8ToCharTest, FormatNumberWithFillMode) {
  EXPECT_THAT(Float8ToChar(1.2, "9RNFM"), IsOkAndHolds(StrEq("I")));
  EXPECT_THAT(Float8ToChar(123.45, "9999.999FM"),
              IsOkAndHolds(StrEq("123.45")));
}

TEST_F(Float8ToCharTest, FormatNumberWithStringLiterals) {
  EXPECT_THAT(Float8ToChar(485.67, "\"Good number:\"999.99"),
              IsOkAndHolds(StrEq("Good number: 485.67")));
}

TEST_F(Float8ToCharTest, FormatNumberEdgeCases) {
  // Maintains digits after 0
  EXPECT_THAT(Float8ToChar(12.34, "0999.9"), IsOkAndHolds(StrEq(" 0012.3")));

  // Angle brackets format along with decimal / thousands separator
  EXPECT_THAT(Float8ToChar(-123.5, "0000.00PR"),
              IsOkAndHolds(StrEq("<0123.50>")));
  EXPECT_THAT(Float8ToChar(-123.5, "0000PR,"), IsOkAndHolds(StrEq("<0124>,")));
  EXPECT_THAT(Float8ToChar(-123.5, "0000PR."), IsOkAndHolds(StrEq("<0124")));
  EXPECT_THAT(Float8ToChar(-123.5, "0000PR,."), IsOkAndHolds(StrEq("<0124,")));

  // Minus sign is replaced by space on positive numbers
  EXPECT_THAT(Float8ToChar(12.49, "0MI0.0"), IsOkAndHolds(StrEq("1 2.5")));
  // Plus sign is replaced by space on negative numbers
  EXPECT_THAT(Float8ToChar(-12.46, "0PL0.0"), IsOkAndHolds(StrEq("-1 2.5")));

  // Ordinal number suffixes are not applied to values < 0
  EXPECT_THAT(Float8ToChar(-123.1, "000TH"), IsOkAndHolds(StrEq("-123")));
  EXPECT_THAT(Float8ToChar(-123.1, "000th"), IsOkAndHolds(StrEq("-123")));
}

TEST_F(Float8ToCharTest,
       ReturnsPlaceholderWhenNumberCanNotBeFormattedWithValidPattern) {
  // Less significant digits in format then in given number
  EXPECT_THAT(Float8ToChar(123.45, "00.00"), IsOkAndHolds(StrEq(" ##.##")));
  EXPECT_THAT(Float8ToChar(123.45, "9PL9"), IsOkAndHolds(StrEq(" #+#")));
  EXPECT_THAT(Float8ToChar(123.45, "9MI9"), IsOkAndHolds(StrEq("# #")));
  // Only 1-3999 roman numbers are supported (0.5 is rounded down / 3999.5 is
  // rounded up)
  EXPECT_THAT(Float8ToChar(0.5, "9RN"), IsOkAndHolds(StrEq("###############")));
  EXPECT_THAT(Float8ToChar(3999.5, "9RN"),
              IsOkAndHolds(StrEq("###############")));
}

TEST_F(Float8ToCharTest, ReturnsEmptyStringWhenFormatIsEmpty) {
  EXPECT_THAT(Float8ToChar(123.45, ""), IsOkAndHolds(StrEq("")));
}

TEST_F(Float8ToCharTest, MsanViolationTest) {
  // Because the format goes beyond the allowed number of double digits the
  // output template (with '#'s) is shrunk. This originally caused a pointer
  // to overflow to an invalid position. We can run this test with msan to
  // guarantee such bug is fixed. This is also tested in the corresponding fuzz
  // tests.
  EXPECT_THAT(Float8ToChar(12345678901, "FM9999999999D999990"),
              IsOkAndHolds(StrEq("##########.####")));
  EXPECT_THAT(Float8ToChar(12345678901, "FM9999999999D9999900000000000000000"),
              IsOkAndHolds(StrEq("##########.####")));
}

TEST_F(Float8ToCharTest, ReturnsErrorWhenFormatIsInvalid) {
  EXPECT_THAT(Float8ToChar(-9.9, "9PR.9"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("\"9\" must be ahead of \"PR\"")));
  EXPECT_THAT(
      Float8ToChar(9.9, "9.9V9"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("cannot use \"V\" and decimal point together")));
  EXPECT_THAT(Float8ToChar(9.9, "9..9"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("multiple decimal points")));
  EXPECT_THAT(Float8ToChar(9.9, "EEEE9.9"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("\"EEEE\" must be the last pattern used")));
  EXPECT_THAT(Float8ToChar(9.9, "9EEEE EEEE"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("cannot use \"EEEE\" twice")));
  EXPECT_THAT(
      Float8ToChar(9.9, "S9.9EEEE"),
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
  EXPECT_THAT(Float8ToChar(test_case.input_value, test_case.input_format),
              IsOkAndHolds(StrEq(test_case.expected_output)));
}
INSTANTIATE_TEST_SUITE_P(
    RegressionTestSuite, RegressionTest,
    testing::Values(
        RegressionTestCase({0, "9G999G999G999G999G999",
                            "                     0"}),
        RegressionTestCase({0, "9G999G999G999G999G999D999G999G999G999G999",
                            "                      .000,000,000,000,00"}),
        RegressionTestCase({0, "9999999999999999.999999999999999PR",
                            "                 .00000000000000 "}),
        RegressionTestCase({0, "9999999999999999.999999999999999S",
                            "                .00000000000000+"}),
        RegressionTestCase({0, "MI9999999999999999.999999999999999",
                            "                 .00000000000000"}),
        RegressionTestCase({0, "FMS9999999999999999.999999999999999", "+0."}),
        RegressionTestCase({0, "FM9999999999999999.999999999999999THPR", "0."}),
        RegressionTestCase({0, "SG9999999999999999.999999999999999th",
                            "+                .00000000000000"}),
        RegressionTestCase({0, "0999999999999999.999999999999999",
                            " 0000000000000000.00000000000000"}),
        RegressionTestCase({0, "S0999999999999999.999999999999999",
                            "+0000000000000000.00000000000000"}),
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
                            "$                 .00000000000000"}),
        RegressionTestCase({0, "FM9999999999999999.99999999999999", "0."}),
        RegressionTestCase({0,
                            "S 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 9 "
                            "9 9 9 9 9 9 9 9 9 9",
                            "                                 +. 0 0 0 0 0 0 0 "
                            "0 0 0 0 0 0 0 "}),
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

        RegressionTestCase({-24926804.045047420, "9G999G999G999G999G999",
                            "           -24,926,804"}),
        RegressionTestCase({-24926804.045047420,
                            "9G999G999G999G999G999D999G999G999G999G999",
                            "           -24,926,804.045,047,4"}),
        RegressionTestCase({-24926804.045047420,
                            "9999999999999999.999999999999999PR",
                            "        <24926804.0450474>"}),
        RegressionTestCase({-24926804.045047420,
                            "9999999999999999.999999999999999S",
                            "        24926804.0450474-"}),
        RegressionTestCase({-24926804.045047420,
                            "MI9999999999999999.999999999999999",
                            "-        24926804.0450474"}),
        RegressionTestCase({-24926804.045047420,
                            "FMS9999999999999999.999999999999999",
                            "-24926804.0450474"}),
        RegressionTestCase({-24926804.045047420,
                            "FM9999999999999999.999999999999999THPR",
                            "<24926804.0450474>"}),
        RegressionTestCase({-24926804.045047420,
                            "SG9999999999999999.999999999999999th",
                            "-        24926804.0450474"}),
        RegressionTestCase({-24926804.045047420,
                            "0999999999999999.999999999999999",
                            "-0000000024926804.0450474"}),
        RegressionTestCase({-24926804.045047420,
                            "S0999999999999999.999999999999999",
                            "-0000000024926804.0450474"}),
        RegressionTestCase({-24926804.045047420,
                            "FM0999999999999999.999999999999999",
                            "-0000000024926804.0450474"}),
        RegressionTestCase({-24926804.045047420,
                            "FM9999999999999999.099999999999999",
                            "-24926804.0450474"}),
        RegressionTestCase({-24926804.045047420,
                            "FM9999999999990999.990999999999999",
                            "-24926804.0450474"}),
        RegressionTestCase({-24926804.045047420,
                            "FM0999999999999999.999909999999999",
                            "-0000000024926804.0450474"}),
        RegressionTestCase({-24926804.045047420,
                            "FM9999999990999999.099999999999999",
                            "-24926804.0450474"}),
        RegressionTestCase({-24926804.045047420,
                            "L9999999999999999.099999999999999",
                            "$        -24926804.0450474"}),
        RegressionTestCase({-24926804.045047420,
                            "FM9999999999999999.99999999999999",
                            "-24926804.0450474"}),
        RegressionTestCase(
            {-24926804.045047420,
             "S 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 9 9 9 9 9 9 9 9 "
             "9 9 9",
             "                 -2 4 9 2 6 8 0 4 . 0 4 5 0 4 7 4 "}),
        RegressionTestCase(
            {-24926804.045047420,
             "FMS 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 9 9 9 9 9 9 9 "
             "9 9 9 9",
             "         -2 4 9 2 6 8 0 4 . 0 4 5 0 4 7 4          "}),
        RegressionTestCase(
            {-24926804.045047420,
             "99999 \"text\" 9999 \"9999\" 999 \"\\\"text between quote "
             "marks\\\"\" 9999",
             "      text    -2 9999 492 \"text between quote marks\" 6804"}),
        RegressionTestCase({-24926804.045047420, "999999SG9999999999",
                            "      -  24926804"}),
        RegressionTestCase({-24926804.045047420,
                            "FM9999999999999999.999999999999999",
                            "-24926804.0450474"}),
        RegressionTestCase({-24926804.045047420, "9.999EEEE", "-2.493e+07"}),

        RegressionTestCase({7799461.4119, "9G999G999G999G999G999",
                            "             7,799,461"}),
        RegressionTestCase({7799461.4119,
                            "9G999G999G999G999G999D999G999G999G999G999",
                            "             7,799,461.411,900,00"}),
        RegressionTestCase({7799461.4119, "9999999999999999.999999999999999PR",
                            "          7799461.41190000 "}),
        RegressionTestCase({7799461.4119, "9999999999999999.999999999999999S",
                            "         7799461.41190000+"}),
        RegressionTestCase({7799461.4119, "MI9999999999999999.999999999999999",
                            "          7799461.41190000"}),
        RegressionTestCase({7799461.4119, "FMS9999999999999999.999999999999999",
                            "+7799461.4119"}),
        RegressionTestCase({7799461.4119,
                            "FM9999999999999999.999999999999999THPR",
                            "7799461.4119"}),
        RegressionTestCase({7799461.4119,
                            "SG9999999999999999.999999999999999th",
                            "+         7799461.41190000"}),
        RegressionTestCase({7799461.4119, "0999999999999999.999999999999999",
                            " 0000000007799461.41190000"}),
        RegressionTestCase({7799461.4119, "S0999999999999999.999999999999999",
                            "+0000000007799461.41190000"}),
        RegressionTestCase({7799461.4119, "FM0999999999999999.999999999999999",
                            "0000000007799461.4119"}),
        RegressionTestCase({7799461.4119, "FM9999999999999999.099999999999999",
                            "7799461.4119"}),
        RegressionTestCase({7799461.4119, "FM9999999999990999.990999999999999",
                            "7799461.4119"}),
        RegressionTestCase({7799461.4119, "FM0999999999999999.999909999999999",
                            "0000000007799461.41190"}),
        RegressionTestCase({7799461.4119, "FM9999999990999999.099999999999999",
                            "7799461.4119"}),
        RegressionTestCase({7799461.4119, "L9999999999999999.099999999999999",
                            "$          7799461.41190000"}),
        RegressionTestCase({7799461.4119, "FM9999999999999999.99999999999999",
                            "7799461.4119"}),
        RegressionTestCase(
            {7799461.4119,
             "S 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 9 9 9 9 9 9 9 9 "
             "9 9 9",
             "                   +7 7 9 9 4 6 1 . 4 1 1 9 0 0 0 0 "}),
        RegressionTestCase({7799461.4119,
                            "FMS 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 "
                            "9 9 9 9 9 9 9 9 9 9 9",
                            "          +7 7 9 9 4 6 1 . 4 1 1 9             "}),
        RegressionTestCase(
            {7799461.4119,
             "99999 \"text\" 9999 \"9999\" 999 \"\\\"text between quote "
             "marks\\\"\" 9999",
             "      text      9999  779 \"text between quote marks\" 9461"}),
        RegressionTestCase({7799461.4119, "999999SG9999999999",
                            "      +   7799461"}),
        RegressionTestCase({7799461.4119, "FM9999999999999999.999999999999999",
                            "7799461.4119"}),
        RegressionTestCase({7799461.4119, "9.999EEEE", " 7.799e+06"})));
}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
