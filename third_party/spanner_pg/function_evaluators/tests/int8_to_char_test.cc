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

#include <limits>

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

class Int8ToCharTest : public PgEvaluatorTest {
 protected:
  void TearDown() override {
    PgEvaluatorTest::TearDown();
    CleanupPostgresNumberCache();
  }
};

TEST_F(Int8ToCharTest, FormatNumberWithDigit9) {
  // The initial space in the beginning of the result is where the sign would
  // go. Since this is a positive number, it is omitted.
  EXPECT_THAT(Int8ToChar(123, "999"), IsOkAndHolds(StrEq(" 123")));
  EXPECT_THAT(Int8ToChar(-123, "999"), IsOkAndHolds(StrEq("-123")));
  // Digit position is dropped if insignificant
  EXPECT_THAT(Int8ToChar(123, "9999"), IsOkAndHolds(StrEq("  123")));
}

TEST_F(Int8ToCharTest, FormatNumberWithDigit0) {
  EXPECT_THAT(Int8ToChar(123, "000"), IsOkAndHolds(StrEq(" 123")));
  // Digit position is kept even if insignificant
  EXPECT_THAT(Int8ToChar(123, "0000"), IsOkAndHolds(StrEq(" 0123")));
}

TEST_F(Int8ToCharTest, FormatNumberWithDecimalPoint) {
  EXPECT_THAT(Int8ToChar(123, "999.99"), IsOkAndHolds(StrEq(" 123.00")));
  EXPECT_THAT(Int8ToChar(123, "999.00"), IsOkAndHolds(StrEq(" 123.00")));
}

TEST_F(Int8ToCharTest, FormatNumberWithThousandsSeparator) {
  EXPECT_THAT(Int8ToChar(123456, "999,999"), IsOkAndHolds(StrEq(" 123,456")));
}

TEST_F(Int8ToCharTest, FormatNumberWithAngleBrackets) {
  EXPECT_THAT(Int8ToChar(123, "999PR"), IsOkAndHolds(StrEq(" 123 ")));
  EXPECT_THAT(Int8ToChar(-123, "999PR"), IsOkAndHolds(StrEq("<123>")));
}

TEST_F(Int8ToCharTest, FormatNumberWithAnchoredSign) {
  EXPECT_THAT(Int8ToChar(123, "999S"), IsOkAndHolds(StrEq("123+")));
  EXPECT_THAT(Int8ToChar(123, "S999"), IsOkAndHolds(StrEq("+123")));
  EXPECT_THAT(Int8ToChar(123, "9S99"), IsOkAndHolds(StrEq("+123")));
  EXPECT_THAT(Int8ToChar(123, "99S9"), IsOkAndHolds(StrEq("+123")));
}

// Locale is hardcoded to en_US in Spangres
TEST_F(Int8ToCharTest, FormatNumberWithLocalePatterns) {
  // Currency symbol
  EXPECT_THAT(Int8ToChar(123, "L999"), IsOkAndHolds(StrEq("$ 123")));
  // Decimal point
  EXPECT_THAT(Int8ToChar(123, "999D99"), IsOkAndHolds(StrEq(" 123.00")));
  // Group separator
  EXPECT_THAT(Int8ToChar(123456, "999G999"), IsOkAndHolds(StrEq(" 123,456")));
  // Minus sign
  EXPECT_THAT(Int8ToChar(-12, "9MI9"), IsOkAndHolds(StrEq("1-2")));
  // Plus sign
  EXPECT_THAT(Int8ToChar(12, "9PL9"), IsOkAndHolds(StrEq(" 1+2")));
  // Plus/minus sign
  EXPECT_THAT(Int8ToChar(-12, "9SG9"), IsOkAndHolds(StrEq("1-2")));
  EXPECT_THAT(Int8ToChar(12, "9SG9"), IsOkAndHolds(StrEq("1+2")));
  // Upper case ordinal number suffix
  EXPECT_THAT(Int8ToChar(0, "9TH"), IsOkAndHolds(StrEq(" 0TH")));
  EXPECT_THAT(Int8ToChar(1, "9TH"), IsOkAndHolds(StrEq(" 1ST")));
  EXPECT_THAT(Int8ToChar(2, "9TH"), IsOkAndHolds(StrEq(" 2ND")));
  EXPECT_THAT(Int8ToChar(3, "9TH"), IsOkAndHolds(StrEq(" 3RD")));
  EXPECT_THAT(Int8ToChar(4, "9TH"), IsOkAndHolds(StrEq(" 4TH")));
  // Lower case ordinal number suffix
  EXPECT_THAT(Int8ToChar(0, "9th"), IsOkAndHolds(StrEq(" 0th")));
  EXPECT_THAT(Int8ToChar(1, "9th"), IsOkAndHolds(StrEq(" 1st")));
  EXPECT_THAT(Int8ToChar(2, "9th"), IsOkAndHolds(StrEq(" 2nd")));
  EXPECT_THAT(Int8ToChar(3, "9th"), IsOkAndHolds(StrEq(" 3rd")));
  EXPECT_THAT(Int8ToChar(4, "9th"), IsOkAndHolds(StrEq(" 4th")));
}

TEST_F(Int8ToCharTest, FormatNumberAsRomanNumerals) {
  EXPECT_THAT(Int8ToChar(1, "9RN"), IsOkAndHolds(StrEq("              I")));
  EXPECT_THAT(Int8ToChar(2, "9RN"), IsOkAndHolds(StrEq("             II")));
  EXPECT_THAT(Int8ToChar(3, "9RN"), IsOkAndHolds(StrEq("            III")));
  EXPECT_THAT(Int8ToChar(4, "9RN"), IsOkAndHolds(StrEq("             IV")));
  EXPECT_THAT(Int8ToChar(5, "9RN"), IsOkAndHolds(StrEq("              V")));
  EXPECT_THAT(Int8ToChar(10, "9RN"), IsOkAndHolds(StrEq("              X")));
  EXPECT_THAT(Int8ToChar(50, "9RN"), IsOkAndHolds(StrEq("              L")));
  EXPECT_THAT(Int8ToChar(100, "9RN"), IsOkAndHolds(StrEq("              C")));
  EXPECT_THAT(Int8ToChar(500, "9RN"), IsOkAndHolds(StrEq("              D")));
  EXPECT_THAT(Int8ToChar(1000, "9RN"), IsOkAndHolds(StrEq("              M")));
  EXPECT_THAT(Int8ToChar(3999, "9RN"), IsOkAndHolds(StrEq("      MMMCMXCIX")));
}

TEST_F(Int8ToCharTest, ShiftNumberByPowersOfTen) {
  EXPECT_THAT(Int8ToChar(9, "9V"), IsOkAndHolds(StrEq(" 9")));
  EXPECT_THAT(Int8ToChar(9, "9V9"), IsOkAndHolds(StrEq(" 90")));
  EXPECT_THAT(Int8ToChar(9, "9V99999"), IsOkAndHolds(StrEq(" 900000")));
}

TEST_F(Int8ToCharTest, FormatNumberWithScientificNotation) {
  EXPECT_THAT(Int8ToChar(123456, "9.99EEEE"), IsOkAndHolds(StrEq(" 1.23e+05")));
}

TEST_F(Int8ToCharTest, FormatNumberWithFillMode) {
  EXPECT_THAT(Int8ToChar(1, "9RNFM"), IsOkAndHolds(StrEq("I")));
  EXPECT_THAT(Int8ToChar(123, "9999.99FM"), IsOkAndHolds(StrEq("123.")));
}

TEST_F(Int8ToCharTest, FormatNumberWithStringLiterals) {
  EXPECT_THAT(Int8ToChar(485, "\"Good number:\"999"),
              IsOkAndHolds(StrEq("Good number: 485")));
}

TEST_F(Int8ToCharTest, FormatNumberEdgeCases) {
  // Maintains digits after 0
  EXPECT_THAT(Int8ToChar(12, "0999.9"), IsOkAndHolds(StrEq(" 0012.0")));

  // Angle brackets format along with decimal / thousands separator
  EXPECT_THAT(Int8ToChar(-123, "0000PR"), IsOkAndHolds(StrEq("<0123>")));
  EXPECT_THAT(Int8ToChar(-123, "0000PR,"), IsOkAndHolds(StrEq("<0123>,")));
  EXPECT_THAT(Int8ToChar(-123, "0000PR."), IsOkAndHolds(StrEq("<0123")));
  EXPECT_THAT(Int8ToChar(-123, "0000PR,."), IsOkAndHolds(StrEq("<0123,")));

  // Minus sign is replaced by space on positive numbers
  EXPECT_THAT(Int8ToChar(12, "0MI0"), IsOkAndHolds(StrEq("1 2")));
  // Plus sign is replaced by space on negative numbers
  EXPECT_THAT(Int8ToChar(-12, "0PL0"), IsOkAndHolds(StrEq("-1 2")));

  // Ordinal number suffixes are not applied to values < 0
  EXPECT_THAT(Int8ToChar(-123, "000TH"), IsOkAndHolds(StrEq("-123")));
  EXPECT_THAT(Int8ToChar(-123, "000th"), IsOkAndHolds(StrEq("-123")));
}

TEST_F(Int8ToCharTest,
       ReturnsPlaceholderWhenNumberCanNotBeFormattedWithValidPattern) {
  // Less significant digits in format then in given number
  EXPECT_THAT(Int8ToChar(123, "00.00"), IsOkAndHolds(StrEq(" ##.##")));
  EXPECT_THAT(Int8ToChar(123, "9PL9"), IsOkAndHolds(StrEq(" #+#")));
  EXPECT_THAT(Int8ToChar(123, "9MI9"), IsOkAndHolds(StrEq("# #")));
  // Only 1-3999 roman numbers are supported
  EXPECT_THAT(Int8ToChar(0, "9RN"), IsOkAndHolds(StrEq("###############")));
  EXPECT_THAT(Int8ToChar(4000, "9RN"), IsOkAndHolds(StrEq("###############")));
}

TEST_F(Int8ToCharTest, ReturnsEmptyStringWhenFormatIsEmpty) {
  EXPECT_THAT(Int8ToChar(123, ""), IsOkAndHolds(StrEq("")));
}

TEST_F(Int8ToCharTest, ReturnsErrorWhenFormatIsInvalid) {
  EXPECT_THAT(
      Int8ToChar(9, "9.9V9"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("cannot use \"V\" and decimal point together")));
  EXPECT_THAT(Int8ToChar(-1, "S9S"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("cannot use \"S\" twice")));
  EXPECT_THAT(Int8ToChar(std::numeric_limits<int64_t>::max(), "RN"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("integer out of range")));
  EXPECT_THAT(Int8ToChar(std::numeric_limits<int64_t>::max(), "9V9"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("bigint out of range")));
  EXPECT_THAT(Int8ToChar(-1, "S9MI"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("cannot use \"S\" and \"MI\" together")));
  EXPECT_THAT(Int8ToChar(-1, "S9PL"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("cannot use \"S\" and \"PL\" together")));
  EXPECT_THAT(
      Int8ToChar(-1, "MIPL9S"),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr(
              "cannot use \"S\" and \"PL\"/\"MI\"/\"SG\"/\"PR\" together")));
}

struct RegressionTestCase {
  int64_t input_value;
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
  EXPECT_THAT(Int8ToChar(test_case.input_value, test_case.input_format),
              IsOkAndHolds(StrEq(test_case.expected_output)));
}
INSTANTIATE_TEST_SUITE_P(
    RegressionTestSuite, RegressionTest,
    testing::Values(
        RegressionTestCase({123, "9G999G999G999G999G999",
                            "                   123"}),
        RegressionTestCase({-4567890123456789, "9G999G999G999G999G999",
                            "-4,567,890,123,456,789"}),
        RegressionTestCase({123, "9,999,999,999,999,999",
                            "                   123"}),
        RegressionTestCase({-4567890123456789, "9,999,999,999,999,999",
                            "-4,567,890,123,456,789"}),
        RegressionTestCase({123, "9G999G999G999G999G999D999G999",
                            "                   123.000,000"}),
        RegressionTestCase({-4567890123456789, "9G999G999G999G999G999D999G999",
                            "-4,567,890,123,456,789.000,000"}),
        RegressionTestCase({123, "9,999,999,999,999,999.999,999",
                            "                   123.000,000"}),
        RegressionTestCase({-4567890123456789, "9,999,999,999,999,999.999,999",
                            "-4,567,890,123,456,789.000,000"}),
        RegressionTestCase({-123, "9999999999999999PR", "             <123>"}),
        RegressionTestCase({-4567890123456789, "9999999999999999PR",
                            "<4567890123456789>"}),
        RegressionTestCase({-123, "9999999999999999.999PR",
                            "             <123.000>"}),
        RegressionTestCase({-4567890123456789, "9999999999999999.999PR",
                            "<4567890123456789.000>"}),
        RegressionTestCase({-123, "9999999999999999S", "             123-"}),
        RegressionTestCase({-4567890123456789, "9999999999999999S",
                            "4567890123456789-"}),
        RegressionTestCase({-123, "S9999999999999999", "             -123"}),
        RegressionTestCase({-4567890123456789, "S9999999999999999",
                            "-4567890123456789"}),
        RegressionTestCase({123, "MI9999999999999999", "              123"}),
        RegressionTestCase({-4567890123456789, "MI9999999999999999",
                            "-4567890123456789"}),
        RegressionTestCase({123, "FMS9999999999999999", "+123"}),
        RegressionTestCase({-4567890123456789, "FMS9999999999999999",
                            "-4567890123456789"}),
        RegressionTestCase({456, "FM9999999999999999THPR", "456TH"}),
        RegressionTestCase({4567890123456789, "FM9999999999999999THPR",
                            "4567890123456789TH"}),
        RegressionTestCase({-4567890123456789, "FM9999999999999999THPR",
                            "<4567890123456789>"}),
        RegressionTestCase({456, "SG9999999999999999th",
                            "+             456th"}),
        RegressionTestCase({4567890123456789, "SG9999999999999999th",
                            "+4567890123456789th"}),
        RegressionTestCase({-4567890123456789, "SG9999999999999999th",
                            "-4567890123456789"}),
        RegressionTestCase({456, "0999999999999999", " 0000000000000456"}),
        RegressionTestCase({-4567890123456789, "0999999999999999",
                            "-4567890123456789"}),
        RegressionTestCase({456, "S0999999999999999", "+0000000000000456"}),
        RegressionTestCase({-4567890123456789, "S0999999999999999",
                            "-4567890123456789"}),
        RegressionTestCase({456, "FM0999999999999999", "0000000000000456"}),
        RegressionTestCase({-4567890123456789, "FM0999999999999999",
                            "-4567890123456789"}),
        RegressionTestCase({456, "FM9999999999999999.000", "456.000"}),
        RegressionTestCase({-4567890123456789, "FM9999999999999999.000",
                            "-4567890123456789.000"}),
        RegressionTestCase({456, "L9999999999999999.000",
                            "$              456.000"}),
        RegressionTestCase({-4567890123456789, "L9999999999999999.000",
                            "$-4567890123456789.000"}),
        RegressionTestCase({456, "FM9999999999999999.999", "456."}),
        RegressionTestCase({-4567890123456789, "FM9999999999999999.999",
                            "-4567890123456789."}),
        RegressionTestCase({456, "S 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9",
                            "                           +4 5 6 . 0 0 0"}),
        RegressionTestCase({-4567890123456789,
                            "S 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9",
                            " -4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 . 0 0 0"}),
        RegressionTestCase(
            {456,
             "99999 \"text\" 9999 \"9999\" 999 \"\\\"text between quote "
             "marks\\\"\" 9999",
             "      text      9999     \"text between quote marks\"   456"}),
        RegressionTestCase(
            {-4567890123456789,
             "99999 \"text\" 9999 \"9999\" 999 \"\\\"text between quote "
             "marks\\\"\" 9999",
             "-45678 text 9012 9999 345 \"text between quote marks\" 6789"}),
        RegressionTestCase({456, "999999SG9999999999", "      +       456"}),
        RegressionTestCase({-4567890123456789, "999999SG9999999999",
                            "456789-0123456789"})));
}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
