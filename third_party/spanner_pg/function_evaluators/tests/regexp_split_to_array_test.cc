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

#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "third_party/spanner_pg/function_evaluators/tests/test_base.h"
#include "third_party/spanner_pg/interface/regexp_evaluators.h"

namespace postgres_translator::function_evaluators {
namespace {

using ::testing::ContainerEq;
using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::Pointee;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

class RegexpSplitToArrayTest : public PgEvaluatorTest {
  void TearDown() override {
    PgEvaluatorTest::TearDown();
    CleanupRegexCache();
  }
};

TEST_F(RegexpSplitToArrayTest, ReturnsInputSplitByDelimiter) {
  EXPECT_THAT(RegexpSplitToArray("a b  c", "\\s+"),
              IsOkAndHolds(Pointee(ElementsAre("a", "b", "c"))));

  // "c" flag enforces case sensitive matches
  EXPECT_THAT(RegexpSplitToArray("aDbDDc", "D+", "c"),
              IsOkAndHolds(Pointee(ElementsAre("a", "b", "c"))));
  // "i" flag enforces case insensitive matches
  EXPECT_THAT(RegexpSplitToArray("aDbDDc", "D+", "i"),
              IsOkAndHolds(Pointee(ElementsAre("a", "b", "c"))));
  EXPECT_THAT(RegexpSplitToArray("adbDDc", "d+", "i"),
              IsOkAndHolds(Pointee(ElementsAre("a", "b", "c"))));
}

TEST_F(RegexpSplitToArrayTest, ReturnsInputWhenDelimiterIsNotPresent) {
  EXPECT_THAT(RegexpSplitToArray("a.b..c", "\\s+"),
              IsOkAndHolds(Pointee(ElementsAre("a.b..c"))));

  EXPECT_THAT(RegexpSplitToArray("aDbDDc", "d+", "c"),
              IsOkAndHolds(Pointee(ElementsAre("aDbDDc"))));
}

TEST_F(RegexpSplitToArrayTest, ReturnsEmptyStringWhenInputIsEmpty) {
  EXPECT_THAT(RegexpSplitToArray("", "\\s+"),
              IsOkAndHolds(Pointee(ElementsAre(""))));
}

// ReDOS -
// https://owasp.org/www-community/attacks/Regular_expression_Denial_of_Service_-_ReDoS
TEST_F(RegexpSplitToArrayTest, HandlesReDOSGracefully) {
  ZETASQL_EXPECT_OK(RegexpSplitToArray(std::string(10000, 'a'), "(a+)+"));
  ZETASQL_EXPECT_OK(RegexpSplitToArray(std::string(10000, 'a'), "([a-zA-Z]+)*"));
  ZETASQL_EXPECT_OK(RegexpSplitToArray(std::string(10000, 'a'), "(a|aa)+"));
  ZETASQL_EXPECT_OK(RegexpSplitToArray(std::string(10000, 'a'), "(a|a?)+"));
  ZETASQL_EXPECT_OK(
      RegexpSplitToArray(std::string(10000, 'a'), "(.*a){x} for x \\> 10"));
}

// ---- Parameterized tests
// These tests exercise regression scenarios in PostgreSQL

struct RegressionTestCase {
  std::string input_string;
  std::string input_separator;
  std::string input_flags;
  std::vector<std::string> expected_result;
};

class RegressionTest : public PgEvaluatorTestWithParam<RegressionTestCase> {
  void TearDown() override {
    PgEvaluatorTestWithParam<RegressionTestCase>::TearDown();
    CleanupRegexCache();
  }
};

TEST_P(RegressionTest, PostgreSQLRegressionTests) {
  const RegressionTestCase test_case = GetParam();
  if (test_case.input_flags.empty()) {
    EXPECT_THAT(
        RegexpSplitToArray(test_case.input_string, test_case.input_separator),
        IsOkAndHolds(Pointee(ContainerEq(test_case.expected_result))));
  } else {
    EXPECT_THAT(
        RegexpSplitToArray(test_case.input_string, test_case.input_separator,
                           test_case.input_flags),
        IsOkAndHolds(Pointee(ContainerEq(test_case.expected_result))));
  }
}

INSTANTIATE_TEST_SUITE_P(
    RegressionTestSuite, RegressionTest,
    testing::Values(
        // Test cases that do not specify regex flags
        RegressionTestCase({"the quick brown fox jumps over the lazy dog",
                            "\\s+",
                            "",
                            {"the", "quick", "brown", "fox", "jumps", "over",
                             "the", "lazy", "dog"}}),
        RegressionTestCase({"the quick brown fox jumps over the lazy dog",
                            "\\s*",
                            "",
                            {"t", "h", "e", "q", "u", "i", "c", "k", "b",
                             "r", "o", "w", "n", "f", "o", "x", "j", "u",
                             "m", "p", "s", "o", "v", "e", "r", "t", "h",
                             "e", "l", "a", "z", "y", "d", "o", "g"}}),
        RegressionTestCase({"the quick brown fox jumps over the lazy dog",
                            "",
                            "",
                            {"t", "h", "e", " ", "q", "u", "i", "c", "k",
                             " ", "b", "r", "o", "w", "n", " ", "f", "o",
                             "x", " ", "j", "u", "m", "p", "s", " ", "o",
                             "v", "e", "r", " ", "t", "h", "e", " ", "l",
                             "a", "z", "y", " ", "d", "o", "g"}}),
        RegressionTestCase({"the quick brown fox jumps over the lazy dog",
                            "nomatch",
                            "",
                            {"the quick brown fox jumps over the lazy dog"}}),
        RegressionTestCase({"123456", "1", "", {"", "23456"}}),
        RegressionTestCase({"123456", "6", "", {"12345", ""}}),
        RegressionTestCase({"123456", ".", "", {"", "", "", "", "", "", ""}}),
        RegressionTestCase({"123456", "", "", {"1", "2", "3", "4", "5", "6"}}),
        RegressionTestCase(
            {"123456", "(?:)", "", {"1", "2", "3", "4", "5", "6"}}),
        RegressionTestCase({"1", "", "", {"1"}}),

        // Test cases that specify regex flags
        RegressionTestCase({"thE QUick bROWn FOx jUMPs ovEr The lazy dOG",
                            "e",
                            "i",  // case insensitive
                            {"th", " QUick bROWn FOx jUMPs ov", "r Th",
                             " lazy dOG"}})));

TEST_F(RegexpSplitToArrayTest, ReturnsInvalidArgumentErrorWhenRegexIsInvalid) {
  EXPECT_THAT(RegexpSplitToArray("aaaaa", "[a]{4}+"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid regular expression")));

  // "z" flag does not exist
  EXPECT_THAT(RegexpSplitToArray("thE QUick bROWn FOx jUMPs ovEr The lazy dOG",
                                 "e", "iz"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid regular expression")));
  EXPECT_THAT(RegexpSplitToArray("thE QUick bROWn FOx jUMPs ovEr The lazy dOG",
                                 "e", "g"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("does not support the \"global\" option")));
}
}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
