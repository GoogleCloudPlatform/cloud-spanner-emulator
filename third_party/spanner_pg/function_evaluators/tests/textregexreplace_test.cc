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
#include "third_party/spanner_pg/interface/regexp_evaluators.h"

namespace postgres_translator::function_evaluators {
namespace {

using ::testing::HasSubstr;
using ::testing::StrEq;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

class TextregexreplaceTest : public PgEvaluatorTest {
 protected:
  void TearDown() override {
    PgEvaluatorTest::TearDown();
    CleanupRegexCache();
  }
};

TEST_F(TextregexreplaceTest, ReturnsReplacedStringWhenPatternIsMatched) {
  EXPECT_THAT(Textregexreplace("abcde", ".c", "ff"),
              IsOkAndHolds(StrEq("affde")));
  EXPECT_THAT(Textregexreplace("abcde", "", "f"),
              IsOkAndHolds(StrEq("fabcde")));
  EXPECT_THAT(Textregexreplace("abcde", "^", "f"),
              IsOkAndHolds(StrEq("fabcde")));
  EXPECT_THAT(Textregexreplace("abcde", "$", "f"),
              IsOkAndHolds(StrEq("abcdef")));

  // Replaces only the first occurrence of the pattern
  EXPECT_THAT(Textregexreplace("ba_ab", "a", "f"),
              IsOkAndHolds(StrEq("bf_ab")));
  EXPECT_THAT(Textregexreplace("baa_aa_aab", "aa", "f"),
              IsOkAndHolds(StrEq("bf_aa_aab")));
  EXPECT_THAT(Textregexreplace("baaa_aaab", "aaa", "f"),
              IsOkAndHolds(StrEq("bf_aaab")));

  // Replaces all occurrences with 'g' flag
  EXPECT_THAT(Textregexreplace("ba_ab", "a", "f", "g"),
              IsOkAndHolds(StrEq("bf_fb")));
  EXPECT_THAT(Textregexreplace("baa_aa_aab", "aa", "f", "g"),
              IsOkAndHolds(StrEq("bf_f_fb")));
  EXPECT_THAT(Textregexreplace("baaa_aaab", "aaa", "f", "g"),
              IsOkAndHolds(StrEq("bf_fb")));
  // Case insensitive matching with 'i' flag
  EXPECT_THAT(Textregexreplace("ba_Ab", "a", "f", "ig"),
              IsOkAndHolds(StrEq("bf_fb")));
}

TEST_F(TextregexreplaceTest, ReturnsOriginalStringWhenPatternIsNotMatched) {
  EXPECT_THAT(Textregexreplace("abcde", "0", "f"),
              IsOkAndHolds(StrEq("abcde")));
  EXPECT_THAT(Textregexreplace("abcde", " ", "f"),
              IsOkAndHolds(StrEq("abcde")));
  EXPECT_THAT(Textregexreplace("abcde", " ", "f", "ig"),
              IsOkAndHolds(StrEq("abcde")));
}

TEST_F(TextregexreplaceTest, ReturnsErrorForInvalidRegexPattern) {
  EXPECT_THAT(Textregexreplace("abcde", "(.c", "f"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid regular expression")));
  EXPECT_THAT(Textregexreplace("abcde", ".c", "f", "z"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid regular expression option: \"z\"")));
}

// ---- Parameterized tests
// These tests exercise regression scenarios in PostgreSQL

struct RegressionTestCase {
  std::string input_source;
  std::string input_pattern;
  std::string input_replacement;
  std::string input_flags;
  std::string expected_result;
};

class RegressionTest : public PgEvaluatorTestWithParam<RegressionTestCase> {
  void TearDown() override {
    PgEvaluatorTestWithParam<RegressionTestCase>::TearDown();
    CleanupRegexCache();
  }
};

TEST_P(RegressionTest, PostgreSQLRegressionTests) {
  const RegressionTestCase& test_case = GetParam();
  if (test_case.input_flags.empty()) {
    EXPECT_THAT(
        Textregexreplace(test_case.input_source, test_case.input_pattern,
                         test_case.input_replacement),
        IsOkAndHolds(StrEq(test_case.expected_result)));
  } else {
    EXPECT_THAT(
        Textregexreplace(test_case.input_source, test_case.input_pattern,
                         test_case.input_replacement, test_case.input_flags),
        IsOkAndHolds(StrEq(test_case.expected_result)));
  }
}
INSTANTIATE_TEST_SUITE_P(
    RegressionTestSuite, RegressionTest,
    testing::Values(
        RegressionTestCase({"1112223333", "(\\d{3})(\\d{3})(\\d{4})",
                            "(\\1) \\2-\\3", "", "(111) 222-3333"}),
        RegressionTestCase({"AAA     BBB", "\\s+", " ", "", "AAA BBB"}),
        RegressionTestCase({"AAA     BBB  CCC", "\\s+", " ", "g",
                            "AAA BBB CCC"}),
        RegressionTestCase({"AAA", "^", "Z", "", "ZAAA"}),
        RegressionTestCase({"AAA", "$", "Z", "", "AAAZ"}),
        RegressionTestCase({"AAA", "^|$", "Z", "g", "ZAAAZ"}),
        RegressionTestCase({"AAA aaa", "a+", "Z", "", "AAA Z"}),
        RegressionTestCase({"AAA aaa", "A+", "Z", "gi", "Z Z"})));
}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
