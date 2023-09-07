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
#include "third_party/spanner_pg/interface/regexp_evaluators.h"

namespace postgres_translator::function_evaluators {
namespace {

using ::testing::IsFalse;
using ::testing::IsTrue;
using ::zetasql_base::testing::IsOkAndHolds;

class TextregexeqTest : public PgEvaluatorTest {
 protected:
  void TearDown() override {
    PgEvaluatorTest::TearDown();
    CleanupRegexCache();
  }
};

TEST_F(TextregexeqTest, ReturnsTrueWhenExpressionMatchesRegex) {
  EXPECT_THAT(Textregexeq("abcde123", "[0-9]+"), IsOkAndHolds(IsTrue()));
  EXPECT_THAT(Textregexeq("ab cde", "\\s+"), IsOkAndHolds(IsTrue()));
  EXPECT_THAT(Textregexeq("abcde", ""), IsOkAndHolds(IsTrue()));
  EXPECT_THAT(Textregexeq("", ".*"), IsOkAndHolds(IsTrue()));
  EXPECT_THAT(Textregexeq("", ""), IsOkAndHolds(IsTrue()));
}

TEST_F(TextregexeqTest, ReturnsFalseWhenExpressionDoesNotMatchRegex) {
  EXPECT_THAT(Textregexeq("abcde", "[0-9]+"), IsOkAndHolds(IsFalse()));
  EXPECT_THAT(Textregexeq("abcde", "\\s+"), IsOkAndHolds(IsFalse()));
  EXPECT_THAT(Textregexeq("abcde", "f"), IsOkAndHolds(IsFalse()));
  EXPECT_THAT(Textregexeq("", ".+"), IsOkAndHolds(IsFalse()));
  EXPECT_THAT(Textregexeq("", " "), IsOkAndHolds(IsFalse()));
}

// ---- Parameterized tests
// These tests exercise regression scenarios in PostgreSQL

struct RegressionTestCase {
  std::string input_string;
  std::string input_pattern;
  bool expected_result;
};

class RegressionTest : public PgEvaluatorTestWithParam<RegressionTestCase> {
  void TearDown() override {
    PgEvaluatorTestWithParam<RegressionTestCase>::TearDown();
    CleanupRegexCache();
  }
};

TEST_P(RegressionTest, PostgreSQLRegressionTests) {
  const RegressionTestCase& test_case = GetParam();
  EXPECT_THAT(Textregexeq(test_case.input_string, test_case.input_pattern),
              IsOkAndHolds(test_case.expected_result));
}
INSTANTIATE_TEST_SUITE_P(
    RegressionTestSuite, RegressionTest,
    testing::Values(
        // Implements the regression tests from native PostgreSQL:
        RegressionTestCase({"bbbbb", "^([bc])\\1*$", true}),
        RegressionTestCase({"ccc", "^([bc])\\1*$", true}),
        RegressionTestCase({"xxx", "^([bc])\\1*$", false}),
        RegressionTestCase({"bbc", "^([bc])\\1*$", false}),
        RegressionTestCase({"b", "^([bc])\\1*$", true}),
        RegressionTestCase({"abc abc abc", "^(\\w+)( \\1)+$", true}),
        RegressionTestCase({"abc abd abc", "^(\\w+)( \\1)+$", false}),
        RegressionTestCase({"abc abc abd", "^(\\w+)( \\1)+$", false}),
        RegressionTestCase({"abc abc abc", "^(.+)( \\1)+$", true}),
        RegressionTestCase({"abc abd abc", "^(.+)( \\1)+$", false}),
        RegressionTestCase({"abc abc abd", "^(.+)( \\1)+$", false})));
}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
