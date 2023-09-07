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
using ::testing::IsNull;
using ::testing::Pointee;
using ::testing::StrEq;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

class TextregexsubstrTest : public PgEvaluatorTest {
 protected:
  void TearDown() override {
    PgEvaluatorTest::TearDown();
    CleanupRegexCache();
  }
};

TEST_F(TextregexsubstrTest, ReturnsSubstringWhenPatternIsMatched) {
  EXPECT_THAT(Textregexsubstr("abcd", "(bc)"),
              IsOkAndHolds(Pointee(StrEq("bc"))));
  EXPECT_THAT(Textregexsubstr("abcd", "a.c"),
              IsOkAndHolds(Pointee(StrEq("abc"))));
}

TEST_F(TextregexsubstrTest, ReturnsNullWhenPatternIsNotMatched) {
  EXPECT_THAT(Textregexsubstr("abcd", "ab.c"), IsOkAndHolds(IsNull()));
  EXPECT_THAT(Textregexsubstr("foo", "foo(bar)?"), IsOkAndHolds(IsNull()));
}

TEST_F(TextregexsubstrTest, ReturnsErrorForInvalidRegexPattern) {
  EXPECT_THAT(Textregexsubstr("abcd", "(a.c"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid regular expression")));
}

// ReDOS -
// https://owasp.org/www-community/attacks/Regular_expression_Denial_of_Service_-_ReDoS
TEST_F(TextregexsubstrTest, HandlesReDOSGracefully) {
  ZETASQL_EXPECT_OK(Textregexsubstr(std::string(10000, 'a'), "(a+)+"));
  ZETASQL_EXPECT_OK(Textregexsubstr(std::string(10000, 'a'), "([a-zA-Z]+)*"));
  ZETASQL_EXPECT_OK(Textregexsubstr(std::string(10000, 'a'), "(a|aa)+"));
  ZETASQL_EXPECT_OK(Textregexsubstr(std::string(10000, 'a'), "(a|a?)+"));
  ZETASQL_EXPECT_OK(Textregexsubstr(std::string(10000, 'a'), "(.*a){x} for x \\> 10"));
}

// ---- Parameterized tests
// These tests exercise regression scenarios in PostgreSQL

struct RegressionTestCase {
  std::string input_string;
  std::string input_pattern;
  std::string expected_result;
};

class RegressionTest : public PgEvaluatorTestWithParam<RegressionTestCase> {
  void TearDown() override {
    PgEvaluatorTestWithParam<RegressionTestCase>::TearDown();
    CleanupRegexCache();
  }
};

TEST_P(RegressionTest, PostgreSQLRegressionTests) {
  const RegressionTestCase test_case = GetParam();
  EXPECT_THAT(Textregexsubstr(test_case.input_string, test_case.input_pattern),
              IsOkAndHolds(Pointee(StrEq(test_case.expected_result))));
}
INSTANTIATE_TEST_SUITE_P(
    RegressionTestSuite, RegressionTest,
    testing::Values(
        RegressionTestCase({"a", "((a))+", "a"}),
        RegressionTestCase({"a", "((a)+)", "a"}),

        RegressionTestCase({"abcdefg", "c.e", "cde"}),
        RegressionTestCase({"abcdefg", "b(.*)f", "cde"})));
}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
