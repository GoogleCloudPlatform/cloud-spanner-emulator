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

using ::testing::HasSubstr;
using ::testing::IsFalse;
using ::testing::IsTrue;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

class TextregexneTest : public PgEvaluatorTest {
 protected:
  void TearDown() override {
    PgEvaluatorTest::TearDown();
    CleanupRegexCache();
  }
};

TEST_F(TextregexneTest, ReturnsTrueWhenExpressionDoesNOTMatchRegex) {
  EXPECT_THAT(Textregexne("abcde", "[0-9]+"), IsOkAndHolds(IsTrue()));
  EXPECT_THAT(Textregexne("abcde", "\\s+"), IsOkAndHolds(IsTrue()));
  EXPECT_THAT(Textregexne("", "\\s+"), IsOkAndHolds(IsTrue()));
}

TEST_F(TextregexneTest, ReturnsFalseWhenExpressionMatchesRegex) {
  EXPECT_THAT(Textregexne("abcde123", "[0-9]+"), IsOkAndHolds(IsFalse()));
  EXPECT_THAT(Textregexne("ab cde", "\\s+"), IsOkAndHolds(IsFalse()));
  EXPECT_THAT(Textregexne("abcde", ""), IsOkAndHolds(IsFalse()));
  EXPECT_THAT(Textregexne("", ".*"), IsOkAndHolds(IsFalse()));
  EXPECT_THAT(Textregexne("", ""), IsOkAndHolds(IsFalse()));
}

// ReDOS -
// https://owasp.org/www-community/attacks/Regular_expression_Denial_of_Service_-_ReDoS
TEST_F(TextregexneTest, HandlesReDOSGracefully) {
  ZETASQL_EXPECT_OK(Textregexne(std::string(10000, 'a'), "(a+)+"));
  ZETASQL_EXPECT_OK(Textregexne(std::string(10000, 'a'), "([a-zA-Z]+)*"));
  ZETASQL_EXPECT_OK(Textregexne(std::string(10000, 'a'), "(a|aa)+"));
  ZETASQL_EXPECT_OK(Textregexne(std::string(10000, 'a'), "(a|a?)+"));
  ZETASQL_EXPECT_OK(Textregexne(std::string(10000, 'a'), "(.*a){x} for x \\> 10"));
}

// Tests for stack overflow prevention
TEST_F(TextregexneTest, RegexTooComplex) {
  EXPECT_THAT(Textregexne(std::string(10000, 'a'),
                          absl::StrCat(std::string(20000, '('),
                                       std::string(20000, ')'))),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("regular expression is too complex")));
}
}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
