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

#include <memory>
#include <optional>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "third_party/spanner_pg/function_evaluators/tests/test_base.h"
#include "third_party/spanner_pg/interface/regexp_evaluators.h"
#include "third_party/spanner_pg/shims/memory_context_pg_arena.h"
#include "third_party/spanner_pg/shims/stub_memory_reservation_manager.h"

namespace postgres_translator::function_evaluators {
namespace {

using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::IsNull;
using ::testing::Optional;
using ::testing::Pointee;
using ::testing::StrEq;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

class RegexpMatchTest : public PgEvaluatorTest {
 protected:
  void TearDown() override {
    PgEvaluatorTest::TearDown();
    CleanupRegexCache();
  }
};

TEST_F(RegexpMatchTest, ReturnsMatchesSuccessfully) {
  EXPECT_THAT(RegexpMatch("test@email.com",
                          "^([A-Za-z0-9._%-]+)@([A-Za-z0-9.-]+[.][A-Za-z]+)$"),
              IsOkAndHolds(Pointee(ElementsAre(Optional(StrEq("test")),
                                               Optional(StrEq("email.com"))))));
  EXPECT_THAT(RegexpMatch("abc123XYZ", "([a-z]+)([0-9]+)([A-Z]+)"),
              IsOkAndHolds(Pointee(ElementsAre(Optional(StrEq("abc")),
                                               Optional(StrEq("123")),
                                               Optional(StrEq("XYZ"))))));
  // Non-capturing groups are not returned
  EXPECT_THAT(RegexpMatch("abc123XYZ", "([a-z]+)(?:[0-9]+)([A-Z]+)"),
              IsOkAndHolds(Pointee(ElementsAre(Optional(StrEq("abc")),
                                               Optional(StrEq("XYZ"))))));
}

// ReDOS -
// https://owasp.org/www-community/attacks/Regular_expression_Denial_of_Service_-_ReDoS
TEST_F(RegexpMatchTest, HandlesReDOSGracefully) {
  ZETASQL_EXPECT_OK(RegexpMatch(std::string(10000, 'a'), "(a+)+"));
  ZETASQL_EXPECT_OK(RegexpMatch(std::string(10000, 'a'), "([a-zA-Z]+)*"));
  ZETASQL_EXPECT_OK(RegexpMatch(std::string(10000, 'a'), "(a|aa)+"));
  ZETASQL_EXPECT_OK(RegexpMatch(std::string(10000, 'a'), "(a|a?)+"));
  ZETASQL_EXPECT_OK(RegexpMatch(std::string(10000, 'a'), "(.*a){x} for x \\> 10"));
}

TEST_F(RegexpMatchTest, RegressionTestSimpleCases) {
  EXPECT_THAT(RegexpMatch("foobarbequebaz", "(bar)(beque)"),
              IsOkAndHolds(Pointee(ElementsAre(Optional(StrEq("bar")),
                                               Optional(StrEq("beque"))))));
  EXPECT_THAT(RegexpMatch("foObARbEqUEbAz", "(bar)(beque)", "i"),
              IsOkAndHolds(Pointee(ElementsAre(Optional(StrEq("bAR")),
                                               Optional(StrEq("bEqUE"))))));
  EXPECT_THAT(RegexpMatch("foobarbequebazilbarfbonk", "(b[^b]+)(b[^b]+)", "g"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("does not support the \"global\" option")));
  EXPECT_THAT(RegexpMatch("foo\nbar\nbequq\nbaz", "^", "m"),
              IsOkAndHolds(Pointee(ElementsAre(Optional(StrEq(""))))));
  EXPECT_THAT(RegexpMatch("foo\nbar\nbequq\nbaz", "$", "m"),
              IsOkAndHolds(Pointee(ElementsAre(Optional(StrEq(""))))));
  EXPECT_THAT(RegexpMatch("1\n2\n3\n4\n", "^.?", "m"),
              IsOkAndHolds(Pointee(ElementsAre(Optional(StrEq("1"))))));
  EXPECT_THAT(RegexpMatch("\n1\n2\n3\n4\n", ".?$", "m"),
              IsOkAndHolds(Pointee(ElementsAre(Optional(StrEq(""))))));
  EXPECT_THAT(RegexpMatch("\n1\n2\n3\n4", ".?$", "m"),
              IsOkAndHolds(Pointee(ElementsAre(Optional(StrEq(""))))));
  EXPECT_THAT(RegexpMatch("foobarbequebaz", "(bar)(.*)(beque)"),
              IsOkAndHolds(Pointee(ElementsAre(Optional(StrEq("bar")),
                                               Optional(StrEq("")),
                                               Optional(StrEq("beque"))))));
  EXPECT_THAT(RegexpMatch("foobarbequebaz", "(bar)(.+)(beque)"),
              IsOkAndHolds(IsNull()));
  EXPECT_THAT(
      RegexpMatch("foobarbequebaz", "(bar)(.+)?(beque)"),
      IsOkAndHolds(Pointee(ElementsAre(Optional(StrEq("bar")), Eq(std::nullopt),
                                       Optional(StrEq("beque"))))));
  EXPECT_THAT(RegexpMatch("foobarbequebaz", "barbeque"),
              IsOkAndHolds(Pointee(ElementsAre(Optional(StrEq("barbeque"))))));
  EXPECT_THAT(RegexpMatch("foobarbequebaz", "barbeque"),
              IsOkAndHolds(Pointee(ElementsAre(Optional(StrEq("barbeque"))))));
  EXPECT_THAT(RegexpMatch("foobarbequebaz", "(barbeque)", "gz"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid regular expression option: \"z\"")));
  EXPECT_THAT(RegexpMatch("foobarbequebaz", "(barbeque"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid regular expression")));
  EXPECT_THAT(RegexpMatch("foobarbequebaz", "(bar)(beque){2,1}"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid regular expression")));

  EXPECT_THAT(RegexpMatch("abc", ""),
              IsOkAndHolds(Pointee(ElementsAre(Optional(StrEq(""))))));
  EXPECT_THAT(RegexpMatch("abc", "bc"),
              IsOkAndHolds(Pointee(ElementsAre(Optional(StrEq("bc"))))));
  EXPECT_THAT(RegexpMatch("abc", "d"), IsOkAndHolds(IsNull()));
  EXPECT_THAT(RegexpMatch("abc", "(B)(c)", "i"),
              IsOkAndHolds(Pointee(
                  ElementsAre(Optional(StrEq("b")), Optional(StrEq("c"))))));
  EXPECT_THAT(RegexpMatch("abc", "Bd", "ig"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("does not support the \"global\" option")));
}

TEST_F(RegexpMatchTest, RegressionTestLookaheadConstraints) {
  EXPECT_THAT(RegexpMatch("ab", "a(?=b)b*"),
              IsOkAndHolds(Pointee(ElementsAre(Optional(StrEq("ab"))))));
  EXPECT_THAT(RegexpMatch("a", "a(?=b)b*"), IsOkAndHolds(IsNull()));
  EXPECT_THAT(RegexpMatch("abc", "a(?=b)b*(?=c)c*"),
              IsOkAndHolds(Pointee(ElementsAre(Optional(StrEq("abc"))))));
  EXPECT_THAT(RegexpMatch("ab", "a(?=b)b*(?=c)c*"), IsOkAndHolds(IsNull()));
  EXPECT_THAT(RegexpMatch("ab", "a(?!b)b*"), IsOkAndHolds(IsNull()));
  EXPECT_THAT(RegexpMatch("a", "a(?!b)b*"),
              IsOkAndHolds(Pointee(ElementsAre(Optional(StrEq("a"))))));
  EXPECT_THAT(RegexpMatch("b", "(?=b)b"),
              IsOkAndHolds(Pointee(ElementsAre(Optional(StrEq("b"))))));
  EXPECT_THAT(RegexpMatch("a", "(?=b)b"), IsOkAndHolds(IsNull()));
}

TEST_F(RegexpMatchTest, RegressionTestLookbehindConstraints) {
  EXPECT_THAT(RegexpMatch("abb", "(?<=a)b*"),
              IsOkAndHolds(Pointee(ElementsAre(Optional(StrEq("bb"))))));
  EXPECT_THAT(RegexpMatch("a", "a(?<=a)b*"),
              IsOkAndHolds(Pointee(ElementsAre(Optional(StrEq("a"))))));
  EXPECT_THAT(RegexpMatch("abc", "a(?<=a)b*(?<=b)c*"),
              IsOkAndHolds(Pointee(ElementsAre(Optional(StrEq("abc"))))));
  EXPECT_THAT(RegexpMatch("ab", "a(?<=a)b*(?<=b)c*"),
              IsOkAndHolds(Pointee(ElementsAre(Optional(StrEq("ab"))))));
  EXPECT_THAT(RegexpMatch("ab", "a*(?<!a)b*"),
              IsOkAndHolds(Pointee(ElementsAre(Optional(StrEq(""))))));
  EXPECT_THAT(RegexpMatch("ab", "a*(?<!a)b+"), IsOkAndHolds(IsNull()));
  EXPECT_THAT(RegexpMatch("b", "a*(?<!a)b+"),
              IsOkAndHolds(Pointee(ElementsAre(Optional(StrEq("b"))))));
  EXPECT_THAT(RegexpMatch("a", "a(?<!a)b*"), IsOkAndHolds(IsNull()));
  EXPECT_THAT(RegexpMatch("b", "(?<=b)b"), IsOkAndHolds(IsNull()));
  EXPECT_THAT(RegexpMatch("foobar", "(?<=f)b+"), IsOkAndHolds(IsNull()));
  EXPECT_THAT(RegexpMatch("foobar", "(?<=foo)b+"),
              IsOkAndHolds(Pointee(ElementsAre(Optional(StrEq("b"))))));
  EXPECT_THAT(RegexpMatch("foobar", "(?<=oo)b+"),
              IsOkAndHolds(Pointee(ElementsAre(Optional(StrEq("b"))))));
}

TEST_F(RegexpMatchTest, FuzzTestErrors) {
  EXPECT_THAT(RegexpMatch("", "(?!([^\\w\\W]){0}"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("parentheses () not balanced")));
}
}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
