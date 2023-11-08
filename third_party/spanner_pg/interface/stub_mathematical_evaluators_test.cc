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
#include "third_party/spanner_pg/interface/mathematical_evaluators.h"

namespace postgres_translator::function_evaluators {
namespace {

using ::testing::HasSubstr;
using ::zetasql_base::testing::StatusIs;

TEST(StubEvaluatorTest, Abs) {
  EXPECT_THAT(Abs("-123"),
              StatusIs(absl::StatusCode::kUnimplemented, HasSubstr("Abs")));
}

TEST(StubEvaluatorTest, Add) {
  EXPECT_THAT(Add("123.45", "3.45"),
              StatusIs(absl::StatusCode::kUnimplemented, HasSubstr("Add")));
}

TEST(StubEvaluatorTest, Ceil) {
  EXPECT_THAT(Ceil("123.45"),
              StatusIs(absl::StatusCode::kUnimplemented, HasSubstr("Ceil")));
}

TEST(StubEvaluatorTest, Ceiling) {
  EXPECT_THAT(Ceiling("123.45"),
              StatusIs(absl::StatusCode::kUnimplemented, HasSubstr("Ceiling")));
}

TEST(StubEvaluatorTest, Divide) {
  EXPECT_THAT(Divide("123.45", "0.5"),
              StatusIs(absl::StatusCode::kUnimplemented, HasSubstr("Divide")));
}

TEST(StubEvaluatorTest, DivideTruncateTowardsZero) {
  EXPECT_THAT(DivideTruncateTowardsZero("123.45", "2"),
              StatusIs(absl::StatusCode::kUnimplemented,
                       HasSubstr("DivideTruncateTowardsZero")));
}

TEST(StubEvaluatorTest, Floor) {
  EXPECT_THAT(Floor("123.45"),
              StatusIs(absl::StatusCode::kUnimplemented, HasSubstr("Floor")));
}

TEST(StubEvaluatorTest, Mod) {
  EXPECT_THAT(Mod("123", "10"),
              StatusIs(absl::StatusCode::kUnimplemented, HasSubstr("Mod")));
}

TEST(StubEvaluatorTest, Multiply) {
  EXPECT_THAT(
      Multiply("123.45", "2.5"),
      StatusIs(absl::StatusCode::kUnimplemented, HasSubstr("Multiply")));
}

TEST(StubEvaluatorTest, Subtract) {
  EXPECT_THAT(
      Subtract("123.45", "3.45"),
      StatusIs(absl::StatusCode::kUnimplemented, HasSubstr("Subtract")));
}

TEST(StubEvaluatorTest, Trunc) {
  EXPECT_THAT(Trunc("123", 1),
              StatusIs(absl::StatusCode::kUnimplemented, HasSubstr("Trunc")));
}

TEST(StubEvaluatorTest, UnaryMinus) {
  EXPECT_THAT(UnaryMinus("-123"), StatusIs(absl::StatusCode::kUnimplemented,
                                           HasSubstr("UnaryMinus")));
}

}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
