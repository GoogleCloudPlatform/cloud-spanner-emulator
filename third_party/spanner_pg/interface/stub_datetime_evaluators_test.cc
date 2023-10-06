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
#include "third_party/spanner_pg/interface/datetime_evaluators.h"

namespace postgres_translator::function_evaluators {
namespace {

using ::testing::HasSubstr;
using ::zetasql_base::testing::StatusIs;

TEST(StubEvaluatorTest, PgToDate) {
  EXPECT_THAT(PgToDate("a", "b"),
              StatusIs(absl::StatusCode::kUnimplemented, HasSubstr("ToDate")));
}

TEST(StubEvaluatorTest, DateMii) {
  EXPECT_THAT(DateMii(1, 2),
              StatusIs(absl::StatusCode::kUnimplemented, HasSubstr("DateMii")));
}

TEST(StubEvaluatorTest, DatePli) {
  EXPECT_THAT(DatePli(1, 2),
              StatusIs(absl::StatusCode::kUnimplemented, HasSubstr("DatePli")));
}

TEST(StubEvaluatorTest, ToTimestamp) {
  EXPECT_THAT(ToTimestamp("a", "b"), StatusIs(absl::StatusCode::kUnimplemented,
                                              HasSubstr("ToTimestamp")));
}

TEST(StubEvaluatorTest, PgTimestampTzToChar) {
  EXPECT_THAT(PgTimestampTzToChar(absl::Now(), "a"),
              StatusIs(absl::StatusCode::kUnimplemented,
                       HasSubstr("TimestampTzToChar")));
}

TEST(StubEvaluatorTest, DateIn) {
  EXPECT_THAT(DateIn("a", "b"), StatusIs(absl::StatusCode::kUnimplemented,
                                              HasSubstr("DateIn")));
}

TEST(StubEvaluatorTest, TimestamptzIn) {
  EXPECT_THAT(TimestamptzIn("a", "b"), StatusIs(absl::StatusCode::kUnimplemented,
                                              HasSubstr("TimestamptzIn")));
}

TEST(StubEvaluatorTest, PgDateIn) {
  EXPECT_THAT(PgDateIn("a"), StatusIs(absl::StatusCode::kUnimplemented,
                                              HasSubstr("DateIn")));
}

TEST(StubEvaluatorTest, PgTimestamptzIn) {
  EXPECT_THAT(PgTimestamptzIn("a"), StatusIs(absl::StatusCode::kUnimplemented,
                                              HasSubstr("TimestamptzIn")));
}

}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
