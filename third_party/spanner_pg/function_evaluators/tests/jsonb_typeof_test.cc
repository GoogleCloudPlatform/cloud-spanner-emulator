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

#include <string>

#include "googlesql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "third_party/spanner_pg/datatypes/common/numeric_core.h"
#include "third_party/spanner_pg/function_evaluators/tests/test_base.h"
#include "third_party/spanner_pg/interface/jsonb_evaluators.h"

namespace postgres_translator::function_evaluators {
namespace {

using ::googlesql_base::testing::IsOkAndHolds;
using ::googlesql_base::testing::StatusIs;

class JsonbTypeofTest : public PgEvaluatorTest {};

TEST_F(JsonbTypeofTest, ReturnsTypeString) {
  EXPECT_THAT(JsonbTypeof("null"),
              IsOkAndHolds(googlesql::Value::String("null")));
  EXPECT_THAT(JsonbTypeof("[null, null]"),
              IsOkAndHolds(googlesql::Value::String("array")));
  EXPECT_THAT(JsonbTypeof("[1,2,3.56]"),
              IsOkAndHolds(googlesql::Value::String("array")));
  EXPECT_THAT(JsonbTypeof(R"([{"a": "abc"}, {"x": "xyz"}])"),
              IsOkAndHolds(googlesql::Value::String("array")));
  EXPECT_THAT(JsonbTypeof(R"("hello")"),
              IsOkAndHolds(googlesql::Value::String("string")));
  EXPECT_THAT(JsonbTypeof(R"("")"),
              IsOkAndHolds(googlesql::Value::String("string")));
  EXPECT_THAT(JsonbTypeof(R"("nan")"),
              IsOkAndHolds(googlesql::Value::String("string")));
  EXPECT_THAT(JsonbTypeof(R"({ "a" : { "b" : [null, 3.5, -214215, true] } })"),
              IsOkAndHolds(googlesql::Value::String("object")));
  EXPECT_THAT(JsonbTypeof(R"({ "a" : [null, null] })"),
              IsOkAndHolds(googlesql::Value::String("object")));
  EXPECT_THAT(JsonbTypeof("-18446744073709551615124125"),
              IsOkAndHolds(googlesql::Value::String("number")));
  EXPECT_THAT(JsonbTypeof("18446744073709551615124125"),
              IsOkAndHolds(googlesql::Value::String("number")));
  EXPECT_THAT(JsonbTypeof("0.00"),
              IsOkAndHolds(googlesql::Value::String("number")));
  EXPECT_THAT(JsonbTypeof(postgres_translator::spangres::datatypes::common::
                              MaxJsonbNumericString()),
              IsOkAndHolds(googlesql::Value::String("number")));

  EXPECT_THAT(JsonbTypeof("true"),
              IsOkAndHolds(googlesql::Value::String("boolean")));
  EXPECT_THAT(JsonbTypeof("false"),
              IsOkAndHolds(googlesql::Value::String("boolean")));
}

TEST_F(JsonbTypeofTest, ReturnsErrorWhenInvalidArgumentIsGiven) {
  EXPECT_THAT(JsonbTypeof("a"), StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(JsonbTypeof(""), StatusIs(absl::StatusCode::kInvalidArgument));
}

}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
