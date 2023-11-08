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
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"
#include "third_party/spanner_pg/function_evaluators/tests/test_base.h"
#include "third_party/spanner_pg/interface/jsonb_evaluators.h"

namespace postgres_translator::function_evaluators {
namespace {

using spangres::datatypes::CreatePgJsonbValue;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

static zetasql::Value CreatePgJsonBNullValue() {
  static const zetasql::Type* gsql_pg_jsonb =
      spangres::datatypes::GetPgJsonbType();
  return zetasql::values::Null(gsql_pg_jsonb);
}

class JsonBObjectFieldTest : public PgEvaluatorTest {};

TEST_F(JsonBObjectFieldTest, ReturnsJsonbValue) {
  EXPECT_THAT(JsonBObjectField(R"({"a": "string val"})", "a"),
              IsOkAndHolds(*CreatePgJsonbValue(R"("string val")")));
  EXPECT_THAT(JsonBObjectField(R"({"a": {"b": "string_val"}})", "a"),
              IsOkAndHolds(*CreatePgJsonbValue(R"({"b": "string_val"})")));
  EXPECT_THAT(JsonBObjectField(R"([1.00, "string val"])", "a"),
              IsOkAndHolds(CreatePgJsonBNullValue()));
  EXPECT_THAT(JsonBObjectField(R"({"a": "string val"})", "no match"),
              IsOkAndHolds(CreatePgJsonBNullValue()));
  EXPECT_THAT(JsonBObjectField(R"({"a": ""})", "a"),
              IsOkAndHolds(*CreatePgJsonbValue(R"("")")));
  EXPECT_THAT(JsonBObjectField(R"({"a": null})", "a"),
              IsOkAndHolds(*CreatePgJsonbValue("null")));
  EXPECT_THAT(JsonBObjectField(R"({"a": true})", "a"),
              IsOkAndHolds(*CreatePgJsonbValue("true")));
  EXPECT_THAT(JsonBObjectField(R"({"a": false})", "a"),
              IsOkAndHolds(*CreatePgJsonbValue("false")));
  EXPECT_THAT(JsonBObjectField(R"({"a": 1})", "a"),
              IsOkAndHolds(*CreatePgJsonbValue("1")));
  EXPECT_THAT(JsonBObjectField(R"({"a": 1.5})", "a"),
              IsOkAndHolds(*CreatePgJsonbValue("1.5")));
}

TEST_F(JsonBObjectFieldTest, ReturnsErrorWhenInvalidArgumentIsGiven) {
  EXPECT_THAT(JsonBObjectField("a", "a"),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(JsonBObjectField("", "a"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

}  // namespace
}  // namespace postgres_translator::function_evaluators

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
