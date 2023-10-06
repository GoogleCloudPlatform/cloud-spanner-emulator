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

#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"

#include <string>

#include "google/spanner/v1/type.pb.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/types/array_type.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/datatypes/extended/spanner_extended_type.h"
#include "third_party/spanner_pg/util/valid_memory_context_fixture.h"

namespace {

using google::spanner::v1::TypeAnnotationCode;
using ::postgres_translator::spangres::datatypes::CreatePgJsonbValue;
using ::postgres_translator::spangres::datatypes::GetPgJsonbArrayType;
using ::postgres_translator::spangres::datatypes::GetPgJsonbNormalizedValue;
using ::postgres_translator::spangres::datatypes::GetPgJsonbType;
using ::postgres_translator::spangres::datatypes::SpannerExtendedType;

using PgJsonbTypeTest = postgres_translator::test::ValidMemoryContext;

TEST_F(PgJsonbTypeTest, GetExtendedType) {
  const SpannerExtendedType* pg_type = GetPgJsonbType();
  EXPECT_TRUE(pg_type->code() == TypeAnnotationCode::PG_JSONB);
  EXPECT_EQ(pg_type->ShortTypeName(zetasql::PRODUCT_EXTERNAL), "PG.JSONB");
  EXPECT_TRUE(pg_type->Equals(GetPgJsonbType()));
  EXPECT_TRUE(pg_type->Equivalent(GetPgJsonbType()));

  EXPECT_FALSE(pg_type->SupportsEquality());
  EXPECT_FALSE(pg_type->SupportsEquality(zetasql::LanguageOptions{}));
  EXPECT_FALSE(pg_type->SupportsGrouping(zetasql::LanguageOptions{}));
  EXPECT_FALSE(pg_type->SupportsPartitioning(zetasql::LanguageOptions{}));
  EXPECT_FALSE(pg_type->SupportsOrdering());
  EXPECT_FALSE(pg_type->SupportsOrdering(zetasql::LanguageOptions{},
                                         /*type_description=*/nullptr));
}

TEST_F(PgJsonbTypeTest, GetExtendedValue) {
  auto validate_extended_value = [](absl::string_view readable_jsonb) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(zetasql::Value pg_jsonb,
                         CreatePgJsonbValue(readable_jsonb));
    ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Cord normalized_jsonb,
                         GetPgJsonbNormalizedValue(pg_jsonb));
    EXPECT_EQ(normalized_jsonb.Flatten(), readable_jsonb);
  };
  validate_extended_value(
      "{\"a\": \"123456789.0123456789012345678\", \"b\": 12.01}");
  validate_extended_value("[2, 5, 29]");
  validate_extended_value("{}");
  validate_extended_value("[]");
  validate_extended_value("null");
}

TEST(PgJsonbArrayTypeTest, ValidateTypeProperties) {
  const zetasql::ArrayType* type = GetPgJsonbArrayType();
  ASSERT_NE(type, nullptr);
  EXPECT_TRUE(type->element_type()->Equals(GetPgJsonbType()));
  EXPECT_EQ(type->ShortTypeName(zetasql::PRODUCT_EXTERNAL),
            "ARRAY<PG.JSONB>");
}

using ValuePropertiesTest =
    postgres_translator::test::ValidMemoryContextParameterized<std::string>;
using ValueErrorsTest =
    postgres_translator::test::ValidMemoryContextParameterized<std::string>;

TEST_P(ValuePropertiesTest, ValueProperties) {
  std::string input_jsonb = GetParam();

  ZETASQL_ASSERT_OK_AND_ASSIGN(zetasql::Value pg_jsonb,
                       CreatePgJsonbValue(input_jsonb));
  EXPECT_TRUE(pg_jsonb.type()->Equals(GetPgJsonbType()));
  EXPECT_EQ(pg_jsonb.DebugString(), input_jsonb);
  EXPECT_EQ(pg_jsonb.Format(), "PG.JSONB(" + input_jsonb + ")");
  EXPECT_EQ(pg_jsonb.GetSQL(),
            "CAST(" + zetasql::ToSingleQuotedStringLiteral(input_jsonb) +
                " AS PG.JSONB)");
  EXPECT_EQ(pg_jsonb.GetSQLLiteral(),
            "CAST(" + zetasql::ToSingleQuotedStringLiteral(input_jsonb) +
                " AS PG.JSONB)");
  EXPECT_FALSE(pg_jsonb.is_null());
}

TEST_P(ValueErrorsTest, InvalidJsonInput) {
  std::string input_jsonb = GetParam();
  EXPECT_FALSE(CreatePgJsonbValue(input_jsonb).ok());
}

INSTANTIATE_TEST_SUITE_P(
    PgJsonbTypeTest, ValuePropertiesTest,
    testing::Values("{\"a\": \"123456789.0123456789012345678\", \"b\": 12.01}",
                    "{\"c\": \"string with \\\"quotes\\\"(', \\\") "
                    "and non ascii characters(ß, Д).\"}",
                    "[\"string with ' single quote\"]"));
INSTANTIATE_TEST_SUITE_P(PgJsonbTypeTest, ValueErrorsTest,
                         testing::Values("[1,2,[1,2, [1,2], 1,2]",
                                         "\"\\320\\224, \\xD0\\x94\""));

}  // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
