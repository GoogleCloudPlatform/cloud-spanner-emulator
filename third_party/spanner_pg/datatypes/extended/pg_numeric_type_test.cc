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

#include "third_party/spanner_pg/datatypes/extended/pg_numeric_type.h"

#include <cstdint>
#include <string>

#include "google/spanner/v1/type.pb.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/collation.h"
#include "zetasql/public/types/simple_value.h"
#include "zetasql/public/types/type_modifiers.h"
#include "zetasql/public/types/type_parameters.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/datatypes/extended/spanner_extended_type.h"
#include "third_party/spanner_pg/util/valid_memory_context_fixture.h"

namespace {

using ::google::spanner::v1::TypeAnnotationCode;
using ::zetasql::Collation;
using ::zetasql::ExtendedTypeParameters;
using ::zetasql::SimpleValue;
using ::zetasql::TypeModifiers;
using ::zetasql::TypeParameters;
using ::zetasql::TypeParameterValue;
using ::postgres_translator::spangres::datatypes::CreatePgNumericValue;
using ::postgres_translator::spangres::datatypes::GetPgNumericArrayType;
using ::postgres_translator::spangres::datatypes::GetPgNumericType;
using ::postgres_translator::spangres::datatypes::SpannerExtendedType;
using ::zetasql_base::testing::StatusIs;

using PgNumericTypeTest = postgres_translator::test::ValidMemoryContext;

TEST_F(PgNumericTypeTest, ValidateTypeProperties) {
  const SpannerExtendedType* pg_type = GetPgNumericType();
  EXPECT_TRUE(pg_type->code() == TypeAnnotationCode::PG_NUMERIC);
  EXPECT_EQ(pg_type->ShortTypeName(zetasql::PRODUCT_EXTERNAL), "PG.NUMERIC");
  EXPECT_TRUE(pg_type->Equals(GetPgNumericType()));
  EXPECT_TRUE(pg_type->Equivalent(GetPgNumericType()));

  EXPECT_TRUE(pg_type->SupportsEquality());
  EXPECT_TRUE(pg_type->SupportsEquality(zetasql::LanguageOptions{}));
  EXPECT_TRUE(pg_type->SupportsGrouping(zetasql::LanguageOptions{}));
  EXPECT_TRUE(pg_type->SupportsPartitioning(zetasql::LanguageOptions{}));
  EXPECT_TRUE(pg_type->SupportsOrdering());
  EXPECT_TRUE(pg_type->SupportsOrdering(zetasql::LanguageOptions{},
                                        /*type_description=*/nullptr));
}

TypeParameters MakeValidTypeParameters(int64_t precision, int64_t scale) {
  return TypeParameters::MakeExtendedTypeParameters(ExtendedTypeParameters(
      {SimpleValue::Int64(precision), SimpleValue::Int64(scale)}));
}

TEST_F(PgNumericTypeTest, ValidateTypeParameters) {
  const SpannerExtendedType* pg_type = GetPgNumericType();
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::string type_name_with_no_parameters,
                       pg_type->TypeNameWithModifiers(
                           TypeModifiers(), zetasql::PRODUCT_EXTERNAL));
  EXPECT_EQ(type_name_with_no_parameters, "PG.NUMERIC");
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::string type_name_with_parameters,
                       pg_type->TypeNameWithModifiers(
                           TypeModifiers::MakeTypeModifiers(
                               MakeValidTypeParameters(5, 3), Collation()),
                           zetasql::PRODUCT_EXTERNAL));
  EXPECT_EQ(type_name_with_parameters, "PG.NUMERIC(5,3)");

  ZETASQL_EXPECT_OK(pg_type->ValidateAndResolveTypeParameters(
      {TypeParameterValue(SimpleValue::Int64(4)),
       TypeParameterValue(SimpleValue::Int64(2))},
      zetasql::PRODUCT_EXTERNAL));
  EXPECT_THAT(pg_type->ValidateAndResolveTypeParameters(
                  {}, zetasql::PRODUCT_EXTERNAL),
              StatusIs(absl::StatusCode::kInvalidArgument));

  ZETASQL_EXPECT_OK(pg_type->ValidateResolvedTypeParameters(
      MakeValidTypeParameters(4, 2), zetasql::PRODUCT_EXTERNAL));
  EXPECT_THAT(pg_type->ValidateResolvedTypeParameters(
                  TypeParameters(), zetasql::PRODUCT_EXTERNAL),
              StatusIs(absl::StatusCode::kInternal));
}

TEST_F(PgNumericTypeTest, ValueProperties) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(zetasql::Value pg_numeric,
                       CreatePgNumericValue("123456789.01234567890123456789"));
  EXPECT_TRUE(pg_numeric.type()->Equals(GetPgNumericType()));
  EXPECT_EQ(pg_numeric.DebugString(), "123456789.01234567890123456789");
  EXPECT_EQ(pg_numeric.Format(), "PG.NUMERIC(123456789.01234567890123456789)");

  EXPECT_EQ(pg_numeric.GetSQL(),
            "pg.cast_to_numeric('123456789.01234567890123456789')");
  EXPECT_EQ(pg_numeric.GetSQLLiteral(),
            "pg.cast_to_numeric('123456789.01234567890123456789')");
  EXPECT_FALSE(pg_numeric.is_null());

  zetasql::Value copy_pg_numeric = pg_numeric;
  EXPECT_TRUE(pg_numeric.Equals(copy_pg_numeric));
  EXPECT_EQ(pg_numeric, copy_pg_numeric);
  EXPECT_FALSE(pg_numeric.LessThan(copy_pg_numeric));
  EXPECT_FALSE(copy_pg_numeric.LessThan(pg_numeric));

  ZETASQL_ASSERT_OK_AND_ASSIGN(zetasql::Value nan_numeric,
                       CreatePgNumericValue("NaN"));
  EXPECT_FALSE(pg_numeric.Equals(nan_numeric));
  EXPECT_TRUE(pg_numeric.LessThan(nan_numeric));
}

TEST_F(PgNumericTypeTest, SpecialCaseValidation) {
  ZETASQL_EXPECT_OK(CreatePgNumericValue("nan"));
  ZETASQL_EXPECT_OK(CreatePgNumericValue("naN"));
  ZETASQL_EXPECT_OK(CreatePgNumericValue("nAn"));
  ZETASQL_EXPECT_OK(CreatePgNumericValue("nAN"));
  ZETASQL_EXPECT_OK(CreatePgNumericValue("Nan"));
  ZETASQL_EXPECT_OK(CreatePgNumericValue("NaN"));
  ZETASQL_EXPECT_OK(CreatePgNumericValue("NAn"));
  ZETASQL_EXPECT_OK(CreatePgNumericValue("NAN"));
  EXPECT_THAT(CreatePgNumericValue("-NaN"),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(CreatePgNumericValue("+NaN"),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(CreatePgNumericValue("-+00123456789.00012345600000"),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(CreatePgNumericValue("+-00123456789.00012345600000"),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(CreatePgNumericValue("00123456789.00012345600000-"),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(CreatePgNumericValue("00123456789.00012345600000+"),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(CreatePgNumericValue("00-123456789"),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(CreatePgNumericValue("00123456789.000.12345600000"),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(CreatePgNumericValue("00123456789..12345600000"),
              StatusIs(absl::StatusCode::kInvalidArgument));
  ZETASQL_EXPECT_OK(CreatePgNumericValue("00123456789."));
  ZETASQL_EXPECT_OK(CreatePgNumericValue(".00123456789"));
}

TEST_F(PgNumericTypeTest, InfinityValidation) {
  EXPECT_THAT(CreatePgNumericValue("-Infinity"),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(CreatePgNumericValue("+Infinity"),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(CreatePgNumericValue("Infinity"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PgNumericTypeTest, NaNComparison) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(zetasql::Value nan, CreatePgNumericValue("NaN"));
  ZETASQL_ASSERT_OK_AND_ASSIGN(zetasql::Value other_nan, CreatePgNumericValue("NaN"));
  ZETASQL_ASSERT_OK_AND_ASSIGN(zetasql::Value whole_number,
                       CreatePgNumericValue("00001234567890"));
  ZETASQL_ASSERT_OK_AND_ASSIGN(zetasql::Value numeric,
                       CreatePgNumericValue("00123456789.00012345600000"));

  EXPECT_TRUE(nan.Equals(other_nan));
  EXPECT_FALSE(nan.LessThan(other_nan));

  EXPECT_FALSE(nan.Equals(whole_number));
  EXPECT_FALSE(nan.LessThan(whole_number));
  EXPECT_TRUE(whole_number.LessThan(nan));
}

TEST_F(PgNumericTypeTest, ValueComparison) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(zetasql::Value numeric,
                       CreatePgNumericValue("00001234567890.00"));
  ZETASQL_ASSERT_OK_AND_ASSIGN(zetasql::Value smaller_numeric,
                       CreatePgNumericValue("+00123456789.00012345600000"));
  ZETASQL_ASSERT_OK_AND_ASSIGN(zetasql::Value smallest_numeric,
                       CreatePgNumericValue("-00123456789.00012345600000"));

  EXPECT_TRUE(numeric.Equals(numeric));
  EXPECT_TRUE(smaller_numeric.Equals(smaller_numeric));

  EXPECT_TRUE(smaller_numeric.LessThan(numeric));
  EXPECT_FALSE(numeric.LessThan(smaller_numeric));
  EXPECT_FALSE(smaller_numeric.Equals(numeric));

  EXPECT_TRUE(smallest_numeric.LessThan(numeric));
  EXPECT_FALSE(numeric.LessThan(smallest_numeric));
  EXPECT_FALSE(smallest_numeric.Equals(numeric));
}

TEST(PgNumericArrayTypeTest, ValidateTypeProperties) {
  const zetasql::ArrayType* type = GetPgNumericArrayType();
  ASSERT_NE(type, nullptr);
  EXPECT_TRUE(type->element_type()->Equals(GetPgNumericType()));
  EXPECT_EQ(type->ShortTypeName(zetasql::PRODUCT_EXTERNAL),
            "ARRAY<PG.NUMERIC>");
}

}  // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
