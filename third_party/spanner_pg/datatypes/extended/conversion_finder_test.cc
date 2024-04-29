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

#include "third_party/spanner_pg/datatypes/extended/conversion_finder.h"

#include "zetasql/public/cast.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_numeric_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_oid_type.h"
#include "third_party/spanner_pg/interface/pg_arena.h"
#include "third_party/spanner_pg/interface/pg_arena_factory.h"

namespace postgres_translator::spangres {
namespace datatypes {

using ::zetasql::types::BoolType;
using ::zetasql::types::BytesType;
using ::zetasql::types::DoubleType;
using ::zetasql::types::Int64Type;
using ::zetasql::types::StringType;
using FindConversionOptions = ::zetasql::Catalog::FindConversionOptions;
using ConversionSourceExpressionKind =
    ::zetasql::Catalog::ConversionSourceExpressionKind;

struct ConversionFoundTestCase {
  const zetasql::Type* from;
  const zetasql::Type* to;
  const FindConversionOptions options;
  zetasql::Value input;
  zetasql::Value expected_output;
};

class ConversionFoundTest
    : public testing::TestWithParam<ConversionFoundTestCase> {};

TEST_P(ConversionFoundTest, ConversionFound) {
  const ConversionFoundTestCase& test = GetParam();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      zetasql::Conversion conversion,
      FindExtendedTypeConversion(test.from, test.to, test.options));

  // Create the pg arena to create and compare PG.NUMERIC.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      absl::StatusOr<std::unique_ptr<postgres_translator::interfaces::PGArena>>
          pg_arena,
      postgres_translator::interfaces::CreatePGArena(nullptr));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto converted_value,
                       conversion.evaluator().Eval(test.input));
  EXPECT_EQ(converted_value, test.expected_output);
}

std::vector<ConversionFoundTestCase> GetConversionFoundTestCases() {
  return {
      // <TYPE> -> PG.NUMERIC conversions
      {Int64Type(), GetPgNumericType(),
       FindConversionOptions(/*is_explicit=*/true,
                             ConversionSourceExpressionKind::kOther),
       zetasql::Value::Int64(-123456),
       CreatePgNumericValueWithMemoryContext("-123456").value()},
      {DoubleType(), GetPgNumericType(),
       FindConversionOptions(/*is_explicit=*/true,
                             ConversionSourceExpressionKind::kOther),
       zetasql::Value::Double(-123.523535),
       CreatePgNumericValueWithMemoryContext("-123.523535").value()},
      {StringType(), GetPgNumericType(),
       FindConversionOptions(/*is_explicit=*/true,
                             ConversionSourceExpressionKind::kOther),
       zetasql::Value::String("NaN"),
       CreatePgNumericValueWithMemoryContext("NaN").value()},
      // PG.NUMERIC -> <TYPE> conversions
      {GetPgNumericType(), Int64Type(),
       FindConversionOptions(/*is_explicit=*/true,
                             ConversionSourceExpressionKind::kOther),
       CreatePgNumericValueWithMemoryContext("123456").value(),
       zetasql::Value::Int64(123456)},
      {GetPgNumericType(), DoubleType(),
       FindConversionOptions(/*is_explicit=*/true,
                             ConversionSourceExpressionKind::kOther),
       CreatePgNumericValueWithMemoryContext("123.52353500").value(),
       zetasql::Value::Double(123.523535)},
      {GetPgNumericType(), StringType(),
       FindConversionOptions(/*is_explicit=*/true,
                             ConversionSourceExpressionKind::kOther),
       CreatePgNumericValueWithMemoryContext("123.000").value(),
       zetasql::Value::String("123.000")},
      // <TYPE> -> PG.JSONB conversions
      {StringType(), GetPgJsonbType(),
       FindConversionOptions(/*is_explicit=*/true,
                             ConversionSourceExpressionKind::kOther),
       zetasql::Value::String("{\"a\":    1}"),
       CreatePgJsonbValueWithMemoryContext("{\"a\": 1}").value()},
      // PG.JSONB -> <TYPE> conversions
      {GetPgJsonbType(), Int64Type(),
       FindConversionOptions(/*is_explicit=*/true,
                             ConversionSourceExpressionKind::kOther),
       CreatePgJsonbValueWithMemoryContext("123456").value(),
       zetasql::Value::Int64(123456)},
      {GetPgJsonbType(), DoubleType(),
       FindConversionOptions(/*is_explicit=*/true,
                             ConversionSourceExpressionKind::kOther),
       CreatePgJsonbValueWithMemoryContext("123.52353500").value(),
       zetasql::Value::Double(123.523535)},
      {GetPgJsonbType(), GetPgNumericType(),
       FindConversionOptions(/*is_explicit=*/true,
                             ConversionSourceExpressionKind::kOther),
       CreatePgJsonbValueWithMemoryContext("123.52353500").value(),
       CreatePgNumericValueWithMemoryContext("123.52353500").value()},
      {GetPgJsonbType(), StringType(),
       FindConversionOptions(/*is_explicit=*/true,
                             ConversionSourceExpressionKind::kOther),
       CreatePgJsonbValueWithMemoryContext("\"abc\"").value(),
       zetasql::Value::String("\"abc\"")},
      {GetPgOidType(), StringType(),
       FindConversionOptions(/*is_explicit=*/true,
                             ConversionSourceExpressionKind::kOther),
       *CreatePgOidValue(42),
       zetasql::Value::String("42")},
      {StringType(), GetPgOidType(),
       FindConversionOptions(/*is_explicit=*/true,
                             ConversionSourceExpressionKind::kOther),
       zetasql::Value::String("42"),
       *CreatePgOidValue(42)},
      {GetPgOidType(), Int64Type(),
       FindConversionOptions(/*is_explicit=*/true,
                             ConversionSourceExpressionKind::kOther),
       *CreatePgOidValue(42),
       zetasql::Value::Int64(42)},
      {Int64Type(), GetPgOidType(),
       FindConversionOptions(/*is_explicit=*/true,
                             ConversionSourceExpressionKind::kOther),
       zetasql::Value::Int64(42),
       *CreatePgOidValue(42)},
  };
}

INSTANTIATE_TEST_SUITE_P(ConversionsTestSuite, ConversionFoundTest,
                         testing::ValuesIn(GetConversionFoundTestCases()));

struct DummyConversionFoundTestCase {
  const zetasql::Type* from;
  const zetasql::Type* to;
  const FindConversionOptions options;
};

class DummyConversionFoundTest
    : public testing::TestWithParam<DummyConversionFoundTestCase> {};

TEST_P(DummyConversionFoundTest, ConversionFound) {
  const DummyConversionFoundTestCase& test = GetParam();
  ZETASQL_EXPECT_OK(FindExtendedTypeConversion(test.from, test.to, test.options));
}

std::vector<DummyConversionFoundTestCase> GetDummyConversionFoundTestCases() {
  return {// PG.NUMERIC -> PG.NUMERIC (fixed-precision cast)
          {GetPgNumericType(), GetPgNumericType(),
           FindConversionOptions(/*is_explicit=*/true,
                                 ConversionSourceExpressionKind::kOther)}};
}

INSTANTIATE_TEST_SUITE_P(ConversionsTestSuite, DummyConversionFoundTest,
                         testing::ValuesIn(GetDummyConversionFoundTestCases()));

struct ConversionNotFoundTestCase {
  const zetasql::Type* from;
  const zetasql::Type* to;
  const FindConversionOptions options;
};

class ConversionNotFoundTest
    : public testing::TestWithParam<ConversionNotFoundTestCase> {};

TEST_P(ConversionNotFoundTest, ConversionNotFound) {
  const ConversionNotFoundTestCase& test = GetParam();
  EXPECT_THAT(
      FindExtendedTypeConversion(test.from, test.to, test.options),
      zetasql_base::testing::StatusIs(
          testing::_,
          testing::ContainsRegex(
              "(Cast|Coercion) from type .* to type .* not found in catalog")));
}

std::vector<ConversionNotFoundTestCase> GetConversionNotFoundTestCases() {
  return {
      {BoolType(), GetPgNumericType(),
       FindConversionOptions(/*is_explicit=*/true,
                             ConversionSourceExpressionKind::kOther)},
      {GetPgNumericType(), BytesType(),
       FindConversionOptions(/*is_explicit=*/true,
                             ConversionSourceExpressionKind::kOther)},
      {GetPgNumericType(), Int64Type(),
       FindConversionOptions(/*is_explicit=*/false,
                             ConversionSourceExpressionKind::kOther)},
      {BoolType(), GetPgJsonbType(),
       FindConversionOptions(/*is_explicit=*/true,
                             ConversionSourceExpressionKind::kOther)},
      {GetPgJsonbType(), BytesType(),
       FindConversionOptions(/*is_explicit=*/true,
                             ConversionSourceExpressionKind::kOther)},
      {GetPgOidType(), DoubleType(),
       FindConversionOptions(/*is_explicit=*/true,
                             ConversionSourceExpressionKind::kOther)},
      {GetPgNumericType(), GetPgOidType(),
       FindConversionOptions(/*is_explicit=*/true,
                             ConversionSourceExpressionKind::kOther)},
  };
}

INSTANTIATE_TEST_SUITE_P(ConversionsTestSuite, ConversionNotFoundTest,
                         testing::ValuesIn(GetConversionNotFoundTestCases()));

}  // namespace datatypes
}  // namespace postgres_translator::spangres

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
