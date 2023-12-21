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

#include <cstdint>
#include <limits>
#include <optional>
#include <string>

#include "zetasql/public/catalog.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "third_party/spanner_pg/datatypes/extended/conversion_finder.h"
#include "third_party/spanner_pg/datatypes/extended/pg_numeric_type.h"
#include "third_party/spanner_pg/interface/pg_arena.h"
#include "third_party/spanner_pg/interface/pg_arena_factory.h"

namespace postgres_translator::spangres {
namespace datatypes {

using FindConversionOptions = ::zetasql::Catalog::FindConversionOptions;
using ConversionSourceExpressionKind =
    ::zetasql::Catalog::ConversionSourceExpressionKind;

static void TestConversion(
    const zetasql::Type* from, const zetasql::Type* to,
    const zetasql::Value& input,
    const std::optional<zetasql::Value>& expected_output,
    bool is_error = false, std::optional<absl::string_view> error_msg = "") {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      zetasql::Conversion conversion,
      FindExtendedTypeConversion(
          from, to,
          FindConversionOptions(
              /*is_explicit=*/true, ConversionSourceExpressionKind::kOther)));

  // Create the pg arena to create and compare PG.NUMERIC.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      absl::StatusOr<std::unique_ptr<postgres_translator::interfaces::PGArena>>
          pg_arena,
      postgres_translator::interfaces::CreatePGArena(nullptr));

  absl::StatusOr<zetasql::Value> converted_value =
      conversion.evaluator().Eval(input);
  if (!is_error) {
    ZETASQL_ASSERT_OK(converted_value);
    EXPECT_EQ(converted_value.value(), expected_output);
  } else {
    EXPECT_FALSE(converted_value.ok());
    EXPECT_EQ(converted_value.status().message(), error_msg);
  }
}

class PgNumericInt64ConversionTest : public testing::TestWithParam<int64_t> {};

TEST_P(PgNumericInt64ConversionTest, ConvertInt64ToPgNumericSuccess) {
  TestConversion(
      zetasql::types::Int64Type(), GetPgNumericType(),
      zetasql::Value::Int64(GetParam()),
      CreatePgNumericValueWithMemoryContext(std::to_string(GetParam()))
          .value());
}

TEST_P(PgNumericInt64ConversionTest, ConvertPgNumericToInt64Success) {
  TestConversion(
      GetPgNumericType(), zetasql::types::Int64Type(),
      CreatePgNumericValueWithMemoryContext(std::to_string(GetParam())).value(),
      zetasql::Value::Int64(GetParam()));
}

INSTANTIATE_TEST_SUITE_P(NumericInt64, PgNumericInt64ConversionTest,
                         testing::Values(std::numeric_limits<int64_t>::min(),
                                         std::numeric_limits<int64_t>::max()));

class PgNumericDoubleConversionTest : public testing::TestWithParam<double> {};

TEST_P(PgNumericDoubleConversionTest, ConvertDoubleToPgNumericSuccess) {
  TestConversion(
      zetasql::types::DoubleType(), GetPgNumericType(),
      zetasql::Value::Double(GetParam()),
      CreatePgNumericValueWithMemoryContext(std::to_string(GetParam()))
          .value());
}

TEST_P(PgNumericDoubleConversionTest, ConvertPgNumericToDoubleSuccess) {
  TestConversion(
      GetPgNumericType(), zetasql::types::DoubleType(),
      CreatePgNumericValueWithMemoryContext(std::to_string(GetParam())).value(),
      zetasql::Value::Double(GetParam()));
}

INSTANTIATE_TEST_SUITE_P(NumericDouble, PgNumericDoubleConversionTest,
                         testing::Values(123.52353500, -123.52353500));

class PgNumericStringConversionTest
    : public testing::TestWithParam<std::string> {};

TEST_P(PgNumericStringConversionTest, ConvertStringToPgNumericSuccess) {
  TestConversion(zetasql::types::StringType(), GetPgNumericType(),
                 zetasql::Value::String(GetParam()),
                 CreatePgNumericValueWithMemoryContext(GetParam()).value());
}

TEST_P(PgNumericStringConversionTest, ConvertPgNumericToStringSuccess) {
  TestConversion(GetPgNumericType(), zetasql::types::StringType(),
                 CreatePgNumericValueWithMemoryContext(GetParam()).value(),
                 zetasql::Value::String(GetParam()));
}

INSTANTIATE_TEST_SUITE_P(NumericString, PgNumericStringConversionTest,
                         testing::Values("NaN", "123456"));

}  // namespace datatypes
}  // namespace postgres_translator::spangres

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
