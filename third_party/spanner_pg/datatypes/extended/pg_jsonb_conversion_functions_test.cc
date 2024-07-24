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

#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_conversion_functions.h"

#include <string>

#include "zetasql/public/catalog.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/base/no_destructor.h"
#include "absl/status/status.h"
#include "third_party/spanner_pg/datatypes/common/numeric_core.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_numeric_type.h"
#include "third_party/spanner_pg/interface/pg_arena.h"
#include "third_party/spanner_pg/interface/pg_arena_factory.h"

namespace postgres_translator::spangres {
namespace datatypes {
namespace {

const zetasql::Type* gsql_bool = zetasql::types::BoolType();
const zetasql::Type* gsql_double = zetasql::types::DoubleType();
const zetasql::Type* gsql_float = zetasql::types::FloatType();
const zetasql::Type* gsql_int64 = zetasql::types::Int64Type();
const zetasql::Type* gsql_string = zetasql::types::StringType();

using FindConversionOptions = ::zetasql::Catalog::FindConversionOptions;
using ConversionSourceExpressionKind =
    ::zetasql::Catalog::ConversionSourceExpressionKind;

const std::string* const kMaxPGNumericWholeDigitStr =
    new std::string(common::kMaxPGNumericWholeDigits, '9');
const std::string* const kMaxPGNumericFractionalDigitStr =
    new std::string(common::kMaxPGNumericFractionalDigits, '9');
const std::string* const kMaxPGNumericDigitStr = new std::string(
    *kMaxPGNumericWholeDigitStr + "." + *kMaxPGNumericFractionalDigitStr);

// PG Numeric max value supported for PG Jsonb
const std::string* const kMaxPgJsonbNumericWholeDigitStr =
    new std::string(common::kMaxPGJSONBNumericWholeDigits, '9');
const std::string* const kMaxPgJsonbNumericFractionalDigitStr =
    new std::string(common::kMaxPGJSONBNumericFractionalDigits, '9');
const std::string* const kMaxPgJsonbNumericDigitStr =
    new std::string(*kMaxPgJsonbNumericWholeDigitStr + "." +
                    *kMaxPgJsonbNumericFractionalDigitStr);

using ConversionPair =
    std::pair<const zetasql::Type*, const zetasql::Type*>;
using ConversionMap =
    absl::flat_hash_map<ConversionPair,
                        std::function<absl::StatusOr<zetasql::Value>(
                            const absl::Span<const zetasql::Value>)>>;

static const ConversionMap& GetConversionMap() {
  static const zetasql::Type* gsql_pg_jsonb =
      spangres::datatypes::GetPgJsonbType();
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  static const zetasql_base::NoDestructor<ConversionMap> kConversionMap({
      // PG.JSONB -> <TYPE>
      {{gsql_pg_jsonb, gsql_bool}, PgJsonbToBoolConversion},
      {{gsql_pg_jsonb, gsql_double}, PgJsonbToDoubleConversion},
      {{gsql_pg_jsonb, gsql_float}, PgJsonbToFloatConversion},
      {{gsql_pg_jsonb, gsql_int64}, PgJsonbToInt64Conversion},
      {{gsql_pg_jsonb, gsql_pg_numeric}, PgJsonbToPgNumericConversion},
      {{gsql_pg_jsonb, gsql_string}, PgJsonbToStringConversion},
  });
  return *kConversionMap;
}

static void TestConversion(
    const zetasql::Type* from, const zetasql::Type* to,
    const zetasql::Value& input,
    const std::optional<zetasql::Value>& expected_output,
    bool is_error = false, absl::StatusCode status_code = absl::StatusCode::kOk,
    std::string error_msg = "") {
  auto conversion_map = GetConversionMap();
  auto conversion_fn = conversion_map.find(ConversionPair(from, to));
  ASSERT_TRUE(conversion_fn != conversion_map.end());

  // Create the pg arena to create and compare PG.NUMERIC.
  absl::StatusOr<std::unique_ptr<postgres_translator::interfaces::PGArena>>
      pg_arena = postgres_translator::interfaces::CreatePGArena(nullptr);
  ZETASQL_EXPECT_OK(pg_arena);

  if (!is_error) {
    EXPECT_THAT(conversion_fn->second(absl::MakeConstSpan({input})),
                zetasql_base::testing::IsOkAndHolds(expected_output.value()));
  } else {
    EXPECT_THAT(
        conversion_fn->second(absl::MakeConstSpan({input})),
        zetasql_base::testing::StatusIs(status_code, testing::HasSubstr(error_msg)));
  }
}

}  // namespace

TEST(PgJsonbConversionTest, ConvertPgJsonbToBoolSuccess) {
  TestConversion(
      GetPgJsonbType(), zetasql::types::BoolType(),
      zetasql::Value::Null(
          postgres_translator::spangres::datatypes::GetPgJsonbType()),
      zetasql::Value::NullBool());
  TestConversion(GetPgJsonbType(), zetasql::types::BoolType(),
                 CreatePgJsonbValue("true").value(),
                 zetasql::Value::Bool(true));
  TestConversion(GetPgJsonbType(), zetasql::types::BoolType(),
                 CreatePgJsonbValue("false").value(),
                 zetasql::Value::Bool(false));
}

TEST(PgJsonbConversionTest, ConvertPgJsonbToBoolError) {
  std::vector<std::pair<std::string, std::string>> bool_test_cases = {
      {"\"TRUE\"", "cannot cast jsonb string to type boolean"},
      {"[1, 2]", "cannot cast jsonb array to type boolean"},
      {"{\"a\": 2}", "cannot cast jsonb object to type boolean"},
      {"123", "cannot cast jsonb numeric to type boolean"},
      {"null", "cannot cast jsonb null to type boolean"}};
  for (const auto& [input, expected_error] : bool_test_cases) {
    SCOPED_TRACE(
        absl::StrCat("input:", input, " expected_error: ", expected_error));
    TestConversion(GetPgJsonbType(), zetasql::types::BoolType(),
                   CreatePgJsonbValueWithMemoryContext(input).value(),
                   std::nullopt,
                   /* is_error= */ true, absl::StatusCode::kInvalidArgument,
                   expected_error);
  }
}

TEST(PgJsonbConversionTest, ConvertPgJsonbToInt64Success) {
  TestConversion(
      GetPgJsonbType(), zetasql::types::Int64Type(),
      zetasql::Value::Null(
          postgres_translator::spangres::datatypes::GetPgJsonbType()),
      zetasql::Value::NullInt64());
  std::vector<std::pair<std::string, int64_t>> int64_test_cases = {
      {"10", 10L},
      {"123.456", 123L},
      {"123.999", 124L},
      {"-123.456", -123L},
      {"-123.999", -124L},
      {"0.4", 0L},
      {"-0.4", 0L},
      {"0.5", 1L},
      {"-0.5", -1L},
      {"9223372036854775807", std::numeric_limits<int64_t>::max()},
      {"-9223372036854775808", std::numeric_limits<int64_t>::min()},
      {"9223372036854775807.4", std::numeric_limits<int64_t>::max()},
      {"-9223372036854775808.4", std::numeric_limits<int64_t>::min()}};
  for (const auto& [input, expected_output] : int64_test_cases) {
    SCOPED_TRACE(absl::StrCat(
        "input:", input, " expected_output:", std::to_string(expected_output)));
    TestConversion(GetPgJsonbType(), zetasql::types::Int64Type(),
                   CreatePgJsonbValueWithMemoryContext(input).value(),
                   zetasql::Value::Int64(expected_output));
  }
}

TEST(PgJsonbConversionTest, ConvertPgJsonbToInt64Error) {
  std::vector<std::pair<std::string, std::string>>
      int64_invalid_arg_test_cases = {
          {"\"abc\"", "cannot cast jsonb string to type bigint"},
          {"[1, 2]", "cannot cast jsonb array to type bigint"},
          {"{\"a\": 2}", "cannot cast jsonb object to type bigint"},
          {"true", "cannot cast jsonb boolean to type bigint"},
          {"false", "cannot cast jsonb boolean to type bigint"},
          {"null", "cannot cast jsonb null to type bigint"}};
  for (const auto& [input, expected_error] : int64_invalid_arg_test_cases) {
    SCOPED_TRACE(
        absl::StrCat("input:", input, " expected_error: ", expected_error));
    TestConversion(GetPgJsonbType(), zetasql::types::Int64Type(),
                   CreatePgJsonbValueWithMemoryContext(input).value(),
                   std::nullopt,
                   /* is_error= */ true, absl::StatusCode::kInvalidArgument,
                   expected_error);
  }

  std::vector<std::pair<std::string, std::string>>
      int64_out_of_range_test_cases = {
          {"9223372036854775807.5", "bigint out of range"},
          {"-9223372036854775808.5", "bigint out of range"},
          {"92233720368547758075.4", "bigint out of range"},
          {"-92233720368547758075.4", "bigint out of range"},
          {"-9223372036854775807534543.4", "bigint out of range"},
          {"92233720368547758075345453.4", "bigint out of range"}};
  for (const auto& [input, expected_error] : int64_out_of_range_test_cases) {
    SCOPED_TRACE(
        absl::StrCat("input:", input, " expected_error: ", expected_error));
    TestConversion(
        GetPgJsonbType(), zetasql::types::Int64Type(),
        CreatePgJsonbValueWithMemoryContext(input).value(), std::nullopt,
        /* is_error= */ true, absl::StatusCode::kOutOfRange, expected_error);
  }
}

TEST(PgJsonbConversionTest, ConvertPgJsonbToDoubleSuccess) {
  TestConversion(
      GetPgJsonbType(), zetasql::types::DoubleType(),
      zetasql::Value::Null(
          postgres_translator::spangres::datatypes::GetPgJsonbType()),
      zetasql::Value::NullDouble());
  std::vector<std::pair<std::string, double>> double_test_cases = {
      {"0", 0},
      {"0.0", 0.0},
      {"3.14", 3.14},
      {"3.14000000", 3.14},
      {"123", 123},
      {"3.14567897543568997764", 3.14567897543569},
      {"3.14567897543562524102", 3.1456789754356254},
      {"-33.12349545", -33.12349545},
      {"0.00001342", 1.342e-05},
      {"0.0000000000000000000100000000000000001", 1e-20},
      {"0.000000000000000000010000000000000001", 1.0000000000000001e-20},
      {absl::StrCat("17976931348623157", std::string(292, '0')),
       std::numeric_limits<double>::max()},
      {"1.7976931348623157E+308", 1.7976931348623157E+308},
      {"1.7976931348623158E+308", 1.7976931348623157E+308},
      {absl::StrCat("0.", std::string(307, '0'), "22250738585072014"),
       std::numeric_limits<double>::min()},
      {"2.2250738585072014e-308", 2.2250738585072014e-308},
      {"2.2250738585072017E-308", 2.225073858507202E-308},
      {"2.2250738585072014E-309", 2.225073858507203E-309},
      {absl::StrCat("-17976931348623157", std::string(292, '0')),
       std::numeric_limits<double>::lowest()},
      {"-1.7976931348623157E+308", -1.7976931348623157E+308},
      {"-1.7976931348623158E+308", -1.7976931348623157E+308},
      {absl::StrCat("-17976931348623157", std::string(292, '0'),
                    ".123456789123456789123456789"),
       -1.7976931348623157E+308}};
  for (const auto& [input, expected_output] : double_test_cases) {
    SCOPED_TRACE(absl::StrCat(
        "input:", input, " expected_output:", std::to_string(expected_output)));
    TestConversion(GetPgJsonbType(), zetasql::types::DoubleType(),
                   CreatePgJsonbValueWithMemoryContext(input).value(),
                   zetasql::Value::Double(expected_output));
  }
}

TEST(PgJsonbConversionTest, ConvertPgJsonbToDoubleError) {
  std::vector<std::pair<std::string, std::string>>
      double_invalid_arg_test_cases = {
          {"\"abc\"", "cannot cast jsonb string to type double precision"},
          {"[1, 2]", "cannot cast jsonb array to type double precision"},
          {"{\"a\": 2}", "cannot cast jsonb object to type double precision"},
          {"true", "cannot cast jsonb boolean to type double precision"},
          {"false", "cannot cast jsonb boolean to type double precision"},
          {"null", "cannot cast jsonb null to type double precision"},
          {"\"NaN\"", "cannot cast jsonb string to type double precision"},
          {"\"-Infinity\"",
           "cannot cast jsonb string to type double precision"},
          {"\"Infinity\"",
           "cannot cast jsonb string to type double precision"}};
  for (const auto& [input, expected_error] : double_invalid_arg_test_cases) {
    SCOPED_TRACE(
        absl::StrCat("input:", input, " expected_error: ", expected_error));
    TestConversion(GetPgJsonbType(), zetasql::types::DoubleType(),
                   CreatePgJsonbValueWithMemoryContext(input).value(),
                   std::nullopt,
                   /* is_error= */ true, absl::StatusCode::kInvalidArgument,
                   expected_error);
  }

  std::vector<std::pair<std::string, std::string>>
      double_out_of_range_test_cases = {
          {"1.7976931348623159E+308",
           absl::StrCat("\"", "17976931348623159", std::string(292, '0'),
                        "\" is out of range for type double "
                        "precision")},
          {"-1.7976931348623159E+308",
           absl::StrCat("\"-", "17976931348623159", std::string(292, '0'),
                        "\" is out of range for type double "
                        "precision")},
          {"1.7976931348623157E+309",
           absl::StrCat("\"", "17976931348623157", std::string(293, '0'),
                        "\" is out of range for type double "
                        "precision")},
          {"-1.7976931348623158E+309",
           absl::StrCat("\"-", "17976931348623158", std::string(293, '0'),
                        "\" is out of range for type double "
                        "precision")}};
  for (const auto& [input, expected_error] : double_out_of_range_test_cases) {
    SCOPED_TRACE(
        absl::StrCat("input:", input, " expected_error: ", expected_error));
    TestConversion(
        GetPgJsonbType(), zetasql::types::DoubleType(),
        CreatePgJsonbValueWithMemoryContext(input).value(), std::nullopt,
        /* is_error= */ true, absl::StatusCode::kOutOfRange, expected_error);
  }
}

TEST(PgJsonbConversionTest, ConvertPgJsonbToFloatSuccess) {
  TestConversion(
      GetPgJsonbType(), zetasql::types::FloatType(),
      zetasql::Value::Null(
          postgres_translator::spangres::datatypes::GetPgJsonbType()),
      zetasql::Value::NullFloat());

  std::vector<std::pair<std::string, float>> float_test_cases = {
      {"0", 0},
      {"0.0", 0.0f},
      {"3.14", 3.14f},
      {"3.14000000", 3.14f},
      {"123", 123},
      {"3.145678", 3.145678f},
      {"3.1456789", 3.145679f},
      {"-33.12349", -33.12349f},
      {"0.00001342", 1.342e-05f},
      {"0.00000000000000000001000000001", 1e-20f},
      {"0.0000000000000000000100000001", 1.00000001e-20f},

      {absl::StrCat("34028235", std::string(31, '0')),
       std::numeric_limits<float>::max()},
      {absl::StrCat("-34028235", std::string(31, '0')),
       std::numeric_limits<float>::lowest()},
      {absl::StrCat("0.", std::string(37, '0'), "11754944"),
       std::numeric_limits<float>::min()},

      {"3.4028233e+38", 3.4028233e+38f},
      {"3.4028234e+38", 3.4028235e+38f},
      {"3.4028235e+38", 3.4028235e+38f},

      {"1.1754943e-38", 1.1754944e-38f},
      {"1.1754944e-38", 1.1754944e-38f},
      {"1.1754945e-38", 1.1754945e-38f},

      {"-3.4028233e+38", -3.4028233e+38f},
      {"-3.4028234e+38", -3.4028235e+38f},
      {"-3.4028235e+38", -3.4028235e+38f},
  };
  for (const auto& [input, expected_output] : float_test_cases) {
    SCOPED_TRACE(absl::StrCat(
        "input:", input, " expected_output:", std::to_string(expected_output)));
    TestConversion(GetPgJsonbType(), zetasql::types::FloatType(),
                   CreatePgJsonbValueWithMemoryContext(input).value(),
                   zetasql::Value::Float(expected_output));
  }
}

TEST(PgJsonbConversionTest, ConvertPgJsonbToFloatError) {
  std::vector<std::pair<std::string, std::string>>
      float_invalid_arg_test_cases = {
          {"\"abc\"", "cannot cast jsonb string to type real"},
          {"[1, 2]", "cannot cast jsonb array to type real"},
          {"{\"a\": 2}", "cannot cast jsonb object to type real"},
          {"true", "cannot cast jsonb boolean to type real"},
          {"false", "cannot cast jsonb boolean to type real"},
          {"null", "cannot cast jsonb null to type real"},
          {"\"NaN\"", "cannot cast jsonb string to type real"},
          {"\"-Infinity\"", "cannot cast jsonb string to type real"},
          {"\"Infinity\"", "cannot cast jsonb string to type real"},
      };
  for (const auto& [input, expected_error] : float_invalid_arg_test_cases) {
    SCOPED_TRACE(
        absl::StrCat("input:", input, " expected_error: ", expected_error));
    TestConversion(GetPgJsonbType(), zetasql::types::FloatType(),
                   CreatePgJsonbValueWithMemoryContext(input).value(),
                   std::nullopt,
                   /* is_error= */ true, absl::StatusCode::kInvalidArgument,
                   expected_error);
  }

  std::vector<std::pair<std::string, std::string>>
      float_out_of_range_test_cases = {
          {"3.4028236e+38", absl::StrCat("\"34028236", std::string(31, '0'),
                                         "\" is out of range for type real")},
          {"3.4028235e+39", absl::StrCat("\"34028235", std::string(32, '0'),
                                         "\" is out of range for type real")},
          {"-3.4028236e+38", absl::StrCat("\"-34028236", std::string(31, '0'),
                                          "\" is out of range for type real")}};
  for (const auto& [input, expected_error] : float_out_of_range_test_cases) {
    SCOPED_TRACE(
        absl::StrCat("input:", input, " expected_error: ", expected_error));
    TestConversion(
        GetPgJsonbType(), zetasql::types::FloatType(),
        CreatePgJsonbValueWithMemoryContext(input).value(), std::nullopt,
        /* is_error= */ true, absl::StatusCode::kOutOfRange, expected_error);
  }
}

TEST(PgJsonbConversionTest, ConvertPgJsonbToPgNumericSuccess) {
  TestConversion(GetPgJsonbType(), GetPgNumericType(),
                 zetasql::Value::Null(GetPgJsonbType()),
                 zetasql::values::Null(GetPgNumericType()));

  std::vector<std::string> pg_numeric_test_cases = {
      "0", absl::StrCat("-", *kMaxPgJsonbNumericDigitStr),
      *kMaxPgJsonbNumericDigitStr, "0.0000000001230"};
  for (const std::string& input : pg_numeric_test_cases) {
    SCOPED_TRACE(absl::StrCat("input:", input, " expected_output:", input));
    TestConversion(GetPgJsonbType(), GetPgNumericType(),
                   CreatePgJsonbValueWithMemoryContext(input).value(),
                   CreatePgNumericValueWithMemoryContext(input).value());
  }
}

TEST(PgJsonbConversionTest, ConvertPgJsonbToPgNumericError) {
  std::vector<std::pair<std::string, std::string>> pg_numeric_test_cases = {
      {"\"abc\"", "cannot cast jsonb string to type numeric"},
      {"[1, 2]", "cannot cast jsonb array to type numeric"},
      {"{\"a\": 2}", "cannot cast jsonb object to type numeric"},
      {"true", "cannot cast jsonb boolean to type numeric"},
      {"false", "cannot cast jsonb boolean to type numeric"},
      {"null", "cannot cast jsonb null to type numeric"},
      {"\"NaN\"", "cannot cast jsonb string to type numeric"}};
  for (const auto& [input, expected_error] : pg_numeric_test_cases) {
    SCOPED_TRACE(
        absl::StrCat("input:", input, " expected_error: ", expected_error));
    TestConversion(GetPgJsonbType(), GetPgNumericType(),
                   CreatePgJsonbValueWithMemoryContext(input).value(),
                   std::nullopt,
                   /* is_error= */ true, absl::StatusCode::kInvalidArgument,
                   expected_error);
  }
}

TEST(PgJsonbConversionTest, ConvertPgJsonbToString) {
  TestConversion(
      GetPgJsonbType(), zetasql::types::StringType(),
      zetasql::Value::Null(
          postgres_translator::spangres::datatypes::GetPgJsonbType()),
      zetasql::Value::NullString());
  std::vector<std::string> string_test_cases = {
      {"\"hello\""},
      {"\"special characters(', \\\", \\r, \\n)\""},
      {"\"non ascii characters(ß, Д, \\u0001)\""},
      {"\"\""},
      {R"("例子")"},
      {"\"{\\\"a\\\":      1}\""},
      {"123"},
      {"[1, 2]"},
      {"{\"a\": 2}"},
      {"true"},
      {"false"},
      {"null"}};
  for (const std::string& input : string_test_cases) {
    SCOPED_TRACE(absl::StrCat("input:", input, " expected_output:", input));
    TestConversion(GetPgJsonbType(), zetasql::types::StringType(),
                   CreatePgJsonbValue(input).value(),
                   zetasql::Value::String(input));
  }
}

}  // namespace datatypes
}  // namespace postgres_translator::spangres

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
