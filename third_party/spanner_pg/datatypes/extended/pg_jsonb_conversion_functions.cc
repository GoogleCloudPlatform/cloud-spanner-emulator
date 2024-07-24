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

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <iterator>
#include <limits>
#include <string>

#include "zetasql/public/function.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/numbers.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/catalog/emulator_function_evaluators.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_numeric_type.h"

namespace postgres_translator::spangres {
namespace datatypes {

static absl::Status GetPgJsonbCastErrorMessage(char first_char,
                                               absl::string_view type) {
  switch (first_char) {
    case '{':
      return absl::InvalidArgumentError(
          absl::StrCat("cannot cast jsonb object to type ", type));
    case '[':
      return absl::InvalidArgumentError(
          absl::StrCat("cannot cast jsonb array to type ", type));
    case '"':
      return absl::InvalidArgumentError(
          absl::StrCat("cannot cast jsonb string to type ", type));
    case 't':
    case 'f':
      return absl::InvalidArgumentError(
          absl::StrCat("cannot cast jsonb boolean to type ", type));
    case 'n':
      return absl::InvalidArgumentError(
          absl::StrCat("cannot cast jsonb null to type ", type));
    default:
      return absl::InvalidArgumentError(
          absl::StrCat("cannot cast jsonb numeric to type ", type));
  }
}

absl::StatusOr<bool> GetBoolFromPgJsonb(const absl::Cord& jsonb) {
  ZETASQL_RET_CHECK_GT(jsonb.size(), 0);
  if (jsonb == absl::string_view("true")) return true;
  if (jsonb == absl::string_view("false")) return false;
  return GetPgJsonbCastErrorMessage(jsonb[0], "boolean");
}

absl::StatusOr<int64_t> GetInt64FromPgJsonb(const absl::Cord& jsonb) {
  ZETASQL_RET_CHECK_GT(jsonb.size(), 0);
  const char first_char = jsonb[0];
  if (!std::isdigit(first_char) && first_char != '-') {
    return GetPgJsonbCastErrorMessage(first_char, "bigint");
  }
  // We only need first 22 characters (2(-,.) + 19(integer digits for max/min
  // values) + 1(1st fractional digit or additional digit for out of range
  // values)) of the jsonb string to convert it to an integer
  constexpr int32_t kMaxInt64DigitsLength = 22;
  std::array<char, kMaxInt64DigitsLength> buffer;
  int i;
  for (i = 0; i < jsonb.size() && i < kMaxInt64DigitsLength; ++i) {
    buffer[i] = jsonb[i];
  }
  const int number_size = i;
  int whole_part_size = number_size;
  const auto it =
      std::find(buffer.cbegin(), buffer.cbegin() + number_size, '.');
  if (it != buffer.cend()) {
    whole_part_size = std::distance(buffer.cbegin(), it);
  }
  absl::string_view whole_part(buffer.data(), whole_part_size);
  int64_t result;
  if (!absl::SimpleAtoi(whole_part, &result)) {
    return absl::OutOfRangeError(absl::StrCat("bigint out of range"));
  }
  if (whole_part_size == number_size) return result;

  const char fractional_part = buffer[whole_part_size + 1];
  ZETASQL_RET_CHECK(std::isdigit(fractional_part));
  // If the first digit after decimal is greater than 4 then we round the
  // integer part down if it is a negative number else round the integer part
  // up.
  if (fractional_part > '4') {
    if (result == std::numeric_limits<int64_t>::max() ||
        result == std::numeric_limits<int64_t>::min()) {
      return absl::OutOfRangeError(absl::StrCat("bigint out of range"));
    }
    if (first_char == '-') {
      result--;
    } else {
      result++;
    }
  }
  return result;
}

absl::Status OutOfRangeForFloatingPointError(absl::string_view input,
                                             absl::string_view pg_type_name) {
  // We expect that the input value should come from already processed JSON,
  // thus it should be valid UTF-8 string and we don't need to do any additional
  // escaping using functions like EnsureValidUTF8. However, we don't want to
  // return too long error message to a customer, so if the value is very long
  // we try to strip it.
  static constexpr size_t kMaxValueSizeInErrorMessage = 1000;
  absl::string_view terminator{};
  if (input.size() > kMaxValueSizeInErrorMessage) {
    input = input.substr(0, kMaxValueSizeInErrorMessage);
    terminator = "...";
  }
  return absl::OutOfRangeError(absl::StrCat(
      "\"", input, terminator, "\" is out of range for type ", pg_type_name));
}

template <typename T, typename = std::enable_if_t<std::is_floating_point_v<T>>>
inline absl::StatusOr<T> ParseFloatingPoint(absl::string_view input) {
  T result;
  bool is_success;
  if constexpr (std::is_same_v<T, double>) {
    is_success = absl::SimpleAtod(input, &result);
  } else {
    is_success = absl::SimpleAtof(input, &result);
  }
  if (ABSL_PREDICT_FALSE(!is_success || std::isnan(result) ||
                         std::isinf(result))) {
    return OutOfRangeForFloatingPointError(
        input, std::is_same_v<T, double> ? "double precision" : "real");
  }

  return result;
}

template <typename T, typename = std::enable_if_t<std::is_floating_point_v<T>>>
absl::StatusOr<T> GetFloatingPointFromPgJsonb(const absl::Cord& jsonb) {
  ZETASQL_RET_CHECK_GT(jsonb.size(), 0);

  // Check that value is actually a number.
  const char first_char = jsonb[0];
  if (ABSL_PREDICT_FALSE(!std::isdigit(first_char) && first_char != '-')) {
    return GetPgJsonbCastErrorMessage(
        first_char, std::is_same_v<T, double> ? "double precision" : "real");
  }

  // T == FLOAT:
  // We only need first 47 characters (3(-,0,.) + FLT_DIG(6) +
  // FLT_MIN_10_EXP(37) + 1 additional character for detecting out of range
  // values) of the jsonb string to convert it to a float.
  // T == DOUBLE:
  // We only need first 326 characters (3(-,0,.) + DBL_DIG(15) +
  // DBL_MIN_10_EXP(307) + 1 additional character for detecting out of range
  // values) of the jsonb string to convert it to a double.
  constexpr size_t kMaxFloatingPointDigitsLength =
      std::is_same_v<T, double> ? 326 : 47;

  std::array<char, kMaxFloatingPointDigitsLength> buffer;
  int i;
  for (i = 0; i < jsonb.size() && i < kMaxFloatingPointDigitsLength; ++i) {
    buffer[i] = jsonb[i];
  }
  const size_t number_size = i;
  std::string_view input = absl::string_view{buffer.data(), number_size};
  return ParseFloatingPoint<T>(input);
}

absl::StatusOr<zetasql::Value> PgJsonbToBoolConversion(
    const absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK_EQ(args.size(), 1);
  if (args[0].is_null()) {
    return zetasql::Value::NullBool();
  }
  ZETASQL_ASSIGN_OR_RETURN(absl::Cord jsonb, GetPgJsonbNormalizedValue(args[0]));
  ZETASQL_ASSIGN_OR_RETURN(bool bool_value, GetBoolFromPgJsonb(jsonb));
  return zetasql::Value::Bool(bool_value);
}

absl::StatusOr<zetasql::Value> PgJsonbToInt64Conversion(
    const absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK_EQ(args.size(), 1);
  if (args[0].is_null()) {
    return zetasql::Value::NullInt64();
  }
  ZETASQL_ASSIGN_OR_RETURN(absl::Cord jsonb, GetPgJsonbNormalizedValue(args[0]));
  ZETASQL_ASSIGN_OR_RETURN(int64_t rounded, GetInt64FromPgJsonb(jsonb));
  return zetasql::Value::Int64(rounded);
}

absl::StatusOr<zetasql::Value> PgJsonbToDoubleConversion(
    const absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK_EQ(args.size(), 1);
  if (args[0].is_null()) {
    return zetasql::Value::NullDouble();
  }
  ZETASQL_ASSIGN_OR_RETURN(absl::Cord jsonb, GetPgJsonbNormalizedValue(args[0]));
  ZETASQL_ASSIGN_OR_RETURN(double d, GetFloatingPointFromPgJsonb<double>(jsonb));
  return zetasql::Value::Double(d);
}

absl::StatusOr<zetasql::Value> PgJsonbToFloatConversion(
    const absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK_EQ(args.size(), 1);
  if (args[0].is_null()) {
    return zetasql::Value::NullFloat();
  }
  ZETASQL_ASSIGN_OR_RETURN(absl::Cord jsonb, GetPgJsonbNormalizedValue(args[0]));
  ZETASQL_ASSIGN_OR_RETURN(float f, GetFloatingPointFromPgJsonb<float>(jsonb));
  return zetasql::Value::Float(f);
}

absl::StatusOr<zetasql::Value> PgJsonbToPgNumericConversion(
    const absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK_EQ(args.size(), 1);
  if (args[0].is_null()) {
    return zetasql::values::Null(GetPgNumericType());
  }
  ZETASQL_ASSIGN_OR_RETURN(absl::Cord jsonb, GetPgJsonbNormalizedValue(args[0]));
  ZETASQL_RET_CHECK_GT(jsonb.size(), 0);
  const char first_char = jsonb[0];
  if (!std::isdigit(first_char) && first_char != '-') {
    return GetPgJsonbCastErrorMessage(first_char, "numeric");
  }
  return CreatePgNumericValue(std::string(jsonb));
}

absl::StatusOr<zetasql::Value> PgJsonbToStringConversion(
    const absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK_EQ(args.size(), 1);
  if (args[0].is_null()) {
    return zetasql::Value::NullString();
  }
  ZETASQL_ASSIGN_OR_RETURN(absl::Cord jsonb, GetPgJsonbNormalizedValue(args[0]));
  return zetasql::Value::String(jsonb);
}

const zetasql::Function* GetStringToPgJsonbConversion() {
  static const zetasql::Function* kStringToPgJsonbConv =
      new zetasql::Function(
          "string_to_pg_jsonb_conv", "spanner", zetasql::Function::SCALAR,
          /*function_signatures=*/{},
          zetasql::FunctionOptions().set_evaluator(
              [](const absl::Span<const zetasql::Value> args)
                  -> absl::StatusOr<zetasql::Value> {
                ZETASQL_RET_CHECK_EQ(args.size(), 1);
                return datatypes::CreatePgJsonbValueWithMemoryContext(
                    args[0].string_value());
              }));

  return kStringToPgJsonbConv;
}

const zetasql::Function* GetPgJsonbToBoolConversion() {
  static const zetasql::Function* kPgJsonbToBoolConv =
      new zetasql::Function(
          "pg_jsonb_to_bool_conv", "spanner", zetasql::Function::SCALAR,
          /*function_signatures=*/{},
          zetasql::FunctionOptions().set_evaluator(
              PGFunctionEvaluator(PgJsonbToBoolConversion)));
  return kPgJsonbToBoolConv;
}

const zetasql::Function* GetPgJsonbToInt64Conversion() {
  static const zetasql::Function* kPgJsonbToInt64Conv =
      new zetasql::Function(
          "pg_jsonb_to_int64_conv", "spanner", zetasql::Function::SCALAR,
          /*function_signatures=*/{},
          zetasql::FunctionOptions().set_evaluator(
              PGFunctionEvaluator(PgJsonbToInt64Conversion)));
  return kPgJsonbToInt64Conv;
}

const zetasql::Function* GetPgJsonbToDoubleConversion() {
  static const zetasql::Function* kPgJsonbToDoubleConv =
      new zetasql::Function(
          "pg_jsonb_to_double_conv", "spanner", zetasql::Function::SCALAR,
          /*function_signatures=*/{},
          zetasql::FunctionOptions().set_evaluator(
              PGFunctionEvaluator(PgJsonbToDoubleConversion)));
  return kPgJsonbToDoubleConv;
}

const zetasql::Function* GetPgJsonbToFloatConversion() {
  static const zetasql::Function* kPgJsonbToFloatConv =
      new zetasql::Function(
          "pg_jsonb_to_float_conv", "spanner", zetasql::Function::SCALAR,
          /*function_signatures=*/{},
          zetasql::FunctionOptions().set_evaluator(
              PGFunctionEvaluator(PgJsonbToFloatConversion)));
  return kPgJsonbToFloatConv;
}

const zetasql::Function* GetPgJsonbToPgNumericConversion() {
  static const zetasql::Function* kPgJsonbToPgNumericConv =
      new zetasql::Function(
          "pg_jsonb_to_pg_numeric_conv", "spanner", zetasql::Function::SCALAR,
          /*function_signatures=*/{},
          zetasql::FunctionOptions().set_evaluator(
              PGFunctionEvaluator(PgJsonbToPgNumericConversion)));
  return kPgJsonbToPgNumericConv;
}

const zetasql::Function* GetPgJsonbToStringConversion() {
  static const zetasql::Function* kPgJsonbToStringConv =
      new zetasql::Function(
          "pg_jsonb_to_string_conv", "spanner", zetasql::Function::SCALAR,
          /*function_signatures=*/{},
          zetasql::FunctionOptions().set_evaluator(
              PGFunctionEvaluator(PgJsonbToStringConversion)));
  return kPgJsonbToStringConv;
}

}  // namespace datatypes
}  // namespace postgres_translator::spangres
