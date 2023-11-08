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

#include "third_party/spanner_pg/catalog/emulator_functions.h"

#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/function.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/functions/arithmetics.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "absl/base/casts.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "zetasql/common/string_util.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "third_party/spanner_pg/catalog/emulator_function_evaluators.h"
#include "third_party/spanner_pg/datatypes/common/jsonb/jsonb_parse.h"
#include "third_party/spanner_pg/datatypes/common/pg_numeric_parse.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_numeric_type.h"
#include "third_party/spanner_pg/datatypes/extended/spanner_extended_type.h"
#include "third_party/spanner_pg/interface/cast_evaluators.h"
#include "third_party/spanner_pg/interface/datetime_evaluators.h"
#include "third_party/spanner_pg/interface/formatting_evaluators.h"
#include "third_party/spanner_pg/interface/jsonb_evaluators.h"
#include "third_party/spanner_pg/interface/mathematical_evaluators.h"
#include "third_party/spanner_pg/interface/pg_arena.h"
#include "third_party/spanner_pg/interface/pg_arena_factory.h"
#include "third_party/spanner_pg/interface/regexp_evaluators.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {

namespace {

using postgres_translator::spangres::datatypes::GetPgNumericNormalizedValue;
using spangres::datatypes::CreatePgJsonbValue;
using spangres::datatypes::CreatePgNumericValue;
using spangres::datatypes::CreatePgNumericValueWithPrecisionAndScale;
using spangres::datatypes::GetPgJsonbNormalizedValue;
using spangres::datatypes::GetPgNumericNormalizedValue;
using spangres::datatypes::common::jsonb::IsValidJsonbString;
using spangres::datatypes::common::jsonb::NormalizeJsonbString;

const zetasql::Type* gsql_bool = zetasql::types::BoolType();
const zetasql::Type* gsql_bytes = zetasql::types::BytesType();
const zetasql::Type* gsql_date = zetasql::types::DateType();
const zetasql::Type* gsql_double = zetasql::types::DoubleType();
const zetasql::Type* gsql_int32 = zetasql::types::Int32Type();
const zetasql::Type* gsql_int64 = zetasql::types::Int64Type();
const zetasql::Type* gsql_string = zetasql::types::StringType();
const zetasql::Type* gsql_timestamp = zetasql::types::TimestampType();

const zetasql::ArrayType* gsql_bool_array = zetasql::types::BoolArrayType();
const zetasql::ArrayType* gsql_bytes_array =
    zetasql::types::BytesArrayType();
const zetasql::ArrayType* gsql_date_array = zetasql::types::DateArrayType();
const zetasql::ArrayType* gsql_double_array =
    zetasql::types::DoubleArrayType();
const zetasql::ArrayType* gsql_int64_array =
    zetasql::types::Int64ArrayType();
const zetasql::ArrayType* gsql_string_array =
    zetasql::types::StringArrayType();
const zetasql::ArrayType* gsql_timestamp_array =
    zetasql::types::TimestampArrayType();

constexpr char kNan[] = "NaN";
constexpr char kNanString[] = "\"NaN\"";
constexpr char kInfString[] = "\"Infinity\"";
constexpr char kNegInfString[] = "\"-Infinity\"";
constexpr char kFalse[] = "false";
constexpr char kTrue[] = "true";

using ::postgres_translator::EmulatorJsonBArrayElementText;
using ::postgres_translator::EmulatorJsonBObjectFieldText;
using ::postgres_translator::function_evaluators::Abs;
using ::postgres_translator::function_evaluators::Add;
using ::postgres_translator::function_evaluators::CastNumericToInt8;
using ::postgres_translator::function_evaluators::Ceil;
using ::postgres_translator::function_evaluators::CleanupPostgresDateTimeCache;
using ::postgres_translator::function_evaluators::CleanupPostgresNumberCache;
using ::postgres_translator::function_evaluators::CleanupRegexCache;
using ::postgres_translator::function_evaluators::DateMii;
using ::postgres_translator::function_evaluators::DatePli;
using ::postgres_translator::function_evaluators::Divide;
using ::postgres_translator::function_evaluators::DivideTruncateTowardsZero;
using ::postgres_translator::function_evaluators::Float8ToChar;
using ::postgres_translator::function_evaluators::Floor;
using ::postgres_translator::function_evaluators::Int8ToChar;
using ::postgres_translator::function_evaluators::JsonBArrayElement;
using ::postgres_translator::function_evaluators::JsonBObjectField;
using ::postgres_translator::function_evaluators::JsonBTypeof;
using ::postgres_translator::function_evaluators::Mod;
using ::postgres_translator::function_evaluators::Multiply;
using ::postgres_translator::function_evaluators::NumericToChar;
using ::postgres_translator::function_evaluators::NumericToNumber;
using ::postgres_translator::function_evaluators::PgTimestampTzToChar;
using ::postgres_translator::function_evaluators::PgToDate;
using ::postgres_translator::function_evaluators::RegexpMatch;
using ::postgres_translator::function_evaluators::RegexpSplitToArray;
using ::postgres_translator::function_evaluators::Subtract;
using ::postgres_translator::function_evaluators::Textregexne;
using ::postgres_translator::function_evaluators::Textregexsubstr;
using ::postgres_translator::function_evaluators::ToTimestamp;
using ::postgres_translator::function_evaluators::Trunc;
using ::postgres_translator::function_evaluators::UnaryMinus;

// PG array functions

// Used by both array_upper and array_length because they return the same
// result for one-dimensional arrays which have the default lower bound of 1.
// This is the only type of array spangres supports.
absl::StatusOr<zetasql::Value> EvalArrayUpper(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return zetasql::Value::NullInt64();
  }

  // Zero or negative dimensions return NULL
  // Empty arrays return NULL
  if (args[1].int64_value() <= 0 || args[0].num_elements() == 0) {
    return zetasql::Value::NullInt64();
  }

  if (args[1].int64_value() > 1) {
    return absl::InvalidArgumentError(
        "multi-dimensional arrays are not supported");
  }

  return zetasql::Value::Int64(args[0].num_elements());
}

// Used by both array_upper and array_length because they both have similar
// signatures and behavior for one-dimensional arrays which have the default
// lower bound (i.e., starting index) of 1. This is the only type of array
// spangres supports.
std::unique_ptr<zetasql::Function> ArrayUpperFunction(
    absl::string_view catalog_name, absl::string_view function_name) {
  const zetasql::Type* gsql_pg_jsonb_array =
      postgres_translator::spangres::datatypes::GetPgJsonbArrayType();
  const zetasql::Type* gsql_pg_numeric_array =
      postgres_translator::spangres::datatypes::GetPgNumericArrayType();

  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(zetasql::FunctionEvaluator(EvalArrayUpper));
  return std::make_unique<zetasql::Function>(
      function_name, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_int64,
                                       {gsql_bool_array, gsql_int64},
                                       /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{gsql_int64,
                                       {gsql_bytes_array, gsql_int64},
                                       /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{gsql_int64,
                                       {gsql_date_array, gsql_int64},
                                       /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{gsql_int64,
                                       {gsql_double_array, gsql_int64},
                                       /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{gsql_int64,
                                       {gsql_int64_array, gsql_int64},
                                       /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{gsql_int64,
                                       {gsql_pg_jsonb_array, gsql_int64},
                                       /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{gsql_int64,
                                       {gsql_pg_numeric_array, gsql_int64},
                                       /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{gsql_int64,
                                       {gsql_string_array, gsql_int64},
                                       /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{gsql_int64,
                                       {gsql_timestamp_array, gsql_int64},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

// PG comparison functions
absl::StatusOr<zetasql::Value> EvalTextregexne(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return zetasql::Value::NullBool();
  }

  ZETASQL_ASSIGN_OR_RETURN(bool result,
                   Textregexne(args[0].string_value(), args[1].string_value()));
  return zetasql::Value::Bool(result);
}

std::unique_ptr<zetasql::Function> TextregexneFunction(
    absl::string_view catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      PGFunctionEvaluator(EvalTextregexne, CleanupRegexCache));
  return std::make_unique<zetasql::Function>(
      kPGTextregexneFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          gsql_bool, {gsql_string, gsql_string}, /*context_ptr=*/nullptr}},
      function_options);
}

// PG datetime functions
absl::StatusOr<zetasql::Value> EvalDateMi(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return zetasql::Value::NullInt64();
  }

  return zetasql::Value::Int64(args[0].date_value() - args[1].date_value());
}

std::unique_ptr<zetasql::Function> DateMiFunction(
    absl::string_view catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(zetasql::FunctionEvaluator(EvalDateMi));
  return std::make_unique<zetasql::Function>(
      kPGDateMiFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          gsql_int64, {gsql_date, gsql_date}, /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalDateMii(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return zetasql::Value::NullDate();
  }

  ZETASQL_ASSIGN_OR_RETURN(int32_t result,
                   DateMii(args[0].date_value(), args[1].int64_value()));

  return zetasql::Value::Date(result);
}

std::unique_ptr<zetasql::Function> DateMiiFunction(
    absl::string_view catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalDateMii));
  return std::make_unique<zetasql::Function>(
      kPGDateMiiFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          gsql_date, {gsql_date, gsql_int64}, /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalDatePli(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return zetasql::Value::NullDate();
  }

  ZETASQL_ASSIGN_OR_RETURN(int32_t result,
                   DatePli(args[0].date_value(), args[1].int64_value()));

  return zetasql::Value::Date(result);
}

std::unique_ptr<zetasql::Function> DatePliFunction(
    absl::string_view catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalDatePli));
  return std::make_unique<zetasql::Function>(
      kPGDatePliFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          gsql_date, {gsql_date, gsql_int64}, /*context_ptr=*/nullptr}},
      function_options);
}

// PG formatting functions
absl::StatusOr<zetasql::Value> EvalToDate(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return zetasql::Value::NullDate();
  }

  ZETASQL_ASSIGN_OR_RETURN(int32_t result,
                   PgToDate(args[0].string_value(), args[1].string_value()));

  return zetasql::Value::Date(result);
}

std::unique_ptr<zetasql::Function> ToDateFunction(
    absl::string_view catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      PGFunctionEvaluator(EvalToDate, CleanupPostgresDateTimeCache));
  return std::make_unique<zetasql::Function>(
      kPGToDateFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          gsql_date, {gsql_string, gsql_string}, /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalToTimestamp(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return zetasql::Value::NullTimestamp();
  }

  ZETASQL_ASSIGN_OR_RETURN(absl::Time result,
                   ToTimestamp(args[0].string_value(), args[1].string_value()));

  return zetasql::Value::Timestamp(result);
}

std::unique_ptr<zetasql::Function> ToTimestampFunction(
    absl::string_view catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      PGFunctionEvaluator(EvalToTimestamp, CleanupPostgresDateTimeCache));
  return std::make_unique<zetasql::Function>(
      kPGToTimestampFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          gsql_timestamp, {gsql_string, gsql_string}, /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalToChar(
    absl::Span<const zetasql::Value> args) {
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();
  ZETASQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return zetasql::Value::NullString();
  }

  switch (args[0].type_kind()) {
    case zetasql::TYPE_INT64: {
      ZETASQL_ASSIGN_OR_RETURN(std::string result, Int8ToChar(args[0].int64_value(),
                                                      args[1].string_value()));
      return zetasql::Value::String(result);
    }
    case zetasql::TYPE_TIMESTAMP: {
      ZETASQL_ASSIGN_OR_RETURN(
          std::unique_ptr<std::string> result,
          PgTimestampTzToChar(args[0].ToTime(), args[1].string_value()));
      if (result == nullptr) {
        return zetasql::Value::NullString();
      } else {
        return zetasql::Value::String(*result);
      }
    }
    case zetasql::TYPE_DOUBLE: {
      ZETASQL_ASSIGN_OR_RETURN(
          std::string result,
          Float8ToChar(args[0].double_value(), args[1].string_value()));
      return zetasql::Value::String(result);
    }
    case zetasql::TYPE_EXTENDED:
      if (args[0].type()->Equals(gsql_pg_numeric)) {
        ZETASQL_ASSIGN_OR_RETURN(absl::Cord numeric_string,
                         GetPgNumericNormalizedValue(args[0]));
        ZETASQL_ASSIGN_OR_RETURN(
            std::string result,
            NumericToChar(std::string(numeric_string), args[1].string_value()));
        return zetasql::Value::String(result);
      }
      [[fallthrough]];
    default:
      return absl::UnimplementedError(
          absl::StrCat("to_char(", args[0].type()->DebugString(), ", text)"));
  }
}

std::unique_ptr<zetasql::Function> ToCharFunction(
    absl::string_view catalog_name) {
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalToChar, [] {
    CleanupPostgresNumberCache();
    CleanupPostgresDateTimeCache();
  }));
  return std::make_unique<zetasql::Function>(
      kPGToCharFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{
              gsql_string, {gsql_int64, gsql_string}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{gsql_string,
                                       {gsql_timestamp, gsql_string},
                                       /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{gsql_string,
                                       {gsql_double, gsql_string},
                                       /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{gsql_string,
                                       {gsql_pg_numeric, gsql_string},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalToNumber(
    absl::Span<const zetasql::Value> args) {
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();
  ZETASQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return zetasql::Value::Null(gsql_pg_numeric);
  }

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<std::string> result,
      NumericToNumber(args[0].string_value(), args[1].string_value()));

  if (result == nullptr) {
    return zetasql::Value::Null(gsql_pg_numeric);
  }
  return CreatePgNumericValue(*result);
}

std::unique_ptr<zetasql::Function> ToNumberFunction(
    absl::string_view catalog_name) {
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      PGFunctionEvaluator(EvalToNumber, CleanupPostgresNumberCache));

  return std::make_unique<zetasql::Function>(
      kPGToNumberFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{{gsql_pg_numeric,
                                                 {gsql_string, gsql_string},
                                                 /*context_ptr=*/nullptr}},
      function_options);
}

// PG.NUMERIC Mathematical functions

absl::StatusOr<zetasql::Value> EvalNumericAbs(
    absl::Span<const zetasql::Value> args) {
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  ZETASQL_RET_CHECK(args.size() == 1);
  if (args[0].is_null()) {
    return zetasql::Value::Null(gsql_pg_numeric);
  }
  ZETASQL_ASSIGN_OR_RETURN(absl::Cord normalized_value,
                   GetPgNumericNormalizedValue(args[0]));

  ZETASQL_ASSIGN_OR_RETURN(std::string result, Abs(std::string(normalized_value)));

  return CreatePgNumericValue(result);
}

std::unique_ptr<zetasql::Function> NumericAbsFunction(
    absl::string_view catalog_name) {
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalNumericAbs));
  return std::make_unique<zetasql::Function>(
      kPGNumericAbsFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalNumericAdd(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return zetasql::Value::Null(
        postgres_translator::spangres::datatypes::GetPgNumericType());
  }

  ZETASQL_ASSIGN_OR_RETURN(absl::Cord lhs, GetPgNumericNormalizedValue(args[0]));
  ZETASQL_ASSIGN_OR_RETURN(absl::Cord rhs, GetPgNumericNormalizedValue(args[1]));
  ZETASQL_ASSIGN_OR_RETURN(std::string result, Add(std::string(lhs), std::string(rhs)));

  return CreatePgNumericValue(result);
}

std::unique_ptr<zetasql::Function> NumericAddFunction(
    absl::string_view catalog_name) {
  const zetasql::Type* gsql_pg_numeric =
      postgres_translator::spangres::datatypes::GetPgNumericType();
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalNumericAdd));

  return std::make_unique<zetasql::Function>(
      kPGNumericAddFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          {gsql_pg_numeric,
           {gsql_pg_numeric, gsql_pg_numeric},
           /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalNumericCeil(
    absl::Span<const zetasql::Value> args) {
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  ZETASQL_RET_CHECK(args.size() == 1);
  if (args[0].is_null()) {
    return zetasql::Value::Null(gsql_pg_numeric);
  }
  ZETASQL_ASSIGN_OR_RETURN(absl::Cord normalized_value,
                   GetPgNumericNormalizedValue(args[0]));

  ZETASQL_ASSIGN_OR_RETURN(std::string result, Ceil(std::string(normalized_value)));

  return CreatePgNumericValue(result);
}

std::unique_ptr<zetasql::Function> NumericCeilFunction(
    absl::string_view catalog_name) {
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalNumericCeil));
  return std::make_unique<zetasql::Function>(
      kPGNumericCeilFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr}},
      function_options);
}

std::unique_ptr<zetasql::Function> NumericCeilingFunction(
    absl::string_view catalog_name) {
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalNumericCeil));
  return std::make_unique<zetasql::Function>(
      kPGNumericCeilingFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalNumericDivide(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return zetasql::Value::Null(
        postgres_translator::spangres::datatypes::GetPgNumericType());
  }

  ZETASQL_ASSIGN_OR_RETURN(absl::Cord lhs, GetPgNumericNormalizedValue(args[0]));
  ZETASQL_ASSIGN_OR_RETURN(absl::Cord rhs, GetPgNumericNormalizedValue(args[1]));
  ZETASQL_ASSIGN_OR_RETURN(std::string result,
                   Divide(std::string(lhs), std::string(rhs)));

  return CreatePgNumericValue(result);
}

std::unique_ptr<zetasql::Function> NumericDivideFunction(
    absl::string_view catalog_name) {
  const zetasql::Type* gsql_pg_numeric =
      postgres_translator::spangres::datatypes::GetPgNumericType();
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalNumericDivide));

  return std::make_unique<zetasql::Function>(
      kPGNumericDivideFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          {gsql_pg_numeric,
           {gsql_pg_numeric, gsql_pg_numeric},
           /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalNumericDivTrunc(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return zetasql::Value::Null(
        postgres_translator::spangres::datatypes::GetPgNumericType());
  }

  ZETASQL_ASSIGN_OR_RETURN(absl::Cord lhs, GetPgNumericNormalizedValue(args[0]));
  ZETASQL_ASSIGN_OR_RETURN(absl::Cord rhs, GetPgNumericNormalizedValue(args[1]));
  ZETASQL_ASSIGN_OR_RETURN(std::string result, DivideTruncateTowardsZero(
                                           std::string(lhs), std::string(rhs)));

  return CreatePgNumericValue(result);
}

std::unique_ptr<zetasql::Function> NumericDivTruncFunction(
    absl::string_view catalog_name) {
  const zetasql::Type* gsql_pg_numeric =
      postgres_translator::spangres::datatypes::GetPgNumericType();
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalNumericDivTrunc));

  return std::make_unique<zetasql::Function>(
      kPGNumericDivTruncFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          {gsql_pg_numeric,
           {gsql_pg_numeric, gsql_pg_numeric},
           /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalNumericFloor(
    absl::Span<const zetasql::Value> args) {
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  ZETASQL_RET_CHECK(args.size() == 1);
  if (args[0].is_null()) {
    return zetasql::Value::Null(gsql_pg_numeric);
  }
  ZETASQL_ASSIGN_OR_RETURN(absl::Cord normalized_value,
                   GetPgNumericNormalizedValue(args[0]));

  ZETASQL_ASSIGN_OR_RETURN(std::string result, Floor(std::string(normalized_value)));

  return CreatePgNumericValue(result);
}

std::unique_ptr<zetasql::Function> NumericFloorFunction(
    absl::string_view catalog_name) {
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalNumericFloor));
  return std::make_unique<zetasql::Function>(
      kPGNumericFloorFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalNumericMod(
    absl::Span<const zetasql::Value> args) {
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  ZETASQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return zetasql::Value::Null(gsql_pg_numeric);
  }
  ZETASQL_ASSIGN_OR_RETURN(absl::Cord normalized_value_1,
                   GetPgNumericNormalizedValue(args[0]));
  ZETASQL_ASSIGN_OR_RETURN(absl::Cord normalized_value_2,
                   GetPgNumericNormalizedValue(args[1]));

  ZETASQL_ASSIGN_OR_RETURN(std::string result, Mod(std::string(normalized_value_1),
                                           std::string(normalized_value_2)));

  return CreatePgNumericValue(result);
}

std::unique_ptr<zetasql::Function> NumericModFunction(
    absl::string_view catalog_name) {
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalNumericMod));
  return std::make_unique<zetasql::Function>(
      kPGNumericModFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_pg_numeric,
                                       {gsql_pg_numeric, gsql_pg_numeric},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalNumericMultiply(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return zetasql::Value::Null(
        postgres_translator::spangres::datatypes::GetPgNumericType());
  }

  ZETASQL_ASSIGN_OR_RETURN(absl::Cord lhs, GetPgNumericNormalizedValue(args[0]));
  ZETASQL_ASSIGN_OR_RETURN(absl::Cord rhs, GetPgNumericNormalizedValue(args[1]));
  ZETASQL_ASSIGN_OR_RETURN(std::string result,
                   Multiply(std::string(lhs), std::string(rhs)));

  return CreatePgNumericValue(result);
}

std::unique_ptr<zetasql::Function> NumericMultiplyFunction(
    absl::string_view catalog_name) {
  const zetasql::Type* gsql_pg_numeric =
      postgres_translator::spangres::datatypes::GetPgNumericType();
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalNumericMultiply));

  return std::make_unique<zetasql::Function>(
      kPGNumericMultiplyFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          {gsql_pg_numeric,
           {gsql_pg_numeric, gsql_pg_numeric},
           /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalNumericSubtract(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return zetasql::Value::Null(
        postgres_translator::spangres::datatypes::GetPgNumericType());
  }

  ZETASQL_ASSIGN_OR_RETURN(absl::Cord lhs, GetPgNumericNormalizedValue(args[0]));
  ZETASQL_ASSIGN_OR_RETURN(absl::Cord rhs, GetPgNumericNormalizedValue(args[1]));
  ZETASQL_ASSIGN_OR_RETURN(std::string result,
                   Subtract(std::string(lhs), std::string(rhs)));

  return CreatePgNumericValue(result);
}

std::unique_ptr<zetasql::Function> NumericSubtractFunction(
    absl::string_view catalog_name) {
  const zetasql::Type* gsql_pg_numeric =
      postgres_translator::spangres::datatypes::GetPgNumericType();
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalNumericSubtract));

  return std::make_unique<zetasql::Function>(
      kPGNumericSubtractFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          {gsql_pg_numeric,
           {gsql_pg_numeric, gsql_pg_numeric},
           /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalNumericTrunc(
    absl::Span<const zetasql::Value> args) {
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  ZETASQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return zetasql::Value::Null(gsql_pg_numeric);
  }
  ZETASQL_ASSIGN_OR_RETURN(absl::Cord normalized_value,
                   GetPgNumericNormalizedValue(args[0]));

  ZETASQL_ASSIGN_OR_RETURN(std::string result,
                   Trunc(std::string(normalized_value), args[1].int64_value()));

  return CreatePgNumericValue(result);
}

std::unique_ptr<zetasql::Function> NumericTruncFunction(
    absl::string_view catalog_name) {
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalNumericTrunc));
  return std::make_unique<zetasql::Function>(
      kPGNumericTruncFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_pg_numeric,
                                       {gsql_pg_numeric, gsql_int64},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalNumericUminus(
    absl::Span<const zetasql::Value> args) {
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  ZETASQL_RET_CHECK(args.size() == 1);
  if (args[0].is_null()) {
    return zetasql::Value::Null(gsql_pg_numeric);
  }
  ZETASQL_ASSIGN_OR_RETURN(absl::Cord normalized_value,
                   GetPgNumericNormalizedValue(args[0]));

  ZETASQL_ASSIGN_OR_RETURN(std::string result,
                   UnaryMinus(std::string(normalized_value)));

  return CreatePgNumericValue(result);
}

std::unique_ptr<zetasql::Function> NumericUminusFunction(
    absl::string_view catalog_name) {
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalNumericUminus));
  return std::make_unique<zetasql::Function>(
      kPGNumericUminusFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalCastToNumeric(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(!args.empty() && args.size() < 4);

  if ((args.size() == 2 && args[1].is_null()) ||
      (args.size() == 3 && (args[1].is_null() || args[2].is_null()))) {
    return absl::InvalidArgumentError(
        "type modifiers must be simple constants or identifiers");
  }
  int64_t precision = args.size() > 1 ? args[1].int64_value() : 0;
  int64_t scale = args.size() > 2 ? args[2].int64_value() : 0;

  // When there are precision and scale, PG verifies that precision and scale
  // are valid (not out-of-range and not null) first.
  if (args.size() > 1) {
    ZETASQL_RETURN_IF_ERROR(
        spangres::datatypes::common::ValidatePrecisionAndScale(precision, scale)
            .status());
  }

  // Precision and scale are valid at this point. Return null numeric if input
  // value is null.
  if (args[0].is_null()) {
    return zetasql::Value::Null(spangres::datatypes::GetPgNumericType());
  }

  std::string input_to_string;
  switch (args[0].type_kind()) {
    case zetasql::TYPE_INT64:
      input_to_string = absl::StrCat(args[0].int64_value());
      break;
    case zetasql::TYPE_DOUBLE:
      if (std::isnan(args[0].double_value())) {
        input_to_string = kNan;
      }
      if (std::isinf(args[0].double_value())) {
        return absl::InvalidArgumentError("Cannot cast infinity to PG.NUMERIC");
      }
      input_to_string =
          absl::StrFormat("%.*g", std::numeric_limits<double>::digits10,
                          args[0].double_value());
      break;
    case zetasql::TYPE_STRING:
      input_to_string = args[0].string_value();
      break;
    case zetasql::TYPE_EXTENDED: {
      auto type_code =
          static_cast<const spangres::datatypes::SpannerExtendedType*>(
              args[0].type())
              ->code();
      if (type_code == spangres::datatypes::TypeAnnotationCode::PG_NUMERIC) {
        ZETASQL_ASSIGN_OR_RETURN(absl::Cord normalized,
                         GetPgNumericNormalizedValue(args[0]));
        input_to_string = std::string(normalized);
        break;
      }
    }
      [[fallthrough]];
    default:
      return absl::NotFoundError(absl::StrCat(
          "No cast found from ", args[0].type_kind(), " to numeric"));
  }
  return args.size() == 1 ? CreatePgNumericValue(input_to_string)
                          : CreatePgNumericValueWithPrecisionAndScale(
                                input_to_string, precision, scale);
}

std::unique_ptr<zetasql::Function> CastToNumericFunction(
    absl::string_view catalog_name) {
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalCastToNumeric));
  return std::make_unique<zetasql::Function>(
      kPGCastToNumericFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          // signatures without precision and scale
          zetasql::FunctionSignature{
              gsql_pg_numeric, {gsql_int64}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_pg_numeric, {gsql_double}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_pg_numeric, {gsql_string}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr},
          // signatures with precision and optional scale
          zetasql::FunctionSignature{
              gsql_pg_numeric,
              {gsql_int64,
               gsql_int64,
               {gsql_int64, zetasql::FunctionArgumentType::OPTIONAL}},
              /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_pg_numeric,
              {gsql_double,
               gsql_int64,
               {gsql_int64, zetasql::FunctionArgumentType::OPTIONAL}},
              /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_pg_numeric,
              {gsql_string,
               gsql_int64,
               {gsql_int64, zetasql::FunctionArgumentType::OPTIONAL}},
              /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_pg_numeric,
              {gsql_pg_numeric,
               gsql_int64,
               {gsql_int64, zetasql::FunctionArgumentType::OPTIONAL}},
              /*context_ptr=*/nullptr},
      },
      function_options);
}

absl::StatusOr<zetasql::Value> EvalNumericCastToDouble(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 1);
  if (args[0].is_null()) {
    return zetasql::Value::NullDouble();
  }
  ZETASQL_ASSIGN_OR_RETURN(absl::Cord normalized_value,
                   GetPgNumericNormalizedValue(args[0]));
  double out;
  bool result = absl::SimpleAtod(std::string(normalized_value), &out);
  if (!result || std::isinf(out)) {
    return absl::OutOfRangeError(absl::StrCat("Cannot cast to double from ",
                                              std::string(normalized_value)));
  }
  return zetasql::Value::Double(out);
}

std::unique_ptr<zetasql::Function> NumericCastToDoubleFunction(
    absl::string_view catalog_name) {
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalNumericCastToDouble));
  return std::make_unique<zetasql::Function>(
      kPGNumericCastToDoubleFunctionName, catalog_name,
      zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          gsql_double, {gsql_pg_numeric}, /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalNumericCastToString(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 1);
  if (args[0].is_null()) {
    return zetasql::Value::NullString();
  }
  ZETASQL_ASSIGN_OR_RETURN(absl::Cord normalized_value,
                   GetPgNumericNormalizedValue(args[0]));
  return zetasql::Value::String(std::string(normalized_value));
}

std::unique_ptr<zetasql::Function> NumericCastToStringFunction(
    absl::string_view catalog_name) {
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalNumericCastToString));
  return std::make_unique<zetasql::Function>(
      kPGNumericCastToStringFunctionName, catalog_name,
      zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          gsql_string, {gsql_pg_numeric}, /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalNumericCastToInt64(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 1);
  if (args[0].is_null()) {
    return zetasql::Value::NullInt64();
  }
  ZETASQL_ASSIGN_OR_RETURN(absl::Cord normalized_value,
                   GetPgNumericNormalizedValue(args[0]));
  std::string numeric_string(normalized_value);

  ZETASQL_ASSIGN_OR_RETURN(int64_t result, CastNumericToInt8(numeric_string));

  return zetasql::Value::Int64(result);
}

std::unique_ptr<zetasql::Function> NumericCastToInt64Function(
    absl::string_view catalog_name) {
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalNumericCastToInt64));
  return std::make_unique<zetasql::Function>(
      kPGNumericCastToInt64FunctionName, catalog_name,
      zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          gsql_int64, {gsql_pg_numeric}, /*context_ptr=*/nullptr}},
      function_options);
}

// PG String functions
absl::StatusOr<zetasql::Value> EvalQuoteIdent(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 1);
  if (args[0].is_null()) {
    return zetasql::Value::NullString();
  }

  return zetasql::Value::String(
      absl::StrCat("\"", args[0].string_value(), "\""));
}

std::unique_ptr<zetasql::Function> QuoteIdentFunction(
    absl::string_view catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(zetasql::FunctionEvaluator(EvalQuoteIdent));
  return std::make_unique<zetasql::Function>(
      kPGQuoteIdentFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          gsql_string, {gsql_string}, /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalRegexpMatch(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 2 || args.size() == 3);
  for (auto& arg : args) {
    if (arg.is_null()) {
      return zetasql::Value::Null(zetasql::types::StringArrayType());
    }
  }

  std::unique_ptr<std::vector<std::optional<std::string>>> result;
  if (args.size() == 2) {
    ZETASQL_ASSIGN_OR_RETURN(
        result, RegexpMatch(args[0].string_value(), args[1].string_value()));
  } else {
    ZETASQL_ASSIGN_OR_RETURN(result,
                     RegexpMatch(args[0].string_value(), args[1].string_value(),
                                 args[2].string_value()));
  }

  if (result == nullptr) {
    return zetasql::Value::Null(gsql_string_array);
  } else {
    std::vector<zetasql::Value> values;
    values.reserve(result->size());
    for (int i = 0; i < result->size(); ++i) {
      std::optional<std::string> element = (*result)[i];
      if (element.has_value()) {
        values.push_back(zetasql::Value::String(element.value()));
      } else {
        values.push_back(zetasql::Value::Null(gsql_string));
      }
    }
    return zetasql::Value::MakeArray(gsql_string_array, values);
  }
}

std::unique_ptr<zetasql::Function> RegexpMatchFunction(
    absl::string_view catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      PGFunctionEvaluator(EvalRegexpMatch, CleanupRegexCache));
  return std::make_unique<zetasql::Function>(
      kPGRegexpMatchFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_string_array,
                                       {gsql_string, gsql_string},
                                       /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{gsql_string_array,
                                       {gsql_string, gsql_string, gsql_string},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalRegexpSplitToArray(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 2 || args.size() == 3);
  for (auto& arg : args) {
    if (arg.is_null()) {
      return zetasql::Value::Null(zetasql::types::StringArrayType());
    }
  }

  std::unique_ptr<std::vector<std::string>> result;
  if (args.size() == 2) {
    ZETASQL_ASSIGN_OR_RETURN(result, RegexpSplitToArray(args[0].string_value(),
                                                args[1].string_value()));
  } else {
    ZETASQL_ASSIGN_OR_RETURN(result, RegexpSplitToArray(args[0].string_value(),
                                                args[1].string_value(),
                                                args[2].string_value()));
  }

  if (result == nullptr) {
    return absl::InternalError("regex produced null matches");
  }

  std::vector<zetasql::Value> values;
  values.reserve(result->size());
  for (int i = 0; i < result->size(); ++i) {
    values.push_back(zetasql::Value::String((*result)[i]));
  }
  return zetasql::Value::MakeArray(gsql_string_array, values);
}

std::unique_ptr<zetasql::Function> RegexpSplitToArrayFunction(
    absl::string_view catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      PGFunctionEvaluator(EvalRegexpSplitToArray, CleanupRegexCache));
  return std::make_unique<zetasql::Function>(
      kPGRegexpSplitToArrayFunctionName, catalog_name,
      zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_string_array,
                                       {gsql_string, gsql_string},
                                       /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{gsql_string_array,
                                       {gsql_string, gsql_string, gsql_string},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalSubstring(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return zetasql::Value::NullString();
  }

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<std::string> result,
      Textregexsubstr(args[0].string_value(), args[1].string_value()));

  if (result == nullptr) {
    return zetasql::Value::Null(gsql_string);
  } else {
    return zetasql::Value::String(*result);
  }
}

std::unique_ptr<zetasql::Function> SubstringFunction(
    absl::string_view catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      PGFunctionEvaluator(EvalSubstring, CleanupRegexCache));
  return std::make_unique<zetasql::Function>(
      kPGSubstringFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_string,
                                       {gsql_string, gsql_string},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalCastToDate(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 1);

  ZETASQL_ASSIGN_OR_RETURN(int32_t date,
                   function_evaluators::PgDateIn(args[0].string_value()));
  return zetasql::Value::Date(date);
}

std::unique_ptr<zetasql::Function> CastToDateFunction(
    const std::string& catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      PGFunctionEvaluator(EvalCastToDate, CleanupPostgresDateTimeCache));

  return std::make_unique<zetasql::Function>(
      kPGCastToDateFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{zetasql::types::DateType(),
                                       {zetasql::types::StringType()},
                                       nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalCastToTimestamp(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 1);

  ZETASQL_ASSIGN_OR_RETURN(absl::Time time, function_evaluators::PgTimestamptzIn(
                                        args[0].string_value()));
  return zetasql::Value::Timestamp(time);
}

std::unique_ptr<zetasql::Function> CastToTimestampFunction(
    const std::string& catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      PGFunctionEvaluator(EvalCastToTimestamp, CleanupPostgresDateTimeCache));

  return std::make_unique<zetasql::Function>(
      kPGCastToTimestampFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{zetasql::types::TimestampType(),
                                       {zetasql::types::StringType()},
                                       nullptr}},
      function_options);
}

// Maps double value to an integer value in such a way, that the PostgreSQL
// sort order/comparison semantics of DOUBLE PRECISION (FLOAT8) type values is
// preserved in the order of obtained (after mapping) int64_t values ({input x <
// input y} => {output for x < output for y}).
//
// PostgreSQL FLOAT8 comparison semantic rules are as follows:
// * All Nan values are equal (including negative).
// * Nan value is bigger than any other non-null floating point value.
// * Negative zero (-0.0) is equal to positive zero (0.0).
absl::StatusOr<zetasql::Value> EvalMapDoubleToInt(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 1);

  double num = args[0].double_value();
  if (std::isnan(num)) {
    return zetasql::Value::Int64(std::numeric_limits<int64_t>::max());
  }

  // Encodes a double value as int64_t value using mostly isomorphic (values can
  // be converted back) and order preservable (if input x < input y then output
  // for x < output for y) transformations. The exception for isomorphism:
  // negative zero will be round-tripped to positive zero.
  const int64_t enc = absl::bit_cast<int64_t>(num);
  int64_t res = (enc < 0) ? std::numeric_limits<int64_t>::min() - enc : enc;

  return zetasql::Value::Int64(res);
}

std::unique_ptr<zetasql::Function> MapDoubleToIntFunction(
    const std::string& catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      zetasql::FunctionEvaluator(EvalMapDoubleToInt));

  return std::make_unique<zetasql::Function>(
      kPGMapDoubleToIntFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{zetasql::types::Int64Type(),
                                       {zetasql::types::DoubleType()},
                                       nullptr}},
      function_options);
}

// PG Cast functions
absl::StatusOr<zetasql::Value> EvalToJsonBFromValue(zetasql::Value arg);

// Converts a `Value` to its unquoted string representation. `null` value is
// printed as a `null_string`. The return string from certain Value types may be
// normalized later when converted to PG.JSONB by calling `EvalToJsonBFrom<Type>`.
// Otherwise, this function does not guarantee a normalized return.
absl::StatusOr<std::string> GetStringRepresentation(
    const zetasql::Value& value, std::string null_string = "null") {
  if (value.is_null()) {
    return null_string;
  }
  switch (value.type_kind()) {
    case zetasql::TYPE_INT64:
      return absl::StrCat(value.int64_value());
    case zetasql::TYPE_BOOL:
      return value.bool_value() ? kTrue : kFalse;
    case zetasql::TYPE_DOUBLE:
      return zetasql::RoundTripDoubleToString(value.double_value());
    case zetasql::TYPE_STRING:
      return value.string_value();
    case zetasql::TYPE_BYTES:
      return absl::StrCat("\\x", absl::BytesToHexString(value.bytes_value()));
    case zetasql::TYPE_DATE: {
      std::string date_string;
      // `zetasql::values::Date` is always a valid date (`null` check is done
      // above); hence, the following call to `ConvertDateToString` will never
      // return an invalid date error.
      ZETASQL_RETURN_IF_ERROR(zetasql::functions::ConvertDateToString(
          value.date_value(), &date_string));
      return date_string;
    }
    case zetasql::TYPE_TIMESTAMP: {
      std::string timestamp_string;
      // `zetasql::values::Timestamp` is always a valid timestamp (`null`
      // check is done above); hence, the following call to
      // `FormatTimestampToString` will never return an invalid timestamp error.
      ZETASQL_RETURN_IF_ERROR(zetasql::functions::FormatTimestampToString(
          absl::RFC3339_full, absl::ToUnixMicros(value.ToTime()),
          absl::UTCTimeZone(), {}, &timestamp_string));
      return timestamp_string;
    }
    case zetasql::TYPE_ARRAY: {
      if (value.empty()) {
        return "[]";
      }
      absl::Cord ret_cord;
      ret_cord.Append("[");
      ret_cord.Append(GetStringRepresentation(
                          EvalToJsonBFromValue(value.element(0)).value())
                          .value());
      for (int i = 1; i < value.num_elements(); ++i) {
        ret_cord.Append(", ");
        ret_cord.Append(GetStringRepresentation(
                            EvalToJsonBFromValue(value.element(i)).value())
                            .value());
      }
      ret_cord.Append("]");
      return std::string(ret_cord);
    }
    case zetasql::TYPE_EXTENDED: {
      auto type_code =
          static_cast<const spangres::datatypes::SpannerExtendedType*>(
              value.type())
              ->code();
      switch (type_code) {
        case spangres::datatypes::TypeAnnotationCode::PG_JSONB:
          return std::string(GetPgJsonbNormalizedValue(value).value());
        case spangres::datatypes::TypeAnnotationCode::PG_NUMERIC:
          return std::string(GetPgNumericNormalizedValue(value).value());
        default:
          ZETASQL_RET_CHECK_FAIL() << "Encountered unexpected type "
                           << value.type_kind();
      }
    }
    default:
      ZETASQL_RET_CHECK_FAIL() << "Encountered unexpected type " << value.type_kind();
  }
}

// Returns a normalized PG.JSONB value from the int64_t input.
absl::StatusOr<zetasql::Value> EvalToJsonBFromInt64(
    const zetasql::Value arg) {
  return CreatePgJsonbValue(GetStringRepresentation(arg).value());
}

// Returns a normalized PG.JSONB value from the bool input.
absl::StatusOr<zetasql::Value> EvalToJsonBFromBool(
    const zetasql::Value arg) {
  return CreatePgJsonbValue(GetStringRepresentation(arg).value());
}

// Returns a normalized PG.JSONB value from the double input.
absl::StatusOr<zetasql::Value> EvalToJsonBFromDouble(
    const zetasql::Value arg) {
  if (std::isnan(arg.double_value())) {
    return spangres::datatypes::CreatePgJsonbValueFromNormalized(
        absl::Cord(kNanString));
  }
  if (std::isinf(arg.double_value())) {
    return arg.double_value() > 0
               ? spangres::datatypes::CreatePgJsonbValueFromNormalized(
                     absl::Cord(kInfString))
               : spangres::datatypes::CreatePgJsonbValueFromNormalized(
                     absl::Cord(kNegInfString));
  }
  return CreatePgJsonbValue(GetStringRepresentation(arg).value());
}

// Returns a normalized PG.JSONB value from the string input.
absl::StatusOr<zetasql::Value> EvalToJsonBFromString(
    const zetasql::Value arg) {
  if (IsValidJsonbString(arg.string_value())) {
    return CreatePgJsonbValue(
        NormalizeJsonbString(GetStringRepresentation(arg).value()));
  }
  return absl::InvalidArgumentError(
      "unsupported Unicode escape sequence DETAIL: \\u0000 cannot "
      "be converted to text.");
}

// Returns a normalized PG.JSONB value from the bytes input.
absl::StatusOr<zetasql::Value> EvalToJsonBFromBytes(
    const zetasql::Value arg) {
  return EvalToJsonBFromString(
      zetasql::values::String(GetStringRepresentation(arg).value()));
}

// Returns a normalized PG.JSONB value from the date input.
absl::StatusOr<zetasql::Value> EvalToJsonBFromDate(
    const zetasql::Value arg) {
  return CreatePgJsonbValue(
      absl::StrCat("\"", GetStringRepresentation(arg).value(), "\""));
}

// Returns a normalized PG.JSONB value from the timestamp input.
absl::StatusOr<zetasql::Value> EvalToJsonBFromTimestamp(
    const zetasql::Value arg) {
  return CreatePgJsonbValue(
      absl::StrCat("\"", GetStringRepresentation(arg).value(), "\""));
}

// Returns a normalized PG.JSONB value from the array input.
absl::StatusOr<zetasql::Value> EvalToJsonBFromArray(
    const zetasql::Value arg) {
  return CreatePgJsonbValue(GetStringRepresentation(arg).value());
}

// Returns a normalized PG.JSONB value from the PG.JSONB input.
absl::StatusOr<zetasql::Value> EvalToJsonBFromPgJsonB(
    const zetasql::Value arg) {
  return arg;
}

// Returns a normalized PG.JSONB value from the PG.NUMERIC input.
absl::StatusOr<zetasql::Value> EvalToJsonBFromPgNumeric(
    const zetasql::Value arg) {
  if (std::string(GetPgNumericNormalizedValue(arg).value()) == kNan) {
    return spangres::datatypes::CreatePgJsonbValueFromNormalized(
        absl::Cord(kNanString));
  }

  return CreatePgJsonbValue(GetStringRepresentation(arg).value());
}

// Returns a normalized PG.JSONB value from the extended type input.
absl::StatusOr<zetasql::Value> EvalToJsonBFromExtended(
    const zetasql::Value arg) {
  auto type_code =
      static_cast<const spangres::datatypes::SpannerExtendedType*>(arg.type())
          ->code();
  switch (type_code) {
    case spangres::datatypes::TypeAnnotationCode::PG_JSONB:
      return EvalToJsonBFromPgJsonB(arg);
    case spangres::datatypes::TypeAnnotationCode::PG_NUMERIC:
      return EvalToJsonBFromPgNumeric(arg);
    default:
      ZETASQL_RET_CHECK_FAIL() << "Encountered unexpected type " << arg.type_kind();
  }
}

// Returns a normalized PG.JSONB value from the input.
absl::StatusOr<zetasql::Value> EvalToJsonBFromValue(
    const zetasql::Value arg) {
  if (arg.is_null()) {
    // `null` input results in `null` JSONB value.
    return zetasql::values::Null(spangres::datatypes::GetPgJsonbType());
  }

  zetasql::TypeKind type_kind = arg.type_kind();
  switch (type_kind) {
    case zetasql::TYPE_INT64:
      return EvalToJsonBFromInt64(arg);
    case zetasql::TYPE_BOOL:
      return EvalToJsonBFromBool(arg);
    case zetasql::TYPE_DOUBLE:
      return EvalToJsonBFromDouble(arg);
    case zetasql::TYPE_STRING:
      return EvalToJsonBFromString(arg);
    case zetasql::TYPE_BYTES:
      return EvalToJsonBFromBytes(arg);
    case zetasql::TYPE_DATE:
      return EvalToJsonBFromDate(arg);
    case zetasql::TYPE_TIMESTAMP:
      return EvalToJsonBFromTimestamp(arg);
    case zetasql::TYPE_ARRAY:
      return EvalToJsonBFromArray(arg);
    case zetasql::TYPE_EXTENDED:
      return EvalToJsonBFromExtended(arg);
    default:
      ZETASQL_RET_CHECK_FAIL() << "Encountered unexpected type " << type_kind;
  }
}

absl::StatusOr<zetasql::Value> EvalToJsonB(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 1);
  return EvalToJsonBFromValue(args[0]);
}

std::unique_ptr<zetasql::Function> ToJsonBFunction(
    absl::string_view catalog_name) {
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();
  static const zetasql::Type* gsql_pg_jsonb =
      spangres::datatypes::GetPgJsonbType();
  static const zetasql::Type* gsql_pg_numeric_array =
      spangres::datatypes::GetPgNumericArrayType();
  static const zetasql::Type* gsql_pg_jsonb_array =
      spangres::datatypes::GetPgJsonbArrayType();

  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalToJsonB));
  return std::make_unique<zetasql::Function>(
      kPGToJsonBFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{
              gsql_pg_jsonb, {gsql_bool}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_pg_jsonb, {gsql_bool_array}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_pg_jsonb, {gsql_bytes}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_pg_jsonb, {gsql_bytes_array}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_pg_jsonb, {gsql_date}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_pg_jsonb, {gsql_date_array}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_pg_jsonb, {gsql_double}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_pg_jsonb, {gsql_double_array}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_pg_jsonb, {gsql_int64}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_pg_jsonb, {gsql_int64_array}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_pg_jsonb, {gsql_pg_jsonb}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_pg_jsonb, {gsql_pg_jsonb_array}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_pg_jsonb, {gsql_pg_numeric}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_pg_jsonb, {gsql_pg_numeric_array}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_pg_jsonb, {gsql_string}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_pg_jsonb, {gsql_string_array}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_pg_jsonb, {gsql_timestamp}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_pg_jsonb, {gsql_timestamp_array}, /*context_ptr=*/nullptr},
      },
      function_options);
}

absl::StatusOr<zetasql::Value> EvalJsonBSubscriptText(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 2);
  if (args[1].type_kind() != zetasql::TYPE_INT64 &&
      args[1].type_kind() != zetasql::TYPE_STRING) {
    return absl::UnimplementedError(absl::StrCat(
        "jsonb_subscript_text(jsonb, ", args[1].type()->DebugString(), ")"));
  }
  if (args[0].is_null() || args[1].is_null()) {
    return zetasql::Value::NullString();
  }
  const std::string jsonb(GetStringRepresentation(args[0]).value());
  if (args[1].type_kind() == zetasql::TYPE_INT64) {
    const int32_t element = static_cast<int32_t>(args[1].int64_value());
    return EmulatorJsonBArrayElementText(jsonb, element);
  } else {
    const std::string key(args[1].string_value());
    return EmulatorJsonBObjectFieldText(jsonb, key);
  }
}

std::unique_ptr<zetasql::Function> JsonBSubscriptTextFunction(
    absl::string_view catalog_name) {
  const zetasql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonBSubscriptText));
  return std::make_unique<zetasql::Function>(
      kPGJsonBSubscriptTextFunctionName, catalog_name,
      zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_string,
                                       {gsql_pg_jsonb, gsql_int64},
                                       /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{gsql_string,
                                       {gsql_pg_jsonb, gsql_string},
                                       /*context_ptr=*/nullptr},
      },
      function_options);
}

absl::StatusOr<zetasql::Value> EvalJsonBArrayElement(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 2);
  if (args[1].type_kind() != zetasql::TYPE_INT64) {
    return absl::UnimplementedError(absl::StrCat(
        "jsonb_array_element(jsonb, ", args[1].type()->DebugString(), ")"));
  }
  if (args[0].is_null() || args[1].is_null()) {
    return zetasql::Value::Null(
        postgres_translator::spangres::datatypes::GetPgJsonbType());
  }

  ZETASQL_ASSIGN_OR_RETURN(absl::Cord jsonb, GetPgJsonbNormalizedValue(args[0]));
  return JsonBArrayElement(std::string(jsonb), args[1].int64_value());
}

std::unique_ptr<zetasql::Function> JsonBArrayElementFunction(
    absl::string_view catalog_name) {
  const zetasql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonBArrayElement));
  return std::make_unique<zetasql::Function>(
      kPGJsonBArrayElementFunctionName, catalog_name,
      zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_pg_jsonb,
                                       {gsql_pg_jsonb, gsql_int64},
                                       /*context_ptr=*/nullptr},
      },
      function_options);
}

absl::StatusOr<zetasql::Value> EvalJsonBObjectField(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 2);
  if (args[1].type_kind() != zetasql::TYPE_STRING) {
    return absl::UnimplementedError(absl::StrCat(
        "jsonb_object_field(jsonb, ", args[1].type()->DebugString(), ")"));
  }
  if (args[0].is_null() || args[1].is_null()) {
    return zetasql::Value::Null(
        postgres_translator::spangres::datatypes::GetPgJsonbType());
  }

  ZETASQL_ASSIGN_OR_RETURN(absl::Cord jsonb, GetPgJsonbNormalizedValue(args[0]));
  return JsonBObjectField(std::string(jsonb), args[1].string_value());
}

std::unique_ptr<zetasql::Function> JsonBObjectFieldFunction(
    absl::string_view catalog_name) {
  const zetasql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonBObjectField));
  return std::make_unique<zetasql::Function>(
      kPGJsonBObjectFieldFunctionName, catalog_name,
      zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_pg_jsonb,
                                       {gsql_pg_jsonb, gsql_string},
                                       /*context_ptr=*/nullptr},
      },
      function_options);
}

absl::StatusOr<zetasql::Value> EvalJsonBTypeof(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 1);
  if (args[0].is_null()) {
    return zetasql::Value::NullString();
  }

  ZETASQL_ASSIGN_OR_RETURN(absl::Cord jsonb, GetPgJsonbNormalizedValue(args[0]));
  return JsonBTypeof(std::string(jsonb));
}

std::unique_ptr<zetasql::Function> JsonBTypeofFunction(
    absl::string_view catalog_name) {
  const zetasql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonBTypeof));
  return std::make_unique<zetasql::Function>(
      kPGJsonBTypeofFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_string,
                                       {gsql_pg_jsonb},
                                       /*context_ptr=*/nullptr},
      },
      function_options);
}

// PgDoubleLess and PgDoubleGreater are alternatives to std::less<double> and
// std::greater<double> which capture Postgres' ordering semantics.
// std::less<double> and std::greater<double> do not have proper ordering
// semantics for NaN values, they will always return false when one of the
// argument is NaN. In Postgres NaN is the highest valued float8 and NUMERIC.
// LEAST(12::float8, 3::float8, 'nan'::float8, null::float8) => 3
// GREATEST(12::float8, 3::float8, 'nan'::float8, null::float8) => NaN
class PgDoubleLess {
 public:
  // Returns true iff lhs is strictly less than rhs.
  bool operator()(const zetasql::Value lhs,
                  const zetasql::Value rhs) const {
    double typed_lhs = lhs.double_value();
    double typed_rhs = rhs.double_value();

    if (std::isnan(typed_lhs) && std::isnan(typed_rhs)) {
      return false;
    }

    if (std::isnan(typed_rhs)) {
      return true;
    }

    return typed_lhs < typed_rhs;
  }
};

class PgDoubleGreater {
 public:
  // Returns true iff lhs is strictly greater than rhs.
  bool operator()(const zetasql::Value lhs,
                  const zetasql::Value rhs) const {
    double typed_lhs = lhs.double_value();
    double typed_rhs = rhs.double_value();

    if (std::isnan(typed_lhs) && std::isnan(typed_rhs)) {
      return false;
    }

    if (std::isnan(typed_lhs)) {
      return true;
    }

    return typed_lhs > typed_rhs;
  }
};

class PgLess {
 public:
  // Returns true iff lhs is strictly less than rhs.
  bool operator()(const zetasql::Value lhs,
                  const zetasql::Value rhs) const {
    return lhs.LessThan(rhs);
  }
};

class PgGreater {
 public:
  // Returns true iff lhs is strictly greater than rhs.
  bool operator()(const zetasql::Value lhs,
                  const zetasql::Value rhs) const {
    return !(lhs.Equals(rhs) || lhs.LessThan(rhs));
  }
};

template <typename Compare>
absl::StatusOr<zetasql::Value> EvalLeastGreatest(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(!args.empty());

  if (!args[0].is_valid()) {
    return absl::InvalidArgumentError("Bad input at position 0");
  }
  for (int i = 1; i < args.size(); ++i) {
    if (!args[i].is_valid()) {
      return absl::InvalidArgumentError(
          absl::Substitute("Bad input at position $0", i));
    }
    if (!args[i].type()->Equals(args[i - 1].type())) {
      return absl::InvalidArgumentError(absl::Substitute(
          "Mismatched types at position $0 ($1) and position $2 ($3)", i - 1,
          args[i - 1].type()->DebugString(), i, args[i].type()->DebugString()));
    }
  }

  Compare cmp;
  zetasql::Value result = args[0];
  for (int i = 1; i < args.size(); ++i) {
    // Always skip a NULL value. If input was all NULLs will get
    // the NULL result from args[0].
    if (args[i].is_null()) {
      continue;
    }

    // If output is NULL, always overwrite.
    if (result.is_null()) {
      result = args[i];
      continue;
    }

    if (cmp(args[i], result)) {
      result = args[i];
    }
  }

  return result;
}

std::pair<std::unique_ptr<zetasql::Function>,
          std::unique_ptr<zetasql::Function>>
LeastGreatestFunctions(const std::string& catalog_name) {
  auto is_non_double_supported_type = [](const zetasql::Type* type) -> bool {
    return (type->IsInt64() || type->IsBool() || type->IsBytes() ||
            type->IsString() || type->IsDate() || type->IsTimestamp());
  };

  zetasql::FunctionEvaluatorFactory least_evaluator_factory(
      [&](const zetasql::FunctionSignature& signature)
          -> absl::StatusOr<zetasql::FunctionEvaluator> {
        if (signature.result_type().type()->IsDouble()) {
          return EvalLeastGreatest<PgDoubleLess>;
        } else if (is_non_double_supported_type(
                       signature.result_type().type())) {
          return EvalLeastGreatest<PgLess>;
        }
        return absl::InvalidArgumentError(
            absl::Substitute("Unsupported type $0 when calling $1()",
                             signature.result_type().type()->DebugString(),
                             kPGLeastFunctionName));
      });
  zetasql::FunctionOptions least_function_options;
  least_function_options.set_evaluator_factory(least_evaluator_factory);

  zetasql::FunctionEvaluatorFactory greatest_evaluator_factory(
      [&](const zetasql::FunctionSignature& signature)
          -> absl::StatusOr<zetasql::FunctionEvaluator> {
        if (signature.result_type().type()->IsDouble()) {
          return EvalLeastGreatest<PgDoubleGreater>;
        } else if (is_non_double_supported_type(
                       signature.result_type().type())) {
          return EvalLeastGreatest<PgGreater>;
        }
        return absl::InvalidArgumentError(
            absl::Substitute("Unsupported type $0 when calling $1()",
                             signature.result_type().type()->DebugString(),
                             kPGGreatestFunctionName));
      });
  zetasql::FunctionOptions greatest_function_options;
  greatest_function_options.set_evaluator_factory(greatest_evaluator_factory);

  std::vector<const zetasql::Type*> supported_types{
      zetasql::types::DoubleType(),
      zetasql::types::Int64Type(),
      zetasql::types::BoolType(),
      zetasql::types::BytesType(),
      zetasql::types::StringType(),
      zetasql::types::DateType(),
      zetasql::types::TimestampType(),
      postgres_translator::spangres::datatypes::GetPgNumericType(),
      postgres_translator::spangres::datatypes::GetPgJsonbType(),
  };

  // Construct the function signatures for all the supported types.
  std::vector<zetasql::FunctionSignature> function_signatures;
  function_signatures.reserve(supported_types.size());
  for (auto type : supported_types) {
    function_signatures.push_back(zetasql::FunctionSignature{
        type,
        {type, {type, zetasql::FunctionArgumentType::REPEATED}},
        nullptr});
  }

  return {
      // pg.least
      std::make_unique<zetasql::Function>(
          kPGLeastFunctionName, catalog_name, zetasql::Function::SCALAR,
          function_signatures, least_function_options),
      // pg.greatest
      std::make_unique<zetasql::Function>(
          kPGGreatestFunctionName, catalog_name, zetasql::Function::SCALAR,
          function_signatures, greatest_function_options)};
}

// Aggregate functions.

class MinDoubleEvaluator : public zetasql::AggregateFunctionEvaluator {
 public:
  explicit MinDoubleEvaluator() {}
  ~MinDoubleEvaluator() override = default;

  absl::Status Reset() override { return absl::OkStatus(); }

  absl::Status Accumulate(absl::Span<const zetasql::Value*> args,
                          bool* stop_accumulation) override {
    // No args left to accumulate.
    if (args.empty()) {
      *stop_accumulation = true;
      return absl::OkStatus();
    }

    const zetasql::Value value = *args[0];
    if (!value.type()->IsDouble()) {
      return absl::InvalidArgumentError(
          "Cannot accumulate value which is not of type double.");
    }

    // TODO: Figure out why IgnoreNulls(), which defaults to true
    // is not working.
    if (value.is_null()) {
      return absl::OkStatus();
    }

    // If the existing stored result is uninitialized, set it to NaN before
    // comparison with the current value in context as NaN is greater than all
    // other values in PostgreSQL.
    if (result_.is_null()) {
      result_ =
          zetasql::Value::Double(std::numeric_limits<double>::quiet_NaN());
    }

    // Use the comparison function that respects the NaN-ordering semantics of
    // PostgreSQL.
    if (PgDoubleLess()(value, result_)) {
      result_ = value;
    }

    return absl::OkStatus();
  }

  absl::StatusOr<zetasql::Value> GetFinalResult() override { return result_; }

 private:
  // Initialized to NULL as it's the default value to return if no values are
  // provided to aggregate or if all the values to aggregate are NULL.
  zetasql::Value result_ = zetasql::values::NullDouble();
};

std::unique_ptr<zetasql::Function> MinAggregator(
    const std::string& catalog_name) {
  zetasql::AggregateFunctionEvaluatorFactory aggregate_fn =
      [](const zetasql::FunctionSignature& sig) {
        return std::make_unique<MinDoubleEvaluator>();
      };

  zetasql::FunctionOptions options;
  options.set_aggregate_function_evaluator_factory(aggregate_fn);
  return std::make_unique<zetasql::Function>(
      kPGMinFunctionName, catalog_name, zetasql::Function::AGGREGATE,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{zetasql::types::DoubleType(),
                                       {zetasql::types::DoubleType()},
                                       nullptr}},
      options);
}

class MinMaxNumericEvaluator : public zetasql::AggregateFunctionEvaluator {
 public:
  explicit MinMaxNumericEvaluator(bool is_min) : is_min_(is_min) {}
  ~MinMaxNumericEvaluator() override = default;

  absl::Status Reset() override { return absl::OkStatus(); }

  absl::Status Accumulate(absl::Span<const zetasql::Value*> args,
                          bool* stop_accumulation) override {
    // No args left to accumulate.
    if (args.empty()) {
      *stop_accumulation = true;
      return absl::OkStatus();
    }

    const zetasql::Value value = *args[0];
    // TODO: Figure out why IgnoreNulls(), which defaults to true
    // is not working.
    if (value.is_null()) {
      return absl::OkStatus();
    }

    // First non-null value we're seeing so set to this value.
    if (result_.is_null()) {
      result_ = value;
      // Setup the memory context arena which is required for future collated
      // comparisons called by LessThan().
      ZETASQL_ASSIGN_OR_RETURN(arena_,
                       postgres_translator::interfaces::CreatePGArena(nullptr));
      return absl::OkStatus();
    }

    if (is_min_ && !result_.LessThan(value)) {
      // Evaluating as MIN().
      result_ = value;
    } else if (!is_min_ && result_.LessThan(value)) {
      // Evaluating as MAX().
      result_ = value;
    }

    return absl::OkStatus();
  }

  absl::StatusOr<zetasql::Value> GetFinalResult() override { return result_; }

 private:
  const zetasql::Type* gsql_pg_numeric_ =
      spangres::datatypes::GetPgNumericType();
  // Initialized to NULL as it's the default value to return if no values are
  // provided to aggregate or if all the values to aggregate are NULL.
  zetasql::Value result_ = zetasql::values::Null(gsql_pg_numeric_);
  const bool is_min_;
  std::unique_ptr<postgres_translator::interfaces::PGArena> arena_;
};

std::unique_ptr<zetasql::Function> MinNumericAggregator(
    const std::string& catalog_name) {
  const zetasql::Type* gsql_pg_numeric =
      postgres_translator::spangres::datatypes::GetPgNumericType();

  zetasql::AggregateFunctionEvaluatorFactory aggregate_fn =
      [](const zetasql::FunctionSignature& sig) {
        return std::make_unique<MinMaxNumericEvaluator>(/* is_min =*/true);
      };

  zetasql::FunctionOptions options;
  options.set_aggregate_function_evaluator_factory(aggregate_fn);
  return std::make_unique<zetasql::Function>(
      kPGNumericMinFunctionName, catalog_name, zetasql::Function::AGGREGATE,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          gsql_pg_numeric, {gsql_pg_numeric}, nullptr}},
      options);
}

std::unique_ptr<zetasql::Function> MaxNumericAggregator(
    const std::string& catalog_name) {
  const zetasql::Type* gsql_pg_numeric =
      postgres_translator::spangres::datatypes::GetPgNumericType();

  zetasql::AggregateFunctionEvaluatorFactory aggregate_fn =
      [](const zetasql::FunctionSignature& sig) {
        return std::make_unique<MinMaxNumericEvaluator>(/* is_min =*/false);
      };

  zetasql::FunctionOptions options;
  options.set_aggregate_function_evaluator_factory(aggregate_fn);
  return std::make_unique<zetasql::Function>(
      kPGNumericMaxFunctionName, catalog_name, zetasql::Function::AGGREGATE,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          gsql_pg_numeric, {gsql_pg_numeric}, nullptr}},
      options);
}

// Can evaluate a sum for INT64, DOUBLE and PG.NUMERIC.
class SumEvaluator : public zetasql::AggregateFunctionEvaluator {
 public:
  explicit SumEvaluator() {}
  ~SumEvaluator() override = default;

  absl::Status Reset() override { return absl::OkStatus(); }

  absl::Status Accumulate(absl::Span<const zetasql::Value*> args,
                          bool* stop_accumulation) override {
    // No args left to accumulate.
    if (args.empty()) {
      *stop_accumulation = true;
      return absl::OkStatus();
    }

    const zetasql::Value value = *args[0];

    // This is the first time we're seeing a value so set the type for
    // accumulation based on the type of the first argument and set the result_
    // to the NULL value for that type.
    if (kind_ == zetasql::TYPE_UNKNOWN) {
      if (value.type_kind() == zetasql::TYPE_DOUBLE) {
        kind_ = zetasql::TYPE_DOUBLE;
        result_ = zetasql::values::NullDouble();
      } else if (value.type_kind() == zetasql::TYPE_INT64 ||
                 (value.type_kind() == zetasql::TYPE_EXTENDED &&
                  value.type()->Equals(gsql_pg_numeric_))) {
        // Both INT64 and PG.NUMERIC return PG.NUMERIC.
        ZETASQL_ASSIGN_OR_RETURN(
            arena_, postgres_translator::interfaces::CreatePGArena(nullptr));
        kind_ = value.type_kind();
        result_ = zetasql::values::Null(gsql_pg_numeric_);
      } else {
        return absl::InvalidArgumentError(
            "Cannot accumulate value which is not of type INT64, DOUBLE or "
            "PG.NUMERIC.");
      }
    } else if (value.type_kind() != kind_ ||
               (value.type_kind() == zetasql::TYPE_EXTENDED &&
                !value.type()->Equals(gsql_pg_numeric_))) {
      // We've accumulated values before so make sure the type stays consistent
      // across the accumulation.
      return absl::InvalidArgumentError(
          "Values being accumulated must all be of the same type");
    }

    // TODO: Figure out why IgnoreNulls(), which defaults to true
    // is not working.
    if (value.is_null()) {
      return absl::OkStatus();
    }

    // If the result is null, the first value must've been null so set the
    // result to the new value.
    if (result_.is_null()) {
      if (value.type_kind() == zetasql::TYPE_INT64) {
        // Result must be of type PG.NUMERIC so convert the result to the
        // correct type.
        ZETASQL_ASSIGN_OR_RETURN(
            result_, CreatePgNumericValue(absl::StrCat(value.int64_value())));
      } else {
        result_ = value;
      }

      count_++;
      return absl::OkStatus();
    }

    // Now do the addition.
    if (value.type_kind() == zetasql::TYPE_INT64) {
      ZETASQL_ASSIGN_OR_RETURN(auto value_as_numeric,
                       CreatePgNumericValue(absl::StrCat(value.int64_value())));
      ZETASQL_ASSIGN_OR_RETURN(
          result_,
          EvalNumericAdd(absl::MakeConstSpan({result_, value_as_numeric})));
    } else if (value.type_kind() == zetasql::TYPE_DOUBLE) {
      double result;
      absl::Status status;
      if (!zetasql::functions::Add(result_.double_value(),
                                     value.double_value(), &result, &status)) {
        return status;
      }
      result_ = zetasql::values::Double(result);
    } else if (value.type_kind() == zetasql::TYPE_EXTENDED) {
      ZETASQL_ASSIGN_OR_RETURN(result_,
                       EvalNumericAdd(absl::MakeConstSpan({result_, value})));
    }  // No else because we've already validated the type above.

    count_++;
    return absl::OkStatus();
  }

  absl::StatusOr<zetasql::Value> GetFinalResult() override {
    if (kind_ == zetasql::TYPE_UNKNOWN) {
      // This is not quite correct because we'll be returning a value of type
      // PG.NUMERIC even if the column being aggregated is of type DOUBLE. We
      // can't do much about it because a zetasql::AggregateFunctionEvaluator
      // doesn't know anything about the return type.
      return zetasql::values::Null(gsql_pg_numeric_);
    }
    return result_;
  }

 protected:
  uint64_t count_ = 0;
  zetasql::TypeKind kind_ = zetasql::TYPE_UNKNOWN;
  zetasql::Value result_;
  const zetasql::Type* gsql_pg_numeric_ =
      spangres::datatypes::GetPgNumericType();

 private:
  std::unique_ptr<postgres_translator::interfaces::PGArena> arena_;
};

std::unique_ptr<zetasql::Function> SumAggregator(
    const std::string& catalog_name) {
  zetasql::AggregateFunctionEvaluatorFactory aggregate_fn =
      [](const zetasql::FunctionSignature& sig) {
        return std::make_unique<SumEvaluator>();
      };

  const zetasql::Type* gsql_pg_numeric =
      postgres_translator::spangres::datatypes::GetPgNumericType();

  zetasql::FunctionOptions options;
  options.set_aggregate_function_evaluator_factory(aggregate_fn);
  return std::make_unique<zetasql::Function>(
      kPGSumFunctionName, catalog_name, zetasql::Function::AGGREGATE,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{
              gsql_pg_numeric, {zetasql::types::Int64Type()}, nullptr},
          zetasql::FunctionSignature{zetasql::types::DoubleType(),
                                       {zetasql::types::DoubleType()},
                                       nullptr},
          zetasql::FunctionSignature{
              gsql_pg_numeric, {gsql_pg_numeric}, nullptr}},
      options);
}

// Can evaluate the avg for INT64, DOUBLE and PG.NUMERIC.
class AvgEvaluator : public SumEvaluator {
 public:
  absl::StatusOr<zetasql::Value> GetFinalResult() override {
    if (kind_ == zetasql::TYPE_UNKNOWN) {
      // This is not quite correct because we'll be returning a value of type
      // PG.NUMERIC even if the column being aggregated is of type DOUBLE. We
      // can't do much about it because a zetasql::AggregateFunctionEvaluator
      // doesn't know anything about the return type.
      return zetasql::values::Null(gsql_pg_numeric_);
    }

    if (kind_ == zetasql::TYPE_DOUBLE) {
      if (result_.is_null()) {
        return zetasql::values::NullDouble();
      }
      double result;
      absl::Status status;
      if (!zetasql::functions::Divide(result_.double_value(),
                                        static_cast<double>(count_), &result,
                                        &status)) {
        return status;
      }
      return zetasql::values::Double(result);
    }

    // INT64 or PG.NUMERIC
    ZETASQL_ASSIGN_OR_RETURN(auto count_as_numeric,
                     CreatePgNumericValue(absl::StrCat(count_)));
    return EvalNumericDivide(absl::MakeConstSpan({result_, count_as_numeric}));
  }
};

std::unique_ptr<zetasql::Function> AvgAggregator(
    const std::string& catalog_name) {
  zetasql::AggregateFunctionEvaluatorFactory aggregate_fn =
      [](const zetasql::FunctionSignature& sig) {
        return std::make_unique<AvgEvaluator>();
      };

  const zetasql::Type* gsql_pg_numeric =
      postgres_translator::spangres::datatypes::GetPgNumericType();

  zetasql::FunctionOptions options;
  options.set_aggregate_function_evaluator_factory(aggregate_fn);
  return std::make_unique<zetasql::Function>(
      kPGAvgFunctionName, catalog_name, zetasql::Function::AGGREGATE,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{
              gsql_pg_numeric, {zetasql::types::Int64Type()}, nullptr},
          zetasql::FunctionSignature{zetasql::types::DoubleType(),
                                       {zetasql::types::DoubleType()},
                                       nullptr},
          zetasql::FunctionSignature{
              gsql_pg_numeric, {gsql_pg_numeric}, nullptr}},
      options);
}

}  // namespace

SpannerPGFunctions GetSpannerPGFunctions(const std::string& catalog_name) {
  SpannerPGFunctions functions;

  auto cast_to_date_func = CastToDateFunction(catalog_name);
  functions.push_back(std::move(cast_to_date_func));

  auto cast_to_timestamp_func = CastToTimestampFunction(catalog_name);
  functions.push_back(std::move(cast_to_timestamp_func));

  auto map_double_to_int_func = MapDoubleToIntFunction(catalog_name);
  functions.push_back(std::move(map_double_to_int_func));

  auto least_greatest_funcs = LeastGreatestFunctions(catalog_name);
  functions.push_back(std::move(least_greatest_funcs.first));   // least
  functions.push_back(std::move(least_greatest_funcs.second));  // greatest

  auto min_agg = MinAggregator(catalog_name);
  functions.push_back(std::move(min_agg));
  auto min_numeric_agg = MinNumericAggregator(catalog_name);
  functions.push_back(std::move(min_numeric_agg));
  auto max_numeric_agg = MaxNumericAggregator(catalog_name);
  functions.push_back(std::move(max_numeric_agg));
  auto sum_agg = SumAggregator(catalog_name);
  functions.push_back(std::move(sum_agg));
  auto avg_agg = AvgAggregator(catalog_name);
  functions.push_back(std::move(avg_agg));

  auto array_length_func =
      ArrayUpperFunction(catalog_name, kPGArrayLengthFunctionName);
  functions.push_back(std::move(array_length_func));

  auto array_upper_func =
      ArrayUpperFunction(catalog_name, kPGArrayUpperFunctionName);
  functions.push_back(std::move(array_upper_func));

  auto textregexne_func = TextregexneFunction(catalog_name);
  functions.push_back(std::move(textregexne_func));

  auto date_mi_func = DateMiFunction(catalog_name);
  functions.push_back(std::move(date_mi_func));
  auto date_mii_func = DateMiiFunction(catalog_name);
  functions.push_back(std::move(date_mii_func));
  auto date_pli_func = DatePliFunction(catalog_name);
  functions.push_back(std::move(date_pli_func));

  auto to_date_func = ToDateFunction(catalog_name);
  functions.push_back(std::move(to_date_func));
  auto to_timestamp_func = ToTimestampFunction(catalog_name);
  functions.push_back(std::move(to_timestamp_func));
  auto to_char_func = ToCharFunction(catalog_name);
  functions.push_back(std::move(to_char_func));
  auto to_number_func = ToNumberFunction(catalog_name);
  functions.push_back(std::move(to_number_func));

  auto quote_ident_func = QuoteIdentFunction(catalog_name);
  functions.push_back(std::move(quote_ident_func));
  auto regexp_match_func = RegexpMatchFunction(catalog_name);
  functions.push_back(std::move(regexp_match_func));
  auto regexp_split_to_array_func = RegexpSplitToArrayFunction(catalog_name);
  functions.push_back(std::move(regexp_split_to_array_func));
  auto substring_func = SubstringFunction(catalog_name);
  functions.push_back(std::move(substring_func));

  auto to_jsonb_func = ToJsonBFunction(catalog_name);
  functions.push_back(std::move(to_jsonb_func));
  auto jsonb_subscript_text_func = JsonBSubscriptTextFunction(catalog_name);
  functions.push_back(std::move(jsonb_subscript_text_func));
  auto jsonb_array_element_func = JsonBArrayElementFunction(catalog_name);
  functions.push_back(std::move(jsonb_array_element_func));
  auto jsonb_object_field_func = JsonBObjectFieldFunction(catalog_name);
  functions.push_back(std::move(jsonb_object_field_func));
  auto jsonb_typeof_func = JsonBTypeofFunction(catalog_name);
  functions.push_back(std::move(jsonb_typeof_func));

  auto numeric_abs_func = NumericAbsFunction(catalog_name);
  functions.push_back(std::move(numeric_abs_func));
  auto numeric_add_func = NumericAddFunction(catalog_name);
  functions.push_back(std::move(numeric_add_func));
  auto numeric_ceil_func = NumericCeilFunction(catalog_name);
  functions.push_back(std::move(numeric_ceil_func));
  auto numeric_ceiling_func = NumericCeilingFunction(catalog_name);
  functions.push_back(std::move(numeric_ceiling_func));
  auto numeric_divide_func = NumericDivideFunction(catalog_name);
  functions.push_back(std::move(numeric_divide_func));
  auto numeric_div_trunc_func = NumericDivTruncFunction(catalog_name);
  functions.push_back(std::move(numeric_div_trunc_func));
  auto numeric_floor_func = NumericFloorFunction(catalog_name);
  functions.push_back(std::move(numeric_floor_func));
  auto numeric_mod_func = NumericModFunction(catalog_name);
  functions.push_back(std::move(numeric_mod_func));
  auto numeric_multiply_func = NumericMultiplyFunction(catalog_name);
  functions.push_back(std::move(numeric_multiply_func));
  auto numeric_subtract_func = NumericSubtractFunction(catalog_name);
  functions.push_back(std::move(numeric_subtract_func));
  auto numeric_trunc_func = NumericTruncFunction(catalog_name);
  functions.push_back(std::move(numeric_trunc_func));
  auto numeric_uminus_func = NumericUminusFunction(catalog_name);
  functions.push_back(std::move(numeric_uminus_func));
  auto numeric_cast_to_int64_func = NumericCastToInt64Function(catalog_name);
  functions.push_back(std::move(numeric_cast_to_int64_func));
  auto numeric_cast_to_double_func = NumericCastToDoubleFunction(catalog_name);
  functions.push_back(std::move(numeric_cast_to_double_func));
  auto cast_to_numeric_func = CastToNumericFunction(catalog_name);
  functions.push_back(std::move(cast_to_numeric_func));
  auto numeric_cast_to_string_func = NumericCastToStringFunction(catalog_name);
  functions.push_back(std::move(numeric_cast_to_string_func));

  return functions;
}

}  // namespace postgres_translator
