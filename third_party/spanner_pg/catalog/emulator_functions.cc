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

#include <stdbool.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "zetasql/public/function.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/functions/arithmetics.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/functions/json.h"
#include "zetasql/public/interval_value.h"
#include "zetasql/public/json_value.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "absl/algorithm/container.h"
#include "absl/base/casts.h"
#include "absl/base/optimization.h"
#include "absl/flags/flag.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "zetasql/base/mathutil.h"
#include "zetasql/common/string_util.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/time/time.h"
#include "third_party/spanner_pg/catalog/emulator_function_evaluators.h"
#include "third_party/spanner_pg/catalog/jsonb_array_elements_table_valued_function.h"
#include "third_party/spanner_pg/datatypes/common/jsonb/jsonb_value.h"
#include "third_party/spanner_pg/datatypes/common/pg_numeric_parse.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_conversion_functions.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_numeric_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_oid_type.h"
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
using spangres::datatypes::CreatePgJsonbValueFromNormalized;
using spangres::datatypes::CreatePgNumericValue;
using spangres::datatypes::CreatePgNumericValueWithMemoryContext;
using spangres::datatypes::CreatePgNumericValueWithPrecisionAndScale;
using spangres::datatypes::GetPgJsonbNormalizedValue;
using spangres::datatypes::GetPgNumericNormalizedValue;
using spangres::datatypes::GetPgOidValue;
using spangres::datatypes::common::jsonb::IsValidJsonbString;
using spangres::datatypes::common::jsonb::SerializeJsonbString;
using spangres::datatypes::common::jsonb::PgJsonbValue;
using spangres::datatypes::common::jsonb::TreeNode;

using ::zetasql::FunctionArgumentType;
using ::zetasql::FunctionArgumentTypeOptions;
using ::zetasql::FunctionOptions;
using ::zetasql::FunctionSignature;
using ::zetasql::FunctionSignatureOptions;
using ::zetasql::FunctionSignatureRewriteOptions;

using MathUtil = ::zetasql_base::MathUtil;

const zetasql::Type* gsql_bool = zetasql::types::BoolType();
const zetasql::Type* gsql_bytes = zetasql::types::BytesType();
const zetasql::Type* gsql_date = zetasql::types::DateType();
const zetasql::Type* gsql_float = zetasql::types::FloatType();
const zetasql::Type* gsql_double = zetasql::types::DoubleType();
const zetasql::Type* gsql_int64 = zetasql::types::Int64Type();
const zetasql::Type* gsql_string = zetasql::types::StringType();
const zetasql::Type* gsql_timestamp = zetasql::types::TimestampType();

const zetasql::Type* gsql_interval = zetasql::types::IntervalType();
const zetasql::Type* gsql_interval_array =
    zetasql::types::IntervalArrayType();

const zetasql::ArrayType* gsql_bool_array = zetasql::types::BoolArrayType();
const zetasql::ArrayType* gsql_bytes_array =
    zetasql::types::BytesArrayType();
const zetasql::ArrayType* gsql_date_array = zetasql::types::DateArrayType();
const zetasql::ArrayType* gsql_double_array =
    zetasql::types::DoubleArrayType();
const zetasql::ArrayType* gsql_float_array =
    zetasql::types::FloatArrayType();
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

using ::postgres_translator::EmulatorJsonbArrayElementText;
using ::postgres_translator::EmulatorJsonbObjectFieldText;
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
using ::postgres_translator::function_evaluators::Float4ToChar;
using ::postgres_translator::function_evaluators::Float8ToChar;
using ::postgres_translator::function_evaluators::Floor;
using ::postgres_translator::function_evaluators::Int8ToChar;
using ::postgres_translator::function_evaluators::JsonbArrayElement;
using ::postgres_translator::function_evaluators::JsonbObjectField;
using ::postgres_translator::function_evaluators::JsonbTypeof;
using ::postgres_translator::function_evaluators::Mod;
using ::postgres_translator::function_evaluators::Multiply;
using ::postgres_translator::function_evaluators::NumericToChar;
using ::postgres_translator::function_evaluators::NumericToNumber;
using ::postgres_translator::function_evaluators::PgDateExtract;
using ::postgres_translator::function_evaluators::PgTimestamptzAdd;
using ::postgres_translator::function_evaluators::PgTimestamptzBin;
using ::postgres_translator::function_evaluators::PgTimestamptzExtract;
using ::postgres_translator::function_evaluators::PgTimestamptzSubtract;
using ::postgres_translator::function_evaluators::PgTimestampTzToChar;
using ::postgres_translator::function_evaluators::PgTimestamptzTrunc;
using ::postgres_translator::function_evaluators::PgToDate;
using ::postgres_translator::function_evaluators::RegexpMatch;
using ::postgres_translator::function_evaluators::RegexpSplitToArray;
using ::postgres_translator::function_evaluators::Subtract;
using ::postgres_translator::function_evaluators::Textregexne;
using ::postgres_translator::function_evaluators::Textregexsubstr;
using ::postgres_translator::function_evaluators::ToTimestamp;
using ::postgres_translator::function_evaluators::Trunc;
using ::postgres_translator::function_evaluators::UnaryMinus;

using ::postgres_translator::function_evaluators::PgIntervalDivide;
using ::postgres_translator::function_evaluators::PgIntervalExtract;
using ::postgres_translator::function_evaluators::PgIntervalIn;
using ::postgres_translator::function_evaluators::PgIntervalMultiply;
using ::postgres_translator::function_evaluators::PgIntervalOut;
using ::postgres_translator::function_evaluators::PgIntervalToChar;
using ::postgres_translator::function_evaluators::PgMakeInterval;

using ::postgres_translator::InitializePGTimezoneToDefault;

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
                                       {gsql_float_array, gsql_int64},
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
                                       /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{gsql_int64,
                                       {gsql_interval_array, gsql_int64},
                                       /*context_ptr=*/nullptr},
      },
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
  function_options.set_evaluator(PGFunctionEvaluator(
      EvalTextregexne, InitializePGTimezoneToDefault, CleanupRegexCache));
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
  function_options.set_evaluator(PGFunctionEvaluator(
      EvalToDate, InitializePGTimezoneToDefault, CleanupPostgresDateTimeCache));
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
      PGFunctionEvaluator(EvalToTimestamp, InitializePGTimezoneToDefault,
                          CleanupPostgresDateTimeCache));
  return std::make_unique<zetasql::Function>(
      kPGToTimestampFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          gsql_timestamp, {gsql_string, gsql_string}, /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::IntervalValue> RoundPrecision(
    const zetasql::IntervalValue& interval) {
  int64_t micros = MathUtil::Round<int64_t>(
      (interval.get_nanos() * 1.0) / zetasql::IntervalValue::kNanosInMicro);
  return zetasql::IntervalValue::FromMonthsDaysMicros(
      interval.get_months(), interval.get_days(), micros);
}

std::unique_ptr<zetasql::Function> ToCharFunction(
    absl::string_view catalog_name) {
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      PGFunctionEvaluator(EvalToChar, InitializePGTimezoneToDefault, [] {
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
                                       {gsql_float, gsql_string},
                                       /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{gsql_string,
                                       {gsql_pg_numeric, gsql_string},
                                       /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{gsql_string,
                                       {gsql_interval, gsql_string},
                                       /*context_ptr=*/nullptr},
      },
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
  function_options.set_evaluator(PGFunctionEvaluator(
      EvalToNumber, InitializePGTimezoneToDefault, CleanupPostgresNumberCache));

  return std::make_unique<zetasql::Function>(
      kPGToNumberFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{{gsql_pg_numeric,
                                                 {gsql_string, gsql_string},
                                                 /*context_ptr=*/nullptr}},
      function_options);
}

// PG.NUMERIC Mathematical functions

absl::StatusOr<zetasql::Value> EvalZetaSQLAbs(
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
  function_options.set_evaluator(PGFunctionEvaluator(EvalZetaSQLAbs));
  return std::make_unique<zetasql::Function>(
    kZetaSQLAbsFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalZetaSQLAdd(
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
  function_options.set_evaluator(PGFunctionEvaluator(EvalZetaSQLAdd));

  return std::make_unique<zetasql::Function>(
      kZetaSQLAddFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          {gsql_pg_numeric,
           {gsql_pg_numeric, gsql_pg_numeric},
           /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalZetaSQLCeil(
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
  function_options.set_evaluator(PGFunctionEvaluator(EvalZetaSQLCeil));
  function_options.set_alias_name("ceiling");
  return std::make_unique<zetasql::Function>(
      kZetaSQLCeilFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalZetaSQLDivide(
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
  function_options.set_evaluator(PGFunctionEvaluator(EvalZetaSQLDivide));

  return std::make_unique<zetasql::Function>(
      kZetaSQLDivideFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          {gsql_pg_numeric,
           {gsql_pg_numeric, gsql_pg_numeric},
           /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalZetaSQLDivTrunc(
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
  function_options.set_evaluator(PGFunctionEvaluator(EvalZetaSQLDivTrunc));

  return std::make_unique<zetasql::Function>(
      kZetaSQLDivTruncFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          {gsql_pg_numeric,
           {gsql_pg_numeric, gsql_pg_numeric},
           /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalZetaSQLFloor(
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
  function_options.set_evaluator(PGFunctionEvaluator(EvalZetaSQLFloor));
  return std::make_unique<zetasql::Function>(
      kZetaSQLFloorFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalZetaSQLMod(
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
  function_options.set_evaluator(PGFunctionEvaluator(EvalZetaSQLMod));
  return std::make_unique<zetasql::Function>(
      kZetaSQLModFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_pg_numeric,
                                       {gsql_pg_numeric, gsql_pg_numeric},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalZetaSQLMultiply(
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
  function_options.set_evaluator(PGFunctionEvaluator(EvalZetaSQLMultiply));

  return std::make_unique<zetasql::Function>(
      kZetaSQLMultiplyFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          {gsql_pg_numeric,
           {gsql_pg_numeric, gsql_pg_numeric},
           /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalZetaSQLSubtract(
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
  function_options.set_evaluator(PGFunctionEvaluator(EvalZetaSQLSubtract));

  return std::make_unique<zetasql::Function>(
      kZetaSQLSubtractFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          {gsql_pg_numeric,
           {gsql_pg_numeric, gsql_pg_numeric},
           /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalZetaSQLTrunc(
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
  function_options.set_evaluator(PGFunctionEvaluator(EvalZetaSQLTrunc));
  return std::make_unique<zetasql::Function>(
      kZetaSQLTruncFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_pg_numeric,
                                       {gsql_pg_numeric, gsql_int64},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalZetaSQLUminus(
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
  function_options.set_evaluator(PGFunctionEvaluator(EvalZetaSQLUminus));
  return std::make_unique<zetasql::Function>(
      kZetaSQLUminusFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr}},
      function_options);
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
              gsql_pg_numeric, {gsql_float}, /*context_ptr=*/nullptr},
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

std::unique_ptr<zetasql::Function> CastNumericToDoubleFunction(
    absl::string_view catalog_name) {
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalCastNumericToDouble));
  return std::make_unique<zetasql::Function>(
      kPGCastNumericToDoubleFunctionName, catalog_name,
      zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          gsql_double, {gsql_pg_numeric}, /*context_ptr=*/nullptr}},
      function_options);
}

std::unique_ptr<zetasql::Function> CastNumericToFloatFunction(
    absl::string_view catalog_name) {
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalCastNumericToFloat));
  return std::make_unique<zetasql::Function>(
      kPGCastNumericToFloatFunctionName, catalog_name,
      zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          gsql_float, {gsql_pg_numeric}, /*context_ptr=*/nullptr}},
      function_options);
}

std::unique_ptr<zetasql::Function> CastToStringFunction(
    absl::string_view catalog_name) {
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalCastToString));

  return std::make_unique<zetasql::Function>(
      kPGCastToStringFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{
              gsql_string, {gsql_pg_numeric}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_string, {gsql_interval}, /*context_ptr=*/nullptr},
      },
      function_options);
}

std::unique_ptr<zetasql::Function> CastNumericToInt64Function(
    absl::string_view catalog_name) {
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalCastNumericToInt64));
  return std::make_unique<zetasql::Function>(
      kPGCastNumericToInt64FunctionName, catalog_name,
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
  function_options.set_evaluator(PGFunctionEvaluator(
      EvalRegexpMatch, InitializePGTimezoneToDefault, CleanupRegexCache));
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
      PGFunctionEvaluator(EvalRegexpSplitToArray, InitializePGTimezoneToDefault,
                          CleanupRegexCache));
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
  function_options.set_evaluator(PGFunctionEvaluator(
      EvalSubstring, InitializePGTimezoneToDefault, CleanupRegexCache));
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
      PGFunctionEvaluator(EvalCastToDate, InitializePGTimezoneToDefault,
                          CleanupPostgresDateTimeCache));

  return std::make_unique<zetasql::Function>(
      kPGCastToDateFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{zetasql::types::DateType(),
                                       {zetasql::types::StringType()},
                                       nullptr}},
      function_options);
}

std::unique_ptr<zetasql::Function> ArrayOverlapFunction(
    const std::string& catalog_name) {
  constexpr absl::string_view kArrayOverlapSql = R"sql(
                  CASE
                    WHEN array_to_search IS NULL OR search_values is NULL
                    THEN NULL
                  ELSE
                    EXISTS(
                      SELECT 1 FROM UNNEST(array_to_search) AS element WHERE
                      element IN UNNEST (search_values))
                  END
                )sql";
  FunctionArgumentType array_to_search_arg(
      zetasql::ARG_ARRAY_TYPE_ANY_1,
      FunctionArgumentTypeOptions()
          .set_array_element_must_support_equality()
          .set_argument_name("array_to_search", zetasql::kPositionalOnly));

  FunctionArgumentType search_values_arg(
      zetasql::ARG_ARRAY_TYPE_ANY_1,
      FunctionArgumentTypeOptions().set_argument_name(
          "search_values", zetasql::kPositionalOnly));

  FunctionSignature signature{
      gsql_bool,
      {array_to_search_arg, search_values_arg},
      /*context_id=*/-1,
      FunctionSignatureOptions().set_rewrite_options(
          FunctionSignatureRewriteOptions()
              .set_enabled(true)
              .set_rewriter(zetasql::REWRITE_BUILTIN_FUNCTION_INLINER)
              .set_sql(kArrayOverlapSql))};

  return std::make_unique<zetasql::Function>(
      "pg.array_overlap", catalog_name, zetasql::Function::SCALAR,
      std::vector<FunctionSignature>{signature}, FunctionOptions());
}

std::unique_ptr<zetasql::Function> ArrayContainsOrContainedFunction(
    const std::string& catalog_name, bool is_array_contains) {
  constexpr absl::string_view kArrayContainsSql = R"sql(
                  CASE
                    WHEN array_to_search IS NULL OR search_values is NULL THEN NULL
                    WHEN pg.array_length(search_values, 1) IS NULL THEN TRUE
                  ELSE
                    (SELECT LOGICAL_AND(
                      COALESCE(element IN UNNEST (array_to_search), FALSE))
                    FROM UNNEST(search_values) AS element)
                  END
                )sql";

  FunctionArgumentType array_to_search(
      zetasql::ARG_ARRAY_TYPE_ANY_1,
      FunctionArgumentTypeOptions()
          .set_array_element_must_support_equality()
          .set_argument_name("array_to_search", zetasql::kPositionalOnly));

  FunctionArgumentType search_values(
      zetasql::ARG_ARRAY_TYPE_ANY_1,
      FunctionArgumentTypeOptions()
          .set_array_element_must_support_equality()
          .set_argument_name("search_values", zetasql::kPositionalOnly));

  zetasql::FunctionArgumentTypeList argument_type_list;
  if (is_array_contains) {
    argument_type_list = {array_to_search, search_values};
  } else {
    argument_type_list = {search_values, array_to_search};
  }

  FunctionSignature signature{
      gsql_bool, argument_type_list,
      /*context_id=*/-1,
      FunctionSignatureOptions().set_rewrite_options(
          FunctionSignatureRewriteOptions()
              .set_enabled(true)
              .set_rewriter(zetasql::REWRITE_BUILTIN_FUNCTION_INLINER)
              .set_sql(kArrayContainsSql))};

  return std::make_unique<zetasql::Function>(
      is_array_contains ? "pg.array_contains" : "pg.array_contained",
      catalog_name, zetasql::Function::SCALAR,
      std::vector<FunctionSignature>{signature}, FunctionOptions());
}

std::unique_ptr<zetasql::Function> ArrayAllFunction(
    const std::string& catalog_name, const std::string& operator_str,
    const std::string& function_name) {
  constexpr absl::string_view kArrayAllTemplateSql = R"sql(
        CASE
          WHEN PG.ARRAY_LENGTH(array_to_search, 1) IS NULL THEN TRUE
          WHEN array_to_search IS NULL OR search_value is NULL THEN NULL
          WHEN
            (SELECT LOGICAL_or(element IS NULL) FROM UNNEST(array_to_search) as element) THEN NULL
        ELSE
          (SELECT LOGICAL_AND(search_value %s element) FROM UNNEST(array_to_search) AS element)
        END
      )sql";

  FunctionArgumentType array_to_search(
      zetasql::ARG_ARRAY_TYPE_ANY_1,
      FunctionArgumentTypeOptions()
          .set_array_element_must_support_equality()
          .set_argument_name("array_to_search", zetasql::kPositionalOnly));

  FunctionArgumentType search_value(
      zetasql::ARG_TYPE_ANY_1,
      FunctionArgumentTypeOptions().set_argument_name(
          "search_value", zetasql::kPositionalOnly));

  FunctionSignature signature{
      gsql_bool,
      {search_value, array_to_search},
      /*context_id=*/-1,
      FunctionSignatureOptions().set_rewrite_options(
          FunctionSignatureRewriteOptions()
              .set_enabled(true)
              .set_rewriter(zetasql::REWRITE_BUILTIN_FUNCTION_INLINER)
              .set_sql(absl::StrFormat(kArrayAllTemplateSql, operator_str)))};

  return std::make_unique<zetasql::Function>(
      function_name, catalog_name, zetasql::Function::SCALAR,
      std::vector<FunctionSignature>{signature}, FunctionOptions());
}

std::unique_ptr<zetasql::Function> ArraySliceFunction(
    const std::string& catalog_name) {
  // We add 1 to the offset (i.e., idx) because ZetaSQL returns zero-based
  // offset while Postgres array slicing expects one-based offset.
  constexpr absl::string_view kArraySliceSql =
      R"sql(
        CASE
          WHEN
            array_to_slice IS NULL
            OR start_offset IS NULL
            OR end_offset IS NULL
            THEN NULL
          WHEN PG.ARRAY_LENGTH(array_to_slice, 1) IS NULL
            THEN []
          ELSE
            ARRAY(
              SELECT e
              FROM UNNEST(array_to_slice) AS e WITH OFFSET AS idx
              WHERE start_offset <= (idx + 1) AND (idx + 1) <= end_offset
              ORDER BY idx nulls last)
        END
      )sql";
  FunctionArgumentType array_to_slice_arg(
      zetasql::ARG_ARRAY_TYPE_ANY_1,
      FunctionArgumentTypeOptions()
          .set_array_element_must_support_equality()
          .set_argument_name("array_to_slice", zetasql::kPositionalOnly));

  FunctionArgumentType start_offset_arg(
      gsql_int64, FunctionArgumentTypeOptions().set_argument_name(
                      "start_offset", zetasql::kPositionalOnly));

  FunctionArgumentType end_offset_arg(
      gsql_int64, FunctionArgumentTypeOptions().set_argument_name(
                      "end_offset", zetasql::kPositionalOnly));

  FunctionSignature signature{
      zetasql::ARG_ARRAY_TYPE_ANY_1,
      {array_to_slice_arg, start_offset_arg, end_offset_arg},
      /*context_id=*/-1,
      FunctionSignatureOptions().set_rewrite_options(
          FunctionSignatureRewriteOptions()
              .set_enabled(true)
              .set_rewriter(zetasql::REWRITE_BUILTIN_FUNCTION_INLINER)
              .set_sql(kArraySliceSql))};

  return std::make_unique<zetasql::Function>(
      "pg.array_slice", catalog_name, zetasql::Function::SCALAR,
      std::vector<FunctionSignature>{signature}, FunctionOptions());
}

std::unique_ptr<zetasql::Function> CastToTimestampFunction(
    const std::string& catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      PGFunctionEvaluator(EvalCastToTimestamp, InitializePGTimezoneToDefault,
                          CleanupPostgresDateTimeCache));

  return std::make_unique<zetasql::Function>(
      kPGCastToTimestampFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{zetasql::types::TimestampType(),
                                       {zetasql::types::StringType()},
                                       nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalTimestamptzAdd(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 2);

  if (args[1].type_kind() == zetasql::TYPE_INTERVAL) {
    ZETASQL_ASSIGN_OR_RETURN(zetasql::IntervalValue interval_arg,
                     RoundPrecision(args[1].interval_value()));
    ZETASQL_ASSIGN_OR_RETURN(absl::Time time,
                     PgTimestamptzAdd(args[0].ToTime(), interval_arg));
    return zetasql::Value::Timestamp(time);
  }

  ZETASQL_ASSIGN_OR_RETURN(absl::Time time,
                   PgTimestamptzAdd(args[0].ToTime(), args[1].string_value()));
  return zetasql::Value::Timestamp(time);
}

std::unique_ptr<zetasql::Function> TimestamptzAddFunction(
    const std::string& catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      PGFunctionEvaluator(EvalTimestamptzAdd, InitializePGTimezoneToDefault,
                          CleanupPostgresDateTimeCache));
  return std::make_unique<zetasql::Function>(
      kPGTimestamptzAddFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{zetasql::types::TimestampType(),
                                       {zetasql::types::TimestampType(),
                                        zetasql::types::StringType()},
                                       nullptr},
          zetasql::FunctionSignature{zetasql::types::TimestampType(),
                                       {zetasql::types::TimestampType(),
                                        zetasql::types::IntervalType()},
                                       nullptr},
      },
      function_options);
}

absl::StatusOr<zetasql::Value> EvalTimestamptzSubtract(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 2);

  if (args[1].type_kind() == zetasql::TYPE_INTERVAL) {
    ZETASQL_ASSIGN_OR_RETURN(zetasql::IntervalValue interval_arg,
                     RoundPrecision(args[1].interval_value()));
    ZETASQL_ASSIGN_OR_RETURN(absl::Time time,
                     PgTimestamptzSubtract(args[0].ToTime(), interval_arg));
    return zetasql::Value::Timestamp(time);
  }

  ZETASQL_ASSIGN_OR_RETURN(
      absl::Time time,
      PgTimestamptzSubtract(args[0].ToTime(), args[1].string_value()));
  return zetasql::Value::Timestamp(time);
}

std::unique_ptr<zetasql::Function> TimestamptzSubtractFunction(
    const std::string& catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(
      EvalTimestamptzSubtract, InitializePGTimezoneToDefault,
      CleanupPostgresDateTimeCache));

  return std::make_unique<zetasql::Function>(
      kPGTimestamptzSubtractFunctionName, catalog_name,
      zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{zetasql::types::TimestampType(),
                                       {zetasql::types::TimestampType(),
                                        zetasql::types::StringType()},
                                       nullptr},
          zetasql::FunctionSignature{zetasql::types::TimestampType(),
                                       {zetasql::types::TimestampType(),
                                        zetasql::types::IntervalType()},
                                       nullptr},
      },
      function_options);
}

absl::StatusOr<zetasql::Value> EvalTimestamptzBin(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 3);
  ZETASQL_ASSIGN_OR_RETURN(absl::Time time,
                   PgTimestamptzBin(args[0].string_value(), args[1].ToTime(),
                                    args[2].ToTime()));
  return zetasql::Value::Timestamp(time);
}

std::unique_ptr<zetasql::Function> TimestamptzBinFunction(
    const std::string& catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      PGFunctionEvaluator(EvalTimestamptzBin, InitializePGTimezoneToDefault,
                          CleanupPostgresDateTimeCache));

  return std::make_unique<zetasql::Function>(
      kPGTimestamptzBinFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          zetasql::types::TimestampType(),
          {zetasql::types::StringType(), zetasql::types::TimestampType(),
           zetasql::types::TimestampType()},
          nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalTimestamptzTrunc(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 2 || args.size() == 3);
  if (args.size() == 2) {
    ZETASQL_ASSIGN_OR_RETURN(absl::Time time, PgTimestamptzTrunc(args[0].string_value(),
                                                         args[1].ToTime()));
    return zetasql::Value::Timestamp(time);
  } else {
    ZETASQL_ASSIGN_OR_RETURN(
        absl::Time time,
        PgTimestamptzTrunc(args[0].string_value(), args[1].ToTime(),
                           args[2].string_value()));
    return zetasql::Value::Timestamp(time);
  }
}

std::unique_ptr<zetasql::Function> TimestamptzTruncFunction(
    const std::string& catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      PGFunctionEvaluator(EvalTimestamptzTrunc, InitializePGTimezoneToDefault,
                          CleanupPostgresDateTimeCache));

  return std::make_unique<zetasql::Function>(
      kPGTimestamptzTruncFunctionName, catalog_name,
      zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{zetasql::types::TimestampType(),
                                       {zetasql::types::StringType(),
                                        zetasql::types::TimestampType()},
                                       nullptr},
          zetasql::FunctionSignature{zetasql::types::TimestampType(),
                                       {zetasql::types::StringType(),
                                        zetasql::types::TimestampType(),
                                        zetasql::types::StringType()},
                                       nullptr}},
      function_options);
}

std::unique_ptr<zetasql::Function> ExtractFunction(
    const std::string& catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      PGFunctionEvaluator(EvalExtract, InitializePGTimezoneToDefault,
                          CleanupPostgresDateTimeCache));

  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();
  return std::make_unique<zetasql::Function>(
      kPGExtractFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_pg_numeric,
                                       {zetasql::types::StringType(),
                                        zetasql::types::TimestampType()},
                                       nullptr},
          zetasql::FunctionSignature{
              gsql_pg_numeric,
              {zetasql::types::StringType(), zetasql::types::DateType()},
              nullptr}},
      function_options);
}

// Maps float and double value to an integer value in such a way, that the
// PostgreSQL sort order/comparison semantics of FLOAT4 and FLOAT8 type
// values is preserved in the order of obtained (after mapping) int64_t values
// ({input x < input y} => {output for x < output for y}).
//
// PostgreSQL FLOAT4 or FLOAT8 comparison semantic rules are as follows:
// * All Nan values are equal (including negative).
// * Nan value is bigger than any other non-null floating point value.
// * Negative zero (-0.0) is equal to positive zero (0.0).
template <typename T, typename = std::enable_if_t<std::is_floating_point_v<T>>>
absl::StatusOr<zetasql::Value> EvalMapFloatingPointToInt(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 1);

  double num = static_cast<double>(args[0].Get<T>());

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
      zetasql::FunctionEvaluator(EvalMapFloatingPointToInt<double>));

  return std::make_unique<zetasql::Function>(
      kPGMapDoubleToIntFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{zetasql::types::Int64Type(),
                                       {zetasql::types::DoubleType()},
                                       nullptr}},
      function_options);
}

std::unique_ptr<zetasql::Function> MapFloatToIntFunction(
    const std::string& catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      zetasql::FunctionEvaluator(EvalMapFloatingPointToInt<float>));

  return std::make_unique<zetasql::Function>(
      kPGMapFloatToIntFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{zetasql::types::Int64Type(),
                                       {zetasql::types::FloatType()},
                                       nullptr}},
      function_options);
}

// PG Cast functions
absl::StatusOr<zetasql::Value> EvalToJsonbFromValue(zetasql::Value arg);

absl::StatusOr<zetasql::Value> EvalToJsonb(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 1);
  return EvalToJsonbFromValue(args[0]);
}

// Converts a `Value` to its unquoted string representation. `null` value is
// printed as a `null_string`. The return string from certain Value types may be
// normalized later when converted to PG.JSONB by calling
// `EvalToJsonbFrom<Type>`. Otherwise, this function does not guarantee a
// normalized return.
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
    case zetasql::TYPE_FLOAT:
      return zetasql::RoundTripFloatToString(value.float_value());
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
    case zetasql::TYPE_INTERVAL: {
      std::string interval_string;
      ZETASQL_ASSIGN_OR_RETURN(zetasql::IntervalValue interval,
                       RoundPrecision(value.interval_value()));
      ZETASQL_ASSIGN_OR_RETURN(interval_string, PgIntervalOut(interval));
      return interval_string;
    }
    case zetasql::TYPE_ARRAY: {
      if (value.empty()) {
        return "[]";
      }
      absl::Cord ret_cord;
      ret_cord.Append("[");
      ret_cord.Append(GetStringRepresentation(
                          EvalToJsonbFromValue(value.element(0)).value())
                          .value());
      for (int i = 1; i < value.num_elements(); ++i) {
        ret_cord.Append(", ");
        ret_cord.Append(GetStringRepresentation(
                            EvalToJsonbFromValue(value.element(i)).value())
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
        case spangres::datatypes::TypeAnnotationCode::PG_OID:
          return absl::StrCat(GetPgOidValue(value).value());
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
absl::StatusOr<zetasql::Value> EvalToJsonbFromInt64(
    const zetasql::Value arg) {
  return CreatePgJsonbValue(GetStringRepresentation(arg).value());
}

// Returns a normalized PG.JSONB value from the bool input.
absl::StatusOr<zetasql::Value> EvalToJsonbFromBool(
    const zetasql::Value arg) {
  return CreatePgJsonbValue(GetStringRepresentation(arg).value());
}

// Returns a normalized PG.JSONB value from the floating point input.
template <typename T, typename = std::enable_if_t<std::is_floating_point_v<T>>>
absl::StatusOr<zetasql::Value> EvalToJsonbFromFloatingPoint(
    const zetasql::Value arg) {
  if (std::isnan(arg.Get<T>())) {
    return CreatePgJsonbValueFromNormalized(absl::Cord(kNanString));
  }
  if (std::isinf(arg.Get<T>())) {
    return arg.Get<T>() > 0
               ? CreatePgJsonbValueFromNormalized(absl::Cord(kInfString))
               : CreatePgJsonbValueFromNormalized(absl::Cord(kNegInfString));
  }
  return CreatePgJsonbValue(GetStringRepresentation(arg).value());
}

// Returns a normalized PG.JSONB value from the string input.
absl::StatusOr<zetasql::Value> EvalToJsonbFromString(
    const zetasql::Value arg) {
  if (IsValidJsonbString(arg.string_value())) {
    return CreatePgJsonbValue(
        SerializeJsonbString(GetStringRepresentation(arg).value()));
  }
  return absl::InvalidArgumentError(
      "unsupported Unicode escape sequence DETAIL: \\u0000 cannot "
      "be converted to text.");
}

// Returns a normalized PG.JSONB value from the bytes input.
absl::StatusOr<zetasql::Value> EvalToJsonbFromBytes(
    const zetasql::Value arg) {
  return EvalToJsonbFromString(
      zetasql::values::String(GetStringRepresentation(arg).value()));
}

// Returns a normalized PG.JSONB value from the date input.
absl::StatusOr<zetasql::Value> EvalToJsonbFromDate(
    const zetasql::Value arg) {
  return CreatePgJsonbValue(
      absl::StrCat("\"", GetStringRepresentation(arg).value(), "\""));
}

// Returns a normalized PG.JSONB value from the timestamp input.
absl::StatusOr<zetasql::Value> EvalToJsonbFromTimestamp(
    const zetasql::Value arg) {
  return CreatePgJsonbValue(
      absl::StrCat("\"", GetStringRepresentation(arg).value(), "\""));
}

// Returns a normalized PG.JSONB value from the array input.
absl::StatusOr<zetasql::Value> EvalToJsonbFromArray(
    const zetasql::Value arg) {
  return CreatePgJsonbValue(GetStringRepresentation(arg).value());
}

// Returns a normalized PG.JSONB value from the PG.JSONB input.
absl::StatusOr<zetasql::Value> EvalToJsonbFromPgJsonb(
    const zetasql::Value arg) {
  return arg;
}

// Returns a normalized PG.JSONB value from the PG.NUMERIC input.
absl::StatusOr<zetasql::Value> EvalToJsonbFromPgNumeric(
    const zetasql::Value arg) {
  if (std::string(GetPgNumericNormalizedValue(arg).value()) == kNan) {
    return CreatePgJsonbValueFromNormalized(absl::Cord(kNanString));
  }

  return CreatePgJsonbValue(GetStringRepresentation(arg).value());
}

// Returns a normalized PG.JSONB value from the PG.OID input.
absl::StatusOr<zetasql::Value> EvalToJsonbFromPgOid(
    const zetasql::Value arg) {
  return CreatePgJsonbValue(GetStringRepresentation(arg).value());
}

// Returns a normalized PG.JSONB value from the extended type input.
absl::StatusOr<zetasql::Value> EvalToJsonbFromExtended(
    const zetasql::Value arg) {
  auto type_code =
      static_cast<const spangres::datatypes::SpannerExtendedType*>(arg.type())
          ->code();
  switch (type_code) {
    case spangres::datatypes::TypeAnnotationCode::PG_JSONB:
      return EvalToJsonbFromPgJsonb(arg);
    case spangres::datatypes::TypeAnnotationCode::PG_NUMERIC:
      return EvalToJsonbFromPgNumeric(arg);
    case spangres::datatypes::TypeAnnotationCode::PG_OID:
      return EvalToJsonbFromPgOid(arg);
    default:
      ZETASQL_RET_CHECK_FAIL() << "Encountered unexpected type " << arg.type_kind();
  }
}

// Returns a normalized PG.JSONB value from the int64_t input.
absl::StatusOr<zetasql::Value> EvalToJsonbFromInterval(
    const zetasql::Value arg) {
  return CreatePgJsonbValue(
      absl::StrCat("\"", GetStringRepresentation(arg).value(), "\""));
}

// Returns a normalized PG.JSONB value from the input.
absl::StatusOr<zetasql::Value> EvalToJsonbFromValue(
    const zetasql::Value arg) {
  if (arg.is_null()) {
    // `null` input results in `null` JSONB value.
    return zetasql::values::Null(spangres::datatypes::GetPgJsonbType());
  }

  zetasql::TypeKind type_kind = arg.type_kind();
  switch (type_kind) {
    case zetasql::TYPE_INT64:
      return EvalToJsonbFromInt64(arg);
    case zetasql::TYPE_BOOL:
      return EvalToJsonbFromBool(arg);
    case zetasql::TYPE_DOUBLE:
      return EvalToJsonbFromFloatingPoint<double>(arg);
    case zetasql::TYPE_FLOAT:
      return EvalToJsonbFromFloatingPoint<float>(arg);
    case zetasql::TYPE_STRING:
      return EvalToJsonbFromString(arg);
    case zetasql::TYPE_BYTES:
      return EvalToJsonbFromBytes(arg);
    case zetasql::TYPE_DATE:
      return EvalToJsonbFromDate(arg);
    case zetasql::TYPE_TIMESTAMP:
      return EvalToJsonbFromTimestamp(arg);
    case zetasql::TYPE_ARRAY:
      return EvalToJsonbFromArray(arg);
    case zetasql::TYPE_EXTENDED:
      return EvalToJsonbFromExtended(arg);
    case zetasql::TYPE_INTERVAL:
      return EvalToJsonbFromInterval(arg);
    default:
      ZETASQL_RET_CHECK_FAIL() << "Encountered unexpected type " << type_kind;
  }
}

std::unique_ptr<zetasql::Function> ToJsonbFunction(
    absl::string_view catalog_name) {
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();
  static const zetasql::Type* gsql_pg_jsonb =
      spangres::datatypes::GetPgJsonbType();
  static const zetasql::Type* gsql_pg_numeric_array =
      spangres::datatypes::GetPgNumericArrayType();
  static const zetasql::Type* gsql_pg_jsonb_array =
      spangres::datatypes::GetPgJsonbArrayType();
  static const zetasql::Type* gsql_pg_oid =
      spangres::datatypes::GetPgOidType();
  static const zetasql::Type* gsql_pg_oid_array =
      spangres::datatypes::GetPgOidArrayType();

  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalToJsonb));
  return std::make_unique<zetasql::Function>(
      kPGToJsonbFunctionName, catalog_name, zetasql::Function::SCALAR,
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
              gsql_pg_jsonb, {gsql_float}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_pg_jsonb, {gsql_float_array}, /*context_ptr=*/nullptr},
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
          zetasql::FunctionSignature{
              gsql_pg_jsonb, {gsql_pg_oid}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_pg_jsonb, {gsql_pg_oid_array}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_pg_jsonb, {gsql_interval}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_pg_jsonb, {gsql_interval_array}, /*context_ptr=*/nullptr},
      },
      function_options);
}

// Returns a normalized PG.JSONB value from the input.
template <zetasql::TypeKind T>
absl::StatusOr<zetasql::Value> EvalCastFromJsonb(
    absl::Span<const zetasql::Value> args) {
  switch (T) {
    case zetasql::TYPE_INT64:
      return spangres::datatypes::PgJsonbToInt64Conversion(args);
    case zetasql::TYPE_BOOL:
      return spangres::datatypes::PgJsonbToBoolConversion(args);
    case zetasql::TYPE_DOUBLE:
      return spangres::datatypes::PgJsonbToDoubleConversion(args);
    case zetasql::TYPE_FLOAT:
      return spangres::datatypes::PgJsonbToFloatConversion(args);
    case zetasql::TYPE_STRING:
      return spangres::datatypes::PgJsonbToStringConversion(args);
    default:
      return absl::InvalidArgumentError(
          absl::StrCat("cannot cast jsonb object to type ", T));
  }
}

std::unique_ptr<zetasql::Function> CastFromJsonbFunction(
    absl::string_view catalog_name) {
  static const zetasql::Type* gsql_pg_jsonb =
      spangres::datatypes::GetPgJsonbType();

  zetasql::FunctionEvaluatorFactory evaluator_factory(
      [&](const zetasql::FunctionSignature& signature)
          -> absl::StatusOr<zetasql::FunctionEvaluator> {
        if (signature.result_type().type()->IsInt64()) {
          return EvalCastFromJsonb<zetasql::TYPE_INT64>;
        } else if (signature.result_type().type()->IsBool()) {
          return EvalCastFromJsonb<zetasql::TYPE_BOOL>;
        } else if (signature.result_type().type()->IsDouble()) {
          return EvalCastFromJsonb<zetasql::TYPE_DOUBLE>;
        } else if (signature.result_type().type()->IsString()) {
          return EvalCastFromJsonb<zetasql::TYPE_STRING>;
        } else {
          return absl::InvalidArgumentError(
              absl::StrCat("cannot cast jsonb object to type ",
                           signature.result_type().type()->DebugString()));
        }
      });
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator_factory(evaluator_factory);
  return std::make_unique<zetasql::Function>(
      kPGCastFromJsonbFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{
              gsql_bool, {gsql_pg_jsonb}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_double, {gsql_pg_jsonb}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_int64, {gsql_pg_jsonb}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_string, {gsql_pg_jsonb}, /*context_ptr=*/nullptr},
      },
      function_options);
}

absl::StatusOr<zetasql::Value> EvalJsonbSubscriptText(
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
    return EmulatorJsonbArrayElementText(jsonb, element);
  } else {
    const std::string key(args[1].string_value());
    return EmulatorJsonbObjectFieldText(jsonb, key);
  }
}

std::unique_ptr<zetasql::Function> JsonbSubscriptTextFunction(
    absl::string_view catalog_name) {
  const zetasql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbSubscriptText));
  return std::make_unique<zetasql::Function>(
      kPGJsonbSubscriptTextFunctionName, catalog_name,
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

absl::StatusOr<zetasql::Value> EvalSubscript(
  absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 2);
  if ((args[1].type_kind() != zetasql::TYPE_INT64) &&
      (args[1].type_kind() != zetasql::TYPE_STRING)) {
    return absl::UnimplementedError(absl::StrCat(
        "$subscript(PG.JSONB, ", args[1].type()->DebugString(), ")"));
  }
  if (args[0].is_null() || args[1].is_null()) {
    return zetasql::Value::Null(
        postgres_translator::spangres::datatypes::GetPgJsonbType());
  }

  ZETASQL_ASSIGN_OR_RETURN(absl::Cord jsonb, GetPgJsonbNormalizedValue(args[0]));
  if (args[1].type_kind() == zetasql::TYPE_INT64) {
    return JsonbArrayElement(std::string(jsonb), args[1].int64_value());
  } else {
  return JsonbObjectField(std::string(jsonb), args[1].string_value());
  }
}

std::unique_ptr<zetasql::Function> ZetaSQLSubscriptFunction(
    absl::string_view catalog_name) {
  const zetasql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalSubscript));
  return std::make_unique<zetasql::Function>(
      kZetaSQLSubscriptFunctionName, catalog_name,
      zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_pg_jsonb,
                                       {gsql_pg_jsonb, gsql_int64},
                                       /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{gsql_pg_jsonb,
                                       {gsql_pg_jsonb, gsql_string},
                                       /*context_ptr=*/nullptr},
      },
      function_options);
}

absl::StatusOr<zetasql::Value> EvalJsonbTypeof(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 1);
  if (args[0].is_null()) {
    return zetasql::Value::NullString();
  }

  ZETASQL_ASSIGN_OR_RETURN(absl::Cord jsonb, GetPgJsonbNormalizedValue(args[0]));
  return JsonbTypeof(std::string(jsonb));
}

std::unique_ptr<zetasql::Function> JsonbTypeofFunction(
    absl::string_view catalog_name) {
  const zetasql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbTypeof));
  return std::make_unique<zetasql::Function>(
      kZetaSQLJsonTypeFunctionName, catalog_name,
      zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_string,
                                       {gsql_pg_jsonb},
                                       /*context_ptr=*/nullptr},
      },
      function_options);
}

absl::StatusOr<zetasql::Value> EvalJsonbQueryArray(
    absl::Span<const zetasql::Value> args) {
  static const zetasql::ArrayType* gsql_pg_jsonb_array =
      spangres::datatypes::GetPgJsonbArrayType();
  ZETASQL_RET_CHECK(args.size() == 1);
  if (args[0].is_null()) {
    return zetasql::values::Null(spangres::datatypes::GetPgJsonbType());
  }

  ZETASQL_ASSIGN_OR_RETURN(absl::Cord jsonb, GetPgJsonbNormalizedValue(args[0]));
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSIGN_OR_RETURN(PgJsonbValue jsonb_value,
                   PgJsonbValue::Parse(std::string(jsonb), &tree_nodes));
  ZETASQL_ASSIGN_OR_RETURN(std::vector<absl::Cord> array,
                   jsonb_value.GetSerializedArrayElements());
  std::vector<zetasql::Value> values;
  values.reserve(array.size());
  std::for_each(array.begin(), array.end(), [&values](absl::Cord json_element) {
    values.emplace_back(CreatePgJsonbValueFromNormalized(json_element));
  });
  return zetasql::Value::MakeArray(gsql_pg_jsonb_array, values);
}

std::unique_ptr<zetasql::Function> JsonbQueryArrayFunction(
    absl::string_view catalog_name) {
  const zetasql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  const zetasql::Type* gsql_pg_jsonb_array =
      postgres_translator::spangres::datatypes::GetPgJsonbArrayType();

  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbQueryArray));
  return std::make_unique<zetasql::Function>(
      kZetaSQLJsonQueryArrayFunctionName, catalog_name,
      zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_pg_jsonb_array,
                                       {gsql_pg_jsonb},
                                       /*context_ptr=*/nullptr},
      },
      function_options);
}

absl::StatusOr<zetasql::Value> EvalJsonbBuildArray(
    absl::Span<const zetasql::Value> args) {
  absl::Cord jsonb_value("[");
  for (int i = 0; i < args.size(); ++i) {
    if (i > 0) {
      jsonb_value.Append(", ");
    }
    if (args[i].is_null()) {
      jsonb_value.Append("null");
    } else {
      ZETASQL_ASSIGN_OR_RETURN(zetasql::Value value_as_jsonb,
                       EvalToJsonbFromValue(args[i]));
      ZETASQL_ASSIGN_OR_RETURN(absl::Cord value,
                       GetPgJsonbNormalizedValue(value_as_jsonb));
      jsonb_value.Append(value);
    }
  }
  jsonb_value.Append("]");
  return CreatePgJsonbValueFromNormalized(jsonb_value);
}

std::unique_ptr<zetasql::Function> JsonbBuildArrayFunction(
    absl::string_view catalog_name) {
  const zetasql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbBuildArray));
  return std::make_unique<zetasql::Function>(
      kPGJsonbBuildArrayFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature(
          gsql_pg_jsonb,
          {{zetasql::ARG_TYPE_ARBITRARY,
            zetasql::FunctionArgumentTypeOptions().set_cardinality(
                zetasql::FunctionArgumentType::REPEATED)}},
          /*context_ptr=*/nullptr)},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalJsonbBuildObject(
    absl::Span<const zetasql::Value> args) {
  absl::Cord jsonb_value("{");
  for (int i = 0; i < args.size(); i += 2) {
    if (i > 0) {
      jsonb_value.Append(", ");
    }
    if (args[i].is_null()) {
      return absl::InvalidArgumentError("JSONB key must not be null");
    }
    ZETASQL_ASSIGN_OR_RETURN(zetasql::Value key_as_jsonb,
                     EvalToJsonbFromValue(args[i]));
    ZETASQL_ASSIGN_OR_RETURN(absl::Cord key, GetPgJsonbNormalizedValue(key_as_jsonb));
    jsonb_value.Append(key);
    jsonb_value.Append(": ");

    if (args[i + 1].is_null()) {
      jsonb_value.Append("null");
    } else {
      ZETASQL_ASSIGN_OR_RETURN(zetasql::Value value_as_jsonb,
                       EvalToJsonbFromValue(args[i + 1]));
      ZETASQL_ASSIGN_OR_RETURN(absl::Cord value,
                       GetPgJsonbNormalizedValue(value_as_jsonb));
      jsonb_value.Append(value);
    }
  }
  jsonb_value.Append("}");
  return CreatePgJsonbValueFromNormalized(jsonb_value);
}

std::unique_ptr<zetasql::Function> JsonbBuildObjectFunction(
    absl::string_view catalog_name) {
  const zetasql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbBuildObject));
  return std::make_unique<zetasql::Function>(
      kPGJsonbBuildObjectFunctionName, catalog_name,
      zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature(
          gsql_pg_jsonb,
          {{gsql_string,
            zetasql::FunctionArgumentTypeOptions().set_cardinality(
                zetasql::FunctionArgumentType::REPEATED)},
           {zetasql::ARG_TYPE_ARBITRARY,
            zetasql::FunctionArgumentTypeOptions().set_cardinality(
                zetasql::FunctionArgumentType::REPEATED)}},
          /*context_ptr=*/nullptr)},
      function_options);
}

bool HasNullValue(absl::Span<const zetasql::Value> args) {
  return absl::c_any_of(
      args, [](const zetasql::Value& arg) { return arg.is_null(); });
}

absl::StatusOr<zetasql::Value> EvalJsonbDelete(
    absl::Span<const zetasql::Value> args) {
  if (HasNullValue(args)) {
    return zetasql::Value::Null(
        postgres_translator::spangres::datatypes::GetPgJsonbType());
  }
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSIGN_OR_RETURN(std::string jsonb, GetStringRepresentation(args[0]));
  ZETASQL_ASSIGN_OR_RETURN(PgJsonbValue jsonb_value,
                   PgJsonbValue::Parse(jsonb, &tree_nodes));

  if (!jsonb_value.IsObject() && !jsonb_value.IsArray()) {
    return absl::InvalidArgumentError("cannot delete from scalar");
  }

  if (jsonb_value.IsObject()) {
    if (args[1].type_kind() == zetasql::TYPE_STRING) {
      std::string key = args[1].string_value();
      jsonb_value.RemoveMember(key);
    } else if (args[1].type_kind() == zetasql::TYPE_INT64) {
      return absl::InvalidArgumentError(
          "cannot delete from object using integer index");
    } else if (args[1].type_kind() == zetasql::TYPE_ARRAY) {
      return absl::InvalidArgumentError(
          "Deleting from array not currently supported");
    }
  } else if (jsonb_value.IsArray()) {
    if (args[1].type_kind() == zetasql::TYPE_STRING) {
      std::string del_string = args[1].string_value();
      for (int i = 0; i < jsonb_value.GetArraySize(); ++i) {
        if (jsonb_value.GetArrayElementIfExists(i)->IsString()) {
          std::string serialized_delete_string =
              SerializeJsonbString(del_string);
          absl::string_view element_string =
              jsonb_value.GetArrayElementIfExists(i)->GetSerializedString();
          if (element_string == serialized_delete_string) {
            jsonb_value.RemoveArrayElement(i);
            --i;
          }
        }
      }
    } else if (args[1].type_kind() == zetasql::TYPE_INT64) {
      int64_t index = args[1].int64_value();
      jsonb_value.RemoveArrayElement(index);
    } else if (args[1].type_kind() == zetasql::TYPE_ARRAY) {
      return absl::UnimplementedError(
          "jsonb_delete(jsonb, array) is currently not supported");
    }
  }
  return CreatePgJsonbValueFromNormalized(jsonb_value.Serialize());
}

std::unique_ptr<zetasql::Function> JsonbDeleteFunction(
    absl::string_view catalog_name) {
  const zetasql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbDelete));
  return std::make_unique<zetasql::Function>(
      kPGJsonbDeleteFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature(gsql_pg_jsonb,
                                       {{gsql_pg_jsonb, gsql_string}},
                                       /*context_ptr=*/nullptr),
          zetasql::FunctionSignature(gsql_pg_jsonb,
                                       {{gsql_pg_jsonb, gsql_int64}},
                                       /*context_ptr=*/nullptr),
          zetasql::FunctionSignature(gsql_pg_jsonb,
                                       {{gsql_pg_jsonb, gsql_string_array}},
                                       /*context_ptr=*/nullptr),
      },
      function_options);
}

absl::StatusOr<std::optional<PgJsonbValue>> GetRootJsonbHelper(
    PgJsonbValue jsonb_value, const zetasql::Value& path_value,
    std::vector<std::string>& path_vector) {
  if (path_value.is_null()) {
    return std::nullopt;
  }
  ABSL_CHECK(path_value.type_kind() == zetasql::TYPE_ARRAY);
  for (int i = 0; i < path_value.num_elements(); ++i) {
    if (path_value.element(i).is_null()) {
      return absl::InvalidArgumentError(
          absl::Substitute("path element at position $0 is null", i + 1));
    }
    const zetasql::Value& path_element = path_value.element(i);
    std::string path_element_string = path_element.string_value();
    path_vector.push_back(path_element_string);
  }

  // We need to find the parent of the final element of the path in order to
  // do operations such as delete or set. Thus we construct a std::span as it
  // provides a subspan method and then convert to an absl::Span.
  return jsonb_value.FindAtPath(
      {absl::MakeSpan(path_vector).subspan(0, path_vector.size() - 1)});
}

absl::StatusOr<zetasql::Value> EvalJsonbDeletePath(
    absl::Span<const zetasql::Value> args) {
  if (HasNullValue(args)) {
    return zetasql::Value::Null(
        postgres_translator::spangres::datatypes::GetPgJsonbType());
  }
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSIGN_OR_RETURN(std::string jsonb, GetStringRepresentation(args[0]));
  ZETASQL_ASSIGN_OR_RETURN(PgJsonbValue jsonb_value,
                   PgJsonbValue::Parse(jsonb, &tree_nodes));

  if (!jsonb_value.IsObject() && !jsonb_value.IsArray()) {
    return absl::InvalidArgumentError("cannot delete path in scalar");
  }

  if (jsonb_value.IsEmpty()) {
    return CreatePgJsonbValueFromNormalized(jsonb_value.Serialize());
  }

  std::vector<std::string> path_vector;
  ZETASQL_ASSIGN_OR_RETURN(std::optional<PgJsonbValue> root_jsonb_optional,
                   GetRootJsonbHelper(jsonb_value, args[1], path_vector));
  if (!root_jsonb_optional.has_value()) {
    return CreatePgJsonbValueFromNormalized(jsonb_value.Serialize());
  }
  PgJsonbValue root_jsonb = std::move(root_jsonb_optional).value();
  if (root_jsonb.IsObject()) {
    root_jsonb.RemoveMember(path_vector.back());
  } else if (root_jsonb.IsArray()) {
    auto index_or =
        root_jsonb.PathElementToIndex(path_vector.back(), path_vector.size());
    if (!index_or.ok()) {
      return index_or.status();
    }
    root_jsonb.RemoveArrayElement(index_or.value());
  }
  return CreatePgJsonbValueFromNormalized(jsonb_value.Serialize());
}

std::unique_ptr<zetasql::Function> JsonbDeletePathFunction(
    absl::string_view catalog_name) {
  const zetasql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbDeletePath));
  return std::make_unique<zetasql::Function>(
      kPGJsonbDeletePathFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_pg_jsonb,
                                       {gsql_pg_jsonb, gsql_string_array},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalJsonbSet(
    absl::Span<const zetasql::Value> args) {
  // In the case we pass in 5 arguments, this means we called this function from
  // the jsonb_set_lax function and wish to treat the new value as a JSONB null.
  ABSL_CHECK(args.size() == 4 || args.size() == 5);
  if (HasNullValue(args) && args.size() == 4) {
    return zetasql::Value::Null(
        postgres_translator::spangres::datatypes::GetPgJsonbType());
  }
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSIGN_OR_RETURN(std::string jsonb, GetStringRepresentation(args[0]));
  ZETASQL_ASSIGN_OR_RETURN(PgJsonbValue jsonb_value,
                   PgJsonbValue::Parse(jsonb, &tree_nodes));
  if (!jsonb_value.IsObject() && !jsonb_value.IsArray()) {
    return absl::InvalidArgumentError("cannot set path in scalar");
  }
  std::vector<std::string> path_vector;
  ZETASQL_ASSIGN_OR_RETURN(std::optional<PgJsonbValue> root_jsonb_optional,
                   GetRootJsonbHelper(jsonb_value, args[1], path_vector));
  if (!root_jsonb_optional.has_value()) {
    return CreatePgJsonbValueFromNormalized(jsonb_value.Serialize());
  }
  PgJsonbValue root_jsonb = std::move(root_jsonb_optional).value();
  ZETASQL_ASSIGN_OR_RETURN(std::string new_value_string,
                   GetStringRepresentation(args[2]));
  ZETASQL_ASSIGN_OR_RETURN(PgJsonbValue new_value,
                   PgJsonbValue::Parse(new_value_string, &tree_nodes));
  bool create_if_missing = args[3].bool_value();
  if (root_jsonb.IsObject()) {
    if (root_jsonb.HasMember(path_vector.back())) {
      root_jsonb.GetMemberIfExists(path_vector.back())->SetValue(new_value);
    } else if (create_if_missing) {
      ZETASQL_RETURN_IF_ERROR(root_jsonb.CreateMemberIfNotExists(path_vector.back()));
      root_jsonb.GetMemberIfExists(path_vector.back())->SetValue(new_value);
    }
  } else if (root_jsonb.IsArray()) {
    ZETASQL_ASSIGN_OR_RETURN(
        int32_t index,
        root_jsonb.PathElementToIndex(path_vector.back(), path_vector.size()));
    if (root_jsonb.GetArrayElementIfExists(index).has_value()) {
      root_jsonb.GetArrayElementIfExists(index)->SetValue(new_value);
    } else if (create_if_missing) {
      ZETASQL_RETURN_IF_ERROR(root_jsonb.InsertArrayElement(new_value, index));
      // We may have inserted an index at the either end of the array.
      if (index < 0) index = 0;
      if (index >= root_jsonb.GetArraySize()) {
        index = root_jsonb.GetArraySize() - 1;
      }
      root_jsonb.GetArrayElementIfExists(index)->SetValue(new_value);
    }
  }
  return CreatePgJsonbValueFromNormalized(jsonb_value.Serialize());
}

std::unique_ptr<zetasql::Function> JsonbSetFunction(
    absl::string_view catalog_name) {
  const zetasql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbSet));
  return std::make_unique<zetasql::Function>(
      kPGJsonbSetFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          gsql_pg_jsonb,
          {gsql_pg_jsonb, gsql_string_array, gsql_pg_jsonb, gsql_bool},
          /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalJsonbSetLax(
    absl::Span<const zetasql::Value> args) {
  if (args[0].is_null() || args[1].is_null() || args[3].is_null()) {
    return zetasql::Value::Null(
        postgres_translator::spangres::datatypes::GetPgJsonbType());
  }
  if (args[4].is_null()) {
    return absl::InvalidArgumentError(
        "null_value_treatment must be \"delete_key\", "
        "\"return_target\", \"use_json_null\", or \"raise_exception\"");
  }
  if (args[2].is_null()) {
    const std::string& null_value_treatment = args[4].string_value();
    if (null_value_treatment == "delete_key") {
      return EvalJsonbDeletePath(args.subspan(0, 2));
    } else if (null_value_treatment == "return_target") {
      return EvalToJsonb(args.subspan(0, 1));
    } else if (null_value_treatment == "use_json_null") {
      // Passing in 5 arguments will let JsonbSet know to treat the new value as
      // a JSONB null.
      return EvalJsonbSet(args);
    } else if (null_value_treatment == "raise_exception") {
      return absl::InvalidArgumentError("JSON value must not be null");
    } else {
      return absl::InvalidArgumentError(
          "null_value_treatment must be \"delete_key\", "
          "\"return_target\", \"use_json_null\", or \"raise_exception\"");
    }
  } else {
    return EvalJsonbSet(args.subspan(0, 4));
  }
}

std::unique_ptr<zetasql::Function> JsonbSetLaxFunction(
    absl::string_view catalog_name) {
  const zetasql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbSetLax));
  return std::make_unique<zetasql::Function>(
      kPGJsonbSetLaxFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_pg_jsonb,
                                       {gsql_pg_jsonb, gsql_string_array,
                                        gsql_pg_jsonb, gsql_bool, gsql_string},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalJsonbConcat(
    absl::Span<const zetasql::Value> args) {
  if (HasNullValue(args)) {
    return zetasql::Value::Null(
        postgres_translator::spangres::datatypes::GetPgJsonbType());
  }
  ZETASQL_ASSIGN_OR_RETURN(std::string jsonb_1, GetStringRepresentation(args[0]));
  ZETASQL_ASSIGN_OR_RETURN(std::string jsonb_2, GetStringRepresentation(args[1]));
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSIGN_OR_RETURN(PgJsonbValue left_jsonb,
                   PgJsonbValue::Parse(jsonb_1, &tree_nodes));
  ZETASQL_ASSIGN_OR_RETURN(PgJsonbValue right_jsonb,
                   PgJsonbValue::Parse(jsonb_2, &tree_nodes));
  bool create_object = left_jsonb.IsObject() && right_jsonb.IsObject();
  PgJsonbValue result = create_object
                            ? PgJsonbValue::CreateEmptyObject(&tree_nodes)
                            : PgJsonbValue::CreateEmptyArray(&tree_nodes);
  if (result.IsObject()) {
    result.SetValue(left_jsonb);
    for (auto& [key, value] : right_jsonb.GetMembers()) {
      if (result.HasMember(key)) {
        result.GetMemberIfExists(key)->SetValue(value);
      } else {
        ZETASQL_RETURN_IF_ERROR(result.CreateMemberIfNotExists(key));
        result.GetMemberIfExists(key)->SetValue(value);
      }
    }
    return CreatePgJsonbValueFromNormalized(result.Serialize());
  }
  if (left_jsonb.IsArray()) {
    for (int i = 0; i < left_jsonb.GetArraySize(); ++i) {
      ZETASQL_RETURN_IF_ERROR(result.InsertArrayElement(
          left_jsonb.GetArrayElementIfExists(i).value(), i));
    }
  } else {
    ZETASQL_RETURN_IF_ERROR(result.InsertArrayElement(left_jsonb, 0));
  }
  if (right_jsonb.IsArray()) {
    for (int i = 0; i < right_jsonb.GetArraySize(); ++i) {
      ZETASQL_RETURN_IF_ERROR(result.InsertArrayElement(
          right_jsonb.GetArrayElementIfExists(i).value(), i));
    }
  } else {
    ZETASQL_RETURN_IF_ERROR(
        result.InsertArrayElement(right_jsonb, result.GetArraySize()));
  }
  return CreatePgJsonbValueFromNormalized(result.Serialize());
}

std::unique_ptr<zetasql::Function> JsonbConcatFunction(
    absl::string_view catalog_name) {
  const zetasql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbConcat));
  return std::make_unique<zetasql::Function>(
      kPGJsonbConcatFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_pg_jsonb,
                                       {gsql_pg_jsonb, gsql_pg_jsonb},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalJsonbInsert(
    absl::Span<const zetasql::Value> args) {
  if (HasNullValue(args)) {
    return zetasql::Value::Null(
        postgres_translator::spangres::datatypes::GetPgJsonbType());
  }
  ZETASQL_ASSIGN_OR_RETURN(std::string jsonb_string, GetStringRepresentation(args[0]));
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSIGN_OR_RETURN(PgJsonbValue jsonb_value,
                   PgJsonbValue::Parse(jsonb_string, &tree_nodes));
  if (!jsonb_value.IsObject() && !jsonb_value.IsArray()) {
    // matches pg error message
    return absl::InvalidArgumentError("cannot set path in scalar");
  }
  std::vector<std::string> path_vector;
  ZETASQL_ASSIGN_OR_RETURN(std::optional<PgJsonbValue> root_jsonb_optional,
                   GetRootJsonbHelper(jsonb_value, args[1], path_vector));
  if (!root_jsonb_optional.has_value()) {
    return CreatePgJsonbValueFromNormalized(jsonb_value.Serialize());
  }
  PgJsonbValue root_jsonb = std::move(root_jsonb_optional).value();
  ZETASQL_ASSIGN_OR_RETURN(std::string new_value_string,
                   GetStringRepresentation(args[2]));
  ZETASQL_ASSIGN_OR_RETURN(PgJsonbValue new_value,
                   PgJsonbValue::Parse(new_value_string, &tree_nodes));
  bool insert_after = args[3].bool_value();
  if (root_jsonb.IsObject()) {
    if (root_jsonb.HasMember(path_vector.back())) {
      return absl::InvalidArgumentError("cannot replace existing key");
    }
    ZETASQL_RETURN_IF_ERROR(root_jsonb.CreateMemberIfNotExists(path_vector.back()));
    root_jsonb.GetMemberIfExists(path_vector.back())->SetValue(new_value);
  } else if (root_jsonb.IsArray()) {
    ZETASQL_ASSIGN_OR_RETURN(
        int32_t index,
        root_jsonb.PathElementToIndex(path_vector.back(), path_vector.size()));
    // Deal with edge cases for the index. GetArraySize() should never be
    // past the numeric limit but even if it is, InsertArrayElement will fail.
    if (index == -1) index = root_jsonb.GetArraySize() - 1;
    if (index == std::numeric_limits<int32_t>::max()) {
      index = root_jsonb.GetArraySize();
    }
    index = insert_after ? index + 1 : index;
    ZETASQL_RETURN_IF_ERROR(root_jsonb.InsertArrayElement(new_value, index));
  }
  return CreatePgJsonbValueFromNormalized(jsonb_value.Serialize());
}

std::unique_ptr<zetasql::Function> JsonbInsertFunction(
    absl::string_view catalog_name) {
  const zetasql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbInsert));
  return std::make_unique<zetasql::Function>(
      kPGJsonbInsertFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          gsql_pg_jsonb,
          {gsql_pg_jsonb, gsql_string_array, gsql_pg_jsonb, gsql_bool},
          /*context_ptr=*/nullptr}},
      function_options);
}

void JsonbStripNullsImpl(PgJsonbValue& jsonb_value) {
  if (jsonb_value.IsArray()) {
    for (auto element : jsonb_value.GetArrayElements()) {
      JsonbStripNullsImpl(element);
    }
  } else if (jsonb_value.IsObject()) {
    for (auto& [key, value] : jsonb_value.GetMembers()) {
      JsonbStripNullsImpl(value);
    }
    jsonb_value.CleanUpJsonbObject();
  }
}

absl::StatusOr<zetasql::Value> EvalJsonbStripNulls(
    absl::Span<const zetasql::Value> args) {
  if (HasNullValue(args)) {
    return zetasql::Value::Null(
        postgres_translator::spangres::datatypes::GetPgJsonbType());
  }
  ZETASQL_ASSIGN_OR_RETURN(std::string jsonb_string, GetStringRepresentation(args[0]));
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSIGN_OR_RETURN(PgJsonbValue jsonb_value,
                   PgJsonbValue::Parse(jsonb_string, &tree_nodes));
  JsonbStripNullsImpl(jsonb_value);
  return CreatePgJsonbValueFromNormalized(jsonb_value.Serialize());
}

std::unique_ptr<zetasql::Function> JsonbStripNullsFunction(
    absl::string_view catalog_name) {
  const zetasql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbStripNulls));
  return std::make_unique<zetasql::Function>(
      kPGJsonbStripNullsFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_pg_jsonb,
                                       {gsql_pg_jsonb},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalJsonbContains(
    absl::Span<const zetasql::Value> args) {
  ABSL_DCHECK_EQ(args.size(), 2);
  if (args[0].is_null() || args[1].is_null()) {
    return zetasql::Value::NullBool();
  }

  ABSL_DCHECK(args[0].type() == args[1].type());
  ABSL_DCHECK(args[0].type() == GetPgJsonbType());

  ZETASQL_ASSIGN_OR_RETURN(absl::Cord in_1, GetPgJsonbNormalizedValue(args[0]));
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSIGN_OR_RETURN(PgJsonbValue input,
                   PgJsonbValue::Parse(std::string(in_1), &tree_nodes));

  ZETASQL_ASSIGN_OR_RETURN(absl::Cord in_2, GetPgJsonbNormalizedValue(args[1]));
  ZETASQL_ASSIGN_OR_RETURN(PgJsonbValue target,
                   PgJsonbValue::Parse(std::string(in_2), &tree_nodes));

  return zetasql::Value::Bool(JsonbContains(input, target));
}

absl::StatusOr<zetasql::Value> EvalJsonbContained(
    absl::Span<const zetasql::Value> args) {
  ABSL_DCHECK_EQ(args.size(), 2);
  if (args[0].is_null() || args[1].is_null()) {
    return zetasql::Value::NullBool();
  }

  ABSL_DCHECK(args[0].type() == args[1].type());
  ABSL_DCHECK(args[0].type() == GetPgJsonbType());

  ZETASQL_ASSIGN_OR_RETURN(absl::Cord in_1, GetPgJsonbNormalizedValue(args[0]));
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSIGN_OR_RETURN(PgJsonbValue input,
                   PgJsonbValue::Parse(std::string(in_1), &tree_nodes));

  ZETASQL_ASSIGN_OR_RETURN(absl::Cord in_2, GetPgJsonbNormalizedValue(args[1]));
  ZETASQL_ASSIGN_OR_RETURN(PgJsonbValue target,
                   PgJsonbValue::Parse(std::string(in_2), &tree_nodes));

  return zetasql::Value::Bool(JsonbContains(target, input));
}

std::unique_ptr<zetasql::Function> JsonbContainsFunction(
    absl::string_view catalog_name) {
  const zetasql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbContains));
  return std::make_unique<zetasql::Function>(
      kPGJsonbContainsFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_bool,
                                       {gsql_pg_jsonb, gsql_pg_jsonb},
                                       /*context_ptr=*/nullptr},
      },
      function_options);
}

std::unique_ptr<zetasql::Function> JsonbContainedFunction(
    absl::string_view catalog_name) {
  const zetasql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbContained));
  return std::make_unique<zetasql::Function>(
      kPGJsonbContainedFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_bool,
                                       {gsql_pg_jsonb, gsql_pg_jsonb},
                                       /*context_ptr=*/nullptr},
      },
      function_options);
}

absl::StatusOr<zetasql::Value> EvalJsonbExists(
    absl::Span<const zetasql::Value> args) {
  ABSL_DCHECK_EQ(args.size(), 2);
  if (args[0].is_null() || args[1].is_null()) {
    return zetasql::Value::NullBool();
  }

  ABSL_DCHECK(args[0].type() == GetPgJsonbType());
  ABSL_DCHECK(args[1].type() == zetasql::types::StringType());

  ZETASQL_ASSIGN_OR_RETURN(absl::Cord jsonb_string, GetPgJsonbNormalizedValue(args[0]));
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSIGN_OR_RETURN(PgJsonbValue input,
                   PgJsonbValue::Parse(std::string(jsonb_string), &tree_nodes));

  return zetasql::Value::Bool(input.Exists(args[1].string_value()));
}

std::unique_ptr<zetasql::Function> JsonbExistsFunction(
    absl::string_view catalog_name) {
  const zetasql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbExists));
  return std::make_unique<zetasql::Function>(
      kPGJsonbExistsFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_bool,
                                       {gsql_pg_jsonb, gsql_string},
                                       /*context_ptr=*/nullptr},
      },
      function_options);
}

absl::StatusOr<zetasql::Value> EvalJsonbExistsAny(
    absl::Span<const zetasql::Value> args) {
  ABSL_DCHECK_EQ(args.size(), 2);
  if (args[0].is_null() || args[1].is_null()) {
    return zetasql::Value::NullBool();
  }

  ABSL_DCHECK(args[0].type() == GetPgJsonbType());
  ABSL_DCHECK(args[1].type() == zetasql::types::StringArrayType());

  ZETASQL_ASSIGN_OR_RETURN(absl::Cord jsonb_string, GetPgJsonbNormalizedValue(args[0]));
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSIGN_OR_RETURN(PgJsonbValue input,
                   PgJsonbValue::Parse(std::string(jsonb_string), &tree_nodes));

  for (const auto& string_key : args[1].elements()) {
    if (string_key.is_null()) {
      // Ignore null element matching.
      continue;
    }

    if (input.Exists(string_key.string_value())) {
      return zetasql::Value::Bool(true);
    }
  }

  return zetasql::Value::Bool(false);
}

std::unique_ptr<zetasql::Function> JsonbExistsAnyFunction(
    absl::string_view catalog_name) {
  const zetasql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbExistsAny));
  return std::make_unique<zetasql::Function>(
      kPGJsonbExistsAnyFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_bool,
                                       {gsql_pg_jsonb, gsql_string_array},
                                       /*context_ptr=*/nullptr},
      },
      function_options);
}

absl::StatusOr<zetasql::Value> EvalJsonbExistsAll(
    absl::Span<const zetasql::Value> args) {
  ABSL_DCHECK_EQ(args.size(), 2);
  if (args[0].is_null() || args[1].is_null()) {
    return zetasql::Value::NullBool();
  }

  ABSL_DCHECK(args[0].type() == GetPgJsonbType());
  ABSL_DCHECK(args[1].type() == zetasql::types::StringArrayType());

  ZETASQL_ASSIGN_OR_RETURN(absl::Cord jsonb_string, GetPgJsonbNormalizedValue(args[0]));
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSIGN_OR_RETURN(PgJsonbValue input,
                   PgJsonbValue::Parse(std::string(jsonb_string), &tree_nodes));

  for (const auto& string_key : args[1].elements()) {
    if (string_key.is_null()) {
      // Ignore null element matching.
      continue;
    }

    if (!input.Exists(string_key.string_value())) {
      return zetasql::Value::Bool(false);
    }
  }

  return zetasql::Value::Bool(true);
}

std::unique_ptr<zetasql::Function> JsonbExistsAllFunction(
    absl::string_view catalog_name) {
  const zetasql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbExistsAll));
  return std::make_unique<zetasql::Function>(
      kPGJsonbExistsAllFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_bool,
                                       {gsql_pg_jsonb, gsql_string_array},
                                       /*context_ptr=*/nullptr},
      },
      function_options);
}

// PgLesser and PgGreater are alternatives to
// std::less<float|double> and std::greater<float|double> which capture
// Postgres' ordering semantics. std::less<float|double> and
// std::greater<float|double> do not have proper ordering semantics for NaN
// values, they will always return false when one of the argument is NaN. In
// Postgres NaN is the highest valued float4, float8 and NUMERIC.
// LEAST(12::float8, 3::float8, 'nan'::float8, null::float8) => 3
// GREATEST(12::float8, 3::float8, 'nan'::float8, null::float8) => NaN
template <typename T, typename = std::enable_if_t<std::is_floating_point_v<T>>>
class PgFloatingPointLesser {
 public:
  // Returns true iff lhs is strictly less than rhs.
  bool operator()(const zetasql::Value lhs,
                  const zetasql::Value rhs) const {
    T typed_lhs = lhs.Get<T>();
    T typed_rhs = rhs.Get<T>();

    if (std::isnan(typed_lhs) && std::isnan(typed_rhs)) {
      return false;
    }

    if (std::isnan(typed_rhs)) {
      return true;
    }

    return typed_lhs < typed_rhs;
  }
};

template <typename T, typename = std::enable_if_t<std::is_floating_point_v<T>>>
class PgFloatingPointGreater {
 public:
  // Returns true iff lhs is strictly greater than rhs.
  bool operator()(const zetasql::Value lhs,
                  const zetasql::Value rhs) const {
    T typed_lhs = lhs.Get<T>();
    T typed_rhs = rhs.Get<T>();

    if (std::isnan(typed_lhs) && std::isnan(typed_rhs)) {
      return false;
    }

    if (std::isnan(typed_lhs)) {
      return true;
    }

    return typed_lhs > typed_rhs;
  }
};

class PgLesser {
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
  auto is_non_floating_point_supported_type =
      [](const zetasql::Type* type) -> bool {
    return (type->IsInt64() || type->IsBool() || type->IsBytes() ||
            type->IsString() || type->IsDate() || type->IsTimestamp() ||
            type->IsInterval());
  };

  zetasql::FunctionEvaluatorFactory least_evaluator_factory(
      [&](const zetasql::FunctionSignature& signature)
          -> absl::StatusOr<zetasql::FunctionEvaluator> {
        if (signature.result_type().type()->IsDouble()) {
          return EvalLeastGreatest<PgFloatingPointLesser<double>>;
        } else if (signature.result_type().type()->IsFloat()) {
          return EvalLeastGreatest<PgFloatingPointLesser<float>>;
        } else if (is_non_floating_point_supported_type(
                       signature.result_type().type())) {
          return EvalLeastGreatest<PgLesser>;
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
          return EvalLeastGreatest<PgFloatingPointGreater<double>>;
        } else if (signature.result_type().type()->IsFloat()) {
          return EvalLeastGreatest<PgFloatingPointGreater<float>>;
        } else if (is_non_floating_point_supported_type(
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
      zetasql::types::FloatType(),
      zetasql::types::Int64Type(),
      zetasql::types::BoolType(),
      zetasql::types::BytesType(),
      zetasql::types::StringType(),
      zetasql::types::DateType(),
      zetasql::types::TimestampType(),
      postgres_translator::spangres::datatypes::GetPgNumericType(),
      postgres_translator::spangres::datatypes::GetPgJsonbType(),
      zetasql::types::IntervalType(),
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

template <typename T, typename = std::enable_if_t<std::is_floating_point_v<T>>>
class MinFloatingPointEvaluator : public zetasql::AggregateFunctionEvaluator {
 public:
  explicit MinFloatingPointEvaluator() {}
  ~MinFloatingPointEvaluator() override = default;

  absl::Status Reset() override { return absl::OkStatus(); }

  absl::Status Accumulate(absl::Span<const zetasql::Value*> args,
                          bool* stop_accumulation) override {
    // No args left to accumulate.
    if (args.empty()) {
      *stop_accumulation = true;
      return absl::OkStatus();
    }

    const zetasql::Value value = *args[0];
    if (!value.type()->IsDouble() && !value.type()->IsFloat()) {
      return absl::InvalidArgumentError(
          "Cannot accumulate value which is not of type double or float.");
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
      result_ = zetasql::Value::Make<T>(std::numeric_limits<T>::quiet_NaN());
    }

    // Use the comparison function that respects the NaN-ordering semantics of
    // PostgreSQL.
    if (PgFloatingPointLesser<T>()(value, result_)) {
      result_ = value;
    }

    return absl::OkStatus();
  }

  absl::StatusOr<zetasql::Value> GetFinalResult() override { return result_; }

 private:
  // Initialized to NULL as it's the default value to return if no values are
  // provided to aggregate or if all the values to aggregate are NULL.
  zetasql::Value result_ = zetasql::Value::MakeNull<T>();
};

class MinMaxEvaluator : public zetasql::AggregateFunctionEvaluator {
 public:
  explicit MinMaxEvaluator(const zetasql::Type* type, bool is_min) :
    result_(zetasql::Value::Null(type)), is_min_(is_min) {}
  ~MinMaxEvaluator() override = default;

  absl::Status Reset() override { return absl::OkStatus(); }

  absl::Status Accumulate(absl::Span<const zetasql::Value*> args,
                          bool* stop_accumulation) override {
    // No args left to accumulate.
    if (args.empty()) {
      *stop_accumulation = true;
      return absl::OkStatus();
    }

    const zetasql::Value value = *args[0];
    if (value.type()->IsDouble() || value.type()->IsFloat()) {
      return absl::InvalidArgumentError(
          "Incorrect accumulator for floating point types.");
    }

    // TODO: Figure out why IgnoreNulls(), which defaults to true
    // is not working.
    if (value.is_null()) {
      return absl::OkStatus();
    }

    if (result_.is_null()) {
      result_ = value;
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
  // Initialized to NULL as it's the default value to return if no values are
  // provided to aggregate or if all the values to aggregate are NULL.
  zetasql::Value result_;
  bool is_min_;
};

std::unique_ptr<zetasql::Function> MinAggregator(
    const std::string& catalog_name) {
  zetasql::AggregateFunctionEvaluatorFactory aggregate_fn =
      [](const zetasql::FunctionSignature& sig)
      -> std::unique_ptr<zetasql::AggregateFunctionEvaluator> {
    if (sig.result_type().type()->IsFloat()) {
      return std::make_unique<MinFloatingPointEvaluator<float>>();
    } else if (sig.result_type().type()->IsDouble()) {
      return std::make_unique<MinFloatingPointEvaluator<double>>();
    } else {
      return std::make_unique<MinMaxEvaluator>(
          sig.result_type().type(), /* is_min =*/true);
    }
  };

  zetasql::FunctionOptions options;
  options.set_aggregate_function_evaluator_factory(aggregate_fn);
  return std::make_unique<zetasql::Function>(
      kPGMinFunctionName, catalog_name, zetasql::Function::AGGREGATE,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{zetasql::types::DoubleType(),
                                       {zetasql::types::DoubleType()},
                                       nullptr},
          zetasql::FunctionSignature{zetasql::types::FloatType(),
                                       {zetasql::types::FloatType()},
                                       nullptr},
          zetasql::FunctionSignature{spangres::datatypes::GetPgOidType(),
                                       {spangres::datatypes::GetPgOidType()},
                                       nullptr}},
      options);
}

std::unique_ptr<zetasql::Function> MaxAggregator(
    const std::string& catalog_name) {
  zetasql::AggregateFunctionEvaluatorFactory aggregate_fn =
      [](const zetasql::FunctionSignature& sig)
      -> std::unique_ptr<zetasql::AggregateFunctionEvaluator> {
    return std::make_unique<MinMaxEvaluator>(
        sig.result_type().type(), /* is_min =*/false);
  };

  zetasql::FunctionOptions options;
  options.set_aggregate_function_evaluator_factory(aggregate_fn);
  return std::make_unique<zetasql::Function>(
      kPGMaxFunctionName, catalog_name, zetasql::Function::AGGREGATE,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{spangres::datatypes::GetPgOidType(),
                                       {spangres::datatypes::GetPgOidType()},
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
      return absl::OkStatus();
    }

    // Setup the memory context arena which is required for collated comparisons
    // called by LessThan().
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<postgres_translator::interfaces::PGArena> arena,
        postgres_translator::interfaces::CreatePGArena(nullptr));

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

enum SumAvgAggregatorType { Sum, Avg };

// Can evaluate a sum for INT64, FLOAT, DOUBLE and PG.NUMERIC.
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
      } else if (value.type_kind() == zetasql::TYPE_FLOAT) {
        kind_ = zetasql::TYPE_FLOAT;
        // Avg of float returns a double.
        result_ = IsAvgEvaluator() ? zetasql::values::NullDouble()
                                   : zetasql::values::NullFloat();
      } else if (value.type_kind() == zetasql::TYPE_INT64 ||
                 (value.type_kind() == zetasql::TYPE_EXTENDED &&
                  value.type()->Equals(gsql_pg_numeric_))) {
        // Both INT64 and PG.NUMERIC return PG.NUMERIC.
        kind_ = value.type_kind();
        result_ = zetasql::values::Null(gsql_pg_numeric_);
      } else {
        return absl::InvalidArgumentError(
            "Cannot accumulate value which is not of type INT64, FLOAT, DOUBLE "
            "or PG.NUMERIC.");
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
        ZETASQL_ASSIGN_OR_RETURN(result_, CreatePgNumericValueWithMemoryContext(
                                      absl::StrCat(value.int64_value())));
      } else if (IsAvgEvaluator() &&
                 value.type_kind() == zetasql::TYPE_FLOAT) {
        // Convert the float to double for avg calculations.
        result_ =
            zetasql::values::Double(static_cast<double>(value.float_value()));
      } else {
        result_ = value;
      }

      count_++;
      return absl::OkStatus();
    }

    // Now do the addition.
    if (value.type_kind() == zetasql::TYPE_INT64) {
      // Setup the memory context arena which is required for
      // CreatePgNumericValue() and EvalZetaSQLAdd().
      ZETASQL_ASSIGN_OR_RETURN(
          std::unique_ptr<postgres_translator::interfaces::PGArena> arena,
          postgres_translator::interfaces::CreatePGArena(nullptr));
      ZETASQL_ASSIGN_OR_RETURN(auto value_as_numeric,
                       CreatePgNumericValue(absl::StrCat(value.int64_value())));
      ZETASQL_ASSIGN_OR_RETURN(
          result_,
          EvalZetaSQLAdd(absl::MakeConstSpan({result_, value_as_numeric})));
    } else if (value.type_kind() == zetasql::TYPE_DOUBLE) {
      double result;
      absl::Status status;
      if (!zetasql::functions::Add(result_.double_value(),
                                     value.double_value(), &result, &status)) {
        return status;
      }
      result_ = zetasql::values::Double(result);
    } else if (value.type_kind() == zetasql::TYPE_FLOAT &&
               !IsAvgEvaluator()) {
      float result;
      absl::Status status;
      if (!zetasql::functions::Add(result_.float_value(), value.float_value(),
                                     &result, &status)) {
        return status;
      }
      result_ = zetasql::values::Float(result);
    } else if (value.type_kind() == zetasql::TYPE_FLOAT && IsAvgEvaluator()) {
      // Calculations of avg over float values happens in the double domain.
      double result;
      absl::Status status;
      if (!zetasql::functions::Add(result_.double_value(),
                                     static_cast<double>(value.float_value()),
                                     &result, &status)) {
        return status;
      }
      result_ = zetasql::values::Double(result);
    } else if (value.type_kind() == zetasql::TYPE_EXTENDED) {
      // Setup the memory context arena which is required for
      // EvalZetaSQLAdd().
      ZETASQL_ASSIGN_OR_RETURN(
          std::unique_ptr<postgres_translator::interfaces::PGArena> arena,
          postgres_translator::interfaces::CreatePGArena(nullptr));
      ZETASQL_ASSIGN_OR_RETURN(result_,
                       EvalZetaSQLAdd(absl::MakeConstSpan({result_, value})));
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

  virtual bool IsAvgEvaluator() { return false; }
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
          zetasql::FunctionSignature{zetasql::types::FloatType(),
                                       {zetasql::types::FloatType()},
                                       nullptr},
          zetasql::FunctionSignature{
              gsql_pg_numeric, {gsql_pg_numeric}, nullptr}},
      options);
}

// Can evaluate the avg for INT64, FLOAT, DOUBLE and PG.NUMERIC.
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

    if (kind_ == zetasql::TYPE_DOUBLE || kind_ == zetasql::TYPE_FLOAT) {
      if (result_.is_null()) {
        return zetasql::values::NullDouble();
      }
      double result;
      absl::Status status;
      // `result_` is always a double value, even for when the input is float.
      if (!zetasql::functions::Divide(result_.double_value(),
                                        static_cast<double>(count_), &result,
                                        &status)) {
        return status;
      }
      return zetasql::values::Double(result);
    }

    // INT64 or PG.NUMERIC:
    // Setup the memory context arena which is required for
    // CreatePgNumericValue() and EvalZetaSQLDivide().
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<postgres_translator::interfaces::PGArena> arena,
        postgres_translator::interfaces::CreatePGArena(nullptr));
    ZETASQL_ASSIGN_OR_RETURN(auto count_as_numeric,
                     CreatePgNumericValue(absl::StrCat(count_)));
    return EvalZetaSQLDivide(
        absl::MakeConstSpan({result_, count_as_numeric}));
  }

 protected:
  virtual bool IsAvgEvaluator() override { return true; }
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
          zetasql::FunctionSignature{zetasql::types::DoubleType(),
                                       {zetasql::types::FloatType()},
                                       nullptr},
          zetasql::FunctionSignature{
              gsql_pg_numeric, {gsql_pg_numeric}, nullptr}},
      options);
}

template <zetasql::TypeKind T>
absl::StatusOr<zetasql::Value> EvalCastFromOid(
    absl::Span<const zetasql::Value> args) {
  switch (T) {
    case zetasql::TYPE_INT64:
      return EvalCastOidToInt64(args);
    case zetasql::TYPE_STRING:
      return EvalCastOidToString(args);
    default:
      return absl::InvalidArgumentError(
          absl::StrCat("cannot cast oid object to type ", T));
  }
}

std::unique_ptr<zetasql::Function> CastFromOidFunction(
    absl::string_view catalog_name) {
  static const zetasql::Type* gsql_pg_oid =
      spangres::datatypes::GetPgOidType();

  zetasql::FunctionEvaluatorFactory evaluator_factory(
      [&](const zetasql::FunctionSignature& signature)
          -> absl::StatusOr<zetasql::FunctionEvaluator> {
        if (signature.result_type().type()->IsInt64()) {
          return EvalCastOidToInt64;
        } else if (signature.result_type().type()->IsString()) {
          return ::postgres_translator::EvalCastFromOid<zetasql::TYPE_STRING>;
        } else {
          return absl::InvalidArgumentError(
              absl::StrCat("cannot cast oid object to type ",
                           signature.result_type().type()->ShortTypeName(
                               zetasql::PRODUCT_EXTERNAL)));
        }
      });

  zetasql::FunctionOptions function_options;
  function_options.set_evaluator_factory(evaluator_factory);
  return std::make_unique<zetasql::Function>(
      kPGCastFromOidFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{
              gsql_int64, {gsql_pg_oid}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_string, {gsql_pg_oid}, /*context_ptr=*/nullptr},
      },
      function_options);
}

std::unique_ptr<zetasql::Function> CastToOidFunction(
    absl::string_view catalog_name) {
  static const zetasql::Type* gsql_pg_oid =
      spangres::datatypes::GetPgOidType();

  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalCastToOid));
  return std::make_unique<zetasql::Function>(
      kPGCastToOidFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{
              gsql_pg_oid, {gsql_int64}, /*context_ptr=*/nullptr},
          zetasql::FunctionSignature{
              gsql_pg_oid, {gsql_string}, /*context_ptr=*/nullptr},
      },
      function_options);
}

absl::StatusOr<zetasql::JSONValueConstRef> GetJSONValueConstRef(
    const zetasql::Value& jsonb, zetasql::JSONValue& json_storage) {
  ZETASQL_ASSIGN_OR_RETURN(absl::Cord jsonb_cord, GetPgJsonbNormalizedValue(jsonb));
  ZETASQL_ASSIGN_OR_RETURN(
      json_storage,
      zetasql::JSONValue::ParseJSONString(
          jsonb_cord.Flatten(),
          {.wide_number_mode =
               zetasql::JSONParsingOptions::WideNumberMode::kExact}));
  return json_storage.GetConstRef();
}

template <typename T>
std::unique_ptr<zetasql::Function> JsonbArrayExtractionFunction(
    absl::string_view catalog_name) {
  const zetasql::Type* pg_jsonb_type = spangres::datatypes::GetPgJsonbType();
  absl::string_view function_name;
  const zetasql::Type* return_type;

  if constexpr (std::is_same_v<T, bool>) {
    function_name = "bool_array";
    return_type = gsql_bool_array;
  } else if constexpr (std::is_same_v<T, int64_t>) {
    function_name = "int64_array";
    return_type = gsql_int64_array;
  } else {
    static_assert(std::is_same_v<T, std::string>, "Unexpected type");
    function_name = "string_array";
    return_type = gsql_string_array;
  }

  zetasql::FunctionOptions function_options;
  function_options.set_evaluator([return_type](
                                     absl::Span<const zetasql::Value> args)
                                     -> absl::StatusOr<zetasql::Value> {
    ZETASQL_RET_CHECK_EQ(args.size(), 1);
    if (args[0].is_null()) {
      return zetasql::Value::Null(return_type);
    }

    zetasql::JSONValue json_storage;
    ZETASQL_ASSIGN_OR_RETURN(zetasql::JSONValueConstRef json_value_const_ref,
                     GetJSONValueConstRef(args[0], json_storage));

    if constexpr (std::is_same_v<T, bool>) {
      ZETASQL_ASSIGN_OR_RETURN(
          auto result,
          zetasql::functions::ConvertJsonToBoolArray(json_value_const_ref));
      return zetasql::values::BoolArray(result);
    } else if constexpr (std::is_same_v<T, int64_t>) {
      ZETASQL_ASSIGN_OR_RETURN(
          auto result,
          zetasql::functions::ConvertJsonToInt64Array(json_value_const_ref));
      return zetasql::values::Int64Array(result);
    } else {
      static_assert(std::is_same_v<T, std::string>, "Unexpected type");
      ZETASQL_ASSIGN_OR_RETURN(
          auto result,
          zetasql::functions::ConvertJsonToStringArray(json_value_const_ref));
      return zetasql::values::StringArray(result);
    }
  });

  return std::make_unique<zetasql::Function>(
      function_name, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{return_type,
                                       {pg_jsonb_type},
                                       /*context_ptr=*/nullptr},
      },
      function_options);
}

template <typename T>
std::unique_ptr<zetasql::Function> JsonbFloatArrayExtractionFunction(
    absl::string_view catalog_name) {
  const zetasql::Type* pg_jsonb_type = spangres::datatypes::GetPgJsonbType();
  absl::string_view function_name;
  const zetasql::Type* return_type;
  if constexpr (std::is_same_v<T, double>) {
    function_name = "float64_array";
    return_type = gsql_double_array;
  } else {
    static_assert(std::is_same_v<T, float>, "Unexpected type");
    function_name = "float32_array";
    return_type = gsql_float_array;
  }

  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      [return_type](absl::Span<const zetasql::Value> args)
          -> absl::StatusOr<zetasql::Value> {
        ZETASQL_RET_CHECK_EQ(args.size(), 1);
        if (args[0].is_null()) {
          return zetasql::Value::Null(return_type);
        }
        // PG currently does not support optional `wide_number_mode` parameter.
        zetasql::functions::WideNumberMode mode =
            zetasql::functions::WideNumberMode::kRound;
        zetasql::ProductMode product_mode = zetasql::PRODUCT_EXTERNAL;

        zetasql::JSONValue json_storage;
        ZETASQL_ASSIGN_OR_RETURN(zetasql::JSONValueConstRef json_value_const_ref,
                         GetJSONValueConstRef(args[0], json_storage));

        if constexpr (std::is_same_v<T, double>) {
          ZETASQL_ASSIGN_OR_RETURN(auto result,
                           zetasql::functions::ConvertJsonToDoubleArray(
                               json_value_const_ref, mode, product_mode));
          return zetasql::values::DoubleArray(result);
        } else {
          static_assert(std::is_same_v<T, float>, "Unexpected type");
          ZETASQL_ASSIGN_OR_RETURN(auto result,
                           zetasql::functions::ConvertJsonToFloatArray(
                               json_value_const_ref, mode, product_mode));
          return zetasql::values::FloatArray(result);
        }
      });

  return std::make_unique<zetasql::Function>(
      function_name, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{return_type,
                                       {pg_jsonb_type},
                                       /*context_ptr=*/nullptr},
      },
      function_options);
}

zetasql::FunctionArgumentTypeOptions GetOptionalNamedArgumentOptions(
    absl::string_view name) {
  zetasql::FunctionArgumentTypeOptions options;
  options.set_cardinality(zetasql::FunctionArgumentType::OPTIONAL);
  options.set_argument_name(name, zetasql::kPositionalOrNamed);
  return options;
}

}  // namespace

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
  case zetasql::TYPE_FLOAT: {
    ZETASQL_ASSIGN_OR_RETURN(
        std::string result,
        Float4ToChar(args[0].float_value(), args[1].string_value()));
    return zetasql::Value::String(result);
  }
  case zetasql::TYPE_INTERVAL: {
    std::unique_ptr<std::string> result;
    ZETASQL_ASSIGN_OR_RETURN(zetasql::IntervalValue interval,
                     RoundPrecision(args[0].interval_value()));
    ZETASQL_ASSIGN_OR_RETURN(result,
                     PgIntervalToChar(interval, args[1].string_value()));
    return result == nullptr ? zetasql::Value::NullString()
                             : zetasql::Value::String(*result);
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

absl::StatusOr<zetasql::Value> EvalExtract(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 2);
  if (args[1].type_kind() == zetasql::TYPE_TIMESTAMP) {
    ZETASQL_ASSIGN_OR_RETURN(
        absl::Cord result,
        PgTimestamptzExtract(args[0].string_value(), args[1].ToTime()));
    return CreatePgNumericValue(std::string(result));
  } else {
    ZETASQL_ASSIGN_OR_RETURN(absl::Cord result, PgDateExtract(args[0].string_value(),
                                                      args[1].date_value()));
    return CreatePgNumericValue(std::string(result));
  }
}

absl::StatusOr<zetasql::Value> EvalCastToTimestamp(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 1);

  ZETASQL_ASSIGN_OR_RETURN(absl::Time time, function_evaluators::PgTimestamptzIn(
                                        args[0].string_value()));
  return zetasql::Value::Timestamp(time);
}

absl::StatusOr<zetasql::Value> EvalCastToString(
  absl::Span<const zetasql::Value> args) {
ZETASQL_RET_CHECK(args.size() == 1);

switch (args[0].type()->kind()) {
  case zetasql::TYPE_INTERVAL:
    return EvalCastIntervalToString(args);
  case zetasql::TYPE_EXTENDED: {
    auto type_code =
        static_cast<const spangres::datatypes::SpannerExtendedType*>(
            args[0].type())
            ->code();
    switch (type_code) {
      case spangres::datatypes::TypeAnnotationCode::PG_NUMERIC:
        return EvalCastNumericToString(args);
      default:
        return absl::InvalidArgumentError(
            absl::StrCat("Unsupported type for CAST to text: ",
                         args[0].type()->DebugString()));
    }
  }
  default:
    return absl::InvalidArgumentError(
        absl::StrCat("Unsupported type for CAST to text: ",
                     args[0].type()->DebugString()));
}
}

absl::StatusOr<zetasql::Value> EvalCastNumericToInt64(
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

absl::StatusOr<zetasql::Value> EvalCastNumericToDouble(
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

absl::StatusOr<zetasql::Value> EvalCastNumericToFloat(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 1);
  if (args[0].is_null()) {
    return zetasql::Value::NullFloat();
  }
  ZETASQL_ASSIGN_OR_RETURN(absl::Cord normalized_value,
                   GetPgNumericNormalizedValue(args[0]));
  float out;
  bool result = absl::SimpleAtof(std::string(normalized_value), &out);
  if (!result || std::isinf(out)) {
    return absl::OutOfRangeError(absl::StrCat("Cannot cast to float from ",
                                              std::string(normalized_value)));
  }
  return zetasql::Value::Float(out);
}

absl::StatusOr<zetasql::Value> EvalCastNumericToString(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 1);
  if (args[0].is_null()) {
    return zetasql::Value::NullString();
  }
  ZETASQL_ASSIGN_OR_RETURN(absl::Cord normalized_value,
                   GetPgNumericNormalizedValue(args[0]));
  return zetasql::Value::String(std::string(normalized_value));
}

absl::StatusOr<zetasql::Value> EvalCastIntervalToString(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 1);
  ZETASQL_RET_CHECK(args[0].type()->IsInterval());
  if (args[0].is_null()) {
    return zetasql::Value::NullString();
  }

  ZETASQL_ASSIGN_OR_RETURN(std::string result, PgIntervalOut(args[0].interval_value()));
  return zetasql::Value::String(result);
}

absl::StatusOr<zetasql::Value> EvalCastStringToInterval(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 1);
  ZETASQL_RET_CHECK_EQ(args[0].type(), gsql_string);
  if (args[0].is_null()) {
    return zetasql::Value::Null(gsql_interval);
  }

  ZETASQL_ASSIGN_OR_RETURN(zetasql::IntervalValue result,
                   PgIntervalIn(args[0].string_value()));
  return zetasql::Value::Interval(result);
}

namespace {
template <typename T, typename = std::enable_if_t<std::is_floating_point_v<T>>>
absl::StatusOr<std::string> FloatingPointToNumeric(
    zetasql::Value gsql_value) {
  if (std::isnan(gsql_value.Get<T>())) {
    return kNan;
  } else if (std::isinf(gsql_value.Get<T>())) {
    return absl::InvalidArgumentError("Cannot cast infinity to PG.NUMERIC");
  } else {
    return absl::StrFormat("%.*g", std::numeric_limits<T>::digits10,
                           gsql_value.Get<T>());
  }
}
}  // namespace

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
    case zetasql::TYPE_DOUBLE: {
      ZETASQL_ASSIGN_OR_RETURN(input_to_string,
                       FloatingPointToNumeric<double>(args[0]));
      break;
    }
    case zetasql::TYPE_FLOAT: {
      ZETASQL_ASSIGN_OR_RETURN(input_to_string, FloatingPointToNumeric<float>(args[0]));
      break;
    }
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

absl::StatusOr<zetasql::Value> EvalCastToOid(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(!args.empty() && args.size() < 2);
  if (args[0].is_null()) {
    return zetasql::Value::Null(spangres::datatypes::GetPgOidType());
  }
  switch (args[0].type_kind()) {
    case zetasql::TYPE_INT64: {
      // Casting bigint to PG.OID accepts inputs in the range [0, 4294967295]
      // and casts it to OID of the same value.
      int64_t val = args[0].int64_value();
      // PostgreSQL oid values are uint32_t.
      if (val < std::numeric_limits<uint32_t>::min() ||
          val > std::numeric_limits<uint32_t>::max()) {
        return absl::OutOfRangeError("bigint out of range");
      }
      return spangres::datatypes::CreatePgOidValue(val);
    }
    case zetasql::TYPE_STRING: {
      // Casting varchar to PG.OID accepts inputs in the range [-2147483648,
      // 4294967295] and casts it to OID with value as follows:
      // - [-2147483648, -1] is cast to range [2147483648, 4294967295].
      // - [0, 4294967295] is cast to range [0, 4294967295].
      int64_t oid_val;
      if (!absl::SimpleAtoi(args[0].string_value(), &oid_val)) {
        return absl::InvalidArgumentError("invalid varchar");
      }
      // PostgreSQL oid values are in [int32_t::min(), uint32_t::max()]
      if (oid_val < std::numeric_limits<int32_t>::min() ||
          oid_val > std::numeric_limits<uint32_t>::max()) {
        return absl::OutOfRangeError("varchar out of range");
      }
      return spangres::datatypes::CreatePgOidValue(oid_val);
    }
    default:
      return absl::InvalidArgumentError(absl::StrCat(
          "cannot cast type ",
          args[0].type()->ShortTypeName(zetasql::PRODUCT_EXTERNAL),
          " to oid"));
  }
}

absl::StatusOr<zetasql::Value> EvalCastOidToInt64(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK_EQ(args.size(), 1);
  ZETASQL_RET_CHECK_EQ(args[0].type(), spangres::datatypes::GetPgOidType());
  if (args[0].is_null()) {
    return zetasql::Value::NullInt64();
  }
  ZETASQL_ASSIGN_OR_RETURN(int64_t oid, spangres::datatypes::GetPgOidValue(args[0]));
  return zetasql::Value::Int64(oid);
}

absl::StatusOr<zetasql::Value> EvalCastOidToString(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK_EQ(args.size(), 1);
  ZETASQL_RET_CHECK_EQ(args[0].type(), spangres::datatypes::GetPgOidType());
  if (args[0].is_null()) {
    return zetasql::Value::NullString();
  }
  ZETASQL_ASSIGN_OR_RETURN(int64_t oid, spangres::datatypes::GetPgOidValue(args[0]));
  return zetasql::Value::String(absl::StrCat(oid));
}

namespace {
inline bool FloatDivide(float in1, float in2, float* out, absl::Status* error) {
  if (ABSL_PREDICT_FALSE(in2 == 0)) {
    *error = absl::OutOfRangeError(
        absl::StrCat("division by zero: ", in1, " / ", in2));
    return false;
  }
  *out = in1 / in2;
  if (ABSL_PREDICT_TRUE(std::isfinite(*out))) {
    return true;
  } else if (!std::isfinite(in1) || !std::isfinite(in2)) {
    return true;
  } else {
    *error = absl::OutOfRangeError(
        absl::StrCat("float overflow: ", in1, " / ", in2));
    return false;
  }
}
}  // namespace

absl::StatusOr<zetasql::Value> EvalFloatArithmetic(
    absl::Span<const zetasql::Value> args,
    std::function<bool(float, float, float*, absl::Status*)> Fn) {
  ZETASQL_RET_CHECK(args.size() == 2 && Fn != nullptr);
  if (args[0].is_null() || args[1].is_null()) {
    return zetasql::Value::Null(zetasql::types::FloatType());
  }

  float arg0 = args[0].float_value();
  float arg1 = args[1].float_value();

  float result;
  absl::Status error;
  bool is_success = Fn(arg0, arg1, &result, &error);

  if (!is_success || !error.ok()) {
    return error;
  }

  return zetasql::Value::Float(result);
}

std::unique_ptr<zetasql::Function> FloatArithmeticFunction(
    absl::string_view function_name, absl::string_view catalog_name) {
  zetasql::FunctionOptions function_options;

  if (function_name == kPGFloatAddFunctionName) {
    function_options.set_evaluator(
        PGFunctionEvaluator([&](absl::Span<const zetasql::Value> args) {
          return EvalFloatArithmetic(args, zetasql::functions::Add<float>);
        }));
  } else if (function_name == kPGFloatSubtractFunctionName) {
    function_options.set_evaluator(
        PGFunctionEvaluator([&](absl::Span<const zetasql::Value> args) {
          return EvalFloatArithmetic(args,
                                     zetasql::functions::Subtract<float>);
        }));
  } else if (function_name == kPGFloatMultiplyFunctionName) {
    function_options.set_evaluator(
        PGFunctionEvaluator([&](absl::Span<const zetasql::Value> args) {
          return EvalFloatArithmetic(args,
                                     zetasql::functions::Multiply<float>);
        }));
  } else if (function_name == kPGFloatDivideFunctionName) {
    function_options.set_evaluator(
        PGFunctionEvaluator([&](absl::Span<const zetasql::Value> args) {
          return EvalFloatArithmetic(args, FloatDivide);
        }));
  } else {
    ABSL_DCHECK(false) << "Unsupported float arithmetic function: "
                  << function_name;
    return nullptr;
  }

  return std::make_unique<zetasql::Function>(
      function_name, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{{gsql_float,
                                                 {gsql_float, gsql_float},
                                                 /*context_ptr=*/nullptr}},
      function_options);
}

std::unique_ptr<zetasql::Function> IntervalAddSubtractFunction(
    absl::string_view function_name, absl::string_view catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator([function_name](
                                     absl::Span<const zetasql::Value> args)
                                     -> absl::StatusOr<zetasql::Value> {
    ZETASQL_RET_CHECK_EQ(args.size(), 2);
    ZETASQL_RET_CHECK_EQ(args[0].type(), zetasql::types::IntervalType());
    ZETASQL_RET_CHECK_EQ(args[1].type(), zetasql::types::IntervalType());

    if (HasNullValue(args)) {
      return zetasql::Value::NullInterval();
    }

    ZETASQL_ASSIGN_OR_RETURN(zetasql::IntervalValue arg0,
                     RoundPrecision(args[0].interval_value()));
    ZETASQL_ASSIGN_OR_RETURN(zetasql::IntervalValue arg1,
                     RoundPrecision(args[1].interval_value()));

    absl::StatusOr<zetasql::IntervalValue> result =
        function_name == kPGIntervalAddFunctionName ? arg0 + arg1 : arg0 - arg1;
    ZETASQL_RETURN_IF_ERROR(result.status());
    return zetasql::Value::Interval(*result);
  });

  return std::make_unique<zetasql::Function>(
      function_name, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_interval,
                                       {gsql_interval, gsql_interval},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

std::unique_ptr<zetasql::Function> IntervalUnaryMinusFunction(
    absl::string_view catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator([](absl::Span<const zetasql::Value> args)
                                     -> absl::StatusOr<zetasql::Value> {
    ZETASQL_RET_CHECK_EQ(args.size(), 1);
    ZETASQL_RET_CHECK_EQ(args[0].type(), zetasql::types::IntervalType());
    if (HasNullValue(args)) {
      return zetasql::Value::NullInterval();
    }
    ZETASQL_ASSIGN_OR_RETURN(zetasql::IntervalValue arg0,
                     RoundPrecision(args[0].interval_value()));
    return zetasql::Value::Interval(-arg0);
  });

  return std::make_unique<zetasql::Function>(
      kPGIntervalUnaryMinusFunctionName, catalog_name,
      zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_interval,
                                       {gsql_interval},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

std::unique_ptr<zetasql::Function> IntervalMultiplyDivideFunction(
    absl::string_view function_name, absl::string_view catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(
      [function_name](absl::Span<const zetasql::Value> args)
          -> absl::StatusOr<zetasql::Value> {
        ZETASQL_RET_CHECK_EQ(args.size(), 2);
        ZETASQL_RET_CHECK_EQ(args[0].type(), zetasql::types::IntervalType());
        ZETASQL_RET_CHECK_EQ(args[1].type(), zetasql::types::DoubleType());
        if (HasNullValue(args)) {
          return zetasql::Value::NullInterval();
        }
        ZETASQL_ASSIGN_OR_RETURN(zetasql::IntervalValue arg0,
                         RoundPrecision(args[0].interval_value()));

        absl::StatusOr<zetasql::IntervalValue> result =
            function_name == kPGIntervalMultiplyFunctionName
                ? PgIntervalMultiply(arg0, args[1].double_value())
                : PgIntervalDivide(arg0, args[1].double_value());
        ZETASQL_RETURN_IF_ERROR(result.status());
        return zetasql::Value::Interval(*result);
      }));

  return std::make_unique<zetasql::Function>(
      function_name, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_interval,
                                       {gsql_interval, gsql_double},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

std::unique_ptr<zetasql::Function> TimestamptzSubtractTimestamptzFunction(
    absl::string_view catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      PGFunctionEvaluator([](absl::Span<const zetasql::Value> args)
                              -> absl::StatusOr<zetasql::Value> {
        ZETASQL_RET_CHECK_EQ(args.size(), 2);
        ZETASQL_RET_CHECK_EQ(args[0].type(), zetasql::types::TimestampType());
        ZETASQL_RET_CHECK_EQ(args[1].type(), zetasql::types::TimestampType());
        if (HasNullValue(args)) {
          return zetasql::Value::NullInterval();
        }
        absl::Time arg0 =
            absl::FromUnixMicros(absl::ToUnixMicros(args[0].ToTime()));
        absl::Time arg1 =
            absl::FromUnixMicros(absl::ToUnixMicros(args[1].ToTime()));
        ZETASQL_ASSIGN_OR_RETURN(
            zetasql::IntervalValue result,
            zetasql::functions::IntervalDiffTimestamps(arg0, arg1));
        return zetasql::Value::Interval(result);
      }));
  return std::make_unique<zetasql::Function>(
      kPGTimestamptzSubtractTimestamptzFunctionName, catalog_name,
      zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_interval,
                                       {gsql_timestamp, gsql_timestamp},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

std::unique_ptr<zetasql::Function> IntervalJustifyFunction(
    absl::string_view function_name, absl::string_view catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(
      [function_name](absl::Span<const zetasql::Value> args)
          -> absl::StatusOr<zetasql::Value> {
        ZETASQL_RET_CHECK_EQ(args.size(), 1);
        ZETASQL_RET_CHECK_EQ(args[0].type(), zetasql::types::IntervalType());
        if (HasNullValue(args)) {
          return zetasql::Value::NullInterval();
        }
        ZETASQL_ASSIGN_OR_RETURN(zetasql::IntervalValue arg0,
                         RoundPrecision(args[0].interval_value()));
        absl::StatusOr<zetasql::IntervalValue> result =
            function_name == kPGIntervalJustifyIntervalFunctionName
                ? zetasql::JustifyInterval(arg0)
            : function_name == kPGIntervalJustifyDaysFunctionName
                ? zetasql::JustifyDays(arg0)
                : zetasql::JustifyHours(arg0);
        ZETASQL_RETURN_IF_ERROR(result.status());
        return zetasql::Value::Interval(*result);
      }));

  return std::make_unique<zetasql::Function>(
      function_name, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_interval,
                                       {gsql_interval},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

std::unique_ptr<zetasql::Function> MakeIntervalFunction(
    absl::string_view catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      PGFunctionEvaluator([](absl::Span<const zetasql::Value> args)
                              -> absl::StatusOr<zetasql::Value> {
        ZETASQL_RET_CHECK_EQ(args.size(), 7);
        for (int i = 0; i < 6; i++) {
          ZETASQL_RET_CHECK_EQ(args[i].type(), zetasql::types::Int64Type());
        }
        ZETASQL_RET_CHECK_EQ(args[6].type(), zetasql::types::DoubleType());
        if (HasNullValue(args)) {
          return zetasql::Value::NullInterval();
        }
        ZETASQL_ASSIGN_OR_RETURN(
            zetasql::IntervalValue result,
            PgMakeInterval(args[0].int64_value(), args[1].int64_value(),
                           args[2].int64_value(), args[3].int64_value(),
                           args[4].int64_value(), args[5].int64_value(),
                           args[6].double_value()));
        return zetasql::Value::Interval(result);
      }));

  zetasql::FunctionArgumentTypeOptions years =
      GetOptionalNamedArgumentOptions("years");
  zetasql::FunctionArgumentTypeOptions months =
      GetOptionalNamedArgumentOptions("months");
  zetasql::FunctionArgumentTypeOptions weeks =
      GetOptionalNamedArgumentOptions("weeks");
  zetasql::FunctionArgumentTypeOptions days =
      GetOptionalNamedArgumentOptions("days");
  zetasql::FunctionArgumentTypeOptions hours =
      GetOptionalNamedArgumentOptions("hours");
  zetasql::FunctionArgumentTypeOptions mins =
      GetOptionalNamedArgumentOptions("mins");
  zetasql::FunctionArgumentTypeOptions secs =
      GetOptionalNamedArgumentOptions("secs");

  return std::make_unique<zetasql::Function>(
      kPGIntervalMakeIntervalFunctionName, catalog_name,
      zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_interval,
                                       {{gsql_int64, years},
                                        {gsql_int64, months},
                                        {gsql_int64, weeks},
                                        {gsql_int64, days},
                                        {gsql_int64, hours},
                                        {gsql_int64, mins},
                                        {gsql_double, secs}},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

std::unique_ptr<zetasql::Function> IntervalExtract(
    absl::string_view catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      PGFunctionEvaluator([](absl::Span<const zetasql::Value> args)
                              -> absl::StatusOr<zetasql::Value> {
        ZETASQL_ASSIGN_OR_RETURN(zetasql::IntervalValue interval_arg,
                         RoundPrecision(args[1].interval_value()));
        ZETASQL_ASSIGN_OR_RETURN(
            absl::Cord result,
            PgIntervalExtract(args[0].string_value(), interval_arg));
        return CreatePgNumericValue(std::string(result));
      }));

  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  return std::make_unique<zetasql::Function>(
      kPGIntervalExtractFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_pg_numeric,
                                       {gsql_string, gsql_interval},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

std::unique_ptr<zetasql::Function> CastToIntervalFunction(
    absl::string_view catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalCastStringToInterval));
  return std::make_unique<zetasql::Function>(
      kPGCastToIntervalFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{gsql_interval,
                                       {gsql_string},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

SpannerPGFunctions GetSpannerPGFunctions(const std::string& catalog_name) {
  SpannerPGFunctions functions;

  auto cast_to_string_func = CastToStringFunction(catalog_name);
  functions.push_back(std::move(cast_to_string_func));

  auto cast_to_date_func = CastToDateFunction(catalog_name);
  functions.push_back(std::move(cast_to_date_func));

  auto cast_to_timestamp_func =
      CastToTimestampFunction(catalog_name);
  functions.push_back(std::move(cast_to_timestamp_func));

  auto timestamptz_add_func = TimestamptzAddFunction(catalog_name);
  functions.push_back(std::move(timestamptz_add_func));
  auto timestamptz_subtract_func = TimestamptzSubtractFunction(catalog_name);
  functions.push_back(std::move(timestamptz_subtract_func));
  auto timestamptz_bin_func = TimestamptzBinFunction(catalog_name);
  functions.push_back(std::move(timestamptz_bin_func));
  auto timestamptz_trunc_func = TimestamptzTruncFunction(catalog_name);
  functions.push_back(std::move(timestamptz_trunc_func));
  auto extract_func = ExtractFunction(catalog_name);
  functions.push_back(std::move(extract_func));

  auto map_double_to_int_func = MapDoubleToIntFunction(catalog_name);
  functions.push_back(std::move(map_double_to_int_func));

  auto map_float_to_int_func = MapFloatToIntFunction(catalog_name);
  functions.push_back(std::move(map_float_to_int_func));

  auto least_greatest_funcs = LeastGreatestFunctions(catalog_name);
  functions.push_back(std::move(least_greatest_funcs.first));   // least
  functions.push_back(std::move(least_greatest_funcs.second));  // greatest

  auto min_agg = MinAggregator(catalog_name);
  functions.push_back(std::move(min_agg));
  auto max_agg = MaxAggregator(catalog_name);
  functions.push_back(std::move(max_agg));
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

  auto to_jsonb_func = ToJsonbFunction(catalog_name);
  functions.push_back(std::move(to_jsonb_func));
  auto cast_from_jsonb_func = CastFromJsonbFunction(catalog_name);
  functions.push_back(std::move(cast_from_jsonb_func));
  auto jsonb_subscript_text_func = JsonbSubscriptTextFunction(catalog_name);
  functions.push_back(std::move(jsonb_subscript_text_func));
  auto jsonb_subscript_func = ZetaSQLSubscriptFunction(catalog_name);
  functions.push_back(std::move(jsonb_subscript_func));
  auto jsonb_typeof_func = JsonbTypeofFunction(catalog_name);
  functions.push_back(std::move(jsonb_typeof_func));
  auto jsonb_query_array_func = JsonbQueryArrayFunction(catalog_name);
  functions.push_back(std::move(jsonb_query_array_func));
  auto jsonb_build_array_func = JsonbBuildArrayFunction(catalog_name);
  functions.push_back(std::move(jsonb_build_array_func));
  auto jsonb_build_object_func = JsonbBuildObjectFunction(catalog_name);
  functions.push_back(std::move(jsonb_build_object_func));
  auto jsonb_delete_func = JsonbDeleteFunction(catalog_name);
  functions.push_back(std::move(jsonb_delete_func));
  auto jsonb_delete_path_func = JsonbDeletePathFunction(catalog_name);
  functions.push_back(std::move(jsonb_delete_path_func));
  auto jsonb_concat_func = JsonbConcatFunction(catalog_name);
  functions.push_back(std::move(jsonb_concat_func));
  auto jsonb_insert_func = JsonbInsertFunction(catalog_name);
  functions.push_back(std::move(jsonb_insert_func));
  auto jsonb_strip_nulls_func = JsonbStripNullsFunction(catalog_name);
  functions.push_back(std::move(jsonb_strip_nulls_func));
  auto jsonb_set_func = JsonbSetFunction(catalog_name);
  functions.push_back(std::move(jsonb_set_func));
  auto jsonb_set_lax_func = JsonbSetLaxFunction(catalog_name);
  functions.push_back(std::move(jsonb_set_lax_func));
  auto jsonb_contains_func = JsonbContainsFunction(catalog_name);
  functions.push_back(std::move(jsonb_contains_func));
  auto jsonb_contained_func = JsonbContainedFunction(catalog_name);
  functions.push_back(std::move(jsonb_contained_func));
  auto jsonb_exists_func = JsonbExistsFunction(catalog_name);
  functions.push_back(std::move(jsonb_exists_func));
  auto jsonb_exists_any_func = JsonbExistsAnyFunction(catalog_name);
  functions.push_back(std::move(jsonb_exists_any_func));
  auto jsonb_exists_all_func = JsonbExistsAllFunction(catalog_name);
  functions.push_back(std::move(jsonb_exists_all_func));

  auto float_add_func =
      FloatArithmeticFunction(kPGFloatAddFunctionName, catalog_name);
  functions.push_back(std::move(float_add_func));
  auto float_subtract_func =
      FloatArithmeticFunction(kPGFloatSubtractFunctionName, catalog_name);
  functions.push_back(std::move(float_subtract_func));
  auto float_multiply_func =
      FloatArithmeticFunction(kPGFloatMultiplyFunctionName, catalog_name);
  functions.push_back(std::move(float_multiply_func));
  auto float_divide_func =
      FloatArithmeticFunction(kPGFloatDivideFunctionName, catalog_name);
  functions.push_back(std::move(float_divide_func));

  auto numeric_abs_func = NumericAbsFunction(catalog_name);
  functions.push_back(std::move(numeric_abs_func));
  auto numeric_add_func = NumericAddFunction(catalog_name);
  functions.push_back(std::move(numeric_add_func));
  auto numeric_ceil_func = NumericCeilFunction(catalog_name);
  functions.push_back(std::move(numeric_ceil_func));
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
  auto cast_numeric_to_int64_func = CastNumericToInt64Function(catalog_name);
  functions.push_back(std::move(cast_numeric_to_int64_func));
  auto cast_numeric_to_double_func = CastNumericToDoubleFunction(catalog_name);
  functions.push_back(std::move(cast_numeric_to_double_func));
  auto cast_numeric_to_float_func = CastNumericToFloatFunction(catalog_name);
  functions.push_back(std::move(cast_numeric_to_float_func));
  auto cast_to_numeric_func = CastToNumericFunction(catalog_name);
  functions.push_back(std::move(cast_to_numeric_func));

  auto cast_to_oid_func = CastToOidFunction(catalog_name);
  functions.push_back(std::move(cast_to_oid_func));
  auto cast_from_oid_func = CastFromOidFunction(catalog_name);
  functions.push_back(std::move(cast_from_oid_func));
  // JSONB extraction functions.
  functions.push_back(JsonbArrayExtractionFunction<int64_t>(catalog_name));
  functions.push_back(JsonbFloatArrayExtractionFunction<double>(catalog_name));
  functions.push_back(JsonbArrayExtractionFunction<bool>(catalog_name));
  functions.push_back(JsonbArrayExtractionFunction<std::string>(catalog_name));

  auto array_overlap_function = ArrayOverlapFunction(catalog_name);
  functions.push_back(std::move(array_overlap_function));
  auto array_contains_function = ArrayContainsOrContainedFunction(
      catalog_name, /*is_array_contains=*/true);
  functions.push_back(std::move(array_contains_function));
  auto array_contained_function = ArrayContainsOrContainedFunction(
      catalog_name, /*is_array_contains=*/false);
  functions.push_back(std::move(array_contained_function));
  auto array_all_equal =
      ArrayAllFunction(catalog_name, "=", "pg.array_all_equal");
  functions.push_back(std::move(array_all_equal));
  auto array_all_greater =
      ArrayAllFunction(catalog_name, ">", "pg.array_all_greater");
  functions.push_back(std::move(array_all_greater));
  auto array_all_greater_equal =
      ArrayAllFunction(catalog_name, ">=", "pg.array_all_greater_equal");
  functions.push_back(std::move(array_all_greater_equal));
  auto array_all_less =
      ArrayAllFunction(catalog_name, "<", "pg.array_all_less");
  functions.push_back(std::move(array_all_less));
  auto array_all_less_equal =
      ArrayAllFunction(catalog_name, "<=", "pg.array_all_less_equal");
  functions.push_back(std::move(array_all_less_equal));
  // `<> all` is intentionally ommitted because it is equivalent to `NOT IN`
  // and the transformer handles it as such.
  auto array_slice_function = ArraySliceFunction(catalog_name);
  functions.push_back(std::move(array_slice_function));

  // interval functions
  auto interval_add_func =
      IntervalAddSubtractFunction(kPGIntervalAddFunctionName, catalog_name);
  functions.push_back(std::move(interval_add_func));
  auto interval_subtract_func = IntervalAddSubtractFunction(
      kPGIntervalSubtractFunctionName, catalog_name);
  functions.push_back(std::move(interval_subtract_func));
  auto interval_unary_minus_func = IntervalUnaryMinusFunction(catalog_name);
  functions.push_back(std::move(interval_unary_minus_func));
  auto interval_multiply_func = IntervalMultiplyDivideFunction(
      kPGIntervalMultiplyFunctionName, catalog_name);
  functions.push_back(std::move(interval_multiply_func));
  auto interval_divide_func = IntervalMultiplyDivideFunction(
      kPGIntervalDivideFunctionName, catalog_name);
  functions.push_back(std::move(interval_divide_func));
  auto interval_justify_func = IntervalJustifyFunction(
      kPGIntervalJustifyIntervalFunctionName, catalog_name);
  functions.push_back(std::move(interval_justify_func));
  auto interval_justify_days_func =
      IntervalJustifyFunction(kPGIntervalJustifyDaysFunctionName, catalog_name);
  functions.push_back(std::move(interval_justify_days_func));
  auto interval_justify_hours_func = IntervalJustifyFunction(
      kPGIntervalJustifyHoursFunctionName, catalog_name);
  functions.push_back(std::move(interval_justify_hours_func));
  auto interval_make_func = MakeIntervalFunction(catalog_name);
  functions.push_back(std::move(interval_make_func));
  auto interval_cast_to_interval_func = CastToIntervalFunction(catalog_name);
  functions.push_back(std::move(interval_cast_to_interval_func));
  auto timestamptz_subtract_timestamptz_func =
      TimestamptzSubtractTimestamptzFunction(catalog_name);
  functions.push_back(std::move(timestamptz_subtract_timestamptz_func));
  auto interval_extract_func = IntervalExtract(catalog_name);
  functions.push_back(std::move(interval_extract_func));
  return functions;
}

SpannerPGTVFs GetSpannerPGTVFs(const std::string& catalog_name) {
  SpannerPGTVFs tvfs;
  auto jsonb_array_elements_tvf =
      std::make_unique<JsonbArrayElementsTableValuedFunction>();
  tvfs.push_back(std::move(jsonb_array_elements_tvf));
  return tvfs;
}

}  // namespace postgres_translator
