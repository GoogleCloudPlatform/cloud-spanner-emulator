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

#include "googlesql/public/function.h"
#include "googlesql/public/function.pb.h"
#include "googlesql/public/function_signature.h"
#include "googlesql/public/functions/arithmetics.h"
#include "googlesql/public/functions/date_time_util.h"
#include "googlesql/public/functions/generate_array.h"
#include "googlesql/public/functions/json.h"
#include "googlesql/public/interval_value.h"
#include "googlesql/public/json_value.h"
#include "googlesql/public/options.pb.h"
#include "googlesql/public/table_valued_function.h"
#include "googlesql/public/type.pb.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/public/value.h"
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
#include "googlesql/base/mathutil.h"
#include "googlesql/common/string_util.h"
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
#include "googlesql/base/ret_check.h"
#include "googlesql/base/status_macros.h"

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

using ::googlesql::FunctionArgumentType;
using ::googlesql::FunctionArgumentTypeOptions;
using ::googlesql::FunctionOptions;
using ::googlesql::FunctionSignature;
using ::googlesql::FunctionSignatureOptions;
using ::googlesql::FunctionSignatureRewriteOptions;

using MathUtil = ::googlesql_base::MathUtil;

const googlesql::Type* gsql_bool = googlesql::types::BoolType();
const googlesql::Type* gsql_bytes = googlesql::types::BytesType();
const googlesql::Type* gsql_date = googlesql::types::DateType();
const googlesql::Type* gsql_float = googlesql::types::FloatType();
const googlesql::Type* gsql_double = googlesql::types::DoubleType();
const googlesql::Type* gsql_int64 = googlesql::types::Int64Type();
const googlesql::Type* gsql_string = googlesql::types::StringType();
const googlesql::Type* gsql_timestamp = googlesql::types::TimestampType();

const googlesql::Type* gsql_interval = googlesql::types::IntervalType();
const googlesql::Type* gsql_interval_array =
    googlesql::types::IntervalArrayType();

const googlesql::Type* gsql_uuid = googlesql::types::UuidType();
const googlesql::Type* gsql_uuid_array = googlesql::types::UuidArrayType();
const googlesql::ArrayType* gsql_bool_array = googlesql::types::BoolArrayType();
const googlesql::ArrayType* gsql_bytes_array =
    googlesql::types::BytesArrayType();
const googlesql::ArrayType* gsql_date_array = googlesql::types::DateArrayType();
const googlesql::ArrayType* gsql_double_array =
    googlesql::types::DoubleArrayType();
const googlesql::ArrayType* gsql_float_array =
    googlesql::types::FloatArrayType();
const googlesql::ArrayType* gsql_int64_array =
    googlesql::types::Int64ArrayType();
const googlesql::ArrayType* gsql_string_array =
    googlesql::types::StringArrayType();
const googlesql::ArrayType* gsql_timestamp_array =
    googlesql::types::TimestampArrayType();

constexpr char kNan[] = "NaN";
constexpr char kNanString[] = "\"NaN\"";
constexpr char kInfString[] = "\"Infinity\"";
constexpr char kNegInfString[] = "\"-Infinity\"";
constexpr char kFalse[] = "false";
constexpr char kTrue[] = "true";

const char kSpannerFunctionGroup[] = "Spanner";

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
using ::postgres_translator::function_evaluators::Texticlike;
using ::postgres_translator::function_evaluators::Texticnlike;
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

googlesql::FunctionArgumentTypeOptions GetOptionalNamedArgumentOptions(
    absl::string_view name) {
  googlesql::FunctionArgumentTypeOptions options;
  options.set_cardinality(googlesql::FunctionArgumentType::OPTIONAL);
  options.set_argument_name(name, googlesql::kPositionalOrNamed);
  return options;
}

googlesql::FunctionArgumentTypeOptions GetOptionalPositionalArgumentOptions(
    absl::string_view name) {
  googlesql::FunctionArgumentTypeOptions options;
  options.set_cardinality(googlesql::FunctionArgumentType::OPTIONAL);
  options.set_argument_name(name, googlesql::kPositionalOnly);
  return options;
}

googlesql::FunctionArgumentTypeOptions GetRequiredPositionalArgumentOptions(
    absl::string_view name) {
  googlesql::FunctionArgumentTypeOptions options;
  options.set_cardinality(googlesql::FunctionArgumentType::REQUIRED);
  options.set_argument_name(name, googlesql::kPositionalOnly);
  return options;
}

bool HasNullValue(absl::Span<const googlesql::Value> args) {
  return absl::c_any_of(
      args, [](const googlesql::Value& arg) { return arg.is_null(); });
}

// PG array functions

// Used by both array_upper and array_length because they return the same
// result for one-dimensional arrays which have the default lower bound of 1.
// This is the only type of array spangres supports.
absl::StatusOr<googlesql::Value> EvalArrayUpper(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return googlesql::Value::NullInt64();
  }

  // Zero or negative dimensions return NULL
  // Empty arrays return NULL
  if (args[1].int64_value() <= 0 || args[0].num_elements() == 0) {
    return googlesql::Value::NullInt64();
  }

  if (args[1].int64_value() > 1) {
    return absl::InvalidArgumentError(
        "multi-dimensional arrays are not supported");
  }

  return googlesql::Value::Int64(args[0].num_elements());
}

// Used by both array_upper and array_length because they both have similar
// signatures and behavior for one-dimensional arrays which have the default
// lower bound (i.e., starting index) of 1. This is the only type of array
// spangres supports.
std::unique_ptr<googlesql::Function> ArrayUpperFunction(
    absl::string_view catalog_name, absl::string_view function_name) {
  const auto gsql_anyarray = googlesql::ARG_KIND_EXPR_ARRAY_ANY_1;

  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(googlesql::FunctionEvaluator(EvalArrayUpper));
  return std::make_unique<googlesql::Function>(
      function_name, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_int64,
                                       {gsql_anyarray, gsql_int64},
                                       /*context_ptr=*/nullptr},
      },
      function_options);
}

// PG comparison functions
absl::StatusOr<googlesql::Value> EvalTextregexne(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return googlesql::Value::NullBool();
  }

  GOOGLESQL_ASSIGN_OR_RETURN(bool result,
                   Textregexne(args[0].string_value(), args[1].string_value()));
  return googlesql::Value::Bool(result);
}

std::unique_ptr<googlesql::Function> TextregexneFunction(
    absl::string_view catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(
      EvalTextregexne, InitializePGTimezoneToDefault, CleanupRegexCache));
  return std::make_unique<googlesql::Function>(
      kPGTextregexneFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{googlesql::FunctionSignature{
          gsql_bool, {gsql_string, gsql_string}, /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalPgILike(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return googlesql::Value::NullBool();
  }

  GOOGLESQL_ASSIGN_OR_RETURN(bool result,
                   Texticlike(args[0].string_value(), args[1].string_value()));
  return googlesql::Value::Bool(result);
}

std::unique_ptr<googlesql::Function> PgILikeFunction(
    absl::string_view catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(true);
  function_options.set_evaluator(PGFunctionEvaluator(
      EvalPgILike, InitializePGTimezoneToDefault, CleanupRegexCache));
  return std::make_unique<googlesql::Function>(
      kPGILikeFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{googlesql::FunctionSignature{
          gsql_bool, {gsql_string, gsql_string}, /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalPgNotILike(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return googlesql::Value::NullBool();
  }

  GOOGLESQL_ASSIGN_OR_RETURN(bool result,
                   Texticnlike(args[0].string_value(), args[1].string_value()));
  return googlesql::Value::Bool(result);
}

std::unique_ptr<googlesql::Function> PgNotILikeFunction(
    absl::string_view catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(true);
  function_options.set_evaluator(PGFunctionEvaluator(
      EvalPgNotILike, InitializePGTimezoneToDefault, CleanupRegexCache));
  return std::make_unique<googlesql::Function>(
      kPGNotILikeFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{googlesql::FunctionSignature{
          gsql_bool, {gsql_string, gsql_string}, /*context_ptr=*/nullptr}},
      function_options);
}

// PG datetime functions
absl::StatusOr<googlesql::Value> EvalDateMi(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return googlesql::Value::NullInt64();
  }

  return googlesql::Value::Int64(args[0].date_value() - args[1].date_value());
}

std::unique_ptr<googlesql::Function> DateMiFunction(
    absl::string_view catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(googlesql::FunctionEvaluator(EvalDateMi));
  return std::make_unique<googlesql::Function>(
      kPGDateMiFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{googlesql::FunctionSignature{
          gsql_int64, {gsql_date, gsql_date}, /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalDateMii(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return googlesql::Value::NullDate();
  }

  GOOGLESQL_ASSIGN_OR_RETURN(int32_t result,
                   DateMii(args[0].date_value(), args[1].int64_value()));

  return googlesql::Value::Date(result);
}

std::unique_ptr<googlesql::Function> DateMiiFunction(
    absl::string_view catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(EvalDateMii));
  return std::make_unique<googlesql::Function>(
      kPGDateMiiFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{googlesql::FunctionSignature{
          gsql_date, {gsql_date, gsql_int64}, /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalDatePli(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return googlesql::Value::NullDate();
  }

  GOOGLESQL_ASSIGN_OR_RETURN(int32_t result,
                   DatePli(args[0].date_value(), args[1].int64_value()));

  return googlesql::Value::Date(result);
}

std::unique_ptr<googlesql::Function> DatePliFunction(
    absl::string_view catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(EvalDatePli));
  return std::make_unique<googlesql::Function>(
      kPGDatePliFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{googlesql::FunctionSignature{
          gsql_date, {gsql_date, gsql_int64}, /*context_ptr=*/nullptr}},
      function_options);
}

// PG formatting functions
absl::StatusOr<googlesql::Value> EvalToDate(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return googlesql::Value::NullDate();
  }

  GOOGLESQL_ASSIGN_OR_RETURN(int32_t result,
                   PgToDate(args[0].string_value(), args[1].string_value()));

  return googlesql::Value::Date(result);
}

std::unique_ptr<googlesql::Function> ToDateFunction(
    absl::string_view catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(
      EvalToDate, InitializePGTimezoneToDefault, CleanupPostgresDateTimeCache));
  return std::make_unique<googlesql::Function>(
      kPGToDateFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{googlesql::FunctionSignature{
          gsql_date, {gsql_string, gsql_string}, /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalToTimestamp(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return googlesql::Value::NullTimestamp();
  }

  GOOGLESQL_ASSIGN_OR_RETURN(absl::Time result,
                   ToTimestamp(args[0].string_value(), args[1].string_value()));

  return googlesql::Value::Timestamp(result);
}

std::unique_ptr<googlesql::Function> ToTimestampFunction(
    absl::string_view catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(
      PGFunctionEvaluator(EvalToTimestamp, InitializePGTimezoneToDefault,
                          CleanupPostgresDateTimeCache));
  return std::make_unique<googlesql::Function>(
      kPGToTimestampFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{googlesql::FunctionSignature{
          gsql_timestamp, {gsql_string, gsql_string}, /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<googlesql::IntervalValue> RoundPrecision(
    const googlesql::IntervalValue& interval) {
  int64_t micros = MathUtil::Round<int64_t>(
      (interval.get_nanos() * 1.0) / googlesql::IntervalValue::kNanosInMicro);
  return googlesql::IntervalValue::FromMonthsDaysMicros(
      interval.get_months(), interval.get_days(), micros);
}

std::unique_ptr<googlesql::Function> ToCharFunction(
    absl::string_view catalog_name) {
  static const googlesql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(
      PGFunctionEvaluator(EvalToChar, InitializePGTimezoneToDefault, [] {
        CleanupPostgresNumberCache();
        CleanupPostgresDateTimeCache();
      }));
  return std::make_unique<googlesql::Function>(
      kPGToCharFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{
              gsql_string, {gsql_int64, gsql_string}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{gsql_string,
                                       {gsql_timestamp, gsql_string},
                                       /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{gsql_string,
                                       {gsql_double, gsql_string},
                                       /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{gsql_string,
                                       {gsql_float, gsql_string},
                                       /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{gsql_string,
                                       {gsql_pg_numeric, gsql_string},
                                       /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{gsql_string,
                                       {gsql_interval, gsql_string},
                                       /*context_ptr=*/nullptr},
      },
      function_options);
}

absl::StatusOr<googlesql::Value> EvalToNumber(
    absl::Span<const googlesql::Value> args) {
  static const googlesql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();
  GOOGLESQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return googlesql::Value::Null(gsql_pg_numeric);
  }

  GOOGLESQL_ASSIGN_OR_RETURN(
      std::unique_ptr<std::string> result,
      NumericToNumber(args[0].string_value(), args[1].string_value()));

  if (result == nullptr) {
    return googlesql::Value::Null(gsql_pg_numeric);
  }
  return CreatePgNumericValue(*result);
}

std::unique_ptr<googlesql::Function> ToNumberFunction(
    absl::string_view catalog_name) {
  static const googlesql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(
      EvalToNumber, InitializePGTimezoneToDefault, CleanupPostgresNumberCache));

  return std::make_unique<googlesql::Function>(
      kPGToNumberFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{{gsql_pg_numeric,
                                                 {gsql_string, gsql_string},
                                                 /*context_ptr=*/nullptr}},
      function_options);
}

// PG.NUMERIC Mathematical functions

absl::StatusOr<googlesql::Value> EvalGoogleSQLAbs(
    absl::Span<const googlesql::Value> args) {
  static const googlesql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  GOOGLESQL_RET_CHECK(args.size() == 1);
  if (args[0].is_null()) {
    return googlesql::Value::Null(gsql_pg_numeric);
  }
  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord normalized_value,
                   GetPgNumericNormalizedValue(args[0]));

  GOOGLESQL_ASSIGN_OR_RETURN(std::string result, Abs(std::string(normalized_value)));

  return CreatePgNumericValue(result);
}

std::unique_ptr<googlesql::Function> NumericAbsFunction(
    absl::string_view catalog_name) {
  static const googlesql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalGoogleSQLAbs));
  return std::make_unique<googlesql::Function>(
    kGoogleSQLAbsFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{googlesql::FunctionSignature{
          gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalGoogleSQLAdd(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return googlesql::Value::Null(
        postgres_translator::spangres::datatypes::GetPgNumericType());
  }

  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord lhs, GetPgNumericNormalizedValue(args[0]));
  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord rhs, GetPgNumericNormalizedValue(args[1]));
  GOOGLESQL_ASSIGN_OR_RETURN(std::string result, Add(std::string(lhs), std::string(rhs)));

  return CreatePgNumericValue(result);
}

std::unique_ptr<googlesql::Function> NumericAddFunction(
    absl::string_view catalog_name) {
  const googlesql::Type* gsql_pg_numeric =
      postgres_translator::spangres::datatypes::GetPgNumericType();
  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalGoogleSQLAdd));

  return std::make_unique<googlesql::Function>(
      kGoogleSQLAddFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          {gsql_pg_numeric,
           {gsql_pg_numeric, gsql_pg_numeric},
           /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalGoogleSQLCeil(
    absl::Span<const googlesql::Value> args) {
  static const googlesql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  GOOGLESQL_RET_CHECK(args.size() == 1);
  if (args[0].is_null()) {
    return googlesql::Value::Null(gsql_pg_numeric);
  }
  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord normalized_value,
                   GetPgNumericNormalizedValue(args[0]));

  GOOGLESQL_ASSIGN_OR_RETURN(std::string result, Ceil(std::string(normalized_value)));

  return CreatePgNumericValue(result);
}

std::unique_ptr<googlesql::Function> NumericCeilFunction(
    absl::string_view catalog_name) {
  static const googlesql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalGoogleSQLCeil));
  function_options.set_alias_name("ceiling");
  return std::make_unique<googlesql::Function>(
      kGoogleSQLCeilFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{googlesql::FunctionSignature{
          gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalGoogleSQLDivide(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return googlesql::Value::Null(
        postgres_translator::spangres::datatypes::GetPgNumericType());
  }

  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord lhs, GetPgNumericNormalizedValue(args[0]));
  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord rhs, GetPgNumericNormalizedValue(args[1]));
  GOOGLESQL_ASSIGN_OR_RETURN(std::string result,
                   Divide(std::string(lhs), std::string(rhs)));

  return CreatePgNumericValue(result);
}

std::unique_ptr<googlesql::Function> NumericDivideFunction(
    absl::string_view catalog_name) {
  const googlesql::Type* gsql_pg_numeric =
      postgres_translator::spangres::datatypes::GetPgNumericType();
  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalGoogleSQLDivide));

  return std::make_unique<googlesql::Function>(
      kGoogleSQLDivideFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          {gsql_pg_numeric,
           {gsql_pg_numeric, gsql_pg_numeric},
           /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalGoogleSQLDivTrunc(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return googlesql::Value::Null(
        postgres_translator::spangres::datatypes::GetPgNumericType());
  }

  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord lhs, GetPgNumericNormalizedValue(args[0]));
  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord rhs, GetPgNumericNormalizedValue(args[1]));
  GOOGLESQL_ASSIGN_OR_RETURN(std::string result, DivideTruncateTowardsZero(
                                           std::string(lhs), std::string(rhs)));

  return CreatePgNumericValue(result);
}

std::unique_ptr<googlesql::Function> NumericDivTruncFunction(
    absl::string_view catalog_name) {
  const googlesql::Type* gsql_pg_numeric =
      postgres_translator::spangres::datatypes::GetPgNumericType();
  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalGoogleSQLDivTrunc));

  return std::make_unique<googlesql::Function>(
      kGoogleSQLDivTruncFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          {gsql_pg_numeric,
           {gsql_pg_numeric, gsql_pg_numeric},
           /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalGoogleSQLFloor(
    absl::Span<const googlesql::Value> args) {
  static const googlesql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  GOOGLESQL_RET_CHECK(args.size() == 1);
  if (args[0].is_null()) {
    return googlesql::Value::Null(gsql_pg_numeric);
  }
  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord normalized_value,
                   GetPgNumericNormalizedValue(args[0]));

  GOOGLESQL_ASSIGN_OR_RETURN(std::string result, Floor(std::string(normalized_value)));

  return CreatePgNumericValue(result);
}

std::unique_ptr<googlesql::Function> NumericFloorFunction(
    absl::string_view catalog_name) {
  static const googlesql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalGoogleSQLFloor));
  return std::make_unique<googlesql::Function>(
      kGoogleSQLFloorFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{googlesql::FunctionSignature{
          gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalGoogleSQLMod(
    absl::Span<const googlesql::Value> args) {
  static const googlesql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  GOOGLESQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return googlesql::Value::Null(gsql_pg_numeric);
  }
  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord normalized_value_1,
                   GetPgNumericNormalizedValue(args[0]));
  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord normalized_value_2,
                   GetPgNumericNormalizedValue(args[1]));

  GOOGLESQL_ASSIGN_OR_RETURN(std::string result, Mod(std::string(normalized_value_1),
                                           std::string(normalized_value_2)));

  return CreatePgNumericValue(result);
}

std::unique_ptr<googlesql::Function> NumericModFunction(
    absl::string_view catalog_name) {
  static const googlesql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalGoogleSQLMod));
  return std::make_unique<googlesql::Function>(
      kGoogleSQLModFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_pg_numeric,
                                       {gsql_pg_numeric, gsql_pg_numeric},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalGoogleSQLMultiply(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return googlesql::Value::Null(
        postgres_translator::spangres::datatypes::GetPgNumericType());
  }

  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord lhs, GetPgNumericNormalizedValue(args[0]));
  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord rhs, GetPgNumericNormalizedValue(args[1]));
  GOOGLESQL_ASSIGN_OR_RETURN(std::string result,
                   Multiply(std::string(lhs), std::string(rhs)));

  return CreatePgNumericValue(result);
}

std::unique_ptr<googlesql::Function> NumericMultiplyFunction(
    absl::string_view catalog_name) {
  const googlesql::Type* gsql_pg_numeric =
      postgres_translator::spangres::datatypes::GetPgNumericType();
  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalGoogleSQLMultiply));

  return std::make_unique<googlesql::Function>(
      kGoogleSQLMultiplyFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          {gsql_pg_numeric,
           {gsql_pg_numeric, gsql_pg_numeric},
           /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalGoogleSQLSubtract(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return googlesql::Value::Null(
        postgres_translator::spangres::datatypes::GetPgNumericType());
  }

  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord lhs, GetPgNumericNormalizedValue(args[0]));
  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord rhs, GetPgNumericNormalizedValue(args[1]));
  GOOGLESQL_ASSIGN_OR_RETURN(std::string result,
                   Subtract(std::string(lhs), std::string(rhs)));

  return CreatePgNumericValue(result);
}

std::unique_ptr<googlesql::Function> NumericSubtractFunction(
    absl::string_view catalog_name) {
  const googlesql::Type* gsql_pg_numeric =
      postgres_translator::spangres::datatypes::GetPgNumericType();
  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalGoogleSQLSubtract));

  return std::make_unique<googlesql::Function>(
      kGoogleSQLSubtractFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          {gsql_pg_numeric,
           {gsql_pg_numeric, gsql_pg_numeric},
           /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalGoogleSQLTrunc(
    absl::Span<const googlesql::Value> args) {
  static const googlesql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  GOOGLESQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return googlesql::Value::Null(gsql_pg_numeric);
  }
  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord normalized_value,
                   GetPgNumericNormalizedValue(args[0]));

  GOOGLESQL_ASSIGN_OR_RETURN(std::string result,
                   Trunc(std::string(normalized_value), args[1].int64_value()));

  return CreatePgNumericValue(result);
}

std::unique_ptr<googlesql::Function> NumericTruncFunction(
    absl::string_view catalog_name) {
  static const googlesql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalGoogleSQLTrunc));
  return std::make_unique<googlesql::Function>(
      kGoogleSQLTruncFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_pg_numeric,
                                       {gsql_pg_numeric, gsql_int64},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalGoogleSQLUminus(
    absl::Span<const googlesql::Value> args) {
  static const googlesql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  GOOGLESQL_RET_CHECK(args.size() == 1);
  if (args[0].is_null()) {
    return googlesql::Value::Null(gsql_pg_numeric);
  }
  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord normalized_value,
                   GetPgNumericNormalizedValue(args[0]));

  GOOGLESQL_ASSIGN_OR_RETURN(std::string result,
                   UnaryMinus(std::string(normalized_value)));

  return CreatePgNumericValue(result);
}

std::unique_ptr<googlesql::Function> NumericUminusFunction(
    absl::string_view catalog_name) {
  static const googlesql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalGoogleSQLUminus));
  return std::make_unique<googlesql::Function>(
      kGoogleSQLUminusFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{googlesql::FunctionSignature{
          gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr}},
      function_options);
}

std::unique_ptr<googlesql::Function> CastToNumericFunction(
    absl::string_view catalog_name) {
  static const googlesql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(EvalCastToNumeric));
  return std::make_unique<googlesql::Function>(
      kPGCastToNumericFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          // signatures without precision and scale
          googlesql::FunctionSignature{
              gsql_pg_numeric, {gsql_int64}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_numeric, {gsql_double}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_numeric, {gsql_float}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_numeric, {gsql_string}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_numeric, {gsql_pg_numeric}, /*context_ptr=*/nullptr},
          // signatures with precision and optional scale
          googlesql::FunctionSignature{
              gsql_pg_numeric,
              {gsql_int64,
               gsql_int64,
               {gsql_int64, googlesql::FunctionArgumentType::OPTIONAL}},
              /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_numeric,
              {gsql_double,
               gsql_int64,
               {gsql_int64, googlesql::FunctionArgumentType::OPTIONAL}},
              /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_numeric,
              {gsql_string,
               gsql_int64,
               {gsql_int64, googlesql::FunctionArgumentType::OPTIONAL}},
              /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_numeric,
              {gsql_pg_numeric,
               gsql_int64,
               {gsql_int64, googlesql::FunctionArgumentType::OPTIONAL}},
              /*context_ptr=*/nullptr},
      },
      function_options);
}

std::unique_ptr<googlesql::Function> CastNumericToDoubleFunction(
    absl::string_view catalog_name) {
  static const googlesql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(EvalCastNumericToDouble));
  return std::make_unique<googlesql::Function>(
      kPGCastNumericToDoubleFunctionName, catalog_name,
      googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{googlesql::FunctionSignature{
          gsql_double, {gsql_pg_numeric}, /*context_ptr=*/nullptr}},
      function_options);
}

std::unique_ptr<googlesql::Function> CastNumericToFloatFunction(
    absl::string_view catalog_name) {
  static const googlesql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(EvalCastNumericToFloat));
  return std::make_unique<googlesql::Function>(
      kPGCastNumericToFloatFunctionName, catalog_name,
      googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{googlesql::FunctionSignature{
          gsql_float, {gsql_pg_numeric}, /*context_ptr=*/nullptr}},
      function_options);
}

std::unique_ptr<googlesql::Function> CastToStringFunction(
    absl::string_view catalog_name) {
  static const googlesql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(EvalCastToString));

  return std::make_unique<googlesql::Function>(
      kPGCastToStringFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{
              gsql_string, {gsql_pg_numeric}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_string, {gsql_interval}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_string, {gsql_uuid}, /*context_ptr=*/nullptr},
      },
      function_options);
}

std::unique_ptr<googlesql::Function> CastNumericToInt64Function(
    absl::string_view catalog_name) {
  static const googlesql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(EvalCastNumericToInt64));
  return std::make_unique<googlesql::Function>(
      kPGCastNumericToInt64FunctionName, catalog_name,
      googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{googlesql::FunctionSignature{
          gsql_int64, {gsql_pg_numeric}, /*context_ptr=*/nullptr}},
      function_options);
}

// PG String functions
absl::StatusOr<googlesql::Value> EvalQuoteIdent(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 1);
  if (args[0].is_null()) {
    return googlesql::Value::NullString();
  }

  return googlesql::Value::String(
      absl::StrCat("\"", args[0].string_value(), "\""));
}

std::unique_ptr<googlesql::Function> QuoteIdentFunction(
    absl::string_view catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(googlesql::FunctionEvaluator(EvalQuoteIdent));
  return std::make_unique<googlesql::Function>(
      kPGQuoteIdentFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{googlesql::FunctionSignature{
          gsql_string, {gsql_string}, /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalRegexpMatch(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 2 || args.size() == 3);
  if (HasNullValue(args)) {
    return googlesql::Value::Null(googlesql::types::StringArrayType());
  }

  std::unique_ptr<std::vector<std::optional<std::string>>> result;
  if (args.size() == 2) {
    GOOGLESQL_ASSIGN_OR_RETURN(
        result, RegexpMatch(args[0].string_value(), args[1].string_value()));
  } else {
    GOOGLESQL_ASSIGN_OR_RETURN(result,
                     RegexpMatch(args[0].string_value(), args[1].string_value(),
                                 args[2].string_value()));
  }

  if (result == nullptr) {
    return googlesql::Value::Null(gsql_string_array);
  } else {
    std::vector<googlesql::Value> values;
    values.reserve(result->size());
    for (int i = 0; i < result->size(); ++i) {
      std::optional<std::string> element = (*result)[i];
      if (element.has_value()) {
        values.push_back(googlesql::Value::String(element.value()));
      } else {
        values.push_back(googlesql::Value::Null(gsql_string));
      }
    }
    return googlesql::Value::MakeArray(gsql_string_array, values);
  }
}

std::unique_ptr<googlesql::Function> RegexpMatchFunction(
    absl::string_view catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(
      EvalRegexpMatch, InitializePGTimezoneToDefault, CleanupRegexCache));
  return std::make_unique<googlesql::Function>(
      kPGRegexpMatchFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_string_array,
                                       {gsql_string, gsql_string},
                                       /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{gsql_string_array,
                                       {gsql_string, gsql_string, gsql_string},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalRegexpSplitToArray(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 2 || args.size() == 3);
  if (HasNullValue(args)) {
      return googlesql::Value::Null(googlesql::types::StringArrayType());
  }

  std::unique_ptr<std::vector<std::string>> result;
  if (args.size() == 2) {
    GOOGLESQL_ASSIGN_OR_RETURN(result, RegexpSplitToArray(args[0].string_value(),
                                                args[1].string_value()));
  } else {
    GOOGLESQL_ASSIGN_OR_RETURN(result, RegexpSplitToArray(args[0].string_value(),
                                                args[1].string_value(),
                                                args[2].string_value()));
  }

  if (result == nullptr) {
    return absl::InternalError("regex produced null matches");
  }

  std::vector<googlesql::Value> values;
  values.reserve(result->size());
  for (int i = 0; i < result->size(); ++i) {
    values.push_back(googlesql::Value::String((*result)[i]));
  }
  return googlesql::Value::MakeArray(gsql_string_array, values);
}

std::unique_ptr<googlesql::Function> RegexpSplitToArrayFunction(
    absl::string_view catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(
      PGFunctionEvaluator(EvalRegexpSplitToArray, InitializePGTimezoneToDefault,
                          CleanupRegexCache));
  return std::make_unique<googlesql::Function>(
      kPGRegexpSplitToArrayFunctionName, catalog_name,
      googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_string_array,
                                       {gsql_string, gsql_string},
                                       /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{gsql_string_array,
                                       {gsql_string, gsql_string, gsql_string},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalSubstring(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return googlesql::Value::NullString();
  }

  GOOGLESQL_ASSIGN_OR_RETURN(
      std::unique_ptr<std::string> result,
      Textregexsubstr(args[0].string_value(), args[1].string_value()));

  if (result == nullptr) {
    return googlesql::Value::Null(gsql_string);
  } else {
    return googlesql::Value::String(*result);
  }
}

std::unique_ptr<googlesql::Function> SubstringFunction(
    absl::string_view catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(
      EvalSubstring, InitializePGTimezoneToDefault, CleanupRegexCache));
  return std::make_unique<googlesql::Function>(
      kPGSubstringFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_string,
                                       {gsql_string, gsql_string},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalCastToDate(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 1);
  if (args[0].is_null()) {
    return googlesql::Value::NullDate();
  }

  GOOGLESQL_ASSIGN_OR_RETURN(int32_t date,
                   function_evaluators::PgDateIn(args[0].string_value()));
  return googlesql::Value::Date(date);
}

std::unique_ptr<googlesql::Function> CastToDateFunction(
    const std::string& catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(
      PGFunctionEvaluator(EvalCastToDate, InitializePGTimezoneToDefault,
                          CleanupPostgresDateTimeCache));

  return std::make_unique<googlesql::Function>(
      kPGCastToDateFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{googlesql::types::DateType(),
                                       {googlesql::types::StringType()},
                                       nullptr}},
      function_options);
}

std::unique_ptr<googlesql::Function> ArrayOverlapFunction(
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
      googlesql::ARG_KIND_EXPR_ARRAY_ANY_1,
      FunctionArgumentTypeOptions()
          .set_array_element_must_support_equality()
          .set_argument_name("array_to_search", googlesql::kPositionalOnly));

  FunctionArgumentType search_values_arg(
      googlesql::ARG_KIND_EXPR_ARRAY_ANY_1,
      FunctionArgumentTypeOptions().set_argument_name(
          "search_values", googlesql::kPositionalOnly));

  FunctionSignature signature{
      gsql_bool,
      {array_to_search_arg, search_values_arg},
      /*context_id=*/-1,
      FunctionSignatureOptions().set_rewrite_options(
          FunctionSignatureRewriteOptions()
              .set_enabled(true)
              .set_rewriter(googlesql::REWRITE_BUILTIN_FUNCTION_INLINER)
              .set_sql(kArrayOverlapSql))};

  return std::make_unique<googlesql::Function>(
      "pg.array_overlap", catalog_name, googlesql::Function::SCALAR,
      std::vector<FunctionSignature>{signature}, FunctionOptions());
}

std::unique_ptr<googlesql::Function> ArrayContainsOrContainedFunction(
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
      googlesql::ARG_KIND_EXPR_ARRAY_ANY_1,
      FunctionArgumentTypeOptions()
          .set_array_element_must_support_equality()
          .set_argument_name("array_to_search", googlesql::kPositionalOnly));

  FunctionArgumentType search_values(
      googlesql::ARG_KIND_EXPR_ARRAY_ANY_1,
      FunctionArgumentTypeOptions()
          .set_array_element_must_support_equality()
          .set_argument_name("search_values", googlesql::kPositionalOnly));

  googlesql::FunctionArgumentTypeList argument_type_list;
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
              .set_rewriter(googlesql::REWRITE_BUILTIN_FUNCTION_INLINER)
              .set_sql(kArrayContainsSql)
              .set_allowed_function_groups({kSpannerFunctionGroup}))};

  return std::make_unique<googlesql::Function>(
      is_array_contains ? "pg.array_contains" : "pg.array_contained",
      catalog_name, googlesql::Function::SCALAR,
      std::vector<FunctionSignature>{signature}, FunctionOptions());
}

std::unique_ptr<googlesql::Function> ArrayAllFunction(
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
      googlesql::ARG_KIND_EXPR_ARRAY_ANY_1,
      FunctionArgumentTypeOptions()
          .set_array_element_must_support_equality()
          .set_argument_name("array_to_search", googlesql::kPositionalOnly));

  FunctionArgumentType search_value(
      googlesql::ARG_KIND_EXPR_ANY_1,
      FunctionArgumentTypeOptions().set_argument_name(
          "search_value", googlesql::kPositionalOnly));

  FunctionSignature signature{
      gsql_bool,
      {search_value, array_to_search},
      /*context_id=*/-1,
      FunctionSignatureOptions().set_rewrite_options(
          FunctionSignatureRewriteOptions()
              .set_enabled(true)
              .set_rewriter(googlesql::REWRITE_BUILTIN_FUNCTION_INLINER)
              .set_sql(absl::StrFormat(kArrayAllTemplateSql, operator_str))
              .set_allowed_function_groups({kSpannerFunctionGroup}))};

  return std::make_unique<googlesql::Function>(
      function_name, catalog_name, googlesql::Function::SCALAR,
      std::vector<FunctionSignature>{signature}, FunctionOptions());
}

std::unique_ptr<googlesql::Function> ArraySliceFunction(
    const std::string& catalog_name) {
  // We add 1 to the offset (i.e., idx) because GoogleSQL returns zero-based
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
      googlesql::ARG_KIND_EXPR_ARRAY_ANY_1,
      FunctionArgumentTypeOptions()
          .set_array_element_must_support_equality()
          .set_argument_name("array_to_slice", googlesql::kPositionalOnly));

  FunctionArgumentType start_offset_arg(
      gsql_int64, FunctionArgumentTypeOptions().set_argument_name(
                      "start_offset", googlesql::kPositionalOnly));

  FunctionArgumentType end_offset_arg(
      gsql_int64, FunctionArgumentTypeOptions().set_argument_name(
                      "end_offset", googlesql::kPositionalOnly));

  FunctionSignature signature{
      googlesql::ARG_KIND_EXPR_ARRAY_ANY_1,
      {array_to_slice_arg, start_offset_arg, end_offset_arg},
      /*context_id=*/-1,
      FunctionSignatureOptions().set_rewrite_options(
          FunctionSignatureRewriteOptions()
              .set_enabled(true)
              .set_rewriter(googlesql::REWRITE_BUILTIN_FUNCTION_INLINER)
              .set_sql(kArraySliceSql)
              .set_allowed_function_groups({kSpannerFunctionGroup}))};

  return std::make_unique<googlesql::Function>(
      "pg.array_slice", catalog_name, googlesql::Function::SCALAR,
      std::vector<FunctionSignature>{signature}, FunctionOptions());
}

std::unique_ptr<googlesql::Function> CastToTimestampFunction(
    const std::string& catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(
      PGFunctionEvaluator(EvalCastToTimestamp, InitializePGTimezoneToDefault,
                          CleanupPostgresDateTimeCache));

  return std::make_unique<googlesql::Function>(
      kPGCastToTimestampFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{googlesql::types::TimestampType(),
                                       {googlesql::types::StringType()},
                                       nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalTimestamptzAdd(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 2);
  if (HasNullValue(args)) {
    return googlesql::Value::NullTimestamp();
  }
  auto unix_picos = args[0].ToUnixPicos();

  if (args[1].type_kind() == googlesql::TYPE_INTERVAL) {
    GOOGLESQL_ASSIGN_OR_RETURN(googlesql::IntervalValue interval_arg,
                     RoundPrecision(args[1].interval_value()));
    GOOGLESQL_ASSIGN_OR_RETURN(absl::Time time,
                     PgTimestamptzAdd(unix_picos.ToAbslTime(), interval_arg));
    return googlesql::Value::Timestamp(time);
  }

  GOOGLESQL_ASSIGN_OR_RETURN(absl::Time time,
                   PgTimestamptzAdd(unix_picos.ToAbslTime(),
                                    args[1].string_value()));
  return googlesql::Value::Timestamp(time);
}

std::unique_ptr<googlesql::Function> TimestamptzAddFunction(
    const std::string& catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(
      PGFunctionEvaluator(EvalTimestamptzAdd, InitializePGTimezoneToDefault,
                          CleanupPostgresDateTimeCache));
  return std::make_unique<googlesql::Function>(
      kPGTimestamptzAddFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{googlesql::types::TimestampType(),
                                       {googlesql::types::TimestampType(),
                                        googlesql::types::StringType()},
                                       nullptr},
          googlesql::FunctionSignature{googlesql::types::TimestampType(),
                                       {googlesql::types::TimestampType(),
                                        googlesql::types::IntervalType()},
                                       nullptr},
      },
      function_options);
}

absl::StatusOr<googlesql::Value> EvalTimestamptzSubtract(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 2);
  if (HasNullValue(args)) {
    return googlesql::Value::NullTimestamp();
  }
  auto unix_picos = args[0].ToUnixPicos();

  if (args[1].type_kind() == googlesql::TYPE_INTERVAL) {
    GOOGLESQL_ASSIGN_OR_RETURN(googlesql::IntervalValue interval_arg,
                     RoundPrecision(args[1].interval_value()));
    GOOGLESQL_ASSIGN_OR_RETURN(absl::Time time,
                     PgTimestamptzSubtract(unix_picos.ToAbslTime(),
                                           interval_arg));
    return googlesql::Value::Timestamp(time);
  }

  GOOGLESQL_ASSIGN_OR_RETURN(
      absl::Time time,
      PgTimestamptzSubtract(unix_picos.ToAbslTime(), args[1].string_value()));
  return googlesql::Value::Timestamp(time);
}

std::unique_ptr<googlesql::Function> TimestamptzSubtractFunction(
    const std::string& catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(
      EvalTimestamptzSubtract, InitializePGTimezoneToDefault,
      CleanupPostgresDateTimeCache));

  return std::make_unique<googlesql::Function>(
      kPGTimestamptzSubtractFunctionName, catalog_name,
      googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{googlesql::types::TimestampType(),
                                       {googlesql::types::TimestampType(),
                                        googlesql::types::StringType()},
                                       nullptr},
          googlesql::FunctionSignature{googlesql::types::TimestampType(),
                                       {googlesql::types::TimestampType(),
                                        googlesql::types::IntervalType()},
                                       nullptr},
      },
      function_options);
}

absl::StatusOr<googlesql::Value> EvalTimestamptzBin(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 3);
  if (HasNullValue(args)) {
    return googlesql::Value::NullTimestamp();
  }

  GOOGLESQL_ASSIGN_OR_RETURN(absl::Time time,
                   PgTimestamptzBin(args[0].string_value(),
                                    args[1].ToUnixPicos().ToAbslTime(),
                                    args[2].ToUnixPicos().ToAbslTime()));
  return googlesql::Value::Timestamp(time);
}

std::unique_ptr<googlesql::Function> TimestamptzBinFunction(
    const std::string& catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(
      PGFunctionEvaluator(EvalTimestamptzBin, InitializePGTimezoneToDefault,
                          CleanupPostgresDateTimeCache));

  return std::make_unique<googlesql::Function>(
      kPGTimestamptzBinFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{googlesql::FunctionSignature{
          googlesql::types::TimestampType(),
          {googlesql::types::StringType(), googlesql::types::TimestampType(),
           googlesql::types::TimestampType()},
          nullptr}},
      function_options);
}

std::unique_ptr<googlesql::Function> TimestamptzTruncFunction(
    const std::string& catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(
      PGFunctionEvaluator(EvalTimestamptzTrunc, InitializePGTimezoneToDefault,
                          CleanupPostgresDateTimeCache));

  return std::make_unique<googlesql::Function>(
      kPGTimestamptzTruncFunctionName, catalog_name,
      googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{googlesql::types::TimestampType(),
                                       {googlesql::types::StringType(),
                                        googlesql::types::TimestampType()},
                                       nullptr},
          googlesql::FunctionSignature{googlesql::types::TimestampType(),
                                       {googlesql::types::StringType(),
                                        googlesql::types::TimestampType(),
                                        googlesql::types::StringType()},
                                       nullptr}},
      function_options);
}

std::unique_ptr<googlesql::Function> ExtractFunction(
    const std::string& catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(
      PGFunctionEvaluator(EvalExtract, InitializePGTimezoneToDefault,
                          CleanupPostgresDateTimeCache));

  static const googlesql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();
  return std::make_unique<googlesql::Function>(
      kPGExtractFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_pg_numeric,
                                       {googlesql::types::StringType(),
                                        googlesql::types::TimestampType()},
                                       nullptr},
          googlesql::FunctionSignature{
              gsql_pg_numeric,
              {googlesql::types::StringType(), googlesql::types::DateType()},
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
absl::StatusOr<googlesql::Value> EvalMapFloatingPointToInt(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 1);
  if (args[0].is_null()) {
    return googlesql::Value::NullInt64();
  }

  double num = static_cast<double>(args[0].Get<T>());

  if (std::isnan(num)) {
    return googlesql::Value::Int64(std::numeric_limits<int64_t>::max());
  }

  // Encodes a double value as int64_t value using mostly isomorphic (values can
  // be converted back) and order preservable (if input x < input y then output
  // for x < output for y) transformations. The exception for isomorphism:
  // negative zero will be round-tripped to positive zero.
  const int64_t enc = absl::bit_cast<int64_t>(num);
  int64_t res = (enc < 0) ? std::numeric_limits<int64_t>::min() - enc : enc;

  return googlesql::Value::Int64(res);
}

std::unique_ptr<googlesql::Function> MapDoubleToIntFunction(
    const std::string& catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(
      googlesql::FunctionEvaluator(EvalMapFloatingPointToInt<double>));

  return std::make_unique<googlesql::Function>(
      kPGMapDoubleToIntFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{googlesql::types::Int64Type(),
                                       {googlesql::types::DoubleType()},
                                       nullptr}},
      function_options);
}

std::unique_ptr<googlesql::Function> MapFloatToIntFunction(
    const std::string& catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(
      googlesql::FunctionEvaluator(EvalMapFloatingPointToInt<float>));

  return std::make_unique<googlesql::Function>(
      kPGMapFloatToIntFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{googlesql::types::Int64Type(),
                                       {googlesql::types::FloatType()},
                                       nullptr}},
      function_options);
}

// PG Cast functions
absl::StatusOr<googlesql::Value> EvalToJsonbFromValue(googlesql::Value arg);

absl::StatusOr<googlesql::Value> EvalToJsonb(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 1);
  return EvalToJsonbFromValue(args[0]);
}

// Converts a `Value` to its unquoted string representation. `null` value is
// printed as a `null_string`. The return string from certain Value types may be
// normalized later when converted to PG.JSONB by calling
// `EvalToJsonbFrom<Type>`. Otherwise, this function does not guarantee a
// normalized return.
absl::StatusOr<std::string> GetStringRepresentation(
    const googlesql::Value& value, std::string null_string = "null") {
  if (value.is_null()) {
    return null_string;
  }
  switch (value.type_kind()) {
    case googlesql::TYPE_INT64:
      return absl::StrCat(value.int64_value());
    case googlesql::TYPE_BOOL:
      return value.bool_value() ? kTrue : kFalse;
    case googlesql::TYPE_DOUBLE:
      return googlesql::RoundTripDoubleToString(value.double_value());
    case googlesql::TYPE_FLOAT:
      return googlesql::RoundTripFloatToString(value.float_value());
    case googlesql::TYPE_STRING:
      return value.string_value();
    case googlesql::TYPE_BYTES:
      return absl::StrCat("\\x", absl::BytesToHexString(value.bytes_value()));
    case googlesql::TYPE_DATE: {
      std::string date_string;
      // `googlesql::values::Date` is always a valid date (`null` check is done
      // above); hence, the following call to `ConvertDateToString` will never
      // return an invalid date error.
      GOOGLESQL_RETURN_IF_ERROR(googlesql::functions::ConvertDateToString(
          value.date_value(), &date_string));
      return date_string;
    }
    case googlesql::TYPE_TIMESTAMP: {
      std::string timestamp_string;
      // `googlesql::values::Timestamp` is always a valid timestamp (`null`
      // check is done above); hence, the following call to
      // `FormatTimestampToString` will never return an invalid timestamp error.
      GOOGLESQL_RETURN_IF_ERROR(googlesql::functions::FormatTimestampToString(
          absl::RFC3339_full,
          absl::ToUnixMicros(value.ToUnixPicos().ToAbslTime()),
          absl::UTCTimeZone(), {}, &timestamp_string));
      return timestamp_string;
    }
    case googlesql::TYPE_INTERVAL: {
      std::string interval_string;
      GOOGLESQL_ASSIGN_OR_RETURN(googlesql::IntervalValue interval,
                       RoundPrecision(value.interval_value()));
      GOOGLESQL_ASSIGN_OR_RETURN(interval_string, PgIntervalOut(interval));
      return interval_string;
    }
    case googlesql::TYPE_UUID: {
      GOOGLESQL_ASSIGN_OR_RETURN(googlesql::UuidValue uuid, value.uuid_value());
      return absl::StrCat("\"", uuid.ToString(), "\"");
    }
    case googlesql::TYPE_ARRAY: {
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
    case googlesql::TYPE_EXTENDED: {
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
          GOOGLESQL_RET_CHECK_FAIL() << "Encountered unexpected type "
                           << value.type_kind();
      }
    }
    default:
      GOOGLESQL_RET_CHECK_FAIL() << "Encountered unexpected type " << value.type_kind();
  }
}

// Returns a normalized PG.JSONB value from the int64_t input.
absl::StatusOr<googlesql::Value> EvalToJsonbFromInt64(
    const googlesql::Value arg) {
  return CreatePgJsonbValue(GetStringRepresentation(arg).value());
}

// Returns a normalized PG.JSONB value from the bool input.
absl::StatusOr<googlesql::Value> EvalToJsonbFromBool(
    const googlesql::Value arg) {
  return CreatePgJsonbValue(GetStringRepresentation(arg).value());
}

// Returns a normalized PG.JSONB value from the UUID input.
absl::StatusOr<googlesql::Value> EvalToJsonbFromUuid(
    const googlesql::Value arg) {
  return CreatePgJsonbValue(GetStringRepresentation(arg).value());
}

// Returns a normalized PG.JSONB value from the floating point input.
template <typename T, typename = std::enable_if_t<std::is_floating_point_v<T>>>
absl::StatusOr<googlesql::Value> EvalToJsonbFromFloatingPoint(
    const googlesql::Value arg) {
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
absl::StatusOr<googlesql::Value> EvalToJsonbFromString(
    const googlesql::Value arg) {
  if (IsValidJsonbString(arg.string_value())) {
    return CreatePgJsonbValue(
        SerializeJsonbString(GetStringRepresentation(arg).value()));
  }
  return absl::InvalidArgumentError(
      "unsupported Unicode escape sequence DETAIL: \\u0000 cannot "
      "be converted to text.");
}

// Returns a normalized PG.JSONB value from the bytes input.
absl::StatusOr<googlesql::Value> EvalToJsonbFromBytes(
    const googlesql::Value arg) {
  return EvalToJsonbFromString(
      googlesql::values::String(GetStringRepresentation(arg).value()));
}

// Returns a normalized PG.JSONB value from the date input.
absl::StatusOr<googlesql::Value> EvalToJsonbFromDate(
    const googlesql::Value arg) {
  return CreatePgJsonbValue(
      absl::StrCat("\"", GetStringRepresentation(arg).value(), "\""));
}

// Returns a normalized PG.JSONB value from the timestamp input.
absl::StatusOr<googlesql::Value> EvalToJsonbFromTimestamp(
    const googlesql::Value arg) {
  return CreatePgJsonbValue(
      absl::StrCat("\"", GetStringRepresentation(arg).value(), "\""));
}

// Returns a normalized PG.JSONB value from the array input.
absl::StatusOr<googlesql::Value> EvalToJsonbFromArray(
    const googlesql::Value arg) {
  return CreatePgJsonbValue(GetStringRepresentation(arg).value());
}

// Returns a normalized PG.JSONB value from the PG.JSONB input.
absl::StatusOr<googlesql::Value> EvalToJsonbFromPgJsonb(
    const googlesql::Value arg) {
  return arg;
}

// Returns a normalized PG.JSONB value from the PG.NUMERIC input.
absl::StatusOr<googlesql::Value> EvalToJsonbFromPgNumeric(
    const googlesql::Value arg) {
  if (std::string(GetPgNumericNormalizedValue(arg).value()) == kNan) {
    return CreatePgJsonbValueFromNormalized(absl::Cord(kNanString));
  }

  return CreatePgJsonbValue(GetStringRepresentation(arg).value());
}

// Returns a normalized PG.JSONB value from the PG.OID input.
absl::StatusOr<googlesql::Value> EvalToJsonbFromPgOid(
    const googlesql::Value arg) {
  return CreatePgJsonbValue(GetStringRepresentation(arg).value());
}

// Returns a normalized PG.JSONB value from the extended type input.
absl::StatusOr<googlesql::Value> EvalToJsonbFromExtended(
    const googlesql::Value arg) {
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
      GOOGLESQL_RET_CHECK_FAIL() << "Encountered unexpected type " << arg.type_kind();
  }
}

// Returns a normalized PG.JSONB value from the int64_t input.
absl::StatusOr<googlesql::Value> EvalToJsonbFromInterval(
    const googlesql::Value arg) {
  return CreatePgJsonbValue(
      absl::StrCat("\"", GetStringRepresentation(arg).value(), "\""));
}

// Returns a normalized PG.JSONB value from the input.
absl::StatusOr<googlesql::Value> EvalToJsonbFromValue(
    const googlesql::Value arg) {
  if (arg.is_null()) {
    // `null` input results in `null` JSONB value.
    return googlesql::values::Null(spangres::datatypes::GetPgJsonbType());
  }

  googlesql::TypeKind type_kind = arg.type_kind();
  switch (type_kind) {
    case googlesql::TYPE_INT64:
      return EvalToJsonbFromInt64(arg);
    case googlesql::TYPE_BOOL:
      return EvalToJsonbFromBool(arg);
    case googlesql::TYPE_DOUBLE:
      return EvalToJsonbFromFloatingPoint<double>(arg);
    case googlesql::TYPE_FLOAT:
      return EvalToJsonbFromFloatingPoint<float>(arg);
    case googlesql::TYPE_STRING:
      return EvalToJsonbFromString(arg);
    case googlesql::TYPE_BYTES:
      return EvalToJsonbFromBytes(arg);
    case googlesql::TYPE_DATE:
      return EvalToJsonbFromDate(arg);
    case googlesql::TYPE_TIMESTAMP:
      return EvalToJsonbFromTimestamp(arg);
    case googlesql::TYPE_ARRAY:
      return EvalToJsonbFromArray(arg);
    case googlesql::TYPE_EXTENDED:
      return EvalToJsonbFromExtended(arg);
    case googlesql::TYPE_INTERVAL:
      return EvalToJsonbFromInterval(arg);
    case googlesql::TYPE_UUID:
      return EvalToJsonbFromUuid(arg);
    default:
      GOOGLESQL_RET_CHECK_FAIL() << "Encountered unexpected type " << type_kind;
  }
}

std::unique_ptr<googlesql::Function> ToJsonbFunction(
    absl::string_view catalog_name) {
  static const googlesql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();
  static const googlesql::Type* gsql_pg_jsonb =
      spangres::datatypes::GetPgJsonbType();
  static const googlesql::Type* gsql_pg_numeric_array =
      spangres::datatypes::GetPgNumericArrayType();
  static const googlesql::Type* gsql_pg_jsonb_array =
      spangres::datatypes::GetPgJsonbArrayType();
  static const googlesql::Type* gsql_pg_oid =
      spangres::datatypes::GetPgOidType();
  static const googlesql::Type* gsql_pg_oid_array =
      spangres::datatypes::GetPgOidArrayType();

  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(EvalToJsonb));
  return std::make_unique<googlesql::Function>(
      kPGToJsonbFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{
              gsql_pg_jsonb, {gsql_bool}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_jsonb, {gsql_bool_array}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_jsonb, {gsql_bytes}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_jsonb, {gsql_bytes_array}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_jsonb, {gsql_date}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_jsonb, {gsql_date_array}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_jsonb, {gsql_double}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_jsonb, {gsql_double_array}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_jsonb, {gsql_float}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_jsonb, {gsql_float_array}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_jsonb, {gsql_int64}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_jsonb, {gsql_int64_array}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_jsonb, {gsql_pg_jsonb}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_jsonb, {gsql_pg_jsonb_array}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_jsonb, {gsql_pg_numeric}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_jsonb, {gsql_pg_numeric_array}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_jsonb, {gsql_string}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_jsonb, {gsql_string_array}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_jsonb, {gsql_timestamp}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_jsonb, {gsql_timestamp_array}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_jsonb, {gsql_pg_oid}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_jsonb, {gsql_pg_oid_array}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_jsonb, {gsql_interval}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_jsonb, {gsql_interval_array}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_jsonb, {gsql_uuid}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_jsonb, {gsql_uuid_array}, /*context_ptr=*/nullptr},
      },
      function_options);
}

// Returns a normalized PG.JSONB value from the input.
template <googlesql::TypeKind T>
absl::StatusOr<googlesql::Value> EvalCastFromJsonb(
    absl::Span<const googlesql::Value> args) {
  switch (T) {
    case googlesql::TYPE_INT64:
      return spangres::datatypes::PgJsonbToInt64Conversion(args);
    case googlesql::TYPE_BOOL:
      return spangres::datatypes::PgJsonbToBoolConversion(args);
    case googlesql::TYPE_DOUBLE:
      return spangres::datatypes::PgJsonbToDoubleConversion(args);
    case googlesql::TYPE_FLOAT:
      return spangres::datatypes::PgJsonbToFloatConversion(args);
    case googlesql::TYPE_STRING:
      return spangres::datatypes::PgJsonbToStringConversion(args);
    default:
      return absl::InvalidArgumentError(
          absl::StrCat("cannot cast jsonb object to type ", T));
  }
}

std::unique_ptr<googlesql::Function> CastFromJsonbFunction(
    absl::string_view catalog_name) {
  static const googlesql::Type* gsql_pg_jsonb =
      spangres::datatypes::GetPgJsonbType();

  googlesql::FunctionEvaluatorFactory evaluator_factory(
      [&](const googlesql::FunctionSignature& signature)
          -> absl::StatusOr<googlesql::FunctionEvaluator> {
        if (signature.result_type().type()->IsInt64()) {
          return EvalCastFromJsonb<googlesql::TYPE_INT64>;
        } else if (signature.result_type().type()->IsBool()) {
          return EvalCastFromJsonb<googlesql::TYPE_BOOL>;
        } else if (signature.result_type().type()->IsDouble()) {
          return EvalCastFromJsonb<googlesql::TYPE_DOUBLE>;
        } else if (signature.result_type().type()->IsString()) {
          return EvalCastFromJsonb<googlesql::TYPE_STRING>;
        } else {
          return absl::InvalidArgumentError(
              absl::StrCat("cannot cast jsonb object to type ",
                           signature.result_type().type()->DebugString()));
        }
      });
  googlesql::FunctionOptions function_options;
  function_options.set_evaluator_factory(evaluator_factory);
  return std::make_unique<googlesql::Function>(
      kPGCastFromJsonbFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{
              gsql_bool, {gsql_pg_jsonb}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_double, {gsql_pg_jsonb}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_int64, {gsql_pg_jsonb}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_string, {gsql_pg_jsonb}, /*context_ptr=*/nullptr},
      },
      function_options);
}

absl::StatusOr<googlesql::Value> EvalJsonbSubscriptText(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 2);
  if (args[1].type_kind() != googlesql::TYPE_INT64 &&
      args[1].type_kind() != googlesql::TYPE_STRING) {
    return absl::UnimplementedError(absl::StrCat(
        "jsonb_subscript_text(jsonb, ", args[1].type()->DebugString(), ")"));
  }
  if (args[0].is_null() || args[1].is_null()) {
    return googlesql::Value::NullString();
  }
  const std::string jsonb(GetStringRepresentation(args[0]).value());
  if (args[1].type_kind() == googlesql::TYPE_INT64) {
    const int32_t element = static_cast<int32_t>(args[1].int64_value());
    return EmulatorJsonbArrayElementText(jsonb, element);
  } else {
    const std::string key(args[1].string_value());
    return EmulatorJsonbObjectFieldText(jsonb, key);
  }
}

std::unique_ptr<googlesql::Function> JsonbSubscriptTextFunction(
    absl::string_view catalog_name) {
  const googlesql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbSubscriptText));
  return std::make_unique<googlesql::Function>(
      kPGJsonbSubscriptTextFunctionName, catalog_name,
      googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_string,
                                       {gsql_pg_jsonb, gsql_int64},
                                       /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{gsql_string,
                                       {gsql_pg_jsonb, gsql_string},
                                       /*context_ptr=*/nullptr},
      },
      function_options);
}

absl::StatusOr<googlesql::Value> EvalSubscript(
  absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 2);
  if ((args[1].type_kind() != googlesql::TYPE_INT64) &&
      (args[1].type_kind() != googlesql::TYPE_STRING)) {
    return absl::UnimplementedError(absl::StrCat(
        "$subscript(PG.JSONB, ", args[1].type()->DebugString(), ")"));
  }
  if (args[0].is_null() || args[1].is_null()) {
    return googlesql::Value::Null(
        postgres_translator::spangres::datatypes::GetPgJsonbType());
  }

  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord jsonb, GetPgJsonbNormalizedValue(args[0]));
  if (args[1].type_kind() == googlesql::TYPE_INT64) {
    return JsonbArrayElement(std::string(jsonb), args[1].int64_value());
  } else {
  return JsonbObjectField(std::string(jsonb), args[1].string_value());
  }
}

std::unique_ptr<googlesql::Function> GoogleSQLSubscriptFunction(
    absl::string_view catalog_name) {
  const googlesql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalSubscript));
  return std::make_unique<googlesql::Function>(
      kGoogleSQLSubscriptFunctionName, catalog_name,
      googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_pg_jsonb,
                                       {gsql_pg_jsonb, gsql_int64},
                                       /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{gsql_pg_jsonb,
                                       {gsql_pg_jsonb, gsql_string},
                                       /*context_ptr=*/nullptr},
      },
      function_options);
}

absl::StatusOr<googlesql::Value> EvalJsonbTypeof(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 1);
  if (args[0].is_null()) {
    return googlesql::Value::NullString();
  }

  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord jsonb, GetPgJsonbNormalizedValue(args[0]));
  return JsonbTypeof(std::string(jsonb));
}

std::unique_ptr<googlesql::Function> JsonbTypeofFunction(
    absl::string_view catalog_name) {
  const googlesql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbTypeof));
  return std::make_unique<googlesql::Function>(
      kGoogleSQLJsonTypeFunctionName, catalog_name,
      googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_string,
                                       {gsql_pg_jsonb},
                                       /*context_ptr=*/nullptr},
      },
      function_options);
}

absl::StatusOr<googlesql::Value> EvalJsonbQueryArray(
    absl::Span<const googlesql::Value> args) {
  static const googlesql::ArrayType* gsql_pg_jsonb_array =
      spangres::datatypes::GetPgJsonbArrayType();
  GOOGLESQL_RET_CHECK(args.size() == 1);
  if (args[0].is_null()) {
    return googlesql::values::Null(spangres::datatypes::GetPgJsonbArrayType());
  }

  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord jsonb, GetPgJsonbNormalizedValue(args[0]));
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  GOOGLESQL_ASSIGN_OR_RETURN(PgJsonbValue jsonb_value,
                   PgJsonbValue::Parse(std::string(jsonb), &tree_nodes));
  GOOGLESQL_ASSIGN_OR_RETURN(std::vector<absl::Cord> array,
                   jsonb_value.GetSerializedArrayElements());
  std::vector<googlesql::Value> values;
  values.reserve(array.size());
  std::for_each(array.begin(), array.end(), [&values](absl::Cord json_element) {
    values.emplace_back(CreatePgJsonbValueFromNormalized(json_element));
  });
  return googlesql::Value::MakeArray(gsql_pg_jsonb_array, values);
}

// Maps to both `jsonb_query_array` and `jsonb_array_elements`.
std::unique_ptr<googlesql::Function> JsonbQueryArrayFunction(
    absl::string_view catalog_name, const char* function_name) {
  const googlesql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  const googlesql::Type* gsql_pg_jsonb_array =
      postgres_translator::spangres::datatypes::GetPgJsonbArrayType();

  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbQueryArray));
  return std::make_unique<googlesql::Function>(
      function_name, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_pg_jsonb_array,
                                       {gsql_pg_jsonb},
                                       /*context_ptr=*/nullptr},
      },
      function_options);
}

absl::StatusOr<googlesql::Value> EvalJsonbBuildArray(
    absl::Span<const googlesql::Value> args) {
  absl::Cord jsonb_value("[");
  for (int i = 0; i < args.size(); ++i) {
    if (i > 0) {
      jsonb_value.Append(", ");
    }
    if (args[i].is_null()) {
      jsonb_value.Append("null");
    } else {
      GOOGLESQL_ASSIGN_OR_RETURN(googlesql::Value value_as_jsonb,
                       EvalToJsonbFromValue(args[i]));
      GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord value,
                       GetPgJsonbNormalizedValue(value_as_jsonb));
      jsonb_value.Append(value);
    }
  }
  jsonb_value.Append("]");
  return CreatePgJsonbValueFromNormalized(jsonb_value);
}

std::unique_ptr<googlesql::Function> JsonbBuildArrayFunction(
    absl::string_view catalog_name) {
  const googlesql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbBuildArray));
  return std::make_unique<googlesql::Function>(
      kPGJsonbBuildArrayFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature(gsql_pg_jsonb, {{}},
                                       /*context_ptr=*/nullptr),
          googlesql::FunctionSignature(
              gsql_pg_jsonb,
              {{googlesql::ARG_KIND_EXPR_ARBITRARY,
                googlesql::FunctionArgumentTypeOptions().set_cardinality(
                    googlesql::FunctionArgumentType::REPEATED)}},
              /*context_ptr=*/nullptr)},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalJsonbBuildObject(
    absl::Span<const googlesql::Value> args) {
  absl::Cord jsonb_value("{");
  for (int i = 0; i < args.size(); i += 2) {
    if (i > 0) {
      jsonb_value.Append(", ");
    }
    if (args[i].is_null()) {
      return absl::InvalidArgumentError("JSONB key must not be null");
    }
    GOOGLESQL_ASSIGN_OR_RETURN(googlesql::Value key_as_jsonb,
                     EvalToJsonbFromValue(args[i]));
    GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord key, GetPgJsonbNormalizedValue(key_as_jsonb));
    jsonb_value.Append(key);
    jsonb_value.Append(": ");

    if (args[i + 1].is_null()) {
      jsonb_value.Append("null");
    } else {
      GOOGLESQL_ASSIGN_OR_RETURN(googlesql::Value value_as_jsonb,
                       EvalToJsonbFromValue(args[i + 1]));
      GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord value,
                       GetPgJsonbNormalizedValue(value_as_jsonb));
      jsonb_value.Append(value);
    }
  }
  jsonb_value.Append("}");
  return CreatePgJsonbValueFromNormalized(jsonb_value);
}

std::unique_ptr<googlesql::Function> JsonbBuildObjectFunction(
    absl::string_view catalog_name) {
  const googlesql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbBuildObject));
  return std::make_unique<googlesql::Function>(
      kPGJsonbBuildObjectFunctionName, catalog_name,
      googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature(gsql_pg_jsonb, {{}},
                                       /*context_ptr=*/nullptr),
          googlesql::FunctionSignature(
              gsql_pg_jsonb,
              {{gsql_string,
                googlesql::FunctionArgumentTypeOptions().set_cardinality(
                    googlesql::FunctionArgumentType::REPEATED)},
               {googlesql::ARG_KIND_EXPR_ARBITRARY,
                googlesql::FunctionArgumentTypeOptions().set_cardinality(
                    googlesql::FunctionArgumentType::REPEATED)}},
              /*context_ptr=*/nullptr)},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalJsonbDelete(
    absl::Span<const googlesql::Value> args) {
  if (HasNullValue(args)) {
    return googlesql::Value::Null(
        postgres_translator::spangres::datatypes::GetPgJsonbType());
  }
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  GOOGLESQL_ASSIGN_OR_RETURN(std::string jsonb, GetStringRepresentation(args[0]));
  GOOGLESQL_ASSIGN_OR_RETURN(PgJsonbValue jsonb_value,
                   PgJsonbValue::Parse(jsonb, &tree_nodes));

  if (!jsonb_value.IsObject() && !jsonb_value.IsArray()) {
    return absl::InvalidArgumentError("cannot delete from scalar");
  }

  if (jsonb_value.IsObject()) {
    if (args[1].type_kind() == googlesql::TYPE_STRING) {
      std::string key = args[1].string_value();
      jsonb_value.RemoveMember(key);
    } else if (args[1].type_kind() == googlesql::TYPE_INT64) {
      return absl::InvalidArgumentError(
          "cannot delete from object using integer index");
    } else if (args[1].type_kind() == googlesql::TYPE_ARRAY) {
      return absl::InvalidArgumentError(
          "Deleting from array not currently supported");
    }
  } else if (jsonb_value.IsArray()) {
    if (args[1].type_kind() == googlesql::TYPE_STRING) {
      std::string del_string = args[1].string_value();
      for (int i = 0; i < jsonb_value.GetArraySize(); ++i) {
        if (jsonb_value.GetArrayElementIfExists(i)->IsString()) {
          absl::string_view element_string =
              jsonb_value.GetArrayElementIfExists(i)->GetString();
          if (element_string == del_string) {
            jsonb_value.RemoveArrayElement(i);
            --i;
          }
        }
      }
    } else if (args[1].type_kind() == googlesql::TYPE_INT64) {
      int64_t index = args[1].int64_value();
      jsonb_value.RemoveArrayElement(index);
    } else if (args[1].type_kind() == googlesql::TYPE_ARRAY) {
      return absl::UnimplementedError(
          "jsonb_delete(jsonb, array) is currently not supported");
    }
  }
  return CreatePgJsonbValueFromNormalized(jsonb_value.Serialize());
}

std::unique_ptr<googlesql::Function> JsonbDeleteFunction(
    absl::string_view catalog_name) {
  const googlesql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbDelete));
  return std::make_unique<googlesql::Function>(
      kPGJsonbDeleteFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature(gsql_pg_jsonb,
                                       {{gsql_pg_jsonb, gsql_string}},
                                       /*context_ptr=*/nullptr),
          googlesql::FunctionSignature(gsql_pg_jsonb,
                                       {{gsql_pg_jsonb, gsql_int64}},
                                       /*context_ptr=*/nullptr),
          googlesql::FunctionSignature(gsql_pg_jsonb,
                                       {{gsql_pg_jsonb, gsql_string_array}},
                                       /*context_ptr=*/nullptr),
      },
      function_options);
}

absl::StatusOr<std::optional<PgJsonbValue>> GetRootJsonbHelper(
    PgJsonbValue jsonb_value, const googlesql::Value& path_value,
    std::vector<std::string>& path_vector) {
  if (path_value.is_null()) {
    return std::nullopt;
  }
  ABSL_CHECK(path_value.type_kind() == googlesql::TYPE_ARRAY);
  for (int i = 0; i < path_value.num_elements(); ++i) {
    if (path_value.element(i).is_null()) {
      return absl::InvalidArgumentError(
          absl::Substitute("path element at position $0 is null", i + 1));
    }
    const googlesql::Value& path_element = path_value.element(i);
    std::string path_element_string = path_element.string_value();
    path_vector.push_back(path_element_string);
  }

  // We need to find the parent of the final element of the path in order to
  // do operations such as delete or set. Thus we construct a std::span as it
  // provides a subspan method and then convert to an absl::Span.
  return jsonb_value.FindAtPath(
      {absl::MakeSpan(path_vector).subspan(0, path_vector.size() - 1)});
}

absl::StatusOr<googlesql::Value> EvalJsonbDeletePath(
    absl::Span<const googlesql::Value> args) {
  if (HasNullValue(args)) {
    return googlesql::Value::Null(
        postgres_translator::spangres::datatypes::GetPgJsonbType());
  }
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  GOOGLESQL_ASSIGN_OR_RETURN(std::string jsonb, GetStringRepresentation(args[0]));
  GOOGLESQL_ASSIGN_OR_RETURN(PgJsonbValue jsonb_value,
                   PgJsonbValue::Parse(jsonb, &tree_nodes));

  if (!jsonb_value.IsObject() && !jsonb_value.IsArray()) {
    return absl::InvalidArgumentError("cannot delete path in scalar");
  }

  if (jsonb_value.IsEmpty()) {
    return CreatePgJsonbValueFromNormalized(jsonb_value.Serialize());
  }

  std::vector<std::string> path_vector;
  GOOGLESQL_ASSIGN_OR_RETURN(std::optional<PgJsonbValue> root_jsonb_optional,
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

std::unique_ptr<googlesql::Function> JsonbDeletePathFunction(
    absl::string_view catalog_name) {
  const googlesql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbDeletePath));
  return std::make_unique<googlesql::Function>(
      kPGJsonbDeletePathFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_pg_jsonb,
                                       {gsql_pg_jsonb, gsql_string_array},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalJsonbSet(
    absl::Span<const googlesql::Value> args) {
  // In the case we pass in 5 arguments, this means we called this function from
  // the jsonb_set_lax function and wish to treat the new value as a JSONB null.
  ABSL_CHECK(args.size() == 4 || args.size() == 5);
  if (HasNullValue(args) && args.size() == 4) {
    return googlesql::Value::Null(
        postgres_translator::spangres::datatypes::GetPgJsonbType());
  }
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  GOOGLESQL_ASSIGN_OR_RETURN(std::string jsonb, GetStringRepresentation(args[0]));
  GOOGLESQL_ASSIGN_OR_RETURN(PgJsonbValue jsonb_value,
                   PgJsonbValue::Parse(jsonb, &tree_nodes));
  if (!jsonb_value.IsObject() && !jsonb_value.IsArray()) {
    return absl::InvalidArgumentError("cannot set path in scalar");
  }
  std::vector<std::string> path_vector;
  GOOGLESQL_ASSIGN_OR_RETURN(std::optional<PgJsonbValue> root_jsonb_optional,
                   GetRootJsonbHelper(jsonb_value, args[1], path_vector));
  if (!root_jsonb_optional.has_value()) {
    return CreatePgJsonbValueFromNormalized(jsonb_value.Serialize());
  }
  PgJsonbValue root_jsonb = std::move(root_jsonb_optional).value();
  GOOGLESQL_ASSIGN_OR_RETURN(std::string new_value_string,
                   GetStringRepresentation(args[2]));
  GOOGLESQL_ASSIGN_OR_RETURN(PgJsonbValue new_value,
                   PgJsonbValue::Parse(new_value_string, &tree_nodes));
  bool create_if_missing = args[3].bool_value();
  if (root_jsonb.IsObject()) {
    if (root_jsonb.HasMember(path_vector.back())) {
      root_jsonb.GetMemberIfExists(path_vector.back())->SetValue(new_value);
    } else if (create_if_missing) {
      GOOGLESQL_RETURN_IF_ERROR(root_jsonb.CreateMemberIfNotExists(path_vector.back()));
      root_jsonb.GetMemberIfExists(path_vector.back())->SetValue(new_value);
    }
  } else if (root_jsonb.IsArray()) {
    GOOGLESQL_ASSIGN_OR_RETURN(
        int32_t index,
        root_jsonb.PathElementToIndex(path_vector.back(), path_vector.size()));
    if (root_jsonb.GetArrayElementIfExists(index).has_value()) {
      root_jsonb.GetArrayElementIfExists(index)->SetValue(new_value);
    } else if (create_if_missing) {
      GOOGLESQL_RETURN_IF_ERROR(root_jsonb.InsertArrayElement(new_value, index));
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

std::unique_ptr<googlesql::Function> JsonbSetFunction(
    absl::string_view catalog_name) {
  const googlesql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbSet));

  googlesql::FunctionArgumentTypeOptions jsonb_in =
      GetRequiredPositionalArgumentOptions("jsonb_in");
  googlesql::FunctionArgumentTypeOptions path =
      GetRequiredPositionalArgumentOptions("path");
  googlesql::FunctionArgumentTypeOptions replacement =
      GetRequiredPositionalArgumentOptions("replacement");
  googlesql::FunctionArgumentTypeOptions create_if_missing =
      GetOptionalPositionalArgumentOptions("create_if_missing");

  return std::make_unique<googlesql::Function>(
      kPGJsonbSetFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_pg_jsonb,
                                       {
                                           {gsql_pg_jsonb, jsonb_in},
                                           {gsql_string_array, path},
                                           {gsql_pg_jsonb, replacement},
                                           {gsql_bool, create_if_missing},
                                       },
                                       /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalJsonbSetLax(
    absl::Span<const googlesql::Value> args) {
  if (args[0].is_null() || args[1].is_null() || args[3].is_null()) {
    return googlesql::Value::Null(
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

std::unique_ptr<googlesql::Function> JsonbSetLaxFunction(
    absl::string_view catalog_name) {
  const googlesql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbSetLax));

  googlesql::FunctionArgumentTypeOptions jsonb_in =
      GetRequiredPositionalArgumentOptions("jsonb_in");
  googlesql::FunctionArgumentTypeOptions path =
      GetRequiredPositionalArgumentOptions("path");
  googlesql::FunctionArgumentTypeOptions replacement =
      GetRequiredPositionalArgumentOptions("replacement");
  googlesql::FunctionArgumentTypeOptions create_if_missing =
      GetOptionalPositionalArgumentOptions("create_if_missing");
  googlesql::FunctionArgumentTypeOptions null_value_treatment =
      GetOptionalPositionalArgumentOptions("null_value_treatment");

  return std::make_unique<googlesql::Function>(
      kPGJsonbSetLaxFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_pg_jsonb,
                                       {
                                           {gsql_pg_jsonb, jsonb_in},
                                           {gsql_string_array, path},
                                           {gsql_pg_jsonb, replacement},
                                           {gsql_bool, create_if_missing},
                                           {gsql_string, null_value_treatment},
                                       },
                                       /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalJsonbConcat(
    absl::Span<const googlesql::Value> args) {
  if (HasNullValue(args)) {
    return googlesql::Value::Null(
        postgres_translator::spangres::datatypes::GetPgJsonbType());
  }
  GOOGLESQL_ASSIGN_OR_RETURN(std::string jsonb_1, GetStringRepresentation(args[0]));
  GOOGLESQL_ASSIGN_OR_RETURN(std::string jsonb_2, GetStringRepresentation(args[1]));
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  GOOGLESQL_ASSIGN_OR_RETURN(PgJsonbValue left_jsonb,
                   PgJsonbValue::Parse(jsonb_1, &tree_nodes));
  GOOGLESQL_ASSIGN_OR_RETURN(PgJsonbValue right_jsonb,
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
        GOOGLESQL_RETURN_IF_ERROR(result.CreateMemberIfNotExists(key));
        result.GetMemberIfExists(key)->SetValue(value);
      }
    }
    return CreatePgJsonbValueFromNormalized(result.Serialize());
  }
  if (left_jsonb.IsArray()) {
    for (int i = 0; i < left_jsonb.GetArraySize(); ++i) {
      GOOGLESQL_RETURN_IF_ERROR(result.InsertArrayElement(
          left_jsonb.GetArrayElementIfExists(i).value(), i));
    }
  } else {
    GOOGLESQL_RETURN_IF_ERROR(result.InsertArrayElement(left_jsonb, 0));
  }
  if (right_jsonb.IsArray()) {
    for (int i = 0; i < right_jsonb.GetArraySize(); ++i) {
      GOOGLESQL_RETURN_IF_ERROR(result.InsertArrayElement(
          right_jsonb.GetArrayElementIfExists(i).value(), i));
    }
  } else {
    GOOGLESQL_RETURN_IF_ERROR(
        result.InsertArrayElement(right_jsonb, result.GetArraySize()));
  }
  return CreatePgJsonbValueFromNormalized(result.Serialize());
}

std::unique_ptr<googlesql::Function> JsonbConcatFunction(
    absl::string_view catalog_name) {
  const googlesql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  googlesql::FunctionOptions function_options;
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbConcat));
  function_options.set_supports_safe_error_mode(false);
  return std::make_unique<googlesql::Function>(
      kPGJsonbConcatFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_pg_jsonb,
                                       {gsql_pg_jsonb, gsql_pg_jsonb},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalJsonbInsert(
    absl::Span<const googlesql::Value> args) {
  if (HasNullValue(args)) {
    return googlesql::Value::Null(
        postgres_translator::spangres::datatypes::GetPgJsonbType());
  }
  GOOGLESQL_ASSIGN_OR_RETURN(std::string jsonb_string, GetStringRepresentation(args[0]));
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  GOOGLESQL_ASSIGN_OR_RETURN(PgJsonbValue jsonb_value,
                   PgJsonbValue::Parse(jsonb_string, &tree_nodes));
  if (!jsonb_value.IsObject() && !jsonb_value.IsArray()) {
    // matches pg error message
    return absl::InvalidArgumentError("cannot set path in scalar");
  }
  std::vector<std::string> path_vector;
  GOOGLESQL_ASSIGN_OR_RETURN(std::optional<PgJsonbValue> root_jsonb_optional,
                   GetRootJsonbHelper(jsonb_value, args[1], path_vector));
  if (!root_jsonb_optional.has_value()) {
    return CreatePgJsonbValueFromNormalized(jsonb_value.Serialize());
  }
  PgJsonbValue root_jsonb = std::move(root_jsonb_optional).value();
  GOOGLESQL_ASSIGN_OR_RETURN(std::string new_value_string,
                   GetStringRepresentation(args[2]));
  GOOGLESQL_ASSIGN_OR_RETURN(PgJsonbValue new_value,
                   PgJsonbValue::Parse(new_value_string, &tree_nodes));
  bool insert_after = args[3].bool_value();
  if (root_jsonb.IsObject()) {
    if (root_jsonb.HasMember(path_vector.back())) {
      return absl::InvalidArgumentError("cannot replace existing key");
    }
    GOOGLESQL_RETURN_IF_ERROR(root_jsonb.CreateMemberIfNotExists(path_vector.back()));
    root_jsonb.GetMemberIfExists(path_vector.back())->SetValue(new_value);
  } else if (root_jsonb.IsArray()) {
    GOOGLESQL_ASSIGN_OR_RETURN(
        int32_t index,
        root_jsonb.PathElementToIndex(path_vector.back(), path_vector.size()));
    // Deal with edge cases for the index. GetArraySize() should never be
    // past the numeric limit but even if it is, InsertArrayElement will fail.
    if (index == -1) index = root_jsonb.GetArraySize() - 1;
    if (index == std::numeric_limits<int32_t>::max()) {
      index = root_jsonb.GetArraySize();
    }
    index = insert_after ? index + 1 : index;
    GOOGLESQL_RETURN_IF_ERROR(root_jsonb.InsertArrayElement(new_value, index));
  }
  return CreatePgJsonbValueFromNormalized(jsonb_value.Serialize());
}

std::unique_ptr<googlesql::Function> JsonbInsertFunction(
    absl::string_view catalog_name) {
  const googlesql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  googlesql::FunctionOptions function_options;
  function_options.set_arguments_are_coercible(false);
  function_options.set_supports_safe_error_mode(false);
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbInsert));

  googlesql::FunctionArgumentTypeOptions jsonb_in =
      GetRequiredPositionalArgumentOptions("jsonb_in");
  googlesql::FunctionArgumentTypeOptions path =
      GetRequiredPositionalArgumentOptions("path");
  googlesql::FunctionArgumentTypeOptions replacement =
      GetRequiredPositionalArgumentOptions("replacement");
  googlesql::FunctionArgumentTypeOptions insert_after =
      GetOptionalPositionalArgumentOptions("insert_after");

  return std::make_unique<googlesql::Function>(
      kPGJsonbInsertFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_pg_jsonb,
                                       {
                                           {gsql_pg_jsonb, jsonb_in},
                                           {gsql_string_array, path},
                                           {gsql_pg_jsonb, replacement},
                                           {gsql_bool, insert_after},
                                       },
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

absl::StatusOr<googlesql::Value> EvalJsonbStripNulls(
    absl::Span<const googlesql::Value> args) {
  if (HasNullValue(args)) {
    return googlesql::Value::Null(
        postgres_translator::spangres::datatypes::GetPgJsonbType());
  }
  GOOGLESQL_ASSIGN_OR_RETURN(std::string jsonb_string, GetStringRepresentation(args[0]));
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  GOOGLESQL_ASSIGN_OR_RETURN(PgJsonbValue jsonb_value,
                   PgJsonbValue::Parse(jsonb_string, &tree_nodes));
  JsonbStripNullsImpl(jsonb_value);
  return CreatePgJsonbValueFromNormalized(jsonb_value.Serialize());
}

std::unique_ptr<googlesql::Function> JsonbStripNullsFunction(
    absl::string_view catalog_name) {
  const googlesql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  googlesql::FunctionOptions function_options;
  function_options.set_arguments_are_coercible(false);
  function_options.set_supports_safe_error_mode(false);
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbStripNulls));
  return std::make_unique<googlesql::Function>(
      kPGJsonbStripNullsFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_pg_jsonb,
                                       {gsql_pg_jsonb},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalJsonbContains(
    absl::Span<const googlesql::Value> args) {
  ABSL_DCHECK_EQ(args.size(), 2);
  if (args[0].is_null() || args[1].is_null()) {
    return googlesql::Value::NullBool();
  }

  ABSL_DCHECK(args[0].type() == args[1].type());
  ABSL_DCHECK(args[0].type() == GetPgJsonbType());

  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord in_1, GetPgJsonbNormalizedValue(args[0]));
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  GOOGLESQL_ASSIGN_OR_RETURN(PgJsonbValue input,
                   PgJsonbValue::Parse(std::string(in_1), &tree_nodes));

  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord in_2, GetPgJsonbNormalizedValue(args[1]));
  GOOGLESQL_ASSIGN_OR_RETURN(PgJsonbValue target,
                   PgJsonbValue::Parse(std::string(in_2), &tree_nodes));

  return googlesql::Value::Bool(JsonbContains(input, target));
}

absl::StatusOr<googlesql::Value> EvalJsonbContained(
    absl::Span<const googlesql::Value> args) {
  ABSL_DCHECK_EQ(args.size(), 2);
  if (args[0].is_null() || args[1].is_null()) {
    return googlesql::Value::NullBool();
  }

  ABSL_DCHECK(args[0].type() == args[1].type());
  ABSL_DCHECK(args[0].type() == GetPgJsonbType());

  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord in_1, GetPgJsonbNormalizedValue(args[0]));
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  GOOGLESQL_ASSIGN_OR_RETURN(PgJsonbValue input,
                   PgJsonbValue::Parse(std::string(in_1), &tree_nodes));

  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord in_2, GetPgJsonbNormalizedValue(args[1]));
  GOOGLESQL_ASSIGN_OR_RETURN(PgJsonbValue target,
                   PgJsonbValue::Parse(std::string(in_2), &tree_nodes));

  return googlesql::Value::Bool(JsonbContains(target, input));
}

std::unique_ptr<googlesql::Function> JsonbContainsFunction(
    absl::string_view catalog_name) {
  const googlesql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbContains));
  return std::make_unique<googlesql::Function>(
      kPGJsonbContainsFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_bool,
                                       {gsql_pg_jsonb, gsql_pg_jsonb},
                                       /*context_ptr=*/nullptr},
      },
      function_options);
}

std::unique_ptr<googlesql::Function> JsonbContainedFunction(
    absl::string_view catalog_name) {
  const googlesql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbContained));
  return std::make_unique<googlesql::Function>(
      kPGJsonbContainedFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_bool,
                                       {gsql_pg_jsonb, gsql_pg_jsonb},
                                       /*context_ptr=*/nullptr},
      },
      function_options);
}

absl::StatusOr<googlesql::Value> EvalJsonbExists(
    absl::Span<const googlesql::Value> args) {
  ABSL_DCHECK_EQ(args.size(), 2);
  if (args[0].is_null() || args[1].is_null()) {
    return googlesql::Value::NullBool();
  }

  ABSL_DCHECK(args[0].type() == GetPgJsonbType());
  ABSL_DCHECK(args[1].type() == googlesql::types::StringType());

  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord jsonb_string, GetPgJsonbNormalizedValue(args[0]));
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  GOOGLESQL_ASSIGN_OR_RETURN(PgJsonbValue input,
                   PgJsonbValue::Parse(std::string(jsonb_string), &tree_nodes));

  return googlesql::Value::Bool(input.Exists(args[1].string_value()));
}

std::unique_ptr<googlesql::Function> JsonbExistsFunction(
    absl::string_view catalog_name) {
  const googlesql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbExists));
  return std::make_unique<googlesql::Function>(
      kPGJsonbExistsFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_bool,
                                       {gsql_pg_jsonb, gsql_string},
                                       /*context_ptr=*/nullptr},
      },
      function_options);
}

absl::StatusOr<googlesql::Value> EvalJsonbExistsAny(
    absl::Span<const googlesql::Value> args) {
  ABSL_DCHECK_EQ(args.size(), 2);
  if (args[0].is_null() || args[1].is_null()) {
    return googlesql::Value::NullBool();
  }

  ABSL_DCHECK(args[0].type() == GetPgJsonbType());
  ABSL_DCHECK(args[1].type() == googlesql::types::StringArrayType());

  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord jsonb_string, GetPgJsonbNormalizedValue(args[0]));
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  GOOGLESQL_ASSIGN_OR_RETURN(PgJsonbValue input,
                   PgJsonbValue::Parse(std::string(jsonb_string), &tree_nodes));

  for (const auto& string_key : args[1].elements()) {
    if (string_key.is_null()) {
      // Ignore null element matching.
      continue;
    }

    if (input.Exists(string_key.string_value())) {
      return googlesql::Value::Bool(true);
    }
  }

  return googlesql::Value::Bool(false);
}

std::unique_ptr<googlesql::Function> JsonbExistsAnyFunction(
    absl::string_view catalog_name) {
  const googlesql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbExistsAny));
  return std::make_unique<googlesql::Function>(
      kPGJsonbExistsAnyFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_bool,
                                       {gsql_pg_jsonb, gsql_string_array},
                                       /*context_ptr=*/nullptr},
      },
      function_options);
}

absl::StatusOr<googlesql::Value> EvalJsonbExistsAll(
    absl::Span<const googlesql::Value> args) {
  ABSL_DCHECK_EQ(args.size(), 2);
  if (args[0].is_null() || args[1].is_null()) {
    return googlesql::Value::NullBool();
  }

  ABSL_DCHECK(args[0].type() == GetPgJsonbType());
  ABSL_DCHECK(args[1].type() == googlesql::types::StringArrayType());

  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord jsonb_string, GetPgJsonbNormalizedValue(args[0]));
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  GOOGLESQL_ASSIGN_OR_RETURN(PgJsonbValue input,
                   PgJsonbValue::Parse(std::string(jsonb_string), &tree_nodes));

  for (const auto& string_key : args[1].elements()) {
    if (string_key.is_null()) {
      // Ignore null element matching.
      continue;
    }

    if (!input.Exists(string_key.string_value())) {
      return googlesql::Value::Bool(false);
    }
  }

  return googlesql::Value::Bool(true);
}

std::unique_ptr<googlesql::Function> JsonbExistsAllFunction(
    absl::string_view catalog_name) {
  const googlesql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbExistsAll));
  return std::make_unique<googlesql::Function>(
      kPGJsonbExistsAllFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_bool,
                                       {gsql_pg_jsonb, gsql_string_array},
                                       /*context_ptr=*/nullptr},
      },
      function_options);
}

absl::StatusOr<googlesql::Value> EvalJsonbObjectKeys(
    absl::Span<const googlesql::Value> args) {
  ABSL_DCHECK_EQ(args.size(), 1);
  if (args[0].is_null()) {
    return googlesql::Value::Null(gsql_string_array);
  }

  ABSL_DCHECK(args[0].type() == GetPgJsonbType()) << args[0].type()->DebugString();

  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord jsonb_string, GetPgJsonbNormalizedValue(args[0]));
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  GOOGLESQL_ASSIGN_OR_RETURN(PgJsonbValue pg_jsonb_value,
                   PgJsonbValue::Parse(std::string(jsonb_string), &tree_nodes));
  if (!pg_jsonb_value.IsObject()) {
    if (pg_jsonb_value.IsArray()) {
      return absl::InvalidArgumentError(
          "cannot call jsonb_object_keys on an array");
    } else {
      return absl::InvalidArgumentError(
          "cannot call jsonb_object_keys on a scalar");
    }
  }
  std::vector<std::string> keys_array = pg_jsonb_value.GetKeys();
  // Order is expected to be by string length first, then alphabetically.
  absl::c_sort(keys_array, [](absl::string_view a, absl::string_view b) {
    return a.size() != b.size() ? a.size() < b.size() : a < b;
  });
  std::vector<googlesql::Value> values;
  values.reserve(keys_array.size());
  for (int i = 0; i < keys_array.size(); ++i) {
    values.push_back(googlesql::Value::String(keys_array[i]));
  }
  return googlesql::Value::MakeArray(gsql_string_array, values);
}

std::unique_ptr<googlesql::Function> JsonbObjectKeysFunction(
    absl::string_view catalog_name) {
  const googlesql::Type* gsql_pg_jsonb =
      postgres_translator::spangres::datatypes::GetPgJsonbType();
  googlesql::FunctionOptions function_options;
  function_options.set_arguments_are_coercible(false);
  function_options.set_supports_safe_error_mode(false);
  function_options.set_evaluator(PGFunctionEvaluator(EvalJsonbObjectKeys));
  return std::make_unique<googlesql::Function>(
      kPGJsonbObjectKeysFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_string_array,
                                       {gsql_pg_jsonb},
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
  bool operator()(const googlesql::Value lhs,
                  const googlesql::Value rhs) const {
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
  bool operator()(const googlesql::Value lhs,
                  const googlesql::Value rhs) const {
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
  bool operator()(const googlesql::Value lhs,
                  const googlesql::Value rhs) const {
    return lhs.LessThan(rhs);
  }
};

class PgGreater {
 public:
  // Returns true iff lhs is strictly greater than rhs.
  bool operator()(const googlesql::Value lhs,
                  const googlesql::Value rhs) const {
    return !(lhs.Equals(rhs) || lhs.LessThan(rhs));
  }
};

template <typename Compare>
absl::StatusOr<googlesql::Value> EvalLeastGreatest(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(!args.empty());

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
  googlesql::Value result = args[0];
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

std::pair<std::unique_ptr<googlesql::Function>,
          std::unique_ptr<googlesql::Function>>
LeastGreatestFunctions(const std::string& catalog_name) {
  auto is_non_floating_point_supported_type =
      [](const googlesql::Type* type) -> bool {
    return (type->IsInt64() || type->IsBool() || type->IsBytes() ||
            type->IsString() || type->IsDate() || type->IsTimestamp() ||
            type->IsInterval());
  };

  googlesql::FunctionEvaluatorFactory least_evaluator_factory(
      [&](const googlesql::FunctionSignature& signature)
          -> absl::StatusOr<googlesql::FunctionEvaluator> {
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
  googlesql::FunctionOptions least_function_options;
  least_function_options.set_supports_safe_error_mode(false);
  least_function_options.set_arguments_are_coercible(false);
  least_function_options.set_evaluator_factory(least_evaluator_factory);

  googlesql::FunctionEvaluatorFactory greatest_evaluator_factory(
      [&](const googlesql::FunctionSignature& signature)
          -> absl::StatusOr<googlesql::FunctionEvaluator> {
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
  googlesql::FunctionOptions greatest_function_options;
  greatest_function_options.set_supports_safe_error_mode(false);
  greatest_function_options.set_arguments_are_coercible(false);
  greatest_function_options.set_evaluator_factory(greatest_evaluator_factory);

  std::vector<const googlesql::Type*> supported_types{
      googlesql::types::DoubleType(),
      googlesql::types::FloatType(),
      googlesql::types::Int64Type(),
      googlesql::types::BoolType(),
      googlesql::types::BytesType(),
      googlesql::types::StringType(),
      googlesql::types::DateType(),
      googlesql::types::TimestampType(),
      postgres_translator::spangres::datatypes::GetPgNumericType(),
      postgres_translator::spangres::datatypes::GetPgJsonbType(),
      googlesql::types::IntervalType(),
  };

  // Construct the function signatures for all the supported types.
  std::vector<googlesql::FunctionSignature> function_signatures;
  function_signatures.reserve(supported_types.size());
  for (auto type : supported_types) {
    function_signatures.push_back(googlesql::FunctionSignature{
        type,
        {type, {type, googlesql::FunctionArgumentType::REPEATED}},
        nullptr});
  }

  return {
      // pg.least
      std::make_unique<googlesql::Function>(
          kPGLeastFunctionName, catalog_name, googlesql::Function::SCALAR,
          function_signatures, least_function_options),
      // pg.greatest
      std::make_unique<googlesql::Function>(
          kPGGreatestFunctionName, catalog_name, googlesql::Function::SCALAR,
          function_signatures, greatest_function_options)};
}

// Aggregate functions.

template <typename T, typename = std::enable_if_t<std::is_floating_point_v<T>>>
class MinFloatingPointEvaluator : public googlesql::AggregateFunctionEvaluator {
 public:
  explicit MinFloatingPointEvaluator() {}
  ~MinFloatingPointEvaluator() override = default;

  absl::Status Reset() override { return absl::OkStatus(); }

  absl::Status Accumulate(absl::Span<const googlesql::Value*> args,
                          bool* stop_accumulation) override {
    // No args left to accumulate.
    if (args.empty()) {
      *stop_accumulation = true;
      return absl::OkStatus();
    }

    const googlesql::Value value = *args[0];
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
      result_ = googlesql::Value::Make<T>(std::numeric_limits<T>::quiet_NaN());
    }

    // Use the comparison function that respects the NaN-ordering semantics of
    // PostgreSQL.
    if (PgFloatingPointLesser<T>()(value, result_)) {
      result_ = value;
    }

    return absl::OkStatus();
  }

  absl::StatusOr<googlesql::Value> GetFinalResult() override { return result_; }

 private:
  // Initialized to NULL as it's the default value to return if no values are
  // provided to aggregate or if all the values to aggregate are NULL.
  googlesql::Value result_ = googlesql::Value::MakeNull<T>();
};

class MinMaxEvaluator : public googlesql::AggregateFunctionEvaluator {
 public:
  explicit MinMaxEvaluator(const googlesql::Type* type, bool is_min) :
    result_(googlesql::Value::Null(type)), is_min_(is_min) {}
  ~MinMaxEvaluator() override = default;

  absl::Status Reset() override { return absl::OkStatus(); }

  absl::Status Accumulate(absl::Span<const googlesql::Value*> args,
                          bool* stop_accumulation) override {
    // No args left to accumulate.
    if (args.empty()) {
      *stop_accumulation = true;
      return absl::OkStatus();
    }

    const googlesql::Value value = *args[0];
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

  absl::StatusOr<googlesql::Value> GetFinalResult() override { return result_; }

 private:
  // Initialized to NULL as it's the default value to return if no values are
  // provided to aggregate or if all the values to aggregate are NULL.
  googlesql::Value result_;
  bool is_min_;
};

class MinMaxNumericEvaluator : public googlesql::AggregateFunctionEvaluator {
 public:
  explicit MinMaxNumericEvaluator(bool is_min) : is_min_(is_min) {}
  ~MinMaxNumericEvaluator() override = default;

  absl::Status Reset() override { return absl::OkStatus(); }

  absl::Status Accumulate(absl::Span<const googlesql::Value*> args,
                          bool* stop_accumulation) override {
    // No args left to accumulate.
    if (args.empty()) {
      *stop_accumulation = true;
      return absl::OkStatus();
    }

    const googlesql::Value value = *args[0];
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
    GOOGLESQL_ASSIGN_OR_RETURN(
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

  absl::StatusOr<googlesql::Value> GetFinalResult() override { return result_; }

 private:
  const googlesql::Type* gsql_pg_numeric_ =
      spangres::datatypes::GetPgNumericType();
  // Initialized to NULL as it's the default value to return if no values are
  // provided to aggregate or if all the values to aggregate are NULL.
  googlesql::Value result_ = googlesql::values::Null(gsql_pg_numeric_);
  const bool is_min_;
};

std::unique_ptr<googlesql::Function> PgMinAggregator(
    const std::string& catalog_name) {
  googlesql::AggregateFunctionEvaluatorFactory aggregate_fn =
      [](const googlesql::FunctionSignature& sig)
      -> std::unique_ptr<googlesql::AggregateFunctionEvaluator> {
    if (sig.result_type().type()->IsFloat()) {
      return std::make_unique<MinFloatingPointEvaluator<float>>();
    } else if (sig.result_type().type()->IsDouble()) {
      return std::make_unique<MinFloatingPointEvaluator<double>>();
    }
    return nullptr;
  };

  googlesql::FunctionOptions options;
  options.set_aggregate_function_evaluator_factory(aggregate_fn);
  return std::make_unique<googlesql::Function>(
      kPGMinFunctionName, catalog_name, googlesql::Function::AGGREGATE,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{googlesql::types::DoubleType(),
                                       {googlesql::types::DoubleType()},
                                       nullptr},
          googlesql::FunctionSignature{googlesql::types::FloatType(),
                                       {googlesql::types::FloatType()},
                                       nullptr}},
      options);
}

std::unique_ptr<googlesql::Function> MinAggregator(
    const std::string& catalog_name) {
  const googlesql::Type* gsql_pg_numeric =
      postgres_translator::spangres::datatypes::GetPgNumericType();
  googlesql::AggregateFunctionEvaluatorFactory aggregate_fn =
      [](const googlesql::FunctionSignature& sig)
      -> std::unique_ptr<googlesql::AggregateFunctionEvaluator> {
    if (sig.result_type().type() == spangres::datatypes::GetPgNumericType()) {
      return std::make_unique<MinMaxNumericEvaluator>(/* is_min =*/true);
    } else {
      return std::make_unique<MinMaxEvaluator>(sig.result_type().type(),
                                               /* is_min =*/true);
    }
  };

  googlesql::FunctionOptions options;
  options.set_aggregate_function_evaluator_factory(aggregate_fn);
  return std::make_unique<googlesql::Function>(
      kGoogleSQLMinFunctionName, catalog_name, googlesql::Function::AGGREGATE,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{spangres::datatypes::GetPgOidType(),
                                       {spangres::datatypes::GetPgOidType()},
                                       nullptr},
          googlesql::FunctionSignature{
              gsql_pg_numeric, {gsql_pg_numeric}, nullptr}},
      options);
}

std::unique_ptr<googlesql::Function> MaxAggregator(
    const std::string& catalog_name) {
  const googlesql::Type* gsql_pg_numeric =
      postgres_translator::spangres::datatypes::GetPgNumericType();
  googlesql::AggregateFunctionEvaluatorFactory aggregate_fn =
      [](const googlesql::FunctionSignature& sig)
      -> std::unique_ptr<googlesql::AggregateFunctionEvaluator> {
    return std::make_unique<MinMaxEvaluator>(sig.result_type().type(),
                                             /* is_min =*/false);
  };

  googlesql::FunctionOptions options;
  options.set_aggregate_function_evaluator_factory(aggregate_fn);
  return std::make_unique<googlesql::Function>(
      kGoogleSQLMaxFunctionName, catalog_name, googlesql::Function::AGGREGATE,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{spangres::datatypes::GetPgOidType(),
                                       {spangres::datatypes::GetPgOidType()},
                                       nullptr},
          googlesql::FunctionSignature{
              gsql_pg_numeric, {gsql_pg_numeric}, nullptr}},
      options);
}

enum SumAvgAggregatorType { Sum, Avg };

// Can evaluate a sum for INT64, FLOAT, DOUBLE and PG.NUMERIC.
class SumEvaluator : public googlesql::AggregateFunctionEvaluator {
 public:
  explicit SumEvaluator() = default;
  ~SumEvaluator() override = default;

  absl::Status Reset() override { return absl::OkStatus(); }

  absl::Status Accumulate(absl::Span<const googlesql::Value*> args,
                          bool* stop_accumulation) override {
    // No args left to accumulate.
    if (args.empty()) {
      *stop_accumulation = true;
      return absl::OkStatus();
    }

    const googlesql::Value value = *args[0];

    // This is the first time we're seeing a value so set the type for
    // accumulation based on the type of the first argument and set the result_
    // to the NULL value for that type.
    if (kind_ == googlesql::TYPE_UNKNOWN) {
      if (value.type_kind() == googlesql::TYPE_DOUBLE) {
        kind_ = googlesql::TYPE_DOUBLE;
        result_ = googlesql::values::NullDouble();
      } else if (value.type_kind() == googlesql::TYPE_FLOAT) {
        kind_ = googlesql::TYPE_FLOAT;
        // Avg of float returns a double.
        result_ = IsAvgEvaluator() ? googlesql::values::NullDouble()
                                   : googlesql::values::NullFloat();
      } else if (value.type_kind() == googlesql::TYPE_INT64 ||
                 (value.type_kind() == googlesql::TYPE_EXTENDED &&
                  value.type()->Equals(gsql_pg_numeric_))) {
        // Both INT64 and PG.NUMERIC return PG.NUMERIC.
        kind_ = value.type_kind();
        result_ = googlesql::values::Null(gsql_pg_numeric_);
      } else {
        return absl::InvalidArgumentError(
            "Cannot accumulate value which is not of type INT64, FLOAT, DOUBLE "
            "or PG.NUMERIC.");
      }
    } else if (value.type_kind() != kind_ ||
               (value.type_kind() == googlesql::TYPE_EXTENDED &&
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
      if (value.type_kind() == googlesql::TYPE_INT64) {
        // Result must be of type PG.NUMERIC so convert the result to the
        // correct type.
        GOOGLESQL_ASSIGN_OR_RETURN(result_, CreatePgNumericValueWithMemoryContext(
                                      absl::StrCat(value.int64_value())));
      } else if (IsAvgEvaluator() &&
                 value.type_kind() == googlesql::TYPE_FLOAT) {
        // Convert the float to double for avg calculations.
        result_ =
            googlesql::values::Double(static_cast<double>(value.float_value()));
      } else {
        result_ = value;
      }

      count_++;
      return absl::OkStatus();
    }

    // Now do the addition.
    if (value.type_kind() == googlesql::TYPE_INT64) {
      // Setup the memory context arena which is required for
      // CreatePgNumericValue() and EvalGoogleSQLAdd().
      GOOGLESQL_ASSIGN_OR_RETURN(
          std::unique_ptr<postgres_translator::interfaces::PGArena> arena,
          postgres_translator::interfaces::CreatePGArena(nullptr));
      GOOGLESQL_ASSIGN_OR_RETURN(auto value_as_numeric,
                       CreatePgNumericValue(absl::StrCat(value.int64_value())));
      GOOGLESQL_ASSIGN_OR_RETURN(
          result_,
          EvalGoogleSQLAdd(absl::MakeConstSpan({result_, value_as_numeric})));
    } else if (value.type_kind() == googlesql::TYPE_DOUBLE) {
      double result;
      absl::Status status;
      if (!googlesql::functions::Add(result_.double_value(),
                                     value.double_value(), &result, &status)) {
        return status;
      }
      result_ = googlesql::values::Double(result);
    } else if (value.type_kind() == googlesql::TYPE_FLOAT &&
               !IsAvgEvaluator()) {
      float result;
      absl::Status status;
      if (!googlesql::functions::Add(result_.float_value(), value.float_value(),
                                     &result, &status)) {
        return status;
      }
      result_ = googlesql::values::Float(result);
    } else if (value.type_kind() == googlesql::TYPE_FLOAT && IsAvgEvaluator()) {
      // Calculations of avg over float values happens in the double domain.
      double result;
      absl::Status status;
      if (!googlesql::functions::Add(result_.double_value(),
                                     static_cast<double>(value.float_value()),
                                     &result, &status)) {
        return status;
      }
      result_ = googlesql::values::Double(result);
    } else if (value.type_kind() == googlesql::TYPE_EXTENDED) {
      // Setup the memory context arena which is required for
      // EvalGoogleSQLAdd().
      GOOGLESQL_ASSIGN_OR_RETURN(
          std::unique_ptr<postgres_translator::interfaces::PGArena> arena,
          postgres_translator::interfaces::CreatePGArena(nullptr));
      GOOGLESQL_ASSIGN_OR_RETURN(result_,
                       EvalGoogleSQLAdd(absl::MakeConstSpan({result_, value})));
    }  // No else because we've already validated the type above.

    count_++;
    return absl::OkStatus();
  }

  absl::StatusOr<googlesql::Value> GetFinalResult() override {
    if (kind_ == googlesql::TYPE_UNKNOWN) {
      // This is not quite correct because we'll be returning a value of type
      // PG.NUMERIC even if the column being aggregated is of type DOUBLE. We
      // can't do much about it because a googlesql::AggregateFunctionEvaluator
      // doesn't know anything about the return type.
      return googlesql::values::Null(gsql_pg_numeric_);
    }
    return result_;
  }

 protected:
  uint64_t count_ = 0;
  googlesql::TypeKind kind_ = googlesql::TYPE_UNKNOWN;
  googlesql::Value result_;
  const googlesql::Type* gsql_pg_numeric_ =
      spangres::datatypes::GetPgNumericType();

  virtual bool IsAvgEvaluator() { return false; }
};

std::unique_ptr<googlesql::Function> SumAggregator(
    const std::string& catalog_name) {
  googlesql::AggregateFunctionEvaluatorFactory aggregate_fn =
      [](const googlesql::FunctionSignature& sig) {
        return std::make_unique<SumEvaluator>();
      };

  const googlesql::Type* gsql_pg_numeric =
      postgres_translator::spangres::datatypes::GetPgNumericType();

  googlesql::FunctionOptions options;
  options.set_aggregate_function_evaluator_factory(aggregate_fn);
  return std::make_unique<googlesql::Function>(
      kPGSumFunctionName, catalog_name, googlesql::Function::AGGREGATE,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{
              gsql_pg_numeric, {googlesql::types::Int64Type()}, nullptr},
          googlesql::FunctionSignature{googlesql::types::DoubleType(),
                                       {googlesql::types::DoubleType()},
                                       nullptr},
          googlesql::FunctionSignature{googlesql::types::FloatType(),
                                       {googlesql::types::FloatType()},
                                       nullptr},
          googlesql::FunctionSignature{
              gsql_pg_numeric, {gsql_pg_numeric}, nullptr}},
      options);
}

// Can evaluate the avg for INT64, FLOAT, DOUBLE and PG.NUMERIC.
class AvgEvaluator : public SumEvaluator {
 public:
  absl::StatusOr<googlesql::Value> GetFinalResult() override {
    if (kind_ == googlesql::TYPE_UNKNOWN) {
      // This is not quite correct because we'll be returning a value of type
      // PG.NUMERIC even if the column being aggregated is of type DOUBLE. We
      // can't do much about it because a googlesql::AggregateFunctionEvaluator
      // doesn't know anything about the return type.
      return googlesql::values::Null(gsql_pg_numeric_);
    }

    if (kind_ == googlesql::TYPE_DOUBLE || kind_ == googlesql::TYPE_FLOAT) {
      if (result_.is_null()) {
        return googlesql::values::NullDouble();
      }
      double result;
      absl::Status status;
      // `result_` is always a double value, even for when the input is float.
      if (!googlesql::functions::Divide(result_.double_value(),
                                        static_cast<double>(count_), &result,
                                        &status)) {
        return status;
      }
      return googlesql::values::Double(result);
    }

    // INT64 or PG.NUMERIC:
    // Setup the memory context arena which is required for
    // CreatePgNumericValue() and EvalGoogleSQLDivide().
    GOOGLESQL_ASSIGN_OR_RETURN(
        std::unique_ptr<postgres_translator::interfaces::PGArena> arena,
        postgres_translator::interfaces::CreatePGArena(nullptr));
    GOOGLESQL_ASSIGN_OR_RETURN(auto count_as_numeric,
                     CreatePgNumericValue(absl::StrCat(count_)));
    return EvalGoogleSQLDivide(
        absl::MakeConstSpan({result_, count_as_numeric}));
  }

 protected:
  virtual bool IsAvgEvaluator() override { return true; }
};

std::unique_ptr<googlesql::Function> AvgAggregator(
    const std::string& catalog_name) {
  googlesql::AggregateFunctionEvaluatorFactory aggregate_fn =
      [](const googlesql::FunctionSignature& sig) {
        return std::make_unique<AvgEvaluator>();
      };

  const googlesql::Type* gsql_pg_numeric =
      postgres_translator::spangres::datatypes::GetPgNumericType();

  googlesql::FunctionOptions options;
  options.set_aggregate_function_evaluator_factory(aggregate_fn);
  return std::make_unique<googlesql::Function>(
      kPGAvgFunctionName, catalog_name, googlesql::Function::AGGREGATE,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{
              gsql_pg_numeric, {googlesql::types::Int64Type()}, nullptr},
          googlesql::FunctionSignature{googlesql::types::DoubleType(),
                                       {googlesql::types::DoubleType()},
                                       nullptr},
          googlesql::FunctionSignature{googlesql::types::DoubleType(),
                                       {googlesql::types::FloatType()},
                                       nullptr},
          googlesql::FunctionSignature{
              gsql_pg_numeric, {gsql_pg_numeric}, nullptr}},
      options);
}

template <googlesql::TypeKind T>
absl::StatusOr<googlesql::Value> EvalCastFromOid(
    absl::Span<const googlesql::Value> args) {
  switch (T) {
    case googlesql::TYPE_INT64:
      return EvalCastOidToInt64(args);
    case googlesql::TYPE_STRING:
      return EvalCastOidToString(args);
    default:
      return absl::InvalidArgumentError(
          absl::StrCat("cannot cast oid object to type ", T));
  }
}

std::unique_ptr<googlesql::Function> CastFromOidFunction(
    absl::string_view catalog_name) {
  static const googlesql::Type* gsql_pg_oid =
      spangres::datatypes::GetPgOidType();

  googlesql::FunctionEvaluatorFactory evaluator_factory(
      [&](const googlesql::FunctionSignature& signature)
          -> absl::StatusOr<googlesql::FunctionEvaluator> {
        if (signature.result_type().type()->IsInt64()) {
          return EvalCastOidToInt64;
        } else if (signature.result_type().type()->IsString()) {
          return ::postgres_translator::EvalCastFromOid<googlesql::TYPE_STRING>;
        } else {
          return absl::InvalidArgumentError(
              absl::StrCat("cannot cast oid object to type ",
                           signature.result_type().type()->ShortTypeName(
                               googlesql::PRODUCT_EXTERNAL)));
        }
      });

  googlesql::FunctionOptions function_options;
  function_options.set_evaluator_factory(evaluator_factory);
  return std::make_unique<googlesql::Function>(
      kPGCastFromOidFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{
              gsql_int64, {gsql_pg_oid}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_string, {gsql_pg_oid}, /*context_ptr=*/nullptr},
      },
      function_options);
}

std::unique_ptr<googlesql::Function> CastToOidFunction(
    absl::string_view catalog_name) {
  static const googlesql::Type* gsql_pg_oid =
      spangres::datatypes::GetPgOidType();

  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(EvalCastToOid));
  return std::make_unique<googlesql::Function>(
      kPGCastToOidFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{
              gsql_pg_oid, {gsql_int64}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_pg_oid, {gsql_string}, /*context_ptr=*/nullptr},
      },
      function_options);
}

absl::StatusOr<googlesql::JSONValueConstRef> GetJSONValueConstRef(
    const googlesql::Value& jsonb, googlesql::JSONValue& json_storage) {
  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord jsonb_cord, GetPgJsonbNormalizedValue(jsonb));
  GOOGLESQL_ASSIGN_OR_RETURN(
      json_storage,
      googlesql::JSONValue::ParseJSONString(
          jsonb_cord.Flatten(),
          {.wide_number_mode =
               googlesql::JSONParsingOptions::WideNumberMode::kExact}));
  return json_storage.GetConstRef();
}

template <typename T>
std::unique_ptr<googlesql::Function> JsonbArrayExtractionFunction(
    absl::string_view catalog_name) {
  const googlesql::Type* pg_jsonb_type = spangres::datatypes::GetPgJsonbType();
  absl::string_view function_name;
  const googlesql::Type* return_type;

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

  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator([return_type](
                                     absl::Span<const googlesql::Value> args)
                                     -> absl::StatusOr<googlesql::Value> {
    GOOGLESQL_RET_CHECK_EQ(args.size(), 1);
    if (args[0].is_null()) {
      return googlesql::Value::Null(return_type);
    }

    googlesql::JSONValue json_storage;
    GOOGLESQL_ASSIGN_OR_RETURN(googlesql::JSONValueConstRef json_value_const_ref,
                     GetJSONValueConstRef(args[0], json_storage));

    if constexpr (std::is_same_v<T, bool>) {
      GOOGLESQL_ASSIGN_OR_RETURN(
          auto result,
          googlesql::functions::ConvertJsonToBoolArray(json_value_const_ref));
      return googlesql::values::BoolArray(result);
    } else if constexpr (std::is_same_v<T, int64_t>) {
      GOOGLESQL_ASSIGN_OR_RETURN(
          auto result,
          googlesql::functions::ConvertJsonToInt64Array(json_value_const_ref));
      return googlesql::values::Int64Array(result);
    } else {
      static_assert(std::is_same_v<T, std::string>, "Unexpected type");
      GOOGLESQL_ASSIGN_OR_RETURN(
          auto result,
          googlesql::functions::ConvertJsonToStringArray(json_value_const_ref));
      return googlesql::values::StringArray(result);
    }
  });

  return std::make_unique<googlesql::Function>(
      function_name, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{return_type,
                                       {pg_jsonb_type},
                                       /*context_ptr=*/nullptr},
      },
      function_options);
}

template <typename T>
std::unique_ptr<googlesql::Function> JsonbFloatArrayExtractionFunction(
    absl::string_view catalog_name) {
  const googlesql::Type* pg_jsonb_type = spangres::datatypes::GetPgJsonbType();
  absl::string_view function_name;
  const googlesql::Type* return_type;
  if constexpr (std::is_same_v<T, double>) {
    function_name = "float64_array";
    return_type = gsql_double_array;
  } else {
    static_assert(std::is_same_v<T, float>, "Unexpected type");
    function_name = "float32_array";
    return_type = gsql_float_array;
  }

  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(
      [return_type](absl::Span<const googlesql::Value> args)
          -> absl::StatusOr<googlesql::Value> {
        GOOGLESQL_RET_CHECK_EQ(args.size(), 1);
        if (args[0].is_null()) {
          return googlesql::Value::Null(return_type);
        }
        // PG currently does not support optional `wide_number_mode` parameter.
        googlesql::functions::WideNumberMode mode =
            googlesql::functions::WideNumberMode::kRound;
        googlesql::ProductMode product_mode = googlesql::PRODUCT_EXTERNAL;

        googlesql::JSONValue json_storage;
        GOOGLESQL_ASSIGN_OR_RETURN(googlesql::JSONValueConstRef json_value_const_ref,
                         GetJSONValueConstRef(args[0], json_storage));

        if constexpr (std::is_same_v<T, double>) {
          GOOGLESQL_ASSIGN_OR_RETURN(auto result,
                           googlesql::functions::ConvertJsonToDoubleArray(
                               json_value_const_ref, mode, product_mode));
          return googlesql::values::DoubleArray(result);
        } else {
          static_assert(std::is_same_v<T, float>, "Unexpected type");
          GOOGLESQL_ASSIGN_OR_RETURN(auto result,
                           googlesql::functions::ConvertJsonToFloatArray(
                               json_value_const_ref, mode, product_mode));
          return googlesql::values::FloatArray(result);
        }
      });

  return std::make_unique<googlesql::Function>(
      function_name, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{return_type,
                                       {pg_jsonb_type},
                                       /*context_ptr=*/nullptr},
      },
      function_options);
}

}  // namespace

absl::StatusOr<googlesql::Value> EvalTimestamptzTrunc(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 2 || args.size() == 3);
  if (HasNullValue(args)) {
    return googlesql::Value::NullTimestamp();
  }
  auto unix_picos = args[1].ToUnixPicos();
  if (args.size() == 2) {
    GOOGLESQL_ASSIGN_OR_RETURN(
        absl::Time time,
        PgTimestamptzTrunc(args[0].string_value(), unix_picos.ToAbslTime()));
    return googlesql::Value::Timestamp(time);
  } else {
    GOOGLESQL_ASSIGN_OR_RETURN(
        absl::Time time,
        PgTimestamptzTrunc(args[0].string_value(), unix_picos.ToAbslTime(),
                           args[2].string_value()));
    return googlesql::Value::Timestamp(time);
  }
}

absl::StatusOr<googlesql::Value> EvalToChar(
  absl::Span<const googlesql::Value> args) {
  static const googlesql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();
  GOOGLESQL_RET_CHECK(args.size() == 2);
  if (HasNullValue(args)) {
    return googlesql::Value::NullString();
  }

  switch (args[0].type_kind()) {
    case googlesql::TYPE_INT64: {
      GOOGLESQL_ASSIGN_OR_RETURN(std::string result, Int8ToChar(args[0].int64_value(),
                                                      args[1].string_value()));
      return googlesql::Value::String(result);
    }
    case googlesql::TYPE_TIMESTAMP: {
      GOOGLESQL_ASSIGN_OR_RETURN(
          std::unique_ptr<std::string> result,
          PgTimestampTzToChar(args[0].ToUnixPicos().ToAbslTime(),
                              args[1].string_value()));
      if (result == nullptr) {
        return googlesql::Value::NullString();
      } else {
        return googlesql::Value::String(*result);
      }
    }
    case googlesql::TYPE_DOUBLE: {
      GOOGLESQL_ASSIGN_OR_RETURN(
          std::string result,
          Float8ToChar(args[0].double_value(), args[1].string_value()));
      return googlesql::Value::String(result);
    }
    case googlesql::TYPE_FLOAT: {
      GOOGLESQL_ASSIGN_OR_RETURN(
          std::string result,
          Float4ToChar(args[0].float_value(), args[1].string_value()));
      return googlesql::Value::String(result);
    }
    case googlesql::TYPE_INTERVAL: {
      std::unique_ptr<std::string> result;
      GOOGLESQL_ASSIGN_OR_RETURN(googlesql::IntervalValue interval,
                      RoundPrecision(args[0].interval_value()));
      GOOGLESQL_ASSIGN_OR_RETURN(result,
                      PgIntervalToChar(interval, args[1].string_value()));
      return result == nullptr ? googlesql::Value::NullString()
                              : googlesql::Value::String(*result);
    }
    case googlesql::TYPE_EXTENDED:
      if (args[0].type()->Equals(gsql_pg_numeric)) {
        GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord numeric_string,
                        GetPgNumericNormalizedValue(args[0]));
        GOOGLESQL_ASSIGN_OR_RETURN(
            std::string result,
            NumericToChar(std::string(numeric_string), args[1].string_value()));
        return googlesql::Value::String(result);
      }
      [[fallthrough]];
    default:
      return absl::UnimplementedError(
          absl::StrCat("to_char(", args[0].type()->DebugString(), ", text)"));
  }
}

absl::StatusOr<googlesql::Value> EvalExtract(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 2);
  if (HasNullValue(args)) {
    return googlesql::Value::Null(spangres::datatypes::GetPgNumericType());
  }
  if (args[1].type_kind() == googlesql::TYPE_TIMESTAMP) {
    GOOGLESQL_ASSIGN_OR_RETURN(
        absl::Cord result,
        PgTimestamptzExtract(args[0].string_value(),
                             args[1].ToUnixPicos().ToAbslTime()));
    return CreatePgNumericValue(std::string(result));
  } else {
    GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord result, PgDateExtract(args[0].string_value(),
                                                      args[1].date_value()));
    return CreatePgNumericValue(std::string(result));
  }
}

absl::StatusOr<googlesql::Value> EvalCastToTimestamp(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 1);
  if (args[0].is_null()) {
    return googlesql::Value::NullTimestamp();
  }

  GOOGLESQL_ASSIGN_OR_RETURN(absl::Time time, function_evaluators::PgTimestamptzIn(
                                        args[0].string_value()));
  return googlesql::Value::Timestamp(time);
}

absl::StatusOr<googlesql::Value> EvalCastToString(
  absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 1);

  switch (args[0].type()->kind()) {
    case googlesql::TYPE_INTERVAL:
      return EvalCastIntervalToString(args);
    case googlesql::TYPE_EXTENDED: {
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

absl::StatusOr<googlesql::Value> EvalCastNumericToInt64(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 1);
  if (args[0].is_null()) {
    return googlesql::Value::NullInt64();
  }
  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord normalized_value,
                   GetPgNumericNormalizedValue(args[0]));
  std::string numeric_string(normalized_value);

  GOOGLESQL_ASSIGN_OR_RETURN(int64_t result, CastNumericToInt8(numeric_string));

  return googlesql::Value::Int64(result);
}

absl::StatusOr<googlesql::Value> EvalCastNumericToDouble(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 1);
  if (args[0].is_null()) {
    return googlesql::Value::NullDouble();
  }
  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord normalized_value,
                   GetPgNumericNormalizedValue(args[0]));
  double out;
  bool result = absl::SimpleAtod(std::string(normalized_value), &out);
  if (!result || std::isinf(out)) {
    return absl::OutOfRangeError(absl::StrCat("Cannot cast to double from ",
                                              std::string(normalized_value)));
  }
  return googlesql::Value::Double(out);
}

absl::StatusOr<googlesql::Value> EvalCastNumericToFloat(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 1);
  if (args[0].is_null()) {
    return googlesql::Value::NullFloat();
  }
  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord normalized_value,
                   GetPgNumericNormalizedValue(args[0]));
  float out;
  bool result = absl::SimpleAtof(std::string(normalized_value), &out);
  if (!result || std::isinf(out)) {
    return absl::OutOfRangeError(absl::StrCat("Cannot cast to float from ",
                                              std::string(normalized_value)));
  }
  return googlesql::Value::Float(out);
}

absl::StatusOr<googlesql::Value> EvalCastNumericToString(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 1);
  if (args[0].is_null()) {
    return googlesql::Value::NullString();
  }
  GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord normalized_value,
                   GetPgNumericNormalizedValue(args[0]));
  return googlesql::Value::String(std::string(normalized_value));
}

absl::StatusOr<googlesql::Value> EvalCastIntervalToString(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 1);
  GOOGLESQL_RET_CHECK(args[0].type()->IsInterval());
  if (args[0].is_null()) {
    return googlesql::Value::NullString();
  }

  GOOGLESQL_ASSIGN_OR_RETURN(std::string result, PgIntervalOut(args[0].interval_value()));
  return googlesql::Value::String(result);
}

absl::StatusOr<googlesql::Value> EvalCastStringToInterval(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 1);
  GOOGLESQL_RET_CHECK_EQ(args[0].type(), gsql_string);
  if (args[0].is_null()) {
    return googlesql::Value::Null(gsql_interval);
  }

  GOOGLESQL_ASSIGN_OR_RETURN(googlesql::IntervalValue result,
                   PgIntervalIn(args[0].string_value()));
  return googlesql::Value::Interval(result);
}

namespace {
template <typename T, typename = std::enable_if_t<std::is_floating_point_v<T>>>
absl::StatusOr<std::string> FloatingPointToNumeric(
    googlesql::Value gsql_value) {
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

absl::StatusOr<googlesql::Value> EvalCastToNumeric(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(!args.empty() && args.size() < 4);

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
    GOOGLESQL_RETURN_IF_ERROR(
        spangres::datatypes::common::ValidatePrecisionAndScale(precision, scale)
            .status());
  }

  // Precision and scale are valid at this point. Return null numeric if input
  // value is null.
  if (args[0].is_null()) {
    return googlesql::Value::Null(spangres::datatypes::GetPgNumericType());
  }

  std::string input_to_string;
  switch (args[0].type_kind()) {
    case googlesql::TYPE_INT64:
      input_to_string = absl::StrCat(args[0].int64_value());
      break;
    case googlesql::TYPE_DOUBLE: {
      GOOGLESQL_ASSIGN_OR_RETURN(input_to_string,
                       FloatingPointToNumeric<double>(args[0]));
      break;
    }
    case googlesql::TYPE_FLOAT: {
      GOOGLESQL_ASSIGN_OR_RETURN(input_to_string, FloatingPointToNumeric<float>(args[0]));
      break;
    }
    case googlesql::TYPE_STRING:
      input_to_string = args[0].string_value();
      break;
    case googlesql::TYPE_EXTENDED: {
      auto type_code =
          static_cast<const spangres::datatypes::SpannerExtendedType*>(
              args[0].type())
              ->code();
      if (type_code == spangres::datatypes::TypeAnnotationCode::PG_NUMERIC) {
        GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord normalized,
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

absl::StatusOr<googlesql::Value> EvalCastToOid(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(!args.empty() && args.size() < 2);
  if (args[0].is_null()) {
    return googlesql::Value::Null(spangres::datatypes::GetPgOidType());
  }
  switch (args[0].type_kind()) {
    case googlesql::TYPE_INT64: {
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
    case googlesql::TYPE_STRING: {
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
          args[0].type()->ShortTypeName(googlesql::PRODUCT_EXTERNAL),
          " to oid"));
  }
}

absl::StatusOr<googlesql::Value> EvalCastOidToInt64(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK_EQ(args.size(), 1);
  GOOGLESQL_RET_CHECK_EQ(args[0].type(), spangres::datatypes::GetPgOidType());
  if (args[0].is_null()) {
    return googlesql::Value::NullInt64();
  }
  GOOGLESQL_ASSIGN_OR_RETURN(int64_t oid, spangres::datatypes::GetPgOidValue(args[0]));
  return googlesql::Value::Int64(oid);
}

absl::StatusOr<googlesql::Value> EvalCastOidToString(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK_EQ(args.size(), 1);
  GOOGLESQL_RET_CHECK_EQ(args[0].type(), spangres::datatypes::GetPgOidType());
  if (args[0].is_null()) {
    return googlesql::Value::NullString();
  }
  GOOGLESQL_ASSIGN_OR_RETURN(int64_t oid, spangres::datatypes::GetPgOidValue(args[0]));
  return googlesql::Value::String(absl::StrCat(oid));
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

absl::StatusOr<googlesql::Value> EvalFloatArithmetic(
    absl::Span<const googlesql::Value> args,
    std::function<bool(float, float, float*, absl::Status*)> Fn) {
  GOOGLESQL_RET_CHECK(args.size() == 2 && Fn != nullptr);
  if (args[0].is_null() || args[1].is_null()) {
    return googlesql::Value::Null(googlesql::types::FloatType());
  }

  float arg0 = args[0].float_value();
  float arg1 = args[1].float_value();

  float result;
  absl::Status error;
  bool is_success = Fn(arg0, arg1, &result, &error);

  if (!is_success || !error.ok()) {
    return error;
  }

  return googlesql::Value::Float(result);
}

std::unique_ptr<googlesql::Function> FloatArithmeticFunction(
    absl::string_view function_name, absl::string_view catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);

  if (function_name == kPGFloatAddFunctionName) {
    function_options.set_evaluator(
        PGFunctionEvaluator([&](absl::Span<const googlesql::Value> args) {
          return EvalFloatArithmetic(args, googlesql::functions::Add<float>);
        }));
  } else if (function_name == kPGFloatSubtractFunctionName) {
    function_options.set_evaluator(
        PGFunctionEvaluator([&](absl::Span<const googlesql::Value> args) {
          return EvalFloatArithmetic(args,
                                     googlesql::functions::Subtract<float>);
        }));
  } else if (function_name == kPGFloatMultiplyFunctionName) {
    function_options.set_evaluator(
        PGFunctionEvaluator([&](absl::Span<const googlesql::Value> args) {
          return EvalFloatArithmetic(args,
                                     googlesql::functions::Multiply<float>);
        }));
  } else if (function_name == kPGFloatDivideFunctionName) {
    function_options.set_evaluator(
        PGFunctionEvaluator([&](absl::Span<const googlesql::Value> args) {
          return EvalFloatArithmetic(args, FloatDivide);
        }));
  } else {
    ABSL_DCHECK(false) << "Unsupported float arithmetic function: "
                  << function_name;
    return nullptr;
  }

  return std::make_unique<googlesql::Function>(
      function_name, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{{gsql_float,
                                                 {gsql_float, gsql_float},
                                                 /*context_ptr=*/nullptr}},
      function_options);
}

std::unique_ptr<googlesql::Function> IntervalAddSubtractFunction(
    absl::string_view function_name, absl::string_view catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator([function_name](
                                     absl::Span<const googlesql::Value> args)
                                     -> absl::StatusOr<googlesql::Value> {
    GOOGLESQL_RET_CHECK_EQ(args.size(), 2);
    GOOGLESQL_RET_CHECK_EQ(args[0].type(), googlesql::types::IntervalType());
    GOOGLESQL_RET_CHECK_EQ(args[1].type(), googlesql::types::IntervalType());

    if (HasNullValue(args)) {
      return googlesql::Value::NullInterval();
    }

    GOOGLESQL_ASSIGN_OR_RETURN(googlesql::IntervalValue arg0,
                     RoundPrecision(args[0].interval_value()));
    GOOGLESQL_ASSIGN_OR_RETURN(googlesql::IntervalValue arg1,
                     RoundPrecision(args[1].interval_value()));

    absl::StatusOr<googlesql::IntervalValue> result =
        function_name == kPGIntervalAddFunctionName ? arg0 + arg1 : arg0 - arg1;
    GOOGLESQL_RETURN_IF_ERROR(result.status());
    return googlesql::Value::Interval(*result);
  });

  return std::make_unique<googlesql::Function>(
      function_name, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_interval,
                                       {gsql_interval, gsql_interval},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

std::unique_ptr<googlesql::Function> IntervalUnaryMinusFunction(
    absl::string_view catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator([](absl::Span<const googlesql::Value> args)
                                     -> absl::StatusOr<googlesql::Value> {
    GOOGLESQL_RET_CHECK_EQ(args.size(), 1);
    GOOGLESQL_RET_CHECK_EQ(args[0].type(), googlesql::types::IntervalType());
    if (HasNullValue(args)) {
      return googlesql::Value::NullInterval();
    }
    GOOGLESQL_ASSIGN_OR_RETURN(googlesql::IntervalValue arg0,
                     RoundPrecision(args[0].interval_value()));
    return googlesql::Value::Interval(-arg0);
  });

  return std::make_unique<googlesql::Function>(
      kPGIntervalUnaryMinusFunctionName, catalog_name,
      googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_interval,
                                       {gsql_interval},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

std::unique_ptr<googlesql::Function> IntervalMultiplyDivideFunction(
    absl::string_view function_name, absl::string_view catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(
      [function_name](absl::Span<const googlesql::Value> args)
          -> absl::StatusOr<googlesql::Value> {
        GOOGLESQL_RET_CHECK_EQ(args.size(), 2);
        GOOGLESQL_RET_CHECK_EQ(args[0].type(), googlesql::types::IntervalType());
        GOOGLESQL_RET_CHECK_EQ(args[1].type(), googlesql::types::DoubleType());
        if (HasNullValue(args)) {
          return googlesql::Value::NullInterval();
        }
        GOOGLESQL_ASSIGN_OR_RETURN(googlesql::IntervalValue arg0,
                         RoundPrecision(args[0].interval_value()));

        absl::StatusOr<googlesql::IntervalValue> result =
            function_name == kPGIntervalMultiplyFunctionName
                ? PgIntervalMultiply(arg0, args[1].double_value())
                : PgIntervalDivide(arg0, args[1].double_value());
        GOOGLESQL_RETURN_IF_ERROR(result.status());
        return googlesql::Value::Interval(*result);
      }));

  return std::make_unique<googlesql::Function>(
      function_name, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_interval,
                                       {gsql_interval, gsql_double},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

std::unique_ptr<googlesql::Function> TimestamptzSubtractTimestamptzFunction(
    absl::string_view catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(
      PGFunctionEvaluator([](absl::Span<const googlesql::Value> args)
                              -> absl::StatusOr<googlesql::Value> {
        GOOGLESQL_RET_CHECK_EQ(args.size(), 2);
        GOOGLESQL_RET_CHECK_EQ(args[0].type(), googlesql::types::TimestampType());
        GOOGLESQL_RET_CHECK_EQ(args[1].type(), googlesql::types::TimestampType());
        if (HasNullValue(args)) {
          return googlesql::Value::NullInterval();
        }
        absl::Time arg0 =
            absl::FromUnixMicros(absl::ToUnixMicros(
              args[0].ToUnixPicos().ToAbslTime()));
        absl::Time arg1 =
            absl::FromUnixMicros(absl::ToUnixMicros(
              args[1].ToUnixPicos().ToAbslTime()));
        GOOGLESQL_ASSIGN_OR_RETURN(
            googlesql::IntervalValue result,
            googlesql::functions::IntervalDiffTimestamps(arg0, arg1));
        return googlesql::Value::Interval(result);
      }));
  return std::make_unique<googlesql::Function>(
      kPGTimestamptzSubtractTimestamptzFunctionName, catalog_name,
      googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_interval,
                                       {gsql_timestamp, gsql_timestamp},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

std::unique_ptr<googlesql::Function> IntervalJustifyFunction(
    absl::string_view function_name, absl::string_view catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(
      [function_name](absl::Span<const googlesql::Value> args)
          -> absl::StatusOr<googlesql::Value> {
        GOOGLESQL_RET_CHECK_EQ(args.size(), 1);
        GOOGLESQL_RET_CHECK_EQ(args[0].type(), googlesql::types::IntervalType());
        if (HasNullValue(args)) {
          return googlesql::Value::NullInterval();
        }
        GOOGLESQL_ASSIGN_OR_RETURN(googlesql::IntervalValue arg0,
                         RoundPrecision(args[0].interval_value()));
        absl::StatusOr<googlesql::IntervalValue> result =
            function_name == kPGIntervalJustifyIntervalFunctionName
                ? googlesql::JustifyInterval(arg0)
            : function_name == kPGIntervalJustifyDaysFunctionName
                ? googlesql::JustifyDays(arg0)
                : googlesql::JustifyHours(arg0);
        GOOGLESQL_RETURN_IF_ERROR(result.status());
        return googlesql::Value::Interval(*result);
      }));

  return std::make_unique<googlesql::Function>(
      function_name, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_interval,
                                       {gsql_interval},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

std::unique_ptr<googlesql::Function> MakeIntervalFunction(
    absl::string_view catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(
      PGFunctionEvaluator([](absl::Span<const googlesql::Value> args)
                              -> absl::StatusOr<googlesql::Value> {
        GOOGLESQL_RET_CHECK_EQ(args.size(), 7);
        for (int i = 0; i < 6; i++) {
          GOOGLESQL_RET_CHECK_EQ(args[i].type(), googlesql::types::Int64Type());
        }
        GOOGLESQL_RET_CHECK_EQ(args[6].type(), googlesql::types::DoubleType());
        if (HasNullValue(args)) {
          return googlesql::Value::NullInterval();
        }
        GOOGLESQL_ASSIGN_OR_RETURN(
            googlesql::IntervalValue result,
            PgMakeInterval(args[0].int64_value(), args[1].int64_value(),
                           args[2].int64_value(), args[3].int64_value(),
                           args[4].int64_value(), args[5].int64_value(),
                           args[6].double_value()));
        return googlesql::Value::Interval(result);
      }));

  googlesql::FunctionArgumentTypeOptions years =
      GetOptionalNamedArgumentOptions("years");
  googlesql::FunctionArgumentTypeOptions months =
      GetOptionalNamedArgumentOptions("months");
  googlesql::FunctionArgumentTypeOptions weeks =
      GetOptionalNamedArgumentOptions("weeks");
  googlesql::FunctionArgumentTypeOptions days =
      GetOptionalNamedArgumentOptions("days");
  googlesql::FunctionArgumentTypeOptions hours =
      GetOptionalNamedArgumentOptions("hours");
  googlesql::FunctionArgumentTypeOptions mins =
      GetOptionalNamedArgumentOptions("mins");
  googlesql::FunctionArgumentTypeOptions secs =
      GetOptionalNamedArgumentOptions("secs");

  return std::make_unique<googlesql::Function>(
      kPGIntervalMakeIntervalFunctionName, catalog_name,
      googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_interval,
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

std::unique_ptr<googlesql::Function> IntervalExtract(
    absl::string_view catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(
      PGFunctionEvaluator([](absl::Span<const googlesql::Value> args)
                              -> absl::StatusOr<googlesql::Value> {
        GOOGLESQL_RET_CHECK_EQ(args.size(), 2);
        if (HasNullValue(args)) {
          return googlesql::Value::Null(
              spangres::datatypes::GetPgNumericType());
        }
        GOOGLESQL_ASSIGN_OR_RETURN(googlesql::IntervalValue interval_arg,
                         RoundPrecision(args[1].interval_value()));
        GOOGLESQL_ASSIGN_OR_RETURN(
            absl::Cord result,
            PgIntervalExtract(args[0].string_value(), interval_arg));
        return CreatePgNumericValue(std::string(result));
      }));

  static const googlesql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  return std::make_unique<googlesql::Function>(
      kPGIntervalExtractFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_pg_numeric,
                                       {gsql_string, gsql_interval},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

std::unique_ptr<googlesql::Function> CastToIntervalFunction(
    absl::string_view catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(EvalCastStringToInterval));
  return std::make_unique<googlesql::Function>(
      kPGCastToIntervalFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_interval,
                                       {gsql_string},
                                       /*context_ptr=*/nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalGenerateSeries(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 2 || args.size() == 3);

  const googlesql::Type* arg_type = args[0].type();

  if (arg_type->Equals(googlesql::types::Int64Type())) {
    if (args[0].is_null() || args[1].is_null() ||
        (args.size() == 3 && args[2].is_null())) {
      return googlesql::Value::Null(googlesql::types::Int64ArrayType());
    }
    int64_t start = args[0].int64_value();
    int64_t stop = args[1].int64_value();
    int64_t step = 1;
    if (args.size() == 3) {
      step = args[2].int64_value();
    }

    std::vector<int64_t> values;
    GOOGLESQL_RETURN_IF_ERROR((googlesql::functions::GenerateArray<int64_t, int64_t>(
        start, stop, step, &values)));

    std::vector<googlesql::Value> gsql_values;
    gsql_values.reserve(values.size());
    for (int64_t val : values) {
      gsql_values.push_back(googlesql::Value::Int64(val));
    }
    return googlesql::Value::MakeArray(googlesql::types::Int64ArrayType(),
                                       gsql_values);
  } else if (arg_type->Equals(spangres::datatypes::GetPgNumericType())) {
    if (args[0].is_null() || args[1].is_null() ||
        (args.size() == 3 && args[2].is_null())) {
      return googlesql::Value::Null(
          spangres::datatypes::GetPgNumericArrayType());
    }
    // PG.NUMERIC approximation using double
    GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord start_cord,
                     GetPgNumericNormalizedValue(args[0]));
    GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord stop_cord,
                     GetPgNumericNormalizedValue(args[1]));
    double start = 0;
    double stop = 0;
    double step = 1.0;

    GOOGLESQL_RET_CHECK(absl::SimpleAtod(std::string(start_cord), &start))
        << "Failed to parse start as double";
    GOOGLESQL_RET_CHECK(absl::SimpleAtod(std::string(stop_cord), &stop))
        << "Failed to parse stop as double";

    if (args.size() == 3) {
      GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord step_cord,
                       GetPgNumericNormalizedValue(args[2]));
      GOOGLESQL_RET_CHECK(absl::SimpleAtod(std::string(step_cord), &step))
          << "Failed to parse step as double";
    }

    // googlesql::functions::GenerateArray handles step = 0, NaN, and +/-Inf.
    std::vector<double> values;
    GOOGLESQL_RETURN_IF_ERROR((googlesql::functions::GenerateArray<double, double>(
        start, stop, step, &values)));

    std::vector<googlesql::Value> gsql_values;
    gsql_values.reserve(values.size());
    for (double val : values) {
      std::string str_val = absl::StrCat(val);
      GOOGLESQL_ASSIGN_OR_RETURN(googlesql::Value num_val,
                       spangres::datatypes::CreatePgNumericValue(str_val));
      gsql_values.push_back(num_val);
    }
    return googlesql::Value::MakeArray(
        spangres::datatypes::GetPgNumericArrayType(), gsql_values);
  }

  return absl::InvalidArgumentError(
      "Unsupported argument type for generate_series");
}

std::unique_ptr<googlesql::Function> GenerateArrayFunction(
    absl::string_view catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);
  function_options.set_evaluator(PGFunctionEvaluator(EvalGenerateSeries));
  return std::make_unique<googlesql::Function>(
      kPGGenerateArrayFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{
              googlesql::types::Int64ArrayType(),
              {googlesql::types::Int64Type(), googlesql::types::Int64Type()},
              /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              googlesql::types::Int64ArrayType(),
              {googlesql::types::Int64Type(), googlesql::types::Int64Type(),
               googlesql::types::Int64Type()},
              /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              spangres::datatypes::GetPgNumericArrayType(),
              {spangres::datatypes::GetPgNumericType(),
               spangres::datatypes::GetPgNumericType()},
              /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              spangres::datatypes::GetPgNumericArrayType(),
              {spangres::datatypes::GetPgNumericType(),
               spangres::datatypes::GetPgNumericType(),
               spangres::datatypes::GetPgNumericType()},
              /*context_ptr=*/nullptr},
      },
      function_options);
}

SpannerPGFunctions GetSpannerPGFunctions(const std::string& catalog_name) {
  SpannerPGFunctions functions;

  auto generate_array_func = GenerateArrayFunction(catalog_name);
  functions.push_back(std::move(generate_array_func));

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

  auto pg_ilike_func = PgILikeFunction(catalog_name);
  functions.push_back(std::move(pg_ilike_func));

  auto pg_not_ilike_func = PgNotILikeFunction(catalog_name);
  functions.push_back(std::move(pg_not_ilike_func));

  auto pg_min_agg = PgMinAggregator(catalog_name);
  functions.push_back(std::move(pg_min_agg));
  auto min_agg = MinAggregator(catalog_name);
  functions.push_back(std::move(min_agg));
  auto max_agg = MaxAggregator(catalog_name);
  functions.push_back(std::move(max_agg));
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
  auto jsonb_subscript_func = GoogleSQLSubscriptFunction(catalog_name);
  functions.push_back(std::move(jsonb_subscript_func));
  auto jsonb_typeof_func = JsonbTypeofFunction(catalog_name);
  functions.push_back(std::move(jsonb_typeof_func));
  auto jsonb_query_array_func = JsonbQueryArrayFunction(
      catalog_name, kGoogleSQLJsonQueryArrayFunctionName);
  functions.push_back(std::move(jsonb_query_array_func));
  auto jsonb_array_elements_func =
      JsonbQueryArrayFunction(catalog_name, kPGJsonbArrayElementsFunctionName);
  functions.push_back(std::move(jsonb_array_elements_func));
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
  auto jsonb_object_keys_func = JsonbObjectKeysFunction(catalog_name);
  functions.push_back(std::move(jsonb_object_keys_func));

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
  functions.push_back(JsonbFloatArrayExtractionFunction<float>(catalog_name));
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
  // `<> all` is intentionally omitted because it is equivalent to `NOT IN`
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

// TODO: Clean up once usage is removed from mainline.
SpannerPGTVFs GetSpannerPGTVFs(const std::string& catalog_name) {
  SpannerPGTVFs tvfs;
  auto jsonb_array_elements_tvf =
      std::make_unique<JsonbArrayElementsTableValuedFunction>();
  tvfs.push_back(std::move(jsonb_array_elements_tvf));
  return tvfs;
}

}  // namespace postgres_translator
