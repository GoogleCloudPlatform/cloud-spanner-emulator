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
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/function.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "absl/base/casts.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "third_party/spanner_pg/catalog/emulator_function_evaluators.h"
#include "third_party/spanner_pg/function_evaluators/function_evaluators.h"
#include "third_party/spanner_pg/interface/datetime_evaluators.h"
#include "third_party/spanner_pg/interface/formatting_evaluators.h"
#include "third_party/spanner_pg/interface/regexp_evaluators.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {

namespace {

const zetasql::Type* gsql_bool = zetasql::types::BoolType();
const zetasql::Type* gsql_bytes = zetasql::types::BytesType();
const zetasql::Type* gsql_date = zetasql::types::DateType();
const zetasql::Type* gsql_double = zetasql::types::DoubleType();
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

using ::postgres_translator::function_evaluators::CleanupPostgresDateTimeCache;
using ::postgres_translator::function_evaluators::CleanupPostgresNumberCache;
using ::postgres_translator::function_evaluators::CleanupRegexCache;
using ::postgres_translator::function_evaluators::DateMii;
using ::postgres_translator::function_evaluators::DatePli;
using ::postgres_translator::function_evaluators::Float8ToChar;
using ::postgres_translator::function_evaluators::Int8ToChar;
using ::postgres_translator::function_evaluators::PgTimestampTzToChar;
using ::postgres_translator::function_evaluators::PgToDate;
using ::postgres_translator::function_evaluators::RegexpMatch;
using ::postgres_translator::function_evaluators::RegexpSplitToArray;
using ::postgres_translator::function_evaluators::Textregexne;
using ::postgres_translator::function_evaluators::Textregexsubstr;
using ::postgres_translator::function_evaluators::ToTimestamp;

// PG array functions
absl::StatusOr<zetasql::Value> EvalArrayUpper(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 2);

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

std::unique_ptr<zetasql::Function> ArrayUpperFunction(
    absl::string_view catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(zetasql::FunctionEvaluator(EvalArrayUpper));
  return std::make_unique<zetasql::Function>(
      kPGArrayUpperFunctionName, catalog_name, zetasql::Function::SCALAR,
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
  ZETASQL_RET_CHECK(args.size() == 2);

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
    default:
      return absl::UnimplementedError(
          absl::StrCat("to_char(", args[0].type()->DebugString(), ", text)"));
  }
}

std::unique_ptr<zetasql::Function> ToCharFunction(
    absl::string_view catalog_name) {
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
                                       /*context_ptr=*/nullptr}},
      function_options);
}

// PG String functions
absl::StatusOr<zetasql::Value> EvalQuoteIdent(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 1);

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

  ZETASQL_ASSIGN_OR_RETURN(int32_t date, function_evaluators::DateIn(
                                     args[0].string_value(), kDefaultTimeZone));
  return zetasql::Value::Date(date);
}

std::unique_ptr<zetasql::Function> CastToDateFunction(
    const std::string& catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(zetasql::FunctionEvaluator(EvalCastToDate));

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

  ZETASQL_ASSIGN_OR_RETURN(absl::Time time,
                   function_evaluators::TimestamptzIn(args[0].string_value(),
                                                      kDefaultTimeZone));
  return zetasql::Value::Timestamp(time);
}

std::unique_ptr<zetasql::Function> CastToTimestampFunction(
    const std::string& catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      zetasql::FunctionEvaluator(EvalCastToTimestamp));

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
      zetasql::types::DoubleType(),    zetasql::types::Int64Type(),
      zetasql::types::BoolType(),      zetasql::types::BytesType(),
      zetasql::types::StringType(),    zetasql::types::DateType(),
      zetasql::types::TimestampType(),
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

  auto array_upper_func = ArrayUpperFunction(catalog_name);
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

  auto quote_ident_func = QuoteIdentFunction(catalog_name);
  functions.push_back(std::move(quote_ident_func));
  auto regexp_match_func = RegexpMatchFunction(catalog_name);
  functions.push_back(std::move(regexp_match_func));
  auto regexp_split_to_array_func = RegexpSplitToArrayFunction(catalog_name);
  functions.push_back(std::move(regexp_split_to_array_func));
  auto substring_func = SubstringFunction(catalog_name);
  functions.push_back(std::move(substring_func));

  return functions;
}

}  // namespace postgres_translator
