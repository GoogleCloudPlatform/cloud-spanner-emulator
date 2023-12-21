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

#ifndef CATALOG_EMULATOR_FUNCTIONS_H_
#define CATALOG_EMULATOR_FUNCTIONS_H_

// Contains constants and functions for adding Spanner PG functions to the
// emulator catalog for use by the PG to ZetaSQL translator.

#include <memory>
#include <string>
#include <vector>

#include "zetasql/public/function.h"
#include "zetasql/public/value.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"

namespace postgres_translator {

// PG casting functions that override the ZetaSQL/Spanner casting functions.
inline constexpr char kPGCastToDateFunctionName[] = "pg.cast_to_date";
inline constexpr char kPGCastToTimestampFunctionName[] = "pg.cast_to_timestamp";

// Functions to capture NULL/NaN ordering semantics of PG.
inline constexpr char kPGMapDoubleToIntFunctionName[] = "pg.map_double_to_int";
inline constexpr char kPGLeastFunctionName[] = "pg.least";
inline constexpr char kPGGreatestFunctionName[] = "pg.greatest";

// Aggregate functions.
inline constexpr char kPGMinFunctionName[] = "pg.min";
inline constexpr char kPGNumericMaxFunctionName[] = "pg.numeric_max";
inline constexpr char kPGNumericMinFunctionName[] = "pg.numeric_min";
inline constexpr char kPGSumFunctionName[] = "pg.sum";
inline constexpr char kPGAvgFunctionName[] = "pg.avg";

// PG array functions.
inline constexpr char kPGArrayLengthFunctionName[] = "pg.array_length";
inline constexpr char kPGArrayUpperFunctionName[] = "pg.array_upper";

// PG comparison functions.
inline constexpr char kPGTextregexneFunctionName[] = "pg.textregexne";

// PG datetime functions.
inline constexpr char kPGDateMiFunctionName[] = "pg.date_mi";
inline constexpr char kPGDateMiiFunctionName[] = "pg.date_mii";
inline constexpr char kPGDatePliFunctionName[] = "pg.date_pli";
inline constexpr char kPGTimestamptzAddFunctionName[] = "pg.timestamptz_add";
inline constexpr char kPGTimestamptzSubtractFunctionName[] =
    "pg.timestamptz_subtract";
inline constexpr char kPGTimestamptzBinFunctionName[] = "pg.date_bin";
inline constexpr char kPGTimestamptzTruncFunctionName[] = "pg.date_trunc";
inline constexpr char kPGExtractFunctionName[] = "pg.extract";

// PG formatting functions.
inline constexpr char kPGToDateFunctionName[] = "pg.to_date";
inline constexpr char kPGToNumberFunctionName[] = "pg.to_number";
inline constexpr char kPGToTimestampFunctionName[] = "pg.to_timestamp";
inline constexpr char kPGToCharFunctionName[] = "pg.to_char";

// PG mathematical functions.
inline constexpr char kPGNumericAbsFunctionName[] = "pg.numeric_abs";
inline constexpr char kPGNumericCeilFunctionName[] = "pg.numeric_ceil";
inline constexpr char kPGNumericCeilingFunctionName[] = "pg.numeric_ceiling";
inline constexpr char kPGNumericFloorFunctionName[] = "pg.numeric_floor";
inline constexpr char kPGNumericModFunctionName[] = "pg.numeric_mod";
inline constexpr char kPGNumericTruncFunctionName[] = "pg.numeric_trunc";

// PG string functions.
inline constexpr char kPGQuoteIdentFunctionName[] = "pg.quote_ident";
inline constexpr char kPGSubstringFunctionName[] = "pg.substring";
inline constexpr char kPGRegexpMatchFunctionName[] = "pg.regexp_match";
inline constexpr char kPGRegexpSplitToArrayFunctionName[] =
    "pg.regexp_split_to_array";

// PG JSONB functions.
inline constexpr char kPGToJsonBFunctionName[] = "pg.to_jsonb";
inline constexpr char kPGCastFromJsonBFunctionName[] = "pg.cast_from_jsonb";
inline constexpr char kPGJsonBSubscriptTextFunctionName[] =
    "pg.jsonb_subscript_text";
inline constexpr char kPGJsonBArrayElementFunctionName[] =
    "pg.jsonb_array_element";
inline constexpr char kPGJsonBObjectFieldFunctionName[] =
    "pg.jsonb_object_field";
inline constexpr char kPGJsonBTypeofFunctionName[] = "pg.jsonb_typeof";

// PG NUMERIC functions.
inline constexpr char kPGNumericAddFunctionName[] = "pg.numeric_add";
inline constexpr char kPGNumericDivideFunctionName[] = "pg.numeric_divide";
inline constexpr char kPGNumericDivTruncFunctionName[] = "pg.numeric_div_trunc";
inline constexpr char kPGNumericMultiplyFunctionName[] = "pg.numeric_multiply";
inline constexpr char kPGNumericSubtractFunctionName[] = "pg.numeric_subtract";
inline constexpr char kPGNumericUminusFunctionName[] = "pg.numeric_uminus";
inline constexpr char kPGCastToNumericFunctionName[] =
    "pg.cast_to_numeric";
inline constexpr char kPGNumericCastToDoubleFunctionName[] =
    "pg.cast_to_double";
inline constexpr char kPGNumericCastToStringFunctionName[] =
    "pg.cast_to_string";
inline constexpr char kPGNumericCastToInt64FunctionName[] = "pg.cast_to_int64";

// PG NUMERIC operators.
inline constexpr char kPGNumericEqualsFunctionName[] = "pg.numeric_eq";
inline constexpr char kPGNumericNotEqualsFunctionName[] = "pg.numeric_ne";
inline constexpr char kPGNumericLessThanFunctionName[] = "pg.numeric_lt";
inline constexpr char kPGNumericLessThanEqualsFunctionName[] = "pg.numeric_le";
inline constexpr char kPGNumericGreaterThanFunctionName[] = "pg.numeric_gt";
inline constexpr char kPGNumericGreaterThanEqualsFunctionName[] =
    "pg.numeric_ge";

using SpannerPGFunctions = std::vector<std::unique_ptr<zetasql::Function>>;

// Returns Spanner-specific implementations of PG functions.
SpannerPGFunctions GetSpannerPGFunctions(const std::string& catalog_name);

// Evaluators exported for supporting type coercion.
absl::StatusOr<zetasql::Value> EvalNumericCastToInt64(
    absl::Span<const zetasql::Value> args);

absl::StatusOr<zetasql::Value> EvalNumericCastToDouble(
    absl::Span<const zetasql::Value> args);

absl::StatusOr<zetasql::Value> EvalNumericCastToString(
    absl::Span<const zetasql::Value> args);

absl::StatusOr<zetasql::Value> EvalCastToNumeric(
    absl::Span<const zetasql::Value> args);

absl::StatusOr<zetasql::Value> EvalToJsonB(
    absl::Span<const zetasql::Value> args);

}  // namespace postgres_translator

#endif  // CATALOG_EMULATOR_FUNCTIONS_H_
