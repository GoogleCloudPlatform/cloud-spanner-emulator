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


#include "third_party/spanner_pg/interface/mathematical_evaluators.h"

#include <cstdint>
#include <limits>
#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "third_party/spanner_pg/src/backend/utils/fmgroids.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator::function_evaluators {

static absl::StatusOr<Datum> NumericIn(
    absl::string_view numeric_decimal_string) {
  std::string numeric_string = std::string(numeric_decimal_string);
  Datum numeric_in_cstring = CStringGetDatum(numeric_string.c_str());
  Datum numeric_in_typelem = ObjectIdGetDatum(NUMERICOID);
  Datum numeric_in_typmod = ObjectIdGetDatum(-1);  // default typmod

  return postgres_translator::CheckedOidFunctionCall3(
      F_NUMERIC_IN, numeric_in_cstring, numeric_in_typelem, numeric_in_typmod);
}

static absl::StatusOr<std::string> NumericOut(Datum numeric_datum) {
  ZETASQL_ASSIGN_OR_RETURN(Datum numeric_out_datum,
                   postgres_translator::CheckedOidFunctionCall1(F_NUMERIC_OUT,
                                                                numeric_datum));

  return postgres_translator::CheckedPgCStringDatumToCString(numeric_out_datum);
}

absl::StatusOr<std::string> Abs(absl::string_view numeric_decimal_string) {
  ZETASQL_ASSIGN_OR_RETURN(Datum numeric_in, NumericIn(numeric_decimal_string));

  ZETASQL_ASSIGN_OR_RETURN(Datum result, postgres_translator::CheckedOidFunctionCall1(
                                     F_ABS_NUMERIC, numeric_in));

  return NumericOut(result);
}

absl::StatusOr<std::string> Add(absl::string_view numeric_decimal_string_1,
                                absl::string_view numeric_decimal_string_2) {
  ZETASQL_ASSIGN_OR_RETURN(Datum numeric_in_1, NumericIn(numeric_decimal_string_1));
  ZETASQL_ASSIGN_OR_RETURN(Datum numeric_in_2, NumericIn(numeric_decimal_string_2));

  ZETASQL_ASSIGN_OR_RETURN(Datum result,
                   postgres_translator::CheckedOidFunctionCall2(
                       F_NUMERIC_ADD, numeric_in_1, numeric_in_2));

  return NumericOut(result);
}

absl::StatusOr<std::string> Ceil(absl::string_view numeric_decimal_string) {
  ZETASQL_ASSIGN_OR_RETURN(Datum numeric_in, NumericIn(numeric_decimal_string));

  ZETASQL_ASSIGN_OR_RETURN(Datum result, postgres_translator::CheckedOidFunctionCall1(
                                     F_CEIL_NUMERIC, numeric_in));

  return NumericOut(result);
}

absl::StatusOr<std::string> Ceiling(absl::string_view numeric_decimal_string) {
  ZETASQL_ASSIGN_OR_RETURN(Datum numeric_in, NumericIn(numeric_decimal_string));

  ZETASQL_ASSIGN_OR_RETURN(Datum result, postgres_translator::CheckedOidFunctionCall1(
                                     F_CEILING_NUMERIC, numeric_in));

  return NumericOut(result);
}

absl::StatusOr<std::string> Divide(absl::string_view numeric_decimal_string_1,
                                   absl::string_view numeric_decimal_string_2) {
  ZETASQL_ASSIGN_OR_RETURN(Datum numeric_in_1, NumericIn(numeric_decimal_string_1));
  ZETASQL_ASSIGN_OR_RETURN(Datum numeric_in_2, NumericIn(numeric_decimal_string_2));

  ZETASQL_ASSIGN_OR_RETURN(Datum result,
                   postgres_translator::CheckedOidFunctionCall2(
                       F_NUMERIC_DIV, numeric_in_1, numeric_in_2));

  return NumericOut(result);
}

absl::StatusOr<std::string> DivideTruncateTowardsZero(
    absl::string_view numeric_decimal_string_1,
    absl::string_view numeric_decimal_string_2) {
  ZETASQL_ASSIGN_OR_RETURN(Datum numeric_in_1, NumericIn(numeric_decimal_string_1));
  ZETASQL_ASSIGN_OR_RETURN(Datum numeric_in_2, NumericIn(numeric_decimal_string_2));

  ZETASQL_ASSIGN_OR_RETURN(Datum result,
                   postgres_translator::CheckedOidFunctionCall2(
                       F_NUMERIC_DIV_TRUNC, numeric_in_1, numeric_in_2));

  return NumericOut(result);
}

absl::StatusOr<std::string> Floor(absl::string_view numeric_decimal_string) {
  ZETASQL_ASSIGN_OR_RETURN(Datum numeric_in, NumericIn(numeric_decimal_string));

  ZETASQL_ASSIGN_OR_RETURN(Datum result, postgres_translator::CheckedOidFunctionCall1(
                                     F_FLOOR_NUMERIC, numeric_in));

  return NumericOut(result);
}

absl::StatusOr<std::string> Mod(absl::string_view numeric_decimal_string_1,
                                absl::string_view numeric_decimal_string_2) {
  ZETASQL_ASSIGN_OR_RETURN(Datum numeric_in_1, NumericIn(numeric_decimal_string_1));
  ZETASQL_ASSIGN_OR_RETURN(Datum numeric_in_2, NumericIn(numeric_decimal_string_2));

  ZETASQL_ASSIGN_OR_RETURN(Datum result,
                   postgres_translator::CheckedOidFunctionCall2(
                       F_MOD_NUMERIC_NUMERIC, numeric_in_1, numeric_in_2));

  return NumericOut(result);
}

absl::StatusOr<std::string> Multiply(
    absl::string_view numeric_decimal_string_1,
    absl::string_view numeric_decimal_string_2) {
  ZETASQL_ASSIGN_OR_RETURN(Datum numeric_in_1, NumericIn(numeric_decimal_string_1));
  ZETASQL_ASSIGN_OR_RETURN(Datum numeric_in_2, NumericIn(numeric_decimal_string_2));

  ZETASQL_ASSIGN_OR_RETURN(Datum result,
                   postgres_translator::CheckedOidFunctionCall2(
                       F_NUMERIC_MUL, numeric_in_1, numeric_in_2));

  return NumericOut(result);
}

absl::StatusOr<std::string> Subtract(
    absl::string_view numeric_decimal_string_1,
    absl::string_view numeric_decimal_string_2) {
  ZETASQL_ASSIGN_OR_RETURN(Datum numeric_in_1, NumericIn(numeric_decimal_string_1));
  ZETASQL_ASSIGN_OR_RETURN(Datum numeric_in_2, NumericIn(numeric_decimal_string_2));

  ZETASQL_ASSIGN_OR_RETURN(Datum result,
                   postgres_translator::CheckedOidFunctionCall2(
                       F_NUMERIC_SUB, numeric_in_1, numeric_in_2));

  return NumericOut(result);
}

absl::StatusOr<std::string> Trunc(absl::string_view numeric_decimal_string,
                                  int64_t scale) {
  if (scale < std::numeric_limits<int32_t>::min() ||
      scale > std::numeric_limits<int32_t>::max()) {
    return absl::InvalidArgumentError(
        absl::StrCat("scale overflow, it must be within ",
                     std::numeric_limits<int32_t>::min(), " and ",
                     std::numeric_limits<int32_t>::max()));
  }

  ZETASQL_ASSIGN_OR_RETURN(Datum numeric_in, NumericIn(numeric_decimal_string));
  Datum scale_in = Int64GetDatum(scale);

  ZETASQL_ASSIGN_OR_RETURN(Datum result,
                   postgres_translator::CheckedOidFunctionCall2(
                       F_TRUNC_NUMERIC_INT4, numeric_in, scale_in));

  return NumericOut(result);
}

absl::StatusOr<std::string> UnaryMinus(
    absl::string_view numeric_decimal_string) {
  ZETASQL_ASSIGN_OR_RETURN(Datum numeric_in, NumericIn(numeric_decimal_string));

  ZETASQL_ASSIGN_OR_RETURN(Datum result, postgres_translator::CheckedOidFunctionCall1(
                                     F_NUMERIC_UMINUS, numeric_in));

  return NumericOut(result);
}

}  // namespace postgres_translator::function_evaluators
