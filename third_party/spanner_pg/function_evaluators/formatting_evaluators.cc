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

#include "third_party/spanner_pg/interface/formatting_evaluators.h"

#include <memory>
#include <string>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator::function_evaluators {

void CleanupPostgresNumberCache() { CleanupNumberCache(); }

absl::StatusOr<std::string> Int8ToChar(int64_t value,
                                       absl::string_view number_format) {
  Datum value_in_datum = Int64GetDatum(value);
  ZETASQL_ASSIGN_OR_RETURN(
      Datum number_format_in_datum,
      CheckedPgStringToDatum(std::string(number_format).c_str(), TEXTOID));

  ZETASQL_ASSIGN_OR_RETURN(Datum formatted_number_datum,
                   postgres_translator::CheckedOidFunctionCall2(
                       F_INT8_TO_CHAR, value_in_datum, number_format_in_datum));

  return CheckedPgTextDatumGetCString(formatted_number_datum);
}

absl::StatusOr<std::string> Float8ToChar(double value,
                                         absl::string_view number_format) {
  Datum value_in_datum = Float8GetDatum(value);
  ZETASQL_ASSIGN_OR_RETURN(
      Datum number_format_in_datum,
      CheckedPgStringToDatum(std::string(number_format).c_str(), TEXTOID));

  ZETASQL_ASSIGN_OR_RETURN(
      Datum formatted_number_datum,
      postgres_translator::CheckedOidFunctionCall2(
          F_FLOAT8_TO_CHAR, value_in_datum, number_format_in_datum));

  return CheckedPgTextDatumGetCString(formatted_number_datum);
}

absl::StatusOr<std::string> NumericToChar(
    absl::string_view numeric_decimal_string, absl::string_view number_format) {
  std::string numeric_string = std::string(numeric_decimal_string);
  Datum numeric_in_cstring = CStringGetDatum(numeric_string.c_str());
  Datum numeric_in_typelem = ObjectIdGetDatum(NUMERICOID);
  Datum numeric_in_typmod = ObjectIdGetDatum(-1);  // default typmod
  ZETASQL_ASSIGN_OR_RETURN(Datum numeric_in_datum,
                   postgres_translator::CheckedOidFunctionCall3(
                       F_NUMERIC_IN, numeric_in_cstring, numeric_in_typelem,
                       numeric_in_typmod));
  ZETASQL_ASSIGN_OR_RETURN(
      Datum number_format_in_datum,
      CheckedPgStringToDatum(std::string(number_format).c_str(), TEXTOID));

  ZETASQL_ASSIGN_OR_RETURN(
      Datum formatted_number_datum,
      postgres_translator::CheckedOidFunctionCall2(
          F_NUMERIC_TO_CHAR, numeric_in_datum, number_format_in_datum));

  return CheckedPgTextDatumGetCString(formatted_number_datum);
}

absl::StatusOr<std::unique_ptr<std::string>> NumericToNumber(
    absl::string_view value, absl::string_view number_format) {
  ZETASQL_ASSIGN_OR_RETURN(Datum value_in_datum,
                   CheckedPgStringToDatum(std::string(value).c_str(), TEXTOID));
  ZETASQL_ASSIGN_OR_RETURN(
      Datum number_format_in_datum,
      CheckedPgStringToDatum(std::string(number_format).c_str(), TEXTOID));

  ZETASQL_ASSIGN_OR_RETURN(
      Datum result_numeric_datum,
      postgres_translator::CheckedNullableOidFunctionCall2(
          F_NUMERIC_TO_NUMBER, value_in_datum, number_format_in_datum));

  if (result_numeric_datum == (Datum)NULL) return nullptr;

  ZETASQL_ASSIGN_OR_RETURN(Datum result_cstring_datum,
                   postgres_translator::CheckedOidFunctionCall1(
                       F_NUMERIC_OUT, result_numeric_datum));
  return std::make_unique<std::string>(DatumGetCString(result_cstring_datum));
}

}  // namespace postgres_translator::function_evaluators
