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

#include "third_party/spanner_pg/interface/jsonb_evaluators.h"

#include <cstdint>
#include <cstring>
#include <string>

#include "zetasql/public/value.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "third_party/spanner_pg/src/backend/utils/fmgroids.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator::function_evaluators {

static absl::StatusOr<Datum> JsonbIn(absl::string_view jsonb_in) {
  std::string jsonb_string = std::string(jsonb_in);
  Datum jsonb_in_cstring = CStringGetDatum(jsonb_string.c_str());
  return postgres_translator::CheckedNullableOidFunctionCall1(
      F_JSONB_IN, CStringGetDatum(jsonb_in_cstring));
}

static absl::StatusOr<std::string> JsonbOut(Datum jsonb_datum) {
  ZETASQL_ASSIGN_OR_RETURN(
      Datum jsonb_out_datum,
      postgres_translator::CheckedOidFunctionCall1(F_JSONB_OUT, jsonb_datum));
  return postgres_translator::CheckedPgCStringDatumToCString(jsonb_out_datum);
}

absl::StatusOr<zetasql::Value> JsonbArrayElement(
    absl::string_view jsonb_string, int64_t element) {
  if (element < std::numeric_limits<int32_t>::min() ||
      element > std::numeric_limits<int32_t>::max()) {
    return absl::InvalidArgumentError(
        absl::StrCat("element overflow, it must be within ",
                     std::numeric_limits<int32_t>::min(), " and ",
                     std::numeric_limits<int32_t>::max()));
  }

  if (element < 0) {
    return zetasql::Value::Null(
        postgres_translator::spangres::datatypes::GetPgJsonbType());
  }

  Datum element_in = Int64GetDatum(element);
  ZETASQL_ASSIGN_OR_RETURN(Datum jsonb_in, JsonbIn(jsonb_string));

  ZETASQL_ASSIGN_OR_RETURN(Datum jsonb_out,
                   postgres_translator::CheckedNullableOidFunctionCall2(
                       F_JSONB_ARRAY_ELEMENT, jsonb_in, element_in));
  if (jsonb_out == NULL_DATUM) {
    return zetasql::Value::Null(
        postgres_translator::spangres::datatypes::GetPgJsonbType());
  }

  ZETASQL_ASSIGN_OR_RETURN(std::string result, JsonbOut(jsonb_out));

  return postgres_translator::spangres::datatypes::CreatePgJsonbValue(result);
}

absl::StatusOr<zetasql::Value> JsonbObjectField(
    absl::string_view jsonb_string, absl::string_view key) {
  Datum key_in = CStringGetTextDatum(std::string(key).c_str());
  ZETASQL_ASSIGN_OR_RETURN(Datum jsonb_in, JsonbIn(jsonb_string));
  ZETASQL_ASSIGN_OR_RETURN(Datum jsonb_out,
                   postgres_translator::CheckedNullableOidFunctionCall2(
                       F_JSONB_OBJECT_FIELD, jsonb_in, key_in));
  if (jsonb_out == NULL_DATUM) {
    return zetasql::Value::Null(
        postgres_translator::spangres::datatypes::GetPgJsonbType());
  }
  ZETASQL_ASSIGN_OR_RETURN(std::string result, JsonbOut(jsonb_out));
  return postgres_translator::spangres::datatypes::CreatePgJsonbValue(result);
}

absl::StatusOr<zetasql::Value> JsonbTypeof(absl::string_view jsonb_string) {
  ZETASQL_ASSIGN_OR_RETURN(Datum jsonb_in, JsonbIn(jsonb_string));
  ZETASQL_ASSIGN_OR_RETURN(Datum type_out,
                   postgres_translator::CheckedNullableOidFunctionCall1(
                       F_JSONB_TYPEOF, jsonb_in));
  if (type_out == NULL_DATUM) {
    return zetasql::Value::NullString();
  }
  ZETASQL_ASSIGN_OR_RETURN(char* result, CheckedPgTextDatumGetCString(type_out));

  if (strncmp(result, "boolean", 7) == 0) {
    return zetasql::Value::StringValue("bool");
  }

  return zetasql::Value::String(result);
}

}  // namespace postgres_translator::function_evaluators
