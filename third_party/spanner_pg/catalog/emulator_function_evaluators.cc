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

#include "third_party/spanner_pg/catalog/emulator_function_evaluators.h"

#include <cstdint>
#include <string>

#include "zetasql/public/function.h"
#include "zetasql/public/value.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/interface/pg_arena.h"
#include "third_party/spanner_pg/interface/pg_arena_factory.h"
#include "third_party/spanner_pg/interface/pg_timezone.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {

void InitializePGTimezoneToDefault() {
  absl::Status status =
      interfaces::InitPGTimezone(kDefaultTimeZone);
  if (!status.ok()) {
    ABSL_LOG(ERROR) << "Failed to initialize PG timezone to default: " << status;
  }
}

zetasql::FunctionEvaluator PGFunctionEvaluator(
    const zetasql::FunctionEvaluator& function,
    const std::function<void()>& on_compute_begin,
    const std::function<void()>& on_compute_end) {
  return [function, on_compute_begin,
          on_compute_end](absl::Span<const zetasql::Value> args)
             -> absl::StatusOr<zetasql::Value> {
    // Binds PG memory context arena to thread local
    ZETASQL_VLOG(1) << "Creating PG arena and Evaluating PG function";
    absl::StatusOr<std::unique_ptr<postgres_translator::interfaces::PGArena>>
        status_or = postgres_translator::interfaces::CreatePGArena(nullptr);
    if (!status_or.ok()) {
      ABSL_LOG(WARNING) << "Tried to create PG arena but failed: "
                   << status_or.status();
    }

    // Call registered function for starting
    on_compute_begin();

    absl::StatusOr<zetasql::Value> result = function(args);

    // Call registered function for cleanup
    on_compute_end();
    // Clean up thread local timezone
    postgres_translator::interfaces::CleanupPGTimezone();

    return result;
  };
}

absl::StatusOr<zetasql::Value> EmulatorJsonbArrayElementText(
    absl::string_view jsonb, int32_t element) {
  ZETASQL_ASSIGN_OR_RETURN(
      Datum jsonb_in_datum,
      postgres_translator::CheckedNullableOidFunctionCall1(
          F_JSONB_IN, CStringGetDatum(std::string(jsonb.data()).c_str())));
  Datum element_in_datum = Int32GetDatum(element);

  // Call `jsonb_array_element_text` on `jsonb_in_datum` and `element_in_datum`.
  ZETASQL_ASSIGN_OR_RETURN(
      Datum result_text_datum,
      postgres_translator::CheckedNullableOidFunctionCall2(
          F_JSONB_ARRAY_ELEMENT_TEXT, jsonb_in_datum, element_in_datum));
  if (result_text_datum == NULL_DATUM) {
    return zetasql::Value::NullString();
  }
  ZETASQL_ASSIGN_OR_RETURN(char* result,
                   CheckedPgTextDatumGetCString(result_text_datum));
  return zetasql::Value::String(result);
}

absl::StatusOr<zetasql::Value> EmulatorJsonbObjectFieldText(
    absl::string_view jsonb, absl::string_view key) {
  ZETASQL_ASSIGN_OR_RETURN(
      Datum jsonb_in_datum,
      postgres_translator::CheckedNullableOidFunctionCall1(
          F_JSONB_IN, CStringGetDatum(std::string(jsonb.data()).c_str())));
  Datum key_in_datum = CStringGetTextDatum(std::string(key.data()).c_str());

  // Call `jsonb_object_field_text` on `jsonb_in_datum` and `key_in_datum`.
  ZETASQL_ASSIGN_OR_RETURN(
      Datum result_text_datum,
      postgres_translator::CheckedNullableOidFunctionCall2(
          F_JSONB_OBJECT_FIELD_TEXT, jsonb_in_datum, key_in_datum));
  if (result_text_datum == NULL_DATUM) {
    return zetasql::Value::NullString();
  }
  ZETASQL_ASSIGN_OR_RETURN(char* result,
                   CheckedPgTextDatumGetCString(result_text_datum));
  return zetasql::Value::String(result);
}

}  // namespace postgres_translator
