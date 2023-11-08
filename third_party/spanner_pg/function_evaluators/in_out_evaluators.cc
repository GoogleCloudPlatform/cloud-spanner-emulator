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

#include "third_party/spanner_pg/function_evaluators/in_out_evaluators.h"

#include <cstdint>
#include <string>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "third_party/spanner_pg/src/backend/utils/fmgroids.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator::function_evaluators {

absl::StatusOr<Datum> NumericIn(absl::string_view value) {
  std::string numeric_string = std::string(value);
  Datum numeric_in_cstring = CStringGetDatum(numeric_string.c_str());
  Datum numeric_in_typelem = ObjectIdGetDatum(NUMERICOID);
  Datum numeric_in_typmod = ObjectIdGetDatum(-1);  // default typmod

  return CheckedOidFunctionCall3(F_NUMERIC_IN, numeric_in_cstring,
                                 numeric_in_typelem, numeric_in_typmod);
}

absl::StatusOr<std::string> NumericOut(Datum value) {
  ZETASQL_ASSIGN_OR_RETURN(Datum result, CheckedOidFunctionCall1(F_NUMERIC_OUT, value));

  return CheckedPgCStringDatumToCString(result);
}

}  // namespace postgres_translator::function_evaluators
