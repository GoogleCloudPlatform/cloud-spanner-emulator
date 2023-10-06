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

#include "third_party/spanner_pg/datatypes/common/pg_numeric_parse.h"

#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator::spangres::datatypes::common {

absl::StatusOr<std::string> NormalizePgNumeric(absl::string_view pg_numeric) {
  // Create a numeric datum from `readable_value` by indirectly calling PG
  // function `numeric_in`
  // - numeric_in_oid_: the OID value of `numeric_in` function
  // - readable_value: numeric as string
  // - InvalidOid: unused argument
  // - Int32GetDatum(-1): typmod -1 (unlimited/unknown), i.e. numeric without
  // typmod
  ZETASQL_ASSIGN_OR_RETURN(
      Datum numeric_in_result,
      postgres_translator::CheckedOidFunctionCall3(
          F_NUMERIC_IN, CStringGetDatum(std::string(pg_numeric.data()).c_str()),
          ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1)));

  // Create a string datum from numeric datum by indirectly calling PG
  // function `numeric_out`
  // - numeric_out_oid_: the OID value of `numeric_out` function
  // - numeric_in_result: numeric datum
  ZETASQL_ASSIGN_OR_RETURN(Datum numeric_out_result,
                   postgres_translator::CheckedOidFunctionCall1(
                       F_NUMERIC_OUT, numeric_in_result));
  std::string pg_normalized = DatumGetCString(numeric_out_result);

  // Spanner does not support Infinity/-Infinity PG.NUMERIC.
  const char kInfString[] = "Infinity";
  const char kNegInfString[] = "-Infinity";
  ZETASQL_RET_CHECK(pg_normalized != kInfString && pg_normalized != kNegInfString)
          .SetErrorCode(absl::StatusCode::kInvalidArgument)
      << absl::Substitute("Invalid NUMERIC value: $0.", pg_normalized);
  return pg_normalized;
}

}  // namespace postgres_translator::spangres::datatypes::common
