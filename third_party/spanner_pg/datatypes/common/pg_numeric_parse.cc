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

#include <cstdint>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator::spangres::datatypes::common {

namespace {
// Returns a normalized numeric according to the precision and scale given in
// `typmod_datum`.
absl::StatusOr<std::string> NormalizePgNumeric(absl::string_view readable_value,
                                               int32_t typmod) {
  // Create a numeric datum from `readable_value` by indirectly calling PG
  // function `numeric_in`
  // - readable_value: numeric as string
  // - InvalidOid: unused argument
  // - typmod: precision and scale
  ZETASQL_ASSIGN_OR_RETURN(
      Datum numeric_in_result,
      postgres_translator::CheckedOidFunctionCall3(
          F_NUMERIC_IN,
          CStringGetDatum(std::string(readable_value.data()).c_str()),
          ObjectIdGetDatum(InvalidOid), Int32GetDatum(typmod)));

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

// Returns the typmod value computed from precision and scale.
absl::StatusOr<int32_t> GetTypeModifier(int64_t precision, int64_t scale = 0) {
  // Create an array for precision and scale. Elements must be CSTRING.
  std::string precision_str = absl::StrCat(precision);
  std::string scale_str = absl::StrCat(scale);
  std::vector<Datum> typmod_vector = {CStringGetDatum(precision_str.c_str()),
                                      CStringGetDatum(scale_str.c_str())};

  // Create bool `nulls` array corresponding to `typmod_vector`.
  ZETASQL_ASSIGN_OR_RETURN(void* p, CheckedPgPalloc(2 * sizeof(bool)));
  bool* nulls = reinterpret_cast<bool*>(p);
  nulls[0] = false;
  nulls[1] = false;

  // Create PG array of typmod
  Oid element_typid = CSTRINGOID;
  int16_t elmlen;
  bool elmbyval;
  char elmalign;
  ZETASQL_RETURN_IF_ERROR(CheckedPgGetTyplenbyvalalign(element_typid, &elmlen,
                                               &elmbyval, &elmalign));
  int dims = 2;  // `typmod_vector` dim of 2 elements precision and scale
  int lower_bounds = 1;
  ZETASQL_ASSIGN_OR_RETURN(
      ArrayType * pg_array,
      CheckedPgConstructMdArray(typmod_vector.data(), nulls, /*ndims=*/1, &dims,
                                &lower_bounds, element_typid, elmlen, elmbyval,
                                elmalign));

  // Get int32_t typmod datum
  ZETASQL_ASSIGN_OR_RETURN(Datum numerictypmodin_result,
                   postgres_translator::CheckedOidFunctionCall1(
                       F_NUMERICTYPMODIN, PointerGetDatum(pg_array)));

  return DatumGetInt32(numerictypmodin_result);
}
}  // namespace

absl::StatusOr<std::string> NormalizePgNumeric(
    absl::string_view readable_value) {
  return NormalizePgNumeric(readable_value, /*typmod=*/-1);
}

absl::StatusOr<std::string> NormalizePgNumeric(absl::string_view readable_value,
                                               int64_t precision,
                                               int64_t scale) {
  ZETASQL_ASSIGN_OR_RETURN(int32_t numeric_typmod, GetTypeModifier(precision, scale));

  return NormalizePgNumeric(readable_value, numeric_typmod);
}

absl::StatusOr<bool> ValidatePrecisionAndScale(int64_t precision,
                                               int64_t scale) {
  ZETASQL_RETURN_IF_ERROR(GetTypeModifier(precision, scale).status());
  return true;
}

}  // namespace postgres_translator::spangres::datatypes::common
