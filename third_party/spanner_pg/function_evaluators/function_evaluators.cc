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

#include "third_party/spanner_pg/function_evaluators/function_evaluators.h"

#include "zetasql/public/functions/date_time_util.h"
#include "absl/strings/str_format.h"
#include "third_party/spanner_pg/datetime_parsing/date.h"
#include "third_party/spanner_pg/datetime_parsing/datetime_exports.h"
#include "third_party/spanner_pg/util/datetime_conversion.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator::function_evaluators {

absl::Status UnsupportedFunctionEvaluator(absl::string_view function_name) {
  return absl::InvalidArgumentError(
      absl::StrFormat("%s function is not supported", function_name));
}

absl::StatusOr<int32_t> DateIn(absl::string_view input_string,
                               absl::string_view default_timezone) {
  ZETASQL_ASSIGN_OR_RETURN(int32_t pg_date, date_in(input_string, default_timezone));

  // Apply an offset to translate between a PG date and a ZetaSQL date.
  int32_t date_val =
      PgDateOffsetToGsqlDateOffset(static_cast<int32_t>(pg_date));
  if (!zetasql::functions::IsValidDate(date_val)) {
    return absl::InvalidArgumentError("Date is out of supported range");
  }
  return date_val;
}

absl::StatusOr<absl::Time> TimestamptzIn(absl::string_view input_string,
                                         absl::string_view default_timezone) {
  ZETASQL_ASSIGN_OR_RETURN(TimestampTz timestamptz_value,
                   timestamptz_in(input_string, default_timezone));
  absl::Time time_val = PgTimestamptzToAbslTime(timestamptz_value);
  if (!zetasql::functions::IsValidTime(time_val)) {
    return absl::InvalidArgumentError("Timestamp is out of supported range");
  }
  return time_val;
}

}  // namespace postgres_translator::function_evaluators
