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

#ifndef FUNCTION_EVALUATORS_DATETIME_EVALUATORS_H_
#define FUNCTION_EVALUATORS_DATETIME_EVALUATORS_H_

#include <cstdint>
#include <memory>
#include <string>

#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"

namespace postgres_translator::function_evaluators {

// Frees the memory for Postgres date/time cache
void CleanupPostgresDateTimeCache();

// Converts string to int32_t date according to the given format. If the given
// format is invalid or the resulting date is out of range, an invalid argument
// error is returned.
absl::StatusOr<int32_t> PgToDate(absl::string_view date_string,
                                 absl::string_view date_format);

// Subtracts a number of days from a date. The resulting date is validated to be
// within the bounds of `::zetasql::types::kDateMin` and
// `::zetasql::types::kDateMax`. If the date is out of bounds an invalid
// argument error will be returned.
absl::StatusOr<int32_t> DateMii(int32_t date, int64_t days);

// Adds a number of days from a date. The resulting date is validated to be
// within the bounds of `::zetasql::types::kDateMin` and
// `::zetasql::types::kDateMax`. If the date is out of bounds an invalid
// argument error will be returned.
absl::StatusOr<int32_t> DatePli(int32_t date, int64_t days);

// Converts string to timestamp according to the given format. If the given
// format is invalid or the resulting timestamp is out of range, an invalid
// argument error is returned.
absl::StatusOr<absl::Time> ToTimestamp(absl::string_view timestamp_string,
                                       absl::string_view timestamp_format);

// Converts timestamp to string according to the given format.
absl::StatusOr<std::unique_ptr<std::string>> PgTimestampTzToChar(
    absl::Time timestamp, absl::string_view format);

// TODO: Remove once on-branch is using PgDateIn
ABSL_DEPRECATED("Use PgDateIn instead")
absl::StatusOr<int32_t> DateIn(absl::string_view input_string,
                               absl::string_view default_timezone);

// TODO: Remove once on-branch is using PgTimestamptzIn
ABSL_DEPRECATED("Use PgTimestamptzIn instead")
absl::StatusOr<absl::Time> TimestamptzIn(absl::string_view input_string,
                                         absl::string_view default_timezone);

// Casts a string to a date.
absl::StatusOr<int32_t> PgDateIn(absl::string_view date_string);

// Casts a string to a timestamptz.
absl::StatusOr<absl::Time> PgTimestamptzIn(absl::string_view timestamp_string);

}  // namespace postgres_translator::function_evaluators

#endif  // FUNCTION_EVALUATORS_DATETIME_EVALUATORS_H_
