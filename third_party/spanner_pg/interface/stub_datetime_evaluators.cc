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

#include <cstdint>
#include <memory>
#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "third_party/spanner_pg/interface/datetime_evaluators.h"

namespace postgres_translator::function_evaluators {

void CleanupPostgresDateTimeCache() {}

absl::StatusOr<int32_t> PgToDate(absl::string_view date_string,
                                 absl::string_view date_format) {
  return absl::UnimplementedError("invoked stub ToDate");
}

absl::StatusOr<int32_t> DateMii(int32_t date, int64_t days) {
  return absl::UnimplementedError("invoked stub DateMii");
}

absl::StatusOr<int32_t> DatePli(int32_t date, int64_t days) {
  return absl::UnimplementedError("invoked stub DatePli");
}

absl::StatusOr<absl::Time> ToTimestamp(absl::string_view timestamp_string,
                                       absl::string_view timestamp_format) {
  return absl::UnimplementedError("invoked stub ToTimestamp");
}

absl::StatusOr<std::unique_ptr<std::string>> PgTimestampTzToChar(
    absl::Time timestamp, absl::string_view format) {
  return absl::UnimplementedError("invoked stub TimestampTzToChar");
}

absl::StatusOr<std::string> TimestampTzToChar(absl::Time timestamp,
                                              absl::string_view format) {
  return absl::UnimplementedError("invoked stub TimestampTzToChar");
}

absl::StatusOr<int32_t> PgDateIn(absl::string_view date_string) {
  return absl::UnimplementedError("invoked stub PgDateIn");
}

absl::StatusOr<absl::Time> PgTimestamptzIn(absl::string_view timestamp_string) {
  return absl::UnimplementedError("invoked stub PgTimestamptzIn");
}

}  // namespace postgres_translator::function_evaluators
