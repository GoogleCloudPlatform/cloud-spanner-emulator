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

#include "zetasql/public/interval_value.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
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

absl::StatusOr<absl::Time> PgTimestamptzAdd(absl::Time input_time,
                                            absl::string_view interval_string) {
  return absl::UnimplementedError("invoked stub PgTimestamptzAdd");
}

absl::StatusOr<absl::Time> PgTimestamptzSubtract(
    absl::Time input_time, absl::string_view interval_string) {
  return absl::UnimplementedError("invoked stub PgTimestamptzSubtract");
}

absl::StatusOr<absl::Time> PgTimestamptzBin(absl::string_view stride,
                                            absl::Time source,
                                            absl::Time origin) {
  return absl::UnimplementedError("invoked stub PgTimestamptzBin");
}

absl::StatusOr<absl::Time> PgTimestamptzTrunc(absl::string_view field,
                                              absl::Time source) {
  return absl::UnimplementedError("invoked stub PgTimestamptzTrunc");
}

absl::StatusOr<absl::Time> PgTimestamptzTrunc(absl::string_view field,
                                              absl::Time source,
                                              absl::string_view timezone) {
  return absl::UnimplementedError("invoked stub PgTimestamptzTrunc");
}

absl::StatusOr<absl::Cord> PgTimestamptzExtract(absl::string_view field,
                                                absl::Time source) {
  return absl::UnimplementedError("invoked stub PgTimestamptzExtract");
}

absl::StatusOr<absl::Cord> PgDateExtract(absl::string_view field,
                                         int32_t source) {
  return absl::UnimplementedError("invoked stub PgDateExtract");
}

absl::StatusOr<std::string> PgIntervalOut(
    const zetasql::IntervalValue& interval) {
  return absl::UnimplementedError("invoked stub PgIntervalOut");
}

absl::StatusOr<zetasql::IntervalValue> PgIntervalIn(
    absl::string_view interval_string) {
  return absl::UnimplementedError("invoked stub PgIntervalIn");
}

absl::StatusOr<absl::Time> PgTimestamptzAdd(
    absl::Time input_time, const zetasql::IntervalValue& interval) {
  return absl::UnimplementedError("invoked stub PgTimestamptzAdd");
}

absl::StatusOr<absl::Time> PgTimestamptzSubtract(
    absl::Time input_time, const zetasql::IntervalValue& interval) {
  return absl::UnimplementedError("invoked stub PgTimestamptzSubtract");
}

absl::StatusOr<zetasql::IntervalValue> PgMakeInterval(
    int64_t years, int64_t months, int64_t weeks, int64_t days, int64_t hours,
    int64_t minutes, double seconds) {
  return absl::UnimplementedError("invoked stub PgMakeInterval");
}

absl::StatusOr<zetasql::IntervalValue> PgIntervalMultiply(
    const zetasql::IntervalValue& interval, double multiplier) {
  return absl::UnimplementedError("invoked stub PgIntervalMultiply");
}

absl::StatusOr<zetasql::IntervalValue> PgIntervalDivide(
    const zetasql::IntervalValue& interval, double divisor) {
  return absl::UnimplementedError("invoked stub PgIntervalDivide");
}

absl::StatusOr<absl::Cord> PgIntervalExtract(
    absl::string_view field,
    const zetasql::IntervalValue& interval) {
  return absl::UnimplementedError("invoked stub PgIntervalExtract");
}

absl::StatusOr<std::unique_ptr<std::string>> PgIntervalToChar(
    const zetasql::IntervalValue& interval, absl::string_view format) {
      return absl::UnimplementedError("invoked stub PgIntervalToChar");
}

absl::StatusOr<zetasql::IntervalValue> PgRoundIntervalPrecision(
    const zetasql::IntervalValue& interval) {
  return absl::UnimplementedError("invoked stub PgRoundIntervalPrecision");
}

}  // namespace postgres_translator::function_evaluators
