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

#include "third_party/spanner_pg/util/datetime_conversion.h"

#include <cstdint>

#include "absl/status/statusor.h"
#include "third_party/spanner_pg/util/integral_helpers.h"

#define INT64CONST(x) (x##L)

#define SECS_PER_DAY 86400
#define USECS_PER_SEC INT64CONST(1000000)

/* Julian-date equivalents of Day 0 in Unix and Postgres reckoning */
#define UNIX_EPOCH_JDATE 2440588     /* == date2j(1970, 1, 1) */
#define POSTGRES_EPOCH_JDATE 2451545 /* == date2j(2000, 1, 1) */

static timeval TimestamptzToTimeVal(TimestampTz timestamptz_value) {
  // Perform the backward conversion.
  timeval tv{0};
  tv.tv_usec = (timestamptz_value % USECS_PER_SEC);
  timestamptz_value = (timestamptz_value - tv.tv_usec) / USECS_PER_SEC;
  tv.tv_sec = timestamptz_value +
              ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);

  return tv;
}

static TimestampTz TimeValToTimestamptz(timeval tv) {
  // Conversion algo is taken from `TimestampTz GetCurrentTimestamp(void)`
  TimestampTz result =
      (TimestampTz)tv.tv_sec -
      ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);
  return (result * USECS_PER_SEC) + tv.tv_usec;
}

absl::Time PgTimestamptzToAbslTime(TimestampTz pg_timestamptz) {
  return absl::TimeFromTimeval(TimestamptzToTimeVal(pg_timestamptz));
}

TimestampTz AbslTimeToPgTimestamptz(const absl::Time gsql_timestamp) {
  return TimeValToTimestamptz(absl::ToTimeval(gsql_timestamp));
}

int32_t PgDateOffsetToGsqlDateOffset(int32_t pg_offset) {
  return pg_offset + (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE);
}

absl::StatusOr<int32_t> SafePgDateOffsetToGsqlDateOffset(int32_t pg_offset) {
  int32_t gsql_offset;
  if (!SafeAdd(pg_offset, (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE),
               &gsql_offset)
           .ok()) {
    return kInvalidDate;
  }
  return gsql_offset;
}

int32_t GsqlDateOffsetToPgDateOffset(int32_t gsql_offset) {
  return gsql_offset - (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE);
}

absl::StatusOr<int32_t> SafeGsqlDateOffsetToPgDateOffset(int32_t gsql_offset) {
  int32_t pg_offset;
  if (!SafeSubtract(gsql_offset, (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE),
                    &pg_offset)
           .ok()) {
    return kInvalidDate;
  }
  return pg_offset;
}
