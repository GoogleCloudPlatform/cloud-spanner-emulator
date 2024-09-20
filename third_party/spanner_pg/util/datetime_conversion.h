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

#ifndef UTIL_DATETIME_CONVERSION_H_
#define UTIL_DATETIME_CONVERSION_H_

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/time/time.h"

/*
 * A library to convert between PostgreSQL and ZetaSQL datetime
 * representations. This library is used by the Transformer, which links
 * against spangres/src, and also by the function_evaluators library, which
 * links against spangres/datetime_parsing. We've chosen to copy typedef and
 * macro definitions here rather than including spangres/src or
 * spangres/datetime_parsing, which could cause symbol collisions.
 */

typedef int64_t TimestampTz;

const absl::Status kInvalidDate =
    absl::InvalidArgumentError("Date is out of supported range");

const absl::Status kInvalidTimestamp =
    absl::InvalidArgumentError("Timestamp is out of supported range");

const absl::Status kInvalidInterval =
    absl::InvalidArgumentError("Interval is out of supported range");

absl::Time PgTimestamptzToAbslTime(TimestampTz pg_timestamptz);
TimestampTz AbslTimeToPgTimestamptz(const absl::Time gsql_timestamp);

int32_t PgDateOffsetToGsqlDateOffset(int32_t pg_offset);
int32_t GsqlDateOffsetToPgDateOffset(int32_t gsql_offset);

// Converts a pg date into a gsql date. This method is safe, because it returns
// an error in the case of an overflow.
absl::StatusOr<int32_t> SafePgDateOffsetToGsqlDateOffset(int32_t pg_offset);
// Converts a gsql date into a pg date. This method is safe, because it returns
// an error in the case of an overflow.
absl::StatusOr<int32_t> SafeGsqlDateOffsetToPgDateOffset(int32_t gsql_offset);

#endif  // UTIL_DATETIME_CONVERSION_H_
