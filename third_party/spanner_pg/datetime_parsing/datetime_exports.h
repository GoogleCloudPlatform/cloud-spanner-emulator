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

#ifndef DATETIME_PARSING_TIMESTAMP_C_EXPORTS_H_
#define DATETIME_PARSING_TIMESTAMP_C_EXPORTS_H_

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

// This header exists to make the public APIs of this datetime library
// consumable by both the executors and the PostgreSQL analyzer (via cc
// wrappers). Dependencies and includes from either PostgreSQL or the duplicated
// versions in this library are intentionally avoided here to avoid conflicts.

/* SPANNER_PG: remove support for typelem and typmod, add a default timezone */
typedef int64_t TimestampTz;
absl::StatusOr<TimestampTz> timestamptz_in(absl::string_view input_string,
                                           absl::string_view default_timezone);

/* SPANNER_PG: add a default timezone */
typedef int32_t DateADT;
absl::StatusOr<DateADT> date_in(absl::string_view input_string,
                                absl::string_view default_timezone);

/*
 * SPANNER_PG: remove support for typelem and typmod and use an output parameter
 * so that the memory management is handled by the caller
*/
typedef struct IntervalS Interval;
absl::Status interval_in(absl::string_view input_string, Interval* result);

#define INT64CONST(x)  (x##L)
#define USECS_PER_SEC	INT64CONST(1000000)

#endif  // DATETIME_PARSING_TIMESTAMP_C_EXPORTS_H_
