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

#ifndef UTIL_INTERVAL_HELPERS_H_
#define UTIL_INTERVAL_HELPERS_H_

#include <cstdint>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

struct PGInterval {
  int64_t months;
  int64_t days;
  int64_t micros;
};

// ParseInterval turns a postgres-format interval specifier string, like "2
// days" or "7 weeks, 4 days, 15 minutes - 1 month", into canonical
// representation `PGInterval(months, days and micros)`. This has been modified
// to run in a MemoryContext-less path (DDL translation) and should not be used
// if interval_in can be called directly through spangres/src.
absl::StatusOr<PGInterval> ParseInterval(absl::string_view input_string);

// IntervalToSecs turns a postgres-format interval specifier string, like "2
// days" or "7 weeks, 4 days, 15 minutes - 1 month", into the number of
// seconds it represents.
// Internally uses ParseInterval for canonicalizing into `(months, days and
// micros)`.
absl::StatusOr<int64_t> IntervalToSecs(absl::string_view input_string);

#endif  // UTIL_INTERVAL_HELPERS_H_
