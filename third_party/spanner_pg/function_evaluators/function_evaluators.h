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

#ifndef FUNCTION_EVALUATORS_FUNCTION_EVALUATORS_H_
#define FUNCTION_EVALUATORS_FUNCTION_EVALUATORS_H_

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"

namespace postgres_translator::function_evaluators {

// A placeholder which always returns an unsupported error.
// Allows the on-branch evaluators to depend on this library while the real
// functions are being implemented, and assists in on-branch/off-branch
// development synchronization.
absl::Status UnsupportedFunctionEvaluator(absl::string_view function_name);

// A mirrored implementation of PostgreSQL's date_in function that is safe to
// run during Spanner execution. Returns an int32_t which represents the ZetaSQL
// date offset. Input is an std::string to ensure NULL-termination.
absl::StatusOr<int32_t> DateIn(absl::string_view input_string,
                               absl::string_view default_timezone);

// A mirrored implementation of PostgreSQL's timestamptz_in function that is
// safe to run during Spanner execution. Returns an absl::Time, which is the
// internal representation of a ZetaSQL Timestamp value. Input is an
// std::string to ensure NULL-termination.
absl::StatusOr<absl::Time> TimestamptzIn(absl::string_view input_string,
                                         absl::string_view default_timezone);

}  // namespace postgres_translator::function_evaluators

#endif  // FUNCTION_EVALUATORS_FUNCTION_EVALUATORS_H_
