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

#ifndef INTERFACE_FORMATTING_EVALUATORS_H_
#define INTERFACE_FORMATTING_EVALUATORS_H_

#include <cstdint>
#include <memory>
#include <string>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace postgres_translator::function_evaluators {

void CleanupPostgresNumberCache();

// Converts int64_t to string according to the given format.
absl::StatusOr<std::string> Int8ToChar(int64_t value,
                                       absl::string_view number_format);

// Converts float64 to string according to the given format.
absl::StatusOr<std::string> Float8ToChar(double value,
                                         absl::string_view number_format);

// Converts numeric decimal string to string according to the given format
absl::StatusOr<std::string> NumericToChar(
    absl::string_view numeric_decimal_string, absl::string_view number_format);

// Converts string to numeric according to the given format. This function will
// return `NULL` if an empty string format is given.
absl::StatusOr<std::unique_ptr<std::string>> NumericToNumber(
    absl::string_view value, absl::string_view number_format);
}  // namespace postgres_translator::function_evaluators

#endif  // INTERFACE_FORMATTING_EVALUATORS_H_
