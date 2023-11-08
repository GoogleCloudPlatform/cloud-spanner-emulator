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

#ifndef DATATYPES_COMMON_NUMBER_PARSER_H_
#define DATATYPES_COMMON_NUMBER_PARSER_H_

#include <cstdint>
#include <string>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace postgres_translator::spangres::datatypes::common {

// Checks that `readable_value` is a valid PG Numeric value and converts it into
// normalized representation. The normalized representation contains 0 or 1 sign
// character (+/-), digits (0-9), 0 or 1 floating point (.).
absl::StatusOr<std::string> NormalizePgNumeric(
    absl::string_view readable_value);

// Checks that `readable_value` is a valid PG Numeric value and converts it into
// normalized representation according to the `precision` and `scale`. The
// normalized representation contains 0 or 1 sign character (+/-), digits (0-9),
// 0 or 1 floating point (.).
absl::StatusOr<std::string> NormalizePgNumeric(absl::string_view readable_value,
                                               int64_t precision,
                                               int64_t scale = 0);

// Checks that precision and scale form a valid pair of type modifier for
// numeric. It checks if precision in is allowed range [1, 1000] and whether
// scale is in allowed range [0, precision].
absl::StatusOr<bool> ValidatePrecisionAndScale(int64_t precision,
                                               int64_t scale = 0);
}  // namespace postgres_translator::spangres::datatypes::common

#endif  // DATATYPES_COMMON_NUMBER_PARSER_H_
