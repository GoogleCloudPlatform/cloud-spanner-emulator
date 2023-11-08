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
#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/interface/mathematical_evaluators.h"

namespace postgres_translator::function_evaluators {

absl::StatusOr<std::string> Abs(absl::string_view numeric_decimal_string) {
  return absl::UnimplementedError("invoked stub Abs");
}

absl::StatusOr<std::string> Add(absl::string_view numeric_decimal_string_1,
                                absl::string_view numeric_decimal_string_2) {
  return absl::UnimplementedError("invoked stub Add");
}

absl::StatusOr<std::string> Ceil(absl::string_view numeric_decimal_string) {
  return absl::UnimplementedError("invoked stub Ceil");
}

absl::StatusOr<std::string> Ceiling(absl::string_view numeric_decimal_string) {
  return absl::UnimplementedError("invoked stub Ceiling");
}

absl::StatusOr<std::string> Divide(absl::string_view numeric_decimal_string_1,
                                   absl::string_view numeric_decimal_string_2) {
  return absl::UnimplementedError("invoked stub Divide");
}

absl::StatusOr<std::string> DivideTruncateTowardsZero(
    absl::string_view numeric_decimal_string_1,
    absl::string_view numeric_decimal_string_2) {
  return absl::UnimplementedError("invoked stub DivideTruncateTowardsZero");
}

absl::StatusOr<std::string> Floor(absl::string_view numeric_decimal_string) {
  return absl::UnimplementedError("invoked stub Floor");
}

absl::StatusOr<std::string> Mod(absl::string_view numeric_decimal_string_1,
                                absl::string_view numeric_decimal_string_2) {
  return absl::UnimplementedError("invoked stub Mod");
}

absl::StatusOr<std::string> Multiply(
    absl::string_view numeric_decimal_string_1,
    absl::string_view numeric_decimal_string_2) {
  return absl::UnimplementedError("invoked stub Multiply");
}

absl::StatusOr<std::string> Subtract(
    absl::string_view numeric_decimal_string_1,
    absl::string_view numeric_decimal_string_2) {
  return absl::UnimplementedError("invoked stub Subtract");
}

absl::StatusOr<std::string> Trunc(absl::string_view numeric_decimal_string,
                                  int64_t scale) {
  return absl::UnimplementedError("invoked stub Trunc");
}

absl::StatusOr<std::string> UnaryMinus(
    absl::string_view numeric_decimal_string) {
  return absl::UnimplementedError("invoked stub UnaryMinus");
}

}  // namespace postgres_translator::function_evaluators
