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

#include "absl/status/status.h"
#include "third_party/spanner_pg/interface/formatting_evaluators.h"

namespace postgres_translator::function_evaluators {

void CleanupPostgresNumberCache() {}

absl::StatusOr<std::string> Int8ToChar(int64_t value,
                                       absl::string_view number_format) {
  return absl::UnimplementedError("invoked stub Int8ToChar");
}

absl::StatusOr<std::string> Float8ToChar(double value,
                                         absl::string_view number_format) {
  return absl::UnimplementedError("invoked stub Float8ToChar");
}

absl::StatusOr<std::string> NumericToChar(
    absl::string_view numeric_encoded_value, absl::string_view number_format) {
  return absl::UnimplementedError("invoked stub NumericToChar");
}

absl::StatusOr<std::unique_ptr<std::string>> NumericToNumber(
    absl::string_view value, absl::string_view number_format) {
  return absl::UnimplementedError("invoked stub NumericToNumber");
}

}  // namespace postgres_translator::function_evaluators
