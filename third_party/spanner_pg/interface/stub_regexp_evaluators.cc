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
#include "third_party/spanner_pg/interface/regexp_evaluators.h"

namespace postgres_translator::function_evaluators {

void CleanupRegexCache() {}

absl::StatusOr<std::unique_ptr<std::vector<std::string>>> RegexpSplitToArray(
    absl::string_view string, absl::string_view pattern) {
  return absl::UnimplementedError("invoked stub RegexpSplitToArray");
}

absl::StatusOr<std::unique_ptr<std::vector<std::string>>> RegexpSplitToArray(
    absl::string_view string, absl::string_view pattern,
    absl::string_view flags) {
  return absl::UnimplementedError("invoked stub RegexpSplitToArray");
}

absl::StatusOr<std::unique_ptr<std::vector<std::optional<std::string>>>>
RegexpMatch(absl::string_view string, absl::string_view pattern) {
  return absl::UnimplementedError("invoked stub RegexpMatch");
}

absl::StatusOr<std::unique_ptr<std::vector<std::optional<std::string>>>>
RegexpMatch(absl::string_view string, absl::string_view pattern,
            absl::string_view flags) {
  return absl::UnimplementedError("invoked stub RegexpMatch");
}

absl::StatusOr<bool> Textregexeq(absl::string_view string,
                                 absl::string_view pattern) {
  return absl::UnimplementedError("invoked stub Textregexeq");
}

absl::StatusOr<bool> Textregexne(absl::string_view string,
                                 absl::string_view pattern) {
  return absl::UnimplementedError("invoked stub Textregexne");
}

absl::StatusOr<std::unique_ptr<std::string>> Textregexsubstr(
    absl::string_view string, absl::string_view pattern) {
  return absl::UnimplementedError("invoked stub Textregexsubstr");
}

absl::StatusOr<std::string> Textregexreplace(
    absl::string_view source, absl::string_view pattern,
    absl::string_view replacement, std::optional<absl::string_view> flags) {
  return absl::UnimplementedError("invoked stub Textregexreplace");
}

}  // namespace postgres_translator::function_evaluators
