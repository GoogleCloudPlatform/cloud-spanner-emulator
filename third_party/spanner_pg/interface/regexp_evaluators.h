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

#ifndef INTERFACE_REGEXP_EVALUATORS_H_
#define INTERFACE_REGEXP_EVALUATORS_H_

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace postgres_translator::function_evaluators {

// Frees the memory for the regex cache.
void CleanupRegexCache();

// Returns string matches after splitting the input `string` by the given regex
// `pattern`. If the `pattern` does not match anything within the input
// `string`, the input `string` is returned as a single element in the output
// vector.
absl::StatusOr<std::unique_ptr<std::vector<std::string>>> RegexpSplitToArray(
    absl::string_view string, absl::string_view pattern);

// Returns string matches after splitting the input `string` by the given regex
// `pattern` and regex `flags`. If the `pattern` does not match anything
// within the input `string`, the input `string` is returned as a single element
// in the output vector. If invalid flags are given, an error is returned.
absl::StatusOr<std::unique_ptr<std::vector<std::string>>> RegexpSplitToArray(
    absl::string_view string, absl::string_view pattern,
    absl::string_view flags);

// Returns the matches on the given `string` for the given regex `pattern`. If
// there are no matches, `NULL` is returned. If a match is found, and the
// `pattern` contains no parenthesized subexpressions, then the result is a
// single-element array containing the substring matching the whole pattern.
// If a match is found and the pattern contains parenthesized subexpressions,
// then the result is an array whose `n`th element is the substring matching the
// `n`th parenthesized subexpression of the `pattern` (not counting
// non-capturing groups).
absl::StatusOr<std::unique_ptr<std::vector<std::optional<std::string>>>>
RegexpMatch(absl::string_view string, absl::string_view pattern);

// Returns the matches on the given `string` for the given regex `pattern` and
// `flags`. The `flags` parameter should contain zero or more single-letter
// flags that change the function's behavior. Supported flags are desribed
// [here](https://www.postgresql.org/docs/current/functions-matching.html#POSIX-EMBEDDED-OPTIONS-TABLE).
// If there are no matches, `NULL` is returned. If a match is found, and the
// `pattern` contains no parenthesized subexpressions, then the result is a
// single-element array containing the substring matching the whole pattern. If
// a match is found and the pattern contains parenthesized subexpressions, then
// the result is an array whose `n`th element is the substring matching the
// `n`th parenthesized subexpression of the `pattern` (not counting
// non-capturing groups).
absl::StatusOr<std::unique_ptr<std::vector<std::optional<std::string>>>>
RegexpMatch(absl::string_view string, absl::string_view pattern,
            absl::string_view flags);

// Returns `true` if the given `string` matches the given regex `pattern`.
// Returns `false` otherwise.
absl::StatusOr<bool> Textregexeq(absl::string_view string,
                                 absl::string_view pattern);

// Returns `true` if the given `string` does NOT match the given regex
// `pattern`. Returns `false` otherwise.
absl::StatusOr<bool> Textregexne(absl::string_view string,
                                 absl::string_view pattern);

// Returns the substring matching the POSIX regular expression given. If there
// are no matches, `NULL` is returned.
absl::StatusOr<std::unique_ptr<std::string>> Textregexsubstr(
    absl::string_view string, absl::string_view pattern);

// Provides substitution of new text for substrings that match POSIX regular
// expression patterns. The `source` string is returned unchanged if there is no
// match to the `pattern`. If there is a match, the `source` string is returned
// with the `replacement` string substritued for the matching substring. The
// `replacement` string can contain `\n`, where `n` is 1 through 9, to indicate
// that the source substring matching the `n`th parenthesized subexpression of
// the pattern should be inserted, and it can contain `\&` to indicate that the
// substring matching the entire pattern should be inserted. Write `\\` if you
// need to put a literal backslash in the replacement text. `pattern` is
// searched for in `string` from the beginning of the string.  Only the first
// match of the pattern is replaced by default. `flags` can be given in as well
// (described in
// https://www.postgresql.org/docs/current/functions-matching.html#POSIX-EMBEDDED-OPTIONS-TABLE).
// If the `g` flag is given all the matches of the pattern are replaced instead
// of just the first one.
absl::StatusOr<std::string> Textregexreplace(
    absl::string_view source, absl::string_view pattern,
    absl::string_view replacement,
    std::optional<absl::string_view> flags = std::nullopt);
}  // namespace postgres_translator::function_evaluators
#endif  // INTERFACE_REGEXP_EVALUATORS_H_
