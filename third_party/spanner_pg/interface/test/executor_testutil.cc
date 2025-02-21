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

#include "third_party/spanner_pg/interface/test/executor_testutil.h"

#include <string>
#include <vector>

#include "absl/strings/match.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "util/regexp/re2/re2.h"

namespace postgres_translator {

namespace {

// Spangres and ZetaSQL record parse locations for literals, parameters, and
// a few other Resolved AST nodes. The actually locations may differ due to the
// input SQL strings have slightly different syntax. The only node that must
// have a parse location range is the ResolvedLiteral since the location range
// is used for SQL scrubbing. In these cases we replace a string like
// 'parse_location=9-12' with 'parse_location=??-??'. Otherwise the string is
// removed entirely.
static std::string StripParseLocations(
    absl::string_view original_debug_string) {
  const std::string kParseLocationRegex = "parse_location=\\d+-\\d+(?:, )?";
  std::vector<std::string> lines = absl::StrSplit(original_debug_string, '\n');
  std::vector<std::string> stripped_lines;
  for (auto& line : lines) {
    if (absl::StrContains(line, "Literal(parse_location=")) {
      RE2::GlobalReplace(&line, kParseLocationRegex, "parse_location=??-??");
    } else {
      RE2::GlobalReplace(&line, kParseLocationRegex, "");
    }
    if (!absl::EndsWith(line, "+-")) {
      stripped_lines.push_back(line);
    }
  }
  return absl::StrJoin(stripped_lines, "\n");
}

// The has_explicit_type boolean field on a ZetaSQL literal is not used during
// query execution and is not possible for the Spangres transformer to match
// exactly. This method strips the has_explicit_type field from a resolved AST
// debug string so that we can compare debug strings between Spangres and
// ZetaSQL.
static std::string StripHasExplicitType(
    absl::string_view original_debug_string) {
  const std::string kHasExplicitType = ", has_explicit_type=TRUE";
  std::vector<std::string> lines = absl::StrSplit(original_debug_string, '\n');
  std::vector<std::string> stripped_lines;
  for (auto& line : lines) {
    RE2::GlobalReplace(&line, kHasExplicitType, "");
    stripped_lines.push_back(std::string(line));
  }
  return absl::StrJoin(stripped_lines, "\n");
}

// The is_untyped boolean field on a ZetaSQL parameter is not used during
// query execution and is not possible for the Spangres transformer to match
// exactly. This method strips the is_untyped field from a resolved AST
// debug string so that we can compare debug strings between Spangres and
// ZetaSQL.
static std::string StripIsUntyped(
    absl::string_view original_debug_string) {
  const std::string kHasExplicitType = ", is_untyped=TRUE";
  std::vector<std::string> lines = absl::StrSplit(original_debug_string, '\n');
  std::vector<std::string> stripped_lines;
  for (auto& line : lines) {
    RE2::GlobalReplace(&line, kHasExplicitType, "");
    stripped_lines.push_back(std::string(line));
  }
  return absl::StrJoin(stripped_lines, "\n");
}

// The float_literal_id is not used during query execution and is not possible
// for the Spangres transformer to match exactly. This method strips the
// float_literal_id field  from a resolved AST debug string so that we can
// compare debug strings between Spangres and ZetaSQL.
static std::string StripFloatLiteralId(
    absl::string_view original_debug_string) {
  const std::string kFloatLiteralRegex = ", float_literal_id=\\d+";
  std::vector<std::string> lines = absl::StrSplit(original_debug_string, '\n');
  std::vector<std::string> stripped_lines;
  for (auto& line : lines) {
    RE2::GlobalReplace(&line, kFloatLiteralRegex, "");
    stripped_lines.push_back(std::string(line));
  }
  return absl::StrJoin(stripped_lines, "\n");
}

// TOKENLIST annotations that are added by an annotator on-branch.
// To avoid having changes in the annotator break the golden tests,
// we strip the TOKENLIST annotations from the debug string.
static std::string StripTokenlistAnnotations(
    absl::string_view original_debug_string) {
  // Tokenlist annotations are added by the TokenlistAnnotator with an id of
  // 10001. `*?` does lazy matching to avoid over stripping.
  constexpr absl::string_view kTokenlistAnnotationPattern =
      R"re2((\{10001\:\".*?[^\\]\"\}))re2";
  std::vector<std::string> lines = absl::StrSplit(original_debug_string, '\n');
  for (auto& line : lines) {
    RE2::GlobalReplace(&line, kTokenlistAnnotationPattern,
                       "<scrubbed annotations>");
  }
  return absl::StrJoin(lines, "\n");
}

}  // namespace

// Performs stripping functionality on the AST strings that are written to the
// golden files.
std::string StripPgAstString(absl::string_view pg_ast_string) {
  return StripTokenlistAnnotations(pg_ast_string);
}

std::string StripGsqlAstString(absl::string_view gsql_ast_string) {
  return StripParseLocations(StripTokenlistAnnotations(gsql_ast_string));
}

// Performs stripping functionality on the AST strings before comparing them.
// These transformed strings are not written to the golden files.
std::string StripPgDebugString(absl::string_view pg_debug_string) {
  return StripFloatLiteralId(
      StripIsUntyped(StripHasExplicitType(
          StripParseLocations(pg_debug_string))));
}

std::string StripGsqlDebugString(absl::string_view gsql_debug_string) {
  return StripFloatLiteralId(
      StripIsUntyped(StripHasExplicitType(gsql_debug_string)));
}

}  // namespace postgres_translator
