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

#include "third_party/spanner_pg/interface/regexp_evaluators.h"

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "third_party/spanner_pg/shims/regex_shim.h"
#include "third_party/spanner_pg/src/backend/catalog/pg_type_d.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator::function_evaluators {

inline constexpr Datum kNullDatum = 0;

void CleanupRegexCache() { CleanupCompiledRegexCache(); }

static absl::StatusOr<std::unique_ptr<std::vector<std::string>>>
RegexpSplitToArray(absl::string_view string, absl::string_view pattern,
                   std::optional<absl::string_view> flags) {
  ZETASQL_ASSIGN_OR_RETURN(
      Datum string_in_datum,
      CheckedPgStringToDatum(std::string(string).c_str(), TEXTOID));
  ZETASQL_ASSIGN_OR_RETURN(
      Datum pattern_in_datum,
      CheckedPgStringToDatum(std::string(pattern).c_str(), TEXTOID));

  Datum matches_datum;
  if (flags.has_value()) {
    ZETASQL_ASSIGN_OR_RETURN(
        Datum flags_in_datum,
        CheckedPgStringToDatum(std::string(flags.value()).c_str(), TEXTOID));
    ZETASQL_ASSIGN_OR_RETURN(matches_datum,
                     postgres_translator::CheckedOidFunctionCall3(
                         F_REGEXP_SPLIT_TO_ARRAY, string_in_datum,
                         pattern_in_datum, flags_in_datum));
  } else {
    ZETASQL_ASSIGN_OR_RETURN(
        matches_datum,
        postgres_translator::CheckedOidFunctionCall2(
            F_REGEXP_SPLIT_TO_ARRAY, string_in_datum, pattern_in_datum));
  }

  ZETASQL_ASSIGN_OR_RETURN(ArrayType * matches_array,
                   CheckedPgDatumGetArrayTypeP(matches_datum));

  if (matches_array == nullptr) {
    return absl::InternalError("regex produced null matches");
  }
  if (ARR_NDIM(matches_array) != 1) {
    return absl::InternalError(
        "regex produced unsupported multi-dimensional array of matches");
  }
  if (ARR_LBOUND(matches_array)[0] != 1) {
    return absl::InternalError(
        "regex produced invalid lower bound for matches");
  }
  ZETASQL_ASSIGN_OR_RETURN(ArrayIterator it,
                   CheckedPgArrayCreateIterator(matches_array, /*slice_ndim=*/0,
                                                /*mstate=*/nullptr));
  Datum element;
  bool is_null;
  auto result = std::make_unique<std::vector<std::string>>();
  ZETASQL_ASSIGN_OR_RETURN(bool has_next,
                   CheckedPgArrayIterate(it, &element, &is_null));
  while (has_next) {
    if (is_null) {
      return absl::InternalError("null match when trying to split array");
    }
    ZETASQL_ASSIGN_OR_RETURN(char* value, CheckedPgTextDatumGetCString(element));
    result->push_back(value);

    ZETASQL_ASSIGN_OR_RETURN(has_next, CheckedPgArrayIterate(it, &element, &is_null));
  }
  return result;
}

absl::StatusOr<std::unique_ptr<std::vector<std::string>>> RegexpSplitToArray(
    absl::string_view string, absl::string_view pattern) {
  return RegexpSplitToArray(string, pattern, /*flags=*/std::nullopt);
}

absl::StatusOr<std::unique_ptr<std::vector<std::string>>> RegexpSplitToArray(
    absl::string_view string, absl::string_view pattern,
    absl::string_view flags) {
  return RegexpSplitToArray(string, pattern, std::make_optional(flags));
}

static absl::StatusOr<std::unique_ptr<std::vector<std::optional<std::string>>>>
RegexpMatch(absl::string_view string, absl::string_view pattern,
            std::optional<absl::string_view> flags) {
  ZETASQL_ASSIGN_OR_RETURN(
      Datum string_in_datum,
      CheckedPgStringToDatum(std::string(string).c_str(), TEXTOID));
  ZETASQL_ASSIGN_OR_RETURN(
      Datum pattern_in_datum,
      CheckedPgStringToDatum(std::string(pattern).c_str(), TEXTOID));

  Datum matches_datum;
  if (flags.has_value()) {
    ZETASQL_ASSIGN_OR_RETURN(
        Datum flags_in_datum,
        CheckedPgStringToDatum(std::string(flags.value()).c_str(), TEXTOID));
    ZETASQL_ASSIGN_OR_RETURN(
        matches_datum,
        postgres_translator::CheckedNullableOidFunctionCall3(
            F_REGEXP_MATCH, string_in_datum, pattern_in_datum, flags_in_datum));
  } else {
    ZETASQL_ASSIGN_OR_RETURN(
        matches_datum,
        postgres_translator::CheckedNullableOidFunctionCall2(
            F_REGEXP_MATCH_NO_FLAGS, string_in_datum, pattern_in_datum));
  }
  if (matches_datum == kNullDatum) {
    return nullptr;
  }

  ZETASQL_ASSIGN_OR_RETURN(ArrayType * matches_array,
                   CheckedPgDatumGetArrayTypeP(matches_datum));

  if (ARR_NDIM(matches_array) != 1) {
    return absl::InternalError(
        "regex produced unsupported multi-dimensional array of matches");
  }
  if (ARR_LBOUND(matches_array)[0] != 1) {
    return absl::InternalError(
        "regex produced invalid lower bound for matches");
  }
  ZETASQL_ASSIGN_OR_RETURN(ArrayIterator it,
                   CheckedPgArrayCreateIterator(matches_array, /*slice_ndim=*/0,
                                                /*mstate=*/nullptr));
  Datum element;
  bool is_null;
  auto result = std::make_unique<std::vector<std::optional<std::string>>>();
  ZETASQL_ASSIGN_OR_RETURN(bool has_next,
                   CheckedPgArrayIterate(it, &element, &is_null));
  while (has_next) {
    if (is_null) {
      result->push_back(std::nullopt);
    } else {
      ZETASQL_ASSIGN_OR_RETURN(char* value, CheckedPgTextDatumGetCString(element));
      result->push_back(std::make_optional<std::string>(value));
    }

    ZETASQL_ASSIGN_OR_RETURN(has_next, CheckedPgArrayIterate(it, &element, &is_null));
  }
  return result;
}

absl::StatusOr<std::unique_ptr<std::vector<std::optional<std::string>>>>
RegexpMatch(absl::string_view string, absl::string_view pattern) {
  return RegexpMatch(string, pattern, /*flags=*/std::nullopt);
}

absl::StatusOr<std::unique_ptr<std::vector<std::optional<std::string>>>>
RegexpMatch(absl::string_view string, absl::string_view pattern,
            absl::string_view flags) {
  return RegexpMatch(string, pattern, std::make_optional(flags));
}

absl::StatusOr<bool> Textregexeq(absl::string_view string,
                                 absl::string_view pattern) {
  ZETASQL_ASSIGN_OR_RETURN(
      Datum string_in_datum,
      CheckedPgStringToDatum(std::string(string).c_str(), TEXTOID));
  ZETASQL_ASSIGN_OR_RETURN(
      Datum pattern_in_datum,
      CheckedPgStringToDatum(std::string(pattern).c_str(), TEXTOID));

  ZETASQL_ASSIGN_OR_RETURN(Datum result_datum,
                   postgres_translator::CheckedOidFunctionCall2(
                       F_TEXTREGEXEQ, string_in_datum, pattern_in_datum));

  return DatumGetBool(result_datum);
}

absl::StatusOr<bool> Textregexne(absl::string_view string,
                                 absl::string_view pattern) {
  ZETASQL_ASSIGN_OR_RETURN(
      Datum string_in_datum,
      CheckedPgStringToDatum(std::string(string).c_str(), TEXTOID));
  ZETASQL_ASSIGN_OR_RETURN(
      Datum pattern_in_datum,
      CheckedPgStringToDatum(std::string(pattern).c_str(), TEXTOID));

  // The PostgreSQL function returns true if there was no match
  ZETASQL_ASSIGN_OR_RETURN(Datum is_no_match,
                   postgres_translator::CheckedOidFunctionCall2(
                       F_TEXTREGEXNE, string_in_datum, pattern_in_datum));

  return DatumGetBool(is_no_match);
}

absl::StatusOr<std::unique_ptr<std::string>> Textregexsubstr(
    absl::string_view string, absl::string_view pattern) {
  ZETASQL_ASSIGN_OR_RETURN(
      Datum string_in_datum,
      CheckedPgStringToDatum(std::string(string).c_str(), TEXTOID));
  ZETASQL_ASSIGN_OR_RETURN(
      Datum pattern_in_datum,
      CheckedPgStringToDatum(std::string(pattern).c_str(), TEXTOID));

  ZETASQL_ASSIGN_OR_RETURN(Datum substring_datum,
                   postgres_translator::CheckedNullableOidFunctionCall2(
                       F_TEXTREGEXSUBSTR, string_in_datum, pattern_in_datum));

  if (substring_datum == kNullDatum) {
    return nullptr;
  }
  ZETASQL_ASSIGN_OR_RETURN(char* substring,
                   CheckedPgTextDatumGetCString(substring_datum));
  return std::make_unique<std::string>(substring);
}

absl::StatusOr<std::string> Textregexreplace(
    absl::string_view source, absl::string_view pattern,
    absl::string_view replacement, std::optional<absl::string_view> flags) {
  ZETASQL_ASSIGN_OR_RETURN(
      Datum source_in_datum,
      CheckedPgStringToDatum(std::string(source).c_str(), TEXTOID));
  ZETASQL_ASSIGN_OR_RETURN(
      Datum pattern_in_datum,
      CheckedPgStringToDatum(std::string(pattern).c_str(), TEXTOID));
  ZETASQL_ASSIGN_OR_RETURN(
      Datum replacement_in_datum,
      CheckedPgStringToDatum(std::string(replacement).c_str(), TEXTOID));

  Datum result_datum;
  if (flags) {
    ZETASQL_ASSIGN_OR_RETURN(
        Datum flags_in_datum,
        CheckedPgStringToDatum(std::string(flags.value()).c_str(), TEXTOID));
    ZETASQL_ASSIGN_OR_RETURN(result_datum,
                     postgres_translator::CheckedOidFunctionCall4(
                         F_TEXTREGEXREPLACE, source_in_datum, pattern_in_datum,
                         replacement_in_datum, flags_in_datum));
  } else {
    ZETASQL_ASSIGN_OR_RETURN(result_datum,
                     postgres_translator::CheckedOidFunctionCall3(
                         F_TEXTREGEXREPLACE_NOOPT, source_in_datum,
                         pattern_in_datum, replacement_in_datum));
  }
  return CheckedPgTextDatumGetCString(result_datum);
}

}  // namespace postgres_translator::function_evaluators
