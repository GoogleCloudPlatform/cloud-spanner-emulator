//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "backend/query/search/snippet_evaluator.h"

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/value.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "backend/query/search/tokenizer.h"
#include "common/errors.h"
#include "nlohmann/json_fwd.hpp"
#include "nlohmann/json.hpp"
#include "re2/re2.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

namespace {

using const_iterator = absl::string_view::const_iterator;
using JSON = ::nlohmann::json;

static const char kSnippets[] = "snippets";
static const char kSnippet[] = "snippet";
static const char kHighLights[] = "highlights";
static const char kStartPosition[] = "start_position";
static const char kEndPosition[] = "end_position";

static const char kValidContentType[] = "text/html";

}  // namespace

std::vector<std::pair<const_iterator, const_iterator>>
SnippetEvaluator::MatchSnippets(absl::string_view target,
                                absl::string_view pattern,
                                int max_snippet_length,
                                const_iterator* first_match) {
  std::vector<std::pair<const_iterator, const_iterator>> matches;

  RE2 regex(pattern);
  std::string match;
  std::string snippet;

  bool match_found = false;
  while (RE2::FindAndConsume(&target, regex, &match)) {
    const_iterator cur_pos = target.cbegin();
    if (!match_found) {
      *first_match = cur_pos - match.length();
      match_found = true;
    }

    if (cur_pos - *first_match >= max_snippet_length) {
      break;
    }

    matches.emplace_back(cur_pos - match.length(), cur_pos);
  }

  return matches;
}

const_iterator SnippetEvaluator::GetMatchEnd(absl::string_view target,
                                             const_iterator first_match,
                                             int max_snippet_length) {
  const_iterator end_pos = target.cend();
  if (end_pos - first_match >= max_snippet_length) {
    end_pos = first_match + max_snippet_length - 1;
    // This may land in the middle of a word. Move the pointer backward to find
    // the end of the previous word.
    while (end_pos != first_match && isalnum(*end_pos)) {
      // Find the first non-alnum char.
      end_pos--;
    }
    while (end_pos != first_match && !isalnum(*end_pos)) {
      // Find the first alnum char, that's the end of the snippet.
      end_pos--;
    }

    // Now the end_pos points to the last char of the last word. Return the
    // position after it.
    if (end_pos != first_match) {
      ++end_pos;
    }
  }
  return end_pos;
}

const_iterator SnippetEvaluator::GetMatchBegin(absl::string_view target,
                                               const_iterator first_match,
                                               const_iterator match_end,
                                               int max_snippet_length) {
  const_iterator start_pos = target.cbegin();
  if (match_end - start_pos > max_snippet_length) {
    start_pos = match_end - max_snippet_length;
    // Since we ensure max_snippet_length must be longer then any term's length,
    // start_pos must be at a position before first_match.
    while (isalnum(*start_pos)) {
      // Find the first non-alnum char.
      ++start_pos;
    }
    while (!isalnum(*start_pos)) {
      // Find the first alnum char, that's the begining of the snippet. Since
      // first_match points to the mathcing term, the start_pos will not go
      // past first_match.
      start_pos++;
    }

    // No need to adjust start_pos since the append uses [start_pos, end_pos)
  }
  return start_pos;
}

absl::StatusOr<std::string> SnippetEvaluator::BuildSnippets(
    absl::string_view target, absl::string_view query, int max_snippet_length,
    int max_snippets) {
  // We only return one snippet for simplification purpose, Thus max_snippet
  // argument is not used. But still keept it in the argument list as logically
  // it should be passed in to control the total number of snippets to be
  // returned.
  JSON highlights = nlohmann::json::array_t();
  std::string snippet_string;

  std::vector<std::string> terms = absl::StrSplit(
      query, absl::ByAnyChar(kDelimiter), absl::SkipWhitespace());

  size_t min_term_length = max_snippet_length;
  for (const auto& term : terms) {
    min_term_length = std::min(min_term_length, term.length());
  }

  if (terms.empty() || min_term_length < max_snippet_length) {
    std::string joined_terms = absl::StrJoin(terms, "|");
    std::string pattern;
    if (!joined_terms.empty()) {
      absl::StrAppend(&pattern, "(?i)\\b(", joined_terms, ")\\b");
    }

    const_iterator first_match = target.begin();

    std::vector<std::pair<const_iterator, const_iterator>> matches;
    if (!pattern.empty()) {
      matches =
          MatchSnippets(target, pattern, max_snippet_length, &first_match);
    }

    // Grow the suffix.
    const_iterator match_end =
        GetMatchEnd(target, first_match, max_snippet_length);

    // Grow the prefix
    const_iterator match_begin =
        GetMatchBegin(target, first_match, match_end, max_snippet_length);
    snippet_string = std::string(match_begin, match_end);

    // Since the iterators in the matches are iterators pointing to the original
    // string, getting the highlight positions by adjusting the offset based on
    // match_begin.
    for (auto& match : matches) {
      JSON positions;
      positions[kStartPosition] = match.first - match_begin + 1;
      positions[kEndPosition] = match.second - match_begin + 1;
      highlights.push_back(std::move(positions));
    }
  }

  JSON snippet;
  snippet[kHighLights] = std::move(highlights);
  snippet[kSnippet] = std::move(snippet_string);

  JSON snippets = nlohmann::json::array_t();
  snippets.push_back(std::move(snippet));

  JSON result_json;
  result_json[kSnippets] = std::move(snippets);

  return result_json.dump();
}

namespace {
absl::StatusOr<int> TryGetIntParameterValue(
    absl::Span<const zetasql::Value> args, int parameter_index,
    int default_value, absl::string_view parameter_name) {
  int value = GetIntParameterValue(args, parameter_index, default_value);

  if (value > std::numeric_limits<int32_t>::max() || value <= 0) {
    return error::InvalidUseOfSnippetArgs(parameter_name);
  }

  return value;
}

}  // namespace

absl::StatusOr<std::optional<std::string>> SnippetEvaluator::Evaluate(
    absl::Span<const zetasql::Value> args) {
  // argument indexes
  constexpr int kTarget = 0;
  constexpr int kQuery = 1;
  constexpr int kMaxSnippetLength = 4;
  constexpr int kMaxSnippets = 5;
  constexpr int kContentType = 6;

  const zetasql::Value target = args[kTarget];
  const zetasql::Value query = args[kQuery];

  if (!target.type()->IsString()) {
    return error::IncorrectSnippetColumnType(target.type()->DebugString());
  }

  if (!query.type()->IsString()) {
    return error::InvalidSnippetQueryType(query.type()->DebugString());
  }

  if (args.size() > kContentType) {
    const zetasql::Value content_type = args[kContentType];
    if (!content_type.is_null() && content_type.type()->IsString() &&
        !absl::EqualsIgnoreCase(content_type.string_value(),
                                kValidContentType)) {
      return error::InvalidContentType("SNIPPET", content_type.string_value(),
                                       kValidContentType);
    }
  }

  if (target.is_null() || query.is_null()) {
    return std::nullopt;
  }

  ZETASQL_ASSIGN_OR_RETURN(
      auto max_snippet_length,
      TryGetIntParameterValue(args, kMaxSnippetLength, kDefaultMaxSnippetLength,
                              "max_snippet_length"));
  ZETASQL_ASSIGN_OR_RETURN(auto max_snippets, TryGetIntParameterValue(
                                          args, kMaxSnippets,
                                          kDefaultMaxSnippets, "max_snippets"));

  const std::string target_string = target.string_value();
  const std::string query_string = query.string_value();

  return BuildSnippets(target_string, query_string, max_snippet_length,
                       max_snippets);
}

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
