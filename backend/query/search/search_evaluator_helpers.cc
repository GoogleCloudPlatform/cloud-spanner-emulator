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

#include "backend/query/search/search_evaluator_helpers.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/value.h"
#include "absl/algorithm/container.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "backend/query/search/query_parser.h"
#include "backend/query/search/tokenizer.h"
#include "common/errors.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

absl::StatusOr<TokenMap> SearchHelper::BuildTokenMap(
    const zetasql::Value& token_list, absl::string_view func_name,
    bool& source_is_null) {
  ZETASQL_ASSIGN_OR_RETURN(auto tokens, StringsFromTokenList(token_list));

  ZETASQL_RET_CHECK(!tokens.empty() && !tokens[0].empty());

  TokenMap token_map;
  int num_of_signatures = 0;
  int pos = 0;
  for (auto& token : tokens) {
    if (IsTokenizerSignature(token)) {
      // Emulator specific header to store tokenlist annotation. There can be
      // multiple signatures as tokenlist can be made from TOKENLIST_CONCAT.
      std::vector<std::string> signature =
          absl::StrSplit(token, absl::ByChar('-'), absl::SkipEmpty());
      if (signature[0] != kFullTextTokenizer) {
        return error::TokenListNotMatchSearch(func_name, "TOKENIZE_FULLTEXT");
      }
      ZETASQL_RET_CHECK(signature.size() == kTokenizerSignatureArgumentSize);
      if (pos == 0) {
        source_is_null = signature[1] != "0";
      } else {
        source_is_null = false;  // concatenated tokenlist
      }
      ++num_of_signatures;
    } else if (pos == 0) {  // First line must be the signature.
      return error::TokenListNotMatchSearch(func_name, "TOKENIZE_FULLTEXT");
    } else if (token == kGapString) {
      pos += 1000;
    } else {
      token_map[token].push_back(pos - num_of_signatures);
    }

    ++pos;
  }
  return token_map;
}

absl::StatusOr<SimpleNode*> SearchQueryCache::GetParsedQuery(
    const std::string& query_string) {
  if (query_cache_.find(query_string) == query_cache_.end()) {
    QueryParser parser(query_string);
    ZETASQL_RETURN_IF_ERROR(parser.ParseSearchQuery());

    query_cache_[query_string] = std::move(parser.ReleaseTree());
  }

  return query_cache_[query_string].get();
}

MatchResult MatchResult::Match(std::vector<Hit> hits) {
  MatchResult match_result;
  match_result.result_ = SortedUnique(std::move(hits));
  return match_result;
}

std::vector<Hit> MatchResult::SortedUnique(std::vector<Hit> hits) {
  std::sort(hits.begin(), hits.end());
  hits.erase(std::unique(hits.begin(), hits.end()), hits.end());
  return hits;
}

PhraseMatcher::PhraseMatcher(const std::vector<std::vector<Hit>>& inputs,
                             const std::vector<int>& offsets,
                             std::pair<int, int> exterior_gaps)
    : mode_(kPhrase),
      inputs_(inputs),
      offsets_(offsets),
      exterior_gaps_(exterior_gaps) {}

PhraseMatcher::PhraseMatcher(const std::vector<std::vector<Hit>>& inputs,
                             int max_allowed_gap)
    : mode_(kUnear), inputs_(inputs), max_allowed_gap_(max_allowed_gap) {
  input_max_widths_.resize(inputs_.size(), 1);
  for (int i = 0; i < inputs_.size(); ++i) {
    for (const Hit hit : inputs_[i]) {
      input_max_widths_[i] = std::max(input_max_widths_[i], hit.width);
    }
  }
}

std::vector<Hit> PhraseMatcher::FindMatchingHits() {
  std::vector<Hit> output;
  FindMatchingHitsInternal(output);
  absl::c_sort(output);
  output.erase(std::unique(output.begin(), output.end()), output.end());
  return output;
}

void PhraseMatcher::FindMatchingHitsInternal(std::vector<Hit>& output) {
  if (partial_match_.size() == inputs_.size()) {
    // Recursive ending handlings. All positions hit, indicating a potential hit
    // for the phrase. Merget the hits and store the result in the output.

    // Matches are not allowed to contain any overlap. We only need to check
    // this for unear.
    if (mode_ == kUnear && CurrentMatchContainsOverlap()) {
      return;
    }

    int32_t min_position = std::numeric_limits<int32_t>::max();
    int32_t max_limit = std::numeric_limits<int32_t>::lowest();
    for (auto& hit : partial_match_) {
      if (hit.pos < min_position) min_position = hit.pos;
      if (hit.limit() > max_limit) max_limit = hit.limit();
    }

    // 'interior_hit' is the hit without leading or trailing gaps (i.e.
    // gaps added by exterior_gaps_).
    const Hit interior_hit{min_position, max_limit - min_position};

    // 'hit' is the final hit, although it won't be emitted if it starts before
    // position zero.
    const Hit hit{
        interior_hit.pos - exterior_gaps_.first,
        interior_hit.width + exterior_gaps_.first + exterior_gaps_.second};
    output.push_back(hit);
    return;
  }

  const NextHitRequirements requirements = GetNextHitRequirements();
  const absl::Span<const Hit> input = inputs_[partial_match_.size()];

  for (auto it = absl::c_lower_bound(input, Hit{requirements.min_position});
       it != input.end() && it->pos <= requirements.max_position; ++it) {
    if (requirements.min_limit.has_value() &&
        it->limit() < *requirements.min_limit) {
      continue;
    }

    partial_match_.push_back(*it);
    FindMatchingHitsInternal(output);
    partial_match_.pop_back();
  }
}

PhraseMatcher::NextHitRequirements PhraseMatcher::GetNextHitRequirements() {
  if (partial_match_.empty()) {
    // The next hit can occur anywhere, including negative positions. Negative
    // positions could happen if the query contains leading wildcards.
    return {std::numeric_limits<int32_t>::lowest(),
            std::numeric_limits<int32_t>::max()};
  }

  switch (mode_) {
    case kPhrase: {
      const int next_idx = partial_match_.size();
      // Computing the next position requires subtracting 1 from the offset
      // difference. This is because the offset difference is partly covered by
      // limit, which is exclusive (i.e. one past the end of the corresponding
      // hit).
      const int32_t next_position =
          partial_match_.back().limit() +
          (offsets_[next_idx] - offsets_[next_idx - 1] - 1);
      return {next_position, next_position};
    }
    case kUnear: {
      // The next hit can occur anywhere in int32_t space, including
      // negative positions. As of 2021-01, only leading stars in child phrase
      // nodes cause negative position matches.
      int32_t max_position_of_partial_match =
          std::numeric_limits<int32_t>::lowest();
      int32_t min_limit_of_partial_match = std::numeric_limits<int32_t>::max();
      for (const Hit hit : partial_match_) {
        if (hit.limit() < min_limit_of_partial_match) {
          min_limit_of_partial_match = hit.limit();
        }
        if (hit.pos > max_position_of_partial_match) {
          max_position_of_partial_match = hit.pos;
        }
      }
      const int next_idx = partial_match_.size();

      // The minimum matching position for the next hit has to incorporate
      // the max width of the next input that we're looking at. However,
      // this can give us false positive hits if we find a hit that is
      // narrower than the max width such that the limit is positioned earlier
      // than (max_position - max_allowed_gap_). To resolve this issue, we
      // also set min_limit, preventing the inclusion of hits with smaller
      // limits.
      //
      // (From the perspective of the algorithm, the acceptability of the next
      // hit is determined by min_limit and max_position. min_position is
      // provided in order to make the search for conforming positions faster,
      // but it would still be correct even if we just passed zero for
      // min_position.)
      return {.min_position = max_position_of_partial_match - max_allowed_gap_ -
                              input_max_widths_[next_idx],
              .max_position = min_limit_of_partial_match + max_allowed_gap_,
              .min_limit = max_position_of_partial_match - max_allowed_gap_};
    }
  }
}

bool PhraseMatcher::CurrentMatchContainsOverlap() const {
  std::vector<Hit> sorted_hits(partial_match_.begin(), partial_match_.end());
  absl::c_sort(sorted_hits);
  for (int i = 1; i < sorted_hits.size(); ++i) {
    if (sorted_hits[i].pos < sorted_hits[i - 1].limit()) {
      return true;
    }
  }
  return false;
}

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
