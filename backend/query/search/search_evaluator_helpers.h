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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_SEARCH_EVALUATOR_HELPER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_SEARCH_EVALUATOR_HELPER_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/value.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "backend/query/search/SearchQueryParserTree.h"

// Define the helper classes that paricipate in search evaluation.
namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

// Map of each token's positions.
using TokenMap = absl::flat_hash_map<std::string, std::vector<int>>;

class SearchHelper {
 public:
  static const int kTokenizerSignatureArgumentSize = 2;

  // Build a map which stores the positions of each token.
  // Input: the token_list value.
  // Returns: if successful, TokenMap generated from the tokenlist, as well as a
  //          flag indicate if the source is NULL value. Otherwise error status
  //          code.
  static absl::StatusOr<TokenMap> BuildTokenMap(
      const zetasql::Value& token_list, absl::string_view func_name,
      bool& source_is_null);
};

struct Hit {
 public:
  // The limit is the position one past the end of a hit.
  int limit() const { return pos + width; }

  bool operator<(const Hit& other) const {
    return pos < other.pos || (pos == other.pos && width < other.width);
  }

  friend bool operator==(Hit const& left, Hit const& right) {
    return left.pos == right.pos && left.width == right.width;
  }

  // The position of a hit -- where in the document the hit starts.
  int pos = 0;

  // The width of the hit. How many positions it comprises. Hits that are
  // wider than one can represent compositions of multiple child terms.
  // For example, a hit for the phrase "see spot run" would have a width
  // of three.
  int width = 1;
};

class MatchResult {
 public:
  static MatchResult NoMatch() { return MatchResult(); }
  static MatchResult Match(std::vector<Hit> hits = {});

  bool is_match() const { return result_.has_value(); }

  // REQUIRES: is_match()
  const std::vector<Hit>& hits() const { return *result_; }
  std::vector<Hit>& hits() { return *result_; }

 private:
  static std::vector<Hit> SortedUnique(std::vector<Hit> hits);

  // There are three states:
  //  null  --> no-match
  //  empty --> match but don't need position info. This enables quick return
  //            for evaluating non-phrase OR and AND operator.
  //  not-empty --> match with position info.
  std::optional<std::vector<Hit>> result_;
};

// singleton that caches the parsed query so that it doesn't need to be
// parsed on every search evaluation.
class SearchQueryCache {
 public:
  SearchQueryCache(SearchQueryCache& other) = delete;
  void operator=(const SearchQueryCache&) = delete;

  static SearchQueryCache* GetInstance() {
    static SearchQueryCache* instance_ = new SearchQueryCache();
    return instance_;
  }

  absl::StatusOr<SimpleNode*> GetParsedQuery(const std::string& query_string);

 private:
  SearchQueryCache() = default;

  absl::flat_hash_map<std::string, std::unique_ptr<SimpleNode>> query_cache_;
};

class PhraseMatcher {
 public:
  // Matches an exact phrase. 'inputs' is a sorted list of hits for each child
  // of a phrase. We do not copy 'inputs', so it must remain valid until
  // PhraseMatcher is destroyed. 'offsets' represents the relative position
  // where each input may be found. 'exterior_gaps' indicates the leading and
  // trailing wildcards in the searched query.
  PhraseMatcher(const std::vector<std::vector<Hit>>& inputs,
                const std::vector<int>& offsets,
                std::pair<int, int> exterior_gaps);

  // Matches an around expression. inputs' is a sorted list of hits for each
  // child of a phrase. 'max_allowed_gap' is the maximum gaps between terms.
  // the value is stored in the around expression.
  PhraseMatcher(const std::vector<std::vector<Hit>>& inputs,
                int max_allowed_gap);

  std::vector<Hit> FindMatchingHits();

 private:
  // Recursively finds matching hits.
  //
  // Each call to this function looks at the current state of partial_match_
  // and finds the possibly matching hits in the next element of inputs_.
  // For each potentially matching hit, updates partial_match_ with that
  // hit and recursively calls itself.
  void FindMatchingHitsInternal(std::vector<Hit>& output);

  // Returns whether the current partial_match_ contains overlapping hits.
  // (If so, it does not match the query, because none of the match modes allow
  // any overlaps.)
  bool CurrentMatchContainsOverlap() const;

  // Requirements for the matching hits in the next input.
  struct NextHitRequirements {
    // The first position that would be acceptable.
    int32_t min_position;

    // The last position that would be acceptable.
    int32_t max_position;

    // If set, we also require that any hit that is matched have a limit no
    // lower than this value. Only used with unear.
    std::optional<int32_t> min_limit;
  };

  // Returns a requirements for matching hits in the next input, given the
  // current state of partial_match_.
  NextHitRequirements GetNextHitRequirements();

  enum {
    kPhrase,  // Matches phrase
    kUnear    // Unordered near. Matches around expression outside a phrase
  } mode_;

  // The input hit arrays. inputs_[i] is all the hits for child[i] of the given
  // phrase matcher.
  const std::vector<std::vector<Hit>>& inputs_;

  // The offsets for exact phrases. Empty otherwise.
  std::vector<int> offsets_;

  // The gaps before and after the phrase to be matched.
  std::pair<int, int> exterior_gaps_ = {0, 0};

  // The maximum gap between the earliest and latest hit in a match. Not used
  // for exact phrases.
  const int max_allowed_gap_ = 0;

  // Only used during unear matching: for inputs_[i], the maximum width of any
  // hit in the input.
  std::vector<int> input_max_widths_;

  // A partially constructed match candidate. partial_match_[i] is a hit
  // from inputs_[i] that conforms with the offsets_, mode_ and max_allowed_gap_
  // fields in combination with partial_match_[0..i-1].
  std::vector<Hit> partial_match_;
};

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_SEARCH_EVALUATOR_HELPER_H_
