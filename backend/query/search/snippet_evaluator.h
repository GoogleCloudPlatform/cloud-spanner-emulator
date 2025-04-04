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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_SNIPPET_EVALUATOR_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_SNIPPET_EVALUATOR_H_

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/value.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

class SnippetEvaluator {
 public:
  static absl::StatusOr<std::optional<std::string>> Evaluate(
      absl::Span<const zetasql::Value> args);

 private:
  static constexpr int64_t kDefaultMaxSnippetLength = 160;
  static constexpr int64_t kDefaultMaxSnippets = 3;

  // Finds the matching snippets. Returns a list of matches. Each match consists
  // of a pair of iterators pointing to the start and end positions of the match
  // in the original string. Also sets `first_match` to the begining of the
  // first match. Since the first match could be longer than max_snippet_length
  // thus no match is returned from the function, the function needs to return
  // `first_match` to mark the match position.
  static std::vector<std::pair<absl::string_view::const_iterator,
                               absl::string_view::const_iterator>>
  MatchSnippets(absl::string_view target, absl::string_view pattern,
                int max_snippet_length,
                absl::string_view::const_iterator* first_match);

  // Grows forward. Find the last position in the target that does not exceed
  // the max_snippet_length for the final snippet. The position cannot land
  // within a word.
  static absl::string_view::const_iterator GetMatchEnd(
      absl::string_view target, absl::string_view::const_iterator first_match,
      int max_snippet_length);

  // Grows backward. Find the first position in the target that does not exceed
  // the max_snippet_length for the final snippet. The position cannot land
  // within a word.
  static absl::string_view::const_iterator GetMatchBegin(
      absl::string_view target, absl::string_view::const_iterator first_match,
      absl::string_view::const_iterator match_end, int max_snippet_length);

  static absl::StatusOr<std::string> BuildSnippets(absl::string_view target,
                                                   absl::string_view query,
                                                   int max_snippet_length,
                                                   int max_snippets);
};

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_SNIPPET_EVALUATOR_H_
