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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_SEARCH_EVALUATOR_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_SEARCH_EVALUATOR_H_

#include <vector>

#include "zetasql/public/value.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "backend/query/search/SimpleNode.h"
#include "backend/query/search/search_evaluator_helpers.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

// Evaluate the SEARCH(TOKENLIST, STRING) function. The STRING query will be
// parsed into a evaluation tree. Then the tree will be traversed to evaluate
// each of the sub query string is a match in the TOKENLIST. If TOKENLIST is
// NULL, always return FALSE. If STRING query is NULL, always return TRUE.
class SearchEvaluator {
 public:
  static absl::StatusOr<zetasql::Value> Evaluate(
      absl::Span<const zetasql::Value> args);

 private:
  // Default Around distance if it cannot be determined from user input.
  static const int kDefaultMaxAllowedGap = 5;

  // If the the node is a term, check if there are matches in the tokenmap.
  // Return matched positions if there are matches and in_phrase is true. Or
  // just return match if in_phrase is false. Otherwise, returns no match.
  static absl::StatusOr<MatchResult> MatchQueryTerm(const SimpleNode* root,
                                                    const TokenMap& tokenmap,
                                                    bool in_phrase);

  // Recursively check matches of each children.
  static absl::StatusOr<std::vector<MatchResult>> MatchChildren(
      const SimpleNode* root, const TokenMap& tokenmap, bool in_phrase);

  // Evaluate if a given query node has matches in the given tokenmap.
  static absl::StatusOr<MatchResult> MatchQueryNode(const SimpleNode* root,
                                                    const TokenMap& tokenmap,
                                                    bool in_phrase = false);
};

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_SEARCH_EVALUATOR_H_
