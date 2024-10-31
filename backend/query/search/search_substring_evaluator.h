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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_SEARCH_SUBSTRING_EVALUATOR_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_SEARCH_SUBSTRING_EVALUATOR_H_

#include <string>
#include <vector>

#include "zetasql/public/value.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "backend/query/search/tokenizer.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

class SearchSubstringEvaluator {
 public:
  static absl::StatusOr<zetasql::Value> Evaluate(
      absl::Span<const zetasql::Value> args);

 private:
  // SEARCH_SUBSTRING argument indexes.
  static constexpr int kTokenlist = 0;
  static constexpr int kQuery = 1;
  static constexpr int kRelativeSearchType = 2;

  static absl::Status BuildTokenList(const zetasql::Value& tokenlist,
                                     bool& source_is_null,
                                     int& relative_search_types,
                                     int& ngram_min_size,
                                     std::vector<std::string>& token_list);

  static bool MatchesSubstring(absl::Span<const std::string> token_list,
                               absl::string_view substring,
                               RelativeSearchType relative_search_type);

  // search substrings inside tokens based on the relative_search_type.
  // Return true if all substrings are found in the tokenlist.
  static zetasql::Value SearchSubstring(
      int ngram_min_size, absl::Span<const std::string> tokens,
      absl::Span<const std::string> substrings,
      RelativeSearchType relative_search_type);

  static zetasql::Value SearchSubstringPharse(
      int ngram_min_size, absl::Span<const std::string> tokens,
      absl::Span<const std::string> substrings);
};

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_SEARCH_SUBSTRING_EVALUATOR_H_
