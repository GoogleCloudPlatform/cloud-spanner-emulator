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

#include "backend/query/search/score_evaluator.h"

#include <string>
#include <vector>

#include "zetasql/public/value.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_split.h"
#include "absl/types/span.h"
#include "backend/query/search/search_evaluator_helpers.h"
#include "backend/query/search/tokenizer.h"
#include "common/errors.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

absl::StatusOr<zetasql::Value> ScoreEvaluator::Evaluate(
    absl::Span<const zetasql::Value> args) {
  const zetasql::Value tokenlist = args[0];
  const zetasql::Value query_string = args[1];

  if (!tokenlist.type()->IsTokenList()) {
    return error::ColumnNotSearchable(tokenlist.type()->DebugString());
  }

  if (!query_string.type()->IsString()) {
    return error::InvalidQueryType(query_string.type()->DebugString());
  }

  TokenMap token_map;
  bool source_is_null = false;
  if (!tokenlist.is_null()) {
    ZETASQL_ASSIGN_OR_RETURN(token_map, SearchHelper::BuildTokenMap(tokenlist, "SCORE",
                                                            source_is_null));
  }

  double score = 0.0;
  if (!source_is_null && !query_string.is_null()) {
    std::vector<std::string> terms =
        absl::StrSplit(query_string.string_value(), absl::ByAnyChar(kDelimiter),
                       absl::SkipWhitespace());

    for (const std::string& term : terms) {
      // Add the number of occurrences of each queried term in the tokenlist.
      if (token_map.find(term) != token_map.end()) {
        score += token_map[term].size();
      }
    }
  }

  return zetasql::Value::Double(score);
}

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
