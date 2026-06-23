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

#include <cstddef>
#include <string>
#include <vector>

#include "googlesql/public/value.h"
#include "absl/algorithm/container.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "backend/query/search/search_evaluator_helpers.h"
#include "backend/query/search/search_util.h"
#include "backend/query/search/tokenizer.h"
#include "common/errors.h"
#include "googlesql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

namespace {

absl::StatusOr<double> EvaluateWordsScore(absl::string_view query,
                                          const TokenMap& token_map) {
  GOOGLESQL_ASSIGN_OR_RETURN(const std::vector<std::string> terms,
                   GetNormalizedTerms(query));
  double score = 0.0;
  for (const auto& term : terms) {
    auto it = token_map.find(term);
    if (it != token_map.end()) {
      score += it->second.size();
    }
  }
  return score;
}

absl::StatusOr<double> EvaluateWordsPhraseScore(
    absl::string_view query, const googlesql::Value& tokenlist) {
  GOOGLESQL_ASSIGN_OR_RETURN(const std::vector<std::string> terms,
                   GetNormalizedTerms(query));
  if (terms.empty()) {
    return 0.0;
  }
  GOOGLESQL_ASSIGN_OR_RETURN(std::vector<std::string> tokens,
                   StringsFromTokenList(tokenlist));

  double score = 0.0;
  if (tokens.size() >= terms.size()) {
    for (size_t i = 0; i <= tokens.size() - terms.size(); ++i) {
      if (absl::c_equal(terms,
                        absl::MakeSpan(tokens).subspan(i, terms.size()))) {
        score += 1.0;
      }
    }
  }
  return score;
}

}  // namespace

absl::StatusOr<googlesql::Value> ScoreEvaluator::Evaluate(
    absl::Span<const googlesql::Value> args) {
  const googlesql::Value tokenlist = args[0];
  const googlesql::Value query_string = args[1];

  if (!tokenlist.type()->IsTokenList()) {
    return error::ColumnNotSearchable(tokenlist.type()->DebugString());
  }

  if (!query_string.type()->IsString()) {
    return error::InvalidQueryType(query_string.type()->DebugString());
  }

  TokenMap token_map;
  bool source_is_null = false;
  if (!tokenlist.is_null()) {
    GOOGLESQL_ASSIGN_OR_RETURN(token_map, SearchHelper::BuildTokenMap(tokenlist, "SCORE",
                                                            source_is_null));
  }

  if (source_is_null || query_string.is_null()) {
    return googlesql::Value::Double(0.0);
  }

  double score = 0.0;
  Dialect dialect = Dialect::RQUERY;
  if (args.size() > 4 && !args[4].is_null()) {
    GOOGLESQL_ASSIGN_OR_RETURN(dialect, ParseDialect(args[4].string_value()));
  }

  if (dialect == Dialect::WORDS_PHRASE) {
    GOOGLESQL_ASSIGN_OR_RETURN(score, EvaluateWordsPhraseScore(
                                query_string.string_value(), tokenlist));
  } else {
    // Use same evaluation logic for RQUERY and WORDS dialects in emulator.
    GOOGLESQL_ASSIGN_OR_RETURN(
        score, EvaluateWordsScore(query_string.string_value(), token_map));
  }

  return googlesql::Value::Double(score);
}

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
