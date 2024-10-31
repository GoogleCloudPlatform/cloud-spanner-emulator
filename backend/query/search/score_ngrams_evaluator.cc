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

#include "backend/query/search/score_ngrams_evaluator.h"

#include <cstdint>
#include <string>
#include <vector>

#include "zetasql/public/value.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "backend/query/search/ngrams_tokenizer.h"
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

// Generate unique trigrams from both the tokenlist and the query string.
// In
//  tokenlist: the tokenlist value store in the generated column.
//  query: the string value being scored.
// Out
//   tokenlist_trigrams: the returned set of unique trigrams from the tokenlist.
//   query_trigrams: the returned set of unique trigrams from the query.
absl::Status ScoreNgramsEvaluator::BuildTrigrams(
    const zetasql::Value& tokenlist, absl::string_view query,
    bool& source_is_null, absl::flat_hash_set<std::string>& tokenlist_trigrams,
    absl::flat_hash_set<std::string>& query_trigrams) {
  // substring and ngrams tokenizer signature indexes.
  constexpr int64_t kTokenizerSignatureArgumentSize = 4;
  constexpr int kIsNullIndex = 3;

  ZETASQL_ASSIGN_OR_RETURN(auto tokens, StringsFromTokenList(tokenlist));
  for (int i = 0; i < tokens.size(); ++i) {
    if (IsTokenizerSignature(tokens[i])) {
      // There will be multiple signatures when tokenlist is concatenated.
      if (!absl::StartsWith(tokens[i], kSubstringTokenizer) &&
          !absl::StartsWith(tokens[i], kNgramsTokenizer)) {
        return error::TokenListNotMatchSearch(
            "SCORE_NGRAMS", "TOKENIZE_SUBSTRING or TOKENIZE_NGRAMS");
      }
      // both substring and ngrams signatures start with:
      //   [substring|ngrams]-ngram_size_max-ngram_size_min-is_source_null
      std::vector<std::string> signature =
          absl::StrSplit(tokens[i], absl::ByChar('-'), absl::SkipEmpty());
      ZETASQL_RET_CHECK(signature.size() == kTokenizerSignatureArgumentSize ||
                signature.size() == kSubstringTokenizerSignatureArgumentSize);
      source_is_null = signature[kIsNullIndex] != "0";

      std::vector<zetasql::Value> args{
          zetasql::Value::String(query), zetasql::Value::Int64(kTrigrams),
          zetasql::Value::Int64(kTrigrams), zetasql::Value::Bool(false)};
      ZETASQL_ASSIGN_OR_RETURN(auto result, NgramsTokenizer::Tokenize(args));
      ZETASQL_ASSIGN_OR_RETURN(auto ngrams, StringsFromTokenList(result));
      for (auto it = ngrams.begin() + 1; it != ngrams.end(); ++it) {
        query_trigrams.insert(*it);
      }
    } else if (tokens[i] == kGapString) {
      continue;
    } else if (i == 0) {
      return error::TokenListNotMatchSearch(
          "SCORE_NGRAMS", "TOKENIZE_SUBSTRING or TOKENIZE_NGRAMS");
    } else {
      std::vector<zetasql::Value> args{zetasql::Value::String(tokens[i]),
                                         zetasql::Value::Int64(kTrigrams),
                                         zetasql::Value::Int64(kTrigrams),
                                         zetasql::Value::Bool(false)};
      ZETASQL_ASSIGN_OR_RETURN(auto result, NgramsTokenizer::Tokenize(args));
      ZETASQL_ASSIGN_OR_RETURN(auto ngrams, StringsFromTokenList(result));
      for (auto it = ngrams.begin() + 1; it != ngrams.end(); ++it) {
        tokenlist_trigrams.insert(*it);
      }
    }
  }
  return absl::OkStatus();
}

int64_t ScoreNgramsEvaluator::NumMatchingTrigrams(
    absl::flat_hash_set<std::string>& tokenlist_trigrams,
    absl::flat_hash_set<std::string>& query_trigrams) {
  int64_t num_matching_trigrams = 0;
  for (const auto& trigram : query_trigrams) {
    if (tokenlist_trigrams.contains(trigram)) {
      ++num_matching_trigrams;
    }
  }
  return num_matching_trigrams;
}

absl::StatusOr<zetasql::Value> ScoreNgramsEvaluator::Evaluate(
    absl::Span<const zetasql::Value> args) {
  const zetasql::Value& tokenlist = args[0];
  const zetasql::Value& query = args[1];

  if (tokenlist.is_null() || query.is_null()) {
    return zetasql::Value::Double(0.0);
  }

  if (!tokenlist.type()->IsTokenList()) {
    return error::ColumnNotSearchable(tokenlist.type()->DebugString());
  }

  if (!query.type()->IsString()) {
    return error::InvalidQueryType(query.type()->DebugString());
  }

  absl::flat_hash_set<std::string> tokenlist_trigrams;
  absl::flat_hash_set<std::string> query_trigrams;
  bool source_is_null;
  ZETASQL_RETURN_IF_ERROR(BuildTrigrams(tokenlist, query.string_value(), source_is_null,
                                tokenlist_trigrams, query_trigrams));

  if (source_is_null) {
    return zetasql::Value::Double(0.0);
  }

  int64_t match_count = NumMatchingTrigrams(tokenlist_trigrams, query_trigrams);

  double score = static_cast<double>(match_count) /
                 (static_cast<double>(query_trigrams.size()) +
                  static_cast<double>(tokenlist_trigrams.size()) -
                  static_cast<double>(match_count));

  return zetasql::Value::Double(score);
}

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
