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

#include "backend/query/search/search_ngrams_evaluator.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <string>
#include <vector>

#include "zetasql/public/value.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/numbers.h"
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

// Read the tokenization function signature to get the ngram sizes used to
// tokenize the original document. Then generate the set of the unique ngrams
// from both the tokenlist and the query string.
// In
//  tokenlist: the tokenlist value store in the generated column.
//  query: the string value being searched on, when the tokenlist is generated
//  from TOKENIZE_SUBSTRING the string value is broken into words first, then
//  n-grams are generated from the each word.
// Out
//   tokenlist_ngrams: the returned set of unique ngrams from the tokenlist.
//   query_ngrams: the returned set of unique ngrams from the query.
absl::Status SearchNgramsEvaluator::BuildTokenLists(
    const zetasql::Value& tokenlist, absl::string_view query,
    bool& source_is_null, std::vector<std::string>& tokenlist_ngrams,
    absl::flat_hash_set<std::string>& query_ngrams) {
  // substring and ngrams tokenizer signature indexes.
  constexpr int64_t kTokenizerSignatureArgumentSize = 4;
  constexpr int64_t kNgramMaxSizeIndex = 1;
  constexpr int64_t kNgramMinSizeIndex = 2;
  constexpr int kIsNullIndex = 3;

  ZETASQL_ASSIGN_OR_RETURN(auto tokens, StringsFromTokenList(tokenlist));
  int64_t ngram_max_size = -1;
  int64_t ngram_min_size = -1;
  bool is_substring_tokenizer = false;
  std::vector<std::string> tokenlist_substring;
  for (int i = 0; i < tokens.size(); ++i) {
    if (IsTokenizerSignature(tokens[i])) {
      is_substring_tokenizer = absl::StartsWith(tokens[i], kSubstringTokenizer);
      // There will be multiple signatures when tokenlist is concatenated.
      if (!is_substring_tokenizer &&
          !absl::StartsWith(tokens[i], kNgramsTokenizer)) {
        return error::TokenListNotMatchSearch(
            "SEARCH_NGRAMS", "TOKENIZE_SUBSTRING or TOKENIZE_NGRAMS");
      }
      // both substring and ngrams signatures start with:
      //   [substring|ngrams]-ngram_size_max-ngram_size_min-is_source_null
      std::vector<std::string> signature =
          absl::StrSplit(tokens[i], absl::ByChar('-'), absl::SkipEmpty());
      ZETASQL_RET_CHECK(
          (signature.size() == kTokenizerSignatureArgumentSize ||
           signature.size() == kSubstringTokenizerSignatureArgumentSize) &&
          absl::SimpleAtoi(signature[kNgramMaxSizeIndex], &ngram_max_size) &&
          absl::SimpleAtoi(signature[kNgramMinSizeIndex], &ngram_min_size));
      source_is_null = signature[kIsNullIndex] != "0";

      // Break query into words and generate ngrams for each word.
      std::vector<std::string> substrings = absl::StrSplit(
          query, absl::ByAnyChar(kDelimiter), absl::SkipWhitespace());
      for (const auto& substring : substrings) {
        // Query n-grams are generated with length `ngram_max_size` of the
        // original tokenlist provided to SEARCH_NGRAMS.
        std::vector<zetasql::Value> args{
            zetasql::Value::String(substring),
            zetasql::Value::Int64(ngram_max_size),
            zetasql::Value::Int64(std::min(
                static_cast<int64_t>(substring.size()), ngram_max_size)),
            zetasql::Value::Bool(false)};
        ZETASQL_ASSIGN_OR_RETURN(auto result, NgramsTokenizer::Tokenize(args));
        ZETASQL_ASSIGN_OR_RETURN(auto ngrams, StringsFromTokenList(result));
        for (auto it = ngrams.begin() + 1; it != ngrams.end(); ++it) {
          query_ngrams.insert(*it);
        }
      }
    } else if (tokens[i] == kGapString) {
      continue;
    } else if (i == 0) {
      return error::TokenListNotMatchSearch(
          "SEARCH_NGRAMS", "TOKENIZE_SUBSTRING or TOKENIZE_NGRAMS");
    } else if (is_substring_tokenizer) {
      ZETASQL_RETURN_IF_ERROR(TokenizeSubstring(tokens[i], tokenlist_substring));
    } else {
      tokenlist_ngrams.push_back(tokens[i]);
    }
  }
  if (is_substring_tokenizer) {
    for (const auto& token : tokenlist_substring) {
      ZETASQL_RETURN_IF_ERROR(TokenizeNgrams(token, ngram_min_size, ngram_max_size,
                                     tokenlist_ngrams));
    }
  }
  return absl::OkStatus();
}

int64_t SearchNgramsEvaluator::NumMatchingNgrams(
    std::vector<std::string>& tokenlist_ngrams,
    absl::flat_hash_set<std::string>& query_ngrams) {
  int64_t num_matching_ngrams = 0;
  for (const auto& token : query_ngrams) {
    if (std::find(tokenlist_ngrams.begin(), tokenlist_ngrams.end(), token) !=
        tokenlist_ngrams.end()) {
      ++num_matching_ngrams;
    }
  }
  return num_matching_ngrams;
}

absl::StatusOr<zetasql::Value> SearchNgramsEvaluator::Evaluate(
    absl::Span<const zetasql::Value> args) {
  // argument indexes
  constexpr int64_t kTokenlist = 0;
  constexpr int64_t kQuery = 1;
  constexpr int64_t kNgramMin = 2;
  constexpr int64_t kNgramMinPercent = 3;

  const zetasql::Value& tokenlist = args[kTokenlist];
  const zetasql::Value& query = args[kQuery];
  int64_t min_ngrams = GetIntParameterValue(args, kNgramMin, kDefaultMinNgrams);
  double min_ngrams_percent =
      GetDoubleParameterValue(args, kNgramMinPercent, 0);

  if (tokenlist.is_null() || query.is_null()) {
    return zetasql::Value::NullBool();
  }

  if (!tokenlist.type()->IsTokenList()) {
    return error::ColumnNotSearchable(tokenlist.type()->DebugString());
  }

  if (!query.type()->IsString()) {
    return error::InvalidQueryType(query.type()->DebugString());
  }

  std::vector<std::string> tokenlist_ngrams;
  absl::flat_hash_set<std::string> query_ngrams;
  bool source_is_null;
  ZETASQL_RETURN_IF_ERROR(BuildTokenLists(tokenlist, query.string_value(),
                                  source_is_null, tokenlist_ngrams,
                                  query_ngrams));

  if (source_is_null) {
    return zetasql::Value::NullBool();
  }

  int64_t matching_ngrams = NumMatchingNgrams(tokenlist_ngrams, query_ngrams);
  // If the number of query ngrams is less than the minimum ngrams, then we
  // ignore the minimum ngrams and match if all the ngrams in query match.
  if (query_ngrams.size() < min_ngrams) {
    min_ngrams = std::max(1, static_cast<int>(query_ngrams.size()));
  }
  bool matches = false;
  if (matching_ngrams >= min_ngrams) {
    matches = true;
  }
  if (matching_ngrams <
      static_cast<int64_t>(
          std::ceil(query_ngrams.size() * min_ngrams_percent / 100.0))) {
    matches = false;
  }
  return zetasql::Value::Bool(matches);
}

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
