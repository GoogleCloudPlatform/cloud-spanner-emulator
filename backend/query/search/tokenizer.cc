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

#include "backend/query/search/tokenizer.h"

#include <cstdint>
#include <string>
#include <vector>

#include "zetasql/public/simple_token_list.h"
#include "zetasql/public/token_list_util.h"
#include "zetasql/public/value.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "common/errors.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

using TextToken = zetasql::tokens::TextToken;
using TokenList = zetasql::tokens::TokenList;

namespace {
constexpr absl::string_view kRelativeSearchTypeWordPrefix = "word_prefix";
constexpr absl::string_view kRelativeSearchTypeWordSuffix = "word_suffix";
constexpr absl::string_view kRelativeSearchTypeValuePrefix = "value_prefix";
constexpr absl::string_view kRelativeSearchTypeValueSuffix = "value_suffix";
constexpr absl::string_view kRelateiveSearchTypeAdjacentAndInOrder =
    "adjacent_and_in_order";
constexpr absl::string_view kRelativeSearchTypePhrase = "phrase";
}  // namespace

zetasql::Value TokenListFromStrings(std::vector<std::string> strings) {
  return zetasql::TokenListFromStringArray(strings);
}

absl::StatusOr<std::vector<std::string>> StringsFromTokenList(
    const zetasql::Value& tokenList) {
  std::vector<std::string> strings;

  ZETASQL_ASSIGN_OR_RETURN(auto iter, tokenList.tokenlist_value().GetIterator());
  TextToken token;
  while (!iter.done() && iter.Next(token).ok()) {
    strings.push_back(std::string(token.text()));
  }

  return strings;
}

zetasql::Value TokenListFromBytes(std::string& bytes) {
  return zetasql::Value::TokenList(TokenList::FromBytesUnvalidated(bytes));
}

bool IsTokenizerSignature(absl::string_view token) {
  return absl::StartsWith(token, absl::StrCat(kExactMatchTokenizer, "-")) ||
         absl::StartsWith(token, absl::StrCat(kFullTextTokenizer, "-")) ||
         absl::StartsWith(token, absl::StrCat(kSubstringTokenizer, "-")) ||
         absl::StartsWith(token, absl::StrCat(kNumericTokenizer, "-")) ||
         absl::StartsWith(token, absl::StrCat(kBoolTokenizer, "-")) ||
         absl::StartsWith(token, absl::StrCat(kNgramsTokenizer, "-"));
}

int64_t GetIntParameterValue(absl::Span<const zetasql::Value> args,
                             int parameter_index, int default_value) {
  return args.size() > parameter_index && !args[parameter_index].is_null()
             ? args[parameter_index].int64_value()
             : default_value;
}

double GetDoubleParameterValue(absl::Span<const zetasql::Value> args,
                               int parameter_index, double default_value) {
  return args.size() > parameter_index && !args[parameter_index].is_null()
             ? args[parameter_index].double_value()
             : default_value;
}

bool GetBoolParameterValue(absl::Span<const zetasql::Value> args,
                           int parameter_index, bool default_value) {
  return args.size() > parameter_index && !args[parameter_index].is_null()
             ? args[parameter_index].bool_value()
             : default_value;
}

absl::StatusOr<RelativeSearchType> ParseRelativeSearchType(
    absl::string_view relative_search_type) {
  if (absl::EqualsIgnoreCase(relative_search_type,
                             kRelativeSearchTypeWordPrefix)) {
    return RelativeSearchType::Word_Prefix;
  }
  if (absl::EqualsIgnoreCase(relative_search_type,
                             kRelativeSearchTypeWordSuffix)) {
    return RelativeSearchType::Word_Suffix;
  }
  if (absl::EqualsIgnoreCase(relative_search_type,
                             kRelativeSearchTypeValuePrefix)) {
    return RelativeSearchType::Value_Prefix;
  }
  if (absl::EqualsIgnoreCase(relative_search_type,
                             kRelativeSearchTypeValueSuffix)) {
    return RelativeSearchType::Value_Suffix;
  }
  if (absl::EqualsIgnoreCase(relative_search_type, kRelativeSearchTypePhrase) ||
      absl::EqualsIgnoreCase(relative_search_type,
                             kRelateiveSearchTypeAdjacentAndInOrder)) {
    return RelativeSearchType::Phrase;
  }

  return error::InvalidRelativeSearchType(relative_search_type);
}

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
