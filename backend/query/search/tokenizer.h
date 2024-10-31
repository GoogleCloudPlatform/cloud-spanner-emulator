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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_TOKENIZER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_TOKENIZER_H_

#include <cstdint>
#include <string>
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

static constexpr absl::string_view kExactMatchTokenizer = "exact_match";
static constexpr absl::string_view kFullTextTokenizer = "fulltext";
static constexpr absl::string_view kSubstringTokenizer = "substring";
static constexpr absl::string_view kNumericTokenizer = "numeric";
static constexpr absl::string_view kBoolTokenizer = "bool";
static constexpr absl::string_view kNgramsTokenizer = "ngrams";

static constexpr int kSubstringTokenizerSignatureArgumentSize = 5;

static constexpr char kDelimiter[] =
    "~!@#$%^&*()_+-={}[]|\\/\"`:;'<>,.?\r\n\t\b\f ";

// String indicate a gap between tokens. This is used to indicate tokens on both
// sides were created from different elements of an array.
static constexpr char kGapString[] = "\x01";

zetasql::Value TokenListFromStrings(std::vector<std::string> strings);

absl::StatusOr<std::vector<std::string>> StringsFromTokenList(
    const zetasql::Value& tokenList);

zetasql::Value TokenListFromBytes(std::string& bytes);

// Relative search types supported by TOKENIZE_SUBSTRING and SEARCH_SUBSTRING.
enum RelativeSearchType {
  None = 0,
  Word_Prefix = 0x01,
  Word_Suffix = 0x02,
  Value_Prefix = 0x04,
  Value_Suffix = 0x08,
  Phrase = 0x10,

  All = Word_Prefix | Word_Suffix | Value_Prefix | Value_Suffix | Phrase,
};

absl::StatusOr<RelativeSearchType> ParseRelativeSearchType(
    absl::string_view relative_search_type);

// Returns true if `token` is a tokenizer signature rather than normal token.
bool IsTokenizerSignature(absl::string_view token);

int64_t GetIntParameterValue(absl::Span<const zetasql::Value> args,
                             int parameter_index, int default_value);

double GetDoubleParameterValue(absl::Span<const zetasql::Value> args,
                               int parameter_index, double default_value);

bool GetBoolParameterValue(absl::Span<const zetasql::Value> args,
                           int parameter_index, bool default_value);
}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_TOKENIZER_H_
