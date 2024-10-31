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

#include "backend/query/search/ngrams_tokenizer.h"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/query/search/tokenizer.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

using testing::HasSubstr;
using zetasql_base::testing::StatusIs;

struct NgramsTokenizerTestCase {
  std::vector<std::string> original_strs;
  std::optional<int64_t> ngram_size_max;
  std::optional<int64_t> ngram_size_min;
  std::string expected_tokenize_signature;
  std::vector<std::string> expected_result;
  bool is_array_input;
};

using NgramsTokenizerTest = ::testing::TestWithParam<NgramsTokenizerTestCase>;

TEST_P(NgramsTokenizerTest, TestTokenize) {
  const NgramsTokenizerTestCase& test_case = GetParam();

  zetasql::Value value;
  if (test_case.is_array_input) {
    value = zetasql::values::StringArray(test_case.original_strs);
  } else {
    value = zetasql::Value::String(test_case.original_strs[0]);
  }

  zetasql::Value ngram_max =
      test_case.ngram_size_max.has_value()
          ? zetasql::Value::Int64(test_case.ngram_size_max.value())
          : zetasql::Value::NullInt64();
  zetasql::Value ngram_min =
      test_case.ngram_size_min.has_value()
          ? zetasql::Value::Int64(test_case.ngram_size_min.value())
          : zetasql::Value::NullInt64();

  std::vector<zetasql::Value> args{value, ngram_max, ngram_min};

  absl::StatusOr<zetasql::Value> result = NgramsTokenizer::Tokenize(args);
  ZETASQL_EXPECT_OK(result.status());

  zetasql::Value token_list = result.value();
  EXPECT_TRUE(token_list.type()->IsTokenList());

  auto get_tokens = StringsFromTokenList(token_list);
  ZETASQL_EXPECT_OK(get_tokens.status());

  // Always expect the tokenlist has at least one token
  // which stores tokenizer information.
  auto tokens = get_tokens.value();
  EXPECT_FALSE(tokens.empty());
  EXPECT_EQ(test_case.expected_tokenize_signature, tokens[0]);

  std::vector<std::string> token_texts;
  token_texts.insert(token_texts.end(), tokens.begin() + 1, tokens.end());

  EXPECT_EQ(test_case.expected_result, token_texts);
}

INSTANTIATE_TEST_SUITE_P(
    NgramsTokenizerTest, NgramsTokenizerTest,
    testing::ValuesIn<NgramsTokenizerTestCase>({
        {{""}, 4, 1, "ngrams-4-1-0", {}, false},
        {{"abcd"},
         4,
         1,
         "ngrams-4-1-0",
         {"a", "b", "c", "d", "ab", "bc", "cd", "abc", "bcd", "abcd"},
         false},
        {{"aBcD"},
         4,
         1,
         "ngrams-4-1-0",
         {"a", "b", "c", "d", "ab", "bc", "cd", "abc", "bcd", "abcd"},
         false},
        {{"abcd"}, 4, 4, "ngrams-4-4-0", {"abcd"}, false},
        {{"abcd"}, 12, 10, "ngrams-12-10-0", {}, false},
        {{"abcdef"},
         3,
         3,
         "ngrams-3-3-0",
         {{"abc", "bcd", "cde", "def"}},
         false},
        {{"abc", "def", "ghi"},
         4,
         1,
         "ngrams-4-1-0",
         {"a",  "b",   "c",   "ab", "bc", "abc", "\x1", "d",  "e",   "f",  "de",
          "ef", "def", "\x1", "g",  "h",  "i",   "gh",  "hi", "ghi", "\x1"},
         true},
        // The emulator doesn't account for diacritics taking up more than one
        // byte  when generating ngrams.
        {{"Curaçao"}, 7, 7, "ngrams-7-7-0", {"curaça", "uraçao"}, false},
        {{"Caffè", "Crème"},
         6,
         6,
         "ngrams-6-6-0",
         {"caffè", "\x1", "crème", "\x1"},
         true},
        // The emulator doesn't handle converting diacritics to their base
        // characters when generating ngrams.
        {{"Curaçao"}, 7, 7, "ngrams-7-7-0", {"curaça", "uraçao"}, false},
        {{"Caffè", "Crème"},
         6,
         6,
         "ngrams-6-6-0",
         {"caffè", "\x1", "crème", "\x1"},
         true},
    }));

struct NgramsTokenizerArgErrorTestCase {
  int64_t ngram_size_max;
  int64_t ngram_size_min;
  std::string expected_error;
};

using NgramsTokenizerErrorTest =
    ::testing::TestWithParam<NgramsTokenizerArgErrorTestCase>;

TEST_P(NgramsTokenizerErrorTest, TestNgramValues) {
  const NgramsTokenizerArgErrorTestCase& test_case = GetParam();

  std::vector<zetasql::Value> args;
  args.push_back(zetasql::Value::String("test_ngram_values"));
  args.push_back(zetasql::Value::Int64(test_case.ngram_size_max));
  args.push_back(zetasql::Value::Int64(test_case.ngram_size_min));

  EXPECT_THAT(NgramsTokenizer::Tokenize(args),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr(test_case.expected_error)));
}

INSTANTIATE_TEST_SUITE_P(
    NgramsTokenizerErrorTest, NgramsTokenizerErrorTest,
    testing::ValuesIn<NgramsTokenizerArgErrorTestCase>({
        {0, 1, "ngram_size_max must be greater than 0."},
        {3, 0, "ngram_size_min must be greater than 0."},
        {3, 4, "ngram_size_min cannot be greater than ngram_size_max."},
        {13, 4, "ngram_size_max cannot be greater than 12."},
    }));

TEST(NgramsTokenizerTest, NullInputValue) {
  absl::StatusOr<zetasql::Value> result =
      NgramsTokenizer::Tokenize({zetasql::Value::NullString()});
  ZETASQL_EXPECT_OK(result.status());

  zetasql::Value token_list = result.value();
  EXPECT_TRUE(token_list.type()->IsTokenList());

  // Always expect the tokenlist has at least one token
  // which stores tokenizer information.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto tokens, StringsFromTokenList(token_list));
  ASSERT_EQ(tokens.size(), 1);
  EXPECT_EQ("ngrams-4-1-1", tokens[0]);
}

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
