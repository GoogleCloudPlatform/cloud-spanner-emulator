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

#include "backend/query/search/substring_tokenizer.h"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "zetasql/public/types/type_factory.h"
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

struct SubstringTokenizerTestCase {
  std::vector<std::string> original_strs;
  std::optional<int64_t> ngram_size_max;
  std::optional<int64_t> ngram_size_min;
  std::optional<bool> support_relative_search;
  std::vector<std::string> relative_search_types;
  std::string expected_tokenize_signature;
  std::vector<std::string> expected_result;
  bool is_array_input;
};

using SubstringTokenizerTest =
    ::testing::TestWithParam<SubstringTokenizerTestCase>;

TEST_P(SubstringTokenizerTest, TestTokenize) {
  const SubstringTokenizerTestCase& test_case = GetParam();

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

  zetasql::Value support_relative_search =
      test_case.support_relative_search.has_value()
          ? zetasql::Value::Bool(test_case.support_relative_search.value())
          : zetasql::Value::NullBool();

  zetasql::Value content_type = zetasql::Value::NullString();

  zetasql::Value search_types;
  if (test_case.relative_search_types.empty()) {
    search_types = zetasql::Value::Null(zetasql::types::StringArrayType());
  } else {
    search_types =
        zetasql::values::StringArray(test_case.relative_search_types);
  }

  std::vector<zetasql::Value> args{value,        ngram_max,
                                     ngram_min,    support_relative_search,
                                     content_type, search_types};

  absl::StatusOr<zetasql::Value> result = SubstringTokenizer::Tokenize(args);
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

  // Expect the tokenlist to contain the original strings. The strings will be
  // tokenized in the search_ngrams_evaluator and the
  // search_substring_evaluator.
  EXPECT_EQ(test_case.expected_result, token_texts);
}

INSTANTIATE_TEST_SUITE_P(
    SubstringTokenizerTest, SubstringTokenizerTest,
    testing::ValuesIn<SubstringTokenizerTestCase>({
        {{"third~party!cloud@spanner#emulator$backend%query^search&simple"},
         4,
         1,
         std::nullopt,
         {},
         "substring-4-1-0-0",
         {"third~party!cloud@spanner#emulator$backend%query^search&simple"},
         false},
        {{"third\rparty\ncloud spanner\temulator"},
         3,
         2,
         std::nullopt,
         {"all"},
         "substring-3-2-0-31",
         {"third\rparty\ncloud spanner\temulator"},
         false},
        {{"3rd g00gle! 1234"},
         2,
         1,
         std::nullopt,
         {"word_prefix"},
         "substring-2-1-0-1",
         {"3rd g00gle! 1234"},
         false},
        {{"GooGle gOOgLE! GOOGLE"},
         4,
         2,
         std::nullopt,
         {"word_prefix", "word_suffix"},
         "substring-4-2-0-3",
         {"google google! google"},
         false},
        {{"GooGle!@#@clOud*^&Spanner<:?"},
         4,
         2,
         std::nullopt,
         {"value_prefix", "word_suffix"},
         "substring-4-2-0-6",
         {"google!@#@cloud*^&spanner<:?"},
         false},
        {{"Google@-=全文+/검색"},
         4,
         2,
         std::nullopt,
         {"word_prefix", "value_suffix"},
         "substring-4-2-0-9",
         {"google@-=全文+/검색"},
         false},
        {{"~!@# ^*&*(    '?/.|"},
         4,
         2,
         std::nullopt,
         {"adjacent_and_in_order"},
         "substring-4-2-0-16",
         {"~!@# ^*&*(    '?/.|"},
         false},
        {{"third~party!cloud@spanner#emulator$backend%query^search&simple"},
         4,
         1,
         std::nullopt,
         {"all", "word_prefix"},
         "substring-4-1-0-31",
         {"third~party!cloud@spanner#emulator$backend%query^search&simple",
          "\x1"},
         true},
        {{"third~party!", "cloud@spanner#emulator",
          "backend%query^search&simple"},
         4,
         1,
         std::nullopt,
         {"word_prefix", "phrase"},
         "substring-4-1-0-17",
         {"third~party!", "\x1", "cloud@spanner#emulator", "\x1",
          "backend%query^search&simple", "\x1"},
         true},
        {{"third~party!cloud@spanner", "",
          "emulator$backend%query^search&simple"},
         4,
         1,
         std::nullopt,
         {"all"},
         "substring-4-1-0-31",
         {"third~party!cloud@spanner", "\x1", "", "\x1",
          "emulator$backend%query^search&simple", "\x1"},
         true},
        {{"~!@# ", "^*&*( ", "   '?/.|"},
         4,
         2,
         false,
         {},
         "substring-4-2-0-0",
         {"~!@# ", "\x1", "^*&*( ", "\x1", "   '?/.|", "\x1"},
         true},
        {{"third~party!cloud@spanner#emulator$backend%query^search&simple"},
         std::nullopt,
         1,
         true,
         {},
         "substring-4-1-0-31",
         {"third~party!cloud@spanner#emulator$backend%query^search&simple"},
         false},
        {{"third~party!cloud@spanner#emulator$backend%query^search&simple"},
         4,
         std::nullopt,
         std::nullopt,
         {},
         "substring-4-1-0-0",
         {"third~party!cloud@spanner#emulator$backend%query^search&simple"},
         false},
    }));

struct SubstringTokenizerNgramTestCase {
  int64_t ngram_size_max;
  int64_t ngram_size_min;
  std::string expected_error;
};

using SubstringTokenizerNgramTest =
    ::testing::TestWithParam<SubstringTokenizerNgramTestCase>;

TEST_P(SubstringTokenizerNgramTest, TestNgramValues) {
  const SubstringTokenizerNgramTestCase& test_case = GetParam();

  std::vector<zetasql::Value> args;
  args.push_back(zetasql::Value::String("test_ngram_values"));
  args.push_back(zetasql::Value::Int64(test_case.ngram_size_max));
  args.push_back(zetasql::Value::Int64(test_case.ngram_size_min));
  args.push_back(zetasql::Value::Bool(false));

  EXPECT_THAT(SubstringTokenizer::Tokenize(args),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr(test_case.expected_error)));
}

INSTANTIATE_TEST_SUITE_P(
    SubstringTokenizerWrongNgramTest, SubstringTokenizerNgramTest,
    testing::ValuesIn<SubstringTokenizerNgramTestCase>({
        {0, 1, "ngram_size_max must be greater than 0."},
        {3, 0, "ngram_size_min must be greater than 0."},
        {3, 4, "ngram_size_min cannot be greater than ngram_size_max."},
        {13, 4, "ngram_size_max cannot be greater than 12."},
    }));

TEST(SubstringTokenizerTest, NullInputValue) {
  absl::StatusOr<zetasql::Value> result =
      SubstringTokenizer::Tokenize({zetasql::Value::NullString()});
  ZETASQL_EXPECT_OK(result.status());

  zetasql::Value token_list = result.value();
  EXPECT_TRUE(token_list.type()->IsTokenList());

  // Always expect the tokenlist has at least one token
  // which stores tokenizer information.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto tokens, StringsFromTokenList(token_list));
  ASSERT_EQ(tokens.size(), 1);
  EXPECT_EQ("substring-4-1-1-0", tokens[0]);
}

TEST(SubstringTokenizerTest, ConflictSupportRelativeSearchArgs) {
  absl::StatusOr<zetasql::Value> result = SubstringTokenizer::Tokenize(
      {zetasql::Value::String("test"), zetasql::Value::NullInt64(),
       zetasql::Value::NullInt64(), zetasql::Value::Bool(true),
       zetasql::Value::NullString(),
       zetasql::values::StringArray({"all"})});
  EXPECT_THAT(result,
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("only one of 'support_relative_search' and "
                                 "'relative_search_types' can be specified.")));

  result = SubstringTokenizer::Tokenize(
      {zetasql::Value::String("test"), zetasql::Value::NullInt64(),
       zetasql::Value::NullInt64(), zetasql::Value::Bool(false),
       zetasql::Value::NullString(),
       zetasql::values::StringArray({"all"})});
  EXPECT_THAT(result,
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("only one of 'support_relative_search' and "
                                 "'relative_search_types' can be specified.")));
}

TEST(SubstringTokenizerTest, InvalidRelativeSearchTypes) {
  absl::StatusOr<zetasql::Value> result = SubstringTokenizer::Tokenize(
      {zetasql::Value::String("test"), zetasql::Value::NullInt64(),
       zetasql::Value::NullInt64(), zetasql::Value::NullBool(),
       zetasql::Value::NullString(),
       zetasql::values::StringArray({"not_supported"})});
  EXPECT_THAT(result, StatusIs(absl::StatusCode::kInvalidArgument,
                               HasSubstr("Invalid relative_search_type")));
}

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
