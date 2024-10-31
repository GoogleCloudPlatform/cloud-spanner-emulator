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

#include "backend/query/search/plain_full_text_tokenizer.h"

#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/statusor.h"
#include "backend/query/search/tokenizer.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

struct TokenizeTestCase {
  std::vector<std::string> inputs;
  std::vector<std::string> expected_result;
  bool is_array_input;

  TokenizeTestCase(std::vector<std::string> inputs,
                   std::vector<std::string> expected)
      : TokenizeTestCase(inputs, expected, /*is_array_input=*/false) {}

  TokenizeTestCase(std::vector<std::string> inputs,
                   std::vector<std::string> expected, bool is_array_input)
      : inputs(inputs),
        expected_result(expected),
        is_array_input(is_array_input) {}
};

void VerifyTestCase(TokenizeTestCase& tc) {
  zetasql::Value value;
  if (tc.is_array_input) {
    value = zetasql::values::StringArray(tc.inputs);
  } else {
    value = zetasql::Value::String(tc.inputs[0]);
  }

  absl::StatusOr<zetasql::Value> result =
      PlainFullTextTokenizer::Tokenize({value});
  ZETASQL_EXPECT_OK(result.status());

  zetasql::Value token_list = result.value();
  EXPECT_TRUE(token_list.type()->IsTokenList());

  // Always expect the tokenlist has at least one token
  // which stores tokenizer information.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto tokens, StringsFromTokenList(token_list));
  ASSERT_FALSE(tokens.empty());
  EXPECT_EQ(tokens[0], "fulltext-0");

  std::vector<std::string> token_texts;
  token_texts.insert(token_texts.end(), tokens.begin() + 1, tokens.end());

  EXPECT_EQ(tc.expected_result, token_texts);
}

TEST(PlainFullTextTokenizerTest, BasicTokenize) {
  std::vector<TokenizeTestCase> test_cases = {
      TokenizeTestCase(
          {"third~party!cloud@spanner#emulator$backend%query^search&simple"},
          {"third", "party", "cloud", "spanner", "emulator", "backend", "query",
           "search", "simple"}),
      TokenizeTestCase(
          {"third*party(cloud)spanner_emulator+backend-query=search|simple"},
          {"third", "party", "cloud", "spanner", "emulator", "backend", "query",
           "search", "simple"}),
      TokenizeTestCase(
          {"third{party}cloud[spanner]emulator\\backend/query\"search`simple"},
          {"third", "party", "cloud", "spanner", "emulator", "backend", "query",
           "search", "simple"}),
      TokenizeTestCase({"third:party;cloud<spanner>emulator,backend.query"},
                       {"third", "party", "cloud", "spanner", "emulator",
                        "backend", "query"}),
      TokenizeTestCase({"third!\"party--cloud!?/"
                        "spanner({})emulator[<backend>]query+=search"},
                       {"third", "party", "cloud", "spanner", "emulator",
                        "backend", "query", "search"}),
  };

  for (auto& tc : test_cases) {
    VerifyTestCase(tc);
  }
}

TEST(PlainFullTextTokenizerTest, WhiteSpaces) {
  std::vector<TokenizeTestCase> test_cases = {
      TokenizeTestCase({"third\rparty\ncloud spanner\temulator"},
                       {"third", "party", "cloud", "spanner", "emulator"}),
      TokenizeTestCase({"third\r\nparty\r cloud \nspanner\t\temulator"},
                       {"third", "party", "cloud", "spanner", "emulator"}),
      TokenizeTestCase({"third \tparty\r\n\r\ncloud\r\n\tspanner\r\n emulator"},
                       {"third", "party", "cloud", "spanner", "emulator"}),
      TokenizeTestCase({" third  \r\n\tparty \r\n cloud \t spanner! emulator"},
                       {"third", "party", "cloud", "spanner", "emulator"}),
      TokenizeTestCase({" third -- party. \"cloud \" spanner += emulator"},
                       {"third", "party", "cloud", "spanner", "emulator"}),
  };

  for (auto& tc : test_cases) {
    VerifyTestCase(tc);
  }
}

TEST(PlainFullTextTokenizerTest, NullTokenList) {
  std::vector<std::string> test_cases = {
      "", " ", "   ", " \t", "\r\n", "!?", " \"\"", "-\"\r\n\" !", " +\t@\r."};

  for (auto& tc : test_cases) {
    absl::StatusOr<zetasql::Value> result =
        PlainFullTextTokenizer::Tokenize({zetasql::Value::String(tc)});
    ZETASQL_EXPECT_OK(result.status());

    zetasql::Value token_list = result.value();
    EXPECT_TRUE(token_list.type()->IsTokenList());

    ZETASQL_ASSERT_OK_AND_ASSIGN(auto tokens, StringsFromTokenList(token_list));
    ASSERT_EQ(tokens.size(), 1);
    // The sources have value as empty strings but not null. The signature uses
    // 0 to mark it.
    EXPECT_EQ(tokens[0], "fulltext-0");
  }
}

TEST(PlainFullTextTokenizerTest, AlphaNumeric) {
  TokenizeTestCase test_cases =
      TokenizeTestCase({"3rd g00gle! 1234"}, {"3rd", "g00gle", "1234"});

  VerifyTestCase(test_cases);
}

TEST(PlainFullTextTokenizerTest, ToLowerCases) {
  TokenizeTestCase test_cases = TokenizeTestCase(
      {"GooGle gOOgLE! GOOGLE"}, {"google", "google", "google"});

  VerifyTestCase(test_cases);
}

TEST(PlainFullTextTokenizerTest, NullInputValue) {
  absl::StatusOr<zetasql::Value> result =
      PlainFullTextTokenizer::Tokenize({zetasql::Value::NullString()});
  ZETASQL_EXPECT_OK(result.status());

  zetasql::Value token_list = result.value();
  EXPECT_TRUE(token_list.type()->IsTokenList());

  // Always expect the tokenlist has at least one token
  // which stores tokenizer information.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto tokens, StringsFromTokenList(token_list));
  ASSERT_EQ(tokens.size(), 1);
  // The signature uses 1 to indicate that the source is NULL.
  EXPECT_EQ(tokens[0], "fulltext-1");
}

TEST(PlainFullTextTokenizerTest, TokenizeArray) {
  std::vector<TokenizeTestCase> test_cases = {
      TokenizeTestCase(
          {"third~party", "!cloud@spanner", "emulator$backend%query",
           "^search&simple"},
          {"third", "party", "\x1", "cloud", "spanner", "\x1", "emulator",
           "backend", "query", "\x1", "search", "simple", "\x1"},
          true),
      TokenizeTestCase({"third*party(cloud)spanner_emulator", "",
                        "backend-query=search|simple"},
                       {"third", "party", "cloud", "spanner", "emulator", "\x1",
                        "\x1", "backend", "query", "search", "simple", "\x1"},
                       true),
      TokenizeTestCase(
          {"third{party}cloud[spanner]emulator\\backend/query\"search`simple"},
          {"third", "party", "cloud", "spanner", "emulator", "backend", "query",
           "search", "simple", "\x1"},
          true),
      TokenizeTestCase({"*&&", "  *&", ")(&(^*&^&%))"}, {"\x1", "\x1", "\x1"},
                       true),
  };

  for (auto& tc : test_cases) {
    VerifyTestCase(tc);
  }
}

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
