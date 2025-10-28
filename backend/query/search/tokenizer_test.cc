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

#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {
namespace {

using ::testing::ElementsAre;
using ::testing::UnorderedElementsAre;

TEST(TokenizerTest, TokenizeSubstringEmpty) {
  std::vector<std::string> tokens;
  ZETASQL_ASSERT_OK(TokenizeSubstring("", tokens));
  EXPECT_TRUE(tokens.empty());
}

TEST(TokenizerTest, TokenizeSubstringSimple) {
  std::vector<std::string> tokens;
  ZETASQL_ASSERT_OK(TokenizeSubstring("Hello World", tokens));
  EXPECT_THAT(tokens, ElementsAre("Hello", "World"));
}

TEST(TokenizerTest, TokenizeSubstringWithDelimiters) {
  std::vector<std::string> tokens;
  ZETASQL_ASSERT_OK(TokenizeSubstring("foo-bar baz,qux", tokens));
  EXPECT_THAT(tokens, ElementsAre("foo", "bar", "baz", "qux"));
}

TEST(TokenizerTest, TokenizeSubstringSkipsWhitespace) {
  std::vector<std::string> tokens;
  ZETASQL_ASSERT_OK(
      TokenizeSubstring("  leading\n trailing  \tmultiple---delims", tokens));
  EXPECT_THAT(tokens, ElementsAre("leading", "trailing", "multiple", "delims"));
}

TEST(TokenizerTest, TokenizeSubstringKeepsNonAscii) {
  std::vector<std::string> tokens;
  ZETASQL_ASSERT_OK(TokenizeSubstring("Grüße-Welt", tokens));
  EXPECT_THAT(tokens, ElementsAre("Grüße", "Welt"));
}

TEST(TokenizerTest, TokenizeSubstringAppends) {
  std::vector<std::string> tokens = {"existing"};
  ZETASQL_ASSERT_OK(TokenizeSubstring("new tokens", tokens));
  EXPECT_THAT(tokens, ElementsAre("existing", "new", "tokens"));
}

TEST(TokenizerTest, TokenizeSubstringWithCJK) {
  std::vector<std::string> tokens;
  ZETASQL_ASSERT_OK(TokenizeSubstring("你好-世界", tokens));
  EXPECT_THAT(tokens, ElementsAre("你好", "世界"));
}

TEST(TokenizerTest, TokenizeSubstringWithMoreDelimiters) {
  std::vector<std::string> tokens;
  ZETASQL_ASSERT_OK(TokenizeSubstring("a@b#c_d=e[f]g", tokens));
  EXPECT_THAT(tokens, ElementsAre("a", "b", "c", "d", "e", "f", "g"));
}

TEST(TokenizerTest, TokenizeNgramsEmpty) {
  std::vector<std::string> tokens;
  ZETASQL_ASSERT_OK(TokenizeNgrams("", 1, 1, tokens));
  EXPECT_TRUE(tokens.empty());
}

TEST(TokenizerTest, TokenizeNgramsSimple) {
  std::vector<std::string> tokens;
  ZETASQL_ASSERT_OK(TokenizeNgrams("abc", 1, 1, tokens));
  EXPECT_THAT(tokens, ElementsAre("a", "b", "c"));
}

TEST(TokenizerTest, TokenizeNgramsMinMax) {
  std::vector<std::string> tokens;
  ZETASQL_ASSERT_OK(TokenizeNgrams("google", 3, 4, tokens));
  EXPECT_THAT(tokens, UnorderedElementsAre("goo", "oog", "ogl", "gle", "goog",
                                           "oogl", "ogle"));
}

TEST(TokenizerTest, TokenizeNgramsMinGreaterThanMax) {
  std::vector<std::string> tokens;
  ZETASQL_ASSERT_OK(TokenizeNgrams("google", 4, 3, tokens));
  EXPECT_TRUE(tokens.empty());
}

TEST(TokenizerTest, TokenizeNgramsSizeGreaterThanStringLength) {
  std::vector<std::string> tokens;
  ZETASQL_ASSERT_OK(TokenizeNgrams("abc", 4, 5, tokens));
  EXPECT_TRUE(tokens.empty());
}

TEST(TokenizerTest, TokenizeNgramsHandlesUppercase) {
  std::vector<std::string> tokens;
  ZETASQL_ASSERT_OK(TokenizeNgrams("ABC", 2, 2, tokens));
  EXPECT_THAT(tokens, ElementsAre("AB", "BC"));
}

TEST(TokenizerTest, TokenizeNgramsAppends) {
  std::vector<std::string> tokens = {"existing"};
  ZETASQL_ASSERT_OK(TokenizeNgrams("abc", 1, 1, tokens));
  EXPECT_THAT(tokens, ElementsAre("existing", "a", "b", "c"));
}

}  // namespace
}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
