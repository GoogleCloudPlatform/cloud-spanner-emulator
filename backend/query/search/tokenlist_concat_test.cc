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

#include "backend/query/search/tokenlist_concat.h"

#include <string>
#include <vector>

#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/query/search/plain_full_text_tokenizer.h"
#include "backend/query/search/substring_tokenizer.h"
#include "backend/query/search/tokenizer.h"

namespace google::spanner::emulator::backend::query::search {

using testing::HasSubstr;
using zetasql_base::testing::StatusIs;

void ValidateTokenlistConcat(zetasql::Value tokenlist1,
                             zetasql::Value tokenlist2,
                             zetasql::Value concat_tokenlist) {
  std::vector<std::string> expected_tokens;
  const auto tokenlist1_tokens = StringsFromTokenList(tokenlist1);
  const auto tokenlist2_tokens = StringsFromTokenList(tokenlist2);
  expected_tokens.insert(expected_tokens.end(), tokenlist1_tokens->begin(),
                         tokenlist1_tokens->end());
  expected_tokens.push_back(kGapString);
  expected_tokens.insert(expected_tokens.end(), tokenlist2_tokens->begin(),
                         tokenlist2_tokens->end());
  expected_tokens.push_back(kGapString);

  EXPECT_EQ(expected_tokens, *StringsFromTokenList(concat_tokenlist));
}

TEST(TokenlistConcatTest, ConcatFulltext) {
  const auto tokenlist1 =
      PlainFullTextTokenizer::Tokenize({zetasql::Value::String("foo bar")});
  const auto tokenlist2 = PlainFullTextTokenizer::Tokenize(
      {zetasql::Value::String("hello world")});

  // Create a tokenlist array. null tokenlist will be ignored.
  absl::StatusOr<zetasql::Value> tokenlist_array =
      zetasql::Value::MakeArray(
          zetasql::types::TokenListArrayType(),
          {tokenlist1.value(), zetasql::Value::NullTokenList(),
           tokenlist2.value()});
  const auto concat_tokenlist =
      TokenlistConcat::Concat({tokenlist_array.value()});

  ValidateTokenlistConcat(*tokenlist1, *tokenlist2, *concat_tokenlist);
}

TEST(TokenlistConcatTest, ConcatSubstring) {
  const auto tokenlist1 =
      SubstringTokenizer::Tokenize({zetasql::Value::String("foobar")});
  const auto tokenlist2 =
      SubstringTokenizer::Tokenize({zetasql::Value::String("helloworld")});

  absl::StatusOr<zetasql::Value> tokenlist_array =
      zetasql::Value::MakeArray(zetasql::types::TokenListArrayType(),
                                  {tokenlist1.value(), tokenlist2.value()});
  const auto concat_tokenlist =
      TokenlistConcat::Concat({tokenlist_array.value()});

  ValidateTokenlistConcat(*tokenlist1, *tokenlist2, *concat_tokenlist);
}

TEST(TokenlistConcatTest, ConcatNull) {
  const auto concat_tokenlist = TokenlistConcat::Concat(
      {zetasql::Value::Null(zetasql::types::TokenListArrayType())});
  EXPECT_TRUE(concat_tokenlist->is_null());
}

TEST(TokenlistConcatTest, UnmatchSubstringSignature) {
  const auto tokenlist1 = SubstringTokenizer::Tokenize(
      {zetasql::Value::String("foobar"), zetasql::Value::Int64(3),
       zetasql::Value::Int64(2)});
  const auto tokenlist2 = SubstringTokenizer::Tokenize(
      {zetasql::Value::String("foobar"), zetasql::Value::Int64(4),
       zetasql::Value::Int64(1)});
  absl::StatusOr<zetasql::Value> tokenlist_array =
      zetasql::Value::MakeArray(zetasql::types::TokenListArrayType(),
                                  {tokenlist1.value(), tokenlist2.value()});
  EXPECT_THAT(
      TokenlistConcat::Concat({tokenlist_array.value()}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("All elements to TOKENLIST_CONCAT must be produced by "
                         "the same kind of tokenization function.")));
}

}  // namespace google::spanner::emulator::backend::query::search
