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

#include "backend/query/search/exact_match_tokenizer.h"

#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "backend/query/search/tokenizer.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

using googlesql_base::testing::StatusIs;

void ValidateResult(absl::StatusOr<googlesql::Value>& result) {
  GOOGLESQL_EXPECT_OK(result.status());

  googlesql::Value token_list = result.value();
  EXPECT_TRUE(token_list.type()->IsTokenList());

  // For exact_match tokenized column, since no operation is supported on the
  // column, we don't store original text but only the tokenizer information.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto tokens, StringsFromTokenList(token_list));
  ASSERT_EQ(tokens.size(), 1);
  EXPECT_EQ(tokens[0], "exact_match");
}

TEST(ExactMatchTokenizerTest, TestTokenize) {
  std::vector<std::string> original_strs = {{"Hello"},
                                            {"Hello World!"},
                                            {"Hello    World!"},
                                            {"   Hello-World! "},
                                            {" "},
                                            {"   "},
                                            {""}};

  for (auto& s : original_strs) {
    absl::StatusOr<googlesql::Value> result =
        ExactMatchTokenizer::Tokenize({googlesql::Value::String(s)});

    ValidateResult(result);
  }
}

TEST(ExactMatchTokenizerTest, TestArgNotString) {
  EXPECT_THAT(ExactMatchTokenizer::Tokenize({googlesql::Value::Int64(64)}),
              StatusIs(absl::StatusCode::kInternal));
}

TEST(ExactMatchTokenizerTest, TestTokenizeNull) {
  absl::StatusOr<googlesql::Value> result =
      ExactMatchTokenizer::Tokenize({googlesql::Value::NullString()});

  ValidateResult(result);
}

TEST(ExactMatchTokenizerTest, TestTokenizeArray) {
  std::vector<googlesql::Value> values = {googlesql::Value::String("Hello"),
                                          googlesql::Value::String("World!")};
  absl::StatusOr<googlesql::Value> make_array_result =
      googlesql::Value::MakeArray(googlesql::types::StringArrayType(), values);
  GOOGLESQL_ASSERT_OK(make_array_result.status());

  googlesql::Value array_val = make_array_result.value();
  absl::StatusOr<googlesql::Value> result =
      ExactMatchTokenizer::Tokenize({array_val});
  ValidateResult(result);
}

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
