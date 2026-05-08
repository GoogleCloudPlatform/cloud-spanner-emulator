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

#include "backend/query/search/numeric_tokenizer.h"

#include <cstdint>
#include <limits>
#include <vector>

#include "googlesql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "backend/query/search/tokenizer.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

struct NumericTokenizerTestCase {
  std::vector<googlesql::Value> tokenize_args;
};

using NumericTokenizerTest = ::testing::TestWithParam<NumericTokenizerTestCase>;

TEST_P(NumericTokenizerTest, TestTokenize) {
  const NumericTokenizerTestCase& test_case = GetParam();

  absl::StatusOr<googlesql::Value> result =
      NumericTokenizer::Tokenize(test_case.tokenize_args);
  GOOGLESQL_EXPECT_OK(result.status());

  googlesql::Value token_list = result.value();
  EXPECT_TRUE(token_list.type()->IsTokenList());

  // For numeric tokenized column, since no operation is supported on the
  // column, we don't store original text but only the tokenizer information.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto tokens, StringsFromTokenList(token_list));
  ASSERT_EQ(tokens.size(), 1);
  EXPECT_EQ(tokens[0], "numeric");
}

INSTANTIATE_TEST_SUITE_P(
    NumericTokenizerTest, NumericTokenizerTest,
    testing::ValuesIn<NumericTokenizerTestCase>(
        {{{googlesql::Value::Int64(8)}},
         {{googlesql::Value::Int64(8), googlesql::Value::String("RANGE")}},
         {{googlesql::Value::Int64(8), googlesql::Value::String("RANGE"),
           googlesql::Value::String("logtree")}},
         {{googlesql::Value::Double(8), googlesql::Value::String("RANGE"),
           googlesql::Value::String("logtree"),
           googlesql::Value::Double(std::numeric_limits<double>::min())}},
         {{googlesql::Value::Int64(8), googlesql::Value::String("ALL"),
           googlesql::Value::String("logtree"),
           googlesql::Value::Int64(std::numeric_limits<int64_t>::min()),
           googlesql::Value::Int64(std::numeric_limits<int64_t>::max())}},
         {{googlesql::Value::Int64(8), googlesql::Value::String("ALL"),
           googlesql::Value::String("logtree"),
           googlesql::Value::Int64(std::numeric_limits<int64_t>::min()),
           googlesql::Value::Int64(std::numeric_limits<int64_t>::max()),
           googlesql::Value::Int64(2)}},
         {{googlesql::Value::Int64(8), googlesql::Value::String("ALL"),
           googlesql::Value::String("auto"),
           googlesql::Value::Int64(std::numeric_limits<int64_t>::min()),
           googlesql::Value::Int64(std::numeric_limits<int64_t>::max()),
           googlesql::Value::Int64(2), googlesql::Value::Int64(4)}},
         {{googlesql::Value::Int64(8), googlesql::Value::String("ALL"),
           googlesql::Value::String("auto"),
           googlesql::Value::Int64(std::numeric_limits<int64_t>::min()),
           googlesql::Value::Int64(std::numeric_limits<int64_t>::max()),
           googlesql::Value::Int64(2), googlesql::Value::Int64(4),
           googlesql::Value::Int64(8)}},
         {{googlesql::Value::Int64(8), googlesql::Value::String("ALL"),
           googlesql::Value::String("auto"), googlesql::Value::NullInt64(),
           googlesql::Value::NullInt64(), googlesql::Value::Double(2),
           googlesql::Value::Int64(4), googlesql::Value::Int64(8)}}}));

// TODO: Add more code and test to check the parameter values.

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
