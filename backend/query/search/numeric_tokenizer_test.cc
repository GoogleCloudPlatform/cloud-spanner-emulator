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

#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "backend/query/search/tokenizer.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

struct NumericTokenizerTestCase {
  std::vector<zetasql::Value> tokenize_args;
};

using NumericTokenizerTest = ::testing::TestWithParam<NumericTokenizerTestCase>;

TEST_P(NumericTokenizerTest, TestTokenize) {
  const NumericTokenizerTestCase& test_case = GetParam();

  absl::StatusOr<zetasql::Value> result =
      NumericTokenizer::Tokenize(test_case.tokenize_args);
  ZETASQL_EXPECT_OK(result.status());

  zetasql::Value token_list = result.value();
  EXPECT_TRUE(token_list.type()->IsTokenList());

  // For numeric tokenized column, since no operation is supported on the
  // column, we don't store original text but only the tokenizer information.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto tokens, StringsFromTokenList(token_list));
  ASSERT_EQ(tokens.size(), 1);
  EXPECT_EQ(tokens[0], "numeric");
}

INSTANTIATE_TEST_SUITE_P(
    NumericTokenizerTest, NumericTokenizerTest,
    testing::ValuesIn<NumericTokenizerTestCase>(
        {{{zetasql::Value::Int64(8)}},
         {{zetasql::Value::Int64(8), zetasql::Value::String("RANGE")}},
         {{zetasql::Value::Int64(8), zetasql::Value::String("RANGE"),
           zetasql::Value::String("logtree")}},
         {{zetasql::Value::Double(8), zetasql::Value::String("RANGE"),
           zetasql::Value::String("logtree"),
           zetasql::Value::Double(std::numeric_limits<double>::min())}},
         {{zetasql::Value::Int64(8), zetasql::Value::String("ALL"),
           zetasql::Value::String("logtree"),
           zetasql::Value::Int64(std::numeric_limits<int64_t>::min()),
           zetasql::Value::Int64(std::numeric_limits<int64_t>::max())}},
         {{zetasql::Value::Int64(8), zetasql::Value::String("ALL"),
           zetasql::Value::String("logtree"),
           zetasql::Value::Int64(std::numeric_limits<int64_t>::min()),
           zetasql::Value::Int64(std::numeric_limits<int64_t>::max()),
           zetasql::Value::Int64(2)}},
         {{zetasql::Value::Int64(8), zetasql::Value::String("ALL"),
           zetasql::Value::String("auto"),
           zetasql::Value::Int64(std::numeric_limits<int64_t>::min()),
           zetasql::Value::Int64(std::numeric_limits<int64_t>::max()),
           zetasql::Value::Int64(2), zetasql::Value::Int64(4)}},
         {{zetasql::Value::Int64(8), zetasql::Value::String("ALL"),
           zetasql::Value::String("auto"),
           zetasql::Value::Int64(std::numeric_limits<int64_t>::min()),
           zetasql::Value::Int64(std::numeric_limits<int64_t>::max()),
           zetasql::Value::Int64(2), zetasql::Value::Int64(4),
           zetasql::Value::Int64(8)}},
         {{zetasql::Value::Int64(8), zetasql::Value::String("ALL"),
           zetasql::Value::String("auto"), zetasql::Value::NullInt64(),
           zetasql::Value::NullInt64(), zetasql::Value::Double(2),
           zetasql::Value::Int64(4), zetasql::Value::Int64(8)}}}));

// TODO: Add more code and test to check the parameter values.

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
