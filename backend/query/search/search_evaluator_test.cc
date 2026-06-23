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

#include "backend/query/search/search_evaluator.h"

#include <optional>
#include <string>
#include <vector>

#include "googlesql/public/value.h"
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

using testing::HasSubstr;
using googlesql_base::testing::StatusIs;

// The test suite focuses on verifying the safety code that handles unexpected
// input for SearchEvaluator class since they indicate abnormal status
// in the process. Normal test scenarios (including customer misusing of the
// function) are covered in search_test.cc.

TEST(SearchEvaluatorTest, EvaluateWrongSearchColumnType) {
  std::vector<googlesql::Value> args;
  args.push_back(googlesql::Value::Bool(false));
  args.push_back(googlesql::Value::String("test"));

  EXPECT_THAT(
      SearchEvaluator::Evaluate(args),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("Invalid search query. Trying to execute search related "
                    "function on unsupported column type: BOOL.")));
}

TEST(SearchEvaluatorTest, EvaluateWrongSearchQueryType) {
  std::vector<googlesql::Value> args;
  args.push_back(googlesql::Value::NullTokenList());
  args.push_back(googlesql::Value::Bool(false));

  EXPECT_THAT(SearchEvaluator::Evaluate(args),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid search query type: BOOL.")));
}

TEST(SearchEvaluatorTest, EvaluateInvalidDialect) {
  std::vector<googlesql::Value> args;
  args.push_back(TokenListFromStrings({"fulltext-0", "cloud"}));
  args.push_back(googlesql::Value::String("query"));
  args.push_back(googlesql::Value::NullBool());                 // enhance_query
  args.push_back(googlesql::Value::NullString());               // language_tag
  args.push_back(googlesql::Value::String("invalid_dialect"));  // dialect

  EXPECT_THAT(SearchEvaluator::Evaluate(args),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid dialect: invalid_dialect")));
}

struct DialectTestCase {
  std::vector<std::string> tokens;
  std::string query;
  std::optional<std::string> dialect;
  bool expected_result;
};

class DialectTest : public ::testing::TestWithParam<DialectTestCase> {};

TEST_P(DialectTest, TestEvaluation) {
  const DialectTestCase& test_case = GetParam();

  std::vector<std::string> tokens = test_case.tokens;
  tokens.insert(tokens.begin(), "fulltext-0");
  googlesql::Value tokenlist = TokenListFromStrings(tokens);

  std::vector<googlesql::Value> args;
  args.push_back(tokenlist);
  args.push_back(googlesql::Value::String(test_case.query));
  args.push_back(googlesql::Value::NullBool());    // enhance_query
  args.push_back(googlesql::Value::NullString());  // language_tag
  if (!test_case.dialect.has_value()) {
    args.push_back(googlesql::Value::NullString());
  } else {
    args.push_back(googlesql::Value::String(*test_case.dialect));
  }

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(const googlesql::Value result,
                       SearchEvaluator::Evaluate(args));
  EXPECT_EQ(test_case.expected_result, result.bool_value());
}

INSTANTIATE_TEST_SUITE_P(
    WordsDialectBasicTests, DialectTest,
    testing::ValuesIn<DialectTestCase>({
        {{"cloud", "spanner", "emulator"}, "cloud spanner", "words", true},
        {{"cloud", "spanner", "emulator"}, "cloud postgres", "WORDS", false},
        {{"cloud", "spanner", "emulator"}, "CLOUD SPANNER", "WoRdS", true},
        {{"cloud", "spanner", "emulator"}, "spanner cloud", "words", true},
    }));

INSTANTIATE_TEST_SUITE_P(WordsDialectEmptyQueryTests, DialectTest,
                         testing::ValuesIn<DialectTestCase>({
                             {{"cloud"}, "", "words", false},
                             {{"cloud"}, "###", "words", false},
                         }));

INSTANTIATE_TEST_SUITE_P(WordsDialectNonAsciiTests, DialectTest,
                         testing::ValuesIn<DialectTestCase>({
                             {{"谷歌"}, "谷歌", "words", true},
                             {{"你好", "谷歌"}, "你好 谷歌", "words", true},
                             {{"你好", "谷歌"}, "谷歌 你好", "words", true},
                         }));

INSTANTIATE_TEST_SUITE_P(
    WordsDialectNoOperatorSupportTests, DialectTest,
    testing::ValuesIn<DialectTestCase>({
        {{"cloud", "emulator"}, "cloud | spanner", "words", false},
        {{"cloud", "spanner"}, "cloud OR spanner", "words", false},
        {{"cloud", "or", "spanner"}, "cloud OR spanner", "words", true},
        {{"cloud", "spanner"}, "-cloud", "words", true},
        {{"spanner"}, "-cloud", "words", false},
    }));

INSTANTIATE_TEST_SUITE_P(WordsPhraseDialectBasicTests, DialectTest,
                         testing::ValuesIn<DialectTestCase>({
                             {{"cloud", "spanner", "emulator"},
                              "cloud spanner",
                              "WORDS_PHRASE",
                              true},
                             {{"cloud", "spanner", "emulator"},
                              "spanner cloud",
                              "WoRdS_PhRaSe",
                              false},
                             {{"cloud", "spanner", "emulator"},
                              "cloud emulator",
                              "words_phrase",
                              false},
                             {{"cloud", "spanner", "emulator"},
                              "CLOUD SPANNER",
                              "words_phrase",
                              true},
                         }));

INSTANTIATE_TEST_SUITE_P(WordsPhraseDialectEmptyQueryTests, DialectTest,
                         testing::ValuesIn<DialectTestCase>({
                             {{"cloud"}, "", "words_phrase", false},
                             {{"cloud"}, "###", "words_phrase", false},
                         }));

INSTANTIATE_TEST_SUITE_P(
    WordsPhraseDialectNonAsciiTests, DialectTest,
    testing::ValuesIn<DialectTestCase>({
        {{"谷歌"}, "谷歌", "words_phrase", true},
        {{"你好", "谷歌"}, "你好 谷歌", "words_phrase", true},
        {{"你好", "谷歌"}, "谷歌 你好", "words_phrase", false},
    }));

INSTANTIATE_TEST_SUITE_P(
    WordsPhraseDialectNoOperatorSupportTests, DialectTest,
    testing::ValuesIn<DialectTestCase>({
        {{"cloud", "spanner"}, "cloud | spanner", "words_phrase", true},
        {{"cloud", "other", "spanner"},
         "cloud | spanner",
         "words_phrase",
         false},
        {{"cloud", "spanner"}, "cloud OR spanner", "words_phrase", false},
        {{"cloud", "or", "spanner"}, "cloud OR spanner", "words_phrase", true},
    }));

INSTANTIATE_TEST_SUITE_P(
    WordsPhraseDialectNoSubstringMatchingTests, DialectTest,
    testing::ValuesIn<DialectTestCase>({
        {{"cloud", "emulator"}, "cloud emu", "words_phrase", false},
        {{"cloud", "emulator"}, "emu", "words_phrase", false},
    }));

INSTANTIATE_TEST_SUITE_P(
    RQueryDialectBasicAndTests, DialectTest,
    testing::ValuesIn<DialectTestCase>({
        {{"cloud", "spanner", "emulator"}, "cloud spanner", "rquery", true},
        {{"cloud", "spanner", "emulator"}, "spanner cloud", "RQUERY", true},
        {{"cloud", "spanner", "emulator"}, "cloud postgres", "RquErY", false},
    }));

INSTANTIATE_TEST_SUITE_P(
    RQueryDialectOrOperatorTests, DialectTest,
    testing::ValuesIn<DialectTestCase>({
        {{"cloud", "emulator"}, "cloud | spanner", "rquery", true},
        {{"spanner", "emulator"}, "cloud | spanner", "rquery", true},
        {{"emulator"}, "cloud | spanner", "rquery", false},
        {{"cloud", "emulator"}, "cloud OR spanner", "rquery", true},
    }));

INSTANTIATE_TEST_SUITE_P(
    RQueryDialectNotOperatorTests, DialectTest,
    testing::ValuesIn<DialectTestCase>({
        {{"spanner", "emulator"}, "-cloud spanner", "rquery", true},
        {{"cloud", "spanner", "emulator"}, "-cloud spanner", "rquery", false},
    }));

INSTANTIATE_TEST_SUITE_P(
    RQueryDialectAroundOperatorTests, DialectTest,
    testing::ValuesIn<DialectTestCase>({
        {{"cloud", "spanner"}, "cloud AROUND(1) spanner", "rquery", true},
        {{"cloud", "other", "spanner"},
         "cloud AROUND(1) spanner",
         "rquery",
         true},
        {{"cloud", "other1", "other2", "spanner"},
         "cloud AROUND(1) spanner",
         "rquery",
         false},
        {{"cloud", "other1", "other2", "spanner"},
         "cloud AROUND(2) spanner",
         "rquery",
         true},
    }));

INSTANTIATE_TEST_SUITE_P(
    RQueryDialectPhraseTests, DialectTest,
    testing::ValuesIn<DialectTestCase>({
        {{"cloud", "spanner"}, "\"cloud spanner\"", "rquery", true},
        {{"spanner", "cloud"}, "\"cloud spanner\"", "rquery", false},
        {{"cloud", "other", "spanner"}, "\"cloud spanner\"", "rquery", false},
    }));

INSTANTIATE_TEST_SUITE_P(RQueryDialectEmptyQueryTests, DialectTest,
                         testing::ValuesIn<DialectTestCase>({
                             {{"cloud"}, "", "rquery", false},
                             {{"cloud"}, "###", "rquery", false},
                         }));

INSTANTIATE_TEST_SUITE_P(
    DefaultDialectIsRQuery, DialectTest,
    testing::ValuesIn<DialectTestCase>({
        {{"cloud", "emulator"}, "cloud | spanner", std::nullopt, true},
        {{"spanner", "emulator"}, "-cloud spanner", std::nullopt, true},
    }));

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
