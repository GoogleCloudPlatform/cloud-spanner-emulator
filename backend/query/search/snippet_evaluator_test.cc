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

#include "backend/query/search/snippet_evaluator.h"

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/json_value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/types/span.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

namespace {

using testing::HasSubstr;
using zetasql_base::testing::StatusIs;

TEST(SnippetEvaluatorTest, EvaluateWrongSnippetColumnType) {
  std::vector<zetasql::Value> args{zetasql::Value::Bool(false),
                                     zetasql::Value::String("test")};

  EXPECT_THAT(SnippetEvaluator::Evaluate(args),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Unable to execute snippet function on "
                                 "unsupported column type: BOOL.")));
}

TEST(SnippetEvaluatorTest, EvaluateWrongSnippetQueryType) {
  std::vector<zetasql::Value> args{zetasql::Value::String("foo bar baz"),
                                     zetasql::Value::Bool(false)};

  EXPECT_THAT(SnippetEvaluator::Evaluate(args),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid snippet query type: BOOL.")));
}

TEST(SnippetEvaluatorTest, EvaluateNullSource) {
  std::vector<zetasql::Value> args{zetasql::Value::NullString(),
                                     zetasql::Value::String("foo")};

  absl::StatusOr<std::optional<std::string>> result =
      SnippetEvaluator::Evaluate(args);
  ZETASQL_EXPECT_OK(result.status());
  std::optional<std::string> snippets = result.value();
  EXPECT_FALSE(snippets.has_value());
}

TEST(SnippetEvaluatorTest, EvaluateNullQuery) {
  std::vector<zetasql::Value> args{zetasql::Value::String("foo bar"),
                                     zetasql::Value::NullString()};

  absl::StatusOr<std::optional<std::string>> result =
      SnippetEvaluator::Evaluate(args);
  ZETASQL_EXPECT_OK(result.status());

  std::optional<std::string> snippets = result.value();
  EXPECT_FALSE(snippets.has_value());
}

struct SnippetEvaluatorTestCase {
  std::string target;
  std::string query;
  std::optional<int> max_snippet_length;
  std::optional<int> max_snippets;
  std::string expected_result;
};

using SnippetEvaluatorTest = ::testing::TestWithParam<SnippetEvaluatorTestCase>;

TEST_P(SnippetEvaluatorTest, TestEvaluation) {
  const SnippetEvaluatorTestCase& test_case = GetParam();

  zetasql::Value max_snippet_length =
      test_case.max_snippet_length.has_value()
          ? zetasql::Value::Int64(*test_case.max_snippet_length)
          : zetasql::Value::NullInt64();
  zetasql::Value max_snippets =
      test_case.max_snippets.has_value()
          ? zetasql::Value::Int64(*test_case.max_snippets)
          : zetasql::Value::NullInt64();
  std::vector<zetasql::Value> args{zetasql::Value::String(test_case.target),
                                     zetasql::Value::String(test_case.query),
                                     zetasql::Value::Bool(false),
                                     zetasql::Value::String("AUTO"),
                                     max_snippet_length,
                                     max_snippets};

  absl::StatusOr<std::optional<std::string>> result =
      SnippetEvaluator::Evaluate(args);
  ZETASQL_EXPECT_OK(result.status());

  std::optional<std::string> snippets = result.value();
  EXPECT_TRUE(snippets.has_value());
  EXPECT_EQ(test_case.expected_result, snippets.value());
}

std::string GetExpectedResult(absl::Span<const std::pair<int, int>> positions,
                              absl::string_view snippet) {
  static constexpr absl::string_view positions_format =
      "{\"end_position\":$0,\"start_position\":$1}";
  static constexpr absl::string_view snippets_format =
      "{\"snippets\":[{\"highlights\":[$0],\"snippet\":\"$1\"}]}";

  std::string highlights = absl::StrJoin(
      positions, ",", [](std::string* result, const std::pair<int, int>& pos) {
        return result->append(
            absl::Substitute(positions_format, pos.first, pos.second));
      });

  std::string result = absl::Substitute(snippets_format, highlights, snippet);

  return result;
}

INSTANTIATE_TEST_SUITE_P(
    SnippetEvaluatorTest, SnippetEvaluatorTest,
    testing::ValuesIn<SnippetEvaluatorTestCase>(
        {{"foo", "foo", 160, 3, GetExpectedResult({{4, 1}}, "foo")},
         {"foo bar", "foo", 160, 3, GetExpectedResult({{4, 1}}, "foo bar")},
         {"foo foo", "foo", 160, 3,
          GetExpectedResult({{4, 1}, {8, 5}}, "foo foo")},
         {"foo foo", "foo", 7, 3, GetExpectedResult({{4, 1}}, "foo")},
         {"foo foo", "foo", 8, 3,
          GetExpectedResult({{4, 1}, {8, 5}}, "foo foo")},
         {"foo bar foo", "foo", 160, 3,
          GetExpectedResult({{4, 1}, {12, 9}}, "foo bar foo")},
         {"foo bar foo", "foo", std::nullopt, std::nullopt,
          GetExpectedResult({{4, 1}, {12, 9}}, "foo bar foo")},
         {"foo bar foo", "foo", 7, 3, GetExpectedResult({{4, 1}}, "foo")},
         {"foo bar foo", "bar", 6, 3, GetExpectedResult({{4, 1}}, "bar")},
         {"foo", "", 160, 3, GetExpectedResult({}, "foo")},
         {"foo foo", "fo", 160, 3, GetExpectedResult({}, "foo foo")},
         {"foo", "foo", 2, 3, GetExpectedResult({}, "")},
         {"foo foo foo", "foo", 160, 3,
          GetExpectedResult({{4, 1}, {8, 5}, {12, 9}}, "foo foo foo")},
         {"foo foo", "foo", 4, 3, GetExpectedResult({{4, 1}}, "foo")},
         {"foo foo", "foo", 5, 3, GetExpectedResult({{4, 1}}, "foo")},
         {"bar foo", "foo", 5, 3, GetExpectedResult({{4, 1}}, "foo")},
         {"bar, bar foo", "foo", 7, 3, GetExpectedResult({{4, 1}}, "foo")},
         {"bar, bar foo", "foo", 8, 3, GetExpectedResult({{8, 5}}, "bar foo")},
         {"bar foo foo", "bar", 8, 3, GetExpectedResult({{4, 1}}, "bar foo")},
         {"bar foo", "foo bar", 120, 3,
          GetExpectedResult({{4, 1}, {8, 5}}, "bar foo")},
         {"bar foo", "BaR", 5, 3, GetExpectedResult({{4, 1}}, "bar")},
         {"bar foobar foo", "foobar", 5, 3, GetExpectedResult({}, "")}}));

struct SnippetContentTypeTestCase {
  std::string content_type;
};

using SnippetContentTypeTest =
    ::testing::TestWithParam<SnippetContentTypeTestCase>;

TEST_P(SnippetContentTypeTest, TestContentType) {
  const SnippetContentTypeTestCase& test_case = GetParam();
  std::vector<zetasql::Value> args{
      zetasql::Value::String("foo"),
      zetasql::Value::String("foo"),
      zetasql::Value::Bool(false),
      zetasql::Value::String("AUTO"),
      zetasql::Value::Int64(160),
      zetasql::Value::Int64(3),
      zetasql::Value::String(test_case.content_type)};

  absl::StatusOr<std::optional<std::string>> result =
      SnippetEvaluator::Evaluate(args);
  ZETASQL_EXPECT_OK(result.status());
  std::optional<std::string> snippets = result.value();
  EXPECT_TRUE(snippets.has_value());
  EXPECT_EQ(GetExpectedResult({{4, 1}}, "foo"), snippets.value());
}

INSTANTIATE_TEST_SUITE_P(SnippetContentTypeTest, SnippetContentTypeTest,
                         testing::ValuesIn<SnippetContentTypeTestCase>({
                             {"text/html"},
                             {"text/plain"},
                         }));

}  // namespace

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
