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

#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"

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

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
