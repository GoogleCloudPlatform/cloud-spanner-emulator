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

#include "backend/query/search/jsonb_tokenizer.h"

#include <vector>

#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/statusor.h"
#include "backend/query/search/tokenizer.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

using postgres_translator::spangres::datatypes::CreatePgJsonbValue;
using postgres_translator::spangres::datatypes::GetPgJsonbType;

void CheckResult(absl::StatusOr<zetasql::Value>& result) {
  ZETASQL_EXPECT_OK(result.status());
  zetasql::Value token_list = result.value();
  EXPECT_TRUE(token_list.type()->IsTokenList());
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto tokens, StringsFromTokenList(token_list));
  ASSERT_EQ(tokens.size(), 1);
  EXPECT_EQ(tokens[0], "jsonb");
}

TEST(JsonTokenizerTest, TestTokenize) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto pg_jsonb_value, CreatePgJsonbValue("true"));

  absl::StatusOr<zetasql::Value> result =
      JsonbTokenizer::Tokenize({pg_jsonb_value});
  CheckResult(result);
}

TEST(JsonTokenizerTest, TestTokenizeNull) {
  zetasql::Value pg_jsonb_value = zetasql::Value::Null(GetPgJsonbType());
  absl::StatusOr<zetasql::Value> result =
      JsonbTokenizer::Tokenize({pg_jsonb_value});
  CheckResult(result);
}

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
