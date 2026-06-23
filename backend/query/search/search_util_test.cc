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

#include "backend/query/search/search_util.h"

#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "absl/status/status.h"
namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

using ::testing::HasSubstr;
using ::googlesql_base::testing::StatusIs;

TEST(SearchUtilTest, TestParseDialect) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(Dialect dialect1, ParseDialect("rquery"));
  EXPECT_EQ(Dialect::RQUERY, dialect1);

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(Dialect dialect2, ParseDialect("words"));
  EXPECT_EQ(Dialect::WORDS, dialect2);

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(Dialect dialect3, ParseDialect("words_phrase"));
  EXPECT_EQ(Dialect::WORDS_PHRASE, dialect3);

  // Case insensitivity
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(Dialect dialect4, ParseDialect("Words_Phrase"));
  EXPECT_EQ(Dialect::WORDS_PHRASE, dialect4);

  // Invalid dialect
  EXPECT_THAT(ParseDialect("invalid_dialect"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid dialect: invalid_dialect")));
}

TEST(SearchUtilTest, TestGetNormalizedTerms) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::vector<std::string> terms1,
                       GetNormalizedTerms("Cloud Spanner Emulator"));
  EXPECT_THAT(terms1, testing::ElementsAre("cloud", "spanner", "emulator"));

  // With delimiters
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::vector<std::string> terms2,
                       GetNormalizedTerms("Cloud-Spanner!Emulator"));
  EXPECT_THAT(terms2, testing::ElementsAre("cloud", "spanner", "emulator"));

  // Empty query
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::vector<std::string> terms3, GetNormalizedTerms(""));
  EXPECT_TRUE(terms3.empty());

  // Only delimiters
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::vector<std::string> terms4,
                       GetNormalizedTerms("###"));
  EXPECT_TRUE(terms4.empty());

  // Mixed case and non-ascii
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::vector<std::string> terms5,
                       GetNormalizedTerms("谷歌 Cloud"));
  EXPECT_THAT(terms5, testing::ElementsAre("谷歌", "cloud"));
}

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
