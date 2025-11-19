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

#include "common/pg_literals.h"

#include <string>
#include <utility>

#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"

namespace {

class PgLiteralsTest
    : public testing::TestWithParam<std::pair<std::string, std::string>> {
 protected:
  PgLiteralsTest() = default;
};

INSTANTIATE_TEST_SUITE_P(
    PgLiteralsTests, PgLiteralsTest,
    testing::Values(
        std::make_pair("sequence", "sequence"),
        std::make_pair("\"sequence\"", "sequence"),
        std::make_pair("schema.sequence", "schema.sequence"),
        std::make_pair("\"schema.sequence\"", "schema.sequence"),
        std::make_pair("\"schema\".sequence", "schema.sequence"),
        std::make_pair("schema.\"sequence\"", "schema.sequence"),
        std::make_pair("\"schema\".\"sequence\"", "schema.sequence"),

        // These PG identifiers should be caught during parsing.
        std::make_pair("database.schema.sequence", "database.schema.sequence"),
        std::make_pair("\"database\".\"schema\".\"sequence\"",
                       "\"database\".\"schema\".\"sequence\"")));

TEST_P(PgLiteralsTest, GetFullyQualifiedNameFromPgLiteral) {
  auto [pg_literal, expected_result] = GetParam();
  EXPECT_EQ(
      google::spanner::emulator::GetFullyQualifiedNameFromPgLiteral(pg_literal),
      expected_result);
}

}  // namespace
