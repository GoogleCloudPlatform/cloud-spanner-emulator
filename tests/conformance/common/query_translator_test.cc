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

#include "tests/conformance/common/query_translator.h"

#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"

namespace google::spanner::emulator::test {
namespace {

TEST(QueryTranslatorTest, HintSyntax) {
  QueryTranslator translator = QueryTranslator().UsesHintSyntax();
  EXPECT_EQ(translator.Translate("SELECT 1"), "SELECT 1");
  EXPECT_EQ(translator.Translate("SELECT 1 @{foo=bar}"),
            "SELECT 1 /*@ foo=bar */");
  EXPECT_EQ(translator.Translate("SELECT 1 @{foo=bar, baz=qux}"),
            "SELECT 1 /*@ foo=bar, baz=qux */");
}

TEST(QueryTranslatorTest, LiteralCasts) {
  QueryTranslator translator = QueryTranslator().UsesLiteralCasts();
  EXPECT_EQ(translator.Translate("SELECT CAST(NULL AS STRING)"),
            "SELECT null::text");
}

TEST(QueryTranslatorTest, StringLiterals) {
  QueryTranslator translator = QueryTranslator().UsesArgumentStringLiterals();
  EXPECT_EQ(translator.Translate("SELECT reverse(\"foo\")"),
            "SELECT reverse('foo'::text)");
  EXPECT_EQ(translator.Translate("SELECT reverse('foo')"),
            "SELECT reverse('foo'::text)");
}

TEST(QueryTranslatorTest, StringNullLiteral) {
  QueryTranslator translator = QueryTranslator().UsesStringNullLiteral();
  EXPECT_EQ(translator.Translate("SELECT NULL"), "SELECT null::text");
}

TEST(QueryTranslatorTest, StringArrayLiterals) {
  QueryTranslator translator = QueryTranslator().UsesStringArrayLiterals();
  EXPECT_EQ(translator.Translate("SELECT []"), "SELECT '{}'::text[]");
}

TEST(QueryTranslatorTest, NamespacedFunctions) {
  QueryTranslator translator = QueryTranslator().UsesNamespacedFunctions();
  EXPECT_EQ(translator.Translate("SELECT pending_commit_timestamp()"),
            "SELECT spanner.pending_commit_timestamp()");
  EXPECT_EQ(translator.Translate("SELECT spanner.pending_commit_timestamp()"),
            "SELECT spanner.pending_commit_timestamp()");
  EXPECT_EQ(translator.Translate("SELECT unmapped_function()"),
            "SELECT unmapped_function()");
}

}  // namespace
}  // namespace google::spanner::emulator::test
