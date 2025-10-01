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

#include "backend/query/search/query_parser.h"

#include <memory>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "backend/query/search/SearchQueryParserTreeConstants.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

namespace {

std::string GetNodeDebugString(SimpleNode* node) {
  std::string str;
  str.append("(");

  int node_id = node->getId();
  str.append(jjtNodeName[node_id]);
  if (node_id == JJTTERM || node_id == JJTNUMBER) {
    str.append(" " + node->image());
  }

  for (int i = 0; i < node->jjtGetNumChildren(); i++) {
    SimpleNode* child = dynamic_cast<SimpleNode*>(node->jjtGetChild(i));
    if (child != nullptr) {
      str.append(" ");
      str.append(GetNodeDebugString(child));
    }
  }

  str.append(")");

  return str;
}

void GetRQueryString(const SimpleNode* node, std::string& str) {
  if (node == nullptr) {
    return;
  }

  int node_id = node->getId();
  bool append_close_para = true;

  switch (node_id) {
    case JJTAND:
      str.append("(a");
      break;
    case JJTAROUND:
      str.append("(ar");
      break;
    case JJTOR:
      str.append("(o");
      break;
    case JJTNOT:
      str.append("(n");
      break;
    case JJTPHRASE:
      str.append("(p");
      break;
    case JJTTERM:
    case JJTNUMBER:
      str.append(node->image());
      append_close_para = false;
      break;
    default:
      append_close_para = false;
  }

  for (int i = 0; i < node->jjtGetNumChildren(); i++) {
    SimpleNode* child = dynamic_cast<SimpleNode*>(node->jjtGetChild(i));
    if (child != nullptr) {
      str.append(" ");
      GetRQueryString(child, str);
    }
  }

  if (append_close_para) {
    str.append(")");
  }
}

void TestParseSearchQuery(absl::string_view search_query,
                          absl::string_view expected_result) {
  SCOPED_TRACE(absl::StrCat("\nParsing: ", search_query,
                            "\texpect: ", expected_result, "\n"));

  QueryParser parser(search_query);
  ZETASQL_EXPECT_OK(parser.ParseSearchQuery());

  std::string rquery;
  GetRQueryString(parser.Tree(), rquery);
  EXPECT_EQ(rquery, expected_result);
}

void TestParseFailure(absl::string_view search_query,
                      absl::string_view expected_error) {
  QueryParser parser(search_query);
  absl::Status status = parser.ParseSearchQuery();
  EXPECT_THAT(status, ::zetasql_base::testing::StatusIs(
                          absl::StatusCode::kInvalidArgument,
                          ::testing::HasSubstr(expected_error)));
}

TEST(SearchQueryParserTest, BasicParse) {
  TestParseSearchQuery("cloud spanner emulator", "(a cloud spanner emulator)");
  TestParseSearchQuery("cloud | spanner | emulator",
                       "(o cloud spanner emulator)");
  TestParseSearchQuery("cloud|spanner|emulator", "(o cloud spanner emulator)");
  TestParseSearchQuery("cloud OR spanner OR emulator",
                       "(o cloud spanner emulator)");
  TestParseSearchQuery("cloudORspannerOR emulator ORany",
                       "(a cloudorspanneror emulator orany)");
  TestParseSearchQuery("\"cloudORspannerOR emulator ORany\"",
                       "(p cloudorspanneror emulator orany)");
  TestParseSearchQuery("-cloud", "(n cloud)");
  TestParseSearchQuery("cloud-spanner-emulator", "(p cloud spanner emulator)");
  TestParseSearchQuery("\"cloud spanner emulator\"",
                       "(p cloud spanner emulator)");
  TestParseSearchQuery("cloud AROUND(3) spanner AROUND(5) emulator",
                       "(ar cloud 3 spanner 5 emulator)");
}

TEST(SearchQueryParserTest, SimpleCombinations) {
  TestParseSearchQuery("cloud AROUND(3) spanner emulator",
                       "(a (ar cloud 3 spanner) emulator)");
  TestParseSearchQuery("cloud spanner AROUND(3) emulator",
                       "(a cloud (ar spanner 3 emulator))");
  TestParseSearchQuery("cloud | spanner emulator",
                       "(a (o cloud spanner) emulator)");
  TestParseSearchQuery("cloud spanner | emulator",
                       "(a cloud (o spanner emulator))");
  TestParseSearchQuery("cloud spanner OR emulator",
                       "(a cloud (o spanner emulator))");
  TestParseSearchQuery("-cloud spanner -emulator",
                       "(a (n cloud) spanner (n emulator))");
  TestParseSearchQuery("cloud spanner-emulator",
                       "(a cloud (p spanner emulator))");
  TestParseSearchQuery("cloud-spanner emulator",
                       "(a (p cloud spanner) emulator)");
  TestParseSearchQuery("cloud \"spanner emulator\"",
                       "(a cloud (p spanner emulator))");
  TestParseSearchQuery("\"cloud spanner\" emulator",
                       "(a (p cloud spanner) emulator)");

  TestParseSearchQuery("cloud | spanner AROUND(3) emulator",
                       "(ar (o cloud spanner) 3 emulator)");
  TestParseSearchQuery("cloud AROUND(3) spanner | emulator",
                       "(ar cloud 3 (o spanner emulator))");
  TestParseSearchQuery("-cloud AROUND(3) spanner | -emulator",
                       "(ar (n cloud) 3 (o spanner (n emulator)))");
  TestParseSearchQuery("cloud AROUND(3) spanner-emulator",
                       "(ar cloud 3 (p spanner emulator))");
  TestParseSearchQuery("\"cloud spanner\" AROUND(5) emulator",
                       "(ar (p cloud spanner) 5 emulator)");

  TestParseSearchQuery("cloud | spanner -emulator",
                       "(a (o cloud spanner) (n emulator))");
  TestParseSearchQuery("-cloud | spanner emulator",
                       "(a (o (n cloud) spanner) emulator)");
  TestParseSearchQuery("-cloud -spanner | emulator",
                       "(a (n cloud) (o (n spanner) emulator))");
  TestParseSearchQuery("cloud | spanner-emulator",
                       "(o cloud (p spanner emulator))");
  TestParseSearchQuery("cloud-spanner | emulator",
                       "(o (p cloud spanner) emulator)");
  TestParseSearchQuery("cloud | \"spanner emulator\"",
                       "(o cloud (p spanner emulator))");
  TestParseSearchQuery("\"cloud spanner\" | emulator",
                       "(o (p cloud spanner) emulator)");

  TestParseSearchQuery("-cloud-spanner-emulator",
                       "(n (p cloud spanner emulator))");
  TestParseSearchQuery("-\"cloud spanner emulator\"",
                       "(n (p cloud spanner emulator))");
  TestParseSearchQuery("---cloud-spanner-emulator",
                       "(n (n (n (p cloud spanner emulator))))");
}

TEST(SearchQueryParserTest, ConnectedPhrase) {
  TestParseSearchQuery("cloud=spanner-emulator", "(p cloud spanner emulator)");
  TestParseSearchQuery("cloud.spanner/emulator", "(p cloud spanner emulator)");
  TestParseSearchQuery("cloud\\\\spanner'emulator",
                       "(p cloud spanner emulator)");
  TestParseSearchQuery("cloud:=spanner//=emulator",
                       "(p cloud spanner emulator)");
}

TEST(SearchQueryParserTest, OrTermTest) {
  TestParseSearchQuery("cloud|spanner-emulator",
                       "(p (o cloud spanner) emulator)");
  TestParseSearchQuery("google|cloud:=spanner|emulator",
                       "(p (o google cloud) (o spanner emulator))");
  TestParseSearchQuery("\"cloud|spanner emulator\"",
                       "(p (o cloud spanner) emulator)");
  TestParseSearchQuery("\"cloud|spanner -emulator\"",
                       "(p (o cloud spanner) emulator)");
  TestParseSearchQuery("\"google|cloud:=spanner|emulator\"",
                       "(p (o google cloud) (o spanner emulator))");
  TestParseSearchQuery(
      "google|alphabet-cloud-spanner|sql-emulator|prod",
      "(p (o google alphabet) cloud (o spanner sql) (o emulator prod))");
}

TEST(SearchQueryParserTest, AlphaNumeric) {
  TestParseSearchQuery("22 cloud emulator", "(a 22 cloud emulator)");
  TestParseSearchQuery("cloud | 22 emulator", "(a (o cloud 22) emulator)");
  TestParseSearchQuery("c10oud emulator", "(a c10oud emulator)");
  TestParseSearchQuery("2panner-c10ud-4mulator", "(p 2panner c10ud 4mulator)");
}

TEST(SearchQueryParserTest, Capital) {
  TestParseSearchQuery("CLOUD EMULATOR", "(a cloud emulator)");
  TestParseSearchQuery("CLOud | emulaTOR", "(o cloud emulator)");
  TestParseSearchQuery("\"ClOUd EmuLAtor\"", "(p cloud emulator)");
  TestParseSearchQuery("ClOUd AROUND(3) EmuLAtor", "(ar cloud 3 emulator)");
}

TEST(SearchQueryParserTest, ParsingError) {
  TestParseFailure("cloud || spanner", "Encountered error");
  TestParseFailure("cloud | AROUND(3) spanner", "Syntax error");
  TestParseFailure("cloud| spanner emulator", "Encountered error");
  TestParseFailure("|cloud spanner emulator", "Encountered error");
  TestParseFailure("OR cloud spanner emulator", "Encountered error");
  TestParseFailure("cloud spanner emulator|", "Encountered error");
  TestParseFailure("cloud spanner emulator OR", "Encountered error");
  TestParseFailure("\"cloud AROUND(3) spanner", "Encountered error");
  TestParseFailure("-(cloud spanner) | emulator",
                   "Using parentheses to group query terms is not supported in "
                   "rquery parser");

  // Not support query non-ascii unicode string yet.
  TestParseFailure("谷歌", "Encountered error");
}

TEST(SearchQueryParserTest, EmptyQuery) { TestParseSearchQuery("", ""); }

TEST(SearchQueryParserTest, TrimSeparators) {
  TestParseSearchQuery("term!", "term");
  TestParseSearchQuery("term!@", "term");
  TestParseSearchQuery("cloud spanner!@", "(a cloud spanner)");
  TestParseSearchQuery("term!!!", "term");
  TestParseSearchQuery("!", "");
  TestParseSearchQuery("! ", "");
  TestParseSearchQuery("!term", "term");
  TestParseSearchQuery("@!term", "term");
  TestParseSearchQuery("@!cloud spanner", "(a cloud spanner)");
  TestParseSearchQuery("!!!term", "term");
  TestParseSearchQuery("@!term!@", "term");
  TestParseSearchQuery(" @!cloud spanner!@ ", "(a cloud spanner)");
  TestParseSearchQuery("\\!term", "term");
  TestParseSearchQuery("\\'term", "term");
  TestParseSearchQuery("\\\"term", "term");
  TestParseSearchQuery("term\\!", "term");
  TestParseSearchQuery("term\\'", "term");
  TestParseSearchQuery("term\\\"", "term");
  TestParseSearchQuery("term\\'!", "term");
  TestParseSearchQuery("@\\!term", "term");
  TestParseSearchQuery("\\!\\@term", "term");
  TestParseSearchQuery("term\\!@", "term");
}

TEST(SearchQueryParserTest, CaseSensitive) {
  TestParseSearchQuery("CLOUD or EMULATOR", "(a cloud or emulator)");
  TestParseSearchQuery("CLOUD oR EMULATOR", "(a cloud or emulator)");
  TestParseSearchQuery("CLOUD OR EMULATOR", "(o cloud emulator)");
  TestParseFailure("ClOUd ARouND(3) EmuLAtor", "Syntax error");
}

}  // namespace

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
