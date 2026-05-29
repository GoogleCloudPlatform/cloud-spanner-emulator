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
#include "googlesql/base/testing/status_matchers.h"
#include "absl/status/status.h"
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

void TestParseRQuery(const absl::string_view query,
                     const absl::string_view expected_result) {
  SCOPED_TRACE(
      absl::StrCat("\nParsing: ", query, "\texpect: ", expected_result, "\n"));

  RQueryParser parser(query);
  GOOGLESQL_EXPECT_OK(parser.Parse());

  std::string rquery;
  GetRQueryString(parser.Tree(), rquery);
  EXPECT_EQ(rquery, expected_result);
}

void TestParseFailure(absl::string_view query,
                      absl::string_view expected_error) {
  RQueryParser parser(query);
  EXPECT_THAT(parser.Parse(), ::googlesql_base::testing::StatusIs(
                                  absl::StatusCode::kInvalidArgument,
                                  ::testing::HasSubstr(expected_error)));
}

TEST(RQueryParserTest, BasicParse) {
  TestParseRQuery("cloud spanner emulator", "(a cloud spanner emulator)");
  TestParseRQuery("cloud | spanner | emulator", "(o cloud spanner emulator)");
  TestParseRQuery("cloud|spanner|emulator", "(o cloud spanner emulator)");
  TestParseRQuery("cloud OR spanner OR emulator", "(o cloud spanner emulator)");
  TestParseRQuery("cloudORspannerOR emulator ORany",
                  "(a cloudorspanneror emulator orany)");
  TestParseRQuery("\"cloudORspannerOR emulator ORany\"",
                  "(p cloudorspanneror emulator orany)");
  TestParseRQuery("-cloud", "(n cloud)");
  TestParseRQuery("cloud-spanner-emulator", "(p cloud spanner emulator)");
  TestParseRQuery("\"cloud spanner emulator\"", "(p cloud spanner emulator)");
  TestParseRQuery("cloud AROUND(3) spanner AROUND(5) emulator",
                  "(ar cloud 3 spanner 5 emulator)");
}

TEST(RQueryParserTest, SimpleCombinations) {
  TestParseRQuery("cloud AROUND(3) spanner emulator",
                  "(a (ar cloud 3 spanner) emulator)");
  TestParseRQuery("cloud spanner AROUND(3) emulator",
                  "(a cloud (ar spanner 3 emulator))");
  TestParseRQuery("cloud | spanner emulator", "(a (o cloud spanner) emulator)");
  TestParseRQuery("cloud spanner | emulator", "(a cloud (o spanner emulator))");
  TestParseRQuery("cloud spanner OR emulator",
                  "(a cloud (o spanner emulator))");
  TestParseRQuery("-cloud spanner -emulator",
                  "(a (n cloud) spanner (n emulator))");
  TestParseRQuery("cloud spanner-emulator", "(a cloud (p spanner emulator))");
  TestParseRQuery("cloud-spanner emulator", "(a (p cloud spanner) emulator)");
  TestParseRQuery("cloud \"spanner emulator\"",
                  "(a cloud (p spanner emulator))");
  TestParseRQuery("\"cloud spanner\" emulator",
                  "(a (p cloud spanner) emulator)");

  TestParseRQuery("cloud | spanner AROUND(3) emulator",
                  "(ar (o cloud spanner) 3 emulator)");
  TestParseRQuery("cloud AROUND(3) spanner | emulator",
                  "(ar cloud 3 (o spanner emulator))");
  TestParseRQuery("-cloud AROUND(3) spanner | -emulator",
                  "(ar (n cloud) 3 (o spanner (n emulator)))");
  TestParseRQuery("cloud AROUND(3) spanner-emulator",
                  "(ar cloud 3 (p spanner emulator))");
  TestParseRQuery("\"cloud spanner\" AROUND(5) emulator",
                  "(ar (p cloud spanner) 5 emulator)");

  TestParseRQuery("cloud | spanner -emulator",
                  "(a (o cloud spanner) (n emulator))");
  TestParseRQuery("-cloud | spanner emulator",
                  "(a (o (n cloud) spanner) emulator)");
  TestParseRQuery("-cloud -spanner | emulator",
                  "(a (n cloud) (o (n spanner) emulator))");
  TestParseRQuery("cloud | spanner-emulator", "(o cloud (p spanner emulator))");
  TestParseRQuery("cloud-spanner | emulator", "(o (p cloud spanner) emulator)");
  TestParseRQuery("cloud | \"spanner emulator\"",
                  "(o cloud (p spanner emulator))");
  TestParseRQuery("\"cloud spanner\" | emulator",
                  "(o (p cloud spanner) emulator)");

  TestParseRQuery("-cloud-spanner-emulator", "(n (p cloud spanner emulator))");
  TestParseRQuery("-\"cloud spanner emulator\"",
                  "(n (p cloud spanner emulator))");
  TestParseRQuery("---cloud-spanner-emulator",
                  "(n (n (n (p cloud spanner emulator))))");
}

TEST(RQueryParserTest, ConnectedPhrase) {
  TestParseRQuery("cloud=spanner-emulator", "(p cloud spanner emulator)");
  TestParseRQuery("cloud.spanner/emulator", "(p cloud spanner emulator)");
  TestParseRQuery("cloud\\\\spanner'emulator", "(p cloud spanner emulator)");
  TestParseRQuery("cloud:=spanner//=emulator", "(p cloud spanner emulator)");
}

TEST(RQueryParserTest, OrTermTest) {
  TestParseRQuery("cloud|spanner-emulator", "(p (o cloud spanner) emulator)");
  TestParseRQuery("google|cloud:=spanner|emulator",
                  "(p (o google cloud) (o spanner emulator))");
  TestParseRQuery("\"cloud|spanner emulator\"",
                  "(p (o cloud spanner) emulator)");
  TestParseRQuery("\"cloud|spanner -emulator\"",
                  "(p (o cloud spanner) emulator)");
  TestParseRQuery("\"google|cloud:=spanner|emulator\"",
                  "(p (o google cloud) (o spanner emulator))");
  TestParseRQuery(
      "google|alphabet-cloud-spanner|sql-emulator|prod",
      "(p (o google alphabet) cloud (o spanner sql) (o emulator prod))");
}

TEST(RQueryParserTest, AlphaNumeric) {
  TestParseRQuery("22 cloud emulator", "(a 22 cloud emulator)");
  TestParseRQuery("cloud | 22 emulator", "(a (o cloud 22) emulator)");
  TestParseRQuery("c10oud emulator", "(a c10oud emulator)");
  TestParseRQuery("2panner-c10ud-4mulator", "(p 2panner c10ud 4mulator)");
}

TEST(RQueryParserTest, Capital) {
  TestParseRQuery("CLOUD EMULATOR", "(a cloud emulator)");
  TestParseRQuery("CLOud | emulaTOR", "(o cloud emulator)");
  TestParseRQuery("\"ClOUd EmuLAtor\"", "(p cloud emulator)");
  TestParseRQuery("ClOUd AROUND(3) EmuLAtor", "(ar cloud 3 emulator)");
}

TEST(RQueryParserTest, ParsingError) {
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

TEST(RQueryParserTest, EmptyQuery) { TestParseRQuery("", ""); }

TEST(RQueryParserTest, TrimSeparators) {
  TestParseRQuery("term!", "term");
  TestParseRQuery("term!@", "term");
  TestParseRQuery("cloud spanner!@", "(a cloud spanner)");
  TestParseRQuery("term!!!", "term");
  TestParseRQuery("!", "");
  TestParseRQuery("! ", "");
  TestParseRQuery("!term", "term");
  TestParseRQuery("@!term", "term");
  TestParseRQuery("@!cloud spanner", "(a cloud spanner)");
  TestParseRQuery("!!!term", "term");
  TestParseRQuery("@!term!@", "term");
  TestParseRQuery(" @!cloud spanner!@ ", "(a cloud spanner)");
  TestParseRQuery("\\!term", "term");
  TestParseRQuery("\\'term", "term");
  TestParseRQuery("\\\"term", "term");
  TestParseRQuery("term\\!", "term");
  TestParseRQuery("term\\'", "term");
  TestParseRQuery("term\\\"", "term");
  TestParseRQuery("term\\'!", "term");
  TestParseRQuery("@\\!term", "term");
  TestParseRQuery("\\!\\@term", "term");
  TestParseRQuery("term\\!@", "term");
}

TEST(RQueryParserTest, CaseSensitive) {
  TestParseRQuery("CLOUD or EMULATOR", "(a cloud or emulator)");
  TestParseRQuery("CLOUD oR EMULATOR", "(a cloud or emulator)");
  TestParseRQuery("CLOUD OR EMULATOR", "(o cloud emulator)");
  TestParseFailure("ClOUd ARouND(3) EmuLAtor", "Syntax error");
}

}  // namespace

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
