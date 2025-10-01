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
#include <vector>

#include "zetasql/public/functions/string.h"
#include "absl/log/check.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "backend/query/search/ErrorHandler.h"
#include "backend/query/search/JavaCC.h"
#include "backend/query/search/SearchQueryParser.h"
#include "backend/query/search/SearchQueryParserTokenManager.h"
#include "backend/query/search/SearchQueryParserTreeConstants.h"
#include "backend/query/search/Token.h"
#include "backend/query/search/query_char_stream.h"
#include "common/errors.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

namespace {
constexpr absl::string_view kSeparators = "~`!@#$%^&_+{}[]<>,?* \r\n\t\b\f_;";
}  // namespace

namespace {

// Borrowed from backend/schema/parser/query_parser.cc DDLErrorHandler.
// The class is used by query parser to record the parsing errors.
// The errors then can be populated to caller for future process.
class SearchQueryParserErrorHandler : public ErrorHandler {
 public:
  explicit SearchQueryParserErrorHandler(std::vector<std::string>* errors)
      : errors_(errors), ignore_further_errors_(false) {}
  ~SearchQueryParserErrorHandler() override = default;

  void handleUnexpectedToken(int expected_kind, const JJString& expected_token,
                             Token* actual,
                             SearchQueryParser* parser) override {
    if (ignore_further_errors_) {
      return;
    }
    // expected_kind is -1 when the next token is not expected, when choosing
    // the next rule based on next token. Every invocation of
    // handleUnexpectedToken with expeced_kind=-1 is followed by a call to
    // handleParserError. We process the error there.
    if (expected_kind == -1) {
      return;
    }

    // The parser would continue to throw unexpected token at us but only the
    // first error is the cause.
    ignore_further_errors_ = true;

    errors_->push_back(absl::StrCat("Syntax error on column ",
                                    actual->beginColumn, ": Expecting '",
                                    absl::AsciiStrToUpper(expected_token)));
  }

  void handleParseError(Token* last, Token* unexpected,
                        const JJSimpleString& production,
                        SearchQueryParser* parser) override {
    if (ignore_further_errors_) {
      return;
    }
    ignore_further_errors_ = true;

    std::string extra_info;
    if (unexpected->kind == OPEN_PARE || unexpected->kind == CLOSE_PARE) {
      extra_info =
          "Using parentheses to group query terms is not supported in rquery "
          "parser.";
    }

    errors_->push_back(
        absl::StrCat("Encountered error on column ", unexpected->beginColumn,
                     " while parsing: ", production, ". ", extra_info));
  }

  int getErrorCount() override { return errors_->size(); }

 private:
  // List of errors found during the parse.  Will be empty IFF
  // there were no problems parsing.
  std::vector<std::string>* errors_;
  bool ignore_further_errors_ = false;
};

}  // namespace

QueryParser::QueryParser(absl::string_view search_query)
    : search_query_(search_query), tree_(nullptr) {}

absl::Status QueryParser::NormalizeParsedTree(SimpleNode* tree) {
  if (tree == nullptr) {
    return absl::OkStatus();
  }

  if (tree->getId() == JJTTERM) {
    std::string normalized_str;
    absl::Status status;

    zetasql::functions::LowerUtf8(tree->image(), &normalized_str, &status);
    if (!status.ok()) {
      return error::FailToParseSearchQuery(
          search_query_, "Failed to normalize search query tree.");
    }

    tree->set_image(normalized_str);
  }

  for (int i = 0; i < tree->jjtGetNumChildren(); i++) {
    SimpleNode* child = dynamic_cast<SimpleNode*>(tree->jjtGetChild(i));
    ZETASQL_RETURN_IF_ERROR(NormalizeParsedTree(child));
  }

  return absl::OkStatus();
}

absl::Status QueryParser::ParseSearchQuery() {
  // Trim leading and trailing separators to avoid parsing errors.
  auto start = search_query_.find_first_not_of(kSeparators.data(), 0,
                                               kSeparators.size());
  if (start == std::string::npos) {
    search_query_.clear();
  } else {
    auto end = search_query_.find_last_not_of(
        kSeparators.data(), std::string::npos, kSeparators.size());
    search_query_.erase(end + 1);
    search_query_.erase(0, start);
  }
  if (search_query_.empty()) {
    return absl::OkStatus();
  }

  // Create the JavaCC generated parser.
  SearchQueryCharStream char_stream(search_query_);
  SearchQueryParserTokenManager token_manager(&char_stream);
  SearchQueryParser parser(&token_manager);

  std::vector<std::string> errors;
  // The parser owns the error handler and deletes it.
  parser.setErrorHandler(new SearchQueryParserErrorHandler(&errors));

  tree_ = absl::WrapUnique<SimpleNode>(parser.ParseSearchQuery());

  if (tree_ == nullptr) {
    std::string errors_string;
    switch (errors.size()) {
      case 0:
        errors_string = "Unknown error while parsing search query.";
        break;
      case 1:
        errors_string = errors[0];
        break;
      default:
        errors_string = absl::StrJoin(errors, "\n-");
        break;
    }
    return error::FailToParseSearchQuery(search_query_, errors_string);
  }

  ZETASQL_RETURN_IF_ERROR(NormalizeParsedTree(tree_.get()));

  return absl::OkStatus();
}

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
