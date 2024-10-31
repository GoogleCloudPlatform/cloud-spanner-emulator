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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_QUERY_PARSER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_QUERY_PARSER_H_

#include <memory>
#include <string>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "backend/query/search/SearchQueryParserTree.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

class QueryParser {
  // Wrapper class of the search query parser. The class takes a passed in query
  // string, preprocess it, and parse the processed string into an AST for
  // SEARCH evaluation.
 public:
  // Initialize a query parser object from a search query. The search query is
  // the user input in the SEARCH function. Some accepted search queries are:
  // [cloud spanner], [cloud | spanner emulator], [cloud-spanner-emulator],
  // [google around(3) cloud-spanner|sql-emulator], etc.
  // The grammar of a valid search query is defined in query_parser.jjt.
  // An error will be returned when the input fails to parse.
  explicit QueryParser(absl::string_view search_query);

  // Parses a search query.
  absl::Status ParseSearchQuery();

  // Taking ownership of the parsed tree.
  std::unique_ptr<SimpleNode>& ReleaseTree() { return tree_; }

  const SimpleNode* Tree() const { return tree_.get(); }

 private:
  // Normalize query string. e.g. transfer all chars into lower case.
  absl::Status NormalizeParsedTree(SimpleNode* tree);

  std::string search_query_;

  std::unique_ptr<SimpleNode> tree_;
};

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_QUERY_PARSER_H_
