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

#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/value.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/numbers.h"
#include "absl/types/span.h"
#include "backend/query/search/SearchQueryParserTreeConstants.h"
#include "backend/query/search/search_evaluator_helpers.h"
#include "common/errors.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

absl::StatusOr<MatchResult> SearchEvaluator::MatchQueryTerm(
    const SimpleNode* root, const TokenMap& tokenmap, bool in_phrase) {
  if (tokenmap.find(root->image()) == tokenmap.end()) {
    return MatchResult::NoMatch();
  }

  if (!in_phrase) {
    // No position info is needed if not in phrase.
    return MatchResult::Match();
  }

  std::vector<Hit> hits;
  const std::vector<int>& positions = tokenmap.at(root->image());
  hits.reserve(positions.size());
  for (auto pos : positions) {
    hits.push_back({pos, 1});
  }
  return MatchResult::Match(hits);
}

absl::StatusOr<std::vector<MatchResult>> SearchEvaluator::MatchChildren(
    const SimpleNode* root, const TokenMap& tokenmap, bool in_phrase) {
  std::vector<MatchResult> ret;
  ret.reserve(root->jjtGetNumChildren());
  for (int i = 0; i < root->jjtGetNumChildren(); i++) {
    SimpleNode* child = dynamic_cast<SimpleNode*>(root->jjtGetChild(i));
    ZETASQL_RET_CHECK(child != nullptr);

    if (child->getId() != JJTNUMBER) {
      ZETASQL_ASSIGN_OR_RETURN(auto result, MatchQueryNode(child, tokenmap, in_phrase));
      ret.emplace_back(result);
    }
  }
  return ret;
}

absl::StatusOr<MatchResult> SearchEvaluator::MatchQueryNode(
    const SimpleNode* root, const TokenMap& tokenmap, bool in_phrase) {
  int node_id = root->getId();
  switch (node_id) {
    case JJTNUMBER:
      // Number nodes are used only as children of AROUND node so there's
      // no need to match number nodes.
      break;
    case JJTTERM:
      return MatchQueryTerm(root, tokenmap, in_phrase);
    case JJTOR: {
      ZETASQL_ASSIGN_OR_RETURN(std::vector<MatchResult> child_results,
                       MatchChildren(root, tokenmap, in_phrase));
      bool did_match = false;
      std::vector<Hit> hits;
      for (const auto& result : child_results) {
        if (!result.is_match()) continue;
        if (in_phrase) {
          hits.insert(hits.end(), result.hits().begin(), result.hits().end());
        }
        did_match = true;
      }
      if (!did_match) return MatchResult::NoMatch();
      return MatchResult::Match(std::move(hits));
    }
    case JJTNOT: {
      // NOT cannot appear in a phrase, so we never need to report hit position.
      ZETASQL_RET_CHECK(!in_phrase) << "NOT nodes cannot appear in phrases";
      ZETASQL_RET_CHECK_EQ(1, root->jjtGetNumChildren());
      SimpleNode* child = dynamic_cast<SimpleNode*>(root->jjtGetChild(0));
      ZETASQL_RET_CHECK(child != nullptr);

      ZETASQL_ASSIGN_OR_RETURN(auto child_matches, MatchQueryNode(child, tokenmap));
      if (child_matches.is_match()) return MatchResult::NoMatch();
      return MatchResult::Match();
    }
    case JJTAND: {
      // AND cannot appear in a phrase.
      ZETASQL_RET_CHECK(!in_phrase) << "AND nodes cannot appear in phrases";
      ZETASQL_ASSIGN_OR_RETURN(std::vector<MatchResult> child_results,
                       MatchChildren(root, tokenmap, in_phrase));
      for (const auto& result : child_results) {
        if (!result.is_match()) return result;
      }
      // AND cannot appear in a phrases, so there is no need to
      // report hit positions.
      return MatchResult::Match();
    }
    case JJTAROUND: {
      ZETASQL_ASSIGN_OR_RETURN(std::vector<MatchResult> child_results,
                       MatchChildren(root, tokenmap, /*in_phrase=*/true));

      int max_allowed_gap = -1;
      std::vector<std::vector<Hit>> hits;
      hits.reserve(child_results.size());
      int cur_child_result = 0;
      for (int i = 0; i < root->jjtGetNumChildren(); ++i) {
        SimpleNode* child = dynamic_cast<SimpleNode*>(root->jjtGetChild(i));
        ZETASQL_RET_CHECK(child != nullptr);

        if (child->getId() == JJTNUMBER) {
          int gap;
          if (absl::SimpleAtoi(child->image(), &gap)) {
            if (max_allowed_gap == -1) {
              // First distance child
              max_allowed_gap = gap;
            } else if (gap != max_allowed_gap) {
              // Multiple distance specified, drop them all but using the
              // default distance 5
              max_allowed_gap = kDefaultMaxAllowedGap;
            }
          }
        } else if (child->image() == "*") {
          // No need to record wildcard position.
          cur_child_result++;
        } else {
          // All non-wildcard children must match.
          if (!child_results[cur_child_result].is_match()) {
            return MatchResult::NoMatch();
          }

          hits.emplace_back(std::move(child_results[cur_child_result].hits()));
          cur_child_result++;
        }
      }

      if (max_allowed_gap == -1) {
        return absl::InvalidArgumentError(
            "Invalid distance specified in AROUND expression.");
      }

      PhraseMatcher matcher(hits, max_allowed_gap);
      std::vector<Hit> result = matcher.FindMatchingHits();
      return result.empty() ? MatchResult::NoMatch()
                            : MatchResult::Match(std::move(result));
    }
    case JJTPHRASE: {
      ZETASQL_ASSIGN_OR_RETURN(std::vector<MatchResult> child_results,
                       MatchChildren(root, tokenmap, /*in_phrase=*/true));

      int current_offset = 0;
      std::vector<int> offsets;
      std::vector<std::vector<Hit>> hits;
      hits.reserve(child_results.size());

      // exterior_gaps indicates the gaps (denoted by wildcards) before
      // and after literal terms. e.g. "* cloud spanner *" has exterior_gaps
      // of (1, 1).
      std::pair<int, int> exterior_gaps = {0, 0};
      for (int i = 0; i < root->jjtGetNumChildren(); ++i) {
        SimpleNode* child = dynamic_cast<SimpleNode*>(root->jjtGetChild(i));
        ZETASQL_RET_CHECK(child != nullptr);

        // In phrases, wildcards increments the next offset, rather than
        // being matched directly. Offsets before the first non-wildcard
        // term have no effect.
        if (child->image() != "*") {
          // All non-wildcard children must match.
          if (!child_results[i].is_match()) return MatchResult::NoMatch();

          hits.emplace_back(std::move(child_results[i].hits()));

          if (offsets.empty()) {
            exterior_gaps.first = current_offset;
          }
          offsets.push_back(current_offset);
        }

        ++current_offset;
      }

      // current_offset is incremented after each term, so we need to subtract
      // 1 from the difference to find the number of star terms after the last
      // non-star term.
      exterior_gaps.second =
          offsets.empty() ? 0 : current_offset - offsets.back() - 1;

      PhraseMatcher matcher(hits, offsets, exterior_gaps);
      std::vector<Hit> result = matcher.FindMatchingHits();
      return result.empty() ? MatchResult::NoMatch()
                            : MatchResult::Match(std::move(result));
    }
    default:
      ZETASQL_RET_CHECK_FAIL() << "Bad query: unsupported search query node type "
                       << node_id;
  }

  ZETASQL_RET_CHECK_FAIL() << "Bad query: unable to get match result from the query ";
}

absl::StatusOr<zetasql::Value> SearchEvaluator::Evaluate(
    absl::Span<const zetasql::Value> args) {
  const zetasql::Value tokenlist = args[0];
  const zetasql::Value query_string = args[1];

  if (!tokenlist.type()->IsTokenList()) {
    return error::ColumnNotSearchable(tokenlist.type()->DebugString());
  }

  if (!query_string.type()->IsString()) {
    return error::InvalidQueryType(query_string.type()->DebugString());
  }

  TokenMap token_map;
  bool source_is_null = false;
  if (!tokenlist.is_null()) {
    ZETASQL_ASSIGN_OR_RETURN(token_map, SearchHelper::BuildTokenMap(tokenlist, "SEARCH",
                                                            source_is_null));
  }

  if (source_is_null || query_string.is_null()) {
    // Return FALSE if query is null.
    return zetasql::Value::NullBool();
  }

  if (query_string.string_value().empty()) {
    // Return TRUE if query is empty string.
    return zetasql::Value::Bool(false);
  }

  ZETASQL_ASSIGN_OR_RETURN(SimpleNode * search_query,
                   SearchQueryCache::GetInstance()->GetParsedQuery(
                       query_string.string_value()));

  ZETASQL_ASSIGN_OR_RETURN(MatchResult match_result,
                   MatchQueryNode(search_query, token_map));

  return zetasql::Value::Bool(match_result.is_match());
}

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
