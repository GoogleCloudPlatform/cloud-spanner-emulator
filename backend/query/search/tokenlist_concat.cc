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

#include "backend/query/search/tokenlist_concat.h"

#include <string>
#include <vector>

#include "zetasql/public/value.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_split.h"
#include "absl/types/span.h"
#include "backend/query/search/tokenizer.h"
#include "common/errors.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google::spanner::emulator::backend::query::search {

absl::StatusOr<zetasql::Value> TokenlistConcat::Concat(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.size() == 1 && args[0].type()->IsArray());
  const auto& arg = args[0];
  if (arg.is_null()) {
    // TOKENLIST_CONCAT(NULL) should return NULL.
    return zetasql::Value::Null(arg.type());
  }
  std::vector<std::string> result;
  std::vector<std::string> first_signature;
  for (const auto& tokenlist : arg.elements()) {
    if (tokenlist.is_null()) {
      // NULL TOKENLIST inside array will just be skipped.
      continue;
    }
    ZETASQL_ASSIGN_OR_RETURN(auto tokens, StringsFromTokenList(tokenlist));
    if (first_signature.empty()) {
      first_signature = absl::StrSplit(tokens[0], '-');
    } else {
      std::vector<std::string> current_signature =
          absl::StrSplit(tokens[0], '-');
      if (current_signature[0] != first_signature[0]) {
        return error::TokenlistTypeMergeConflict();
      }
      if (current_signature[0] == kNgramsTokenizer ||
          current_signature[0] == kSubstringTokenizer) {
        // For substring and ngram tokenizer, also need to check ngram sizes.
        if (current_signature[1] != first_signature[1] ||
            current_signature[2] != first_signature[2]) {
          return error::TokenlistTypeMergeConflict();
        }
      }
    }
    result.insert(result.end(), tokens.begin(), tokens.end());
    result.push_back(kGapString);
  }
  return TokenListFromStrings(result);
}

}  // namespace google::spanner::emulator::backend::query::search
