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

#include "backend/query/search/plain_full_text_tokenizer.h"

#include <string>
#include <vector>

#include "zetasql/public/functions/string.h"
#include "zetasql/public/value.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "backend/query/search/tokenizer.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

absl::Status PlainFullTextTokenizer::TokenizeString(
    absl::string_view str, std::vector<std::string>& token_list) {
  std::string lower_str;
  absl::Status status;
  zetasql::functions::LowerUtf8(str, &lower_str, &status);
  ZETASQL_RETURN_IF_ERROR(status);

  std::vector<std::string> tokens = absl::StrSplit(
      lower_str, absl::ByAnyChar(kDelimiter), absl::SkipWhitespace());

  token_list.reserve(token_list.size() + tokens.size());
  token_list.insert(token_list.end(), tokens.begin(), tokens.end());

  return absl::OkStatus();
}

absl::StatusOr<zetasql::Value> PlainFullTextTokenizer::Tokenize(
    absl::Span<const zetasql::Value> args) {
  std::vector<std::string> token_list;

  const zetasql::Value& text = args[0];
  // Add tokenization signature. The first part is the tokenization function
  // name. The second part shows if the source is null, which is used to
  // differentiate NULL and empty string cases.
  token_list.push_back(std::string(kFullTextTokenizer) + "-" +
                       std::to_string(text.is_null()));

  if (!text.is_null()) {
    if (text.type()->IsArray()) {
      for (auto& value : text.elements()) {
        // Tokenize each string in the array and append them to the token list.
        ZETASQL_RETURN_IF_ERROR(TokenizeString(value.string_value(), token_list));
        // Add array gap so evaluator will handle cross array phrase.
        // TODO: handle array gap in search evaluator.
        token_list.push_back(kGapString);
      }
    } else {
      ZETASQL_RETURN_IF_ERROR(TokenizeString(text.string_value(), token_list));
    }
  }

  return TokenListFromStrings(token_list);
}

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
