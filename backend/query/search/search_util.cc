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

#include "googlesql/public/functions/string.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "backend/query/search/tokenizer.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

absl::StatusOr<Dialect> ParseDialect(absl::string_view input) {
  if (absl::EqualsIgnoreCase(input, "rquery")) {
    return Dialect::RQUERY;
  } else if (absl::EqualsIgnoreCase(input, "words")) {
    return Dialect::WORDS;
  } else if (absl::EqualsIgnoreCase(input, "words_phrase")) {
    return Dialect::WORDS_PHRASE;
  }

  return absl::InvalidArgumentError(absl::StrCat("Invalid dialect: ", input));
}

absl::StatusOr<std::vector<std::string>> GetNormalizedTerms(
    absl::string_view query_string) {
  std::vector<std::string> raw_terms = absl::StrSplit(
      query_string, absl::ByAnyChar(kDelimiter), absl::SkipWhitespace());
  std::vector<std::string> normalized_terms;
  normalized_terms.reserve(raw_terms.size());
  for (const auto& raw_term : raw_terms) {
    std::string normalized_term;
    absl::Status status;
    googlesql::functions::LowerUtf8(raw_term, &normalized_term, &status);
    if (!status.ok()) {
      return status;
    }
    normalized_terms.push_back(normalized_term);
  }
  return normalized_terms;
}

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
