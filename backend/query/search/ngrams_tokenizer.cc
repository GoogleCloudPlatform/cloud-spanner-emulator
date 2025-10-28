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

#include "backend/query/search/ngrams_tokenizer.h"

#include <string>
#include <vector>

#include "zetasql/public/functions/string.h"
#include "zetasql/public/value.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "backend/query/search/tokenizer.h"
#include "common/errors.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

absl::Status NgramsTokenizer::ValidateNgramSize(int ngram_size_min,
                                                int ngram_size_max) {
  if (ngram_size_max <= 0) {
    return error::InvalidNgramSize("ngram_size_max must be greater than 0.");
  }
  if (ngram_size_min <= 0) {
    return error::InvalidNgramSize("ngram_size_min must be greater than 0.");
  }
  if (ngram_size_min > ngram_size_max) {
    return error::InvalidNgramSize(
        "ngram_size_min cannot be greater than ngram_size_max.");
  }
  if (ngram_size_max > kMaxAllowedNgramSizeMax) {
    return error::InvalidNgramSize(
        absl::StrCat("ngram_size_max cannot be greater than ",
                     kMaxAllowedNgramSizeMax, "."));
  }

  return absl::OkStatus();
}

absl::StatusOr<zetasql::Value> NgramsTokenizer::Tokenize(
    absl::Span<const zetasql::Value> args) {
  // argument indexes
  constexpr int kValue = 0;
  constexpr int kNgramMax = 1;
  constexpr int kNgramMin = 2;

  int ngram_size_max =
      GetIntParameterValue(args, kNgramMax, kDefaultNgramSizeMax);
  int ngram_size_min =
      GetIntParameterValue(args, kNgramMin, kDefaultNgramSizeMin);
  ZETASQL_RETURN_IF_ERROR(ValidateNgramSize(ngram_size_min, ngram_size_max));

  std::vector<std::string> token_list;
  const zetasql::Value& text = args[kValue];
  // Always add tokenize function signature as the first token. Also add value
  // to indicate if the source is null.
  // remove_diacritics argument is not currently respected so it is not embedded
  // in the signature.
  std::string tokenize_signature =
      absl::StrCat(kNgramsTokenizer, "-", ngram_size_max, "-", ngram_size_min,
                   "-", std::to_string(text.is_null()));
  token_list.push_back(tokenize_signature);

  if (!text.is_null()) {
    auto tokenize_single_value =
        [&](const zetasql::Value& value) -> absl::Status {
      std::string lower_str;
      absl::Status status;
      zetasql::functions::LowerUtf8(value.string_value(), &lower_str,
                                      &status);
      ZETASQL_RETURN_IF_ERROR(status);
      return TokenizeNgrams(lower_str, ngram_size_min, ngram_size_max,
                            token_list);
    };

    if (text.type()->IsArray()) {
      for (auto& value : text.elements()) {
        ZETASQL_RETURN_IF_ERROR(tokenize_single_value(value));
        token_list.push_back(kGapString);
      }
    } else {
      ZETASQL_RETURN_IF_ERROR(tokenize_single_value(text));
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
