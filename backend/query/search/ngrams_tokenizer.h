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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_NGRAMS_TOKENIZER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_NGRAMS_TOKENIZER_H_

#include <cstdint>

#include "zetasql/public/value.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

class NgramsTokenizer {
  // Generate tokenlist value by doing ngrams tokenization on the original
  // string (without splitting into separate words) from source column.
 public:
  static absl::StatusOr<zetasql::Value> Tokenize(
      absl::Span<const zetasql::Value> args);

 private:
  static constexpr int64_t kMaxAllowedNgramSizeMax = 12;
  static constexpr int64_t kDefaultNgramSizeMax = 4;
  static constexpr int64_t kDefaultNgramSizeMin = 1;

  static absl::Status ValidateNgramSize(int ngram_size_min, int ngram_size_max);
};

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_NGRAMS_TOKENIZER_H_
