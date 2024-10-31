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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_SCORE_NGRAMS_EVALUATOR_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_SCORE_NGRAMS_EVALUATOR_H_

#include <cstdint>
#include <string>

#include "zetasql/public/value.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

class ScoreNgramsEvaluator {
 public:
  static absl::StatusOr<zetasql::Value> Evaluate(
      absl::Span<const zetasql::Value> args);

 private:
  static constexpr int64_t kTrigrams = 3;

  static absl::Status BuildTrigrams(
      const zetasql::Value& tokenlist, absl::string_view query_string,
      bool& source_is_null,
      absl::flat_hash_set<std::string>& tokenlist_trigrams,
      absl::flat_hash_set<std::string>& query_trigrams);

  static int64_t NumMatchingTrigrams(
      absl::flat_hash_set<std::string>& tokenlist_trigrams,
      absl::flat_hash_set<std::string>& query_trigrams);
};

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_SCORE_NGRAMS_EVALUATOR_H_
