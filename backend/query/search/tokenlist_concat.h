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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_TOKENLIST_CONCAT_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_TOKENLIST_CONCAT_H_

#include "zetasql/public/value.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"

namespace google::spanner::emulator::backend::query::search {

// Generates a TOKENLIST by concatenating array of TOKENLIST. Note different
// types of TOKENLIST can be still concatenated together, but only meaningful
// concatenation will be with the same type, either fulltext or substring.
// Currently emulator support tokenlist concatenation via TOKENLIST_CONCAT, but
// not with CONCAT yet.
class TokenlistConcat {
 public:
  static absl::StatusOr<zetasql::Value> Concat(
      absl::Span<const zetasql::Value> args);
};

}  // namespace google::spanner::emulator::backend::query::search

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_TOKENLIST_CONCAT_H_
