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

#include "backend/query/search/exact_match_tokenizer.h"

#include <string>

#include "absl/strings/string_view.h"
#include "backend/query/search/tokenizer.h"
#include "zetasql/base/ret_check.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

absl::StatusOr<zetasql::Value> ExactMatchTokenizer::Tokenize(
    absl::Span<const zetasql::Value> args) {
  bool isValidType = args[0].type()->IsString() || args[0].type()->IsBytes();
  if (!isValidType && args[0].type()->IsArray()) {
    const zetasql::Type* element_type =
        args[0].type()->AsArray()->element_type();
    isValidType = element_type->IsString() || element_type->IsBytes();
  }
  ZETASQL_RET_CHECK(isValidType);

  return TokenListFromStrings({std::string(kExactMatchTokenizer)});
}

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
