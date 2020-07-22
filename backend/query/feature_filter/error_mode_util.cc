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

#include "backend/query/feature_filter/error_mode_util.h"

#include <string>

#include "zetasql/public/function.h"
#include "absl/container/flat_hash_set.h"
#include "absl/strings/string_view.h"

namespace google::spanner::emulator::backend {

bool SupportsSafeErrorMode(const zetasql::Function* function) {
  if (function->GetGroup() == "UDF") {
    return false;
  }

  if (function->IsAggregate()) {
    return false;
  }

  const std::string name = function->FullName(false);
  static const auto* supported_functions =
      new absl::flat_hash_set<absl::string_view>{
          "$safe_array_at_offset",
          "$safe_array_at_ordinal",
          "st_expr_eval",
          "supported_optimizer_versions",
          "test_fn_nondeterministic_value",
      };
  if (supported_functions->contains(name)) {
    return true;
  }
  return function->IsZetaSQLBuiltin() && function->SupportsSafeErrorMode();
}

}  // namespace google::spanner::emulator::backend
