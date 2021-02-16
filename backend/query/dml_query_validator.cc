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

#include "backend/query/dml_query_validator.h"

#include "common/constants.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

absl::Status DMLQueryValidator::VisitResolvedFunctionCall(
    const zetasql::ResolvedFunctionCall* node) {
  const std::string name = node->function()->FullName(false);
  // Check if the function is a DML-specific function.
  if (name == kPendingCommitTimestampFunctionName) {
    return absl::OkStatus();
  }
  // Check for other generally-available functions.
  return QueryValidator::VisitResolvedFunctionCall(node);
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
