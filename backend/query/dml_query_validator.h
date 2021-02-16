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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_DML_QUERY_VALIDATOR_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_DML_QUERY_VALIDATOR_H_

#include "absl/status/status.h"
#include "backend/query/query_validator.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Extends QueryValidator to validate DML specific nodes in an AST.
class DMLQueryValidator : public QueryValidator {
 public:
  DMLQueryValidator(const Schema* schema, QueryEngineOptions* options)
      : QueryValidator(schema, options) {}

 protected:
  absl::Status VisitResolvedFunctionCall(
      const zetasql::ResolvedFunctionCall* node) override;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_DML_QUERY_VALIDATOR_H_
