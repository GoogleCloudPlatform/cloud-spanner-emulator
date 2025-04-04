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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_ANN_FUNCTIONS_REWRITER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_ANN_FUNCTIONS_REWRITER_H_

#include <string>

#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

bool IsANNFunction(std::string function_name);

// Implements ResolvedASTDeepCopyVisitor to rewrite ANN functions for ZetaSQL
class ANNFunctionsRewriter : public zetasql::ResolvedASTDeepCopyVisitor {
 public:
  absl::Status VisitResolvedFunctionCall(
      const zetasql::ResolvedFunctionCall* node) override;

  absl::flat_hash_set<const zetasql::ResolvedFunctionCall*> ann_functions() {
    return ann_functions_;
  }

 private:
  // This will be used to compare with the ANN functions stored in the
  // ANNValidator to make sure all the ANN functions passed the validation.
  absl::flat_hash_set<const zetasql::ResolvedFunctionCall*> ann_functions_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_ANN_FUNCTIONS_REWRITER_H_
