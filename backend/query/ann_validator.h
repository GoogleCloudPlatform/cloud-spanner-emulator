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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_ANN_VALIDATOR_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_ANN_VALIDATOR_H_

#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_visitor.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/catalog/schema.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
// ANNValidator is a ResolvedASTVisitor that validates the usage of
// approximate nearest neighbor (ANN) functions in a query. It checks that
// ANN functions are used correctly with vector indexes, including:
//   - The ANN function is used with a vector index.
//   - The query has an ORDER BY clause with a single column that is the ANN
//     function.
//   - The distance type of the ANN function matches the distance type of the
//     vector index.
//   - The vector length of the ANN function matches the vector length of the
//     vector index.
//   - The column used in ANN query must be null filtered if the vector index
//     used is null filtered.
//   - The ANN function is not used in a join.
//   - The ANN function is not used with a zero vector for cosine distance.
//   - The two inputs of the ANN function must be a column reference and one of
//     parameter or literal.
//
// ANNValidator also keeps track of the ANN functions it has visited to make
// sure all the ANN functions passed the validation.
class ANNValidator : public zetasql::ResolvedASTVisitor {
 public:
  explicit ANNValidator(const Schema* schema) : schema_(schema) {}

  absl::Status VisitResolvedLimitOffsetScan(
      const zetasql::ResolvedLimitOffsetScan* node) override;

  absl::flat_hash_set<const zetasql::ResolvedFunctionCall*>& ann_functions() {
    return ann_functions_;
  }

 private:
  // The database schema.
  const Schema* schema_;

  absl::flat_hash_set<const zetasql::ResolvedFunctionCall*> ann_functions_;
};
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_ANN_VALIDATOR_H_
