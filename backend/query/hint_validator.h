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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_HINT_VALIDATOR_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_HINT_VALIDATOR_H_

#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_visitor.h"
#include "backend/common/case.h"
#include "backend/schema/catalog/schema.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Implements ResolvedASTVisitor to validate hints in an AST.
class HintValidator : public zetasql::ResolvedASTVisitor {
 public:
  explicit HintValidator(const Schema* schema) : schema_(schema) {}

  absl::Status DefaultVisit(const zetasql::ResolvedNode* node) override {
    ZETASQL_RETURN_IF_ERROR(ValidateHints(node));
    return zetasql::ResolvedASTVisitor::DefaultVisit(node);
  }

 private:
  // Validates the child hint nodes of `node`.
  absl::Status ValidateHints(const zetasql::ResolvedNode* node) const;

  // Returns an OK if `name` is a supported hint name for nodes with kind
  // `node_kind`; otherwise, returns an invalid argument error.
  absl::Status CheckHintName(absl::string_view name,
                             const zetasql::ResolvedNodeKind node_kind) const;

  // Returns an OK if `value` represents a valid value for hints with name
  // `name` specified on a node of kind `node_kind`; otherwise, returns an
  // invalid argument error. `hint_map` is initialized to a mapping of
  // hint_name->hint_value of all the hints specified on the node.
  absl::Status CheckHintValue(
      absl::string_view name, const zetasql::Value& value,
      const zetasql::ResolvedNodeKind node_kind,
      const absl::flat_hash_map<absl::string_view, zetasql::Value>& hint_map)
      const;

  const Schema* schema_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_HINT_VALIDATOR_H_
