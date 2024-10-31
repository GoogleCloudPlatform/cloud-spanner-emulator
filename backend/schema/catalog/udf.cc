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

#include "backend/schema/catalog/udf.h"

#include "absl/status/status.h"
#include "backend/schema/graph/schema_graph_editor.h"
#include "backend/schema/graph/schema_node.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

absl::Status Udf::Validate(SchemaValidationContext* context) const {
  return validate_(this, context);
}

absl::Status Udf::ValidateUpdate(const SchemaNode* orig,
                                 SchemaValidationContext* context) const {
  return validate_update_(this, orig->As<const Udf>(), context);
}

absl::Status Udf::DeepClone(SchemaGraphEditor* editor, const SchemaNode* orig) {
  // We just need to propagate the clone event to dependencies and dependents.
  for (const SchemaNode*& dep : dependencies_) {
    ZETASQL_ASSIGN_OR_RETURN(const SchemaNode* cloned_dep, editor->Clone(dep));
    dep = cloned_dep;
  }

  for (const SchemaNode*& dependent : dependents_) {
    ZETASQL_ASSIGN_OR_RETURN(const SchemaNode* cloned_dependent,
                     editor->Clone(dependent));
    dependent = cloned_dependent;
  }
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
