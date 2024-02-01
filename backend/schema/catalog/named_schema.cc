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

#include "backend/schema/catalog/named_schema.h"

#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.pb.h"
#include "absl/status/status.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/catalog/sequence.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/catalog/view.h"
#include "backend/schema/graph/schema_graph_editor.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

absl::Status NamedSchema::Validate(SchemaValidationContext* context) const {
  return validate_(this, context);
}

absl::Status NamedSchema::ValidateUpdate(
    const SchemaNode* orig, SchemaValidationContext* context) const {
  return validate_update_(this, orig->template As<const NamedSchema>(),
                          context);
}

absl::Status NamedSchema::DeepClone(SchemaGraphEditor* editor,
                                    const SchemaNode* orig) {
  for (auto& table : tables_) {
    ZETASQL_ASSIGN_OR_RETURN(const auto* schema_node, editor->Clone(table));
    table = schema_node->As<const Table>();
  }
  for (auto& index : indexes_) {
    ZETASQL_ASSIGN_OR_RETURN(const auto* schema_node, editor->Clone(index));
    index = schema_node->As<const Index>();
  }
  for (auto& view : views_) {
    ZETASQL_ASSIGN_OR_RETURN(const auto* schema_node, editor->Clone(view));
    view = schema_node->As<const View>();
  }
  for (auto& sequence : sequences_) {
    ZETASQL_ASSIGN_OR_RETURN(const auto* schema_node, editor->Clone(sequence));
    sequence = schema_node->As<const Sequence>();
  }
  return absl::OkStatus();
}
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
