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

#include "backend/schema/catalog/property_graph.h"

#include <string>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "backend/schema/graph/schema_graph_editor.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/updater/schema_validation_context.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

absl::Status PropertyGraph::Validate(SchemaValidationContext* context) const {
  return validate_(this, context);
}

absl::Status PropertyGraph::ValidateUpdate(
    const SchemaNode* orig, SchemaValidationContext* context) const {
  return validate_update_(this, orig->As<const PropertyGraph>(), context);
}

absl::Status PropertyGraph::DeepClone(SchemaGraphEditor* editor,
                                      const SchemaNode* orig) {
  return absl::OkStatus();
}

std::string PropertyGraph::DebugString() const {
  std::string debug_string;
  absl::StrAppend(&debug_string, "PropertyGraph(\"", name_, "\") {\n");
  absl::StrAppend(&debug_string, "  ddl_body: \"", ddl_body_, "\"\n");
  absl::StrAppend(&debug_string, "  property_declarations: [\n");
  for (const auto& property_declaration : property_declarations_) {
    absl::StrAppend(&debug_string, "    { name: \"", property_declaration.name,
                    "\", type: \"", property_declaration.type, "\" },\n");
  }
  absl::StrAppend(&debug_string, "  ]\n");
  absl::StrAppend(&debug_string, "  labels: [\n");
  for (const auto& label : labels_) {
    absl::StrAppend(&debug_string, "    { name: \"", label.name,
                    "\", properties: [");
    for (const auto& property_name : label.property_names) {
      absl::StrAppend(&debug_string, "\"", property_name, "\", ");
    }
    absl::StrAppend(&debug_string, "] },\n");
  }
  absl::StrAppend(&debug_string, "  ]\n");
  absl::StrAppend(&debug_string, "  node_tables: [\n");
  for (const auto& node_table_element : node_tables_) {
    absl::StrAppend(&debug_string, "    ", node_table_element.DebugString(),
                    ",\n");
  }
  absl::StrAppend(&debug_string, "  ]\n");
  absl::StrAppend(&debug_string, "  edge_tables: [\n");
  for (const auto& edge_table_element : edge_tables_) {
    absl::StrAppend(&debug_string, "    ", edge_table_element.DebugString(),
                    ",\n");
  }
  absl::StrAppend(&debug_string, "  ]\n");
  absl::StrAppend(&debug_string, "}");
  return debug_string;
}

std::string PropertyGraph::GraphElementTable::DebugString() const {
  std::string debug_string;
  absl::StrAppend(&debug_string, "GraphElementTable {\n");
  absl::StrAppend(&debug_string, "  name: ", name_, "\n");
  absl::StrAppend(&debug_string, "  alias: ", alias_, "\n");
  absl::StrAppend(&debug_string, "  element_kind: ", element_kind_, "\n");
  absl::StrAppend(&debug_string, "  key_clause_columns: ",
                  absl::StrJoin(key_clause_columns_, ", "), "\n");
  absl::StrAppend(&debug_string,
                  "  label_names: ", absl::StrJoin(label_names_, ", "), "\n");
  if (element_kind_ == GraphElementKind::EDGE) {
    absl::StrAppend(&debug_string, "  source_node_reference: ",
                    source_node_reference_.DebugString(), "\n");
    absl::StrAppend(&debug_string, "  target_node_reference: ",
                    target_node_reference_.DebugString(), "\n");
  }
  absl::StrAppend(
      &debug_string, "  property_definitions: ",
      absl::StrJoin(property_definitions_, ", ",
                    [](std::string* out, const PropertyDefinition& pd) {
                      absl::StrAppend(out, "{name: ", pd.name,
                                      ", value_expression_string: ",
                                      pd.value_expression_string, "}");
                    }),
      "\n");
  absl::StrAppend(&debug_string, "}");
  return debug_string;
}

absl::Status PropertyGraph::FindLabelByName(
    absl::string_view name, const PropertyGraph::Label*& label) const {
  for (const auto& l : labels_) {
    if (l.name == name) {
      label = &l;
      return absl::OkStatus();
    }
  }
  return absl::NotFoundError(absl::StrCat("Label not found: ", name));
}

absl::Status PropertyGraph::FindPropertyDeclarationByName(
    absl::string_view name,
    const PropertyGraph::PropertyDeclaration*& property_declaration) const {
  for (const auto& pd : property_declarations_) {
    if (pd.name == name) {
      property_declaration = &pd;
      return absl::OkStatus();
    }
  }
  return absl::NotFoundError(
      absl::StrCat("Property declaration not found: ", name));
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
