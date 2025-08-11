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
#include "backend/schema/catalog/property_graph.pb.h"
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

inline void SetNodeTableReferenceProto(
    const PropertyGraph::GraphElementTable::GraphNodeReference& data,
    catalog::GraphNodeTableReferenceProto& proto) {
  proto.set_node_table_name(data.node_table_name);
  for (const auto& node_table_column_name : data.node_table_column_names) {
    proto.add_node_table_columns(node_table_column_name);
  }
  for (const auto& edge_table_column_name : data.edge_table_column_names) {
    proto.add_edge_table_columns(edge_table_column_name);
  }
}

catalog::PropertyGraphProto PropertyGraph::ToProto() const {
  catalog::PropertyGraphProto proto;
  proto.set_catalog("");
  proto.set_schema("");
  proto.set_name(name_);
  for (const auto& node : node_tables_) {
    auto& node_proto = *proto.add_node_tables();
    node_proto.set_name(node.name());
    node_proto.set_kind(catalog::GraphElementTableProto::NODE);
    node_proto.set_base_table_name(node.name());
    node_proto.set_base_catalog_name("");
    node_proto.set_base_schema_name("");
    for (const auto& key_clause_column : node.key_clause_columns()) {
      node_proto.add_key_columns(key_clause_column);
    }
    for (const auto& label_name : node.label_names()) {
      node_proto.add_label_names(label_name);
    }
    for (const auto& property_definition : node.property_definitions()) {
      auto& property_definition_proto = *node_proto.add_property_definitions();
      property_definition_proto.set_property_declaration_name(
          property_definition.name);
      property_definition_proto.set_value_expression_sql(
          property_definition.value_expression_string);
    }
    if (node.has_dynamic_label_expression()) {
      node_proto.set_dynamic_label_expr(node.dynamic_label_expression());
    }
    if (node.has_dynamic_properties_expression()) {
      node_proto.set_dynamic_property_expr(
          node.dynamic_properties_expression());
    }
  }
  for (const auto& edge : edge_tables_) {
    auto& edge_proto = *proto.add_edge_tables();
    edge_proto.set_name(edge.name());
    edge_proto.set_kind(catalog::GraphElementTableProto::EDGE);
    edge_proto.set_base_table_name(edge.name());
    edge_proto.set_base_catalog_name("");
    edge_proto.set_base_schema_name("");
    for (const auto& key_clause_column : edge.key_clause_columns()) {
      edge_proto.add_key_columns(key_clause_column);
    }
    for (const auto& label_name : edge.label_names()) {
      edge_proto.add_label_names(label_name);
    }
    for (const auto& property_definition : edge.property_definitions()) {
      auto& property_definition_proto = *edge_proto.add_property_definitions();
      property_definition_proto.set_property_declaration_name(
          property_definition.name);
      property_definition_proto.set_value_expression_sql(
          property_definition.value_expression_string);
    }
    if (edge.has_dynamic_label_expression()) {
      edge_proto.set_dynamic_label_expr(edge.dynamic_label_expression());
    }
    if (edge.has_dynamic_properties_expression()) {
      edge_proto.set_dynamic_property_expr(
          edge.dynamic_properties_expression());
    }
    SetNodeTableReferenceProto(edge.source_node_reference(),
                               *edge_proto.mutable_source_node_table());
    SetNodeTableReferenceProto(edge.target_node_reference(),
                               *edge_proto.mutable_destination_node_table());
  }
  for (const auto& label : labels_) {
    auto& label_proto = *proto.add_labels();
    label_proto.set_name(label.name);
    for (const auto& property_name : label.property_names) {
      label_proto.add_property_declaration_names(property_name);
    }
  }
  for (const auto& property_declaration : property_declarations_) {
    auto& property_declaration_proto = *proto.add_property_declarations();
    property_declaration_proto.set_name(property_declaration.name);
    property_declaration_proto.set_type(property_declaration.type);
  }
  return proto;
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
  if (has_dynamic_label_expression()) {
    absl::StrAppend(&debug_string,
                    "  dynamic_label_expression: ", dynamic_label_expression(),
                    "\n");
  }
  if (has_dynamic_properties_expression()) {
    absl::StrAppend(&debug_string, "  dynamic_properties_expression: ",
                    dynamic_properties_expression(), "\n");
  }
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
