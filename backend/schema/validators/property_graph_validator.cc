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

#include "backend/schema/validators/property_graph_validator.h"

#include <string>

#include "absl/status/status.h"
#include "backend/common/case.h"
#include "backend/schema/catalog/property_graph.h"
#include "backend/schema/updater/global_schema_names.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "common/errors.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

absl::Status PropertyGraphValidator::Validate(
    const PropertyGraph* graph, SchemaValidationContext* context) {
  ZETASQL_RET_CHECK(!graph->name_.empty());
  ZETASQL_RETURN_IF_ERROR(
      GlobalSchemaNames::ValidateSchemaName("PropertyGraph", graph->name_));

  CaseInsensitiveStringSet unique_label_names;
  for (const PropertyGraph::Label& label : graph->Labels()) {
    if (auto it = unique_label_names.insert(label.name); !it.second) {
      return error::PropertyGraphDuplicateLabel(graph->name_, label.name);
    }
  }

  CaseInsensitiveStringSet unique_property_declaration_names;
  for (const PropertyGraph::PropertyDeclaration& property_declaration :
       graph->PropertyDeclarations()) {
    if (auto it =
            unique_property_declaration_names.insert(property_declaration.name);
        !it.second) {
      return error::PropertyGraphDuplicatePropertyDeclaration(
          graph->name_, property_declaration.name);
    }
  }
  // TODO: Add dependency validation.
  return absl::OkStatus();
}

absl::Status PropertyGraphValidator::ValidateGraphElementTable(
    const PropertyGraph* graph, const PropertyGraph::GraphElementTable* table,
    SchemaValidationContext* context) {
  CaseInsensitiveStringSet graph_label_names;
  for (const PropertyGraph::Label& label : graph->Labels()) {
    graph_label_names.insert(label.name);
  }
  for (const std::string& label_name : table->label_names()) {
    if (!graph_label_names.contains(label_name)) {
      return error::GraphElementTableLabelNotFound(graph->name_, table->name(),
                                                   label_name);
    }
  }
  CaseInsensitiveStringSet graph_property_declaration_names;
  for (const PropertyGraph::PropertyDeclaration& property_declaration :
       graph->PropertyDeclarations()) {
    graph_property_declaration_names.insert(property_declaration.name);
  }
  for (const PropertyGraph::GraphElementTable::PropertyDefinition&
           property_definition : table->property_definitions()) {
    if (!graph_property_declaration_names.contains(property_definition.name)) {
      return error::GraphElementTablePropertyDefinitionNotFound(
          graph->name_, table->name(), property_definition.name);
    }
  }

  CaseInsensitiveStringSet graph_node_table_names;
  for (const PropertyGraph::GraphElementTable& node_table :
       graph->NodeTables()) {
    graph_node_table_names.insert(node_table.name());
  }
  if (table->element_kind() == PropertyGraph::GraphElementKind::EDGE) {
    if (!graph_node_table_names.contains(
            table->source_node_reference().node_table_name)) {
      return error::GraphEdgeTableSourceNodeTableNotFound(
          graph->name_, table->name(),
          table->source_node_reference().node_table_name);
    }
    if (!graph_node_table_names.contains(
            table->target_node_reference().node_table_name)) {
      return error::GraphEdgeTableDestinationNodeTableNotFound(
          graph->name_, table->name(),
          table->target_node_reference().node_table_name);
    }
  }
  return absl::OkStatus();
}

absl::Status PropertyGraphValidator::ValidateUpdate(
    const PropertyGraph* graph, const PropertyGraph* old_graph,
    SchemaValidationContext* context) {
  if (graph->is_deleted()) {
    context->global_names()->RemoveName(graph->name_);
    return absl::OkStatus();
  }
  ZETASQL_RET_CHECK_EQ(graph->name_, old_graph->name_);
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
