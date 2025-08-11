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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERYABLE_PROPERTY_GRAPH_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERYABLE_PROPERTY_GRAPH_H_

#include <memory>
#include <string>
#include <vector>

#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/property_graph.h"
#include "zetasql/public/types/type.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "backend/common/case.h"
#include "backend/schema/catalog/property_graph.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Forward declarations
class QueryablePropertyGraph;
class QueryableGraphElementTable;
class QueryableGraphNodeTable;
class QueryableGraphEdgeTable;
class QueryableGraphElementLabel;
class QueryableGraphNodeTableReference;
class QueryableGraphPropertyDeclaration;
class QueryableGraphPropertyDefinition;

class QueryableGraphPropertyDeclaration
    : public zetasql::GraphPropertyDeclaration {
 public:
  explicit QueryableGraphPropertyDeclaration(
      const QueryablePropertyGraph* property_graph, zetasql::Catalog* catalog,
      zetasql::TypeFactory* type_factory,
      const google::spanner::emulator::backend::PropertyGraph::
          PropertyDeclaration* wrapped_property_declaration);

  std::string FullName() const override;
  std::string Name() const override {
    return wrapped_property_declaration_->name;
  }
  const zetasql::Type* Type() const override { return type_; }

 private:
  const QueryablePropertyGraph* const property_graph_;
  const google::spanner::emulator::backend::PropertyGraph::
      PropertyDeclaration* const wrapped_property_declaration_;
  const zetasql::Type* type_;
};

class QueryableGraphPropertyDefinition
    : public zetasql::GraphPropertyDefinition {
 public:
  QueryableGraphPropertyDefinition(
      zetasql::Catalog* catalog, zetasql::TypeFactory* type_factory,
      const zetasql::Table* data_source_table,
      const zetasql::GraphPropertyDeclaration* property_declaration,
      const google::spanner::emulator::backend::PropertyGraph::
          GraphElementTable::PropertyDefinition* wrapped_property_definition);

  const zetasql::GraphPropertyDeclaration& GetDeclaration() const override {
    return *property_declaration_;
  }

  absl::string_view expression_sql() const override {
    return wrapped_property_definition_->value_expression_string;
  }

  absl::StatusOr<const zetasql::ResolvedExpr*> GetValueExpression()
      const override {
    return analyzer_output_->resolved_expr();
  }

 private:
  const zetasql::GraphPropertyDeclaration* property_declaration_;
  const google::spanner::emulator::backend::PropertyGraph::GraphElementTable::
      PropertyDefinition* const wrapped_property_definition_;
  std::unique_ptr<const zetasql::AnalyzerOutput> analyzer_output_;
};

class QueryableGraphElementLabel : public zetasql::GraphElementLabel {
 public:
  QueryableGraphElementLabel(
      const QueryablePropertyGraph* property_graph,
      const google::spanner::emulator::backend::PropertyGraph::Label* label)
      : property_graph_(property_graph), label_(label) {}

  std::string FullName() const override;
  std::string Name() const override { return label_->name; }

  absl::Status GetPropertyDeclarations(
      absl::flat_hash_set<const zetasql::GraphPropertyDeclaration*>& output)
      const override;

 private:
  const QueryablePropertyGraph* const property_graph_;
  const google::spanner::emulator::backend::PropertyGraph::Label* const label_;
};

class QueryableGraphDynamicLabel : public zetasql::GraphDynamicLabel {
 public:
  QueryableGraphDynamicLabel(
      zetasql::Catalog* catalog, zetasql::TypeFactory* type_factory,
      const QueryablePropertyGraph* property_graph,
      const google::spanner::emulator::backend::PropertyGraph::
          GraphElementTable* element_table);

  absl::string_view label_expression() const final { return label_expression_; }

  absl::StatusOr<const zetasql::ResolvedExpr*> GetValueExpression()
      const final {
    return analyzer_output_->resolved_expr();
  }

 private:
  std::string label_expression_;
  std::unique_ptr<const zetasql::AnalyzerOutput> analyzer_output_;
};

class QueryableGraphDynamicProperties
    : public zetasql::GraphDynamicProperties {
 public:
  QueryableGraphDynamicProperties(
      zetasql::Catalog* catalog, zetasql::TypeFactory* type_factory,
      const QueryablePropertyGraph* property_graph,
      const google::spanner::emulator::backend::PropertyGraph::
          GraphElementTable* element_table);

  absl::string_view properties_expression() const final {
    return properties_expression_;
  }

  absl::StatusOr<const zetasql::ResolvedExpr*> GetValueExpression()
      const final {
    if (analyzer_output_ == nullptr) {
      return absl::InternalError("Analyzer output is null");
    }
    return analyzer_output_->resolved_expr();
  }

 private:
  std::string properties_expression_;
  std::unique_ptr<const zetasql::AnalyzerOutput> analyzer_output_;
};

class QueryableGraphElementTableInternal {
 public:
  QueryableGraphElementTableInternal(
      zetasql::Catalog* catalog, zetasql::TypeFactory* type_factory,
      const QueryablePropertyGraph* property_graph,
      const google::spanner::emulator::backend::PropertyGraph::
          GraphElementTable* element_table);

  std::string Name() const { return wrapped_element_table_->name(); }
  absl::Span<const std::string> PropertyGraphNamePath() const;
  std::string FullName() const;

  const zetasql::Table* GetTable() const;

  const std::vector<int>& GetKeyColumns() const {
    return ordinal_key_columns_idxs_;
  }

  absl::Status FindPropertyDefinitionByName(
      absl::string_view property_name,
      const zetasql::GraphPropertyDefinition*& property_definition) const;

  absl::Status GetPropertyDefinitions(
      absl::flat_hash_set<const zetasql::GraphPropertyDefinition*>& output)
      const;

  absl::Status FindLabelByName(
      absl::string_view name, const zetasql::GraphElementLabel*& label) const;

  absl::Status GetLabels(
      absl::flat_hash_set<const zetasql::GraphElementLabel*>& output) const;

  bool HasDynamicLabel() const { return dynamic_label_.has_value(); }
  absl::Status GetDynamicLabel(
      const zetasql::GraphDynamicLabel*& dynamic_label) const {
    if (!dynamic_label_.has_value()) {
      return absl::NotFoundError(absl::StrCat(
          "Dynamic label is not configured for the element table: ", Name()));
    }
    dynamic_label = dynamic_label_.value().get();
    return absl::OkStatus();
  }

  bool HasDynamicProperties() const { return dynamic_properties_.has_value(); }
  absl::Status GetDynamicProperties(
      const zetasql::GraphDynamicProperties*& dynamic_properties) const {
    if (!dynamic_properties_.has_value()) {
      return absl::NotFoundError(
          absl::StrCat("Dynamic properties is not configured for the element "
                       "table: ",
                       Name()));
    }
    dynamic_properties = dynamic_properties_.value().get();
    return absl::OkStatus();
  }

 protected:
  const QueryablePropertyGraph* const property_graph_;
  const google::spanner::emulator::backend::PropertyGraph::
      GraphElementTable* const wrapped_element_table_;
  const zetasql::Table* data_source_table_;

  // Pointers to the labels applicable to this element table.
  // Labels are owned by the 'QueryablePropertyGraph'.
  CaseInsensitiveStringMap<const zetasql::GraphElementLabel*> label_ptrs_;

  std::vector<int> ordinal_key_columns_idxs_;
  CaseInsensitiveStringMap<
      std::unique_ptr<const QueryableGraphPropertyDefinition>>
      property_definitions_;
  std::optional<std::unique_ptr<const QueryableGraphDynamicLabel>>
      dynamic_label_;
  std::optional<std::unique_ptr<const QueryableGraphDynamicProperties>>
      dynamic_properties_;
};

class QueryableGraphNodeTable : public zetasql::GraphNodeTable {
 public:
  QueryableGraphNodeTable(zetasql::Catalog* catalog,
                          zetasql::TypeFactory* type_factory,
                          const QueryablePropertyGraph* property_graph,
                          const google::spanner::emulator::backend::
                              PropertyGraph::GraphElementTable* node_table);

  Kind kind() const override { return Kind::kNode; }
  std::string Name() const override { return element_table_internals_->Name(); }
  absl::Span<const std::string> PropertyGraphNamePath() const override {
    return element_table_internals_->PropertyGraphNamePath();
  };
  std::string FullName() const override {
    return element_table_internals_->FullName();
  };

  const zetasql::Table* GetTable() const override {
    return element_table_internals_->GetTable();
  }

  const std::vector<int>& GetKeyColumns() const override {
    return element_table_internals_->GetKeyColumns();
  }

  absl::Status FindPropertyDefinitionByName(
      absl::string_view property_name,
      const zetasql::GraphPropertyDefinition*& property_definition)
      const override {
    return element_table_internals_->FindPropertyDefinitionByName(
        property_name, property_definition);
  }

  absl::Status GetPropertyDefinitions(
      absl::flat_hash_set<const zetasql::GraphPropertyDefinition*>& output)
      const override {
    return element_table_internals_->GetPropertyDefinitions(output);
  }

  absl::Status FindLabelByName(
      absl::string_view name,
      const zetasql::GraphElementLabel*& label) const override {
    return element_table_internals_->FindLabelByName(name, label);
  }

  absl::Status GetLabels(
      absl::flat_hash_set<const zetasql::GraphElementLabel*>& output)
      const override {
    return element_table_internals_->GetLabels(output);
  }

  bool HasDynamicLabel() const override {
    return element_table_internals_->HasDynamicLabel();
  }
  absl::Status GetDynamicLabel(
      const zetasql::GraphDynamicLabel*& dynamic_label) const override {
    return element_table_internals_->GetDynamicLabel(dynamic_label);
  }

  bool HasDynamicProperties() const override {
    return element_table_internals_->HasDynamicProperties();
  }
  absl::Status GetDynamicProperties(const zetasql::GraphDynamicProperties*&
                                        dynamic_properties) const override {
    return element_table_internals_->GetDynamicProperties(dynamic_properties);
  }

 private:
  std::unique_ptr<QueryableGraphElementTableInternal> element_table_internals_;
};

class QueryableGraphNodeTableReference
    : public zetasql::GraphNodeTableReference {
 public:
  QueryableGraphNodeTableReference(
      zetasql::Catalog* catalog, const QueryablePropertyGraph* property_graph,
      const google::spanner::emulator::backend::PropertyGraph::
          GraphElementTable::GraphNodeReference* wrapped_node_reference,
      const google::spanner::emulator::backend::PropertyGraph::
          GraphElementTable* wrapped_edge_table);

  const zetasql::GraphNodeTable* GetReferencedNodeTable() const override;

  const std::vector<int>& GetEdgeTableColumns() const override {
    return edge_table_column_idxs_;
  }

  const std::vector<int>& GetNodeTableColumns() const override {
    return node_table_column_idxs_;
  }

 private:
  const QueryablePropertyGraph* const property_graph_;
  const google::spanner::emulator::backend::PropertyGraph::GraphElementTable::
      GraphNodeReference* const wrapped_node_reference_;
  std::vector<int> node_table_column_idxs_;
  std::vector<int> edge_table_column_idxs_;
};

class QueryableGraphEdgeTable : public zetasql::GraphEdgeTable {
 public:
  QueryableGraphEdgeTable(zetasql::Catalog* catalog,
                          zetasql::TypeFactory* type_factory,
                          const QueryablePropertyGraph* property_graph,
                          const google::spanner::emulator::backend::
                              PropertyGraph::GraphElementTable* edge_table);

  Kind kind() const override { return Kind::kEdge; }
  std::string Name() const override { return element_table_internals_->Name(); }
  absl::Span<const std::string> PropertyGraphNamePath() const override {
    return element_table_internals_->PropertyGraphNamePath();
  };
  std::string FullName() const override {
    return element_table_internals_->FullName();
  };

  const zetasql::Table* GetTable() const override {
    return element_table_internals_->GetTable();
  }

  const std::vector<int>& GetKeyColumns() const override {
    return element_table_internals_->GetKeyColumns();
  }

  absl::Status FindPropertyDefinitionByName(
      absl::string_view property_name,
      const zetasql::GraphPropertyDefinition*& property_definition)
      const override {
    return element_table_internals_->FindPropertyDefinitionByName(
        property_name, property_definition);
  }

  absl::Status GetPropertyDefinitions(
      absl::flat_hash_set<const zetasql::GraphPropertyDefinition*>& output)
      const override {
    return element_table_internals_->GetPropertyDefinitions(output);
  }

  absl::Status FindLabelByName(
      absl::string_view name,
      const zetasql::GraphElementLabel*& label) const override {
    return element_table_internals_->FindLabelByName(name, label);
  }

  absl::Status GetLabels(
      absl::flat_hash_set<const zetasql::GraphElementLabel*>& output)
      const override {
    return element_table_internals_->GetLabels(output);
  }
  const zetasql::GraphNodeTableReference* GetSourceNodeTable()
      const override {
    return source_node_table_reference_.get();
  }

  const zetasql::GraphNodeTableReference* GetDestNodeTable() const override {
    return target_node_table_reference_.get();
  }

  bool HasDynamicLabel() const override {
    return element_table_internals_->HasDynamicLabel();
  }
  absl::Status GetDynamicLabel(
      const zetasql::GraphDynamicLabel*& dynamic_label) const override {
    return element_table_internals_->GetDynamicLabel(dynamic_label);
  }

  bool HasDynamicProperties() const override {
    return element_table_internals_->HasDynamicProperties();
  }
  absl::Status GetDynamicProperties(const zetasql::GraphDynamicProperties*&
                                        dynamic_properties) const override {
    return element_table_internals_->GetDynamicProperties(dynamic_properties);
  }

 private:
  std::unique_ptr<QueryableGraphElementTableInternal> element_table_internals_;
  std::unique_ptr<QueryableGraphNodeTableReference>
      source_node_table_reference_;
  std::unique_ptr<QueryableGraphNodeTableReference>
      target_node_table_reference_;
};

class QueryablePropertyGraph : public zetasql::PropertyGraph {
 public:
  QueryablePropertyGraph(
      zetasql::Catalog* catalog, zetasql::TypeFactory* type_factory,
      const google::spanner::emulator::backend::PropertyGraph*
          wrapped_property_graph)
      : wrapped_property_graph_(wrapped_property_graph) {
    for (const auto& label : wrapped_property_graph->Labels()) {
      labels_[label.name] =
          std::make_unique<QueryableGraphElementLabel>(this, &label);
    }
    for (const auto& property_decl :
         wrapped_property_graph->PropertyDeclarations()) {
      property_declarations_[property_decl.name] =
          std::make_unique<QueryableGraphPropertyDeclaration>(
              this, catalog, type_factory, &property_decl);
    }
    for (const auto& node_table : wrapped_property_graph->NodeTables()) {
      node_tables_[node_table.name()] =
          std::make_unique<QueryableGraphNodeTable>(catalog, type_factory, this,
                                                    &node_table);
    }
    for (const auto& edge_table : wrapped_property_graph_->EdgeTables()) {
      edge_tables_[edge_table.name()] =
          std::make_unique<QueryableGraphEdgeTable>(catalog, type_factory, this,
                                                    &edge_table);
    }
  }

  zetasql::Catalog* catalog() const { return catalog_; }
  std::string Name() const override { return wrapped_property_graph_->Name(); }

  absl::Span<const std::string> NamePath() const override {
    // TODO: Support full graph name path after spanner supports it
    return {&wrapped_property_graph_->Name(), 1};
  }

  std::string FullName() const override { return Name(); }

  absl::Status FindLabelByName(
      absl::string_view name,
      const zetasql::GraphElementLabel*& label) const override {
    auto it = labels_.find(std::string(name));
    if (it == labels_.end()) {
      return absl::NotFoundError(absl::StrCat("Label not found: ", name));
    }
    label = it->second.get();
    return absl::OkStatus();
  }

  absl::Status FindPropertyDeclarationByName(
      absl::string_view name,
      const zetasql::GraphPropertyDeclaration*& property_declaration)
      const override {
    auto it = property_declarations_.find(std::string(name));
    if (it == property_declarations_.end()) {
      return absl::NotFoundError(
          absl::StrCat("Property declaration not found: ", name));
    }
    property_declaration = it->second.get();
    return absl::OkStatus();
  }

  absl::Status FindElementTableByName(
      absl::string_view name,
      const zetasql::GraphElementTable*& element_table) const override {
    auto node_it = node_tables_.find(std::string(name));
    if (node_it != node_tables_.end()) {
      element_table = node_it->second.get();
      return absl::OkStatus();
    }

    auto edge_it = edge_tables_.find(std::string(name));
    if (edge_it != edge_tables_.end()) {
      element_table = edge_it->second.get();
      return absl::OkStatus();
    }
    return absl::NotFoundError(absl::StrCat("Element table not found: ", name));
  }

  absl::Status GetNodeTables(
      absl::flat_hash_set<const zetasql::GraphNodeTable*>& output)
      const override {
    for (const auto& [_, node_table] : node_tables_) {
      output.insert(node_table.get());
    }
    return absl::OkStatus();
  }

  absl::Status GetEdgeTables(
      absl::flat_hash_set<const zetasql::GraphEdgeTable*>& output)
      const override {
    for (const auto& [_, edge_table] : edge_tables_) {
      output.insert(edge_table.get());
    }
    return absl::OkStatus();
  }

  absl::Status GetLabels(
      absl::flat_hash_set<const zetasql::GraphElementLabel*>& output)
      const override {
    for (const auto& [_, label] : labels_) {
      output.insert(label.get());
    }
    return absl::OkStatus();
  }

  absl::Status GetPropertyDeclarations(
      absl::flat_hash_set<const zetasql::GraphPropertyDeclaration*>& output)
      const override {
    for (const auto& [_, property_declaration] : property_declarations_) {
      output.insert(property_declaration.get());
    }
    return absl::OkStatus();
  }

 private:
  // The catalog to which this property graph belongs. Not owned.
  zetasql::Catalog* catalog_;
  const google::spanner::emulator::backend::PropertyGraph* const
      wrapped_property_graph_;

  CaseInsensitiveStringMap<std::unique_ptr<const QueryableGraphElementLabel>>
      labels_;
  CaseInsensitiveStringMap<
      std::unique_ptr<const QueryableGraphPropertyDeclaration>>
      property_declarations_;

  CaseInsensitiveStringMap<std::unique_ptr<const QueryableGraphNodeTable>>
      node_tables_;
  CaseInsensitiveStringMap<std::unique_ptr<const QueryableGraphEdgeTable>>
      edge_tables_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERYABLE_PROPERTY_GRAPH_H_
