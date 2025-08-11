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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_PROPERTY_GRAPH_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_PROPERTY_GRAPH_H_

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "backend/schema/catalog/property_graph.pb.h"
#include "backend/schema/graph/schema_node.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

class PropertyGraph : public SchemaNode {
 public:
  struct PropertyDeclaration {
    std::string name;
    std::string type;
  };

  struct Label {
    std::string name;
    absl::flat_hash_set<std::string> property_names;
  };

  enum GraphElementKind { UNDEFINED, NODE, EDGE };

  class GraphElementTable {
   public:
    struct PropertyDefinition {
      std::string name;
      std::string value_expression_string;
    };

    struct GraphNodeReference {
      // Referenced node table's name.
      std::string node_table_name;
      // Referenced node table's column names.
      std::vector<std::string> node_table_column_names;
      // Edge table's column names.
      std::vector<std::string> edge_table_column_names;

      std::string DebugString() const {
        return absl::StrCat("node_table_name: ", node_table_name, "\n",
                            "node_table_column_names: ",
                            absl::StrJoin(node_table_column_names, ", "), "\n",
                            "edge_table_column_names: ",
                            absl::StrJoin(edge_table_column_names, ", "));
      }
    };

    const std::string& name() const { return name_; }
    const std::string& alias() const { return alias_; }
    GraphElementKind element_kind() const { return element_kind_; }
    const std::vector<std::string>& key_clause_columns() const {
      return key_clause_columns_;
    }
    const std::vector<std::string>& label_names() const { return label_names_; }
    const std::vector<PropertyDefinition>& property_definitions() const {
      return property_definitions_;
    }
    const GraphNodeReference& source_node_reference() const {
      return source_node_reference_;
    }
    const GraphNodeReference& target_node_reference() const {
      return target_node_reference_;
    }

    void set_name(std::string_view name) { name_ = name; }
    void set_alias(std::string_view alias) { alias_ = alias; }
    void set_element_kind(GraphElementKind element_kind) {
      element_kind_ = element_kind;
    }
    void add_key_clause_column(std::string_view key_clause_column) {
      key_clause_columns_.push_back(std::string(key_clause_column));
    }
    void add_label_name(std::string_view label_name) {
      label_names_.push_back(std::string(label_name));
    }
    void add_property_definition(std::string_view name,
                                 std::string_view value_expression_string) {
      property_definitions_.push_back(
          {.name = std::string(name),
           .value_expression_string = std::string(value_expression_string)});
    }

    void set_source_node_reference(
        std::string_view node_table_name,
        const std::vector<std::string> node_table_column_names,
        const std::vector<std::string> edge_table_column_names) {
      source_node_reference_.node_table_name = std::string(node_table_name);
      source_node_reference_.node_table_column_names = node_table_column_names;
      source_node_reference_.edge_table_column_names = edge_table_column_names;
    }
    void set_target_node_reference(
        std::string_view node_table_name,
        const std::vector<std::string> node_table_column_names,
        const std::vector<std::string> edge_table_column_names) {
      target_node_reference_.node_table_name = std::string(node_table_name);
      target_node_reference_.node_table_column_names = node_table_column_names;
      target_node_reference_.edge_table_column_names = edge_table_column_names;
    }

    bool has_dynamic_label_expression() const {
      return dynamic_label_expression_.has_value();
    }
    bool has_dynamic_properties_expression() const {
      return dynamic_properties_expression_.has_value();
    }
    // REQUIRES: has_dynamic_label_expression() == true
    const std::string& dynamic_label_expression() const {
      return dynamic_label_expression_.value();
    }
    // REQUIRES: has_dynamic_property_expression() == true
    const std::string& dynamic_properties_expression() const {
      return dynamic_properties_expression_.value();
    }
    void set_dynamic_label_expression(std::string_view expression) {
      dynamic_label_expression_ = std::string(expression);
    }
    void set_dynamic_properties_expression(std::string_view expression) {
      dynamic_properties_expression_ = std::string(expression);
    }

    std::string DebugString() const;

   private:
    // Name of the element table.
    std::string name_;
    // Alias of the element table.
    std::string alias_;
    // Kind of the element table: NODE or EDGE.
    GraphElementKind element_kind_;
    // Key clause columns of the element table.
    std::vector<std::string> key_clause_columns_;
    // Label names linked to this element table.
    std::vector<std::string> label_names_;
    // Property definitions in this element table.
    std::vector<PropertyDefinition> property_definitions_;
    // Dynamic label for an element table.
    std::optional<std::string> dynamic_label_expression_;
    // Dynamic property for an element table.
    std::optional<std::string> dynamic_properties_expression_;
    // Source node reference for an edge table.
    GraphNodeReference source_node_reference_;
    // Target node reference for an edge table.
    GraphNodeReference target_node_reference_;
  };

  const std::string& Name() const { return name_; }
  const std::string& DdlBody() const { return ddl_body_; }
  const std::vector<PropertyDeclaration>& PropertyDeclarations() const {
    return property_declarations_;
  }
  const std::vector<Label>& Labels() const { return labels_; }
  const std::vector<GraphElementTable>& NodeTables() const {
    return node_tables_;
  }
  const std::vector<GraphElementTable>& EdgeTables() const {
    return edge_tables_;
  }

  // const PropertyGraph::Label* FindLabelByName(absl::string_view name) const;
  // const PropertyGraph::PropertyDeclaration* FindPropertyDeclarationByName(
  //     absl::string_view name) const;
  absl::Status FindLabelByName(absl::string_view name,
                               const PropertyGraph::Label*& label) const;
  absl::Status FindPropertyDeclarationByName(
      absl::string_view name,
      const PropertyGraph::PropertyDeclaration*& property_declaration) const;

  // SchemaNode interface implementation.
  // ------------------------------------
  std::optional<SchemaNameInfo> GetSchemaNameInfo() const override {
    return SchemaNameInfo{
        .name = name_, .kind = "PropertyGraph", .global = true};
  }
  absl::Status Validate(SchemaValidationContext* context) const override;
  absl::Status ValidateUpdate(const SchemaNode* orig,
                              SchemaValidationContext* context) const override;
  std::string DebugString() const override;
  catalog::PropertyGraphProto ToProto() const;

  class Builder;
  class Editor;

 private:
  friend class PropertyGraphValidator;
  using ValidationFn = std::function<absl::Status(const PropertyGraph*,
                                                  SchemaValidationContext*)>;
  using UpdateValidationFn = std::function<absl::Status(
      const PropertyGraph*, const PropertyGraph*, SchemaValidationContext*)>;

  // Constructors are private and only friend classes are able to build.
  PropertyGraph(const ValidationFn& validate,
                const UpdateValidationFn& validate_update)
      : validate_(validate), validate_update_(validate_update) {}
  PropertyGraph(const PropertyGraph&) = default;
  std::unique_ptr<SchemaNode> ShallowClone() const override {
    return absl::WrapUnique(new PropertyGraph(*this));
  }
  absl::Status DeepClone(SchemaGraphEditor* editor,
                         const SchemaNode* orig) override;

  // Validation delegates.
  const ValidationFn validate_;
  const UpdateValidationFn validate_update_;

  // The name of the property graph.
  std::string name_;
  // The input DDL string for this property graph.
  std::string ddl_body_;
  // The set of property declarations in this property graph.
  std::vector<PropertyDeclaration> property_declarations_;
  // The set of labels in this property graph.
  std::vector<Label> labels_;
  // The list of node tables in this property graph.
  std::vector<GraphElementTable> node_tables_;
  // The list of edge tables in this property graph.
  std::vector<GraphElementTable> edge_tables_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_PROPERTY_GRAPH_H_
