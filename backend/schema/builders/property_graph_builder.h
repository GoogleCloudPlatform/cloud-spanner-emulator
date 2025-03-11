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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_PROPERTY_GRAPH_BUILDER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_PROPERTY_GRAPH_BUILDER_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/memory/memory.h"
#include "backend/schema/catalog/property_graph.h"
#include "backend/schema/validators/property_graph_validator.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

class PropertyGraph::Builder {
 public:
  Builder()
      : instance_(absl::WrapUnique(
            new PropertyGraph(PropertyGraphValidator::Validate,
                              PropertyGraphValidator::ValidateUpdate))) {}

  const PropertyGraph* get() const { return instance_.get(); }

  Builder& set_ddl_body(const std::string& ddl_body) {
    instance_->ddl_body_ = ddl_body;
    return *this;
  }
  Builder& set_name(const std::string& name) {
    instance_->name_ = name;
    return *this;
  }
  Builder& add_property_declaration(
      const PropertyDeclaration& property_declaration) {
    instance_->property_declarations_.push_back(property_declaration);
    return *this;
  }
  Builder& add_label(const Label& label) {
    instance_->labels_.push_back(label);
    return *this;
  }
  Builder& add_node_table(const GraphElementTable& node_table) {
    instance_->node_tables_.push_back(node_table);
    return *this;
  }
  Builder& add_edge_table(const GraphElementTable& edge_table) {
    instance_->edge_tables_.push_back(edge_table);
    return *this;
  }

  std::unique_ptr<const PropertyGraph> build() { return std::move(instance_); }

 private:
  std::unique_ptr<PropertyGraph> instance_;
};

class PropertyGraph::Editor {
 public:
  explicit Editor(PropertyGraph* instance) : instance_(instance) {}
  const PropertyGraph* get() const { return instance_; }

  Editor& set_ddl_body(const std::string& ddl_body) {
    instance_->ddl_body_ = ddl_body;
    return *this;
  }
  Editor& set_name(const std::string& name) {
    instance_->name_ = name;
    return *this;
  }
  Editor& add_property_declaration(
      const PropertyDeclaration& property_declaration) {
    instance_->property_declarations_.push_back(property_declaration);
    return *this;
  }
  Editor& add_label(const Label& label) {
    instance_->labels_.push_back(label);
    return *this;
  }
  Editor& add_node_table(const GraphElementTable& node_table) {
    instance_->node_tables_.push_back(node_table);
    return *this;
  }
  Editor& add_edge_table(const GraphElementTable& edge_table) {
    instance_->edge_tables_.push_back(edge_table);
    return *this;
  }
  Editor& copy_from(const PropertyGraph* property_graph) {
    instance_->ddl_body_ = property_graph->ddl_body_;
    instance_->name_ = property_graph->name_;
    instance_->property_declarations_.clear();
    for (const auto& property_declaration :
         property_graph->property_declarations_) {
      instance_->property_declarations_.push_back(property_declaration);
    }
    instance_->labels_.clear();
    for (const auto& label : property_graph->labels_) {
      instance_->labels_.push_back(label);
    }
    instance_->node_tables_.clear();
    for (const auto& node_table : property_graph->node_tables_) {
      instance_->node_tables_.push_back(node_table);
    }
    instance_->edge_tables_.clear();
    for (const auto& edge_table : property_graph->edge_tables_) {
      instance_->edge_tables_.push_back(edge_table);
    }
    return *this;
  }

 private:
  // Not owned.
  PropertyGraph* instance_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_PROPERTY_GRAPH_BUILDER_H_
