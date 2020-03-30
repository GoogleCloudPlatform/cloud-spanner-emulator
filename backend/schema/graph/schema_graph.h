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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_GRAPH_SCHEMA_GRAPH_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_GRAPH_SCHEMA_GRAPH_H_

#include <memory>

#include "backend/schema/graph/schema_node.h"
#include "backend/schema/graph/schema_objects_pool.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

class SchemaGraph {
 public:
  SchemaGraph() : pool_(absl::make_unique<SchemaObjectsPool>()) {}

  // Constructor for creating a graph from an externally-maintained list of
  // nodes.
  SchemaGraph(std::vector<const SchemaNode*> schema_nodes,
              std::unique_ptr<SchemaObjectsPool> pool)
      : schema_nodes_(std::move(schema_nodes)), pool_(std::move(pool)) {}

  // Get a list of all the nodes in the graph in the order in which they were
  // added.
  absl::Span<const SchemaNode* const> GetSchemaNodes() const {
    return schema_nodes_;
  }

  // Adds a new node to the graph.
  void Add(std::unique_ptr<const SchemaNode> node_ptr) {
    const SchemaNode* node = node_ptr.get();
    schema_nodes_.push_back(node);
    pool_->Add(std::move(node_ptr));
  }

 private:
  // List representing the order of the nodes in the graph.
  std::vector<const SchemaNode*> schema_nodes_;

  // Pool for managing the lifetime of the nodes in the graph.
  std::unique_ptr<SchemaObjectsPool> pool_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_GRAPH_SCHEMA_GRAPH_H_
