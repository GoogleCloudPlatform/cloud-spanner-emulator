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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_GRAPH_SCHEMA_OBJECTS_POOL_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_GRAPH_SCHEMA_OBJECTS_POOL_H_

#include <algorithm>
#include <memory>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/strings/str_cat.h"
#include "backend/schema/graph/schema_node.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// A class for managing the lifetime of all the objects present in a
// SchemaGraph.
class SchemaObjectsPool {
 public:
  SchemaObjectsPool() {}

  // Takes ownership of a 'node'.
  void Add(std::unique_ptr<const SchemaNode> node) {
    schema_node_pool_.insert(std::move(node));
  }

  // Removes the given 'node' from the pool. Returns true if the node
  // existed in the pool and was deleted.
  bool Remove(const SchemaNode* node) {
    return schema_node_pool_.erase(node) > 0;
  }

  // Removes deleted nodes from the pool. Returns the number of removed nodes.
  int Trim() {
    int trim_count = 0;
    for (auto it = schema_node_pool_.begin(); it != schema_node_pool_.end();) {
      if ((*it)->is_deleted()) {
        ++trim_count;
        schema_node_pool_.erase(it++);
      } else {
        ++it;
      }
    }
    return trim_count;
  }

  int size() const { return schema_node_pool_.size(); }

  std::string DebugString() {
    std::string out;
    for (const auto& node : schema_node_pool_) {
      absl::StrAppend(&out, "\n", node->DebugString());
    }
    return out;
  }

 private:
  absl::flat_hash_set<std::unique_ptr<const SchemaNode>> schema_node_pool_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_GRAPH_SCHEMA_OBJECTS_POOL_H_
