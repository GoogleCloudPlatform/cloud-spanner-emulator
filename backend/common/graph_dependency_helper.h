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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_COMMON_GRAPH_DEPENDENCY_HELPER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_COMMON_GRAPH_DEPENDENCY_HELPER_H_

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "backend/common/case.h"
#include "common/errors.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Helper class for working with multi-component DAGs.
//
// Functions supported include cycle detection and producing a topological
// sort of graph nodes.
//
// Example usage, assume you have the following graph:
// A --> B --> C
//       ^     |
//       ------
// GraphDependencyHelper<absl::string_view, IdentityFunction>
//   gdh("generated_column");
//
// ZETASQL_RETURN_IF_ERROR(gdh.AddNode("A"));
// ZETASQL_RETURN_IF_ERROR(gdh.AddNode("B"));
// ZETASQL_RETURN_IF_ERROR(gdh.AddNode("C"));
// ZETASQL_RETURN_IF_ERROR(gdh.AddEdge("A", "B"));
// ZETASQL_RETURN_IF_ERROR(gdh.AddEdge("B", "C"));
// ZETASQL_RETURN_IF_ERROR(gdh.AddEdge("C", "B"));
//
// ZETASQL_RETURN_IF_ERROR(gdh.DetectCycle()); // <-- Will fail because there is a cycle
//
// Template Parameters:
// * TData: Type of object stored in the graph. It must have a public copy
// constructor.
// * TGetId: A function that retrieves a absl::string_view from the TData
// object. The absl::string_view should live as long as the TData object.
template <class TData, absl::string_view (*TGetId)(const TData&)>
class GraphDependencyHelper {
 public:
  // Constructor.
  // - 'object_type': Used for error messages.
  explicit GraphDependencyHelper(absl::string_view object_type)
      : object_type_(object_type) {}

  // Class may not be copied or moved.
  GraphDependencyHelper(const GraphDependencyHelper&) = delete;
  GraphDependencyHelper& operator=(const GraphDependencyHelper&) = delete;

  // Adds the node 'node' into the graph.
  absl::Status AddNodeIfNotExists(const TData& data);

  // Adds a directed edge in the graph.
  // Returns an error if either node was not added to the graph yet.
  absl::Status AddEdgeIfNotExists(absl::string_view from, absl::string_view to);

  // Returns an error if the graph has cycle.
  absl::Status DetectCycle();

  // Returns a topological sort of the nodes. Returns an error if the graph has
  // a cycle. The topological order is stable and determined by node and edge
  // insertion order.
  absl::Status TopologicalOrder(std::vector<TData>* data_values);

 private:
  // Wrapper for storing the TData.
  struct NodeInfo {
    TData value;
    // Edges from *this to the nodes in the set.
    std::vector<NodeInfo*> edges;
    absl::flat_hash_set<NodeInfo*> edges_set;

    explicit NodeInfo(TData value) : value(value) {}
  };

  // Struct used for keeping track of the state during the visit.
  struct VisitingState {
    // Nodes that were visited, this set keeps growing during the
    // VisitNode() calls and a node is added pre traversal.
    absl::flat_hash_set<NodeInfo*> pre_traversal;
    // The current nodes that are being visited, it keeps growing and
    // shrinking following the call stack of nodes being visited.
    std::vector<NodeInfo*> visiting;
    // Topological order. Populated post traversal.
    std::vector<NodeInfo*> post_traversal;
  };

  // Object_type is used for building the error string.
  std::string object_type_;
  // The nodes of the graph in insertion order. This is also the ownership
  // of Node instances.
  std::vector<std::unique_ptr<NodeInfo>> nodes_;
  // Map from names to NodeInfo*.
  CaseInsensitiveStringMap<NodeInfo*> nodes_by_name_;

  // Visits 'node' and properly updates the 'state', continuing the search
  // if there are still nodes that are not in the 'state->pre_traversal' set.
  absl::Status VisitNode(VisitingState* state, NodeInfo* node);

  // Visits all nodes in the graph by calling VisitNode if the node is not
  // already visited (in the visited aka pre_traversal set).
  absl::Status VisitNodes(VisitingState* state);
};

// Implementation details

template <class TData, absl::string_view (*TGetId)(const TData&)>
absl::Status GraphDependencyHelper<TData, TGetId>::AddNodeIfNotExists(
    const TData& data) {
  nodes_.emplace_back(absl::make_unique<NodeInfo>(data));
  absl::string_view id = TGetId(nodes_.back()->value);
  nodes_by_name_.insert({std::string(id), nodes_.back().get()});
  return absl::OkStatus();
}

template <class TData, absl::string_view (*TGetId)(const TData&)>
absl::Status GraphDependencyHelper<TData, TGetId>::AddEdgeIfNotExists(
    absl::string_view from, absl::string_view to) {
  auto it = nodes_by_name_.find(std::string(from));
  ZETASQL_RET_CHECK(it != nodes_by_name_.end()) << "from: " << from;
  NodeInfo* node_from = it->second;

  it = nodes_by_name_.find(std::string(to));
  ZETASQL_RET_CHECK(it != nodes_by_name_.end()) << "to: " << to;
  NodeInfo* node_to = it->second;

  if (node_from->edges_set.insert(node_to).second) {
    node_from->edges.push_back(node_to);
  }
  return absl::OkStatus();
}

template <class TData, absl::string_view (*TGetId)(const TData&)>
absl::Status GraphDependencyHelper<TData, TGetId>::DetectCycle() {
  VisitingState state;
  return VisitNodes(&state);
}

template <class TData, absl::string_view (*TGetId)(const TData&)>
absl::Status GraphDependencyHelper<TData, TGetId>::TopologicalOrder(
    std::vector<TData>* data_values) {
  VisitingState state;
  ZETASQL_RETURN_IF_ERROR(VisitNodes(&state));
  data_values->reserve(state.post_traversal.size());
  for (NodeInfo* node_info : state.post_traversal) {
    data_values->push_back(node_info->value);
  }
  return absl::OkStatus();
}

template <class TData, absl::string_view (*TGetId)(const TData&)>
absl::Status GraphDependencyHelper<TData, TGetId>::VisitNode(
    VisitingState* state, NodeInfo* node) {
  state->visiting.push_back(node);
  ZETASQL_RET_CHECK(state->pre_traversal.insert(node).second);
  for (NodeInfo* node_adj : node->edges) {
    if (!state->pre_traversal.contains(node_adj)) {
      ZETASQL_RETURN_IF_ERROR(VisitNode(state, node_adj));
    } else {
      for (NodeInfo* visiting_node : state->visiting) {
        if (visiting_node == node_adj) {
          return error::CycleDetected(
              object_type_,
              absl::StrJoin(state->visiting.begin(), state->visiting.end(), ",",
                            [](std::string* out, const NodeInfo* node_info) {
                              absl::StrAppend(out, TGetId(node_info->value));
                            }));
        }
      }
    }
  }
  state->post_traversal.push_back(node);
  state->visiting.pop_back();
  return absl::OkStatus();
}

template <class TData, absl::string_view (*TGetId)(const TData&)>
absl::Status GraphDependencyHelper<TData, TGetId>::VisitNodes(
    VisitingState* state) {
  for (auto& node : nodes_) {
    if (!state->pre_traversal.contains(node.get())) {
      ZETASQL_RETURN_IF_ERROR(VisitNode(state, node.get()));
    }
  }
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_COMMON_GRAPH_DEPENDENCY_HELPER_H_
