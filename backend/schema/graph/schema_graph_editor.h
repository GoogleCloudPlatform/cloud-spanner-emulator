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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_GRAPH_SCHEMA_GRAPH_EDITOR_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_GRAPH_SCHEMA_GRAPH_EDITOR_H_

#include <algorithm>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "backend/schema/graph/schema_graph.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/graph/schema_objects_pool.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "zetasql/base/ret_check.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// SchemaGraphEditor clones a SchemaGraph of immutable SchemaNode(s) and applies
// changes to it.
//
// Multiple nodes may be added to the schema graph or modified but only a
// single node may be deleted at a time. Cascading deletes should be handled
// by calling is_deleted()/MarkDeleted() on referenced/referencing nodes
// respectively inside DeepClone().
//
// Updates/deletions are made on clones of the nodes in the original graph and
// the CanonicalizeGraph() method must be called to obtain a new graph with the
// additions/updates/deletions applied. An instance of SchemaGraphEditor should
// not be re-used after a call to CanonicalizeGraph().
class SchemaGraphEditor {
 public:
  SchemaGraphEditor(const SchemaGraph* original_graph,
                    SchemaValidationContext* context)
      : original_graph_(original_graph),
        context_(context),
        cloned_pool_(absl::make_unique<SchemaObjectsPool>()) {
    context_->set_added_nodes(&added_nodes_);
  }

  template <typename T>
  using EditCallback = std::function<absl::Status(typename T::Editor*)>;

  // Creates a modifiable clone of 'node'. If the node is already cloned
  // for modifcation, re-uses the existing clone. The clone is modified through
  // a callback which is passed a T::Editor object. `T` should be a concrete
  // type implementing the SchemaNode interface and should provide for
  // construction of a T::Editor(T*) object that will be used to access the
  // internal state of the clone(of type T) for modifications. During editing,
  // the callback can signal validation or other errors by returning a non-OK
  // status. When the graph is canonicalized, the modified clone replaces all
  // references to the original node.
  template <typename T>
  absl::Status EditNode(const SchemaNode* node,
                        const EditCallback<T>& edit_cb) {
    ZETASQL_RET_CHECK_EQ(deleted_node_, nullptr)
        << "Graph has a deleted node. It must be canonicalized before "
        << "making further changes.";

    // Get an editable node.
    T* editable = const_cast<SchemaNode*>(node)->As<T>();
    ZETASQL_RET_CHECK_NE(editable, nullptr);

    // Clone the node if it already exists.
    if (IsOriginalNode(node)) {
      // Create a clone of the schema first.
      if (clone_map_.empty()) {
        ZETASQL_RETURN_IF_ERROR(InitCloneMap());
      }

      // Edit the clone.
      const auto* clone = FindClone(node);
      ZETASQL_RET_CHECK_NE(clone, nullptr);
      editable = const_cast<SchemaNode*>(clone)->As<T>();
      ZETASQL_RET_CHECK_NE(editable, nullptr);

      edited_clones_.insert(clone);
    }

    // Edit the node.
    typename T::Editor editor(editable);
    return edit_cb(&editor);
  }

  // Marks 'node' as deleted from the graph. The delete may result in
  // cascading deletes depending on how the different SchemaNode(s)
  // handle deletes of children in their respective DeepClone()
  // implementations.
  absl::Status DeleteNode(const SchemaNode* node);

  // Adds 'node' to the graph.
  absl::Status AddNode(std::unique_ptr<const SchemaNode> node);

  // Makes a new clone of the schema graph, fixing up node-relationships
  // after edits and calling validation on the node graph being edited.
  zetasql_base::StatusOr<std::unique_ptr<SchemaGraph>> CanonicalizeGraph();

  // Deep-clones starting from the SchemaNode 'node' in schema graph. Any
  // nodes reachable from 'node' in the schema graph will also be cloned and
  // owned by the 'cloned_pool_'. As a result this method doesn't guarantee to
  // clone the entire schema graph, only the sub-graph reachable from 'node' and
  // ownership of the cloned sub-graph cannot be released from this class.
  // Callers should not directly call this method.
  zetasql_base::StatusOr<const SchemaNode*> Clone(const SchemaNode* node);

  // Clones an iterable container of nodes in-place. Erases deleted nodes.
  template <typename T, typename C>
  absl::Status CloneContainer(C* nodes) {
    for (auto it = nodes->begin(); it != nodes->end();) {
      ZETASQL_ASSIGN_OR_RETURN(const auto* schema_node, Clone(*it));
      if (schema_node->is_deleted()) {
        it = nodes->erase(it);
      } else {
        *it = schema_node->template As<T>();
        ++it;
      }
    }
    return absl::OkStatus();
  }

  // Clones a vector of nodes in-place. Erases deleted nodes.
  template <typename T>
  absl::Status CloneVector(std::vector<const T*>* nodes) {
    return CloneContainer<T>(nodes);
  }

  // Returns true if the graph has any modifications.
  bool HasModifications() const {
    return deleted_node_ != nullptr || !edited_clones_.empty() ||
           !added_nodes_.empty();
  }

  // Returns a pointer to the original schema graph.
  const SchemaGraph* original_graph() const { return original_graph_; }

 private:
  const SchemaNode* FindClone(const SchemaNode* node) const {
    auto it = clone_map_.find(node);
    if (it == clone_map_.end()) {
      return nullptr;
    }
    return it->second;
  }

  enum NodeKind {
    kOriginal = 0,
    kAdded = 1,
    kEdited = 2,
    kDropped = 3,
    kCloned = 4,
  };

  std::string NodeKindString(const SchemaNode* node) const {
    switch (GetNodeKind(node)) {
      case kOriginal:
        return "ORIGINAL";
      case kAdded:
        return "ADDED";
      case kEdited:
        return "EDITED";
      case kDropped:
        return "DROPPED";
      case kCloned:
        return "CLONED";
    }
  }

  NodeKind GetNodeKind(const SchemaNode* node) const {
    if (IsOriginalNode(node)) {
      return kOriginal;
    }
    auto it = std::find_if(
        added_nodes_.begin(), added_nodes_.end(),
        [&node](const std::unique_ptr<const SchemaNode>& node_ptr) {
          return node_ptr.get() == node;
        });
    if (it != added_nodes_.end()) {
      return kAdded;
    }
    if (node == deleted_node_) {
      return kDropped;
    }
    const auto* clone = FindClone(node);
    if (edited_clones_.contains(clone)) {
      return kEdited;
    }
    return kCloned;
  }

  int num_original_nodes() const {
    return original_graph_->GetSchemaNodes().size();
  }

  // Clones the original schema and creates the mapping of
  // original nodes to clones.
  absl::Status InitCloneMap();

  // Returns OK if 'node' is present in the original graph.
  bool IsOriginalNode(const SchemaNode* node) const;

  // Creates and registers a clone for 'node'.
  SchemaNode* MakeNewClone(const SchemaNode* node);

  // Updates the pointers to other SchemaNodes in the graph held by 'node'
  // to point to their canonicalized versions. Does not create any clones.
  absl::Status Fixup(const SchemaNode* node);

  absl::Status FixupInternal(const SchemaNode* original,
                             SchemaNode* mutable_clone);

  // Canonicalizes the graph to process any pending edits/additions.
  absl::Status CanonicalizeEdits();

  // Canonicalizes the graph to process any pending deletes.
  absl::Status CanonicalizeDeletion();

  // Checks certain invariants about number of nodes and clones.
  absl::Status CheckInvariants() const;

  // Calls Validate() and ValidateUpdate() on the new nodes.
  absl::Status CheckValid() const;

  // The current depth of the cloning stack.
  int depth_ = 0;

  // The original graph.
  const SchemaGraph* original_graph_ = nullptr;

  // Validation context passed to Validate() and ValidateUpdate() methods for
  // SchemaNode.
  SchemaValidationContext* context_ = nullptr;

  // Canonicalization/cloning state.
  // -----------------------

  // Number of deleted nodes.
  int trimmed_ = 0;

  // Mapping of original nodes to clones.
  absl::flat_hash_map<const SchemaNode*, const SchemaNode*> clone_map_;

  // If true, the SchemaGraph is being visited in the delete fixup phase.
  bool delete_fixup_ = false;

  // The set of nodes (cloned + newly added) that will constitue
  // the new SchemaGraph.
  std::vector<const SchemaNode*> new_nodes_;

  // The pool to store cloned schema objects in.
  std::unique_ptr<SchemaObjectsPool> cloned_pool_;

  // Editing state.
  // -----------------------

  // The node being deleted.
  const SchemaNode* deleted_node_ = nullptr;

  // The nodes added to the graph.
  std::vector<std::unique_ptr<const SchemaNode>> added_nodes_;

  // Clones that were modified/edited.
  absl::flat_hash_set<const SchemaNode*> edited_clones_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_GRAPH_SCHEMA_GRAPH_EDITOR_H_
