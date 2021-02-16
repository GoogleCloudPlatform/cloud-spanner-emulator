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

#include "backend/schema/graph/schema_graph_editor.h"

#include <memory>

#include "zetasql/base/logging.h"
#include "absl/container/flat_hash_map.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "absl/strings/str_join.h"
#include "backend/schema/graph/schema_node.h"
#include "zetasql/base/ret_check.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

SchemaNode* SchemaGraphEditor::MakeNewClone(const SchemaNode* node) {
  std::unique_ptr<SchemaNode> clone = node->ShallowClone();
  SchemaNode* mutable_clone = clone.get();
  cloned_pool_->Add(std::move(clone));
  clone_map_[node] = mutable_clone;
  return mutable_clone;
}

absl::Status SchemaGraphEditor::InitCloneMap() {
  // First, make a clone of the graph.
  ZETASQL_VLOG(2) << "First cloning pass";
  for (const auto* schema_node : original_graph_->GetSchemaNodes()) {
    ZETASQL_ASSIGN_OR_RETURN(const auto* cloned_node, Clone(schema_node));
    new_nodes_.push_back(cloned_node);
  }
  ZETASQL_RET_CHECK_EQ(clone_map_.size(), num_original_nodes());
  return absl::OkStatus();
}

absl::Status SchemaGraphEditor::FixupInternal(const SchemaNode* original,
                                              SchemaNode* mutable_clone) {
  ZETASQL_VLOG(4) << std::string(depth_, ' ') << "Fixing "
          << NodeKindString(mutable_clone) << " node :" << mutable_clone;
  ++depth_;
  ZETASQL_RETURN_IF_ERROR(mutable_clone->DeepClone(this, original));
  --depth_;
  ZETASQL_VLOG(4) << std::string(depth_, ' ')
          << "Finished fixing node: " << mutable_clone->DebugString();
  return absl::OkStatus();
}

// The cloning process is guaranteed to terminate even in case of cycles in the
// graph because:
// -  Besides the 'kOriginal' nodes, Clone() is called on only 3 other kinds of
//    nodes : kEdited, kAdded and kCloned.
// -  Each of these adds itself to the clone map before calling FixupInternal
//    which in turn calls DeepClone which may call Clone() recursively on other
//    nodes.
// -  At some point every node has had Clone() called on itself and therefore
//    added itself to the clone map. After that point, no recursive calls to
//    Clone() are made, guaranteeing termination of the cloning procedure.
zetasql_base::StatusOr<const SchemaNode*> SchemaGraphEditor::Clone(
    const SchemaNode* node) {
  ZETASQL_RET_CHECK_NE(node, nullptr);

  // During the delete_fixup_ phase, we don't do any recursive calls.
  if (delete_fixup_) {
    return node;
  }

  const SchemaNode* ret = nullptr;
  NodeKind kind = GetNodeKind(node);
  const SchemaNode* clone = FindClone(node);
  if (clone != nullptr) {
    ZETASQL_VLOG(5) << std::string(depth_, ' ') << "Found already visited "
            << NodeKindString(clone) << " node :" << clone->DebugString();
    ret = clone;
  } else if (kind == kAdded || kind == kEdited || kind == kCloned) {
    SchemaNode* mutable_node = const_cast<SchemaNode*>(node);
    // When called with non-original nodes, clone_map_ acts as a 'visited' set.
    clone_map_[node] = node;
    ZETASQL_RETURN_IF_ERROR(FixupInternal(node, mutable_node));
    ret = node;
  } else {
    ZETASQL_RET_CHECK_EQ(kind, kOriginal);
    ZETASQL_RET_CHECK(!node->is_deleted());
    ZETASQL_VLOG(3) << std::string(depth_, ' ') << "Cloning " << NodeKindString(node)
            << " node: " << node->DebugString();

    SchemaNode* mutable_clone = MakeNewClone(node);
    ZETASQL_RETURN_IF_ERROR(FixupInternal(node, mutable_clone));
    ZETASQL_VLOG(3) << std::string(depth_, ' ')
            << "Finished cloning node: " << node->DebugString();
    ret = mutable_clone;
  }
  return ret;
}

absl::Status SchemaGraphEditor::DeleteNode(const SchemaNode* node) {
  ZETASQL_RET_CHECK(!HasModifications())
      << "Graph already contains modifications. "
      << "Graph must be canonicalized before deleting another node.";
  ZETASQL_RET_CHECK(IsOriginalNode(node));
  deleted_node_ = node;
  return absl::OkStatus();
}

absl::Status SchemaGraphEditor::AddNode(
    std::unique_ptr<const SchemaNode> node) {
  ZETASQL_RET_CHECK_EQ(deleted_node_, nullptr)
      << "Graph already has a deleted node. It must be canonicalized before "
      << "making further changes.";
  added_nodes_.emplace_back(std::move(node));
  return absl::OkStatus();
}

bool SchemaGraphEditor::IsOriginalNode(const SchemaNode* node) const {
  for (const auto* schema_node : original_graph_->GetSchemaNodes()) {
    if (schema_node == node) {
      return true;
    }
  }
  return false;
}

zetasql_base::StatusOr<std::unique_ptr<SchemaGraph>>
SchemaGraphEditor::CanonicalizeGraph() {
  std::unique_ptr<SchemaGraph> cloned_graph = nullptr;
  // Set the edited nodes in the context so that nodes can check
  // if they were edited/cloned inside ValidateUpdate()/Validate().
  context_->set_edited_nodes(&edited_clones_);
  if (deleted_node_) {
    ZETASQL_VLOG(2) << "Canonicalizing deletes";
    ZETASQL_RETURN_IF_ERROR(CanonicalizeDeletion());
  } else {
    ZETASQL_VLOG(2) << "Canonicalizing edits and additions";
    ZETASQL_RETURN_IF_ERROR(CanonicalizeEdits());
  }
  ZETASQL_RETURN_IF_ERROR(CheckInvariants());
  ZETASQL_RETURN_IF_ERROR(CheckValid());
  cloned_graph = absl::make_unique<SchemaGraph>(std::move(new_nodes_),
                                                std::move(cloned_pool_));
  return cloned_graph;
}

absl::Status SchemaGraphEditor::CheckValid() const {
  if (!deleted_node_) {
    // ValidateUpdate was already called on deleted nodes.
    auto orig_nodes = original_graph_->GetSchemaNodes();
    for (int i = 0; i < num_original_nodes(); ++i) {
      ZETASQL_RETURN_IF_ERROR(new_nodes_[i]->ValidateUpdate(orig_nodes[i], context_));
    }
  }

  // A final pass on the canonicalized set of nodes to perform per-node
  // validation.
  for (const auto* node : new_nodes_) {
    ZETASQL_RETURN_IF_ERROR(node->Validate(context_));
  }
  return absl::OkStatus();
}

absl::Status SchemaGraphEditor::CanonicalizeEdits() {
  if (clone_map_.empty()) {
    ZETASQL_RETURN_IF_ERROR(InitCloneMap());
  }

  // Run a fixup/cloning pass so that changes from edit nodes in the cloned
  // graph are propagated to their neighbors.
  ZETASQL_VLOG(2) << "Fixing clones";
  for (const auto* clone : new_nodes_) {
    ZETASQL_RETURN_IF_ERROR(Fixup(clone));
  }

  // No new clones were added.
  ZETASQL_RET_CHECK_EQ(new_nodes_.size(), num_original_nodes());
  ZETASQL_RET_CHECK_EQ(cloned_pool_->size(), num_original_nodes());

  ZETASQL_VLOG(2) << "Fixing added nodes";
  for (auto& added_node : added_nodes_) {
    ZETASQL_RETURN_IF_ERROR(Fixup(added_node.get()));
    new_nodes_.push_back(added_node.get());
    cloned_pool_->Add(std::move(added_node));
  }
  ZETASQL_RET_CHECK_EQ(cloned_pool_->size(),
               num_original_nodes() + added_nodes_.size());
  return absl::OkStatus();
}

absl::Status SchemaGraphEditor::Fixup(const SchemaNode* node) {
  ZETASQL_ASSIGN_OR_RETURN(const auto* cloned_node, Clone(node));
  ZETASQL_RET_CHECK_NE(cloned_node, nullptr);
  return absl::OkStatus();
}

absl::Status SchemaGraphEditor::CanonicalizeDeletion() {
  if (clone_map_.empty()) {
    ZETASQL_RETURN_IF_ERROR(InitCloneMap());
  }

  // Mark the clone of the node as deleted.
  const SchemaNode* deleted_clone = FindClone(deleted_node_);
  ZETASQL_RET_CHECK_NE(deleted_clone, nullptr);
  const_cast<SchemaNode*>(deleted_clone)->MarkDeleted();

  // To propagate the deletion information across the graph so that every node
  // can take action on deletion of its neighbor, we run Fixup as many times as
  // the number of nodes (the length of the longest non-cyclical path in the
  // graph or until the number of deletions has  converged).
  delete_fixup_ = true;
  int deletions = 1;
  for (int i = 0; i < num_original_nodes(); ++i) {
    int new_deletions = 0;
    for (const auto* node : original_graph_->GetSchemaNodes()) {
      const SchemaNode* clone = FindClone(node);
      ZETASQL_RET_CHECK_NE(clone, nullptr);
      ZETASQL_RETURN_IF_ERROR(FixupInternal(clone, const_cast<SchemaNode*>(clone)));
      if (clone->is_deleted()) {
        ++new_deletions;
      }
    }
    ZETASQL_RET_CHECK_LE(deletions, new_deletions);

    // No need to run more fixup passes if the number of updates have converged.
    if (new_deletions == deletions) {
      break;
    } else {
      deletions = new_deletions;
    }
    ZETASQL_VLOG(5) << "Fixup pass " << i + 1 << " deletions: " << new_deletions;
  }
  delete_fixup_ = false;

  for (const auto* orig_node : original_graph_->GetSchemaNodes()) {
    auto clone = FindClone(orig_node);
    ZETASQL_RET_CHECK_NE(clone, nullptr);
    // Validate the update on cloned/edited nodes.
    ZETASQL_RETURN_IF_ERROR(clone->ValidateUpdate(orig_node, context_));
  }

  // Erase deleted nodes from the new graph.
  new_nodes_.erase(
      std::remove_if(new_nodes_.begin(), new_nodes_.end(),
                     [](const SchemaNode* node) { return node->is_deleted(); }),
      new_nodes_.end());
  trimmed_ = cloned_pool_->Trim();
  return absl::OkStatus();
}

absl::Status SchemaGraphEditor::CheckInvariants() const {
  ZETASQL_RET_CHECK_EQ(cloned_pool_->size(), new_nodes_.size())
      << "\nNodes:\n"
      << cloned_pool_->DebugString();

  ZETASQL_RET_CHECK_EQ(cloned_pool_->size(),
               num_original_nodes() - trimmed_ + added_nodes_.size())
      << "Internal error while cloning schema graph "
      << "Original: " << num_original_nodes() << "\n"
      << "Cloned: " << cloned_pool_->size() << "\n"
      << "Added: " << added_nodes_.size() << "\n"
      << "Trimmed: " << trimmed_ << "\n"
      << "Nodes:\n"
      << cloned_pool_->DebugString();

  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
