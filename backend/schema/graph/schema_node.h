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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_GRAPH_SCHEMA_NODE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_GRAPH_SCHEMA_NODE_H_

#include <memory>

#include "absl/status/status.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

class SchemaGraphEditor;
class SchemaValidationContext;

// The name of a schema node, plus additional metadata.
struct SchemaNameInfo {
  // The name of the schema object, such as the name of a table.
  std::string name;
  // The type of schema node, such as "Table" for tables.
  std::string kind;
  // Whether or not this is a global schema name. For example, table names are
  // global and must be unique in a schema, whereas column names are not.
  bool global = false;
};

// Base class for all Schema objects. All objects in the schema must sub-class
// from this and implement its interface. This interface is used to clone the
// schema and apply modifications it it. The cloning and modifcation is done
// by the SchemaGraphEditor class which is oblivious to the internal
// implementation details of the various schema objects. For the cloning process
// to work correctly, it relies on the following explicit and implicit interface
// of SchemaNode objects in a SchemaGraph:
//
// -  Each node implements a ShallowClone() method to return a POD copy of it's
//    state.
//
// -  Each node implements a DeepClone() method that is called on the copy
//    obtained from ShallowClone() by the SchemaGraphCloner(). Inside the
//    DeepClone() method, the node should modify pointers to other nodes held in
//    it's internal state(aka 'child nodes'), calling Clone() on each of them to
//    get their latest versions. Clone() must be called on every child node that
//    'this' node holds in it's state. This is to ensure correct propagation of
//    changes in the graph. Validation errors, if any, during DeepClone()ing
//    should be returned after all the child_nodes have been Clone()d.
//
// -  Deletions: When a node is marked for deletion, it will return true from
//    is_deleted(). Deletions may result in cascading deletions. If a node n1
//    depends on node n2 (i.e. it should be deleted if n2 is deleted), then it
//    must hold a pointer to n2 and must call this->MarkDelete() while
//    DeepClone()ing itself in response to a is_deleted() on n2. A node may
//    chose to ignore or check validation errors on deletion inside the
//    ValidateUpdate() methods. For e.g. if a node does not expect itself or
//    some other node it depends on to be deleted, then it should check for
//    this->is_deleted() inside ValidateUpdate() and return an error.
//
class SchemaNode {
 public:
  SchemaNode() {}
  virtual ~SchemaNode() {}

  // For accessing the typed implementations.
  template <typename T>
  const T* As() const {
    return dynamic_cast<const T*>(this);
  }

  // Returns the name of this node if any, plus additional metadata.
  virtual std::optional<SchemaNameInfo> GetSchemaNameInfo() const { return {}; }

  //   Note for implementing validation:
  //   During validation, inconsistencies in state that cannot be arrived at
  //   through a syntactically valid DDL statement should be reported as
  //   check/assert-failures, i.e. internal failures that point to a bug in
  //   DDL processing.
  //
  //   Inconsistencies in state that can result from a semantically
  //   invalid DDL statement should be reported as user-visible, properly
  //   formatted errors.

  // Returns absl::OkStatus() if the node's state is self-conistent.
  virtual absl::Status Validate(SchemaValidationContext* context) const = 0;

  // Validates that the state of the cloned node after deep-cloning is
  // a valid change compared to the state of the original node.
  virtual absl::Status ValidateUpdate(
      const SchemaNode* orig, SchemaValidationContext* context) const = 0;

  // Returns a debug string for uniquely identifying a node in a SchemaGraph.
  virtual std::string DebugString() const = 0;

  // Returns true if this node is marked for deletion.
  bool is_deleted() const { return is_deleted_; }

 protected:
  // Marks a node as deleted.
  void MarkDeleted() { is_deleted_ = true; }

 private:
  friend class SchemaGraphEditor;
  // Methods used to clone the nodes in a SchemaGraph.
  //
  // Should return a shallow (POD) copy of a SchemaNode.
  virtual std::unique_ptr<SchemaNode> ShallowClone() const = 0;

  // Called by SchemaGraphEditor on a clone obtained from ShallowClone
  // to perform a deep copy/cloning, updating any pointers to other
  // SchemeNodes that 'this' might hold to their respective clones.
  //
  // Note: Direct pointer comparisons to check schema edges will not be valid
  // inside DeepClone as a node may still be holding pointers to neighbors that
  // have not been cloned (and vice-versa) when DeepClone is called on it.
  // Instead, implementations should rely on node identifiers that do not
  // change during cloning for doing such comparisons.
  virtual absl::Status DeepClone(SchemaGraphEditor* editor,
                                 const SchemaNode* orig) = 0;

  template <typename T>
  T* As() {
    return dynamic_cast<T*>(this);
  }

  // Indicates if this node is deleted.
  bool is_deleted_ = false;
};

inline std::ostream& operator<<(std::ostream& out, const SchemaNode* node) {
  if (node == nullptr) {
    return out << "NULL";
  }
  return out << node->DebugString();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_GRAPH_SCHEMA_NODE_H_
