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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_SCHEMA_VALIDATION_CONTEXT_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_SCHEMA_VALIDATION_CONTEXT_H_

#include <memory>
#include <vector>

#include "zetasql/public/types/type_factory.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/time/time.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/storage/storage.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

class Schema;
class GlobalSchemaNames;

// A class used to collect and execute verification/backfill actions resulting
// from a schema change. A `SchemaChangeAction` object can be constructed and
// added to this context during processing/validation of schema changes that
// requires verifying that the data in the database conforms to an
// existing/newly added constraint/invariant or backfilling values in the
// database.
// Note: Currently SchemaChangeActions that modify the state of a database
// cannot be rolled back. Consequently, action execution should be atomic, i.e.
// the action should either apply all the effects entailed by the schema change
// or leave the datatabse unchanged.
class SchemaValidationContext {
 public:
  // The actual function doing the verification to which `context_` is passed.
  using SchemaChangeAction =
      std::function<absl::Status(const SchemaValidationContext*)>;

  // Test-only constructor.
  // TODO : Split out into a separate Schema update validation
  // context that is used by SchemaUpdater and can therefore be mocked
  // separately.
  SchemaValidationContext() {}

  SchemaValidationContext(Storage* storage, GlobalSchemaNames* global_names,
                          zetasql::TypeFactory* type_factory,
                          absl::Time pending_commit_timestamp)
      : storage_(storage),
        global_names_(global_names),
        type_factory_(type_factory),
        pending_commit_timestamp_(pending_commit_timestamp) {}

  // TODO : Split out into a separate StatementValidationContext.
  // Interface accessed by validators to enqueue schema
  // change actions.
  // -----------------------------------------------------

  // Adds a SchemaChangeAction to this validation context.
  void AddAction(SchemaChangeAction action_fn) {
    actions_.emplace_back(std::move(action_fn));
  }

  // Interface used by a SchemaChangeAction to access the
  // database
  // --------------------------------------------------

  // Provides SchemaVerifiers running under this validation context
  // access to the database storage to perform reads.
  Storage* storage() const { return storage_; }

  // Access to the current schema's set of global names.
  GlobalSchemaNames* global_names() const { return global_names_; }

  // Access to the current database's type factory.
  zetasql::TypeFactory* type_factory() const { return type_factory_; }

  // Returns the pending commit timestamp.
  absl::Time pending_commit_timestamp() const {
    return pending_commit_timestamp_;
  }

  const Schema* old_schema() const { return old_schema_snapshot_; }

  const Schema* new_schema() const { return new_schema_snapshot_; }

  // Interface accessed by SchemaUpdater to execute queued
  // actions.
  // -----------------------------------------------------

  void SetOldSchemaSnapshot(const Schema* old_schema) {
    old_schema_snapshot_ = old_schema;
  }

  void SetNewSchemaSnapshot(const Schema* new_schema) {
    new_schema_snapshot_ = new_schema;
  }

  // Runs all SchemaVerifiers added to this validation context.
  absl::Status RunSchemaChangeActions() const {
    for (auto& action : actions_) {
      ZETASQL_RETURN_IF_ERROR(action(this));
    }
    return absl::OkStatus();
  }

  // Returns the number of pending schema change actions.
  int num_actions() const { return actions_.size(); }

  // Returns true if 'node' is a node that was modified using a DDL
  // statement/operation as a part of the schema change associated
  // with this SchemaValidationContext.
  bool IsModifiedNode(const SchemaNode* node) const {
    return edited_nodes_->contains(node);
  }

  // Returns the pointer to the first node added with a given name
  // (case-insensitive) using the node's SchemaNameInfo. Returns nullptr if not
  // found. Also returns nullptr if nodes with that name were found but were the
  // wrong type.
  template <typename T>
  const T* FindAddedNode(absl::string_view name) const {
    for (const auto& node : *added_nodes_) {
      auto info = node->GetSchemaNameInfo().value_or(SchemaNameInfo{});
      if (info.name == name) {
        const T* candidate = node.get()->As<T>();
        if (info.global || candidate != nullptr) {
          return candidate;
        }
      }
    }
    return nullptr;
  }

 private:
  friend class SchemaGraphEditor;

  // Saves a pointer to the 'edited_nodes' maintained by the 'SchemaGraphEditor'
  // to allow nodes to check through the SchemaValidationContext if they were
  // modified by user action or just cloned during the processing of a schema
  // update.
  void set_edited_nodes(
      const absl::flat_hash_set<const SchemaNode*>* edited_nodes) {
    edited_nodes_ = edited_nodes;
  }

  const absl::flat_hash_set<const SchemaNode*>* edited_nodes_ = nullptr;

  // Saves a pointer to the 'added_nodes' maintained by the 'SchemaGraphEditor'
  // to allow nodes to check through the SchemaValidationContext if they were
  // added by user action.
  void set_added_nodes(
      const std::vector<std::unique_ptr<const SchemaNode>>* added_nodes) {
    added_nodes_ = added_nodes;
  }

  const std::vector<std::unique_ptr<const SchemaNode>>* added_nodes_ = nullptr;

  // Used to read data from the database for verifiers. Not owned.
  Storage* storage_;

  // Global names for schema objects.
  GlobalSchemaNames* global_names_;

  // Type factory used for all ZetaSQL operations on the database.
  zetasql::TypeFactory* type_factory_;

  // Planned commit time for the schema change.
  absl::Time pending_commit_timestamp_;

  // The list of pending schema change actions (verifications/backfills) to run.
  std::vector<SchemaChangeAction> actions_;

  // The old schema.
  const Schema* old_schema_snapshot_;

  // The new schema.
  const Schema* new_schema_snapshot_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_SCHEMA_VALIDATION_CONTEXT_H_
