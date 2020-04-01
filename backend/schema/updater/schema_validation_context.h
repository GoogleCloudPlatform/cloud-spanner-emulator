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

#include "absl/time/time.h"
#include "backend/storage/storage.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

class Schema;

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
      std::function<zetasql_base::Status(const SchemaValidationContext*)>;

  explicit SchemaValidationContext(Storage* storage,
                                   absl::Time pending_commit_timestamp)
      : storage_(storage),
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
  zetasql_base::Status RunSchemaChangeActions() const {
    for (auto& action : actions_) {
      ZETASQL_RETURN_IF_ERROR(action(this));
    }
    return zetasql_base::OkStatus();
  }

  // Returns the number of pending schema change actions.
  int num_actions() const { return actions_.size(); }

 private:
  // Used to read data from the database for verifiers. Not owned.
  Storage* storage_;

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
