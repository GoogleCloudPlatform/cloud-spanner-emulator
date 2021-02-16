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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_MANAGER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_MANAGER_H_

#include <memory>

#include "absl/container/node_hash_map.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "backend/actions/action.h"
#include "backend/actions/context.h"
#include "backend/actions/ops.h"
#include "backend/query/catalog.h"
#include "backend/query/function_catalog.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// ActionRegistry is a collection of actions for a given schema.
//
// Transactions use this registry for constraint checking the writes to a
// database.
class ActionRegistry {
 public:
  explicit ActionRegistry(const Schema* schema,
                          const FunctionCatalog* function_catalog);

  // Executes the list of validators that apply to the given operation.
  absl::Status ExecuteValidators(const ActionContext* ctx, const WriteOp& op);

  // Executes the list of effectors that apply to the given operation.
  absl::Status ExecuteEffectors(const ActionContext* ctx, const WriteOp& op);

  // Executes the list of modifiers that apply to the given operation.
  absl::Status ExecuteModifiers(const ActionContext* ctx, const WriteOp& op);

  // Executes the list of verifiers that apply to the given operation.
  absl::Status ExecuteVerifiers(const ActionContext* ctx, const WriteOp& op);

 private:
  // Initialize the validators, effectors, modifiers and verifiers for each
  // table in the given schema.
  void BuildActionRegistry();

  // Schema used to define the registry of actions.
  const Schema* schema_;

  // List of validators per table.
  absl::node_hash_map<const Table*, std::vector<std::unique_ptr<Validator>>>
      table_validators_;

  // List of effectors per table.
  absl::node_hash_map<const Table*, std::vector<std::unique_ptr<Effector>>>
      table_effectors_;

  // List of modifiers per table.
  absl::node_hash_map<const Table*, std::vector<std::unique_ptr<Modifier>>>
      table_modifiers_;

  // List of verifiers per table.
  absl::node_hash_map<const Table*, std::vector<std::unique_ptr<Verifier>>>
      table_verifiers_;

  // Used for function resolution in actions.
  Catalog catalog_;
};

// ActionManager manages the registry of actions for each schema in the
// database.
class ActionManager {
 public:
  // Builds the registry of actions for given schema and function_catalog.
  void AddActionsForSchema(const Schema* schema,
                           const FunctionCatalog* function_catalog);

  // Returns the action registry for given schema.
  zetasql_base::StatusOr<ActionRegistry*> GetActionsForSchema(
      const Schema* schema) const;

 private:
  absl::node_hash_map<const Schema*, std::unique_ptr<ActionRegistry>> registry_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_MANAGER_H_
