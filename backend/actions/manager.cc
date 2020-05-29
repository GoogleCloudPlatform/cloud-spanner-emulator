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

#include "backend/actions/manager.h"

#include <memory>

#include "backend/actions/column_value.h"
#include "backend/actions/existence.h"
#include "backend/actions/index.h"
#include "backend/actions/interleave.h"
#include "backend/actions/unique_index.h"
#include "common/errors.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

absl::Status ActionRegistry::ExecuteValidators(const ActionContext* ctx,
                                               const WriteOp& op) {
  for (auto& validator : table_validators_[TableOf(op)]) {
    ZETASQL_RETURN_IF_ERROR(validator->Validate(ctx, op));
  }
  return absl::OkStatus();
}

absl::Status ActionRegistry::ExecuteEffectors(const ActionContext* ctx,
                                              const WriteOp& op) {
  for (auto& effector : table_effectors_[TableOf(op)]) {
    ZETASQL_RETURN_IF_ERROR(effector->Effect(ctx, op));
  }
  return absl::OkStatus();
}

absl::Status ActionRegistry::ExecuteModifiers(const ActionContext* ctx,
                                              const WriteOp& op) {
  for (auto& modifier : table_modifiers_[TableOf(op)]) {
    ZETASQL_RETURN_IF_ERROR(modifier->Modify(ctx, op));
  }
  return absl::OkStatus();
}

absl::Status ActionRegistry::ExecuteVerifiers(const ActionContext* ctx,
                                              const WriteOp& op) {
  for (auto& verifier : table_verifiers_[TableOf(op)]) {
    ZETASQL_RETURN_IF_ERROR(verifier->Verify(ctx, op));
  }
  return absl::OkStatus();
}

ActionRegistry::ActionRegistry(const Schema* schema) : schema_(schema) {
  BuildActionRegistry();
}

void ActionRegistry::BuildActionRegistry() {
  for (const Table* table : schema_->tables()) {
    // Column value checks for all tables.
    table_validators_[table].emplace_back(
        absl::make_unique<ColumnValueValidator>());

    // Row existence checks for all tables.
    table_validators_[table].emplace_back(
        absl::make_unique<RowExistenceValidator>());

    // Interleave actions for child tables.
    for (const Table* child : table->children()) {
      table_validators_[table].emplace_back(
          absl::make_unique<InterleaveParentValidator>(table, child));

      table_effectors_[table].emplace_back(
          absl::make_unique<InterleaveParentEffector>(table, child));
    }

    // Interleave actions for parent table.
    if (table->parent() != nullptr) {
      table_validators_[table].emplace_back(
          absl::make_unique<InterleaveChildValidator>(table->parent(), table));
    }

    // Actions for Index.
    for (const Index* index : table->indexes()) {
      // Index effects.
      table_effectors_[table].emplace_back(
          absl::make_unique<IndexEffector>(index));

      // Index uniqueness checks.
      if (index->is_unique()) {
        table_verifiers_[index->index_data_table()].emplace_back(
            absl::make_unique<UniqueIndexVerifier>(index));
      }
    }
  }
}

void ActionManager::AddActionsForSchema(const Schema* schema) {
  registry_[schema] = absl::make_unique<ActionRegistry>(schema);
}

zetasql_base::StatusOr<ActionRegistry*> ActionManager::GetActionsForSchema(
    const Schema* schema) const {
  auto itr = registry_.find(schema);
  if (itr == registry_.end()) {
    return error::Internal(
        absl::StrCat("Schema generation ", schema->generation(),
                     " was not registered with the Action Manager"));
  }
  return itr->second.get();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
