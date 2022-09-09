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
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "backend/access/write.h"
#include "backend/actions/check_constraint.h"
#include "backend/actions/column_value.h"
#include "backend/actions/existence.h"
#include "backend/actions/foreign_key.h"
#include "backend/actions/generated_column.h"
#include "backend/actions/index.h"
#include "backend/actions/interleave.h"
#include "backend/actions/unique_index.h"
#include "backend/schema/catalog/check_constraint.h"
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

absl::Status ActionRegistry::ExecuteGeneratedKeyEffectors(
    const MutationOp& op, std::vector<zetasql::Value>* generated_values,
    std::vector<const Column*>* columns_with_generated_values) {
  if (table_generated_key_effectors_.find(op.table) ==
      table_generated_key_effectors_.end()) {
    return absl::OkStatus();
  }

  ZETASQL_RETURN_IF_ERROR(table_generated_key_effectors_[op.table]->Effect(
      op, generated_values, columns_with_generated_values));
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

ActionRegistry::ActionRegistry(const Schema* schema,
                               const FunctionCatalog* function_catalog)
    : schema_(schema), catalog_(schema, function_catalog) {
  BuildActionRegistry();
}

void ActionRegistry::BuildActionRegistry() {
  for (const Table* table : schema_->tables()) {
    // Column value checks for all tables.
    table_validators_[table].emplace_back(
        std::make_unique<ColumnValueValidator>());

    // Row existence checks for all tables.
    table_validators_[table].emplace_back(
        std::make_unique<RowExistenceValidator>());

    // Interleave actions for child tables.
    for (const Table* child : table->children()) {
      table_validators_[table].emplace_back(
          std::make_unique<InterleaveParentValidator>(table, child));

      table_effectors_[table].emplace_back(
          std::make_unique<InterleaveParentEffector>(table, child));
    }

    // Interleave actions for parent table.
    if (table->parent() != nullptr) {
      table_validators_[table].emplace_back(
          std::make_unique<InterleaveChildValidator>(table->parent(), table));
    }

    // Actions for Index.
    for (const Index* index : table->indexes()) {
      // Index effects.
      table_effectors_[table].emplace_back(
          std::make_unique<IndexEffector>(index));

      // Index uniqueness checks.
      if (index->is_unique()) {
        table_verifiers_[index->index_data_table()].emplace_back(
            std::make_unique<UniqueIndexVerifier>(index));
      }
    }

    // Actions for foreign keys.
    for (const ForeignKey* foreign_key : table->foreign_keys()) {
      table_verifiers_[foreign_key->referencing_data_table()].emplace_back(
          std::make_unique<ForeignKeyReferencingVerifier>(foreign_key));
    }
    for (const ForeignKey* foreign_key : table->referencing_foreign_keys()) {
      table_verifiers_[foreign_key->referenced_data_table()].emplace_back(
          std::make_unique<ForeignKeyReferencedVerifier>(foreign_key));
    }

    // Actions for check constraints.
    for (const CheckConstraint* check_constraint : table->check_constraints()) {
      table_verifiers_[table].emplace_back(
          std::make_unique<CheckConstraintVerifier>(check_constraint,
                                                    &catalog_));
    }

    // A set containing key columns with default values.
    absl::flat_hash_set<std::string> default_key_columns;
    // Effector for primary key default columns.
    for (const Column* column : table->columns()) {
      if (column->has_default_value() &&
          table->FindKeyColumn(column->Name()) != nullptr) {
        default_key_columns.insert(column->Name());
        table_generated_key_effectors_[table->Name()] =
            std::make_unique<GeneratedColumnEffector>(table, &catalog_,
                                                      /*for_keys=*/true);
        break;
      }
    }

    // Effector for non-key generated and default columns.
    for (const Column* column : table->columns()) {
      if (!default_key_columns.contains(column->Name()) &&
          (column->is_generated() || column->has_default_value())) {
        table_effectors_[table].emplace_back(
            std::make_unique<GeneratedColumnEffector>(table, &catalog_));
        break;
      }
    }
  }
}

void ActionManager::AddActionsForSchema(
    const Schema* schema, const FunctionCatalog* function_catalog) {
  registry_[schema] =
      std::make_unique<ActionRegistry>(schema, function_catalog);
}

absl::StatusOr<ActionRegistry*> ActionManager::GetActionsForSchema(
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
