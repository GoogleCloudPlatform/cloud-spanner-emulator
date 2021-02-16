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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_GENERATED_COLUMN_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_GENERATED_COLUMN_H_

#include <memory>

#include "zetasql/public/catalog.h"
#include "zetasql/public/evaluator.h"
#include "zetasql/public/value.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "backend/actions/action.h"
#include "backend/actions/ops.h"
#include "backend/schema/catalog/table.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// GeneratedColumnEffector generates and adds operations required to maintain
// generated columns to the transaction. It runs after all mutations are
// processed and buffered to the transaction. For each generated column in the
// table in topological order, it sends a query to compute the generated value
// and immediately adds an update operation to the transaction, such that the
// next generated column in topological order will see its effect.
class GeneratedColumnEffector : public Effector {
 public:
  explicit GeneratedColumnEffector(const Table* table,
                                   zetasql::Catalog* function_catalog);

  // Computes the value of the given 'generated_column' based on the given
  // 'row_column_values'.
  zetasql_base::StatusOr<zetasql::Value> ComputeGeneratedColumnValue(
      const Column* generated_column,
      const zetasql::ParameterValueMap& row_column_values) const;

 private:
  absl::Status Initialize(zetasql::Catalog* function_catalog);
  absl::Status Effect(const ActionContext* ctx,
                      const InsertOp& op) const override;
  absl::Status Effect(const ActionContext* ctx,
                      const UpdateOp& op) const override;
  absl::Status Effect(const ActionContext* ctx,
                      const DeleteOp& op) const override {
    return absl::OkStatus();
  }

  absl::Status Effect(const ActionContext* ctx, const Key& key,
                      zetasql::ParameterValueMap* column_values) const;

  const Table* table_;

  // List of generated columns in topological order.
  std::vector<const Column*> generated_columns_;

  // Map of generated columns to their corresponding expressions.
  absl::flat_hash_map<const Column*,
                      std::unique_ptr<zetasql::PreparedExpression>>
      expressions_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_GENERATED_COLUMN_H_
