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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_WRITE_UTIL_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_WRITE_UTIL_H_

#include <queue>

#include "backend/access/write.h"
#include "backend/actions/ops.h"
#include "backend/datamodel/value.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "backend/transaction/transaction_store.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Converts each MutationOp row to a WriteOp based on the MutationOpType:
// - MutatioOpTyp::kReplace: converts to DeleteOp followed by InsertOp.
// - MutationOpType::kInsertOrUpdate: if the row already exists, converts
//   to UpdateOp. Otherwise converts to InsertOp.
// - MutationOpType::kInsert | kDelete | kUpdate: converts to
//   corresponding WriteOp of the same type.
zetasql_base::Status FlattenMutationOpRow(
    const Table* table, const std::vector<const Column*>& columns,
    const std::vector<absl::optional<int>>& key_indices, ValueList row,
    const MutationOpType& type, const TransactionStore* transaction_store,
    std::queue<WriteOp>* write_ops_queue, absl::Time now);

// Extracts the primary key column indices from the given list of columns. The
// returned indices will be in the order specified by the primary key. Nullable
// primary key columns do not need to be specified, in which the index entry
// will be nullopt.
zetasql_base::StatusOr<std::vector<absl::optional<int>>> ExtractPrimaryKeyIndices(
    absl::Span<const std::string> columns,
    absl::Span<const KeyColumn* const> primary_key);

// Flattens delete mutations within write request.
zetasql_base::Status FlattenDelete(const MutationOp& mutation_op, const Table* table,
                           std::vector<const Column*> columns,
                           const TransactionStore* transaction_store,
                           std::queue<WriteOp>* write_ops_queue,
                           absl::Time now);

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_WRITE_UTIL_H_
