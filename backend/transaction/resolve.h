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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_RESOLVE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_RESOLVE_H_

#include <vector>

#include "zetasql/base/statusor.h"
#include "absl/time/time.h"
#include "backend/access/read.h"
#include "backend/access/write.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// ResolvedReadArg holds validated schema objects corresponding to an input
// ReadArg.
struct ResolvedReadArg {
  // The table from which to read rows.
  const Table* table;

  // Canonicalized (disjoint and closed-open) key ranges to read.
  std::vector<KeyRange> key_ranges;

  // Set of columns to read.
  std::vector<const Column*> columns;
};

// ResolvedMutationOp holds validated schema objects corresponding to an input
// MutationOp.
struct ResolvedMutationOp {
  // The type of the mutation operation.
  MutationOpType type;

  // The table on which the mutation operation operates.
  const Table* table;

  // Set of columns to mutate.
  std::vector<const Column*> columns;

  // Values of rows with which to update columns.
  std::vector<ValueList> rows;

  // Keys corresponding to each of the row in rows.
  std::vector<Key> keys;

  // Canonicalized key ranges (disjoint and closed-open) to delete for mutation
  // op of type Delete.
  std::vector<KeyRange> key_ranges;
};

// Converts input ReadArg into ResolveReadArg after validating that input table,
// index and columns are valid schema objects.
zetasql_base::StatusOr<ResolvedReadArg> ResolveReadArg(const ReadArg& read_arg,
                                               const Schema* schema);

// Converts input MutationOp into ResolvedMutationOp after validating that input
// table, columns and rows are valid schema objects. Validates that user
// supplied values for commit timestamp are not in future by comparing against
// now.
zetasql_base::StatusOr<ResolvedMutationOp> ResolveMutationOp(
    const MutationOp& mutation_op, const Schema* schema, absl::Time now);

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_RESOLVE_H_
