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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_OPS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_OPS_H_

#include <string>
#include <vector>

#include "zetasql/public/value.h"
#include "backend/common/rows.h"
#include "backend/common/variant.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/value.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// InsertOp encapsulates a single row insert operation.
struct InsertOp {
  // The table on which the operation is performed.
  const Table* table;

  // Primary key for this row.
  Key key;

  // The columns & values for this row.
  std::vector<const Column*> columns;
  std::vector<zetasql::Value> values;
};

// UpdateOp encapsulates a single row update operation.
struct UpdateOp {
  // The table on which the operation is performed.
  const Table* table;

  // Primary key for this row.
  Key key;

  // The columns & values for this row.
  std::vector<const Column*> columns;
  std::vector<zetasql::Value> values;
};

// DeleteOp encapsulates a single row delete operation.
struct DeleteOp {
  // The table on which the operation is performed.
  const Table* table;

  // Primary key for this row.
  Key key;
};

// A variant over all possible row operations defined above.
// WriteOp represents an operation performed on a single row in the database.
using WriteOp = absl::variant<InsertOp, UpdateOp, DeleteOp>;

// Returns the table of the row operation.
const Table* TableOf(const WriteOp& op);

// Streams out a string representation of the WriteOp.
std::ostream& operator<<(std::ostream& out, const WriteOp& op);
std::ostream& operator<<(std::ostream& out, const InsertOp& op);
std::ostream& operator<<(std::ostream& out, const UpdateOp& op);
std::ostream& operator<<(std::ostream& out, const DeleteOp& op);

bool operator==(const InsertOp& op1, const InsertOp& op2);
bool operator==(const UpdateOp& op1, const UpdateOp& op2);
bool operator==(const DeleteOp& op1, const DeleteOp& op2);

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_OPS_H_
