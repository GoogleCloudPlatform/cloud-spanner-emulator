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

#include "backend/actions/existence.h"

#include "backend/actions/action.h"
#include "backend/actions/ops.h"
#include "common/errors.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

absl::Status RowExistenceValidator::Validate(const ActionContext* ctx,
                                             const InsertOp& op) const {
  ZETASQL_ASSIGN_OR_RETURN(bool row_exists, ctx->store()->Exists(op.table, op.key));
  if (row_exists) {
    return error::RowAlreadyExists(op.table->Name(), op.key.DebugString());
  }
  return absl::OkStatus();
}

absl::Status RowExistenceValidator::Validate(const ActionContext* ctx,
                                             const UpdateOp& op) const {
  ZETASQL_ASSIGN_OR_RETURN(bool row_exists, ctx->store()->Exists(op.table, op.key));
  if (!row_exists) {
    return error::RowNotFound(op.table->Name(), op.key.DebugString());
  }
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
