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

#include "backend/actions/foreign_key.h"

#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "backend/actions/action.h"
#include "backend/actions/context.h"
#include "backend/actions/ops.h"
#include "backend/datamodel/key_range.h"
#include "backend/storage/iterator.h"
#include "common/errors.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

ForeignKeyReferencingVerifier::ForeignKeyReferencingVerifier(
    const ForeignKey* foreign_key)
    : foreign_key_(foreign_key) {}

absl::Status ForeignKeyReferencingVerifier::Verify(const ActionContext* ctx,
                                                   const InsertOp& op) const {
  // Check that the corresponding row exists in the referenced index. Exclude
  // any extra columns from the primary key that are not used by the foreign
  // key.
  Key key(std::vector<zetasql::Value>(
      op.key.column_values().begin(),
      op.key.column_values().begin() +
          foreign_key_->referencing_columns().size()));
  ZETASQL_ASSIGN_OR_RETURN(
      bool exists,
      ctx->store()->PrefixExists(foreign_key_->referenced_data_table(), key));
  if (!exists) {
    return error::ForeignKeyReferencedKeyNotFound(
        foreign_key_->Name(), foreign_key_->referencing_table()->Name(),
        foreign_key_->referenced_table()->Name(), key.DebugString());
  }
  return absl::OkStatus();
}

ForeignKeyReferencedVerifier::ForeignKeyReferencedVerifier(
    const ForeignKey* foreign_key)
    : foreign_key_(foreign_key) {}

absl::Status ForeignKeyReferencedVerifier::Verify(const ActionContext* ctx,
                                                  const DeleteOp& op) const {
  // Check that the corresponding row does not exist in the referencing index.
  // Exclude any extra columns from the primary key that are not used by the
  // foreign key.
  Key key(std::vector<zetasql::Value>(
      op.key.column_values().begin(),
      op.key.column_values().begin() +
          foreign_key_->referencing_columns().size()));

  // It is possible that a deleted key is inserted back in the same transaction
  // later. So check whether the key is really deleted before validating the
  // foreign key.
  ZETASQL_ASSIGN_OR_RETURN(
      bool referenced_key_exists,
      ctx->store()->PrefixExists(foreign_key_->referenced_data_table(), key));
  if (!referenced_key_exists) {
    ZETASQL_ASSIGN_OR_RETURN(bool referencing_key_exists,
                     ctx->store()->PrefixExists(
                         foreign_key_->referencing_data_table(), key));
    if (referencing_key_exists) {
      return error::ForeignKeyReferencingKeyFound(
          foreign_key_->Name(), foreign_key_->referencing_table()->Name(),
          foreign_key_->referenced_table()->Name(), key.DebugString());
    }
  }
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
