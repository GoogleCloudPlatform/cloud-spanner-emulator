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

#include "backend/actions/unique_index.h"

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

UniqueIndexVerifier::UniqueIndexVerifier(const Index* index) : index_(index) {}

absl::Status UniqueIndexVerifier::Verify(const ActionContext* ctx,
                                         const InsertOp& op) const {
  if (!index_->is_unique()) {
    return absl::OkStatus();
  }

  // Prefix key from the index data table.
  Key index_key = op.key.Prefix(index_->key_columns().size());

  // Find all entries for the the given index key.
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<StorageIterator> itr,
                   ctx->store()->Read(index_->index_data_table(),
                                      KeyRange::Prefix(index_key), {}));
  if (!itr->Next()) {
    return error::Internal(
        absl::StrCat("Missing entry for index: ", index_->Name(),
                     " key: ", index_key.DebugString()));
  }
  if (itr->Next()) {
    // Return an error for multiple entries.
    return error::UniqueIndexConstraintViolation(index_->Name(),
                                                 index_key.DebugString());
  }
  ZETASQL_RETURN_IF_ERROR(itr->Status());
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
