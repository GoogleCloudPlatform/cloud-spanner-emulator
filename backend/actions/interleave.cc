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

#include "backend/actions/interleave.h"

#include <memory>

#include "backend/datamodel/key_range.h"
#include "backend/storage/iterator.h"
#include "common/errors.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

InterleaveParentValidator::InterleaveParentValidator(const Table* parent,
                                                     const Table* child)
    : parent_(parent),
      child_(child),
      on_delete_action_(child->on_delete_action()) {}

absl::Status InterleaveParentValidator::Validate(const ActionContext* ctx,
                                                 const DeleteOp& op) const {
  switch (on_delete_action_) {
    case Table::OnDeleteAction::kNoAction: {
      ZETASQL_ASSIGN_OR_RETURN(bool has_children,
                       ctx->store()->PrefixExists(child_, op.key));
      if (has_children) {
        return error::ChildKeyExists(parent_->Name(), child_->Name(),
                                     op.key.DebugString());
      }
      return absl::OkStatus();
    }
    case Table::OnDeleteAction::kCascade: {
      return absl::OkStatus();
    }
  }
}

InterleaveParentEffector::InterleaveParentEffector(const Table* parent,
                                                   const Table* child)
    : parent_(parent),
      child_(child),
      on_delete_action_(child->on_delete_action()) {}

absl::Status InterleaveParentEffector::Effect(const ActionContext* ctx,
                                              const DeleteOp& op) const {
  switch (on_delete_action_) {
    case Table::OnDeleteAction::kNoAction: {
      return absl::OkStatus();
    }
    case Table::OnDeleteAction::kCascade: {
      ZETASQL_ASSIGN_OR_RETURN(
          std::unique_ptr<StorageIterator> itr,
          ctx->store()->Read(child_, KeyRange::Prefix(op.key), {}));
      while (itr->Next()) {
        ctx->effects()->Delete(child_, itr->Key());
      }
      return itr->Status();
    }
  }
}

InterleaveChildValidator::InterleaveChildValidator(const Table* parent,
                                                   const Table* child)
    : parent_(parent),
      child_(child),
      on_delete_action_(child->on_delete_action()) {}

absl::Status InterleaveChildValidator::Validate(const ActionContext* ctx,
                                                const InsertOp& op) const {
  // Compute the parent key as prefix of the child key.
  Key parent_key = op.key.Prefix(parent_->primary_key().size());

  ZETASQL_ASSIGN_OR_RETURN(bool has_parent, ctx->store()->Exists(parent_, parent_key));
  if (!has_parent) {
    return error::ParentKeyNotFound(parent_->Name(), child_->Name(),
                                    parent_key.DebugString());
  }
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
