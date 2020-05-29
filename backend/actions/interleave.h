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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_INTERLEAVE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_INTERLEAVE_H_

#include "backend/actions/action.h"
#include "backend/actions/ops.h"
#include "backend/schema/catalog/table.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// InterleaveParentValidator triggers on mutations to a parent table in an
// interleave relationship.
//
// Insert and Update operations to a row in parent table do not need any
// validation for interleaved relationship. For delete operation to the parent
// row, there are the following two cases:
// - kNoAction: Returns an error if child rows exist.
// - kCascade : Always allowed, deletions for child rows will be added by the
// effector below.
class InterleaveParentValidator : public Validator {
 public:
  InterleaveParentValidator(const Table* parent, const Table* child);

 private:
  absl::Status Validate(const ActionContext* ctx,
                        const DeleteOp& op) const override;

  const Table* parent_;
  const Table* child_;
  const Table::OnDeleteAction on_delete_action_;
};

// InterleaveParentEffector triggers on mutations to a parent table in an
// interleave relationship.
//
// Insert and Update operations to a row in parent table do not add any
// effects for interleaved relationship. For delete operation to the parent row,
// there are the following two cases:
// - kNoAction: No extra mutations are added.
// - kCascade : Additional mutations are added to delete child rows.
class InterleaveParentEffector : public Effector {
 public:
  InterleaveParentEffector(const Table* parent, const Table* child);

 private:
  absl::Status Effect(const ActionContext* ctx,
                      const DeleteOp& op) const override;

  const Table* parent_;
  const Table* child_;
  const Table::OnDeleteAction on_delete_action_;
};

// InterleaveChildValidator validates row operations on a child table.
//
// Update and Delete operations to a row in child table do not need any
// validation for interleaved relationship. For Insert operation to a child row,
// it will validate if a parent row exist.
class InterleaveChildValidator : public Validator {
 public:
  InterleaveChildValidator(const Table* parent, const Table* child);

 private:
  absl::Status Validate(const ActionContext* ctx,
                        const InsertOp& op) const override;

  const Table* parent_;
  const Table* child_;
  const Table::OnDeleteAction on_delete_action_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_INTERLEAVE_H_
