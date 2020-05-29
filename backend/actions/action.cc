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

#include "backend/actions/action.h"

#include "absl/status/status.h"
#include "backend/actions/ops.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

absl::Status Validator::Validate(const ActionContext* ctx,
                                 const WriteOp& op) const {
  return std::visit(overloaded{
                        [&](const InsertOp& op) { return Validate(ctx, op); },
                        [&](const UpdateOp& op) { return Validate(ctx, op); },
                        [&](const DeleteOp& op) { return Validate(ctx, op); },
                    },
                    op);
}

absl::Status Validator::Validate(const ActionContext* ctx,
                                 const InsertOp& op) const {
  return absl::OkStatus();
}
absl::Status Validator::Validate(const ActionContext* ctx,
                                 const UpdateOp& op) const {
  return absl::OkStatus();
}
absl::Status Validator::Validate(const ActionContext* ctx,
                                 const DeleteOp& op) const {
  return absl::OkStatus();
}

absl::Status Modifier::Modify(const ActionContext* ctx,
                              const WriteOp& op) const {
  return std::visit(overloaded{
                        [&](const InsertOp& op) { return Modify(ctx, op); },
                        [&](const UpdateOp& op) { return Modify(ctx, op); },
                        [&](const DeleteOp& op) { return Modify(ctx, op); },
                    },
                    op);
}

absl::Status Modifier::Modify(const ActionContext* ctx,
                              const InsertOp& op) const {
  return absl::OkStatus();
}
absl::Status Modifier::Modify(const ActionContext* ctx,
                              const UpdateOp& op) const {
  return absl::OkStatus();
}
absl::Status Modifier::Modify(const ActionContext* ctx,
                              const DeleteOp& op) const {
  return absl::OkStatus();
}

absl::Status Effector::Effect(const ActionContext* ctx,
                              const WriteOp& op) const {
  return std::visit(overloaded{
                        [&](const InsertOp& op) { return Effect(ctx, op); },
                        [&](const UpdateOp& op) { return Effect(ctx, op); },
                        [&](const DeleteOp& op) { return Effect(ctx, op); },
                    },
                    op);
}

absl::Status Effector::Effect(const ActionContext* ctx,
                              const InsertOp& op) const {
  return absl::OkStatus();
}
absl::Status Effector::Effect(const ActionContext* ctx,
                              const UpdateOp& op) const {
  return absl::OkStatus();
}
absl::Status Effector::Effect(const ActionContext* ctx,
                              const DeleteOp& op) const {
  return absl::OkStatus();
}

absl::Status Verifier::Verify(const ActionContext* ctx,
                              const WriteOp& op) const {
  return std::visit(overloaded{
                        [&](const InsertOp& op) { return Verify(ctx, op); },
                        [&](const UpdateOp& op) { return Verify(ctx, op); },
                        [&](const DeleteOp& op) { return Verify(ctx, op); },
                    },
                    op);
}

absl::Status Verifier::Verify(const ActionContext* ctx,
                              const InsertOp& op) const {
  return absl::OkStatus();
}
absl::Status Verifier::Verify(const ActionContext* ctx,
                              const UpdateOp& op) const {
  return absl::OkStatus();
}
absl::Status Verifier::Verify(const ActionContext* ctx,
                              const DeleteOp& op) const {
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
