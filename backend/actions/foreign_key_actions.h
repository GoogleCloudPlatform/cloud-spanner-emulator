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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_FOREIGN_KEY_ACTIONS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_FOREIGN_KEY_ACTIONS_H_

#include "absl/status/status.h"
#include "backend/actions/action.h"
#include "backend/actions/context.h"
#include "backend/actions/ops.h"
#include "backend/datamodel/key.h"
#include "backend/schema/catalog/foreign_key.h"
#include "backend/schema/catalog/table.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

enum class FKPrefixShape {
  kNone,
  kInOrder,        // FK is in the same order as the PK.
  kInOrderPrefix,  // FK is in the same order as the PK, but is a prefix.
  kOutOfOrder,  // FK is in a different order than the PK, and may be a prefix.
};

// ForeignKeyActionEffector triggers on mutations for the referenced table.
// When a row is deleted from the referenced table, all rows in the referencing
// table that reference that foreign key are also deleted.
class ForeignKeyActionEffector : public Effector {
 public:
  explicit ForeignKeyActionEffector(const ForeignKey* foreign_key);

 private:
  absl::Status Effect(const ActionContext* ctx,
                      const DeleteOp& op) const override;
  absl::Status EffectForUnorderedReferencedKey(const ActionContext* ctx,
                                               const DeleteOp& op) const;
  absl::Status EffectForNonPKReferencedKey(const ActionContext* ctx,
                                           const DeleteOp& op) const;
  // Retrive all rows in the referencing table that reference the foreign key,
  // and then delete those rows.
  absl::Status ProcessDeleteByKey(const ActionContext* ctx,
                                  const Key& referenced_key) const;
  absl::Status ProcessDeleteForUnorderedReferencingKey(
      const ActionContext* ctx, const Key& referenced_key) const;
  absl::Status ProcessDeleteForNonPKReferencingKey(
      const ActionContext* ctx, const Key& referenced_key) const;

  const ForeignKey* foreign_key_;
  const ForeignKey::Action on_delete_action_;
  const FKPrefixShape referenced_key_prefix_shape_;
  const FKPrefixShape referencing_key_prefix_shape_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_FOREIGN_KEY_ACTIONS_H_
