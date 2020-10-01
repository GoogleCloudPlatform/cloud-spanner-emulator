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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_FOREIGN_KEY_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_FOREIGN_KEY_H_

#include "backend/actions/action.h"
#include "backend/actions/context.h"
#include "backend/actions/ops.h"
#include "backend/schema/catalog/foreign_key.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// ForeignKeyReferencingVerifier triggers on mutations to a foreign key's
// referencing index data table.
//
// Verifies that inserted and updated rows in the referencing index have
// corresponding rows in the referenced index. The index effector translates
// an update operation into a delete and insert operation. Deletions of
// referencing rows are always permitted, so we only need to verify the insert.
//
// A subsequent operation may fix a prior operation's constraint violation. For
// example, it is not a constraint violation if one operation inserts a
// referencing row, and a following operation inserts the referenced row. Since
// verifiers are triggered after all operations have been evaluated and applied,
// this verifier will only see the final result.
class ForeignKeyReferencingVerifier : public Verifier {
 public:
  explicit ForeignKeyReferencingVerifier(const ForeignKey* foreign_key);

 private:
  absl::Status Verify(const ActionContext* ctx,
                      const InsertOp& op) const override;

  const ForeignKey* foreign_key_;
};

// ForeignKeyReferencedVerifier triggers on mutations to a foreign key's
// referenced index data table.
//
// Verifies that rows deleted from the referenced index do not have
// corresponding rows in the referencing index.
//
// The index effector converts an update into delete and insert operations.
// Foreign keys always allow inserts into the referenced table. We only need to
// verify the deletion does not violate the constraint.
//
// A subsequent operation may fix a prior operation's constraint violation. For
// example, it is not a constraint violation if one operation deletes a
// referenced row, but a following operation inserts it. Since verifiers are
// triggered after all operations have been evaluated and applied, this verifier
// will only see the final result.
class ForeignKeyReferencedVerifier : public Verifier {
 public:
  explicit ForeignKeyReferencedVerifier(const ForeignKey* foreign_key);

 private:
  absl::Status Verify(const ActionContext* ctx,
                      const DeleteOp& op) const override;

  const ForeignKey* foreign_key_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_FOREIGN_KEY_H_
