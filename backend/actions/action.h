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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_ACTION_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_ACTION_H_

#include <string>

#include "backend/actions/context.h"
#include "backend/actions/ops.h"
#include "backend/schema/catalog/table.h"
#include "zetasql/base/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// A Validator validates an incoming write operation.
//
// Validator is executed for each write operation to ensure that they don't
// violate database constraints when applied to the database.
//
// Usage:
//   class InterleaveChildAction : Validator {
//    public:
//     InterleaveChildAction(...);
//
//    private:
//     zetasql_base::Status Validate(const ActionContext* ctx,
//                           const InsertOp& op) const override {
//       // Perform validation checks for parent row existence.
//       return zetasql_base::OkStatus();
//     }
//   };
class Validator {
 public:
  virtual ~Validator() {}

  // Validates the given WriteOp within the give action context.
  zetasql_base::Status Validate(const ActionContext* ctx, const WriteOp& op) const;

 private:
  virtual zetasql_base::Status Validate(const ActionContext* ctx,
                                const InsertOp& op) const;
  virtual zetasql_base::Status Validate(const ActionContext* ctx,
                                const UpdateOp& op) const;
  virtual zetasql_base::Status Validate(const ActionContext* ctx,
                                const DeleteOp& op) const;
};

// A Modifier modifies an incoming row operation.
//
// Usage:
//   class DefaultAction : Modifier {
//    public:
//     DefaultAction(...);
//
//    private:
//     zetasql_base::Status Modify(const ActionContext* ctx,
//                         const InsertOp& op) override {
//       // Update the column based on given InsertOp.
//       UpdateOp op = ...
//       // Add this row operation to the context.
//       ctx()->effects()->Add(op);
//       return zetasql_base::OkStatus();
//     }
//
//     zetasql_base::Status Modify(const ActionContext* ctx,
//                         const UpdateOp& op) override {
//       // Update the column based on given UpdateOp.
//       UpdateOp op = ...
//       // Add this row operation to the context.
//       ctx()->effects()->Add(op);
//       return zetasql_base::OkStatus();
//     }
//   };
class Modifier {
 public:
  virtual ~Modifier() {}

  // Modifies the given WriteOp within the give action context.
  zetasql_base::Status Modify(const ActionContext* ctx, const WriteOp& op) const;

 private:
  zetasql_base::Status Modify(const ActionContext* ctx, const InsertOp& op) const;
  zetasql_base::Status Modify(const ActionContext* ctx, const UpdateOp& op) const;
  zetasql_base::Status Modify(const ActionContext* ctx, const DeleteOp& op) const;
};

// A Effector adds extra row operations to a transaction.
//
// Usage:
//   class IndexAction : Effector {
//    public:
//     Index(...);
//
//    private:
//     zetasql_base::Status Effect(const ActionContext* ctx,
//                         const InsertOp& op) override {
//       // Add an Insert row operation for index given InsertOp on base table.
//       InsertOp op = ...
//       // Add this row operation to the context.
//       ctx()->effects()->Add(op);
//       return zetasql_base::OkStatus();
//     }
//
//     zetasql_base::Status Effect(const ActionContext* ctx,
//                         const DeleteOp& op) override {
//       // Add a Delete row operation for index given DeleteOp on base table.
//       DeleteOp op = ...
//       // Add this row operation to the context.
//       ctx()->effects()->Add(op);
//       return zetasql_base::OkStatus();
//     }
//   };
class Effector {
 public:
  virtual ~Effector() {}

  // Creates additional WriteOp(s) based on the given WriteOp within the give
  // action context.
  zetasql_base::Status Effect(const ActionContext* ctx, const WriteOp& op) const;

 private:
  virtual zetasql_base::Status Effect(const ActionContext* ctx,
                              const InsertOp& op) const;
  virtual zetasql_base::Status Effect(const ActionContext* ctx,
                              const UpdateOp& op) const;
  virtual zetasql_base::Status Effect(const ActionContext* ctx,
                              const DeleteOp& op) const;
};

// A Verifier verifies whether some database constraint is met. This
// executes at the end of the statement or transaction.
class Verifier {
 public:
  virtual ~Verifier() {}

  // Executes the verification on the given WriteOp within the give action
  // context.
  zetasql_base::Status Verify(const ActionContext* ctx, const WriteOp& op) const;

 private:
  virtual zetasql_base::Status Verify(const ActionContext* ctx,
                              const InsertOp& op) const;
  virtual zetasql_base::Status Verify(const ActionContext* ctx,
                              const UpdateOp& op) const;
  virtual zetasql_base::Status Verify(const ActionContext* ctx,
                              const DeleteOp& op) const;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_ACTION_H_
