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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_CHECK_CONSTRAINT_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_CHECK_CONSTRAINT_H_

#include <memory>

#include "zetasql/public/evaluator.h"
#include "backend/actions/action.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// CheckConstraintVerifier validates if the inserted/updated rows constaint
// values which will violate the check constraints that reference the
// corresponding columns.
class CheckConstraintVerifier : public Verifier {
 public:
  explicit CheckConstraintVerifier(const CheckConstraint* check_constraint,
                                   zetasql::Catalog* function_catalog);
  // Verifies that column values of the row with a given key satisfy the check
  // constraint.
  absl::Status VerifyRow(const zetasql::ParameterValueMap& column_values,
                         const Key& key) const;

 private:
  // Initializes an executable expression (i.e. expression_) given the check
  // constraint expression.
  absl::Status PrepareExpression(const CheckConstraint* check_constraint,
                                 zetasql::Catalog* function_catalog);
  // Computes the vaule of the check constraint expression.
  absl::Status EvaluateCheckConstraintExpression(
      const zetasql::ParameterValueMap& row_column_values) const;
  // An internal function used by both InsertOp and UpdateOp verification.
  absl::Status VerifyInsertUpdateOp(const ActionContext* ctx,
                                    const Table* table,
                                    const std::vector<const Column*>& columns,
                                    const std::vector<zetasql::Value>& values,
                                    const Key& key) const;
  absl::Status Verify(const ActionContext* ctx,
                      const InsertOp& op) const override;
  absl::Status Verify(const ActionContext* ctx,
                      const UpdateOp& op) const override;

  const CheckConstraint* check_constraint_;
  std::unique_ptr<zetasql::PreparedExpression> expression_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_CHECK_CONSTRAINT_H_
