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

#include "backend/schema/validators/check_constraint_validator.h"

#include "absl/status/status.h"
#include "backend/schema/updater/global_schema_names.h"
#include "common/errors.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {
// Returns true if the node directly or indirectly references a
// non-generated column.
template <typename T>
bool DependsOnNonGenCol(T* node) {
  for (const Column* column : node->dependent_columns()) {
    if (!column->is_generated() || DependsOnNonGenCol(column)) {
      return true;
    }
  }
  return false;
}

absl::Status ValidateDependsOnNonGenCol(
    const CheckConstraint* check_constraint) {
  if (!DependsOnNonGenCol(check_constraint)) {
    return error::CheckConstraintNotUsingAnyNonGeneratedColumn(
        check_constraint->table()->Name(), check_constraint->Name(),
        check_constraint->expression());
  }
  return absl::OkStatus();
}
}  // namespace

absl::Status CheckConstraintValidator::Validate(
    const CheckConstraint* check_constraint, SchemaValidationContext* context) {
  ZETASQL_RET_CHECK_NE(check_constraint->table_, nullptr);
  // Validates check constraint name.
  // The constraint type is not present to be consistent with production code.
  ZETASQL_RETURN_IF_ERROR(GlobalSchemaNames::ValidateConstraintName(
      check_constraint->table()->Name(), /*constraint_type=*/"",
      check_constraint->Name()));

  // The expression must reference at least one non-generated
  // column, whether directly or through a generated column which references a
  // non-generated column.
  ZETASQL_RETURN_IF_ERROR(ValidateDependsOnNonGenCol(check_constraint));

  return absl::OkStatus();
}

absl::Status CheckConstraintValidator::ValidateUpdate(
    const CheckConstraint* check_constraint,
    const CheckConstraint* old_check_constraint,
    SchemaValidationContext* context) {
  if (check_constraint->is_deleted()) {
    context->global_names()->RemoveName(check_constraint->Name());
  }
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
