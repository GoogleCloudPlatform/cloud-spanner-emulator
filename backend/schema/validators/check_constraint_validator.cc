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

#include <string>
#include <vector>

#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "backend/schema/catalog/check_constraint.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/catalog/udf.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/updater/global_schema_names.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "backend/schema/updater/sql_expression_validators.h"
#include "common/errors.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

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

absl::Status ValidateNotUsingCommitTimestampColumns(
    const CheckConstraint* check_constraint) {
  for (const Column* column : check_constraint->dependent_columns()) {
    if (column->allows_commit_timestamp()) {
      return error::CannotUseCommitTimestampColumnOnCheckConstraint(
          column->Name());
    }
  }
  return absl::OkStatus();
}

const Column* FindDepColumnInCheckConstraintByName(
    std::string column_name, const CheckConstraint* check_constraint) {
  for (const Column* dep : check_constraint->dependent_columns()) {
    if (dep->Name() == column_name) {
      return dep;
    }
  }
  return nullptr;
}

absl::Status ValidateCheckConstraintSignatureChange(
    absl::string_view modify_action, absl::string_view dependency_name,
    const CheckConstraint* dependent_check_constraint,
    const Table* dependent_table, const Schema* temp_new_schema,
    zetasql::TypeFactory* type_factory) {
  // Re-analyze the dependent view based on the new definition of the
  // dependency in the temporary new schema.
  absl::flat_hash_set<const SchemaNode*> unused_udf_dependencies;
  absl::flat_hash_set<std::string> unused_dependent_column_names;
  std::vector<zetasql::SimpleTable::NameAndType> name_and_types;
  for (const Column* column : dependent_table->columns()) {
    name_and_types.emplace_back(column->Name(), column->GetType());
  }

  auto status = AnalyzeColumnExpression(
      dependent_check_constraint->expression(), zetasql::types::BoolType(),
      dependent_table, temp_new_schema, type_factory, name_and_types,
      "check constraints", &unused_dependent_column_names,
      /*dependent_sequences=*/nullptr,
      /*allow_volatile_expression=*/false, &unused_udf_dependencies);
  if (!status.ok()) {
    return error::DependentCheckConstraintBecomesInvalid(
        modify_action, dependency_name, dependent_check_constraint->Name(),
        status.message());
  }

  return absl::OkStatus();
}

}  // namespace

absl::Status CheckConstraintValidator::Validate(
    const CheckConstraint* check_constraint, SchemaValidationContext* context) {
  ZETASQL_RET_CHECK_NE(check_constraint->table_, nullptr);
  if (context->is_postgresql_dialect()) {
    ZETASQL_RET_CHECK(check_constraint->postgresql_oid().has_value());
  } else {
    ZETASQL_RET_CHECK(!check_constraint->postgresql_oid().has_value());
  }
  // Validates check constraint name.
  // The constraint type is not present to be consistent with production code.
  ZETASQL_RETURN_IF_ERROR(GlobalSchemaNames::ValidateConstraintName(
      check_constraint->table()->Name(), /*constraint_type=*/"",
      check_constraint->Name()));

  // The expression must reference at least one non-generated
  // column, whether directly or through a generated column which references a
  // non-generated column.
  ZETASQL_RETURN_IF_ERROR(ValidateDependsOnNonGenCol(check_constraint));

  ZETASQL_RETURN_IF_ERROR(ValidateNotUsingCommitTimestampColumns(check_constraint));

  return absl::OkStatus();
}

absl::Status CheckConstraintValidator::ValidateUpdate(
    const CheckConstraint* check_constraint,
    const CheckConstraint* old_check_constraint,
    SchemaValidationContext* context) {
  if (check_constraint->is_deleted()) {
    context->global_names()->RemoveName(check_constraint->Name());
  }

  if (check_constraint->table()->is_deleted()) {
    return absl::OkStatus();
  }

  for (const Column* dep : check_constraint->dependent_columns()) {
    if (dep->is_deleted()) {
      const auto& dep_info = dep->GetSchemaNameInfo();
      std::string dependency_type =
          (dep_info->global ? absl::AsciiStrToUpper(dep_info->kind)
                            : absl::AsciiStrToLower(dep_info->kind));
      return error::InvalidDropDependentCheckConstraint(
          dependency_type, dep_info->name, check_constraint->Name());
    }

    const Column* old_dep =
        FindDepColumnInCheckConstraintByName(dep->Name(), old_check_constraint);
    ZETASQL_RET_CHECK_NE(old_dep, nullptr);
    if (old_dep->GetType() != dep->GetType()) {
      return error::CannotAlterColumnDataTypeWithDependentCheckConstraint(
          dep->Name(), check_constraint->Name());
    }
  }
  if (context->is_postgresql_dialect()) {
    ZETASQL_RET_CHECK(check_constraint->postgresql_oid().has_value());
    ZETASQL_RET_CHECK(old_check_constraint->postgresql_oid().has_value());
    ZETASQL_RET_CHECK_EQ(check_constraint->postgresql_oid().value(),
                 old_check_constraint->postgresql_oid().value());
  } else {
    ZETASQL_RET_CHECK(!check_constraint->postgresql_oid().has_value());
    ZETASQL_RET_CHECK(!old_check_constraint->postgresql_oid().has_value());
  }

  for (const SchemaNode* dep : check_constraint->udf_dependencies()) {
    // TODO When dropping support is added, a check should be added
    // to ensure that this check references a UDF that is also being dropped.
    if (dep->is_deleted()) {
      const auto& dep_info = dep->GetSchemaNameInfo();
      std::string dependency_type =
          (dep_info->global ? absl::AsciiStrToUpper(dep_info->kind)
                            : absl::AsciiStrToLower(dep_info->kind));
      return error::InvalidDropDependentCheckConstraint(
          dependency_type, dep_info->name, check_constraint->Name());
    }

    if (context->IsModifiedNode(dep)) {
      const auto& dep_info = dep->GetSchemaNameInfo();
      std::string dependency_type =
          (dep_info->global ? absl::AsciiStrToUpper(dep_info->kind)
                            : absl::AsciiStrToLower(dep_info->kind));
      std::string modify_action = absl::StrCat("alter ", dependency_type);

      std::string dependency_name;
      if (auto dep_udf = dep->As<const Udf>(); dep_udf != nullptr) {
        dependency_name = dep_udf->Name();
      }
      // No need to check modifications on index dependencies as indexes
      // cannot currently be altered.
      ZETASQL_RETURN_IF_ERROR(ValidateCheckConstraintSignatureChange(
          modify_action, dependency_name, check_constraint,
          check_constraint->table(), context->tmp_new_schema(),
          context->type_factory()));
    }
  }

  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
