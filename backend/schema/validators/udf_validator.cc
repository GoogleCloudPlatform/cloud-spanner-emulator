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

#include "backend/schema/validators/udf_validator.h"

#include <memory>
#include <string>
#include <vector>

#include "zetasql/public/function_signature.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/catalog/udf.h"
#include "backend/schema/catalog/view.h"
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

absl::Status ValidateUdfSignatureChange(absl::string_view modify_action,
                                        absl::string_view dependency_name,
                                        absl::string_view param_list,
                                        const Udf* dependent_udf,
                                        const Schema* temp_new_schema,
                                        zetasql::TypeFactory* type_factory) {
  // Re-analyze the dependent udf based on the new definition of the dependency
  // in the temporary new schema.
  absl::flat_hash_set<const SchemaNode*> unused_new_deps;
  std::unique_ptr<zetasql::FunctionSignature> unused_signature;
  Udf::Determinism determinism_level =
      Udf::Determinism::DETERMINISM_UNSPECIFIED;
  auto status = AnalyzeUdfDefinition(
      dependent_udf->Name(), param_list, dependent_udf->body(), temp_new_schema,
      type_factory, &unused_new_deps, &unused_signature, &determinism_level);
  if (!status.ok()) {
    return error::DependentFunctionBecomesInvalid(
        modify_action, dependency_name, dependent_udf->Name(),
        status.message());
  }

  return absl::OkStatus();
}

}  // namespace

absl::Status UdfValidator::Validate(const Udf* udf,
                                    SchemaValidationContext* context) {
  ZETASQL_RET_CHECK(!udf->name_.empty());
  ZETASQL_RET_CHECK(!udf->body_.empty());
  if (context->is_postgresql_dialect()) {
    ZETASQL_RET_CHECK(udf->postgresql_oid().has_value());
  } else {
    ZETASQL_RET_CHECK(!udf->postgresql_oid().has_value());
  }

  ZETASQL_RETURN_IF_ERROR(GlobalSchemaNames::ValidateSchemaName(
      udf->GetSchemaNameInfo()->kind, udf->Name()));

  for (const SchemaNode* dependency : udf->dependencies()) {
    ZETASQL_RET_CHECK(!dependency->is_deleted());
  }

  return absl::OkStatus();
}

// TODO Implement this function once UDFs are added to the catalog.
absl::Status UdfValidator::ValidateUpdate(const Udf* udf, const Udf* old_udf,
                                          SchemaValidationContext* context) {
  // During a REPLACE, the udf name's case can change.
  if (context->IsModifiedNode(udf)) {
    ZETASQL_RET_CHECK(absl::EqualsIgnoreCase(udf->Name(), old_udf->Name()));
  } else {
    ZETASQL_RET_CHECK_EQ(udf->Name(), old_udf->Name());
  }
  if (udf->is_deleted()) {
    context->global_names()->RemoveName(udf->Name());
    return absl::OkStatus();
  }
  if (context->is_postgresql_dialect()) {
    ZETASQL_RET_CHECK(udf->postgresql_oid().has_value());
    ZETASQL_RET_CHECK(old_udf->postgresql_oid().has_value());
    ZETASQL_RET_CHECK_EQ(udf->postgresql_oid().value(),
                 old_udf->postgresql_oid().value());
  } else {
    ZETASQL_RET_CHECK(!udf->postgresql_oid().has_value());
    ZETASQL_RET_CHECK(!old_udf->postgresql_oid().has_value());
  }

  for (const SchemaNode* dependency : udf->dependencies()) {
    // Cannot drop dependencies of the udf.
    if (dependency->is_deleted()) {
      // If the deleted dependency is a column that was marked as deleted
      // as a result of a table drop, then we use the table to report the error
      // message.
      if (auto dep_column = dependency->As<const Column>();
          dep_column != nullptr) {
        const Table* dep_table = dep_column->table();
        if (dep_table->is_deleted()) {
          dependency = dep_table;
        }
      }
      const auto& dep_info = dependency->GetSchemaNameInfo();
      std::string dependency_type =
          (dep_info->global ? absl::AsciiStrToUpper(dep_info->kind)
                            : absl::AsciiStrToLower(dep_info->kind));
      return error::InvalidDropDependentFunction(dependency_type,
                                                 dep_info->name, udf->Name());
    }

    // If a dependency was updated during the schema change then we need to
    // re-analyze *this.
    if (context->IsModifiedNode(dependency)) {
      const auto& dep_info = dependency->GetSchemaNameInfo();
      std::string dependency_type =
          (dep_info->global ? absl::AsciiStrToUpper(dep_info->kind)
                            : absl::AsciiStrToLower(dep_info->kind));
      std::string modify_action = absl::StrCat("alter ", dependency_type);
      std::string dependency_name;
      if (auto dep_view = dependency->As<const View>(); dep_view != nullptr) {
        dependency_name = dep_view->Name();
      }
      if (auto dep_table = dependency->As<const Table>();
          dep_table != nullptr) {
        dependency_name = dep_table->Name();
      }
      if (auto dep_column = dependency->As<const Column>();
          dep_column != nullptr) {
        dependency_name = dep_column->FullName();
      }
      if (auto dep_udf = dependency->As<const Udf>(); dep_udf != nullptr) {
        dependency_name = dep_udf->Name();
      }
      // No need to check modifications on index dependencies as indexes
      // cannot currently be altered.
      std::string param_list = "";
      auto args = udf->signature()->arguments();
      for (int i = 0; i < args.size(); i++) {
        const auto& param = args[i];
        param_list += param.argument_name() + " " +
                      param.type()->TypeName(zetasql::PRODUCT_EXTERNAL);
        if (i < args.size() - 1) {
          param_list += ", ";
        }
      }
      ZETASQL_RETURN_IF_ERROR(ValidateUdfSignatureChange(
          modify_action, dependency_name, param_list, udf,
          context->tmp_new_schema(), context->type_factory()));
    }
  }
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
