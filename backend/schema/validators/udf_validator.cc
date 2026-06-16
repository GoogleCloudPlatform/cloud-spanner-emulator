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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "googlesql/public/function_signature.h"
#include "googlesql/public/strings.h"
#include "googlesql/public/type.pb.h"
#include "googlesql/public/types/struct_type.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/types/type_factory.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
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
#include "googlesql/base/ret_check.h"
#include "googlesql/base/status_macros.h"
#include "third_party/spanner_pg/catalog/spangres_type.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

absl::Status IsSupportedTypeForRemoteFunction(const googlesql::Type* type,
                                              absl::string_view function_name) {
  GOOGLESQL_RET_CHECK(type != nullptr);

  switch (type->kind()) {
    case googlesql::TYPE_INT64:
    case googlesql::TYPE_BOOL:
    case googlesql::TYPE_STRING:
    case googlesql::TYPE_BYTES:
    case googlesql::TYPE_FLOAT:
    case googlesql::TYPE_DOUBLE:
    case googlesql::TYPE_TIMESTAMP:
    case googlesql::TYPE_DATE:
    case googlesql::TYPE_ENUM:
    case googlesql::TYPE_PROTO:
    case googlesql::TYPE_NUMERIC:
    case googlesql::TYPE_JSON:
    case googlesql::TYPE_INTERVAL:
    case googlesql::TYPE_UUID:
      return absl::OkStatus();

    case googlesql::TYPE_EXTENDED: {
      if (type->Equals(
              postgres_translator::spangres::types::PgNumericMapping())) {
        return absl::OkStatus();
      } else if (type->Equals(
                     postgres_translator::spangres::types::PgJsonbMapping())) {
        return absl::OkStatus();
      } else if (type->Equals(
                     postgres_translator::spangres::types::PgOidMapping())) {
        return absl::OkStatus();
      }
      break;
    }

    case googlesql::TYPE_ARRAY:
      return IsSupportedTypeForRemoteFunction(type->AsArray()->element_type(),
                                              function_name);

    case googlesql::TYPE_MAP:
      GOOGLESQL_RETURN_IF_ERROR(IsSupportedTypeForRemoteFunction(
          type->AsMap()->key_type(), function_name));
      return IsSupportedTypeForRemoteFunction(type->AsMap()->value_type(),
                                              function_name);

    case googlesql::TYPE_STRUCT: {
      absl::flat_hash_set<std::string> field_names;
      for (const googlesql::StructField& field : type->AsStruct()->fields()) {
        if (!field_names.insert(field.name).second) {
          return error::DuplicateStructFieldNamesInRemoteFunction(function_name,
                                                                  field.name);
        }
        GOOGLESQL_RETURN_IF_ERROR(
            IsSupportedTypeForRemoteFunction(field.type, function_name));
      }
      return absl::OkStatus();
    }

    // Types not supported by remote functions.
    case googlesql::TYPE_TOKENLIST:
    default:
      break;
  }

  return error::UnsupportedTypeInRemoteFunction(
      function_name, type->TypeName(googlesql::PRODUCT_EXTERNAL));
}

absl::Status ValidateUdfSignatureChange(absl::string_view modify_action,
                                        absl::string_view dependency_name,
                                        absl::string_view param_list,
                                        const Udf* dependent_udf,
                                        const Schema* temp_new_schema,
                                        googlesql::TypeFactory* type_factory) {
  // Re-analyze the dependent udf based on the new definition of the dependency
  // in the temporary new schema.
  absl::flat_hash_set<const SchemaNode*> unused_new_deps;
  std::unique_ptr<googlesql::FunctionSignature> unused_signature;
  Udf::Determinism determinism_level =
      Udf::Determinism::DETERMINISM_UNSPECIFIED;

  // Re-create the options string based on the dependent UDF's options.
  absl::flat_hash_map<std::string, std::string> options_map;
  if (dependent_udf->endpoint().has_value()) {
    options_map["endpoint"] =
        googlesql::ToDoubleQuotedStringLiteral(*dependent_udf->endpoint());
  }
  if (dependent_udf->max_batching_rows().has_value()) {
    options_map["max_batching_rows"] =
        absl::StrCat(*dependent_udf->max_batching_rows());
  }

  std::string options = "";
  if (!options_map.empty()) {
    options = absl::StrCat(
        " OPTIONS (",
        absl::StrJoin(options_map, ", ", absl::PairFormatter(" = ")), ")");
  }

  std::string language = "";
  switch (dependent_udf->language()) {
    case Udf::Language::SQL:
      language = "SQL";
      break;
    case Udf::Language::REMOTE:
      language = "REMOTE";
      break;
    case Udf::Language::LANGUAGE_UNSPECIFIED:
      break;
  }

  std::optional<std::string> unused_endpoint;
  std::optional<int64_t> unused_max_batching_rows;
  auto status = AnalyzeUdfDefinition(
      dependent_udf->Name(), param_list, dependent_udf->body(),
      dependent_udf->is_remote(), language,
      dependent_udf->signature()->result_type().type()->TypeName(
          googlesql::PRODUCT_EXTERNAL),
      options, temp_new_schema, type_factory, &unused_new_deps,
      &unused_signature, &determinism_level, &unused_endpoint,
      &unused_max_batching_rows);
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
  GOOGLESQL_RET_CHECK(!udf->name_.empty());
  if (context->is_postgresql_dialect()) {
    GOOGLESQL_RET_CHECK(udf->postgresql_oid().has_value());
  } else {
    GOOGLESQL_RET_CHECK(!udf->postgresql_oid().has_value());
  }

  GOOGLESQL_RETURN_IF_ERROR(GlobalSchemaNames::ValidateSchemaName(
      udf->GetSchemaNameInfo()->kind, udf->Name()));

  for (const SchemaNode* dependency : udf->dependencies()) {
    GOOGLESQL_RET_CHECK(!dependency->is_deleted());
  }

  if (udf->is_remote()) {
    GOOGLESQL_RET_CHECK_EQ(udf->language(), Udf::Language::LANGUAGE_UNSPECIFIED);
  }

  bool remote_udf =
      udf->language() == Udf::Language::REMOTE || udf->is_remote();
  if (remote_udf) {
    GOOGLESQL_RET_CHECK(udf->body_.empty());
    if (!udf->endpoint_.has_value()) {
      return error::MissingOptionForFunction("endpoint", udf->Name());
    }
    if (udf->max_batching_rows_.has_value() && *udf->max_batching_rows_ < 0) {
      return error::InvalidOptionValueForFunction(
          absl::StrCat(*udf->max_batching_rows_), "max_batching_rows",
          udf->Name());
    }

    // Validate that all types are supported for remote functions.
    GOOGLESQL_RET_CHECK(udf->signature() != nullptr);
    for (const googlesql::FunctionArgumentType& arg :
         udf->signature()->arguments()) {
      GOOGLESQL_RETURN_IF_ERROR(
          IsSupportedTypeForRemoteFunction(arg.type(), udf->Name()));
    }

    GOOGLESQL_RETURN_IF_ERROR(IsSupportedTypeForRemoteFunction(
        udf->signature()->result_type().type(), udf->Name()));

    // Validate that determinism is set for remote functions.
    if (udf->determinism_level() != Udf::NOT_DETERMINISTIC_VOLATILE) {
      return error::RemoteUdfMustBeNotDeterministic(
          udf->Name(),
          context->is_postgresql_dialect() ? "VOLATILE" : "NOT DETERMINISTIC");
    }
  } else {
    GOOGLESQL_RET_CHECK(udf->language() == Udf::Language::SQL ||
              udf->language() == Udf::Language::LANGUAGE_UNSPECIFIED);
    GOOGLESQL_RET_CHECK(!udf->body_.empty());

    if (udf->endpoint_.has_value()) {
      return error::InvalidOptionForFunction("endpoint", udf->Name());
    }

    if (udf->max_batching_rows_.has_value()) {
      return error::InvalidOptionValueForFunction("max_batching_rows",
                                                  udf->Name());
    }
  }

  return absl::OkStatus();
}

// TODO Implement this function once UDFs are added to the catalog.
absl::Status UdfValidator::ValidateUpdate(const Udf* udf, const Udf* old_udf,
                                          SchemaValidationContext* context) {
  // During a REPLACE, the udf name's case can change.
  if (context->IsModifiedNode(udf)) {
    GOOGLESQL_RET_CHECK(absl::EqualsIgnoreCase(udf->Name(), old_udf->Name()));
  } else {
    GOOGLESQL_RET_CHECK_EQ(udf->Name(), old_udf->Name());
  }
  if (udf->is_deleted()) {
    context->global_names()->RemoveName(udf->Name());
    return absl::OkStatus();
  }
  if (context->is_postgresql_dialect()) {
    GOOGLESQL_RET_CHECK(udf->postgresql_oid().has_value());
    GOOGLESQL_RET_CHECK(old_udf->postgresql_oid().has_value());
    GOOGLESQL_RET_CHECK_EQ(udf->postgresql_oid().value(),
                 old_udf->postgresql_oid().value());
  } else {
    GOOGLESQL_RET_CHECK(!udf->postgresql_oid().has_value());
    GOOGLESQL_RET_CHECK(!old_udf->postgresql_oid().has_value());
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
                      param.type()->TypeName(googlesql::PRODUCT_EXTERNAL);
        if (i < args.size() - 1) {
          param_list += ", ";
        }
      }
      GOOGLESQL_RETURN_IF_ERROR(ValidateUdfSignatureChange(
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
