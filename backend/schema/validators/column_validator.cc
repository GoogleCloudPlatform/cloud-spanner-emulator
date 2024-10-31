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

#include "backend/schema/validators/column_validator.h"

#include <string>
#include <vector>

#include "zetasql/public/options.pb.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "backend/datamodel/types.h"
#include "backend/schema/backfills/column_value_backfill.h"
#include "backend/schema/catalog/change_stream.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/proto_bundle.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/catalog/udf.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "backend/schema/updater/sql_expression_validators.h"
#include "backend/schema/verifiers/column_value_verifiers.h"
#include "common/errors.h"
#include "common/feature_flags.h"
#include "common/limits.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

bool IsResizeable(const zetasql::Type* type) {
  if (type->IsString() || type->IsBytes()) {
    return true;
  }
  return false;
}

bool IsAllowedTypeChange(const zetasql::Type* old_column_type,
                         const zetasql::Type* new_column_type) {
  if (old_column_type->Equals(new_column_type)) {
    return true;
  }

  if (old_column_type->IsArray() != new_column_type->IsArray()) {
    return false;
  }

  if (old_column_type->IsArray()) {
    return IsAllowedTypeChange(BaseType(old_column_type),
                               BaseType(new_column_type));
  }

  // Allow conversions from BYTES to STRING and STRING to BYTES.
  if ((new_column_type->IsString() && old_column_type->IsBytes()) ||
      (new_column_type->IsBytes() && old_column_type->IsString())) {
    return true;
  }

  // Allow conversions from PROTO to BYTES and BYTES to PROTO
  if ((new_column_type->IsProto() && old_column_type->IsBytes()) ||
      (new_column_type->IsBytes() && old_column_type->IsProto())) {
    return true;
  }

  // Allow conversion from PROTO to PROTO or enum to enum
  if ((new_column_type->IsProto() && old_column_type->IsProto()) ||
      (new_column_type->IsEnum() && old_column_type->IsEnum())) {
    return true;
  }

  // Allow conversion from enum to INT64 and INT64 to enum (this should ideally
  // be INT32 but since cloud spanner doesn't support INT32 columns we support
  // INT64 here)
  if ((new_column_type->IsInt64() && old_column_type->IsEnum()) ||
      (new_column_type->IsEnum() && old_column_type->IsInt64())) {
    return true;
  }

  return false;
}

// Validates size reductions and column type changes.
absl::Status CheckAllowedColumnTypeChange(
    const Column* old_column, const Column* new_column,
    const zetasql::Type* old_column_type,
    const zetasql::Type* new_column_type, SchemaValidationContext* context) {
  if (!IsAllowedTypeChange(old_column_type, new_column_type)) {
    return error::CannotChangeColumnType(new_column->Name(),
                                         ToString(old_column_type),
                                         ToString(new_column_type));
  }

  const auto* old_base_type = BaseType(old_column_type);
  const auto* new_base_type = BaseType(new_column_type);
  if (new_base_type->Equals(old_base_type)) {
    if (IsResizeable(old_base_type) && new_column->effective_max_length() <
                                           old_column->effective_max_length()) {
      context->AddAction([old_column,
                          new_column](const SchemaValidationContext* context) {
        return VerifyColumnLength(old_column->table(), old_column,
                                  new_column->effective_max_length(), context);
      });
    }
  } else {
    context->AddAction(
        [old_column, new_column](const SchemaValidationContext* context) {
          return VerifyColumnTypeChange(old_column->table(), old_column,
                                        new_column, context);
        });
    // After verifying that the type change is acceptable, run a backfill
    // to apply the type change to the column values in storage.
    context->AddAction(
        [old_column, new_column](const SchemaValidationContext* context) {
          return BackfillColumnValue(old_column, new_column, context);
        });
  }
  return absl::OkStatus();
}

absl::Status ValidateColumnSignatureChange(
    absl::string_view modify_action, absl::string_view dependency_name,
    const Column* dependent_column, const Table* dependent_table,
    const Schema* temp_new_schema, zetasql::TypeFactory* type_factory) {
  // Re-analyze the dependent view based on the new definition of the
  // dependency in the temporary new schema.
  if (!dependent_column->expression().has_value()) {
    return absl::OkStatus();
  }
  absl::flat_hash_set<const SchemaNode*> unused_new_deps;
  absl::flat_hash_set<const SchemaNode*> unused_udf_dependencies;
  absl::flat_hash_set<std::string> dependent_column_names;
  std::vector<zetasql::SimpleTable::NameAndType> name_and_types;
  for (const Column* column : dependent_table->columns()) {
    name_and_types.emplace_back(column->Name(), column->GetType());
  }

  auto status = AnalyzeColumnExpression(
      dependent_column->expression().value(), dependent_column->GetType(),
      dependent_table, temp_new_schema, type_factory, name_and_types,
      "check constraints", &dependent_column_names,
      /*dependent_sequences=*/nullptr,
      /*allow_volatile_expression=*/false, &unused_udf_dependencies);
  if (!status.ok()) {
    return error::DependentColumnBecomesInvalid(modify_action, dependency_name,
                                                dependent_column->Name(),
                                                status.message());
  }

  return absl::OkStatus();
}

}  // namespace

bool ColumnValidator::TypeExistsInProtoBundle(const zetasql::Type* type,
                                              const ProtoBundle* proto_bundle) {
  if (type->IsProto()) {
    const google::protobuf::Descriptor* type_descriptor = type->AsProto()->descriptor();
    absl::StatusOr<const google::protobuf::Descriptor*> descriptor =
        proto_bundle->GetTypeDescriptor(type_descriptor->full_name());
    return descriptor.ok();
  }
  if (type->IsEnum()) {
    const google::protobuf::EnumDescriptor* enum_descriptor =
        type->AsEnum()->enum_descriptor();
    absl::StatusOr<const google::protobuf::EnumDescriptor*> descriptor =
        proto_bundle->GetEnumTypeDescriptor(enum_descriptor->full_name());
    return descriptor.ok();
  }
  return false;
}

absl::Status ColumnValidator::ValidateTypeExistsInProtoBundle(
    const zetasql::Type* type, const ProtoBundle* proto_bundle,
    const std::string& column_name) {
  ZETASQL_RET_CHECK(proto_bundle != nullptr && (type->IsProto() || type->IsEnum()));

  return TypeExistsInProtoBundle(type, proto_bundle)
             ? absl::OkStatus()
             : error::DeletedTypeStillInUse(
                   type->TypeName(zetasql::PRODUCT_EXTERNAL,
                                  /*use_external_float32=*/true),
                   column_name);
}

absl::Status ColumnValidator::Validate(const Column* column,
                                       SchemaValidationContext* context) {
  ZETASQL_RET_CHECK_NE(column->table_, nullptr);
  ZETASQL_RET_CHECK(!column->name_.empty());
  ZETASQL_RET_CHECK(!column->id_.empty());
  ZETASQL_RET_CHECK(column->type_ != nullptr && IsSupportedColumnType(column->type_));
  const zetasql::Type* base_type = BaseType(column->type_);
  ZETASQL_RET_CHECK(!column->declared_max_length_.has_value() ||
            base_type->IsString() || base_type->IsBytes());

  if (column->name_.length() > limits::kMaxSchemaIdentifierLength) {
    return error::InvalidSchemaName("Column", column->Name());
  }

  if (column->source_column_) {
    ZETASQL_RET_CHECK(column->type_->Equals(column->source_column_->type_));
    ZETASQL_RET_CHECK(column->declared_max_length_ ==
              column->source_column_->declared_max_length_);
  }

  if (column->declared_max_length_.has_value()) {
    if (base_type->IsString() && (column->declared_max_length_.value() == 0 ||
                                  column->declared_max_length_.value() >
                                      limits::kMaxStringColumnLength)) {
      return error::InvalidColumnLength(column->FullName(),
                                        column->declared_max_length_.value(), 1,
                                        limits::kMaxStringColumnLength);
    }
    if (base_type->IsBytes() && (column->declared_max_length_.value() == 0 ||
                                 column->declared_max_length_.value() >
                                     limits::kMaxBytesColumnLength)) {
      return error::InvalidColumnLength(column->FullName(),
                                        column->declared_max_length_.value(), 1,
                                        limits::kMaxBytesColumnLength);
    }
  }

  if (column->has_vector_length()) {
    if (!column->type_->IsArray() ||
        (column->type_->IsArray() && !base_type->IsFloat() &&
         !base_type->IsDouble())) {
      return error::InvalidTypeForVectorLength(column->FullName());
    } else if (column->is_generated() || column->has_default_value()) {
      return error::VectorLengthOnGeneratedOrDefaultColumn(column->FullName());
    }
  }

  if (base_type->IsProto() || base_type->IsEnum()) {
    ZETASQL_RETURN_IF_ERROR(ValidateTypeExistsInProtoBundle(
        base_type, context->proto_bundle(), column->FullName()));
  }

  if (column->has_allows_commit_timestamp() && !column->type_->IsTimestamp()) {
    return error::UnallowedCommitTimestampOption(column->FullName());
  }

  if (column->has_default_value()) {
    if (column->allows_commit_timestamp()) {
      return error::CannotUseCommitTimestampWithColumnDefaultValue(
          column->Name());
    }
    if (context->is_postgresql_dialect()) {
      ZETASQL_RET_CHECK(column->postgresql_oid().has_value());
    } else {
      ZETASQL_RET_CHECK(!column->postgresql_oid().has_value());
    }
  }

  if (column->is_generated()) {
    if (context->is_postgresql_dialect()) {
      ZETASQL_RET_CHECK(column->postgresql_oid().has_value());
    } else {
      ZETASQL_RET_CHECK(!column->postgresql_oid().has_value());
    }
    if (!EmulatorFeatureFlags::instance().flags().enable_generated_pk &&
        column->table()->FindKeyColumn(column->Name())) {
      return error::CannotUseGeneratedColumnInPrimaryKey(
          column->table()->Name(), column->Name());
    }
    for (const Column* dep : column->dependent_columns()) {
      if (dep->allows_commit_timestamp()) {
        return error::CannotUseCommitTimestampOnGeneratedColumnDependency(
            dep->Name());
      }
    }
  }

  return absl::OkStatus();
}

absl::Status ColumnValidator::ValidateUpdate(const Column* column,
                                             const Column* old_column,
                                             SchemaValidationContext* context) {
  // if column has row deletion policy, then can't delete the column or change
  // type.
  bool has_row_deletion_policy =
      column->table()->row_deletion_policy().has_value() &&
      column->table()->row_deletion_policy()->column_name() == column->Name();
  if (has_row_deletion_policy && !column->table_->is_deleted() &&
      (column->is_deleted() || !column->GetType()->IsTimestamp())) {
    return error::RowDeletionPolicyWillBreak(column->Name(),
                                             column->table()->Name());
  }

  if (!column->change_streams_explicitly_tracking_column().empty() &&
      column->is_deleted()) {
    std::string change_stream_names;
    for (int i = 0;
         i < column->change_streams_explicitly_tracking_column().size(); ++i) {
      change_stream_names.append(
          column->change_streams_explicitly_tracking_column()[i]->Name());
    }
    return error::DropColumnWithChangeStream(
        column->table()->Name(), column->Name(),
        column->change_streams_explicitly_tracking_column().size(),
        change_stream_names);
  }
  if (column->is_deleted()) {
    return absl::OkStatus();
  }

  // Once set, column ID should never change.
  ZETASQL_RET_CHECK_EQ(column->id(), old_column->id());

  // For a non-deleted column, the objects it depends on should
  // also be alive.
  ZETASQL_RET_CHECK(!column->table_->is_deleted());
  // It is invalid to drop a column which is referenced by a generated column.
  for (const Column* dep : column->dependent_columns()) {
    if (dep->is_deleted()) {
      return error::InvalidDropColumnReferencedByGeneratedColumn(
          dep->Name(), column->table()->Name(), column->Name());
    }
  }
  if (column->is_generated() && !old_column->is_generated()) {
    return error::CannotConvertRegularColumnToGeneratedColumn(
        column->table()->Name(), column->Name());
  }
  if (!column->is_generated() && old_column->is_generated()) {
    return error::CannotConvertGeneratedColumnToRegularColumn(
        column->table()->Name(), column->Name());
  }
  if (column->is_generated() && old_column->is_generated()) {
    if (!column->GetType()->Equals(old_column->GetType())) {
      return error::CannotAlterStoredGeneratedColumnDataType(
          column->table()->Name(), column->Name());
    }
    if (column->expression().value() != old_column->expression().value()) {
      return error::CannotAlterGeneratedColumnExpression(
          column->table()->Name(), column->Name());
    }
    if (column->is_stored() != old_column->is_stored()) {
      return error::CannotAlterGeneratedColumnStoredAttribute(
          column->table()->Name(), column->Name());
    }
  }
  if (!column->GetType()->Equals(old_column->GetType())) {
    for (const Column* generated_column : column->table()->columns()) {
      if (generated_column->is_generated()) {
        for (const Column* dep : generated_column->dependent_columns()) {
          if (column == dep) {
            return error::
                CannotAlterColumnDataTypeWithDependentStoredGeneratedColumn(
                    column->Name());
          }
        }
      }
    }
  }

  if (column->source_column_) {
    // There is no valid scenario under which a source column drop should
    // trigger a cascading drop on referencing column.
    if (column->source_column_->is_deleted()) {
      ZETASQL_RET_CHECK_NE(column->table_->owner_index(), nullptr);
      return error::InvalidDropColumnWithDependency(
          column->name_, column->table_->owner_index()->indexed_table()->Name(),
          column->table_->owner_index()->Name());
    }
  }

  if (old_column->is_nullable_ && !column->is_nullable_) {
    context->AddAction([old_column](const SchemaValidationContext* context) {
      return VerifyColumnNotNull(old_column->table(), old_column, context);
    });
  }

  // Check for size reduction and type change.
  ZETASQL_RETURN_IF_ERROR(CheckAllowedColumnTypeChange(
      old_column, column, old_column->GetType(), column->type_, context));

  if (column->type_->IsTimestamp()) {
    if (column->allows_commit_timestamp() &&
        !old_column->allows_commit_timestamp()) {
      context->AddAction([column](const SchemaValidationContext* context) {
        return VerifyColumnCommitTimestamp(column->table_, column, context);
      });
    }
  }

  for (const SchemaNode* dependency : column->sequences_used()) {
    // Cannot drop a sequence if a column depends on it.
    if (dependency->is_deleted()) {
      const auto& dep_info = dependency->GetSchemaNameInfo();
      std::string dependency_type =
          (dep_info->global ? absl::AsciiStrToUpper(dep_info->kind)
                            : absl::AsciiStrToLower(dep_info->kind));
      return error::InvalidDropDependentColumn(dependency_type, dep_info->name,
                                               column->FullName());
    }
  }

  for (const SchemaNode* dependency : column->udf_dependencies()) {
    if (dependency->is_deleted()) {
      const auto& dep_info = dependency->GetSchemaNameInfo();
      std::string dependency_type =
          (dep_info->global ? absl::AsciiStrToUpper(dep_info->kind)
                            : absl::AsciiStrToLower(dep_info->kind));
      return error::InvalidDropDependentColumn(dependency_type, dep_info->name,
                                               column->FullName());
    }
  }

  if (context->is_postgresql_dialect()) {
    // Default and generated columns must have OIDs.
    if (old_column->is_generated() || old_column->has_default_value()) {
      ZETASQL_RET_CHECK(old_column->postgresql_oid().has_value());
    }
    if (column->is_generated() || column->has_default_value()) {
      ZETASQL_RET_CHECK(column->postgresql_oid().has_value());
    }
    // Alter statement may change the default value which would be assigned a
    // new OID so don't assert that the OIDs are the same.
  } else {
    ZETASQL_RET_CHECK(!old_column->postgresql_oid().has_value());
    ZETASQL_RET_CHECK(!column->postgresql_oid().has_value());
  }

  for (const SchemaNode* dep : column->udf_dependencies()) {
    // TODO When dropping support is added, a check should be added
    // to ensure that this column references a UDF that is also being dropped.
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
      ZETASQL_RETURN_IF_ERROR(ValidateColumnSignatureChange(
          modify_action, dependency_name, column, column->table(),
          context->tmp_new_schema(), context->type_factory()));
    }
  }

  return absl::OkStatus();
}

absl::Status KeyColumnValidator::Validate(const KeyColumn* key_column,
                                          SchemaValidationContext* context) {
  ZETASQL_RET_CHECK_NE(key_column->column_, nullptr);
  const std::string type_name =
      key_column->column_->GetType()->IsArray()
          ? "ARRAY"
          : key_column->column_->GetType()->ShortTypeName(
                zetasql::PRODUCT_EXTERNAL, /*use_external_float32=*/true);
  if (!IsSupportedKeyColumnType(key_column->column_->GetType())) {
    auto owner_index = key_column->column()->table()->owner_index();

    if (owner_index != nullptr && owner_index->is_search_index() &&
        key_column->column_->GetType()->IsTokenListType()) {
      return absl::OkStatus();
    }

    if (owner_index != nullptr) {
      return error::CannotCreateIndexOnColumn(
          key_column->column()->table()->owner_index()->Name(),
          key_column->column()->Name(), type_name);
    }
    return error::InvalidPrimaryKeyColumnType(key_column->column_->FullName(),
                                              type_name);
  }
  return absl::OkStatus();
}

absl::Status KeyColumnValidator::ValidateUpdate(
    const KeyColumn* key_column, const KeyColumn* old_key_column,
    SchemaValidationContext* context) {
  if (key_column->is_deleted()) {
    return absl::OkStatus();
  }

  const auto* column = key_column->column_;
  // If the underlying column of the key column has been altered,
  // reject the update if the column is also a parent key column,
  // unless it is a timestamp typed column and the update involves
  // chaging the allow timestamp option.
  if (context->IsModifiedNode(column)) {
    bool is_commit_timestamp_option_change =
        column->allows_commit_timestamp() !=
        old_key_column->column_->allows_commit_timestamp();
    if (!is_commit_timestamp_option_change) {
      // If the key column is a child table column.
      const auto* table_parent = column->table()->parent();
      if (table_parent != nullptr) {
        const auto* parent_column = table_parent->FindColumn(column->Name());
        if (parent_column != nullptr) {
          return error::AlteringParentColumn(column->FullName());
        }
      }

      // If the key column is a parent table column.
      for (const auto* child_table : column->table()->children()) {
        if (child_table->FindKeyColumn(column->Name())) {
          return error::CannotChangeKeyColumnWithChildTables(
              column->FullName());
        }
      }
    }
  }

  ZETASQL_RET_CHECK(!key_column->column_->is_deleted());
  ZETASQL_RET_CHECK_EQ(key_column->is_descending_, old_key_column->is_descending_);
  if (context->is_postgresql_dialect()) {
    ZETASQL_RET_CHECK_EQ(old_key_column->postgresql_oid().has_value(),
                 key_column->postgresql_oid().has_value());
    if (old_key_column->postgresql_oid().has_value() &&
        key_column->postgresql_oid().has_value()) {
      ZETASQL_RET_CHECK_EQ(old_key_column->postgresql_oid().value(),
                   key_column->postgresql_oid().value());
    }
  } else {
    ZETASQL_RET_CHECK(!old_key_column->postgresql_oid().has_value());
    ZETASQL_RET_CHECK(!key_column->postgresql_oid().has_value());
  }
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
