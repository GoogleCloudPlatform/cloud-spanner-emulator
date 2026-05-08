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

#include "backend/schema/validators/model_validator.h"

#include <vector>

#include "googlesql/public/types/struct_type.h"
#include "googlesql/public/types/type.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "backend/common/case.h"
#include "backend/schema/catalog/model.h"
#include "backend/schema/updater/global_schema_names.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "common/errors.h"
#include "common/limits.h"
#include "googlesql/base/ret_check.h"
#include "googlesql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {
absl::Status ValidateModelColumnTypeSupported(const Model* model,
                                              const Model::ModelColumn& column,
                                              const googlesql::Type* type) {
  switch (type->kind()) {
    case googlesql::TypeKind::TYPE_INT64:
    case googlesql::TypeKind::TYPE_BOOL:
    case googlesql::TypeKind::TYPE_DOUBLE:
    case googlesql::TypeKind::TYPE_STRING:
    case googlesql::TypeKind::TYPE_BYTES:
      return absl::OkStatus();

    case googlesql::TypeKind::TYPE_ARRAY: {
      const googlesql::Type* element_type = type->AsArray()->element_type();
      if (element_type->IsArray()) {
        break;
      }
      return ValidateModelColumnTypeSupported(model, column, element_type);
    }

    case googlesql::TypeKind::TYPE_STRUCT: {
      const std::vector<googlesql::StructField>& fields =
          type->AsStruct()->fields();
      if (fields.empty()) {
        return error::EmptyStruct();
      }
      if (fields.size() > limits::kMaxStructFieldCount) {
        return error::StructFieldNumberExceedsLimit(
            limits::kMaxStructFieldCount);
      }
      CaseInsensitiveStringSet unique_field_names;
      for (const googlesql::StructField& field : fields) {
        if (field.name.empty()) {
          return error::MissingStructFieldName(type->TypeName(
              googlesql::PRODUCT_EXTERNAL, /*use_external_float32=*/true));
        }

        if (auto i = unique_field_names.insert(field.name); !i.second) {
          if (field.name != *i.first) {
            return error::CaseInsensitiveDuplicateStructName(
                type->TypeName(googlesql::PRODUCT_EXTERNAL,
                               /*use_external_float32=*/true),
                field.name, *i.first);
          }
          return error::DuplicateStructName(
              type->TypeName(googlesql::PRODUCT_EXTERNAL,
                             /*use_external_float32=*/true),
              field.name);
        }

        GOOGLESQL_RETURN_IF_ERROR(
            ValidateModelColumnTypeSupported(model, column, field.type));
      }
      return absl::OkStatus();
    }

    default:
      break;
  }

  return error::ModelColumnTypeUnsupported(
      model->Name(), column.name,
      column.type->TypeName(googlesql::PRODUCT_EXTERNAL,
                            /*use_external_float32=*/true));
}

absl::Status ValidateModelColumn(const Model* model,
                                 const Model::ModelColumn& column) {
  GOOGLESQL_RET_CHECK(!column.name.empty());
  GOOGLESQL_RET_CHECK(column.type != nullptr);
  GOOGLESQL_RETURN_IF_ERROR(ValidateModelColumnTypeSupported(model, column, column.type));
  return absl::OkStatus();
}

}  // namespace

absl::Status ModelValidator::Validate(const Model* model,
                                      SchemaValidationContext* context) {
  GOOGLESQL_RET_CHECK(!model->name_.empty());
  GOOGLESQL_RETURN_IF_ERROR(GlobalSchemaNames::ValidateSchemaName("Model", model->name_));

  if (!model->is_remote()) {
    return error::LocalModelUnsupported(model->Name());
  }

  if (model->input_.empty()) {
    return error::NoColumnsModel(model->Name(), "Input");
  }
  if (model->input_.size() > limits::kMaxColumnsPerTable) {
    return error::TooManyModelColumns(model->Name(), "Inputs",
                                      limits::kMaxColumnsPerTable);
  }
  CaseInsensitiveStringSet unique_column_names;
  for (const Model::ModelColumn& column : model->input()) {
    GOOGLESQL_RETURN_IF_ERROR(ValidateModelColumn(model, column));
    if (auto i = unique_column_names.insert(column.name); !i.second) {
      if (column.name != *i.first) {
        return error::ModelCaseInsensitiveDuplicateColumn(
            absl::StrCat(model->Name(), ".", column.name),
            absl::StrCat(model->Name(), ".", *i.first));
      }

      return error::ModelDuplicateColumn(
          absl::StrCat(model->Name(), ".", column.name));
    }
  }

  if (model->output_.empty()) {
    return error::NoColumnsModel(model->Name(), "Output");
  }
  if (model->output_.size() > limits::kMaxColumnsPerTable) {
    return error::TooManyModelColumns(model->Name(), "Outputs",
                                      limits::kMaxColumnsPerTable);
  }
  for (const Model::ModelColumn& column : model->output_) {
    GOOGLESQL_RETURN_IF_ERROR(ValidateModelColumn(model, column));
    if (auto i = unique_column_names.insert(column.name); !i.second) {
      if (column.name != *i.first) {
        return error::ModelCaseInsensitiveDuplicateColumn(
            absl::StrCat(model->Name(), ".", column.name),
            absl::StrCat(model->Name(), ".", *i.first));
      }

      return error::ModelDuplicateColumn(
          absl::StrCat(model->Name(), ".", column.name));
    }
  }
  if (!model->endpoint_.has_value() && model->endpoints_.empty()) {
    return error::NoModelEndpoint(model->Name());
  }
  if (model->endpoint_.has_value() && !model->endpoints_.empty()) {
    return error::AmbiguousModelEndpoint(model->Name());
  }
  if (model->default_batch_size_.has_value()) {
    if (*model->default_batch_size_ <= 0 ||
        *model->default_batch_size_ > limits::kMaxModelDefaultBatchSize) {
      return error::InvalidModelDefaultBatchSize(
          model->Name(), *model->default_batch_size_,
          limits::kMaxModelDefaultBatchSize);
    }
  }
  return absl::OkStatus();
}

absl::Status ModelValidator::ValidateUpdate(const Model* model,
                                            const Model* old_model,
                                            SchemaValidationContext* context) {
  if (model->is_deleted()) {
    context->global_names()->RemoveName(model->Name());
    return absl::OkStatus();
  }
  GOOGLESQL_RET_CHECK_EQ(model->Name(), old_model->Name());
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
