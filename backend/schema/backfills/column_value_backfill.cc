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

#include "backend/schema/backfills/column_value_backfill.h"

#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "zetasql/base/statusor.h"
#include "backend/actions/generated_column.h"
#include "backend/datamodel/types.h"
#include "backend/query/catalog.h"
#include "backend/query/function_catalog.h"
#include "backend/schema/catalog/table.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

zetasql_base::StatusOr<zetasql::Value> RewriteColumnValue(
    const zetasql::Type* old_column_type,
    const zetasql::Type* new_column_type, const zetasql::Value& value) {
  ZETASQL_RET_CHECK(old_column_type != nullptr && new_column_type != nullptr);

  if (!value.is_valid()) {
    return value;
  }

  if (value.is_null()) {
    return zetasql::Value::Null(new_column_type);
  }

  if (old_column_type->IsArray()) {
    ZETASQL_RET_CHECK(new_column_type->IsArray());
    const auto* old_elem_type = BaseType(old_column_type);
    const auto* new_elem_type = BaseType(new_column_type);
    std::vector<zetasql::Value> array_elements;
    array_elements.reserve(value.elements().size());
    for (const auto& element : value.elements()) {
      ZETASQL_ASSIGN_OR_RETURN(
          auto new_element,
          RewriteColumnValue(old_elem_type, new_elem_type, element));
      array_elements.push_back(new_element);
    }
    return zetasql::Value::Array(new_column_type->AsArray(), array_elements);
  }

  if (old_column_type->IsString() && new_column_type->IsBytes()) {
    return zetasql::Value::Bytes(value.string_value());
  }

  ZETASQL_RET_CHECK(old_column_type->IsBytes() && new_column_type->IsString());
  return zetasql::Value::String(value.bytes_value());
}

}  // namespace

absl::Status BackfillColumnValue(const Column* old_column,
                                 const Column* new_column,
                                 const SchemaValidationContext* context) {
  ZETASQL_RET_CHECK_EQ(old_column->id(), new_column->id());
  auto column_id = old_column->id();
  const Table* table = old_column->table();

  std::unique_ptr<StorageIterator> itr;
  ZETASQL_RETURN_IF_ERROR(context->storage()->Read(context->pending_commit_timestamp(),
                                           table->id(), KeyRange::All(),
                                           {column_id}, &itr));

  while (itr->Next()) {
    std::vector<zetasql::Value> row_values;
    ZETASQL_RET_CHECK_EQ(itr->NumColumns(), 1);
    const zetasql::Value& orig_value = itr->ColumnValue(0);
    ZETASQL_ASSIGN_OR_RETURN(const auto new_column_value,
                     RewriteColumnValue(old_column->GetType(),
                                        new_column->GetType(), orig_value));
    ZETASQL_RETURN_IF_ERROR(context->storage()->Write(
        context->pending_commit_timestamp(), table->id(), itr->Key(),
        {column_id}, {new_column_value}));
  }

  return absl::OkStatus();
}

absl::Status BackfillGeneratedColumnValue(
    const Column* generated_column, const SchemaValidationContext* context) {
  ZETASQL_RET_CHECK(generated_column != nullptr && generated_column->is_generated());
  ZETASQL_RET_CHECK_NE(context, nullptr);
  FunctionCatalog function_catalog(context->type_factory());
  Catalog catalog(context->new_schema(), &function_catalog);
  const Table* table = generated_column->table();
  GeneratedColumnEffector effector(table, &catalog);

  std::vector<ColumnID> column_ids = GetColumnIDs(table->columns());
  std::unique_ptr<StorageIterator> itr;
  ZETASQL_RETURN_IF_ERROR(context->storage()->Read(context->pending_commit_timestamp(),
                                           table->id(), KeyRange::All(),
                                           column_ids, &itr));
  while (itr->Next()) {
    zetasql::ParameterValueMap row_column_values;
    for (int i = 0; i < itr->NumColumns(); ++i) {
      // Storage returns invalid values if a value is not present, in which case
      // we convert it into a typed NULL.
      row_column_values[table->columns()[i]->Name()] =
          itr->ColumnValue(i).is_valid()
              ? itr->ColumnValue(i)
              : zetasql::Value::Null(table->columns()[i]->GetType());
    }

    ZETASQL_ASSIGN_OR_RETURN(zetasql::Value value,
                     effector.ComputeGeneratedColumnValue(generated_column,
                                                          row_column_values));

    ZETASQL_RETURN_IF_ERROR(context->storage()->Write(
        context->pending_commit_timestamp(), table->id(), itr->Key(),
        {generated_column->id()}, {value}));
  }

  return itr->Status();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
