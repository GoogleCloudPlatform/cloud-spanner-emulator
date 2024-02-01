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

#include "backend/schema/updater/ddl_type_conversion.h"

#include <memory>
#include <vector>

#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/schema/ddl/operations.pb.h"
#include "third_party/spanner_pg/catalog/spangres_type.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

absl::StatusOr<const zetasql::Type*> DDLColumnTypeToGoogleSqlType(
    const ddl::TypeDefinition& ddl_type_def,
    zetasql::TypeFactory* type_factory
) {
  ZETASQL_RET_CHECK(ddl_type_def.has_type())
      << "No type field specification in "
      << "ddl::TypeDefinition input: " << ddl_type_def.ShortDebugString();

  switch (ddl_type_def.type()) {
    case ddl::TypeDefinition::DOUBLE:
      return type_factory->get_double();
    case ddl::TypeDefinition::INT64:
      return type_factory->get_int64();
    case ddl::TypeDefinition::BOOL:
      return type_factory->get_bool();
    case ddl::TypeDefinition::STRING:
      return type_factory->get_string();
    case ddl::TypeDefinition::BYTES:
      return type_factory->get_bytes();
    case ddl::TypeDefinition::TIMESTAMP:
      return type_factory->get_timestamp();
    case ddl::TypeDefinition::DATE:
      return type_factory->get_date();
    case ddl::TypeDefinition::NUMERIC:
      return type_factory->get_numeric();
    case ddl::TypeDefinition::PG_NUMERIC:
      return postgres_translator::spangres::types::PgNumericMapping()
          ->mapped_type();
    case ddl::TypeDefinition::JSON:
      return type_factory->get_json();
    case ddl::TypeDefinition::PG_JSONB:
      return postgres_translator::spangres::types::PgJsonbMapping()
          ->mapped_type();
    case ddl::TypeDefinition::ARRAY: {
      ZETASQL_RET_CHECK(ddl_type_def.has_array_subtype())
          << "Missing array_subtype field for ddl::TypeDefinition input: "
          << ddl_type_def.ShortDebugString();
      if (ddl_type_def.array_subtype().type() == ddl::TypeDefinition::ARRAY) {
        // TODO : Update when we have a proper way to
        // construct user-facing error messages in the error catalog.
        return absl::Status(
            absl::StatusCode::kInvalidArgument,
            "Array of arrays type is not supported by the schema.");
      }
      ZETASQL_ASSIGN_OR_RETURN(
          auto array_element_type,
          DDLColumnTypeToGoogleSqlType(ddl_type_def.array_subtype(),
                                       type_factory
                                       ));
      ZETASQL_RET_CHECK_NE(array_element_type, nullptr);
      const zetasql::Type* array_type;
      ZETASQL_RETURN_IF_ERROR(
          type_factory->MakeArrayType(array_element_type, &array_type));
      return array_type;
    }
    case ddl::TypeDefinition::STRUCT: {
      std::vector<zetasql::StructType::StructField> struct_fields;
      for (const ddl::TypeDefinition::StructDescriptor::Field& field :
           ddl_type_def.struct_descriptor().field()) {
        ZETASQL_ASSIGN_OR_RETURN(
            const zetasql::Type* type,
            DDLColumnTypeToGoogleSqlType(field.type(),
                                         type_factory
                                         ));
        struct_fields.push_back({field.name(), type});
      }
      const zetasql::Type* struct_type;
      ZETASQL_RETURN_IF_ERROR(
          type_factory->MakeStructType(struct_fields, &struct_type));
      return struct_type;
    }
    default:
      ZETASQL_RET_CHECK_FAIL() << "Unrecognized ddl::TypeDefinition: "
                       << ddl_type_def.ShortDebugString();
  }
}

absl::StatusOr<const zetasql::Type*> DDLColumnTypeToGoogleSqlType(
    const ddl::ColumnDefinition& ddl_column_def,
    zetasql::TypeFactory* type_factory
) {
  ZETASQL_RET_CHECK(ddl_column_def.has_type())
      << "No type field specification in "
      << "ddl::ColumnDefinition input: " << ddl_column_def.ShortDebugString();

  switch (ddl_column_def.type()) {
    case ddl::ColumnDefinition::DOUBLE:
      return type_factory->get_double();
    case ddl::ColumnDefinition::INT64:
      return type_factory->get_int64();
    case ddl::ColumnDefinition::BOOL:
      return type_factory->get_bool();
    case ddl::ColumnDefinition::STRING:
      return type_factory->get_string();
    case ddl::ColumnDefinition::BYTES:
      return type_factory->get_bytes();
    case ddl::ColumnDefinition::TIMESTAMP:
      return type_factory->get_timestamp();
    case ddl::ColumnDefinition::DATE:
      return type_factory->get_date();
    case ddl::ColumnDefinition::NUMERIC:
      return type_factory->get_numeric();
    case ddl::ColumnDefinition::PG_NUMERIC:
      return postgres_translator::spangres::types::PgNumericMapping()
          ->mapped_type();
    case ddl::ColumnDefinition::JSON:
      return type_factory->get_json();
    case ddl::ColumnDefinition::PG_JSONB:
      return postgres_translator::spangres::types::PgJsonbMapping()
          ->mapped_type();
    case ddl::ColumnDefinition::ARRAY: {
      ZETASQL_RET_CHECK(ddl_column_def.has_array_subtype())
          << "Missing array_subtype field for ddl::ColumnDefinition input: "
          << ddl_column_def.ShortDebugString();
      if (ddl_column_def.array_subtype().type() ==
          ddl::ColumnDefinition::ARRAY) {
        // TODO : Update when we have a proper way to
        // construct user-facing error messages in the error catalog.
        return absl::Status(
            absl::StatusCode::kInvalidArgument,
            "Array of arrays type is not supported by the schema.");
      }
      ZETASQL_ASSIGN_OR_RETURN(
          auto array_element_type,
          DDLColumnTypeToGoogleSqlType(ddl_column_def.array_subtype(),
                                       type_factory
                                       ));
      ZETASQL_RET_CHECK_NE(array_element_type, nullptr);
      const zetasql::Type* array_type;
      ZETASQL_RETURN_IF_ERROR(
          type_factory->MakeArrayType(array_element_type, &array_type));
      return array_type;
    }
    case ddl::ColumnDefinition::STRUCT: {
      return DDLColumnTypeToGoogleSqlType(ddl_column_def.type_definition(),
                                          type_factory
      );
    }
    default:
      ZETASQL_RET_CHECK_FAIL() << "Unrecognized ddl::ColumnDefinition: "
                       << ddl_column_def.ShortDebugString();
  }
}

ddl::TypeDefinition GoogleSqlTypeToDDLType(const zetasql::Type* type) {
  ddl::TypeDefinition type_def;
  if (type->IsArray()) {
    type_def.set_type(ddl::TypeDefinition::ARRAY);
    *type_def.mutable_array_subtype() =
        GoogleSqlTypeToDDLType(type->AsArray()->element_type());
    return type_def;
  }
  if (type->IsStruct()) {
    type_def.set_type(ddl::TypeDefinition::STRUCT);
    for (const zetasql::StructField& field : type->AsStruct()->fields()) {
      ddl::TypeDefinition::StructDescriptor::Field* ddl_field =
          type_def.mutable_struct_descriptor()->add_field();

      ddl_field->set_name(field.name);
      *ddl_field->mutable_type() = GoogleSqlTypeToDDLType(field.type);
    }
    return type_def;
  }

  type_def.set_type(ddl::TypeDefinition::NONE);
  if (type->IsDouble()) type_def.set_type(ddl::TypeDefinition::DOUBLE);
  if (type->IsInt64()) type_def.set_type(ddl::TypeDefinition::INT64);
  if (type->IsBool()) type_def.set_type(ddl::TypeDefinition::BOOL);
  if (type->IsString()) type_def.set_type(ddl::TypeDefinition::STRING);
  if (type->IsBytes()) type_def.set_type(ddl::TypeDefinition::BYTES);
  if (type->IsTimestamp()) type_def.set_type(ddl::TypeDefinition::TIMESTAMP);
  if (type->IsDate()) type_def.set_type(ddl::TypeDefinition::DATE);
  if (type->IsNumericType()) type_def.set_type(ddl::TypeDefinition::NUMERIC);
  if (type->IsJson()) type_def.set_type(ddl::TypeDefinition::JSON);
  if (type->IsExtendedType()) {
    if (type->Equals(postgres_translator::spangres::types::PgNumericMapping()
                         ->mapped_type())) {
      type_def.set_type(ddl::TypeDefinition::PG_NUMERIC);
    } else if (type->Equals(
                   postgres_translator::spangres::types::PgJsonbMapping()
                       ->mapped_type())) {
      type_def.set_type(ddl::TypeDefinition::PG_JSONB);
    }
  }
  return type_def;
}

ddl::ColumnDefinition GoogleSqlTypeToDDLColumnType(
    const zetasql::Type* type) {
  ddl::ColumnDefinition ddl_column_def;
  if (type->IsArray()) {
    ddl_column_def.set_type(ddl::ColumnDefinition::ARRAY);
    *ddl_column_def.mutable_array_subtype() =
        GoogleSqlTypeToDDLColumnType(type->AsArray()->element_type());
    return ddl_column_def;
  }
  if (type->IsStruct()) {
    ddl_column_def.set_type(ddl::ColumnDefinition::STRUCT);
    *ddl_column_def.mutable_type_definition() = GoogleSqlTypeToDDLType(type);
    return ddl_column_def;
  }

  ddl_column_def.set_type(ddl::ColumnDefinition::NONE);
  if (type->IsDouble()) ddl_column_def.set_type(ddl::ColumnDefinition::DOUBLE);
  if (type->IsFloat()) ddl_column_def.set_type(ddl::ColumnDefinition::FLOAT);
  if (type->IsInt64()) ddl_column_def.set_type(ddl::ColumnDefinition::INT64);
  if (type->IsBool()) ddl_column_def.set_type(ddl::ColumnDefinition::BOOL);
  if (type->IsString()) ddl_column_def.set_type(ddl::ColumnDefinition::STRING);
  if (type->IsBytes()) ddl_column_def.set_type(ddl::ColumnDefinition::BYTES);
  if (type->IsTimestamp())
    ddl_column_def.set_type(ddl::ColumnDefinition::TIMESTAMP);
  if (type->IsDate()) ddl_column_def.set_type(ddl::ColumnDefinition::DATE);
  if (type->IsNumericType())
    ddl_column_def.set_type(ddl::ColumnDefinition::NUMERIC);
  if (type->IsJson()) ddl_column_def.set_type(ddl::ColumnDefinition::JSON);
  if (type->IsExtendedType()) {
    if (type->Equals(postgres_translator::spangres::types::PgNumericMapping()
                         ->mapped_type())) {
      ddl_column_def.set_type(ddl::ColumnDefinition::PG_NUMERIC);
    } else if (type->Equals(
                   postgres_translator::spangres::types::PgJsonbMapping()
                       ->mapped_type())) {
      ddl_column_def.set_type(ddl::ColumnDefinition::PG_JSONB);
    }
  }
  return ddl_column_def;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
