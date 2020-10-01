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

#include "zetasql/base/statusor.h"
#include "backend/schema/ddl/operations.pb.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

zetasql_base::StatusOr<const zetasql::Type*> DDLColumnTypeToGoogleSqlType(
    const ddl::ColumnType& ddl_type, zetasql::TypeFactory* type_factory) {
  ZETASQL_RET_CHECK(ddl_type.has_type())
      << "No type field specification in "
      << "ddl::ColumnType input: " << ddl_type.ShortDebugString();

  switch (ddl_type.type()) {
    case ddl::ColumnType::FLOAT64:
      return type_factory->get_double();
    case ddl::ColumnType::INT64:
      return type_factory->get_int64();
    case ddl::ColumnType::BOOL:
      return type_factory->get_bool();
    case ddl::ColumnType::STRING:
      return type_factory->get_string();
    case ddl::ColumnType::BYTES:
      return type_factory->get_bytes();
    case ddl::ColumnType::TIMESTAMP:
      return type_factory->get_timestamp();
    case ddl::ColumnType::DATE:
      return type_factory->get_date();
    case ddl::ColumnType::NUMERIC:
      return type_factory->get_numeric();
    case ddl::ColumnType::ARRAY: {
      ZETASQL_RET_CHECK(ddl_type.has_array_subtype())
          << "Missing array_subtype field for ddl::ColumnType input: "
          << ddl_type.ShortDebugString();
      if (ddl_type.array_subtype().type() == ddl::ColumnType::ARRAY) {
        // TODO : Update when we have a proper way to
        // construct user-facing error messages in the error catalog.
        return absl::Status(absl::StatusCode::kInvalidArgument,
                            "ARRAYs of ARRAY column types are not supported.");
      }
      ZETASQL_ASSIGN_OR_RETURN(
          auto array_element_type,
          DDLColumnTypeToGoogleSqlType(ddl_type.array_subtype(), type_factory));
      ZETASQL_RET_CHECK_NE(array_element_type, nullptr);
      const zetasql::Type* array_type;
      ZETASQL_RETURN_IF_ERROR(
          type_factory->MakeArrayType(array_element_type, &array_type));
      return array_type;
    }
    default:
      ZETASQL_RET_CHECK(false) << "Unrecognized ddl::ColumnType: "
                       << ddl_type.ShortDebugString();
  }
}

ddl::ColumnType GoogleSqlTypeToDDLColumnType(const zetasql::Type* type) {
  ddl::ColumnType ddl_type;
  if (type->IsArray()) {
    ddl_type.set_type(ddl::ColumnType::ARRAY);
    *ddl_type.mutable_array_subtype() =
        GoogleSqlTypeToDDLColumnType(type->AsArray()->element_type());
    return ddl_type;
  }

  ddl_type.set_type(ddl::ColumnType::UNKNOWN_TYPE);
  if (type->IsDouble()) ddl_type.set_type(ddl::ColumnType::FLOAT64);
  if (type->IsInt64()) ddl_type.set_type(ddl::ColumnType::INT64);
  if (type->IsBool()) ddl_type.set_type(ddl::ColumnType::BOOL);
  if (type->IsString()) ddl_type.set_type(ddl::ColumnType::STRING);
  if (type->IsBytes()) ddl_type.set_type(ddl::ColumnType::BYTES);
  if (type->IsTimestamp()) ddl_type.set_type(ddl::ColumnType::TIMESTAMP);
  if (type->IsDate()) ddl_type.set_type(ddl::ColumnType::DATE);
  if (type->IsNumericType()) ddl_type.set_type(ddl::ColumnType::NUMERIC);
  return ddl_type;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
