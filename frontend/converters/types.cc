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

#include "frontend/converters/types.h"

#include "absl/strings/str_cat.h"
#include "common/errors.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

absl::Status TypeFromProto(const google::spanner::v1::Type& type_pb,
                           zetasql::TypeFactory* factory,
                           const zetasql::Type** type) {
  switch (type_pb.code()) {
    case google::spanner::v1::TypeCode::BOOL: {
      *type = factory->get_bool();
      return absl::OkStatus();
    }

    case google::spanner::v1::TypeCode::INT64: {
      *type = factory->get_int64();
      return absl::OkStatus();
    }

    case google::spanner::v1::TypeCode::TIMESTAMP: {
      *type = factory->get_timestamp();
      return absl::OkStatus();
    }

    case google::spanner::v1::TypeCode::FLOAT64: {
      *type = factory->get_double();
      return absl::OkStatus();
    }

    case google::spanner::v1::TypeCode::DATE: {
      *type = factory->get_date();
      return absl::OkStatus();
    }

    case google::spanner::v1::TypeCode::STRING: {
      *type = factory->get_string();
      return absl::OkStatus();
    }

    case google::spanner::v1::TypeCode::BYTES: {
      *type = factory->get_bytes();
      return absl::OkStatus();
    }

    case google::spanner::v1::TypeCode::NUMERIC: {
      *type = factory->get_numeric();
      return absl::OkStatus();
    }

    case google::spanner::v1::TypeCode::ARRAY: {
      const zetasql::Type* element_type;
      if (!type_pb.has_array_element_type()) {
        return error::ArrayTypeMustSpecifyElementType(type_pb.DebugString());
      }
      ZETASQL_RETURN_IF_ERROR(
          TypeFromProto(type_pb.array_element_type(), factory, &element_type))
          << "\nWhen parsing array element type of " << type_pb.DebugString();
      ZETASQL_RETURN_IF_ERROR(factory->MakeArrayType(element_type, type))
          << "\nWhen parsing " << type_pb.DebugString();
      return absl::OkStatus();
    }

    case google::spanner::v1::TypeCode::STRUCT: {
      std::vector<zetasql::StructField> fields;
      for (int i = 0; i < type_pb.struct_type().fields_size(); ++i) {
        const zetasql::Type* type;
        ZETASQL_RETURN_IF_ERROR(TypeFromProto(type_pb.struct_type().fields(i).type(),
                                      factory, &type))
            << "\nWhen parsing field #" << i << " of " << type_pb.DebugString();
        zetasql::StructField field(type_pb.struct_type().fields(i).name(),
                                     type);
        fields.push_back(field);
      }
      ZETASQL_RETURN_IF_ERROR(factory->MakeStructTypeFromVector(fields, type))
          << "\nWhen parsing " << type_pb.DebugString();
      return absl::OkStatus();
    }

    default:
      return error::UnspecifiedType(type_pb.DebugString());
  }
}

absl::Status TypeToProto(const zetasql::Type* type,
                         google::spanner::v1::Type* type_pb) {
  switch (type->kind()) {
    case zetasql::TYPE_BOOL: {
      type_pb->set_code(google::spanner::v1::TypeCode::BOOL);
      return absl::OkStatus();
    }

    case zetasql::TYPE_INT64: {
      type_pb->set_code(google::spanner::v1::TypeCode::INT64);
      return absl::OkStatus();
    }

    case zetasql::TYPE_DOUBLE: {
      type_pb->set_code(google::spanner::v1::TypeCode::FLOAT64);
      return absl::OkStatus();
    }

    case zetasql::TYPE_TIMESTAMP: {
      type_pb->set_code(google::spanner::v1::TypeCode::TIMESTAMP);
      return absl::OkStatus();
    }

    case zetasql::TYPE_DATE: {
      type_pb->set_code(google::spanner::v1::TypeCode::DATE);
      return absl::OkStatus();
    }

    case zetasql::TYPE_STRING: {
      type_pb->set_code(google::spanner::v1::TypeCode::STRING);
      return absl::OkStatus();
    }

    case zetasql::TYPE_BYTES: {
      type_pb->set_code(google::spanner::v1::TypeCode::BYTES);
      return absl::OkStatus();
    }

    case zetasql::TYPE_NUMERIC: {
      type_pb->set_code(google::spanner::v1::TypeCode::NUMERIC);
      return absl::OkStatus();
    }

    case zetasql::TYPE_ARRAY: {
      type_pb->set_code(google::spanner::v1::TypeCode::ARRAY);
      ZETASQL_RETURN_IF_ERROR(TypeToProto(type->AsArray()->element_type(),
                                  type_pb->mutable_array_element_type()))
          << "\nWhen converting array element type of " << type->DebugString()
          << " to proto";
      return absl::OkStatus();
    }

    case zetasql::TYPE_STRUCT: {
      type_pb->set_code(google::spanner::v1::TypeCode::STRUCT);
      const zetasql::StructType* struct_type = type->AsStruct();
      auto struct_pb_type = type_pb->mutable_struct_type();
      for (int i = 0; i < struct_type->num_fields(); ++i) {
        auto field = struct_pb_type->add_fields();
        field->set_name(struct_type->field(i).name);
        ZETASQL_RETURN_IF_ERROR(
            TypeToProto(struct_type->field(i).type, field->mutable_type()))
            << "\nWhen converting field #" << i << " of " << type->DebugString()
            << " to proto";
      }
      return absl::OkStatus();
    }

    default:
      return error::Internal(absl::StrCat("Unsupported ZetaSQL type ",
                                          type->DebugString(),
                                          " passed to TypeToProto"));
  }
}

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
