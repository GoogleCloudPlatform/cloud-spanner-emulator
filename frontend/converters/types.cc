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

#include <memory>
#include <string>
#include <vector>

#include "google/spanner/v1/type.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/enum_type.h"
#include "zetasql/public/types/proto_type.h"
#include "zetasql/public/types/struct_type.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "backend/schema/catalog/proto_bundle.h"
#include "common/errors.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_numeric_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_oid_type.h"
#include "third_party/spanner_pg/datatypes/extended/spanner_extended_type.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

using postgres_translator::spangres::datatypes::GetPgJsonbType;
using postgres_translator::spangres::datatypes::GetPgNumericType;
using postgres_translator::spangres::datatypes::GetPgOidType;

absl::Status TypeFromProto(
    const google::spanner::v1::Type& type_pb, zetasql::TypeFactory* factory,
    const zetasql::Type** type
    ,
    std::shared_ptr<const backend::ProtoBundle> proto_bundle
) {
  switch (type_pb.code()) {
    case google::spanner::v1::TypeCode::BOOL: {
      *type = factory->get_bool();
      return absl::OkStatus();
    }

    case google::spanner::v1::TypeCode::INT64: {
      if (type_pb.type_annotation() == v1::TypeAnnotationCode::PG_OID) {
        *type = GetPgOidType();
      } else {
        *type = factory->get_int64();
      }
      return absl::OkStatus();
    }

    case google::spanner::v1::TypeCode::TIMESTAMP: {
      *type = factory->get_timestamp();
      return absl::OkStatus();
    }
    case google::spanner::v1::TypeCode::FLOAT32: {
      *type = factory->get_float();
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
      if (type_pb.type_annotation() == v1::TypeAnnotationCode::PG_NUMERIC) {
        *type = GetPgNumericType();
      } else {
        *type = factory->get_numeric();
      }
      return absl::OkStatus();
    }

    case google::spanner::v1::TypeCode::JSON: {
      if (type_pb.type_annotation() == v1::TypeAnnotationCode::PG_JSONB) {
        *type = GetPgJsonbType();
      } else {
        *type = factory->get_json();
      }
      return absl::OkStatus();
    }

    case google::spanner::v1::TypeCode::ARRAY: {
      const zetasql::Type* element_type;
      if (!type_pb.has_array_element_type()) {
        return error::ArrayTypeMustSpecifyElementType(type_pb.DebugString());
      }
      ZETASQL_RETURN_IF_ERROR(TypeFromProto(type_pb.array_element_type(), factory,
                                    &element_type
                                    ,
                                    proto_bundle
                                    ))
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
                                      factory,
                                      &type
                                      ,
                                      proto_bundle
                                      ))
            << "\nWhen parsing field #" << i << " of " << type_pb.DebugString();
        zetasql::StructField field(type_pb.struct_type().fields(i).name(),
                                     type);
        fields.push_back(field);
      }
      ZETASQL_RETURN_IF_ERROR(factory->MakeStructTypeFromVector(fields, type))
          << "\nWhen parsing " << type_pb.DebugString();
      return absl::OkStatus();
    }
    case google::spanner::v1::TypeCode::PROTO: {
      ZETASQL_RET_CHECK_NE(proto_bundle, nullptr);
      std::string proto_type_fqn = type_pb.proto_type_fqn();
      ZETASQL_ASSIGN_OR_RETURN(auto descriptor,
                       proto_bundle->GetTypeDescriptor(proto_type_fqn));
      ZETASQL_RETURN_IF_ERROR(factory->MakeProtoType(descriptor, type));
      return absl::OkStatus();
    }

    case google::spanner::v1::TypeCode::ENUM: {
      ZETASQL_RET_CHECK_NE(proto_bundle, nullptr);
      std::string proto_type_fqn = type_pb.proto_type_fqn();
      ZETASQL_ASSIGN_OR_RETURN(auto descriptor,
                       proto_bundle->GetEnumTypeDescriptor(proto_type_fqn));
      ZETASQL_RETURN_IF_ERROR(factory->MakeEnumType(descriptor, type));
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

    case zetasql::TYPE_FLOAT: {
      type_pb->set_code(google::spanner::v1::TypeCode::FLOAT32);
      return absl::OkStatus();
    }

    case zetasql::TYPE_DOUBLE: {
      type_pb->set_code(google::spanner::v1::TypeCode::FLOAT64);
      return absl::OkStatus();
    }

    case zetasql::TYPE_EXTENDED: {
      auto type_code = static_cast<const postgres_translator::spangres::
                                       datatypes::SpannerExtendedType*>(type)
                           ->code();
      switch (type_code) {
        case v1::TypeAnnotationCode::PG_JSONB:
          type_pb->set_code(google::spanner::v1::TypeCode::JSON);
          type_pb->set_type_annotation(v1::TypeAnnotationCode::PG_JSONB);
          return absl::OkStatus();
        case v1::TypeAnnotationCode::PG_NUMERIC:
          type_pb->set_code(google::spanner::v1::TypeCode::NUMERIC);
          type_pb->set_type_annotation(v1::TypeAnnotationCode::PG_NUMERIC);
          return absl::OkStatus();
        case v1::TypeAnnotationCode::PG_OID:
          type_pb->set_code(google::spanner::v1::TypeCode::INT64);
          type_pb->set_type_annotation(v1::TypeAnnotationCode::PG_OID);
          return absl::OkStatus();
        default:
          return error::Internal(
              absl::StrCat("Unsupported ZetaSQL Extended type ",
                           type->DebugString(), " passed to TypeToProto"));
      }
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

    case zetasql::TYPE_JSON: {
      type_pb->set_code(google::spanner::v1::TypeCode::JSON);
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

    case zetasql::TYPE_PROTO: {
      const zetasql::ProtoType* proto_type = type->AsProto();
      type_pb->set_code(google::spanner::v1::TypeCode::PROTO);
      type_pb->set_proto_type_fqn(proto_type->descriptor()->full_name());
      return absl::OkStatus();
    }

    case zetasql::TYPE_ENUM: {
      const zetasql::EnumType* enum_type = type->AsEnum();
      type_pb->set_code(google::spanner::v1::TypeCode::ENUM);
      type_pb->set_proto_type_fqn(enum_type->enum_descriptor()->full_name());
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
