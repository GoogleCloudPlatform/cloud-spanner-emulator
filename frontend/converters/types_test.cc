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
#include <utility>
#include <vector>

#include "google/spanner/v1/type.pb.h"
#include "google/protobuf/descriptor.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/struct_type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "backend/schema/catalog/proto_bundle.h"
#include "tests/common/proto_matchers.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_numeric_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_oid_type.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {

using zetasql::StructType;
using zetasql::types::BoolType;
using zetasql::types::BytesType;
using zetasql::types::DateType;
using zetasql::types::DoubleType;
using zetasql::types::EmptyStructType;
using zetasql::types::FloatType;
using zetasql::types::Int64ArrayType;
using zetasql::types::Int64Type;
using zetasql::types::JsonArrayType;
using zetasql::types::JsonType;
using zetasql::types::NumericArrayType;
using zetasql::types::NumericType;
using zetasql::types::StringType;
using zetasql::types::TimestampType;
using postgres_translator::spangres::datatypes::GetPgJsonbArrayType;
using postgres_translator::spangres::datatypes::GetPgJsonbType;
using postgres_translator::spangres::datatypes::GetPgNumericArrayType;
using postgres_translator::spangres::datatypes::GetPgNumericType;
using postgres_translator::spangres::datatypes::GetPgOidArrayType;
using postgres_translator::spangres::datatypes::GetPgOidType;

class TypeProtos : public ::testing::Test {
 public:
  TypeProtos() = default;

  std::string GenerateProtoDescriptorBytesAsString() {
    const google::protobuf::FileDescriptorProto file_descriptor = PARSE_TEXT_PROTO(R"pb(
      syntax: "proto2"
      name: "0"
      package: "customer.app"
      message_type { name: "User" }
      enum_type {
        name: "State"
        value { name: "UNSPECIFIED" number: 0 }
      }
    )pb");
    google::protobuf::FileDescriptorSet file_descriptor_set;
    *file_descriptor_set.add_file() = file_descriptor;
    return file_descriptor_set.SerializeAsString();
  }

  // Sets up the proto bundle and populates FileDescriptor bytes.
  absl::StatusOr<std::shared_ptr<const backend::ProtoBundle>>
  SetUpProtoBundle() {
    auto insert_proto_types = std::vector<std::string>{};
    insert_proto_types.push_back("customer.app.User");
    insert_proto_types.push_back("customer.app.State");
    ZETASQL_ASSIGN_OR_RETURN(auto builder, backend::ProtoBundle::Builder::New(
                                       GenerateProtoDescriptorBytesAsString()));
    ZETASQL_RETURN_IF_ERROR(builder->InsertTypes(insert_proto_types));
    ZETASQL_ASSIGN_OR_RETURN(auto proto_bundle, builder->Build());
    return proto_bundle;
  }
};

TEST_F(TypeProtos, ConvertsBasicTypesBetweenTypesAndProtos) {
  zetasql::TypeFactory factory;
  const zetasql::Type* str_int_pair_type;
  ZETASQL_ASSERT_OK(factory.MakeStructType(
      {StructType::StructField("str", factory.get_string()),
       StructType::StructField("int", factory.get_int64())},
      &str_int_pair_type));

  const zetasql::Type* array_struct_type;
  ZETASQL_ASSERT_OK(factory.MakeArrayType(str_int_pair_type, &array_struct_type));

  const zetasql::Type* array_empty_struct_type;
  ZETASQL_ASSERT_OK(factory.MakeArrayType(EmptyStructType(), &array_empty_struct_type));


  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle, SetUpProtoBundle());
  const zetasql::Type* proto_type;
  std::string proto_type_fqn = "customer.app.User";
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto descriptor,
                       proto_bundle->GetTypeDescriptor(proto_type_fqn));
  ZETASQL_ASSERT_OK(factory.MakeProtoType(descriptor, &proto_type));

  const zetasql::Type* enum_type;
  std::string proto_enum_type_fqn = "customer.app.State";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto enum_descriptor,
      proto_bundle->GetEnumTypeDescriptor(proto_enum_type_fqn));
  ZETASQL_ASSERT_OK(factory.MakeEnumType(enum_descriptor, &enum_type));

  const zetasql::Type* array_proto_type;
  ZETASQL_ASSERT_OK(factory.MakeArrayType(proto_type, &array_proto_type));

  const zetasql::Type* array_enum_type;
  ZETASQL_ASSERT_OK(factory.MakeArrayType(enum_type, &array_enum_type));

  std::vector<std::pair<const zetasql::Type*, std::string>> test_cases{
      {BoolType(), "code: BOOL"},
      {Int64Type(), "code: INT64"},
      {GetPgOidType(), "code: INT64 type_annotation: PG_OID"},
      {FloatType(), "code: FLOAT32"},
      {DoubleType(), "code: FLOAT64"},
      {TimestampType(), "code: TIMESTAMP"},
      {NumericType(), "code: NUMERIC"},
      {GetPgNumericType(), "code: NUMERIC type_annotation: PG_NUMERIC"},
      {JsonType(), "code: JSON"},
      {GetPgJsonbType(), "code: JSON type_annotation: PG_JSONB"},
      {DateType(), "code: DATE"},
      {StringType(), "code: STRING"},
      {BytesType(), "code: BYTES"},
      {Int64ArrayType(), "code: ARRAY array_element_type { code: INT64 }"},
      {GetPgOidArrayType(),
       R"(code: ARRAY
          array_element_type { code: INT64 type_annotation: PG_OID })"},
      {NumericArrayType(), "code: ARRAY array_element_type { code: NUMERIC }"},
      {GetPgNumericArrayType(),
       R"(code: ARRAY
          array_element_type { code: NUMERIC type_annotation: PG_NUMERIC })"},
      {JsonArrayType(), "code: ARRAY array_element_type { code: JSON }"},
      {GetPgJsonbArrayType(),
       R"(code: ARRAY
          array_element_type { code: JSON type_annotation: PG_JSONB })"},
      {EmptyStructType(), "code: STRUCT struct_type: { }"},
      {str_int_pair_type,
       R"(code: STRUCT
          struct_type: {
            fields: [
              {
                name: 'str'
                type: { code: STRING }
              }, {
                name: 'int'
                type: { code: INT64 }
              }
            ]
          })"},
      {array_struct_type,
       R"(
        code: ARRAY
        array_element_type {
          code: STRUCT
          struct_type: {
            fields: [
              {
                name: 'str'
                type: { code: STRING }
              }, {
                name: 'int'
                type: { code: INT64 }
              }
            ]
          }
        })"},
      {array_empty_struct_type,
       R"(
        code: ARRAY
        array_element_type {
          code: STRUCT struct_type: { }
        }
        )"},
      {proto_type,
       R"(
        code: PROTO
        proto_type_fqn : "customer.app.User"
        )"},
      {enum_type,
       R"(
        code: ENUM
        proto_type_fqn : "customer.app.State"
        )"},
      {array_proto_type,
       R"(
        code: ARRAY
        array_element_type {
          code: PROTO
          proto_type_fqn: "customer.app.User"
        }
        )"},
      {array_enum_type,
       R"(
        code: ARRAY
        array_element_type {
          code: ENUM
          proto_type_fqn: "customer.app.State"
        }
        )"},
  };
  for (const auto& entry : test_cases) {
    const zetasql::Type* expected_type = entry.first;
    const std::string& expected_type_pb_txt = entry.second;

    // Check proto -> type conversion.
    const zetasql::Type* actual_type;
    ZETASQL_EXPECT_OK(TypeFromProto(PARSE_TEXT_PROTO(expected_type_pb_txt), &factory,
                            &actual_type
                            ,
                            proto_bundle
                            ));
    EXPECT_TRUE(expected_type->Equals(actual_type))
        << "When parsing {" << expected_type_pb_txt << "} "
        << " expected " << expected_type->DebugString() << " got "
        << actual_type->DebugString();

    // Check type -> proto conversions
    google::spanner::v1::Type actual_type_pb;
    ZETASQL_EXPECT_OK(TypeToProto(expected_type, &actual_type_pb));
    EXPECT_THAT(actual_type_pb, test::EqualsProto(expected_type_pb_txt))
        << "When encoding {" << expected_type->DebugString() << "}";
  }
}

TEST_F(TypeProtos, DoesNotConvertUnknownTypesToProtos) {
  google::spanner::v1::Type actual_type_pb;
  EXPECT_EQ(
      absl::StatusCode::kInternal,
      TypeToProto(zetasql::types::Int32ArrayType(), &actual_type_pb).code());
}

TEST(ValueProtos, DoesNotConvertUnknownProtosToTypes) {
  google::spanner::v1::Type unspecified_type_pb;
  zetasql::TypeFactory factory;
  const zetasql::Type* actual_type;
  EXPECT_EQ(absl::StatusCode::kInvalidArgument,
            TypeFromProto(unspecified_type_pb, &factory, &actual_type).code());
}

TEST_F(TypeProtos, DoesNotConvertProtoArrayTypeWithUnspecifiedElementType) {
  zetasql::TypeFactory factory;
  const zetasql::Type* actual_type;
  EXPECT_EQ(
      absl::StatusCode::kInvalidArgument,
      TypeFromProto(PARSE_TEXT_PROTO("code: ARRAY"), &factory, &actual_type)
          .code());
}

}  // namespace

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
