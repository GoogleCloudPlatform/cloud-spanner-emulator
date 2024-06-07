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
#include <string>
#include <vector>

#include "google/protobuf/descriptor.pb.h"
#include "zetasql/public/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/schema/catalog/proto_bundle.h"
#include "backend/schema/ddl/operations.pb.h"
#include "tests/common/proto_matchers.h"
#include "third_party/spanner_pg/catalog/spangres_type.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

using ::testing::HasSubstr;
using ::zetasql_base::testing::StatusIs;

class DDLColumnTypeToGoogleSqlTypeTest : public ::testing::Test {
 public:
  DDLColumnTypeToGoogleSqlTypeTest() = default;

  ddl::ColumnDefinition MakeColumnDefinitionForType(
      ddl::ColumnDefinition::Type column_type) {
    ddl::ColumnDefinition ddl_type;
    ddl_type.set_type(column_type);
    return ddl_type;
  }


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

  // Sets up the proto bundle and populates descriptor bytes
  absl::StatusOr<std::shared_ptr<const ProtoBundle> > SetUpProtoBundle() {
    auto insert_proto_types =
        std::vector<std::string>{"customer.app.User", "customer.app.State"};
    ZETASQL_ASSIGN_OR_RETURN(auto builder, ProtoBundle::Builder::New(
                                       GenerateProtoDescriptorBytesAsString()));
    ZETASQL_RETURN_IF_ERROR(builder->InsertTypes(insert_proto_types));
    ZETASQL_ASSIGN_OR_RETURN(auto proto_bundle, builder->Build());
    return proto_bundle;
  }

  ddl::ColumnDefinition MakeColumnType(ddl::ColumnDefinition::Type column_type,
                                       std::string proto_type_fqn) {
    auto ddl_type = MakeColumnDefinitionForType(column_type);
    ddl_type.set_proto_type_name(proto_type_fqn);
    return ddl_type;
  }

 protected:
  zetasql::TypeFactory type_factory_;
};

TEST_F(DDLColumnTypeToGoogleSqlTypeTest, Float32) {
  auto ddl_type = MakeColumnDefinitionForType(ddl::ColumnDefinition::FLOAT);
  ZETASQL_ASSERT_OK_AND_ASSIGN(const zetasql::Type* converted_type,
                       DDLColumnTypeToGoogleSqlType(ddl_type, &type_factory_));
  EXPECT_TRUE(converted_type->Equals(type_factory_.get_float()));
  EXPECT_THAT(GoogleSqlTypeToDDLColumnType(converted_type),
              test::EqualsProto(ddl_type));
}

TEST_F(DDLColumnTypeToGoogleSqlTypeTest, ArrayOfFloat32) {
  auto array_element_type =
      MakeColumnDefinitionForType(ddl::ColumnDefinition::FLOAT);
  auto array_type = MakeColumnDefinitionForType(ddl::ColumnDefinition::ARRAY);
  *(array_type.mutable_array_subtype()) = array_element_type;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const zetasql::Type* converted_type,
      DDLColumnTypeToGoogleSqlType(array_type, &type_factory_));

  const zetasql::Type* googlesql_array_type;
  ZETASQL_ASSERT_OK(type_factory_.MakeArrayType(type_factory_.get_float(),
                                        &googlesql_array_type));

  EXPECT_TRUE(converted_type->Equals(googlesql_array_type));
  EXPECT_THAT(GoogleSqlTypeToDDLColumnType(converted_type),
              test::EqualsProto(array_type));
}

TEST_F(DDLColumnTypeToGoogleSqlTypeTest, StructOfFloat32) {
  ddl::ColumnDefinition struct_type = PARSE_TEXT_PROTO(R"pb(
    type: STRUCT
    type_definition {
      type: STRUCT
      struct_descriptor {
        field {
          name: "foo"
          type { type: FLOAT }
        }
      }
    }
  )pb");
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const zetasql::Type* converted_type,
      DDLColumnTypeToGoogleSqlType(struct_type, &type_factory_));

  const zetasql::Type* googlesql_struct_type;
  ZETASQL_ASSERT_OK(type_factory_.MakeStructType(
      {{"foo", zetasql::types::FloatType()}}, &googlesql_struct_type));
  EXPECT_TRUE(converted_type->Equals(googlesql_struct_type));
  EXPECT_THAT(GoogleSqlTypeToDDLColumnType(converted_type),
              test::EqualsProto(struct_type));
}

TEST_F(DDLColumnTypeToGoogleSqlTypeTest, Float64) {
  auto ddl_type = MakeColumnDefinitionForType(ddl::ColumnDefinition::DOUBLE);
  ZETASQL_ASSERT_OK_AND_ASSIGN(const zetasql::Type* converted_type,
                       DDLColumnTypeToGoogleSqlType(ddl_type, &type_factory_));
  EXPECT_TRUE(converted_type->Equals(type_factory_.get_double()));
  EXPECT_THAT(GoogleSqlTypeToDDLColumnType(converted_type),
              test::EqualsProto(ddl_type));
}

TEST_F(DDLColumnTypeToGoogleSqlTypeTest, Int64) {
  auto ddl_type = MakeColumnDefinitionForType(ddl::ColumnDefinition::INT64);
  ZETASQL_ASSERT_OK_AND_ASSIGN(const zetasql::Type* converted_type,
                       DDLColumnTypeToGoogleSqlType(ddl_type, &type_factory_));
  EXPECT_TRUE(converted_type->Equals(type_factory_.get_int64()));
  EXPECT_THAT(GoogleSqlTypeToDDLColumnType(converted_type),
              test::EqualsProto(ddl_type));
}

TEST_F(DDLColumnTypeToGoogleSqlTypeTest, Bool) {
  auto ddl_type = MakeColumnDefinitionForType(ddl::ColumnDefinition::BOOL);
  ZETASQL_ASSERT_OK_AND_ASSIGN(const zetasql::Type* converted_type,
                       DDLColumnTypeToGoogleSqlType(ddl_type, &type_factory_));
  EXPECT_TRUE(converted_type->Equals(type_factory_.get_bool()));
  EXPECT_THAT(GoogleSqlTypeToDDLColumnType(converted_type),
              test::EqualsProto(ddl_type));
}

TEST_F(DDLColumnTypeToGoogleSqlTypeTest, String) {
  auto ddl_type = MakeColumnDefinitionForType(ddl::ColumnDefinition::STRING);
  ZETASQL_ASSERT_OK_AND_ASSIGN(const zetasql::Type* converted_type,
                       DDLColumnTypeToGoogleSqlType(ddl_type, &type_factory_));
  EXPECT_TRUE(converted_type->Equals(type_factory_.get_string()));
  EXPECT_THAT(GoogleSqlTypeToDDLColumnType(converted_type),
              test::EqualsProto(ddl_type));
}

TEST_F(DDLColumnTypeToGoogleSqlTypeTest, Bytes) {
  auto ddl_type = MakeColumnDefinitionForType(ddl::ColumnDefinition::BYTES);
  ZETASQL_ASSERT_OK_AND_ASSIGN(const zetasql::Type* converted_type,
                       DDLColumnTypeToGoogleSqlType(ddl_type, &type_factory_));
  EXPECT_TRUE(converted_type->Equals(type_factory_.get_bytes()));
  EXPECT_THAT(GoogleSqlTypeToDDLColumnType(converted_type),
              test::EqualsProto(ddl_type));
}

TEST_F(DDLColumnTypeToGoogleSqlTypeTest, Timestamp) {
  auto ddl_type = MakeColumnDefinitionForType(ddl::ColumnDefinition::TIMESTAMP);
  ZETASQL_ASSERT_OK_AND_ASSIGN(const zetasql::Type* converted_type,
                       DDLColumnTypeToGoogleSqlType(ddl_type, &type_factory_));
  EXPECT_TRUE(converted_type->Equals(type_factory_.get_timestamp()));
  EXPECT_THAT(GoogleSqlTypeToDDLColumnType(converted_type),
              test::EqualsProto(ddl_type));
}

TEST_F(DDLColumnTypeToGoogleSqlTypeTest, Date) {
  auto ddl_type = MakeColumnDefinitionForType(ddl::ColumnDefinition::DATE);
  ZETASQL_ASSERT_OK_AND_ASSIGN(const zetasql::Type* converted_type,
                       DDLColumnTypeToGoogleSqlType(ddl_type, &type_factory_));
  EXPECT_TRUE(converted_type->Equals(type_factory_.get_date()));
  EXPECT_THAT(GoogleSqlTypeToDDLColumnType(converted_type),
              test::EqualsProto(ddl_type));
}

TEST_F(DDLColumnTypeToGoogleSqlTypeTest, Numeric) {
  auto ddl_type = MakeColumnDefinitionForType(ddl::ColumnDefinition::NUMERIC);
  ZETASQL_ASSERT_OK_AND_ASSIGN(const zetasql::Type* converted_type,
                       DDLColumnTypeToGoogleSqlType(ddl_type, &type_factory_));
  EXPECT_TRUE(converted_type->Equals(type_factory_.get_numeric()));
  EXPECT_THAT(GoogleSqlTypeToDDLColumnType(converted_type),
              test::EqualsProto(ddl_type));
}

TEST_F(DDLColumnTypeToGoogleSqlTypeTest, Json) {
  auto ddl_type = MakeColumnDefinitionForType(ddl::ColumnDefinition::JSON);
  ZETASQL_ASSERT_OK_AND_ASSIGN(const zetasql::Type* converted_type,
                       DDLColumnTypeToGoogleSqlType(ddl_type, &type_factory_));
  EXPECT_TRUE(converted_type->Equals(type_factory_.get_json()));
  EXPECT_THAT(GoogleSqlTypeToDDLColumnType(converted_type),
              test::EqualsProto(ddl_type));
}

TEST_F(DDLColumnTypeToGoogleSqlTypeTest, PgNumeric) {
  auto ddl_type =
      MakeColumnDefinitionForType(ddl::ColumnDefinition::PG_NUMERIC);
  ZETASQL_ASSERT_OK_AND_ASSIGN(const zetasql::Type* converted_type,
                       DDLColumnTypeToGoogleSqlType(ddl_type, &type_factory_));
  EXPECT_TRUE(converted_type->Equals(
      postgres_translator::spangres::types::PgNumericMapping()->mapped_type()));
  EXPECT_THAT(GoogleSqlTypeToDDLColumnType(converted_type),
              test::EqualsProto(ddl_type));
}

TEST_F(DDLColumnTypeToGoogleSqlTypeTest, JsonB) {
  auto ddl_type = MakeColumnDefinitionForType(ddl::ColumnDefinition::PG_JSONB);
  ZETASQL_ASSERT_OK_AND_ASSIGN(const zetasql::Type* converted_type,
                       DDLColumnTypeToGoogleSqlType(ddl_type, &type_factory_));
  EXPECT_TRUE(converted_type->Equals(
      postgres_translator::spangres::types::PgJsonbMapping()->mapped_type()));
  EXPECT_THAT(GoogleSqlTypeToDDLColumnType(converted_type),
              test::EqualsProto(ddl_type));
}

TEST_F(DDLColumnTypeToGoogleSqlTypeTest, TestUnrecognizedColumnType) {
  auto ddl_type = MakeColumnDefinitionForType(ddl::ColumnDefinition::NONE);
  EXPECT_THAT(DDLColumnTypeToGoogleSqlType(ddl_type, &type_factory_),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("Unrecognized ddl::ColumnDefinition:")));
}

TEST_F(DDLColumnTypeToGoogleSqlTypeTest, Proto) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle, SetUpProtoBundle());
  std::string proto_type_fqn = "customer.app.User";
  auto ddl_type = MakeColumnType(ddl::ColumnDefinition::NONE, proto_type_fqn);
  ZETASQL_ASSERT_OK_AND_ASSIGN(const zetasql::Type* converted_type,
                       DDLColumnTypeToGoogleSqlType(ddl_type, &type_factory_,
                                                    proto_bundle.get()));
  const zetasql::Type* googlesql_proto_type;
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto descriptor,
                       proto_bundle->GetTypeDescriptor(proto_type_fqn));
  ZETASQL_ASSERT_OK(type_factory_.MakeProtoType(descriptor, &googlesql_proto_type));

  EXPECT_TRUE(converted_type->Equals(googlesql_proto_type));
  EXPECT_THAT(GoogleSqlTypeToDDLColumnType(converted_type),
              test::EqualsProto(ddl_type));
}

TEST_F(DDLColumnTypeToGoogleSqlTypeTest, Enum) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle, SetUpProtoBundle());
  std::string proto_type_fqn = "customer.app.State";
  auto ddl_type = MakeColumnType(ddl::ColumnDefinition::NONE, proto_type_fqn);
  ZETASQL_ASSERT_OK_AND_ASSIGN(const zetasql::Type* converted_type,
                       DDLColumnTypeToGoogleSqlType(ddl_type, &type_factory_,
                                                    proto_bundle.get()));
  const zetasql::Type* googlesql_enum_type;
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto descriptor,
                       proto_bundle->GetEnumTypeDescriptor(proto_type_fqn));
  ZETASQL_ASSERT_OK(type_factory_.MakeEnumType(descriptor, &googlesql_enum_type));

  EXPECT_TRUE(converted_type->Equals(googlesql_enum_type));
  EXPECT_THAT(GoogleSqlTypeToDDLColumnType(converted_type),
              test::EqualsProto(ddl_type));
}

TEST_F(DDLColumnTypeToGoogleSqlTypeTest, ArrayOfProto) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle, SetUpProtoBundle());
  std::string proto_type_fqn = "customer.app.User";
  auto array_element_type =
      MakeColumnType(ddl::ColumnDefinition::NONE, proto_type_fqn);
  auto array_type = MakeColumnDefinitionForType(ddl::ColumnDefinition::ARRAY);
  *(array_type.mutable_array_subtype()) = array_element_type;
  ZETASQL_ASSERT_OK_AND_ASSIGN(const zetasql::Type* converted_type,
                       DDLColumnTypeToGoogleSqlType(array_type, &type_factory_,
                                                    proto_bundle.get()));
  const zetasql::Type* proto_type;
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto descriptor,
                       proto_bundle->GetTypeDescriptor(proto_type_fqn));
  ZETASQL_ASSERT_OK(type_factory_.MakeProtoType(descriptor, &proto_type));

  const zetasql::Type* googlesql_array_type;
  ZETASQL_ASSERT_OK(type_factory_.MakeArrayType(proto_type, &googlesql_array_type));

  EXPECT_TRUE(converted_type->Equals(googlesql_array_type));
  EXPECT_THAT(GoogleSqlTypeToDDLColumnType(converted_type),
              test::EqualsProto(array_type));
}

TEST_F(DDLColumnTypeToGoogleSqlTypeTest, ArrayOfEnum) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle, SetUpProtoBundle());
  std::string proto_type_fqn = "customer.app.State";
  auto array_element_type =
      MakeColumnType(ddl::ColumnDefinition::NONE, proto_type_fqn);
  auto array_type = MakeColumnDefinitionForType(ddl::ColumnDefinition::ARRAY);
  *(array_type.mutable_array_subtype()) = array_element_type;
  ZETASQL_ASSERT_OK_AND_ASSIGN(const zetasql::Type* converted_type,
                       DDLColumnTypeToGoogleSqlType(array_type, &type_factory_,
                                                    proto_bundle.get()));
  const zetasql::Type* enum_type;
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto descriptor,
                       proto_bundle->GetEnumTypeDescriptor(proto_type_fqn));
  ZETASQL_ASSERT_OK(type_factory_.MakeEnumType(descriptor, &enum_type));

  const zetasql::Type* googlesql_array_type;
  ZETASQL_ASSERT_OK(type_factory_.MakeArrayType(enum_type, &googlesql_array_type));

  EXPECT_TRUE(converted_type->Equals(googlesql_array_type));
  EXPECT_THAT(GoogleSqlTypeToDDLColumnType(converted_type),
              test::EqualsProto(array_type));
}

TEST_F(DDLColumnTypeToGoogleSqlTypeTest, Array) {
  auto array_element_type =
      MakeColumnDefinitionForType(ddl::ColumnDefinition::STRING);
  auto array_type = MakeColumnDefinitionForType(ddl::ColumnDefinition::ARRAY);
  *(array_type.mutable_array_subtype()) = array_element_type;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const zetasql::Type* converted_type,
      DDLColumnTypeToGoogleSqlType(array_type, &type_factory_));

  const zetasql::Type* googlesql_array_type;
  ZETASQL_ASSERT_OK(type_factory_.MakeArrayType(type_factory_.get_string(),
                                        &googlesql_array_type));

  EXPECT_TRUE(converted_type->Equals(googlesql_array_type));
  EXPECT_THAT(GoogleSqlTypeToDDLColumnType(converted_type),
              test::EqualsProto(array_type));
}

TEST_F(DDLColumnTypeToGoogleSqlTypeTest, ArrayOfArray) {
  auto array_element_type =
      MakeColumnDefinitionForType(ddl::ColumnDefinition::STRING);
  auto array_type1 = MakeColumnDefinitionForType(ddl::ColumnDefinition::ARRAY);
  *(array_type1.mutable_array_subtype()) = array_element_type;

  auto array_type2 = MakeColumnDefinitionForType(ddl::ColumnDefinition::ARRAY);
  *(array_type2.mutable_array_subtype()) = array_type1;

  EXPECT_THAT(DDLColumnTypeToGoogleSqlType(array_type2, &type_factory_),
              ::zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(DDLColumnTypeToGoogleSqlTypeTest, Struct) {
  ddl::ColumnDefinition struct_type = PARSE_TEXT_PROTO(R"pb(
    type: STRUCT
    type_definition {
      type: STRUCT
      struct_descriptor {
        field {
          name: "foo"
          type { type: INT64 }
        }
        field {
          name: "bar"
          type {
            type: ARRAY
            array_subtype { type: BOOL }
          }
        }
        field {
          name: "baz"
          type {
            type: STRUCT
            struct_descriptor {
              field {
                name: "qux"
                type { type: STRING }
              }
            }
          }
        }
      }
    }
  )pb");
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const zetasql::Type* converted_type,
      DDLColumnTypeToGoogleSqlType(struct_type, &type_factory_));

  const zetasql::Type* googlesql_array_field_type;
  ZETASQL_ASSERT_OK(type_factory_.MakeArrayType(zetasql::types::BoolType(),
                                        &googlesql_array_field_type));
  const zetasql::Type* googlesql_struct_field_type;
  ZETASQL_ASSERT_OK(type_factory_.MakeStructType(
      {
          {"qux", zetasql::types::StringType()},
      },
      &googlesql_struct_field_type));
  const zetasql::Type* googlesql_struct_type;
  ZETASQL_ASSERT_OK(type_factory_.MakeStructType(
      {
          {"foo", zetasql::types::Int64Type()},
          {"bar", googlesql_array_field_type},
          {"baz", googlesql_struct_field_type},
      },
      &googlesql_struct_type));

  EXPECT_TRUE(converted_type->Equals(googlesql_struct_type));
  EXPECT_THAT(GoogleSqlTypeToDDLColumnType(converted_type),
              test::EqualsProto(struct_type));
}

TEST_F(DDLColumnTypeToGoogleSqlTypeTest, InvalidGoogleSqlType) {
  ddl::ColumnDefinition unknown_type;
  unknown_type.set_type(ddl::ColumnDefinition::NONE);
  EXPECT_THAT(GoogleSqlTypeToDDLColumnType(type_factory_.get_geography()),
              test::EqualsProto(unknown_type));
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
