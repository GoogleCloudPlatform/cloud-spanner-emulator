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

#include "zetasql/public/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"

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
using zetasql::types::Int64ArrayType;
using zetasql::types::Int64Type;
using zetasql::types::JsonArrayType;
using zetasql::types::JsonType;
using zetasql::types::NumericArrayType;
using zetasql::types::NumericType;
using zetasql::types::StringType;
using zetasql::types::TimestampType;

TEST(TypeProtos, ConvertsBasicTypesBetweenTypesAndProtos) {
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

  std::vector<std::pair<const zetasql::Type*, std::string>> test_cases{
      {BoolType(), "code: BOOL"},
      {Int64Type(), "code: INT64"},
      {DoubleType(), "code: FLOAT64"},
      {TimestampType(), "code: TIMESTAMP"},
      {NumericType(), "code: NUMERIC"},
      {JsonType(), "code: JSON"},
      {DateType(), "code: DATE"},
      {StringType(), "code: STRING"},
      {BytesType(), "code: BYTES"},
      {Int64ArrayType(), "code: ARRAY array_element_type { code: INT64 }"},
      {NumericArrayType(), "code: ARRAY array_element_type { code: NUMERIC }"},
      {JsonArrayType(), "code: ARRAY array_element_type { code: JSON }"},
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
  };
  for (const auto& entry : test_cases) {
    const zetasql::Type* expected_type = entry.first;
    const std::string& expected_type_pb_txt = entry.second;

    // Check proto -> type conversion.
    const zetasql::Type* actual_type;
    ZETASQL_EXPECT_OK(TypeFromProto(PARSE_TEXT_PROTO(expected_type_pb_txt), &factory,
                            &actual_type));
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

TEST(TypeProtos, DoesNotConvertUnknownTypesToProtos) {
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

TEST(TypeProtos, DoesNotConvertProtoArrayTypeWithUnspecifiedElementType) {
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
