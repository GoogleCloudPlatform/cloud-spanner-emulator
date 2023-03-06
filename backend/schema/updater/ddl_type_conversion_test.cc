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
#include "backend/schema/ddl/operations.pb.h"
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

 protected:
  zetasql::TypeFactory type_factory_;
};

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

TEST_F(DDLColumnTypeToGoogleSqlTypeTest, TestUnrecognizedColumnType) {
  auto ddl_type = MakeColumnDefinitionForType(ddl::ColumnDefinition::NONE);
  EXPECT_THAT(DDLColumnTypeToGoogleSqlType(ddl_type, &type_factory_),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("Unrecognized ddl::ColumnDefinition:")));
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

TEST_F(DDLColumnTypeToGoogleSqlTypeTest, InvalidGoogleSqlType) {
  ddl::ColumnDefinition unknown_type;
  unknown_type.set_type(ddl::ColumnDefinition::NONE);
  EXPECT_THAT(GoogleSqlTypeToDDLColumnType(type_factory_.get_float()),
              test::EqualsProto(unknown_type));
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
