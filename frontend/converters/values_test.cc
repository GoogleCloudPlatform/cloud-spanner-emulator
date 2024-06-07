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

#include "frontend/converters/values.h"

#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "backend/schema/catalog/proto_bundle.h"
#include "tests/common/proto_matchers.h"
#include "tests/common/test.pb.h"
#include "third_party/spanner_pg/datatypes/common/numeric_core.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_numeric_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_oid_type.h"
#include "third_party/spanner_pg/interface/pg_arena.h"
#include "third_party/spanner_pg/interface/pg_arena_factory.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {

using zetasql::StructType;
using zetasql::types::BytesType;
using zetasql::types::DateType;
using zetasql::types::EmptyStructType;
using zetasql::types::Int64ArrayType;
using zetasql::types::Int64Type;
using zetasql::types::JsonArrayType;
using zetasql::types::JsonType;
using zetasql::types::NumericArrayType;
using zetasql::types::NumericType;
using zetasql::types::FloatType;
using zetasql::types::StringType;
using zetasql::types::TimestampType;

using zetasql::values::Bool;
using zetasql::values::Bytes;
using zetasql::values::Date;
using zetasql::values::Float;
using zetasql::values::Double;
using zetasql::values::Int64;
using zetasql::values::Int64Array;
using zetasql::values::Json;
using zetasql::values::JsonArray;
using zetasql::values::Null;
using zetasql::values::Numeric;
using zetasql::values::NumericArray;
using zetasql::values::String;
using zetasql::values::Struct;
using zetasql::values::Timestamp;
using zetasql::values::Array;
using zetasql::values::Enum;
using zetasql::values::Proto;

using postgres_translator::spangres::datatypes::CreatePgJsonbValue;
using postgres_translator::spangres::datatypes::CreatePgNumericValue;
using postgres_translator::spangres::datatypes::CreatePgOidValue;
using postgres_translator::spangres::datatypes::GetPgJsonbArrayType;
using postgres_translator::spangres::datatypes::GetPgJsonbType;
using postgres_translator::spangres::datatypes::GetPgNumericArrayType;
using postgres_translator::spangres::datatypes::GetPgNumericType;
using postgres_translator::spangres::datatypes::GetPgOidArrayType;
using postgres_translator::spangres::datatypes::GetPgOidType;
using ::postgres_translator::spangres::datatypes::common::MaxNumericString;

using zetasql_base::testing::StatusIs;

// Create PG.JSONB value in a valid memory context which is required for calling
// PG code.
static absl::StatusOr<zetasql::Value> CreatePgJsonbValueWithMemoryContext(
    absl::string_view jsonb_string) {
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<postgres_translator::interfaces::PGArena> pg_arena,
      postgres_translator::interfaces::CreatePGArena(nullptr));
  return CreatePgJsonbValue(jsonb_string);
}

// Create PG.NUMERIC value in a valid memory context which is required for
// calling PG code.
static absl::StatusOr<zetasql::Value> CreatePgNumericValueWithMemoryContext(
    absl::string_view numeric_string) {
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<postgres_translator::interfaces::PGArena> pg_arena,
      postgres_translator::interfaces::CreatePGArena(nullptr));
  return CreatePgNumericValue(numeric_string);
}

// Performs equality with the memory arena initialized. This is necessary for pg
// types that call internal functions in order to convert values into a
// comparable representation (e.g. pg numeric, which uses `numeric_in`).
MATCHER_P(EqPG, result,
          absl::StrCat("EqualPostgreSQLValue(", result.DebugString(), ")")) {
  auto pg_arena = postgres_translator::interfaces::CreatePGArena(nullptr);
  if (!pg_arena.ok()) {
    *result_listener << "pg memory arena could not be initialized "
                     << pg_arena.status();
    return false;
  }
  return arg == result;
}

class ValueProtos : public ::testing::Test {
 public:
  ValueProtos() = default;

  std::string read_descriptors() {
    google::protobuf::FileDescriptorSet proto_files;
    ::emulator::tests::common::Simple::descriptor()->file()->CopyTo(
        proto_files.add_file());
    return proto_files.SerializeAsString();
  }

  // Sets up the proto bundle and populates FileDescriptor bytes.
  absl::StatusOr<std::shared_ptr<const backend::ProtoBundle>>
  SetUpProtoBundle() {
    auto insert_proto_types = std::vector<std::string>{};
    insert_proto_types.push_back("emulator.tests.common.Simple");
    insert_proto_types.push_back("emulator.tests.common.TestEnum");
    ZETASQL_ASSIGN_OR_RETURN(auto builder,
                     backend::ProtoBundle::Builder::New(read_descriptors()));
    ZETASQL_RETURN_IF_ERROR(builder->InsertTypes(insert_proto_types));
    ZETASQL_ASSIGN_OR_RETURN(auto proto_bundle, builder->Build());
    return proto_bundle;
  }
};

TEST_F(ValueProtos, ConvertsBasicTypesBetweenValuesAndProtos) {
  zetasql::TypeFactory factory;
  const StructType* str_int_pair;
  ZETASQL_ASSERT_OK(factory.MakeStructType(
      {StructType::StructField("str", factory.get_string()),
       StructType::StructField("int", factory.get_int64())},
      &str_int_pair));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle, SetUpProtoBundle());

  std::string proto_type_fqn = "emulator.tests.common.Simple";
  const zetasql::Type* proto_type;
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto descriptor,
                       proto_bundle->GetTypeDescriptor(proto_type_fqn));
  ZETASQL_ASSERT_OK(factory.MakeProtoType(descriptor, &proto_type));

  std::string proto_enum_type_fqn = "emulator.tests.common.TestEnum";
  const zetasql::Type* enum_type;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto enum_descriptor,
      proto_bundle->GetEnumTypeDescriptor(proto_enum_type_fqn));
  ZETASQL_ASSERT_OK(factory.MakeEnumType(enum_descriptor, &enum_type));

  const zetasql::Type* array_proto_type;
  ZETASQL_ASSERT_OK(factory.MakeArrayType(proto_type, &array_proto_type));

  const zetasql::Type* array_enum_type;
  ZETASQL_ASSERT_OK(factory.MakeArrayType(enum_type, &array_enum_type));

  ::emulator::tests::common::Simple simple_proto;
  simple_proto.set_field("ProtoValue");
  std::string encoded_field;
  absl::Base64Escape(simple_proto.SerializeAsString(), &encoded_field);

  std::vector<std::pair<zetasql::Value, std::string>> test_cases{
      {Null(StringType()), "null_value: NULL_VALUE"},
      {Bool(true), "bool_value: true"},
      {Int64(101), "string_value: '101'"},
      {Int64(4147483647), "string_value: '4147483647'"},
      {CreatePgOidValue(12345).value(), "string_value: '12345'"},
      {CreatePgOidValue(4147483647).value(), "string_value: '4147483647'"},
      {Float(-1), "number_value: -1"},
      {Float(std::numeric_limits<float>::infinity()),
       "string_value: 'Infinity'"},
      {Float(-std::numeric_limits<float>::infinity()),
       "string_value: '-Infinity'"},
      {Float(std::numeric_limits<float>::quiet_NaN()), "string_value: 'NaN'"},
      {Double(-1), "number_value: -1"},
      {Double(std::numeric_limits<double>::infinity()),
       "string_value: 'Infinity'"},
      {Double(-std::numeric_limits<double>::infinity()),
       "string_value: '-Infinity'"},
      {Double(std::numeric_limits<double>::quiet_NaN()), "string_value: 'NaN'"},
      {Timestamp(absl::FromCivil(absl::CivilSecond(2015, 1, 2, 3, 4, 5),
                                 absl::UTCTimeZone()) +
                 absl::Nanoseconds(67)),
       "string_value: '2015-01-02T03:04:05.000000067Z'"},
      {String("Hello, World!"), "string_value: 'Hello, World!'"},
      {Bytes("Hello, World!"), "string_value: 'SGVsbG8sIFdvcmxkIQ=='"},
      {Numeric(zetasql::NumericValue::FromStringStrict("-123456789.987654321")
                   .value()),
       "string_value: '-123456789.987654321'"},
      {CreatePgNumericValueWithMemoryContext("-123456789.987654321").value(),
       "string_value: '-123456789.987654321'"},
      {Json(zetasql::JSONValue::ParseJSONString("{\"key\":123}").value()),
       "string_value: '{\"key\":123}'"},
      {CreatePgJsonbValueWithMemoryContext("{\"key\":123}").value(),
       "string_value: '{\"key\": 123}'"},
      {Int64Array({}), "list_value: { values [] }"},
      {Int64Array({1, 2, 3}),
       "list_value: { values [{string_value: '1'}, {string_value: '2'}, "
       "{string_value: '3'}] }"},
      {zetasql::Value::MakeArray(GetPgOidArrayType(), {}).value(),
       "list_value: { values [] }"},
      {zetasql::Value::MakeArray(
           GetPgOidArrayType(),
           {CreatePgOidValue(1).value(), CreatePgOidValue(2).value(),
            CreatePgOidValue(3).value()})
           .value(),
       "list_value: { values [{string_value: '1'}, {string_value: '2'}, "
       "{string_value: '3'}] }"},
      {NumericArray(
           {zetasql::NumericValue::FromStringStrict("-23.923").value(),
            zetasql::NumericValue::FromStringStrict("987.234").value(),
            zetasql::NumericValue::FromStringStrict("987.234e-3").value()}),
       "list_value: { values [{string_value: '-23.923'}, {string_value: "
       "'987.234'}, {string_value: '0.987234' }] }"},
      {zetasql::Value::MakeArray(
           GetPgNumericArrayType(),
           {CreatePgNumericValueWithMemoryContext("-23.923").value(),
            CreatePgNumericValueWithMemoryContext("NaN").value(),
            CreatePgNumericValueWithMemoryContext("987.234").value(),
            Null(GetPgNumericType()),
            CreatePgNumericValueWithMemoryContext("987.234e-3").value()})
           .value(),
       "list_value: { values [{string_value: '-23.923'}, {string_value: 'NaN'},"
       " {string_value: '987.234'}, {null_value: NULL_VALUE}, {string_value: "
       "'0.987234' }] }"},
      {JsonArray(
           {zetasql::JSONValue::ParseJSONString("{\"intkey\":123}").value(),
            zetasql::JSONValue::ParseJSONString("{\"boolkey\":true}").value(),
            zetasql::JSONValue::ParseJSONString("{\"strkey\":\"strval\"}")
                .value()}),
       "list_value: { values [{string_value: '{\"intkey\":123}'}, "
       "{string_value: '{\"boolkey\":true}'}, "
       "{string_value: '{\"strkey\":\"strval\"}'}]}"},
      {zetasql::Value::MakeArray(
           GetPgJsonbArrayType(),
           {CreatePgJsonbValueWithMemoryContext("{\"intkey\":123}").value(),
            CreatePgJsonbValueWithMemoryContext("{\"boolkey\":true}").value(),
            CreatePgJsonbValueWithMemoryContext("{\"strkey\":\"strval\"}")
                .value()})
           .value(),
       "list_value: { values [{string_value: '{\"intkey\": 123}'}, "
       "{string_value: '{\"boolkey\": true}'}, "
       "{string_value: '{\"strkey\": \"strval\"}'}]}"},
      {Struct(EmptyStructType(), {}), "list_value: { values: [] }"},
      {Struct(str_int_pair, {String("One"), Int64(2)}),
       "list_value: { values[{string_value: 'One'}, {string_value: '2'}] }"},
      {Proto(proto_type->AsProto(), simple_proto),
       "string_value: \"" + encoded_field + "\""},
      {Enum(enum_type->AsEnum(), ::emulator::tests::common::TEST_ENUM_ONE),
       "string_value: '1'"},
      {Array(array_proto_type->AsArray(),
             {Proto(proto_type->AsProto(), simple_proto)}),
       "list_value: { values [{string_value: \"" + encoded_field + "\"}] }"},
      {Array(array_enum_type->AsArray(),
             {Enum(enum_type->AsEnum(),
                   ::emulator::tests::common::TEST_ENUM_ONE)}),
       "list_value: { values [{string_value: '1'}] }"},
  };

  for (const auto& entry : test_cases) {
    const zetasql::Value& expected_value = entry.first;
    const std::string& expected_value_pb_txt = entry.second;

    // Check proto -> value conversion.
    ZETASQL_ASSERT_OK_AND_ASSIGN(zetasql::Value actual_value,
                         ValueFromProto(PARSE_TEXT_PROTO(expected_value_pb_txt),
                                        expected_value.type()));
    EXPECT_THAT(expected_value, EqPG(actual_value))
        << "When parsing {" << expected_value_pb_txt << "}";

    // Check value -> proto conversions
    ZETASQL_ASSERT_OK_AND_ASSIGN(google::protobuf::Value actual_value_pb,
                         ValueToProto(expected_value));
    EXPECT_THAT(actual_value_pb, test::EqualsProto(expected_value_pb_txt))
        << "When encoding {" << expected_value << "}";
  }
}

TEST_F(ValueProtos, DoesNotConvertUnknownValueTypesToProtos) {
  EXPECT_THAT(ValueToProto(zetasql::values::Invalid()),
              StatusIs(absl::StatusCode::kInternal));
  EXPECT_THAT(ValueToProto(zetasql::values::Int32(0)),
              StatusIs(absl::StatusCode::kInternal));
}

TEST_F(ValueProtos, DoesNotParseProtosWithMismatchingTypes) {
  std::vector<std::pair<const zetasql::Type*, std::string>> test_cases{
      {Int64Type(), "string_value: 'not a number'"},
      {GetPgOidType(), "string_value: 'not a number'"},
      {TimestampType(), "number_value: -1"},
      {StringType(), "number_value: 1.0"},
      {BytesType(), "number_value: -1"},
      {NumericType(), "number_value: 1"},
      {GetPgNumericType(), "number_value: 1"},
      {JsonType(), "number_value: 1"},
      {GetPgJsonbType(), "number_value: 1"},
      {Int64ArrayType(), "string_value: '1'"},
      {GetPgOidArrayType(), "string_value: '1'"},
      {NumericArrayType(), "string_value: '1'"},
      {GetPgNumericArrayType(), "string_value: '1'"},
      {JsonArrayType(), "string_value: '1'"},
      {GetPgJsonbArrayType(), "string_value: '1'"},
      {EmptyStructType(), "bool_value: false"},
  };
  for (const auto& entry : test_cases) {
    const zetasql::Type* expected_type = entry.first;
    const std::string& expected_value_pb_txt = entry.second;

    // Check proto -> value conversion.
    EXPECT_THAT(
        ValueFromProto(PARSE_TEXT_PROTO(expected_value_pb_txt), expected_type),
        StatusIs(absl::StatusCode::kFailedPrecondition));
  }
}

TEST_F(ValueProtos, ParsesSpannerCommitTimestamp) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      zetasql::Value value,
      ValueFromProto(
          PARSE_TEXT_PROTO("string_value: 'spanner.commit_timestamp()'"),
          TimestampType()));
  EXPECT_EQ(String("spanner.commit_timestamp()"), value);
}

TEST_F(ValueProtos, DoesNotParseInvalidTimestamps) {
  // Missing 'Z' offset.
  EXPECT_THAT(
      ValueFromProto(
          PARSE_TEXT_PROTO("string_value: '2015-01-02T03:04:05.000000067'"),
          TimestampType()),
      StatusIs(absl::StatusCode::kFailedPrecondition));

  // Unsupported time format.
  EXPECT_THAT(
      ValueFromProto(
          PARSE_TEXT_PROTO("string_value: 'Mar 16 2015 10:04:05.000000067Z'"),
          TimestampType()),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(ValueProtos, DoesNotParseInvalidDates) {
  // Before 0001-01-01.
  EXPECT_THAT(ValueFromProto(PARSE_TEXT_PROTO("string_value: '0000-01-02'"),
                             DateType()),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // After 9999-12-31.
  EXPECT_THAT(ValueFromProto(PARSE_TEXT_PROTO("string_value: '10000-12-31'"),
                             DateType()),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(ValueProtos, DoesNotParseInvalidBytes) {
  EXPECT_THAT(
      ValueFromProto(PARSE_TEXT_PROTO("string_value: ';;Z'"), BytesType()),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(ValueProtos, DoesNotParseInvalidNumeric) {
  EXPECT_THAT(ValueFromProto(PARSE_TEXT_PROTO("string_value: '9252.a53'"),
                             NumericType()),
              StatusIs(absl::StatusCode::kFailedPrecondition));
  // Scale exceeds 9
  EXPECT_THAT(ValueFromProto(PARSE_TEXT_PROTO("string_value: '0.0000000001'"),
                             NumericType()),
              StatusIs(absl::StatusCode::kFailedPrecondition));
  // Integer portion has more than 29 digits.
  EXPECT_THAT(
      ValueFromProto(
          PARSE_TEXT_PROTO("string_value: '123456789123456789123456789000.1'"),
          NumericType()),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(ValueProtos, DoesNotParseInvalidPgNumeric) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<postgres_translator::interfaces::PGArena> pg_arena,
      postgres_translator::interfaces::CreatePGArena(nullptr));

  EXPECT_THAT(ValueFromProto(PARSE_TEXT_PROTO("string_value: '9252.a53'"),
                             GetPgNumericType()),
              StatusIs(absl::StatusCode::kFailedPrecondition));
  // Scale exceeds PG.NUMERIC max
  EXPECT_THAT(ValueFromProto(PARSE_TEXT_PROTO(absl::StrCat(
                                 "string_value: '", MaxNumericString(), "1'")),
                             GetPgNumericType()),
              StatusIs(absl::StatusCode::kFailedPrecondition));
  // Integer portion exceeds PG.NUMERIC
  EXPECT_THAT(ValueFromProto(PARSE_TEXT_PROTO(absl::StrCat(
                                 "string_value: '1", MaxNumericString(), "'")),
                             GetPgNumericType()),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(ValueProtos, DoesNotParseInvalidJson) {
  EXPECT_THAT(ValueFromProto(PARSE_TEXT_PROTO("string_value: 'invalid string'"),
                             JsonType()),
              StatusIs(absl::StatusCode::kFailedPrecondition));
  // String value should be quoted
  EXPECT_THAT(
      ValueFromProto(PARSE_TEXT_PROTO("string_value: '{\"key\":invalid}'"),
                     JsonType()),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(ValueProtos, DoesNotParseInvalidPgJsonB) {
  EXPECT_THAT(ValueFromProto(PARSE_TEXT_PROTO("string_value: 'invalid string'"),
                             GetPgJsonbType()),
              StatusIs(absl::StatusCode::kFailedPrecondition));
  // String value should be quoted
  EXPECT_THAT(
      ValueFromProto(PARSE_TEXT_PROTO("string_value: '{\"key\":invalid}'"),
                     GetPgJsonbType()),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(ValueProtos, DoesNotParseInvalidProtoAndEnum) {
  zetasql::TypeFactory factory;
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle, SetUpProtoBundle());

  std::string proto_type_fqn = "emulator.tests.common.Simple";
  const zetasql::Type* proto_type;
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto descriptor,
                       proto_bundle->GetTypeDescriptor(proto_type_fqn));
  ZETASQL_ASSERT_OK(factory.MakeProtoType(descriptor, &proto_type));

  std::string proto_enum_type_fqn = "emulator.tests.common.TestEnum";
  const zetasql::Type* enum_type;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto enum_descriptor,
      proto_bundle->GetEnumTypeDescriptor(proto_enum_type_fqn));
  ZETASQL_ASSERT_OK(factory.MakeEnumType(enum_descriptor, &enum_type));

  EXPECT_THAT(ValueFromProto(PARSE_TEXT_PROTO("string_value: 'invalid string'"),
                             proto_type),
              StatusIs(absl::StatusCode::kFailedPrecondition));

  std::string encoded_field;
  absl::Base64Escape("invalid_field : random bytes", &encoded_field);
  EXPECT_EQ("Proto<emulator.tests.common.Simple>{<unparseable>}",
            ValueFromProto(
                PARSE_TEXT_PROTO("string_value: \"" + encoded_field + "\""),
                proto_type)
                ->FullDebugString());

  EXPECT_THAT(ValueFromProto(PARSE_TEXT_PROTO("string_value: 'TEST_ENUM_ONE'"),
                             enum_type),
              StatusIs(absl::StatusCode::kFailedPrecondition));

  EXPECT_THAT(ValueFromProto(PARSE_TEXT_PROTO("string_value: '10'"), enum_type),
              zetasql::Value::Invalid());
}

TEST_F(ValueProtos, DoesNotParseInvalidFloat32) {
  // Invalid string value.
  EXPECT_THAT(
      ValueFromProto(PARSE_TEXT_PROTO("string_value: '9252.a53'"), FloatType()),
      StatusIs(absl::StatusCode::kFailedPrecondition));

  // Value lower than float's lower limit.
  double value_lower_float_min = -4e+38;
  std::string raw_proto_string =
      absl::StrCat("number_value: ", value_lower_float_min);
  EXPECT_THAT(ValueFromProto(PARSE_TEXT_PROTO(raw_proto_string), FloatType()),
              StatusIs(absl::StatusCode::kFailedPrecondition));

  // Value higher than float's upper limit.
  double value_lower_float_max = 4e+38;
  raw_proto_string = absl::StrCat("number_value: ", value_lower_float_max);
  EXPECT_THAT(ValueFromProto(PARSE_TEXT_PROTO(raw_proto_string), FloatType()),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

}  // namespace

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
