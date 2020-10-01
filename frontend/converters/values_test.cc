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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/time/time.h"

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
using zetasql::types::NumericArrayType;
using zetasql::types::NumericType;
using zetasql::types::StringType;
using zetasql::types::TimestampType;

using zetasql::values::Bool;
using zetasql::values::Bytes;
using zetasql::values::Date;
using zetasql::values::Double;
using zetasql::values::Int64;
using zetasql::values::Int64Array;
using zetasql::values::Null;
using zetasql::values::Numeric;
using zetasql::values::NumericArray;
using zetasql::values::String;
using zetasql::values::Struct;
using zetasql::values::Timestamp;

using zetasql_base::testing::StatusIs;

TEST(ValueProtos, ConvertsBasicTypesBetweenValuesAndProtos) {
  zetasql::TypeFactory factory;
  const StructType* str_int_pair;
  ZETASQL_ASSERT_OK(factory.MakeStructType(
      {StructType::StructField("str", factory.get_string()),
       StructType::StructField("int", factory.get_int64())},
      &str_int_pair));
  std::vector<std::pair<zetasql::Value, std::string>> test_cases{
      {Null(StringType()), "null_value: NULL_VALUE"},
      {Bool(true), "bool_value: true"},
      {Int64(101), "string_value: '101'"},
      {Int64(4147483647), "string_value: '4147483647'"},
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
      {Int64Array({}), "list_value: { values [] }"},
      {Int64Array({1, 2, 3}),
       "list_value: { values [{string_value: '1'}, {string_value: '2'}, "
       "{string_value: '3'}] }"},
      {NumericArray(
           {zetasql::NumericValue::FromStringStrict("-23.923").value(),
            zetasql::NumericValue::FromStringStrict("987.234").value(),
            zetasql::NumericValue::FromStringStrict("987.234e-3").value()}),
       "list_value: { values [{string_value: '-23.923'}, {string_value: "
       "'987.234'}, {string_value: '0.987234' }] }"},
      {Struct(EmptyStructType(), {}), "list_value: { values: [] }"},
      {Struct(str_int_pair, {String("One"), Int64(2)}),
       "list_value: { values[{string_value: 'One'}, {string_value: '2'}] }"},
  };
  for (const auto& entry : test_cases) {
    const zetasql::Value& expected_value = entry.first;
    const std::string& expected_value_pb_txt = entry.second;

    // Check proto -> value conversion.
    ZETASQL_ASSERT_OK_AND_ASSIGN(zetasql::Value actual_value,
                         ValueFromProto(PARSE_TEXT_PROTO(expected_value_pb_txt),
                                        expected_value.type()));
    EXPECT_EQ(expected_value, actual_value)
        << "When parsing {" << expected_value_pb_txt << "}";

    // Check value -> proto conversions
    ZETASQL_ASSERT_OK_AND_ASSIGN(google::protobuf::Value actual_value_pb,
                         ValueToProto(expected_value));
    EXPECT_THAT(actual_value_pb, test::EqualsProto(expected_value_pb_txt))
        << "When encoding {" << expected_value << "}";
  }
}

TEST(ValueProtos, DoesNotConvertUnknownValueTypesToProtos) {
  EXPECT_THAT(ValueToProto(zetasql::values::Invalid()),
              StatusIs(absl::StatusCode::kInternal));
  EXPECT_THAT(ValueToProto(zetasql::values::Int32(0)),
              StatusIs(absl::StatusCode::kInternal));
}

TEST(ValueProtos, DoesNotParseProtosWithMismatchingTypes) {
  std::vector<std::pair<const zetasql::Type*, std::string>> test_cases{
      {Int64Type(), "string_value: 'not a number'"},
      {TimestampType(), "number_value: -1"},
      {StringType(), "number_value: 1.0"},
      {BytesType(), "number_value: -1"},
      {NumericType(), "number_value: 1"},
      {Int64ArrayType(), "string_value: '1'"},
      {NumericArrayType(), "string_value: '1'"},
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

TEST(ValueProtos, ParsesSpannerCommitTimestamp) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      zetasql::Value value,
      ValueFromProto(
          PARSE_TEXT_PROTO("string_value: 'spanner.commit_timestamp()'"),
          TimestampType()));
  EXPECT_EQ(String("spanner.commit_timestamp()"), value);
}

TEST(ValueProtos, DoesNotParseInvalidTimestamps) {
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

TEST(ValueProtos, DoesNotParseInvalidDates) {
  // Before 0001-01-01.
  EXPECT_THAT(ValueFromProto(PARSE_TEXT_PROTO("string_value: '0000-01-02'"),
                             DateType()),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // After 9999-12-31.
  EXPECT_THAT(ValueFromProto(PARSE_TEXT_PROTO("string_value: '10000-12-31'"),
                             DateType()),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(ValueProtos, DoesNotParseInvalidBytes) {
  EXPECT_THAT(
      ValueFromProto(PARSE_TEXT_PROTO("string_value: ';;Z'"), BytesType()),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST(ValueProtos, DoesNotParseInvalidNumeric) {
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

}  // namespace

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
