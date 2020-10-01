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

#include "frontend/converters/reads.h"

#include "google/spanner/v1/mutation.pb.h"
#include "google/spanner/v1/result_set.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "backend/access/write.h"
#include "backend/datamodel/key_range.h"
#include "backend/datamodel/key_set.h"
#include "backend/schema/catalog/schema.h"
#include "tests/common/row_cursor.h"
#include "tests/common/schema_constructor.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {

using ::google::spanner::emulator::test::TestRowCursor;
using ::google::spanner::v1::PartialResultSet;
using ::google::spanner::v1::ReadRequest;
using ::google::spanner::v1::ResultSet;
using zetasql::StructField;
using zetasql::Value;
using zetasql::types::BoolType;
using zetasql::types::BytesType;
using zetasql::types::DatetimeType;
using zetasql::types::DateType;
using zetasql::types::DoubleType;
using zetasql::types::GeographyType;
using zetasql::types::Int64ArrayType;
using zetasql::types::Int64Type;
using zetasql::types::NumericType;
using zetasql::types::StringType;
using zetasql::types::TimestampType;
using zetasql::values::Bool;
using zetasql::values::Datetime;
using zetasql::values::Double;
using zetasql::values::Int64;
using zetasql::values::NullString;
using zetasql::values::String;
using zetasql_base::testing::StatusIs;

class AccessProtosTest : public testing::Test {
 public:
  AccessProtosTest()
      : type_factory_(absl::make_unique<zetasql::TypeFactory>()),
        schema_(test::CreateSchemaWithOneTable(type_factory_.get())) {}

 protected:
  void SetUp() override {
    // Schema created using test schema constructor is equivalent to the
    // following DDL statement:
    //
    // CREATE TABLE test_table (
    //   int64_col INT64 NOT NULL,
    //   string_col STRING(MAX)
    // ) PRIMARY_KEY(int64_col);
    //
    // CREATE INDEX test_index ON test_table(string_col DESC);
    //
    // And thus should have exactly one table and name should be "test_table".
    ASSERT_EQ(schema_->tables().size(), 1);
    ASSERT_EQ(schema_->tables()[0]->Name(), "test_table");
  }

  // The type factory must outlive the type objects that it has made.
  const std::unique_ptr<zetasql::TypeFactory> type_factory_;
  const std::unique_ptr<const backend::Schema> schema_;
};

TEST_F(AccessProtosTest, CanConvertRowCursorToResultSet) {
  TestRowCursor cursor(
      {"int64", "bool", "double", "string", "string"},
      {Int64Type(), BoolType(), DoubleType(), StringType(), StringType()},
      {{Int64(1), Bool(true), Double(1.234), String("Test"), NullString()},
       {Int64(2), Bool(true), Double(0.0), String(""), NullString()},
       {Int64(3), Bool(false), Double(-9.999), String("Key:Value"),
        NullString()}});
  ResultSet result_pb;
  ZETASQL_EXPECT_OK(RowCursorToResultSetProto(&cursor, 0, &result_pb));

  EXPECT_THAT(result_pb, test::EqualsProto(
                             R"(metadata {
                                  row_type {
                                    fields {
                                      name: "int64"
                                      type { code: INT64 }
                                    }
                                    fields {
                                      name: "bool"
                                      type { code: BOOL }
                                    }
                                    fields {
                                      name: "double"
                                      type { code: FLOAT64 }
                                    }
                                    fields {
                                      name: "string"
                                      type { code: STRING }
                                    }
                                    fields {
                                      name: "string"
                                      type { code: STRING }
                                    }
                                  }
                                }
                                rows {
                                  values { string_value: "1" }
                                  values { bool_value: true }
                                  values { number_value: 1.234 }
                                  values { string_value: "Test" }
                                  values { null_value: NULL_VALUE }
                                }
                                rows {
                                  values { string_value: "2" }
                                  values { bool_value: true }
                                  values { number_value: 0 }
                                  values { string_value: "" }
                                  values { null_value: NULL_VALUE }
                                }
                                rows {
                                  values { string_value: "3" }
                                  values { bool_value: false }
                                  values { number_value: -9.999 }
                                  values { string_value: "Key:Value" }
                                  values { null_value: NULL_VALUE }
                                })"));
}

TEST_F(AccessProtosTest, CanConvertRowCursorToResultSetWithLimit) {
  TestRowCursor cursor(
      {"int64", "bool", "double", "string", "string"},
      {Int64Type(), BoolType(), DoubleType(), StringType(), StringType()},
      {{Int64(1), Bool(true), Double(1.234), String("Test"), NullString()},
       {Int64(2), Bool(true), Double(0.0), String(""), NullString()},
       {Int64(3), Bool(false), Double(-9.999), String("Key:Value"),
        NullString()}});
  ResultSet result_pb;
  ZETASQL_EXPECT_OK(RowCursorToResultSetProto(&cursor, 1, &result_pb));

  EXPECT_THAT(result_pb, test::EqualsProto(
                             R"(metadata {
                                  row_type {
                                    fields {
                                      name: "int64"
                                      type { code: INT64 }
                                    }
                                    fields {
                                      name: "bool"
                                      type { code: BOOL }
                                    }
                                    fields {
                                      name: "double"
                                      type { code: FLOAT64 }
                                    }
                                    fields {
                                      name: "string"
                                      type { code: STRING }
                                    }
                                    fields {
                                      name: "string"
                                      type { code: STRING }
                                    }
                                  }
                                }
                                rows {
                                  values { string_value: "1" }
                                  values { bool_value: true }
                                  values { number_value: 1.234 }
                                  values { string_value: "Test" }
                                  values { null_value: NULL_VALUE }
                                })"));
}

TEST_F(AccessProtosTest, CanConvertRowCursorToPartialResultSet) {
  TestRowCursor cursor(
      {"int64", "bool", "double", "string", "string"},
      {Int64Type(), BoolType(), DoubleType(), StringType(), StringType()},
      {{Int64(1), Bool(true), Double(1.234), String("Test"), NullString()},
       {Int64(2), Bool(true), Double(0.0), String(""), NullString()},
       {Int64(3), Bool(false), Double(-9.999), String("Key:Value"),
        NullString()}});
  std::vector<PartialResultSet> results;
  ZETASQL_ASSERT_OK_AND_ASSIGN(results, RowCursorToPartialResultSetProtos(&cursor, 0));

  EXPECT_THAT(results[0], test::EqualsProto(
                              R"(metadata {
                                   row_type {
                                     fields {
                                       name: "int64"
                                       type { code: INT64 }
                                     }
                                     fields {
                                       name: "bool"
                                       type { code: BOOL }
                                     }
                                     fields {
                                       name: "double"
                                       type { code: FLOAT64 }
                                     }
                                     fields {
                                       name: "string"
                                       type { code: STRING }
                                     }
                                     fields {
                                       name: "string"
                                       type { code: STRING }
                                     }
                                   }
                                 }
                                 values { string_value: "1" }
                                 values { bool_value: true }
                                 values { number_value: 1.234 }
                                 values { string_value: "Test" }
                                 values { null_value: NULL_VALUE }
                                 values { string_value: "2" }
                                 values { bool_value: true }
                                 values { number_value: 0 }
                                 values { string_value: "" }
                                 values { null_value: NULL_VALUE }
                                 values { string_value: "3" }
                                 values { bool_value: false }
                                 values { number_value: -9.999 }
                                 values { string_value: "Key:Value" }
                                 values { null_value: NULL_VALUE }
                              )"));
}

TEST_F(AccessProtosTest, CanConvertRowCursorToPartialResultSetWithLimit) {
  TestRowCursor cursor(
      {"int64", "bool", "double", "string", "string"},
      {Int64Type(), BoolType(), DoubleType(), StringType(), StringType()},
      {{Int64(1), Bool(true), Double(1.234), String("Test"), NullString()},
       {Int64(2), Bool(true), Double(0.0), String(""), NullString()},
       {Int64(3), Bool(false), Double(-9.999), String("Key:Value"),
        NullString()}});
  std::vector<PartialResultSet> results;
  ZETASQL_ASSERT_OK_AND_ASSIGN(results, RowCursorToPartialResultSetProtos(&cursor, 1));

  EXPECT_THAT(results[0], test::EqualsProto(
                              R"(metadata {
                                   row_type {
                                     fields {
                                       name: "int64"
                                       type { code: INT64 }
                                     }
                                     fields {
                                       name: "bool"
                                       type { code: BOOL }
                                     }
                                     fields {
                                       name: "double"
                                       type { code: FLOAT64 }
                                     }
                                     fields {
                                       name: "string"
                                       type { code: STRING }
                                     }
                                     fields {
                                       name: "string"
                                       type { code: STRING }
                                     }
                                   }
                                 }
                                 values { string_value: "1" }
                                 values { bool_value: true }
                                 values { number_value: 1.234 }
                                 values { string_value: "Test" }
                                 values { null_value: NULL_VALUE }
                              )"));
}

TEST_F(AccessProtosTest, CanConvertEmptyRowCursorToResultSet) {
  const zetasql::Type* struct_array;
  ZETASQL_EXPECT_OK(type_factory_->MakeStructTypeFromVector(
      {StructField("element1", Int64Type()),
       StructField("element2", Int64Type()),
       StructField("element3", Int64Type())},
      &struct_array));

  TestRowCursor cursor(
      {"int64", "bool", "double", "string", "bytes", "timestamp", "date",
       "array", "struct"},
      {Int64Type(), BoolType(), DoubleType(), StringType(), BytesType(),
       TimestampType(), DateType(), Int64ArrayType(), struct_array},
      {});
  ResultSet result_pb;
  ZETASQL_EXPECT_OK(RowCursorToResultSetProto(&cursor, 0, &result_pb));

  EXPECT_THAT(result_pb, test::EqualsProto(
                             R"(metadata {
                                  row_type {
                                    fields {
                                      name: "int64"
                                      type { code: INT64 }
                                    }
                                    fields {
                                      name: "bool"
                                      type { code: BOOL }
                                    }
                                    fields {
                                      name: "double"
                                      type { code: FLOAT64 }
                                    }
                                    fields {
                                      name: "string"
                                      type { code: STRING }
                                    }
                                    fields {
                                      name: "bytes"
                                      type { code: BYTES }
                                    }
                                    fields {
                                      name: "timestamp"
                                      type { code: TIMESTAMP }
                                    }
                                    fields {
                                      name: "date"
                                      type { code: DATE }
                                    }
                                    fields {
                                      name: "array"
                                      type {
                                        code: ARRAY
                                        array_element_type { code: INT64 }
                                      }
                                    }
                                    fields {
                                      name: "struct"
                                      type {
                                        code: STRUCT
                                        struct_type {
                                          fields {
                                            name: "element1"
                                            type { code: INT64 }
                                          }
                                          fields {
                                            name: "element2"
                                            type { code: INT64 }
                                          }
                                          fields {
                                            name: "element3"
                                            type { code: INT64 }
                                          }
                                        }
                                      }
                                    }
                                  }
                                }
                             )"));
}

TEST_F(AccessProtosTest, CannotConvertInvalidRowCursorToResultSet) {
  ResultSet result_pb;

  TestRowCursor cursor1({"col1"}, {GeographyType()}, {});
  EXPECT_THAT(RowCursorToResultSetProto(&cursor1, 0, &result_pb),
              StatusIs(absl::StatusCode::kInternal));

  TestRowCursor cursor3({"col1"}, {DatetimeType()}, {});
  EXPECT_THAT(RowCursorToResultSetProto(&cursor3, 0, &result_pb),
              StatusIs(absl::StatusCode::kInternal));

  // Test invalid value.
  TestRowCursor cursor4({"col1"}, {Int64Type()},
                        {{Value(Datetime(zetasql::DatetimeValue()))}});
  EXPECT_THAT(RowCursorToResultSetProto(&cursor4, 0, &result_pb),
              StatusIs(absl::StatusCode::kInternal));
}

TEST_F(AccessProtosTest, CanReadArgsFromProto) {
  // Creates a ReadRequest with one key and one key range.
  ReadRequest request = PARSE_TEXT_PROTO(R"(
    table: "test_table"
    columns: "int64_col"
    key_set {
      keys { values { string_value: "123" } }
      ranges {
        start_closed { values { string_value: "456" } }
        end_open { values { string_value: "789" } }
      }
    }
  )");
  backend::ReadArg read_arg;
  ZETASQL_EXPECT_OK(ReadArgFromProto(*schema_.get(), request, &read_arg));

  // Verify the populated ReadArg
  EXPECT_EQ(read_arg.table, request.table());
  EXPECT_THAT(read_arg.columns,
              testing::Pointwise(testing::Eq(), request.columns()));

  auto key = read_arg.key_set.keys()[0];
  EXPECT_EQ(key.NumColumns(), 1);
  EXPECT_EQ(key.ColumnValue(0), zetasql::values::Int64(123));

  auto key_range = read_arg.key_set.ranges()[0];
  EXPECT_EQ(key_range.start_type(), backend::EndpointType::kClosed);
  EXPECT_EQ(key_range.start_key().NumColumns(), 1);
  EXPECT_EQ(key_range.start_key().ColumnValue(0),
            zetasql::values::Int64(456));
  EXPECT_EQ(key_range.limit_type(), backend::EndpointType::kOpen);
  EXPECT_EQ(key_range.limit_key().NumColumns(), 1);
  EXPECT_EQ(key_range.limit_key().ColumnValue(0),
            zetasql::values::Int64(789));
}

TEST_F(AccessProtosTest, CannotReadArgsFromProtoWithNoKeySet) {
  // Creates a ReadRequest with key_set not populated.
  ReadRequest request = PARSE_TEXT_PROTO(R"(
    table: "test_table"
    columns: "int64_col"
  )");
  backend::ReadArg read_arg;
  EXPECT_THAT(ReadArgFromProto(*schema_.get(), request, &read_arg),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(AccessProtosTest, CanReadArgsFromProtoWithEmptyKeySet) {
  // Creates a ReadRequest with an empty key_set.
  ReadRequest request = PARSE_TEXT_PROTO(R"(
    table: "test_table"
    columns: "int64_col"
    key_set {}
  )");
  backend::ReadArg read_arg;
  ZETASQL_EXPECT_OK(ReadArgFromProto(*schema_.get(), request, &read_arg));
  // Verify the populated ReadArg
  EXPECT_EQ(read_arg.table, request.table());
  EXPECT_THAT(read_arg.columns,
              testing::Pointwise(testing::Eq(), request.columns()));
  EXPECT_TRUE(read_arg.key_set.keys().empty());
  EXPECT_TRUE(read_arg.key_set.ranges().empty());
}

TEST_F(AccessProtosTest, CannotReadArgsFromProtoWithInvalidTable) {
  ReadRequest request = PARSE_TEXT_PROTO(R"(
    table: "nonexist_table"
    columns: "int64_col"
    key_set {
      keys { values { string_value: "123" } }
      ranges {
        start_closed { values { string_value: "456" } }
        end_open { values { string_value: "789" } }
      }
    }
  )");
  backend::ReadArg read_arg;
  EXPECT_THAT(ReadArgFromProto(*schema_.get(), request, &read_arg),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(AccessProtosTest,
       CannotReadArgsFromProtoWithIncorrectNumberOfColumnsInPrimaryKey) {
  ReadRequest request = PARSE_TEXT_PROTO(R"(
    table: "test_table"
    columns: "int64_col"
    key_set {
      keys {
        values { string_value: "123" }
        values { string_value: "abc" }
      }
      ranges {
        start_closed { values { string_value: "456" } }
        end_open { values { string_value: "789" } }
      }
    }
  )");
  backend::ReadArg read_arg;
  EXPECT_THAT(ReadArgFromProto(*schema_.get(), request, &read_arg),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(AccessProtosTest, ParsesAllKeySet) {
  ReadRequest request = PARSE_TEXT_PROTO(R"(
    table: "test_table"
    columns: "int64_col"
    key_set { all: true }
  )");
  backend::ReadArg read_arg;
  ZETASQL_EXPECT_OK(ReadArgFromProto(*schema_.get(), request, &read_arg));
  EXPECT_EQ("test_table", read_arg.table);
  ASSERT_EQ(1, read_arg.columns.size());
  EXPECT_EQ("int64_col", read_arg.columns[0]);
  EXPECT_EQ(0, read_arg.key_set.keys().size());
  ASSERT_EQ(1, read_arg.key_set.ranges().size());
  EXPECT_EQ(backend::KeyRange::All(), read_arg.key_set.ranges()[0]);
}

TEST_F(AccessProtosTest, ReadIndex) {
  ReadRequest request = PARSE_TEXT_PROTO(R"(
    table: "test_table"
    index: "test_index"
    columns: "string_col"
    key_set {
      keys { values { string_value: "123" } }
      ranges {
        start_closed { values { string_value: "456" } }
        end_open { values { string_value: "789" } }
      }
    }
  )");
  backend::ReadArg read_arg;
  ZETASQL_EXPECT_OK(ReadArgFromProto(*schema_.get(), request, &read_arg));

  // Verify the populated ReadArg
  EXPECT_EQ(read_arg.table, request.table());
  EXPECT_EQ(read_arg.index, request.index());
  EXPECT_THAT(read_arg.columns,
              testing::Pointwise(testing::Eq(), request.columns()));

  auto key = read_arg.key_set.keys()[0];
  EXPECT_EQ(key.NumColumns(), 1);
  EXPECT_EQ(key.ColumnValue(0), zetasql::values::String("123"));
  EXPECT_TRUE(key.IsColumnDescending(0));

  auto key_range = read_arg.key_set.ranges()[0];
  EXPECT_EQ(key_range.start_type(), backend::EndpointType::kClosed);
  EXPECT_EQ(key_range.start_key().NumColumns(), 1);
  EXPECT_EQ(key_range.start_key().ColumnValue(0),
            zetasql::values::String("456"));
  EXPECT_TRUE(key_range.start_key().IsColumnDescending(0));
  EXPECT_EQ(key_range.limit_type(), backend::EndpointType::kOpen);
  EXPECT_EQ(key_range.limit_key().NumColumns(), 1);
  EXPECT_EQ(key_range.limit_key().ColumnValue(0),
            zetasql::values::String("789"));
  EXPECT_TRUE(key_range.limit_key().IsColumnDescending(0));
}

}  // namespace

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
