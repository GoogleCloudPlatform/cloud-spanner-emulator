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

#include "frontend/converters/mutations.h"

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
    // And thus should have exactly one table and name should be "test_table".
    ASSERT_EQ(schema_->tables().size(), 1);
    ASSERT_EQ(schema_->tables()[0]->Name(), "test_table");
  }

  // The type factory must outlive the type objects that it has made.
  const std::unique_ptr<zetasql::TypeFactory> type_factory_;
  const std::unique_ptr<const backend::Schema> schema_;
};

TEST_F(AccessProtosTest, CanCreateMutationFromProto) {
  google::protobuf::RepeatedPtrField<google::spanner::v1::Mutation> mutation_pb;

  google::spanner::v1::Mutation::Write insert = PARSE_TEXT_PROTO(R"(
    table: "test_table"
    columns: "int64_col"
    values { values { string_value: "123" } }
  )");
  *mutation_pb.Add()->mutable_insert() = insert;
  google::spanner::v1::Mutation::Write update = PARSE_TEXT_PROTO(R"(
    table: "test_table"
    columns: "int64_col"
    values { values { string_value: "456" } }
  )");
  *mutation_pb.Add()->mutable_update() = update;
  google::spanner::v1::Mutation::Write insert_or_update = PARSE_TEXT_PROTO(R"(
    table: "test_table"
    columns: "int64_col"
    values { values { string_value: "789" } }
  )");
  *mutation_pb.Add()->mutable_insert_or_update() = insert_or_update;
  google::spanner::v1::Mutation::Write replace = PARSE_TEXT_PROTO(R"(
    table: "test_table"
    columns: "int64_col"
    values { values { string_value: "1111" } }
  )");
  *mutation_pb.Add()->mutable_replace() = replace;
  google::spanner::v1::Mutation::Delete delete_mutation = PARSE_TEXT_PROTO(R"(
    table: "test_table"
    key_set {
      keys { values { string_value: "123" } }
      ranges {
        start_closed { values { string_value: "456" } }
        end_open { values { string_value: "789" } }
      }
    }
  )");
  *mutation_pb.Add()->mutable_delete_() = delete_mutation;

  backend::Mutation mutation;
  ZETASQL_EXPECT_OK(MutationFromProto(*schema_.get(), mutation_pb, &mutation));

  EXPECT_EQ(mutation.ops().size(), 5);
  EXPECT_EQ(mutation.ops()[0].type, backend::MutationOpType::kInsert);
  EXPECT_EQ(mutation.ops()[1].type, backend::MutationOpType::kUpdate);
  EXPECT_EQ(mutation.ops()[2].type, backend::MutationOpType::kInsertOrUpdate);
  EXPECT_EQ(mutation.ops()[3].type, backend::MutationOpType::kReplace);
  EXPECT_EQ(mutation.ops()[4].type, backend::MutationOpType::kDelete);

  // Check ops values.
  EXPECT_EQ(mutation.ops()[0].columns.size(), 1);
  EXPECT_EQ(mutation.ops()[0].rows.size(), 1);
  EXPECT_EQ(mutation.ops()[0].columns[0], "int64_col");
  EXPECT_EQ(mutation.ops()[0].rows[0][0], zetasql::values::Int64(123));

  EXPECT_EQ(mutation.ops()[1].columns.size(), 1);
  EXPECT_EQ(mutation.ops()[1].rows.size(), 1);
  EXPECT_EQ(mutation.ops()[1].columns[0], "int64_col");
  EXPECT_EQ(mutation.ops()[1].rows[0][0], zetasql::values::Int64(456));

  EXPECT_EQ(mutation.ops()[2].columns.size(), 1);
  EXPECT_EQ(mutation.ops()[2].rows.size(), 1);
  EXPECT_EQ(mutation.ops()[2].columns[0], "int64_col");
  EXPECT_EQ(mutation.ops()[2].rows[0][0], zetasql::values::Int64(789));

  EXPECT_EQ(mutation.ops()[3].columns.size(), 1);
  EXPECT_EQ(mutation.ops()[3].rows.size(), 1);
  EXPECT_EQ(mutation.ops()[3].columns[0], "int64_col");
  EXPECT_EQ(mutation.ops()[3].rows[0][0], zetasql::values::Int64(1111));

  EXPECT_EQ(mutation.ops()[4].columns.size(), 0);
  EXPECT_EQ(mutation.ops()[4].rows.size(), 0);
  EXPECT_EQ(mutation.ops()[4].key_set.DebugString(),
            "Key{Int64(123)}, Range[{Int64(456)} ... {Int64(789)})");
}

TEST_F(AccessProtosTest, CannotCreateMutationFromInvalidProto) {
  google::protobuf::RepeatedPtrField<google::spanner::v1::Mutation> mutation_pb;

  google::spanner::v1::Mutation::Write invalid_table = PARSE_TEXT_PROTO(R"(
    table: "invalid"
    columns: "int64_col"
    values { values { string_value: "123" } }
  )");
  *mutation_pb.Add()->mutable_insert() = invalid_table;

  backend::Mutation mutation;
  EXPECT_THAT(MutationFromProto(*schema_.get(), mutation_pb, &mutation),
              StatusIs(absl::StatusCode::kNotFound));

  google::spanner::v1::Mutation::Write invalid_column = PARSE_TEXT_PROTO(R"(
    table: "test_table"
    columns: "invalid"
    values { values { string_value: "123" } }
  )");
  *mutation_pb.Add()->mutable_insert() = invalid_column;

  EXPECT_THAT(MutationFromProto(*schema_.get(), mutation_pb, &mutation),
              StatusIs(absl::StatusCode::kNotFound));
}

}  // namespace

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
