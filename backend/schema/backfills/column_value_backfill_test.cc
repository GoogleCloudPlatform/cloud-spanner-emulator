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

#include "backend/schema/backfills/column_value_backfill.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "backend/database/database.h"
#include "backend/datamodel/key_set.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/updater/schema_updater.h"
#include "backend/transaction/options.h"
#include "common/errors.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/common/proto_matchers.h"
#include "tests/common/test.pb.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

using zetasql::values::Array;
using zetasql::values::Bytes;
using zetasql::values::Int64;
using zetasql::values::Null;
using zetasql::values::NullBytes;
using zetasql::values::NullString;
using zetasql::values::String;
using zetasql::values::BytesArray;
using zetasql::values::Enum;
using zetasql::values::Proto;

constexpr char kDatabaseId[] = "test-db";

class ColumnValueBackfillTest : public ::testing::Test {
 public:
  ColumnValueBackfillTest()
      : emulator_feature_flags_({.enable_column_default_values = true}) {}

 protected:
  void SetUp() override {
    std::vector<std::string> create_statements = {R"(
                            CREATE TABLE TestTable (
                              int64_col INT64,
                              string_col STRING(10),
                              string_array_col ARRAY<STRING(MAX)>,
                              bytes_array_col ARRAY<BYTES(MAX)>
                            ) PRIMARY KEY (int64_col)
                         )"};
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        database_, Database::Create(
                       &clock_, kDatabaseId,
                       SchemaChangeOperation{.statements = create_statements}));

    ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadWriteTransaction> txn,
                         database_->CreateReadWriteTransaction(
                             ReadWriteOptions(), RetryState()));

    const auto* test_table = txn->schema()->FindTable("TestTable");
    ASSERT_NE(test_table, nullptr);

    string_array_type_ =
        test_table->FindColumn("string_array_col")->GetType()->AsArray();
    bytes_array_type_ =
        test_table->FindColumn("bytes_array_col")->GetType()->AsArray();

    Mutation m;
    m.AddWriteOp(
        MutationOpType::kInsert, "TestTable",
        {"int64_col", "string_col", "string_array_col"},
        {{Int64(1), String("ФдΣβaA"),
          Array(string_array_type_, {String("ФдΣβaA"), NullString()})}});

    // Add a row with null values as well.
    m.AddWriteOp(MutationOpType::kInsert, "TestTable", {"int64_col"},
                 {{Int64(2)}});

    ZETASQL_EXPECT_OK(txn->Write(m));
    ZETASQL_EXPECT_OK(txn->Commit());
  }

  absl::Status UpdateSchema(absl::Span<const std::string> update_statements) {
    int num_succesful;
    absl::Status backfill_status;
    absl::Time update_time;
    ZETASQL_RETURN_IF_ERROR(database_->UpdateSchema(
        SchemaChangeOperation{.statements = update_statements}, &num_succesful,
        &update_time, &backfill_status));
    return backfill_status;
  }

  std::vector<zetasql::Value> ColumnValues(const std::string& column_name) {
    absl::StatusOr<std::unique_ptr<ReadOnlyTransaction>> status_or =
        database_->CreateReadOnlyTransaction(ReadOnlyOptions());
    ZETASQL_EXPECT_OK(status_or.status());

    auto txn = std::move(status_or.value());
    std::unique_ptr<backend::RowCursor> cursor;
    ReadArg args;
    args.table = "TestTable";
    args.key_set = KeySet::All();
    args.columns = std::vector<std::string>{column_name};
    ZETASQL_EXPECT_OK(txn->Read(args, &cursor));

    std::vector<zetasql::Value> values;
    while (cursor->Next()) {
      values.push_back(cursor->ColumnValue(0));
    }
    return values;
  }

  Clock clock_;
  std::unique_ptr<Database> database_;
  const zetasql::ArrayType* string_array_type_ = nullptr;
  const zetasql::ArrayType* bytes_array_type_ = nullptr;
  emulator::test::ScopedEmulatorFeatureFlagsSetter emulator_feature_flags_;
};

TEST_F(ColumnValueBackfillTest, FailedBackfillHasNoEffect) {
  // Failed column type change.
  EXPECT_EQ(
      UpdateSchema({R"(
    ALTER TABLE TestTable ALTER COLUMN string_col BYTES(7)
    )"}),
      error::InvalidColumnSizeReduction("string_col", 7, 10, "{Int64(1)}"));

  // Check that the column values are unchanged.
  EXPECT_THAT(ColumnValues("string_col"), testing::ElementsAreArray({
                                              String("ФдΣβaA"),
                                              NullString(),
                                          }));
}

TEST_F(ColumnValueBackfillTest, BackfillAtomicType) {
  ZETASQL_EXPECT_OK(UpdateSchema({R"(
    ALTER TABLE TestTable ALTER COLUMN string_col BYTES(10)
    )"}));

  EXPECT_THAT(ColumnValues("string_col"), testing::ElementsAreArray({
                                              Bytes("ФдΣβaA"),
                                              NullBytes(),
                                          }));
}

TEST_F(ColumnValueBackfillTest, BackfillArrayType) {
  ZETASQL_EXPECT_OK(UpdateSchema({R"(
    ALTER TABLE TestTable ALTER COLUMN string_array_col ARRAY<BYTES(10)>
    )"}));

  EXPECT_THAT(ColumnValues("string_array_col"),
              testing::ElementsAreArray({
                  Array(bytes_array_type_, {Bytes("ФдΣβaA"), NullBytes()}),
                  Null(bytes_array_type_),
              }));
}

TEST_F(ColumnValueBackfillTest, BackfillGeneratedColumn) {
  ZETASQL_ASSERT_OK(UpdateSchema({R"(
    ALTER TABLE TestTable
      ADD COLUMN gen_col_1 STRING(MAX) AS (string_col) STORED
    )"}));

  EXPECT_THAT(ColumnValues("gen_col_1"), testing::ElementsAreArray({
                                             String("ФдΣβaA"),
                                             NullString(),
                                         }));
  ZETASQL_ASSERT_OK(UpdateSchema({R"(
    ALTER TABLE TestTable
      ADD COLUMN gen_col_2 STRING(MAX) AS (gen_col_1) STORED
    )"}));
  EXPECT_THAT(ColumnValues("gen_col_2"), testing::ElementsAreArray({
                                             String("ФдΣβaA"),
                                             NullString(),
                                         }));
}

TEST_F(ColumnValueBackfillTest, BackfillGeneratedColumnNotNullFail) {
  EXPECT_THAT(
      UpdateSchema({R"(
    ALTER TABLE TestTable
      ADD COLUMN gen_col STRING(MAX) NOT NULL AS (string_col) STORED
    )"}),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kFailedPrecondition,
          testing::HasSubstr("Cannot specify a null value for column")));
}

TEST_F(ColumnValueBackfillTest, BackfillDefaultColumn) {
  ZETASQL_ASSERT_OK(UpdateSchema({R"(
    ALTER TABLE TestTable
      ADD COLUMN default_col INT64 DEFAULT (1+1)
    )"}));

  EXPECT_THAT(ColumnValues("default_col"), testing::ElementsAreArray({
                                               Int64(2),
                                               Int64(2),
                                           }));
  ZETASQL_ASSERT_OK(UpdateSchema({R"(
    ALTER TABLE TestTable
      ADD COLUMN default_col_2 STRING(MAX) DEFAULT ("Hello")
    )"}));
  EXPECT_THAT(ColumnValues("default_col_2"), testing::ElementsAreArray({
                                                 String("Hello"),
                                                 String("Hello"),
                                             }));
}

TEST_F(ColumnValueBackfillTest, BackfillDefaultColumnNotNullFail) {
  EXPECT_THAT(
      UpdateSchema({R"(
    ALTER TABLE TestTable
      ADD COLUMN default_col STRING(MAX) NOT NULL DEFAULT (NULL)
    )"}),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kFailedPrecondition,
          testing::HasSubstr("Cannot specify a null value for column")));
}

class ProtoColumnValueBackfillTest : public ColumnValueBackfillTest {
 protected:
  std::string read_descriptors() {
    google::protobuf::FileDescriptorSet proto_files;
    ::emulator::tests::common::Simple::descriptor()->file()->CopyTo(
        proto_files.add_file());
    return proto_files.SerializeAsString();
  }

  void SetUp() override {
    // Serialized value of `emulator.tests.common.Simple{field: "TestValue"}`
    // has the byte length of 11. Since byte columns have to be at least this
    // wide to be able to store this test value, we use 11 to define the length
    // of the byte columns below.
    std::vector<std::string> statements = {
        R"sql(
                            CREATE PROTO BUNDLE (
                              emulator.tests.common.Simple,
                              emulator.tests.common.Parent,
                              emulator.tests.common.TestEnum,
                              emulator.tests.common.SampleEnum,
                              emulator.tests.common.EnumContainer,
                            )

                          )sql",
        R"sql(
                            CREATE TABLE TestTable (
                              int64_col INT64,
                              array_int64_col ARRAY<INT64>,
                              proto_col emulator.tests.common.Simple,
                              enum_col emulator.tests.common.TestEnum,
                              proto_array_col ARRAY<emulator.tests.common.Simple>,
                              enum_array_col ARRAY<emulator.tests.common.TestEnum>,
                              bytes_col BYTES(11),
                              bytes_array_col ARRAY<BYTES(11)>,
                            ) PRIMARY KEY (int64_col)
                          )sql"};
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        database_,
        Database::Create(&clock_, kDatabaseId,
                         SchemaChangeOperation{
                             .statements = statements,
                             .proto_descriptor_bytes = read_descriptors()}));

    ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadWriteTransaction> txn,
                         database_->CreateReadWriteTransaction(
                             ReadWriteOptions(), RetryState()));
    const auto* test_table = txn->schema()->FindTable("TestTable");
    ASSERT_NE(test_table, nullptr);

    proto_type_ = test_table->FindColumn("proto_col")->GetType()->AsProto();
    array_proto_type_ =
        test_table->FindColumn("proto_array_col")->GetType()->AsArray();
    enum_type_ = test_table->FindColumn("enum_col")->GetType()->AsEnum();
    array_enum_type_ =
        test_table->FindColumn("enum_array_col")->GetType()->AsArray();

    Mutation m;
    m.AddWriteOp(
        MutationOpType::kInsert, "TestTable",
        {"int64_col", "array_int64_col", "proto_col", "enum_col",
         "proto_array_col", "enum_array_col", "bytes_col", "bytes_array_col"},
        {{Int64(1), zetasql::values::Int64Array({1}),
          Proto(proto_type_, kSimpleProtoTestValue),
          Enum(enum_type_, ::emulator::tests::common::TEST_ENUM_ONE),
          Array(array_proto_type_, {Proto(proto_type_, kSimpleProtoTestValue)}),
          Array(array_enum_type_,
                {Enum(enum_type_, ::emulator::tests::common::TEST_ENUM_ONE)}),
          Bytes(kSerializedProtoTestValue),
          BytesArray({kSerializedProtoTestValue})}});
    // Add a row with null values as well.
    m.AddWriteOp(MutationOpType::kInsert, "TestTable", {"int64_col"},
                 {{Int64(2)}});
    ZETASQL_ASSERT_OK(txn->Write(m));
    ZETASQL_ASSERT_OK(txn->Commit());
  }
  const zetasql::ProtoType* proto_type_ = nullptr;
  const zetasql::ArrayType* array_proto_type_ = nullptr;
  const ::emulator::tests::common::Simple kSimpleProtoTestValue =
      PARSE_TEXT_PROTO(R"pb(field: "TestValue"
      )pb");
  const std::string kSerializedProtoTestValue =
      kSimpleProtoTestValue.SerializeAsString();
  const std::string kRandomBytesValue = "RandomBytes";
  const zetasql::EnumType* enum_type_ = nullptr;
  const zetasql::ArrayType* array_enum_type_ = nullptr;
};

TEST_F(ProtoColumnValueBackfillTest, BackfillProtoToBytes) {
  ZETASQL_EXPECT_OK(UpdateSchema({R"sql(
    ALTER TABLE TestTable ALTER COLUMN proto_col BYTES(11)
    )sql"}));

  EXPECT_THAT(ColumnValues("proto_col"), testing::ElementsAreArray({
                                             Bytes(kSerializedProtoTestValue),
                                             NullBytes(),
                                         }));
}

TEST_F(ProtoColumnValueBackfillTest, BackfillBytesToProto) {
  ZETASQL_EXPECT_OK(UpdateSchema({R"sql(
    ALTER TABLE TestTable ALTER COLUMN bytes_col emulator.tests.common.Simple
    )sql"}));
  EXPECT_THAT(ColumnValues("bytes_col"),
              testing::ElementsAreArray({
                  Proto(proto_type_, kSimpleProtoTestValue),
                  zetasql::Value::Null(proto_type_),
              }));
}

TEST_F(ProtoColumnValueBackfillTest, BackfillRandomBytesToProto) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ReadWriteTransaction> txn,
      database_->CreateReadWriteTransaction(ReadWriteOptions(), RetryState()));
  Mutation m;
  m.AddWriteOp(MutationOpType::kInsert, "TestTable", {"int64_col", "bytes_col"},
               {{Int64(3), Bytes(kRandomBytesValue)}});
  ZETASQL_ASSERT_OK(txn->Write(m));
  ZETASQL_ASSERT_OK(txn->Commit());

  ZETASQL_EXPECT_OK(UpdateSchema({
      R"sql(
    ALTER TABLE TestTable ALTER COLUMN bytes_col emulator.tests.common.Simple
    )sql"}));
  std::vector<zetasql::Value> values = ColumnValues("bytes_col");
  EXPECT_EQ(values[2].FullDebugString(),
            "Proto<emulator.tests.common.Simple>{<unparseable>}");
}

TEST_F(ProtoColumnValueBackfillTest,
       BackfillProtoToBytesFailsWithInvalidBytesSize) {
  EXPECT_EQ(
      UpdateSchema({R"sql(
    ALTER TABLE TestTable ALTER COLUMN proto_col BYTES(1)
    )sql"}),
      error::InvalidColumnSizeReduction("proto_col", /*specified_length*/ 1,
                                        /*existing_length*/ 11, "{Int64(1)}"));

  // Check that the column values are unchanged.
  EXPECT_THAT(ColumnValues("proto_col"),
              testing::ElementsAreArray({
                  Proto(proto_type_, kSimpleProtoTestValue),
                  zetasql::Value::Null(proto_type_),
              }));
}

TEST_F(ProtoColumnValueBackfillTest, BackfillArrayProtoToArrayBytes) {
  ZETASQL_EXPECT_OK(UpdateSchema({R"sql(
    ALTER TABLE TestTable ALTER COLUMN proto_array_col ARRAY<BYTES(11)>
    )sql"}));

  EXPECT_THAT(ColumnValues("proto_array_col"),
              testing::ElementsAreArray({
                  BytesArray({kSerializedProtoTestValue}),
                  zetasql::Value::Null(zetasql::types::BytesArrayType()),
              }));
}

TEST_F(ProtoColumnValueBackfillTest, BackfillArrayBytesToArrayProto) {
  ZETASQL_EXPECT_OK(UpdateSchema({R"sql(
    ALTER TABLE TestTable ALTER COLUMN bytes_array_col ARRAY<emulator.tests.common.Simple>
    )sql"}));
  EXPECT_THAT(
      ColumnValues("bytes_array_col"),
      testing::ElementsAreArray({
          Array(array_proto_type_, {Proto(proto_type_, kSimpleProtoTestValue)}),
          zetasql::Value::Null(array_proto_type_),
      }));
}

TEST_F(ProtoColumnValueBackfillTest, FailsBackfillProtoToString) {
  EXPECT_THAT(UpdateSchema({R"sql(
    ALTER TABLE TestTable ALTER COLUMN proto_col STRING(30)
    )sql"}),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  testing::HasSubstr("Cannot change type of column")));
}

TEST_F(ProtoColumnValueBackfillTest, FailsBackfillArrayProtoToBytes) {
  EXPECT_THAT(UpdateSchema({R"sql(
    ALTER TABLE TestTable ALTER COLUMN proto_array_col BYTES(30)
    )sql"}),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  testing::HasSubstr("Cannot change type of column")));
}

TEST_F(ProtoColumnValueBackfillTest, FailsBackfillArrayBytesToProto) {
  EXPECT_THAT(UpdateSchema({R"sql(
    ALTER TABLE TestTable ALTER COLUMN bytes_array_col emulator.tests.common.Simple
    )sql"}),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  testing::HasSubstr("Cannot change type of column")));
}

TEST_F(ProtoColumnValueBackfillTest, FailsBackfillIntToProto) {
  EXPECT_THAT(UpdateSchema({R"sql(
    ALTER TABLE TestTable ALTER COLUMN int64_col emulator.tests.common.Simple
    )sql"}),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  testing::HasSubstr("Cannot change type of column")));
}

TEST_F(ProtoColumnValueBackfillTest, BackfillEnumToInt) {
  ZETASQL_EXPECT_OK(UpdateSchema({R"sql(
    ALTER TABLE TestTable ALTER COLUMN enum_col INT64
    )sql"}));
  EXPECT_THAT(ColumnValues("enum_col"),
              testing::ElementsAreArray({
                  Int64(1),
                  zetasql::Value::Null(zetasql::types::Int64Type()),
              }));
}

TEST_F(ProtoColumnValueBackfillTest, BackfillIntToEnum) {
  ZETASQL_EXPECT_OK(UpdateSchema({R"sql(
    ALTER TABLE TestTable ALTER COLUMN int64_col emulator.tests.common.TestEnum
    )sql"}));
  EXPECT_THAT(ColumnValues("int64_col"),
              testing::ElementsAreArray({
                  Enum(enum_type_, ::emulator::tests::common::TEST_ENUM_ONE),
                  Enum(enum_type_, ::emulator::tests::common::TEST_ENUM_TWO),
              }));
}

TEST_F(ProtoColumnValueBackfillTest, BackfillArrayEnumToArrayInt) {
  ZETASQL_EXPECT_OK(UpdateSchema({R"sql(
    ALTER TABLE TestTable ALTER COLUMN enum_array_col ARRAY<INT64>
    )sql"}));
  EXPECT_THAT(ColumnValues("enum_array_col"),
              testing::ElementsAreArray({
                  zetasql::values::Int64Array({1}),
                  zetasql::Value::Null(zetasql::types::Int64ArrayType()),
              }));
}

TEST_F(ProtoColumnValueBackfillTest, BackfillArrayIntToArrayEnum) {
  ZETASQL_EXPECT_OK(UpdateSchema({R"sql(
    ALTER TABLE TestTable ALTER COLUMN array_int64_col ARRAY<emulator.tests.common.TestEnum>
    )sql"}));
  EXPECT_THAT(
      ColumnValues("array_int64_col"),
      testing::ElementsAreArray({
          Array(array_enum_type_,
                {Enum(enum_type_, ::emulator::tests::common::TEST_ENUM_ONE)}),
          zetasql::Value::Null(array_enum_type_),
      }));
}

TEST_F(ProtoColumnValueBackfillTest, FailsBackfillArrayEnumToInt) {
  EXPECT_THAT(UpdateSchema({R"sql(
    ALTER TABLE TestTable ALTER COLUMN enum_array_col INT64
    )sql"}),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  testing::HasSubstr("Cannot change type of column")));
}

TEST_F(ProtoColumnValueBackfillTest, FailsBackfillArrayIntToEnum) {
  EXPECT_THAT(UpdateSchema({R"sql(
    ALTER TABLE TestTable ALTER COLUMN array_int64_col emulator.tests.common.TestEnum
    )sql"}),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  testing::HasSubstr("Cannot change type of column")));
}

TEST_F(ProtoColumnValueBackfillTest, FailsBackfillArrayEnumToEnum) {
  EXPECT_THAT(UpdateSchema({R"sql(
    ALTER TABLE TestTable ALTER COLUMN enum_array_col emulator.tests.common.TestEnum
    )sql"}),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  testing::HasSubstr("Cannot change type of column")));
}

TEST_F(ProtoColumnValueBackfillTest, FailsBackfillEnumToString) {
  EXPECT_THAT(UpdateSchema({R"sql(
    ALTER TABLE TestTable ALTER COLUMN enum_col STRING(30)
    )sql"}),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  testing::HasSubstr("Cannot change type of column")));
}

TEST_F(ProtoColumnValueBackfillTest, BackfillProtoColToAnotherProtoCol) {
  // emulator.tests.common.Simple to emulator.tests.common.Parent conversion is
  // compatible based on
  // https://developers.google.com/protocol-buffers/docs/proto#updating
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"sql(
    ALTER TABLE TestTable ALTER COLUMN proto_col emulator.tests.common.Parent
    )sql"}));
  const auto* test_table = database_->GetLatestSchema()->FindTable("TestTable");
  ASSERT_NE(test_table, nullptr);
  auto proto_type = test_table->FindColumn("proto_col")->GetType()->AsProto();
  ::emulator::tests::common::Parent parent_proto;
  parent_proto.set_field("TestValue");
  EXPECT_THAT(ColumnValues("proto_col"), testing::ElementsAreArray({
                                             Proto(proto_type, parent_proto),
                                             zetasql::Value::Null(proto_type),
                                         }));
}

TEST_F(ProtoColumnValueBackfillTest, BackfillEnumColToAnotherEnumCol) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ReadWriteTransaction> txn,
      database_->CreateReadWriteTransaction(ReadWriteOptions(), RetryState()));
  Mutation m;
  m.AddWriteOp(MutationOpType::kInsert, "TestTable", {"int64_col", "enum_col"},
               {{Int64(3),
                 zetasql::values::Enum(
                     enum_type_, ::emulator::tests::common::TEST_ENUM_THREE)}});
  ZETASQL_ASSERT_OK(txn->Write(m));
  ZETASQL_ASSERT_OK(txn->Commit());
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"sql(
    ALTER TABLE TestTable ALTER COLUMN enum_col emulator.tests.common.SampleEnum
    )sql"}));
  const auto* test_table = database_->GetLatestSchema()->FindTable("TestTable");
  ASSERT_NE(test_table, nullptr);
  auto updated_enum_type =
      test_table->FindColumn("enum_col")->GetType()->AsEnum();

  EXPECT_THAT(
      ColumnValues("enum_col"),
      testing::ElementsAreArray({
          Enum(updated_enum_type, ::emulator::tests::common::SAMPLE_ENUM_ONE),
          zetasql::Value::Null(updated_enum_type),
          zetasql::Value::Null(updated_enum_type),
      }));
}

TEST_F(ProtoColumnValueBackfillTest, BackfillProtoColToIncompatibleProto) {
  // "`Simple` and `EnumContainer` are not compatible based on
  // https://developers.google.com/protocol-buffers/docs/proto#updating.
  // Backfill does not fail, but the column value would be garbage"
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"sql(
    ALTER TABLE TestTable ALTER COLUMN proto_col emulator.tests.common.EnumContainer
    )sql"}));
  std::vector<zetasql::Value> values = ColumnValues("proto_col");
  EXPECT_EQ(values[0].FullDebugString(),
            "Proto<emulator.tests.common.EnumContainer>{1: \"TestValue\"\n}");
  EXPECT_EQ(values[1].FullDebugString(),
            "Proto<emulator.tests.common.EnumContainer>(NULL)");
}


}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
