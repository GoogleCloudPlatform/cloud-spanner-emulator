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

#include "backend/query/query_engine.h"

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "backend/access/read.h"
#include "backend/access/write.h"
#include "backend/actions/manager.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/key_set.h"
#include "backend/datamodel/value.h"
#include "backend/query/query_context.h"
#include "backend/schema/catalog/schema.h"
#include "common/limits.h"
#include "tests/common/row_reader.h"
#include "tests/common/schema_constructor.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/common/test.pb.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_numeric_type.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

using testing::AllOf;
using testing::ElementsAre;
using testing::Field;
using testing::HasSubstr;
using testing::IsTrue;
using testing::Property;
using testing::Return;
using testing::UnorderedElementsAre;
using zetasql_base::testing::IsOkAndHolds;
using zetasql_base::testing::StatusIs;

using zetasql::values::Int64;
using zetasql::values::String;
using postgres_translator::spangres::datatypes::GetPgJsonbType;
using postgres_translator::spangres::datatypes::GetPgNumericType;

inline constexpr char kQueryContainsSubqueryError[] =
    "Query contains subquery.";

inline constexpr char kQueryNotASimpleTableScanError[] =
    "Query is not a simple table scan.";

testing::Matcher<const zetasql::Type*> Int64Type() {
  return Property(&zetasql::Type::IsInt64, IsTrue());
}

testing::Matcher<const zetasql::Type*> StringType() {
  return Property(&zetasql::Type::IsString, IsTrue());
}

testing::Matcher<const zetasql::Type*> BoolType() {
  return Property(&zetasql::Type::IsBool, IsTrue());
}

testing::Matcher<const zetasql::Type*> Float64Type() {
  return Property(&zetasql::Type::IsDouble, IsTrue());
}

testing::Matcher<const zetasql::Type*> BytesType() {
  return Property(&zetasql::Type::IsBytes, IsTrue());
}

testing::Matcher<const zetasql::Type*> TimestampType() {
  return Property(&zetasql::Type::IsTimestamp, IsTrue());
}

testing::Matcher<const zetasql::Type*> DateType() {
  return Property(&zetasql::Type::IsDate, IsTrue());
}

testing::Matcher<const zetasql::Type*> JsonType() {
  return Property(&zetasql::Type::IsJsonType, IsTrue());
}

testing::Matcher<const zetasql::Type*> NumericType() {
  return Property(&zetasql::Type::IsNumericType, IsTrue());
}

testing::Matcher<zetasql::Value> UuidV4StringValue() {
  return Property(&zetasql::Value::string_value,
                  testing::MatchesRegex("[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-["
                                        "89ab][0-9a-f]{3}-[0-9a-f]{12}"));
}

std::string ToString(QueryResult& result) {
  std::string result_string;
  RowCursor& cursor = *result.rows;
  for (int i = 0; i < cursor.NumColumns(); ++i) {
    if (i != 0) result_string += ",";
    result_string += cursor.ColumnName(i);
  }
  result_string += "(";
  for (int i = 0; i < cursor.NumColumns(); ++i) {
    if (i != 0) result_string += ",";
    result_string +=
        cursor.ColumnType(i)->ShortTypeName(zetasql::PRODUCT_INTERNAL);
  }
  result_string += ") : ";
  while (cursor.Next()) {
    for (int i = 0; i < cursor.NumColumns(); ++i) {
      result_string += (cursor.ColumnValue(i).ShortDebugString()) + ',';
    }
  }
  return result_string;
}

std::vector<std::string> GetColumnNames(const backend::RowCursor& cursor) {
  std::vector<std::string> names;
  names.reserve(cursor.NumColumns());
  for (int i = 0; i < cursor.NumColumns(); ++i) {
    names.push_back(cursor.ColumnName(i));
  }
  return names;
}

std::vector<const zetasql::Type*> GetColumnTypes(
    const backend::RowCursor& cursor) {
  std::vector<const zetasql::Type*> types;
  types.reserve(cursor.NumColumns());
  for (int i = 0; i < cursor.NumColumns(); ++i) {
    types.push_back(cursor.ColumnType(i));
  }
  return types;
}

absl::StatusOr<std::vector<std::vector<zetasql::Value>>> GetAllColumnValues(
    std::unique_ptr<backend::RowCursor> cursor) {
  std::vector<std::vector<zetasql::Value>> all_values;
  while (cursor->Next()) {
    all_values.emplace_back();
    all_values.back().reserve(cursor->NumColumns());
    for (int i = 0; i < cursor->NumColumns(); ++i) {
      all_values.back().push_back(cursor->ColumnValue(i));
    }
  }
  ZETASQL_RETURN_IF_ERROR(cursor->Status());
  return all_values;
}

std::vector<std::string> GetParamNames(const backend::QueryResult& result) {
  std::vector<std::string> names;
  names.reserve(result.parameter_types.size());
  for (const auto& param : result.parameter_types) {
    names.push_back(param.first);
  }
  return names;
}

std::vector<const zetasql::Type*> GetParamTypes(
    const backend::QueryResult& result) {
  std::vector<const zetasql::Type*> types;
  types.reserve(result.parameter_types.size());
  for (const auto& param : result.parameter_types) {
    types.push_back(param.second);
  }
  return types;
}

class QueryEngineTestBase : public testing::Test {
 public:
  const Schema* schema() { return schema_.get(); }
  const Schema* multi_table_schema() { return multi_table_schema_.get(); }
  const Schema* views_schema() { return views_schema_.get(); }
  const Schema* change_stream_schema() { return change_stream_schema_.get(); }
  const Schema* model_schema() { return model_schema_.get(); }
  const Schema* sequence_schema() { return sequence_schema_.get(); }
  const Schema* gpk_schema() { return gpk_schema_.get(); }
  RowReader* change_stream_partition_table_reader() {
    return &change_stream_partition_table_reader_;
  }
  RowReader* change_stream_data_table_reader() {
    return &change_stream_data_table_reader_;
  }
  RowReader* gpk_table_reader() { return &gpk_table_reader_; }
  RowReader* reader() { return &reader_; }
  QueryEngine& query_engine() { return query_engine_; }
  zetasql::TypeFactory* type_factory() { return &type_factory_; }
  const Schema* proto_schema() { return proto_schema_.get(); }
  std::string read_descriptors() {
    google::protobuf::FileDescriptorSet proto_files;
    ::emulator::tests::common::Simple::descriptor()->file()->CopyTo(
        proto_files.add_file());
    return proto_files.SerializeAsString();
  }

  absl::StatusOr<const zetasql::ProtoType*> MakeProtoType(
      const Schema* schema, std::string proto_type_fqn) {
    const zetasql::ProtoType* proto_type;
    ZETASQL_ASSIGN_OR_RETURN(auto descriptor,
                     schema->proto_bundle()->GetTypeDescriptor(proto_type_fqn));
    ZETASQL_RETURN_IF_ERROR(type_factory_.MakeProtoType(descriptor, &proto_type));
    return proto_type;
  }

  absl::StatusOr<const zetasql::EnumType*> MakeEnumType(
      const Schema* schema, std::string proto_type_fqn) {
    const zetasql::EnumType* enum_type;
    ZETASQL_ASSIGN_OR_RETURN(
        auto descriptor,
        schema->proto_bundle()->GetEnumTypeDescriptor(proto_type_fqn));
    ZETASQL_RETURN_IF_ERROR(type_factory_.MakeEnumType(descriptor, &enum_type));
    return enum_type;
  }
  absl::StatusOr<test::TestRowReader> PopulateProtoReader() {
    ZETASQL_ASSIGN_OR_RETURN(
        auto proto_type,
        MakeProtoType(proto_schema(), "emulator.tests.common.Simple"));
    ZETASQL_ASSIGN_OR_RETURN(
        auto enum_type,
        MakeEnumType(proto_schema(), "emulator.tests.common.TestEnum"));
    const zetasql::Type* array_proto_type;
    ZETASQL_RETURN_IF_ERROR(
        type_factory()->MakeArrayType(proto_type, &array_proto_type));
    const zetasql::Type* array_enum_type;
    ZETASQL_RETURN_IF_ERROR(type_factory()->MakeArrayType(enum_type, &array_enum_type));
    ::emulator::tests::common::Simple simple_proto1;
    ::emulator::tests::common::Simple simple_proto2;
    ::emulator::tests::common::Simple simple_proto3;
    simple_proto1.set_field("One");
    simple_proto2.set_field("Two");
    simple_proto3.set_field("Four");
    ::emulator::tests::common::TestEnum enum1 =
        ::emulator::tests::common::TEST_ENUM_ONE;
    ::emulator::tests::common::TestEnum enum2 =
        ::emulator::tests::common::TEST_ENUM_TWO;
    ::emulator::tests::common::TestEnum enum3 =
        ::emulator::tests::common::TEST_ENUM_FOUR;

    test::TestRowReader reader{
        {{"test_table",
          {{"int64_col", "proto_col", "enum_col", "array_proto_col",
            "array_enum_col"},
           {zetasql::types::Int64Type(), proto_type, enum_type,
            array_proto_type, array_enum_type},
           {{zetasql::values::Int64(1),
             zetasql::values::Proto(proto_type, simple_proto1),
             zetasql::values::Enum(enum_type, enum1),
             zetasql::values::Array(
                 array_proto_type->AsArray(),
                 {zetasql::values::Proto(proto_type, simple_proto1)}),
             zetasql::values::Array(
                 array_enum_type->AsArray(),
                 {zetasql::values::Enum(enum_type, enum1)})},
            {zetasql::values::Int64(2),
             zetasql::values::Proto(proto_type, simple_proto2),
             zetasql::values::Enum(enum_type, enum2),
             zetasql::values::Array(
                 array_proto_type->AsArray(),
                 {zetasql::values::Proto(proto_type, simple_proto2)}),
             zetasql::values::Array(
                 array_enum_type->AsArray(),
                 {zetasql::values::Enum(enum_type, enum2)})},
            {zetasql::values::Int64(4),
             zetasql::values::Proto(proto_type, simple_proto3),
             zetasql::values::Enum(enum_type, enum3),
             zetasql::values::Array(
                 array_proto_type->AsArray(),
                 {zetasql::values::Proto(proto_type, simple_proto3)}),
             zetasql::values::Array(
                 array_enum_type->AsArray(),
                 {zetasql::values::Enum(enum_type, enum3)})}}}}}};
    return reader;
  }

 protected:
  zetasql::TypeFactory type_factory_;
  std::unique_ptr<const Schema> schema_;
  std::unique_ptr<const Schema> multi_table_schema_;
  std::unique_ptr<const Schema> change_stream_schema_;
  std::unique_ptr<const Schema> model_schema_;
  std::unique_ptr<const Schema> sequence_schema_;
  std::unique_ptr<const Schema> gpk_schema_;

 private:
  std::unique_ptr<const Schema> views_schema_ =
      test::CreateSchemaWithView(&type_factory_);
  test::TestRowReader reader_{
      {{"test_table",
        {{"int64_col", "string_col"},
         {zetasql::types::Int64Type(), zetasql::types::StringType()},
         {{zetasql::values::Int64(1), zetasql::values::String("one")},
          {zetasql::values::Int64(2), zetasql::values::String("two")},
          {zetasql::values::Int64(4), zetasql::values::String("four")}}}}}};
  QueryEngine query_engine_{&type_factory_};
  test::ScopedEmulatorFeatureFlagsSetter feature_flags_setter_ =
      test::ScopedEmulatorFeatureFlagsSetter(
          {.enable_dml_returning = true,
           .enable_bit_reversed_positive_sequences = true,
           .enable_bit_reversed_positive_sequences_postgresql = true});
  test::TestRowReader change_stream_partition_table_reader_{
      {{"_change_stream_partition_change_stream_test_table",
        {{"partition_token"}, {zetasql::types::StringType()}}}}};
  test::TestRowReader change_stream_data_table_reader_{
      {{"_change_stream_data_change_stream_test_table",
        {{"partition_token"}, {zetasql::types::StringType()}}}}};
  test::TestRowReader gpk_table_reader_{
      {{"test_table",
        {{"k1_pk", "k2", "k3gen_storedpk", "k4", "k5"},
         {zetasql::types::Int64Type(), zetasql::types::Int64Type(),
          zetasql::types::Int64Type(), zetasql::types::Int64Type(),
          zetasql::types::Int64Type()}}}}};
  std::unique_ptr<const Schema> proto_schema_ =
      test::CreateSchemaWithProtoEnumColumn(&type_factory_, read_descriptors());
};

struct TestQuery {
  std::string sql;
  std::string result;
  std::string test_name;
};

class ParameterizedSelectProto : public QueryEngineTestBase,
                                 public testing::WithParamInterface<TestQuery> {
};

class QueryEngineTest
    : public QueryEngineTestBase,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 protected:
  void SetUp() override {
    if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
      schema_ = test::CreateSchemaWithOneTable(
          &type_factory_, database_api::DatabaseDialect::POSTGRESQL);
      multi_table_schema_ = test::CreateSchemaWithMultiTables(
          &type_factory_, database_api::DatabaseDialect::POSTGRESQL);
      change_stream_schema_ = test::CreateSchemaWithOneTableAndOneChangeStream(
          &type_factory_, database_api::DatabaseDialect::POSTGRESQL);
      ZETASQL_ASSERT_OK_AND_ASSIGN(
          gpk_schema_,
          test::CreateGpkSchemaWithOneTable(
              &type_factory_, database_api::DatabaseDialect::POSTGRESQL));
      ZETASQL_ASSERT_OK_AND_ASSIGN(
          sequence_schema_,
          test::CreateSchemaWithOneSequence(
              &type_factory_, database_api::DatabaseDialect::POSTGRESQL));
      query_engine().SetLatestSchemaForFunctionCatalog(sequence_schema_.get());
    } else if (GetParam() ==
               database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
      schema_ = test::CreateSchemaWithOneTable(&type_factory_);
      multi_table_schema_ = test::CreateSchemaWithMultiTables(&type_factory_);
      change_stream_schema_ =
          test::CreateSchemaWithOneTableAndOneChangeStream(&type_factory_);
      model_schema_ = test::CreateSchemaWithOneModel(&type_factory_);
      ZETASQL_ASSERT_OK_AND_ASSIGN(gpk_schema_,
                           test::CreateGpkSchemaWithOneTable(&type_factory_));
      ZETASQL_ASSERT_OK_AND_ASSIGN(sequence_schema_,
                           test::CreateSchemaWithOneSequence(&type_factory_));
      query_engine().SetLatestSchemaForFunctionCatalog(sequence_schema_.get());
    }
  }
};

INSTANTIATE_TEST_SUITE_P(
    QueryEnginePerDialectTests, QueryEngineTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<QueryEngineTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(QueryEngineTest, DetectsDMLQueries) {
  EXPECT_TRUE(IsDMLQuery("INSERT INTO Users VALUES('John')"));
  EXPECT_TRUE(IsDMLQuery("UPDATE Users SET Name = 'John' WHERE UserId = 1"));
  EXPECT_TRUE(IsDMLQuery("DELETE from Users where UserId = 'John'"));
  EXPECT_FALSE(IsDMLQuery("SELECT * from Users"));
}

TEST_P(QueryEngineTest, ExecuteSqlSelectsOneFromTable) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      QueryResult result,
      query_engine().ExecuteSql(Query{"SELECT 1 AS one FROM test_table"},
                                QueryContext{schema(), reader()}));
  ASSERT_NE(result.rows, nullptr);
  EXPECT_THAT(GetColumnNames(*result.rows), ElementsAre("one"));
  EXPECT_THAT(GetColumnTypes(*result.rows), ElementsAre(Int64Type()));
  EXPECT_THAT(
      GetAllColumnValues(std::move(result.rows)),
      IsOkAndHolds(ElementsAre(ElementsAre(Int64(1)), ElementsAre(Int64(1)),
                               ElementsAre(Int64(1)))));
}

TEST_P(QueryEngineTest, PlanSqlSelectsOneFromTable) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      QueryResult result,
      query_engine().ExecuteSql(Query{"SELECT 1 AS one FROM test_table"},
                                QueryContext{schema(), reader()},
                                v1::ExecuteSqlRequest::PLAN));
  ASSERT_NE(result.rows, nullptr);
  EXPECT_THAT(GetColumnNames(*result.rows), ElementsAre("one"));
  EXPECT_THAT(GetColumnTypes(*result.rows), ElementsAre(Int64Type()));
  EXPECT_THAT(GetAllColumnValues(std::move(result.rows)),
              IsOkAndHolds(ElementsAre()));
}

TEST_P(QueryEngineTest, ExecuteSqlSelectsGenerateUUIDFromTable) {
  // When using the postgres  dialect, generate_uuid() is exposed only via the
  // 'spanner' namespace.
  for (std::string function_call :
       {"GENERATE_UUID()", "SPANNER.GENERATE_UUID()"}) {
    SCOPED_TRACE(absl::StrCat("Using function: ", function_call));
    constexpr absl::string_view sql = "SELECT %s AS uuid FROM test_table";
    absl::StatusOr<QueryResult> status_or =
        query_engine().ExecuteSql(Query{absl::StrFormat(sql, function_call)},
                                  QueryContext{schema(), reader()});

    bool using_pg = GetParam() == database_api::DatabaseDialect::POSTGRESQL;
    bool expect_success =
        (!using_pg && function_call == "GENERATE_UUID()") ||
        (using_pg && function_call == "SPANNER.GENERATE_UUID()");
    ASSERT_EQ(status_or.ok(), expect_success) << status_or.status();
    if (expect_success) {
      QueryResult result = std::move(status_or.value());
      ASSERT_NE(result.rows, nullptr);
      EXPECT_THAT(GetColumnNames(*result.rows), ElementsAre("uuid"));
      EXPECT_THAT(GetColumnTypes(*result.rows), ElementsAre(StringType()));

      EXPECT_THAT(GetAllColumnValues(std::move(result.rows)),
                  IsOkAndHolds(ElementsAre(ElementsAre(UuidV4StringValue()),
                                           ElementsAre(UuidV4StringValue()),
                                           ElementsAre(UuidV4StringValue()))));
    }
  }
}

TEST_P(QueryEngineTest, ExecuteSqlSelectBitReverse) {
  // When using the postgres dialect, bit_reverse() is exposed only via the
  // 'spanner' namespace.
  for (std::string function_call :
       {"BIT_REVERSE(1, true)", "SPANNER.BIT_REVERSE(1, true)"}) {
    SCOPED_TRACE(absl::StrCat("Using function: ", function_call));
    constexpr absl::string_view sql =
        "SELECT %s AS bit_reverse FROM test_table";
    absl::StatusOr<QueryResult> status_or =
        query_engine().ExecuteSql(Query{absl::StrFormat(sql, function_call)},
                                  QueryContext{schema(), reader()});

    bool using_pg = GetParam() == database_api::DatabaseDialect::POSTGRESQL;
    bool expect_success =
        (!using_pg && function_call == "BIT_REVERSE(1, true)") ||
        (using_pg && function_call == "SPANNER.BIT_REVERSE(1, true)");
    ASSERT_EQ(status_or.ok(), expect_success) << status_or.status();
    if (expect_success) {
      QueryResult result = std::move(status_or.value());
      ASSERT_NE(result.rows, nullptr);
      EXPECT_THAT(GetColumnNames(*result.rows), ElementsAre("bit_reverse"));
      EXPECT_THAT(GetColumnTypes(*result.rows), ElementsAre(Int64Type()));

      EXPECT_THAT(
          GetAllColumnValues(std::move(result.rows)),
          IsOkAndHolds(ElementsAre(ElementsAre(Int64(4611686018427387904)),
                                   ElementsAre(Int64(4611686018427387904)),
                                   ElementsAre(Int64(4611686018427387904)))));
    }
  }
}

TEST_P(QueryEngineTest, SelectBitReverseWithDifferentArguments) {
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }

  constexpr absl::string_view sql = "SELECT BIT_REVERSE(%s)";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      QueryResult result,
      query_engine().ExecuteSql(Query{absl::StrFormat(sql, "2, false")},
                                QueryContext{sequence_schema(), reader()}));
  ASSERT_NE(result.rows, nullptr);
  EXPECT_THAT(
      GetAllColumnValues(std::move(result.rows)),
      IsOkAndHolds(ElementsAre(ElementsAre(Int64(4611686018427387904)))));

  ZETASQL_ASSERT_OK_AND_ASSIGN(result, query_engine().ExecuteSql(
                                   Query{absl::StrFormat(sql, "0, false")},
                                   QueryContext{sequence_schema(), reader()}));
  ASSERT_NE(result.rows, nullptr);
  EXPECT_THAT(GetAllColumnValues(std::move(result.rows)),
              IsOkAndHolds(ElementsAre(ElementsAre(Int64(0)))));

  ZETASQL_ASSERT_OK_AND_ASSIGN(result, query_engine().ExecuteSql(
                                   Query{absl::StrFormat(sql, "0, true")},
                                   QueryContext{sequence_schema(), reader()}));
  ASSERT_NE(result.rows, nullptr);
  EXPECT_THAT(GetAllColumnValues(std::move(result.rows)),
              IsOkAndHolds(ElementsAre(ElementsAre(Int64(0)))));
}

TEST_P(QueryEngineTest, PG_SelectBitReverseWithDifferentArguments) {
  if (GetParam() == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    GTEST_SKIP();
  }

  constexpr absl::string_view sql = "SELECT SPANNER.BIT_REVERSE(%s)";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      QueryResult result,
      query_engine().ExecuteSql(Query{absl::StrFormat(sql, "2, false")},
                                QueryContext{sequence_schema(), reader()}));
  ASSERT_NE(result.rows, nullptr);
  EXPECT_THAT(
      GetAllColumnValues(std::move(result.rows)),
      IsOkAndHolds(ElementsAre(ElementsAre(Int64(4611686018427387904)))));

  ZETASQL_ASSERT_OK_AND_ASSIGN(result, query_engine().ExecuteSql(
                                   Query{absl::StrFormat(sql, "0, false")},
                                   QueryContext{sequence_schema(), reader()}));
  ASSERT_NE(result.rows, nullptr);
  EXPECT_THAT(GetAllColumnValues(std::move(result.rows)),
              IsOkAndHolds(ElementsAre(ElementsAre(Int64(0)))));

  ZETASQL_ASSERT_OK_AND_ASSIGN(result, query_engine().ExecuteSql(
                                   Query{absl::StrFormat(sql, "0, true")},
                                   QueryContext{sequence_schema(), reader()}));
  ASSERT_NE(result.rows, nullptr);
  EXPECT_THAT(GetAllColumnValues(std::move(result.rows)),
              IsOkAndHolds(ElementsAre(ElementsAre(Int64(0)))));
}

TEST_P(QueryEngineTest, ExecuteSqlSelectGetInternalSequenceState) {
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }
  // When using the postgres dialect, GET_INTERNAL_SEQUENCE_STATE() is exposed
  // only via the 'spanner' namespace.
  for (std::string function_call :
       {"GET_INTERNAL_SEQUENCE_STATE(SEQUENCE myseq)",
        "SPANNER.GET_INTERNAL_SEQUENCE_STATE('myseq')"}) {
    SCOPED_TRACE(absl::StrCat("Using function: ", function_call));
    constexpr absl::string_view sql = "SELECT %s AS state FROM test_table";
    absl::StatusOr<QueryResult> status_or =
        query_engine().ExecuteSql(Query{absl::StrFormat(sql, function_call)},
                                  QueryContext{sequence_schema(), reader()});

    bool using_pg = GetParam() == database_api::DatabaseDialect::POSTGRESQL;
    bool expect_success =
        (!using_pg &&
         function_call == "GET_INTERNAL_SEQUENCE_STATE(SEQUENCE myseq)") ||
        (using_pg &&
         function_call == "SPANNER.GET_INTERNAL_SEQUENCE_STATE('myseq')");
    ASSERT_EQ(status_or.ok(), expect_success) << status_or.status();
    if (expect_success) {
      QueryResult result = std::move(status_or.value());
      ASSERT_NE(result.rows, nullptr);
      EXPECT_THAT(GetColumnNames(*result.rows), ElementsAre("state"));
      EXPECT_THAT(GetColumnTypes(*result.rows), ElementsAre(Int64Type()));

      EXPECT_THAT(GetAllColumnValues(std::move(result.rows)),
                  IsOkAndHolds(ElementsAre(
                      ElementsAre(zetasql::values::NullInt64()),
                      ElementsAre(zetasql::values::NullInt64()),
                      ElementsAre(zetasql::values::NullInt64()))));
    }
  }
}

TEST_P(QueryEngineTest, ExecuteSqlSelectGetInternalSequenceStateInvalidArg) {
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }

  constexpr absl::string_view sql = "SELECT GET_INTERNAL_SEQUENCE_STATE(%s)";

  // Invalid input: SEQUENCE keyword without the identifier
  EXPECT_THAT(
      query_engine().ExecuteSql(Query{absl::StrFormat(sql, "SEQUENCE")},
                                QueryContext{sequence_schema(), reader()}),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kInvalidArgument,
          testing::HasSubstr("Unrecognized name: SEQUENCE")));

  // Invalid input: empty argument
  EXPECT_THAT(
      query_engine().ExecuteSql(Query{absl::StrFormat(sql, "")},
                                QueryContext{sequence_schema(), reader()}),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kInvalidArgument,
          testing::HasSubstr("No matching signature for function")));

  // Invalid input: invalid type
  EXPECT_THAT(
      query_engine().ExecuteSql(Query{absl::StrFormat(sql, "1234")},
                                QueryContext{sequence_schema(), reader()}),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kInvalidArgument,
          testing::HasSubstr("No matching signature for function")));

  // Invalid input: extra argument
  EXPECT_THAT(
      query_engine().ExecuteSql(
          Query{absl::StrFormat(sql, "SEQUENCE myseq, SEQUENCE myseq2")},
          QueryContext{sequence_schema(), reader()}),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kInvalidArgument,
          testing::HasSubstr("No matching signature for function")));
}

TEST_P(QueryEngineTest, ExecuteSqlSelectsOneColumnFromTable) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      QueryResult result,
      query_engine().ExecuteSql(Query{"SELECT string_col FROM test_table"},
                                QueryContext{schema(), reader()}));
  ASSERT_NE(result.rows, nullptr);
  EXPECT_THAT(GetColumnNames(*result.rows), ElementsAre("string_col"));
  EXPECT_THAT(GetColumnTypes(*result.rows), ElementsAre(StringType()));
  EXPECT_THAT(GetAllColumnValues(std::move(result.rows)),
              IsOkAndHolds(UnorderedElementsAre(ElementsAre(String("one")),
                                                ElementsAre(String("two")),
                                                ElementsAre(String("four")))));
}

TEST_P(QueryEngineTest, PlanSqlSelectsOneColumnFromTable) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      QueryResult result,
      query_engine().ExecuteSql(Query{"SELECT string_col FROM test_table"},
                                QueryContext{schema(), reader()},
                                v1::ExecuteSqlRequest::PLAN));
  ASSERT_NE(result.rows, nullptr);
  EXPECT_THAT(GetColumnNames(*result.rows), ElementsAre("string_col"));
  EXPECT_THAT(GetColumnTypes(*result.rows), ElementsAre(StringType()));
  EXPECT_THAT(GetAllColumnValues(std::move(result.rows)),
              IsOkAndHolds(ElementsAre()));
}

TEST_P(QueryEngineTest, PlanSqlRecognizesAllParameterTypes) {
  Query query;
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    query = Query{
        "SELECT "
        "cast($1 as varchar) as string_param, "
        "cast($2 as bigint) as int64_param, "
        "cast($3 as bool) as bool_param, "
        "cast($4 as bytea) as bytes_param, "
        "cast($5 as float8) as float64_param, "
        "cast($6 as jsonb) as json_param, "
        "cast($7 as numeric) as numeric_param, "
        "cast($8 as timestamptz) as timestamp_param, "
        "cast($9 as date) as date_param"};
  } else {
    query = Query{
        "SELECT "
        "cast(@p1 as string) as string_param, "
        "@p2 as int64_param, "
        "cast(@p3 as bool) as bool_param, "
        "cast(@p4 as bytes) as bytes_param, "
        "cast(@p5 as float64) as float64_param, "
        "cast(@p6 as json) as json_param, "
        "cast(@p7 as numeric) as numeric_param, "
        "cast(@p8 as timestamp) as timestamp_param, "
        "cast(@p9 as date) as date_param"};
  }

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      QueryResult result,
      query_engine().ExecuteSql(query, QueryContext{schema(), reader()},
                                v1::ExecuteSqlRequest::PLAN));
  ASSERT_NE(result.rows, nullptr);
  EXPECT_THAT(GetParamNames(result), ElementsAre("p1", "p2", "p3", "p4", "p5",
                                                 "p6", "p7", "p8", "p9"));
  EXPECT_THAT(
      GetParamTypes(result),
      ElementsAre(StringType(), Int64Type(), BoolType(), BytesType(),
                  Float64Type(),
                  GetParam() == database_api::DatabaseDialect::POSTGRESQL
                      ? testing::Eq(GetPgJsonbType())
                      : JsonType(),
                  GetParam() == database_api::DatabaseDialect::POSTGRESQL
                      ? testing::Eq(GetPgNumericType())
                      : NumericType(),
                  TimestampType(), DateType()));
}

TEST_P(QueryEngineTest, PlanSqlAcceptsIncompleteParameters) {
  Query query;
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    query = {
        "SELECT string_col FROM test_table "
        "WHERE string_col=$1 and int64_col=$2",
        {{"p2", zetasql::values::Int64(1)}}};
  } else {
    query = {
        "SELECT string_col FROM test_table "
        "WHERE string_col=@p1 and int64_col=@p2",
        {{"p2", zetasql::values::Int64(1)}}};
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      QueryResult result,
      query_engine().ExecuteSql(query, QueryContext{schema(), reader()},
                                v1::ExecuteSqlRequest::PLAN));
  ASSERT_NE(result.rows, nullptr);
  EXPECT_THAT(GetColumnNames(*result.rows), ElementsAre("string_col"));
  EXPECT_THAT(GetColumnTypes(*result.rows), ElementsAre(StringType()));
  EXPECT_THAT(GetAllColumnValues(std::move(result.rows)),
              IsOkAndHolds(ElementsAre()));
  EXPECT_THAT(GetParamNames(result), ElementsAre("p1", "p2"));
  EXPECT_THAT(GetParamTypes(result), ElementsAre(StringType(), Int64Type()));
}

TEST_P(QueryEngineTest, ExecuteSqlRefusesIncompleteParameters) {
  Query query;
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    query = {"SELECT string_col FROM test_table WHERE string_col=$1"};
  } else {
    query = {"SELECT string_col FROM test_table WHERE string_col=@p1"};
  }
  EXPECT_THAT(query_engine().ExecuteSql(query, QueryContext{schema(), reader()},
                                        v1::ExecuteSqlRequest::NORMAL),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Incomplete query parameters")));
}

TEST_P(QueryEngineTest, ExecuteSqlSelectsOneColumnFromTableWithForceIndexHint) {
  std::string hint = (GetParam() == database_api::DatabaseDialect::POSTGRESQL)
                         ? "/*@ force_index=test_index */"
                         : "@{force_index=test_index}";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      QueryResult result,
      query_engine().ExecuteSql(
          Query{absl::Substitute("SELECT string_col FROM test_table$0", hint)},
          QueryContext{schema(), reader()}));
  ASSERT_NE(result.rows, nullptr);
  EXPECT_THAT(GetColumnNames(*result.rows), ElementsAre("string_col"));
  EXPECT_THAT(GetColumnTypes(*result.rows), ElementsAre(StringType()));
  EXPECT_THAT(GetAllColumnValues(std::move(result.rows)),
              IsOkAndHolds(UnorderedElementsAre(ElementsAre(String("one")),
                                                ElementsAre(String("two")),
                                                ElementsAre(String("four")))));
}

TEST_P(QueryEngineTest,
       ExecuteSqlSelectsOneColumnFromTableWithBaseTableStatementHint) {
  std::string hint = (GetParam() == database_api::DatabaseDialect::POSTGRESQL)
                         ? "/*@ force_index=_base_table */"
                         : "@{force_index=_base_table}";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      QueryResult result,
      query_engine().ExecuteSql(
          Query{absl::Substitute("$0 SELECT string_col FROM test_table", hint)},
          QueryContext{schema(), reader()}));
  ASSERT_NE(result.rows, nullptr);
  EXPECT_THAT(GetColumnNames(*result.rows), ElementsAre("string_col"));
  EXPECT_THAT(GetColumnTypes(*result.rows), ElementsAre(StringType()));
  EXPECT_THAT(GetAllColumnValues(std::move(result.rows)),
              IsOkAndHolds(UnorderedElementsAre(ElementsAre(String("one")),
                                                ElementsAre(String("two")),
                                                ElementsAre(String("four")))));
}

TEST_P(QueryEngineTest, ExecuteSqlSelectsAllColumnsFromTable) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      QueryResult result,
      query_engine().ExecuteSql(Query{"SELECT * FROM test_table"},
                                QueryContext{schema(), reader()}));
  ASSERT_NE(result.rows, nullptr);
  EXPECT_THAT(GetColumnNames(*result.rows),
              ElementsAre("int64_col", "string_col"));
  EXPECT_THAT(GetColumnTypes(*result.rows),
              ElementsAre(Int64Type(), StringType()));
  EXPECT_THAT(GetAllColumnValues(std::move(result.rows)),
              IsOkAndHolds(
                  UnorderedElementsAre(ElementsAre(Int64(1), String("one")),
                                       ElementsAre(Int64(2), String("two")),
                                       ElementsAre(Int64(4), String("four")))));
}

TEST_P(QueryEngineTest, ExecuteSqlSelectsParameterValuesFromTable) {
  Query query;
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    query = {"SELECT $1 AS int64_p, $2 AS string_p FROM test_table",
             {{"p1", Int64(24)}, {"p2", String("bar")}}};
  } else {
    query = {
        "SELECT @int64_p AS int64_p, @string_p AS string_p FROM test_table",
        {{"int64_p", Int64(24)}, {"string_p", String("bar")}}};
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      QueryResult result,
      query_engine().ExecuteSql(query, QueryContext{schema(), reader()}));
  ASSERT_NE(result.rows, nullptr);
  EXPECT_THAT(GetColumnNames(*result.rows), ElementsAre("int64_p", "string_p"));
  EXPECT_THAT(GetColumnTypes(*result.rows),
              ElementsAre(Int64Type(), StringType()));
  EXPECT_THAT(GetAllColumnValues(std::move(result.rows)),
              IsOkAndHolds(
                  UnorderedElementsAre(ElementsAre(Int64(24), String("bar")),
                                       ElementsAre(Int64(24), String("bar")),
                                       ElementsAre(Int64(24), String("bar")))));
}

TEST_P(QueryEngineTest, ExecuteSqlSelectsCountFromTable) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(QueryResult result,
                       query_engine().ExecuteSql(
                           Query{"SELECT COUNT(*) AS count FROM test_table"},
                           QueryContext{schema(), reader()}));
  ASSERT_NE(result.rows, nullptr);
  EXPECT_THAT(GetColumnNames(*result.rows), ElementsAre("count"));
  EXPECT_THAT(GetColumnTypes(*result.rows), ElementsAre(Int64Type()));
  EXPECT_THAT(GetAllColumnValues(std::move(result.rows)),
              IsOkAndHolds(ElementsAre(ElementsAre(Int64(3)))));
}

TEST_P(QueryEngineTest, ExecuteSqlQueryStringTooLong) {
  std::string long_str = std::string(limits::kMaxQueryStringSize + 1, 'a');
  EXPECT_THAT(query_engine().ExecuteSql(
                  Query{absl::Substitute("SELECT '$0'", long_str)},
                  QueryContext{schema(), reader()}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("exceeds maximum allowed length")));
}

TEST_P(QueryEngineTest, PartitionableSimpleScan) {
  Query query{"SELECT string_col FROM test_table"};
  ZETASQL_ASSERT_OK(query_engine().IsPartitionable(
      query, QueryContext{multi_table_schema(), reader()}));
}

TEST_P(QueryEngineTest, PartitionableSimpleScanFilter) {
  Query query{"SELECT string_col FROM test_table WHERE string_col = 'a'"};
  ZETASQL_ASSERT_OK(query_engine().IsPartitionable(
      query, QueryContext{multi_table_schema(), reader()}));
}

TEST_P(QueryEngineTest, PartitionableSimpleScanSubqueryColumn) {
  absl::StatusCode error_code = absl::StatusCode::kInvalidArgument;
  std::string error_msg = kQueryContainsSubqueryError;
  Query query{
      "SELECT string_col, ARRAY(SELECT child_key from child_table) FROM "
      "test_table"};
  EXPECT_THAT(query_engine().IsPartitionable(
                  query, QueryContext{multi_table_schema(), reader()}),
              StatusIs(error_code, HasSubstr(error_msg)));
}

TEST_P(QueryEngineTest, PartitionableSimpleScanNoTable) {
  Query query{"SELECT a FROM UNNEST(ARRAY[1, 2, 3]) AS a"};
  ZETASQL_ASSERT_OK(query_engine().IsPartitionable(
      query, QueryContext{multi_table_schema(), reader()}));
}

TEST_P(QueryEngineTest, PartitionableSimpleScanFilterNoTable) {
  Query query{"SELECT a FROM UNNEST(ARRAY[1, 2, 3]) AS a WHERE a = 1"};
  ZETASQL_ASSERT_OK(query_engine().IsPartitionable(
      query, QueryContext{multi_table_schema(), reader()}));
}

TEST_P(QueryEngineTest, PartitionableExecuteSqlSimpleScanFilterSubquery) {
  Query query{
      "SELECT string_col FROM test_table WHERE string_col = 'a' AND EXISTS "
      "(SELECT child_key FROM child_table)"};
  EXPECT_THAT(query_engine().IsPartitionable(
                  query, QueryContext{multi_table_schema(), reader()}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr(kQueryContainsSubqueryError)));
}

TEST_P(QueryEngineTest, PartitionableSimpleScanFilterSubqueryInExpr) {
  Query query{
      "SELECT string_col FROM test_table WHERE int64_col IN "
      "(SELECT child_key FROM child_table)"};
  EXPECT_THAT(query_engine().IsPartitionable(
                  query, QueryContext{multi_table_schema(), reader()}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr(kQueryContainsSubqueryError)));
}

TEST_P(QueryEngineTest, NonPartitionableSelectsFromTwoTable) {
  Query query{"SELECT t1.string_col FROM test_table AS t1, test_table2 AS t2"};
  EXPECT_THAT(query_engine().IsPartitionable(
                  query, QueryContext{multi_table_schema(), reader()}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr(kQueryNotASimpleTableScanError)));
}

// TODO: turn on test once parent child join implemented.
TEST_P(QueryEngineTest, DISABLED_PartitionableParentChildTable) {
  Query query{
      "SELECT t1.string_col FROM test_table AS t1, child_table AS t2 WHERE "
      "t1.int64_col = t2.int64_col"};
  ZETASQL_ASSERT_OK(query_engine().IsPartitionable(
      query, QueryContext{multi_table_schema(), reader()}));
}

class MockRowWriter : public RowWriter {
 public:
  MOCK_METHOD(absl::Status, Write, (const Mutation& m), (override));
};

TEST_P(QueryEngineTest, InsertOrIgnoreDmlFlagDisabled) {
  test::ScopedEmulatorFeatureFlagsSetter setter(
      {.enable_upsert_queries = false});
  MockRowWriter writer;
  if (GetParam() == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    EXPECT_THAT(
        query_engine().ExecuteSql(
            Query{"INSERT OR IGNORE INTO test_table (int64_col) VALUES(1)"},
            QueryContext{schema(), reader(), &writer}),
        StatusIs(
            absl::StatusCode::kUnimplemented,
            HasSubstr(
                "Insert or ignore statement is not supported in Emulator")));
  } else {
    EXPECT_THAT(
        query_engine().ExecuteSql(
            Query{"INSERT INTO test_table (int64_col) VALUES(1) "
                  "ON CONFLICT(int64_col) DO NOTHING"},
            QueryContext{schema(), reader(), &writer}),
        StatusIs(
            absl::StatusCode::kUnimplemented,
            HasSubstr(
                "Insert or ignore statement is not supported in Emulator")));
  }
}

TEST_P(QueryEngineTest, InsertOrIgnoreDmlWithReturning) {
  MockRowWriter writer;
  if (GetParam() == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    EXPECT_THAT(
        query_engine().ExecuteSql(
            Query{"INSERT OR IGNORE INTO test_table (int64_col) VALUES(1) "
                  "THEN RETURN *"},
            QueryContext{schema(), reader(), &writer}),
        StatusIs(
            absl::StatusCode::kUnimplemented,
            HasSubstr("Returning clause in Insert or ignore statement is not "
                      "supported in Emulator")));
  } else {
    EXPECT_THAT(
        query_engine().ExecuteSql(
            Query{"INSERT INTO test_table (int64_col) VALUES(1) "
                  "ON CONFLICT(int64_col) DO NOTHING RETURNING *"},
            QueryContext{schema(), reader(), &writer}),
        StatusIs(
            absl::StatusCode::kUnimplemented,
            HasSubstr("RETURNING with ON CONFLICT clause is not supported")));
  }
}

TEST_P(QueryEngineTest, InsertOrUpdateDmlFlagDisabled) {
  test::ScopedEmulatorFeatureFlagsSetter setter(
      {.enable_upsert_queries = false});
  MockRowWriter writer;
  if (GetParam() == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    EXPECT_THAT(
        query_engine().ExecuteSql(
            Query{"INSERT OR UPDATE INTO test_table (int64_col) VALUES(1)"},
            QueryContext{schema(), reader(), &writer}),
        StatusIs(
            absl::StatusCode::kUnimplemented,
            HasSubstr(
                "Insert or update statement is not supported in Emulator")));
  } else {
    EXPECT_THAT(
        query_engine().ExecuteSql(
            Query{"INSERT INTO test_table (int64_col) VALUES(1) "
                  "ON CONFLICT(int64_col) DO UPDATE "
                  "SET int64_col = excluded.int64_col"},
            QueryContext{schema(), reader(), &writer}),
        StatusIs(
            absl::StatusCode::kUnimplemented,
            HasSubstr(
                "Insert or update statement is not supported in Emulator")));
  }
}

TEST_P(QueryEngineTest, InsertOrUpdateDmlWithReturning) {
  MockRowWriter writer;
  if (GetParam() == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    EXPECT_THAT(
        query_engine().ExecuteSql(
            Query{"INSERT OR UPDATE INTO test_table (int64_col) VALUES(1) "
                  "THEN RETURN *"},
            QueryContext{schema(), reader(), &writer}),
        StatusIs(
            absl::StatusCode::kUnimplemented,
            HasSubstr("Returning clause in Insert or update statement is not "
                      "supported in Emulator")));
  } else {
    EXPECT_THAT(
        query_engine().ExecuteSql(
            Query{"INSERT INTO test_table (int64_col) VALUES(1) "
                  "ON CONFLICT(int64_col) DO UPDATE "
                  "SET int64_col = excluded.int64_col RETURNING *"},
            QueryContext{schema(), reader(), &writer}),
        StatusIs(
            absl::StatusCode::kUnimplemented,
            HasSubstr("RETURNING with ON CONFLICT clause is not supported")));
  }
}

TEST_P(QueryEngineTest, ExecuteInsertsTwoRows) {
  MockRowWriter writer;
  EXPECT_CALL(
      writer,
      Write(Property(
          &Mutation::ops,
          UnorderedElementsAre(
              AllOf(Field(&MutationOp::type, MutationOpType::kInsert),
                    Field(&MutationOp::table, "test_table"),
                    Field(&MutationOp::columns,
                          std::vector<std::string>{"int64_col", "string_col"}),
                    Field(&MutationOp::rows,
                          UnorderedElementsAre(
                              ValueList{Int64(3), String("three")},
                              ValueList{Int64(5), String("five")})))))))
      .Times(1)
      .WillOnce(Return(absl::OkStatus()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      QueryResult result,
      query_engine().ExecuteSql(
          Query{"INSERT INTO test_table (int64_col, string_col) "
                "VALUES(5, 'five'), (3, 'three')"},
          QueryContext{schema(), reader(), &writer}));
  EXPECT_EQ(result.rows, nullptr);
  EXPECT_EQ(result.modified_row_count, 2);
}

TEST_P(QueryEngineTest, ExecuteInsertsTwoRowsIntoSequenceTable) {
  std::string returning =
      (GetParam() == database_api::DatabaseDialect::POSTGRESQL) ? "RETURNING"
                                                                : "THEN RETURN";
  MockRowWriter writer;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      QueryResult result,
      query_engine().ExecuteSql(
          Query{absl::StrCat("INSERT INTO test_table (string_col) VALUES "
                             "('one'), ('two') ",
                             returning, " int64_col as col")},
          QueryContext{sequence_schema(), reader(), &writer}));
  ASSERT_NE(result.rows, nullptr);
  EXPECT_EQ(result.modified_row_count, 2);
  EXPECT_THAT(GetColumnTypes(*result.rows), ElementsAre(Int64Type()));
}

TEST_P(QueryEngineTest, ExecuteSqlDeleteRows) {
  MockRowWriter writer;
  EXPECT_CALL(writer,
              Write(Property(
                  &Mutation::ops,
                  UnorderedElementsAre(AllOf(
                      Field(&MutationOp::type, MutationOpType::kDelete),
                      Field(&MutationOp::table, "test_table"),
                      Field(&MutationOp::key_set,
                            Property(&KeySet::keys, UnorderedElementsAre(
                                                        Key{{Int64(2)}},
                                                        Key{{Int64(4)}}))))))))
      .Times(1)
      .WillOnce(Return(absl::OkStatus()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      QueryResult result,
      query_engine().ExecuteSql(Query{"DELETE FROM test_table "
                                      "WHERE int64_col > 1"},
                                QueryContext{schema(), reader(), &writer}));
  EXPECT_EQ(result.rows, nullptr);
  EXPECT_EQ(result.modified_row_count, 2);
}

TEST_P(QueryEngineTest, ExecuteSqlUpdatesRows) {
  MockRowWriter writer;
  EXPECT_CALL(
      writer,
      Write(Property(
          &Mutation::ops,
          UnorderedElementsAre(AllOf(
              Field(&MutationOp::type, MutationOpType::kUpdate),
              Field(&MutationOp::table, "test_table"),
              Field(&MutationOp::columns,
                    std::vector<std::string>{"int64_col", "string_col"}),
              Field(
                  &MutationOp::rows,
                  UnorderedElementsAre(ValueList{Int64(2), String("foo")},
                                       ValueList{Int64(4), String("foo")})))))))
      .Times(1)
      .WillOnce(Return(absl::OkStatus()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(QueryResult result,
                       query_engine().ExecuteSql(
                           Query{"UPDATE test_table "
                                 "SET string_col = 'foo' WHERE int64_col > 1"},
                           QueryContext{schema(), reader(), &writer}));
  EXPECT_EQ(result.rows, nullptr);
  EXPECT_EQ(result.modified_row_count, 2);
}

TEST_P(QueryEngineTest, CannotInsertDuplicateValuesForPrimaryKey) {
  MockRowWriter writer;
  EXPECT_THAT(query_engine().ExecuteSql(
                  Query{"INSERT INTO test_table (int64_col, string_col) "
                        "VALUES(2, 'another two')"},
                  QueryContext{schema(), reader(), &writer}),
              StatusIs(absl::StatusCode::kAlreadyExists));
}

TEST_P(QueryEngineTest, ConnotUpdatePrimaryKey) {
  MockRowWriter writer;
  EXPECT_THAT(query_engine().ExecuteSql(
                  Query{"UPDATE test_table SET int64_col=2 WHERE int64_col=2"},
                  QueryContext{schema(), reader(), &writer}),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_P(QueryEngineTest, TestGetValidChangeStreamMetadataFromChangeStreamQuery) {
  Query query;
  absl::Time start_time = absl::Now();
  absl::Time end_time = start_time + absl::Minutes(1);
  std::string tvf_name = GetParam() == database_api::DatabaseDialect::POSTGRESQL
                             ? "read_json_change_stream_test_table"
                             : "READ_change_stream_test_table";
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    query = {absl::Substitute(
        "SELECT * FROM "
        "spanner.read_json_change_stream_test_table ('$0'::timestamptz, "
        "'$1'::timestamptz, 'test_token'::text, 1000, NULL::text[] )",
        start_time, end_time)};
  } else {
    query = {
        absl::Substitute("SELECT * FROM "
                         "READ_change_stream_test_table ('$0', "
                         "'$1', 'test_token', 1000 )",
                         start_time, end_time)};
  }

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto metadata, query_engine().TryGetChangeStreamMetadata(
                                          query, change_stream_schema()));
  EXPECT_EQ(metadata.change_stream_name, "change_stream_test_table");
  EXPECT_EQ(metadata.heartbeat_milliseconds, 1000);
  EXPECT_EQ(metadata.partition_token.value(), "test_token");
  // absl::Time have different precision with timestamptz in pg
  // dialect, thus we only compare them up to microseconds precision.
  EXPECT_EQ(absl::FormatTime("%Y-%m-%dT%H:%M:%S.%fZ", metadata.start_timestamp,
                             absl::LocalTimeZone()),
            absl::FormatTime("%Y-%m-%dT%H:%M:%S.%fZ", start_time,
                             absl::LocalTimeZone()));
  EXPECT_EQ(
      absl::FormatTime("%Y-%m-%dT%H:%M:%S.%fZ", metadata.end_timestamp.value(),
                       absl::LocalTimeZone()),
      absl::FormatTime("%Y-%m-%dT%H:%M:%S.%fZ", end_time,
                       absl::LocalTimeZone()));
  EXPECT_EQ(metadata.tvf_name, tvf_name);
  EXPECT_EQ(metadata.partition_table,
            "_change_stream_partition_change_stream_test_table");
  EXPECT_EQ(metadata.data_table,
            "_change_stream_data_change_stream_test_table");
  EXPECT_EQ(metadata.is_pg,
            GetParam() == database_api::DatabaseDialect::POSTGRESQL);
  ASSERT_TRUE(metadata.is_change_stream_query);
}

TEST_P(QueryEngineTest,
       TestCannotGetChangeStreamMetadataFromInvalidChangeStreamQuery) {
  Query query;
  absl::Time start_time = absl::Now();
  absl::Time end_time = start_time + absl::Minutes(1);
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    query = {absl::Substitute(
        "SELECT * FROM "
        "spanner.read_json_change_stream_test_table ('$0'::timestamptz, "
        "'$1'::timestamptz, 'test_token'::text, NULL, NULL::text[] )",
        start_time, end_time)};
  } else {
    query = {
        absl::Substitute("SELECT * FROM "
                         "READ_change_stream_test_table ('$0', "
                         "'$1', 'test_token', NULL )",
                         start_time, end_time)};
  }
  EXPECT_THAT(
      query_engine().TryGetChangeStreamMetadata(query, change_stream_schema()),
      StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_P(QueryEngineTest, TestGetEmptyChangeStreamMetadataFromNormalQuery) {
  Query query{"SELECT * FROM test_table"};
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto metadata, query_engine().TryGetChangeStreamMetadata(
                                          query, change_stream_schema()));
  ASSERT_FALSE(metadata.is_change_stream_query);
}

TEST_P(QueryEngineTest,
       TestPreventChanegStreamQueriesFromGenericExecuteSqlAPI) {
  Query query;
  absl::Time start_time = absl::Now();
  absl::Time end_time = start_time + absl::Minutes(1);
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    query = {absl::Substitute(
        "SELECT * FROM "
        "spanner.read_json_change_stream_test_table ('$0'::timestamptz, "
        "'$1'::timestamptz, 'test_token'::text, 1000, NULL::text[] )",
        start_time, end_time)};
  } else {
    query = {
        absl::Substitute("SELECT * FROM "
                         "READ_change_stream_test_table ('$0', "
                         "'$1', 'test_token', 1000 )",
                         start_time, end_time)};
  }
  EXPECT_THAT(query_engine().ExecuteSql(
                  query, QueryContext{change_stream_schema(), reader()}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Change stream queries are not "
                                 "supported for the ExecuteSql API.")));
}

TEST_P(QueryEngineTest, TestCanQueryChangeStreamPartitionTableInternally) {
  Query query{
      "SELECT partition_token FROM "
      "_change_stream_partition_change_stream_test_table"};
  query.change_stream_internal_lookup = "change_stream_test_table";

  ZETASQL_ASSERT_OK(query_engine().ExecuteSql(
      query, QueryContext{change_stream_schema(),
                          change_stream_partition_table_reader()}));
}

TEST_P(QueryEngineTest, TestCannotQueryChangeStreamPartitionTableExternally) {
  Query query{
      "SELECT partition_token FROM "
      "_change_stream_partition_change_stream_test_table"};
  EXPECT_THAT(query_engine().ExecuteSql(
                  query, QueryContext{change_stream_schema(),
                                      change_stream_partition_table_reader()}),
              StatusIs(GetParam() == database_api::DatabaseDialect::POSTGRESQL
                           ? absl::StatusCode::kNotFound
                           : absl::StatusCode::kInvalidArgument));
}

TEST_P(QueryEngineTest, TestCanQueryChangeStreamDataTableInternally) {
  Query query{
      "SELECT partition_token FROM "
      "_change_stream_data_change_stream_test_table"};
  query.change_stream_internal_lookup = "change_stream_test_table";
  ZETASQL_ASSERT_OK(query_engine().ExecuteSql(
      query,
      QueryContext{change_stream_schema(), change_stream_data_table_reader()}));
}

TEST_P(QueryEngineTest, TestCannotQueryChangeStreamDataTableExternally) {
  Query query{
      "SELECT partition_token FROM "
      "_change_stream_data_change_stream_test_table"};
  EXPECT_THAT(query_engine().ExecuteSql(
                  query, QueryContext{change_stream_schema(),
                                      change_stream_data_table_reader()}),
              StatusIs(GetParam() == database_api::DatabaseDialect::POSTGRESQL
                           ? absl::StatusCode::kNotFound
                           : absl::StatusCode::kInvalidArgument));
}

TEST_P(QueryEngineTest, TestMlQuery) {
  GTEST_SKIP();

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    return;
  }

  EXPECT_THAT(
      query_engine().ExecuteSql(
          Query{"SELECT int64_col, Outcome "
                "FROM ML.PREDICT(MODEL test_model, TABLE test_table)"},
          QueryContext{model_schema(), reader()}),
      StatusIs(
          absl::StatusCode::kUnimplemented,
          HasSubstr("TVF ML.PREDICT does not support the API in evaluator.h")));
}

INSTANTIATE_TEST_SUITE_P(
    ParameterizedSelectProto, ParameterizedSelectProto,
    testing::ValuesIn<TestQuery>(
        {{R"sql(SELECT proto_col.field FROM test_table)sql",
          "field(STRING) : \"One\",\"Two\",\"Four\",", "proto"},
         {R"sql(SELECT enum_col FROM test_table)sql",
          "enum_col(emulator.tests.common.TestEnum) : "
          "TEST_ENUM_TWO,TEST_ENUM_ONE,TEST_ENUM_FOUR,",
          "enum"},
         {R"sql(SELECT array_enum_col FROM test_table)sql",
          "array_enum_col(ARRAY<emulator.tests.common.TestEnum>) : "
          "[TEST_ENUM_TWO],[TEST_ENUM_ONE],[TEST_ENUM_FOUR],",
          "array_enum"},
         {R"sql(SELECT array_proto_col FROM test_table)sql",
          "array_proto_col(ARRAY<emulator.tests.common.Simple>) : [{field: "
          "\"Two\"}],[{field: \"One\"}],[{field: \"Four\"}],",
          "array_proto"},
         {R"sql(SELECT REPLACE_FIELDS(new emulator.tests.common.Simple {
          field : "test1"
        }, "test3" AS field).field)sql",
          "field(STRING) : \"test3\",", "replace_fields"},
         {R"sql(SELECT new
        emulator.tests.common.Simple {
          field : "test1"
        })sql",
          "(emulator.tests.common.Simple) : {field: \"test1\"},",
          "braced_proto_constructor"}}),
    [](const testing::TestParamInfo<ParameterizedSelectProto::ParamType>&
           info) { return info.param.test_name; });

TEST_P(ParameterizedSelectProto, ExecuteSqlSelectsProtoAndEnumColumnFromTable) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto reader, PopulateProtoReader());
  TestQuery test_query = GetParam();

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      QueryResult result,
      query_engine().ExecuteSql(Query{test_query.sql},
                                QueryContext{proto_schema(), &reader}));
  ASSERT_NE(result.rows, nullptr);
  EXPECT_EQ(ToString(result), test_query.result);
}

TEST_P(QueryEngineTest, ExecuteSqlSelectsInvalidProtoField) {
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // Protos are unsupported in the PG dialect.
    return;
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto reader, PopulateProtoReader());
  EXPECT_THAT(
      query_engine().ExecuteSql(
          Query{R"sql(SELECT proto_col.invalid_field FROM test_table)sql"},
          QueryContext{proto_schema(), &reader}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("does not have a field called invalid_field")));
}

TEST_P(QueryEngineTest, ExecuteInsertsTwoProtoAndEnumRows) {
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // Protos are unsupported in the PG dialect.
    return;
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto reader, PopulateProtoReader());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto proto_type,
      MakeProtoType(proto_schema(), "emulator.tests.common.Simple"));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto enum_type,
      MakeEnumType(proto_schema(), "emulator.tests.common.TestEnum"));
  const zetasql::Type* array_proto_type;
  ZETASQL_ASSERT_OK(type_factory()->MakeArrayType(proto_type, &array_proto_type));
  const zetasql::Type* array_enum_type;
  ZETASQL_ASSERT_OK(type_factory()->MakeArrayType(enum_type, &array_enum_type));
  ::emulator::tests::common::Simple simple_proto2;
  ::emulator::tests::common::Simple simple_proto3;
  simple_proto2.set_field("Two");
  simple_proto3.set_field("Four");
  ::emulator::tests::common::TestEnum enum2 =
      ::emulator::tests::common::TEST_ENUM_TWO;
  ::emulator::tests::common::TestEnum enum3 =
      ::emulator::tests::common::TEST_ENUM_FOUR;

  MockRowWriter writer;
  EXPECT_CALL(
      writer,
      Write(Property(
          &Mutation::ops,
          UnorderedElementsAre(AllOf(
              Field(&MutationOp::type, MutationOpType::kInsert),
              Field(&MutationOp::table, "test_table"),
              Field(&MutationOp::columns,
                    std::vector<std::string>{"int64_col", "proto_col",
                                             "enum_col", "array_proto_col",
                                             "array_enum_col"}),
              Field(&MutationOp::rows,
                    UnorderedElementsAre(
                        ValueList{
                            Int64(7),
                            zetasql::values::Proto(proto_type, simple_proto2),
                            zetasql::values::Enum(enum_type, enum2),
                            zetasql::values::Array(
                                array_proto_type->AsArray(),
                                {zetasql::values::Proto(proto_type,
                                                          simple_proto2)}),
                            zetasql::values::Array(
                                array_enum_type->AsArray(),
                                {zetasql::values::Enum(enum_type, enum2)})},
                        ValueList{
                            Int64(8),
                            zetasql::values::Proto(proto_type, simple_proto3),
                            zetasql::values::Enum(enum_type, enum3),
                            zetasql::values::Array(
                                array_proto_type->AsArray(),
                                {zetasql::values::Proto(proto_type,
                                                          simple_proto3)}),
                            zetasql::values::Array(
                                array_enum_type->AsArray(),
                                {zetasql::values::Enum(enum_type,
                                                         enum3)})})))))))
      .Times(1)
      .WillOnce(Return(absl::OkStatus()));
  EXPECT_THAT(
      query_engine().ExecuteSql(
          Query{
              R"sql(INSERT INTO test_table (int64_col, proto_col,enum_col,
                                          array_proto_col,array_enum_col)
              VALUES(8, 'field: "Four"','TEST_ENUM_FOUR',
              ARRAY<emulator.tests.common.Simple>['field: "Four"'],
              ARRAY<emulator.tests.common.TestEnum>['TEST_ENUM_FOUR']),
              (7, 'field: "Two"','TEST_ENUM_TWO',
              ARRAY<emulator.tests.common.Simple>['field: "Two"'],
              ARRAY<emulator.tests.common.TestEnum>['TEST_ENUM_TWO']))sql"},
          QueryContext{proto_schema(), &reader, &writer}),
      IsOkAndHolds(Field(&QueryResult::modified_row_count, 2)));
}

TEST_P(QueryEngineTest, ExecuteSqlInsertsInvalidProtoEnumField) {
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // Protos are unsupported in the PG dialect.
    return;
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto reader, PopulateProtoReader());
  MockRowWriter writer;

  EXPECT_THAT(
      query_engine().ExecuteSql(
          Query{R"sql(INSERT into test_table(int64_col,proto_col,enum_col)
                      VALUES(5,'invalid_field:"Four"','TEST_ENUM_FOUR'))sql"},
          QueryContext{proto_schema(), &reader, &writer}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Error parsing proto:")));
  EXPECT_THAT(
      query_engine().ExecuteSql(
          Query{R"sql(INSERT into test_table(int64_col,proto_col,enum_col)
                      VALUES(8,'field:"Eight"','TEST_ENUM_EIGHT'))sql"},
          QueryContext{proto_schema(), &reader, &writer}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Could not cast literal")));
}

TEST_P(QueryEngineTest, ExecuteSqlDeleteProtoAndEnumColumnFromTable) {
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // Protos are unsupported in the PG dialect.
    return;
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto reader, PopulateProtoReader());

  MockRowWriter writer;
  EXPECT_CALL(
      writer,
      Write(Property(&Mutation::ops,
                     UnorderedElementsAre(AllOf(
                         Field(&MutationOp::type, MutationOpType::kDelete),
                         Field(&MutationOp::table, "test_table"),
                         Field(&MutationOp::key_set,
                               Property(&KeySet::keys, UnorderedElementsAre(Key{
                                                           {Int64(4)}}))))))))
      .Times(1)
      .WillOnce(Return(absl::OkStatus()));
  EXPECT_THAT(
      query_engine().ExecuteSql(Query{R"sql(DELETE FROM test_table
                                WHERE proto_col.field = "Four")sql"},
                                QueryContext{proto_schema(), &reader, &writer}),
      IsOkAndHolds(Field(&QueryResult::modified_row_count, 1)));

  EXPECT_CALL(
      writer,
      Write(Property(&Mutation::ops,
                     UnorderedElementsAre(AllOf(
                         Field(&MutationOp::type, MutationOpType::kDelete),
                         Field(&MutationOp::table, "test_table"),
                         Field(&MutationOp::key_set,
                               Property(&KeySet::keys, UnorderedElementsAre(Key{
                                                           {Int64(2)}}))))))))
      .Times(1)
      .WillOnce(Return(absl::OkStatus()));
  EXPECT_THAT(
      query_engine().ExecuteSql(Query{R"sql(DELETE FROM test_table
                        WHERE enum_col = 'TEST_ENUM_TWO')sql"},
                                QueryContext{proto_schema(), &reader, &writer}),
      IsOkAndHolds(Field(&QueryResult::modified_row_count, 1)));
}

TEST_P(QueryEngineTest, ExecuteSqlDeleteArrayProtoAndArrayEnumColumnFromTable) {
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // Protos are unsupported in the PG dialect.
    return;
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto reader, PopulateProtoReader());

  MockRowWriter writer;
  EXPECT_CALL(
      writer,
      Write(Property(&Mutation::ops,
                     UnorderedElementsAre(AllOf(
                         Field(&MutationOp::type, MutationOpType::kDelete),
                         Field(&MutationOp::table, "test_table"),
                         Field(&MutationOp::key_set,
                               Property(&KeySet::keys, UnorderedElementsAre(Key{
                                                           {Int64(4)}}))))))))
      .Times(1)
      .WillOnce(Return(absl::OkStatus()));
  EXPECT_THAT(
      query_engine().ExecuteSql(Query{R"sql(DELETE FROM test_table
                                WHERE
                                array_proto_col[OFFSET(0)].field = "Four")sql"},
                                QueryContext{proto_schema(), &reader, &writer}),
      IsOkAndHolds(Field(&QueryResult::modified_row_count, 1)));

  EXPECT_CALL(
      writer,
      Write(Property(&Mutation::ops,
                     UnorderedElementsAre(AllOf(
                         Field(&MutationOp::type, MutationOpType::kDelete),
                         Field(&MutationOp::table, "test_table"),
                         Field(&MutationOp::key_set,
                               Property(&KeySet::keys, UnorderedElementsAre(Key{
                                                           {Int64(2)}}))))))))
      .Times(1)
      .WillOnce(Return(absl::OkStatus()));
  EXPECT_THAT(
      query_engine().ExecuteSql(Query{R"sql(DELETE FROM test_table
                WHERE array_enum_col[OFFSET(0)] = 'TEST_ENUM_TWO')sql"},
                                QueryContext{proto_schema(), &reader, &writer}),
      IsOkAndHolds(Field(&QueryResult::modified_row_count, 1)));
}

TEST_P(QueryEngineTest, ExecuteSqlDeleteInvalidProtoAndEnumColumnFromTable) {
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // Protos are unsupported in the PG dialect.
    return;
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto reader, PopulateProtoReader());

  MockRowWriter writer;
  EXPECT_THAT(
      query_engine().ExecuteSql(Query{R"sql(DELETE FROM test_table
                                WHERE proto_col.invalid_field = "Four")sql"},
                                QueryContext{proto_schema(), &reader, &writer}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("does not have a field called invalid_field")));

  EXPECT_THAT(
      query_engine().ExecuteSql(Query{R"sql(DELETE FROM test_table
                WHERE enum_col = 'TEST_ENUM_EIGHT')sql"},
                                QueryContext{proto_schema(), &reader, &writer}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Could not cast literal")));
}

TEST_P(QueryEngineTest, ExecuteSqlUpdateProtoWithEnumColumnInTable) {
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // Protos are unsupported in the PG dialect.
    return;
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto reader, PopulateProtoReader());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto proto_type,
      MakeProtoType(proto_schema(), "emulator.tests.common.Simple"));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto enum_type,
      MakeEnumType(proto_schema(), "emulator.tests.common.TestEnum"));
  const zetasql::Type* array_proto_type;
  ZETASQL_ASSERT_OK(type_factory()->MakeArrayType(proto_type, &array_proto_type));
  const zetasql::Type* array_enum_type;
  ZETASQL_ASSERT_OK(type_factory()->MakeArrayType(enum_type, &array_enum_type));
  MockRowWriter writer;

  ::emulator::tests::common::Simple simple_proto2;
  simple_proto2.set_field("Two");
  ::emulator::tests::common::TestEnum enum2 =
      ::emulator::tests::common::TEST_ENUM_TWO;

  ::emulator::tests::common::Simple simple_proto_updated;
  simple_proto_updated.set_field("Updated");
  EXPECT_CALL(
      writer,
      Write(Property(
          &Mutation::ops,
          UnorderedElementsAre(AllOf(
              Field(&MutationOp::type, MutationOpType::kUpdate),
              Field(&MutationOp::table, "test_table"),
              Field(&MutationOp::columns,
                    std::vector<std::string>{"int64_col", "proto_col",
                                             "enum_col", "array_proto_col",
                                             "array_enum_col"}),
              Field(
                  &MutationOp::rows,
                  UnorderedElementsAre(ValueList{
                      Int64(2),
                      zetasql::values::Proto(proto_type,
                                               simple_proto_updated),
                      zetasql::values::Enum(enum_type, enum2),
                      zetasql::values::Array(array_proto_type->AsArray(),
                                               {zetasql::values::Proto(
                                                   proto_type, simple_proto2)}),
                      zetasql::values::Array(
                          array_enum_type->AsArray(),
                          {zetasql::values::Enum(enum_type, enum2)})})))))))
      .Times(1)
      .WillOnce(Return(absl::OkStatus()));
  EXPECT_THAT(
      query_engine().ExecuteSql(Query{R"sql(UPDATE test_table
                        SET proto_col = 'field: "Updated"'
                        WHERE enum_col = 'TEST_ENUM_TWO')sql"},
                                QueryContext{proto_schema(), &reader, &writer}),
      IsOkAndHolds(Field(&QueryResult::modified_row_count, 1)));

  ::emulator::tests::common::Simple simple_proto3;
  simple_proto3.set_field("Four");
  ::emulator::tests::common::TestEnum enum3 =
      ::emulator::tests::common::TEST_ENUM_FOUR;
  ::emulator::tests::common::TestEnum updated_enum =
      ::emulator::tests::common::TEST_ENUM_ONE;
  EXPECT_CALL(
      writer,
      Write(Property(
          &Mutation::ops,
          UnorderedElementsAre(AllOf(
              Field(&MutationOp::type, MutationOpType::kUpdate),
              Field(&MutationOp::table, "test_table"),
              Field(&MutationOp::columns,
                    std::vector<std::string>{"int64_col", "proto_col",
                                             "enum_col", "array_proto_col",
                                             "array_enum_col"}),
              Field(
                  &MutationOp::rows,
                  UnorderedElementsAre(ValueList{
                      Int64(4),
                      zetasql::values::Proto(proto_type, simple_proto3),
                      zetasql::values::Enum(enum_type, updated_enum),
                      zetasql::values::Array(array_proto_type->AsArray(),
                                               {zetasql::values::Proto(
                                                   proto_type, simple_proto3)}),
                      zetasql::values::Array(
                          array_enum_type->AsArray(),
                          {zetasql::values::Enum(enum_type, enum3)})})))))))
      .Times(1)
      .WillOnce(Return(absl::OkStatus()));
  EXPECT_THAT(query_engine().ExecuteSql(
                  Query{
                      R"sql(UPDATE test_table
              SET enum_col='TEST_ENUM_ONE'
               WHERE proto_col.field = 'Four')sql"},
                  QueryContext{proto_schema(), &reader, &writer}),
              IsOkAndHolds(Field(&QueryResult::modified_row_count, 1)));
}

TEST_P(QueryEngineTest, ExecuteSqlUpdateInvalidProtoWithEnumColumnInTable) {
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // Protos are unsupported in the PG dialect.
    return;
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto reader, PopulateProtoReader());
  MockRowWriter writer;
  EXPECT_THAT(query_engine().ExecuteSql(
                  Query{R"sql(UPDATE test_table )sql"
                        R"sql(SET proto_col = 'invalid_field: "Updated"'
                WHERE enum_col = 'TEST_ENUM_TWO')sql"},
                  QueryContext{proto_schema(), &reader, &writer}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Could not cast literal")));
  EXPECT_THAT(query_engine().ExecuteSql(
                  Query{
                      R"sql(UPDATE test_table
              SET enum_col='TEST_ENUM_EIGHT'
              WHERE proto_col.field = 'Four')sql"},
                  QueryContext{proto_schema(), &reader, &writer}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Could not cast literal")));
}

TEST_P(QueryEngineTest, ExecuteSqlUpdateArrayProtoWithArrayEnumColumnInTable) {
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // Protos are unsupported in the PG dialect.
    return;
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto reader, PopulateProtoReader());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto proto_type,
      MakeProtoType(proto_schema(), "emulator.tests.common.Simple"));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto enum_type,
      MakeEnumType(proto_schema(), "emulator.tests.common.TestEnum"));
  const zetasql::Type* array_proto_type;
  ZETASQL_ASSERT_OK(type_factory()->MakeArrayType(proto_type, &array_proto_type));
  const zetasql::Type* array_enum_type;
  ZETASQL_ASSERT_OK(type_factory()->MakeArrayType(enum_type, &array_enum_type));
  ::emulator::tests::common::Simple simple_proto2;
  simple_proto2.set_field("Two");
  ::emulator::tests::common::TestEnum enum2 =
      ::emulator::tests::common::TEST_ENUM_TWO;

  ::emulator::tests::common::Simple simple_proto_updated;
  simple_proto_updated.set_field("Updated");
  MockRowWriter writer;
  EXPECT_CALL(
      writer,
      Write(Property(
          &Mutation::ops,
          UnorderedElementsAre(AllOf(
              Field(&MutationOp::type, MutationOpType::kUpdate),
              Field(&MutationOp::table, "test_table"),
              Field(&MutationOp::columns,
                    std::vector<std::string>{"int64_col", "proto_col",
                                             "enum_col", "array_proto_col",
                                             "array_enum_col"}),
              Field(&MutationOp::rows,
                    UnorderedElementsAre(ValueList{
                        Int64(2),
                        zetasql::values::Proto(proto_type, simple_proto2),
                        zetasql::values::Enum(enum_type, enum2),
                        zetasql::values::Array(
                            array_proto_type->AsArray(),
                            {zetasql::values::Proto(proto_type,
                                                      simple_proto_updated)}),
                        zetasql::values::Array(
                            array_enum_type->AsArray(),
                            {zetasql::values::Enum(enum_type, enum2)})})))))))
      .Times(1)
      .WillOnce(Return(absl::OkStatus()));

  EXPECT_THAT(query_engine().ExecuteSql(
                  Query{
                      R"sql(UPDATE test_table
              SET array_proto_col = ARRAY<emulator.tests.common.Simple>['field: "Updated"']
              WHERE array_enum_col[OFFSET(0)] = 'TEST_ENUM_TWO')sql"},
                  QueryContext{proto_schema(), &reader, &writer}),
              IsOkAndHolds(Field(&QueryResult::modified_row_count, 1)));
  ::emulator::tests::common::Simple simple_proto3;
  simple_proto3.set_field("Four");
  ::emulator::tests::common::TestEnum enum3 =
      ::emulator::tests::common::TEST_ENUM_FOUR;
  ::emulator::tests::common::TestEnum updated_enum =
      ::emulator::tests::common::TEST_ENUM_ONE;
  EXPECT_CALL(
      writer,
      Write(Property(
          &Mutation::ops,
          UnorderedElementsAre(AllOf(
              Field(&MutationOp::type, MutationOpType::kUpdate),
              Field(&MutationOp::table, "test_table"),
              Field(&MutationOp::columns,
                    std::vector<std::string>{"int64_col", "proto_col",
                                             "enum_col", "array_proto_col",
                                             "array_enum_col"}),
              Field(
                  &MutationOp::rows,
                  UnorderedElementsAre(ValueList{
                      Int64(4),
                      zetasql::values::Proto(proto_type, simple_proto3),
                      zetasql::values::Enum(enum_type, enum3),
                      zetasql::values::Array(array_proto_type->AsArray(),
                                               {zetasql::values::Proto(
                                                   proto_type, simple_proto3)}),
                      zetasql::values::Array(
                          array_enum_type->AsArray(),
                          {zetasql::values::Enum(enum_type,
                                                   updated_enum)})})))))))
      .Times(1)
      .WillOnce(Return(absl::OkStatus()));

  EXPECT_THAT(query_engine().ExecuteSql(
                  Query{
                      R"sql(UPDATE test_table
              SET array_enum_col = ARRAY<emulator.tests.common.TestEnum>['TEST_ENUM_ONE']
              WHERE array_proto_col[OFFSET(0)].field = 'Four')sql"},
                  QueryContext{proto_schema(), &reader, &writer}),
              IsOkAndHolds(Field(&QueryResult::modified_row_count, 1)));
}

TEST_P(QueryEngineTest, ParameterProtoField) {
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // Protos are unsupported in the PG dialect.
    return;
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto reader, PopulateProtoReader());
  Query query{
      R"sql(Select int64_col from test_table WHERE proto_col.field=@param)sql",
      {{"param", zetasql::values::String("One")}}};
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      QueryResult result,
      query_engine().ExecuteSql(query, QueryContext{proto_schema(), &reader}));
  EXPECT_THAT(GetAllColumnValues(std::move(result.rows)),
              IsOkAndHolds(UnorderedElementsAre(ElementsAre(Int64(1)))));
}

TEST_P(QueryEngineTest, ParameterEnums) {
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // Protos are unsupported in the PG dialect.
    return;
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto reader, PopulateProtoReader());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto enum_type,
      MakeEnumType(proto_schema(), "emulator.tests.common.TestEnum"));
  Query query{
      R"sql(Select int64_col from test_table WHERE enum_col=@param)sql",
      {{"param", zetasql::values::Enum(
                     enum_type, ::emulator::tests::common::TEST_ENUM_TWO)}}};
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      QueryResult result,
      query_engine().ExecuteSql(query, QueryContext{proto_schema(), &reader}));
  EXPECT_THAT(GetAllColumnValues(std::move(result.rows)),
              IsOkAndHolds(UnorderedElementsAre(ElementsAre(Int64(2)))));
}
// Tests for @{parameter_sensitive=always|never|auto} query hint.
struct ParameterSensitiveHintInfo {
  // Value of @{parameter_sensitive} hint.
  std::string hint_value;
  // A flag to indicate whether the value is supported or not.
  bool is_valid;
  database_api::DatabaseDialect dialect =
      database_api::DatabaseDialect::GOOGLE_STANDARD_SQL;
  static std::vector<ParameterSensitiveHintInfo> TestCases() {
    std::vector<ParameterSensitiveHintInfo> test_cases = {
        {.hint_value = "auto", .is_valid = true},
        {.hint_value = "never", .is_valid = true},
        {.hint_value = "always", .is_valid = true},
        {.hint_value = "abc", .is_valid = false},
        {.hint_value = "12", .is_valid = false},
    };

    int num_tests = test_cases.size();
    test_cases.reserve(num_tests * 2);
    for (int i = 0; i < num_tests; ++i) {
      ParameterSensitiveHintInfo pg_test_case = test_cases[i];
      pg_test_case.dialect = database_api::DatabaseDialect::POSTGRESQL;
      test_cases.emplace_back(pg_test_case);
    }

    return test_cases;
  }
};

class ParameterSensitiveHintTests
    : public QueryEngineTestBase,
      public ::testing::WithParamInterface<ParameterSensitiveHintInfo> {
  void SetUp() override {
    const ParameterSensitiveHintInfo& test_params = GetParam();
    if (test_params.dialect == database_api::DatabaseDialect::POSTGRESQL) {
      schema_ = test::CreateSchemaWithOneTable(
          &type_factory_, database_api::DatabaseDialect::POSTGRESQL);
      multi_table_schema_ = test::CreateSchemaWithMultiTables(
          &type_factory_, database_api::DatabaseDialect::POSTGRESQL);
    } else if (test_params.dialect ==
               database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
      schema_ = test::CreateSchemaWithOneTable(&type_factory_);
      multi_table_schema_ = test::CreateSchemaWithMultiTables(&type_factory_);
    }
  }
};

TEST_P(ParameterSensitiveHintTests, TestParameterSensitiveHint) {
  const ParameterSensitiveHintInfo& test_params = GetParam();
  std::string hint =
      absl::Substitute("@{parameter_sensitive=$0} ", test_params.hint_value);
  if (test_params.dialect == database_api::DatabaseDialect::POSTGRESQL) {
    hint = absl::Substitute("/*@ parameter_sensitive=$0 */ ",
                            test_params.hint_value);
  }
  SCOPED_TRACE(absl::StrCat("hint=", test_params.hint_value));

  const auto query = absl::StrCat(hint, "SELECT string_col FROM test_table");
  if (test_params.is_valid) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(QueryResult result,
                         query_engine().ExecuteSql(
                             Query{query}, QueryContext{schema(), reader()}));
    ASSERT_NE(result.rows, nullptr);
    EXPECT_THAT(GetColumnNames(*result.rows), ElementsAre("string_col"));
    EXPECT_THAT(GetColumnTypes(*result.rows), ElementsAre(StringType()));
    EXPECT_THAT(GetAllColumnValues(std::move(result.rows)),
                IsOkAndHolds(UnorderedElementsAre(
                    ElementsAre(String("one")), ElementsAre(String("two")),
                    ElementsAre(String("four")))));
  } else {
    EXPECT_THAT(
        query_engine().ExecuteSql(Query{query},
                                  QueryContext{schema(), reader()}),
        StatusIs(
            absl::StatusCode::kInvalidArgument,
            HasSubstr("Invalid hint value for: parameter_sensitive hint")));
  }
}

INSTANTIATE_TEST_SUITE_P(
    RunParameterSensitiveHintTests, ParameterSensitiveHintTests,
    testing::ValuesIn(ParameterSensitiveHintInfo::TestCases()));

TEST_P(QueryEngineTest, ExecuteSqlInsertReturning) {
  std::string returning =
      (GetParam() == database_api::DatabaseDialect::POSTGRESQL) ? "RETURNING"
                                                                : "THEN RETURN";
  MockRowWriter writer;
  EXPECT_CALL(
      writer,
      Write(Property(
          &Mutation::ops,
          UnorderedElementsAre(
              AllOf(Field(&MutationOp::type, MutationOpType::kInsert),
                    Field(&MutationOp::table, "test_table"),
                    Field(&MutationOp::columns,
                          std::vector<std::string>{"int64_col", "string_col"}),
                    Field(&MutationOp::rows,
                          UnorderedElementsAre(
                              ValueList{Int64(3), String("three")},
                              ValueList{Int64(5), String("five")})))))))
      .Times(1)
      .WillOnce(Return(absl::OkStatus()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      QueryResult result,
      query_engine().ExecuteSql(
          Query{absl::StrCat(
              "INSERT INTO test_table (int64_col, string_col) "
              "VALUES(5, 'five'), (3, 'three') ",
              returning, " int64_col + 1 AS new_col1, string_col AS new_col")},
          QueryContext{schema(), reader(), &writer}));

  ASSERT_NE(result.rows, nullptr);
  EXPECT_EQ(result.modified_row_count, 2);
  EXPECT_THAT(GetColumnNames(*result.rows), ElementsAre("new_col1", "new_col"));
  EXPECT_THAT(GetColumnTypes(*result.rows),
              ElementsAre(Int64Type(), StringType()));
  EXPECT_THAT(
      GetAllColumnValues(std::move(result.rows)),
      IsOkAndHolds(UnorderedElementsAre(ValueList{Int64(4), String("three")},
                                        ValueList{Int64(6), String("five")})));
}

TEST_P(QueryEngineTest, ExecuteSqlDeleteReturning) {
  std::string returning =
      (GetParam() == database_api::DatabaseDialect::POSTGRESQL) ? "RETURNING"
                                                                : "THEN RETURN";
  MockRowWriter writer;
  EXPECT_CALL(writer,
              Write(Property(
                  &Mutation::ops,
                  UnorderedElementsAre(AllOf(
                      Field(&MutationOp::type, MutationOpType::kDelete),
                      Field(&MutationOp::table, "test_table"),
                      Field(&MutationOp::key_set,
                            Property(&KeySet::keys, UnorderedElementsAre(
                                                        Key{{Int64(2)}},
                                                        Key{{Int64(4)}}))))))))
      .Times(1)
      .WillOnce(Return(absl::OkStatus()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      QueryResult result,
      query_engine().ExecuteSql(
          Query{absl::StrCat("DELETE FROM test_table WHERE int64_col > 1 ",
                             returning, " *")},
          QueryContext{schema(), reader(), &writer}));

  ASSERT_NE(result.rows, nullptr);
  EXPECT_EQ(result.modified_row_count, 2);
  EXPECT_THAT(GetColumnNames(*result.rows),
              ElementsAre("int64_col", "string_col"));
  EXPECT_THAT(GetColumnTypes(*result.rows),
              ElementsAre(Int64Type(), StringType()));
  EXPECT_THAT(
      GetAllColumnValues(std::move(result.rows)),
      IsOkAndHolds(UnorderedElementsAre(ValueList{Int64(2), String("two")},
                                        ValueList{Int64(4), String("four")})));
}

TEST_P(QueryEngineTest, ExecuteSqlUpdatesReturning) {
  std::string returning =
      (GetParam() == database_api::DatabaseDialect::POSTGRESQL) ? "RETURNING"
                                                                : "THEN RETURN";
  MockRowWriter writer;
  EXPECT_CALL(
      writer,
      Write(Property(
          &Mutation::ops,
          UnorderedElementsAre(AllOf(
              Field(&MutationOp::type, MutationOpType::kUpdate),
              Field(&MutationOp::table, "test_table"),
              Field(&MutationOp::columns,
                    std::vector<std::string>{"int64_col", "string_col"}),
              Field(
                  &MutationOp::rows,
                  UnorderedElementsAre(ValueList{Int64(2), String("foo")},
                                       ValueList{Int64(4), String("foo")})))))))
      .Times(1)
      .WillOnce(Return(absl::OkStatus()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      QueryResult result,
      query_engine().ExecuteSql(
          Query{absl::StrCat("UPDATE test_table SET string_col = 'foo' WHERE "
                             "int64_col > 1 ",
                             returning, " *")},
          QueryContext{schema(), reader(), &writer}));

  ASSERT_NE(result.rows, nullptr);
  EXPECT_EQ(result.modified_row_count, 2);
  EXPECT_THAT(GetColumnNames(*result.rows),
              ElementsAre("int64_col", "string_col"));
  EXPECT_THAT(GetColumnTypes(*result.rows),
              ElementsAre(Int64Type(), StringType()));
  EXPECT_THAT(
      GetAllColumnValues(std::move(result.rows)),
      IsOkAndHolds(UnorderedElementsAre(ValueList{Int64(2), String("foo")},
                                        ValueList{Int64(4), String("foo")})));
}

TEST_P(QueryEngineTest, JsonConverterFunctionsForGsql) {
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    return;
  }

  struct JsonFunctionTestCase {
    std::string name;
    std::string function_call;
  };
  // All these functions are ZetaSQL build-in functions and we have compliance
  // tests to cover their correctness already. In this test, we only need to
  // confirm that these functions are callable from emulator interface.
  std::vector<JsonFunctionTestCase> test_cases = {
      // Converter functions.
      {"int64", R"(SELECT INT64(JSON '10'))"},
      {"float64", R"(SELECT FLOAT64(JSON '10'))"},
      {"bool", R"(SELECT BOOL(JSON 'true'))"},
      {"string", R"(SELECT STRING(JSON '"string_value"'))"},
      // JSON type functions.
      {"json_type", R"(SELECT JSON_TYPE(JSON '10'))"},
      // LAX converter functions.
      {"lax_int64", R"(SELECT LAX_INT64(JSON '10'))"},
      {"lax_float64", R"(SELECT LAX_FLOAT64(JSON '10'))"},
      {"lax_bool", R"(SELECT LAX_BOOL(JSON 'true'))"},
      {"lax_string", R"(SELECT LAX_STRING(JSON '"string_value"'))"},
      // SAFE versions of these functions.
      {"safe.int64", R"(SELECT SAFE.INT64(JSON '"10"'))"},
      {"safe.float64", R"(SELECT SAFE.FLOAT64(JSON '"10"'))"},
      {"safe.bool", R"(SELECT SAFE.BOOL(JSON '"TRUE"'))"},
      {"safe.string", R"(SELECT SAFE.STRING(JSON '1'))"},
      {"safe.json_type", R"(SELECT SAFE.JSON_TYPE(JSON '[10]'))"},
      {"safe.lax_int64", R"(SELECT SAFE.LAX_INT64(JSON '"10"'))"},
      {"safe.lax_float64", R"(SELECT SAFE.LAX_FLOAT64(JSON '"10"'))"},
      {"safe.lax_bool", R"(SELECT SAFE.LAX_BOOL(JSON '"True"'))"},
      {
          "safe.lax_string",
          R"(SELECT SAFE.LAX_STRING(JSON '1'))",
      }};

  for (const auto& test_case : test_cases) {
    ZETASQL_ASSERT_OK(query_engine().ExecuteSql(Query{test_case.function_call},
                                        QueryContext{schema(), reader()}))
        << test_case.name
        << " failed with function call: " << test_case.function_call;
  }
}

TEST_P(QueryEngineTest, QueryingOnViews) {
  test::ScopedEmulatorFeatureFlagsSetter setter({.enable_views = true});
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      QueryResult result,
      query_engine().ExecuteSql(Query{"SELECT * FROM test_view"},
                                QueryContext{views_schema(), reader()}));

  ASSERT_NE(result.rows, nullptr);
  EXPECT_EQ(result.modified_row_count, 0);
  EXPECT_THAT(GetColumnNames(*result.rows), ElementsAre("vcol", "col"));
  EXPECT_THAT(GetColumnTypes(*result.rows),
              ElementsAre(Int64Type(), StringType()));
  EXPECT_THAT(
      GetAllColumnValues(std::move(result.rows)),
      IsOkAndHolds(UnorderedElementsAre(ValueList{Int64(3), String("aone")},
                                        ValueList{Int64(5), String("atwo")},
                                        ValueList{Int64(9), String("afour")})));
}

TEST_P(QueryEngineTest, QueryingSelectedViewColumns) {
  test::ScopedEmulatorFeatureFlagsSetter setter({.enable_views = true});
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      QueryResult result,
      query_engine().ExecuteSql(Query{"SELECT col FROM test_view"},
                                QueryContext{views_schema(), reader()}));

  ASSERT_NE(result.rows, nullptr);
  EXPECT_EQ(result.modified_row_count, 0);
  EXPECT_THAT(GetColumnNames(*result.rows), ElementsAre("col"));
  EXPECT_THAT(GetColumnTypes(*result.rows), ElementsAre(StringType()));
  EXPECT_THAT(GetAllColumnValues(std::move(result.rows)),
              IsOkAndHolds(UnorderedElementsAre(ValueList{String("aone")},
                                                ValueList{String("atwo")},
                                                ValueList{String("afour")})));
}

TEST_P(QueryEngineTest, ViewsInsideDML) {
  test::ScopedEmulatorFeatureFlagsSetter setter({.enable_views = true});
  MockRowWriter writer;
  EXPECT_CALL(
      writer,
      Write(Property(
          &Mutation::ops,
          UnorderedElementsAre(
              AllOf(Field(&MutationOp::type, MutationOpType::kInsert),
                    Field(&MutationOp::table, "test_table"),
                    Field(&MutationOp::columns,
                          std::vector<std::string>{"int64_col", "string_col"}),
                    Field(&MutationOp::rows,
                          UnorderedElementsAre(
                              ValueList{Int64(3), String("aone")},
                              ValueList{Int64(5), String("atwo")},
                              ValueList{Int64(9), String("afour")})))))))
      .Times(1)
      .WillOnce(Return(absl::OkStatus()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      QueryResult result,
      query_engine().ExecuteSql(
          Query{"INSERT INTO test_table (int64_col, string_col) "
                "SELECT v.vcol, v.col FROM test_view v"},
          QueryContext{views_schema(), reader(), &writer}));

  EXPECT_EQ(result.rows, nullptr);
  EXPECT_EQ(result.modified_row_count, 3);
}

TEST_P(QueryEngineTest, InsertOrUpdateDmlInsertsNewRows) {
  std::string sql;
  // The insert statement inserts 2 new rows and updates the existing row with
  // primary key (int64_col:1).
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    sql =
        "INSERT INTO test_table (int64_col, string_col) "
        "VALUES(10, 'ten'), (1, 'one updated'), (3, 'three updated') "
        "ON CONFLICT(int64_col) DO UPDATE SET "
        "int64_col = excluded.int64_col, string_col = excluded.string_col";
  } else {
    sql =
        "INSERT OR UPDATE INTO test_table (int64_col, string_col) "
        "VALUES(10, 'ten'), (1, 'one updated'), (3, 'three updated')";
  }
  MockRowWriter writer;
  EXPECT_CALL(
      writer,
      Write(Property(
          &Mutation::ops,
          UnorderedElementsAre(AllOf(
              Field(&MutationOp::type, MutationOpType::kInsertOrUpdate),
              Field(&MutationOp::table, "test_table"),
              Field(&MutationOp::columns,
                    std::vector<std::string>{"int64_col", "string_col"}),
              Field(&MutationOp::rows,
                    UnorderedElementsAre(
                        ValueList{Int64(10), String("ten")},
                        ValueList{Int64(1), String("one updated")},
                        ValueList{Int64(3), String("three updated")})))))))
      .Times(1)
      .WillOnce(Return(absl::OkStatus()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      QueryResult result,
      query_engine().ExecuteSql(Query{sql},
                                QueryContext{schema(), reader(), &writer}));

  ASSERT_EQ(result.rows, nullptr);
  EXPECT_EQ(result.modified_row_count, 3);
}

TEST_P(QueryEngineTest, InsertOrUpdateDuplicateInputRowsReturnError) {
  std::string sql;
  // Spanner does not allow duplicate insert rows with same key.
  // The insert statement inserts 2 rows with same key (int64_col:10).
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    sql =
        "INSERT INTO test_table (int64_col, string_col) "
        "VALUES(10, 'ten'), (1, 'one updated'), (10, 'ten') "
        "ON CONFLICT(int64_col) DO UPDATE SET "
        "int64_col = excluded.int64_col, string_col = excluded.string_col";
  } else {
    sql =
        "INSERT OR UPDATE INTO test_table (int64_col, string_col) "
        "VALUES(10, 'ten'), (1, 'one updated'), (10, 'ten')";
  }
  MockRowWriter writer;

  EXPECT_THAT(query_engine().ExecuteSql(
                  Query{sql}, QueryContext{schema(), reader(), &writer}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Cannot affect a row second time for key: "
                                 "{Int64(10)}")));
}

TEST_P(QueryEngineTest, UpsertPGqueryWithGeneratedKeyUnsupported) {
  if (GetParam() == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    GTEST_SKIP();
  }

  MockRowWriter writer;
  EXPECT_THAT(
      query_engine().ExecuteSql(
          Query{"INSERT INTO test_table(k1_pk, k2, k4) VALUES(1, 1, 1) "
                "ON CONFLICT(k1_pk, k3gen_storedpk) DO NOTHING"},
          QueryContext{gpk_schema(), gpk_table_reader(), &writer}),
      StatusIs(absl::StatusCode::kUnimplemented,
               HasSubstr("ON CONFLICT clause on table with generated key is "
                         "not supported in Emulator")));

  EXPECT_THAT(
      query_engine().ExecuteSql(
          Query{"INSERT INTO test_table(k1_pk, k2, k4) VALUES(1, 1, 1) "
                "ON CONFLICT(k1_pk,k3gen_storedpk) DO UPDATE SET "
                "k1_pk = excluded.k1_pk, k2 = excluded.k2, k4 = excluded.k4"},
          QueryContext{gpk_schema(), gpk_table_reader(), &writer}),
      StatusIs(absl::StatusCode::kUnimplemented,
               HasSubstr("ON CONFLICT clause on table with generated key is "
                         "not supported in Emulator")));
}

TEST_P(QueryEngineTest, BitReverseUnsupportedWhenFlagIsOff) {
  std::string spanner_prefix = "";
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    spanner_prefix = "spanner.";
  }
  test::ScopedEmulatorFeatureFlagsSetter setter(
      {.enable_bit_reversed_positive_sequences = false});

  Query query{absl::StrCat("SELECT ", spanner_prefix, "BIT_REVERSE(1, true)")};
  EXPECT_THAT(
      query_engine().ExecuteSql(query, QueryContext{schema(), reader()}),
      StatusIs(absl::StatusCode::kUnimplemented));
}

TEST_P(QueryEngineTest, GetInternalSequenceStateUnsupportedWhenFlagIsOff) {
  std::string spanner_prefix = "";
  std::string sequence_name = "SEQUENCE myseq";
  absl::StatusCode code = absl::StatusCode::kUnimplemented;
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    spanner_prefix = "spanner.";
    sequence_name = "'myseq'";
  }
  test::ScopedEmulatorFeatureFlagsSetter setter(
      {.enable_bit_reversed_positive_sequences = false});

  Query query{absl::StrCat("SELECT ", spanner_prefix,
                           "get_internal_sequence_state(", sequence_name, ")")};
  EXPECT_THAT(query_engine().ExecuteSql(
                  query, QueryContext{sequence_schema(), reader()}),
              StatusIs(code));
}

TEST_P(QueryEngineTest, GetNextSequenceValueUnsupportedWhenFlagIsOff) {
  MockRowWriter writer;
  test::ScopedEmulatorFeatureFlagsSetter setter(
      {.enable_bit_reversed_positive_sequences = false});

  Query query{"INSERT INTO test_table (string_col) VALUES ('abc')"};
  EXPECT_THAT(query_engine().ExecuteSql(
                  query, QueryContext{sequence_schema(), reader(), &writer}),
              StatusIs(absl::StatusCode::kUnimplemented));
}

class DefaultValuesTest : public QueryEngineTest {
 public:
  const Schema* schema() { return dvschema_.get(); }
  RowReader* reader() { return &reader_; }
  void SetUp() override {
    action_manager_ = std::make_unique<ActionManager>();
    action_manager_->AddActionsForSchema(schema(),
                                         /*function_catalog=*/nullptr,
                                         type_factory());
  }

 private:
  std::unique_ptr<const Schema> dvschema_ =
      test::CreateSimpleDefaultValuesSchema(type_factory());
  test::TestRowReader reader_{
      {{"players",
        {{"player_id", "account_balance"},
         {zetasql::types::Int64Type(), zetasql::types::NumericType()},
         {{Int64(1), zetasql::values::Numeric(1.0)}}}}}};
  std::unique_ptr<ActionManager> action_manager_;
};

TEST_F(DefaultValuesTest, ExecuteInsertsDefaultValues) {
  MockRowWriter writer;
  EXPECT_CALL(
      writer,
      Write(Property(
          &Mutation::ops,
          UnorderedElementsAre(AllOf(
              Field(&MutationOp::type, MutationOpType::kInsert),
              Field(&MutationOp::table, "players"),
              Field(&MutationOp::columns,
                    std::vector<std::string>{"player_id", "account_balance"}),
              Field(&MutationOp::rows,
                    UnorderedElementsAre(ValueList{
                        Int64(2), zetasql::values::Numeric(0)})))))))
      .Times(1)
      .WillOnce(Return(absl::OkStatus()));
  EXPECT_THAT(
      query_engine().ExecuteSql(Query{"INSERT INTO players (player_id) "
                                      "VALUES (2)"},
                                QueryContext{schema(), reader(), &writer}),
      IsOkAndHolds(Field(&QueryResult::modified_row_count, 1)));
}

TEST_F(DefaultValuesTest, InsertOrUpdateDefaultValues) {
  // The insert statement inserts 2 new rows. The existing row with primary
  // key (player_id:1) is updated from the previously inserted value of 1.0 to
  // the new value of `account_balance`.
  std::string sql =
      "INSERT OR UPDATE INTO players (player_id, account_balance) "
      "VALUES(10, 10.0), (1, 100.0), (3, 3.0)";
  MockRowWriter writer;
  EXPECT_CALL(
      writer,
      Write(Property(
          &Mutation::ops,
          UnorderedElementsAre(AllOf(
              Field(&MutationOp::type, MutationOpType::kInsertOrUpdate),
              Field(&MutationOp::table, "players"),
              Field(&MutationOp::columns,
                    std::vector<std::string>{"player_id", "account_balance"}),
              Field(&MutationOp::rows,
                    UnorderedElementsAre(
                        ValueList{Int64(10), zetasql::values::Numeric(10.0)},
                        ValueList{Int64(1), zetasql::values::Numeric(100.0)},
                        ValueList{Int64(3),
                                  zetasql::values::Numeric(3.0)})))))))
      .Times(1)
      .WillOnce(Return(absl::OkStatus()));
  EXPECT_THAT(query_engine().ExecuteSql(
                  Query{sql}, QueryContext{schema(), reader(), &writer}),
              IsOkAndHolds(Field(&QueryResult::modified_row_count, 3)));
}

class DefaultKeyTest
    : public QueryEngineTestBase,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  const Schema* schema() { return dkschema_.get(); }
  RowReader* reader() { return &reader_; }
  void SetUp() override {
    if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
      dkschema_ = test::CreateSimpleDefaultKeySchema(
          type_factory(), database_api::DatabaseDialect::POSTGRESQL);
    } else {
      dkschema_ = test::CreateSimpleDefaultKeySchema(
          type_factory(), database_api::DatabaseDialect::GOOGLE_STANDARD_SQL);
    }
    action_manager_ = std::make_unique<ActionManager>();
    action_manager_->AddActionsForSchema(schema(),
                                         /*function_catalog=*/nullptr,
                                         type_factory());
  }

 private:
  std::unique_ptr<const Schema> dkschema_;
  test::TestRowReader reader_{
      {{"players_default_key",
        {{"prefix", "player_id", "balance"},
         {zetasql::types::Int64Type(), zetasql::types::Int64Type(),
          zetasql::types::Int64Type()},
         {{Int64(100), Int64(1), Int64(100)},
          {Int64(1), Int64(1), Int64(1)}}}}}};
  std::unique_ptr<ActionManager> action_manager_;
};

INSTANTIATE_TEST_SUITE_P(
    DefaultKeyPerDialectTests, DefaultKeyTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<DefaultKeyTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(DefaultKeyTest, InsertOrUpdateDefaultKey) {
  // The insert statement inserts 1 new row. The existing row with
  // default primary key (prefix: 100, player_id:1) is updated to from previous
  // value of 1 to the new value of `balance`.
  std::string sql;
  if (GetParam() == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    sql =
        "INSERT OR UPDATE INTO players_default_key (player_id, balance) "
        "VALUES(1, 1000), (2, 2000)";
  } else {
    sql =
        "INSERT INTO players_default_key (player_id, balance) "
        "VALUES(1, 1000), (2, 2000) ON CONFLICT (prefix, player_id) "
        "DO UPDATE SET player_id = excluded.player_id, balance = "
        "excluded.balance";
  }
  MockRowWriter writer;
  EXPECT_CALL(
      writer,
      Write(Property(
          &Mutation::ops,
          UnorderedElementsAre(AllOf(
              Field(&MutationOp::type, MutationOpType::kInsertOrUpdate),
              Field(&MutationOp::table, "players_default_key"),
              Field(&MutationOp::columns,
                    std::vector<std::string>{"prefix", "player_id", "balance"}),
              Field(&MutationOp::rows,
                    UnorderedElementsAre(
                        ValueList{Int64(100), Int64(1), Int64(1000)},
                        ValueList{Int64(100), Int64(2), Int64(2000)})))))))
      .Times(1)
      .WillOnce(Return(absl::OkStatus()));
  EXPECT_THAT(query_engine().ExecuteSql(
                  Query{sql}, QueryContext{schema(), reader(), &writer}),
              IsOkAndHolds(Field(&QueryResult::modified_row_count, 2)));
}

TEST_P(DefaultKeyTest, InsertOrUpdateDuplicateInputRowsReturnError) {
  // Spanner does not allow duplicate insert rows with same key.
  // The insert statement inserts 2 rows with same key
  // (prefix: 100, player_id:1).
  std::string sql;
  if (GetParam() == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    sql =
        "INSERT OR UPDATE INTO players_default_key (player_id, balance) "
        "VALUES(1, 20), (1, 200)";
  } else {
    sql =
        "INSERT INTO players_default_key (player_id, balance) "
        "VALUES(1, 20), (1, 200) ON CONFLICT (prefix, player_id) "
        "DO UPDATE SET player_id = excluded.player_id, balance = "
        "excluded.balance";
  }
  MockRowWriter writer;

  EXPECT_THAT(query_engine().ExecuteSql(
                  Query{sql}, QueryContext{schema(), reader(), &writer}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Cannot affect a row second time for key: "
                                 "{Int64(100), Int64(1)}")));
}

class GeneratedPrimaryKeyTest : public QueryEngineTest {
 public:
  const Schema* schema() { return gpkschema_.get(); }
  RowReader* reader() { return &reader_; }
  void SetUp() override {
    ZETASQL_ASSERT_OK_AND_ASSIGN(gpkschema_,
                         test::CreateGpkSchemaWithOneTable(type_factory()));
    action_manager_ = std::make_unique<ActionManager>();
    action_manager_->AddActionsForSchema(
        schema(), query_engine().function_catalog(), type_factory());
  }

 private:
  test::ScopedEmulatorFeatureFlagsSetter feature_flags_setter_ =
      test::ScopedEmulatorFeatureFlagsSetter({.enable_generated_pk = true});
  std::unique_ptr<const Schema> gpkschema_;
  test::TestRowReader reader_{
      {{"test_table",
        {{"k1_pk", "k2", "k3gen_storedpk", "k4", "k5"},
         {zetasql::types::Int64Type(), zetasql::types::Int64Type(),
          zetasql::types::Int64Type(), zetasql::types::Int64Type(),
          zetasql::types::Int64Type()},
         {{Int64(1), Int64(1), Int64(1), Int64(1), Int64(2)},
          {Int64(2), Int64(2), Int64(2), Int64(2), Int64(3)},
          {Int64(4), Int64(4), Int64(4), Int64(4), Int64(5)}}}}}};
  std::unique_ptr<ActionManager> action_manager_;
};

TEST_F(GeneratedPrimaryKeyTest, ExecuteInsertsTwoRows) {
  MockRowWriter writer;
  EXPECT_CALL(writer,
              Write(Property(
                  &Mutation::ops,
                  UnorderedElementsAre(AllOf(
                      Field(&MutationOp::type, MutationOpType::kInsert),
                      Field(&MutationOp::table, "test_table"),
                      Field(&MutationOp::columns,
                            std::vector<std::string>{"k1_pk", "k2", "k4"}),
                      Field(&MutationOp::rows,
                            UnorderedElementsAre(
                                ValueList{Int64(3), Int64(3), Int64(5)},
                                ValueList{Int64(3), Int64(4), Int64(5)})))))))
      .Times(1)
      .WillOnce(Return(absl::OkStatus()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      QueryResult result,
      query_engine().ExecuteSql(Query{"INSERT INTO test_table (k1_pk,k2,k4) "
                                      "VALUES(3,3,5), (3,4,5)"},
                                QueryContext{schema(), reader(), &writer}));
  EXPECT_EQ(result.modified_row_count, 2);
}

TEST_F(GeneratedPrimaryKeyTest, FailsExecuteInsertsTwoRowsIfGpkDisabled) {
  test::ScopedEmulatorFeatureFlagsSetter setter({.enable_generated_pk = false});
  MockRowWriter writer;
  EXPECT_THAT(
      query_engine().ExecuteSql(Query{"INSERT INTO test_table (k1_pk,k2,k4) "
                                      "VALUES(3,3,5), (3,4,5)"},
                                QueryContext{schema(), reader(), &writer}),
      StatusIs(absl::StatusCode::kAlreadyExists,
               HasSubstr("Failed to insert row with primary key "
                         "({pk#k1_pk:3, pk#k3gen_storedpk:NULL})")));
}

TEST_F(GeneratedPrimaryKeyTest, ExecuteSqlDeleteRows) {
  MockRowWriter writer;
  EXPECT_CALL(
      writer,
      Write(Property(&Mutation::ops,
                     UnorderedElementsAre(AllOf(
                         Field(&MutationOp::type, MutationOpType::kDelete),
                         Field(&MutationOp::table, "test_table"),
                         Field(&MutationOp::key_set,
                               Property(&KeySet::keys,
                                        UnorderedElementsAre(
                                            Key{{Int64(2), Int64(2)}},
                                            Key{{Int64(4), Int64(4)}}))))))))
      .Times(1)
      .WillOnce(Return(absl::OkStatus()));
  EXPECT_THAT(
      query_engine().ExecuteSql(Query{"DELETE FROM test_table "
                                      "WHERE k3gen_storedpk > 1"},
                                QueryContext{schema(), reader(), &writer}),
      IsOkAndHolds(Field(&QueryResult::modified_row_count, 2)));
}

TEST_F(GeneratedPrimaryKeyTest, ExecuteSqlUpdatesRows) {
  MockRowWriter writer;
  EXPECT_CALL(writer,
              Write(Property(
                  &Mutation::ops,
                  UnorderedElementsAre(AllOf(
                      Field(&MutationOp::type, MutationOpType::kUpdate),
                      Field(&MutationOp::table, "test_table"),
                      Field(&MutationOp::columns,
                            std::vector<std::string>{"k1_pk", "k2", "k4"}),
                      Field(&MutationOp::rows,
                            UnorderedElementsAre(
                                ValueList{Int64(2), Int64(2), Int64(8)},
                                ValueList{Int64(4), Int64(4), Int64(8)})))))))
      .Times(1)
      .WillOnce(Return(absl::OkStatus()));
  EXPECT_THAT(
      query_engine().ExecuteSql(Query{"UPDATE test_table "
                                      "SET k4 = 8 WHERE k3gen_storedpk > 1"},
                                QueryContext{schema(), reader(), &writer}),
      IsOkAndHolds(Field(&QueryResult::modified_row_count, 2)));
}

TEST_F(GeneratedPrimaryKeyTest, CannotInsertDuplicateValuesForPrimaryKey) {
  MockRowWriter writer;
  EXPECT_THAT(
      query_engine().ExecuteSql(Query{"INSERT INTO test_table (k1_pk,k2,k4) "
                                      "VALUES(2,2,8)"},
                                QueryContext{schema(), reader(), &writer}),
      StatusIs(absl::StatusCode::kAlreadyExists,
               HasSubstr("Failed to insert row with primary key "
                         "({pk#k1_pk:2, pk#k3gen_storedpk:2})")));
}

TEST_F(GeneratedPrimaryKeyTest, CannotUpdatePrimaryKey) {
  MockRowWriter writer;
  EXPECT_THAT(
      query_engine().ExecuteSql(
          Query{
              "UPDATE test_table SET k3gen_storedpk=5 WHERE k3gen_storedpk=2"},
          QueryContext{schema(), reader(), &writer}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr(
              "Cannot UPDATE value on non-writable column: k3gen_storedpk")));
}

TEST_F(GeneratedPrimaryKeyTest, ExecuteInsertWithReturning) {
  MockRowWriter writer;
  EXPECT_CALL(writer,
              Write(Property(
                  &Mutation::ops,
                  UnorderedElementsAre(AllOf(
                      Field(&MutationOp::type, MutationOpType::kInsert),
                      Field(&MutationOp::table, "test_table"),
                      Field(&MutationOp::columns,
                            std::vector<std::string>{"k1_pk", "k2", "k4"}),
                      Field(&MutationOp::rows,
                            UnorderedElementsAre(
                                ValueList{Int64(3), Int64(3), Int64(4)},
                                ValueList{Int64(3), Int64(4), Int64(5)})))))))
      .Times(1)
      .WillOnce(Return(absl::OkStatus()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      QueryResult result,
      query_engine().ExecuteSql(
          Query{absl::StrCat("INSERT INTO test_table (k1_pk,k2,k4) "
                             "VALUES(3,3,4), (3,4,5) ",
                             "THEN RETURN k2, k3gen_storedpk, k5")},
          QueryContext{schema(), reader(), &writer}));
  ASSERT_NE(result.rows, nullptr);
  EXPECT_EQ(result.modified_row_count, 2);
  EXPECT_THAT(GetColumnNames(*result.rows),
              ElementsAre("k2", "k3gen_storedpk", "k5"));
  EXPECT_THAT(GetColumnTypes(*result.rows),
              ElementsAre(Int64Type(), Int64Type(), Int64Type()));
  EXPECT_THAT(GetAllColumnValues(std::move(result.rows)),
              IsOkAndHolds(UnorderedElementsAre(
                  ValueList{Int64(3), Int64(3), Int64(5)},
                  ValueList{Int64(4), Int64(4), Int64(6)})));
}

TEST_F(GeneratedPrimaryKeyTest, ExecuteUpdateWithReturning) {
  MockRowWriter writer;
  EXPECT_CALL(writer,
              Write(Property(
                  &Mutation::ops,
                  UnorderedElementsAre(AllOf(
                      Field(&MutationOp::type, MutationOpType::kUpdate),
                      Field(&MutationOp::table, "test_table"),
                      Field(&MutationOp::columns,
                            std::vector<std::string>{"k1_pk", "k2", "k4"}),
                      Field(&MutationOp::rows,
                            UnorderedElementsAre(
                                ValueList{Int64(2), Int64(2), Int64(8)},
                                ValueList{Int64(4), Int64(4), Int64(8)})))))))
      .Times(1)
      .WillOnce(Return(absl::OkStatus()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      QueryResult result,
      query_engine().ExecuteSql(Query{"UPDATE test_table "
                                      "SET k4 = 8 WHERE k3gen_storedpk > 1 "
                                      "THEN RETURN k2, k3gen_storedpk, k5"},
                                QueryContext{schema(), reader(), &writer}));

  ASSERT_NE(result.rows, nullptr);
  EXPECT_EQ(result.modified_row_count, 2);
  EXPECT_THAT(GetColumnNames(*result.rows),
              ElementsAre("k2", "k3gen_storedpk", "k5"));
  EXPECT_THAT(GetColumnTypes(*result.rows),
              ElementsAre(Int64Type(), Int64Type(), Int64Type()));

  EXPECT_THAT(GetAllColumnValues(std::move(result.rows)),
              IsOkAndHolds(UnorderedElementsAre(
                  ValueList{Int64(2), Int64(2), Int64(9)},
                  ValueList{Int64(4), Int64(4), Int64(9)})));
}

TEST_F(GeneratedPrimaryKeyTest, ExecuteDeleteWithReturning) {
  MockRowWriter writer;
  EXPECT_CALL(
      writer,
      Write(Property(&Mutation::ops,
                     UnorderedElementsAre(AllOf(
                         Field(&MutationOp::type, MutationOpType::kDelete),
                         Field(&MutationOp::table, "test_table"),
                         Field(&MutationOp::key_set,
                               Property(&KeySet::keys,
                                        UnorderedElementsAre(
                                            Key{{Int64(2), Int64(2)}},
                                            Key{{Int64(4), Int64(4)}}))))))))
      .Times(1)
      .WillOnce(Return(absl::OkStatus()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      QueryResult result,
      query_engine().ExecuteSql(Query{"DELETE FROM test_table "
                                      "WHERE k3gen_storedpk > 1 "
                                      "THEN RETURN k2, k3gen_storedpk, k5"},
                                QueryContext{schema(), reader(), &writer}));

  ASSERT_NE(result.rows, nullptr);
  EXPECT_EQ(result.modified_row_count, 2);
  EXPECT_THAT(GetColumnNames(*result.rows),
              ElementsAre("k2", "k3gen_storedpk", "k5"));
  EXPECT_THAT(GetColumnTypes(*result.rows),
              ElementsAre(Int64Type(), Int64Type(), Int64Type()));

  EXPECT_THAT(GetAllColumnValues(std::move(result.rows)),
              IsOkAndHolds(UnorderedElementsAre(
                  ValueList{Int64(2), Int64(2), Int64(3)},
                  ValueList{Int64(4), Int64(4), Int64(5)})));
}

TEST_F(GeneratedPrimaryKeyTest, InsertOrUpdateGPK) {
  // The insert statement inserts 1 new row and updates the existing row with
  // primary key (k1_pk:2, k3gen_storedpk:2).
  std::string sql =
      "INSERT OR UPDATE INTO test_table (k1_pk, k2, k4) "
      "VALUES (2, 2, 10), (3, 3, 12)";
  MockRowWriter writer;
  EXPECT_CALL(writer,
              Write(Property(
                  &Mutation::ops,
                  UnorderedElementsAre(AllOf(
                      Field(&MutationOp::type, MutationOpType::kInsertOrUpdate),
                      Field(&MutationOp::table, "test_table"),
                      Field(&MutationOp::columns,
                            std::vector<std::string>{"k1_pk", "k2", "k4"}),
                      Field(&MutationOp::rows,
                            UnorderedElementsAre(
                                ValueList{Int64(2), Int64(2), Int64(10)},
                                ValueList{Int64(3), Int64(3), Int64(12)})))))))
      .Times(1)
      .WillOnce(Return(absl::OkStatus()));
  EXPECT_THAT(query_engine().ExecuteSql(
                  Query{sql}, QueryContext{schema(), reader(), &writer}),
              IsOkAndHolds(Field(&QueryResult::modified_row_count, 2)));
}

TEST_F(GeneratedPrimaryKeyTest, InsertOrUpdateDuplicateInputRowsReturnError) {
  // Spanner does not allow duplicate insert rows with same key.
  // The insert statement inserts 2 rows with same key
  // (k1_pk:2, k3gen_storedpk:5).
  std::string sql =
      "INSERT OR UPDATE INTO test_table (k1_pk, k2, k4) "
      "VALUES (2, 5, 10), (2, 5, 100)";
  MockRowWriter writer;

  EXPECT_THAT(query_engine().ExecuteSql(
                  Query{sql}, QueryContext{schema(), reader(), &writer}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Cannot affect a row second time for key: "
                                 "{Int64(2), Int64(5)}")));
}

class TimestampKeyTest
    : public QueryEngineTestBase,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  const Schema* schema() { return tsschema_.get(); }
  RowReader* reader() { return &reader_; }
  void SetUp() override {
    if (GetParam() == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
      tsschema_ = test::CreateSimpleTimestampKeySchema(
          type_factory(), database_api::DatabaseDialect::GOOGLE_STANDARD_SQL);
    } else {
      tsschema_ = test::CreateSimpleTimestampKeySchema(
          type_factory(), database_api::DatabaseDialect::POSTGRESQL);
    }
    action_manager_ = std::make_unique<ActionManager>();
    action_manager_->AddActionsForSchema(schema(),
                                         /*function_catalog=*/nullptr,
                                         type_factory());
  }

 private:
  std::unique_ptr<const Schema> tsschema_;
  test::TestRowReader reader_{
      {{"timestamp_key_table",
        {{"k", "ts", "val"},
         {zetasql::types::Int64Type(), zetasql::types::TimestampType(),
          zetasql::types::Int64Type()},
         {{Int64(1), zetasql::values::TimestampFromUnixMicros(1), Int64(1)},
          {Int64(2), zetasql::values::TimestampFromUnixMicros(2),
           Int64(2)}}}}}};
  std::unique_ptr<ActionManager> action_manager_;
};

INSTANTIATE_TEST_SUITE_P(
    TimestampKeyPerDialectTests, TimestampKeyTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<TimestampKeyTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(TimestampKeyTest, InsertOrUpdateDuplicateInputRowsReturnError) {
  // Spanner does not allow duplicate insert rows with same key.
  // The insert statement inserts 2 rows with same key
  // (k:1, ts:commit_timestamp).
  std::string sql;
  if (GetParam() == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    sql =
        "INSERT OR UPDATE INTO timestamp_key_table (k, ts, val) "
        "VALUES (1, PENDING_COMMIT_TIMESTAMP(), 1), "
        "(1, PENDING_COMMIT_TIMESTAMP(), 2), (2, PENDING_COMMIT_TIMESTAMP(), "
        "2)";
  } else {
    sql =
        "INSERT INTO timestamp_key_table (k, ts, val) "
        "VALUES (1, SPANNER.PENDING_COMMIT_TIMESTAMP(), 1), "
        "(1, SPANNER.PENDING_COMMIT_TIMESTAMP(), 2), "
        "(2, SPANNER.PENDING_COMMIT_TIMESTAMP(), 2) "
        "ON CONFLICT(k, ts) DO UPDATE SET k = excluded.k, ts = excluded.ts, "
        "val = excluded.val";
  }
  MockRowWriter writer;

  EXPECT_THAT(
      query_engine().ExecuteSql(Query{sql},
                                QueryContext{schema(), reader(), &writer}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("Cannot affect a row second time for key: "
                    "{Int64(1), String(\"spanner.commit_timestamp()\")}")));
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
