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
#include <vector>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/substitute.h"
#include "backend/access/read.h"
#include "backend/access/write.h"
#include "backend/actions/manager.h"
#include "backend/datamodel/key_set.h"
#include "backend/datamodel/value.h"
#include "backend/query/catalog.h"
#include "backend/schema/catalog/schema.h"
#include "tests/common/row_reader.h"
#include "tests/common/schema_constructor.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

using testing::AllOf;
using testing::ElementsAre;
using testing::Field;
using testing::IsTrue;
using testing::Property;
using testing::Return;
using testing::UnorderedElementsAre;
using zetasql_base::testing::IsOkAndHolds;

using zetasql::values::Int64;
using zetasql::values::String;

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

class QueryEngineTestBase : public testing::Test {
 public:
  const Schema* schema() { return schema_.get(); }
  const Schema* multi_table_schema() { return multi_table_schema_.get(); }
  const Schema* views_schema() { return views_schema_.get(); }
  RowReader* reader() { return &reader_; }
  QueryEngine& query_engine() { return query_engine_; }
  zetasql::TypeFactory* type_factory() { return &type_factory_; }

 protected:
  zetasql::TypeFactory type_factory_;
  std::unique_ptr<const Schema> schema_;
  std::unique_ptr<const Schema> multi_table_schema_;

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
      test::ScopedEmulatorFeatureFlagsSetter({.enable_dml_returning = true});
};

class QueryEngineTest
    : public QueryEngineTestBase,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 protected:
  void SetUp() override {
    if (GetParam() == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
      schema_ = test::CreateSchemaWithOneTable(&type_factory_);
      multi_table_schema_ = test::CreateSchemaWithMultiTables(&type_factory_);
    }
  }
};

INSTANTIATE_TEST_SUITE_P(
    QueryEnginePerDialectTests, QueryEngineTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL
                    ),
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

TEST_P(QueryEngineTest, ExecuteSqlSelectsOneColumnFromTableWithForceIndexHint) {
  std::string hint = "@{force_index=test_index}";
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
  std::string hint = "@{force_index=_base_table}";
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
  Query query = {
      "SELECT @int64_p AS int64_p, @string_p AS string_p FROM test_table",
      {{"int64_p", Int64(24)}, {"string_p", String("bar")}}};
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
  EXPECT_THAT(
      query_engine().IsPartitionable(
          query, QueryContext{multi_table_schema(), reader()}),
      zetasql_base::testing::StatusIs(error_code, testing::HasSubstr(error_msg)));
}

TEST_P(QueryEngineTest, PartitionableSimpleScanNoTable) {
  std::string error_msg = kQueryNotASimpleTableScanError;
  Query query{"SELECT a FROM UNNEST([1, 2, 3]) AS a"};
  EXPECT_THAT(query_engine().IsPartitionable(
                  query, QueryContext{multi_table_schema(), reader()}),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument,
                                        testing::HasSubstr(error_msg)));
}

TEST_P(QueryEngineTest, PartitionableSimpleScanFilterNoTable) {
  std::string error_msg = kQueryNotASimpleTableScanError;
  Query query{"SELECT a FROM UNNEST([1, 2, 3]) AS a WHERE a = 1"};
  EXPECT_THAT(query_engine().IsPartitionable(
                  query, QueryContext{multi_table_schema(), reader()}),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument,
                                        testing::HasSubstr(error_msg)));
}

TEST_P(QueryEngineTest, PartitionableExecuteSqlSimpleScanFilterSubquery) {
  Query query{
      "SELECT string_col FROM test_table WHERE string_col = 'a' AND EXISTS "
      "(SELECT child_key FROM child_table)"};
  EXPECT_THAT(query_engine().IsPartitionable(
                  query, QueryContext{multi_table_schema(), reader()}),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  testing::HasSubstr(kQueryContainsSubqueryError)));
}

TEST_P(QueryEngineTest, PartitionableSimpleScanFilterSubqueryInExpr) {
  Query query{
      "SELECT string_col FROM test_table WHERE int64_col IN "
      "(SELECT child_key FROM child_table)"};
  EXPECT_THAT(query_engine().IsPartitionable(
                  query, QueryContext{multi_table_schema(), reader()}),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  testing::HasSubstr(kQueryContainsSubqueryError)));
}

TEST_P(QueryEngineTest, NonPartitionableSelectsFromTwoTable) {
  Query query{"SELECT t1.string_col FROM test_table AS t1, test_table2 AS t2"};
  EXPECT_THAT(query_engine().IsPartitionable(
                  query, QueryContext{multi_table_schema(), reader()}),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  testing::HasSubstr(kQueryNotASimpleTableScanError)));
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
              zetasql_base::testing::StatusIs(absl::StatusCode::kAlreadyExists));
}

TEST_P(QueryEngineTest, ConnotUpdatePrimaryKey) {
  MockRowWriter writer;
  EXPECT_THAT(query_engine().ExecuteSql(
                  Query{"UPDATE test_table SET int64_col=2 WHERE int64_col=2"},
                  QueryContext{schema(), reader(), &writer}),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
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

    return test_cases;
  }
};

class ParameterSensitiveHintTests
    : public QueryEngineTestBase,
      public ::testing::WithParamInterface<ParameterSensitiveHintInfo> {
  void SetUp() override {
    const ParameterSensitiveHintInfo& test_params = GetParam();
    if (test_params.dialect ==
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
    EXPECT_THAT(query_engine().ExecuteSql(Query{query},
                                          QueryContext{schema(), reader()}),
                zetasql_base::testing::StatusIs(
                    absl::StatusCode::kInvalidArgument,
                    testing::HasSubstr(
                        "Invalid hint value for: parameter_sensitive hint")));
  }
}

INSTANTIATE_TEST_SUITE_P(
    RunParameterSensitiveHintTests, ParameterSensitiveHintTests,
    testing::ValuesIn(ParameterSensitiveHintInfo::TestCases()));

TEST_P(QueryEngineTest, ExecuteSqlInsertReturning) {
  std::string returning = "THEN RETURN";
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
  std::string returning = "THEN RETURN";
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
  std::string returning = "THEN RETURN";
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

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
