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

#include <string>

#include "google/spanner/v1/spanner.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/string_view.h"
#include "backend/datamodel/types.h"
#include "common/errors.h"
#include "frontend/converters/types.h"
#include "frontend/converters/values.h"
#include "tests/common/test_env.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {

namespace spanner_api = ::google::spanner::v1;

using testing::ElementsAre;
using test::EqualsProto;
using test::proto::Partially;

class QueryApiTest : public test::ServerTest {
 protected:
  void SetUp() override {
    ZETASQL_ASSERT_OK(CreateTestInstance());
    ZETASQL_ASSERT_OK(CreateTestDatabase());
    ZETASQL_ASSERT_OK_AND_ASSIGN(test_session_uri_, CreateTestSession());
    ZETASQL_ASSERT_OK(PopulateTestDatabase());
  }

  zetasql_base::Status PopulateTestDatabase() {
    spanner_api::CommitRequest commit_request = PARSE_TEXT_PROTO(R"(
      single_use_transaction { read_write {} }
      mutations {
        insert {
          table: "test_table"
          columns: "int64_col"
          columns: "string_col"
          values {
            values { string_value: "1" }
            values { string_value: "row_1" }
          }
          values {
            values { string_value: "2" }
            values { string_value: "row_2" }
          }
          values {
            values { string_value: "3" }
            values { string_value: "row_3" }
          }
        }
      }
    )");
    *commit_request.mutable_session() = test_session_uri_;

    spanner_api::CommitResponse commit_response;
    return Commit(commit_request, &commit_response);
  }

  std::string test_session_uri_;
};

TEST_F(QueryApiTest, ExecuteBatchDml) {
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"(
    options { read_write {} }
  )");
  begin_request.set_session(test_session_uri_);

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  spanner_api::ExecuteBatchDmlRequest request = PARSE_TEXT_PROTO(
      R"""(
        statements {
          sql: "insert into test_table(int64_col, string_col) "
               "values (10, 'row_10')"
        }
        statements {
          sql: "insert into test_table(int64_col, string_col) "
               "values (11, 'row_11')"
        }
      )""");
  request.set_session(test_session_uri_);
  request.mutable_transaction()->set_id(transaction_response.id());

  spanner_api::ExecuteBatchDmlResponse response;
  ZETASQL_ASSERT_OK(ExecuteBatchDml(request, &response));
  EXPECT_THAT(response, EqualsProto(
                            R"(
                              result_sets {
                                metadata { row_type {} }
                                stats { row_count_exact: 1 }
                              }
                              result_sets {
                                metadata { row_type {} }
                                stats { row_count_exact: 1 }
                              }
                            )"));
}

TEST_F(QueryApiTest, ExecuteBatchDmlFailsOnInvalidDmlStatement) {
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"(
    options { read_write {} }
  )");
  begin_request.set_session(test_session_uri_);

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  spanner_api::ExecuteBatchDmlRequest request = PARSE_TEXT_PROTO(
      R"""(
        statements {
          sql: "insert into test_table(int64_col, string_col) "
               "values (10, 'row_10')"
        }
        statements {
          sql: "insert into test_table(int64_t, string) "
               "values (11, 'row_11')"
        }
      )""");
  request.set_session(test_session_uri_);
  request.mutable_transaction()->set_id(transaction_response.id());

  spanner_api::ExecuteBatchDmlResponse response;
  ZETASQL_ASSERT_OK(ExecuteBatchDml(request, &response));
  // Ignoring the status.message field to avoid brittle tests.
  EXPECT_THAT(response, Partially(EqualsProto(
                            R"(
                              result_sets {
                                metadata { row_type {} }
                                stats { row_count_exact: 1 }
                              }
                              status { code: 3 }
                            )")));
}

TEST_F(QueryApiTest, ExecuteSql) {
  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(
      R"(
        transaction { single_use { read_only { strong: true } } }
        sql: "SELECT int64_col, string_col FROM test_table "
             "ORDER BY int64_col ASC, string_col DESC"
      )");
  request.set_session(test_session_uri_);

  spanner_api::ResultSet response;
  ZETASQL_ASSERT_OK(ExecuteSql(request, &response));
  EXPECT_THAT(response, EqualsProto(
                            R"(
                              metadata {
                                row_type {
                                  fields {
                                    name: "int64_col"
                                    type { code: INT64 }
                                  }
                                  fields {
                                    name: "string_col"
                                    type { code: STRING }
                                  }
                                }
                              }
                              rows {
                                values { string_value: "1" }
                                values { string_value: "row_1" }
                              }
                              rows {
                                values { string_value: "2" }
                                values { string_value: "row_2" }
                              }
                              rows {
                                values { string_value: "3" }
                                values { string_value: "row_3" }
                              }
                            )"));
}

TEST_F(QueryApiTest, ExecuteSqlWithParameters) {
  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(
      R"(
        transaction { single_use { read_only { strong: true } } }
        sql: "SELECT @param AS param FROM test_table"
        params {
          fields {
            key: "param"
            value { string_value: "value" }
          }
        }
        param_types {
          key: "param"
          value { code: STRING }
        }
      )");
  request.set_session(test_session_uri_);

  spanner_api::ResultSet response;
  ZETASQL_ASSERT_OK(ExecuteSql(request, &response));
  EXPECT_THAT(response, EqualsProto(
                            R"(
                              metadata {
                                row_type {
                                  fields {
                                    name: "param"
                                    type { code: STRING }
                                  }
                                }
                              }
                              rows { values { string_value: "value" } }
                              rows { values { string_value: "value" } }
                              rows { values { string_value: "value" } }
                            )"));
}

TEST_F(QueryApiTest, ExecuteStreamingSql) {
  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(
      R"(
        transaction { single_use { read_only { strong: true } } }
        sql: "SELECT int64_col, string_col FROM test_table "
             "ORDER BY int64_col ASC, string_col DESC"
      )");
  request.set_session(test_session_uri_);

  std::vector<spanner_api::PartialResultSet> response;
  ZETASQL_EXPECT_OK(ExecuteStreamingSql(request, &response));
  EXPECT_THAT(response, ElementsAre(EqualsProto(
                            R"(metadata {
                                 row_type {
                                   fields {
                                     name: "int64_col"
                                     type { code: INT64 }
                                   }
                                   fields {
                                     name: "string_col"
                                     type { code: STRING }
                                   }
                                 }
                               }
                               values { string_value: "1" }
                               values { string_value: "row_1" }
                               values { string_value: "2" }
                               values { string_value: "row_2" }
                               values { string_value: "3" }
                               values { string_value: "row_3" }
                               chunked_value: false
                            )")));
}

TEST_F(QueryApiTest, ExecuteStreamingSqlWithParameters) {
  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(
      R"(
        transaction { single_use { read_only { strong: true } } }
        sql: "SELECT @param AS param FROM test_table"
        params {
          fields {
            key: "param"
            value { string_value: "value" }
          }
        }
        param_types {
          key: "param"
          value { code: STRING }
        }
      )");
  request.set_session(test_session_uri_);

  std::vector<spanner_api::PartialResultSet> response;
  ZETASQL_EXPECT_OK(ExecuteStreamingSql(request, &response));
  EXPECT_THAT(response, ElementsAre(EqualsProto(
                            R"(
                              metadata {
                                row_type {
                                  fields {
                                    name: "param"
                                    type { code: STRING }
                                  }
                                }
                              }
                              values { string_value: "value" }
                              values { string_value: "value" }
                              values { string_value: "value" }
                            )")));
}

TEST_F(QueryApiTest, RejectsPlanMode) {
  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(
      R"(
        transaction { single_use { read_only { strong: true } } }
        query_mode: PLAN
        sql: "SELECT * FROM test_table"
      )");
  request.set_session(test_session_uri_);

  // PLAN mode rejected in non-streaming case.
  {
    spanner_api::ResultSet response;
    EXPECT_EQ(ExecuteSql(request, &response),
              error::EmulatorDoesNotSupportQueryPlans());
  }

  // PLAN mode rejected in streaming case.
  {
    std::vector<spanner_api::PartialResultSet> response;
    EXPECT_EQ(ExecuteStreamingSql(request, &response),
              error::EmulatorDoesNotSupportQueryPlans());
  }
}

class UntypedParamsApiTest : public test::ServerTest {
 protected:
  void SetUp() override {
    ZETASQL_ASSERT_OK(CreateTestInstance());
    ZETASQL_ASSERT_OK(CreateDatabase());
    ZETASQL_ASSERT_OK_AND_ASSIGN(test_session_uri_, CreateTestSession());
  }

  zetasql_base::Status CreateDatabase() {
    // Create a database that belongs to the instance created above and create
    // a test schema inside the newly created database.
    test::database_api::CreateDatabaseRequest request;
    request.add_extra_statements(
        R"(CREATE TABLE test_table(
              Key INT64,
              BoolValue BOOL,
              IntValue INT64,
              DoubleValue FLOAT64,
              StrValue STRING(MAX),
              ByteValue BYTES(MAX),
              TimestampValue TIMESTAMP,
              DateValue DATE,
              BoolArray ARRAY<BOOL>,
              IntArray ARRAY<INT64>,
              DoubleArray ARRAY<FLOAT64>,
              StrArray ARRAY<STRING(MAX)>,
              ByteArray ARRAY<BYTES(MAX)>,
              TimestampArray ARRAY<TIMESTAMP>,
              DateArray ARRAY<DATE>) PRIMARY KEY(Key))");

    request.set_parent(test_instance_uri_);
    request.set_create_statement(
        absl::StrCat("CREATE DATABASE `", test_database_name_, "`"));
    grpc::ClientContext context;
    longrunning::Operation operation;
    ZETASQL_RETURN_IF_ERROR(test_env()->database_admin_client()->CreateDatabase(
        &context, request, &operation));
    return WaitForOperation(operation.name(), &operation);
  }

  struct Param {
    std::string name;
    zetasql::Value value;
    bool declare_type = false;
  };
  zetasql_base::StatusOr<std::string> Execute(std::string sql,
                                      std::vector<Param> params) {
    // Build the request that will be executed.
    spanner_api::ExecuteSqlRequest request;
    request.mutable_transaction()
        ->mutable_single_use()
        ->mutable_read_only()
        ->set_strong(true);
    *request.mutable_sql() = sql;
    request.set_session(test_session_uri_);

    // Add parameters to the request.
    for (const auto& param : params) {
      ZETASQL_ASSIGN_OR_RETURN(auto proto, ValueToProto(param.value));
      (*request.mutable_params()->mutable_fields())[param.name] = proto;

      if (param.declare_type) {
        google::spanner::v1::Type type;
        ZETASQL_RETURN_IF_ERROR(frontend::TypeToProto(param.value.type(), &type));
        (*request.mutable_param_types())[param.name] = type;
      }
    }

    // Execute the query.
    spanner_api::ResultSet response;
    ZETASQL_RETURN_IF_ERROR(ExecuteSql(request, &response));

    std::string actual;

    // Parse response proto types from metadata and build the actual type string
    // to compare against the expectation.
    zetasql::TypeFactory type_factory;
    std::vector<const zetasql::Type*> row_types;
    for (const auto& field : response.metadata().row_type().fields()) {
      if (!actual.empty()) absl::StrAppend(&actual, ",");

      const zetasql::Type* row_type;
      ZETASQL_RETURN_IF_ERROR(
          frontend::TypeFromProto(field.type(), &type_factory, &row_type));

      absl::StrAppend(&actual, backend::ToString(row_type));
      row_types.push_back(row_type);
    }

    absl::StrAppend(&actual, ":[");
    // Build the actual rows string to compare against the expectation.
    std::vector<std::string> actual_rows;
    for (const auto& row : response.rows()) {
      std::vector<std::string> actual_row;
      for (int i = 0; i < row.values_size(); ++i) {
        const auto& value = row.values()[i];
        ZETASQL_ASSIGN_OR_RETURN(auto v, frontend::ValueFromProto(value, row_types[i]));
        actual_row.push_back(v.DebugString());
      }
      actual_rows.push_back(
          absl::StrCat("[", absl::StrJoin(actual_row, ","), "]"));
    }
    absl::StrAppend(&actual, absl::StrJoin(actual_rows, ","));
    absl::StrAppend(&actual, "]");

    return actual;
  }

  std::string test_session_uri_;
};

TEST_F(UntypedParamsApiTest, UndeclaredParameters) {
  namespace values = zetasql::values;
  namespace types = zetasql::types;

  // Parameter unused in query.
  EXPECT_THAT(Execute("SELECT 1", {{"p", values::String("unused")}}),
              zetasql_base::testing::IsOkAndHolds("INT64:[[1]]"));

  // Parameter used but value not provided.
  EXPECT_THAT(Execute("SELECT @WhereAmI > 1", {}),
              zetasql_base::testing::StatusIs(zetasql_base::StatusCode::kInvalidArgument));

  // Declared and undeclared string parameters in same query.
  EXPECT_THAT(Execute("SELECT @p = @q",
                      {
                          {"p", values::String("str"), /*declare_type=*/false},
                          {"q", values::String("str"), /*declare_type=*/true},
                      }),
              zetasql_base::testing::IsOkAndHolds("BOOL:[[true]]"));

  // Roundtrip values of all supported scalar types.
  EXPECT_THAT(
      Execute(R"(SELECT
                    CAST(@pBool AS BOOL),
                    CAST(@pInt64 AS INT64),
                    CAST(@pDouble AS FLOAT64),
                    CAST(@pString AS STRING),
                    CAST(@pBytes AS BYTES),
                    CAST(@pBytes AS BYTES) = b"bytes")",
              {
                  {"pbool", values::Bool(true)},
                  {"pint64", values::Int64(-1)},
                  {"PDOUBLE", values::Double(1.5)},
                  {"pStRiNg", values::String("str")},
                  {"pbytes", values::Bytes("bytes")},
              }),
      zetasql_base::testing::IsOkAndHolds("BOOL,INT64,FLOAT64,STRING,BYTES,BOOL:"
                                    R"([[true,-1,1.5,"str",b"bytes",true]])"));

  // Roundtrip NULL values of all supported scalar types.
  EXPECT_THAT(
      Execute(R"(SELECT
                    CAST(@pBool AS BOOL),
                    CAST(@pInt64 AS INT64),
                    CAST(@pDouble AS FLOAT64),
                    CAST(@pString AS STRING),
                    CAST(@pBytes AS BYTES),
                    CAST(@pBytes AS BYTES) = b"bytes")",
              {
                  {"pbool", values::NullBool()},
                  {"pint64", values::NullInt64()},
                  {"PDOUBLE", values::NullDouble()},
                  {"pStRiNg", values::NullString()},
                  {"pbytes", values::NullBytes()},
              }),
      zetasql_base::testing::IsOkAndHolds("BOOL,INT64,FLOAT64,STRING,BYTES,BOOL:"
                                    R"([[NULL,NULL,NULL,NULL,NULL,NULL]])"));

  // Roundtrip values of all supported array types.
  EXPECT_THAT(
      Execute(
          R"(SELECT
                CAST(@pBoolArray AS ARRAY<BOOL>),
                CAST(@pInt64Array AS ARRAY<INT64>),
                CAST(@pDoubleArray AS ARRAY<FLOAT64>),
                CAST(@pStringArray AS ARRAY<STRING>),
                CAST(@pBytesArray AS ARRAY<BYTES>))",
          {
              {"pboolarray",
               values::Array(types::BoolArrayType(),
                             {values::Bool(true), values::NullBool()})},
              {"pint64array",
               values::Array(types::Int64ArrayType(),
                             {values::Int64(-1), values::NullInt64()})},
              {"pdoublearray",
               values::Array(types::DoubleArrayType(),
                             {values::NullDouble(), values::Double(1.5)})},
              {"pstringarray",
               values::Array(types::StringArrayType(),
                             {values::NullString(), values::String("str")})},
              {"pbytesarray",
               values::Array(types::BytesArrayType(),
                             {values::NullBytes(), values::Bytes("bytes")})},
          }),
      zetasql_base::testing::IsOkAndHolds(
          "ARRAY<BOOL>,ARRAY<INT64>,ARRAY<FLOAT64>,ARRAY<STRING>,ARRAY<BYTES>:"
          "[[[true, NULL],[-1, NULL],[NULL, 1.5],[NULL, \"str\"],[NULL, "
          "b\"bytes\"]]]"));

  // Roundtrip NULL values of all supported array types.
  EXPECT_THAT(
      Execute(
          R"(SELECT
                CAST(@pBoolArray AS ARRAY<BOOL>),
                CAST(@pInt64Array AS ARRAY<INT64>),
                CAST(@pDoubleArray AS ARRAY<FLOAT64>),
                CAST(@pStringArray AS ARRAY<STRING>),
                CAST(@pBytesArray AS ARRAY<BYTES>))",
          {
              {"pboolarray", values::Null(types::BoolArrayType())},
              {"pint64array", values::Null(types::Int64ArrayType())},
              {"pdoublearray", values::Null(types::DoubleArrayType())},
              {"pstringarray", values::Null(types::StringArrayType())},
              {"pbytesarray", values::Null(types::BytesArrayType())},
          }),
      zetasql_base::testing::IsOkAndHolds(
          "ARRAY<BOOL>,ARRAY<INT64>,ARRAY<FLOAT64>,ARRAY<STRING>,ARRAY<BYTES>:"
          R"([[NULL,NULL,NULL,NULL,NULL]])"));

  // Error message for unsupported undeclared parameter types.
  EXPECT_THAT(Execute("SELECT CAST(@pTimestamp AS TIMESTAMP)",
                      {
                          {"ptimestamp", values::TimestampFromUnixMicros(1)},
                      }),
              zetasql_base::testing::StatusIs(zetasql_base::StatusCode::kInvalidArgument));
  EXPECT_THAT(Execute("SELECT CAST(@pDate AS DATE)",
                      {
                          {"pdate", values::Date(0)},
                      }),
              zetasql_base::testing::StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

TEST_F(UntypedParamsApiTest, UndeclaredParametersBadEncoding) {
  namespace values = zetasql::values;

  // The provided parameter is a STRING, but the query expects a DOUBLE.
  EXPECT_THAT(Execute("SELECT DoubleValue from test_table WHERE DoubleValue=@p",
                      {{"p", values::String("10")}}),
              zetasql_base::testing::StatusIs(zetasql_base::StatusCode::kInvalidArgument,
                                        testing::HasSubstr("FLOAT64")));

  // The provided parameter is a DOUBLE, but the query expects a STRING.
  EXPECT_THAT(Execute("SELECT StrValue FROM test_table WHERE StrValue=@p",
                      {{"p", values::Double(10)}}),
              zetasql_base::testing::StatusIs(zetasql_base::StatusCode::kInvalidArgument,
                                        testing::HasSubstr("STRING")));

  // The provided parameter is a DOUBLE, but the query expects an INT64.
  EXPECT_THAT(Execute("SELECT IntValue FROM test_table WHERE IntValue=@p",
                      {{"p", values::Double(10)}}),
              zetasql_base::testing::StatusIs(zetasql_base::StatusCode::kInvalidArgument,
                                        testing::HasSubstr("INT64")));

  // The provided parameter is a DOUBLE, but the query expects an INT64.
  EXPECT_THAT(Execute("SELECT @p", {{"p", values::Double(10)}}),
              zetasql_base::testing::StatusIs(zetasql_base::StatusCode::kInvalidArgument,
                                        testing::HasSubstr("INT64")));
}

}  // namespace

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
