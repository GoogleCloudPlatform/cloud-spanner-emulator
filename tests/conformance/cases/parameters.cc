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

#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using zetasql_base::testing::IsOk;
using zetasql_base::testing::IsOkAndHolds;
using zetasql_base::testing::StatusIs;

class ParamsApiTest : public test::DatabaseTest {
 protected:
  // Creates a new session for tests using raw grpc client.
  zetasql_base::StatusOr<std::string> CreateTestSession() {
    grpc::ClientContext context;
    spanner_api::CreateSessionRequest request;
    spanner_api::Session response;
    request.set_database(database()->FullName());
    ZETASQL_RETURN_IF_ERROR(raw_client()->CreateSession(&context, request, &response));
    return response.name();
  }

  void SetUp() override {
    test::DatabaseTest::SetUp();
    ZETASQL_ASSERT_OK_AND_ASSIGN(test_session_uri_, CreateTestSession());
  }

  absl::Status SetUpDatabase() override {
    return SetSchema({
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
              DateArray ARRAY<DATE>) PRIMARY KEY(Key))"});
  }

 protected:
  zetasql_base::StatusOr<spanner_api::ResultSet> Execute(
      std::string sql, google::protobuf::Struct params,
      google::protobuf::Map<std::string, google::spanner::v1::Type> param_types) {
    // Build the request that will be executed.
    spanner_api::ExecuteSqlRequest request;
    request.mutable_transaction()
        ->mutable_single_use()
        ->mutable_read_only()
        ->set_strong(true);
    *request.mutable_sql() = sql;
    request.set_session(test_session_uri_);

    (*request.mutable_params()) = params;
    (*request.mutable_param_types()) = param_types;

    // Execute the query.
    spanner_api::ResultSet response;
    grpc::ClientContext context;
    ZETASQL_RETURN_IF_ERROR(raw_client()->ExecuteSql(&context, request, &response));

    // Clear the transaction if any.
    response.mutable_metadata()->clear_transaction();

    return response;
  }

  std::string test_session_uri_;
};

TEST_F(ParamsApiTest, Params) {
  // The majority of the test cases set the parameter to a certain value and
  // expect the returned row to contain said value. This lambda just captures
  // that pattern.
  auto expect_selected = [this](Value v) {
    EXPECT_THAT(QueryWithParams("SELECT @param",
                                {{"param", v}, {"unused_param", Value(6)}}),
                IsOkAndHoldsRow({v}));
  };

  expect_selected(Value(6));
  expect_selected(Value("str"));
  expect_selected(Value(""));
  expect_selected(Value(Bytes("bytes")));
  expect_selected(
      Value(cloud::spanner::MakeNumeric("-2353250901550135.12453024").value()));
  expect_selected(
      Value(MakeTimestamp(absl::ToChronoTime(absl::FromUnixNanos(1)))));
  expect_selected(Value(MakeTimestamp(absl::ToChronoTime(
      absl::FromCivil(absl::CivilDay(1970, 1, 11), absl::FixedTimeZone(0))))));
  expect_selected(Value(std::vector<bool>{true, false}));

  EXPECT_THAT(
      QueryWithParams("SELECT @param * @param",
                      {{"param", Value(-2.0)}, {"unused_param", Value(6)}}),
      IsOkAndHoldsRow({4.0}));

  EXPECT_THAT(
      QueryWithParams("SELECT @param * @param",
                      {{"param", Value(-0.0)}, {"unused_param", Value(6)}}),
      IsOkAndHoldsRow({0.0}));

  EXPECT_THAT(
      QueryWithParams("SELECT @param * @param",
                      {{"param", Value(2.0)}, {"unused_param", Value(6)}}),
      IsOkAndHoldsRow({4.0}));

  EXPECT_THAT(QueryWithParams("SELECT @`p\\`ram`", {{"p`ram", Value(6)}}),
              IsOkAndHoldsRow({6}));

  EXPECT_THAT(
      QueryWithParams("SELECT @param", {{std::string(130, 'x'), Value(6)}}),
      StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(ParamsApiTest, NamedParameterInSqlNotSuppliedParametersInRequest) {
  // The SQL query uses a named parameter, but none are supplied in the actual
  // request.
  google::protobuf::Struct params;
  google::protobuf::Map<std::string, google::spanner::v1::Type> param_types;
  auto query = R"(SELECT @any)";
  EXPECT_THAT(Execute(query, params, param_types),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(ParamsApiTest, UndeclaredParameters) {
  // Parameter unused in query.
  google::protobuf::Struct params = PARSE_TEXT_PROTO(
      R"(fields {
           key: "p"
           value { string_value: "unused" }
         }
         fields {
           key: "ptimestamp"
           value { string_value: "1970-01-01T00:00:00.000001Z" }
         })");
  google::protobuf::Map<std::string, google::spanner::v1::Type> param_types = {};
  spanner_api::ResultSet result = PARSE_TEXT_PROTO(
      R"(metadata { row_type { fields { type { code: INT64 } } } }
         rows { values { string_value: "1" } })");
  EXPECT_THAT(Execute("SELECT 1", params, param_types),
              IsOkAndHolds(test::EqualsProto(result)));

  // Parameter used but value not provided.
  params = PARSE_TEXT_PROTO(
      R"(fields {
           key: "ptimestamp"
           value { string_value: "1970-01-01T00:00:00.000001Z" }
         })");
  param_types = {};
  EXPECT_THAT(Execute("SELECT @WhereAmI > 1", params, param_types),
              testing::Not(IsOk()));

  // Declared and undeclared string parameters in same query.
  params = PARSE_TEXT_PROTO(
      R"(fields {
           key: "p"
           value { string_value: "str" }
         }
         fields {
           key: "ptimestamp"
           value { string_value: "1970-01-01T00:00:00.000001Z" }
         }
         fields {
           key: "q"
           value { string_value: "str" }
         })");
  param_types = {};
  param_types["q"] = PARSE_TEXT_PROTO("code: STRING");
  result = PARSE_TEXT_PROTO(
      R"(metadata { row_type { fields { type { code: BOOL } } } }
         rows { values { bool_value: true } })");
  EXPECT_THAT(Execute("SELECT @p = @q", params, param_types),
              IsOkAndHolds(test::EqualsProto(result)));

  // Roundtrip values of all supported scalar types.
  params = PARSE_TEXT_PROTO(
      R"(fields {
           key: "PDOUBLE"
           value { number_value: 1.5 }
         }
         fields {
           key: "pStRiNg"
           value { string_value: "str" }
         }
         fields {
           key: "pbool"
           value { bool_value: true }
         }
         fields {
           key: "pbytes"
           value { string_value: "Ynl0ZXM=" }
         }
         fields {
           key: "pint64"
           value { string_value: "-1" }
         }
         fields {
           key: "pnumeric"
           value { string_value: "-99999900001412413135315315.3140124" }
         }
         fields {
           key: "ptimestamp"
           value { string_value: "1970-01-01T00:00:00.000001Z" }
         }
      )");
  param_types = {};
  result = PARSE_TEXT_PROTO(
      R"(metadata {
           row_type {
             fields { type { code: BOOL } }
             fields { type { code: INT64 } }
             fields { type { code: FLOAT64 } }
             fields { type { code: STRING } }
             fields { type { code: BYTES } }
             fields { type { code: BOOL } }
             fields { type { code: NUMERIC } }
           }
         }
         rows {
           values { bool_value: true }
           values { string_value: "-1" }
           values { number_value: 1.5 }
           values { string_value: "str" }
           values { string_value: "Ynl0ZXM=" }
           values { bool_value: true }
           values { string_value: "-99999900001412413135315315.3140124" }
         })");
  EXPECT_THAT(Execute(R"(SELECT
                    CAST(@pBool AS BOOL),
                    CAST(@pInt64 AS INT64),
                    CAST(@pDouble AS FLOAT64),
                    CAST(@pString AS STRING),
                    CAST(@pBytes AS BYTES),
                    CAST(@pBytes AS BYTES) = b"bytes",
                    CAST(@pNumeric AS NUMERIC))",
                      params, param_types),
              IsOkAndHolds(test::EqualsProto(result)));

  // Roundtrip NULL values of all supported scalar types.
  params = PARSE_TEXT_PROTO(
      R"(fields {
           key: "PDOUBLE"
           value { null_value: NULL_VALUE }
         }
         fields {
           key: "pStRiNg"
           value { null_value: NULL_VALUE }
         }
         fields {
           key: "pbool"
           value { null_value: NULL_VALUE }
         }
         fields {
           key: "pbytes"
           value { null_value: NULL_VALUE }
         }
         fields {
           key: "pint64"
           value { null_value: NULL_VALUE }
         }
         fields {
           key: "pnumeric"
           value { null_value: NULL_VALUE }
         }
         fields {
           key: "ptimestamp"
           value { string_value: "1970-01-01T00:00:00.000001Z" }
         }
      )");
  param_types = {};
  result = PARSE_TEXT_PROTO(R"(metadata {
                                 row_type {
                                   fields { type { code: BOOL } }
                                   fields { type { code: INT64 } }
                                   fields { type { code: FLOAT64 } }
                                   fields { type { code: STRING } }
                                   fields { type { code: BYTES } }
                                   fields { type { code: BOOL } }
                                   fields { type { code: NUMERIC } }
                                 }
                               }
                               rows {
                                 values { null_value: NULL_VALUE }
                                 values { null_value: NULL_VALUE }
                                 values { null_value: NULL_VALUE }
                                 values { null_value: NULL_VALUE }
                                 values { null_value: NULL_VALUE }
                                 values { null_value: NULL_VALUE }
                                 values { null_value: NULL_VALUE }
                               })");
  EXPECT_THAT(Execute(R"(SELECT
                    CAST(@pBool AS BOOL),
                    CAST(@pInt64 AS INT64),
                    CAST(@pDouble AS FLOAT64),
                    CAST(@pString AS STRING),
                    CAST(@pBytes AS BYTES),
                    CAST(@pBytes AS BYTES) = b"bytes",
                    CAST(@pNumeric AS NUMERIC))",
                      params, param_types),
              IsOkAndHolds(test::EqualsProto(result)));

  // Roundtrip values of all supported array types.
  params = PARSE_TEXT_PROTO(
      R"(fields {
           key: "pboolarray"
           value {
             list_value {
               values { bool_value: true }
               values { null_value: NULL_VALUE }
             }
           }
         }
         fields {
           key: "pbytesarray"
           value {
             list_value {
               values { null_value: NULL_VALUE }
               values { string_value: "Ynl0ZXM=" }
             }
           }
         }
         fields {
           key: "pdoublearray"
           value {
             list_value {
               values { null_value: NULL_VALUE }
               values { number_value: 1.5 }
             }
           }
         }
         fields {
           key: "pint64array"
           value {
             list_value {
               values { string_value: "-1" }
               values { null_value: NULL_VALUE }
             }
           }
         }
         fields {
           key: "pstringarray"
           value {
             list_value {
               values { null_value: NULL_VALUE }
               values { string_value: "str" }
             }
           }
         }
         fields {
           key: "pnumericarray"
           value {
             list_value {
               values { null_value: NULL_VALUE }
               values { string_value: "123.456" }
             }
           }
         }
         fields {
           key: "ptimestamp"
           value { string_value: "1970-01-01T00:00:00.000001Z" }
         }
      )");
  param_types = {};
  result = PARSE_TEXT_PROTO(R"(metadata {
                                 row_type {
                                   fields {
                                     type {
                                       code: ARRAY
                                       array_element_type { code: BOOL }
                                     }
                                   }
                                   fields {
                                     type {
                                       code: ARRAY
                                       array_element_type { code: INT64 }
                                     }
                                   }
                                   fields {
                                     type {
                                       code: ARRAY
                                       array_element_type { code: FLOAT64 }
                                     }
                                   }
                                   fields {
                                     type {
                                       code: ARRAY
                                       array_element_type { code: STRING }
                                     }
                                   }
                                   fields {
                                     type {
                                       code: ARRAY
                                       array_element_type { code: BYTES }
                                     }
                                   }
                                   fields {
                                     type {
                                       code: ARRAY
                                       array_element_type { code: NUMERIC }
                                     }
                                   }
                                 }
                               }
                               rows {
                                 values {
                                   list_value {
                                     values { bool_value: true }
                                     values { null_value: NULL_VALUE }
                                   }
                                 }
                                 values {
                                   list_value {
                                     values { string_value: "-1" }
                                     values { null_value: NULL_VALUE }
                                   }
                                 }
                                 values {
                                   list_value {
                                     values { null_value: NULL_VALUE }
                                     values { number_value: 1.5 }
                                   }
                                 }
                                 values {
                                   list_value {
                                     values { null_value: NULL_VALUE }
                                     values { string_value: "str" }
                                   }
                                 }
                                 values {
                                   list_value {
                                     values { null_value: NULL_VALUE }
                                     values { string_value: "Ynl0ZXM=" }
                                   }
                                 }
                                 values {
                                   list_value {
                                     values { null_value: NULL_VALUE }
                                     values { string_value: "123.456" }
                                   }
                                 }
                               })");
  EXPECT_THAT(Execute(
                  R"(SELECT
                CAST(@pBoolArray AS ARRAY<BOOL>),
                CAST(@pInt64Array AS ARRAY<INT64>),
                CAST(@pDoubleArray AS ARRAY<FLOAT64>),
                CAST(@pStringArray AS ARRAY<STRING>),
                CAST(@pBytesArray AS ARRAY<BYTES>),
                CAST(@pNumericArray AS ARRAY<NUMERIC>))",
                  params, param_types),
              IsOkAndHolds(test::EqualsProto(result)));

  // Roundtrip NULL values of all supported array types.
  params = PARSE_TEXT_PROTO(
      R"(fields {
           key: "pboolarray"
           value { null_value: NULL_VALUE }
         }
         fields {
           key: "pbytesarray"
           value { null_value: NULL_VALUE }
         }
         fields {
           key: "pdoublearray"
           value { null_value: NULL_VALUE }
         }
         fields {
           key: "pint64array"
           value { null_value: NULL_VALUE }
         }
         fields {
           key: "pstringarray"
           value { null_value: NULL_VALUE }
         }
         fields {
           key: "pnumericarray"
           value { null_value: NULL_VALUE }
         }
         fields {
           key: "ptimestamp"
           value { string_value: "1970-01-01T00:00:00.000001Z" }
         }
      )");
  param_types = {};
  result = PARSE_TEXT_PROTO(
      R"(metadata {
           row_type {
             fields {
               type {
                 code: ARRAY
                 array_element_type { code: BOOL }
               }
             }
             fields {
               type {
                 code: ARRAY
                 array_element_type { code: INT64 }
               }
             }
             fields {
               type {
                 code: ARRAY
                 array_element_type { code: FLOAT64 }
               }
             }
             fields {
               type {
                 code: ARRAY
                 array_element_type { code: STRING }
               }
             }
             fields {
               type {
                 code: ARRAY
                 array_element_type { code: BYTES }
               }
             }
             fields {
               type {
                 code: ARRAY
                 array_element_type { code: NUMERIC }
               }
             }
           }
         }
         rows {
           values { null_value: NULL_VALUE }
           values { null_value: NULL_VALUE }
           values { null_value: NULL_VALUE }
           values { null_value: NULL_VALUE }
           values { null_value: NULL_VALUE }
           values { null_value: NULL_VALUE }
         })");
  EXPECT_THAT(Execute(
                  R"(SELECT
                CAST(@pBoolArray AS ARRAY<BOOL>),
                CAST(@pInt64Array AS ARRAY<INT64>),
                CAST(@pDoubleArray AS ARRAY<FLOAT64>),
                CAST(@pStringArray AS ARRAY<STRING>),
                CAST(@pBytesArray AS ARRAY<BYTES>),
                CAST(@pNumericArray AS ARRAY<NUMERIC>))",
                  params, param_types),
              IsOkAndHolds(test::EqualsProto(result)));

  // Error message for unsupported undeclared parameter types.
  params = PARSE_TEXT_PROTO(
      R"(fields {
           key: "ptimestamp"
           value { string_value: "1970-01-01T00:00:00.000001Z" }
         })");
  param_types = {};
  EXPECT_THAT(
      Execute("SELECT CAST(@pTimestamp AS TIMESTAMP)", params, param_types),
      StatusIs(absl::StatusCode::kInvalidArgument));
  params = PARSE_TEXT_PROTO(
      R"(fields {
           key: "pdate"
           value { string_value: "1970-01-01" }
         }
         fields {
           key: "ptimestamp"
           value { string_value: "1970-01-01T00:00:00.000001Z" }
         })");
  param_types = {};
  EXPECT_THAT(Execute("SELECT CAST(@pDate AS DATE)", params, param_types),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(ParamsApiTest, UndeclaredParametersBadEncoding) {
  // The provided parameter is a STRING, but the query expects a DOUBLE.
  google::protobuf::Struct params = PARSE_TEXT_PROTO(
      R"(fields {
           key: "p"
           value { string_value: "10" }
         }
         fields {
           key: "pbar"
           value { string_value: "1970-01-01T00:00:00.000001Z" }
         }
         fields {
           key: "ptimestamp"
           value { string_value: "1970-01-01T00:00:00.000001Z" }
         }
      )");
  google::protobuf::Map<std::string, google::spanner::v1::Type> param_types = {};
  EXPECT_THAT(Execute("SELECT DoubleValue from test_table WHERE DoubleValue=@p",
                      params, param_types),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr("FLOAT64")));

  // The provided parameter is a DOUBLE, but the query expects a STRING.
  params = PARSE_TEXT_PROTO(
      R"(fields {
           key: "p"
           value { number_value: 10 }
         }
         fields {
           key: "pbar"
           value { string_value: "1970-01-01T00:00:00.000001Z" }
         }
         fields {
           key: "ptimestamp"
           value { string_value: "1970-01-01T00:00:00.000001Z" }
         }
      )");
  param_types = {};
  EXPECT_THAT(Execute("SELECT StrValue FROM test_table WHERE StrValue=@p",
                      params, param_types),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr("STRING")));

  // The provided parameter is a DOUBLE, but the query expects an INT64.
  params = PARSE_TEXT_PROTO(
      R"(fields {
           key: "p"
           value { number_value: 10 }
         }
         fields {
           key: "pbar"
           value { string_value: "1970-01-01T00:00:00.000001Z" }
         }
         fields {
           key: "ptimestamp"
           value { string_value: "1970-01-01T00:00:00.000001Z" }

         })");
  param_types = {};
  EXPECT_THAT(Execute("SELECT IntValue FROM test_table WHERE IntValue=@p",
                      params, param_types),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr("INT64")));

  // The provided parameter is a DOUBLE, but the query expects an INT64.
  params = PARSE_TEXT_PROTO(
      R"(fields {
           key: "p"
           value { number_value: 10 }
         }
         fields {
           key: "pbar"
           value { string_value: "1970-01-01T00:00:00.000001Z" }
         }
         fields {
           key: "ptimestamp"
           value { string_value: "1970-01-01T00:00:00.000001Z" }
         })");
  param_types = {};
  EXPECT_THAT(Execute("SELECT @p", params, param_types),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr("INT64")));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
