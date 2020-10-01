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
#include "absl/status/status.h"
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
using zetasql_base::testing::StatusIs;

class QueryApiTest : public test::ServerTest {
 protected:
  void SetUp() override {
    ZETASQL_ASSERT_OK(CreateTestInstance());
    ZETASQL_ASSERT_OK(CreateTestDatabase());
    ZETASQL_ASSERT_OK_AND_ASSIGN(test_session_uri_, CreateTestSession());
    ZETASQL_ASSERT_OK(PopulateTestDatabase());
  }

  absl::Status PopulateTestDatabase() {
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
                                stats { row_count_exact: 1 }
                              }
                              status { code: 0 }
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
                              result_sets { stats { row_count_exact: 1 } }
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
    EXPECT_THAT(ExecuteSql(request, &response),
                StatusIs(absl::StatusCode::kUnimplemented));
  }

  // PLAN mode rejected in streaming case.
  {
    std::vector<spanner_api::PartialResultSet> response;
    EXPECT_THAT(ExecuteStreamingSql(request, &response),
                StatusIs(absl::StatusCode::kUnimplemented));
  }
}

}  // namespace

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
