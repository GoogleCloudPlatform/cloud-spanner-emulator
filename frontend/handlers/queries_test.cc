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
#include <vector>

#include "google/spanner/admin/database/v1/spanner_database_admin.pb.h"
#include "google/spanner/v1/commit_response.pb.h"
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
#include "tests/common/proto_matchers.h"
#include "tests/common/test_env.h"
#include "grpcpp/client_context.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {

namespace spanner_api = ::google::spanner::v1;
namespace database_api = ::google::spanner::admin::database::v1;
namespace operations_api = ::google::longrunning;

using testing::ElementsAre;
using test::EqualsProto;
using test::proto::Partially;
using zetasql_base::testing::StatusIs;

enum class SessionType {
  kRegularSession,
  kMultiplexedSession,
};

class QueryApiTest : public test::ServerTest,
                     public testing::WithParamInterface<SessionType> {
 protected:
  void SetUp() override {
    ZETASQL_ASSERT_OK(CreateTestInstance());
    ZETASQL_ASSERT_OK(CreateTestDatabase());
    ZETASQL_ASSERT_OK_AND_ASSIGN(test_session_uri_,
                         CreateTestSession(/*multiplexed=*/false));
    ZETASQL_ASSERT_OK_AND_ASSIGN(test_multiplexed_session_uri_,
                         CreateTestSession(/*multiplexed=*/true));
    ZETASQL_ASSERT_OK(PopulateTestTable());
  }

  std::string GetSessionUri(bool multiplexed) {
    return multiplexed ? test_multiplexed_session_uri_ : test_session_uri_;
  }

  absl::Status PopulateTestTable() {
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

  absl::Status AddProtoTables() {
    grpc::ClientContext context;
    database_api::UpdateDatabaseDdlRequest request;
    request.set_database(test_database_uri_);
    request.add_statements(R"sql(
      CREATE PROTO BUNDLE (
        customer.app.User,
      )
    )sql");
    request.add_statements(R"sql(
      CREATE TABLE proto_table(
        int64_col INT64 NOT NULL,
        proto_col customer.app.User,
      ) PRIMARY KEY(int64_col)
    )sql");
    request.set_proto_descriptors(GenerateProtoDescriptorBytesAsString());
    operations_api::Operation operation;
    ZETASQL_RETURN_IF_ERROR(test_env()->database_admin_client()->UpdateDatabaseDdl(
        &context, request, &operation));
    ZETASQL_RETURN_IF_ERROR(WaitForOperation(operation.name(), &operation));
    google::rpc::Status status = operation.error();
    return absl::Status(static_cast<absl::StatusCode>(status.code()),
                        status.message());
  }

  absl::Status PopulateProtoTable() {
    // `int_field: 314` is encoded as CLoC
    // `int_field: 271` is encoded as CI8C
    spanner_api::CommitRequest commit_request = PARSE_TEXT_PROTO(R"pb(
      single_use_transaction { read_write {} }
      mutations {
        insert {
          table: "proto_table"
          columns: "int64_col"
          columns: "proto_col"
          values {
            values { string_value: "1" }
            values { string_value: "CLoC" }
          }
          values {
            values { string_value: "2" }
            values { string_value: "CI8C" }
          }
        }
      }
    )pb");
    *commit_request.mutable_session() = test_session_uri_;

    spanner_api::CommitResponse commit_response;
    return Commit(commit_request, &commit_response);
  }

  SessionType GetSessionType() { return GetParam(); }

  std::string test_session_uri_;
  std::string test_multiplexed_session_uri_;

 private:
  std::string GenerateProtoDescriptorBytesAsString() {
    const google::protobuf::FileDescriptorProto file_descriptor = PARSE_TEXT_PROTO(R"pb(
      syntax: "proto2"
      name: "0"
      package: "customer.app"
      message_type {
        name: "User"
        field {
          name: "int_field"
          type: TYPE_INT64
          number: 1
          label: LABEL_OPTIONAL
        }
      }
      enum_type {
        name: "State"
        value { name: "UNSPECIFIED" number: 0 }
      }
    )pb");
    google::protobuf::FileDescriptorSet file_descriptor_set;
    *file_descriptor_set.add_file() = file_descriptor;
    return file_descriptor_set.SerializeAsString();
  }
};

INSTANTIATE_TEST_SUITE_P(SessionTypes, QueryApiTest,
                         testing::Values(SessionType::kRegularSession,
                                         SessionType::kMultiplexedSession));

TEST_P(QueryApiTest, ExecuteBatchDml) {
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"(
    options { read_write {} }
  )");
  begin_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

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
  request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));
  request.mutable_transaction()->set_id(transaction_response.id());

  spanner_api::ExecuteBatchDmlResponse response;
  ZETASQL_ASSERT_OK(ExecuteBatchDml(request, &response));
  EXPECT_THAT(response, Partially(EqualsProto(
                            R"pb(
                              result_sets {
                                metadata { row_type {} }
                                stats { row_count_exact: 1 }
                              }
                              result_sets { stats { row_count_exact: 1 } }
                              status { code: 0 }
                            )pb")));
  if (GetSessionType() == SessionType::kMultiplexedSession) {
    ASSERT_TRUE(response.has_precommit_token());
  }

  spanner_api::CommitRequest commit_request;
  commit_request.set_transaction_id(transaction_response.id());
  commit_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));
  if (GetSessionType() == SessionType::kMultiplexedSession) {
    *commit_request.mutable_precommit_token() = response.precommit_token();
  }

  spanner_api::CommitResponse commit_response1;
  ZETASQL_EXPECT_OK(Commit(commit_request, &commit_response1));
}

TEST_P(QueryApiTest, ExecuteBatchDmlWithProtos) {
  ZETASQL_ASSERT_OK(AddProtoTables());

  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"pb(
    options { read_write {} }
  )pb");
  begin_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  spanner_api::ExecuteBatchDmlRequest request = PARSE_TEXT_PROTO(
      R"""(
        statements {
          sql: "insert into proto_table(int64_col, proto_col) "
               "values (10, 'int_field: 314')"
        }
        statements {
          sql: "insert into proto_table(int64_col, proto_col) "
               "values (11, 'int_field: 271')"
        }
      )""");
  request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));
  request.mutable_transaction()->set_id(transaction_response.id());

  spanner_api::ExecuteBatchDmlResponse response;
  ZETASQL_ASSERT_OK(ExecuteBatchDml(request, &response));
  if (GetSessionType() == SessionType::kMultiplexedSession) {
    ASSERT_TRUE(response.has_precommit_token());
  }
  ASSERT_THAT(response, Partially(EqualsProto(
                            R"pb(
                              result_sets {
                                metadata { row_type {} }
                                stats { row_count_exact: 1 }
                              }
                              result_sets { stats { row_count_exact: 1 } }
                              status { code: 0 }
                            )pb")));
}

TEST_P(QueryApiTest, ExecuteBatchDmlFailsOnInvalidDmlStatement) {
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"(
    options { read_write {} }
  )");
  begin_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

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
  request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));
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

TEST_P(QueryApiTest, ExecuteSql) {
  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(
      R"(
        transaction { single_use { read_only { strong: true } } }
        sql: "SELECT int64_col, string_col FROM test_table "
             "ORDER BY int64_col ASC, string_col DESC"
      )");
  request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::ResultSet response;
  ZETASQL_ASSERT_OK(ExecuteSql(request, &response));
  if (GetSessionType() == SessionType::kMultiplexedSession) {
    // No precommit token for single use transactions.
    ASSERT_FALSE(response.has_precommit_token());
  }
  EXPECT_THAT(response, Partially(EqualsProto(
                            R"pb(
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
                            )pb")));
}

TEST_P(QueryApiTest, ExecuteSqlWithParameters) {
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
  request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::ResultSet response;
  ZETASQL_ASSERT_OK(ExecuteSql(request, &response));
  if (GetSessionType() == SessionType::kMultiplexedSession) {
    // no precommit token for single use transactions.
    ASSERT_FALSE(response.has_precommit_token());
  }
  EXPECT_THAT(response, Partially(EqualsProto(
                            R"pb(
                              metadata {
                                row_type {
                                  fields {
                                    name: "param"
                                    type { code: STRING }
                                  }
                                }
                                undeclared_parameters {
                                  fields {
                                    name: "param"
                                    type { code: STRING }
                                  }
                                }
                              }
                              rows { values { string_value: "value" } }
                              rows { values { string_value: "value" } }
                              rows { values { string_value: "value" } }
                            )pb")));
}

TEST_P(QueryApiTest, ExecuteSqlWithProtoParameters) {
  ZETASQL_ASSERT_OK(AddProtoTables());

  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(
      R"pb(
        transaction { single_use { read_only { strong: true } } }
        sql: "SELECT @param.int_field AS intval FROM test_table"
        params {
          fields {
            key: "param"
            value { string_value: "CI8C" }
          }
        }
        param_types {
          key: "param"
          value { code: PROTO proto_type_fqn: "customer.app.User" }
        }
      )pb");
  request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::ResultSet response;
  ZETASQL_ASSERT_OK(ExecuteSql(request, &response));
  if (GetSessionType() == SessionType::kMultiplexedSession) {
    // no precommit token for single use transactions.
    ASSERT_FALSE(response.has_precommit_token());
  }
  EXPECT_THAT(
      response,
      EqualsProto(
          R"pb(
            metadata {
              row_type {
                fields {
                  name: "intval"
                  type { code: INT64 }
                }
              }
              undeclared_parameters {
                fields {
                  name: "param"
                  type { code: PROTO proto_type_fqn: "customer.app.User" }
                }
              }
            }
            rows { values { string_value: "271" } }
            rows { values { string_value: "271" } }
            rows { values { string_value: "271" } }
          )pb"));
}

TEST_P(QueryApiTest, ExecuteSqlWithDmlAndParameters) {
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"pb(
    options { read_write {} }
  )pb");
  begin_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(
      R"""(
        sql: "INSERT INTO test_table (int64_col, string_col) "
             "VALUES (@p1, @p2)"
      )""");
  request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));
  request.set_query_mode(spanner_api::ExecuteSqlRequest::PLAN);
  request.mutable_transaction()->set_id(transaction_response.id());

  spanner_api::ResultSet response;
  ZETASQL_ASSERT_OK(ExecuteSql(request, &response));
  if (GetSessionType() == SessionType::kMultiplexedSession) {
    ASSERT_TRUE(response.has_precommit_token());
  }
  EXPECT_THAT(
      response,
      Partially(EqualsProto(
          R"pb(
            metadata {
              row_type {}
              undeclared_parameters {
                fields {
                  name: "p1"
                  type { code: INT64 }
                }
                fields {
                  name: "p2"
                  type { code: STRING }
                }
              }
            }
            stats {
              query_plan { plan_nodes { display_name: "No query plan" } }
              row_count_exact: 0
            }
          )pb")));
}

TEST_P(QueryApiTest, ExecuteSqlWithDmlReturningAndParameters) {
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"pb(
    options { read_write {} }
  )pb");
  begin_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(
      R"""(
        sql: "INSERT INTO test_table (int64_col, string_col) "
             "VALUES (@p1, @p2) THEN RETURN int64_col, string_col"
      )""");
  request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));
  request.set_query_mode(spanner_api::ExecuteSqlRequest::PLAN);
  request.mutable_transaction()->set_id(transaction_response.id());

  spanner_api::ResultSet response;
  ZETASQL_ASSERT_OK(ExecuteSql(request, &response));
  if (GetSessionType() == SessionType::kMultiplexedSession) {
    ASSERT_TRUE(response.has_precommit_token());
  }
  EXPECT_THAT(
      response,
      Partially(EqualsProto(
          R"pb(
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
              undeclared_parameters {
                fields {
                  name: "p1"
                  type { code: INT64 }
                }
                fields {
                  name: "p2"
                  type { code: STRING }
                }
              }
            }
            stats {
              query_plan { plan_nodes { display_name: "No query plan" } }
              row_count_exact: 0
            }
          )pb")));
}

TEST_P(QueryApiTest, ExecuteSqlWithDmlReturningReturnsStats) {
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"pb(
    options { read_write {} }
  )pb");
  begin_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(
      R"""(
        sql: "INSERT INTO test_table (int64_col, string_col) "
             "VALUES (10, 'row_10') THEN RETURN int64_col, string_col"
      )""");
  request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));
  request.mutable_transaction()->set_id(transaction_response.id());

  spanner_api::ResultSet response;
  ZETASQL_ASSERT_OK(ExecuteSql(request, &response));
  if (GetSessionType() == SessionType::kMultiplexedSession) {
    ASSERT_TRUE(response.has_precommit_token());
  }
  EXPECT_THAT(response, Partially(EqualsProto(
                            R"pb(
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
                                values { string_value: "10" }
                                values { string_value: "row_10" }
                              }
                              stats { row_count_exact: 1 }
                            )pb")));
}

TEST_P(QueryApiTest, ExecuteStreamingSqlWithDmlReturningReturnsStats) {
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"pb(
    options { read_write {} }
  )pb");
  begin_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(
      R"""(
        sql: "INSERT INTO test_table (int64_col, string_col) "
             "VALUES (10, 'row_10') THEN RETURN int64_col, string_col"
      )""");
  request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));
  request.mutable_transaction()->set_id(transaction_response.id());

  std::vector<spanner_api::PartialResultSet> response;
  ZETASQL_EXPECT_OK(ExecuteStreamingSql(request, &response));
  if (GetSessionType() == SessionType::kMultiplexedSession) {
    ASSERT_TRUE(response[0].has_precommit_token());
  }
  EXPECT_THAT(response, ElementsAre(Partially(EqualsProto(
                            R"pb(metadata {
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
                                 values { string_value: "10" }
                                 values { string_value: "row_10" }
                                 chunked_value: false
                                 stats { row_count_exact: 1 }
                            )pb"))));
}

TEST_P(QueryApiTest,
       ExecuteStreamingSqlWithDmlReturningInPlanModeReturnsEmptyStats) {
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"pb(
    options { read_write {} }
  )pb");
  begin_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(
      R"""(
        sql: "INSERT INTO test_table (int64_col, string_col) "
             "VALUES (10, 'row_10') THEN RETURN int64_col, string_col"
      )""");
  request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));
  request.set_query_mode(spanner_api::ExecuteSqlRequest::PLAN);
  request.mutable_transaction()->set_id(transaction_response.id());

  std::vector<spanner_api::PartialResultSet> response;
  ZETASQL_EXPECT_OK(ExecuteStreamingSql(request, &response));
  if (GetSessionType() == SessionType::kMultiplexedSession) {
    ASSERT_TRUE(response[0].has_precommit_token());
  }
  EXPECT_THAT(response, ElementsAre(Partially(EqualsProto(
                            R"pb(metadata {
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
                                 chunked_value: false
                                 stats { row_count_exact: 0 }
                            )pb"))));
}

TEST_P(QueryApiTest, ExecuteSqlWithDmlReturningStar) {
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"pb(
    options { read_write {} }
  )pb");
  begin_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(
      R"""(
        sql: "DELETE test_table WHERE TRUE THEN RETURN *"
      )""");
  request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));
  request.set_query_mode(spanner_api::ExecuteSqlRequest::PLAN);
  request.mutable_transaction()->set_id(transaction_response.id());

  spanner_api::ResultSet response;
  ZETASQL_ASSERT_OK(ExecuteSql(request, &response));
  if (GetSessionType() == SessionType::kMultiplexedSession) {
    ASSERT_TRUE(response.has_precommit_token());
  }
  EXPECT_THAT(
      response,
      Partially(EqualsProto(
          R"pb(
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
            stats {
              query_plan { plan_nodes { display_name: "No query plan" } }
              row_count_exact: 0
            }
          )pb")));
}

TEST_P(QueryApiTest, ExecuteSqlUpdateReturning) {
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"pb(
    options { read_write {} }
  )pb");
  begin_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(
      R"""(
        sql: "UPDATE test_table SET string_col=@p1 "
             "WHERE int64_col=@p2 THEN RETURN string_col"
      )""");
  request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));
  request.set_query_mode(spanner_api::ExecuteSqlRequest::PLAN);
  request.mutable_transaction()->set_id(transaction_response.id());

  spanner_api::ResultSet response;
  ZETASQL_ASSERT_OK(ExecuteSql(request, &response));
  if (GetSessionType() == SessionType::kMultiplexedSession) {
    ASSERT_TRUE(response.has_precommit_token());
  }
  EXPECT_THAT(
      response,
      Partially(EqualsProto(
          R"pb(
            metadata {
              row_type {
                fields {
                  name: "string_col"
                  type { code: STRING }
                }
              }
              undeclared_parameters {
                fields {
                  name: "p1"
                  type { code: STRING }
                }
                fields {
                  name: "p2"
                  type { code: INT64 }
                }
              }
            }
            stats {
              query_plan { plan_nodes { display_name: "No query plan" } }
              row_count_exact: 0
            }
          )pb")));
}

TEST_P(QueryApiTest, ExecuteSqlDmlPlanWithoutReturning) {
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"pb(
    options { read_write {} }
  )pb");
  begin_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(
      R"""(
        sql: "UPDATE test_table SET string_col=@p1 "
             "WHERE int64_col=@p2"
      )""");
  request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));
  request.set_query_mode(spanner_api::ExecuteSqlRequest::PLAN);
  request.mutable_transaction()->set_id(transaction_response.id());

  spanner_api::ResultSet response;
  ZETASQL_ASSERT_OK(ExecuteSql(request, &response));
  if (GetSessionType() == SessionType::kMultiplexedSession) {
    ASSERT_TRUE(response.has_precommit_token());
  }
  EXPECT_THAT(
      response,
      Partially(EqualsProto(
          R"pb(
            metadata {
              row_type {}
              undeclared_parameters {
                fields {
                  name: "p1"
                  type { code: STRING }
                }
                fields {
                  name: "p2"
                  type { code: INT64 }
                }
              }
            }
            stats {
              query_plan { plan_nodes { display_name: "No query plan" } }
              row_count_exact: 0
            }
          )pb")));
}

TEST_P(QueryApiTest, ExecuteSqlWithDmlAndProtoParameters) {
  ZETASQL_ASSERT_OK(AddProtoTables());

  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"pb(
    options { read_write {} }
  )pb");
  begin_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(
      R"""(
        sql: "INSERT INTO proto_table (int64_col, proto_col) "
             "VALUES (@p1, @p2)"
      )""");
  request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));
  request.set_query_mode(spanner_api::ExecuteSqlRequest::PLAN);
  request.mutable_transaction()->set_id(transaction_response.id());

  spanner_api::ResultSet response;
  ZETASQL_ASSERT_OK(ExecuteSql(request, &response));
  if (GetSessionType() == SessionType::kMultiplexedSession) {
    ASSERT_TRUE(response.has_precommit_token());
  }
  EXPECT_THAT(
      response,
      Partially(EqualsProto(
          R"pb(
            metadata {
              row_type {}
              undeclared_parameters {
                fields {
                  name: "p1"
                  type { code: INT64 }
                }
                fields {
                  name: "p2"
                  type { code: PROTO proto_type_fqn: "customer.app.User" }
                }
              }
            }
            stats {
              query_plan { plan_nodes { display_name: "No query plan" } }
              row_count_exact: 0
            }
          )pb")));
}

TEST_P(QueryApiTest, ExecuteStreamingSql) {
  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(
      R"(
        transaction { single_use { read_only { strong: true } } }
        sql: "SELECT int64_col, string_col FROM test_table "
             "ORDER BY int64_col ASC, string_col DESC"
      )");
  request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  std::vector<spanner_api::PartialResultSet> response;
  ZETASQL_EXPECT_OK(ExecuteStreamingSql(request, &response));
  if (GetSessionType() == SessionType::kMultiplexedSession) {
    ASSERT_FALSE(response.back().has_precommit_token());
  }
  EXPECT_THAT(response, ElementsAre(Partially(EqualsProto(
                            R"pb(metadata {
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
                            )pb"))));
}

TEST_P(QueryApiTest, ExecuteStreamingSqlWithParameters) {
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
  request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  std::vector<spanner_api::PartialResultSet> response;
  ZETASQL_EXPECT_OK(ExecuteStreamingSql(request, &response));
  if (GetSessionType() == SessionType::kMultiplexedSession) {
    ASSERT_FALSE(response.back().has_precommit_token());
  }
  EXPECT_THAT(response, ElementsAre(Partially(EqualsProto(
                            R"pb(
                              metadata {
                                row_type {
                                  fields {
                                    name: "param"
                                    type { code: STRING }
                                  }
                                }
                                undeclared_parameters {
                                  fields {
                                    name: "param"
                                    type { code: STRING }
                                  }
                                }
                              }
                              values { string_value: "value" }
                              values { string_value: "value" }
                              values { string_value: "value" }
                            )pb"))));
}

TEST_P(QueryApiTest, ExecuteStreamingSqlWithProtoParameters) {
  ZETASQL_ASSERT_OK(AddProtoTables());
  ZETASQL_ASSERT_OK(PopulateProtoTable());

  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(
      R"pb(
        transaction { single_use { read_only { strong: true } } }
        sql: "SELECT @param.int_field AS intval FROM test_table"
        params {
          fields {
            key: "param"
            value { string_value: "CLoC" }
          }
        }
        param_types {
          key: "param"
          value { code: PROTO proto_type_fqn: "customer.app.User" }
        }
      )pb");
  request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  std::vector<spanner_api::PartialResultSet> response;
  ZETASQL_EXPECT_OK(ExecuteStreamingSql(request, &response));
  if (GetSessionType() == SessionType::kMultiplexedSession) {
    ASSERT_FALSE(response.back().has_precommit_token());
  }
  EXPECT_THAT(
      response,
      ElementsAre(EqualsProto(
          R"pb(
            metadata {
              row_type {
                fields {
                  name: "intval"
                  type { code: INT64 }
                }
              }
              undeclared_parameters {
                fields {
                  name: "param"
                  type { code: PROTO proto_type_fqn: "customer.app.User" }
                }
              }
            }
            values { string_value: "314" }
            values { string_value: "314" }
            values { string_value: "314" }
          )pb")));
}

TEST_P(QueryApiTest, ExecuteStreamingSqlWithDmlAndParameters) {
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"pb(
    options { read_write {} }
  )pb");
  begin_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(
      R"""(
        sql: "INSERT INTO test_table (int64_col, string_col) "
             "VALUES (@p1, @p2)"
      )""");
  request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));
  request.set_query_mode(spanner_api::ExecuteSqlRequest::PLAN);
  request.mutable_transaction()->set_id(transaction_response.id());

  std::vector<spanner_api::PartialResultSet> response;
  ZETASQL_EXPECT_OK(ExecuteStreamingSql(request, &response));
  if (GetSessionType() == SessionType::kMultiplexedSession) {
    ASSERT_TRUE(response.back().has_precommit_token());
  }
  EXPECT_THAT(response, ElementsAre(Partially(EqualsProto(
                            R"pb(
                              metadata {
                                row_type {}
                                undeclared_parameters {
                                  fields {
                                    name: "p1"
                                    type { code: INT64 }
                                  }
                                  fields {
                                    name: "p2"
                                    type { code: STRING }
                                  }
                                }
                              }
                              stats { row_count_exact: 0 }
                            )pb"))));
}

TEST_P(QueryApiTest, AcceptsPlanMode) {
  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(
      R"pb(
        transaction { single_use { read_only { strong: true } } }
        query_mode: PLAN
        sql: "SELECT * FROM test_table"
      )pb");
  request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  // PLAN mode accepted in non-streaming case.
  {
    spanner_api::ResultSet response;
    EXPECT_THAT(ExecuteSql(request, &response),
                StatusIs(absl::StatusCode::kOk));
    if (GetSessionType() == SessionType::kMultiplexedSession) {
      ASSERT_FALSE(response.has_precommit_token());
    }
  }

  // PLAN mode accepted in streaming case.
  {
    std::vector<spanner_api::PartialResultSet> response;
    EXPECT_THAT(ExecuteStreamingSql(request, &response),
                StatusIs(absl::StatusCode::kOk));
    if (GetSessionType() == SessionType::kMultiplexedSession) {
      ASSERT_FALSE(response.back().has_precommit_token());
    }
  }
}

TEST_P(QueryApiTest, DirectedReadsWithROTxnSucceeds) {
  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(
      R"pb(
        transaction { single_use { read_only { strong: true } } }
        sql: "SELECT int64_col, string_col FROM test_table "
             "ORDER BY int64_col ASC, string_col DESC"
        directed_read_options {
          include_replicas { replica_selections { type: READ_ONLY } }
        }
      )pb");
  request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  // Directed Reads accepted in non-streaming case.
  {
    spanner_api::ResultSet unused_response;
    EXPECT_THAT(ExecuteSql(request, &unused_response),
                StatusIs(absl::StatusCode::kOk));
    if (GetSessionType() == SessionType::kMultiplexedSession) {
      ASSERT_FALSE(unused_response.has_precommit_token());
    }
  }

  // Directed Reads accepted in streaming case.
  {
    std::vector<spanner_api::PartialResultSet> unused_response;
    EXPECT_THAT(ExecuteStreamingSql(request, &unused_response),
                StatusIs(absl::StatusCode::kOk));
    if (GetSessionType() == SessionType::kMultiplexedSession) {
      ASSERT_FALSE(unused_response.back().has_precommit_token());
    }
  }
}

TEST_P(QueryApiTest, DirectedReadsWithRWTxnFails) {
  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(
      R"pb(
        transaction { begin { read_write {} } }
        sql: "SELECT int64_col, string_col FROM test_table "
             "ORDER BY int64_col ASC, string_col DESC"
        directed_read_options {
          include_replicas { replica_selections { type: READ_ONLY } }
        }
      )pb");
  request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  // Directed Reads rejected in non-streaming case.
  {
    spanner_api::ResultSet unused_response;
    EXPECT_THAT(ExecuteSql(request, &unused_response),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }

  // Directed Reads rejected in streaming case.
  {
    std::vector<spanner_api::PartialResultSet> unused_response;
    EXPECT_THAT(ExecuteStreamingSql(request, &unused_response),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
}

}  // namespace

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
