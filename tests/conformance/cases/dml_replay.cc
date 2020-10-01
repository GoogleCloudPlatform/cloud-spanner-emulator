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

#include "google/spanner/v1/spanner.pb.h"
#include "google/spanner/v1/transaction.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "tests/common/proto_matchers.h"
#include "tests/conformance/common/database_test_base.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using test::EqualsProto;
using test::proto::Partially;
using zetasql_base::testing::StatusIs;

class DmlReplayTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    ZETASQL_RETURN_IF_ERROR(SetSchema({
        R"(
          CREATE TABLE Users(
            ID       INT64 NOT NULL,
            Name     STRING(MAX),
            Age      INT64,
            Updated  TIMESTAMP,
          ) PRIMARY KEY (ID)
        )",
    }));

    // Create a raw session for tests which cannot use the C++ client library
    // directly.
    ZETASQL_RETURN_IF_ERROR(CreateSession(database()->FullName()));
    return absl::OkStatus();
  }

 protected:
  absl::Status CreateSession(absl::string_view database_uri) {
    grpc::ClientContext context;
    spanner_api::CreateSessionRequest request;
    request.set_database(std::string(database_uri));  // NOLINT
    spanner_api::Session response;
    ZETASQL_RETURN_IF_ERROR(raw_client()->CreateSession(&context, request, &response));
    session_name_ = response.name();
    return absl::OkStatus();
  }

  absl::Status BeginReadWriteTransaction(spanner_api::Transaction* txn) {
    spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"(
      options { read_write {} }
    )");
    begin_request.set_session(session_name_);

    grpc::ClientContext context;
    return raw_client()->BeginTransaction(&context, begin_request, txn);
  }

  absl::Status Commit(const std::string txn_id,
                      spanner_api::CommitResponse* response) {
    spanner_api::CommitRequest request;
    request.set_transaction_id(txn_id);
    request.set_session(session_name_);
    grpc::ClientContext context;
    return raw_client()->Commit(&context, request, response);
  }

  absl::Status ReadFromClientReader(
      std::unique_ptr<grpc::ClientReader<spanner_api::PartialResultSet>> reader,
      std::vector<spanner_api::PartialResultSet>* response) {
    response->clear();
    spanner_api::PartialResultSet result;
    while (reader->Read(&result)) {
      response->push_back(result);
    }
    return reader->Finish();
  }

  std::string session_name_;
};

TEST_F(DmlReplayTest, DMLSequenceNumberOutOfOrderReturnsError) {
  spanner_api::Transaction txn;
  ZETASQL_EXPECT_OK(BeginReadWriteTransaction(&txn));

  {
    spanner_api::ExecuteSqlRequest dml_request = PARSE_TEXT_PROTO(R"(
      sql: "INSERT INTO Users(ID, Name) VALUES(2, \'value\') "
      params {}
      seqno: 2
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    spanner_api::ResultSet response;
    grpc::ClientContext context;
    ZETASQL_EXPECT_OK(raw_client()->ExecuteSql(&context, dml_request, &response));
  }

  {
    // Send a request with an out of order sequence number.
    spanner_api::ExecuteSqlRequest dml_request = PARSE_TEXT_PROTO(R"(
      sql: "INSERT INTO Users(ID, Name) VALUES(1, \'value\') "
      params {}
      seqno: 1
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    spanner_api::ResultSet response;
    grpc::ClientContext context;
    EXPECT_THAT(raw_client()->ExecuteSql(&context, dml_request, &response),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }

  {
    // Next request should replay the invalid argument from before.
    spanner_api::ExecuteSqlRequest dml_request = PARSE_TEXT_PROTO(R"(
      sql: "INSERT INTO Users(ID, Name) VALUES(3, \'value\') "
      params {}
      seqno: 3
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    spanner_api::ResultSet response;
    grpc::ClientContext context;
    ZETASQL_EXPECT_OK(raw_client()->ExecuteSql(&context, dml_request, &response));
  }
}

TEST_F(DmlReplayTest, StreamingDMLSequenceNumberOutOfOrderReturnsError) {
  spanner_api::Transaction txn;
  ZETASQL_EXPECT_OK(BeginReadWriteTransaction(&txn));

  {
    spanner_api::ExecuteSqlRequest dml_request = PARSE_TEXT_PROTO(R"(
      sql: "INSERT INTO Users(ID, Name) VALUES(2, \'value\') "
      params {}
      seqno: 2
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    std::vector<spanner_api::PartialResultSet> response;
    grpc::ClientContext context;
    auto client_reader =
        raw_client()->ExecuteStreamingSql(&context, dml_request);
    ZETASQL_EXPECT_OK(ReadFromClientReader(std::move(client_reader), &response));
  }

  {
    // Send a request with an out of order sequence number.
    spanner_api::ExecuteSqlRequest dml_request = PARSE_TEXT_PROTO(R"(
      sql: "INSERT INTO Users(ID, Name) VALUES(1, \'value\') "
      params {}
      seqno: 1
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    std::vector<spanner_api::PartialResultSet> response;
    grpc::ClientContext context;
    auto client_reader =
        raw_client()->ExecuteStreamingSql(&context, dml_request);
    EXPECT_THAT(ReadFromClientReader(std::move(client_reader), &response),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }

  {
    // Next request should replay the invalid argument from before.
    spanner_api::ExecuteSqlRequest dml_request = PARSE_TEXT_PROTO(R"(
      sql: "INSERT INTO Users(ID, Name) VALUES(3, \'value\') "
      params {}
      seqno: 3
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    std::vector<spanner_api::PartialResultSet> response;
    grpc::ClientContext context;
    auto client_reader =
        raw_client()->ExecuteStreamingSql(&context, dml_request);
    ZETASQL_EXPECT_OK(ReadFromClientReader(std::move(client_reader), &response));
  }
}

TEST_F(DmlReplayTest, BatchDMLSequenceNumberOutOfOrderReturnsError) {
  spanner_api::Transaction txn;
  ZETASQL_EXPECT_OK(BeginReadWriteTransaction(&txn));

  {
    spanner_api::ExecuteBatchDmlRequest dml_request = PARSE_TEXT_PROTO(R"(
      statements { sql: "INSERT INTO Users(ID, Name) VALUES(2, \'value\') " }
      seqno: 2
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    spanner_api::ExecuteBatchDmlResponse response;
    grpc::ClientContext context;
    ZETASQL_EXPECT_OK(raw_client()->ExecuteBatchDml(&context, dml_request, &response));
    ZETASQL_EXPECT_OK(absl::Status(absl::StatusCode(response.status().code()),
                           response.status().message()));
  }

  {
    // Send a request with an out of order sequence number.
    spanner_api::ExecuteBatchDmlRequest dml_request = PARSE_TEXT_PROTO(R"(
      statements { sql: "INSERT INTO Users(ID, Name) VALUES(1, \'value\') " }
      seqno: 1
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    spanner_api::ExecuteBatchDmlResponse response;
    grpc::ClientContext context;
    EXPECT_THAT(raw_client()->ExecuteBatchDml(&context, dml_request, &response),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }

  {
    // Next request should replay the invalid argument from before.
    spanner_api::ExecuteBatchDmlRequest dml_request = PARSE_TEXT_PROTO(R"(
      statements { sql: "INSERT INTO Users(ID, Name) VALUES(3, \'value\') " }
      seqno: 3
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    spanner_api::ExecuteBatchDmlResponse response;
    grpc::ClientContext context;
    ZETASQL_EXPECT_OK(raw_client()->ExecuteBatchDml(&context, dml_request, &response));
    ZETASQL_EXPECT_OK(absl::Status(absl::StatusCode(response.status().code()),
                           response.status().message()));
  }
}

TEST_F(DmlReplayTest, DMLRequestHashMismatchReturnsError) {
  spanner_api::Transaction txn;
  ZETASQL_EXPECT_OK(BeginReadWriteTransaction(&txn));

  {
    spanner_api::ExecuteSqlRequest dml_request = PARSE_TEXT_PROTO(R"(
      sql: "INSERT INTO Users(ID, Name) VALUES(1, \'value\') "
      params {}
      seqno: 1
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    spanner_api::ResultSet response;
    grpc::ClientContext context;
    ZETASQL_EXPECT_OK(raw_client()->ExecuteSql(&context, dml_request, &response));
  }

  {
    // Mismatching request hash should return error.
    spanner_api::ExecuteSqlRequest dml_request = PARSE_TEXT_PROTO(R"(
      sql: "INSERT INTO Users(ID, Name) VALUES(55, \'test\') "
      params {}
      seqno: 1
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    spanner_api::ResultSet response;
    grpc::ClientContext context;
    EXPECT_THAT(raw_client()->ExecuteSql(&context, dml_request, &response),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }

  {
    // Next request should replay the invalid argument from before.
    spanner_api::ExecuteSqlRequest dml_request = PARSE_TEXT_PROTO(R"(
      sql: "INSERT INTO Users(ID, Name) VALUES(2, \'value\') "
      params {}
      seqno: 2
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    spanner_api::ResultSet response;
    grpc::ClientContext context;
    ZETASQL_EXPECT_OK(raw_client()->ExecuteSql(&context, dml_request, &response));
  }

  spanner_api::CommitResponse response;
  ZETASQL_EXPECT_OK(Commit(txn.id(), &response));
}

TEST_F(DmlReplayTest, StreamingDMLRequestHashMismatchReturnsError) {
  spanner_api::Transaction txn;
  ZETASQL_EXPECT_OK(BeginReadWriteTransaction(&txn));

  {
    spanner_api::ExecuteSqlRequest dml_request = PARSE_TEXT_PROTO(R"(
      sql: "INSERT INTO Users(ID, Name) VALUES(1, \'value\') "
      params {}
      seqno: 1
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    std::vector<spanner_api::PartialResultSet> response;
    grpc::ClientContext context;
    auto client_reader =
        raw_client()->ExecuteStreamingSql(&context, dml_request);
    ZETASQL_EXPECT_OK(ReadFromClientReader(std::move(client_reader), &response));
  }

  {
    // Mismatching request hash should return error.
    spanner_api::ExecuteSqlRequest dml_request = PARSE_TEXT_PROTO(R"(
      sql: "INSERT INTO Users(ID, Name) VALUES(55, \'test\') "
      params {}
      seqno: 1
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    std::vector<spanner_api::PartialResultSet> response;
    grpc::ClientContext context;
    auto client_reader =
        raw_client()->ExecuteStreamingSql(&context, dml_request);
    EXPECT_THAT(ReadFromClientReader(std::move(client_reader), &response),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }

  {
    // Next request should replay the invalid argument from before.
    spanner_api::ExecuteSqlRequest dml_request = PARSE_TEXT_PROTO(R"(
      sql: "INSERT INTO Users(ID, Name) VALUES(2, \'value\') "
      params {}
      seqno: 2
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    std::vector<spanner_api::PartialResultSet> response;
    grpc::ClientContext context;
    auto client_reader =
        raw_client()->ExecuteStreamingSql(&context, dml_request);
    ZETASQL_EXPECT_OK(ReadFromClientReader(std::move(client_reader), &response));
  }

  spanner_api::CommitResponse response;
  ZETASQL_EXPECT_OK(Commit(txn.id(), &response));
}

TEST_F(DmlReplayTest, BatchDMLRequestHashMismatchReturnsError) {
  spanner_api::Transaction txn;
  ZETASQL_EXPECT_OK(BeginReadWriteTransaction(&txn));

  {
    spanner_api::ExecuteBatchDmlRequest dml_request = PARSE_TEXT_PROTO(R"(
      statements { sql: "INSERT INTO Users(ID, Name) VALUES(1, \'value\') " }
      seqno: 1
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    spanner_api::ExecuteBatchDmlResponse response;
    grpc::ClientContext context;
    ZETASQL_EXPECT_OK(raw_client()->ExecuteBatchDml(&context, dml_request, &response));
    ZETASQL_EXPECT_OK(absl::Status(absl::StatusCode(response.status().code()),
                           response.status().message()));
  }

  {
    // Mismatching request hash should return error.
    spanner_api::ExecuteBatchDmlRequest dml_request = PARSE_TEXT_PROTO(R"(
      statements { sql: "INSERT INTO Users(ID, Name) VALUES(55, \'test\') " }
      seqno: 1
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    spanner_api::ExecuteBatchDmlResponse response;
    grpc::ClientContext context;
    EXPECT_THAT(raw_client()->ExecuteBatchDml(&context, dml_request, &response),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }

  {
    // Next request should replay the invalid argument from before.
    spanner_api::ExecuteBatchDmlRequest dml_request = PARSE_TEXT_PROTO(R"(
      statements { sql: "INSERT INTO Users(ID, Name) VALUES(2, \'value\') " }
      seqno: 2
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    spanner_api::ExecuteBatchDmlResponse response;
    grpc::ClientContext context;
    ZETASQL_EXPECT_OK(raw_client()->ExecuteBatchDml(&context, dml_request, &response));
    ZETASQL_EXPECT_OK(absl::Status(absl::StatusCode(response.status().code()),
                           response.status().message()));
  }

  spanner_api::CommitResponse response;
  ZETASQL_EXPECT_OK(Commit(txn.id(), &response));
}

TEST_F(DmlReplayTest, DMLSequenceReplaySucceeds) {
  spanner_api::Transaction txn;
  ZETASQL_EXPECT_OK(BeginReadWriteTransaction(&txn));

  {
    spanner_api::ExecuteSqlRequest dml_request = PARSE_TEXT_PROTO(R"(
      sql: "INSERT INTO Users(ID, Name) VALUES(1, \'value\') "
      params {}
      seqno: 1
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    spanner_api::ResultSet response;
    grpc::ClientContext context;
    ZETASQL_EXPECT_OK(raw_client()->ExecuteSql(&context, dml_request, &response));
    EXPECT_THAT(response, EqualsProto(R"(
                  metadata { row_type {} }
                  stats { row_count_exact: 1 }
                )"));
  }

  {
    // Attempt to insert a value that already exists.
    spanner_api::ExecuteSqlRequest dml_request = PARSE_TEXT_PROTO(R"(
      sql: "INSERT INTO Users(ID, Name) VALUES(1, \'value\') "
      params {}
      seqno: 2
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    spanner_api::ResultSet response;
    grpc::ClientContext context;
    EXPECT_THAT(raw_client()->ExecuteSql(&context, dml_request, &response),
                StatusIs(absl::StatusCode::kAlreadyExists));
  }

  {
    // Replay ok status from first sequence.
    spanner_api::ExecuteSqlRequest dml_request = PARSE_TEXT_PROTO(R"(
      sql: "INSERT INTO Users(ID, Name) VALUES(1, \'value\') "
      params {}
      seqno: 1
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    spanner_api::ResultSet response;
    grpc::ClientContext context;
    ZETASQL_EXPECT_OK(raw_client()->ExecuteSql(&context, dml_request, &response));
    EXPECT_THAT(response, EqualsProto(R"(
                  metadata { row_type {} }
                  stats { row_count_exact: 1 }
                )"));
  }

  {
    // Replay error from second sequence.
    spanner_api::ExecuteSqlRequest dml_request = PARSE_TEXT_PROTO(R"(
      sql: "INSERT INTO Users(ID, Name) VALUES(1, \'value\') "
      params {}
      seqno: 2
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    spanner_api::ResultSet response;
    grpc::ClientContext context;
    EXPECT_THAT(raw_client()->ExecuteSql(&context, dml_request, &response),
                StatusIs(absl::StatusCode::kAlreadyExists));
  }
}

TEST_F(DmlReplayTest, StreamingDMLSequenceReplaySucceeds) {
  spanner_api::Transaction txn;
  ZETASQL_EXPECT_OK(BeginReadWriteTransaction(&txn));

  {
    spanner_api::ExecuteSqlRequest dml_request = PARSE_TEXT_PROTO(R"(
      sql: "INSERT INTO Users(ID, Name) VALUES(1, \'value\') "
      params {}
      seqno: 1
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    std::vector<spanner_api::PartialResultSet> response;
    grpc::ClientContext context;
    auto client_reader =
        raw_client()->ExecuteStreamingSql(&context, dml_request);
    ZETASQL_EXPECT_OK(ReadFromClientReader(std::move(client_reader), &response));
    EXPECT_THAT(response[0], EqualsProto(R"(
                  metadata { row_type {} }
                  stats { row_count_exact: 1 }
                )"));
  }

  {
    // Attempt to insert a value that already exists.
    spanner_api::ExecuteSqlRequest dml_request = PARSE_TEXT_PROTO(R"(
      sql: "INSERT INTO Users(ID, Name) VALUES(1, \'value\') "
      params {}
      seqno: 2
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    std::vector<spanner_api::PartialResultSet> response;
    grpc::ClientContext context;
    auto client_reader =
        raw_client()->ExecuteStreamingSql(&context, dml_request);
    EXPECT_THAT(ReadFromClientReader(std::move(client_reader), &response),
                StatusIs(absl::StatusCode::kAlreadyExists));
  }

  {
    // Replay ok status from first sequence.
    spanner_api::ExecuteSqlRequest dml_request = PARSE_TEXT_PROTO(R"(
      sql: "INSERT INTO Users(ID, Name) VALUES(1, \'value\') "
      params {}
      seqno: 1
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    std::vector<spanner_api::PartialResultSet> response;
    grpc::ClientContext context;
    auto client_reader =
        raw_client()->ExecuteStreamingSql(&context, dml_request);
    ZETASQL_EXPECT_OK(ReadFromClientReader(std::move(client_reader), &response));
    EXPECT_THAT(response[0], EqualsProto(R"(
                  metadata { row_type {} }
                  stats { row_count_exact: 1 }
                )"));
  }

  {
    // Replay error from second sequence.
    spanner_api::ExecuteSqlRequest dml_request = PARSE_TEXT_PROTO(R"(
      sql: "INSERT INTO Users(ID, Name) VALUES(1, \'value\') "
      params {}
      seqno: 2
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    std::vector<spanner_api::PartialResultSet> response;
    grpc::ClientContext context;
    auto client_reader =
        raw_client()->ExecuteStreamingSql(&context, dml_request);
    EXPECT_THAT(ReadFromClientReader(std::move(client_reader), &response),
                StatusIs(absl::StatusCode::kAlreadyExists));
  }
}

TEST_F(DmlReplayTest, BatchDMLSequenceReplaySucceeds) {
  spanner_api::Transaction txn;
  ZETASQL_EXPECT_OK(BeginReadWriteTransaction(&txn));

  {
    spanner_api::ExecuteBatchDmlRequest dml_request = PARSE_TEXT_PROTO(R"(
      statements { sql: "INSERT INTO Users(ID, Name) VALUES(1, \'value\') " }
      seqno: 1
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    spanner_api::ExecuteBatchDmlResponse response;
    grpc::ClientContext context;
    ZETASQL_EXPECT_OK(raw_client()->ExecuteBatchDml(&context, dml_request, &response));
    EXPECT_THAT(response, EqualsProto(R"(
                  result_sets {
                    metadata { row_type {} }
                    stats { row_count_exact: 1 }
                  }
                  status { code: 0 }
                )"));
  }

  {
    // Attempt to insert a value that already exists.
    spanner_api::ExecuteBatchDmlRequest dml_request = PARSE_TEXT_PROTO(R"(
      statements { sql: "INSERT INTO Users(ID, Name) VALUES(1, \'value\') " }
      seqno: 2
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    spanner_api::ExecuteBatchDmlResponse response;
    grpc::ClientContext context;
    ZETASQL_EXPECT_OK(raw_client()->ExecuteBatchDml(&context, dml_request, &response));
    EXPECT_THAT(response, Partially(EqualsProto(R"(
                  status { code: 6 }
                )")));
  }

  {
    // Replay ok status from first sequence.
    spanner_api::ExecuteBatchDmlRequest dml_request = PARSE_TEXT_PROTO(R"(
      statements { sql: "INSERT INTO Users(ID, Name) VALUES(1, \'value\') " }
      seqno: 1
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    spanner_api::ExecuteBatchDmlResponse response;
    grpc::ClientContext context;
    ZETASQL_EXPECT_OK(raw_client()->ExecuteBatchDml(&context, dml_request, &response));
    EXPECT_THAT(response, EqualsProto(R"(
                  result_sets {
                    metadata { row_type {} }
                    stats { row_count_exact: 1 }
                  }
                  status { code: 0 }
                )"));
  }

  {
    // Replay error from second sequence.
    spanner_api::ExecuteBatchDmlRequest dml_request = PARSE_TEXT_PROTO(R"(
      statements { sql: "INSERT INTO Users(ID, Name) VALUES(1, \'value\') " }
      seqno: 2
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    spanner_api::ExecuteBatchDmlResponse response;
    grpc::ClientContext context;
    ZETASQL_EXPECT_OK(raw_client()->ExecuteBatchDml(&context, dml_request, &response));
    EXPECT_THAT(response, Partially(EqualsProto(R"(
                  status { code: 6 }
                )")));
  }
}

TEST_F(DmlReplayTest, ExecuteAndCommitBatchDMLWithReplaySucceeds) {
  spanner_api::Transaction txn;
  ZETASQL_EXPECT_OK(BeginReadWriteTransaction(&txn));

  {
    spanner_api::ExecuteBatchDmlRequest dml_request = PARSE_TEXT_PROTO(R"(
      statements { sql: "INSERT INTO Users(ID, Name) VALUES(1, \'value\') " }
      statements { sql: "INSERT INTO Users(ID, Name) VALUES(2, \'value\') " }
      statements { sql: "INSERT INTO Users(ID, Name) VALUES(3, \'value\') " }
      seqno: 1
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    spanner_api::ExecuteBatchDmlResponse response;
    grpc::ClientContext context;
    ZETASQL_EXPECT_OK(raw_client()->ExecuteBatchDml(&context, dml_request, &response));
    EXPECT_THAT(response, EqualsProto(R"(
                  result_sets {
                    metadata { row_type {} }
                    stats { row_count_exact: 1 }
                  }
                  result_sets { stats { row_count_exact: 1 } }
                  result_sets { stats { row_count_exact: 1 } }
                  status { code: 0 }
                )"));
  }

  {
    // Replay ok status from first sequence.
    spanner_api::ExecuteBatchDmlRequest dml_request = PARSE_TEXT_PROTO(R"(
      statements { sql: "INSERT INTO Users(ID, Name) VALUES(1, \'value\') " }
      statements { sql: "INSERT INTO Users(ID, Name) VALUES(2, \'value\') " }
      statements { sql: "INSERT INTO Users(ID, Name) VALUES(3, \'value\') " }
      seqno: 1
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    spanner_api::ExecuteBatchDmlResponse response;
    grpc::ClientContext context;
    ZETASQL_EXPECT_OK(raw_client()->ExecuteBatchDml(&context, dml_request, &response));
    EXPECT_THAT(response, EqualsProto(R"(
                  result_sets {
                    metadata { row_type {} }
                    stats { row_count_exact: 1 }
                  }
                  result_sets { stats { row_count_exact: 1 } }
                  result_sets { stats { row_count_exact: 1 } }
                  status { code: 0 }
                )"));
  }

  spanner_api::CommitResponse response;
  ZETASQL_EXPECT_OK(Commit(txn.id(), &response));
}

TEST_F(DmlReplayTest, BatchDMLReturnsTransactionInResponse) {
  spanner_api::Transaction txn;
  ZETASQL_EXPECT_OK(BeginReadWriteTransaction(&txn));

  {
    spanner_api::ExecuteBatchDmlRequest dml_request = PARSE_TEXT_PROTO(R"(
      statements { sql: "INSERT INTO Users(ID, Name) VALUES(1, \'value\') " }
      statements { sql: "INSERT INTO Users(ID, Name) VALUES(2, \'value\') " }
      seqno: 1
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    spanner_api::ExecuteBatchDmlResponse response;
    grpc::ClientContext context;
    ZETASQL_EXPECT_OK(raw_client()->ExecuteBatchDml(&context, dml_request, &response));
    EXPECT_THAT(response, EqualsProto(R"(
                  result_sets {
                    metadata { row_type {} }
                    stats { row_count_exact: 1 }
                  }
                  result_sets { stats { row_count_exact: 1 } }
                  status { code: 0 }
                )"));
  }

  {
    // Replay ok status from first sequence.
    spanner_api::ExecuteBatchDmlRequest dml_request = PARSE_TEXT_PROTO(R"(
      statements { sql: "INSERT INTO Users(ID, Name) VALUES(1, \'value\') " }
      statements { sql: "INSERT INTO Users(ID, Name) VALUES(2, \'value\') " }
      seqno: 1
    )");
    dml_request.mutable_transaction()->set_id(txn.id());
    dml_request.set_session(session_name_);

    spanner_api::ExecuteBatchDmlResponse response;
    grpc::ClientContext context;
    ZETASQL_EXPECT_OK(raw_client()->ExecuteBatchDml(&context, dml_request, &response));
    EXPECT_THAT(response, EqualsProto(R"(
                  result_sets {
                    metadata { row_type {} }
                    stats { row_count_exact: 1 }
                  }
                  result_sets { stats { row_count_exact: 1 } }
                  status { code: 0 }
                )"));
  }
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
