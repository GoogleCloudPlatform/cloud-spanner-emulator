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

#include <memory>

#include "google/spanner/v1/spanner.pb.h"
#include "google/spanner/v1/transaction.pb.h"
#include "grpcpp/client_context.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "backend/transaction/options.h"
#include "common/errors.h"
#include "frontend/common/uris.h"
#include "frontend/converters/mutations.h"
#include "frontend/entities/database.h"
#include "frontend/entities/session.h"
#include "frontend/server/environment.h"
#include "frontend/server/handler.h"
#include "frontend/server/request_context.h"
#include "tests/common/proto_matchers.h"
#include "tests/common/test_env.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {
namespace {

using ::zetasql_base::testing::StatusIs;

namespace spanner_api = ::google::spanner::v1;

class TransactionApiTest : public test::ServerTest {
 protected:
  void SetUp() override {
    ZETASQL_ASSERT_OK(CreateTestInstance());
    ZETASQL_ASSERT_OK(CreateTestDatabase());
    ZETASQL_ASSERT_OK_AND_ASSIGN(test_session_uri_, CreateTestSession());
  }

  std::string test_session_uri_;
};

TEST_F(TransactionApiTest, CanBeginTransaction) {
  spanner_api::BeginTransactionRequest request = PARSE_TEXT_PROTO(R"(
    options { read_only {} }
  )");
  request.set_session(test_session_uri_);

  spanner_api::Transaction response;
  ZETASQL_EXPECT_OK(BeginTransaction(request, &response));
}

TEST_F(TransactionApiTest, CanBeginMultipleTransactionsOnSameSession) {
  spanner_api::BeginTransactionRequest request1 = PARSE_TEXT_PROTO(R"(
    options { read_only {} }
  )");
  request1.set_session(test_session_uri_);

  spanner_api::Transaction response1;
  ZETASQL_EXPECT_OK(BeginTransaction(request1, &response1));

  spanner_api::BeginTransactionRequest request2 = PARSE_TEXT_PROTO(R"(
    options { read_write {} }
  )");
  request2.set_session(test_session_uri_);

  spanner_api::Transaction response2;
  ZETASQL_EXPECT_OK(BeginTransaction(request2, &response2));
}

TEST_F(TransactionApiTest, CanCommitAlreadyStartedTransaction) {
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"(
    options { read_write {} }
  )");
  begin_request.set_session(test_session_uri_);

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  spanner_api::CommitRequest commit_request;
  commit_request.set_transaction_id(transaction_response.id());
  commit_request.set_session(test_session_uri_);

  spanner_api::CommitResponse commit_response;
  ZETASQL_EXPECT_OK(Commit(commit_request, &commit_response));
}

TEST_F(TransactionApiTest, CannotCommitNonExistentTransaction) {
  spanner_api::CommitRequest commit_request;
  commit_request.set_transaction_id(std::to_string(1));
  commit_request.set_session(test_session_uri_);

  spanner_api::CommitResponse commit_response;
  EXPECT_THAT(Commit(commit_request, &commit_response),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(TransactionApiTest, CanCommitSingleUseReadWriteTransaction) {
  spanner_api::CommitRequest commit_request = PARSE_TEXT_PROTO(R"(
    single_use_transaction { read_write {} }
  )");
  commit_request.set_session(test_session_uri_);

  spanner_api::CommitResponse commit_response;
  ZETASQL_EXPECT_OK(Commit(commit_request, &commit_response));
}

TEST_F(TransactionApiTest, CannotCommitSingleUseReadOnlyTransaction) {
  spanner_api::CommitRequest commit_request = PARSE_TEXT_PROTO(R"(
    single_use_transaction { read_only {} }
  )");
  commit_request.set_session(test_session_uri_);

  spanner_api::CommitResponse commit_response;
  EXPECT_THAT(Commit(commit_request, &commit_response),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(TransactionApiTest, CannotCommitSingleUseInvalidTransactionMode) {
  spanner_api::CommitRequest commit_request = PARSE_TEXT_PROTO(R"(
    single_use_transaction {}
  )");
  commit_request.set_session(test_session_uri_);

  spanner_api::CommitResponse commit_response;
  EXPECT_THAT(Commit(commit_request, &commit_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(TransactionApiTest, CannotCommitInvalidTransactionType) {
  spanner_api::CommitRequest commit_request = PARSE_TEXT_PROTO(R"(
  )");
  commit_request.set_session(test_session_uri_);

  spanner_api::CommitResponse commit_response;
  EXPECT_THAT(Commit(commit_request, &commit_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(TransactionApiTest, CommitTransactionIsIdempotent) {
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"(
    options { read_write {} }
  )");
  begin_request.set_session(test_session_uri_);

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  spanner_api::CommitRequest commit_request1;
  commit_request1.set_transaction_id(transaction_response.id());
  commit_request1.set_session(test_session_uri_);

  spanner_api::CommitResponse commit_response1;
  ZETASQL_EXPECT_OK(Commit(commit_request1, &commit_response1));

  spanner_api::CommitRequest commit_request2;
  commit_request2.set_transaction_id(transaction_response.id());
  commit_request2.set_session(test_session_uri_);

  spanner_api::CommitResponse commit_response2;
  ZETASQL_EXPECT_OK(Commit(commit_request2, &commit_response2));

  // Responses should match.
  EXPECT_THAT(commit_response1, test::EqualsProto(commit_response2));
}

TEST_F(TransactionApiTest, CanRollbackAlreadyStartedTransaction) {
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"(
    options { read_write {} }
  )");
  begin_request.set_session(test_session_uri_);

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  spanner_api::RollbackRequest rollback_request;
  rollback_request.set_transaction_id(transaction_response.id());
  rollback_request.set_session(test_session_uri_);

  ZETASQL_EXPECT_OK(Rollback(rollback_request));
}

TEST_F(TransactionApiTest, CannotRollbackSnapshotTransaction) {
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"(
    options { read_only {} }
  )");
  begin_request.set_session(test_session_uri_);

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  spanner_api::RollbackRequest rollback_request;
  rollback_request.set_transaction_id(transaction_response.id());
  rollback_request.set_session(test_session_uri_);

  EXPECT_THAT(Rollback(rollback_request),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(TransactionApiTest, CannotRollbackAfterCommit) {
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"(
    options { read_write {} }
  )");
  begin_request.set_session(test_session_uri_);

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  spanner_api::CommitRequest commit_request1;
  commit_request1.set_transaction_id(transaction_response.id());
  commit_request1.set_session(test_session_uri_);

  spanner_api::CommitResponse commit_response1;
  ZETASQL_EXPECT_OK(Commit(commit_request1, &commit_response1));

  spanner_api::RollbackRequest rollback_request;
  rollback_request.set_transaction_id(transaction_response.id());
  rollback_request.set_session(test_session_uri_);

  EXPECT_THAT(Rollback(rollback_request),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(TransactionApiTest, CannotCommitAfterRollback) {
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"(
    options { read_write {} }
  )");
  begin_request.set_session(test_session_uri_);

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  spanner_api::RollbackRequest rollback_request;
  rollback_request.set_transaction_id(transaction_response.id());
  rollback_request.set_session(test_session_uri_);

  ZETASQL_EXPECT_OK(Rollback(rollback_request));

  spanner_api::CommitRequest commit_request1;
  commit_request1.set_transaction_id(transaction_response.id());
  commit_request1.set_session(test_session_uri_);

  spanner_api::CommitResponse commit_response1;
  EXPECT_THAT(Commit(commit_request1, &commit_response1),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(TransactionApiTest, CanUseTransactionAfterReadError) {
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"(
    options { read_write {} }
  )");
  begin_request.set_session(test_session_uri_);

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  // Invalid read request.
  spanner_api::ReadRequest read_request = PARSE_TEXT_PROTO(R"(
    table: "non_existent_table"
    columns: "int64_col"
    columns: "string_col"
    key_set { all: true }
  )");
  read_request.set_session(test_session_uri_);
  spanner_api::TransactionSelector selector;
  selector.set_id(transaction_response.id());
  *read_request.mutable_transaction() = selector;

  spanner_api::ResultSet read_response;
  EXPECT_THAT(Read(read_request, &read_response),
              StatusIs(absl::StatusCode::kNotFound));

  // Subsequent operations on the transaction should work as normal since a not
  // found error on read does not invalidate the transaction.
  spanner_api::CommitRequest commit_request;
  commit_request.set_transaction_id(transaction_response.id());
  commit_request.set_session(test_session_uri_);

  spanner_api::CommitResponse commit_response;
  ZETASQL_EXPECT_OK(Commit(commit_request, &commit_response));
}

}  // namespace

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
