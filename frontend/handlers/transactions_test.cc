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
#include <string>

#include "google/spanner/v1/commit_response.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "google/spanner/v1/transaction.pb.h"
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
#include "grpcpp/client_context.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {
namespace {

using ::zetasql_base::testing::StatusIs;

namespace spanner_api = ::google::spanner::v1;

enum class SessionType {
  kRegularSession,
  kMultiplexedSession,
};

class TransactionApiTest : public test::ServerTest,
                           public testing::WithParamInterface<SessionType> {
 protected:
  void SetUp() override {
    ZETASQL_ASSERT_OK(CreateTestInstance());
    ZETASQL_ASSERT_OK(CreateTestDatabase());
    ZETASQL_ASSERT_OK_AND_ASSIGN(test_session_uri_,
                         CreateTestSession(/*multiplexed=*/false));
    ZETASQL_ASSERT_OK_AND_ASSIGN(test_multiplexed_session_uri_,
                         CreateTestSession(/*multiplexed=*/true));
  }

  std::string GetSessionUri(bool multiplexed) {
    return multiplexed ? test_multiplexed_session_uri_ : test_session_uri_;
  }

  SessionType GetSessionType() { return GetParam(); }

  std::string test_session_uri_;
  std::string test_multiplexed_session_uri_;
};

INSTANTIATE_TEST_SUITE_P(SessionTypes, TransactionApiTest,
                         testing::Values(SessionType::kRegularSession,
                                         SessionType::kMultiplexedSession));

TEST_P(TransactionApiTest, CanBeginTransaction) {
  spanner_api::BeginTransactionRequest request = PARSE_TEXT_PROTO(R"(
    options { read_only {} }
  )");
  request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::Transaction response;
  ZETASQL_EXPECT_OK(BeginTransaction(request, &response));
}

TEST_P(TransactionApiTest, CanBeginMultipleTransactionsOnSameSession) {
  spanner_api::BeginTransactionRequest request1 = PARSE_TEXT_PROTO(R"(
    options { read_only {} }
  )");
  request1.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::Transaction response1;
  ZETASQL_EXPECT_OK(BeginTransaction(request1, &response1));

  spanner_api::BeginTransactionRequest request2 = PARSE_TEXT_PROTO(R"(
    options { read_write {} }
  )");
  request2.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::Transaction response2;
  ZETASQL_EXPECT_OK(BeginTransaction(request2, &response2));
}

TEST_P(TransactionApiTest, CanCommitAlreadyStartedTransaction) {
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"(
    options { read_write {} }
  )");
  begin_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  spanner_api::CommitRequest commit_request;
  commit_request.set_transaction_id(transaction_response.id());
  commit_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::CommitResponse commit_response;
  ZETASQL_EXPECT_OK(Commit(commit_request, &commit_response));
  if (GetSessionType() == SessionType::kMultiplexedSession) {
    // commit retry protocol will kick in since the it was an empty commit.
    ASSERT_TRUE(commit_response.has_precommit_token());
    *commit_request.mutable_precommit_token() =
        commit_response.precommit_token();
    ZETASQL_ASSERT_OK(Commit(commit_request, &commit_response));
  }
}

TEST_P(TransactionApiTest, CannotCommitNonExistentTransaction) {
  spanner_api::CommitRequest commit_request;
  commit_request.set_transaction_id(std::to_string(1));
  commit_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::CommitResponse commit_response;
  EXPECT_THAT(Commit(commit_request, &commit_response),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_P(TransactionApiTest, CanCommitSingleUseReadWriteTransaction) {
  spanner_api::CommitRequest commit_request = PARSE_TEXT_PROTO(R"(
    single_use_transaction { read_write {} }
  )");
  commit_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::CommitResponse commit_response;
  ZETASQL_EXPECT_OK(Commit(commit_request, &commit_response));
}

TEST_P(TransactionApiTest, CannotCommitSingleUseReadOnlyTransaction) {
  spanner_api::CommitRequest commit_request = PARSE_TEXT_PROTO(R"(
    single_use_transaction { read_only {} }
  )");
  commit_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::CommitResponse commit_response;
  EXPECT_THAT(Commit(commit_request, &commit_response),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_P(TransactionApiTest, CannotCommitSingleUseInvalidTransactionMode) {
  spanner_api::CommitRequest commit_request = PARSE_TEXT_PROTO(R"(
    single_use_transaction {}
  )");
  commit_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::CommitResponse commit_response;
  EXPECT_THAT(Commit(commit_request, &commit_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_P(TransactionApiTest, CannotCommitInvalidTransactionType) {
  spanner_api::CommitRequest commit_request = PARSE_TEXT_PROTO(R"(
  )");
  commit_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::CommitResponse commit_response;
  EXPECT_THAT(Commit(commit_request, &commit_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_P(TransactionApiTest, CommitTransactionIsIdempotent) {
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"(
    options { read_write {} }
  )");
  begin_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  spanner_api::CommitRequest commit_request1;
  commit_request1.set_transaction_id(transaction_response.id());
  commit_request1.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));
  if (GetSessionType() == SessionType::kMultiplexedSession) {
    commit_request1.mutable_precommit_token();
  }

  spanner_api::CommitResponse commit_response1;
  ZETASQL_EXPECT_OK(Commit(commit_request1, &commit_response1));

  spanner_api::CommitRequest commit_request2;
  commit_request2.set_transaction_id(transaction_response.id());
  commit_request2.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));
  if (GetSessionType() == SessionType::kMultiplexedSession) {
    commit_request2.mutable_precommit_token();
  }

  spanner_api::CommitResponse commit_response2;
  ZETASQL_EXPECT_OK(Commit(commit_request2, &commit_response2));

  // Responses should match.
  EXPECT_THAT(commit_response1, test::EqualsProto(commit_response2));
}

TEST_P(TransactionApiTest, CanRollbackAlreadyStartedTransaction) {
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"(
    options { read_write {} }
  )");
  begin_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  spanner_api::RollbackRequest rollback_request;
  rollback_request.set_transaction_id(transaction_response.id());
  rollback_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  ZETASQL_EXPECT_OK(Rollback(rollback_request));
}

TEST_P(TransactionApiTest, CannotRollbackSnapshotTransaction) {
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"(
    options { read_only {} }
  )");
  begin_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  spanner_api::RollbackRequest rollback_request;
  rollback_request.set_transaction_id(transaction_response.id());
  rollback_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  EXPECT_THAT(Rollback(rollback_request),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_P(TransactionApiTest, CannotRollbackAfterCommit) {
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"(
    options { read_write {} }
  )");
  begin_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  spanner_api::CommitRequest commit_request1;
  commit_request1.set_transaction_id(transaction_response.id());
  commit_request1.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));
  if (GetSessionType() == SessionType::kMultiplexedSession) {
    commit_request1.mutable_precommit_token();
  }

  spanner_api::CommitResponse commit_response1;
  ZETASQL_EXPECT_OK(Commit(commit_request1, &commit_response1));

  spanner_api::RollbackRequest rollback_request;
  rollback_request.set_transaction_id(transaction_response.id());
  rollback_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  EXPECT_THAT(Rollback(rollback_request),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_P(TransactionApiTest, CannotCommitAfterRollback) {
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"(
    options { read_write {} }
  )");
  begin_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  spanner_api::RollbackRequest rollback_request;
  rollback_request.set_transaction_id(transaction_response.id());
  rollback_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  ZETASQL_EXPECT_OK(Rollback(rollback_request));

  spanner_api::CommitRequest commit_request1;
  commit_request1.set_transaction_id(transaction_response.id());
  commit_request1.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));
  if (GetSessionType() == SessionType::kMultiplexedSession) {
    commit_request1.mutable_precommit_token();
  }

  spanner_api::CommitResponse commit_response1;
  EXPECT_THAT(Commit(commit_request1, &commit_response1),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_P(TransactionApiTest, CanUseTransactionAfterReadError) {
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"(
    options { read_write {} }
  )");
  begin_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  // Invalid read request.
  spanner_api::ReadRequest read_request = PARSE_TEXT_PROTO(R"(
    table: "non_existent_table"
    columns: "int64_col"
    columns: "string_col"
    key_set { all: true }
  )");
  read_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));
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
  commit_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));
  if (GetSessionType() == SessionType::kMultiplexedSession) {
    commit_request.mutable_precommit_token();
  }

  spanner_api::CommitResponse commit_response;
  ZETASQL_EXPECT_OK(Commit(commit_request, &commit_response));
}

TEST_P(TransactionApiTest,
       CanCreateReadWriteTransactionWithAllIsolationLevels) {
  // Can create a SERIALIZABLE transaction.
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"pb(
    options {
      read_write {}
      isolation_level: SERIALIZABLE
    }
  )pb");
  begin_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  // Can create a REPEATABLE_READ transaction.
  begin_request = PARSE_TEXT_PROTO(R"pb(
    options {
      read_write {}
      isolation_level: REPEATABLE_READ
    }
  )pb");
  begin_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));
}

TEST_P(TransactionApiTest, CanOnlyCreateSerializableReadOnlyTransaction) {
  // Can create a SERIALIZABLE transaction.
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"pb(
    options {
      read_only {}
      isolation_level: SERIALIZABLE
    }
  )pb");
  begin_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  // Can't create a REPEATABLE_READ transaction.
  begin_request = PARSE_TEXT_PROTO(R"pb(
    options {
      read_only {}
      isolation_level: REPEATABLE_READ
    }
  )pb");
  begin_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  EXPECT_THAT(BeginTransaction(begin_request, &transaction_response),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       ::testing::HasSubstr("only allowed on read-write")));
}

TEST_P(TransactionApiTest, CanOnlyCreateSerializablePartitionedDmlTransaction) {
  // Can create a SERIALIZABLE transaction.
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"pb(
    options {
      partitioned_dml {}
      isolation_level: SERIALIZABLE
    }
  )pb");
  begin_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  // Can't create a REPEATABLE_READ transaction.
  begin_request = PARSE_TEXT_PROTO(R"pb(
    options {
      partitioned_dml {}
      isolation_level: REPEATABLE_READ
    }
  )pb");
  begin_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  EXPECT_THAT(BeginTransaction(begin_request, &transaction_response),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       ::testing::HasSubstr("only allowed on read-write")));
}

TEST_P(TransactionApiTest,
       CanCreateReadWriteSerializableTransactionWithReadLockMode) {
  // Can create a SERIALIZABLE transaction.
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"pb(
    options {
      read_write { read_lock_mode: PESSIMISTIC }
      isolation_level: SERIALIZABLE
    }
  )pb");
  begin_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));
}

TEST_P(TransactionApiTest,
       CannotCreateReadWriteRepeatableReadTransactionWithReadLockMode) {
  // Can't create a REPEATABLE_READ transaction with read lock mode.
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"pb(
    options {
      read_write { read_lock_mode: PESSIMISTIC }
      isolation_level: REPEATABLE_READ
    }
  )pb");
  begin_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));

  spanner_api::Transaction transaction_response;
  EXPECT_THAT(BeginTransaction(begin_request, &transaction_response),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       ::testing::HasSubstr("Read lock mode must not be set")));
}

TEST_P(TransactionApiTest, CommitTransactionWithMutations) {
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"pb(
    options { read_write {} }
  )pb");
  begin_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));
  google::spanner::v1::Mutation m;
  google::spanner::v1::Mutation::Write insert = PARSE_TEXT_PROTO(R"pb(
    table: "test_table"
    columns: "int64_col"
    columns: "string_col"
    values {
      values { string_value: "1" }
      values { string_value: "test_string" }
    }
  )pb");
  *m.mutable_insert() = insert;
  // Add mutation key to begin request.
  if (GetSessionType() == SessionType::kMultiplexedSession) {
    *begin_request.mutable_mutation_key() = m;
  }
  spanner_api::Transaction begin_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &begin_response));
  if (GetSessionType() == SessionType::kMultiplexedSession) {
    ASSERT_TRUE(begin_response.has_precommit_token());
  }
  spanner_api::CommitRequest commit_request;

  // Add Mutations to the transaction.
  commit_request.set_session(
      GetSessionUri(GetSessionType() == SessionType::kMultiplexedSession));
  commit_request.set_transaction_id(begin_response.id());
  *commit_request.add_mutations() = m;
  if (GetSessionType() == SessionType::kMultiplexedSession) {
    *commit_request.mutable_precommit_token() =
        begin_response.precommit_token();
  }
  spanner_api::CommitResponse commit_response;
  ZETASQL_ASSERT_OK(Commit(commit_request, &commit_response));
  ASSERT_TRUE(commit_response.has_commit_timestamp());
}

}  // namespace

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
