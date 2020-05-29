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

#include "google/protobuf/empty.pb.h"
#include "google/spanner/admin/database/v1/spanner_database_admin.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/container/flat_hash_set.h"
#include "frontend/common/protos.h"
#include "frontend/common/uris.h"
#include "tests/common/test_env.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {

using ::zetasql_base::testing::StatusIs;

namespace protobuf_api = ::google::protobuf;
namespace spanner_api = google::spanner::v1;

class SessionApiTest : public test::ServerTest {
 protected:
  void SetUp() override {
    ZETASQL_EXPECT_OK(CreateTestInstance());
    ZETASQL_EXPECT_OK(CreateTestDatabase());
  }

  spanner_api::Session response_;
  grpc::ClientContext context_;
};

TEST_F(SessionApiTest, CannotReadUsingExpiredTransactions) {
  // Create a test session.
  spanner_api::CreateSessionRequest request;
  request.set_database(test_database_uri_);
  ZETASQL_EXPECT_OK(test_env()->spanner_client()->CreateSession(&context_, request,
                                                        &response_));
  std::string test_sessions_uri = response_.name();
  EXPECT_THAT(test_sessions_uri,
              testing::HasSubstr(
                  "projects/test-project/instances/test-instance/databases/"
                  "test-database/sessions/"));

  // Begin 50 transactions in the same session. Don't use any of the
  // transactions started yet.
  backend::TransactionID id;
  for (int i = 0; i < 50; ++i) {
    spanner_api::BeginTransactionRequest txn_request = PARSE_TEXT_PROTO(R"(
      options { read_only {} }
    )");
    txn_request.set_session(test_sessions_uri);

    spanner_api::Transaction txn_response;
    ZETASQL_ASSERT_OK(BeginTransaction(txn_request, &txn_response));
    id = TransactionIDFromProto(txn_response.id());
  }

  // Build a read request to use transactions created above.
  spanner_api::ReadRequest read_request = PARSE_TEXT_PROTO(R"(
    table: "test_table"
    columns: "int64_col"
    columns: "string_col"
    key_set { all: true }
  )");
  *read_request.mutable_session() = test_sessions_uri;

  // A given session only tracks last 32 created transactions. Any older
  // transactions are deleted from the session object on the server. Thus,
  // attempt to use these older transactions will result in transaction not
  // found errors.
  spanner_api::TransactionSelector selector;
  spanner_api::ResultSet read_response;
  for (int i = 49; i >= 32; --i) {
    *selector.mutable_id() = std::to_string(id - i);
    *read_request.mutable_transaction() = selector;
    EXPECT_THAT(Read(read_request, &read_response),
                StatusIs(absl::StatusCode::kNotFound));
  }
}

TEST_F(SessionApiTest, CanBeginAndUseMultipleTransactionsInSameSession) {
  // Create a test session.
  spanner_api::CreateSessionRequest request;
  request.set_database(test_database_uri_);
  ZETASQL_EXPECT_OK(test_env()->spanner_client()->CreateSession(&context_, request,
                                                        &response_));
  std::string test_sessions_uri = response_.name();
  EXPECT_THAT(test_sessions_uri,
              testing::HasSubstr(
                  "projects/test-project/instances/test-instance/databases/"
                  "test-database/sessions/"));

  // Create 50 transactions in the test session created above, note that though
  // only the last 32 transactions are tracked by a single session. First 18
  // transactions will be invalidated.
  backend::TransactionID id;
  for (int i = 0; i < 50; ++i) {
    spanner_api::BeginTransactionRequest txn_request = PARSE_TEXT_PROTO(R"(
      options { read_only {} }
    )");
    txn_request.set_session(test_sessions_uri);

    spanner_api::Transaction txn_response;
    ZETASQL_ASSERT_OK(BeginTransaction(txn_request, &txn_response));
    id = TransactionIDFromProto(txn_response.id());
  }

  spanner_api::ReadRequest read_request = PARSE_TEXT_PROTO(R"(
    table: "test_table"
    columns: "int64_col"
    key_set {}
  )");
  read_request.set_session(test_sessions_uri);

  // Check that the last 32 created transactions exist in oldest to newest
  // order. There can only be one active transaction at a given time in a
  // given session, thus sequential read using same session for different
  // transactions should succeed.
  spanner_api::TransactionSelector selector;
  spanner_api::ResultSet read_response;
  for (int i = 31; i >= 0; --i) {
    *selector.mutable_id() = std::to_string(id - i);
    *read_request.mutable_transaction() = selector;
    ZETASQL_EXPECT_OK(Read(read_request, &read_response));
    EXPECT_THAT(read_response, test::EqualsProto(
                                   R"(metadata {
                                        row_type {
                                          fields {
                                            name: "int64_col"
                                            type { code: INT64 }
                                          }
                                        }
                                      })"));
  }

  // Trying to read a transaction older than the most recent 32 transactions
  // should return a failed precondition error since min transaction id that
  // can be a valid transaction id for the given session has moved to 50 with
  // the last read performed above.
  *selector.mutable_id() = std::to_string(id - 40);
  *read_request.mutable_transaction() = selector;
  EXPECT_THAT(Read(read_request, &read_response),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

}  // namespace

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
