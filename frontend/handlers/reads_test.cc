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

#include "frontend/converters/reads.h"

#include <string>

#include "google/spanner/v1/result_set.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "google/spanner/v1/transaction.pb.h"
#include "grpcpp/server_context.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "frontend/common/protos.h"
#include "frontend/common/uris.h"
#include "frontend/entities/database.h"
#include "frontend/entities/session.h"
#include "frontend/entities/transaction.h"
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

class ReadApiTest : public test::ServerTest {
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

TEST_F(ReadApiTest, CannotReadBeyondVersionGCLimit) {
  // Cloud Spanner does not allow read only transactions with a staleness > 1h.
  spanner_api::ReadRequest read_request = PARSE_TEXT_PROTO(
      R"(
        transaction {
          single_use { read_only { exact_staleness { seconds: 3601 } } }
        }
        table: "test_table"
        columns: "int64_col"
        key_set { keys { values { string_value: "1" } } }
      )");
  read_request.set_session(test_session_uri_);

  spanner_api::ResultSet read_response;
  EXPECT_THAT(Read(read_request, &read_response),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(ReadApiTest, CanReadUsingAnAlreadyStartedTransaction) {
  // Begin a new read only transaction.
  spanner_api::BeginTransactionRequest txn_request = PARSE_TEXT_PROTO(R"(
    options { read_only {} }
  )");
  txn_request.set_session(test_session_uri_);

  spanner_api::Transaction txn_response;
  ZETASQL_ASSERT_OK(BeginTransaction(txn_request, &txn_response));

  // Perform read using the transaction that was started above.
  spanner_api::TransactionSelector selector;
  selector.set_id(txn_response.id());

  spanner_api::ReadRequest read_request = PARSE_TEXT_PROTO(R"(
    table: "test_table"
    columns: "int64_col"
    columns: "string_col"
    key_set {
      keys { values { string_value: "1" } }
      keys { values { string_value: "2" } }
    }
  )");
  read_request.set_session(test_session_uri_);
  *read_request.mutable_transaction() = selector;

  // Read.
  spanner_api::ResultSet read_response;
  ZETASQL_EXPECT_OK(Read(read_request, &read_response));
  EXPECT_THAT(read_response, test::EqualsProto(
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
                                    rows {
                                      values { string_value: "1" }
                                      values { string_value: "row_1" }
                                    }
                                    rows {
                                      values { string_value: "2" }
                                      values { string_value: "row_2" }
                                    })"));

  // StreamingRead
  std::vector<spanner_api::PartialResultSet> streaming_read_response;
  ZETASQL_EXPECT_OK(StreamingRead(read_request, &streaming_read_response));
  EXPECT_THAT(streaming_read_response,
              testing::ElementsAre(test::EqualsProto(
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
                     chunked_value: false
                  )")));
}

TEST_F(ReadApiTest, CanPerformStrongReadUsingSingleUseTransaction) {
  // Perform a strong read using a read only single use transaction which will
  // be created on the fly on the server.
  // Read using a close-open range, only keys 1 and 2 will be read.
  spanner_api::ReadRequest read_request = PARSE_TEXT_PROTO(R"(
    transaction { single_use { read_only { strong: true } } }
    table: "test_table"
    columns: "int64_col"
    columns: "string_col"
    key_set {
      ranges {
        start_closed { values { string_value: "1" } }
        end_open { values { string_value: "3" } }
      }
    }
  )");
  read_request.set_session(test_session_uri_);

  spanner_api::ResultSet read_response;
  ZETASQL_EXPECT_OK(Read(read_request, &read_response));
  EXPECT_THAT(read_response, test::EqualsProto(
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
                                    rows {
                                      values { string_value: "1" }
                                      values { string_value: "row_1" }
                                    }
                                    rows {
                                      values { string_value: "2" }
                                      values { string_value: "row_2" }
                                    })"));
}

TEST_F(ReadApiTest, CanPerformDefaultStrongReadUsingTemporaryTransaction) {
  // Perform a strong read using a read only single use transaction when
  // transaction selector is not set.
  // Read using a open-open range, only key 2 will be read.
  spanner_api::ReadRequest read_request = PARSE_TEXT_PROTO(R"(
    transaction {}
    table: "test_table"
    columns: "int64_col"
    columns: "string_col"
    key_set {
      ranges {
        start_open { values { string_value: "1" } }
        end_open { values { string_value: "3" } }
      }
    }
  )");
  read_request.set_session(test_session_uri_);

  spanner_api::ResultSet read_response;
  ZETASQL_EXPECT_OK(Read(read_request, &read_response));
  EXPECT_THAT(read_response, test::EqualsProto(
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
                                    rows {
                                      values { string_value: "2" }
                                      values { string_value: "row_2" }
                                    })"));
}

TEST_F(ReadApiTest, CanBeginNewReadWriteTransactionAndPerformRead) {
  // A new read write transaction will be started by the server, which will
  // also be used to perform the read. The transaction returned may be re-used
  // for further read and/or writes until it's aborted or completed.
  // Read using an open-open range, only key 2 will be read.
  spanner_api::ReadRequest read_request = PARSE_TEXT_PROTO(R"(
    transaction { begin { read_write {} } }
    table: "test_table"
    columns: "int64_col"
    columns: "string_col"
    key_set {
      ranges {
        start_open { values { string_value: "1" } }
        end_open { values { string_value: "3" } }
      }
    }
  )");
  read_request.set_session(test_session_uri_);

  spanner_api::ResultSet read_response;
  ZETASQL_EXPECT_OK(Read(read_request, &read_response));
  EXPECT_THAT(read_response, test::EqualsProto(
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
                                      transaction { id: "2" }
                                    }
                                    rows {
                                      values { string_value: "2" }
                                      values { string_value: "row_2" }
                                    })"));
}

TEST_F(ReadApiTest, CannotReadUsingPartitionedDMLTransaction) {
  // Partitioned DML transactions are not supported.
  spanner_api::ReadRequest read_request = PARSE_TEXT_PROTO(R"(
    transaction { begin { partitioned_dml {} } }
    table: "test_table"
    columns: "int64_col"
    columns: "string_col"
    key_set { all: true }
  )");
  read_request.set_session(test_session_uri_);

  spanner_api::ResultSet read_response;
  EXPECT_THAT(Read(read_request, &read_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(ReadApiTest, CannotReadUsingInvalidTransaction) {
  spanner_api::BeginTransactionRequest txn_request = PARSE_TEXT_PROTO(R"(
    options { read_only {} }
  )");
  txn_request.set_session(test_session_uri_);

  spanner_api::Transaction txn_response;
  ZETASQL_EXPECT_OK(BeginTransaction(txn_request, &txn_response));

  backend::TransactionID id = TransactionIDFromProto(txn_response.id());
  spanner_api::TransactionSelector selector;
  // Transaction id+1 doesn't exist already on server and is thus invalid.
  selector.set_id(std::to_string(id + 1));

  spanner_api::ReadRequest read_request = PARSE_TEXT_PROTO(R"(
    table: "test_table"
    columns: "int64_col"
    columns: "string_col"
    key_set { all: true }
  )");
  read_request.set_session(test_session_uri_);
  *read_request.mutable_transaction() = selector;

  spanner_api::ResultSet read_response;
  EXPECT_THAT(Read(read_request, &read_response),
              StatusIs(absl::StatusCode::kNotFound));
}

}  // namespace

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
