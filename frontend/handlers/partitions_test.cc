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

#include "google/spanner/v1/mutation.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "google/spanner/v1/transaction.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "common/errors.h"
#include "tests/common/test_env.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {

using ::zetasql_base::testing::StatusIs;

namespace spanner_api = ::google::spanner::v1;

class PartitionApiTest : public test::ServerTest {
 protected:
  void SetUp() override {
    ZETASQL_ASSERT_OK(CreateTestInstance());
    ZETASQL_ASSERT_OK(CreateTestDatabase());
    ZETASQL_ASSERT_OK_AND_ASSIGN(test_session_uri_, CreateTestSession());
  }

  std::string test_session_uri_;
};

TEST_F(PartitionApiTest, RequiredSession) {
  spanner_api::PartitionReadRequest partition_read_request;

  spanner_api::PartitionResponse partition_read_response;
  EXPECT_THAT(PartitionRead(partition_read_request, &partition_read_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionApiTest, RequiredTransaction) {
  spanner_api::PartitionReadRequest partition_read_request;
  partition_read_request.set_session(test_session_uri_);

  spanner_api::PartitionResponse partition_read_response;
  EXPECT_THAT(PartitionRead(partition_read_request, &partition_read_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionApiTest, CannotReadUsingSingleUseTransaction) {
  spanner_api::PartitionReadRequest partition_read_request = PARSE_TEXT_PROTO(
      R"(
        transaction { single_use { read_only {} } }
      )");
  partition_read_request.set_session(test_session_uri_);

  spanner_api::PartitionResponse partition_read_response;
  EXPECT_THAT(PartitionRead(partition_read_request, &partition_read_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionApiTest, CannotReadUsingBeginReadWriteTransaction) {
  // Begin a new read only transaction.
  spanner_api::PartitionReadRequest partition_read_request = PARSE_TEXT_PROTO(
      R"(
        transaction { begin { read_write {} } }
      )");
  partition_read_request.set_session(test_session_uri_);

  spanner_api::PartitionResponse partition_read_response;
  EXPECT_THAT(PartitionRead(partition_read_request, &partition_read_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionApiTest, CannotReadUsingExistingReadWriteTransaction) {
  spanner_api::BeginTransactionRequest txn_request = PARSE_TEXT_PROTO(R"(
    options { read_write {} }
  )");
  txn_request.set_session(test_session_uri_);

  spanner_api::Transaction txn_response;
  ZETASQL_ASSERT_OK(BeginTransaction(txn_request, &txn_response));

  // Perform read using the transaction that was started above.
  spanner_api::TransactionSelector selector;
  selector.set_id(txn_response.id());

  spanner_api::PartitionReadRequest partition_read_request =
      PARSE_TEXT_PROTO(R"(
        table: "test_table"
      )");
  partition_read_request.set_session(test_session_uri_);
  *partition_read_request.mutable_transaction() = selector;

  spanner_api::PartitionResponse partition_read_response;
  EXPECT_THAT(PartitionRead(partition_read_request, &partition_read_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionApiTest, CannotReadUsingInvalidPartitionOptions) {
  // Test that negative partition_size_bytes is not allowed.
  spanner_api::PartitionReadRequest partition_read_request = PARSE_TEXT_PROTO(
      R"(
        transaction { begin { read_only {} } }
        partition_options { partition_size_bytes: -1 max_partitions: 100 }
      )");
  partition_read_request.set_session(test_session_uri_);

  spanner_api::PartitionResponse partition_read_response;
  EXPECT_THAT(PartitionRead(partition_read_request, &partition_read_response),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Test that negative max_partitions is not allowed.
  partition_read_request = PARSE_TEXT_PROTO(
      R"(
        transaction { begin { read_only {} } }
        partition_options { partition_size_bytes: 10000 max_partitions: -1 }
      )");
  partition_read_request.set_session(test_session_uri_);

  EXPECT_THAT(PartitionRead(partition_read_request, &partition_read_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

}  // namespace

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
