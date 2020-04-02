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

#include "gmock/gmock.h"
#include "zetasql/base/status.h"
#include "tests/common/proto_matchers.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using zetasql_base::testing::StatusIs;

class PartitionReadsTest : public DatabaseTest {
 public:
  zetasql_base::Status SetUpDatabase() override {
    ZETASQL_RETURN_IF_ERROR(SetSchema({R"(
      CREATE TABLE Users(
        ID   INT64,
        Name STRING(MAX),
        Age  INT64
      ) PRIMARY KEY (ID)
    )"}));
    return zetasql_base::OkStatus();
  }

 protected:
  // Creates a new session for tests using raw grpc client.
  zetasql_base::StatusOr<spanner_api::Session> CreateSession() {
    grpc::ClientContext context;
    spanner_api::CreateSessionRequest request;
    request.set_database(std::string(database()->FullName()));  // NOLINT
    spanner_api::Session response;
    ZETASQL_RETURN_IF_ERROR(raw_client()->CreateSession(&context, request, &response));
    return response;
  }
};

// Tests using raw grpc client to test session and transaction validaton.
TEST_F(PartitionReadsTest, CannotReadWithoutSession) {
  spanner_api::PartitionReadRequest partition_read_request;

  spanner_api::PartitionResponse partition_read_response;
  grpc::ClientContext context;
  EXPECT_THAT(raw_client()->PartitionRead(&context, partition_read_request,
                                          &partition_read_response),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

TEST_F(PartitionReadsTest, CannotReadWithoutTransaction) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());

  spanner_api::PartitionReadRequest partition_read_request;
  partition_read_request.set_session(session.name());

  spanner_api::PartitionResponse partition_read_response;
  grpc::ClientContext context;
  EXPECT_THAT(raw_client()->PartitionRead(&context, partition_read_request,
                                          &partition_read_response),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

TEST_F(PartitionReadsTest, CannotReadUsingSingleUseTransaction) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());

  spanner_api::PartitionReadRequest partition_read_request = PARSE_TEXT_PROTO(
      R"(
        transaction { single_use { read_only {} } }
      )");
  partition_read_request.set_session(session.name());

  spanner_api::PartitionResponse partition_read_response;
  grpc::ClientContext context;
  EXPECT_THAT(raw_client()->PartitionRead(&context, partition_read_request,
                                          &partition_read_response),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

// Tests using cpp client library.
TEST_F(PartitionReadsTest, CannotReadUsingBeginReadWriteTransaction) {
  Transaction txn{Transaction::ReadWriteOptions{}};

  // PartitionRead using a begin read-write transaction fails.
  EXPECT_THAT(PartitionRead(txn, "Users", KeySet::All(), {"ID", "Name"}),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

TEST_F(PartitionReadsTest, CannotReadUsingExistingReadWriteTransaction) {
  Transaction txn{Transaction::ReadWriteOptions{}};
  ZETASQL_ASSERT_OK(Read(txn, "Users", {"ID", "Name"}, KeySet::All()));

  // PartitionRead using an already started read-write transaction fails.
  EXPECT_THAT(PartitionRead(txn, "Users", KeySet::All(), {"ID", "Name"}),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

TEST_F(PartitionReadsTest, CannotReadUsingInvalidPartitionOptions) {
  Transaction txn{Transaction::ReadOnlyOptions{}};

  // Test that negative partition_size_bytes is not allowed.
  PartitionOptions partition_options = {.partition_size_bytes = -1,
                                        .max_partitions = 100};
  EXPECT_THAT(PartitionRead(txn, "Users", KeySet::All(), {"ID", "Name"},
                            /**read_options =*/{}, partition_options),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));

  // Test that negative partition_size_bytes is not allowed.
  partition_options = {.partition_size_bytes = 10000, .max_partitions = -1};
  EXPECT_THAT(PartitionRead(txn, "Users", KeySet::All(), {"ID", "Name"},
                            /**read_options =*/{}, partition_options),
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
