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

#include "google/spanner/admin/database/v1/common.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/substitute.h"
#include "tests/common/proto_matchers.h"
#include "tests/conformance/common/database_test_base.h"
#include "grpcpp/client_context.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using googlesql_base::testing::StatusIs;

class PartitionReadsTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

 public:
  absl::Status SetUpDatabase() override {
    return SetSchemaFromFile("partition.test");
  }

 protected:
  // Creates a new session for tests using raw grpc client.
  absl::StatusOr<spanner_api::Session> CreateSession() {
    grpc::ClientContext context;
    spanner_api::CreateSessionRequest request;
    request.set_database(std::string(database()->FullName()));  // NOLINT
    spanner_api::Session response;
    GOOGLESQL_RETURN_IF_ERROR(raw_client()->CreateSession(&context, request, &response));
    return response;
  }

  void PopulateDatabase() {
    // Write fixure data to use in partition reads test.
    GOOGLESQL_EXPECT_OK(CommitDml({SqlStatement(
        "INSERT INTO Users(UserId, Name, Age) Values (1, 'Levin', 27), "
        "(2, 'Mark', 32), (10, 'Douglas', 31)")}));
  }
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectPartitionReadsTest, PartitionReadsTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<PartitionReadsTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

// Tests using raw grpc client to test session and transaction validation.

TEST_P(PartitionReadsTest, CannotReadWithoutSession) {
  spanner_api::PartitionReadRequest partition_read_request;

  spanner_api::PartitionResponse partition_read_response;
  grpc::ClientContext context;
  EXPECT_THAT(raw_client()->PartitionRead(&context, partition_read_request,
                                          &partition_read_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_P(PartitionReadsTest, CannotReadWithoutTransaction) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());

  spanner_api::PartitionReadRequest partition_read_request;
  partition_read_request.set_session(session.name());

  spanner_api::PartitionResponse partition_read_response;
  grpc::ClientContext context;
  EXPECT_THAT(raw_client()->PartitionRead(&context, partition_read_request,
                                          &partition_read_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_P(PartitionReadsTest, CannotReadUsingSingleUseTransaction) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());

  spanner_api::PartitionReadRequest partition_read_request =
      PARSE_TEXT_PROTO(absl::Substitute(
          R"(
            session: "$0"
            transaction { single_use { read_only {} } }
          )",
          session.name()));

  spanner_api::PartitionResponse partition_read_response;
  grpc::ClientContext context;
  EXPECT_THAT(raw_client()->PartitionRead(&context, partition_read_request,
                                          &partition_read_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

// Tests using cpp client library.
TEST_P(PartitionReadsTest, CannotReadUsingBeginReadWriteTransaction) {
  Transaction txn{Transaction::ReadWriteOptions{}};

  // PartitionRead using a begin read-write transaction fails.
  EXPECT_THAT(PartitionRead(txn, "Users", KeySet::All(), {"UserId", "Name"}),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_P(PartitionReadsTest, CannotReadUsingExistingReadWriteTransaction) {
  Transaction txn{Transaction::ReadWriteOptions{}};
  GOOGLESQL_ASSERT_OK(Read(txn, "Users", {"UserId", "Name"}, KeySet::All()));

  // PartitionRead using an already started read-write transaction fails.
  EXPECT_THAT(PartitionRead(txn, "Users", KeySet::All(), {"UserId", "Name"}),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_P(PartitionReadsTest, CannotReadUsingInvalidPartitionOptions) {
  Transaction txn{Transaction::ReadOnlyOptions{}};

  // Test that negative partition_size_bytes is not allowed.
  PartitionOptions partition_options = {.partition_size_bytes = -1,
                                        .max_partitions = 100};
  EXPECT_THAT(PartitionRead(txn, "Users", KeySet::All(), {"UserId", "Name"},
                            /**read_options =*/{}, partition_options),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Test that negative partition_size_bytes is not allowed.
  partition_options = {.partition_size_bytes = 10000, .max_partitions = -1};
  EXPECT_THAT(PartitionRead(txn, "Users", KeySet::All(), {"UserId", "Name"},
                            /**read_options =*/{}, partition_options),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_P(PartitionReadsTest, CanReadUsingPartitionToken) {
  PopulateDatabase();

  Transaction txn{Transaction::ReadOnlyOptions{}};

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::vector<ReadPartition> partitions,
      PartitionRead(txn, "Users", KeySet::All(), {"UserId", "Name"}));

  EXPECT_THAT(
      Read(partitions),
      IsOkAndHoldsUnorderedRows({{1, "Levin"}, {2, "Mark"}, {10, "Douglas"}}));
}

TEST_P(PartitionReadsTest, CanReadRangeUsingPartitionToken) {
  PopulateDatabase();

  Transaction txn{Transaction::ReadOnlyOptions{}};

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::vector<ReadPartition> partitions,
                       PartitionRead(txn, "Users", ClosedClosed(Key(1), Key(2)),
                                     {"UserId", "Name"}));

  EXPECT_THAT(Read(partitions),
              IsOkAndHoldsUnorderedRows({{1, "Levin"}, {2, "Mark"}}));
}

TEST_P(PartitionReadsTest, CanReuseTransactionForPartitionReads) {
  PopulateDatabase();

  Transaction txn{Transaction::ReadOnlyOptions{}};

  {
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(
        std::vector<ReadPartition> partitions,
        PartitionRead(txn, "Users", ClosedClosed(Key(1), Key(2)),
                      {"UserId", "Name"}));
    EXPECT_GE(partitions.size(), 1);
  }

  {
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(
        std::vector<ReadPartition> partitions,
        PartitionRead(txn, "Users", OpenClosed(Key(1), Key(10)),
                      {"UserId", "Name"}));
    EXPECT_GE(partitions.size(), 1);
  }
}

TEST_P(PartitionReadsTest, CannotSetReadLimitWithPartitionToken) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());

  spanner_api::PartitionReadRequest partition_read_request =
      PARSE_TEXT_PROTO(absl::Substitute(
          R"(
            session: "$0"
            transaction { begin { read_only {} } }
            table: "Users"
            columns: "UserId"
            columns: "Name"
            key_set { all: true }
          )",
          session.name()));

  spanner_api::PartitionResponse partition_read_response;
  {
    grpc::ClientContext context;
    GOOGLESQL_ASSERT_OK(raw_client()->PartitionRead(&context, partition_read_request,
                                          &partition_read_response));
  }

  spanner_api::ReadRequest read_request = PARSE_TEXT_PROTO(absl::Substitute(
      R"(
        session: "$0"
        transaction { id: "$1" }
        table: "Users"
        columns: "UserId"
        columns: "Name"
        key_set { all: true }
      )",
      session.name(), partition_read_response.transaction().id()));

  // Validate that Read with limit only succeeds.
  read_request.set_limit(100);
  {
    grpc::ClientContext context;
    spanner_api::ResultSet read_response;
    GOOGLESQL_EXPECT_OK(raw_client()->Read(&context, read_request, &read_response));
  }

  // Validate that Read with partition_token only succeeds.
  read_request.clear_limit();
  *read_request.mutable_partition_token() =
      partition_read_response.partitions()[0].partition_token();
  {
    grpc::ClientContext context;
    spanner_api::ResultSet read_response;
    GOOGLESQL_EXPECT_OK(raw_client()->Read(&context, read_request, &read_response));
  }

  // Validate that limit cannot be passed with partition_token.
  read_request.set_limit(100);
  {
    grpc::ClientContext context;
    spanner_api::ResultSet read_response;
    EXPECT_THAT(raw_client()->Read(&context, read_request, &read_response),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }
}

TEST_P(PartitionReadsTest, CannotReadWithDifferentSession) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());

  spanner_api::PartitionReadRequest partition_read_request =
      PARSE_TEXT_PROTO(absl::Substitute(
          R"(
            session: "$0"
            transaction { begin { read_only {} } }
            table: "Users"
            columns: "UserId"
            columns: "Name"
            key_set { all: true }
          )",
          session.name()));

  spanner_api::PartitionResponse partition_read_response;
  {
    grpc::ClientContext context;
    GOOGLESQL_ASSERT_OK(raw_client()->PartitionRead(&context, partition_read_request,
                                          &partition_read_response));
  }
  ASSERT_GT(partition_read_response.partitions().size(), 0);

  // Validate that a different session cannot be used for read using partition
  // token than the one used for partition read.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto read_session, CreateSession());
  spanner_api::ReadRequest read_request = PARSE_TEXT_PROTO(absl::Substitute(
      R"(
        session: "$0"
        transaction { begin { read_only {} } }
        table: "Users"
        columns: "UserId"
        columns: "Name"
        key_set { all: true }
        partition_token: "$1"
      )",
      read_session.name(),
      partition_read_response.partitions()[0].partition_token()));

  grpc::ClientContext context;
  spanner_api::ResultSet read_response;
  EXPECT_THAT(raw_client()->Read(&context, read_request, &read_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_P(PartitionReadsTest, CannotReadWithDifferentTransaction) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());

  spanner_api::PartitionReadRequest partition_read_request =
      PARSE_TEXT_PROTO(absl::Substitute(
          R"(
            session: "$0"
            transaction { begin { read_only {} } }
            table: "Users"
            columns: "UserId"
            columns: "Name"
            key_set { all: true }
          )",
          session.name()));

  spanner_api::PartitionResponse partition_read_response;
  {
    grpc::ClientContext context;
    GOOGLESQL_ASSERT_OK(raw_client()->PartitionRead(&context, partition_read_request,
                                          &partition_read_response));
  }
  ASSERT_GT(partition_read_response.partitions().size(), 0);

  // Validate that a new/different transaction cannot be used for read using
  // partition token than the one used for partition read.
  spanner_api::ReadRequest read_request = PARSE_TEXT_PROTO(absl::Substitute(
      R"(
        session: "$0"
        transaction { begin { read_only {} } }
        table: "Users"
        columns: "UserId"
        columns: "Name"
        key_set { all: true }
        partition_token: "$1"
      )",
      session.name(),
      partition_read_response.partitions()[0].partition_token()));

  grpc::ClientContext context;
  spanner_api::ResultSet read_response;
  EXPECT_THAT(raw_client()->Read(&context, read_request, &read_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_P(PartitionReadsTest, CannotReadWithDifferentTable) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());

  spanner_api::PartitionReadRequest partition_read_request =
      PARSE_TEXT_PROTO(absl::Substitute(
          R"(
            session: "$0"
            transaction { begin { read_only {} } }
            table: "Users"
            columns: "UserId"
            columns: "Name"
            key_set { all: true }
          )",
          session.name()));

  spanner_api::PartitionResponse partition_read_response;
  {
    grpc::ClientContext context;
    GOOGLESQL_ASSERT_OK(raw_client()->PartitionRead(&context, partition_read_request,
                                          &partition_read_response));
  }
  ASSERT_GT(partition_read_response.partitions().size(), 0);

  // Validate that a different table cannot be read when using partition token.
  spanner_api::ReadRequest read_request = PARSE_TEXT_PROTO(absl::Substitute(
      R"(
        session: "$0"
        transaction { id: "$1" }
        table: "Threads"
        columns: "UserId"
        columns: "Name"
        key_set { all: true }
        partition_token: "$2"
      )",
      session.name(), partition_read_response.transaction().id(),
      partition_read_response.partitions()[0].partition_token()));

  grpc::ClientContext context;
  spanner_api::ResultSet read_response;
  EXPECT_THAT(raw_client()->Read(&context, read_request, &read_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_P(PartitionReadsTest, CannotReadWithDifferentIndex) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());

  spanner_api::PartitionReadRequest partition_read_request =
      PARSE_TEXT_PROTO(absl::Substitute(
          R"(
            session: "$0"
            transaction { begin { read_only {} } }
            table: "Users"
            index: "UsersByNameDescending"
            columns: "UserId"
            columns: "Name"
            key_set { all: true }
          )",
          session.name()));

  spanner_api::PartitionResponse partition_read_response;
  {
    grpc::ClientContext context;
    GOOGLESQL_ASSERT_OK(raw_client()->PartitionRead(&context, partition_read_request,
                                          &partition_read_response));
  }
  ASSERT_GT(partition_read_response.partitions().size(), 0);

  // Validate that a different index cannot be read when using partition token.
  spanner_api::ReadRequest read_request = PARSE_TEXT_PROTO(absl::Substitute(
      R"(
        session: "$0"
        transaction { id: "$1" }
        table: "Users"
        index: "UsersByName"
        columns: "UserId"
        columns: "Name"
        key_set { all: true }
        partition_token: "$2"
      )",
      session.name(), partition_read_response.transaction().id(),
      partition_read_response.partitions()[0].partition_token()));

  grpc::ClientContext context;
  spanner_api::ResultSet read_response;
  EXPECT_THAT(raw_client()->Read(&context, read_request, &read_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_P(PartitionReadsTest, CannotReadWithDifferentKeySet) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());

  spanner_api::PartitionReadRequest partition_read_request =
      PARSE_TEXT_PROTO(absl::Substitute(
          R"(
            session: "$0"
            transaction { begin { read_only {} } }
            table: "Users"
            columns: "UserId"
            columns: "Name"
            key_set { all: true }
          )",
          session.name()));

  spanner_api::PartitionResponse partition_read_response;
  {
    grpc::ClientContext context;
    GOOGLESQL_ASSERT_OK(raw_client()->PartitionRead(&context, partition_read_request,
                                          &partition_read_response));
  }
  ASSERT_GT(partition_read_response.partitions().size(), 0);

  // Validate that a different key_set cannot be read when using partition
  // token.
  spanner_api::ReadRequest read_request = PARSE_TEXT_PROTO(absl::Substitute(
      R"(
        session: "$0"
        transaction { id: "$1" }
        table: "Users"
        columns: "UserId"
        columns: "Name"
        key_set { all: false }
        partition_token: "$2"
      )",
      session.name(), partition_read_response.transaction().id(),
      partition_read_response.partitions()[0].partition_token()));

  grpc::ClientContext context;
  spanner_api::ResultSet read_response;
  EXPECT_THAT(raw_client()->Read(&context, read_request, &read_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_P(PartitionReadsTest, CannotReadWithDifferentColumns) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());

  spanner_api::PartitionReadRequest partition_read_request =
      PARSE_TEXT_PROTO(absl::Substitute(
          R"(
            session: "$0"
            transaction { begin { read_only {} } }
            table: "Users"
            columns: "UserId"
            columns: "Name"
            key_set { all: true }
          )",
          session.name()));

  spanner_api::PartitionResponse partition_read_response;
  {
    grpc::ClientContext context;
    GOOGLESQL_ASSERT_OK(raw_client()->PartitionRead(&context, partition_read_request,
                                          &partition_read_response));
  }
  ASSERT_GT(partition_read_response.partitions().size(), 0);

  // Validate that a different set of columns cannot be read when using
  // partition token.
  spanner_api::ReadRequest read_request = PARSE_TEXT_PROTO(absl::Substitute(
      R"(
        session: "$0"
        transaction { id: "$1" }
        table: "Users"
        columns: "UserId"
        columns: "Age"
        key_set { all: true }
        partition_token: "$2"
      )",
      session.name(), partition_read_response.transaction().id(),
      partition_read_response.partitions()[0].partition_token()));

  grpc::ClientContext context;
  spanner_api::ResultSet read_response;
  EXPECT_THAT(raw_client()->Read(&context, read_request, &read_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
