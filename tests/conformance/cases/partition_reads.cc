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
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "absl/strings/substitute.h"
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
  absl::Status SetUpDatabase() override {
    ZETASQL_RETURN_IF_ERROR(SetSchema({
        R"(
          CREATE TABLE Users(
            UserId   INT64 NOT NULL,
            Name STRING(MAX),
            Age  INT64
          ) PRIMARY KEY (UserId)
        )",
        "CREATE INDEX UsersByName ON Users(Name)",
        "CREATE INDEX UsersByNameDescending ON Users(Name DESC)",
        R"(
          CREATE TABLE Threads (
            UserId     INT64 NOT NULL,
            ThreadId   INT64 NOT NULL,
            Starred    BOOL
          ) PRIMARY KEY (UserId, ThreadId),
          INTERLEAVE IN PARENT Users ON DELETE CASCADE
        )"}));
    return absl::OkStatus();
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

  void PopulateDatabase() {
    // Write fixure data to use in partition reads test.
    ZETASQL_EXPECT_OK(CommitDml({SqlStatement(
        "INSERT Users(UserId, Name, Age) Values (1, 'Levin', 27), "
        "(2, 'Mark', 32), (10, 'Douglas', 31)")}));
  }
};

// Tests using raw grpc client to test session and transaction validaton.
TEST_F(PartitionReadsTest, CannotReadWithoutSession) {
  spanner_api::PartitionReadRequest partition_read_request;

  spanner_api::PartitionResponse partition_read_response;
  grpc::ClientContext context;
  EXPECT_THAT(raw_client()->PartitionRead(&context, partition_read_request,
                                          &partition_read_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionReadsTest, CannotReadWithoutTransaction) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());

  spanner_api::PartitionReadRequest partition_read_request;
  partition_read_request.set_session(session.name());

  spanner_api::PartitionResponse partition_read_response;
  grpc::ClientContext context;
  EXPECT_THAT(raw_client()->PartitionRead(&context, partition_read_request,
                                          &partition_read_response),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionReadsTest, CannotReadUsingSingleUseTransaction) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());

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
TEST_F(PartitionReadsTest, CannotReadUsingBeginReadWriteTransaction) {
  Transaction txn{Transaction::ReadWriteOptions{}};

  // PartitionRead using a begin read-write transaction fails.
  EXPECT_THAT(PartitionRead(txn, "Users", KeySet::All(), {"UserId", "Name"}),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionReadsTest, CannotReadUsingExistingReadWriteTransaction) {
  Transaction txn{Transaction::ReadWriteOptions{}};
  ZETASQL_ASSERT_OK(Read(txn, "Users", {"UserId", "Name"}, KeySet::All()));

  // PartitionRead using an already started read-write transaction fails.
  EXPECT_THAT(PartitionRead(txn, "Users", KeySet::All(), {"UserId", "Name"}),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(PartitionReadsTest, CannotReadUsingInvalidPartitionOptions) {
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

TEST_F(PartitionReadsTest, CanReadUsingPartitionToken) {
  PopulateDatabase();

  Transaction txn{Transaction::ReadOnlyOptions{}};

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::vector<ReadPartition> partitions,
      PartitionRead(txn, "Users", KeySet::All(), {"UserId", "Name"}));

  EXPECT_THAT(
      Read(partitions),
      IsOkAndHoldsUnorderedRows({{1, "Levin"}, {2, "Mark"}, {10, "Douglas"}}));
}

TEST_F(PartitionReadsTest, CanReadRangeUsingPartitionToken) {
  PopulateDatabase();

  Transaction txn{Transaction::ReadOnlyOptions{}};

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<ReadPartition> partitions,
                       PartitionRead(txn, "Users", ClosedClosed(Key(1), Key(2)),
                                     {"UserId", "Name"}));

  EXPECT_THAT(Read(partitions),
              IsOkAndHoldsUnorderedRows({{1, "Levin"}, {2, "Mark"}}));
}

TEST_F(PartitionReadsTest, CanReuseTransactionForPartitionReads) {
  PopulateDatabase();

  Transaction txn{Transaction::ReadOnlyOptions{}};

  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        std::vector<ReadPartition> partitions,
        PartitionRead(txn, "Users", ClosedClosed(Key(1), Key(2)),
                      {"UserId", "Name"}));
    EXPECT_GE(partitions.size(), 1);
  }

  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        std::vector<ReadPartition> partitions,
        PartitionRead(txn, "Users", OpenClosed(Key(1), Key(10)),
                      {"UserId", "Name"}));
    EXPECT_GE(partitions.size(), 1);
  }
}

TEST_F(PartitionReadsTest, CannotSetReadLimitWithPartitionToken) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());

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
    ZETASQL_ASSERT_OK(raw_client()->PartitionRead(&context, partition_read_request,
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
    ZETASQL_EXPECT_OK(raw_client()->Read(&context, read_request, &read_response));
  }

  // Validate that Read with partition_token only succeeds.
  read_request.clear_limit();
  *read_request.mutable_partition_token() =
      partition_read_response.partitions()[0].partition_token();
  {
    grpc::ClientContext context;
    spanner_api::ResultSet read_response;
    ZETASQL_EXPECT_OK(raw_client()->Read(&context, read_request, &read_response));
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

TEST_F(PartitionReadsTest, CannotReadWithDifferentSession) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());

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
    ZETASQL_ASSERT_OK(raw_client()->PartitionRead(&context, partition_read_request,
                                          &partition_read_response));
  }
  ASSERT_GT(partition_read_response.partitions().size(), 0);

  // Validate that a different session cannot be used for read using partition
  // token than the one used for partition read.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto read_session, CreateSession());
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

TEST_F(PartitionReadsTest, CannotReadWithDifferentTransaction) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());

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
    ZETASQL_ASSERT_OK(raw_client()->PartitionRead(&context, partition_read_request,
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

TEST_F(PartitionReadsTest, CannotReadWithDifferentTable) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());

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
    ZETASQL_ASSERT_OK(raw_client()->PartitionRead(&context, partition_read_request,
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

TEST_F(PartitionReadsTest, CannotReadWithDifferentIndex) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());

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
    ZETASQL_ASSERT_OK(raw_client()->PartitionRead(&context, partition_read_request,
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

TEST_F(PartitionReadsTest, CannotReadWithDifferentKeySet) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());

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
    ZETASQL_ASSERT_OK(raw_client()->PartitionRead(&context, partition_read_request,
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

TEST_F(PartitionReadsTest, CannotReadWithDifferentColumns) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto session, CreateSession());

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
    ZETASQL_ASSERT_OK(raw_client()->PartitionRead(&context, partition_read_request,
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
