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
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "tests/common/proto_matchers.h"
#include "tests/common/test_env.h"
#include "grpcpp/client_context.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {
namespace spanner_api = ::google::spanner::v1;
using testing::ElementsAre;
using testing::Eq;
using test::EqualsProto;
using testing::Property;
using test::proto::Partially;

class BatchWriteApiTest : public test::ServerTest {
 protected:
  void SetUp() override {
    ZETASQL_ASSERT_OK(CreateTestInstance());
    ZETASQL_ASSERT_OK(CreateTestDatabase());
    ZETASQL_ASSERT_OK_AND_ASSIGN(test_session_uri_, CreateTestSession());
    ZETASQL_ASSERT_OK(PopulateTestTable());
  }

  absl::Status ClearTestTable() {
    spanner_api::CommitRequest commit_request = PARSE_TEXT_PROTO(R"pb(
      single_use_transaction { read_write {} }
      mutations {
        delete {
          table: "test_table"
          key_set { all: true }
        }
      }
    )pb");
    *commit_request.mutable_session() = test_session_uri_;

    spanner_api::CommitResponse commit_response;
    return Commit(commit_request, &commit_response);
  }

  void TearDown() override { ZETASQL_ASSERT_OK(ClearTestTable()); }

  absl::Status PopulateTestTable() {
    spanner_api::CommitRequest commit_request = PARSE_TEXT_PROTO(R"pb(
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
    )pb");
    *commit_request.mutable_session() = test_session_uri_;

    spanner_api::CommitResponse commit_response;
    return Commit(commit_request, &commit_response);
  }

  std::string test_session_uri_;
};

TEST_F(BatchWriteApiTest, BatchWriteInsert) {
  spanner_api::BatchWriteRequest request;
  request.set_session(test_session_uri_);

  // Add mutation to insert a new row
  spanner_api::Mutation* mutation =
      request.add_mutation_groups()->add_mutations();
  spanner_api::Mutation::Write* insert = mutation->mutable_insert();
  insert->set_table("test_table");
  insert->add_columns("int64_col");
  insert->add_columns("string_col");
  protobuf::ListValue* values = insert->add_values();
  values->add_values()->set_string_value("6");
  values->add_values()->set_string_value("row_6");

  std::vector<spanner_api::BatchWriteResponse> response;
  grpc::ClientContext context;
  ZETASQL_EXPECT_OK(BatchWrite(request, &response));

  EXPECT_THAT(response,
              ElementsAre(AllOf(
                  Partially(EqualsProto(R"pb(
                    indexes: 0
                    commit_timestamp {}
                  )pb")),
                  Property(&spanner_api::BatchWriteResponse::status,
                           Property(&google::rpc::Status::code, Eq(0))))));
}

TEST_F(BatchWriteApiTest, BatchWriteUpdate) {
  spanner_api::BatchWriteRequest request;
  request.set_session(test_session_uri_);

  // Add mutation to update an existing row
  spanner_api::Mutation* mutation =
      request.add_mutation_groups()->add_mutations();
  spanner_api::Mutation::Write* update = mutation->mutable_update();
  update->set_table("test_table");
  update->add_columns("int64_col");
  update->add_columns("string_col");
  protobuf::ListValue* values = update->add_values();
  values->add_values()->set_string_value("1");
  values->add_values()->set_string_value("updated_row_1");

  std::vector<spanner_api::BatchWriteResponse> response;
  grpc::ClientContext context;
  ZETASQL_EXPECT_OK(BatchWrite(request, &response));

  EXPECT_THAT(response,
              ElementsAre(AllOf(
                  Partially(EqualsProto(R"pb(
                    indexes: 0
                    commit_timestamp {}
                  )pb")),
                  Property(&spanner_api::BatchWriteResponse::status,
                           Property(&google::rpc::Status::code, Eq(0))))));
}

TEST_F(BatchWriteApiTest, BatchWriteInsertOrUpdate) {
  spanner_api::BatchWriteRequest request;
  request.set_session(test_session_uri_);

  // Add mutation to insert or update a row
  spanner_api::Mutation* mutation =
      request.add_mutation_groups()->add_mutations();
  spanner_api::Mutation::Write* update = mutation->mutable_insert_or_update();
  update->set_table("test_table");
  update->add_columns("int64_col");
  update->add_columns("string_col");
  protobuf::ListValue* values = update->add_values();
  values->add_values()->set_string_value("1");
  values->add_values()->set_string_value("updated_new_row_1");

  std::vector<spanner_api::BatchWriteResponse> response;
  grpc::ClientContext context;
  ZETASQL_EXPECT_OK(BatchWrite(request, &response));

  EXPECT_THAT(response, ElementsAre(Partially(EqualsProto(R"pb(
                indexes: 0
                status { code: 0 }
                commit_timestamp {}
              )pb"))));
}

TEST_F(BatchWriteApiTest, BatchWriteDelete) {
  spanner_api::BatchWriteRequest request;
  request.set_session(test_session_uri_);

  // Add mutation to delete a row
  spanner_api::Mutation* mutation =
      request.add_mutation_groups()->add_mutations();
  spanner_api::Mutation::Delete* delete_mutation = mutation->mutable_delete_();
  delete_mutation->set_table("test_table");
  // Specify the key to delete (e.g., the row with int64_col = 1)
  protobuf::ListValue* key_values =
      delete_mutation->mutable_key_set()->add_keys();
  key_values->add_values()->set_string_value("1");

  std::vector<spanner_api::BatchWriteResponse> response;
  grpc::ClientContext context;
  ZETASQL_EXPECT_OK(BatchWrite(request, &response));

  EXPECT_THAT(response, ElementsAre(AllOf(Partially(EqualsProto(R"pb(
                indexes: 0
                status { code: 0 }
                commit_timestamp {}
              )pb")))));
  // Optionally, add a read operation to verify the deletion
  spanner_api::ReadRequest read_request;
  read_request.set_session(test_session_uri_);
  read_request.set_table("test_table");
  read_request.add_columns("int64_col");
  read_request.mutable_key_set()->add_keys()->add_values()->set_string_value(
      "1");

  spanner_api::ResultSet read_response;
  grpc::ClientContext read_context;
  absl::Status read_status = Read(read_request, &read_response);
  ZETASQL_EXPECT_OK(read_status);
  EXPECT_EQ(read_response.rows_size(), 0);
}

TEST_F(BatchWriteApiTest, BatchWriteMultipleMutationGroups) {
  spanner_api::BatchWriteRequest request;
  request.set_session(test_session_uri_);

  // Add the first mutation group with an insert mutation
  spanner_api::Mutation* mutation1 =
      request.add_mutation_groups()->add_mutations();
  spanner_api::Mutation::Write* insert = mutation1->mutable_insert();
  insert->set_table("test_table");
  insert->add_columns("int64_col");
  insert->add_columns("string_col");
  protobuf::ListValue* values1 = insert->add_values();
  values1->add_values()->set_string_value("6");
  values1->add_values()->set_string_value("row_6");

  // Add the second mutation group with an update mutation
  spanner_api::Mutation* mutation2 =
      request.add_mutation_groups()->add_mutations();
  spanner_api::Mutation::Write* update = mutation2->mutable_update();
  update->set_table("test_table");
  update->add_columns("int64_col");
  update->add_columns("string_col");
  protobuf::ListValue* values2 = update->add_values();
  values2->add_values()->set_string_value("2");
  values2->add_values()->set_string_value("updated_row_2");

  // Add a third mutation group with an insert_or_update mutation
  spanner_api::Mutation* mutation3 =
      request.add_mutation_groups()->add_mutations();
  spanner_api::Mutation::Write* insert_or_update =
      mutation3->mutable_insert_or_update();
  insert_or_update->set_table("test_table");
  insert_or_update->add_columns("int64_col");
  insert_or_update->add_columns("string_col");
  protobuf::ListValue* values3 = insert_or_update->add_values();
  values3->add_values()->set_string_value("3");
  values3->add_values()->set_string_value("updated_row_3");

  std::vector<spanner_api::BatchWriteResponse> response;
  grpc::ClientContext context;
  ZETASQL_EXPECT_OK(BatchWrite(request, &response));

  EXPECT_THAT(response, UnorderedElementsAre(AllOf(Partially(EqualsProto(R"pb(
                                               indexes: 0
                                               status { code: 0 }
                                               commit_timestamp {}
                                             )pb"))),
                                             AllOf(Partially(EqualsProto(R"pb(
                                               indexes: 1
                                               status { code: 0 }
                                               commit_timestamp {}
                                             )pb"))),
                                             AllOf(Partially(EqualsProto(R"pb(
                                               indexes: 2
                                               status { code: 0 }
                                               commit_timestamp {}
                                             )pb")))));
}

TEST_F(BatchWriteApiTest, TestInsertMultipleMutationGroupsWithFailure) {
  spanner_api::BatchWriteRequest request;
  request.set_session(test_session_uri_);

  // Add a mutation group with an insert mutation
  spanner_api::Mutation* mutation1 =
      request.add_mutation_groups()->add_mutations();
  spanner_api::Mutation::Write* insert1 = mutation1->mutable_insert();
  insert1->set_table("test_table");
  insert1->add_columns("int64_col");
  insert1->add_columns("string_col");
  protobuf::ListValue* values1 = insert1->add_values();
  values1->add_values()->set_string_value("6");
  values1->add_values()->set_string_value("row_6");

  // Add another mutation group with an insert mutation that will fail due to
  // PK violation
  spanner_api::Mutation* mutation2 =
      request.add_mutation_groups()->add_mutations();
  spanner_api::Mutation::Write* insert2 = mutation2->mutable_insert();
  insert2->set_table("test_table");
  insert2->add_columns("int64_col");
  insert2->add_columns("string_col");
  protobuf::ListValue* values2 = insert2->add_values();
  values2->add_values()->set_string_value("1");  // PK violation
  values2->add_values()->set_string_value("row_2");

  spanner_api::Mutation* mutation3 =
      request.add_mutation_groups()->add_mutations();
  spanner_api::Mutation::Write* insert3 = mutation3->mutable_insert();
  insert3->set_table("test_table");
  insert3->add_columns("int64_col");
  insert3->add_columns("string_col");
  protobuf::ListValue* values3 = insert3->add_values();
  values3->add_values()->set_string_value("7");
  values3->add_values()->set_string_value("row_7");

  std::vector<spanner_api::BatchWriteResponse> response;
  grpc::ClientContext context;

  // Call the BatchWrite function
  absl::Status status = BatchWrite(request, &response);

  // Assert that the stream contains an response for each mutation
  // group and one mutation group will fail
  EXPECT_THAT(
      response,
      UnorderedElementsAre(
          AllOf(Partially(EqualsProto(R"pb(
            indexes: 0
            status { code: 0 }
            commit_timestamp {}
          )pb"))),
          AllOf(Partially(EqualsProto(R"pb(
            indexes: 1
            status {
              code: 6
              message: "Table test_table: Row {Int64(1)} already exists."
            }
          )pb"))),
          AllOf(Partially(EqualsProto(R"pb(
            indexes: 2
            status { code: 0 }
            commit_timestamp {}
          )pb")))));

  // First mutation group was committed successfully
  EXPECT_THAT(response[0].status().code(), Eq(0));
  // Check the error message of the second mutation group
  EXPECT_THAT(
      response[1].status().message(),
      testing::HasSubstr("Table test_table: Row {Int64(1)} already exists."));

  // Third mutation group was committed successfully
  EXPECT_THAT(response[2].status().code(), Eq(0));

  // Overall status should be an OK status
  ZETASQL_EXPECT_OK(status);
}

TEST_F(BatchWriteApiTest, TestInsertMutationGroupWithSuccessAndFailure) {
  spanner_api::BatchWriteRequest request;
  request.set_session(test_session_uri_);

  // Add a mutation group with an insert mutation that will succeed
  spanner_api::BatchWriteRequest::MutationGroup* mutation_group1 =
      request.add_mutation_groups();
  spanner_api::Mutation* mutation1 = mutation_group1->add_mutations();
  spanner_api::Mutation::Write* insert1 = mutation1->mutable_insert();
  insert1->set_table("test_table");
  insert1->add_columns("int64_col");
  insert1->add_columns("string_col");
  protobuf::ListValue* values1 = insert1->add_values();
  values1->add_values()->set_string_value("6");
  values1->add_values()->set_string_value("row_6");

  // Add another mutation to the same group that will fail due to PK violation
  spanner_api::Mutation* mutation2 = mutation_group1->add_mutations();
  spanner_api::Mutation::Write* insert2 = mutation2->mutable_insert();
  insert2->set_table("test_table");
  insert2->add_columns("int64_col");
  insert2->add_columns("string_col");
  protobuf::ListValue* values2 = insert2->add_values();
  values2->add_values()->set_string_value("1");  // PK violation
  values2->add_values()->set_string_value("row_2");

  // Add another mutation group with an insert mutation that will succeed
  spanner_api::BatchWriteRequest::MutationGroup* mutation_group2 =
      request.add_mutation_groups();
  spanner_api::Mutation* mutation3 = mutation_group2->add_mutations();
  spanner_api::Mutation::Write* insert3 = mutation3->mutable_insert();
  insert3->set_table("test_table");
  insert3->add_columns("int64_col");
  insert3->add_columns("string_col");
  protobuf::ListValue* values3 = insert3->add_values();
  values3->add_values()->set_string_value("7");
  values3->add_values()->set_string_value("row_7");

  std::vector<spanner_api::BatchWriteResponse> response;
  grpc::ClientContext context;

  // Call the BatchWrite function
  absl::Status status = BatchWrite(request, &response);

  // Assert that the stream contains two responses for the mutation groups
  // and that the first group failed and the second group succeeded
  EXPECT_THAT(
      response,
      UnorderedElementsAre(
          AllOf(Partially(EqualsProto(R"pb(
            indexes: 0
            status {
              code: 6
              message: "Table test_table: Row {Int64(1)} already exists."
            }
          )pb"))),
          AllOf(Partially(EqualsProto(R"pb(
            indexes: 1
            status { code: 0 }
            commit_timestamp {}
          )pb")))));
  // Check the error message of the first mutation group
  EXPECT_THAT(
      response[0].status().message(),
      testing::HasSubstr("Table test_table: Row {Int64(1)} already exists."));

  // Second mutation group was committed successfully
  EXPECT_THAT(response[1].status().code(), Eq(0));

  // Overall status should be an OK status
  ZETASQL_EXPECT_OK(status);
}

TEST_F(BatchWriteApiTest, ConcurrentTransactions) {
  spanner_api::BeginTransactionRequest begin_request = PARSE_TEXT_PROTO(R"pb(
    options { read_write {} }
  )pb");
  begin_request.set_session(test_session_uri_);

  // 1. Start a normal read/write transaction and execute a DML statement.
  spanner_api::Transaction transaction_response;
  ZETASQL_EXPECT_OK(BeginTransaction(begin_request, &transaction_response));

  spanner_api::ExecuteSqlRequest request = PARSE_TEXT_PROTO(
      R"""(
        sql: "INSERT INTO test_table (int64_col, string_col) "
             "VALUES (10, 'row_10') THEN RETURN int64_col, string_col"
      )""");
  request.set_session(test_session_uri_);
  request.mutable_transaction()->set_id(transaction_response.id());

  spanner_api::ResultSet response;
  ZETASQL_ASSERT_OK(ExecuteSql(request, &response));

  // 2. Execute a BatchWrite with a couple of mutation groups.
  spanner_api::BatchWriteRequest batch_write_request;
  batch_write_request.set_session(test_session_uri_);
  // Add a couple of mutation groups
  auto* mutation_group1 = batch_write_request.add_mutation_groups();
  spanner_api::Mutation* mutation1 = mutation_group1->add_mutations();
  spanner_api::Mutation::Write* insert1 = mutation1->mutable_insert();
  insert1->set_table("test_table");
  insert1->add_columns("int64_col");
  insert1->add_columns("string_col");
  protobuf::ListValue* values1 = insert1->add_values();
  values1->add_values()->set_string_value("6");
  values1->add_values()->set_string_value("row_6");

  auto* mutation_group2 = batch_write_request.add_mutation_groups();
  spanner_api::Mutation* mutation2 = mutation_group2->add_mutations();
  spanner_api::Mutation::Write* insert2 = mutation2->mutable_insert();
  insert2->set_table("test_table");
  insert2->add_columns("int64_col");
  insert2->add_columns("string_col");
  protobuf::ListValue* values2 = insert2->add_values();
  values2->add_values()->set_string_value("7");
  values2->add_values()->set_string_value("row_7");

  // Simulate a BatchWrite call
  std::vector<spanner_api::BatchWriteResponse> batch_write_responses;
  grpc::ClientContext batch_write_context;
  ZETASQL_ASSERT_OK(BatchWrite(batch_write_request, &batch_write_responses));

  // 3. Then try to commit the first transaction.
  spanner_api::CommitRequest commit_request;
  commit_request.set_session(test_session_uri_);
  commit_request.mutable_transaction_id()->assign(transaction_response.id());
  spanner_api::CommitResponse commit_response;
  absl::Status rw_commit_status = Commit(commit_request, &commit_response);

  // Assertions
  // Check that either all mutation groups failed with Aborted OR
  // that some mutation groups started committing successfully and the
  // first RW transaction failed with Aborted.
  EXPECT_EQ(batch_write_responses.size(), 2);
  int num_committed_mutation_groups = 0;
  for (const auto& response : batch_write_responses) {
    // During transaction write when we check for lock contention, sometimes
    // current transaction can be aborted for another new transaction which
    // can lead to flaky tests.
    // So only check aborted status if error code is not 0.
    std::string status_message = response.status().message();
    absl::AsciiStrToLower(&status_message);
    if (response.status().code() != 0) {
      ASSERT_THAT(status_message, testing::HasSubstr("aborted"));
      EXPECT_FALSE(response.has_commit_timestamp());
    } else {
      ASSERT_THAT(status_message, testing::IsEmpty());
      EXPECT_TRUE(response.has_commit_timestamp());
      num_committed_mutation_groups++;
    }
  }
  if (num_committed_mutation_groups > 0) {
    EXPECT_THAT(rw_commit_status,
                zetasql_base::testing::StatusIs(absl::StatusCode::kAborted));
  } else {
    ZETASQL_EXPECT_OK(rw_commit_status);
  }
  // Construct a new BatchWrite request with a single mutation group
  spanner_api::BatchWriteRequest new_batch_write_request;
  new_batch_write_request.set_session(test_session_uri_);
  auto* mutation_group = new_batch_write_request.add_mutation_groups();
  spanner_api::Mutation* mutation = mutation_group->add_mutations();
  spanner_api::Mutation::Write* insert = mutation->mutable_insert();
  insert->set_table("test_table");
  insert->add_columns("int64_col");
  insert->add_columns("string_col");
  protobuf::ListValue* values = insert->add_values();
  values->add_values()->set_string_value("8");
  values->add_values()->set_string_value("row_8");

  // Simulate a new BatchWrite call after the first transaction was committed
  std::vector<spanner_api::BatchWriteResponse> new_batch_write_response;
  grpc::ClientContext new_batch_write_context;
  ZETASQL_ASSERT_OK(BatchWrite(new_batch_write_request, &new_batch_write_response));

  // Check that all mutation groups completed successfully
  for (const auto& response : new_batch_write_response) {
    EXPECT_TRUE(response.has_commit_timestamp());
    ASSERT_THAT(response.status().message(), testing::IsEmpty());
    EXPECT_THAT(response.status().code(), Eq(0));
  }
}

}  // namespace
}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
