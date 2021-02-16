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
#include "tests/conformance/common/database_test_base.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using zetasql_base::testing::StatusIs;

class OperationsTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    return SetSchema({R"(
      CREATE TABLE T(
        k1 INT64
      ) PRIMARY KEY(k1)
    )"});
  }

  // Updates the schema (using a specified `operation_id`, if provided)
  // and populates `*op` with the long-running operation that can be used to
  // check the status of the schema update.
  zetasql_base::StatusOr<UpdateDatabaseDdlMetadata> UpdateSchemaOp(
      const std::vector<std::string>& schema,
      const std::string& operation_id = "",
      operations_api::Operation* op = nullptr) {
    grpc::ClientContext context;
    database_api::UpdateDatabaseDdlRequest request;
    request.set_database(database()->FullName());
    for (const auto& statement : schema) {
      request.add_statements(statement);
    }
    if (!operation_id.empty()) {
      *request.mutable_operation_id() = operation_id;
    }
    operations_api::Operation operation;
    ZETASQL_RETURN_IF_ERROR(raw_database_client()->UpdateDatabaseDdl(&context, request,
                                                             &operation));
    ZETASQL_RETURN_IF_ERROR(WaitForOperation(operation.name(), &operation));
    UpdateDatabaseDdlMetadata metadata;
    ZETASQL_RET_CHECK(operation.metadata().UnpackTo(&metadata));
    google::rpc::Status status = operation.error();
    auto status_code = static_cast<absl::StatusCode>(status.code());
    if (status_code != absl::StatusCode::kOk) {
      return absl::Status(status_code, status.message());
    }
    if (op) {
      op->Swap(&operation);
    }
    return metadata;
  }

  // Waits for the long-running operation identified by `operation_uri` to
  // finish.
  absl::Status WaitForOperation(absl::string_view operation_uri,
                                operations_api::Operation* op) {
    absl::Duration deadline = absl::Seconds(25);
    absl::Time start = absl::Now();
    while (true) {
      ZETASQL_RETURN_IF_ERROR(GetOperation(operation_uri, op));
      if (op->done()) return absl::OkStatus();
      if (absl::Now() - start > deadline) {
        return absl::Status(absl::StatusCode::kDeadlineExceeded,
                            "Exceeded deadline while waiting for operation " +
                                op->name() + " to complete.");
      }
      absl::SleepFor(absl::Milliseconds(1));
    }
  }

  // Returns the long-operation identified by `operation_uri` in `op`.
  absl::Status GetOperation(absl::string_view operation_uri,
                            operations_api::Operation* op) {
    longrunning::GetOperationRequest request;
    request.set_name(std::string(operation_uri));  // NOLINT
    grpc::ClientContext context;
    return raw_operations_client()->GetOperation(&context, request, op);
  }

  // Lists all long-running operations whose status may be currently maintained
  // by the database.
  zetasql_base::StatusOr<std::vector<operations_api::Operation>>
  ListDatabaseOperations() {
    std::vector<operations_api::Operation> operations;
    operations_api::ListOperationsRequest request;
    request.set_name(absl::StrCat(database()->FullName(), "/operations"));
    // Get all operations.
    request.set_page_size(-1);
    operations_api::ListOperationsResponse reply;
    grpc::ClientContext context;
    ZETASQL_RETURN_IF_ERROR(
        raw_operations_client()->ListOperations(&context, request, &reply));
    for (const auto& op : reply.operations()) {
      operations.emplace_back(op);
    }
    return operations;
  }
};

TEST_F(OperationsTest, LongRunningOperationIds) {
  operations_api::Operation op;
  ZETASQL_EXPECT_OK(UpdateSchemaOp({"ALTER TABLE T ADD COLUMN c1 INT64"},
                           /*operation_id=*/"a_abc123", &op));
  EXPECT_THAT(op.name(), testing::EndsWith("a_abc123"));

  // Uppercase operation IDs are not allowed.
  EXPECT_THAT(UpdateSchemaOp({"ALTER TABLE T ADD COLUMN c2 INT64"},
                             /*operation_id=*/"a_A"),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Hyphens are not allowed.
  EXPECT_THAT(UpdateSchemaOp({"ALTER TABLE T ADD COLUMN c2 INT64"},
                             /*operation_id=*/"a-123"),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Operation IDs must be a minumum of 2 characters in length.
  EXPECT_THAT(UpdateSchemaOp({"ALTER TABLE T ADD COLUMN c2 INT64"},
                             /*operation_id=*/"a"),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Operation IDs must be a maximum of 128 characters in length.
  EXPECT_THAT(UpdateSchemaOp({"ALTER TABLE T ADD COLUMN c2 INT64"},
                             /*operation_id=*/std::string(150, 'a')),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(OperationsTest, ListOperations) {
  ZETASQL_EXPECT_OK(UpdateSchemaOp({"ALTER TABLE T ADD COLUMN c1 INT64"},
                           /*operation_id=*/"o1"));

  ZETASQL_EXPECT_OK(UpdateSchemaOp({"ALTER TABLE T ADD COLUMN c2 INT64"},
                           /*operation_id=*/"o2"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto operations, ListDatabaseOperations());
  std::vector<std::string> op_names;
  for (const auto& operation : operations) {
    EXPECT_TRUE(operation.done());
    op_names.push_back(operation.name());
  }
  EXPECT_THAT(op_names, testing::Contains(testing::EndsWith("o1")));
  EXPECT_THAT(op_names, testing::Contains(testing::EndsWith("o2")));
}

TEST_F(OperationsTest, GetOperation) {
  ZETASQL_EXPECT_OK(UpdateSchemaOp({"ALTER TABLE T ADD COLUMN c1 INT64"},
                           /*operation_id=*/"a_abc123"));

  operations_api::Operation op;
  ZETASQL_EXPECT_OK(GetOperation(
      absl::StrCat(database()->FullName(), "/operations/a_abc123"), &op));
  EXPECT_THAT(op.name(), testing::EndsWith("a_abc123"));
}

TEST_F(OperationsTest, NonExistentDatabaseOperation) {
  operations_api::Operation op;
  EXPECT_THAT(GetOperation(absl::StrCat(database()->FullName(),
                                        "/operations/non_existent"),
                           &op),
              StatusIs(absl::StatusCode::kNotFound));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
