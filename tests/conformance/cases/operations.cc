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
#include "tests/conformance/common/database_test_base.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using googlesql_base::testing::StatusIs;

class OperationsTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

  absl::Status SetUpDatabase() override {
    return SetSchemaFromFile("operations.test");
  }

  // Updates the schema (using a specified `operation_id`, if provided)
  // and populates `*op` with the long-running operation that can be used to
  // check the status of the schema update.
  absl::StatusOr<UpdateDatabaseDdlMetadata> UpdateSchemaOp(
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
    GOOGLESQL_RETURN_IF_ERROR(raw_database_client()->UpdateDatabaseDdl(&context, request,
                                                             &operation));
    GOOGLESQL_RETURN_IF_ERROR(WaitForOperation(operation.name(), &operation));
    UpdateDatabaseDdlMetadata metadata;
    GOOGLESQL_RET_CHECK(operation.metadata().UnpackTo(&metadata));
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

  // Lists all long-running operations whose status may be currently maintained
  // by the database.
  absl::StatusOr<std::vector<operations_api::Operation>>
  ListDatabaseOperations() {
    std::vector<operations_api::Operation> operations;
    operations_api::ListOperationsRequest request;
    request.set_name(absl::StrCat(database()->FullName(), "/operations"));
    // Get all operations.
    request.set_page_size(-1);
    operations_api::ListOperationsResponse reply;
    grpc::ClientContext context;
    GOOGLESQL_RETURN_IF_ERROR(
        raw_operations_client()->ListOperations(&context, request, &reply));
    for (const auto& op : reply.operations()) {
      operations.emplace_back(op);
    }
    return operations;
  }
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectOperationsTest, OperationsTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<OperationsTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(OperationsTest, LongRunningOperationIds) {
  operations_api::Operation op;
  if (dialect_ == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    GOOGLESQL_EXPECT_OK(UpdateSchemaOp({"ALTER TABLE T ADD COLUMN c1 INT64"},
                             /*operation_id=*/"a_abc123", &op));
  } else {
    GOOGLESQL_EXPECT_OK(UpdateSchemaOp({"ALTER TABLE T ADD COLUMN c1 bigint"},
                             /*operation_id=*/"a_abc123", &op));
  }
  EXPECT_THAT(op.name(), testing::EndsWith("a_abc123"));

  // Uppercase operation IDs are not allowed.
  if (dialect_ == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    EXPECT_THAT(UpdateSchemaOp({"ALTER TABLE T ADD COLUMN c2 INT64"},
                               /*operation_id=*/"a_A"),
                StatusIs(absl::StatusCode::kInvalidArgument));
  } else {
    EXPECT_THAT(UpdateSchemaOp({"ALTER TABLE T ADD COLUMN c2 bigint"},
                               /*operation_id=*/"a_A"),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }

  // Hyphens are not allowed.
  if (dialect_ == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    EXPECT_THAT(UpdateSchemaOp({"ALTER TABLE T ADD COLUMN c2 INT64"},
                               /*operation_id=*/"a-123"),
                StatusIs(absl::StatusCode::kInvalidArgument));
  } else {
    EXPECT_THAT(UpdateSchemaOp({"ALTER TABLE T ADD COLUMN c2 bigint"},
                               /*operation_id=*/"a-123"),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }

  // Operation IDs must be a minumum of 2 characters in length.
  if (dialect_ == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    EXPECT_THAT(UpdateSchemaOp({"ALTER TABLE T ADD COLUMN c2 INT64"},
                               /*operation_id=*/"a"),
                StatusIs(absl::StatusCode::kInvalidArgument));
  } else {
    EXPECT_THAT(UpdateSchemaOp({"ALTER TABLE T ADD COLUMN c2 bigint"},
                               /*operation_id=*/"a"),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }

  // Operation IDs must be a maximum of 128 characters in length.
  if (dialect_ == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    EXPECT_THAT(UpdateSchemaOp({"ALTER TABLE T ADD COLUMN c2 INT64"},
                               /*operation_id=*/std::string(150, 'a')),
                StatusIs(absl::StatusCode::kInvalidArgument));
  } else {
    EXPECT_THAT(UpdateSchemaOp({"ALTER TABLE T ADD COLUMN c2 bigint"},
                               /*operation_id=*/std::string(150, 'a')),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }
}

TEST_P(OperationsTest, ListOperations) {
  if (dialect_ == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    GOOGLESQL_EXPECT_OK(UpdateSchemaOp({"ALTER TABLE T ADD COLUMN c1 INT64"},
                             /*operation_id=*/"o1"));
    GOOGLESQL_EXPECT_OK(UpdateSchemaOp({"ALTER TABLE T ADD COLUMN c2 INT64"},
                             /*operation_id=*/"o2"));
  } else {
    GOOGLESQL_EXPECT_OK(UpdateSchemaOp({"ALTER TABLE T ADD COLUMN c1 bigint"},
                             /*operation_id=*/"o1"));
    GOOGLESQL_EXPECT_OK(UpdateSchemaOp({"ALTER TABLE T ADD COLUMN c2 bigint"},
                             /*operation_id=*/"o2"));
  }

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto operations, ListDatabaseOperations());
  std::vector<std::string> op_names;
  for (const auto& operation : operations) {
    EXPECT_TRUE(operation.done());
    op_names.push_back(operation.name());
  }
  EXPECT_THAT(op_names, testing::Contains(testing::EndsWith("o1")));
  EXPECT_THAT(op_names, testing::Contains(testing::EndsWith("o2")));
}

TEST_P(OperationsTest, GetOperation) {
  if (dialect_ == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    GOOGLESQL_EXPECT_OK(UpdateSchemaOp({"ALTER TABLE T ADD COLUMN c1 INT64"},
                             /*operation_id=*/"a_abc123"));
  } else {
    GOOGLESQL_EXPECT_OK(UpdateSchemaOp({"ALTER TABLE T ADD COLUMN c1 bigint"},
                             /*operation_id=*/"a_abc123"));
  }

  operations_api::Operation op;
  GOOGLESQL_EXPECT_OK(GetOperation(
      absl::StrCat(database()->FullName(), "/operations/a_abc123"), &op));
  EXPECT_THAT(op.name(), testing::EndsWith("a_abc123"));
}

TEST_P(OperationsTest, NonExistentDatabaseOperation) {
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
