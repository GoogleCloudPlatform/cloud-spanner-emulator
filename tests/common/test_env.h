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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_TEST_ENV_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_TEST_ENV_H_

#include <memory>
#include <string>
#include <thread>  // NOLINT(build/c++11)

#include "google/longrunning/operations.grpc.pb.h"
#include "google/spanner/admin/database/v1/spanner_database_admin.grpc.pb.h"
#include "google/spanner/admin/instance/v1/spanner_instance_admin.grpc.pb.h"
#include "google/spanner/v1/result_set.pb.h"
#include "google/spanner/v1/spanner.grpc.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/notification.h"
#include "absl/time/clock.h"
#include "frontend/common/uris.h"
#include "frontend/server/server.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace protobuf_api = ::google::protobuf;
namespace database_api = ::google::spanner::admin::database::v1;
namespace instance_api = ::google::spanner::admin::instance::v1;
namespace spanner_api = ::google::spanner::v1;

// TestEnv sets up the gRPC server interface and (in a separate thread) the
// client stubs for integration testing.
class TestEnv {
  using DatabaseAdminStub = admin::database::v1::DatabaseAdmin::Stub;
  using InstanceAdminStub = admin::instance::v1::InstanceAdmin::Stub;
  using OperationsStub = longrunning::Operations::Stub;
  using SpannerStub = v1::Spanner::Stub;

 public:
  TestEnv();
  ~TestEnv();

  DatabaseAdminStub* database_admin_client() const {
    return database_admin_client_.get();
  }
  InstanceAdminStub* instance_admin_client() const {
    return instance_admin_client_.get();
  }
  OperationsStub* operations_client() const { return operations_client_.get(); }
  SpannerStub* spanner_client() const { return spanner_client_.get(); }

 private:
  void SetupServer();
  void SetupClientStubs();
  void WaitForServerReady();

  std::unique_ptr<SpannerStub> spanner_client_;
  std::unique_ptr<DatabaseAdminStub> database_admin_client_;
  std::unique_ptr<InstanceAdminStub> instance_admin_client_;
  std::unique_ptr<OperationsStub> operations_client_;
  std::unique_ptr<std::thread> server_thread_;
  std::unique_ptr<frontend::Server> server_;
  std::string host_port_;
  absl::Notification ready_;
};

// A convenience class that sets up a TestEnv as a class member. Test fixtures
// can inherit from this class to run integration tests.
class ServerTest : public testing::Test {
 public:
  TestEnv* test_env() { return &test_env_; }

 protected:
  const std::string test_project_name_ = "test-project";
  const std::string test_project_uri_ = MakeProjectUri(test_project_name_);
  const std::string test_instance_name_ = "test-instance";
  const std::string test_instance_uri_ =
      MakeInstanceUri(test_project_name_, test_instance_name_);
  const std::string test_database_name_ = "test-database";
  const std::string test_database_uri_ =
      MakeDatabaseUri(test_instance_uri_, test_database_name_);

  absl::Status CreateTestInstance() {
    // Create an instance.
    instance_api::CreateInstanceRequest request = PARSE_TEXT_PROTO(R"(
      instance { config: "emulator-config" display_name: "" node_count: 3 }
    )");
    request.set_parent(test_project_uri_);
    request.set_instance_id(test_instance_name_);
    grpc::ClientContext context;
    longrunning::Operation operation;
    ZETASQL_RETURN_IF_ERROR(test_env()->instance_admin_client()->CreateInstance(
        &context, request, &operation));
    return WaitForOperation(operation.name(), &operation);
  }

  absl::Status CreateTestDatabase() {
    // Create a database that belongs to the instance created above and create
    // a test schema inside the newly created database.
    database_api::CreateDatabaseRequest request;
    request.add_extra_statements(
        R"(
              CREATE TABLE test_table(
                int64_col INT64 NOT NULL,
                string_col STRING(MAX)
              ) PRIMARY KEY(int64_col)
        )");
    request.set_parent(test_instance_uri_);
    request.set_create_statement(
        absl::StrCat("CREATE DATABASE `", test_database_name_, "`"));
    grpc::ClientContext context;
    longrunning::Operation operation;
    ZETASQL_RETURN_IF_ERROR(test_env()->database_admin_client()->CreateDatabase(
        &context, request, &operation));
    return WaitForOperation(operation.name(), &operation);
  }

  zetasql_base::StatusOr<std::string> CreateTestSession() {
    // Create a session that belongs to the database created above.
    grpc::ClientContext context;
    spanner_api::CreateSessionRequest request;
    spanner_api::Session response;
    request.set_database(test_database_uri_);
    ZETASQL_RETURN_IF_ERROR(test_env()->spanner_client()->CreateSession(
        &context, request, &response));
    return response.name();
  }

  absl::Status WaitForOperation(absl::string_view operation_uri,
                                longrunning::Operation* op) {
    absl::Duration deadline = absl::Seconds(10);
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

  absl::Status GetOperation(absl::string_view operation_uri,
                            longrunning::Operation* op) {
    longrunning::GetOperationRequest request;
    request.set_name(std::string(operation_uri));  // NOLINT
    grpc::ClientContext context;
    return test_env()->operations_client()->GetOperation(&context, request, op);
  }

  absl::Status BeginTransaction(
      const spanner_api::BeginTransactionRequest& request,
      spanner_api::Transaction* response) {
    grpc::ClientContext ctx;
    return test_env()->spanner_client()->BeginTransaction(&ctx, request,
                                                          response);
  }

  absl::Status Commit(const spanner_api::CommitRequest& request,
                      spanner_api::CommitResponse* response) {
    grpc::ClientContext ctx;
    return test_env()->spanner_client()->Commit(&ctx, request, response);
  }

  absl::Status Rollback(const spanner_api::RollbackRequest& request) {
    grpc::ClientContext ctx;
    protobuf_api::Empty empty_response;
    return test_env()->spanner_client()->Rollback(&ctx, request,
                                                  &empty_response);
  }

  absl::Status Read(const spanner_api::ReadRequest& request,
                    spanner_api::ResultSet* response) {
    grpc::ClientContext ctx;
    return test_env()->spanner_client()->Read(&ctx, request, response);
  }

  absl::Status StreamingRead(
      const spanner_api::ReadRequest& request,
      std::vector<spanner_api::PartialResultSet>* response);

  absl::Status PartitionRead(const spanner_api::PartitionReadRequest& request,
                             spanner_api::PartitionResponse* response) {
    grpc::ClientContext ctx;
    return test_env()->spanner_client()->PartitionRead(&ctx, request, response);
  }

  absl::Status ExecuteSql(const spanner_api::ExecuteSqlRequest& request,
                          spanner_api::ResultSet* response) {
    grpc::ClientContext ctx;
    return test_env()->spanner_client()->ExecuteSql(&ctx, request, response);
  }

  absl::Status ExecuteBatchDml(
      const spanner_api::ExecuteBatchDmlRequest& request,
      spanner_api::ExecuteBatchDmlResponse* response) {
    grpc::ClientContext ctx;
    return test_env()->spanner_client()->ExecuteBatchDml(&ctx, request,
                                                         response);
  }

  absl::Status ExecuteStreamingSql(
      const spanner_api::ExecuteSqlRequest& request,
      std::vector<spanner_api::PartialResultSet>* response);

 private:
  TestEnv test_env_;
};

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_TEST_ENV_H_
