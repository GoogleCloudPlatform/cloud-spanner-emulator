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

#include "tests/common/test_env.h"

#include <memory>
#include <thread>  // NOLINT(build/c++11)

#include "zetasql/base/logging.h"
#include "google/longrunning/operations.grpc.pb.h"
#include "google/spanner/admin/database/v1/spanner_database_admin.grpc.pb.h"
#include "google/spanner/admin/instance/v1/spanner_instance_admin.grpc.pb.h"
#include "google/spanner/v1/spanner.grpc.pb.h"
#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "absl/strings/str_cat.h"
#include "frontend/server/server.h"
#include "grpcpp/channel.h"
#include "grpcpp/create_channel.h"
#include "grpcpp/security/credentials.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

absl::Status ReadFromClientReader(
    std::unique_ptr<grpc::ClientReader<spanner_api::PartialResultSet>> reader,
    std::vector<spanner_api::PartialResultSet>* response) {
  response->clear();
  spanner_api::PartialResultSet result;
  while (reader->Read(&result)) {
    response->push_back(result);
  }
  return reader->Finish();
}

}  // namespace

TestEnv::TestEnv() {
  // Set up gRPC server in a detached thread.
  server_thread_ = absl::make_unique<std::thread>(&TestEnv::SetupServer, this);
  WaitForServerReady();
  SetupClientStubs();
}

TestEnv::~TestEnv() {
  server_->Shutdown();
  server_thread_->join();
}

void TestEnv::SetupServer() {
  frontend::Server::Options options;
  options.server_address = "localhost:0";
  server_ = frontend::Server::Create(options);
  ZETASQL_CHECK(server_ != nullptr);  // Crash ok
  ASSERT_EQ(server_->host(), "localhost");
  host_port_ = absl::StrCat(server_->host(), ":", server_->port());
  ZETASQL_LOG(INFO) << "Cloud Spanner Emulator running in test.";
  ZETASQL_LOG(INFO) << "Server address: " << host_port_;
  ready_.Notify();
  server_->WaitForShutdown();
}

void TestEnv::SetupClientStubs() {
  std::shared_ptr<::grpc::Channel> channel =
      ::grpc::CreateChannel(host_port_, ::grpc::InsecureChannelCredentials());
  spanner_client_ = v1::Spanner::NewStub(channel);
  database_admin_client_ =
      admin::database::v1::DatabaseAdmin::NewStub(channel);
  instance_admin_client_ =
      admin::instance::v1::InstanceAdmin::NewStub(channel);
  operations_client_ = longrunning::Operations::NewStub(channel);
  ZETASQL_LOG(INFO) << "Client stubs setup finished.";
}

void TestEnv::WaitForServerReady() { ready_.WaitForNotification(); }

absl::Status ServerTest::StreamingRead(
    const spanner_api::ReadRequest& request,
    std::vector<spanner_api::PartialResultSet>* response) {
  grpc::ClientContext ctx;
  auto client_reader =
      test_env()->spanner_client()->StreamingRead(&ctx, request);
  return ReadFromClientReader(std::move(client_reader), response);
}

absl::Status ServerTest::ExecuteStreamingSql(
    const spanner_api::ExecuteSqlRequest& request,
    std::vector<spanner_api::PartialResultSet>* response) {
  grpc::ClientContext ctx;
  auto client_reader =
      test_env()->spanner_client()->ExecuteStreamingSql(&ctx, request);
  return ReadFromClientReader(std::move(client_reader), response);
}

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
