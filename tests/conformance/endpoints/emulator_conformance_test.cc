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

#include <memory>
#include <utility>

#include "zetasql/base/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "google/cloud/common_options.h"
#include "google/cloud/grpc_options.h"
#include "google/cloud/spanner/admin/instance_admin_client.h"
#include "google/cloud/spanner/create_instance_request_builder.h"
#include "common/feature_flags.h"
#include "frontend/server/server.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/environment.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

// Project name used in emulator tests.
constexpr char kProjectName[] = "test-project";

// Instance name used in emulator tests.
constexpr char kInstanceName[] = "test-instance";

// Instance config name used for creating the instance above.
constexpr char kInstanceConfigName[] = "test-config";

}  // namespace

// Environment for emulator conformance tests.
class EmulatorConformanceTestEnvironment : public testing::Environment {
 public:
  EmulatorConformanceTestEnvironment()
      : feature_flags_({
            .enable_check_constraint = true,
            .enable_column_default_values = true,
            .enable_views = true,
            .enable_generated_pk = true,
            .enable_fk_delete_cascade_action = true,
            .enable_batch_query_with_no_table_scan = true,
            .enable_fk_enforcement_option = true,
            .enable_property_graph_information_schema = true,
        }) {}
  void SetUp() override {
    // Setup emulator server.
    frontend::Server::Options options;
    options.server_address = "localhost:0";
    server_ = frontend::Server::Create(options);
    ASSERT_NE(server_, nullptr);

    // Initialize connection options required by the client library.
    auto connection_options = std::make_unique<google::cloud::Options>();
    connection_options->set<google::cloud::GrpcCredentialOption>(
        grpc::InsecureChannelCredentials());
    connection_options->set<google::cloud::EndpointOption>(
        absl::StrCat(server_->host(), ":", server_->port()));

    // Setup an instance which will be reused for all tests.
    google::cloud::spanner::Instance instance(kProjectName, kInstanceName);
    auto instance_client =
        std::make_unique<google::cloud::spanner_admin::InstanceAdminClient>(
            google::cloud::spanner_admin::MakeInstanceAdminConnection(
                *connection_options));
    ZETASQL_ASSERT_OK(google::spanner::emulator::test::ToUtilStatusOr(
        instance_client
            ->CreateInstance(
                google::cloud::spanner::CreateInstanceRequestBuilder(
                    instance, kInstanceConfigName)
                    .SetDisplayName(kInstanceConfigName)
                    .SetNodeCount(1)
                    .Build())
            .get()));

    // Set globals for the test.
    globals_ = std::make_unique<ConformanceTestGlobals>();
    globals_->project_id = instance.project_id();
    globals_->instance_id = instance.instance_id();
    globals_->connection_options = std::move(connection_options);
    globals_->in_prod_env = false;
    SetConformanceTestGlobals(globals_.get());
  }

 private:
  // Emulator gRPC server.
  std::unique_ptr<frontend::Server> server_;

  // Globals that need to be provided by a conformance test endpoint.
  std::unique_ptr<ConformanceTestGlobals> globals_;

  test::ScopedEmulatorFeatureFlagsSetter feature_flags_;
};

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  testing::AddGlobalTestEnvironment(new google::spanner::emulator::test::
                                        EmulatorConformanceTestEnvironment());
  return RUN_ALL_TESTS();
}
