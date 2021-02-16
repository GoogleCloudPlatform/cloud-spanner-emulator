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

#include "google/longrunning/operations.pb.h"
#include "google/protobuf/empty.pb.h"
#include "google/spanner/admin/instance/v1/spanner_instance_admin.pb.h"
#include "grpcpp/client_context.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/strings/str_cat.h"
#include "absl/types/optional.h"
#include "frontend/common/uris.h"
#include "frontend/server/server.h"
#include "tests/common/test_env.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {

namespace instance_api = ::google::spanner::admin::instance::v1;

using grpc::Status;
using test::EqualsProto;
using testing::MatchesRegex;
using test::proto::IgnoringRepeatedFieldOrdering;
using test::proto::Partially;
using zetasql_base::testing::StatusIs;

class InstanceApiTest : public test::ServerTest {
 protected:
  const std::string kTestConfigId = "emulator-config";

  absl::Status GetInstance(absl::string_view instance_uri,
                           instance_api::Instance* instance) {
    instance_api::GetInstanceRequest request;
    request.set_name(MakeInstanceUri(test_project_name_, instance_uri));
    grpc::ClientContext context;
    return test_env()->instance_admin_client()->GetInstance(&context, request,
                                                            instance);
  }

  absl::Status ListInstances(const std::string& project_uri, int32_t page_size,
                             const std::string& page_token,
                             instance_api::ListInstancesResponse* response) {
    grpc::ClientContext context;
    instance_api::ListInstancesRequest request;
    request.set_parent(project_uri);
    request.set_page_size(page_size);
    request.set_page_token(page_token);
    return test_env()->instance_admin_client()->ListInstances(&context, request,
                                                              response);
  }

  absl::Status CreateInstance(const absl::string_view instance_id) {
    longrunning::Operation operation;
    ZETASQL_RETURN_IF_ERROR(CreateInstance(instance_id, &operation));
    return WaitForOperation(operation.name(), &operation);
  }

  absl::Status CreateInstance(const absl::string_view instance_id,
                              longrunning::Operation* operation) {
    instance_api::CreateInstanceRequest request;
    request.set_parent(test_project_uri_);
    // Open-source protobuf compiler does not generate a string_view setter.
    request.set_instance_id(std::string(instance_id));  // NOLINT
    request.mutable_instance()->set_config(
        MakeInstanceConfigUri(test_project_name_, kTestConfigId));
    request.mutable_instance()->set_display_name(
        absl::StrCat(instance_id, "-display"));
    request.mutable_instance()->set_node_count(5);
    request.mutable_instance()->mutable_labels()->insert({"a", "b"});
    grpc::ClientContext context;
    return test_env()->instance_admin_client()->CreateInstance(
        &context, request, operation);
  }
};

TEST_F(InstanceApiTest, ListInstanceConfigs) {
  instance_api::ListInstanceConfigsRequest request;
  request.set_parent(test_project_uri_);
  instance_api::ListInstanceConfigsResponse response;
  grpc::ClientContext context;
  ZETASQL_EXPECT_OK(test_env()->instance_admin_client()->ListInstanceConfigs(
      &context, request, &response));
  ASSERT_EQ(response.instance_configs_size(), 1);
}

TEST_F(InstanceApiTest, GetInstanceConfig) {
  instance_api::GetInstanceConfigRequest request;
  request.set_name(MakeInstanceConfigUri(test_project_name_, kTestConfigId));
  instance_api::InstanceConfig response;
  grpc::ClientContext context;
  ZETASQL_EXPECT_OK(test_env()->instance_admin_client()->GetInstanceConfig(
      &context, request, &response));
}

TEST_F(InstanceApiTest, CreateInstanceWithInvalidName) {
  longrunning::Operation operation;
  // Instance name less than 2 characters.
  EXPECT_THAT(CreateInstance("a", &operation),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Instance name more than 64 characters.
  EXPECT_THAT(CreateInstance("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                             "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                             &operation),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Underscore is not allowed.
  EXPECT_THAT(CreateInstance("aaaa_aaaa", &operation),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Only lowercase is allowed.
  EXPECT_THAT(CreateInstance("AAAA", &operation),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Cannot end with hypen.
  EXPECT_THAT(CreateInstance("aaaa-aaaa-", &operation),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Non-alphanumeric characters are not allowed.
  EXPECT_THAT(CreateInstance("aaaa!@#$aaa", &operation),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(InstanceApiTest, CreateInstance) {
  longrunning::Operation operation;
  ZETASQL_EXPECT_OK(CreateInstance(test_instance_name_, &operation));
  ZETASQL_EXPECT_OK(WaitForOperation(operation.name(), &operation));
  EXPECT_THAT(
      operation, Partially(EqualsProto(R"(
        name: "projects/test-project/instances/test-instance/operations/_auto0"
        metadata {
          [type.googleapis.com/
           google.spanner.admin.instance.v1.CreateInstanceMetadata] {
            instance {
              name: "projects/test-project/instances/test-instance"
              config: "projects/test-project/instanceConfigs/emulator-config"
              display_name: "test-instance-display"
              node_count: 5
              state: READY
              labels { key: "a" value: "b" }
            }
          }
        }
      )")));
}

TEST_F(InstanceApiTest, InstanceAlreadyExists) {
  ZETASQL_EXPECT_OK(CreateInstance(test_instance_name_));
  EXPECT_THAT(CreateInstance(test_instance_name_),
              StatusIs(absl::StatusCode::kAlreadyExists,
                       MatchesRegex(".*Instance already exists.*")));
}

TEST_F(InstanceApiTest, GetInstance) {
  ZETASQL_EXPECT_OK(CreateInstance(test_instance_name_));
  instance_api::Instance instance;
  ZETASQL_EXPECT_OK(GetInstance(test_instance_name_, &instance));
  EXPECT_THAT(instance, EqualsProto(R"(
                name: "projects/test-project/instances/test-instance"
                config: "projects/test-project/instanceConfigs/emulator-config"
                display_name: "test-instance-display"
                node_count: 5
                state: READY
                labels { key: "a" value: "b" }
              )"));
  EXPECT_THAT(GetInstance("nonexist-instance", &instance),
              StatusIs(absl::StatusCode::kNotFound,
                       MatchesRegex(".*Instance not found.*")));
}

TEST_F(InstanceApiTest, ListInstances) {
  ZETASQL_EXPECT_OK(CreateInstance("test-instance-1"));
  ZETASQL_EXPECT_OK(CreateInstance("test-instance-2"));
  instance_api::ListInstancesResponse response;
  ZETASQL_EXPECT_OK(ListInstances(test_project_uri_, 0 /*page_size*/, "" /*page_token*/,
                          &response));
  EXPECT_THAT(
      response, IgnoringRepeatedFieldOrdering(EqualsProto(R"(
        instances {
          name: "projects/test-project/instances/test-instance-1"
          config: "projects/test-project/instanceConfigs/emulator-config"
          display_name: "test-instance-1-display"
          node_count: 5
          state: READY
          labels { key: "a" value: "b" }
        }
        instances {
          name: "projects/test-project/instances/test-instance-2"
          config: "projects/test-project/instanceConfigs/emulator-config"
          display_name: "test-instance-2-display"
          node_count: 5
          state: READY
          labels { key: "a" value: "b" }
        }
      )")));
}

TEST_F(InstanceApiTest, ListsPaginatedInstances) {
  // Create 5+3 = 8 instances test-instance0, test-instance1,...,
  // test-instance7 in test-instance.
  int32_t page_size = 5;
  int32_t last_page_size = 3;
  for (int i = 0; i < page_size + last_page_size; i++) {
    ZETASQL_EXPECT_OK(CreateInstance(absl::StrCat(test_instance_name_, i)));
  }

  // List instances from test-instance with page_size being 5, i.e., only 5
  // instances test-instance0, test-instance1,..., test-instance4 should be
  // returned with next_page_token pointing to test-instance5.
  instance_api::ListInstancesResponse response;
  ZETASQL_EXPECT_OK(ListInstances(test_project_uri_, page_size, "" /*page_token*/,
                          &response));
  EXPECT_EQ(response.instances_size(), page_size);
  for (int i = 0; i < page_size; i++) {
    EXPECT_EQ(response.instances(i).name(),
              absl::StrCat(test_instance_uri_, i));
  }
  EXPECT_EQ(response.next_page_token(),
            absl::StrCat(test_instance_uri_, page_size));

  // Using the next_page_token pointing to test-instance5, list next at most 5
  // instances test-instance5, test-instance6 and test-instance7.
  instance_api::ListInstancesResponse response2;
  ZETASQL_EXPECT_OK(ListInstances(test_instance_uri_, page_size,
                          response.next_page_token(), &response2));
  EXPECT_EQ(response2.instances_size(), last_page_size);
  for (int i = 0; i < last_page_size; i++) {
    EXPECT_EQ(response2.instances(i).name(),
              absl::StrCat(test_instance_uri_, i + page_size));
  }
  // No more instances left to be returned and thus next_page_token is not set.
  EXPECT_EQ(response2.next_page_token(), "");
}

TEST_F(InstanceApiTest, DeleteInstance) {
  ZETASQL_EXPECT_OK(CreateInstance(test_instance_name_));
  instance_api::DeleteInstanceRequest request;
  request.set_name(test_instance_uri_);
  protobuf::Empty response;
  grpc::ClientContext context;
  ZETASQL_EXPECT_OK(test_env()->instance_admin_client()->DeleteInstance(
      &context, request, &response));
  instance_api::Instance instance;
  EXPECT_THAT(GetInstance(test_instance_name_, &instance),
              StatusIs(absl::StatusCode::kNotFound,
                       MatchesRegex(".*Instance not found.*")));
}

}  // namespace

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
