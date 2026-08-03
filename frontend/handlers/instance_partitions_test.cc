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

#include <cstdint>
#include <string>
#include <vector>

#include "google/longrunning/operations.pb.h"
#include "google/protobuf/empty.pb.h"
#include "google/spanner/admin/database/v1/spanner_database_admin.pb.h"
#include "google/spanner/admin/instance/v1/spanner_instance_admin.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "frontend/common/uris.h"
#include "tests/common/proto_matchers.h"
#include "tests/common/test_env.h"
#include "googlesql/base/status_macros.h"
#include "grpcpp/client_context.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {

namespace instance_api = ::google::spanner::admin::instance::v1;
namespace database_api = ::google::spanner::admin::database::v1;

using ::google::spanner::emulator::test::EqualsProto;
using ::google::spanner::emulator::test::proto::Partially;
using ::googlesql_base::testing::StatusIs;

class InstancePartitionsApiTest : public test::ServerTest {
 protected:
  const std::string kTestConfigId = "emulator-config";

  void SetUp() override {
    test::ServerTest::SetUp();
    GOOGLESQL_EXPECT_OK(CreateInstance(test_instance_name_));
  }

  absl::Status CreateInstance(const absl::string_view instance_id) {
    instance_api::CreateInstanceRequest request;
    request.set_parent(test_project_uri_);
    request.set_instance_id(instance_id);
    request.mutable_instance()->set_config(
        MakeInstanceConfigUri(test_project_name_, kTestConfigId));
    request.mutable_instance()->set_display_name(
        absl::StrCat(instance_id, "-display"));
    request.mutable_instance()->set_node_count(5);
    grpc::ClientContext context;
    longrunning::Operation operation;
    GOOGLESQL_RETURN_IF_ERROR(test_env()->instance_admin_client()->CreateInstance(
        &context, request, &operation));
    return WaitForOperation(operation.name(), &operation);
  }

  absl::Status CreateInstancePartition(const absl::string_view partition_id,
                                       longrunning::Operation* operation,
                                       int32_t node_count = 1) {
    instance_api::CreateInstancePartitionRequest request;
    request.set_parent(test_instance_uri_);
    request.set_instance_partition_id(partition_id);
    request.mutable_instance_partition()->set_config(
        MakeInstanceConfigUri(test_project_name_, kTestConfigId));
    request.mutable_instance_partition()->set_display_name(
        absl::StrCat(partition_id, "-display"));
    if (node_count > 0) {
      request.mutable_instance_partition()->set_node_count(node_count);
    }
    grpc::ClientContext context;
    return test_env()->instance_admin_client()->CreateInstancePartition(
        &context, request, operation);
  }

  absl::Status CreateInstancePartition(const absl::string_view partition_id,
                                       int32_t node_count = 1) {
    longrunning::Operation operation;
    GOOGLESQL_RETURN_IF_ERROR(
        CreateInstancePartition(partition_id, &operation, node_count));
    return WaitForOperation(operation.name(), &operation);
  }

  absl::Status GetInstancePartition(
      absl::string_view partition_id,
      instance_api::InstancePartition* partition) {
    instance_api::GetInstancePartitionRequest request;
    request.set_name(
        MakeInstancePartitionUri(test_instance_uri_, partition_id));
    grpc::ClientContext context;
    return test_env()->instance_admin_client()->GetInstancePartition(
        &context, request, partition);
  }

  absl::Status ListInstancePartitions(
      int32_t page_size, const std::string& page_token,
      instance_api::ListInstancePartitionsResponse* response) {
    grpc::ClientContext context;
    instance_api::ListInstancePartitionsRequest request;
    request.set_parent(test_instance_uri_);
    request.set_page_size(page_size);
    request.set_page_token(page_token);
    return test_env()->instance_admin_client()->ListInstancePartitions(
        &context, request, response);
  }

  absl::Status ListInstancePartitionOperations(
      int32_t page_size, const std::string& page_token,
      instance_api::ListInstancePartitionOperationsResponse* response) {
    grpc::ClientContext context;
    instance_api::ListInstancePartitionOperationsRequest request;
    request.set_parent(test_instance_uri_);
    request.set_page_size(page_size);
    request.set_page_token(page_token);
    return test_env()->instance_admin_client()->ListInstancePartitionOperations(
        &context, request, response);
  }

  absl::Status DeleteInstancePartition(absl::string_view partition_id) {
    instance_api::DeleteInstancePartitionRequest request;
    request.set_name(
        MakeInstancePartitionUri(test_instance_uri_, partition_id));
    protobuf::Empty response;
    grpc::ClientContext context;
    return test_env()->instance_admin_client()->DeleteInstancePartition(
        &context, request, &response);
  }

  absl::Status CreateDatabase(
      absl::string_view database_name,
      const std::vector<std::string>& extra_statements = {}) {
    grpc::ClientContext context;
    database_api::CreateDatabaseRequest request;
    request.set_parent(test_instance_uri_);
    request.set_create_statement(
        absl::StrCat("CREATE DATABASE `", database_name, "`"));
    for (const auto& stmt : extra_statements) {
      request.add_extra_statements(stmt);
    }
    longrunning::Operation operation;
    GOOGLESQL_RETURN_IF_ERROR(test_env()->database_admin_client()->CreateDatabase(
        &context, request, &operation));
    return WaitForOperation(operation.name(), &operation);
  }
};

TEST_F(InstancePartitionsApiTest, CreateInstancePartition) {
  longrunning::Operation operation;
  GOOGLESQL_EXPECT_OK(CreateInstancePartition("test-partition", &operation));
  GOOGLESQL_EXPECT_OK(WaitForOperation(operation.name(), &operation));
  EXPECT_THAT(
      operation, Partially(EqualsProto(R"pb(
        metadata {
          [type.googleapis.com/
           google.spanner.admin.instance.v1.CreateInstancePartitionMetadata] {
            instance_partition {
              name: "projects/test-project/instances/test-instance/"
                    "instancePartitions/test-partition"
              config: "projects/test-project/instanceConfigs/emulator-config"
              display_name: "test-partition-display"
              node_count: 1
              state: READY
            }
          }
        }
      )pb")));
}

TEST_F(InstancePartitionsApiTest, CreateInstancePartitionWithInvalidName) {
  longrunning::Operation operation;
  EXPECT_THAT(CreateInstancePartition("a", &operation),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(CreateInstancePartition("AAAA", &operation),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(CreateInstancePartition("aaaa_aaaa", &operation),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(InstancePartitionsApiTest, CreateInstancePartitionAlreadyExists) {
  GOOGLESQL_EXPECT_OK(CreateInstancePartition("test-partition"));
  longrunning::Operation operation;
  EXPECT_THAT(CreateInstancePartition("test-partition", &operation),
              StatusIs(absl::StatusCode::kAlreadyExists));
}

TEST_F(InstancePartitionsApiTest, GetInstancePartition) {
  GOOGLESQL_EXPECT_OK(CreateInstancePartition("test-partition"));
  instance_api::InstancePartition partition;
  GOOGLESQL_EXPECT_OK(GetInstancePartition("test-partition", &partition));
  EXPECT_TRUE(partition.has_create_time());
  EXPECT_TRUE(partition.has_update_time());
  EXPECT_THAT(partition, Partially(EqualsProto(R"pb(
                name: 'projects/test-project/instances/test-instance/'
                      'instancePartitions/test-partition'
                config: 'projects/test-project/instanceConfigs/emulator-config'
                display_name: 'test-partition-display'
                node_count: 1
                state: READY
              )pb")));
  EXPECT_THAT(GetInstancePartition("nonexist-part", &partition),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(InstancePartitionsApiTest, ListInstancePartitions) {
  GOOGLESQL_EXPECT_OK(CreateInstancePartition("test-partition-1"));
  GOOGLESQL_EXPECT_OK(CreateInstancePartition("test-partition-2"));
  instance_api::ListInstancePartitionsResponse response;
  GOOGLESQL_EXPECT_OK(ListInstancePartitions(0, "", &response));
  EXPECT_EQ(response.instance_partitions_size(), 2);
}

TEST_F(InstancePartitionsApiTest, DeleteInstancePartition) {
  GOOGLESQL_EXPECT_OK(CreateInstancePartition("test-partition"));
  GOOGLESQL_EXPECT_OK(DeleteInstancePartition("test-partition"));
  instance_api::InstancePartition partition;
  EXPECT_THAT(GetInstancePartition("test-partition", &partition),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(InstancePartitionsApiTest, DeleteInstancePartitionReferencedByDatabase) {
  GOOGLESQL_EXPECT_OK(CreateInstancePartition("test-partition"));
  GOOGLESQL_EXPECT_OK(CreateDatabase(
      "test-db",
      {"CREATE PLACEMENT p OPTIONS (instance_partition = 'test-partition')"}));
  instance_api::InstancePartition partition;
  GOOGLESQL_EXPECT_OK(GetInstancePartition("test-partition", &partition));
  EXPECT_EQ(partition.referencing_databases_size(), 1);
  EXPECT_EQ(partition.referencing_databases(0),
            MakeDatabaseUri(test_instance_uri_, "test-db"));

  instance_api::ListInstancePartitionsResponse list_response;
  GOOGLESQL_EXPECT_OK(ListInstancePartitions(0, "", &list_response));
  ASSERT_EQ(list_response.instance_partitions_size(), 1);
  EXPECT_EQ(list_response.instance_partitions(0).referencing_databases_size(),
            1);
  EXPECT_EQ(list_response.instance_partitions(0).referencing_databases(0),
            MakeDatabaseUri(test_instance_uri_, "test-db"));

  EXPECT_THAT(DeleteInstancePartition("test-partition"),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(InstancePartitionsApiTest, CascadeDeleteWithInstance) {
  GOOGLESQL_EXPECT_OK(CreateInstancePartition("test-partition"));
  instance_api::DeleteInstanceRequest request;
  request.set_name(test_instance_uri_);
  protobuf::Empty response;
  grpc::ClientContext context;
  GOOGLESQL_EXPECT_OK(test_env()->instance_admin_client()->DeleteInstance(
      &context, request, &response));

  instance_api::InstancePartition partition;
  EXPECT_THAT(GetInstancePartition("test-partition", &partition),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(InstancePartitionsApiTest, ListInstancePartitionOperations) {
  GOOGLESQL_EXPECT_OK(CreateInstancePartition("test-partition"));

  instance_api::ListInstancePartitionOperationsResponse response;
  GOOGLESQL_EXPECT_OK(ListInstancePartitionOperations(0, "", &response));
  EXPECT_EQ(response.operations_size(), 1);
  EXPECT_THAT(response.operations(0).name(),
              testing::StartsWith(MakeInstancePartitionUri(test_instance_uri_,
                                                           "test-partition") +
                                  "/operations/"));
}

TEST_F(InstancePartitionsApiTest, CreateInstancePartitionInvalidUnits) {
  instance_api::CreateInstancePartitionRequest request;
  request.set_parent(test_instance_uri_);
  request.set_instance_partition_id("test-partition");
  request.mutable_instance_partition()->set_processing_units(550);
  request.mutable_instance_partition()->set_config(
      "projects/test-project/instanceConfigs/emulator-config");

  grpc::ClientContext context;
  longrunning::Operation operation;
  EXPECT_THAT(
      test_env()->instance_admin_client()->CreateInstancePartition(
          &context, request, &operation),
      StatusIs(absl::StatusCode::kInvalidArgument,
               testing::HasSubstr("Processing units should be multiple of 100 "
                                  "for values below 1000")));
}

TEST_F(InstancePartitionsApiTest, UpdateInstancePartitionReturnsUnimplemented) {
  instance_api::UpdateInstancePartitionRequest request;
  request.mutable_instance_partition()->set_name(
      MakeInstancePartitionUri(test_instance_uri_, "test-partition"));
  request.mutable_field_mask()->add_paths("display_name");

  grpc::ClientContext context;
  longrunning::Operation operation;
  EXPECT_THAT(test_env()->instance_admin_client()->UpdateInstancePartition(
                  &context, request, &operation),
              StatusIs(absl::StatusCode::kUnimplemented,
                       testing::HasSubstr("does not support updating instance "
                                          "partitions")));
}

TEST_F(InstancePartitionsApiTest, ListInstancePartitionsPagination) {
  GOOGLESQL_EXPECT_OK(CreateInstancePartition("partition-1"));
  GOOGLESQL_EXPECT_OK(CreateInstancePartition("partition-2"));
  GOOGLESQL_EXPECT_OK(CreateInstancePartition("partition-3"));

  instance_api::ListInstancePartitionsResponse response;
  // Page 1
  GOOGLESQL_EXPECT_OK(ListInstancePartitions(2, "", &response));
  EXPECT_EQ(response.instance_partitions_size(), 2);
  EXPECT_EQ(response.instance_partitions(0).name(),
            MakeInstancePartitionUri(test_instance_uri_, "partition-1"));
  EXPECT_EQ(response.instance_partitions(1).name(),
            MakeInstancePartitionUri(test_instance_uri_, "partition-2"));
  std::string next_page_token = response.next_page_token();
  EXPECT_FALSE(next_page_token.empty());

  // Page 2
  response.Clear();
  GOOGLESQL_EXPECT_OK(ListInstancePartitions(2, next_page_token, &response));
  EXPECT_EQ(response.instance_partitions_size(), 1);
  EXPECT_EQ(response.instance_partitions(0).name(),
            MakeInstancePartitionUri(test_instance_uri_, "partition-3"));
  EXPECT_TRUE(response.next_page_token().empty());
}

}  // namespace

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
