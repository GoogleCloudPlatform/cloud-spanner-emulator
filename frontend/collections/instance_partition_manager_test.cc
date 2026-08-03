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

#include "frontend/collections/instance_partition_manager.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "frontend/entities/instance_partition.h"
#include "tests/common/proto_matchers.h"
#include "googlesql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {
namespace {

namespace instance_api = ::google::spanner::admin::instance::v1;
using ::testing::MatchesRegex;
using ::googlesql_base::testing::StatusIs;

TEST(InstancePartitionManagerTest, CreateInstancePartitionWithNode) {
  InstancePartitionManager manager;
  instance_api::InstancePartition proto;
  proto.set_config("projects/123/instanceConfigs/emulator-config");
  proto.set_display_name("Test Partition");
  proto.set_node_count(1);

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<InstancePartition> partition,
      manager.CreateInstancePartition(
          "projects/123/instances/456/instancePartitions/test-partition",
          proto));

  EXPECT_EQ(partition->node_count(), 1);
  EXPECT_EQ(partition->processing_units(), 1000);
}

TEST(InstancePartitionManagerTest, CreateInstancePartitionWithProcessingUnits) {
  InstancePartitionManager manager;
  instance_api::InstancePartition proto;
  proto.set_config("projects/123/instanceConfigs/emulator-config");
  proto.set_display_name("Test Partition");
  proto.set_processing_units(500);

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<InstancePartition> partition,
      manager.CreateInstancePartition(
          "projects/123/instances/456/instancePartitions/test-partition",
          proto));

  EXPECT_EQ(partition->node_count(), 0);
  EXPECT_EQ(partition->processing_units(), 500);
}

TEST(InstancePartitionManagerTest,
     CannotCreateInstancePartitionWithInvalidUnits) {
  InstancePartitionManager manager;
  instance_api::InstancePartition proto;
  proto.set_config("projects/123/instanceConfigs/emulator-config");
  proto.set_display_name("Test Partition");
  proto.set_processing_units(550);

  EXPECT_THAT(
      manager.CreateInstancePartition(
          "projects/123/instances/456/instancePartitions/test-partition",
          proto),
      StatusIs(absl::StatusCode::kInvalidArgument,
               MatchesRegex(".*Processing units should be multiple of 100.*")));
}

TEST(InstancePartitionManagerTest, GetInstancePartition) {
  InstancePartitionManager manager;
  std::string uri =
      "projects/123/instances/456/instancePartitions/test-partition";
  instance_api::InstancePartition proto;
  proto.set_config("projects/123/instanceConfigs/emulator-config");
  proto.set_display_name("Test Partition");
  proto.set_node_count(1);

  GOOGLESQL_ASSERT_OK(manager.CreateInstancePartition(uri, proto));

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::shared_ptr<InstancePartition> partition,
                       manager.GetInstancePartition(uri));
  EXPECT_EQ(partition->partition_uri(), uri);
}

TEST(InstancePartitionManagerTest, ListInstancePartitions) {
  InstancePartitionManager manager;
  std::string instance_uri = "projects/123/instances/456";

  instance_api::InstancePartition proto1;
  proto1.set_config("projects/123/instanceConfigs/emulator-config");
  proto1.set_display_name("Partition 1");
  proto1.set_node_count(1);
  GOOGLESQL_ASSERT_OK(manager.CreateInstancePartition(
      instance_uri + "/instancePartitions/part-1", proto1));

  instance_api::InstancePartition proto2;
  proto2.set_config("projects/123/instanceConfigs/emulator-config");
  proto2.set_display_name("Partition 2");
  proto2.set_node_count(2);
  GOOGLESQL_ASSERT_OK(manager.CreateInstancePartition(
      instance_uri + "/instancePartitions/part-2", proto2));

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::vector<std::shared_ptr<InstancePartition>> partitions,
      manager.ListInstancePartitions(instance_uri));
  EXPECT_EQ(partitions.size(), 2);
}

TEST(InstancePartitionManagerTest, DeleteInstancePartition) {
  InstancePartitionManager manager;
  std::string uri =
      "projects/123/instances/456/instancePartitions/test-partition";
  instance_api::InstancePartition proto;
  proto.set_config("projects/123/instanceConfigs/emulator-config");
  proto.set_display_name("Test Partition");
  proto.set_node_count(1);

  GOOGLESQL_ASSERT_OK(manager.CreateInstancePartition(uri, proto));
  manager.DeleteInstancePartition(uri);

  EXPECT_THAT(manager.GetInstancePartition(uri),
              StatusIs(absl::StatusCode::kNotFound));
}

}  // namespace
}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
