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

#include "frontend/collections/instance_manager.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "frontend/entities/instance.h"
#include "tests/common/proto_matchers.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {

namespace instance_api = ::google::spanner::admin::instance::v1;

using ::google::spanner::emulator::test::EqualsProto;
using ::google::spanner::emulator::test::proto::Partially;
using ::testing::MatchesRegex;
using ::zetasql_base::testing::StatusIs;

TEST(InstanceManagerTest, CreateInstanceWithNode) {
  InstanceManager instance_manager;
  admin::instance::v1::Instance instance_proto;
  instance_proto.set_config("projects/123/instanceConfigs/emulator-config");
  instance_proto.set_name("projects/123/instances/456");
  instance_proto.set_state(admin::instance::v1::Instance::READY);
  instance_proto.set_display_name("Test Instance");
  instance_proto.set_node_count(3);
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::shared_ptr<Instance> instance,
                       instance_manager.CreateInstance(
                           "projects/123/instances/456", instance_proto));

  instance->ToProto(&instance_proto);
  EXPECT_TRUE(instance_proto.has_create_time());
  EXPECT_TRUE(instance_proto.has_update_time());
  EXPECT_THAT(instance_proto, Partially(EqualsProto(R"pb(
                name: 'projects/123/instances/456'
                config: 'projects/123/instanceConfigs/emulator-config'
                display_name: 'Test Instance'
                node_count: 3
                processing_units: 3000
                state: READY
              )pb")));
}

TEST(InstanceManagerTest, CreateInstanceWithTime) {
  InstanceManager instance_manager;
  admin::instance::v1::Instance instance_proto;
  instance_proto.set_config("projects/123/instanceConfigs/emulator-config");
  instance_proto.set_display_name("Test Instance");
  instance_proto.set_node_count(3);
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::shared_ptr<Instance> instance,
                       instance_manager.CreateInstance(
                           "projects/123/instances/456", instance_proto));

  instance->ToProto(&instance_proto);
  EXPECT_TRUE(instance_proto.has_create_time());
  EXPECT_TRUE(instance_proto.has_update_time());
  EXPECT_THAT(instance_proto, Partially(EqualsProto(R"pb(
                name: 'projects/123/instances/456'
                config: 'projects/123/instanceConfigs/emulator-config'
                display_name: 'Test Instance'
                node_count: 3
                state: READY
              )pb")));
}

TEST(InstanceManagerTest, CreateInstanceWithMultipleof100ProcessingUnits) {
  InstanceManager instance_manager;
  admin::instance::v1::Instance instance_proto;
  instance_proto.set_config("projects/123/instanceConfigs/emulator-config");
  instance_proto.set_display_name("Test Instance");
  instance_proto.set_processing_units(300);
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::shared_ptr<Instance> instance,
                       instance_manager.CreateInstance(
                           "projects/123/instances/456", instance_proto));

  instance->ToProto(&instance_proto);
  EXPECT_THAT(instance_proto, Partially(EqualsProto(R"pb(
                name: 'projects/123/instances/456'
                config: 'projects/123/instanceConfigs/emulator-config'
                display_name: 'Test Instance'
                processing_units: 300
                state: READY
              )pb")));
}

TEST(InstanceManagerTest, CreateInstanceWithMultipleof1000ProcessingUnits) {
  InstanceManager instance_manager;
  admin::instance::v1::Instance instance_proto;
  instance_proto.set_config("projects/123/instanceConfigs/emulator-config");
  instance_proto.set_display_name("Test Instance");
  instance_proto.set_processing_units(2000);
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::shared_ptr<Instance> instance,
                       instance_manager.CreateInstance(
                           "projects/123/instances/456", instance_proto));

  instance->ToProto(&instance_proto);
  EXPECT_THAT(instance_proto, Partially(EqualsProto(R"pb(
                name: 'projects/123/instances/456'
                config: 'projects/123/instanceConfigs/emulator-config'
                display_name: 'Test Instance'
                node_count: 2
                processing_units: 2000
                state: READY
              )pb")));
}

TEST(InstanceManagerTest, CreateInstanceWithoutEitherNodesOrProcessingUnits) {
  InstanceManager instance_manager;
  admin::instance::v1::Instance instance_proto;
  instance_proto.set_config("projects/123/instanceConfigs/emulator-config");
  instance_proto.set_display_name("Test Instance");
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::shared_ptr<Instance> instance,
                       instance_manager.CreateInstance(
                           "projects/123/instances/456", instance_proto));
  instance->ToProto(&instance_proto);
  EXPECT_THAT(instance_proto, Partially(EqualsProto(R"pb(
                name: 'projects/123/instances/456'
                config: 'projects/123/instanceConfigs/emulator-config'
                display_name: 'Test Instance'
                state: READY
              )pb")));
}

TEST(InstanceManagerTest, CannotCreateInstanceWithBothNodesAndProcessingUnits) {
  InstanceManager instance_manager;
  admin::instance::v1::Instance instance_proto;
  instance_proto.set_config("projects/123/instanceConfigs/emulator-config");
  instance_proto.set_display_name("Test Instance");
  instance_proto.set_node_count(3);
  instance_proto.set_processing_units(300);
  EXPECT_THAT(instance_manager.CreateInstance("projects/123/instances/456",
                                              instance_proto),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       MatchesRegex(".*Only one of nodes or "
                                    "processing units should be specified.*")));
}

TEST(InstanceManagerTest,
     CannotCreateInstanceWithNonMultiple100ProcessingUnits) {
  InstanceManager instance_manager;
  admin::instance::v1::Instance instance_proto;
  instance_proto.set_config("projects/123/instanceConfigs/emulator-config");
  instance_proto.set_display_name("Test Instance");
  instance_proto.set_processing_units(250);
  EXPECT_THAT(
      instance_manager.CreateInstance("projects/123/instances/456",
                                      instance_proto),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          MatchesRegex(".*Processing units should be "
                       "multiple of 100 for values below 1000 and multiples of "
                       "1000 for values above 1000.*")));
}

TEST(InstanceManagerTest,
     CannotCreateInstanceWithNonMultiple1000ProcessingUnits) {
  InstanceManager instance_manager;
  admin::instance::v1::Instance instance_proto;
  instance_proto.set_config("projects/123/instanceConfigs/emulator-config");
  instance_proto.set_display_name("Test Instance");
  instance_proto.set_processing_units(1500);
  EXPECT_THAT(
      instance_manager.CreateInstance("projects/123/instances/456",
                                      instance_proto),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          MatchesRegex(".*Processing units should be "
                       "multiple of 100 for values below 1000 and multiples of "
                       "1000 for values above 1000.*")));
}

TEST(InstanceManagerTest, GetInstance) {
  InstanceManager instance_manager;
  ZETASQL_ASSERT_OK(instance_manager.CreateInstance(
      "projects/123/instances/456", PARSE_TEXT_PROTO(R"pb(
        config: 'projects/123/instanceConfigs/emulator-config'
        display_name: 'Test Instance'
        node_count: 3
      )pb")));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<Instance> instance,
      instance_manager.GetInstance("projects/123/instances/456"));

  instance_api::Instance instance_proto;
  instance->ToProto(&instance_proto);
  EXPECT_TRUE(instance_proto.has_create_time());
  EXPECT_TRUE(instance_proto.has_update_time());
  EXPECT_THAT(instance_proto, Partially(EqualsProto(R"pb(
                name: 'projects/123/instances/456'
                config: 'projects/123/instanceConfigs/emulator-config'
                display_name: 'Test Instance'
                node_count: 3
                state: READY
              )pb")));
}

TEST(InstanceManagerTest, ListInstances) {
  InstanceManager instance_manager;
  ZETASQL_ASSERT_OK(instance_manager.CreateInstance(
      "projects/123/instances/456", PARSE_TEXT_PROTO(R"pb(
        config: 'projects/123/instanceConfigs/emulator-config'
        display_name: 'Test Instance'
        node_count: 3
      )pb")));
  ZETASQL_ASSERT_OK(instance_manager.CreateInstance(
      "projects/123/instances/789", PARSE_TEXT_PROTO(R"pb(
        config: 'projects/123/instanceConfigs/emulator-config'
        display_name: 'Test Instance'
        node_count: 6
      )pb")));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Instance>> instances,
                       instance_manager.ListInstances("projects/123"));

  instance_api::Instance instance_proto;
  instances[0]->ToProto(&instance_proto);
  EXPECT_TRUE(instance_proto.has_create_time());
  EXPECT_TRUE(instance_proto.has_update_time());
  EXPECT_THAT(instance_proto, Partially(EqualsProto(R"pb(
                name: 'projects/123/instances/456'
                config: 'projects/123/instanceConfigs/emulator-config'
                display_name: 'Test Instance'
                node_count: 3
                state: READY
              )pb")));
  instances[1]->ToProto(&instance_proto);
  EXPECT_TRUE(instance_proto.has_create_time());
  EXPECT_TRUE(instance_proto.has_update_time());

  EXPECT_THAT(instance_proto, Partially(EqualsProto(R"pb(
                name: 'projects/123/instances/789'
                config: 'projects/123/instanceConfigs/emulator-config'
                display_name: 'Test Instance'
                node_count: 6
                state: READY
              )pb")));
}

TEST(InstanceManagerTest, DeleteInstance) {
  InstanceManager instance_manager;
  ZETASQL_ASSERT_OK(instance_manager.CreateInstance(
      "projects/123/instances/456", PARSE_TEXT_PROTO(R"pb(
        config: 'projects/123/instanceConfigs/emulator-config'
        display_name: 'Test Instance'
        node_count: 6
      )pb")));

  instance_manager.DeleteInstance("projects/123/instances/456");

  EXPECT_THAT(instance_manager.GetInstance("projects/123/instances/456"),
              StatusIs(absl::StatusCode::kNotFound,
                       MatchesRegex(".*Instance not found.*")));
}

}  // namespace

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
