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
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {

namespace instance_api = ::google::spanner::admin::instance::v1;

TEST(InstanceManagerTest, CreateInstance) {
  InstanceManager instance_manager;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<Instance> instance,
      instance_manager.CreateInstance(
          "projects/123/instances/456", PARSE_TEXT_PROTO(R"(
            config: 'projects/123/instanceConfigs/emulator-config'
            display_name: 'Test Instance'
            node_count: 3
          )")));

  instance_api::Instance instance_proto;
  instance->ToProto(&instance_proto);
  EXPECT_THAT(instance_proto, test::EqualsProto(R"(
                name: 'projects/123/instances/456'
                config: 'projects/123/instanceConfigs/emulator-config'
                display_name: 'Test Instance'
                node_count: 3
                state: READY
              )"));
}

TEST(InstanceManagerTest, GetInstance) {
  InstanceManager instance_manager;
  ZETASQL_ASSERT_OK(instance_manager.CreateInstance(
      "projects/123/instances/456", PARSE_TEXT_PROTO(R"(
        config: 'projects/123/instanceConfigs/emulator-config'
        display_name: 'Test Instance'
        node_count: 3
      )")));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<Instance> instance,
      instance_manager.GetInstance("projects/123/instances/456"));

  instance_api::Instance instance_proto;
  instance->ToProto(&instance_proto);
  EXPECT_THAT(instance_proto, test::EqualsProto(R"(
                name: 'projects/123/instances/456'
                config: 'projects/123/instanceConfigs/emulator-config'
                display_name: 'Test Instance'
                node_count: 3
                state: READY
              )"));
}

TEST(InstanceManagerTest, ListInstances) {
  InstanceManager instance_manager;
  ZETASQL_ASSERT_OK(instance_manager.CreateInstance(
      "projects/123/instances/456", PARSE_TEXT_PROTO(R"(
        config: 'projects/123/instanceConfigs/emulator-config'
        display_name: 'Test Instance'
        node_count: 3
      )")));
  ZETASQL_ASSERT_OK(instance_manager.CreateInstance(
      "projects/123/instances/789", PARSE_TEXT_PROTO(R"(
        config: 'projects/123/instanceConfigs/emulator-config'
        display_name: 'Test Instance'
        node_count: 6
      )")));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Instance>> instances,
                       instance_manager.ListInstances("projects/123"));

  instance_api::Instance instance_proto;
  instances[0]->ToProto(&instance_proto);
  EXPECT_THAT(instance_proto, test::EqualsProto(R"(
                name: 'projects/123/instances/456'
                config: 'projects/123/instanceConfigs/emulator-config'
                display_name: "Test Instance"
                node_count: 3
                state: READY
              )"));
  instances[1]->ToProto(&instance_proto);
  EXPECT_THAT(instance_proto, test::EqualsProto(R"(
                name: 'projects/123/instances/789'
                config: 'projects/123/instanceConfigs/emulator-config'
                display_name: "Test Instance"
                node_count: 6
                state: READY
              )"));
}

TEST(InstanceManagerTest, DeleteInstance) {
  InstanceManager instance_manager;
  ZETASQL_ASSERT_OK(instance_manager.CreateInstance(
      "projects/123/instances/456", PARSE_TEXT_PROTO(R"(
        config: 'projects/123/instanceConfigs/emulator-config'
        display_name: 'Test Instance'
        node_count: 6
      )")));

  instance_manager.DeleteInstance("projects/123/instances/456");

  EXPECT_THAT(instance_manager.GetInstance("projects/123/instances/456"),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kNotFound,
                  testing::MatchesRegex(".*Instance not found.*")));
}

}  // namespace

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
