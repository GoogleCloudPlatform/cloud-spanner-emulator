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

#include "frontend/entities/instance.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {
namespace {

namespace instance_api = ::google::spanner::admin::instance::v1;

TEST(InstanceTest, Basic) {
  Labels labels;
  labels["a"] = "b";
  Instance instance(/*name=*/"projects/test-project/instances/test-instance",
                    /*config=*/"emulator-config",
                    /*display_name=*/"Test Instance",
                    /*node_count=*/5,
                    /*labels*/ labels);
  instance_api::Instance instance_pb;
  instance.ToProto(&instance_pb);

  EXPECT_THAT(instance_pb, test::EqualsProto(R"(
                name: "projects/test-project/instances/test-instance"
                config: "emulator-config"
                display_name: "Test Instance"
                node_count: 5
                state: READY
                labels { key: "a" value: "b" }
              )"));
}

}  // namespace
}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
