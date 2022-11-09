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

#include "google/protobuf/timestamp.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/time/time.h"
#include "frontend/converters/time.h"
#include "tests/common/proto_matchers.h"
#include "zetasql/base/clock.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {
namespace {

namespace instance_api = ::google::spanner::admin::instance::v1;
using ::google::spanner::emulator::test::EqualsProto;
using ::google::spanner::emulator::test::proto::Partially;

TEST(InstanceTest, Basic) {
  zetasql_base::SimulatedClock clock;
  auto current_time = zetasql_base::Clock::RealClock()->TimeNow();
  clock.SetTime(current_time);
  Labels labels;
  labels["a"] = "b";
  Instance instance(/*name=*/"projects/test-project/instances/test-instance",
                    /*config=*/"emulator-config",
                    /*display_name=*/"Test Instance",
                    /*processing_units=*/5000,
                    /*labels*/ labels, &clock);
  instance_api::Instance instance_pb;
  instance.ToProto(&instance_pb);
  EXPECT_TRUE(instance_pb.has_create_time());
  EXPECT_TRUE(instance_pb.has_update_time());
  EXPECT_EQ(
      absl::ToUnixNanos(TimestampFromProto(instance_pb.create_time()).value()),
      absl::ToUnixNanos(current_time));
  EXPECT_EQ(
      absl::ToUnixNanos(TimestampFromProto(instance_pb.update_time()).value()),
      absl::ToUnixNanos(current_time));
  EXPECT_THAT(instance_pb, Partially(EqualsProto(R"pb(
                name: 'projects/test-project/instances/test-instance'
                config: 'emulator-config'
                display_name: 'Test Instance'
                node_count: 5
                state: READY
                labels { key: "a" value: "b" }
              )pb")));
}

}  // namespace
}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
