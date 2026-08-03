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

#include "frontend/entities/instance_partition.h"

#include "google/protobuf/timestamp.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/time/time.h"
#include "frontend/converters/time.h"
#include "tests/common/proto_matchers.h"
#include "googlesql/base/clock.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {
namespace {

namespace instance_api = ::google::spanner::admin::instance::v1;
using ::google::spanner::emulator::test::EqualsProto;
using ::google::spanner::emulator::test::proto::Partially;

TEST(InstancePartitionTest, Basic) {
  googlesql_base::SimulatedClock clock;
  auto current_time = googlesql_base::Clock::RealClock()->TimeNow();
  clock.SetTime(current_time);

  InstancePartition partition(
      /*name=*/
      "projects/test-project/instances/test-instance/instancePartitions/"
      "test-partition",
      /*config=*/"emulator-config",
      /*display_name=*/"Test Partition",
      /*node_count=*/1,
      /*processing_units=*/1000, &clock);

  instance_api::InstancePartition partition_pb;
  partition.ToProto(&partition_pb);

  EXPECT_TRUE(partition_pb.has_create_time());
  EXPECT_TRUE(partition_pb.has_update_time());
  EXPECT_EQ(
      absl::ToUnixNanos(TimestampFromProto(partition_pb.create_time()).value()),
      absl::ToUnixNanos(current_time));
  EXPECT_EQ(
      absl::ToUnixNanos(TimestampFromProto(partition_pb.update_time()).value()),
      absl::ToUnixNanos(current_time));
  EXPECT_THAT(partition_pb, Partially(EqualsProto(R"pb(
                name: 'projects/test-project/instances/test-instance/'
                      'instancePartitions/test-partition'
                config: 'emulator-config'
                display_name: 'Test Partition'
                node_count: 1
                state: READY
              )pb")));
}

TEST(InstancePartitionTest, ToProtoClearsExistingFields) {
  googlesql_base::SimulatedClock clock;
  InstancePartition partition(
      /*name=*/
      "projects/test-project/instances/test-instance/instancePartitions/"
      "test-partition",
      /*config=*/"emulator-config",
      /*display_name=*/"Test Partition",
      /*node_count=*/1,
      /*processing_units=*/1000, &clock);

  instance_api::InstancePartition partition_pb;
  // Pre-populate some fields that should be cleared by ToProto.
  partition_pb.set_name("old-name");
  partition_pb.set_config("old-config");
  partition_pb.set_display_name("old-display");
  partition_pb.set_processing_units(500);

  // referencing_databases is not set by ToProto, so it's a good candidate to
  // test Clear().
  partition_pb.add_referencing_databases(
      "projects/test-project/instances/test-instance/databases/test-db");

  partition.ToProto(&partition_pb);

  // Verify that old fields are cleared/overwritten.
  EXPECT_EQ(partition_pb.name(),
            "projects/test-project/instances/test-instance/instancePartitions/"
            "test-partition");
  EXPECT_EQ(partition_pb.config(), "emulator-config");
  EXPECT_EQ(partition_pb.display_name(), "Test Partition");
  EXPECT_EQ(partition_pb.node_count(), 1);
  EXPECT_EQ(partition_pb.referencing_databases_size(), 0);
}

}  // namespace
}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
