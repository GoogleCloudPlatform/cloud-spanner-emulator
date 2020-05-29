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

#include "frontend/entities/operation.h"

#include "google/spanner/admin/instance/v1/spanner_instance_admin.pb.h"
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

TEST(Operation, SerializesMetadataOnlyOperation) {
  Operation operation("projects/123/instances/456/operations/789");

  instance_api::CreateInstanceMetadata cimd;
  cimd.mutable_instance()->set_name("projects/123/instances/456");
  operation.SetMetadata(cimd);

  google::longrunning::Operation operation_pb;
  operation.ToProto(&operation_pb);

  EXPECT_THAT(operation_pb, test::EqualsProto(R"(
                name: "projects/123/instances/456/operations/789"
                metadata {
                  [type.googleapis.com/
                   google.spanner.admin.instance.v1.CreateInstanceMetadata] {
                    instance { name: "projects/123/instances/456" }
                  }
                }
              )"));
}

TEST(Operation, SerializesResponseOnly) {
  Operation operation("projects/123/instances/456/operations/789");

  instance_api::Instance instance;
  instance.set_name("projects/123/instances/456");
  operation.SetResponse(instance);

  google::longrunning::Operation operation_pb;
  operation.ToProto(&operation_pb);

  EXPECT_THAT(
      operation_pb, test::EqualsProto(R"(
        name: "projects/123/instances/456/operations/789"
        done: true
        response {
          [type.googleapis.com/google.spanner.admin.instance.v1.Instance] {
            name: "projects/123/instances/456"
          }
        }
      )"));
}

TEST(Operation, SerializesMetadataAndResponse) {
  Operation operation("projects/123/instances/456/operations/789");

  instance_api::CreateInstanceMetadata cimd;
  cimd.mutable_instance()->set_name("projects/123/instances/456");
  operation.SetMetadata(cimd);

  instance_api::Instance instance;
  instance.set_name("projects/123/instances/456");
  operation.SetResponse(instance);

  google::longrunning::Operation operation_pb;
  operation.ToProto(&operation_pb);

  EXPECT_THAT(
      operation_pb, test::EqualsProto(R"(
        name: "projects/123/instances/456/operations/789"
        done: true
        metadata {
          [type.googleapis.com/
           google.spanner.admin.instance.v1.CreateInstanceMetadata] {
            instance { name: "projects/123/instances/456" }
          }
        }
        response {
          [type.googleapis.com/google.spanner.admin.instance.v1.Instance] {
            name: "projects/123/instances/456"
          }
        }
      )"));
}

TEST(Operation, SerializesMetadataAndError) {
  Operation operation("projects/123/instances/456/operations/789");

  instance_api::CreateInstanceMetadata cimd;
  cimd.mutable_instance()->set_name("projects/123/instances/456");
  operation.SetMetadata(cimd);

  operation.SetError(absl::Status(absl::StatusCode::kAborted,
                                  "Aborted by higher-priority operation."));

  google::longrunning::Operation operation_pb;
  operation.ToProto(&operation_pb);

  EXPECT_THAT(
      operation_pb, test::EqualsProto(R"(
        name: "projects/123/instances/456/operations/789"
        metadata {
          [type.googleapis.com/
           google.spanner.admin.instance.v1.CreateInstanceMetadata] {
            instance { name: "projects/123/instances/456" }
          }
        }
        done: true
        error { code: 10 message: "Aborted by higher-priority operation." }
      )"));
}

TEST(Operation, SetErrorClearsResponse) {
  Operation operation("projects/123/instances/456/operations/789");

  instance_api::Instance instance;
  instance.set_name("projects/123/instances/456");
  operation.SetResponse(instance);

  operation.SetError(absl::Status(absl::StatusCode::kAborted,
                                  "Aborted by higher-priority operation."));

  google::longrunning::Operation operation_pb;
  operation.ToProto(&operation_pb);

  EXPECT_THAT(
      operation_pb, test::EqualsProto(R"(
        name: "projects/123/instances/456/operations/789"
        done: true
        error { code: 10 message: "Aborted by higher-priority operation." }
      )"));
}

TEST(Operation, SetResponseClearsError) {
  Operation operation("projects/123/instances/456/operations/789");

  operation.SetError(absl::Status(absl::StatusCode::kAborted,
                                  "Aborted by higher-priority operation."));

  instance_api::Instance instance;
  instance.set_name("projects/123/instances/456");
  operation.SetResponse(instance);

  google::longrunning::Operation operation_pb;
  operation.ToProto(&operation_pb);

  EXPECT_THAT(
      operation_pb, test::EqualsProto(R"(
        name: "projects/123/instances/456/operations/789"
        done: true
        response {
          [type.googleapis.com/google.spanner.admin.instance.v1.Instance] {
            name: "projects/123/instances/456"
          }
        }
      )"));
}

}  // namespace

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
