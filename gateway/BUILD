#
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

load("@io_bazel_rules_go//go:def.bzl", "go_library")
load("//build/bazel:grpc_gateway.bzl", "generate_grpc_gateway")

package(
    default_visibility = ["//:__subpackages__"],
)

licenses(["unencumbered"])

generate_grpc_gateway(
    name = "spanner_gateway",
    src = "@com_google_googleapis//google/spanner/v1:spanner_proto",
    out_filename = "spanner.pb.gw.go",
    proto_imports = ["cloud.google.com/go/spanner/apiv1/spannerpb"],
    deps = ["@com_google_googleapis//google/spanner/v1:spanner_go_proto"],
)

generate_grpc_gateway(
    name = "spanner_admin_database_gateway",
    src = "@com_google_googleapis//google/spanner/admin/database/v1:database_proto",
    out_filename = "spanner_database_admin.pb.gw.go",
    proto_imports = ["cloud.google.com/go/spanner/admin/database/apiv1/databasepb"],
    deps = [
        "@com_google_googleapis//google/iam/v1:iam_go_proto",
        "@com_google_googleapis//google/spanner/admin/database/v1:database_go_proto",
    ],
)

generate_grpc_gateway(
    name = "spanner_admin_instance_gateway",
    src = "@com_google_googleapis//google/spanner/admin/instance/v1:instance_proto",
    out_filename = "spanner_instance_admin.pb.gw.go",
    proto_imports = ["cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"],
    deps = [
        "@com_google_googleapis//google/iam/v1:iam_go_proto",
        "@com_google_googleapis//google/spanner/admin/instance/v1:instance_go_proto",
    ],
)

generate_grpc_gateway(
    name = "longrunning_operations_gateway",
    src = "@com_google_googleapis//google/longrunning:operations_proto",
    out_filename = "operations.pb.gw.go",
    proto_imports = ["cloud.google.com/go/longrunning/autogen/longrunningpb"],
    yaml_config = "operations.yaml",
    deps = ["@com_google_googleapis//google/longrunning:longrunning_go_proto"],
)

go_library(
    name = "gateway",
    srcs = ["gateway.go"],
    importpath = "cloud_spanner_emulator/gateway",
    deps = [
        ":longrunning_operations_gateway",
        ":spanner_admin_database_gateway",
        ":spanner_admin_instance_gateway",
        ":spanner_gateway",
        "@com_google_googleapis//google/spanner/admin/instance/v1:instance_go_proto",
        "@grpc_ecosystem_grpc_gateway//runtime:go_default_library",
        "@org_golang_google_grpc//:go_default_library",
    ],
)
