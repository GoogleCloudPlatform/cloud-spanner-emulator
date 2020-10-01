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

package(default_visibility = ["//:__subpackages__"])

licenses(["unencumbered"])

cc_library(
    name = "request_context",
    srcs = ["request_context.cc"],
    hdrs = ["request_context.h"],
    deps = [
        ":environment",
        "//frontend/common:uris",
        "//frontend/entities:instance",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_zetasql//zetasql/base:statusor",
    ],
)

cc_test(
    name = "request_context_test",
    srcs = ["request_context_test.cc"],
    deps = [
        ":request_context",
        "//frontend/common:uris",
        "//tests/common:proto_matchers",
        "//tests/common:test_env",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_googleapis//google/spanner/admin/instance/v1:instance_cc_grpc",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "handler",
    srcs = ["handler.cc"],
    hdrs = ["handler.h"],
    deps = [
        ":request_context",
        "//common:config",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/synchronization",
        "@com_google_zetasql//zetasql/base",
    ],
)

cc_test(
    name = "handler_test",
    srcs = ["handler_test.cc"],
    deps = [
        ":handler",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_grpc",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "server",
    srcs = [
        "server.cc",
    ],
    hdrs = [
        "server.h",
    ],
    deps = [
        ":environment",
        ":handler",
        ":request_context",
        "//common:constants",
        "//common:errors",
        "//common:limits",
        "//frontend/common:status",
        "//frontend/handlers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/memory",
        "@com_google_googleapis//google/iam/v1:iam_policy_cc_proto",
        "@com_google_googleapis//google/iam/v1:policy_cc_proto",
        "@com_google_googleapis//google/rpc:error_details_cc_proto",
        "@com_google_googleapis//google/spanner/admin/database/v1:database_cc_grpc",
        "@com_google_googleapis//google/spanner/admin/instance/v1:instance_cc_grpc",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_grpc",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_zetasql//zetasql/base",
    ],
)

cc_library(
    name = "environment",
    hdrs = [
        "environment.h",
    ],
    deps = [
        "//common:clock",
        "//frontend/collections:database_manager",
        "//frontend/collections:instance_manager",
        "//frontend/collections:operation_manager",
        "//frontend/collections:session_manager",
        "@com_github_grpc_grpc//:grpc++",
    ],
)
