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

load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_cc//cc:cc_test.bzl", "cc_test")

# Add go_grpc_library rules using go_grpc_library targets defined by GoogleAPIs is not supported
# when building via Bazel Central Registry.
load("@rules_proto_grpc_go//:defs.bzl", "go_grpc_library")

package(
    default_visibility = ["//:__subpackages__"],
)

licenses(["notice"])

cc_library(
    name = "clock",
    srcs = ["clock.cc"],
    hdrs = ["clock.h"],
    deps = [
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
    ],
)

cc_test(
    name = "clock_test",
    srcs = ["clock_test.cc"],
    deps = [
        ":clock",
        "//tests/common:proto_matchers",
        "@com_github_google_benchmark//:benchmark",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googlesql//googlesql/base/testing:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "constants",
    hdrs = [
        "constants.h",
    ],
    deps = ["@com_google_googlesql//googlesql/public:type"],
)

cc_library(
    name = "change_stream",
    srcs = [
        "change_stream.cc",
    ],
    hdrs = [
        "change_stream.h",
    ],
    deps = [
        ":constants",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "errors",
    srcs = [
        "errors.cc",
    ],
    hdrs = [
        "errors.h",
    ],
    deps = [
        ":constants",
        ":feature_flags",
        ":limits",
        "//backend/common:ids",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/time",
        "@com_google_googleapis//google/rpc:error_details_cc_proto",
    ],
)

cc_library(
    name = "limits",
    hdrs = [
        "limits.h",
    ],
)

cc_library(
    name = "config",
    srcs = ["config.cc"],
    hdrs = ["config.h"],
    deps = ["@com_google_absl//absl/flags:flag"],
)

cc_library(
    name = "feature_flags",
    hdrs = ["feature_flags.h"],
    deps = [
        "@com_google_absl//absl/synchronization",
    ],
)

cc_test(
    name = "feature_flags_test",
    srcs = ["feature_flags_test.cc"],
    deps = [
        ":feature_flags",
        "//tests/common:proto_matchers",
        "//tests/common:scoped_feature_flags_setter",
        "@com_github_google_benchmark//:benchmark",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googlesql//googlesql/base/testing:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "bit_reverse",
    srcs = [
        "bit_reverse.cc",
    ],
    hdrs = [
        "bit_reverse.h",
    ],
    deps = [
        "@com_google_googlesql//googlesql/base:bits",
    ],
)

cc_library(
    name = "pg_literals",
    srcs = [
        "pg_literals.cc",
    ],
    hdrs = [
        "pg_literals.h",
    ],
    deps = [
        "@com_google_absl//absl/strings",
        "@com_googlesource_code_re2//:re2",
    ],
)

cc_test(
    name = "pg_literals_test",
    srcs = ["pg_literals_test.cc"],
    deps = [
        ":pg_literals",
        "//tests/common:proto_matchers",
        "@com_github_google_benchmark//:benchmark",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googlesql//googlesql/base/testing:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

go_grpc_library(
    name = "instance_go_proto",
    importpath = "cloud.google.com/go/spanner/admin/instance/apiv1/instancepb",
    protos = ["@com_google_googleapis//google/spanner/admin/instance/v1:instance_proto"],
    deps = [
        ":iam_go_proto",
        ":longrunning_go_proto",
        "@org_golang_google_genproto_googleapis_api//annotations",
    ],
)

go_grpc_library(
    name = "database_go_proto",
    importpath = "cloud.google.com/go/spanner/admin/database/apiv1/databasepb",
    protos = ["@com_google_googleapis//google/spanner/admin/database/v1:database_proto"],
    deps = [
        ":iam_go_proto",
        ":longrunning_go_proto",
        "@org_golang_google_genproto_googleapis_api//annotations",
        "@org_golang_google_genproto_googleapis_rpc//status",
    ],
)

go_grpc_library(
    name = "spanner_go_proto",
    importpath = "cloud.google.com/go/spanner/apiv1/spannerpb",
    protos = ["@com_google_googleapis//google/spanner/v1:spanner_proto"],
    deps = [
        "@org_golang_google_genproto_googleapis_api//annotations",
        "@org_golang_google_genproto_googleapis_rpc//status",
    ],
)

go_grpc_library(
    name = "longrunning_go_proto",
    importpath = "cloud.google.com/go/longrunning/autogen/longrunningpb",
    protos = ["@com_google_googleapis//google/longrunning:operations_proto"],
    deps = [
        "@org_golang_google_genproto_googleapis_api//annotations",
        "@org_golang_google_genproto_googleapis_rpc//status",
        "@org_golang_google_protobuf//types/descriptorpb",
        "@org_golang_google_protobuf//types/known/anypb",
        "@org_golang_google_protobuf//types/known/durationpb",
        "@org_golang_google_protobuf//types/known/emptypb",
    ],
)

go_grpc_library(
    name = "iam_go_proto",
    importpath = "cloud.google.com/go/iam/apiv1/iampb",
    protos = [
        "@com_google_googleapis//google/iam/v1:iam_policy_proto",
        "@com_google_googleapis//google/iam/v1:options_proto",
        "@com_google_googleapis//google/iam/v1:policy_proto",
        "@com_google_googleapis//google/iam/v1:resource_policy_member_proto",
    ],
    deps = [
        "@org_golang_google_genproto//googleapis/type/expr",
        "@org_golang_google_genproto_googleapis_api//annotations",
    ],
)
