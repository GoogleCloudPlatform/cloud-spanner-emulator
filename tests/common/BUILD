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
    name = "test_row_cursor",
    srcs = [],
    hdrs = [
        "row_cursor.h",
    ],
    deps = [
        "//backend/access:read",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "test_row_reader",
    srcs = [],
    hdrs = [
        "row_reader.h",
    ],
    deps = [
        ":test_row_cursor",
        "//backend/access:read",
        "//backend/common:case",
        "//common:errors",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "test_env",
    testonly = 1,
    srcs = [
        "test_env.cc",
    ],
    hdrs = [
        "test_env.h",
    ],
    deps = [
        ":proto_matchers",
        "//frontend/common:uris",
        "//frontend/server",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_googleapis//google/spanner/admin/database/v1:database_cc_grpc",
        "@com_google_googleapis//google/spanner/admin/instance/v1:instance_cc_grpc",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_grpc",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base",
        "@com_google_zetasql//zetasql/base:statusor",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "proto_matchers",
    testonly = 1,
    srcs = ["proto_matchers.cc"],
    hdrs = ["proto_matchers.h"],
    deps = [
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@com_google_protobuf//:protobuf",
        "@com_google_protobuf//:protobuf_headers",
        "@com_google_zetasql//zetasql/base",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "test_schema_constructor",
    testonly = 1,
    hdrs = [
        "schema_constructor.h",
    ],
    deps = [
        "//backend/schema/catalog:schema",
        "//backend/schema/updater:schema_updater",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_zetasql//zetasql/base:statusor",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_library(
    name = "actions",
    testonly = 1,
    srcs = ["actions.cc"],
    hdrs = ["actions.h"],
    deps = [
        ":proto_matchers",
        "//backend/actions:context",
        "//backend/actions:ops",
        "//backend/storage:in_memory_iterator",
        "//backend/storage:in_memory_storage",
        "//common:clock",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base:statusor",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "chunking",
    testonly = 1,
    srcs = ["chunking.cc"],
    hdrs = ["chunking.h"],
    deps = [
        "//common:errors",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_grpc",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_zetasql//zetasql/base:statusor",
    ],
)

cc_library(
    name = "file_based_test_runner",
    testonly = 1,
    srcs = ["file_based_test_runner.cc"],
    hdrs = ["file_based_test_runner.h"],
    deps = [
        ":proto_matchers",
        "@bazel_tools//tools/cpp/runfiles",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "scoped_feature_flags_setter",
    hdrs = [
        "scoped_feature_flags_setter.h",
    ],
    deps = [
        "//common:feature_flags",
    ],
)
