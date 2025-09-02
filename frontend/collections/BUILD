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

package(
    default_visibility = ["//:__subpackages__"],
)

licenses(["notice"])

cc_library(
    name = "database_manager",
    srcs = ["database_manager.cc"],
    hdrs = ["database_manager.h"],
    deps = [
        "//backend/database",
        "//backend/schema/updater:schema_updater",
        "//common:clock",
        "//common:errors",
        "//common:limits",
        "//frontend/common:uris",
        "//frontend/entities:database",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_test(
    name = "database_manager_test",
    size = "small",
    srcs = [
        "database_manager_test.cc",
    ],
    deps = [
        ":database_manager",
        "//frontend/entities:database",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "multiplexed_session_transaction_manager_test",
    size = "small",
    srcs = [
        "multiplexed_session_transaction_manager_test.cc",
    ],
    deps = [
        ":multiplexed_session_transaction_manager",
        "//backend/actions:manager",
        "//backend/locking:manager",
        "//backend/schema/catalog:schema",
        "//backend/schema/catalog:versioned_catalog",
        "//backend/storage:in_memory_storage",
        "//backend/transaction:foreign_key_restrictions",
        "//backend/transaction:read_write_transaction",
        "//common:clock",
        "//frontend/entities:transaction",
        "//tests/common:proto_matchers",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_library(
    name = "session_manager",
    srcs = ["session_manager.cc"],
    hdrs = [
        "session_manager.h",
    ],
    deps = [
        ":multiplexed_session_transaction_manager",
        "//common:clock",
        "//common:errors",
        "//frontend/common:labels",
        "//frontend/common:uris",
        "//frontend/entities:database",
        "//frontend/entities:session",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "multiplexed_session_transaction_manager",
    srcs = ["multiplexed_session_transaction_manager.cc"],
    hdrs = [
        "multiplexed_session_transaction_manager.h",
    ],
    deps = [
        "//backend/common:ids",
        "//common:errors",
        "//frontend/entities:transaction",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
    ],
)

cc_test(
    name = "session_manager_test",
    size = "small",
    srcs = [
        "session_manager_test.cc",
    ],
    deps = [
        ":database_manager",
        ":session_manager",
        "//common:clock",
        "//common:errors",
        "//frontend/entities:database",
        "//frontend/entities:session",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "instance_manager",
    srcs = ["instance_manager.cc"],
    hdrs = [
        "instance_manager.h",
    ],
    deps = [
        "//common:errors",
        "//frontend/common:labels",
        "//frontend/entities:instance",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_googleapis//google/spanner/admin/instance/v1:instance_cc_grpc",
    ],
)

cc_test(
    name = "instance_manager_test",
    size = "small",
    srcs = [
        "instance_manager_test.cc",
    ],
    deps = [
        ":instance_manager",
        "//frontend/entities:instance",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "operation_manager",
    srcs = ["operation_manager.cc"],
    hdrs = ["operation_manager.h"],
    deps = [
        "//common:errors",
        "//frontend/common:uris",
        "//frontend/entities:operation",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_test(
    name = "operation_manager_test",
    srcs = [
        "operation_manager_test.cc",
    ],
    deps = [
        ":operation_manager",
        "//frontend/entities:operation",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)
