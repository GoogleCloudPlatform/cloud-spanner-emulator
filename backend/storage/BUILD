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
    name = "iterator",
    hdrs = [
        "iterator.h",
    ],
    deps = [
        "//backend/datamodel:key",
        "@com_google_absl//absl/status",
        "@com_google_googlesql//googlesql/public:value",
    ],
)

cc_library(
    name = "storage",
    hdrs = [
        "storage.h",
    ],
    deps = [
        ":iterator",
        "//backend/common:ids",
        "//backend/datamodel:key",
        "//backend/datamodel:key_range",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/time",
        "@com_google_googlesql//googlesql/public:value",
    ],
)

cc_library(
    name = "in_memory_storage",
    srcs = ["in_memory_storage.cc"],
    hdrs = [
        "in_memory_storage.h",
    ],
    deps = [
        ":in_memory_iterator",
        ":iterator",
        ":storage",
        "//backend/common:ids",
        "//backend/datamodel:key",
        "//backend/datamodel:key_range",
        "//common:errors",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_googlesql//googlesql/public:value",
    ],
)

cc_test(
    name = "in_memory_storage_test",
    srcs = [
        "in_memory_storage_test.cc",
    ],
    deps = [
        ":in_memory_storage",
        ":iterator",
        "//backend/datamodel:key_range",
        "//tests/common:proto_matchers",
        "@com_github_google_benchmark//:benchmark",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/time",
        "@com_google_googlesql//googlesql/base/testing:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "in_memory_iterator",
    srcs = ["in_memory_iterator.cc"],
    hdrs = [
        "in_memory_iterator.h",
    ],
    deps = [
        ":iterator",
        "//backend/datamodel:key",
        "@com_google_absl//absl/status",
        "@com_google_googlesql//googlesql/public:value",
    ],
)

cc_test(
    name = "in_memory_iterator_test",
    srcs = [
        "in_memory_iterator_test.cc",
    ],
    deps = [
        ":in_memory_iterator",
        "//backend/datamodel:key",
        "//tests/common:proto_matchers",
        "@com_github_google_benchmark//:benchmark",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googlesql//googlesql/base/testing:status_matchers",
        "@com_google_googlesql//googlesql/public:value",
        "@com_google_googletest//:gtest_main",
    ],
)

proto_library(
    name = "persistence_proto",
    srcs = ["persistence.proto"],
)

cc_proto_library(
    name = "persistence_cc_proto",
    deps = [":persistence_proto"],
)

cc_library(
    name = "value_serializer",
    srcs = ["value_serializer.cc"],
    hdrs = ["value_serializer.h"],
    deps = [
        ":persistence_cc_proto",
        "//backend/datamodel:key",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:type_cc_proto",
        "@com_google_zetasql//zetasql/public:value",
        "@com_google_zetasql//zetasql/public:value_cc_proto",
    ],
)

cc_library(
    name = "wal_writer",
    srcs = ["wal_writer.cc"],
    hdrs = ["wal_writer.h"],
    deps = [
        ":persistence_cc_proto",
        "@com_google_absl//absl/crc:crc32c",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_library(
    name = "persistent_storage",
    srcs = ["persistent_storage.cc"],
    hdrs = ["persistent_storage.h"],
    deps = [
        ":in_memory_storage",
        ":iterator",
        ":persistence_cc_proto",
        ":storage",
        ":value_serializer",
        ":wal_writer",
        "//backend/common:ids",
        "//backend/datamodel:key",
        "//backend/datamodel:key_range",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/time",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "snapshot_writer",
    srcs = ["snapshot_writer.cc"],
    hdrs = ["snapshot_writer.h"],
    deps = [
        ":persistence_cc_proto",
        ":value_serializer",
        "//backend/access:read",
        "//backend/common:ids",
        "//backend/database:database",
        "//backend/datamodel:key_set",
        "//backend/schema/catalog:schema",
        "//backend/schema/printer:print_ddl",
        "//backend/transaction:read_only_transaction",
        "//frontend/collections:database_manager",
        "//frontend/collections:instance_manager",
        "//frontend/entities:database",
        "//frontend/entities:instance",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_googleapis//google/spanner/admin/instance/v1:instance_cc_proto",
        "@com_google_googlesql//googlesql/base:status",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "snapshot_loader",
    srcs = ["snapshot_loader.cc"],
    hdrs = ["snapshot_loader.h"],
    deps = [
        ":persistence_cc_proto",
        ":value_serializer",
        "//backend/access:write",
        "//backend/common:ids",
        "//backend/database:database",
        "//backend/schema/catalog:schema",
        "//backend/schema/updater:schema_updater",
        "//backend/transaction:read_write_transaction",
        "//frontend/collections:database_manager",
        "//frontend/collections:instance_manager",
        "//frontend/entities:database",
        "//frontend/server:environment",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_googleapis//google/spanner/admin/database/v1:database_cc_proto",
        "@com_google_googleapis//google/spanner/admin/instance/v1:instance_cc_proto",
        "@com_google_googlesql//googlesql/base:status",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "value_serializer_test",
    srcs = ["value_serializer_test.cc"],
    deps = [
        ":value_serializer",
        "//backend/datamodel:key",
        "//tests/common:proto_matchers",
        "@com_github_google_benchmark//:benchmark",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
        "@com_google_googlesql//googlesql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "wal_writer_test",
    srcs = ["wal_writer_test.cc"],
    deps = [
        ":persistence_cc_proto",
        ":wal_writer",
        "//tests/common:proto_matchers",
        "@com_github_google_benchmark//:benchmark",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest_main",
        "@com_google_googlesql//googlesql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "persistent_storage_test",
    srcs = ["persistent_storage_test.cc"],
    deps = [
        ":persistent_storage",
        ":persistence_cc_proto",
        ":wal_writer",
        "//backend/datamodel:key",
        "//backend/datamodel:key_range",
        "//tests/common:proto_matchers",
        "@com_github_google_benchmark//:benchmark",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
        "@com_google_googlesql//googlesql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:value",
    ],
)
