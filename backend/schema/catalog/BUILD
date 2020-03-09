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
    name = "schema",
    srcs = [
        "column.cc",
        "index.cc",
        "schema.cc",
        "table.cc",
    ],
    hdrs = [
        "column.h",
        "index.h",
        "schema.h",
        "table.h",
    ],
    deps = [
        ":schema_graph",
        ":schema_graph_editor",
        ":schema_node",
        "//backend/common:case",
        "//backend/common:ids",
        "//backend/datamodel:types",
        "//backend/schema/updater:schema_validation_context",
        "//common:errors",
        "//common:limits",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/base:status",
        "@com_google_zetasql//zetasql/public:options_cc_proto",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:type_cc_proto",
    ],
)

cc_library(
    name = "versioned_catalog",
    srcs = [
        "versioned_catalog.cc",
    ],
    hdrs = [
        "versioned_catalog.h",
    ],
    deps = [
        ":schema",
        "//common:errors",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_zetasql//zetasql/base:status",
    ],
)

cc_test(
    name = "schema_test",
    srcs = [
        "schema_test.cc",
    ],
    deps = [
        ":schema",
        "//backend/schema/builders:schema_builders",
        "//backend/schema/printer:print_ddl",
        "//common:errors",
        "//common:limits",
        "//tests/common:proto_matchers",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_test(
    name = "versioned_catalog_test",
    srcs = [
        "versioned_catalog_test.cc",
    ],
    deps = [
        ":versioned_catalog",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "schema_node",
    hdrs = ["schema_node.h"],
    deps = [
        "@com_google_zetasql//zetasql/base:status",
        "@com_google_zetasql//zetasql/base:statusor",
    ],
)

cc_library(
    name = "schema_graph",
    hdrs = ["schema_graph.h"],
    deps = [
        ":schema_node",
        ":schema_objects_pool",
    ],
)

cc_library(
    name = "schema_objects_pool",
    hdrs = ["schema_objects_pool.h"],
    deps = [
        ":schema_node",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "schema_graph_editor",
    srcs = ["schema_graph_editor.cc"],
    hdrs = ["schema_graph_editor.h"],
    deps = [
        ":schema_graph",
        ":schema_node",
        ":schema_objects_pool",
        "//backend/schema/updater:schema_validation_context",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/base",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/base:status",
        "@com_google_zetasql//zetasql/base:statusor",
    ],
)
