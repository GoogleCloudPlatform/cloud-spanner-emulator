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

proto_library(
    name = "property_graph_proto",
    srcs = ["property_graph.proto"],
)

cc_proto_library(
    name = "property_graph_cc_proto",
    deps = [":property_graph_proto"],
)

cc_library(
    name = "schema",
    srcs = [
        "change_stream.cc",
        "check_constraint.cc",
        "column.cc",
        "database_options.cc",
        "foreign_key.cc",
        "index.cc",
        "locality_group.cc",
        "model.cc",
        "named_schema.cc",
        "placement.cc",
        "property_graph.cc",
        "schema.cc",
        "sequence.cc",
        "table.cc",
        "udf.cc",
        "view.cc",
    ],
    hdrs = [
        "change_stream.h",
        "check_constraint.h",
        "column.h",
        "database_options.h",
        "foreign_key.h",
        "index.h",
        "locality_group.h",
        "model.h",
        "named_schema.h",
        "placement.h",
        "property_graph.h",
        "schema.h",
        "sequence.h",
        "table.h",
        "udf.h",
        "view.h",
    ],
    deps = [
        ":property_graph_cc_proto",
        ":proto_bundle",
        "//backend/common:case",
        "//backend/common:ids",
        "//backend/datamodel:types",
        "//backend/schema/ddl:operations_cc_proto",
        "//backend/schema/graph:schema_graph",
        "//backend/schema/graph:schema_graph_editor",
        "//backend/schema/graph:schema_node",
        "//backend/schema/parser:ddl_parser",
        "//backend/schema/updater:ddl_type_conversion",
        "//backend/schema/updater:schema_validation_context",
        "//common:bit_reverse",
        "//common:constants",
        "//common:errors",
        "//common:limits",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:absl_log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:span",
        "@com_google_googleapis//google/spanner/admin/database/v1:database_cc_proto",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:function_cc_proto",
        "@com_google_zetasql//zetasql/public:options_cc_proto",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:type_cc_proto",
        "@com_google_zetasql//zetasql/public:value",
        "@com_googlesource_code_re2//:re2",
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
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
    ],
)

cc_test(
    name = "sequence_test",
    srcs = [
        "sequence_test.cc",
    ],
    deps = [
        ":schema",
        "//backend/schema/builders:schema_builders",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/synchronization",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "schema_test",
    srcs = [
        "schema_test.cc",
    ],
    args = [
        "--spangres_use_emulator_jsonb_type=true",
        "--spangres_use_emulator_numeric_type=true",
        "--spangres_use_emulator_oid_type=true",
    ],
    deps = [
        ":schema",
        "//backend/schema/builders:schema_builders",
        "//backend/schema/printer:print_ddl",
        "//backend/schema/updater:schema_validation_context",
        "//common:errors",
        "//common:limits",
        "//tests/common:proto_matchers",
        "//tests/common:scoped_feature_flags_setter",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/time",
        "@com_google_googleapis//google/spanner/admin/database/v1:database_cc_proto",
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
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "foreign_key_test",
    srcs = [
        "foreign_key_test.cc",
    ],
    deps = [
        ":schema",
        "//tests/common:proto_matchers",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "proto_bundle",
    srcs = ["proto_bundle.cc"],
    hdrs = ["proto_bundle.h"],
    deps = [
        "//common:errors",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_protobuf//:protobuf",
        "@com_google_protobuf//:protobuf_headers",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_test(
    name = "proto_bundle_test",
    srcs = [
        "proto_bundle_test.cc",
    ],
    deps = [
        ":proto_bundle",
        "//tests/common:proto_matchers",
        "//tests/common:test_2_cc_proto",
        "//tests/common:test_cc_proto",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)
