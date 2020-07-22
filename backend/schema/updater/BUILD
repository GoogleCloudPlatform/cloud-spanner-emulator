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

# Build rules for some utilties used in schema processing.

package(default_visibility = [
    "//:__subpackages__",
])

licenses(["unencumbered"])

cc_library(
    name = "ddl_type_conversion",
    srcs = ["ddl_type_conversion.cc"],
    hdrs = ["ddl_type_conversion.h"],
    deps = [
        "//backend/schema/ddl:operations_cc_proto",
        "@com_google_absl//absl/status",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_test(
    name = "ddl_type_conversion_test",
    srcs = ["ddl_type_conversion_test.cc"],
    deps = [
        ":ddl_type_conversion",
        "//backend/schema/ddl:operations_cc_proto",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_library(
    name = "global_schema_names",
    srcs = [
        "global_schema_names.cc",
    ],
    hdrs = [
        "global_schema_names.h",
    ],
    deps = [
        "//backend/common:case",
        "//backend/schema/catalog:schema",
        "//common:errors",
        "//common:limits",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_farmhash//:farmhash_fingerprint",
    ],
)

cc_test(
    name = "global_schema_names_test",
    srcs = [
        "global_schema_names_test.cc",
    ],
    deps = [
        ":global_schema_names",
        "//common:errors",
        "//common:limits",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "schema_updater",
    srcs = [
        "schema_updater.cc",
    ],
    hdrs = [
        "schema_updater.h",
    ],
    deps = [
        ":ddl_type_conversion",
        ":global_schema_names",
        ":schema_validation_context",
        "//backend/common:ids",
        "//backend/datamodel:types",
        "//backend/schema/backfills:schema_backfillers",
        "//backend/schema/builders:schema_builders",
        "//backend/schema/catalog:schema",
        "//backend/schema/ddl:operations_cc_proto",
        "//backend/schema/graph:schema_graph",
        "//backend/schema/graph:schema_graph_editor",
        "//backend/schema/parser:ddl_parser",
        "//backend/schema/parser:javacc_ddl_parser",
        "//common:errors",
        "//common:limits",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
        "@com_google_protobuf//:protobuf_headers",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_test(
    name = "schema_updater_test",
    srcs = ["schema_updater_test.cc"],
    deps = [
        "//backend/schema/updater/schema_updater_tests",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "schema_validation_context",
    hdrs = ["schema_validation_context.h"],
    deps = [
        "//backend/schema/graph:schema_node",
        "//backend/storage",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "scoped_schema_change_lock",
    hdrs = ["scoped_schema_change_lock.h"],
    deps = [
        "//backend/datamodel:key_range",
        "//backend/locking:manager",
        "//common:errors",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/time",
        "@com_google_zetasql//zetasql/base:statusor",
    ],
)
