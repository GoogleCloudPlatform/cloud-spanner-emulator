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

package(
    default_visibility = ["//:__subpackages__"],
)

licenses(["notice"])

cc_library(
    name = "action",
    srcs = ["action.cc"],
    hdrs = ["action.h"],
    deps = [
        ":context",
        ":ops",
        "//backend/schema/catalog:schema",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "context",
    hdrs = ["context.h"],
    deps = [
        ":ops",
        "//backend/datamodel:key",
        "//backend/datamodel:key_range",
        "//backend/datamodel:value",
        "//backend/schema/catalog:schema",
        "//backend/storage:iterator",
        "//common:clock",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "ops",
    srcs = ["ops.cc"],
    hdrs = ["ops.h"],
    deps = [
        "//backend/common:rows",
        "//backend/common:variant",
        "//backend/datamodel:key",
        "//backend/datamodel:value",
        "//backend/schema/catalog:schema",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "interleave",
    srcs = ["interleave.cc"],
    hdrs = ["interleave.h"],
    deps = [
        ":action",
        ":ops",
        "//backend/datamodel:key_range",
        "//backend/schema/catalog:schema",
        "//backend/storage:iterator",
        "//common:errors",
        "@com_google_absl//absl/status",
    ],
)

cc_test(
    name = "interleave_test",
    srcs = [
        "interleave_test.cc",
    ],
    deps = [
        ":context",
        ":interleave",
        ":ops",
        "//tests/common:actions",
        "//tests/common:proto_matchers",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/types:variant",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "check_constraint",
    srcs = ["check_constraint.cc"],
    hdrs = ["check_constraint.h"],
    deps = [
        ":action",
        ":context",
        ":ops",
        "//backend/datamodel:key",
        "//backend/datamodel:key_range",
        "//backend/query:analyzer_options",
        "//backend/schema/catalog:schema",
        "//backend/storage:iterator",
        "//common:errors",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:analyzer_options",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:evaluator",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "column_value",
    srcs = ["column_value.cc"],
    hdrs = ["column_value.h"],
    deps = [
        ":action",
        ":context",
        ":ops",
        "//backend/datamodel:key",
        "//common:clock",
        "//common:constants",
        "//common:errors",
        "//common:limits",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/public/functions:string",
    ],
)

cc_test(
    name = "column_value_test",
    srcs = ["column_value_test.cc"],
    deps = [
        ":column_value",
        ":context",
        ":ops",
        "//common:limits",
        "//tests/common:actions",
        "//tests/common:proto_matchers",
        "//tests/common:scoped_feature_flags_setter",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:distributions",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:variant",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:value",
        "@com_google_zetasql//zetasql/public/functions:string",
    ],
)

cc_library(
    name = "existence",
    srcs = ["existence.cc"],
    hdrs = ["existence.h"],
    deps = [
        ":action",
        ":context",
        ":ops",
        "//common:errors",
        "@com_google_absl//absl/status",
    ],
)

cc_test(
    name = "existence_test",
    srcs = [
        "existence_test.cc",
    ],
    deps = [
        ":existence",
        "//tests/common:actions",
        "//tests/common:proto_matchers",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/types:variant",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "change_stream",
    srcs = ["change_stream.cc"],
    hdrs = ["change_stream.h"],
    deps = [
        ":action",
        ":context",
        ":ops",
        "//backend/common:ids",
        "//backend/common:variant",
        "//backend/datamodel:key",
        "//backend/datamodel:key_range",
        "//backend/datamodel:value",
        "//backend/schema/catalog:schema",
        "//backend/storage",
        "//backend/storage:iterator",
        "//backend/transaction:actions",
        "//common:constants",
        "//common:errors",
        "//common:limits",
        "//third_party/spanner_pg/datatypes/extended:spanner_extended_type",
        "@com_github_googleapis_google_cloud_cpp//:spanner",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
        "@com_google_zetasql//zetasql/public:json_value",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
        "@nlohmann_json//:json",
    ],
)

cc_library(
    name = "index",
    srcs = ["index.cc"],
    hdrs = ["index.h"],
    deps = [
        ":action",
        ":ops",
        "//backend/common:indexing",
        "//backend/schema/catalog:schema",
        "//common:errors",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_library(
    name = "generated_column",
    srcs = ["generated_column.cc"],
    hdrs = ["generated_column.h"],
    deps = [
        ":action",
        ":context",
        ":ops",
        "//backend/access:write",
        "//backend/common:graph_dependency_helper",
        "//backend/common:ids",
        "//backend/datamodel:key",
        "//backend/datamodel:key_range",
        "//backend/query:analyzer_options",
        "//backend/schema/catalog:schema",
        "//backend/storage:iterator",
        "//common:errors",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:analyzer_options",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:evaluator",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "change_stream_test",
    srcs = ["change_stream_test.cc"],
    args = [
        "--spangres_use_emulator_jsonb_type=true",
        "--spangres_use_emulator_numeric_type=true",
        "--spangres_use_emulator_oid_type=true",
    ],
    deps = [
        ":change_stream",
        ":ops",
        "//backend/datamodel:key",
        "//backend/schema/catalog:schema",
        "//tests/common:actions",
        "//tests/common:proto_matchers",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/types:span",
        "@com_google_absl//absl/types:variant",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:json_value",
        "@com_google_zetasql//zetasql/public:numeric_value",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
        "@nlohmann_json//:json",
    ],
)

cc_test(
    name = "index_test",
    srcs = ["index_test.cc"],
    deps = [
        ":context",
        ":index",
        ":ops",
        "//tests/common:actions",
        "//tests/common:proto_matchers",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/types:variant",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "unique_index",
    srcs = ["unique_index.cc"],
    hdrs = ["unique_index.h"],
    deps = [
        ":action",
        ":context",
        ":ops",
        "//backend/datamodel:key_range",
        "//backend/schema/catalog:schema",
        "//backend/storage:iterator",
        "//common:errors",
        "@com_google_absl//absl/status",
    ],
)

cc_test(
    name = "unique_index_test",
    srcs = ["unique_index_test.cc"],
    deps = [
        ":unique_index",
        "//tests/common:actions",
        "//tests/common:proto_matchers",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "foreign_key",
    srcs = ["foreign_key.cc"],
    hdrs = ["foreign_key.h"],
    deps = [
        ":action",
        ":context",
        ":ops",
        "//backend/datamodel:key_range",
        "//backend/schema/catalog:schema",
        "//backend/storage:iterator",
        "//common:errors",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "foreign_key_test",
    srcs = ["foreign_key_test.cc"],
    deps = [
        ":context",
        ":foreign_key",
        ":ops",
        "//tests/common:actions",
        "//tests/common:proto_matchers",
        "//tests/common:scoped_feature_flags_setter",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/types:variant",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "foreign_key_actions",
    srcs = ["foreign_key_actions.cc"],
    hdrs = ["foreign_key_actions.h"],
    deps = [
        ":action",
        ":context",
        ":ops",
        "//backend/common:case",
        "//backend/datamodel:key",
        "//backend/datamodel:key_range",
        "//backend/schema/catalog:schema",
        "//backend/storage:iterator",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "foreign_key_actions_test",
    srcs = ["foreign_key_actions_test.cc"],
    deps = [
        ":action",
        ":foreign_key_actions",
        ":ops",
        "//backend/datamodel:key",
        "//backend/schema/catalog:schema",
        "//tests/common:actions",
        "//tests/common:proto_matchers",
        "//tests/common:scoped_feature_flags_setter",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "manager",
    srcs = ["manager.cc"],
    hdrs = ["manager.h"],
    deps = [
        ":action",
        ":check_constraint",
        ":column_value",
        ":context",
        ":existence",
        ":foreign_key",
        ":foreign_key_actions",
        ":generated_column",
        ":index",
        ":interleave",
        ":ops",
        ":unique_index",
        "//backend/access:write",
        "//backend/query:catalog",
        "//backend/query:function_catalog",
        "//backend/schema/catalog:schema",
        "//common:errors",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/container:node_hash_map",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)
