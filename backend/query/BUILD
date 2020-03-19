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
    name = "query_engine",
    srcs = ["query_engine.cc"],
    hdrs = ["query_engine.h"],
    deps = [
        ":analyzer_options",
        ":catalog",
        ":function_catalog",
        ":hint_rewriter",
        ":hint_validator",
        "//backend/access:read",
        "//backend/access:write",
        "//backend/common:case",
        "//backend/datamodel:value",
        "//backend/schema/catalog:schema",
        "//common:constants",
        "//common:errors",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/base:status",
        "@com_google_zetasql//zetasql/base:statusor",
        "@com_google_zetasql//zetasql/public:analyzer",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:evaluator",
        "@com_google_zetasql//zetasql/public:evaluator_table_iterator",
        "@com_google_zetasql//zetasql/public:language_options",
        "@com_google_zetasql//zetasql/public:options_cc_proto",
        "@com_google_zetasql//zetasql/public:parse_helpers",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
        "@com_google_zetasql//zetasql/resolved_ast",
        "@com_google_zetasql//zetasql/resolved_ast:resolved_node_kind_cc_proto",
    ],
)

cc_test(
    name = "query_engine_test",
    srcs = [
        "query_engine_test.cc",
    ],
    deps = [
        ":catalog",
        ":query_engine",
        "//backend/access:read",
        "//backend/access:write",
        "//backend/datamodel:key_set",
        "//backend/datamodel:value",
        "//backend/schema/catalog:schema",
        "//tests/common:proto_matchers",
        "//tests/common:test_row_reader",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/memory",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base:status",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "analyzer_options",
    srcs = ["analyzer_options.cc"],
    hdrs = ["analyzer_options.h"],
    deps = [
        "@com_google_absl//absl/time",
        "@com_google_zetasql//zetasql/public:analyzer",
        "@com_google_zetasql//zetasql/public:options_cc_proto",
    ],
)

cc_library(
    name = "hint_validator",
    srcs = ["hint_validator.cc"],
    hdrs = ["hint_validator.h"],
    deps = [
        "//backend/common:case",
        "//backend/schema/catalog:schema",
        "//common:errors",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/base:status",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/resolved_ast",
    ],
)

cc_test(
    name = "hint_validator_test",
    srcs = [
        "hint_validator_test.cc",
    ],
    deps = [
        ":hint_validator",
        ":queryable_table",
        "//backend/schema/catalog:schema",
        "//tests/common:proto_matchers",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base:status",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/resolved_ast",
    ],
)

cc_library(
    name = "hint_rewriter",
    srcs = ["hint_rewriter.cc"],
    hdrs = ["hint_rewriter.h"],
    deps = [
        "@com_google_zetasql//zetasql/base:status",
        "@com_google_zetasql//zetasql/resolved_ast",
    ],
)

cc_test(
    name = "hint_rewriter_test",
    srcs = [
        "hint_rewriter_test.cc",
    ],
    deps = [
        ":hint_rewriter",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:simple_catalog",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/resolved_ast",
    ],
)

cc_library(
    name = "information_schema_catalog",
    srcs = ["information_schema_catalog.cc"],
    hdrs = ["information_schema_catalog.h"],
    deps = [
        "//backend/schema/catalog:schema",
        "//backend/schema/printer:print_ddl",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:variant",
        "@com_google_zetasql//zetasql/public:simple_catalog",
    ],
)

cc_library(
    name = "catalog",
    srcs = [
        "catalog.cc",
    ],
    hdrs = [
        "catalog.h",
    ],
    deps = [
        ":function_catalog",
        ":information_schema_catalog",
        ":queryable_table",
        "//backend/access:read",
        "//backend/common:case",
        "//backend/schema/catalog:schema",
        "//common:errors",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/base:status",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:simple_catalog",
    ],
)

cc_test(
    name = "catalog_test",
    srcs = [
        "catalog_test.cc",
    ],
    deps = [
        ":catalog",
        ":function_catalog",
        "//backend/schema/catalog:schema",
        "//tests/common:proto_matchers",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base:status",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:analyzer",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:function",
    ],
)

cc_test(
    name = "queryable_table_test",
    srcs = ["queryable_table_test.cc"],
    deps = [
        ":catalog",
        ":queryable_column",
        ":queryable_table",
        "//backend/access:read",
        "//tests/common:proto_matchers",
        "//tests/common:test_row_cursor",
        "//tests/common:test_row_reader",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "queryable_column",
    srcs = [
    ],
    hdrs = [
        "queryable_column.h",
    ],
    deps = [
        "//backend/schema/catalog:schema",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_library(
    name = "queryable_table",
    srcs = [
        "queryable_table.cc",
    ],
    hdrs = [
        "queryable_table.h",
    ],
    deps = [
        ":queryable_column",
        "//backend/access:read",
        "//backend/schema/catalog:schema",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/base:status",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:evaluator_table_iterator",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "function_catalog",
    srcs = [
        "function_catalog.cc",
    ],
    hdrs = [
        "function_catalog.h",
    ],
    deps = [
        "//backend/common:case",
        "//common:constants",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/base:status",
        "@com_google_zetasql//zetasql/public:builtin_function",
        "@com_google_zetasql//zetasql/public:builtin_function_options",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:type",
    ],
)
