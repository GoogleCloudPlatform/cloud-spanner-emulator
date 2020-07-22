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
    name = "sql_features_view",
    hdrs = ["sql_features_view.h"],
)

cc_library(
    name = "sql_feature_filter",
    srcs = ["sql_feature_filter.cc"],
    hdrs = ["sql_feature_filter.h"],
    deps = [
        ":error_mode_util",
        ":sql_features_view",
        "//common:errors",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/base",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:language_options",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
        "@com_google_zetasql//zetasql/public/functions:datetime_cc_proto",
        "@com_google_zetasql//zetasql/resolved_ast",
        "@com_google_zetasql//zetasql/resolved_ast:resolved_ast_enums_cc_proto",
        "@com_google_zetasql//zetasql/resolved_ast:resolved_node_kind_cc_proto",
    ],
)

cc_library(
    name = "gsql_supported_functions",
    srcs = ["gsql_supported_functions.cc"],
    hdrs = ["gsql_supported_functions.h"],
    deps = [
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/public:function",
    ],
)

cc_library(
    name = "error_mode_util",
    srcs = ["error_mode_util.cc"],
    hdrs = ["error_mode_util.h"],
    deps = [
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/public:function",
    ],
)

cc_test(
    name = "gsql_supported_functions_test",
    srcs = ["gsql_supported_functions_test.cc"],
    deps = [
        ":gsql_supported_functions",
        "//backend/query:analyzer_options",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:builtin_function",
        "@com_google_zetasql//zetasql/public:builtin_function_options",
        "@com_google_zetasql//zetasql/public:function",
    ],
)

cc_library(
    name = "query_size_limits_checker",
    srcs = ["query_size_limits_checker.cc"],
    hdrs = ["query_size_limits_checker.h"],
    deps = [
        "//common:errors",
        "@com_google_absl//absl/status",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/resolved_ast",
        "@com_google_zetasql//zetasql/resolved_ast:resolved_node_kind_cc_proto",
    ],
)
