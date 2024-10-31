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

licenses(["unencumbered"])

cc_library(
    name = "queryable_change_stream_tvf",
    srcs = [
        "queryable_change_stream_tvf.cc",
    ],
    hdrs = [
        "queryable_change_stream_tvf.h",
    ],
    deps = [
        "//backend/schema/catalog:schema",
        "//common:constants",
        "//third_party/spanner_pg/catalog:spangres_type",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/public:analyzer",
        "@com_google_zetasql//zetasql/public:analyzer_options",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_test(
    name = "queryable_change_stream_tvf_test",
    srcs = ["queryable_change_stream_tvf_test.cc"],
    deps = [
        ":queryable_change_stream_tvf",
        "//backend/query:analyzer_options",
        "//backend/query:catalog",
        "//backend/query:function_catalog",
        "//backend/schema/catalog:schema",
        "//tests/common:proto_matchers",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:analyzer",
        "@com_google_zetasql//zetasql/public:analyzer_options",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_test(
    name = "pg_queryable_change_stream_tvf_test",
    srcs = ["pg_queryable_change_stream_tvf_test.cc"],
    args = [
        "--spangres_use_emulator_jsonb_type=true",
        "--spangres_use_emulator_numeric_type=true",
        "--spangres_use_emulator_oid_type=true",
    ],
    deps = [
        ":queryable_change_stream_tvf",
        "//backend/query:analyzer_options",
        "//backend/query:catalog",
        "//backend/query:function_catalog",
        "//backend/schema/catalog:schema",
        "//tests/common:proto_matchers",
        "//tests/common:test_schema_constructor",
        "//third_party/spanner_pg/interface:emulator_parser",
        "//third_party/spanner_pg/interface:pg_arena_factory",
        "//third_party/spanner_pg/shims:memory_context_pg_arena",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googleapis//google/spanner/admin/database/v1:database_cc_proto",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:analyzer_options",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_library(
    name = "change_stream_query_validator",
    srcs = [
        "change_stream_query_validator.cc",
    ],
    hdrs = [
        "change_stream_query_validator.h",
    ],
    deps = [
        "//backend/schema/catalog:schema",
        "//common:constants",
        "//common:errors",
        "//common:limits",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_zetasql//zetasql/public:value",
        "@com_google_zetasql//zetasql/resolved_ast",
        "@com_google_zetasql//zetasql/resolved_ast:resolved_node_kind_cc_proto",
    ],
)

cc_test(
    name = "change_stream_query_validator_test",
    srcs = ["change_stream_query_validator_test.cc"],
    deps = [
        ":change_stream_query_validator",
        "//backend/query:analyzer_options",
        "//backend/query:catalog",
        "//backend/query:function_catalog",
        "//backend/schema/catalog:schema",
        "//common:errors",
        "//common:limits",
        "//tests/common:proto_matchers",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:analyzer",
        "@com_google_zetasql//zetasql/public:analyzer_options",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "pg_change_stream_query_validator_test",
    srcs = ["pg_change_stream_query_validator_test.cc"],
    args = [
        "--spangres_use_emulator_jsonb_type=true",
        "--spangres_use_emulator_numeric_type=true",
        "--spangres_use_emulator_oid_type=true",
    ],
    deps = [
        ":change_stream_query_validator",
        "//backend/query:analyzer_options",
        "//backend/query:catalog",
        "//backend/query:function_catalog",
        "//backend/schema/catalog:schema",
        "//common:errors",
        "//common:limits",
        "//tests/common:proto_matchers",
        "//tests/common:test_schema_constructor",
        "//third_party/spanner_pg/interface:emulator_parser",
        "//third_party/spanner_pg/interface:pg_arena_factory",
        "//third_party/spanner_pg/shims:memory_context_pg_arena",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_googleapis//google/spanner/admin/database/v1:database_cc_proto",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:analyzer_options",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)
