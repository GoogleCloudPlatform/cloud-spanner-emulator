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

load("@rules_cc//cc:cc_binary.bzl", "cc_binary")
load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_cc//cc:cc_test.bzl", "cc_test")

package(
    default_visibility = ["//:__subpackages__"],
)

licenses(["notice"])

cc_library(
    name = "query_engine_options",
    hdrs = ["query_engine_options.h"],
)

cc_library(
    name = "query_context",
    hdrs = ["query_context.h"],
    deps = [
        "//backend/access:read",
        "//backend/access:write",
        "//backend/schema/catalog:schema",
        "//backend/transaction:commit_timestamp",
    ],
)

cc_library(
    name = "insert_on_conflict_dml_execution",
    srcs = ["insert_on_conflict_dml_execution.cc"],
    hdrs = ["insert_on_conflict_dml_execution.h"],
    deps = [
        ":catalog",
        ":function_catalog",
        ":query_engine_util",
        "//backend/access:read",
        "//backend/access:write",
        "//backend/datamodel:key",
        "//backend/schema/catalog:schema",
        "//common:constants",
        "//common:errors",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:analyzer_options",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:id_string",
        "@com_google_zetasql//zetasql/public:options_cc_proto",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/resolved_ast",
        "@com_google_zetasql//zetasql/resolved_ast:rewrite_utils",
    ],
)

cc_library(
    name = "query_engine_util",
    srcs = ["query_engine_util.cc"],
    hdrs = ["query_engine_util.h"],
    deps = [
        ":function_catalog",
        "//backend/access:read",
        "//backend/access:write",
        "//common:errors",
        "//common:limits",
        "//third_party/spanner_pg/interface:emulator_parser",
        "//third_party/spanner_pg/interface:pg_arena_factory",
        "//third_party/spanner_pg/shims:memory_context_pg_arena",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_zetasql//zetasql/analyzer:function_signature_matcher",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:analyzer",
        "@com_google_zetasql//zetasql/public:analyzer_options",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:coercer",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:options_cc_proto",
        "@com_google_zetasql//zetasql/public:signature_match_result",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/resolved_ast",
    ],
)

cc_library(
    name = "query_engine",
    srcs = ["query_engine.cc"],
    hdrs = ["query_engine.h"],
    deps = [
        ":analyzer_options",
        ":ann_functions_rewriter",
        ":ann_validator",
        ":catalog",
        ":dml_query_validator",
        ":function_catalog",
        ":hint_rewriter",
        ":index_hint_validator",
        ":insert_on_conflict_dml_execution",
        ":partitionability_validator",
        ":partitioned_dml_validator",
        ":query_context",
        ":query_engine_options",
        ":query_engine_util",
        ":query_validator",
        ":queryable_column",
        ":queryable_view",
        "//backend/access:read",
        "//backend/access:write",
        "//backend/common:case",
        "//backend/datamodel:key",
        "//backend/datamodel:key_set",
        "//backend/datamodel:value",
        "//backend/query/change_stream:change_stream_query_validator",
        "//backend/query/feature_filter:query_size_limits_checker",
        "//backend/schema/catalog:schema",
        "//backend/transaction:commit_timestamp",
        "//common:config",
        "//common:constants",
        "//common:errors",
        "//common:feature_flags",
        "//frontend/converters:values",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/time",
        "@com_google_googleapis//google/spanner/admin/database/v1:database_cc_proto",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_grpc",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:analyzer",
        "@com_google_zetasql//zetasql/public:analyzer_options",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:evaluator",
        "@com_google_zetasql//zetasql/public:evaluator_table_iterator",
        "@com_google_zetasql//zetasql/public:language_options",
        "@com_google_zetasql//zetasql/public:options_cc_proto",
        "@com_google_zetasql//zetasql/public:parse_helpers",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
        "@com_google_zetasql//zetasql/resolved_ast",
        "@com_google_zetasql//zetasql/resolved_ast:resolved_ast_enums_cc_proto",
        "@com_google_zetasql//zetasql/resolved_ast:resolved_node_kind_cc_proto",
    ],
)

cc_test(
    name = "query_engine_test",
    srcs = [
        "query_engine_test.cc",
    ],
    args = [
        "--spangres_use_emulator_jsonb_type=true",
        "--spangres_use_emulator_numeric_type=true",
        "--spangres_use_emulator_oid_type=true",
    ],
    deps = [
        ":query_context",
        ":query_engine",
        "//backend/access:read",
        "//backend/access:write",
        "//backend/actions:manager",
        "//backend/datamodel:key",
        "//backend/datamodel:key_set",
        "//backend/datamodel:value",
        "//backend/schema/catalog:schema",
        "//common:feature_flags",
        "//common:limits",
        "//tests/common:proto_matchers",
        "//tests/common:scoped_feature_flags_setter",
        "//tests/common:test_cc_proto",
        "//tests/common:test_row_reader",
        "//tests/common:test_schema_constructor",
        "//third_party/spanner_pg/datatypes/extended:pg_jsonb_type",
        "//third_party/spanner_pg/datatypes/extended:pg_numeric_type",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_grpc",
        "@com_google_googletest//:gtest_main",
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
        "//common:constants",
        "//common:feature_flags",
        "@com_google_absl//absl/time",
        "@com_google_googleapis//google/spanner/admin/database/v1:database_cc_proto",
        "@com_google_zetasql//zetasql/public:analyzer_options",
        "@com_google_zetasql//zetasql/public:builtin_function_options",
        "@com_google_zetasql//zetasql/public:language_options",
        "@com_google_zetasql//zetasql/public:options_cc_proto",
        "@com_google_zetasql//zetasql/resolved_ast:resolved_node_kind_cc_proto",
    ],
)

cc_library(
    name = "query_validator",
    srcs = ["query_validator.cc"],
    hdrs = ["query_validator.h"],
    deps = [
        ":analyzer_options",
        ":query_context",
        ":query_engine_options",
        ":queryable_table",
        "//backend/query/feature_filter:gsql_supported_functions",
        "//backend/query/feature_filter:sql_feature_filter",
        "//backend/query/feature_filter:sql_features_view",
        "//backend/schema/catalog:schema",
        "//backend/transaction:read_write_transaction",
        "//common:constants",
        "//common:errors",
        "//common:pg_literals",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:language_options",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/resolved_ast",
        "@com_google_zetasql//zetasql/resolved_ast:resolved_node_kind_cc_proto",
    ],
)

cc_library(
    name = "dml_query_validator",
    srcs = ["dml_query_validator.cc"],
    hdrs = ["dml_query_validator.h"],
    deps = [
        ":query_context",
        ":query_engine_options",
        ":query_validator",
        "//common:constants",
        "//common:errors",
        "@com_google_absl//absl/status",
        "@com_google_zetasql//zetasql/resolved_ast",
    ],
)

cc_test(
    name = "query_validator_test",
    srcs = [
        "query_validator_test.cc",
    ],
    deps = [
        ":query_engine_options",
        ":query_validator",
        ":queryable_table",
        "//backend/schema/catalog:schema",
        "//tests/common:proto_matchers",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:builtin_function",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/resolved_ast",
        "@com_google_zetasql//zetasql/resolved_ast:make_node_vector",
    ],
)

cc_library(
    name = "hint_rewriter",
    srcs = ["hint_rewriter.cc"],
    hdrs = ["hint_rewriter.h"],
    deps = [
        "@com_google_absl//absl/status",
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
    name = "ann_functions_rewriter",
    srcs = ["ann_functions_rewriter.cc"],
    hdrs = ["ann_functions_rewriter.h"],
    deps = [
        "//common:errors",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:json_value",
        "@com_google_zetasql//zetasql/public:value",
        "@com_google_zetasql//zetasql/resolved_ast",
    ],
)

cc_library(
    name = "ann_validator",
    srcs = ["ann_validator.cc"],
    hdrs = ["ann_validator.h"],
    deps = [
        ":ann_functions_rewriter",
        "//backend/schema/catalog:schema",
        "//backend/schema/ddl:operations_cc_proto",
        "//common:errors",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:value",
        "@com_google_zetasql//zetasql/resolved_ast",
    ],
)

cc_binary(
    name = "populate_info_schema_columns_metadata",
    srcs = ["populate_info_schema_columns_metadata.cc"],
    data = [
        ":info_schema_columns_metadata.csv",
        ":info_schema_columns_metadata_for_index.csv",
        ":pg_info_schema_columns_metadata.csv",
        ":pg_info_schema_columns_metadata_for_index.csv",
    ],
    deps = [
        "@com_google_absl//absl/flags:parse",
        "@com_google_absl//absl/strings",
        "@com_google_googleapis//google/spanner/admin/database/v1:database_cc_proto",
        "@com_google_zetasql//zetasql/base",
        "@com_googlesource_code_riegeli//riegeli/bytes:cfile_reader",
        "@com_googlesource_code_riegeli//riegeli/csv:csv_reader",
        "@com_googlesource_code_riegeli//riegeli/csv:csv_record",
    ],
)

genrule(
    name = "gen_populate_info_schema_columns_metadata",
    srcs = [
        "info_schema_columns_metadata.csv",
        "info_schema_columns_metadata_for_index.csv",
        ":pg_catalog_columns_metadata.csv",
        ":pg_info_schema_columns_metadata.csv",
        ":pg_info_schema_columns_metadata_for_index.csv",
        "spanner_sys_columns_metadata.csv",
    ],
    outs = ["info_schema_columns_metadata_values.h"],
    cmd = "$(location :populate_info_schema_columns_metadata) > $@",
    tools = [":populate_info_schema_columns_metadata"],
)

cc_library(
    name = "spanner_sys_catalog",
    srcs = ["spanner_sys_catalog.cc"],
    hdrs = [
        "spanner_sys_catalog.h",
    ],
    deps = [
        ":info_schema_columns_metadata_values",
        ":tables_from_metadata",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/base:no_destructor",
        "@com_google_zetasql//zetasql/public:simple_catalog",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "info_schema_columns_metadata_values",
    hdrs = [
        "info_schema_columns_metadata_values.h",
    ],
    deps = [
        "@com_google_zetasql//zetasql/base:no_destructor",
    ],
)

cc_library(
    name = "tables_from_metadata",
    srcs = [
        "tables_from_metadata.cc",
    ],
    hdrs = [
        "tables_from_metadata.h",
    ],
    deps = [
        ":info_schema_columns_metadata_values",
        "//third_party/spanner_pg/datatypes/extended:pg_oid_type",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/base:no_destructor",
        "@com_google_zetasql//zetasql/public:simple_catalog",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "tables_from_metadata_test",
    srcs = ["tables_from_metadata_test.cc"],
    deps = [
        ":tables_from_metadata",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base:no_destructor",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "information_schema_catalog",
    srcs = ["information_schema_catalog.cc"],
    hdrs = [
        "information_schema_catalog.h",
    ],
    deps = [
        ":analyzer_options",
        ":info_schema_columns_metadata_values",
        ":spanner_sys_catalog",
        ":tables_from_metadata",
        "//backend/schema/catalog:schema",
        "//backend/schema/ddl:operations_cc_proto",
        "//backend/schema/parser:ddl_parser",
        "//backend/schema/printer:print_ddl",
        "//backend/schema/updater:ddl_type_conversion",
        "//third_party/spanner_pg/catalog:spangres_type",
        "//third_party/spanner_pg/ddl:spangres_direct_schema_printer_impl",
        "//third_party/spanner_pg/ddl:spangres_schema_printer",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_googleapis//google/spanner/admin/database/v1:database_cc_proto",
        "@com_google_protobuf//:protobuf",
        "@com_google_zetasql//zetasql/base:no_destructor",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:simple_catalog",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "information_schema_catalog_test",
    srcs = ["information_schema_catalog_test.cc"],
    deps = [
        ":info_schema_columns_metadata_values",
        ":information_schema_catalog",
        ":spanner_sys_catalog",
        "//tests/common:proto_matchers",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "pg_catalog",
    srcs = ["pg_catalog.cc"],
    hdrs = [
        "pg_catalog.h",
    ],
    deps = [
        ":info_schema_columns_metadata_values",
        ":tables_from_metadata",
        "//backend/schema/catalog:schema",
        "//third_party/spanner_pg/bootstrap_catalog",
        "//third_party/spanner_pg/bootstrap_catalog:bootstrap_catalog_accessor_impl",  # build_cleaner: keep
        "//third_party/spanner_pg/catalog:engine_system_catalog",
        "//third_party/spanner_pg/catalog:type",
        "//third_party/spanner_pg/datatypes/extended:pg_oid_type",
        "//third_party/spanner_pg/interface:bootstrap_catalog_accessor",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/base:no_destructor",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:simple_catalog",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
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
        ":analyzer_options",
        ":function_catalog",
        ":information_schema_catalog",
        ":pg_catalog",
        ":queryable_model",
        ":queryable_named_schema",
        ":queryable_property_graph",
        ":queryable_sequence",
        ":queryable_table",
        ":queryable_udf",
        ":queryable_view",
        ":spanner_sys_catalog",
        "//backend/access:read",
        "//backend/common:case",
        "//backend/query/change_stream:queryable_change_stream_tvf",
        "//backend/schema/catalog:schema",
        "//common:constants",
        "//common:errors",
        "//third_party/spanner_pg/datatypes/extended:conversion_finder",
        "//third_party/spanner_pg/datatypes/extended:pg_jsonb_type",
        "//third_party/spanner_pg/datatypes/extended:pg_numeric_type",
        "//third_party/spanner_pg/datatypes/extended:pg_oid_type",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:span",
        "@com_google_protobuf//:protobuf",
        "@com_google_zetasql//zetasql/public:analyzer_options",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_test(
    name = "catalog_test",
    srcs = [
        "catalog_test.cc",
    ],
    deps = [
        ":analyzer_options",
        ":catalog",
        ":function_catalog",
        ":queryable_named_schema",
        ":queryable_table",
        "//backend/schema/catalog:schema",
        "//common:constants",
        "//tests/common:proto_matchers",
        "//tests/common:scoped_feature_flags_setter",
        "//tests/common:test_schema_constructor",
        "//third_party/spanner_pg/datatypes/extended:pg_jsonb_type",
        "//third_party/spanner_pg/datatypes/extended:pg_numeric_type",
        "//third_party/spanner_pg/datatypes/extended:pg_oid_type",
        "//third_party/spanner_pg/test_catalog",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_googleapis//google/spanner/admin/database/v1:database_cc_proto",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:analyzer",
        "@com_google_zetasql//zetasql/public:analyzer_output",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:type",
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
        "@com_google_zetasql//zetasql/public:analyzer_output",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_library(
    name = "queryable_sequence",
    srcs = [
        "queryable_sequence.cc",
    ],
    hdrs = [
        "queryable_sequence.h",
    ],
    deps = [
        "//backend/schema/catalog:schema",
        "@com_google_zetasql//zetasql/public:catalog",
    ],
)

cc_library(
    name = "queryable_named_schema",
    srcs = [
        "queryable_named_schema.cc",
    ],
    hdrs = [
        "queryable_named_schema.h",
    ],
    deps = [
        ":function_catalog",
        ":queryable_model",
        ":queryable_sequence",
        ":queryable_table",
        ":queryable_view",
        "//backend/common:case",
        "//backend/schema/catalog:schema",
        "//common:errors",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
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
        "//backend/datamodel:key_set",
        "//backend/schema/catalog:schema",
        "//common:constants",
        "//common:feature_flags",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:analyzer",
        "@com_google_zetasql//zetasql/public:analyzer_options",
        "@com_google_zetasql//zetasql/public:analyzer_output",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:evaluator_table_iterator",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "queryable_view",
    srcs = [
        "queryable_view.cc",
    ],
    hdrs = [
        "queryable_view.h",
    ],
    deps = [
        "//backend/access:read",
        "//backend/schema/catalog:schema",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:evaluator_table_iterator",
        "@com_google_zetasql//zetasql/public:simple_catalog",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "queryable_view_test",
    srcs = ["queryable_view_test.cc"],
    deps = [
        ":catalog",
        ":queryable_column",
        ":queryable_view",
        "//common:feature_flags",
        "//tests/common:proto_matchers",
        "//tests/common:scoped_feature_flags_setter",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:type_cc_proto",
    ],
)

cc_library(
    name = "prepare_property_graph_catalog",
    hdrs = [
        "prepare_property_graph_catalog.h",
    ],
    deps = [
        ":analyzer_options",
        ":catalog",
        ":function_catalog",
        "//backend/schema/catalog:schema",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/public:analyzer_options",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:type",
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
        ":analyzer_options",
        "//backend/common:case",
        "//backend/query/ml:ml_predict_row_function",
        "//backend/query/ml:ml_predict_table_valued_function",
        "//backend/query/search:search_function_catalog",
        "//backend/schema/catalog:schema",
        "//common:bit_reverse",
        "//common:constants",
        "//common:errors",
        "//common:feature_flags",
        "//common:pg_literals",
        "//third_party/spanner_pg/catalog:emulator_function_evaluators",
        "//third_party/spanner_pg/catalog:emulator_functions",
        "//third_party/spanner_pg/datatypes/extended:pg_jsonb_type",
        "//third_party/spanner_pg/datatypes/extended:pg_numeric_type",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:builtin_function",
        "@com_google_zetasql//zetasql/public:builtin_function_options",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "partitionability_validator",
    srcs = ["partitionability_validator.cc"],
    hdrs = ["partitionability_validator.h"],
    deps = [
        "//backend/schema/catalog:schema",
        "//common:errors",
        "//common:feature_flags",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_zetasql//zetasql/resolved_ast",
        "@com_google_zetasql//zetasql/resolved_ast:resolved_node_kind_cc_proto",
    ],
)

cc_test(
    name = "partitionability_validator_test",
    srcs = [
        "partitionability_validator_test.cc",
    ],
    deps = [
        ":partitionability_validator",
        ":query_engine",
        ":queryable_table",
        "//backend/schema/catalog:schema",
        "//common:feature_flags",
        "//tests/common:proto_matchers",
        "//tests/common:scoped_feature_flags_setter",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:analyzer",
        "@com_google_zetasql//zetasql/public:analyzer_options",
        "@com_google_zetasql//zetasql/public:analyzer_output",
        "@com_google_zetasql//zetasql/public:simple_catalog",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:type_cc_proto",
        "@com_google_zetasql//zetasql/resolved_ast",
    ],
)

cc_library(
    name = "index_hint_validator",
    srcs = ["index_hint_validator.cc"],
    hdrs = ["index_hint_validator.h"],
    deps = [
        ":queryable_table",
        "//backend/schema/catalog:schema",
        "//backend/schema/updater:global_schema_names",
        "//common:errors",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:value",
        "@com_google_zetasql//zetasql/resolved_ast",
        "@com_google_zetasql//zetasql/resolved_ast:resolved_node_kind_cc_proto",
        "@com_googlesource_code_re2//:re2",
    ],
)

cc_test(
    name = "index_hint_validator_test",
    srcs = ["index_hint_validator_test.cc"],
    deps = [
        ":analyzer_options",
        ":catalog",
        ":function_catalog",
        ":index_hint_validator",
        ":queryable_table",
        "//backend/schema/catalog:schema",
        "//common:constants",
        "//common:errors",
        "//tests/common:proto_matchers",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/memory",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:analyzer",
        "@com_google_zetasql//zetasql/public:analyzer_options",
    ],
)

cc_library(
    name = "partitioned_dml_validator",
    hdrs = ["partitioned_dml_validator.h"],
    deps = [
        "//common:errors",
        "@com_google_absl//absl/status",
        "@com_google_zetasql//zetasql/resolved_ast",
        "@com_google_zetasql//zetasql/resolved_ast:resolved_node_kind_cc_proto",
    ],
)

cc_library(
    name = "queryable_model",
    srcs = ["queryable_model.cc"],
    hdrs = ["queryable_model.h"],
    deps = [
        "//backend/schema/catalog:schema",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_library(
    name = "queryable_property_graph",
    srcs = ["queryable_property_graph.cc"],
    hdrs = ["queryable_property_graph.h"],
    deps = [
        ":analyzer_options",
        "//backend/common:case",
        "//backend/schema/catalog:schema",
        "//common:constants",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:analyzer",
        "@com_google_zetasql//zetasql/public:analyzer_options",
        "@com_google_zetasql//zetasql/public:analyzer_output",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_library(
    name = "queryable_udf",
    srcs = [
        "queryable_udf.cc",
    ],
    hdrs = [
        "queryable_udf.h",
    ],
    deps = [
        ":analyzer_options",
        ":function_catalog",
        ":hint_rewriter",
        ":index_hint_validator",
        "//backend/query/feature_filter:query_size_limits_checker",
        "//backend/schema/catalog:schema",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:analyzer",
        "@com_google_zetasql//zetasql/public:analyzer_options",
        "@com_google_zetasql//zetasql/public:evaluator",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
        "@com_google_zetasql//zetasql/resolved_ast",
    ],
)

cc_test(
    name = "partitioned_dml_validator_test",
    srcs = ["partitioned_dml_validator_test.cc"],
    deps = [
        ":analyzer_options",
        ":catalog",
        ":function_catalog",
        ":partitioned_dml_validator",
        ":queryable_table",
        "//backend/schema/catalog:schema",
        "//common:constants",
        "//common:errors",
        "//tests/common:proto_matchers",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:analyzer",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:type_cc_proto",
        "@com_google_zetasql//zetasql/resolved_ast",
    ],
)
