#
# PostgreSQL is released under the PostgreSQL License, a liberal Open Source
# license, similar to the BSD or MIT licenses.
#
# PostgreSQL Database Management System
# (formerly known as Postgres, then as Postgres95)
#
# Portions Copyright © 1996-2020, The PostgreSQL Global Development Group
#
# Portions Copyright © 1994, The Regents of the University of California
#
# Portions Copyright 2023 Google LLC
#
# Permission to use, copy, modify, and distribute this software and its
# documentation for any purpose, without fee, and without a written agreement
# is hereby granted, provided that the above copyright notice and this
# paragraph and the following two paragraphs appear in all copies.
#
# IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
# DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
# LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
# EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
# SUCH DAMAGE.
#
# THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
# INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN
# "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
# MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
#------------------------------------------------------------------------------

# Libraries for transforming between a PostgreSQL Query* and a GoogleSQL resolved AST

package(
    default_visibility = ["//:__subpackages__"],
)

licenses(["notice"])

cc_library(
    name = "transformer_helper",
    srcs = ["transformer_helper.cc"],
    hdrs = ["transformer_helper.h"],
    deps = [
        "//third_party/spanner_pg/postgres_includes",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/analyzer:expr_matching_helpers",
        "@com_google_zetasql//zetasql/base:map_util",
        "@com_google_zetasql//zetasql/public:id_string",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/resolved_ast",
        "@com_google_zetasql//zetasql/resolved_ast:resolved_ast_enums_cc_proto",
        "@com_google_zetasql//zetasql/resolved_ast:resolved_node_kind_cc_proto",
    ],
)

cc_library(
    name = "transformer",
    srcs = ["transformer.cc"],
    hdrs = ["transformer.h"],
    deps = [
        ":transformer_helper",
        "//third_party/spanner_pg/catalog:catalog_adapter",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/shims:error_shim",
        "//third_party/spanner_pg/src/backend",
        "//third_party/spanner_pg/util:postgres",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/base",
        "@com_google_zetasql//zetasql/public:analyzer_options",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:language_options",
        "@com_google_zetasql//zetasql/public:parse_location",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/resolved_ast",
    ],
)

cc_library(
    name = "forward_transformer",
    srcs = [
        "forward_dml.cc",
        "forward_expr.cc",
        "forward_function.cc",
        "forward_query.cc",
        "forward_type.cc",
    ],
    hdrs = [
        "expr_transformer_helper.h",
        "forward_transformer.h",
    ],
    deps = [
        ":transformer_helper",
        "//third_party/spanner_pg/bootstrap_catalog",
        "//third_party/spanner_pg/catalog:catalog_adapter",
        "//third_party/spanner_pg/catalog:engine_system_catalog",
        "//third_party/spanner_pg/catalog:engine_user_catalog",
        "//third_party/spanner_pg/catalog:function",
        "//third_party/spanner_pg/catalog:function_identifier",
        "//third_party/spanner_pg/catalog:pg_catalog_util",
        "//third_party/spanner_pg/catalog:table_name",
        "//third_party/spanner_pg/catalog:type",
        "//third_party/spanner_pg/catalog:udf_support",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/shims:error_shim",
        "//third_party/spanner_pg/src/backend",
        "//third_party/spanner_pg/util:nodetag_to_string",
        "//third_party/spanner_pg/util:oid_to_string",
        "//third_party/spanner_pg/util:pg_list_iterators",
        "//third_party/spanner_pg/util:postgres",
        "@com_google_absl//absl/cleanup",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/analyzer:expr_matching_helpers",
        "@com_google_zetasql//zetasql/base:logging",
        "@com_google_zetasql//zetasql/base:map_util",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:analyzer_options",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:id_string",
        "@com_google_zetasql//zetasql/public:language_options",
        "@com_google_zetasql//zetasql/public:parse_location",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
        "@com_google_zetasql//zetasql/resolved_ast",
        "@com_google_zetasql//zetasql/resolved_ast:resolved_ast_enums_cc_proto",
        "@com_google_zetasql//zetasql/resolved_ast:resolved_node_kind_cc_proto",
    ],
)

cc_library(
    name = "transformer_test",
    testonly = 1,
    hdrs = ["transformer_test.h"],
    deps = [
        ":forward_transformer",
        "//third_party/spanner_pg/catalog:catalog_adapter",
        "//third_party/spanner_pg/test_catalog",
        "//third_party/spanner_pg/util:postgres",
        "//third_party/spanner_pg/util:unittest_utils",
        "//third_party/spanner_pg/util:valid_memory_context_fixture",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/public:analyzer_options",
    ],
)

cc_test(
    name = "expr_test",
    srcs = ["expr_test.cc"],
    deps = [
        ":forward_transformer",
        ":transformer_test",
        "//third_party/spanner_pg/catalog:catalog_adapter",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/shims:error_shim",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "//third_party/spanner_pg/test_catalog",
        "//third_party/spanner_pg/util:postgres",
        "//third_party/spanner_pg/util:valid_memory_context_fixture",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:parse_location",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
        "@com_google_zetasql//zetasql/resolved_ast",
    ],
)

cc_test(
    name = "spangres_expr_test",
    srcs = ["spangres_expr_test.cc"],
    deps = [
        ":transformer_test",
        "//third_party/spanner_pg/catalog:catalog_adapter",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "//third_party/spanner_pg/test_catalog",
        "//third_party/spanner_pg/util:postgres",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/resolved_ast",
    ],
)

cc_test(
    name = "function_test",
    srcs = ["function_test.cc"],
    deps = [
        ":forward_transformer",
        ":transformer_test",
        "//third_party/spanner_pg/catalog:catalog_adapter",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "//third_party/spanner_pg/test_catalog",
        "//third_party/spanner_pg/util:postgres",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:analyzer_options",
        "@com_google_zetasql//zetasql/resolved_ast",
    ],
)

cc_test(
    name = "query_test",
    srcs = ["query_test.cc"],
    deps = [
        ":forward_transformer",
        ":transformer",
        ":transformer_test",
        "//third_party/spanner_pg/bootstrap_catalog",
        "//third_party/spanner_pg/catalog:catalog_adapter",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/shims:catalog_shim",
        "//third_party/spanner_pg/shims:catalog_shim_cc_wrappers",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "//third_party/spanner_pg/test_catalog",
        "//third_party/spanner_pg/test_catalog:spanner_test_catalog",
        "//third_party/spanner_pg/util:nodetag_to_string",
        "//third_party/spanner_pg/util:pg_list_iterators",
        "//third_party/spanner_pg/util:postgres",
        "//third_party/spanner_pg/util:unittest_utils",
        "//third_party/spanner_pg/util:valid_memory_context_fixture",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base:logging",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:analyzer",
        "@com_google_zetasql//zetasql/public:analyzer_options",
        "@com_google_zetasql//zetasql/public:analyzer_output",
        "@com_google_zetasql//zetasql/public:parse_location",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/resolved_ast",
        "@com_google_zetasql//zetasql/resolved_ast:resolved_node_kind_cc_proto",
        "@com_google_zetasql//zetasql/resolved_ast:validator",
        "@com_googlesource_code_re2//:re2",
    ],
)

cc_test(
    name = "type_test",
    srcs = ["type_test.cc"],
    deps = [
        ":forward_transformer",
        ":transformer",
        ":transformer_test",
        "//third_party/spanner_pg/catalog:catalog_adapter",
        "//third_party/spanner_pg/catalog:type",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "//third_party/spanner_pg/test_catalog",
        "@com_google_absl//absl/memory",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_test(
    name = "unsupported_query_test",
    srcs = ["unsupported_query_test.cc"],
    deps = [
        ":forward_transformer",
        "//third_party/spanner_pg/catalog:catalog_adapter_holder",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "//third_party/spanner_pg/test_catalog",
        "//third_party/spanner_pg/util:unittest_utils",
        "//third_party/spanner_pg/util:valid_memory_context_fixture",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:analyzer_options",
        "@com_google_zetasql//zetasql/resolved_ast",
    ],
)
