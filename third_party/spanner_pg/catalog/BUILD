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

load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library", "cc_test")

# Catalog libraries for mappings between Postgres and GoogleSQL.

package(
    default_visibility = [
        "//:__subpackages__",
    ],
)

licenses(["notice"])

cc_library(
    name = "engine_system_catalog",
    srcs = ["engine_system_catalog.cc"],
    hdrs = ["engine_system_catalog.h"],
    deps = [
        ":builtin_functions",
        ":function",
        ":function_identifier",
        ":type",
        "//third_party/spanner_pg/bootstrap_catalog",
        "//third_party/spanner_pg/interface:bootstrap_catalog_data_cc_proto",
        "//third_party/spanner_pg/interface:interfaces_with_stub_implementations",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/util:nodetag_to_string",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/analyzer:function_signature_matcher",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:coercer",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:function_cc_proto",
        "@com_google_zetasql//zetasql/public:language_options",
        "@com_google_zetasql//zetasql/public:options_cc_proto",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/resolved_ast",
    ],
)

cc_library(
    name = "spangres_function_mapper",
    srcs = [
        "spangres_function_filter.cc",
        "spangres_function_grouper.cc",
        "spangres_function_mapper.cc",
        "spangres_function_verifier.cc",
    ],
    hdrs = [
        "spangres_function_filter.h",
        "spangres_function_grouper.h",
        "spangres_function_mapper.h",
        "spangres_function_verifier.h",
    ],
    deps = [
        ":builtin_functions",
        ":engine_system_catalog",
        ":type",
        "//third_party/spanner_pg/codegen:postgresql_catalog_cc_proto",
        "//third_party/spanner_pg/src/backend/catalog:generated_catalog_headers",
        "//third_party/spanner_pg/src/include:client_include",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/flags:commandlineflag",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/flags:reflection",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf_lite",
        "@com_google_zetasql//zetasql/base:no_destructor",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:function_cc_proto",
        "@com_google_zetasql//zetasql/public/types:type",
    ],
)

cc_test(
    name = "spangres_function_verifier_test",
    srcs = ["spangres_function_verifier_test.cc"],
    tags = ["spanner.spangres"],
    deps = [
        ":spangres_function_mapper",
        "//third_party/spanner_pg/test_catalog",  # buildcleaner: keep
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:function_cc_proto",
    ],
)

cc_test(
    name = "spangres_function_filter_test",
    srcs = ["spangres_function_filter_test.cc"],
    tags = ["spanner.spangres"],
    deps = [
        ":spangres_function_mapper",
        "//third_party/spanner_pg/test_catalog",  # buildcleaner: keep
        "@com_google_absl//absl/flags:flag",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "spangres_function_grouper_test",
    srcs = ["spangres_function_grouper_test.cc"],
    tags = ["spanner.spangres"],
    deps = [
        ":builtin_functions",
        ":spangres_function_mapper",
        "//third_party/spanner_pg/test_catalog",  # buildcleaner: keep
        "@com_google_absl//absl/container:btree",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "spangres_function_mapper_test",
    srcs = ["spangres_function_mapper_test.cc"],
    tags = ["spanner.spangres"],
    deps = [
        ":builtin_functions",
        ":engine_system_catalog",
        ":spangres_function_mapper",
        "//third_party/spanner_pg/test_catalog",  # buildcleaner: keep
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:function_cc_proto",
        "@com_google_zetasql//zetasql/public:options_cc_proto",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public/types:type",
    ],
)

cc_library(
    name = "spangres_system_catalog",
    srcs = ["spangres_system_catalog.cc"],
    hdrs = ["spangres_system_catalog.h"],
    visibility = ["//:__subpackages__"],
    deps = [
        ":builtin_functions",
        ":engine_system_catalog",
        ":function",
        ":spangres_function_mapper",
        ":spangres_type",
        ":type",
        "//third_party/spanner_pg/bootstrap_catalog",
        "//third_party/spanner_pg/codegen:postgresql_catalog_reader",
        "//third_party/spanner_pg/datatypes/extended:conversion_finder",
        "//third_party/spanner_pg/datatypes/extended:pg_jsonb_type",
        "//third_party/spanner_pg/datatypes/extended:pg_numeric_type",
        "//third_party/spanner_pg/datatypes/extended:pg_oid_type",
        "//third_party/spanner_pg/interface:emulator_builtin_function_catalog",
        "//third_party/spanner_pg/postgres_includes",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/analyzer:function_signature_matcher",
        "@com_google_zetasql//zetasql/base:no_destructor",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:language_options",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:type_cc_proto",
        "@com_google_zetasql//zetasql/resolved_ast",
    ],
)

cc_test(
    name = "spangres_system_catalog_test",
    srcs = ["spangres_system_catalog_test.cc"],
    tags = ["spanner.spangres"],
    deps = [
        ":engine_system_catalog",
        ":function",
        ":function_identifier",
        ":spangres_system_catalog",
        ":spangres_type",
        ":type",
        "//third_party/spanner_pg/bootstrap_catalog",
        "//third_party/spanner_pg/datatypes/extended:pg_numeric_type",
        "//third_party/spanner_pg/datatypes/extended:pg_oid_type",
        "//third_party/spanner_pg/interface:emulator_builtin_function_catalog",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "//third_party/spanner_pg/src/backend/catalog:generated_catalog_headers",
        "//third_party/spanner_pg/test_catalog",
        "//third_party/spanner_pg/util:valid_memory_context_fixture",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:language_options",
        "@com_google_zetasql//zetasql/public:options_cc_proto",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
        "@com_google_zetasql//zetasql/resolved_ast",
    ],
)

cc_library(
    name = "engine_user_catalog",
    srcs = ["engine_user_catalog.cc"],
    hdrs = ["engine_user_catalog.h"],
    deps = [
        ":table_name",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/base:case",
        "@com_google_zetasql//zetasql/base:die_if_null",
        "@com_google_zetasql//zetasql/base:logging",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_test(
    name = "engine_system_catalog_test",
    srcs = ["engine_system_catalog_test.cc"],
    tags = ["spanner.spangres"],
    deps = [
        ":engine_system_catalog",
        ":function_identifier",
        ":spangres_type",
        ":type",
        "//third_party/spanner_pg/interface:interfaces_with_stub_implementations",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "//third_party/spanner_pg/util:valid_memory_context_fixture",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:language_options",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_library(
    name = "function",
    hdrs = ["function.h"],
    deps = [
        "//third_party/spanner_pg/postgres_includes",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/public:function",
    ],
)

cc_library(
    name = "type",
    srcs = ["type.cc"],
    hdrs = ["type.h"],
    deps = [
        ":pg_catalog_util",
        "//third_party/spanner_pg/bootstrap_catalog",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/shims:error_shim",
        "//third_party/spanner_pg/util:datetime_conversion",
        "//third_party/spanner_pg/util:oid_to_string",
        "//third_party/spanner_pg/util:postgres",
        "//third_party/spanner_pg/util:uuid_conversion",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/base",
        "@com_google_zetasql//zetasql/base:no_destructor",
        "@com_google_zetasql//zetasql/public:interval_value",
        "@com_google_zetasql//zetasql/public:options_cc_proto",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:uuid_value",
        "@com_google_zetasql//zetasql/public:value",
        "@com_google_zetasql//zetasql/public/functions:date_time_util",
        "@com_googlesource_code_re2//:re2",
    ],
)

cc_library(
    name = "spangres_type",
    srcs = ["spangres_type.cc"],
    hdrs = ["spangres_type.h"],
    visibility = ["//:__subpackages__"],
    deps = [
        ":type",
        "//third_party/spanner_pg/bootstrap_catalog",
        "//third_party/spanner_pg/datatypes/extended:pg_jsonb_type",
        "//third_party/spanner_pg/datatypes/extended:pg_numeric_type",
        "//third_party/spanner_pg/datatypes/extended:pg_oid_type",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/shims:error_shim",
        "//third_party/spanner_pg/util:postgres",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_zetasql//zetasql/base:no_destructor",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
        "@com_googlesource_code_re2//:re2",
    ],
)

cc_test(
    name = "type_test",
    srcs = ["type_test.cc"],
    tags = ["spanner.spangres"],
    deps = [
        ":type",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/shims:error_shim",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "//third_party/spanner_pg/src/backend/catalog:generated_catalog_headers",
        "//third_party/spanner_pg/util:postgres",
        "//third_party/spanner_pg/util:uuid_conversion",
        "//third_party/spanner_pg/util:valid_memory_context_fixture",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:language_options",
        "@com_google_zetasql//zetasql/public:options_cc_proto",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:uuid_value",
        "@com_google_zetasql//zetasql/public:value",
        "@com_google_zetasql//zetasql/resolved_ast",
    ],
)

cc_test(
    name = "spangres_type_test",
    srcs = ["spangres_type_test.cc"],
    tags = ["spanner.spangres"],
    deps = [
        ":spangres_type",
        ":type",
        "//third_party/spanner_pg/datatypes/extended:pg_jsonb_type",
        "//third_party/spanner_pg/datatypes/extended:pg_numeric_type",
        "//third_party/spanner_pg/datatypes/extended:pg_oid_type",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/shims:error_shim",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "//third_party/spanner_pg/util:postgres",
        "//third_party/spanner_pg/util:valid_memory_context_fixture",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:language_options",
        "@com_google_zetasql//zetasql/public:options_cc_proto",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "pg_catalog_util",
    srcs = ["pg_catalog_util.cc"],
    hdrs = ["pg_catalog_util.h"],
    deps = [
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/base:logging",
    ],
)

cc_test(
    name = "pg_catalog_util_test",
    srcs = ["pg_catalog_util_test.cc"],
    tags = ["spanner.spangres"],
    deps = [
        ":pg_catalog_util",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "catalog_adapter",
    srcs = [
        "catalog_adapter.cc",
    ],
    hdrs = [
        "catalog_adapter.h",
    ],
    deps = [
        ":engine_system_catalog",
        ":engine_user_catalog",
        ":table_name",
        "//third_party/spanner_pg/bootstrap_catalog",
        "//third_party/spanner_pg/errors:error_catalog",
        "//third_party/spanner_pg/postgres_includes",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_zetasql//zetasql/base:case",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:analyzer_options",
        "@com_google_zetasql//zetasql/public:function",
    ],
)

cc_test(
    name = "catalog_adapter_test",
    srcs = ["catalog_adapter_test.cc"],
    tags = ["spanner.spangres"],
    deps = [
        ":catalog_adapter",
        ":spangres_user_catalog",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "//third_party/spanner_pg/test_catalog",
        "//third_party/spanner_pg/test_catalog:spanner_test_catalog",
        "//third_party/spanner_pg/util:valid_memory_context_fixture",
        "@com_google_absl//absl/memory",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "catalog_adapter_holder",
    srcs = [
        "catalog_adapter_holder.cc",
    ],
    hdrs = [
        "catalog_adapter_holder.h",
    ],
    deps = [
        ":catalog_adapter",
        ":engine_system_catalog",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_zetasql//zetasql/public:catalog",
    ],
)

cc_test(
    name = "catalog_adapter_holder_test",
    srcs = ["catalog_adapter_holder_test.cc"],
    tags = ["spanner.spangres"],
    deps = [
        ":catalog_adapter",
        ":catalog_adapter_holder",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "//third_party/spanner_pg/test_catalog",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "builtin_functions",
    srcs = [
        "builtin_aggregate_functions.cc",
        "builtin_expression_functions.cc",
        "builtin_function.cc",
        "builtin_scalar_functions.cc",
    ],
    hdrs = [
        "builtin_aggregate_functions.h",
        "builtin_expression_functions.h",
        "builtin_function.h",
        "builtin_scalar_functions.h",
    ],
    deps = [
        ":function",
        ":function_identifier",
        ":spangres_type",
        "//third_party/spanner_pg/postgres_includes",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_library(
    name = "function_identifier",
    hdrs = ["function_identifier.h"],
    deps = [
        "//third_party/spanner_pg/postgres_includes",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_zetasql//zetasql/base",
        "@com_google_zetasql//zetasql/base:logging",
    ],
)

cc_library(
    name = "table_name",
    srcs = ["table_name.cc"],
    hdrs = ["table_name.h"],
    deps = [
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/base:logging",
    ],
)

cc_test(
    name = "table_name_test",
    srcs = ["table_name_test.cc"],
    tags = ["spanner.spangres"],
    deps = [
        ":table_name",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "spangres_user_catalog",
    srcs = ["spangres_user_catalog.cc"],
    hdrs = ["spangres_user_catalog.h"],
    deps = [
        ":engine_user_catalog",
        "//backend/query:queryable_table",
        "//backend/schema/catalog:schema",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:function",
    ],
)

cc_test(
    name = "spangres_user_catalog_test",
    srcs = ["spangres_user_catalog_test.cc"],
    tags = ["spanner.spangres"],
    deps = [
        ":spangres_system_catalog",
        ":spangres_user_catalog",
        ":table_name",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "//third_party/spanner_pg/test_catalog",
        "//third_party/spanner_pg/test_catalog:spanner_test_catalog",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:function",
    ],
)

cc_library(
    name = "udf_support",
    srcs = ["udf_support.cc"],
    hdrs = ["udf_support.h"],
    deps = [
        ":catalog_adapter",
        "//third_party/spanner_pg/bootstrap_catalog",
        "//third_party/spanner_pg/postgres_includes",
    ],
)

cc_library(
    name = "emulator_function_evaluators",
    srcs = ["emulator_function_evaluators.cc"],
    hdrs = ["emulator_function_evaluators.h"],
    visibility = [
        "//:__subpackages__",
        "//backend/query:__subpackages__",
    ],
    deps = [
        "//third_party/spanner_pg/function_evaluators:pg_evaluators_implementations",  # build_cleaner: keep
        "//third_party/spanner_pg/interface:pg_evaluators",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/shims:error_shim",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "emulator_function_evaluators_test",
    srcs = ["emulator_function_evaluators_test.cc"],
    tags = ["spanner.spangres"],
    deps = [
        ":emulator_function_evaluators",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "//third_party/spanner_pg/shims:error_shim",
        "//third_party/spanner_pg/shims:memory_context_pg_arena",
        "//third_party/spanner_pg/shims:stub_memory_reservation_manager",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "jsonb_array_elements_table_valued_function",
    srcs = ["jsonb_array_elements_table_valued_function.cc"],
    hdrs = ["jsonb_array_elements_table_valued_function.h"],
    deps = [
        "//third_party/spanner_pg/datatypes/common/jsonb:jsonb_value",
        "//third_party/spanner_pg/datatypes/extended:pg_jsonb_type",
        "//third_party/spanner_pg/interface:pg_arena_factory",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "//third_party/spanner_pg/shims:error_shim",
        "@com_google_absl//absl/status:statusor",
        "@com_google_zetasql//zetasql/public:evaluator_table_iterator",
        "@com_google_zetasql//zetasql/public:function",
    ],
)

cc_library(
    name = "emulator_functions",
    srcs = ["emulator_functions.cc"],
    hdrs = ["emulator_functions.h"],
    visibility = [
        "//:__subpackages__",
        "//backend/query:__subpackages__",
    ],
    deps = [
        ":emulator_function_evaluators",
        ":jsonb_array_elements_table_valued_function",
        "//third_party/spanner_pg/datatypes/common:pg_numeric_parse",
        "//third_party/spanner_pg/datatypes/common/jsonb:jsonb_value",
        "//third_party/spanner_pg/datatypes/extended:pg_jsonb_conversion_functions",
        "//third_party/spanner_pg/datatypes/extended:pg_jsonb_type",
        "//third_party/spanner_pg/datatypes/extended:pg_numeric_type",
        "//third_party/spanner_pg/datatypes/extended:pg_oid_type",
        "//third_party/spanner_pg/datatypes/extended:spanner_extended_type",
        "//third_party/spanner_pg/function_evaluators:pg_evaluators_implementations",  # build_cleaner: keep
        "//third_party/spanner_pg/interface:pg_evaluators",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/base:mathutil",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/common:string_util",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:function_cc_proto",
        "@com_google_zetasql//zetasql/public:interval_value",
        "@com_google_zetasql//zetasql/public:json_value",
        "@com_google_zetasql//zetasql/public:options_cc_proto",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:type_cc_proto",
        "@com_google_zetasql//zetasql/public:value",
        "@com_google_zetasql//zetasql/public/functions:arithmetics",
        "@com_google_zetasql//zetasql/public/functions:date_time_util",
        "@com_google_zetasql//zetasql/public/functions:json",
    ],
)

cc_test(
    name = "emulator_functions_test",
    srcs = ["emulator_functions_test.cc"],
    deps = [
        ":emulator_functions",
        "//third_party/spanner_pg/datatypes/common:numeric_core",
        "//third_party/spanner_pg/datatypes/extended:pg_jsonb_type",
        "//third_party/spanner_pg/datatypes/extended:pg_numeric_type",
        "//third_party/spanner_pg/datatypes/extended:pg_oid_type",
        "//third_party/spanner_pg/interface:pg_arena_factory",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:interval_value",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:uuid_value",
        "@com_google_zetasql//zetasql/public:value",
        "@com_google_zetasql//zetasql/public/functions:date_time_util",
        "@com_google_zetasql//zetasql/public/types:timestamp_util",
    ],
)

cc_library(
    name = "catalog_wrappers",
    srcs = [
        "catalog_wrappers.cc",
    ],
    copts = [
        # The ereport module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    features = ["-use_header_modules"],
    deps = [
        ":catalog_adapter",
        ":catalog_adapter_holder",
        ":udf_support",
        "//third_party/spanner_pg/bootstrap_catalog",
        "//third_party/spanner_pg/errors:error_catalog",
        "//third_party/spanner_pg/interface:catalog_wrappers",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/transformer",
        "//third_party/spanner_pg/util:pg_list_iterators",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:function",
    ],
    alwayslink = 1,
)

# This test tests a library that throws exceptions.
# It doesn't throw exceptions itself, but it does catch thrown exceptions.
cc_test(
    name = "catalog_wrappers_test",
    srcs = ["catalog_wrappers_test.cc"],
    copts = [
        # The ereport module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    features = ["-use_header_modules"],
    tags = ["spanner.spangres"],
    deps = [
        ":catalog_wrappers",
        "//third_party/spanner_pg/bootstrap_catalog",
        "//third_party/spanner_pg/catalog:catalog_adapter_holder",
        "//third_party/spanner_pg/interface:catalog_wrappers",
        "//third_party/spanner_pg/interface:ereport",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "//third_party/spanner_pg/test_catalog",
        "//third_party/spanner_pg/util:postgres",
        "//third_party/spanner_pg/util:valid_memory_context_fixture",
        "@com_google_absl//absl/log:scoped_mock_log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:catalog",
    ],
)
