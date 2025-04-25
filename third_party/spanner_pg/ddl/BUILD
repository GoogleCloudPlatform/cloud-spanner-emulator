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

# Package contains functionality for translation between PostgreSQL and Spanner DDL dialects.

package(
    default_visibility = ["//:__subpackages__"],
)

licenses(["notice"])

cc_library(
    name = "ddl_translator",
    srcs = ["ddl_translator.cc"],
    hdrs = ["ddl_translator.h"],
    visibility = ["//:__subpackages__"],
    deps = [
        "//backend/schema/ddl:operations_cc_proto",
        "//third_party/spanner_pg/interface:parser_interface",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_zetasql//zetasql/base:ret_check",
    ],
)

cc_library(
    name = "pg_to_spanner_ddl_translator",
    srcs = ["pg_to_spanner_ddl_translator.cc"],
    hdrs = ["pg_to_spanner_ddl_translator.h"],
    visibility = ["//:__subpackages__"],
    deps = [
        ":ddl_translator",
        ":pg_parse_tree_validator",
        ":translation_utils",
        "//third_party/spanner_pg/interface:parser_interface",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/shims:error_shim",
        "//third_party/spanner_pg/util:interval_helpers",
        "//third_party/spanner_pg/util:pg_list_iterators",
        "//third_party/spanner_pg/util:postgres",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:optional",
        "@com_google_protobuf//:protobuf_lite",
        "@com_google_zetasql//zetasql/base",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_googlesource_code_re2//:re2",
    ],
)

cc_library(
    name = "spangres_schema_printer",
    hdrs = ["spangres_schema_printer.h"],
    visibility = ["//:__subpackages__"],
    deps = [
        "//backend/schema/ddl:operations_cc_proto",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_library(
    name = "ddl_test_helper",
    testonly = 1,
    srcs = ["ddl_test_helper.cc"],
    hdrs = ["ddl_test_helper.h"],
    deps = [
        ":ddl_translator",
        ":pg_to_spanner_ddl_translator",
        ":spangres_direct_schema_printer_impl",
        ":spangres_schema_printer",
        "//third_party/spanner_pg/interface:interfaces_with_stub_implementations",
        "//third_party/spanner_pg/interface:parser_interface",
        "//third_party/spanner_pg/interface:parser_without_serialization_only",  # buildcleaner: keep
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/base",
    ],
)

cc_test(
    name = "ddl_test",
    srcs = ["ddl_test.cc"],
    deps = [
        ":ddl_test_helper",
        ":ddl_translator",
        ":spangres_schema_printer",
        ":translation_utils",
        "//backend/schema/ddl:operations_cc_proto",
        "//third_party/spanner_pg/interface:parser_interface",
        "//third_party/spanner_pg/shims:memory_context_pg_arena",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
        "@com_google_zetasql//zetasql/base:logging",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "translation_utils",
    srcs = ["translation_utils.cc"],
    hdrs = ["translation_utils.h"],
    visibility = ["//:__subpackages__"],
    deps = [
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/src/include:kwlist_include",
        "//third_party/spanner_pg/src/include:pg_config_include",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@com_googlesource_code_re2//:re2",
    ],
)

cc_test(
    name = "translation_utils_test",
    srcs = ["translation_utils_test.cc"],
    deps = [
        ":translation_utils",
        "//third_party/spanner_pg/postgres_includes",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "spangres_direct_schema_printer_impl",
    srcs = ["spangres_direct_schema_printer_impl.cc"],
    hdrs = ["spangres_direct_schema_printer_impl.h"],
    visibility = ["//:__subpackages__"],
    deps = [
        ":spangres_schema_printer",
        ":translation_utils",
        "//backend/schema/ddl:operations_cc_proto",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@com_google_protobuf//:protobuf_headers",
        "@com_google_protobuf//:protobuf_lite",
        "@com_google_zetasql//zetasql/base",
        "@com_google_zetasql//zetasql/base:ret_check",
    ],
)

filegroup(
    name = "exported_jsondata",
    srcs = glob(["types/testdata/*.json"]),
    visibility = ["//third_party/spanner_pg/datatypes/common/jsonb:__subpackages__"],
)

cc_library(
    name = "pg_parse_tree_validator",
    srcs = ["pg_parse_tree_validator.cc"],
    hdrs = ["pg_parse_tree_validator.h"],
    deps = [
        ":ddl_translator",
        ":translation_utils",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/shims:error_shim",
        "//third_party/spanner_pg/util:pg_list_iterators",
        "//third_party/spanner_pg/util:postgres",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/base:ret_check",
    ],
)
