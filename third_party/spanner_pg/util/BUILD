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

# Utility libraries for Spangres

load("@rules_cc//cc:defs.bzl", "cc_library", "cc_test")

package(
    default_visibility = [
        "//:__subpackages__",
    ],
)

licenses(["restricted"])

cc_library(
    name = "valid_memory_context_fixture",
    testonly = True,
    srcs = ["valid_memory_context_fixture.cc"],
    hdrs = ["valid_memory_context_fixture.h"],
    visibility = [
        "//:__subpackages__",
        "//spanner/datatypes/common/jsonb:__subpackages__",
    ],
    deps = [
        "//third_party/spanner_pg/interface:memory_reservation_manager",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/shims:memory_context_manager",
        "//third_party/spanner_pg/shims:memory_reservation_holder",
        "//third_party/spanner_pg/shims:stub_memory_reservation_manager",
        "//third_party/spanner_pg/shims:timezone_helper",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "pg_list_iterators",
    hdrs = [
        "pg_list_iterators.h",
    ],
    deps = [
        "//third_party/spanner_pg/postgres_includes",
    ],
)

cc_test(
    name = "pg_list_iterators_test",
    srcs = [
        "pg_list_iterators_test.cc",
    ],
    deps = [
        ":pg_list_iterators",
        "//third_party/spanner_pg/postgres_includes:placeholder_progname",
        "//third_party/spanner_pg/src/backend:backend_with_stub_shims",
        "//third_party/spanner_pg/util:valid_memory_context_fixture",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "oid_to_string",
    srcs = [
        "oid_to_string.cc",
        "oids.inc",
    ],
    hdrs = ["oid_to_string.h"],
    deps = [
        "//third_party/spanner_pg/postgres_includes",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "nodetag_to_string",
    srcs = [
        "nodetag_to_string.cc",
    ],
    hdrs = ["nodetag_to_string.h"],
    deps = [
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/postgres_includes:nodes_inc",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "oid_to_string_test",
    srcs = [
        "oid_to_string_test.cc",
        "oids.inc",
    ],
    deps = [
        ":oid_to_string",
        "//third_party/spanner_pg/postgres_includes",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "nodetag_to_string_test",
    srcs = [
        "nodetag_to_string_test.cc",
    ],
    deps = [
        ":nodetag_to_string",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/postgres_includes:nodes_inc",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "postgres",
    srcs = ["postgres.cc"],
    hdrs = ["postgres.h"],
    deps = [
        "//third_party/spanner_pg/bootstrap_catalog",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/shims:error_shim",
        "//third_party/spanner_pg/src/backend",
        "//third_party/spanner_pg/src/backend/catalog:generated_catalog_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/base",
    ],
)

cc_test(
    name = "postgres_test",
    srcs = ["postgres_test.cc"],
    deps = [
        "postgres",
        "//third_party/spanner_pg/bootstrap_catalog",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "//third_party/spanner_pg/src/backend/catalog:generated_catalog_headers",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "datetime_conversion",
    srcs = ["datetime_conversion.cc"],
    hdrs = ["datetime_conversion.h"],
    deps = [
        ":integral_helpers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/time",
    ],
)

cc_test(
    name = "datetime_conversion_test",
    srcs = ["datetime_conversion_test.cc"],
    deps = [
        ":datetime_conversion",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "interval_helpers",
    srcs = ["interval_helpers.cc"],
    hdrs = ["interval_helpers.h"],
    deps = [
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_test(
    name = "interval_helpers_test",
    srcs = ["interval_helpers_test.cc"],
    deps = [
        ":interval_helpers",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "uuid_conversion",
    srcs = ["uuid_conversion.cc"],
    hdrs = ["uuid_conversion.h"],
    deps = [
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/shims:error_shim",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "uuid_conversion_test",
    srcs = ["uuid_conversion_test.cc"],
    deps = [
        ":uuid_conversion",
        ":valid_memory_context_fixture",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "unittest_utils",
    testonly = True,
    srcs = ["unittest_utils.cc"],
    hdrs = ["unittest_utils.h"],
    deps = [
        "//third_party/spanner_pg/catalog:catalog_adapter_holder",
        "//third_party/spanner_pg/catalog:engine_system_catalog",
        "//third_party/spanner_pg/catalog:spangres_type",
        "//third_party/spanner_pg/interface/test:postgres_transformer",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/shims:error_shim",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "//third_party/spanner_pg/src/spangres:memory",
        "//third_party/spanner_pg/test_catalog",
        "//third_party/spanner_pg/test_catalog:spanner_test_catalog",
        "//third_party/spanner_pg/transformer",
        "//third_party/spanner_pg/transformer:forward_transformer",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/public:analyzer",
        "@com_google_zetasql//zetasql/public:analyzer_options",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_test(
    name = "unittest_utils_test",
    srcs = [
        "unittest_utils_test.cc",
    ],
    deps = [
        ":unittest_utils",
        ":valid_memory_context_fixture",
        "//third_party/spanner_pg/shims:memory_context_manager",
        "//third_party/spanner_pg/shims:memory_reservation_holder",
        "//third_party/spanner_pg/shims:stub_memory_reservation_manager",
        "//third_party/spanner_pg/test_catalog",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:analyzer",
        "@com_google_zetasql//zetasql/public:catalog",
    ],
)

cc_library(
    name = "integral_types",
    hdrs = ["integral_types.h"],
)

cc_library(
    name = "integral_helpers",
    hdrs = ["integral_helpers.h"],
    deps = [
        "@com_google_absl//absl/numeric:int128",
        "@com_google_absl//absl/status",
    ],
)

cc_test(
    name = "integral_helpers_test",
    srcs = ["integral_helpers_test.cc"],
    deps = [
        ":integral_helpers",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)
