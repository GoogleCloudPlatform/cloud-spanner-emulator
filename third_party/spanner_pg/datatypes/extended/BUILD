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

# Description:
# Libraries used for dealing with extended types that are spanner-specific and not part
# of the GoogleSQL standard.
package(
    default_visibility = ["//:__subpackages__"],
)

licenses(["notice"])

cc_library(
    name = "spanner_extended_type",
    srcs = ["spanner_extended_type.cc"],
    hdrs = ["spanner_extended_type.h"],
    deps = [
        "@com_google_googleapis//google/spanner/v1:spanner_cc_proto",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_library(
    name = "pg_jsonb_type",
    srcs = ["pg_jsonb_type.cc"],
    hdrs = ["pg_jsonb_type.h"],
    deps = [
        ":spanner_extended_type",
        "//third_party/spanner_pg/datatypes/common/jsonb:jsonb_parse",
        "//third_party/spanner_pg/interface:pg_arena_factory",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_proto",
        "@com_google_zetasql//zetasql/base:compact_reference_counted",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:language_options",
        "@com_google_zetasql//zetasql/public:options_cc_proto",
        "@com_google_zetasql//zetasql/public:strings",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:type_cc_proto",
        "@com_google_zetasql//zetasql/public:value",
        "@com_google_zetasql//zetasql/public:value_content",
    ],
)

cc_test(
    name = "pg_jsonb_type_test",
    srcs = ["pg_jsonb_type_test.cc"],
    tags = [
        "spanner.spangres",
    ],
    deps = [
        ":pg_jsonb_type",
        ":spanner_extended_type",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "//third_party/spanner_pg/util:valid_memory_context_fixture",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_proto",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:language_options",
        "@com_google_zetasql//zetasql/public:strings",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "spanner_extended_type_deserializer",
    srcs = ["spanner_extended_type_deserializer.cc"],
    hdrs = ["spanner_extended_type_deserializer.h"],
    deps = [
        ":pg_jsonb_type",
        ":pg_numeric_type",
        "//third_party/spanner_pg/errors:error_catalog",
        "@com_google_absl//absl/status:statusor",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:options_cc_proto",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:type_cc_proto",
    ],
)

cc_test(
    name = "spanner_extended_type_deserializer_test",
    srcs = ["spanner_extended_type_deserializer_test.cc"],
    tags = [
        "spanner.spangres",
    ],
    deps = [
        ":pg_jsonb_type",
        ":pg_numeric_type",
        ":spanner_extended_type_deserializer",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:options_cc_proto",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:type_cc_proto",
    ],
)

cc_library(
    name = "pg_numeric_type",
    srcs = ["pg_numeric_type.cc"],
    hdrs = ["pg_numeric_type.h"],
    deps = [
        ":spanner_extended_type",
        "//third_party/spanner_pg/datatypes/common:pg_numeric_parse",
        "//third_party/spanner_pg/interface:pg_arena_factory",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/shims:error_shim",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_proto",
        "@com_google_zetasql//zetasql/base:compact_reference_counted",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:language_options",
        "@com_google_zetasql//zetasql/public:options_cc_proto",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:type_cc_proto",
        "@com_google_zetasql//zetasql/public:value",
        "@com_google_zetasql//zetasql/public:value_content",
    ],
)

cc_test(
    name = "pg_numeric_type_test",
    srcs = ["pg_numeric_type_test.cc"],
    tags = [
        "spanner.spangres",
    ],
    deps = [
        ":pg_numeric_type",
        ":spanner_extended_type",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "//third_party/spanner_pg/util:valid_memory_context_fixture",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_proto",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:language_options",
        "@com_google_zetasql//zetasql/public:options_cc_proto",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "conversion_finder",
    srcs = [
        "conversion_finder.cc",
        "conversion_functions.cc",
        "pg_jsonb_conversion_functions.cc",
        "pg_numeric_conversion_functions.cc",
        "pg_oid_conversion_functions.cc",
    ],
    hdrs = [
        "conversion_finder.h",
        "conversion_functions.h",
        "pg_jsonb_conversion_functions.h",
        "pg_numeric_conversion_functions.h",
        "pg_oid_conversion_functions.h",
    ],
    deps = [
        ":pg_jsonb_type",
        ":pg_numeric_type",
        ":pg_oid_type",
        "//third_party/spanner_pg/catalog:emulator_function_evaluators",
        "//third_party/spanner_pg/catalog:emulator_functions",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_zetasql//zetasql/base:no_destructor",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:coercer",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:type_cc_proto",
    ],
)

cc_test(
    name = "conversion_finder_test",
    srcs = ["conversion_finder_test.cc"],
    deps = [
        ":conversion_finder",
        ":pg_jsonb_type",
        ":pg_numeric_type",
        ":pg_oid_type",
        "//third_party/spanner_pg/interface:pg_arena_factory",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:coercer",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "pg_numeric_conversion_functions_test",
    srcs = ["pg_numeric_conversion_functions_test.cc"],
    deps = [
        ":conversion_finder",
        ":pg_numeric_type",
        "//third_party/spanner_pg/interface:pg_arena_factory",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "pg_oid_conversion_functions_test",
    srcs = ["pg_oid_conversion_functions_test.cc"],
    deps = [
        ":conversion_finder",
        ":pg_oid_type",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:coercer",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "pg_jsonb_conversion_functions",
    srcs = [
        "pg_jsonb_conversion_functions.cc",
    ],
    hdrs = [
        "pg_jsonb_conversion_functions.h",
    ],
    deps = [
        ":pg_jsonb_type",
        ":pg_numeric_type",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_zetasql//zetasql/public:function",
    ],
)

cc_test(
    name = "pg_jsonb_conversion_functions_test",
    srcs = ["pg_jsonb_conversion_functions_test.cc"],
    deps = [
        ":conversion_finder",
        ":pg_jsonb_type",
        ":pg_numeric_type",
        "//third_party/spanner_pg/datatypes/common:numeric_core",
        "//third_party/spanner_pg/interface:pg_arena_factory",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base:no_destructor",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "pg_oid_type",
    srcs = ["pg_oid_type.cc"],
    hdrs = ["pg_oid_type.h"],
    deps = [
        ":spanner_extended_type",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:language_options",
        "@com_google_zetasql//zetasql/public:options_cc_proto",
        "@com_google_zetasql//zetasql/public:strings",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
        "@com_google_zetasql//zetasql/public:value_content",
    ],
)

cc_test(
    name = "pg_oid_type_test",
    srcs = ["pg_oid_type_test.cc"],
    tags = [
        "spanner.spangres",
    ],
    deps = [
        ":pg_oid_type",
        ":spanner_extended_type",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "@com_google_absl//absl/strings",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_proto",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:language_options",
        "@com_google_zetasql//zetasql/public:options_cc_proto",
        "@com_google_zetasql//zetasql/public:value",
    ],
)
