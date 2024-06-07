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

# This package contain tests for each of the functions in function_evaluators

licenses(["notice"])

cc_library(
    name = "test_base",
    testonly = True,
    hdrs = [
        "test_base.h",
    ],
    deps = [
        "//third_party/spanner_pg/function_evaluators:pg_evaluators_implementations",  # buildcleaner: keep
        "//third_party/spanner_pg/shims:memory_context_pg_arena",
        "//third_party/spanner_pg/shims:stub_memory_reservation_manager",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
    ],
)

test_suite(
    name = "tests",
    tests = [],  # when empty, includes all tests in the BUILD file (not tagged as manual)
)

cc_test(
    name = "timestamptz_to_char_test",
    srcs = [
        "timestamptz_to_char_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/shims:timezone_helper",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "date_in_test",
    srcs = [
        "date_in_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/shims:timezone_helper",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public/types:timestamp_util",
    ],
)

cc_test(
    name = "timestamptz_in_test",
    srcs = [
        "timestamptz_in_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/shims:timezone_helper",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public/types:timestamp_util",
    ],
)

cc_test(
    name = "to_date_test",
    srcs = [
        "to_date_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/shims:timezone_helper",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public/types:timestamp_util",
    ],
)

cc_test(
    name = "date_mii_test",
    srcs = [
        "date_mii_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public/types:timestamp_util",
    ],
)

cc_test(
    name = "date_pli_test",
    srcs = [
        "date_pli_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public/types:timestamp_util",
    ],
)

cc_test(
    name = "regexp_split_to_array_test",
    srcs = [
        "regexp_split_to_array_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "regexp_match_test",
    srcs = [
        "regexp_match_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "//third_party/spanner_pg/shims:memory_context_pg_arena",
        "//third_party/spanner_pg/shims:stub_memory_reservation_manager",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "textregexeq_test",
    srcs = [
        "textregexeq_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "textregexne_test",
    srcs = [
        "textregexne_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "textregexsubstr_test",
    srcs = [
        "textregexsubstr_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "textregexreplace_test",
    srcs = [
        "textregexreplace_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "to_timestamp_test",
    srcs = [
        "to_timestamp_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/shims:timezone_helper",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "int8_to_char_test",
    srcs = [
        "int8_to_char_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "float8_to_char_test",
    srcs = [
        "float8_to_char_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "numeric_to_char_test",
    srcs = [
        "numeric_to_char_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "numeric_to_number_test",
    srcs = [
        "numeric_to_number_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "abs_test",
    srcs = [
        "abs_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/datatypes/common:numeric_core",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "add_test",
    srcs = [
        "add_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/datatypes/common:numeric_core",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

# ceiling is an alias to ceil, so we don't have a specific test for it
cc_test(
    name = "ceil_test",
    srcs = [
        "ceil_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/datatypes/common:numeric_core",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "divide_test",
    srcs = [
        "divide_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/datatypes/common:numeric_core",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "divide_truncate_towards_zero_test",
    srcs = [
        "divide_truncate_towards_zero_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/datatypes/common:numeric_core",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "floor_test",
    srcs = [
        "floor_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/datatypes/common:numeric_core",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "jsonb_array_element_test",
    srcs = [
        "jsonb_array_element_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/catalog:spangres_type",
        "//third_party/spanner_pg/datatypes/extended:pg_jsonb_type",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "jsonb_object_field_test",
    srcs = [
        "jsonb_object_field_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/datatypes/extended:pg_jsonb_type",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "jsonb_typeof_test",
    srcs = [
        "jsonb_typeof_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/datatypes/common:numeric_core",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "mod_test",
    srcs = [
        "mod_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/datatypes/common:numeric_core",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "multiply_test",
    srcs = [
        "multiply_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/datatypes/common:numeric_core",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "subtract_test",
    srcs = [
        "subtract_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/datatypes/common:numeric_core",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "trunc_test",
    srcs = [
        "trunc_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/datatypes/common:numeric_core",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "//third_party/spanner_pg/postgres_includes",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "unary_minus_test",
    srcs = [
        "unary_minus_test.cc",
    ],
    deps = [
        ":test_base",
        "//third_party/spanner_pg/datatypes/common:numeric_core",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)
