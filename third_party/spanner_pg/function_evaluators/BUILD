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

# The function_evaluators package enables running PostgreSQL functions during
# Spanner execution

licenses(["notice"])

cc_library(
    name = "function_evaluators",
    srcs = [
        "function_evaluators.cc",
    ],
    hdrs = [
        "function_evaluators.h",
    ],
    visibility = ["//:__subpackages__"],
    deps = [
        "//third_party/spanner_pg/datetime_parsing",
        "//third_party/spanner_pg/util:datetime_conversion",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
        "@com_google_zetasql//zetasql/base",
        "@com_google_zetasql//zetasql/public/functions:date_time_util",
    ],
)

cc_library(
    name = "pg_evaluators_implementations",
    srcs = [
        "datetime_evaluators.cc",
        "formatting_evaluators.cc",
        "pg_arena_factory.cc",
        "pg_timezone.cc",
        "regexp_evaluators.cc",
    ],
    visibility = ["//:__subpackages__"],
    deps = [
        "//third_party/spanner_pg/interface:memory_reservation_manager",
        "//third_party/spanner_pg/interface:pg_evaluators",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/shims:error_shim",
        "//third_party/spanner_pg/shims:memory_context_pg_arena",
        "//third_party/spanner_pg/shims:regex_shim_header",
        "//third_party/spanner_pg/shims:timezone_helper",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "//third_party/spanner_pg/src/backend/catalog:generated_catalog_headers",
        "//third_party/spanner_pg/util:datetime_conversion",
        "//third_party/spanner_pg/util:integral_helpers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_zetasql//zetasql/public/functions:date_time_util",
    ],
)

exports_files(
    srcs = ["pg_arena_factory.cc"],
    visibility = ["//third_party/spanner_pg/interface:__pkg__"],
)
