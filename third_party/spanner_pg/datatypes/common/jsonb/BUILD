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

# jsonb package contains all code files for parsing and binary format of PG.JSONB

cc_library(
    name = "jsonb_parse",
    srcs = [
        "jsonb_parse.cc",
    ],
    hdrs = [
        "jsonb_parse.h",
    ],
    visibility = [
        "//:__subpackages__",
    ],
    deps = [
        "//third_party/spanner_pg/datatypes/common:numeric_core",
        "//third_party/spanner_pg/datatypes/common:pg_numeric_parse",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_zetasql//zetasql/base:logging",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@nlohmann_json//:json",
    ],
)

cc_test(
    name = "jsonb_parse_test",
    srcs = [
        "jsonb_parse_test.cc",
    ],
    data = ["//third_party/spanner_pg/ddl:exported_jsondata"],
    shard_count = 4,
    tags = [
        "spanner.programmability",
    ],
    deps = [
        ":jsonb_parse",
        ":random_json_creator",
        "//tests/common:file_based_test_util",
        "//third_party/spanner_pg/datatypes/common:numeric_core",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/shims:error_shim",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "//third_party/spanner_pg/util:valid_memory_context_fixture",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base:no_destructor",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/common:utf_util",
    ],
)

cc_library(
    name = "random_json_creator",
    srcs = ["random_json_creator.cc"],
    hdrs = ["random_json_creator.h"],
    deps = [
        "@com_google_absl//absl/strings",
    ],
)
