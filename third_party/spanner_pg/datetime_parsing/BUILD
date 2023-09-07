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

# The datetime_parsing package enables doing PostgreSQL parsing during Spanner
# execution

package(
    features = ["-use_header_modules"],  # Incompatible with -fexceptions.
)

licenses(["notice"])

cc_library(
    name = "datetime_parsing",
    srcs = [
        # A mix of private headers and source files.
        "c.h",
        "date.cc",
        "datetime.cc",
        "datetime.h",
        "localtime.cc",
        "miscadmin.h",
        "pg_config_manual.h",
        "pgstrcasecmp.cc",
        "pgtz.cc",
        "pgtime.h",
        "pgtz.h",
        "port.h",
        "private.h",
        "strlcpy.cc",
        "timestamp.cc",
        "tzfile.h",
    ],
    hdrs = [
        "date.h",
        "datetime.h",
        "datetime_constants.h",
        "datetime_exports.h",
        "pgtime.h",
        "timestamp.h",
        "timestamp_macros.h",
    ],
    copts = [
        "-DSYSTEMTZDIR=\\\"/usr/share/zoneinfo\\\"",
    ],
    visibility = [
        "//third_party/spanner_pg/function_evaluators:__subpackages__",
        "//third_party/spanner_pg/shims:__pkg__",
        "//third_party/spanner_pg/util:__pkg__",
    ],
    deps = [
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/synchronization",
        "@com_google_zetasql//zetasql/base:no_destructor",
        "@com_google_zetasql//zetasql/base:ret_check",
    ],
)

cc_test(
    name = "timezone_abbreviations_test",
    srcs = [
        "timezone_abbreviations_test.cc",
    ],
    deps = [
        ":datetime_parsing",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "timestamp_overflow_test",
    srcs = [
        "timestamp_overflow_test.cc",
    ],
    deps = [
        ":datetime_parsing",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)
