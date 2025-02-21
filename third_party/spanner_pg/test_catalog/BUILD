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

# Test catalog for Spanner PG dialect unit tests.

package(
    default_visibility = ["//:__subpackages__"],
)

licenses(["notice"])

cc_library(
    name = "test_catalog",
    testonly = 1,
    srcs = ["test_catalog.cc"],
    hdrs = ["test_catalog.h"],
    deps = [
        ":emulator_catalog",
        ":spanner_test_catalog",
        "//backend/query:analyzer_options",
        "//third_party/spanner_pg/catalog:catalog_adapter",
        "//third_party/spanner_pg/catalog:catalog_adapter_holder",
        "//third_party/spanner_pg/catalog:engine_system_catalog",
        "//third_party/spanner_pg/catalog:spangres_system_catalog",
        "//third_party/spanner_pg/catalog:spangres_user_catalog",
        "//third_party/spanner_pg/catalog:type",
        "//third_party/spanner_pg/interface:emulator_builtin_function_catalog",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/public:analyzer_options",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:language_options",
        "@com_google_zetasql//zetasql/public:simple_catalog",
    ],
)

cc_test(
    name = "test_catalog_test",
    srcs = ["test_catalog_test.cc"],
    deps = [
        ":emulator_catalog",
        ":spanner_test_catalog",
        ":test_catalog",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:catalog",
    ],
)

cc_library(
    name = "spanner_test_catalog",
    testonly = 1,
    srcs = ["spanner_test_catalog.cc"],
    hdrs = ["spanner_test_catalog.h"],
    deps = [
        ":emulator_catalog",
        "@com_google_absl//absl/status:statusor",
        "@com_google_zetasql//zetasql/public:catalog",
    ],
)

cc_test(
    name = "spanner_test_catalog_test",
    srcs = ["spanner_test_catalog_test.cc"],
    deps = [
        ":emulator_catalog",
        ":spanner_test_catalog",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:catalog",
    ],
)

cc_library(
    name = "emulator_catalog",
    testonly = 1,
    srcs = ["emulator_catalog.cc"],
    hdrs = ["emulator_catalog.h"],
    deps = [
        "//backend/query:catalog",
        "//backend/query:function_catalog",
        "//backend/schema/builders:schema_builders",
        "//backend/schema/catalog:schema",
        "//backend/schema/graph:schema_graph",
        "//third_party/spanner_pg/catalog:spangres_type",
        "//third_party/spanner_pg/catalog:type",
        "@com_google_zetasql//zetasql/public:analyzer_options",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:options_cc_proto",
        "@com_google_zetasql//zetasql/public:type",
    ],
)
