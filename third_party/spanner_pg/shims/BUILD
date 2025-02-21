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

# Shim libraries and other support packages for linking PostgreSQL parser and
# analyzer code to Spanner code for Spangres.

package(
    default_visibility = ["//:__subpackages__"],
    features = ["-use_header_modules"],  # Incompatible with -fexceptions.
)

licenses(["notice"])

proto_library(
    name = "parser_output_proto",
    srcs = ["parser_output.proto"],
    deps = [
    ],
)

cc_proto_library(
    name = "parser_output_cc_proto",
    deps = [":parser_output_proto"],
)

# Error shim intercepts the Postgres error handler to jump back into shim code
# and translate the error to a util::Status error.
# This library DOES NOT throw exceptions, but it uses them internally.
cc_library(
    name = "error_shim",
    srcs = ["error_shim.cc"],
    hdrs = ["error_shim.h"],
    copts = [
        # The ereport module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    visibility = [
        "//:__subpackages__",
        "//spanner/datatypes/common/jsonb:__subpackages__",
    ],
    deps = [
        "//third_party/spanner_pg/errors",
        "//third_party/spanner_pg/errors:error_catalog",
        "//third_party/spanner_pg/interface:ereport",
        "//third_party/spanner_pg/interface:parser_interface",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/postgres_includes:deparser",
        "//third_party/spanner_pg/src/backend",
        "//third_party/spanner_pg/src/spangres:parser",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_zetasql//zetasql/base",
        "@com_google_zetasql//zetasql/base:ret_check",
    ],
)

cc_library(
    name = "parser_output_serialization",
    srcs = ["parser_output_serialization.cc"],
    hdrs = ["parser_output_serialization.h"],
    copts = [
        # The ereport module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    deps = [
        ":error_shim",
        ":parser_output_cc_proto",
        "//third_party/spanner_pg/interface:parser_interface",
        "//third_party/spanner_pg/util:postgres",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "error_shim_test",
    srcs = ["error_shim_test.cc"],
    copts = [
        # The ereport module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    deps = [
        ":error_shim",
        "//third_party/spanner_pg/function_evaluators:pg_evaluators_implementations",  # build_cleaner: keep
        "//third_party/spanner_pg/interface:pg_evaluators",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "//third_party/spanner_pg/util:pg_list_iterators",
        "//third_party/spanner_pg/util:postgres",
        "//third_party/spanner_pg/util:valid_memory_context_fixture",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "stub_memory_reservation_manager",
    hdrs = [
        "stub_memory_reservation_manager.h",
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
    deps = [
        "//third_party/spanner_pg/interface:memory_reservation_manager",
        "@com_google_zetasql//zetasql/base",
    ],
)

cc_library(
    name = "memory_reservation_holder",
    srcs = ["memory_reservation_holder.cc"],
    hdrs = ["memory_reservation_holder.h"],
    copts = [
        # The ereport module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    deps = [
        "//third_party/spanner_pg/interface:memory_reservation_manager",
        "//third_party/spanner_pg/src/spangres:memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_zetasql//zetasql/base",
        "@com_google_zetasql//zetasql/base:status",
    ],
)

cc_test(
    name = "memory_reservation_holder_test",
    srcs = ["memory_reservation_holder_test.cc"],
    copts = [
        # The ereport module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    deps = [
        ":error_shim",
        ":memory_context_manager",
        ":memory_reservation_holder",
        ":stub_memory_reservation_manager",
        "//third_party/spanner_pg/interface:memory_reservation_manager",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "//third_party/spanner_pg/src/spangres:memory",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:optional",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "memory_context_manager",
    srcs = ["memory_context_manager.cc"],
    hdrs = ["memory_context_manager.h"],
    copts = [
        # The ereport module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    deps = [
        ":error_shim",
        "//third_party/spanner_pg/postgres_includes",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/utility",
        "@com_google_zetasql//zetasql/base",
        "@com_google_zetasql//zetasql/base:logging",
        "@com_google_zetasql//zetasql/base:ret_check",
    ],
)

cc_library(
    name = "memory_context_pg_arena",
    hdrs = ["memory_context_pg_arena.h"],
    copts = [
        # The ereport module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    visibility = ["//:__subpackages__"],
    deps = [
        ":memory_context_manager",
        ":memory_reservation_holder",
        ":stub_memory_reservation_manager",
        "//third_party/spanner_pg/interface:parser_interface",
        "@com_google_absl//absl/status",
    ],
)

cc_test(
    name = "memory_context_manager_test",
    srcs = ["memory_context_manager_test.cc"],
    copts = [
        # The ereport module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    deps = [
        ":memory_context_manager",
        ":memory_reservation_holder",
        ":stub_memory_reservation_manager",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "//third_party/spanner_pg/src/spangres:memory",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "serializer_deserializer_test",
    srcs = ["serializer_deserializer_test.cc"],
    copts = [
        # The ereport module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    deps = [
        ":error_shim",
        "//third_party/spanner_pg/src/backend:backend_with_stub_shims",
        "//third_party/spanner_pg/util:postgres",
        "//third_party/spanner_pg/util:valid_memory_context_fixture",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "timezone_helper",
    srcs = ["timezone_helper.cc"],
    hdrs = ["timezone_helper.h"],
    copts = [
        # The ereport module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    deps = [
        ":error_shim",
        "//third_party/spanner_pg/postgres_includes",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_zetasql//zetasql/base:ret_check",
    ],
)

cc_test(
    name = "timezone_helper_test",
    srcs = ["timezone_helper_test.cc"],
    copts = [
        # The ereport module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    deps = [
        ":memory_context_manager",
        ":memory_reservation_holder",
        ":stub_memory_reservation_manager",
        ":timezone_helper",
        "//third_party/spanner_pg/interface:memory_reservation_manager",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/src/backend:backend_with_stub_shims",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "assert_test",
    srcs = ["assert_test.cc"],
    copts = [
        # The ereport module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    deps = [
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/src/backend:backend_with_stub_shims",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "postgres_end_to_end_test",
    srcs = ["postgres_end_to_end_test.cc"],
    copts = [
        # The ereport module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    deps = [
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/test_catalog",
        "//third_party/spanner_pg/util:unittest_utils",
        "//third_party/spanner_pg/util:valid_memory_context_fixture",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)
