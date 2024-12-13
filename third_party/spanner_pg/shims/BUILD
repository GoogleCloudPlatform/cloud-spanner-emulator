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

cc_library(
    name = "parser_shim",
    srcs = ["parser_shim.cc"],
    hdrs = ["parser_shim.h"],
    copts = [
        # The ereport_shim module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport_shim.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    deps = [
        "//third_party/spanner_pg/postgres_includes",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/types:optional",
    ],
)

# Error shim intercepts the Postgres error handler to jump back into shim code
# and translate the error to a util::Status error.
# This library DOES NOT throw exceptions, but it uses them internally.
cc_library(
    name = "error_shim",
    srcs = ["error_shim.cc"],
    hdrs = ["error_shim.h"],
    copts = [
        # The ereport_shim module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport_shim.h` for details.
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
        ":catalog_shim_header",
        ":ereport_shim",
        ":parser_shim",
        "//third_party/spanner_pg/errors",
        "//third_party/spanner_pg/errors:error_catalog",
        "//third_party/spanner_pg/interface:parser_interface",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/postgres_includes:deparser",
        "//third_party/spanner_pg/src/backend",
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
        # The ereport_shim module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport_shim.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    deps = [
        ":error_shim",
        ":parser_output_cc_proto",
        ":parser_shim",
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
        # The ereport_shim module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport_shim.h` for details.
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

# THIS LIBRARY CAN THROW EXCEPTIONS!
# Specifically, the "spangres_throw_exception()" method will always throw.
# (Other methods exposed by this library will not throw.)
# Callers must be prepared to handle or re-throw the exception.
cc_library(
    name = "ereport_shim",
    srcs = ["ereport_shim.cc"],
    hdrs = ["ereport_shim.h"],
    copts = [
        # The ereport_shim module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport_shim.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    deps = [
        "//third_party/spanner_pg/postgres_includes",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/base:die_if_null",
    ],
)

cc_test(
    name = "ereport_shim_test",
    srcs = ["ereport_shim_test.cc"],
    copts = [
        # The ereport_shim module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport_shim.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    deps = [
        ":ereport_shim",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "//third_party/spanner_pg/util:valid_memory_context_fixture",
        "@com_google_absl//absl/log:scoped_mock_log",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

# Memory shim intercepts Postgres malloc calls and makes a corresponding
# reservation in Spanner before performing the allocation.
cc_library(
    name = "memory_shim",
    srcs = ["memory_shim.cc"],
    hdrs = [
        "memory_shim.h",
        "memory_shim_cc.h",
    ],
    copts = [
        # The ereport_shim module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport_shim.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    deps = [
        "//third_party/spanner_pg/interface:memory_reservation_manager",
        "//third_party/spanner_pg/postgres_includes",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/base",
    ],
    alwayslink = 1,
)

cc_library(
    name = "stub_memory_reservation_manager",
    hdrs = [
        "stub_memory_reservation_manager.h",
    ],
    copts = [
        # The ereport_shim module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport_shim.h` for details.
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
        # The ereport_shim module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport_shim.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    deps = [
        ":memory_shim",
        "//third_party/spanner_pg/interface:memory_reservation_manager",
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
        # The ereport_shim module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport_shim.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    deps = [
        ":error_shim",
        ":memory_context_manager",
        ":memory_reservation_holder",
        ":memory_shim",
        ":stub_memory_reservation_manager",
        "//third_party/spanner_pg/interface:memory_reservation_manager",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
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
        # The ereport_shim module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport_shim.h` for details.
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
        # The ereport_shim module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport_shim.h` for details.
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
        # The ereport_shim module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport_shim.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    deps = [
        ":memory_context_manager",
        ":memory_reservation_holder",
        ":memory_shim",
        ":stub_memory_reservation_manager",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "catalog_shim_cc_wrappers_header",
    hdrs = [
        "catalog_shim_cc_wrappers.h",
    ],
    copts = [
        # The ereport_shim module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport_shim.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    deps = [
        "//third_party/spanner_pg/postgres_includes",
    ],
)

cc_library(
    name = "catalog_shim_cc_wrappers",
    srcs = [
        "catalog_shim_cc_wrappers.cc",
    ],
    hdrs = [
        "catalog_shim_cc_wrappers.h",
    ],
    copts = [
        # The ereport_shim module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport_shim.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    deps = [
        ":catalog_shim_header",
        ":catalog_shim_transforms",
        "//third_party/spanner_pg/bootstrap_catalog",
        "//third_party/spanner_pg/catalog:catalog_adapter",
        "//third_party/spanner_pg/catalog:catalog_adapter_holder",
        "//third_party/spanner_pg/catalog:udf_support",
        "//third_party/spanner_pg/errors:error_catalog",
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
)

# This test tests a library that throws exceptions.
# It doesn't throw exceptions itself, but it does catch thrown exceptions.
cc_test(
    name = "catalog_shim_cc_wrappers_test",
    srcs = ["catalog_shim_cc_wrappers_test.cc"],
    copts = [
        # The ereport_shim module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport_shim.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    deps = [
        ":catalog_shim",
        ":catalog_shim_cc_wrappers",
        ":ereport_shim",
        "//third_party/spanner_pg/bootstrap_catalog",
        "//third_party/spanner_pg/catalog:catalog_adapter_holder",
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

cc_library(
    name = "catalog_shim_header",
    hdrs = [
        "catalog_shim.h",
    ],
    copts = [
        # The ereport_shim module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport_shim.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    deps = [
        "//third_party/spanner_pg/postgres_includes",
    ],
)

cc_library(
    name = "catalog_shim",
    srcs = [
        "catalog_shim.c",
        "catalog_shim_deparser.c",
        "catalog_shim_expected_errors.c",
        "catalog_shim_ruleutils.c",
        "catalog_shim_tokenlist.c",
        "catalog_shim_typcache.c",
    ],
    hdrs = [
        "catalog_shim.h",
    ],
    copts = [
        # The ereport_shim module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport_shim.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    deps = [
        ":catalog_shim_cc_wrappers",
        ":catalog_shim_header",
        ":parser_shim",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/util:nodetag_to_string",
    ],
    alwayslink = 1,
)

cc_library(
    name = "fake_catalog_shim",
    srcs = [
        "fake_catalog_shim.c",
    ],
    hdrs = [
        "catalog_shim.h",
    ],
    copts = [
        # The ereport_shim module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport_shim.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    deps = [
        ":catalog_shim_header",
        "//third_party/spanner_pg/postgres_includes",
    ],
)

# Unit tests of public functions in catalog_shim package.
cc_test(
    name = "catalog_shim_test",
    srcs = [
        "catalog_shim_test.cc",
    ],
    copts = [
        # The ereport_shim module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport_shim.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    deps = [
        ":catalog_shim",
        ":catalog_shim_transforms",
        ":ereport_shim",
        ":error_shim",
        ":parser_shim",
        "//third_party/spanner_pg/bootstrap_catalog",
        "//third_party/spanner_pg/catalog:catalog_adapter_holder",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/postgres_includes:deparser",
        "//third_party/spanner_pg/src/backend",
        "//third_party/spanner_pg/src/backend/catalog:generated_catalog_headers",
        "//third_party/spanner_pg/test_catalog",
        "//third_party/spanner_pg/util:nodetag_to_string",
        "//third_party/spanner_pg/util:pg_list_iterators",
        "//third_party/spanner_pg/util:postgres",
        "//third_party/spanner_pg/util:unittest_utils",
        "//third_party/spanner_pg/util:valid_memory_context_fixture",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "catalog_shim_transforms",
    srcs = ["catalog_shim_transforms.cc"],
    hdrs = ["catalog_shim_transforms.h"],
    copts = [
        # The ereport_shim module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport_shim.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    deps = [
        "//third_party/spanner_pg/catalog:table_name",
        "//third_party/spanner_pg/postgres_includes",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "serializer_deserializer_test",
    srcs = ["serializer_deserializer_test.cc"],
    copts = [
        # The ereport_shim module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport_shim.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    deps = [
        ":error_shim",
        ":parser_shim",
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
        # The ereport_shim module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport_shim.h` for details.
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
        # The ereport_shim module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport_shim.h` for details.
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
        # The ereport_shim module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport_shim.h` for details.
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
        # The ereport_shim module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport_shim.h` for details.
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

cc_library(
    name = "json_shim",
    srcs = ["json_shim.c"],
    copts = [
        # The ereport_shim module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport_shim.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
)

cc_library(
    name = "pg_locale_shim",
    srcs = ["pg_locale_shim.cc"],
    hdrs = ["pg_locale_shim.h"],
)

cc_test(
    name = "pg_locale_shim_test",
    srcs = ["pg_locale_shim_test.cc"],
    deps = [
        ":pg_locale_shim",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "regex_shim_header",
    hdrs = [
        "regex_shim.h",
    ],
)
