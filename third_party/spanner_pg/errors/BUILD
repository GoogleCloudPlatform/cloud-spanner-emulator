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

# -*- mode: python; -*-
#
# Description:
# Libraries shared by all spangres code.
#

load("@bazel_skylib//:bzl_library.bzl", "bzl_library")
load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library", "cc_test")
load("//third_party/spanner_pg/errors:error_generator.bzl", "spangres_error_catalog")

package(
    default_visibility = [
        "//:__subpackages__",
    ],
)

licenses(["notice"])

exports_files(
    ["error_catalog.textproto"],
    visibility = [
        "//:__subpackages__",
    ],
)

spangres_error_catalog(
    name = "error_catalog",
)

bzl_library(
    name = "error_generator_bzl",
    srcs = ["error_generator.bzl"],
    visibility = ["//visibility:private"],
    deps = ["@rules_cc//cc:defs.bzl"],
)

cc_binary(
    name = "error_generator",
    srcs = ["error_generator.cc"],
    deps = [
        ":errors_cc_proto",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/flags:parse",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_googleapis//google/rpc:code_cc_proto",
        "@com_google_googleapis//google/rpc:error_details_cc_proto",
        "@com_google_protobuf//:protobuf",
        "@com_google_zetasql//zetasql/base",
        "@com_google_zetasql//zetasql/base:file_util",
        "@com_google_zetasql//zetasql/base:path",
        "@com_google_zetasql//zetasql/base:status",
        "@com_googlesource_code_re2//:re2",
    ],
)

proto_library(
    name = "errors_proto",
    srcs = ["errors.proto"],
    deps = [
        "@com_google_googleapis//google/rpc:code_proto",
        "@com_google_googleapis//google/rpc:status_proto",
    ],
)

cc_proto_library(
    name = "errors_cc_proto",
    visibility = ["//:__subpackages__"],
    deps = [":errors_proto"],
)

cc_library(
    name = "errors",
    srcs = ["errors.cc"],
    hdrs = ["errors.h"],
    visibility = ["//:__subpackages__"],
    deps = [
        ":errors_cc_proto",
        "//third_party/spanner_pg/postgres_includes",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_zetasql//zetasql/base:logging",
    ],
)

cc_test(
    name = "errors_test",
    srcs = ["errors_test.cc"],
    tags = ["spanner.spangres"],
    deps = [
        ":errors",
        "//third_party/spanner_pg/postgres_includes",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

# THIS LIBRARY CAN THROW EXCEPTIONS!
# Specifically, the "spangres_throw_exception()" method will always throw.
# (Other methods exposed by this library will not throw.)
# Callers must be prepared to handle or re-throw the exception.
cc_library(
    name = "ereport",
    srcs = ["ereport.cc"],
    copts = [
        # The ereport module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    features = ["-use_header_modules"],
    deps = [
        "//third_party/spanner_pg/interface:ereport",
        "//third_party/spanner_pg/postgres_includes",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/base:die_if_null",
    ],
    alwayslink = 1,
)

cc_test(
    name = "ereport_test",
    srcs = ["ereport_test.cc"],
    copts = [
        # The ereport module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    features = ["-use_header_modules"],
    tags = ["spanner.spangres"],
    deps = [
        ":ereport",
        "//third_party/spanner_pg/interface:ereport",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "//third_party/spanner_pg/util:valid_memory_context_fixture",
        "@com_google_absl//absl/log:scoped_mock_log",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)
