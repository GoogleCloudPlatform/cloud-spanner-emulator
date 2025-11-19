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

load("@rules_cc//cc:defs.bzl", "cc_library", "cc_test")



package(
    default_visibility = [
        "//:__subpackages__",
    ],
    # Disable layering_check because the Postgres binary is one giant component.
    # Disable parse_headers because postgres implicitly includes postgres.h
    # before each header, so, the headers cannot be parsed independently.
    features = [
        "-layering_check",
        "-parse_headers",
    ],
)

licenses(["restricted"])

cc_library(
    name = "cache",
    srcs = [
        "attoptcache.c",
        "catcache.c",
        "evtcache.c",
        "inval.c",
        "lsyscache.c",
        "partcache.c",
        "plancache.c",
        "relcache.c",
        "relfilenodemap.c",
        "relmapper.c",
        "spccache.c",
        "syscache.c",
        "ts_cache.c",
        "typcache.c",
    ],
    copts = [
        "-D_GNU_SOURCE",
        "-fexceptions",
    ],
    deps = [
        "//third_party/spanner_pg/interface:catalog_wrappers",
        "//third_party/spanner_pg/src/include",
    ],
    alwayslink = 1,
)

cc_test(
    name = "lsyscache_test",
    srcs = ["lsyscache_test.cc"],
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
        ":cache",
        "//third_party/spanner_pg/interface:ereport",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "//third_party/spanner_pg/util:valid_memory_context_fixture",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)
