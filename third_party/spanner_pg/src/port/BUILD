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

load("@rules_cc//cc:defs.bzl", "cc_library")

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

filegroup(
    name = "_common_files",
    srcs = [
        "bsearch_arg.c",
        "chklocale.c",
        "explicit_bzero.c",
        "fls.c",
        "getpeereid.c",
        "inet_net_ntop.c",
        "noblock.c",
        "path.c",
        "pg_bitutils.c",
        "pg_config_paths.h",
        "pg_crc32c_sb8.c",
        "pg_strong_random.c",
        "pgcheckdir.c",
        "pgmkdirp.c",
        "pgsleep.c",
        "pgstrcasecmp.c",
        "pgstrsignal.c",
        "pqsignal.c",
        "pread.c",
        "pwrite.c",
        "pwritev.c",
        "qsort.c",
        "qsort_arg.c",
        "quotes.c",
        "snprintf.c",
        "strerror.c",
        "strlcat.c",
        "strlcpy.c",
        "tar.c",
        "thread.c",
    ] + select({
        "@platforms//cpu:aarch64": [
            "pg_crc32c_armv8.c",
            "pg_crc32c_armv8_choose.c",
        ],
        "@platforms//cpu:x86_64": [
            "pg_crc32c_sse42.c",
            "pg_crc32c_sse42_choose.c",
        ],
        "//conditions:default": [],
    }),
)

filegroup(
    name = "unused",
    srcs = [
        "dirent.c",
        "dirmod.c",
        "getaddrinfo.c",
        "getopt.c",
        "getopt_long.c",
        "getrusage.c",
        "gettimeofday.c",
        "inet_aton.c",
        "kill.c",
        "mkdtemp.c",
        "open.c",
        "pg_crc32c_armv8.c",
        "pg_crc32c_armv8_choose.c",
        "pthread-win32.h",
        "strnlen.c",
        "system.c",
        "unsetenv.c",
        "win32env.c",
        "win32error.c",
        "win32security.c",
        "win32setlocale.c",
    ],
)

cc_library(
    name = "port_srv",
    srcs = [
        ":_common_files",
    ],
    copts = [
        "-D_GNU_SOURCE",
        "-fexceptions",
    ] + select({
        # _mm_crc32_* calls can't be inlined without this compiler option in
        # GCC.
        "@platforms//cpu:aarch64": [
            "-march=armv8-a+crc",
        ],
        # __crc32* calls can't be inlined without this compiler option in GCC.
        "@platforms//cpu:x86_64": [
            "-msse4.2",
        ],
        "//conditions:default": [],
    }),
    deps = [
        "//third_party/spanner_pg/src/include",
    ],
)

cc_library(
    name = "port_frontend",
    srcs = [
        ":_common_files",
    ],
    copts = [
        "-D_GNU_SOURCE",
        "-DFRONTEND",
        "-fexceptions",
    ],
    includes = [
        ".",
    ],
    deps = [
        "//third_party/spanner_pg/src/include",
    ],
)
