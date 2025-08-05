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
    name = "_common",
    srcs = [
        "archive.c",
        "base64.c",
        "checksum_helper.c",
        "compression.c",
        "config_info.c",
        "controldata_utils.c",
        "cryptohash.c",
        "encnames.c",
        "exec.c",
        "f2s.c",
        "file_perm.c",
        "file_utils.c",
        "hashfn.c",
        "hmac.c",
        "ip.c",
        "jsonapi.c",
        "keywords.c",
        "kwlookup.c",
        "link-canary.c",
        "md5.c",
        "md5_common.c",
        "pg_get_line.c",
        "pg_lzcompress.c",
        "pg_prng.c",
        "pgfnames.c",
        "psprintf.c",
        "relpath.c",
        "rmtree.c",
        "saslprep.c",
        "scram-common.c",
        "sha1.c",
        "sha2.c",
        "string.c",
        "stringinfo.c",
        "unicode_norm.c",
        "username.c",
        "wait_error.c",
        "wchar.c",
    ],
)

filegroup(
    name = "_common_hdrs",
    srcs = [
        "md5_int.h",
        "sha1_int.h",
        "sha2_int.h",
    ],
)

filegroup(
    name = "unused",
    srcs = [
        "unicode/norm_test.c",
    ],
)

filegroup(
    name = "_frontend",
    srcs = [
        "fe_memutils.c",
        "logging.c",
        "restricted_token.c",
        "sprompt.c",
    ],
)

cc_library(
    name = "common_srv",
    srcs = [
        ":_common",
    ],
    hdrs = [
        ":_common_hdrs",
    ],
    copts = [
        "-D_GNU_SOURCE",
        "-fexceptions",
    ],
    deps = [
        ":d2s",
        "//third_party/spanner_pg/src/include",
        "//third_party/spanner_pg/src/include:kwlist_d_header",
        "//third_party/spanner_pg/src/port:port_srv",
        "@net_zstd//:zstdlib",
        "@zlib",
    ],
)

cc_library(
    name = "common_frontend",
    srcs = [
        ":_common",
        ":_frontend",
    ],
    hdrs = [
        ":_common_hdrs",
    ],
    copts = [
        "-DFRONTEND",
        "-D_GNU_SOURCE",
        "-fexceptions",
    ],
    deps = [
        ":d2s",
        "//third_party/spanner_pg/src/include",
        "//third_party/spanner_pg/src/include:kwlist_d_header",
        "//third_party/spanner_pg/src/port:port_frontend",
        "@net_zstd//:zstdlib",
        "@zlib",
    ],
)

cc_library(
    name = "d2s",
    srcs = ["d2s.c"],
    hdrs = [
        "d2s_full_table.h",
        "d2s_intrinsics.h",
        "digit_table.h",
        "ryu_common.h",
    ],
    deps = [
        "//third_party/spanner_pg/src/include",
    ],
)
