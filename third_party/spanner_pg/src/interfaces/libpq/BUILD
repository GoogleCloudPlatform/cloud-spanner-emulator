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

package(
    default_visibility = [
        "//:__subpackages__",
        "//spanner/tests/spangres_end_to_end_test:__subpackages__",
    ],
    # Disable layering_check because the Postgres binary is one giant component.
    # Disable parse_headers because postgres implicitly includes postgres.h
    # before each header, so, the headers cannot be parsed independently.
    features = [
        "-layering_check",
        "-parse_headers",
    ],
)

licenses(["notice"])

filegroup(
    name = "client_include_files",
    srcs = [
        "libpq-fe.h",
        "pqexpbuffer.h",
    ],
)

cc_library(
    name = "headers",
    hdrs = [
        ":client_include_files",
    ],
    copts = [
        "-D_GNU_SOURCE",
        "-fexceptions",
    ],
    includes = [
        ".",
    ],
    deps = [
        "//third_party/spanner_pg/src/include:client_include",
    ],
)

cc_library(
    name = "libpq_static",
    srcs = [
        "fe-auth.c",
        "fe-auth.h",
        "fe-auth-sasl.h",
        "fe-auth-scram.c",
        "fe-connect.c",
        "fe-exec.c",
        "fe-lobj.c",
        "fe-misc.c",
        "fe-print.c",
        "fe-protocol3.c",
        "fe-secure.c",
        "fe-trace.c",
        "libpq-events.c",
        "libpq-events.h",
        "libpq-int.h",
        "pqexpbuffer.c",
        "test/libpq_uri_regress.c",
    ],
    hdrs = [
        "libpq-fe.h",
        "pqexpbuffer.h",
    ],
    copts = [
        "-DFRONTEND",
        "-D_GNU_SOURCE",
        "-fexceptions",
    ],
    includes = [
        ".",
    ],
    linkopts = [
        "-lpthread",
    ],
    deps = [
        "//third_party/spanner_pg/src/common:common_frontend",
        "//third_party/spanner_pg/src/port:port_frontend",
    ],
)

filegroup(
    name = "unused",
    srcs = [
        "fe-secure-common.c",
        "fe-secure-common.h",
        "fe-secure-openssl.c",
        "pthread-win32.c",
        "win32.c",
        "win32.h",
    ],
)

cc_binary(
    name = "libpq.so",
    copts = [
        "-D_GNU_SOURCE",
        "-fexceptions",
    ],
    linkopts = [
        "-Wl,--version-script",
        "$(location :exports.ld)",
    ],
    linkshared = 1,
    linkstatic = 1,
    deps = [
        ":exports.ld",
        ":libpq_static",
    ],
)

cc_library(
    name = "libpq",
    srcs = [
        ":libpq.so",
    ],
    hdrs = [
        "libpq-fe.h",
        "pqexpbuffer.h",
    ],
    copts = [
        "-D_GNU_SOURCE",
        "-fexceptions",
    ],
    includes = [
        ".",
    ],
    deps = [
        "//third_party/spanner_pg/src/include",
    ],
)
