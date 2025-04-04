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

# TODO: fix linker errors for initdb
# cc_binary(
#     name = "initdb",
#     srcs = [
#         "findtimezone.c",
#         "initdb.c",
#     ],
#     copts = [
#         "-DFRONTEND",
#         "-DSYSTEMTZDIR=\\\"/usr/share/zoneinfo\\\"",
#         "-D_GNU_SOURCE",
#         "-fexceptions",
#     ],
#     linkopts = [
#         "-lrt",
#         "-Wl,-rpath=\\$$ORIGIN/../lib",
#         "-Wl,-rpath=\\$$EXEC_ORIGIN/../lib",
#     ],
#     deps = [
#         "//third_party/spanner_pg/src/common:common_frontend",
#         "//third_party/spanner_pg/src/fe_utils",
#         "//third_party/spanner_pg/src/include",
#         "//third_party/spanner_pg/src/interfaces/libpq",
#         "//third_party/spanner_pg/src/port:port_frontend",
#         "//third_party/spanner_pg/src/timezone:timezone_frontend",
#     ],
# )
