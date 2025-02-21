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

# Header-only (or -mostly) that export PostgreSQL symbols
# without polluting the global macro namespace, and in
# an #include-order-independent way

licenses(["notice"])

package(
    default_visibility = ["//:__subpackages__"],
    # Disable parse_headers because postgres implicitly includes postgres.h
    # before each header, so, the headers cannot be parsed independently.
    features = [
        "-parse_headers",
    ],
)

# Defines a placeholder Postgres global, "progname", to permit headless (without main.c)
# Postgres compilation.
cc_library(
    name = "placeholder_progname",
    srcs = ["placeholder_progname.cc"],
    copts = [
        # This module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport.h` for details.
        "-fexceptions",
    ],
    features = ["-use_header_modules"],  # Incompatible with -fexceptions.
    tags = ["keep_dep"],
)

cc_library(
    name = "postgres_includes",
    srcs = [
        "end-postgres-header-region.inc",
        "pg-exports.h",
        "pg-include-list.h",
        "start-postgres-header-region.inc",
    ],
    hdrs = [
        "all.h",
    ],
    copts = [
        # This module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport.h` for details.
        "-fexceptions",
    ],
    features = ["-use_header_modules"],  # Incompatible with -fexceptions.
    visibility = [
        "//:__subpackages__",
        "//spanner/tests/spangres_end_to_end_test:__subpackages__",
    ],
    deps = [
        ":placeholder_progname",
        "//third_party/spanner_pg/src/backend/catalog:generated_catalog_headers",
        "//third_party/spanner_pg/src/backend/utils:generated_headers",
        "//third_party/spanner_pg/src/include",
    ],
)

cc_library(
    name = "deparser",
    srcs = ["deparser.cc"],
    hdrs = ["deparser.h"],
    copts = [
        # This module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport.h` for details.
        "-fexceptions",
    ],
    features = ["-use_header_modules"],  # Incompatible with -fexceptions.
    deps = [
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/src/backend",
    ],
)

genrule(
    name = "generate_nodes_inc",
    srcs = [
        "//third_party/spanner_pg/src/include:node_header",
    ],
    outs = [
        "nodes.inc",
    ],
    cmd = """\
        sed -rn "s/^\\s+T_([A-Za-z0-9_]+),$$/NODE(\\1)/p" \
            < $(location //third_party/spanner_pg/src/include:node_header) \
            | sort > $(location nodes.inc)
    """,
)

cc_library(
    name = "nodes_inc_header",
    hdrs = [
        ":generate_nodes_inc",
    ],
)

cc_library(
    name = "nodes_inc",
    srcs = [
        "placeholder.cc",
    ],
    hdrs = [
        "nodes.inc",
    ],
    deps = [
        ":nodes_inc_header",
    ],
)
