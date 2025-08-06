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
    default_visibility = ["//:__subpackages__"],
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
    name = "include",
    hdrs = glob(["**/*.h"]),
    copts = [
        "-D_GNU_SOURCE",
        "-fexceptions",
    ],
    includes = [
        ".",
        "catalog",
    ],
    visibility = [
        "//spanner/datatypes/common/jsonb:__pkg__",
        "//spanner/tests/spangres_end_to_end_test:__subpackages__",
        # Use spangres/postgres_includes for safe PostgreSQL headers in non-PostgreSQL code.
        "//third_party/spanner_pg/postgres_includes:__pkg__",
        "//third_party/spanner_pg/src:__subpackages__",
        # Bootstrap Catalog needs to directly parse the PostgreSQL headers to build the catalog.
        "//third_party/spanner_pg/bootstrap_catalog:__pkg__",
    ],
    deps = [
        "//third_party/spanner_pg/src/backend:backend_include",
        "//third_party/spanner_pg/src/backend/catalog:generated_catalog_headers",
        "//third_party/spanner_pg/src/backend/parser:include",
        "//third_party/spanner_pg/src/backend/utils:generated_headers",
        "//third_party/spanner_pg/util:integral_types",
    ],
)

filegroup(
    name = "all_headers",
    srcs = glob([
        "*.h",
        "**/*.h",
    ]),
)

cc_library(
    name = "snowball_include",
    hdrs = glob([
        "snowball/*.h",
        "snowball/**/*.h",
    ]),
    copts = [
        "-D_GNU_SOURCE",
        "-fexceptions",
    ],
    includes = [
        "snowball",
        "snowball/libstemmer",
    ],
)

cc_library(
    name = "client_include",
    srcs = [
        "pg_config_ext.h",
    ],
    hdrs = [
        "postgres_ext.h",
    ],
    copts = [
        "-D_GNU_SOURCE",
        "-fexceptions",
    ],
    includes = [
        ".",
    ],
)

cc_library(
    name = "kwlist_include",
    hdrs = [
        "parser/kwlist.h",
    ],
)

cc_library(
    name = "pg_config_include",
    hdrs = [
        "pg_config.h",
    ],
    deps = ["//third_party/spanner_pg/util:integral_types"],
)

filegroup(
    name = "client_include_files",
    srcs = [
        "pg_config_ext.h",
        "postgres_ext.h",
    ],
)

filegroup(
    name = "node_header",
    srcs = [
        "nodes/nodes.h",
    ],
    visibility = [
        # Use spangres/postgres_includes for safe PostgreSQL headers in non-PostgreSQL code.
        "//third_party/spanner_pg/postgres_includes:__pkg__",
    ],
)

filegroup(
    name = "catalog_headers",
    srcs = glob([
        "catalog/pg_*.h",
    ]) + [
        "catalog/indexing.h",
        "catalog/toasting.h",
    ],
)

filegroup(
    name = "catalog_datfiles",
    srcs = glob([
        "catalog/pg_*.dat",
    ]),
)

# Subset of catalog datfiles required for building fmgr headers.
# List is extracted from the code in src/backend/utils/Makefile.
filegroup(
    name = "fmgr_datfiles",
    srcs = [
        "catalog/pg_language.dat",
        "catalog/pg_proc.dat",
    ],
)

# Existing headers required for building generated fmgr headers.
# List is extracted from the code in src/backend/utils/Gen_fmgrtab.pl.
filegroup(
    name = "fmgr_dependencies",
    srcs = [
        "access/transam.h",
        "catalog/pg_language.h",
        "catalog/pg_proc.h",
    ],
)

filegroup(
    name = "kwlist_header",
    srcs = ["parser/kwlist.h"],
)

cc_library(
    name = "kwlist_d_header",
    hdrs = [":kwlist_d_header_gen"],
)

# Generate kwlist_d.h from kwlist.h
genrule(
    name = "kwlist_d_header_gen",
    srcs = [
        ":kwlist_header",
        "//third_party/spanner_pg/src:tools_gen_keywords",
        "//third_party/spanner_pg/src:tools_perfhash",
    ],
    outs = ["kwlist_d.h"],
    cmd = """perl $(location //third_party/spanner_pg/src:tools_gen_keywords) --output /tmp --extern $(location :kwlist_header)

    mv /tmp/kwlist_d.h $(location kwlist_d.h)
""",
    tools = [
        "//third_party/spanner_pg/src:tools_gen_keywords",
        "//third_party/spanner_pg/src:tools_perfhash",
    ],
)
