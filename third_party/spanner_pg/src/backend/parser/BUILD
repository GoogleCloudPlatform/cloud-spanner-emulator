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

load("@com_google_zetasql//bazel:bison.bzl", "genyacc")
load("@com_google_zetasql//bazel:flex.bzl", "genlex")



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

licenses(["notice"])

cc_library(
    name = "parser",
    srcs = [
        "analyze.c",
        "gram.c",
        "parse_agg.c",
        "parse_clause.c",
        "parse_coerce.c",
        "parse_collate.c",
        "parse_cte.c",
        "parse_enr.c",
        "parse_expr.c",
        "parse_func.c",
        "parse_merge.c",
        "parse_node.c",
        "parse_oper.c",
        "parse_param.c",
        "parse_relation.c",
        "parse_target.c",
        "parse_type.c",
        "parse_utilcmd.c",
        "parser.c",
        "scan.c",
        "scansup.c",
    ],
    copts = [
        "-D_GNU_SOURCE",
        "-fexceptions",
    ],
    includes = [
        ".",
    ],
    textual_hdrs = [
        "gram.h",
    ],
    deps = [
        "//third_party/spanner_pg/shims:catalog_shim_cc_wrappers_header",
        "//third_party/spanner_pg/shims:catalog_shim_header",
        "//third_party/spanner_pg/shims:parser_shim",
        "//third_party/spanner_pg/src/include",
    ],
    alwayslink = 1,
)

genlex(
    name = "scan",
    src = "scan.l",
    out = "scan.c",
)

genyacc(
    name = "gram",
    src = "gram.y",
    extra_options = [
        "-Wno-deprecated",
        "-Werror",
    ],
    header_out = "gram.h",
    source_out = "gram.c",
)

cc_library(
    name = "include",
    hdrs = [
        "gram.h",
    ],
    copts = [
        "-D_GNU_SOURCE",
        "-fexceptions",
    ],
)
