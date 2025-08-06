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
    name = "adt",
    srcs = [
        "acl.c",
        "amutils.c",
        "array_expanded.c",
        "array_selfuncs.c",
        "array_typanalyze.c",
        "array_userfuncs.c",
        "arrayfuncs.c",
        "arraysubs.c",
        "arrayutils.c",
        "ascii.c",
        "bool.c",
        "cash.c",
        "char.c",
        "cryptohashfuncs.c",
        "date.c",
        "datetime.c",
        "datum.c",
        "dbsize.c",
        "domains.c",
        "encode.c",
        "enum.c",
        "expandeddatum.c",
        "expandedrecord.c",
        "float.c",
        "format_type.c",
        "formatting.c",
        "genfile.c",
        "geo_ops.c",
        "geo_selfuncs.c",
        "geo_spgist.c",
        "hbafuncs.c",
        "inet_cidr_ntop.c",
        "inet_net_pton.c",
        "int.c",
        "int8.c",
        "json.c",
        "jsonb.c",
        "jsonb_gin.c",
        "jsonb_op.c",
        "jsonb_util.c",
        "jsonbsubs.c",
        "jsonfuncs.c",
        "jsonpath.c",
        "jsonpath_exec.c",
        #   "jsonpath_gram.c",
        #   "jsonpath_scan.c",
        "like.c",
        "like_support.c",
        "lockfuncs.c",
        "mac.c",
        "mac8.c",
        "mcxtfuncs.c",
        "misc.c",
        "multirangetypes.c",
        "multirangetypes_selfuncs.c",
        "name.c",
        "network.c",
        "network_gist.c",
        "network_selfuncs.c",
        "network_spgist.c",
        "numeric.c",
        "numutils.c",
        "oid.c",
        "oracle_compat.c",
        "orderedsetaggs.c",
        "partitionfuncs.c",
        "pg_locale.c",
        "pg_lsn.c",
        "pg_upgrade_support.c",
        "pgstatfuncs.c",
        "pseudotypes.c",
        "quote.c",
        "rangetypes.c",
        "rangetypes_gist.c",
        "rangetypes_selfuncs.c",
        "rangetypes_spgist.c",
        "rangetypes_typanalyze.c",
        "regexp.c",
        "regproc.c",
        "ri_triggers.c",
        "rowtypes.c",
        "ruleutils.c",
        "selfuncs.c",
        "tid.c",
        "timestamp.c",
        "trigfuncs.c",
        "tsginidx.c",
        "tsgistidx.c",
        "tsquery.c",
        "tsquery_cleanup.c",
        "tsquery_gist.c",
        "tsquery_op.c",
        "tsquery_rewrite.c",
        "tsquery_util.c",
        "tsrank.c",
        "tsvector.c",
        "tsvector_op.c",
        "tsvector_parser.c",
        "uuid.c",
        "varbit.c",
        "varchar.c",
        "varlena.c",
        "version.c",
        "windowfuncs.c",
        "xid.c",
        "xid8funcs.c",
        "xml.c",
    ],
    # TODO: Fix the underlying integer overflows,
    # remove this flag, and enable aggressive optimizations.
    copts = [
        "-D_GNU_SOURCE",
        "-fwrapv",
        "-fexceptions",
    ],
    includes = ["."],
    textual_hdrs = [
        "levenshtein.c",
        "like_match.c",
        "jsonpath_gram.h",
    ],
    deps = [
        "//third_party/spanner_pg/interface:catalog_wrappers",
        "//third_party/spanner_pg/src/backend/utils:generated_headers",
        "//third_party/spanner_pg/src/include",
        "//third_party/spanner_pg/util:nodetag_to_string",
    ],
    alwayslink = 1,
)

cc_test(
    name = "pg_locale_test",
    srcs = ["pg_locale_test.cc"],
    deps = [
        "//third_party/spanner_pg/src/backend:backend_with_stub_shims",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

genlex(
    name = "jsonpath_scan",
    src = "jsonpath_scan.l",
    out = "jsonpath_scan.c",
)

genyacc(
    name = "jsonpath_gram",
    src = "jsonpath_gram.y",
    extra_options = ["-Wno-deprecated"],
    header_out = "jsonpath_gram.h",
    source_out = "jsonpath_gram.c",
)
