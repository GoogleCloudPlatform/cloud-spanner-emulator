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

licenses(["notice"])

cc_library(
    name = "nodes",
    srcs = [
        "bitmapset.c",
        "copyfuncs.c",
        "equalfuncs.c",
        "extensible.c",
        "list.c",
        "makefuncs.c",
        "multibitmapset.c",
        "nodeFuncs.c",
        "nodes.c",
        "outfuncs.c",
        "params.c",
        "print.c",
        "queryjumblefuncs.c",
        "read.c",
        "readfuncs.c",
        "tidbitmap.c",
        "value.c",
    ],
    copts = [
        "-D_GNU_SOURCE",
        "-fexceptions",
    ],
    includes = ["."],
    textual_hdrs = [
        "copyfuncs.funcs.c",
        "copyfuncs.switch.c",
        "equalfuncs.funcs.c",
        "equalfuncs.switch.c",
        "outfuncs.funcs.c",
        "outfuncs.switch.c",
        "readfuncs.funcs.c",
        "readfuncs.switch.c",
        "queryjumblefuncs.funcs.c",
        "queryjumblefuncs.switch.c",
    ],
    deps = [
        "//third_party/spanner_pg/src/include",
        "//third_party/spanner_pg/util:nodetag_to_string",
    ],
    alwayslink = 1,
)

cc_test(
    name = "serializer_deserializer_test",
    srcs = ["serializer_deserializer_test.cc"],
    deps = [
        "//third_party/spanner_pg/src/backend:backend_with_stub_shims",
        "//third_party/spanner_pg/util:postgres",
        "//third_party/spanner_pg/util:valid_memory_context_fixture",
        "@com_google_googlesql//googlesql/base/testing:status_matchers",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "node_tag_gen_include",
    hdrs = ["nodetags.h"],
    visibility = ["//:__subpackages__"],
    deps = [
        ":node_headers",
    ],
)

genrule(
    name = "node_headers",
    srcs = [
        "//third_party/spanner_pg/src/include:node_header_files",
    ],
    outs = [
        "copyfuncs.funcs.c",
        "copyfuncs.switch.c",
        "equalfuncs.funcs.c",
        "equalfuncs.switch.c",
        "nodetags.h",
        "outfuncs.funcs.c",
        "outfuncs.switch.c",
        "readfuncs.funcs.c",
        "readfuncs.switch.c",
        "queryjumblefuncs.funcs.c",
        "queryjumblefuncs.switch.c",
    ],
    cmd = """perl -I$$(dirname $(location //third_party/spanner_pg/src/backend/catalog:perl_utils)) $(location gen_node_support.pl) --outdir $(RULEDIR) $(SRCS)""",
    tools = [
        "gen_node_support.pl",
        "//third_party/spanner_pg/src/backend/catalog:perl_utils",
    ],
    visibility = [
        "//:__subpackages__",
        "//third_party/spanner_pg/postgres_includes:__pkg__",
    ],
)
