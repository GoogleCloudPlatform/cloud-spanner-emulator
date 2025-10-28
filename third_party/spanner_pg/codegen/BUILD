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

# Catalog generated code

load("@com_google_protobuf//bazel:py_proto_library.bzl", "py_proto_library")
load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library")
load("@rules_proto//proto:defs.bzl", "proto_library")
load("@rules_python//python:defs.bzl", "py_binary")

package(
    default_visibility = [
        "//visibility:private",
    ],
)

licenses(["notice"])

###################################
# Function catalog generation

proto_library(
    name = "postgresql_catalog_proto",
    srcs = ["postgresql_catalog.proto"],
    deps = [
        "@com_google_zetasql//zetasql/public:function_proto",
    ],
)

cc_proto_library(
    name = "postgresql_catalog_cc_proto",
    visibility = [
        "//:__subpackages__",
    ],
    deps = [":postgresql_catalog_proto"],
)

genrule(
    name = "gen_emulator_postgresql_catalog_embed",
    srcs = ["emulator_postgresql_catalog.textproto"],
    outs = ["emulator_postgresql_catalog_embed.h"],
    cmd = "echo 'namespace postgres_translator { constexpr char kPostgreSQLCatalogEmbed[] = R\"(' > $@ && cat $(location emulator_postgresql_catalog.textproto) >> $@ && echo ')\"; }' >> $@",
)

cc_library(
    name = "emulator_postgresql_catalog_embed",
    hdrs = ["emulator_postgresql_catalog_embed.h"],
    deps = [":gen_emulator_postgresql_catalog_embed"],
)

cc_library(
    name = "postgresql_catalog_reader",
    srcs = ["postgresql_catalog_reader.cc"],
    hdrs = ["postgresql_catalog_reader.h"],
    visibility = [
        "//:__subpackages__",
    ],
    deps = [
        ":emulator_postgresql_catalog_embed",
        ":postgresql_catalog_cc_proto",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "@com_google_protobuf//:protobuf",
    ],
)

###################################
# Bootstrap catalog generation
py_proto_library(
    name = "postgresql_catalog_py_proto",
    deps = [":postgresql_catalog_proto"],
)

py_binary(
    name = "gen_pg_proc",
    srcs = ["gen_pg_proc.py"],
    data = [
        "emulator_postgresql_catalog.textproto",
        "pg_proc.dat.jinja2",
    ],
    deps = [
        ":postgresql_catalog_py_proto",
        "@com_google_protobuf//:protobuf_python",
        "@jinja2",
    ],
)

genrule(
    name = "run_pg_proc",
    srcs = [
        "emulator_postgresql_catalog.textproto",
    ],
    outs = [
        "pg_proc.dat",
    ],
    cmd = "$(location :gen_pg_proc) > $(OUTS)",
    tools = [":gen_pg_proc"],
    visibility = [
        "//:__subpackages__",
    ],
)
