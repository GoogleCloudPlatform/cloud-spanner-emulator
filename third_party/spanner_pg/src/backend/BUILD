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

licenses(["notice"])

cc_library(
    name = "backend_with_shims",
    visibility = [
        "//:__subpackages__",
        "//spanner/datatypes/common/jsonb:__subpackages__",
    ],
    deps = [
        ":backend",
        "//third_party/spanner_pg/shims:catalog_shim",
        "//third_party/spanner_pg/shims:memory_shim",
    ],
)

cc_library(
    name = "backend_with_stub_shims",
    deps = [
        ":backend",
        "//third_party/spanner_pg/shims:fake_catalog_shim",
        # `memory_shim` is very widely used and very lightweight.
        # Don't stub it, just use it.
        "//third_party/spanner_pg/shims:memory_shim",
    ],
)

cc_library(
    name = "backend",
    srcs = [
        "partitioning/partbounds.c",
        "partitioning/partdesc.c",
        "partitioning/partprune.c",
    ],
    copts = [
        "-D_GNU_SOURCE",
        "-fexceptions",
    ],
    features = ["-use_header_modules"],  # Incompatible with -fexceptions.
    linkopts = [
        "-ldl",
        "-lpthread",
        "-lrt",
    ],
    deps = [
        "//third_party/spanner_pg/shims:catalog_shim_header",
        "//third_party/spanner_pg/shims:syscache_shim",
        "//third_party/spanner_pg/src/backend/access/brin",
        "//third_party/spanner_pg/src/backend/access/common",
        "//third_party/spanner_pg/src/backend/access/gin",
        "//third_party/spanner_pg/src/backend/access/gist",
        "//third_party/spanner_pg/src/backend/access/hash",
        "//third_party/spanner_pg/src/backend/access/heap",
        "//third_party/spanner_pg/src/backend/access/index",
        "//third_party/spanner_pg/src/backend/access/nbtree",
        "//third_party/spanner_pg/src/backend/access/rmgrdesc",
        "//third_party/spanner_pg/src/backend/access/spgist",
        "//third_party/spanner_pg/src/backend/access/table",
        "//third_party/spanner_pg/src/backend/access/tablesample",
        "//third_party/spanner_pg/src/backend/access/transam",
        "//third_party/spanner_pg/src/backend/bootstrap",
        "//third_party/spanner_pg/src/backend/catalog",
        "//third_party/spanner_pg/src/backend/commands",
        "//third_party/spanner_pg/src/backend/executor",
        "//third_party/spanner_pg/src/backend/foreign",
        "//third_party/spanner_pg/src/backend/jit",
        "//third_party/spanner_pg/src/backend/lib",
        "//third_party/spanner_pg/src/backend/libpq",
        "//third_party/spanner_pg/src/backend/nodes",
        "//third_party/spanner_pg/src/backend/optimizer/geqo",
        "//third_party/spanner_pg/src/backend/optimizer/path",
        "//third_party/spanner_pg/src/backend/optimizer/plan",
        "//third_party/spanner_pg/src/backend/optimizer/prep",
        "//third_party/spanner_pg/src/backend/optimizer/util",
        "//third_party/spanner_pg/src/backend/parser",
        "//third_party/spanner_pg/src/backend/port",
        "//third_party/spanner_pg/src/backend/postmaster",
        "//third_party/spanner_pg/src/backend/regex",
        "//third_party/spanner_pg/src/backend/replication",
        "//third_party/spanner_pg/src/backend/replication/logical",
        "//third_party/spanner_pg/src/backend/rewrite",
        "//third_party/spanner_pg/src/backend/statistics",
        "//third_party/spanner_pg/src/backend/storage/buffer",
        "//third_party/spanner_pg/src/backend/storage/file",
        "//third_party/spanner_pg/src/backend/storage/freespace",
        "//third_party/spanner_pg/src/backend/storage/ipc",
        "//third_party/spanner_pg/src/backend/storage/large_object",
        "//third_party/spanner_pg/src/backend/storage/lmgr",
        "//third_party/spanner_pg/src/backend/storage/page",
        "//third_party/spanner_pg/src/backend/storage/smgr",
        "//third_party/spanner_pg/src/backend/tcop",
        "//third_party/spanner_pg/src/backend/tsearch",
        "//third_party/spanner_pg/src/backend/utils",
        "//third_party/spanner_pg/src/backend/utils/adt",
        "//third_party/spanner_pg/src/backend/utils/cache",
        "//third_party/spanner_pg/src/backend/utils/error",
        "//third_party/spanner_pg/src/backend/utils/fmgr",
        "//third_party/spanner_pg/src/backend/utils/hash",
        "//third_party/spanner_pg/src/backend/utils/init",
        "//third_party/spanner_pg/src/backend/utils/mb",
        "//third_party/spanner_pg/src/backend/utils/misc",
        "//third_party/spanner_pg/src/backend/utils/mmgr",
        "//third_party/spanner_pg/src/backend/utils/resowner",
        "//third_party/spanner_pg/src/backend/utils/sort",
        "//third_party/spanner_pg/src/backend/utils/time",
        "//third_party/spanner_pg/src/common:common_srv",
        "//third_party/spanner_pg/src/port:port_srv",
        "//third_party/spanner_pg/src/timezone",
    ],
    alwayslink = 1,
)

cc_library(
    name = "backend_include",
    copts = [
        "-D_GNU_SOURCE",
        "-fexceptions",
    ],
    includes = [
        ".",
    ],
    deps = [
        # Build generated headers
        "//third_party/spanner_pg/src/backend/storage/lmgr:lwlocknames",
    ],
)
