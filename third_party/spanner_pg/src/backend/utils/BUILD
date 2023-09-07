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
    name = "utils",
    srcs = [
        "fmgrtab.c",
    ],
    copts = [
        "-D_GNU_SOURCE",
        "-fexceptions",
    ],
    includes = [
        ".",
    ],
    deps = [
        ":generated_headers",
        "//third_party/spanner_pg/src/include",
    ],
    alwayslink = 1,
)

cc_library(
    name = "errcodes_header",
    hdrs = [":generate_errcodes"],
)

genrule(
    name = "generate_errcodes",
    srcs = [
        "errcodes.txt",
    ],
    outs = [
        "errcodes.h",
    ],
    cmd = """\
        $(location generate-errcodes.pl) \
            $(location errcodes.txt) > $(location errcodes.h)
    """,
    tools = [
        "generate-errcodes.pl",
    ],
)

cc_library(
    name = "fmgr_sources_header",
    hdrs = [":generate_fmgr_sources"],
)

genrule(
    name = "generate_fmgr_sources",
    srcs = [
        "//third_party/spanner_pg/src/backend/catalog:perl_utils",
        "//third_party/spanner_pg/src/include:fmgr_datfiles",
        "//third_party/spanner_pg/src/include:fmgr_dependencies",
    ],
    outs = [
        "fmgroids.h",
        "fmgrprotos.h",
        "fmgrtab.c",
    ],
    cmd = """
        PERL_LIBS=( $(locations //third_party/spanner_pg/src/backend/catalog:perl_utils) )
        FMGR_DATFILES=( $(locations //third_party/spanner_pg/src/include:fmgr_datfiles) )
        FMGR_DEPS=( $(locations //third_party/spanner_pg/src/include:fmgr_dependencies) )
        perl \
            -I $$(dirname $${PERL_LIBS[0]}) \
            $(location Gen_fmgrtab.pl) \
            -I $$(dirname $$(dirname $${FMGR_DATFILES[0]})) \
            -o $$(dirname $(location fmgroids.h)) \
            $${FMGR_DATFILES[@]}
    """,
    tools = [
        "Gen_fmgrtab.pl",
    ],
)

cc_library(
    name = "probes_header",
    hdrs = [":generate_probes"],
)

genrule(
    name = "generate_probes",
    srcs = [
        "probes.d",
        "Gen_dummy_probes.sed",
    ],
    outs = [
        "probes.h",
    ],
    # Commands taken from the 'probes.h' target in ./Makefile
    cmd = """\
        sed -f $(location Gen_dummy_probes.sed) \
            < $(location probes.d) \
            > $(location probes.h)
    """,
)

cc_library(
    name = "generated_headers",
    srcs = [
        "placeholder.cc",
    ],
    hdrs = [
        "errcodes.h",
        "fmgroids.h",
        "fmgrprotos.h",
        "probes.h",
    ],
    deps = [
        ":errcodes_header",
        ":fmgr_sources_header",
        ":probes_header",
    ],
)
