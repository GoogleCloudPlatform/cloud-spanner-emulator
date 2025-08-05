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

load("@rules_cc//cc:defs.bzl", "cc_binary")

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

cc_binary(
    name = "dict_snowball.so",
    srcs = [
        "dict_snowball.c",
        "libstemmer/api.c",
        "libstemmer/stem_ISO_8859_1_danish.c",
        "libstemmer/stem_ISO_8859_1_dutch.c",
        "libstemmer/stem_ISO_8859_1_english.c",
        "libstemmer/stem_ISO_8859_1_finnish.c",
        "libstemmer/stem_ISO_8859_1_french.c",
        "libstemmer/stem_ISO_8859_1_german.c",
        "libstemmer/stem_ISO_8859_1_italian.c",
        "libstemmer/stem_ISO_8859_1_norwegian.c",
        "libstemmer/stem_ISO_8859_1_porter.c",
        "libstemmer/stem_ISO_8859_1_portuguese.c",
        "libstemmer/stem_ISO_8859_1_spanish.c",
        "libstemmer/stem_ISO_8859_1_swedish.c",
        "libstemmer/stem_ISO_8859_2_romanian.c",
        "libstemmer/stem_KOI8_R_russian.c",
        "libstemmer/stem_UTF_8_danish.c",
        "libstemmer/stem_UTF_8_dutch.c",
        "libstemmer/stem_UTF_8_english.c",
        "libstemmer/stem_UTF_8_finnish.c",
        "libstemmer/stem_UTF_8_french.c",
        "libstemmer/stem_UTF_8_german.c",
        "libstemmer/stem_UTF_8_hungarian.c",
        "libstemmer/stem_UTF_8_italian.c",
        "libstemmer/stem_UTF_8_norwegian.c",
        "libstemmer/stem_UTF_8_porter.c",
        "libstemmer/stem_UTF_8_portuguese.c",
        "libstemmer/stem_UTF_8_romanian.c",
        "libstemmer/stem_UTF_8_russian.c",
        "libstemmer/stem_UTF_8_spanish.c",
        "libstemmer/stem_UTF_8_swedish.c",
        "libstemmer/stem_UTF_8_turkish.c",
        "libstemmer/utilities.c",
    ],
    copts = [
        "-D_GNU_SOURCE",
        "-fexceptions",
    ],
    linkshared = 1,
    linkstatic = 1,
    deps = [
        "//third_party/spanner_pg/src/include",
        "//third_party/spanner_pg/src/include:snowball_include",
    ],
)

exports_files(
    [
        "snowball_create.sql",
    ],
)

filegroup(
    name = "tsearch_data",
    srcs = glob(["stopwords/*.stop"]),
)
