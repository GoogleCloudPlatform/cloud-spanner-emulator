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

cc_library(
    name = "map_files",
    copts = [
        "-D_GNU_SOURCE",
        "-fexceptions",
    ],
    textual_hdrs = [
        "big5_to_utf8.map",
        "euc_cn_to_utf8.map",
        "euc_jis_2004_to_utf8.map",
        "euc_jp_to_utf8.map",
        "euc_kr_to_utf8.map",
        "euc_tw_to_utf8.map",
        "gb18030_to_utf8.map",
        "gbk_to_utf8.map",
        "iso8859_10_to_utf8.map",
        "iso8859_13_to_utf8.map",
        "iso8859_14_to_utf8.map",
        "iso8859_15_to_utf8.map",
        "iso8859_16_to_utf8.map",
        "iso8859_2_to_utf8.map",
        "iso8859_3_to_utf8.map",
        "iso8859_4_to_utf8.map",
        "iso8859_5_to_utf8.map",
        "iso8859_6_to_utf8.map",
        "iso8859_7_to_utf8.map",
        "iso8859_8_to_utf8.map",
        "iso8859_9_to_utf8.map",
        "johab_to_utf8.map",
        "koi8r_to_utf8.map",
        "koi8u_to_utf8.map",
        "shift_jis_2004_to_utf8.map",
        "sjis_to_utf8.map",
        "uhc_to_utf8.map",
        "utf8_to_big5.map",
        "utf8_to_euc_cn.map",
        "utf8_to_euc_jis_2004.map",
        "utf8_to_euc_jp.map",
        "utf8_to_euc_kr.map",
        "utf8_to_euc_tw.map",
        "utf8_to_gb18030.map",
        "utf8_to_gbk.map",
        "utf8_to_iso8859_10.map",
        "utf8_to_iso8859_13.map",
        "utf8_to_iso8859_14.map",
        "utf8_to_iso8859_15.map",
        "utf8_to_iso8859_16.map",
        "utf8_to_iso8859_2.map",
        "utf8_to_iso8859_3.map",
        "utf8_to_iso8859_4.map",
        "utf8_to_iso8859_5.map",
        "utf8_to_iso8859_6.map",
        "utf8_to_iso8859_7.map",
        "utf8_to_iso8859_8.map",
        "utf8_to_iso8859_9.map",
        "utf8_to_johab.map",
        "utf8_to_koi8r.map",
        "utf8_to_koi8u.map",
        "utf8_to_shift_jis_2004.map",
        "utf8_to_sjis.map",
        "utf8_to_uhc.map",
        "utf8_to_win1250.map",
        "utf8_to_win1251.map",
        "utf8_to_win1252.map",
        "utf8_to_win1253.map",
        "utf8_to_win1254.map",
        "utf8_to_win1255.map",
        "utf8_to_win1256.map",
        "utf8_to_win1257.map",
        "utf8_to_win1258.map",
        "utf8_to_win866.map",
        "utf8_to_win874.map",
        "win1250_to_utf8.map",
        "win1251_to_utf8.map",
        "win1252_to_utf8.map",
        "win1253_to_utf8.map",
        "win1254_to_utf8.map",
        "win1255_to_utf8.map",
        "win1256_to_utf8.map",
        "win1257_to_utf8.map",
        "win1258_to_utf8.map",
        "win866_to_utf8.map",
        "win874_to_utf8.map",
    ],
)
