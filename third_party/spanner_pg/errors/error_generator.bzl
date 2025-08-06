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

"""
This module contains build rules for generating a cc_library of absl::Status and exception-throwing
factory functions from a data file of error messages.
"""

load("@rules_cc//cc:defs.bzl", "cc_library")

def spangres_error_catalog(
        name,
        message_file = "//third_party/spanner_pg/errors:error_catalog.textproto",
        # Errors are surrounded by a namespace that corresponds to the error code,
        # e.g., "BadUsage" => "bad_usage", so the error:: namespace is redundant
        # and is left out.
        namespace = "spangres",
        deps = [],
        visibility = None):
    """
    Generates a cc_library of factory functions from a file of error messages.


    Args:
      name: the name of the cc_library to generate. The header file will
            be named <name>.h.
      message_file: the data file, which contains a text-format
                    spangres.error.FileProto. A typical value is
                    //third_party/spanner_pg/errors:error_catalog.textproto.
      namespace: the c++ namespace in which to generate the error
                 factory functions.
      deps: the c++/proto libraries needed for any parameter types
            used by the errors that will appear in this catalog.
      visibility: visibility of generated library.
    """
    package = "third_party/spanner_pg/errors"
    h_file = name + ".h"
    cc_file = name + ".cc"

    # Run the error generator, then post-process its output with
    # clang-format so it is readable for debugging.
    native.genrule(
        name = "gen_" + name,
        srcs = [message_file],
        outs = [h_file, cc_file],
        cmd = (
            "$(location //third_party/spanner_pg/errors:error_generator)" +
            " --google3_root=$(GENDIR)" +
            " --package=" + package +
            " --namespace=" + namespace +
            " --file_basename=" + name +
            " $(location " + message_file +
            ")"
        ),
        visibility = visibility,
        tools = [
            "//third_party/spanner_pg/errors:error_generator",
        ],
    )

    # The actual library users of the catalog depend on.
    cc_library(
        name = name,
        srcs = [cc_file],
        hdrs = [h_file],
        visibility = visibility,
        copts = ["-fexceptions"],
        features = ["-use_header_modules"],  # Incompatible with -fexceptions.
        deps = deps + [
            # This list should only include libraries required by the boilerplate
            # the error generator emits. It should not include libraries required
            # for the parameter types, which should come in through "deps".
            "@com_google_zetasql//zetasql/base",
            "@com_google_googleapis//google/rpc:code_cc_proto",
            "@com_google_googleapis//google/rpc:error_details_cc_proto",
            "@com_google_googleapis//google/rpc:status_cc_proto",
            "@com_google_absl//absl/container:flat_hash_map",
            "@com_google_absl//absl/status",
            "@com_google_absl//absl/strings",
            "@com_google_absl//absl/strings:str_format",
            "@com_google_absl//absl/time",
            "//third_party/spanner_pg/interface:ereport",
            "//third_party/spanner_pg/postgres_includes",
            "//third_party/spanner_pg/errors:errors",
            "//third_party/spanner_pg/errors:errors_cc_proto",
            "@com_google_zetasql//zetasql/base:no_destructor",
        ],
    )
