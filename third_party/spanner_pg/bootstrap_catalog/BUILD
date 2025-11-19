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

# Bootstrap Catalog
# Makes PostgreSQL system table data (pg_type, pg_proc, etc.) available to
# Spanner PG shims and transformer code. This package is separate from other
# shim packages because it needs to directly include PostgreSQL src files
# instead of the standard postgres_includes package used elsewhere.

load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library", "cc_test")

package(
    default_visibility = ["//visibility:private"],
)

licenses(["notice"])

cc_library(
    name = "bootstrap_catalog",
    srcs = ["bootstrap_catalog.cc"],
    hdrs = ["bootstrap_catalog.h"],
    visibility = [
        "//:__subpackages__",
        "//backend/query:__subpackages__",
    ],
    deps = [
        ":bootstrap_catalog_info",
        ":proc_changelist",
        "//third_party/spanner_pg/interface:bootstrap_catalog_data_cc_proto",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/shims:error_shim",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_protobuf//:protobuf",
        "@com_google_zetasql//zetasql/base:logging",
    ],
)

cc_library(
    name = "bootstrap_catalog_info",
    srcs = ["bootstrap_catalog_info.c"],
    hdrs = [
        "bootstrap_catalog_info.h",
        "bootstrap_catalog_textproto.h",
    ],
    copts = ["-Ithird_party/spanner_pg/shims"],
    deps = [
        ":bootstrap_catalog_srcs",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/src/include",
    ],
)

cc_library(
    name = "proc_changelist",
    srcs = ["proc_changelist.cc"],
    hdrs = ["proc_changelist.h"],
    deps = [
        "//third_party/spanner_pg/codegen:default_values_reader",
        "//third_party/spanner_pg/interface:bootstrap_catalog_data_cc_proto",
        "//third_party/spanner_pg/postgres_includes",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_zetasql//zetasql/base:no_destructor",
    ],
)

cc_test(
    name = "bootstrap_catalog_test",
    srcs = ["bootstrap_catalog_test.cc"],
    tags = ["spanner.spanner_pg"],
    deps = [
        ":bootstrap_catalog",
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "//third_party/spanner_pg/util:valid_memory_context_fixture",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

filegroup(
    name = "spanner_pg_datfiles",
    srcs = glob([
        "spanner_pg_data/pg_*.dat",
    ]),
)

cc_library(
    name = "bootstrap_catalog_srcs",
    hdrs = [":generate_bootstrap_catalog_srcs"],
    deps = [
        "//third_party/spanner_pg/postgres_includes",
        "//third_party/spanner_pg/src/include",
    ],
)

filegroup(
    name = "spanner_pg_fmgr_datfiles",
    srcs = [
        "//third_party/spanner_pg/codegen:pg_proc.dat",
    ],
    visibility = [
        "//:__subpackages__",
    ],
)

genrule(
    name = "generate_bootstrap_catalog_srcs",
    srcs = [
        ":spanner_pg_datfiles",
        "//third_party/spanner_pg/codegen:pg_proc.dat",
        "//third_party/spanner_pg/src/include:all_headers",
        "//third_party/spanner_pg/src/include:catalog_headers",
        "//third_party/spanner_pg/src/include:catalog_datfiles",
        "//third_party/spanner_pg/src/backend/catalog:perl_utils",
    ],
    outs = [
        "bootstrap_catalog_info.h",
        "bootstrap_catalog_info.c",
        "bootstrap_catalog_textproto.h",
    ],
    # Includes Spanner-specific bootstrap catalog data.
    # The order of the files in the following list is important.
    # The order comes from src/backend/catalog/Makefile:POSTGRES_BKI_SRCS
    # Specifically, it must be a valid partial order of the dependency
    # structure of the catalog.
    # If catalog object B refers to catalog object A, then A must be
    # included first.  Otherwise gen_catalog_info.pl will not link
    # B back to A.
    cmd = """\
        set -e # Exit on error

        INCLUDE_PATH=third_party/spanner_pg/src/include
        ENGINE_CATALOG_PATH=third_party/spanner_pg/bootstrap_catalog/spanner_pg_data

        # Copies generated files into the engine catalog path
        cp $(location //third_party/spanner_pg/codegen:pg_proc.dat) $$ENGINE_CATALOG_PATH

        $(location gen_catalog_info.pl) \
            -output /tmp \
            --include-path=$$INCLUDE_PATH \
            --engine-catalog-path=$$ENGINE_CATALOG_PATH \
            third_party/spanner_pg/src/include/catalog/pg_proc.h \
            third_party/spanner_pg/src/include/catalog/pg_type.h \
            third_party/spanner_pg/src/include/catalog/pg_attribute.h \
            third_party/spanner_pg/src/include/catalog/pg_class.h \
            third_party/spanner_pg/src/include/catalog/pg_attrdef.h \
            third_party/spanner_pg/src/include/catalog/pg_constraint.h \
            third_party/spanner_pg/src/include/catalog/pg_inherits.h \
            third_party/spanner_pg/src/include/catalog/pg_index.h \
            third_party/spanner_pg/src/include/catalog/pg_operator.h \
            third_party/spanner_pg/src/include/catalog/pg_opfamily.h \
            third_party/spanner_pg/src/include/catalog/pg_opclass.h \
            third_party/spanner_pg/src/include/catalog/pg_am.h \
            third_party/spanner_pg/src/include/catalog/pg_amop.h \
            third_party/spanner_pg/src/include/catalog/pg_amproc.h \
            third_party/spanner_pg/src/include/catalog/pg_language.h \
            third_party/spanner_pg/src/include/catalog/pg_largeobject_metadata.h \
            third_party/spanner_pg/src/include/catalog/pg_largeobject.h \
            third_party/spanner_pg/src/include/catalog/pg_aggregate.h \
            third_party/spanner_pg/src/include/catalog/pg_statistic_ext.h \
            third_party/spanner_pg/src/include/catalog/pg_statistic.h \
            third_party/spanner_pg/src/include/catalog/pg_rewrite.h \
            third_party/spanner_pg/src/include/catalog/pg_trigger.h \
            third_party/spanner_pg/src/include/catalog/pg_event_trigger.h \
            third_party/spanner_pg/src/include/catalog/pg_description.h \
            third_party/spanner_pg/src/include/catalog/pg_cast.h \
            third_party/spanner_pg/src/include/catalog/pg_enum.h \
            third_party/spanner_pg/src/include/catalog/pg_namespace.h \
            third_party/spanner_pg/src/include/catalog/pg_conversion.h \
            third_party/spanner_pg/src/include/catalog/pg_depend.h \
            third_party/spanner_pg/src/include/catalog/pg_database.h \
            third_party/spanner_pg/src/include/catalog/pg_db_role_setting.h \
            third_party/spanner_pg/src/include/catalog/pg_tablespace.h \
            third_party/spanner_pg/src/include/catalog/pg_authid.h \
            third_party/spanner_pg/src/include/catalog/pg_auth_members.h \
            third_party/spanner_pg/src/include/catalog/pg_shdepend.h \
            third_party/spanner_pg/src/include/catalog/pg_shdescription.h \
            third_party/spanner_pg/src/include/catalog/pg_ts_config.h \
            third_party/spanner_pg/src/include/catalog/pg_ts_config_map.h \
            third_party/spanner_pg/src/include/catalog/pg_ts_dict.h \
            third_party/spanner_pg/src/include/catalog/pg_ts_parser.h \
            third_party/spanner_pg/src/include/catalog/pg_ts_template.h \
            third_party/spanner_pg/src/include/catalog/pg_extension.h \
            third_party/spanner_pg/src/include/catalog/pg_foreign_data_wrapper.h \
            third_party/spanner_pg/src/include/catalog/pg_foreign_server.h \
            third_party/spanner_pg/src/include/catalog/pg_user_mapping.h \
            third_party/spanner_pg/src/include/catalog/pg_foreign_table.h \
            third_party/spanner_pg/src/include/catalog/pg_policy.h \
            third_party/spanner_pg/src/include/catalog/pg_replication_origin.h \
            third_party/spanner_pg/src/include/catalog/pg_default_acl.h \
            third_party/spanner_pg/src/include/catalog/pg_parameter_acl.h \
            third_party/spanner_pg/src/include/catalog/pg_publication_namespace.h \
            third_party/spanner_pg/src/include/catalog/pg_init_privs.h \
            third_party/spanner_pg/src/include/catalog/pg_seclabel.h \
            third_party/spanner_pg/src/include/catalog/pg_shseclabel.h \
            third_party/spanner_pg/src/include/catalog/pg_collation.h \
            third_party/spanner_pg/src/include/catalog/pg_partitioned_table.h \
            third_party/spanner_pg/src/include/catalog/pg_range.h \
            third_party/spanner_pg/src/include/catalog/pg_transform.h \
            third_party/spanner_pg/src/include/catalog/pg_sequence.h \
            third_party/spanner_pg/src/include/catalog/pg_publication.h \
            third_party/spanner_pg/src/include/catalog/pg_publication_rel.h \
            third_party/spanner_pg/src/include/catalog/pg_subscription.h \
            third_party/spanner_pg/src/include/catalog/pg_subscription_rel.h

        mv /tmp/bootstrap_catalog_info.h $(location bootstrap_catalog_info.h)
        mv /tmp/bootstrap_catalog_info.c $(location bootstrap_catalog_info.c)
        mv /tmp/bootstrap_catalog_textproto.h $(location bootstrap_catalog_textproto.h)
    """,
    tools = ["gen_catalog_info.pl"],
)

cc_library(
    name = "bootstrap_catalog_accessor_impl",
    srcs = ["bootstrap_catalog_accessor.cc"],
    visibility = [
        "//backend/query:__subpackages__",
        "//tests/conformance/common:__subpackages__",
        "//third_party/spanner_pg/catalog:__subpackages__",
        "//third_party/spanner_pg/interface:__subpackages__",
    ],
    deps = [
        ":bootstrap_catalog",
        "//third_party/spanner_pg/interface:bootstrap_catalog_accessor",
        "//third_party/spanner_pg/interface:bootstrap_catalog_data_cc_proto",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
    ],
)
