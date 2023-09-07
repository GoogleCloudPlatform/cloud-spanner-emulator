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
    name = "catalog",
    srcs = [
        "aclchk.c",
        "catalog.c",
        "dependency.c",
        "heap.c",
        "index.c",
        "indexing.c",
        "namespace.c",
        "objectaccess.c",
        "objectaddress.c",
        "partition.c",
        "pg_aggregate.c",
        "pg_cast.c",
        "pg_collation.c",
        "pg_constraint.c",
        "pg_conversion.c",
        "pg_db_role_setting.c",
        "pg_depend.c",
        "pg_enum.c",
        "pg_inherits.c",
        "pg_largeobject.c",
        "pg_namespace.c",
        "pg_operator.c",
        "pg_proc.c",
        "pg_publication.c",
        "pg_range.c",
        "pg_shdepend.c",
        "pg_subscription.c",
        "pg_type.c",
        "storage.c",
        "toasting.c",
    ],
    copts = [
        "-D_GNU_SOURCE",
        "-fexceptions",
    ],
    deps = [
        "//third_party/spanner_pg/shims:catalog_shim_header",
        "//third_party/spanner_pg/shims:parser_shim",
        "//third_party/spanner_pg/src/include",
    ],
    alwayslink = 1,
)

filegroup(
    name = "share",
    srcs = [
        "information_schema.sql",
        "postgres.bki",
        "postgres.description",
        "postgres.shdescription",
        "sql_features.txt",
        "system_views.sql",
    ],
)

filegroup(
    name = "perl_utils",
    srcs = [
        "Catalog.pm",
    ],
)

GENERATED_CATALOG_HEADER_FILES = [
    "pg_aggregate_d.h",
    "pg_am_d.h",
    "pg_amop_d.h",
    "pg_amproc_d.h",
    "pg_attrdef_d.h",
    "pg_attribute_d.h",
    "pg_auth_members_d.h",
    "pg_authid_d.h",
    "pg_cast_d.h",
    "pg_class_d.h",
    "pg_collation_d.h",
    "pg_constraint_d.h",
    "pg_conversion_d.h",
    "pg_database_d.h",
    "pg_db_role_setting_d.h",
    "pg_default_acl_d.h",
    "pg_depend_d.h",
    "pg_description_d.h",
    "pg_enum_d.h",
    "pg_event_trigger_d.h",
    "pg_extension_d.h",
    "pg_foreign_data_wrapper_d.h",
    "pg_foreign_server_d.h",
    "pg_foreign_table_d.h",
    "pg_index_d.h",
    "pg_inherits_d.h",
    "pg_init_privs_d.h",
    "pg_language_d.h",
    "pg_largeobject_d.h",
    "pg_largeobject_metadata_d.h",
    "pg_namespace_d.h",
    "pg_opclass_d.h",
    "pg_operator_d.h",
    "pg_opfamily_d.h",
    "pg_partitioned_table_d.h",
    "pg_policy_d.h",
    "pg_proc_d.h",
    "pg_publication_d.h",
    "pg_publication_rel_d.h",
    "pg_range_d.h",
    "pg_replication_origin_d.h",
    "pg_rewrite_d.h",
    "pg_seclabel_d.h",
    "pg_sequence_d.h",
    "pg_shdepend_d.h",
    "pg_shdescription_d.h",
    "pg_shseclabel_d.h",
    "pg_statistic_d.h",
    "pg_statistic_ext_d.h",
    "pg_statistic_ext_data_d.h",
    "pg_subscription_d.h",
    "pg_subscription_rel_d.h",
    "pg_tablespace_d.h",
    "pg_transform_d.h",
    "pg_trigger_d.h",
    "pg_ts_config_d.h",
    "pg_ts_config_map_d.h",
    "pg_ts_dict_d.h",
    "pg_ts_parser_d.h",
    "pg_ts_template_d.h",
    "pg_type_d.h",
    "pg_user_mapping_d.h",
    "schemapg.h",
]

genrule(
    name = "generate_catalog_headers",
    srcs = [
        ":perl_utils",
        "//third_party/spanner_pg/src/include:all_headers",
        "//third_party/spanner_pg/src/include:catalog_headers",
        "//third_party/spanner_pg/src/include:catalog_datfiles",
    ],
    outs = GENERATED_CATALOG_HEADER_FILES,
    # The order of the files in the following list is important.
    # The order comes from Makefile:POSTGRES_BKI_SRCS
    # Specifically, it must be a valid partial order of the dependency
    # structure of the catalog.
    # If catalog object B refers to catalog object A, then A must be
    # included first.  Otherwise genbki.pl will not link
    # B back to A.
    cmd = """\
        $(location genbki.pl) --output=/tmp --set-version=13 --include-path=third_party/spanner_pg/src/include \
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
            third_party/spanner_pg/src/include/catalog/pg_statistic_ext_data.h \
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
            third_party/spanner_pg/src/include/catalog/pg_subscription_rel.h \
            third_party/spanner_pg/src/include/catalog/toasting.h \
            third_party/spanner_pg/src/include/catalog/indexing.h

        OUTPUTDIR=$$(dirname $(location pg_enum_d.h))
        for HEADERFILE in $(locations //third_party/spanner_pg/src/include:catalog_headers); do
            OUTFILENAME="$$(basename $$HEADERFILE)"
            OUTFILENAME="$${OUTFILENAME/.h/_d.h}"
            if [[ -f "/tmp/$$OUTFILENAME" ]]; then
                 mv -v "/tmp/$$OUTFILENAME" "$$OUTPUTDIR/$$OUTFILENAME"
            else
                echo "No output file for $$HEADERFILE"
            fi
        done
        mv -v "/tmp/schemapg.h" "$(location schemapg.h)"
    """,
    tools = [
        "genbki.pl",
        ":perl_utils",
    ],
)

cc_library(
    name = "generated_catalog_headers",
    srcs = ["placeholder.cc"],
    hdrs = [":generate_catalog_headers"],
    includes = ["."],
)
