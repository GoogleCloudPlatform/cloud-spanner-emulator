#
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

load("@rules_cc//cc:cc_library.bzl", "cc_library")

package(
    default_visibility = ["//:__subpackages__"],
)

licenses(["notice"])

cc_library(
    name = "schema_validators",
    srcs = [
        "change_stream_validator.cc",
        "check_constraint_validator.cc",
        "column_validator.cc",
        "database_options_validator.cc",
        "foreign_key_validator.cc",
        "index_validator.cc",
        "locality_group_validator.cc",
        "model_validator.cc",
        "named_schema_validator.cc",
        "placement_validator.cc",
        "property_graph_validator.cc",
        "sequence_validator.cc",
        "table_validator.cc",
        "udf_validator.cc",
        "view_validator.cc",
    ],
    hdrs = [
        "change_stream_validator.h",
        "check_constraint_validator.h",
        "column_validator.h",
        "database_options_validator.h",
        "foreign_key_validator.h",
        "index_validator.h",
        "locality_group_validator.h",
        "model_validator.h",
        "named_schema_validator.h",
        "placement_validator.h",
        "property_graph_validator.h",
        "sequence_validator.h",
        "table_validator.h",
        "udf_validator.h",
        "view_validator.h",
    ],
    deps = [
        "//backend/common:case",
        "//backend/common:graph_dependency_helper",
        "//backend/common:ids",
        "//backend/datamodel:types",
        "//backend/schema/backfills:schema_backfillers",
        "//backend/schema/catalog:proto_bundle",
        "//backend/schema/catalog:schema",
        "//backend/schema/ddl:operations_cc_proto",
        "//backend/schema/graph:schema_node",
        "//backend/schema/updater:global_schema_names",
        "//backend/schema/updater:schema_validation_context",
        "//backend/schema/updater:sql_expression_validators",
        "//backend/schema/verifiers:column_value_verifiers",
        "//common:errors",
        "//common:feature_flags",
        "//common:limits",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/functional:function_ref",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:span",
        "@com_google_protobuf//:protobuf",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:options_cc_proto",
        "@com_google_zetasql//zetasql/public:simple_catalog",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:type_cc_proto",
    ],
)
