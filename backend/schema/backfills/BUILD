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
load("@rules_cc//cc:cc_test.bzl", "cc_test")

package(
    default_visibility = ["//:__subpackages__"],
)

licenses(["notice"])

cc_library(
    name = "schema_backfillers",
    srcs = [
        "change_stream_backfill.cc",
        "column_value_backfill.cc",
        "index_backfill.cc",
    ],
    hdrs = [
        "change_stream_backfill.h",
        "column_value_backfill.h",
        "index_backfill.h",
    ],
    deps = [
        "//backend/actions:generated_column",
        "//backend/common:ids",
        "//backend/common:indexing",
        "//backend/common:rows",
        "//backend/datamodel:key",
        "//backend/datamodel:key_range",
        "//backend/datamodel:types",
        "//backend/datamodel:value",
        "//backend/query:analyzer_options",
        "//backend/query:catalog",
        "//backend/query:function_catalog",
        "//backend/schema/catalog:schema",
        "//backend/schema/updater:schema_validation_context",
        "//backend/storage:in_memory_storage",
        "//backend/storage:iterator",
        "//common:errors",
        "//common:limits",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:optional",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:analyzer_options",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:type_cc_proto",
        "@com_google_zetasql//zetasql/public:value",
        "@com_google_zetasql//zetasql/public/functions:string",
    ],
)

cc_test(
    name = "change_stream_backfill_test",
    srcs = [
        "change_stream_backfill_test.cc",
    ],
    deps = [
        ":schema_backfillers",
        "//backend/database",
        "//backend/schema/catalog:schema",
        "//backend/schema/updater:schema_updater",
        "//backend/transaction:read_only_transaction",
        "//common:errors",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "index_backfill_test",
    srcs = [
        "index_backfill_test.cc",
    ],
    deps = [
        ":schema_backfillers",
        "//backend/access:read",
        "//backend/access:write",
        "//backend/database",
        "//backend/datamodel:value",
        "//backend/schema/catalog:schema",
        "//backend/schema/updater:schema_updater",
        "//backend/transaction:read_only_transaction",
        "//backend/transaction:read_write_transaction",
        "//common:clock",
        "//common:errors",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "column_value_backfill_test",
    srcs = [
        "column_value_backfill_test.cc",
    ],
    deps = [
        ":schema_backfillers",
        "//backend/database",
        "//backend/datamodel:key_set",
        "//backend/schema/catalog:schema",
        "//backend/schema/updater:schema_updater",
        "//backend/transaction:read_only_transaction",
        "//common:errors",
        "//tests/common:proto_matchers",
        "//tests/common:scoped_feature_flags_setter",
        "//tests/common:test_cc_proto",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)
