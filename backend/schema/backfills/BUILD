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

package(default_visibility = [
    "//:__subpackages__",
])

licenses(["unencumbered"])

cc_library(
    name = "schema_backfillers",
    srcs = [
        "column_value_backfill.cc",
        "index_backfill.cc",
    ],
    hdrs = [
        "column_value_backfill.h",
        "index_backfill.h",
    ],
    deps = [
        "//backend/actions:generated_column",
        "//backend/common:ids",
        "//backend/common:indexing",
        "//backend/common:rows",
        "//backend/datamodel:types",
        "//backend/datamodel:value",
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
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:optional",
        "@com_google_zetasql//zetasql/base:statusor",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:type_cc_proto",
        "@com_google_zetasql//zetasql/public:value",
        "@com_google_zetasql//zetasql/public/functions:string",
    ],
)

cc_test(
    name = "index_backfill_test",
    srcs = [
        "index_backfill_test.cc",
    ],
    deps = [
        ":schema_backfillers",
        "//backend/database",
        "//backend/schema/catalog:schema",
        "//backend/transaction:read_only_transaction",
        "//backend/transaction:read_write_transaction",
        "//common:errors",
        "//tests/common:actions",
        "//tests/common:proto_matchers",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
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
        "//backend/transaction:read_only_transaction",
        "//backend/transaction:read_write_transaction",
        "//common:errors",
        "//tests/common:actions",
        "//tests/common:proto_matchers",
        "//tests/common:scoped_feature_flags_setter",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base:statusor",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)
