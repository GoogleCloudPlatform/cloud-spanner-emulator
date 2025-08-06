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

package(
    default_visibility = ["//:__subpackages__"],
)

licenses(["notice"])

cc_library(
    name = "column_value_verifiers",
    srcs = ["column_value_verifiers.cc"],
    hdrs = ["column_value_verifiers.h"],
    deps = [
        "//backend/common:ids",
        "//backend/common:indexing",
        "//backend/common:rows",
        "//backend/datamodel:key",
        "//backend/datamodel:types",
        "//backend/datamodel:value",
        "//backend/schema/catalog:schema",
        "//backend/schema/updater:schema_validation_context",
        "//backend/storage:in_memory_storage",
        "//backend/storage:iterator",
        "//common:errors",
        "//common:limits",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:optional",
        "@com_google_zetasql//zetasql/public:type_cc_proto",
        "@com_google_zetasql//zetasql/public:value",
        "@com_google_zetasql//zetasql/public/functions:string",
    ],
)

cc_test(
    name = "column_value_verifiers_test",
    srcs = ["column_value_verifiers_test.cc"],
    deps = [
        ":column_value_verifiers",
        "//backend/database",
        "//common:clock",
        "//common:errors",
        "//tests/common:proto_matchers",
        "//tests/common:test_cc_proto",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "check_constraint_verifiers",
    srcs = ["check_constraint_verifiers.cc"],
    hdrs = ["check_constraint_verifiers.h"],
    deps = [
        "//backend/actions:check_constraint",
        "//backend/datamodel:key_range",
        "//backend/query:analyzer_options",
        "//backend/query:catalog",
        "//backend/query:function_catalog",
        "//backend/schema/catalog:schema",
        "//backend/schema/updater:schema_validation_context",
        "//common:errors",
        "@com_google_absl//absl/status",
        "@com_google_zetasql//zetasql/public:analyzer_options",
    ],
)

cc_test(
    name = "check_constraint_verifiers_test",
    srcs = ["check_constraint_verifiers_test.cc"],
    deps = [
        ":check_constraint_verifiers",
        "//backend/database",
        "//backend/schema/updater:schema_updater",
        "//backend/transaction:read_write_transaction",
        "//common:clock",
        "//tests/common:proto_matchers",
        "//tests/common:scoped_feature_flags_setter",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "foreign_key_verifiers",
    srcs = ["foreign_key_verifiers.cc"],
    hdrs = ["foreign_key_verifiers.h"],
    deps = [
        "//backend/common:ids",
        "//backend/schema/catalog:schema",
        "//backend/schema/updater:schema_validation_context",
        "//common:errors",
        "@com_google_absl//absl/status",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "foreign_key_verifiers_test",
    srcs = ["foreign_key_verifiers_test.cc"],
    deps = [
        ":foreign_key_verifiers",
        "//backend/access:write",
        "//backend/database",
        "//backend/schema/updater:schema_updater",
        "//backend/transaction:read_write_transaction",
        "//common:clock",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "interleaving_verifiers",
    srcs = ["interleaving_verifiers.cc"],
    hdrs = ["interleaving_verifiers.h"],
    deps = [
        "//backend/common:ids",
        "//backend/common:rows",
        "//backend/datamodel:key",
        "//backend/datamodel:key_range",
        "//backend/query:analyzer_options",
        "//backend/query:catalog",
        "//backend/query:function_catalog",
        "//backend/schema/catalog:schema",
        "//backend/schema/updater:schema_validation_context",
        "//backend/storage",
        "//backend/storage:iterator",
        "//common:errors",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/time",
        "@com_google_zetasql//zetasql/public:analyzer_options",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "interleaving_verifiers_test",
    srcs = ["interleaving_verifiers_test.cc"],
    deps = [
        "//backend/access:write",
        "//backend/database",
        "//backend/schema/updater:schema_updater",
        "//backend/transaction:read_write_transaction",
        "//common:clock",
        "//tests/common:proto_matchers",
        "//tests/common:scoped_feature_flags_setter",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:value",
    ],
)
