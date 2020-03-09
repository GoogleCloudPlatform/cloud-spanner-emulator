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

package(default_visibility = ["//:__subpackages__"])

licenses(["unencumbered"])

cc_library(
    name = "read_write_transaction",
    srcs = [
        "read_write_transaction.cc",
    ],
    hdrs = [
        "options.h",
        "read_write_transaction.h",
    ],
    deps = [
        ":actions",
        ":read_util",
        ":row_cursor",
        ":transaction_store",
        ":write_util",
        "//backend/access:read",
        "//backend/access:write",
        "//backend/actions:context",
        "//backend/actions:manager",
        "//backend/common:ids",
        "//backend/common:rows",
        "//backend/datamodel:key",
        "//backend/datamodel:key_range",
        "//backend/datamodel:value",
        "//backend/locking:manager",
        "//backend/schema/catalog:schema",
        "//backend/schema/catalog:versioned_catalog",
        "//backend/storage",
        "//backend/storage:in_memory_iterator",
        "//backend/storage:iterator",
        "//common:clock",
        "//common:errors",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/base:status",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "read_write_transaction_test",
    srcs = [
        "read_write_transaction_test.cc",
    ],
    deps = [
        ":actions",
        ":read_write_transaction",
        ":write_util",
        "//backend/access:write",
        "//backend/actions:manager",
        "//backend/actions:ops",
        "//backend/common:ids",
        "//backend/datamodel:key",
        "//backend/datamodel:value",
        "//backend/schema/catalog:versioned_catalog",
        "//backend/storage:in_memory_storage",
        "//common:clock",
        "//tests/common:proto_matchers",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base:status",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "row_cursor",
    srcs = ["row_cursor.cc"],
    hdrs = ["row_cursor.h"],
    deps = [
        "//backend/access:read",
        "//backend/schema/catalog:schema",
        "//backend/storage:in_memory_iterator",
        "//backend/storage:iterator",
    ],
)

cc_test(
    name = "row_cursor_test",
    srcs = ["row_cursor_test.cc"],
    deps = [
        ":row_cursor",
        "//backend/storage:in_memory_iterator",
        "//tests/common:proto_matchers",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "read_only_transaction",
    srcs = [
        "read_only_transaction.cc",
    ],
    hdrs = [
        "options.h",
        "read_only_transaction.h",
    ],
    deps = [
        ":read_util",
        ":row_cursor",
        ":transaction_store",
        "//backend/access:read",
        "//backend/access:write",
        "//backend/common:ids",
        "//backend/datamodel:key_set",
        "//backend/locking:manager",
        "//backend/schema/catalog:versioned_catalog",
        "//backend/storage",
        "//backend/storage:in_memory_iterator",
        "//common:clock",
        "//common:errors",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_zetasql//zetasql/base:status",
    ],
)

cc_test(
    name = "read_only_transaction_test",
    srcs = [
        "read_only_transaction_test.cc",
    ],
    deps = [
        ":read_only_transaction",
        "//backend/common:ids",
        "//backend/schema/catalog:schema",
        "//backend/schema/catalog:versioned_catalog",
        "//backend/storage:in_memory_storage",
        "//common:clock",
        "//tests/common:proto_matchers",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base:status",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_library(
    name = "read_util",
    srcs = ["read_util.cc"],
    hdrs = ["read_util.h"],
    deps = [
        ":transaction_store",
        "//backend/access:read",
        "//backend/common:ids",
        "//backend/datamodel:key_set",
        "//backend/schema/catalog:schema",
        "//backend/storage",
        "//common:errors",
        "@com_google_absl//absl/time",
        "@com_google_zetasql//zetasql/base:status",
    ],
)

cc_test(
    name = "read_util_test",
    srcs = ["read_util_test.cc"],
    deps = [
        ":read_util",
        "//backend/datamodel:key_range",
        "//backend/datamodel:key_set",
        "//backend/schema/catalog:schema",
        "//backend/storage:in_memory_storage",
        "//tests/common:proto_matchers",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base:status",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "write_util",
    srcs = ["write_util.cc"],
    hdrs = ["write_util.h"],
    deps = [
        ":read_util",
        ":transaction_store",
        "//backend/access:write",
        "//backend/actions:ops",
        "//backend/datamodel:key_range",
        "//backend/datamodel:key_set",
        "//backend/datamodel:value",
        "//backend/schema/catalog:schema",
        "//backend/storage:iterator",
        "//common:constants",
        "//common:errors",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/base:status",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "write_util_test",
    srcs = ["write_util_test.cc"],
    deps = [
        ":write_util",
        "//backend/actions:ops",
        "//backend/datamodel:key_range",
        "//backend/datamodel:key_set",
        "//backend/locking:manager",
        "//backend/schema/catalog:schema",
        "//backend/storage:in_memory_storage",
        "//common:clock",
        "//common:errors",
        "//tests/common:proto_matchers",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base:status",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "transaction_store",
    srcs = [
        "transaction_store.cc",
    ],
    hdrs = [
        "transaction_store.h",
    ],
    deps = [
        "//backend/actions:ops",
        "//backend/common:ids",
        "//backend/common:rows",
        "//backend/datamodel:key",
        "//backend/datamodel:key_range",
        "//backend/datamodel:value",
        "//backend/locking:manager",
        "//backend/schema/catalog:schema",
        "//backend/storage",
        "//backend/storage:in_memory_iterator",
        "//backend/storage:iterator",
        "//common:constants",
        "//common:errors",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/time",
        "@com_google_zetasql//zetasql/base:status",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "transaction_store_test",
    srcs = [
        "transaction_store_test.cc",
    ],
    deps = [
        ":transaction_store",
        "//backend/datamodel:key_range",
        "//backend/datamodel:value",
        "//backend/locking:manager",
        "//backend/storage:in_memory_storage",
        "//common:clock",
        "//tests/common:proto_matchers",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "actions",
    srcs = ["actions.cc"],
    hdrs = ["actions.h"],
    deps = [
        ":transaction_store",
        "//backend/actions:context",
        "//backend/actions:existence",
        "//backend/actions:interleave",
        "//backend/datamodel:key_range",
        "//backend/schema/catalog:schema",
        "//backend/storage:iterator",
        "@com_google_absl//absl/memory",
        "@com_google_zetasql//zetasql/base:status",
        "@com_google_zetasql//zetasql/base:statusor",
        "@com_google_zetasql//zetasql/public:value",
    ],
)
