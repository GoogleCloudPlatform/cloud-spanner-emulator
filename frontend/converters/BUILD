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
    name = "keys",
    srcs = ["keys.cc"],
    hdrs = ["keys.h"],
    deps = [
        ":values",
        "//backend/datamodel:key",
        "//backend/datamodel:key_range",
        "//backend/datamodel:key_set",
        "//backend/schema/catalog:schema",
        "//common:errors",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_grpc",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_zetasql//zetasql/base:statusor",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "keys_test",
    srcs = ["keys_test.cc"],
    deps = [
        ":keys",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "chunking",
    srcs = ["chunking.cc"],
    hdrs = ["chunking.h"],
    deps = [
        "//common:errors",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_grpc",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_zetasql//zetasql/base:statusor",
    ],
)

cc_test(
    name = "chunking_test",
    srcs = [
        "chunking_test.cc",
    ],
    deps = [
        ":chunking",
        ":reads",
        "//common:limits",
        "//tests/common:chunking",
        "//tests/common:proto_matchers",
        "//tests/common:test_row_cursor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/time",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_grpc",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "reads",
    srcs = [
        "reads.cc",
    ],
    hdrs = [
        "reads.h",
    ],
    deps = [
        ":chunking",
        ":keys",
        ":partition",
        ":time",
        ":types",
        ":values",
        "//backend/access:read",
        "//backend/access:write",
        "//backend/common:ids",
        "//backend/datamodel:key",
        "//backend/datamodel:key_range",
        "//backend/datamodel:key_set",
        "//backend/datamodel:value",
        "//backend/schema/catalog:schema",
        "//backend/transaction:read_only_transaction",
        "//common:errors",
        "//common:limits",
        "//frontend/proto:partition_token_cc_proto",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_grpc",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_zetasql//zetasql/base:statusor",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "reads_test",
    srcs = ["reads_test.cc"],
    deps = [
        ":reads",
        "//backend/access:write",
        "//backend/datamodel:key_range",
        "//backend/datamodel:key_set",
        "//backend/schema/catalog:schema",
        "//tests/common:proto_matchers",
        "//tests/common:test_row_cursor",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_grpc",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "time",
    srcs = [
        "time.cc",
    ],
    hdrs = [
        "time.h",
    ],
    deps = [
        "//common:errors",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_zetasql//zetasql/base:statusor",
    ],
)

cc_test(
    name = "time_test",
    srcs = ["time_test.cc"],
    deps = [
        ":time",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "mutations",
    srcs = ["mutations.cc"],
    hdrs = ["mutations.h"],
    deps = [
        ":keys",
        ":types",
        ":values",
        "//backend/access:read",
        "//backend/access:write",
        "//backend/common:ids",
        "//backend/datamodel:key",
        "//backend/datamodel:key_range",
        "//backend/datamodel:key_set",
        "//backend/datamodel:value",
        "//backend/schema/catalog:schema",
        "//common:errors",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_grpc",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "mutations_test",
    srcs = ["mutations_test.cc"],
    deps = [
        ":mutations",
        "//backend/access:write",
        "//backend/datamodel:key_range",
        "//backend/datamodel:key_set",
        "//backend/schema/catalog:schema",
        "//tests/common:proto_matchers",
        "//tests/common:test_row_cursor",
        "//tests/common:test_schema_constructor",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_grpc",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "types",
    srcs = ["types.cc"],
    hdrs = ["types.h"],
    deps = [
        "//common:errors",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_grpc",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_test(
    name = "types_test",
    srcs = ["types_test.cc"],
    deps = [
        ":types",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_library(
    name = "values",
    srcs = ["values.cc"],
    hdrs = ["values.h"],
    deps = [
        "//common:constants",
        "//common:errors",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_zetasql//zetasql/base:statusor",
        "@com_google_zetasql//zetasql/public:options_cc_proto",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
        "@com_google_zetasql//zetasql/public/functions:date_time_util",
    ],
)

cc_test(
    name = "values_test",
    srcs = ["values_test.cc"],
    deps = [
        ":values",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "query",
    srcs = ["query.cc"],
    hdrs = ["query.h"],
    deps = [
        ":types",
        ":values",
        "//backend/query:query_engine",
        "//common:errors",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_grpc",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_zetasql//zetasql/base:statusor",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_library(
    name = "partition",
    srcs = ["partition.cc"],
    hdrs = ["partition.h"],
    deps = [
        "//common:errors",
        "//frontend/proto:partition_token_cc_proto",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/base:statusor",
    ],
)
