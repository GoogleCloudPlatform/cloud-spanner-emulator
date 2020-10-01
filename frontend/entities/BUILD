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
    name = "session",
    srcs = [
        "session.cc",
    ],
    hdrs = [
        "session.h",
    ],
    deps = [
        ":database",
        ":transaction",
        "//backend/common:ids",
        "//backend/transaction:read_only_transaction",
        "//backend/transaction:read_write_transaction",
        "//common:errors",
        "//common:limits",
        "//frontend/common:labels",
        "//frontend/common:protos",
        "//frontend/converters:reads",
        "//frontend/converters:time",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_grpc",
        "@com_google_zetasql//zetasql/base:statusor",
    ],
)

cc_library(
    name = "database",
    srcs = ["database.cc"],
    hdrs = [
        "database.h",
    ],
    deps = [
        "//backend/database",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/time",
        "@com_google_googleapis//google/spanner/admin/database/v1:database_cc_grpc",
    ],
)

cc_library(
    name = "instance",
    srcs = [
        "instance.cc",
    ],
    hdrs = [
        "instance.h",
    ],
    deps = [
        "//frontend/common:labels",
        "@com_google_absl//absl/strings",
        "@com_google_googleapis//google/spanner/admin/instance/v1:instance_cc_grpc",
    ],
)

cc_test(
    name = "instance_test",
    srcs = [
        "instance_test.cc",
    ],
    deps = [
        ":instance",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "operation",
    srcs = ["operation.cc"],
    hdrs = ["operation.h"],
    deps = [
        "//frontend/common:protos",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_googleapis//google/longrunning:longrunning_cc_grpc",
        "@com_google_googleapis//google/rpc:status_cc_proto",
        "@com_google_protobuf//:protobuf_headers",
    ],
)

cc_test(
    name = "operation_test",
    srcs = ["operation_test.cc"],
    deps = [
        ":operation",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googleapis//google/spanner/admin/instance/v1:instance_cc_grpc",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "transaction",
    srcs = ["transaction.cc"],
    hdrs = ["transaction.h"],
    deps = [
        ":database",
        "//backend/access:read",
        "//backend/access:write",
        "//backend/common:ids",
        "//backend/common:variant",
        "//backend/database",
        "//backend/query:query_engine",
        "//backend/schema/catalog:schema",
        "//backend/transaction:read_only_transaction",
        "//backend/transaction:read_write_transaction",
        "//common:clock",
        "//common:constants",
        "//common:errors",
        "//frontend/converters:time",
        "//frontend/converters:types",
        "//frontend/converters:values",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:variant",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_grpc",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/base:statusor",
        "@com_google_zetasql//zetasql/base:time_proto_util",
        "@com_google_zetasql//zetasql/public:value",
    ],
)
