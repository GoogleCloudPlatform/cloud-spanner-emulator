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
    name = "databases",
    srcs = ["databases.cc"],
    deps = [
        "//backend/database",
        "//backend/schema/ddl:operations_cc_proto",
        "//backend/schema/parser:ddl_parser",
        "//backend/schema/printer:print_ddl",
        "//common:errors",
        "//common:limits",
        "//frontend/common:uris",
        "//frontend/converters:time",
        "//frontend/entities:database",
        "//frontend/server:handler",
        "@com_google_absl//absl/status",
        "@com_google_googleapis//google/longrunning:longrunning_cc_grpc",
        "@com_google_googleapis//google/spanner/admin/database/v1:database_cc_grpc",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_googlesource_code_re2//:re2",
    ],
    alwayslink = 1,
)

cc_test(
    name = "databases_test",
    srcs = ["databases_test.cc"],
    deps = [
        ":databases",
        "//backend/database",
        "//backend/schema/printer:print_ddl",
        "//common:limits",
        "//frontend/common:uris",
        "//tests/common:proto_matchers",
        "//tests/common:test_env",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_googleapis//google/spanner/admin/database/v1:database_cc_grpc",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "instances",
    srcs = ["instances.cc"],
    deps = [
        "//common:errors",
        "//common:limits",
        "//frontend/collections:operation_manager",
        "//frontend/common:labels",
        "//frontend/common:uris",
        "//frontend/converters:time",
        "//frontend/entities:instance",
        "//frontend/entities:operation",
        "//frontend/server:handler",
        "@com_google_absl//absl/status",
        "@com_google_googleapis//google/longrunning:longrunning_cc_grpc",
        "@com_google_googleapis//google/spanner/admin/instance/v1:instance_cc_grpc",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_googlesource_code_re2//:re2",
    ],
    alwayslink = 1,
)

cc_test(
    name = "instances_test",
    srcs = [
        "instances_test.cc",
    ],
    deps = [
        "//frontend/common:uris",
        "//frontend/server",
        "//tests/common:proto_matchers",
        "//tests/common:test_env",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@com_google_googleapis//google/longrunning:longrunning_cc_grpc",
        "@com_google_googleapis//google/spanner/admin/instance/v1:instance_cc_grpc",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "operations",
    srcs = ["operations.cc"],
    deps = [
        "//frontend/common:uris",
        "//frontend/server:handler",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_googleapis//google/longrunning:longrunning_cc_grpc",
        "@com_google_protobuf//:cc_wkt_protos",
    ],
    alwayslink = 1,
)

cc_test(
    name = "operations_test",
    srcs = ["operations_test.cc"],
    deps = [
        ":operations",
        "//tests/common:proto_matchers",
        "//tests/common:test_env",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "partitions",
    srcs = ["partitions.cc"],
    deps = [
        "//backend/query:query_engine",
        "//common:config",
        "//common:errors",
        "//frontend/converters:partition",
        "//frontend/converters:query",
        "//frontend/converters:reads",
        "//frontend/entities:session",
        "//frontend/proto:partition_token_cc_proto",
        "//frontend/server:handler",
        "@com_google_absl//absl/status",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_grpc",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_zetasql//zetasql/base:statusor",
    ],
    alwayslink = 1,
)

cc_test(
    name = "partitions_test",
    srcs = ["partitions_test.cc"],
    deps = [
        ":partitions",
        "//common:errors",
        "//tests/common:proto_matchers",
        "//tests/common:test_env",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_grpc",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "policies",
    srcs = ["policies.cc"],
    deps = [
        "//common:errors",
        "//frontend/server:handler",
        "@com_google_googleapis//google/iam/v1:iam_policy_cc_proto",
        "@com_google_googleapis//google/iam/v1:policy_cc_proto",
    ],
    alwayslink = 1,
)

cc_test(
    name = "policies_test",
    srcs = ["policies_test.cc"],
    deps = [
        ":policies",
        "//tests/common:proto_matchers",
        "//tests/common:test_env",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googleapis//google/iam/v1:iam_policy_cc_proto",
        "@com_google_googleapis//google/iam/v1:policy_cc_proto",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "queries",
    srcs = ["queries.cc"],
    deps = [
        "//backend/access:read",
        "//backend/query:query_engine",
        "//common:constants",
        "//common:errors",
        "//frontend/common:protos",
        "//frontend/converters:partition",
        "//frontend/converters:query",
        "//frontend/converters:reads",
        "//frontend/converters:types",
        "//frontend/converters:values",
        "//frontend/entities:session",
        "//frontend/entities:transaction",
        "//frontend/proto:partition_token_cc_proto",
        "//frontend/server:handler",
        "//frontend/server:request_context",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:variant",
        "@com_google_farmhash//:farmhash_fingerprint",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_grpc",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_zetasql//zetasql/base:statusor",
    ],
    alwayslink = 1,
)

cc_test(
    name = "queries_test",
    srcs = ["queries_test.cc"],
    deps = [
        ":queries",
        "//backend/datamodel:types",
        "//common:errors",
        "//frontend/converters:types",
        "//frontend/converters:values",
        "//tests/common:proto_matchers",
        "//tests/common:test_env",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_grpc",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "reads",
    srcs = ["reads.cc"],
    deps = [
        "//backend/common:ids",
        "//common:errors",
        "//frontend/common:protos",
        "//frontend/converters:reads",
        "//frontend/entities:session",
        "//frontend/entities:transaction",
        "//frontend/server:handler",
        "@com_google_absl//absl/status",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_grpc",
    ],
    alwayslink = 1,
)

cc_test(
    name = "reads_test",
    srcs = ["reads_test.cc"],
    deps = [
        ":reads",
        "//frontend/common:protos",
        "//frontend/common:uris",
        "//frontend/converters:reads",
        "//frontend/entities:database",
        "//frontend/entities:session",
        "//frontend/entities:transaction",
        "//tests/common:proto_matchers",
        "//tests/common:test_env",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_grpc",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "sessions",
    srcs = [
        "sessions.cc",
    ],
    deps = [
        "//common:errors",
        "//common:limits",
        "//frontend/collections:database_manager",
        "//frontend/common:labels",
        "//frontend/common:uris",
        "//frontend/entities:database",
        "//frontend/entities:session",
        "//frontend/server:environment",
        "//frontend/server:handler",
        "@com_google_absl//absl/status",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_grpc",
        "@com_google_protobuf//:cc_wkt_protos",
    ],
    alwayslink = 1,
)

cc_test(
    name = "sessions_test",
    srcs = ["sessions_test.cc"],
    deps = [
        ":sessions",
        "//frontend/common:protos",
        "//frontend/common:uris",
        "//tests/common:proto_matchers",
        "//tests/common:test_env",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_googleapis//google/spanner/admin/database/v1:database_cc_grpc",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_grpc",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "transactions",
    srcs = ["transactions.cc"],
    deps = [
        "//backend/transaction:read_only_transaction",
        "//backend/transaction:read_write_transaction",
        "//common:errors",
        "//frontend/common:protos",
        "//frontend/converters:mutations",
        "//frontend/converters:time",
        "//frontend/entities:session",
        "//frontend/entities:transaction",
        "//frontend/server:handler",
        "@com_google_absl//absl/status",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_grpc",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_zetasql//zetasql/base:statusor",
    ],
    alwayslink = 1,
)

cc_test(
    name = "transactions_test",
    srcs = ["transactions_test.cc"],
    deps = [
        ":transactions",
        "//backend/transaction:read_only_transaction",
        "//backend/transaction:read_write_transaction",
        "//common:errors",
        "//frontend/common:uris",
        "//frontend/converters:mutations",
        "//frontend/entities:database",
        "//frontend/entities:session",
        "//frontend/server:environment",
        "//frontend/server:handler",
        "//frontend/server:request_context",
        "//tests/common:proto_matchers",
        "//tests/common:test_env",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_grpc",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "handlers",
    deps = [
        ":databases",
        ":instances",
        ":operations",
        ":partitions",
        ":policies",
        ":queries",
        ":reads",
        ":sessions",
        ":transactions",
    ],
)
