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

cc_library(
    name = "remote_udf_evaluator",
    srcs = ["remote_udf_evaluator.cc"],
    hdrs = ["remote_udf_evaluator.h"],
    deps = [
        "//third_party/spanner_pg/datatypes/extended:pg_jsonb_type",
        "//third_party/spanner_pg/datatypes/extended:pg_numeric_type",
        "//third_party/spanner_pg/datatypes/extended:pg_oid_type",
        "//third_party/spanner_pg/datatypes/extended:spanner_extended_type",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/numeric:int128",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:span",
        "@com_google_farmhash//:farmhash_fingerprint",
        "@com_google_googlesql//googlesql/base:ret_check",
        "@com_google_googlesql//googlesql/base:status",
        "@com_google_googlesql//googlesql/public:function",
        "@com_google_googlesql//googlesql/public:interval_value",
        "@com_google_googlesql//googlesql/public:json_value",
        "@com_google_googlesql//googlesql/public:numeric_value",
        "@com_google_googlesql//googlesql/public:type",
        "@com_google_googlesql//googlesql/public:type_cc_proto",
        "@com_google_googlesql//googlesql/public:uuid_value",
        "@com_google_googlesql//googlesql/public:value",
        "@com_google_googlesql//googlesql/public/functions:convert_string",
        "@com_google_googlesql//googlesql/public/functions:date_time_util",
        "@com_google_googlesql//googlesql/public/functions:json",
        "@com_google_googlesql//googlesql/public/functions:json_format",
        "@com_google_googlesql//googlesql/public/types:timestamp_util",
        "@com_google_googlesql//googlesql/public/types:type",
        "@com_google_protobuf//:protobuf",
        "@cpp-httplib//:httplib",
    ],
)

cc_test(
    name = "remote_udf_evaluator_test",
    srcs = ["remote_udf_evaluator_test.cc"],
    deps = [
        ":remote_udf_evaluator",
        "//tests/common:proto_matchers",
        "//tests/common:test_cc_proto",
        "//third_party/spanner_pg/datatypes/extended:pg_jsonb_type",
        "//third_party/spanner_pg/datatypes/extended:pg_numeric_type",
        "//third_party/spanner_pg/datatypes/extended:pg_oid_type",
        "//third_party/spanner_pg/interface:pg_arena_factory",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "@com_github_google_benchmark//:benchmark",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/time",
        "@com_google_googlesql//googlesql/base/testing:status_matchers",
        "@com_google_googlesql//googlesql/public:interval_value",
        "@com_google_googlesql//googlesql/public:json_value",
        "@com_google_googlesql//googlesql/public:numeric_value",
        "@com_google_googlesql//googlesql/public:type",
        "@com_google_googlesql//googlesql/public:uuid_value",
        "@com_google_googlesql//googlesql/public:value",
        "@com_google_googlesql//googlesql/public/functions:date_time_util",
        "@com_google_googletest//:gtest_main",
        "@cpp-httplib//:httplib",
    ],
)
