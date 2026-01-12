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
    name = "value",
    hdrs = ["value.h"],
    deps = [
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "key",
    srcs = ["key.cc"],
    hdrs = ["key.h"],
    deps = [
        "@com_google_zetasql//zetasql/public:numeric_value",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "key_test",
    srcs = ["key_test.cc"],
    deps = [
        ":key",
        ":value",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "key_range",
    srcs = ["key_range.cc"],
    hdrs = ["key_range.h"],
    deps = [
        ":key",
    ],
)

cc_test(
    name = "key_range_test",
    srcs = ["key_range_test.cc"],
    deps = [
        ":key_range",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "key_set",
    srcs = ["key_set.cc"],
    hdrs = ["key_set.h"],
    deps = [
        ":key",
        ":key_range",
    ],
)

cc_test(
    name = "key_set_test",
    srcs = ["key_set_test.cc"],
    deps = [
        ":key_range",
        ":key_set",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/flags:flag",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "types",
    srcs = ["types.cc"],
    hdrs = ["types.h"],
    deps = [
        "//common:feature_flags",
        "//third_party/spanner_pg/datatypes/extended:spanner_extended_type",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_proto",
        "@com_google_zetasql//zetasql/public:options_cc_proto",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:type_cc_proto",
    ],
)

cc_test(
    name = "types_test",
    srcs = ["types_test.cc"],
    deps = [
        ":types",
        "//tests/common:proto_matchers",
        "//third_party/spanner_pg/datatypes/extended:pg_jsonb_type",
        "//third_party/spanner_pg/datatypes/extended:pg_numeric_type",
        "//third_party/spanner_pg/test_catalog",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:type",
    ],
)
