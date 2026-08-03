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
    name = "model_evaluator",
    srcs = ["model_evaluator.cc"],
    hdrs = ["model_evaluator.h"],
    deps = [
        "//backend/common:case",
        "//backend/query:queryable_model",
        "//backend/query/remote_udf:remote_udf_evaluator",
        "//backend/schema/catalog:schema",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_googlesql//googlesql/base:ret_check",
        "@com_google_googlesql//googlesql/base:status",
        "@com_google_googlesql//googlesql/public:catalog",
        "@com_google_googlesql//googlesql/public:json_value",
        "@com_google_googlesql//googlesql/public:type",
        "@com_google_googlesql//googlesql/public:value",
    ],
)

cc_test(
    name = "model_evaluator_test",
    srcs = ["model_evaluator_test.cc"],
    deps = [
        ":model_evaluator",
        "//backend/common:case",
        "//backend/query:queryable_model",
        "//backend/query/remote_udf:remote_udf_evaluator",
        "//backend/schema/catalog:schema",
        "//tests/common:proto_matchers",
        "//tests/common:test_schema_constructor",
        "@com_github_google_benchmark//:benchmark",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googlesql//googlesql/base/testing:status_matchers",
        "@com_google_googlesql//googlesql/public:json_value",
        "@com_google_googlesql//googlesql/public:type",
        "@com_google_googlesql//googlesql/public:value",
        "@com_google_googletest//:gtest_main",
        "@cpp-httplib//:httplib",
    ],
)

cc_library(
    name = "ml_predict_table_valued_function",
    srcs = ["ml_predict_table_valued_function.cc"],
    hdrs = ["ml_predict_table_valued_function.h"],
    deps = [
        ":model_evaluator",
        "//backend/common:case",
        "//backend/query:queryable_model",
        "//common:errors",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_googlesql//googlesql/base:ret_check",
        "@com_google_googlesql//googlesql/base:status",
        "@com_google_googlesql//googlesql/public:analyzer_options",
        "@com_google_googlesql//googlesql/public:catalog",
        "@com_google_googlesql//googlesql/public:evaluator_table_iterator",
        "@com_google_googlesql//googlesql/public:function",
        "@com_google_googlesql//googlesql/public:type",
        "@com_google_googlesql//googlesql/public:value",
    ],
)

cc_library(
    name = "ml_predict_row_function",
    srcs = ["ml_predict_row_function.cc"],
    hdrs = ["ml_predict_row_function.h"],
    deps = [
        ":model_evaluator",
        "//common:errors",
        "//third_party/spanner_pg/datatypes/extended:pg_jsonb_type",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:span",
        "@com_google_googlesql//googlesql/base:ret_check",
        "@com_google_googlesql//googlesql/base:status",
        "@com_google_googlesql//googlesql/public:json_value",
        "@com_google_googlesql//googlesql/public:value",
    ],
)
