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

package(
    default_visibility = ["//:__subpackages__"],
)

licenses(["notice"])

cc_library(
    name = "queryable_queue_tvfs",
    srcs = [
        "queryable_queue_tvfs.cc",
    ],
    hdrs = [
        "queryable_queue_tvfs.h",
    ],
    deps = [
        "//backend/schema/catalog:schema",
        "//common:constants",
        "//common:errors",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_googlesql//googlesql/base:ret_check",
        "@com_google_googlesql//googlesql/base:status",
        "@com_google_googlesql//googlesql/public:catalog",
        "@com_google_googlesql//googlesql/public:evaluator_table_iterator",
        "@com_google_googlesql//googlesql/public:function",
        "@com_google_googlesql//googlesql/public:type",
        "@com_google_googlesql//googlesql/public:value",
    ],
)
