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
    name = "iterator",
    hdrs = [
        "iterator.h",
    ],
    deps = [
        "//backend/datamodel:key",
        "@com_google_absl//absl/status",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "storage",
    hdrs = [
        "storage.h",
    ],
    deps = [
        ":iterator",
        "//backend/common:ids",
        "//backend/datamodel:key",
        "//backend/datamodel:key_range",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/time",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "in_memory_storage",
    srcs = ["in_memory_storage.cc"],
    hdrs = [
        "in_memory_storage.h",
    ],
    deps = [
        ":in_memory_iterator",
        ":iterator",
        ":storage",
        "//backend/common:ids",
        "//backend/datamodel:key",
        "//backend/datamodel:key_range",
        "//common:errors",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "in_memory_storage_test",
    srcs = [
        "in_memory_storage_test.cc",
    ],
    deps = [
        ":in_memory_storage",
        ":iterator",
        "//backend/datamodel:key_range",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "in_memory_iterator",
    srcs = ["in_memory_iterator.cc"],
    hdrs = [
        "in_memory_iterator.h",
    ],
    deps = [
        ":iterator",
        "//backend/datamodel:key",
        "@com_google_absl//absl/status",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "in_memory_iterator_test",
    srcs = [
        "in_memory_iterator_test.cc",
    ],
    deps = [
        ":in_memory_iterator",
        "//backend/datamodel:key",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:value",
    ],
)
