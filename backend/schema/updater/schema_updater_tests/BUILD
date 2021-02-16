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

# Test cases for schema_updater_test.cc.

package(default_visibility = [
    "//:__subpackages__",
])

licenses(["unencumbered"])

cc_library(
    name = "base",
    testonly = 1,
    srcs = ["base.cc"],
    hdrs = ["base.h"],
    deps = [
        "//backend/common:ids",
        "//backend/schema/catalog:schema",
        "//backend/schema/graph:schema_graph",
        "//backend/schema/graph:schema_graph_editor",
        "//backend/schema/graph:schema_node",
        "//backend/schema/updater:schema_updater",
        "//backend/storage:in_memory_storage",
        "//common:errors",
        "//common:limits",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base:statusor",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_library(
    name = "schema_updater_tests",
    testonly = 1,
    srcs = [
        "check_constraint.cc",
        "common.cc",
        "foreign_key.cc",
        "generated_column.cc",
        "index.cc",
        "table.cc",
    ],
    deps = [
        ":base",
        "//backend/schema/catalog:schema",
        "//backend/schema/updater:global_schema_names",
        "//common:feature_flags",
        "//tests/common:scoped_feature_flags_setter",
        "@com_google_absl//absl/strings",
    ],
    alwayslink = 1,
)
