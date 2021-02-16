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
    name = "schema_node",
    hdrs = ["schema_node.h"],
    deps = ["@com_google_absl//absl/status"],
)

cc_library(
    name = "schema_graph",
    hdrs = ["schema_graph.h"],
    deps = [
        ":schema_node",
        ":schema_objects_pool",
    ],
)

cc_library(
    name = "schema_objects_pool",
    hdrs = ["schema_objects_pool.h"],
    deps = [
        ":schema_node",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "schema_graph_editor",
    srcs = ["schema_graph_editor.cc"],
    hdrs = ["schema_graph_editor.h"],
    deps = [
        ":schema_graph",
        ":schema_node",
        ":schema_objects_pool",
        "//backend/schema/updater:schema_validation_context",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/base",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/base:statusor",
    ],
)
