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
    name = "environment",
    testonly = 1,
    srcs = ["environment.cc"],
    hdrs = ["environment.h"],
    deps = [
        "@com_github_googleapis_google_cloud_cpp//:spanner",
        "@com_github_googleapis_google_cloud_cpp//google/cloud:google_cloud_cpp_common",
        "@com_google_absl//absl/status",
        "@com_google_zetasql//zetasql/base:statusor",
    ],
)

cc_library(
    name = "database_test_base",
    testonly = 1,
    srcs = ["database_test_base.cc"],
    hdrs = ["database_test_base.h"],
    deps = [
        ":environment",
        "//frontend/server",
        "//tests/common:proto_matchers",
        "@com_github_googleapis_google_cloud_cpp//:spanner",
        "@com_github_googleapis_google_cloud_cpp//google/cloud:google_cloud_cpp_common",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_googleapis//google/spanner/admin/database/v1:database_cc_grpc",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_grpc",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base:statusor",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)
