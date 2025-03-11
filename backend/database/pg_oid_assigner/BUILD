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

package(
    default_visibility = ["//:__subpackages__"],
)

licenses(["notice"])

cc_library(
    name = "pg_oid_assigner",
    hdrs = [
        "pg_oid_assigner.h",
    ],
    deps = [
        "@com_google_absl//absl/status",
        "@com_google_zetasql//zetasql/base:ret_check",
    ],
)

cc_test(
    name = "pg_oid_assigner_test",
    size = "small",
    srcs = [
        "pg_oid_assigner_test.cc",
    ],
    deps = [
        ":pg_oid_assigner",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)
