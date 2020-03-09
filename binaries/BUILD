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

load("@io_bazel_rules_go//go:def.bzl", "go_binary")

package(default_visibility = ["//:__subpackages__"])

licenses(["unencumbered"])

cc_binary(
    name = "emulator_main",
    srcs = ["emulator_main.cc"],
    deps = [
        "//common:config",
        "//frontend/server",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/flags:parse",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/base",
    ],
)

go_binary(
    name = "gateway_main",
    srcs = ["gateway_main.go"],
    data = [
        ":emulator_main",
    ],
    deps = ["//gateway"],
)
