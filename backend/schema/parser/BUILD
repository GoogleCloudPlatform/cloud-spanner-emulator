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

load(
    "//backend/schema/parser:javacc_parser.bzl",
    "generate_javacc_parser",
)

package(default_visibility = ["//:__subpackages__"])

licenses(["unencumbered"])

cc_library(
    name = "ddl_parser",
    srcs = [
        "ddl_includes.h",
        "ddl_parser.cc",
    ],
    hdrs = [
        "ddl_parser.h",
    ],
    deps = [
        ":javacc_ddl_parser",
        "//backend/schema/ddl:operations_cc_proto",
        "//common:errors",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/base",
        "@com_google_zetasql//zetasql/base:statusor",
    ],
)

cc_test(
    name = "ddl_parser_test",
    srcs = ["ddl_parser_test.cc"],
    deps = [
        ":ddl_parser",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

generate_javacc_parser(
    name = "javacc_ddl_parser",
    # Do not sort: "ddl_parser_cpp.inc" must come before "ddl_parser.jjt".
    srcs = [
        "ddl_parser_cpp.inc",
        "ddl_parser.jjt",
    ],
    extra_headers = [
        "ddl_node_base.h",
        "ddl_token_base.h",
    ],
    parser_class_name = "DDLParser",
)
