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
load(
    "//backend/common:javacc_parser.bzl",
    "generate_javacc_parser",
)

package(
    default_visibility = ["//:__subpackages__"],
)

licenses(["notice"])

cc_library(
    name = "search_query_parser",
    srcs = [
        "query_includes.h",
        "query_parser.cc",
    ],
    hdrs = [
        "query_parser.h",
    ],
    deps = [
        ":javacc_search_query_parser",
        "//common:errors",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/public/functions:string",
    ],
)

cc_test(
    name = "search_query_parser_test",
    srcs = ["query_parser_test.cc"],
    deps = [
        ":javacc_search_query_parser",
        ":search_query_parser",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

genrule(
    name = "copy_node_base_header",
    srcs = ["query_node_base.h"],
    outs = ["QueryNodeBase.h"],
    cmd = "cat $(SRCS) > $(OUTS)",
)

generate_javacc_parser(
    name = "javacc_search_query_parser",
    # Do not sort: "search_query_parser_cpp.inc" must come before ".jjt".
    srcs = [
        "query_parser_cpp.inc",
        "query_parser.jjt",
    ],
    extra_deps = [],
    extra_headers = [
        "query_char_stream.h",
        "query_node_base.h",
        "query_token_base.h",
    ],
    extra_srcs = [
        "QueryNodeBase.h",
    ],
    parser_class_name = "SearchQueryParser",
)

cc_library(
    name = "search_function_catalog",
    srcs = ["search_function_catalog.cc"],
    hdrs = ["search_function_catalog.h"],
    deps = [
        ":bool_tokenizer",
        ":exact_match_tokenizer",
        ":json_tokenizer",
        ":jsonb_tokenizer",
        ":ngrams_tokenizer",
        ":numeric_tokenizer",
        ":plain_full_text_tokenizer",
        ":score_evaluator",
        ":score_ngrams_evaluator",
        ":search_evaluator",
        ":search_ngrams_evaluator",
        ":search_substring_evaluator",
        ":snippet_evaluator",
        ":substring_tokenizer",
        ":tokenlist_concat",
        "//backend/schema/catalog:schema",
        "//third_party/spanner_pg/datatypes/extended:pg_jsonb_type",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:function_cc_proto",
        "@com_google_zetasql//zetasql/public:json_value",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "tokenizer",
    srcs = ["tokenizer.cc"],
    hdrs = ["tokenizer.h"],
    deps = [
        "//common:errors",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/public:simple_token_list",
        "@com_google_zetasql//zetasql/public:token_list_util",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "exact_match_tokenizer",
    srcs = ["exact_match_tokenizer.cc"],
    hdrs = ["exact_match_tokenizer.h"],
    deps = [
        ":tokenizer",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "exact_match_tokenizer_test",
    srcs = ["exact_match_tokenizer_test.cc"],
    deps = [
        ":exact_match_tokenizer",
        ":tokenizer",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "plain_full_text_tokenizer",
    srcs = ["plain_full_text_tokenizer.cc"],
    hdrs = ["plain_full_text_tokenizer.h"],
    deps = [
        ":tokenizer",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/public:value",
        "@com_google_zetasql//zetasql/public/functions:string",
    ],
)

cc_test(
    name = "plain_full_text_tokenizer_test",
    srcs = ["plain_full_text_tokenizer_test.cc"],
    deps = [
        ":plain_full_text_tokenizer",
        ":tokenizer",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "search_evaluator_helpers",
    srcs = ["search_evaluator_helpers.cc"],
    hdrs = ["search_evaluator_helpers.h"],
    deps = [
        ":javacc_search_query_parser",
        ":search_query_parser",
        ":tokenizer",
        "//common:errors",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "search_evaluator_helpers_test",
    srcs = ["search_evaluator_helpers_test.cc"],
    deps = [
        ":search_evaluator_helpers",
        ":tokenizer",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "search_evaluator",
    srcs = ["search_evaluator.cc"],
    hdrs = ["search_evaluator.h"],
    deps = [
        ":javacc_search_query_parser",
        ":search_evaluator_helpers",
        "//common:errors",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "search_evaluator_test",
    srcs = ["search_evaluator_test.cc"],
    deps = [
        ":search_evaluator",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "substring_tokenizer",
    srcs = ["substring_tokenizer.cc"],
    hdrs = ["substring_tokenizer.h"],
    deps = [
        ":tokenizer",
        "//common:errors",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
        "@com_google_zetasql//zetasql/public/functions:string",
    ],
)

cc_test(
    name = "substring_tokenizer_test",
    srcs = ["substring_tokenizer_test.cc"],
    deps = [
        ":substring_tokenizer",
        ":tokenizer",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "search_substring_evaluator",
    srcs = ["search_substring_evaluator.cc"],
    hdrs = ["search_substring_evaluator.h"],
    deps = [
        ":tokenizer",
        "//common:errors",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:value",
        "@com_google_zetasql//zetasql/public/functions:string",
    ],
)

cc_test(
    name = "search_substring_evaluator_test",
    srcs = ["search_substring_evaluator_test.cc"],
    deps = [
        ":plain_full_text_tokenizer",
        ":search_substring_evaluator",
        ":tokenizer",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "numeric_tokenizer",
    srcs = ["numeric_tokenizer.cc"],
    hdrs = ["numeric_tokenizer.h"],
    deps = [
        ":tokenizer",
        "//backend/common:case",
        "//common:errors",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "numeric_tokenizer_test",
    srcs = ["numeric_tokenizer_test.cc"],
    deps = [
        ":numeric_tokenizer",
        ":tokenizer",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "bool_tokenizer",
    srcs = ["bool_tokenizer.cc"],
    hdrs = ["bool_tokenizer.h"],
    deps = [
        ":tokenizer",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "bool_tokenizer_test",
    srcs = ["bool_tokenizer_test.cc"],
    deps = [
        ":bool_tokenizer",
        ":tokenizer",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "score_evaluator",
    srcs = ["score_evaluator.cc"],
    hdrs = ["score_evaluator.h"],
    deps = [
        ":search_evaluator_helpers",
        ":tokenizer",
        "//common:errors",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "score_evaluator_test",
    srcs = ["score_evaluator_test.cc"],
    deps = [
        ":exact_match_tokenizer",
        ":score_evaluator",
        ":tokenizer",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "snippet_evaluator",
    srcs = ["snippet_evaluator.cc"],
    hdrs = ["snippet_evaluator.h"],
    deps = [
        ":tokenizer",
        "//common:errors",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/public:value",
        "@com_googlesource_code_re2//:re2",
        "@nlohmann_json//:json",
    ],
)

cc_test(
    name = "snippet_evaluator_test",
    srcs = ["snippet_evaluator_test.cc"],
    deps = [
        ":snippet_evaluator",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:json_value",
    ],
)

cc_library(
    name = "tokenlist_concat",
    srcs = ["tokenlist_concat.cc"],
    hdrs = ["tokenlist_concat.h"],
    deps = [
        ":tokenizer",
        "//common:errors",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "tokenlist_concat_test",
    srcs = ["tokenlist_concat_test.cc"],
    deps = [
        ":plain_full_text_tokenizer",
        ":substring_tokenizer",
        ":tokenizer",
        ":tokenlist_concat",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:type",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "ngrams_tokenizer",
    srcs = ["ngrams_tokenizer.cc"],
    hdrs = ["ngrams_tokenizer.h"],
    deps = [
        ":tokenizer",
        "//common:errors",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/public:value",
        "@com_google_zetasql//zetasql/public/functions:string",
    ],
)

cc_test(
    name = "ngrams_tokenizer_test",
    srcs = ["ngrams_tokenizer_test.cc"],
    deps = [
        ":ngrams_tokenizer",
        ":tokenizer",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "search_ngrams_evaluator",
    srcs = ["search_ngrams_evaluator.cc"],
    hdrs = ["search_ngrams_evaluator.h"],
    deps = [
        ":ngrams_tokenizer",
        ":tokenizer",
        "//common:errors",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "search_ngrams_evaluator_test",
    srcs = ["search_ngrams_evaluator_test.cc"],
    deps = [
        ":plain_full_text_tokenizer",
        ":search_ngrams_evaluator",
        ":tokenizer",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "score_ngrams_evaluator",
    srcs = ["score_ngrams_evaluator.cc"],
    hdrs = ["score_ngrams_evaluator.h"],
    deps = [
        ":ngrams_tokenizer",
        ":tokenizer",
        "//common:errors",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "score_ngrams_evaluator_test",
    srcs = ["score_ngrams_evaluator_test.cc"],
    deps = [
        ":ngrams_tokenizer",
        ":plain_full_text_tokenizer",
        ":score_ngrams_evaluator",
        ":tokenizer",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "json_tokenizer",
    srcs = ["json_tokenizer.cc"],
    hdrs = ["json_tokenizer.h"],
    deps = [
        ":tokenizer",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "json_tokenizer_test",
    srcs = ["json_tokenizer_test.cc"],
    deps = [
        ":json_tokenizer",
        ":tokenizer",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:json_value",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "jsonb_tokenizer",
    srcs = ["jsonb_tokenizer.cc"],
    hdrs = ["jsonb_tokenizer.h"],
    deps = [
        ":tokenizer",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "jsonb_tokenizer_test",
    srcs = ["jsonb_tokenizer_test.cc"],
    deps = [
        ":jsonb_tokenizer",
        ":tokenizer",
        "//tests/common:proto_matchers",
        "//third_party/spanner_pg/datatypes/extended:pg_jsonb_type",
        "//third_party/spanner_pg/test_catalog",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:value",
    ],
)
