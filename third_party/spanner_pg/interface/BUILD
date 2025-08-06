#
# PostgreSQL is released under the PostgreSQL License, a liberal Open Source
# license, similar to the BSD or MIT licenses.
#
# PostgreSQL Database Management System
# (formerly known as Postgres, then as Postgres95)
#
# Portions Copyright © 1996-2020, The PostgreSQL Global Development Group
#
# Portions Copyright © 1994, The Regents of the University of California
#
# Portions Copyright 2023 Google LLC
#
# Permission to use, copy, modify, and distribute this software and its
# documentation for any purpose, without fee, and without a written agreement
# is hereby granted, provided that the above copyright notice and this
# paragraph and the following two paragraphs appear in all copies.
#
# IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
# DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
# LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
# EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
# SUCH DAMAGE.
#
# THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
# INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN
# "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
# MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
#------------------------------------------------------------------------------

# entry point from Spanner to Spangres

load("@rules_cc//cc:defs.bzl", "cc_library", "cc_test")

package(
    default_visibility = [
        "//:__subpackages__",
    ],
)

licenses(["notice"])

cc_library(
    name = "memory_reservation_manager",
    hdrs = ["memory_reservation_manager.h"],
    # This file is Google-authored.
    licenses = ["unencumbered"],
    visibility = ["//:__subpackages__"],
)

cc_library(
    name = "pg_arena_factory",
    srcs = ["//third_party/spanner_pg/function_evaluators:pg_arena_factory.cc"],
    hdrs = [
        "pg_arena.h",
        "pg_arena_factory.h",
    ],
    visibility = ["//:__subpackages__"],
    deps = [
        ":memory_reservation_manager",
        "//third_party/spanner_pg/shims:memory_context_pg_arena",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_library(
    name = "interfaces_with_stub_implementations",
    srcs = [
        "spangres_translator_factory.cc",
        "spangres_translator_interface.cc",
    ],
    hdrs = [
        "bootstrap_catalog_accessor.h",
        "engine_builtin_function_catalog.h",
        "spangres_translator_factory.h",
        "spangres_translator_interface.h",
        "stub_builtin_function_catalog.h",
        "stub_spangres_translator.h",
    ],
    # The interface and stub implementation are Google-authored.
    licenses = ["unencumbered"],
    visibility = ["//:__subpackages__"],
    deps = [
        ":bootstrap_catalog_data_cc_proto",
        ":memory_reservation_manager",
        ":parser_interface",
        ":pg_evaluators_stubs",  # buildcleaner: keep
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/flags:parse",  # buildcleaner: keep
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:variant",
        "@com_google_absl//absl/utility",
        "@com_google_zetasql//zetasql/base",
        "@com_google_zetasql//zetasql/public:analyzer",
        "@com_google_zetasql//zetasql/public:analyzer_options",
        "@com_google_zetasql//zetasql/public:builtin_function",
        "@com_google_zetasql//zetasql/public:builtin_function_options",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:language_options",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_library(
    name = "implementations_without_parser",
    srcs = [
        "spangres_translator.cc",
        "spangres_translator_factory_override.cc",
    ],
    hdrs = [
        "spangres_translator.h",
    ],
    tags = ["keep_dep"],
    deps = [
        ":interfaces_with_stub_implementations",
        ":memory_reservation_manager",
        ":parser_interface",
        ":sql_builder",
        "//third_party/spanner_pg/bootstrap_catalog:bootstrap_catalog_accessor_impl",  # buildcleaner: keep
        "//third_party/spanner_pg/catalog:catalog_adapter",
        "//third_party/spanner_pg/catalog:catalog_adapter_holder",
        "//third_party/spanner_pg/catalog:engine_system_catalog",
        "//third_party/spanner_pg/catalog:spangres_system_catalog",
        "//third_party/spanner_pg/catalog:spangres_user_catalog",
        "//third_party/spanner_pg/catalog:type",
        "//third_party/spanner_pg/function_evaluators:pg_evaluators_implementations",  # buildcleaner: keep
        "//third_party/spanner_pg/shims:error_shim",
        "//third_party/spanner_pg/shims:memory_context_pg_arena",
        "//third_party/spanner_pg/shims:parser_output_serialization",
        "//third_party/spanner_pg/shims:stub_memory_reservation_manager",
        "//third_party/spanner_pg/shims:timezone_helper",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "//third_party/spanner_pg/transformer",
        "//third_party/spanner_pg/transformer:forward_transformer",
        "//third_party/spanner_pg/util:postgres",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/base",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:analyzer",
        "@com_google_zetasql//zetasql/public:analyzer_options",
        "@com_google_zetasql//zetasql/public:analyzer_output",
        "@com_google_zetasql//zetasql/public:analyzer_output_properties",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:options_cc_proto",
        "@com_google_zetasql//zetasql/public:strings",
        "@com_google_zetasql//zetasql/public/types:type",
        "@com_google_zetasql//zetasql/resolved_ast",
        "@com_google_zetasql//zetasql/resolved_ast:validator",
    ],
    alwayslink = 1,  # for REGISTER_MODULE_INITIALIZER
)

cc_library(
    name = "parser_interface",
    srcs = [
        "parser_output.cc",
    ],
    hdrs = [
        "parser_interface.h",
        "parser_output.h",
        "pg_arena.h",
    ],
    visibility = ["//:__subpackages__"],
    deps = [
        ":memory_reservation_manager",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/base:status",
    ],
)

cc_library(
    name = "abstract_parser",
    srcs = ["abstract_parser.cc"],
    hdrs = ["abstract_parser.h"],
    deps = [
        ":parser_interface",
        "//third_party/spanner_pg/shims:memory_context_pg_arena",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "parser_without_serialization_only",
    srcs = ["parser_without_serialization.cc"],
    hdrs = [
        "parser_without_serialization.h",
    ],
    visibility = [
        "//:__subpackages__",  # Scheuklappen: keep
    ],
    deps = [
        ":abstract_parser",
        ":parser_interface",
        "//third_party/spanner_pg/shims:error_shim",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "parser_without_serialization_test",
    srcs = ["parser_without_serialization_test.cc"],
    deps = [
        ":implementations_without_serialization",
        ":parser_interface",
        ":parser_without_serialization_only",
        "//third_party/spanner_pg/shims:memory_context_pg_arena",
        "//third_party/spanner_pg/shims:memory_reservation_holder",
        "//third_party/spanner_pg/shims:stub_memory_reservation_manager",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_library(
    name = "implementations_without_serialization",
    testonly = 1,
    visibility = [
        "//:__subpackages__",  # Scheuklappen: keep
    ],
    deps = [
        ":implementations_without_parser",
        ":parser_without_serialization_only",
    ],
    alwayslink = 1,  # for REGISTER_MODULE_INITIALIZER
)

cc_library(
    name = "spangres_translator_test_wrapper",
    testonly = 1,
    hdrs = ["spangres_translator_test_wrapper.h"],
    visibility = ["//:__subpackages__"],
    deps = [
        ":interfaces_with_stub_implementations",
        ":memory_reservation_manager",
        ":parser_interface",
        "//third_party/spanner_pg/shims:stub_memory_reservation_manager",
        "//third_party/spanner_pg/test_catalog",
        "//third_party/spanner_pg/test_catalog:spanner_test_catalog",
        "@com_google_absl//absl/time",
        "@com_google_zetasql//zetasql/base:logging",
        "@com_google_zetasql//zetasql/public:catalog",
    ],
)

cc_test(
    name = "stub_spangres_translator_test",
    srcs = ["stub_spangres_translator_test.cc"],
    deps = [
        ":interfaces_with_stub_implementations",
        ":parser_without_serialization_only",  # buildcleaner: keep
        ":spangres_translator_test_wrapper",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "stub_builtin_function_catalog_test",
    srcs = ["stub_builtin_function_catalog_test.cc"],
    deps = [
        ":interfaces_with_stub_implementations",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:analyzer",
        "@com_google_zetasql//zetasql/public:function",
    ],
)

cc_library(
    name = "sql_builder",
    hdrs = ["sql_builder.h"],
    deps = [
        "@com_google_zetasql//zetasql/public:strings",
        "@com_google_zetasql//zetasql/resolved_ast:sql_builder",
    ],
)

cc_library(
    name = "pg_evaluators",
    hdrs = [
        "cast_evaluators.h",
        "datetime_evaluators.h",
        "formatting_evaluators.h",
        "jsonb_evaluators.h",
        "mathematical_evaluators.h",
        "pg_arena.h",
        "pg_arena_factory.h",
        "pg_timezone.h",
        "regexp_evaluators.h",
    ],
    visibility = ["//:__subpackages__"],
    deps = [
        ":memory_reservation_manager",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/time",
        "@com_google_zetasql//zetasql/public:interval_value",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_library(
    name = "pg_evaluators_stubs",
    srcs = [
        "stub_cast_evaluators.cc",
        "stub_datetime_evaluators.cc",
        "stub_formatting_evaluators.cc",
        "stub_jsonb_evaluators.cc",
        "stub_mathematical_evaluators.cc",
        "stub_pg_arena_factory.cc",
        "stub_pg_timezone.cc",
        "stub_regexp_evaluators.cc",
    ],
    deps = [
        ":pg_evaluators",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/time",
        "@com_google_zetasql//zetasql/public:interval_value",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "stub_datetime_evaluators_test",
    srcs = [
        "stub_datetime_evaluators_test.cc",
    ],
    deps = [
        ":pg_evaluators",
        ":pg_evaluators_stubs",  # buildcleaner: keep
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:interval_value",
    ],
)

cc_test(
    name = "stub_formatting_evaluators_test",
    srcs = [
        "stub_formatting_evaluators_test.cc",
    ],
    deps = [
        ":pg_evaluators",
        ":pg_evaluators_stubs",  # buildcleaner: keep
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "stub_jsonb_evaluators_test",
    srcs = [
        "stub_jsonb_evaluators_test.cc",
    ],
    deps = [
        ":pg_evaluators",
        ":pg_evaluators_stubs",  # buildcleaner: keep
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "stub_mathematical_evaluators_test",
    srcs = [
        "stub_mathematical_evaluators_test.cc",
    ],
    deps = [
        ":pg_evaluators",
        ":pg_evaluators_stubs",  # buildcleaner: keep
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "stub_pg_timezone_test",
    srcs = [
        "stub_pg_timezone_test.cc",
    ],
    deps = [
        ":pg_evaluators",
        ":pg_evaluators_stubs",  # buildcleaner: keep
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

cc_test(
    name = "stub_regexp_evaluators_test",
    srcs = [
        "stub_regexp_evaluators_test.cc",
    ],
    deps = [
        ":pg_evaluators",
        ":pg_evaluators_stubs",  # buildcleaner: keep
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
    ],
)

proto_library(
    name = "bootstrap_catalog_data",
    srcs = ["bootstrap_catalog_data.proto"],
)

cc_proto_library(
    name = "bootstrap_catalog_data_cc_proto",
    visibility = [
        "//:__subpackages__",
    ],
    deps = [":bootstrap_catalog_data"],
)

cc_library(
    name = "bootstrap_catalog_accessor",
    hdrs = [
        "bootstrap_catalog_accessor.h",
    ],
    visibility = [
        "//backend/query:__subpackages__",
        "//tests/conformance/common:__subpackages__",
        "//third_party/spanner_pg/bootstrap_catalog:__subpackages__",
        "//third_party/spanner_pg/catalog:__subpackages__",
    ],
    deps = [
        ":bootstrap_catalog_data_cc_proto",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "emulator_builtin_function_catalog",
    hdrs = [
        "emulator_builtin_function_catalog.h",
        "engine_builtin_function_catalog.h",
    ],
    deps = [
        "//backend/query:function_catalog",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:language_options",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_library(
    name = "emulator_parser",
    srcs = ["emulator_parser.cc"],
    hdrs = ["emulator_parser.h"],
    deps = [
        ":emulator_builtin_function_catalog",
        ":implementations_without_parser",
        ":interfaces_with_stub_implementations",
        ":parser_interface",
        "//backend/query:function_catalog",
        "//third_party/spanner_pg/bootstrap_catalog:bootstrap_catalog_accessor_impl",
        "//third_party/spanner_pg/shims:error_shim",
        "//third_party/spanner_pg/src/backend:backend_with_shims",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/public:analyzer",
        "@com_google_zetasql//zetasql/public:catalog",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:type",
    ],
)

cc_test(
    name = "emulator_parser_test",
    srcs = ["emulator_parser_test.cc"],
    deps = [
        ":emulator_parser",
        ":pg_arena_factory",
        "//third_party/spanner_pg/shims:memory_context_pg_arena",
        "//third_party/spanner_pg/test_catalog:emulator_catalog",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:simple_catalog",
    ],
)

cc_library(
    name = "catalog_wrappers",
    hdrs = ["catalog_wrappers.h"],
    deps = [
        "//third_party/spanner_pg/catalog:table_name",
        "//third_party/spanner_pg/postgres_includes",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_library(
    name = "stub_catalog_wrappers",
    srcs = ["stub_catalog_wrappers.cc"],
    deps = [
        ":catalog_wrappers",
        "//third_party/spanner_pg/postgres_includes",
    ],
)

# THIS LIBRARY CAN THROW EXCEPTIONS!
# Specifically, the "spangres_throw_exception()" method will always throw.
# (Other methods exposed by this library will not throw.)
# Callers must be prepared to handle or re-throw the exception.
cc_library(
    name = "ereport",
    hdrs = ["ereport.h"],
    copts = [
        # The ereport module reimplements PostgreSQL's ereport() in terms of
        # C++ exceptions, on "lesser of two evils" grounds.
        # See docstrings in `ereport.h` for details.
        # This means that all of the shims (which may have exceptions pass through them)
        # need to be compiled with -fexceptions or risk missing destructors (and causing
        # TSAN OOMs).
        "-fexceptions",
    ],
    features = ["-use_header_modules"],
    deps = [
        "//third_party/spanner_pg/postgres_includes",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/base:die_if_null",
    ],
)
