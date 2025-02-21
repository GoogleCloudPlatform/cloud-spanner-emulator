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
    name = "change_stream_partition_churner",
    srcs = [
        "change_stream_partition_churner.cc",
    ],
    hdrs = [
        "change_stream_partition_churner.h",
    ],
    deps = [
        "//backend/access:read",
        "//backend/access:write",
        "//backend/datamodel:key_set",
        "//backend/schema/backfills:schema_backfillers",
        "//backend/schema/catalog:schema",
        "//backend/transaction:read_write_transaction",
        "//common:change_stream",
        "//common:clock",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_zetasql//zetasql/base:ret_check",
        "@com_google_zetasql//zetasql/public:value",
    ],
)

cc_test(
    name = "change_stream_partition_churner_test",
    size = "small",
    srcs = [
        "change_stream_partition_churner_test.cc",
    ],
    deps = [
        ":change_stream_partition_churner",
        "//backend/access:read",
        "//backend/database",
        "//backend/datamodel:key_set",
        "//backend/schema/updater:schema_updater",
        "//backend/transaction:read_only_transaction",
        "//common:clock",
        "//tests/common:proto_matchers",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
        "@com_google_zetasql//zetasql/base/testing:status_matchers",
        "@com_google_zetasql//zetasql/public:value",
    ],
)
