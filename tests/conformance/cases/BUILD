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
    name = "cases",
    testonly = 1,
    srcs = [
        "arrays.cc",
        "batch_dml.cc",
        "case_sensitivity.cc",
        "column_constraints.cc",
        "commit_timestamps.cc",
        "delete_range.cc",
        "dml.cc",
        "index_backfill.cc",
        "index_read_write.cc",
        "information_schema.cc",
        "interleaving.cc",
        "malformed_writes.cc",
        "multi_row_writes.cc",
        "primary_keys.cc",
        "query.cc",
        "query_hints.cc",
        "query_modes.cc",
        "range_delete.cc",
        "range_reads.cc",
        "read_your_writes.cc",
        "schema_changes.cc",
        "sessions.cc",
        "single_row_writes.cc",
        "singleton_tables.cc",
        "snapshot_reads.cc",
        "thread_safety.cc",
        "transactions.cc",
        "values.cc",
    ],
    data = [
        "//tests/conformance/data/schema_changes:test_files",
    ],
    deps = [
        "//tests/common:file_based_test_runner",
        "//tests/conformance/common:database_test_base",
        "@com_github_googleapis_google_cloud_cpp_spanner//google/cloud/spanner:spanner_client",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_grpc",
        "@com_google_googletest//:gtest",
        "@com_google_zetasql//zetasql/base:status",
        "@com_google_zetasql//zetasql/public:value",
    ],
    alwayslink = 1,
)
