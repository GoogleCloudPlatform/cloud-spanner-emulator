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
        "check_constraint_read_write.cc",
        "column_constraints.cc",
        "commit_timestamps.cc",
        "delete_range.cc",
        "dml.cc",
        "dml_replay.cc",
        "foreign_key_transactions.cc",
        "foreign_key_verifiers.cc",
        "generated_column_read_write.cc",
        "index_backfill.cc",
        "index_read_write.cc",
        "information_schema.cc",
        "interleaving.cc",
        "large_reads.cc",
        "limits.cc",
        "malformed_writes.cc",
        "multi_row_writes.cc",
        "operations.cc",
        "parameters.cc",
        "partition_query.cc",
        "partition_reads.cc",
        "partitioned_dml.cc",
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
        "tablesample.cc",
        "thread_safety.cc",
        "transaction_errors.cc",
        "transactions.cc",
        "values.cc",
    ],
    data = [
        "//tests/conformance/data/schema_changes:test_files",
    ],
    deps = [
        "//tests/common:file_based_test_runner",
        "//tests/common:proto_matchers",
        "//tests/common:scoped_feature_flags_setter",
        "//tests/conformance/common:database_test_base",
        "@com_github_googleapis_google_cloud_cpp//:spanner",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
        "@com_google_googleapis//google/spanner/v1:spanner_cc_grpc",
        "@com_google_googletest//:gtest",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_zetasql//zetasql/base:statusor",
        "@com_google_zetasql//zetasql/public:value",
    ],
    alwayslink = 1,
)
