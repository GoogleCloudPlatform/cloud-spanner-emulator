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
    name = "schema_validators",
    srcs = ["column_validator.cc"],
    hdrs = ["column_validator.h"],
    deps = [
        "//backend/datamodel:types",
        "//backend/schema/backfills:schema_backfillers",
        "//backend/schema/catalog:schema",
        "//backend/schema/updater:schema_validation_context",
        "//backend/schema/verifiers:column_value_verifiers",
        "//common:errors",
        "//common:limits",
        "@com_google_absl//absl/strings",
        "@com_google_zetasql//zetasql/base:status",
        "@com_google_zetasql//zetasql/public:options_cc_proto",
        "@com_google_zetasql//zetasql/public:type_cc_proto",
    ],
)
