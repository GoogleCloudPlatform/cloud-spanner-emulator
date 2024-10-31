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

licenses(["unencumbered"])

cc_library(
    name = "print_ddl",
    srcs = [
        "print_ddl.cc",
    ],
    hdrs = [
        "print_ddl.h",
    ],
    deps = [
        "//backend/common:utils",
        "//backend/schema/catalog:proto_bundle",
        "//backend/schema/catalog:schema",
        "//backend/schema/ddl:operations_cc_proto",
        "//backend/schema/graph:schema_node",
        "//backend/schema/parser:ddl_parser",
        "//backend/schema/parser:ddl_reserved_words",  # buildcleaner: keep
        "//third_party/spanner_pg/ddl:spangres_direct_schema_printer_impl",
        "//third_party/spanner_pg/ddl:spangres_schema_printer",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
        "@com_google_zetasql//zetasql/public:function",
        "@com_google_zetasql//zetasql/public:options_cc_proto",
        "@com_google_zetasql//zetasql/public:strings",
        "@com_google_zetasql//zetasql/public:type",
    ],
)
