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

proto_library(
    name = "partition_token_proto",
    srcs = ["partition_token.proto"],
    deps = [
        "@com_google_googleapis//google/spanner/v1:spanner_proto",
        "@com_google_protobuf//:struct_proto",
    ],
)

cc_proto_library(
    name = "partition_token_cc_proto",
    deps = [":partition_token_proto"],
)
