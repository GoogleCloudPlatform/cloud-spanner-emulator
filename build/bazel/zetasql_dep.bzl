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

"""Loads ZetaSQL dependency needed to compile the OSS version of emulator."""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def zetasql_dep():
    http_archive(
        name = "com_google_zetasql",
        url = "https://github.com/google/zetasql/archive/dd883180de6387ad80bcf7534b5aae8191e66621.tar.gz",
        strip_prefix = "zetasql-dd883180de6387ad80bcf7534b5aae8191e66621",
        # Patches applied:
        # - Give visibility to ZetaSQL's base library to reuse some utilities
        patches = ["//build/bazel:zetasql.patch"],
        sha256 = "9141d9755d1f91ff4d1c5a73087255bd9c5eb020e46109db92022906c77d760c",
    )
