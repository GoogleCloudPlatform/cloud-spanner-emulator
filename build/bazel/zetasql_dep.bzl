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
        url = "https://github.com/google/zetasql/archive/2022.02.1.tar.gz",
        strip_prefix = "zetasql-2022.02.1",
        # Patches applied:
        # - Give visibility to ZetaSQL's base library to reuse some utilities
        patches = ["//build/bazel:zetasql.patch"],
        sha256 = "8617dfc6ea01bb09a550bb61415a3619fc19f10698c7e42c34e1caff578bdfae",
    )
