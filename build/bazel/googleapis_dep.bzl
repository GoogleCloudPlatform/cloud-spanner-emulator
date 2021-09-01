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

"""Loads googleapis dependency needed to compile the OSS version of emulator."""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def googleapis_dep():
    http_archive(
        name = "com_google_googleapis",
        url = "https://github.com/googleapis/googleapis/archive/299383230f69e9f2272adda8e0d93c4fdf231deb.tar.gz",
        strip_prefix = "googleapis-299383230f69e9f2272adda8e0d93c4fdf231deb",
        sha256 = "b31e408b7d50066ce4d50a8f974e37891c2b470da87f2310dc54ee41aee5c06a",
        build_file = "@//build/bazel:googleapis.BUILD",
    )
