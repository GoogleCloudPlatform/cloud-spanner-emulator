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
        url = "https://github.com/googleapis/googleapis/archive/4381242ee4d8c0694ae1188520e9ac1a72edf817.tar.gz",
        strip_prefix = "googleapis-4381242ee4d8c0694ae1188520e9ac1a72edf817",
        sha256 = "1bdcffd9cc6ebe9263beade2585a4875d2de515f97f6bb57bc6fe39ac5903984",
        build_file = "@//build/bazel:googleapis.BUILD",
    )
