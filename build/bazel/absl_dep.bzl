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

"""Loads absl dependency needed to compile the OSS version of emulator."""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def absl_dep():
    http_archive(
        name = "com_google_absl",
        strip_prefix = "abseil-cpp-a50ae369a30f99f79d7559002aba3413dac1bd48",
        url = "https://github.com/abseil/abseil-cpp/archive/a50ae369a30f99f79d7559002aba3413dac1bd48.tar.gz",
        sha256 = "be2a9d7ea7ee15f9317b57beff37e8ffb67418fb0df64592366b04c8618c2584",
    )
