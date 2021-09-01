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

"""Loads google cloud cpp dependency needed to compile the OSS version of emulator."""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def google_cloud_cpp_dep():
    http_archive(
        name = "com_github_googleapis_google_cloud_cpp",
        url = "https://github.com/googleapis/google-cloud-cpp/archive/v1.26.0.tar.gz",
        strip_prefix = "google-cloud-cpp-1.26.0",
        sha256 = "b61ced7ae18cb4a296377fcba17683319c06c7626c889be9acb4f44e21568f3c",
    )
