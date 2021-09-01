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

workspace(name = "com_google_cloud_spanner_emulator")

################################################################################
# Generic Bazel Support                                                        #
################################################################################

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "bazel_skylib",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-skylib/releases/download/1.0.2/bazel-skylib-1.0.2.tar.gz",
        "https://github.com/bazelbuild/bazel-skylib/releases/download/1.0.2/bazel-skylib-1.0.2.tar.gz",
    ],
    sha256 = "97e70364e9249702246c0e9444bccdc4b847bed1eb03c5a3ece4f83dfe6abc44",
)

load("@bazel_skylib//:workspace.bzl", "bazel_skylib_workspace")

bazel_skylib_workspace()

################################################################################
# Bazel utility rules                                                          #
################################################################################

http_archive(
    name = "rules_pkg",
    url = "https://github.com/bazelbuild/rules_pkg/releases/download/0.2.4/rules_pkg-0.2.4.tar.gz",
    sha256 = "4ba8f4ab0ff85f2484287ab06c0d871dcb31cc54d439457d28fd4ae14b18450a",
)

load("@rules_pkg//:deps.bzl", "rules_pkg_dependencies")
rules_pkg_dependencies()

http_archive(
    name = "com_github_bazelbuild_buildtools",
    strip_prefix = "buildtools-bf564b4925ab5876a3f64d8b90fab7f769013d42",
    url = "https://github.com/bazelbuild/buildtools/archive/bf564b4925ab5876a3f64d8b90fab7f769013d42.zip",
    sha256 = "b5d7dbc6832f11b6468328a376de05959a1a9e4e9f5622499d3bab509c26b46a"
)

load("@com_github_bazelbuild_buildtools//buildifier:deps.bzl", "buildifier_dependencies")

buildifier_dependencies()

################################################################################
# Google APIs protos                                                           #
################################################################################

load("//build/bazel:googleapis_dep.bzl", "googleapis_dep")

googleapis_dep()

load("@com_google_googleapis//:repository_rules.bzl", "switched_rules_by_language")

switched_rules_by_language(
    name = "com_google_googleapis_imports",
    cc = True,
    go = True,
    grpc = True,
)

################################################################################
# C++ Libraries                                                                #
################################################################################

http_archive(
    name = "com_googlesource_code_re2",
    strip_prefix = "re2-d1394506654e0a19a92f3d8921e26f7c3f4de969",
    url = "https://github.com/google/re2/archive/d1394506654e0a19a92f3d8921e26f7c3f4de969.tar.gz",
    sha256 = "ac855fb93dfa6878f88bc1c399b9a2743fdfcb3dc24b94ea9a568a1c990b1212",
)

http_archive(
    name = "com_google_protobuf",
    strip_prefix = "protobuf-3.11.2",
    url = "https://github.com/protocolbuffers/protobuf/archive/v3.11.2.tar.gz",
    sha256 = "e8c7601439dbd4489fe5069c33d374804990a56c2f710e00227ee5d8fd650e67"
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

load("//build/bazel:absl_dep.bzl", "absl_dep")

absl_dep()

load("//build/bazel:googletest_dep.bzl", "googletest_dep")

googletest_dep()

http_archive(
    name = "com_github_grpc_grpc",
    urls = ["https://github.com/grpc/grpc/archive/v1.26.0.tar.gz"],
    strip_prefix = "grpc-1.26.0",
    # Patches applied:
    # - Adding implicit conversion between grpc::Status and absl::Status
    patches = ["//build/bazel:grpc.patch"],
    sha256 = "2fcb7f1ab160d6fd3aaade64520be3e5446fc4c6fa7ba6581afdc4e26094bd81",
)

load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")

grpc_deps()

load("//build/bazel:zetasql_dep.bzl", "zetasql_dep")

zetasql_dep()

load("@com_google_zetasql//bazel:zetasql_deps_step_1.bzl", "zetasql_deps_step_1")

zetasql_deps_step_1()

load("@com_google_zetasql//bazel:zetasql_deps_step_2.bzl", "zetasql_deps_step_2")

zetasql_deps_step_2()

load("@com_google_zetasql//bazel:zetasql_deps_step_3.bzl", "zetasql_deps_step_3")

zetasql_deps_step_3()

load("@com_google_zetasql//bazel:zetasql_deps_step_4.bzl", "zetasql_deps_step_4")

zetasql_deps_step_4()

load("//build/bazel:google_cloud_cpp_dep.bzl", "google_cloud_cpp_dep")

google_cloud_cpp_dep()

load("@com_github_googleapis_google_cloud_cpp//bazel:google_cloud_cpp_deps.bzl", "google_cloud_cpp_deps")

google_cloud_cpp_deps()

################################################################################
# Go Build Support                                                             #
################################################################################

http_archive(
    name = "io_bazel_rules_go",
    urls = [
        "https://storage.googleapis.com/bazel-mirror/github.com/bazelbuild/rules_go/releases/download/v0.20.3/rules_go-v0.20.3.tar.gz",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.20.3/rules_go-v0.20.3.tar.gz",
    ],
    sha256 = "e88471aea3a3a4f19ec1310a55ba94772d087e9ce46e41ae38ecebe17935de7b",
)


load("@io_bazel_rules_go//go:deps.bzl", "go_rules_dependencies", "go_register_toolchains")

go_rules_dependencies()

go_register_toolchains()

http_archive(
    name = "bazel_gazelle",
    urls = [
        "https://storage.googleapis.com/bazel-mirror/github.com/bazelbuild/bazel-gazelle/releases/download/v0.19.1/bazel-gazelle-v0.19.1.tar.gz",
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.19.1/bazel-gazelle-v0.19.1.tar.gz",
    ],
    sha256 = "86c6d481b3f7aedc1d60c1c211c6f76da282ae197c3b3160f54bd3a8f847896f",
)

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies", "go_repository")

gazelle_dependencies()

################################################################################
# GRPC Gateway                                                                 #
################################################################################

http_archive(
    name = "grpc_ecosystem_grpc_gateway",
    strip_prefix="grpc-gateway-1.12.1",
    urls = ["https://github.com/grpc-ecosystem/grpc-gateway/archive/v1.12.1.zip"],
    sha256 = "7566d466d1b7319b73a13395def529ce5a04f9298e1c8fabbc151dd89987ce58",
)

go_repository(
    name = "com_github_golang_protobuf",
    tag="v1.3.0",
    importpath = "github.com/golang/protobuf"
)

go_repository(
        name = "in_gopkg_yaml_v2",
        importpath = "gopkg.in/yaml.v2",
        sum = "h1:+t9dhfO+GNOIGJof6kPOAenx7YgrZMTdRPV+EsnPabk=",
        version = "v2.0.0-20170812160011-eb3733d160e7",
)

go_repository(
        name = "com_github_ghodss_yaml",
        importpath = "github.com/ghodss/yaml",
        sum = "h1:wQHKEahhL6wmXdzwWG11gIVCkOv05bNOh+Rxn0yngAk=",
        version = "v1.0.0",
)

go_repository(
        name = "com_github_golang_glog",
        importpath = "github.com/golang/glog",
        sum = "h1:VKtxabqXZkF25pY9ekfRL6a582T4P37/31XEstQ5p58=",
        version = "v0.0.0-20160126235308-23def4e6c14b",
)

go_repository(
    name = "org_golang_google_grpc",
    importpath = "google.golang.org/grpc",
    sum = "h1:cfg4PD8YEdSFnm7qLV4++93WcmhH2nIUhMjhdCvl3j8=",
    version = "v1.19.0",
)

go_repository(
    name = "org_golang_x_net",
    importpath = "golang.org/x/net",
    sum = "h1:eH6Eip3UpmR+yM/qI9Ijluzb1bNv/cAU/n+6l8tRSis=",
    version = "v0.0.0-20181220203305-927f97764cc3",
)

go_repository(
    name = "org_golang_x_text",
    importpath = "golang.org/x/text",
    sum = "h1:g61tztE5qeGQ89tm6NTjjM9VPIm088od1l6aSorWRWg=",
    version = "v0.3.0",
)

################################################################################
# Python Libraries                                                             #
################################################################################

http_archive(
    name = "org_python_pypi_portpicker",
    urls = [
        "https://pypi.python.org/packages/d9/f4/0188bc07d38b5f9dd192b8329c5e098e3b23552c01a96fd08973dba9e315/portpicker-1.3.1.tar.gz",
    ],
    sha256 = "d2cdc776873635ed421315c4d22e63280042456bbfa07397817e687b142b9667",
    strip_prefix = "portpicker-1.3.1/src",
    build_file = "//build/bazel:portpicker.BUILD",
)

################################################################################
# Java Libraries                                                               #
################################################################################

RULES_JVM_EXTERNAL_TAG = "3.2"
RULES_JVM_EXTERNAL_SHA = "82262ff4223c5fda6fb7ff8bd63db8131b51b413d26eb49e3131037e79e324af"

http_archive(
    name = "rules_jvm_external",
    strip_prefix = "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
    sha256 = RULES_JVM_EXTERNAL_SHA,
    url = "https://github.com/bazelbuild/rules_jvm_external/archive/%s.zip" % RULES_JVM_EXTERNAL_TAG,
)

load("@rules_jvm_external//:defs.bzl", "maven_install")

maven_install(
    artifacts = [
        "net.java.dev.javacc:javacc:jar:6.1.2",
    ],
    repositories = [
        "https://repo1.maven.org/maven2",
    ],
)
