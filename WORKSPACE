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

################################################################################
# Bazel utility rules                                                          #
################################################################################
_rules_pkg_version = "0.8.0"

http_archive(
    name = "rules_pkg",
    sha256 = "eea0f59c28a9241156a47d7a8e32db9122f3d50b505fae0f33de6ce4d9b61834",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_pkg/releases/download/{0}/rules_pkg-{0}.tar.gz".format(_rules_pkg_version),
        "https://github.com/bazelbuild/rules_pkg/releases/download/{0}/rules_pkg-{0}.tar.gz".format(_rules_pkg_version),
    ],
)

load("@rules_pkg//:deps.bzl", "rules_pkg_dependencies")

rules_pkg_dependencies()

################################################################################
# Google APIs protos                                                           #
################################################################################
http_archive(
    name = "com_google_googleapis",
    build_file = "@//build/bazel:googleapis.BUILD",
    patch_args = ["-p1"],
    patches = ["//build/bazel:googleapis.patch"],
    sha256 = "dcafbafe1455816bd3c208041f024ea07bc843efa6181412cb0dbbee7222578b",
    strip_prefix = "googleapis-9f7c0ffdaa8ceb2f27982bad713a03306157a4d2",
    url = "https://github.com/googleapis/googleapis/archive/9f7c0ffdaa8ceb2f27982bad713a03306157a4d2.tar.gz",
)

load("@com_google_googleapis//:repository_rules.bzl", "switched_rules_by_language")

switched_rules_by_language(
    name = "com_google_googleapis_imports",
    cc = True,
    go = True,
    grpc = True,
)

################################################################################
# Go Build Support                                                             #
################################################################################
_rules_go_version = "v0.37.0"

http_archive(
    name = "io_bazel_rules_go",
    sha256 = "56d8c5a5c91e1af73eca71a6fab2ced959b67c86d12ba37feedb0a2dfea441a6",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/{0}/rules_go-{0}.zip".format(_rules_go_version),
        "https://github.com/bazelbuild/rules_go/releases/download/{0}/rules_go-{0}.zip.format(_rules_go_version)",
    ],
)

load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains", "go_rules_dependencies")

go_register_toolchains(version = "1.19.4")

_bazel_gazelle_version = "0.28.0"

http_archive(
    name = "bazel_gazelle",
    sha256 = "448e37e0dbf61d6fa8f00aaa12d191745e14f07c31cabfa731f0c8e8a4f41b97",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-gazelle/releases/download/v{0}/bazel-gazelle-v{0}.tar.gz".format(_bazel_gazelle_version),
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/v{0}/bazel-gazelle-v{0}.tar.gz".format(_bazel_gazelle_version),
    ],
)

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies", "go_repository")

gazelle_dependencies()

################################################################################
# GRPC Gateway                                                                 #
################################################################################

go_repository(
    name = "org_golang_google_grpc",
    build_file_proto_mode = "disable_global",
    importpath = "google.golang.org/grpc",
    sum = "h1:EhTqbhiYeixwWQtAEZAxmV9MGqcjEU2mFx52xCzNyag=",
    version = "v1.54.0",
)

go_repository(
    name = "grpc_ecosystem_grpc_gateway",
    importpath = "github.com/grpc-ecosystem/grpc-gateway/v2",
    sum = "h1:1JYBfzqrWPcCclBwxFCPAou9n+q86mfnu7NAeHfte7A=",
    version = "v2.15.0",
    build_file_proto_mode = "disable_global",
)

go_repository(
    name = "org_golang_google_genproto",
    build_file_proto_mode = "disable_global",
    importpath = "google.golang.org/genproto",
    sum = "h1:Kd6tRRHXw8z4TlPlWi+NaK10gsePL6GdZBQChptOLGA=",
    version = "v0.0.0-20230327215041-6ac7f18bb9d5",
)

go_repository(
    name = "org_golang_google_protobuf",
    build_file_proto_mode = "disable_global",
    importpath = "google.golang.org/protobuf",
    patch_args = ["-p1"],
    patches = ["//build/bazel:golang_protobuf.patch"],
    sum = "h1:kPPoIgf3TsEvrm0PFe15JQ+570QVxYzEvvHqChK+cng=",
    version = "v1.30.0",
)

go_rules_dependencies()

go_repository(
    name = "com_github_golang_protobuf",
    build_file_proto_mode = "disable_global",
    importpath = "github.com/golang/protobuf",
    sum = "h1:KhyjKVUg7Usr/dYsdSqoFveMYd5ko72D+zANwlG1mmg=",
    version = "v1.5.3",
)

go_repository(
    name = "in_gopkg_yaml_v3",
    build_file_proto_mode = "disable_global",
    importpath = "gopkg.in/yaml.v3",
    sum = "h1:fxVm/GzAzEWqLHuvctI91KS9hhNmmWOoWu0XTYJS7CA=",
    version = "v3.0.1",
)

go_repository(
    name = "com_github_golang_glog",
    build_file_proto_mode = "disable_global",
    importpath = "github.com/golang/glog",
    sum = "h1:nfP3RFugxnNRyKgeWd4oI1nYvXpxrx8ck8ZrcizshdQ=",
    version = "v1.0.0",
)

go_repository(
    name = "org_golang_x_net",
    build_file_proto_mode = "disable_global",
    importpath = "golang.org/x/net",
    sum = "h1:Zrh2ngAOFYneWTAIAPethzeaQLuHwhuBkuV6ZiRnUaQ=",
    version = "v0.8.0",
)

go_repository(
    name = "org_golang_x_sys",
    build_file_proto_mode = "disable_global",
    importpath = "golang.org/x/sys",
    sum = "h1:MVltZSvRTcU2ljQOhs94SXPftV6DCNnZViHeQps87pQ=",
    version = "v0.6.0",
)

go_repository(
    name = "org_golang_x_text",
    build_file_proto_mode = "disable_global",
    importpath = "golang.org/x/text",
    sum = "h1:57P1ETyNKtuIjB4SRd15iJxuhj8Gc416Y78H3qgMh68=",
    version = "v0.8.0",
)

################################################################################
# C++ Libraries                                                                #
################################################################################

http_archive(
    name = "com_googlesource_code_re2",
    sha256 = "ac855fb93dfa6878f88bc1c399b9a2743fdfcb3dc24b94ea9a568a1c990b1212",
    strip_prefix = "re2-d1394506654e0a19a92f3d8921e26f7c3f4de969",
    url = "https://github.com/google/re2/archive/d1394506654e0a19a92f3d8921e26f7c3f4de969.tar.gz",
)

http_archive(
    name = "com_googlesource_code_riegeli",
    sha256 = "218dbf85957bd8f66f0f02770d26181b7368716b3d7503252ac21f75633ba62e",
    strip_prefix = "riegeli-0d1dd32ae5a78120e45111f24d06fec22cb98480",
    url = "https://github.com/google/riegeli/archive/0d1dd32ae5a78120e45111f24d06fec22cb98480.tar.gz",
)

http_archive(
    name = "com_google_protobuf",
    sha256 = "9c0fd39c7a08dff543c643f0f4baf081988129a411b977a07c46221793605638",
    strip_prefix = "protobuf-3.20.3",
    url = "https://github.com/protocolbuffers/protobuf/archive/v3.20.3.tar.gz",
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

http_archive(
    name = "com_google_absl",
    sha256 = "61d0af0262a0131bb8917fcb883e5e831ee5ad1535433f2f13f85906d1607f81",
    strip_prefix = "abseil-cpp-20230125.1",
    url = "https://github.com/abseil/abseil-cpp/archive/refs/tags/20230125.1.zip",
)

http_archive(
    name = "com_google_googletest",
    sha256 = "81964fe578e9bd7c94dfdb09c8e4d6e6759e19967e397dbea48d1c10e45d0df2",
    strip_prefix = "googletest-release-1.12.1",
    urls = ["https://github.com/google/googletest/archive/release-1.12.1.tar.gz"],
)

http_archive(
    name = "com_github_grpc_grpc",
    # Patches applied:
    # - Adding implicit conversion between grpc::Status and absl::Status
    patches = ["//build/bazel:grpc.patch"],
    sha256 = "9647220c699cea4dafa92ec0917c25c7812be51a18143af047e20f3fb05adddc",
    strip_prefix = "grpc-1.43.0",
    urls = ["https://github.com/grpc/grpc/archive/v1.43.0.tar.gz"],
)

load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")

grpc_deps()

################################################################################
# ZetaSQL                                                                      #
################################################################################

http_archive(
    name = "com_google_zetasql",
    # Patches applied:
    # - Give visibility to ZetaSQL's base library to reuse some utilities
    patches = ["//build/bazel:zetasql.patch"],
    sha256 = "9aa01afab8defc7a30b157a5c1dd594b1291bb85e3bfacc9f765b4bc1479dd25",
    strip_prefix = "zetasql-f764f4e986ac1516ab5ae95e6d6ce2f4416cc6ff",
    url = "https://github.com/google/zetasql/archive/f764f4e986ac1516ab5ae95e6d6ce2f4416cc6ff.tar.gz",
)

http_archive(
    name = "rules_jvm_external",
    sha256 = "b17d7388feb9bfa7f2fa09031b32707df529f26c91ab9e5d909eb1676badd9a6",
    strip_prefix = "rules_jvm_external-4.5",
    urls = ["https://github.com/bazelbuild/rules_jvm_external/archive/4.5.zip"],
)

# gRPC Java
http_archive(
    name = "io_grpc_grpc_java",
    sha256 = "b6cfc524647cc680e66989ab22a10b66dc5de8c6d8499f91a7e633634c594c61",
    strip_prefix = "grpc-java-1.51.1",
    url = "https://github.com/grpc/grpc-java/archive/v1.51.1.tar.gz",
)

load("@rules_jvm_external//:repositories.bzl", "rules_jvm_external_deps")

rules_jvm_external_deps()

load("@rules_jvm_external//:setup.bzl", "rules_jvm_external_setup")

rules_jvm_external_setup()

load("@rules_jvm_external//:defs.bzl", "maven_install")

maven_install(
    artifacts = [
        "net.java.dev.javacc:javacc:jar:6.1.2",
    ],
    maven_install_json = "@//:maven_install.json",
    repositories = [
        "https://repo1.maven.org/maven2",
    ],
)

load("@maven//:defs.bzl", "pinned_maven_install")

pinned_maven_install()

load("@com_google_zetasql//bazel:zetasql_java_deps.bzl", "zetasql_java_deps")

zetasql_java_deps()

# If java support is not required, copy starting from here
load("@com_google_zetasql//bazel:zetasql_deps_step_1.bzl", "zetasql_deps_step_1")

zetasql_deps_step_1()

load("@com_google_zetasql//bazel:zetasql_deps_step_2.bzl", "zetasql_deps_step_2")

zetasql_deps_step_2()

load("@com_google_zetasql//bazel:zetasql_deps_step_3.bzl", "zetasql_deps_step_3")

zetasql_deps_step_3()

# Required only for java builds
load("@io_grpc_grpc_java//:repositories.bzl", "grpc_java_repositories")

grpc_java_repositories()

load("@com_google_zetasql//bazel:zetasql_deps_step_4.bzl", "zetasql_deps_step_4")

zetasql_deps_step_4()

http_archive(
    name = "com_github_googleapis_google_cloud_cpp",
    sha256 = "e8d904bbff788a26aa9cd67d6c0725f9798448fcf73ab809ec2d7b80f89a1dc5",
    strip_prefix = "google-cloud-cpp-2.2.0",
    url = "https://github.com/googleapis/google-cloud-cpp/archive/v2.2.0.tar.gz",
)

load("@com_github_googleapis_google_cloud_cpp//bazel:google_cloud_cpp_deps.bzl", "google_cloud_cpp_deps")

google_cloud_cpp_deps()

################################################################################
# Python Libraries                                                             #
################################################################################

http_archive(
    name = "org_python_pypi_portpicker",
    build_file = "//build/bazel:portpicker.BUILD",
    sha256 = "c55683ad725f5c00a41bc7db0225223e8be024b1fa564d039ed3390e4fd48fb3",
    strip_prefix = "portpicker-1.5.2/src",
    urls = [
        "https://files.pythonhosted.org/packages/3b/34/bfbd5236c7726452080a92a9f6ea9770cd65f51b05cef319ccb767ed32bf/portpicker-1.5.2.tar.gz",
    ],
)
