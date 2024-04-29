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
_rules_pkg_version = "0.9.0"

http_archive(
    name = "rules_pkg",
    sha256 = "335632735e625d408870ec3e361e192e99ef7462315caa887417f4d88c4c8fb8",
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
    sha256 = "d10709f46a9936ac4eab1fdf37b0688d064c4c43e1b205ce5ac9e6e0caa6b9dc",
    strip_prefix = "googleapis-9a9bc9b427e4516f79b9753c4fad91d425334755",
    url = "https://github.com/googleapis/googleapis/archive/9a9bc9b427e4516f79b9753c4fad91d425334755.tar.gz",
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
_rules_go_version = "v0.39.1"

http_archive(
    name = "io_bazel_rules_go",
    sha256 = "6dc2da7ab4cf5d7bfc7c949776b1b7c733f05e56edc4bcd9022bb249d2e2a996",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/{0}/rules_go-{0}.zip".format(_rules_go_version),
        "https://github.com/bazelbuild/rules_go/releases/download/{0}/rules_go-{0}.zip.format(_rules_go_version)",
    ],
)

load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains", "go_rules_dependencies")

go_register_toolchains(version = "1.20.13")

_bazel_gazelle_version = "0.31.1"

http_archive(
    name = "bazel_gazelle",
    sha256 = "b8b6d75de6e4bf7c41b7737b183523085f56283f6db929b86c5e7e1f09cf59c9",
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
    build_file_proto_mode = "disable_global",
    importpath = "github.com/grpc-ecosystem/grpc-gateway/v2",
    sum = "h1:1JYBfzqrWPcCclBwxFCPAou9n+q86mfnu7NAeHfte7A=",
    version = "v2.15.0",
)

go_repository(
    name = "org_golang_google_genproto",
    build_file_proto_mode = "disable_global",
    importpath = "google.golang.org/genproto",
    sum = "h1:KpwkzHKEF7B9Zxg18WzOa7djJ+Ha5DzthMyZYQfEn2A=",
    version = "v0.0.0-20230410155749-daa745c078e1",
)

go_repository(
    name = "org_golang_google_protobuf",
    build_file_proto_mode = "disable_global",
    importpath = "google.golang.org/protobuf",
    patch_args = ["-p1"],
    patches = ["//build/bazel:golang_protobuf.patch"],
    sum = "h1:g0LDEJHgrBl9N9r17Ru3sqWhkIx2NB67okBHPwC7hs8=",
    version = "v1.31.0",
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
    sum = "h1:/d3pCKDPWNnvIWe0vVUpNP32qc8U3PDVxySP/y360qE=",
    version = "v1.1.0",
)

go_repository(
    name = "org_golang_x_net",
    build_file_proto_mode = "disable_global",
    importpath = "golang.org/x/net",
    sum = "h1:ugBLEUaxABaB5AJqW9enI0ACdci2RUd4eP51NTBvuJ8=",
    version = "v0.15.0",
)

go_repository(
    name = "org_golang_x_sys",
    build_file_proto_mode = "disable_global",
    importpath = "golang.org/x/sys",
    sum = "h1:CM0HF96J0hcLAwsHPJZjfdNzs0gftsLfgKt57wWHJ0o=",
    version = "v0.12.0",
)

go_repository(
    name = "org_golang_x_text",
    build_file_proto_mode = "disable_global",
    importpath = "golang.org/x/text",
    sum = "h1:ablQoSUd0tRdKxZewP80B+BaqeKJuVhuRxj/dkrun3k=",
    version = "v0.13.0",
)

################################################################################
# C++ Libraries                                                                #
################################################################################

http_archive(
    name = "com_googlesource_code_re2",
    sha256 = "ef516fb84824a597c4d5d0d6d330daedb18363b5a99eda87d027e6bdd9cba299",
    strip_prefix = "re2-03da4fc0857c285e3a26782f6bc8931c4c950df4",
    url = "https://github.com/google/re2/archive/03da4fc0857c285e3a26782f6bc8931c4c950df4.tar.gz",
)

http_archive(
    name = "com_googlesource_code_riegeli",
    sha256 = "603c4d35224cf00f1d4a68c45cc4c5ca598613886886f93e1cffbe49a18aa6ea",
    strip_prefix = "riegeli-3966874f4ce0b05bb32ae184f1fb44411992e12d",
    url = "https://github.com/google/riegeli/archive/3966874f4ce0b05bb32ae184f1fb44411992e12d.tar.gz",
)

http_archive(
    name = "com_google_protobuf",
    sha256 = "21fcb4b0df6a8e6279e5843af8c9f2245919cf0d3ec2021c76fccc4fc4bf9aca",
    strip_prefix = "protobuf-4.23.3",
    url = "https://github.com/protocolbuffers/protobuf/archive/v4.23.3.tar.gz",
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

http_archive(
    name = "com_google_absl",
    sha256 = "3439843ac7d7b9cc354dd6735b6790fa7589b73429bbda77976e0db61e92f1fd",
    strip_prefix = "abseil-cpp-0697762c62cdb51ead8d9c2f0d299c5d4a4ff9db",
    url = "https://github.com/abseil/abseil-cpp/archive/0697762c62cdb51ead8d9c2f0d299c5d4a4ff9db.tar.gz",
)

http_archive(
    name = "com_google_googletest",
    sha256 = "ad7fdba11ea011c1d925b3289cf4af2c66a352e18d4c7264392fead75e919363",
    strip_prefix = "googletest-1.13.0",
    urls = ["https://github.com/google/googletest/archive/refs/tags/v1.13.0.tar.gz"],
)

http_archive(
    name = "com_github_grpc_grpc",
    # Patches applied:
    # - Adding implicit conversion between grpc::Status and absl::Status
    patch_args = ["-p1"],
    patches = ["//build/bazel:grpc.patch"],
    sha256 = "e034992a0b464042021f6d440f2090acc2422c103a322b0844e3921ccea981dc",
    strip_prefix = "grpc-1.56.0",
    urls = ["https://github.com/grpc/grpc/archive/v1.56.0.tar.gz"],
)

http_archive(
    name = "upb",
    patch_args = ["-p1"],
    patches = ["//build/bazel:upb.patch"],
    sha256 = "046b5f134523eaad9265a41a2ec0701cc45973841070af2772e3578a9f3bfed0",
    strip_prefix = "upb-0ea9f73be35e35db242ccc65aa9c87487b792324",
    urls = ["https://github.com/protocolbuffers/upb/archive/0ea9f73be35e35db242ccc65aa9c87487b792324.tar.gz"],
)

http_archive(
    name = "nlohmann_json",
    build_file_content = """
cc_library(
  name = "json",
  hdrs = glob([
    "include/nlohmann/**/*.hpp",
  ]),
  includes = ["include"],
  visibility = ["//visibility:public"],
  alwayslink = True,
)""",
    strip_prefix = "json-3.10.5",
    urls = [
        "https://github.com/nlohmann/json/archive/refs/tags/v3.10.5.tar.gz",
    ],
)

load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")

grpc_deps()

################################################################################
# ZetaSQL                                                                      #
################################################################################

http_archive(
    name = "com_google_zetasql",
    patch_args = ["-p1"],
    patches = ["//build/bazel:zetasql.patch"],
    # Patches applied:
    # - Give visibility to ZetaSQL's base library to reuse some utilities
    sha256 = "c9519f5b71f6e23b6eb4939456fe56ba1831afc2cd6a863b21d0563db0655dda",
    strip_prefix = "zetasql-b7b67f55f8792dc418959f3b60de2d1d00b33c7a",
    url = "https://github.com/google/zetasql/archive/b7b67f55f8792dc418959f3b60de2d1d00b33c7a.tar.gz",
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
    sha256 = "4af5ecbaed16455fcda9fdab36e131696f5092858dd130f026069fcf11817a21",
    strip_prefix = "grpc-java-1.56.0",
    url = "https://github.com/grpc/grpc-java/archive/v1.56.0.tar.gz",
)

load("@rules_jvm_external//:repositories.bzl", "rules_jvm_external_deps")

rules_jvm_external_deps()

load("@rules_jvm_external//:setup.bzl", "rules_jvm_external_setup")

rules_jvm_external_setup()

load("@rules_jvm_external//:defs.bzl", "maven_install")

maven_install(
    artifacts = [
        "net.java.dev.javacc:javacc:jar:7.0.3",
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

google_cloud_cpp_version = "2.12.0"

http_archive(
    name = "com_github_googleapis_google_cloud_cpp",
    sha256 = "8cda870803925c62de8716a765e03eb9d34249977e5cdb7d0d20367e997a55e2",
    strip_prefix = "google-cloud-cpp-{0}".format(google_cloud_cpp_version),
    url = "https://github.com/googleapis/google-cloud-cpp/archive/v{0}.tar.gz".format(google_cloud_cpp_version),
)

load("@com_github_googleapis_google_cloud_cpp//bazel:google_cloud_cpp_deps.bzl", "google_cloud_cpp_deps")

google_cloud_cpp_deps()

################################################################################
# Python Libraries                                                             #
################################################################################

http_archive(
    name = "org_python_pypi_portpicker",
    build_file = "//build/bazel:portpicker.BUILD",
    sha256 = "bd507fd6f96f65ee02781f2e674e9dc6c99bbfa6e3c39992e3916204c9d431fa",
    strip_prefix = "portpicker-1.6.0/src",
    urls = [
        "https://files.pythonhosted.org/packages/4d/d0/cda2fc582f09510c84cd6b7d7b9e22a02d4e45dbad2b2ef1c6edd7847e00/portpicker-1.6.0.tar.gz",
    ],
)

################################################################################
# Libraries for PostgreSQL interface                                           #
################################################################################

http_archive(
    name = "lz4",
    build_file = "//build/bazel:lz4.BUILD",
    sha256 = "0b8bf249fd54a0b974de1a50f0a13ba809a78fd48f90c465c240ee28a9e4784d",
    strip_prefix = "lz4-1.9.2/lib",
    urls = ["https://github.com/lz4/lz4/archive/refs/tags/v1.9.2.zip"],
)

http_archive(
    name = "net_zstd",
    build_file = "//build/bazel:net_zstd.BUILD",
    sha256 = "3b1c3b46e416d36931efd34663122d7f51b550c87f74de2d38249516fe7d8be5",
    strip_prefix = "zstd-1.5.6/lib",
    urls = ["https://github.com/facebook/zstd/archive/v1.5.6.zip"],
)

http_archive(
    name = "zlib",
    build_file = "//build/bazel:zlib.BUILD",
    sha256 = "c3e5e9fdd5004dcb542feda5ee4f0ff0744628baf8ed2dd5d66f8ca1197cb1a1",
    strip_prefix = "zlib-1.3.1",
    urls = ["http://zlib.net/fossils/zlib-1.3.1.tar.gz"],
)
