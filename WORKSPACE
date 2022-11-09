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
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-skylib/releases/download/1.2.1/bazel-skylib-1.2.1.tar.gz",
        "https://github.com/bazelbuild/bazel-skylib/releases/download/1.2.1/bazel-skylib-1.2.1.tar.gz",
    ],
    sha256 = "f7be3474d42aae265405a592bb7da8e171919d74c16f082a5457840f06054728",
)

load("@bazel_skylib//:workspace.bzl", "bazel_skylib_workspace")

bazel_skylib_workspace()

################################################################################
# Bazel utility rules                                                          #
################################################################################

http_archive(
    name = "rules_pkg",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_pkg/releases/download/0.7.0/rules_pkg-0.7.0.tar.gz",
        "https://github.com/bazelbuild/rules_pkg/releases/download/0.7.0/rules_pkg-0.7.0.tar.gz",
    ],
    sha256 = "8a298e832762eda1830597d64fe7db58178aa84cd5926d76d5b744d6558941c2",
)

load("@rules_pkg//:deps.bzl", "rules_pkg_dependencies")

rules_pkg_dependencies()

http_archive(
    name = "com_github_bazelbuild_buildtools",
    strip_prefix = "buildtools-c802c3b06ba674e8a76d04c0677d153ab9f660c9",
    url = "https://github.com/bazelbuild/buildtools/archive/c802c3b06ba674e8a76d04c0677d153ab9f660c9.zip",
    sha256 = "3a2d2890d6a2948adb3040ee555483eeb483f73b839376c0fac60443e9a3d5b2",
)

load("@com_github_bazelbuild_buildtools//buildifier:deps.bzl", "buildifier_dependencies")

buildifier_dependencies()

################################################################################
# Google APIs protos                                                           #
################################################################################
http_archive(
    name = "com_google_googleapis",
    url = "https://github.com/googleapis/googleapis/archive/9f7c0ffdaa8ceb2f27982bad713a03306157a4d2.tar.gz",
    strip_prefix = "googleapis-9f7c0ffdaa8ceb2f27982bad713a03306157a4d2",
    sha256 = "dcafbafe1455816bd3c208041f024ea07bc843efa6181412cb0dbbee7222578b",
    build_file = "@//build/bazel:googleapis.BUILD",
    patches = ["//build/bazel:googleapis.patch"],
    patch_args = ["-p1"],
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

http_archive(
    name = "io_bazel_rules_go",
    sha256 = "16e9fca53ed6bd4ff4ad76facc9b7b651a89db1689a2877d6fd7b82aa824e366",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.34.0/rules_go-v0.34.0.zip",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.34.0/rules_go-v0.34.0.zip",
    ],
)

load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains", "go_rules_dependencies")

go_register_toolchains(version = "1.19.2")

http_archive(
    name = "bazel_gazelle",
    sha256 = "501deb3d5695ab658e82f6f6f549ba681ea3ca2a5fb7911154b5aa45596183fa",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-gazelle/releases/download/v0.26.0/bazel-gazelle-v0.26.0.tar.gz",
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.26.0/bazel-gazelle-v0.26.0.tar.gz",
    ],
)

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies", "go_repository")

gazelle_dependencies()

################################################################################
# GRPC Gateway                                                                 #
################################################################################

go_repository(
    name = "org_golang_google_grpc",
    importpath = "google.golang.org/grpc",
    sum = "h1:rQOsyJ/8+ufEDJd/Gdsz7HG220Mh9HAhFHRGnIjda0w=",
    version = "v1.48.0",
    build_file_proto_mode = "disable_global",
)

go_repository(
    name = "grpc_ecosystem_grpc_gateway",
    importpath = "github.com/grpc-ecosystem/grpc-gateway/v2",
    sum = "h1:/sDbPb60SusIXjiJGYLUoS/rAQurQmvGWmwn2bBPM9c=",
    version = "v2.11.1",
    build_file_proto_mode = "disable_global",
)

go_repository(
    name = "org_golang_google_genproto",
    importpath = "google.golang.org/genproto",
    sum = "h1:XGQ6tc+EnM35IAazg4y6AHmUg4oK8NXsXaILte1vRlk=",
    version = "v0.0.0-20220413183235-5e96e2839df9",
    build_file_proto_mode = "disable_global",
)

go_repository(
    name = "org_golang_google_protobuf",
    importpath = "google.golang.org/protobuf",
    sum = "h1:w43yiav+6bVFTBQFZX0r7ipe9JQ1QsbMgHwbBziscLw=",
    version = "v1.28.0",
    build_file_proto_mode = "disable_global",
    patches = ["//build/bazel:golang_protobuf.patch"],
    patch_args = ["-p1"],
)

go_rules_dependencies()

go_repository(
    name = "com_github_golang_protobuf",
    tag = "v1.5.2",
    importpath = "github.com/golang/protobuf",
    build_file_proto_mode = "disable_global",
)

go_repository(
    name = "in_gopkg_yaml_v3",
    importpath = "gopkg.in/yaml.v3",
    sum = "h1:dUUwHk2QECo/6vqA44rthZ8ie2QXMNeKRTHCNY2nXvo=",
    version = "v3.0.0-20200313102051-9f266ea9e77c",
    build_file_proto_mode = "disable_global",
)

go_repository(
    name = "com_github_ghodss_yaml",
    importpath = "github.com/ghodss/yaml",
    sum = "h1:wQHKEahhL6wmXdzwWG11gIVCkOv05bNOh+Rxn0yngAk=",
    version = "v1.0.0",
    build_file_proto_mode = "disable_global",
)

go_repository(
    name = "com_github_golang_glog",
    importpath = "github.com/golang/glog",
    sum = "h1:nfP3RFugxnNRyKgeWd4oI1nYvXpxrx8ck8ZrcizshdQ=",
    version = "v1.0.0",
    build_file_proto_mode = "disable_global",
)

go_repository(
    name = "org_golang_x_net",
    importpath = "golang.org/x/net",
    sum = "h1:eH6Eip3UpmR+yM/qI9Ijluzb1bNv/cAU/n+6l8tRSis=",
    version = "v0.0.0-20181220203305-927f97764cc3",
    build_file_proto_mode = "disable_global",
)

go_repository(
    name = "org_golang_x_sys",
    importpath = "golang.org/x/sys",
    sum = "h1:v1W7bwXHsnLLloWYTVEdvGvA7BHMeBYsPcF0GLDxIRs=",
    version = "v0.0.0-20220808155132-1c4a2a72c664",
    build_file_proto_mode = "disable_global",
)

go_repository(
    name = "org_golang_x_text",
    importpath = "golang.org/x/text",
    sum = "h1:g61tztE5qeGQ89tm6NTjjM9VPIm088od1l6aSorWRWg=",
    version = "v0.3.0",
    build_file_proto_mode = "disable_global",
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
    strip_prefix = "protobuf-3.19.1",
    url = "https://github.com/protocolbuffers/protobuf/archive/v3.19.1.tar.gz",
    sha256 = "87407cd28e7a9c95d9f61a098a53cf031109d451a7763e7dd1253abf8b4df422",
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

http_archive(
    name = "com_google_absl",
    strip_prefix = "abseil-cpp-20220623.0",
    url = "https://github.com/abseil/abseil-cpp/archive/refs/tags/20220623.0.zip",
    sha256 = "5b7640be0e119de1a9d941cb6b2607d76978eba5720196f1d4fc6de0421d2241",
)

http_archive(
    name = "com_google_googletest",
    strip_prefix = "googletest-release-1.12.1",
    urls = ["https://github.com/google/googletest/archive/release-1.12.1.tar.gz"],
    sha256 = "81964fe578e9bd7c94dfdb09c8e4d6e6759e19967e397dbea48d1c10e45d0df2",
)

http_archive(
    name = "com_github_grpc_grpc",
    urls = ["https://github.com/grpc/grpc/archive/v1.43.0.tar.gz"],
    strip_prefix = "grpc-1.43.0",
    # Patches applied:
    # - Adding implicit conversion between grpc::Status and absl::Status
    patches = ["//build/bazel:grpc.patch"],
    sha256 = "9647220c699cea4dafa92ec0917c25c7812be51a18143af047e20f3fb05adddc",
)

load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")

grpc_deps()

# Manually load six before loading zetasql. zetasql will try to load six from
# protobuf. More recent versions of protobuf have dropped the six dependency.
http_archive(
    name = "six_archive",
    urls = [
        "https://files.pythonhosted.org/packages/71/39/171f1c67cd00715f190ba0b100d606d440a28c93c7714febeca8b79af85e/six-1.16.0.tar.gz",
    ],
    sha256 = "1e61c37477a1626458e36f7b1d82aa5c9b094fa4802892072e49de9c60c4c926",
    strip_prefix = "six-1.16.0",
    build_file = "@//build/bazel:six.BUILD",
)

################################################################################
# ZetaSQL                                                                      #
################################################################################

http_archive(
    name = "com_google_zetasql",
    url = "https://github.com/google/zetasql/archive/177d495a064e38684c462cf883e22428273bd996.tar.gz",
    strip_prefix = "zetasql-177d495a064e38684c462cf883e22428273bd996",
    # Patches applied:
    # - Give visibility to ZetaSQL's base library to reuse some utilities
    patches = ["//build/bazel:zetasql.patch"],
    sha256 = "4092dce28d3fb5b0071d0268bcb3ba13e28eb4f981c5c267688b8c3590ca7705",
)

http_archive(
    name = "rules_jvm_external",
    sha256 = "f36441aa876c4f6427bfb2d1f2d723b48e9d930b62662bf723ddfb8fc80f0140",
    strip_prefix = "rules_jvm_external-4.1",
    urls = ["https://github.com/bazelbuild/rules_jvm_external/archive/4.1.zip"],
)

# gRPC Java
http_archive(
    name = "io_grpc_grpc_java",
    url = "https://github.com/grpc/grpc-java/archive/v1.43.2.tar.gz",
    strip_prefix = "grpc-java-1.43.2",
    sha256 = "6c39c5feecda4f1ccafe88d8928d9a0f2a686d9a9a9c03888a2e5ac92f7ee34a",
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
    repositories = [
        "https://repo1.maven.org/maven2",
    ],
    maven_install_json = "@//:maven_install.json",
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
    url = "https://github.com/googleapis/google-cloud-cpp/archive/v2.1.0.tar.gz",
    strip_prefix = "google-cloud-cpp-2.1.0",
    sha256 = "168c38219feb5a2c6b81bec5960cd067f6cda3daa83cd9761fa04f27d2b78f17",
)

load("@com_github_googleapis_google_cloud_cpp//bazel:google_cloud_cpp_deps.bzl", "google_cloud_cpp_deps")

google_cloud_cpp_deps()

################################################################################
# Python Libraries                                                             #
################################################################################

http_archive(
    name = "org_python_pypi_portpicker",
    urls = [
        "https://files.pythonhosted.org/packages/3b/34/bfbd5236c7726452080a92a9f6ea9770cd65f51b05cef319ccb767ed32bf/portpicker-1.5.2.tar.gz",
    ],
    sha256 = "c55683ad725f5c00a41bc7db0225223e8be024b1fa564d039ed3390e4fd48fb3",
    strip_prefix = "portpicker-1.5.2/src",
    build_file = "//build/bazel:portpicker.BUILD",
)
