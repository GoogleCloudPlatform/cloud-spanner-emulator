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

load("@io_bazel_rules_go//go:def.bzl", "go_library")

"""grpc_gateway provides bazel rules for dealing with grpc-gateway.

We use grpc-gateway (https://github.com/grpc-ecosystem/grpc-gateway) to generate
json-grpc proxies but cannot use the generated file as-is. generate_grpc_gateway
runs the grpc-gateway generator, then applies patches to the generate file. For
more details about the patches applied and why they are required, see comments
in fix_grpc_gateway.go in this directory.
"""

def _generate_grpc_gateway_file_impl(ctx):
    # gen-grpc-gateway generates the output file in the same directory as the
    # proto file from which contained the service. This output directory is not
    # controllable and we need to infer it. Assume that it is the same as the
    # directory of the first direct source.
    proto_info = ctx.attr.src[ProtoInfo]
    proto_dir = proto_info.direct_sources[0].dirname
    proto_root = proto_info.direct_sources[0].owner.workspace_root
    if proto_root:
        proto_dir = proto_dir[len(proto_root) + 1:]
    original_genfile = ctx.actions.declare_file(
        proto_dir + "/" + ctx.attr.out_filename,
    )

    # We will output the modified gateway file in the package directory.
    modified_genfile = ctx.actions.declare_file(ctx.attr.out_filename)

    # Collect include paths for transitive sources.
    include_paths = {}
    for src in proto_info.transitive_proto_path.to_list():
        include_paths[src] = True

    # Setup the arguments for the grpc_gateway plugin.
    plugin_args = []

    # Generate the output file with the same directory structure as the input
    # file. By default, the output file is generated in a directory whose
    # structure follows the import path for the go proto package which we cannot
    # introspect easily. Using source_relative allows us to predict the output
    # file path which is needed for bazel. For more information on this
    # argument, see https://github.com/golang/protobuf#packages-and-input-paths.
    plugin_args.append("paths=source_relative")

    # Add an optional yaml config override to the generator. These override the
    # proto http annotations defined on the service in the input proto file. For
    # more information, see the comments in the gateway/*.yaml files.
    if ctx.files.yaml_config:
        plugin_args.append(
            "grpc_api_configuration=" + ctx.files.yaml_config[0].path,
        )

    # First generate the gateway file via gen-grpc-gateway.
    args = ctx.actions.args()
    for include_path in include_paths:
        args.add("-I", include_path)
    args.add(
        "--plugin",
        ctx.expand_location(
            "$(location %s)" % ctx.attr.protoc_gen_grpc_gateway.label,
            targets = [ctx.attr.protoc_gen_grpc_gateway],
        ),
    )
    args.add(
        "--grpc-gateway_out",
        ",".join(plugin_args) + ":" + modified_genfile.dirname,
    )
    args.add_all([x.path for x in proto_info.direct_sources])
    ctx.actions.run(
        inputs = proto_info.transitive_sources.to_list() + ctx.files.yaml_config,
        outputs = [original_genfile],
        executable = ctx.executable.protoc,
        arguments = [args],
        tools = [ctx.executable.protoc_gen_grpc_gateway],
    )

    # Now fix-up the file so we can use it as a standalone package.
    args = ctx.actions.args()
    args.add("--input_gateway_file", original_genfile.path)
    args.add("--output_gateway_file", modified_genfile.path)
    args.add("--proto_imports", ",".join(ctx.attr.proto_imports))

    ctx.actions.run(
        inputs = [original_genfile],
        outputs = [modified_genfile],
        executable = ctx.executable.fix_grpc_gateway,
        arguments = [args],
    )

GRPC_GATEWAY_PKG = "@grpc_ecosystem_grpc_gateway//protoc-gen-grpc-gateway"
GRPC_GATEWAY_BIN = "protoc-gen-grpc-gateway"

generate_grpc_gateway_file = rule(
    attrs = {
        "src": attr.label(
            providers = [ProtoInfo],
            mandatory = True,
        ),
        "proto_imports": attr.string_list(),
        "package_name": attr.string(),
        "out_filename": attr.string(),
        "yaml_config": attr.label(allow_files = True),
        "protoc": attr.label(
            executable = True,
            cfg = "host",
            default = "@com_google_protobuf//:protoc",
        ),
        "protoc_gen_grpc_gateway": attr.label(
            executable = True,
            cfg = "host",
            default = GRPC_GATEWAY_PKG + ":" + GRPC_GATEWAY_BIN,
        ),
        "fix_grpc_gateway": attr.label(
            executable = True,
            cfg = "host",
            default = ":fix_grpc_gateway",
        ),
    },
    outputs = {
        "out": "%{out_filename}",
    },
    implementation = _generate_grpc_gateway_file_impl,
)

def generate_grpc_gateway(
        name,
        src,
        proto_imports,
        out_filename,
        deps,
        yaml_config = None):
    generate_grpc_gateway_file(
        name = name + "_gen",
        package_name = name,
        src = src,
        proto_imports = proto_imports,
        out_filename = out_filename,
        yaml_config = yaml_config,
    )

    go_library(
        importpath = "cloud_spanner_emulator/gateway/" + name,
        name = name,
        srcs = [":" + name + "_gen"],
        deps = deps + [
            "@org_golang_google_grpc//:go_default_library",
            "@org_golang_google_grpc//codes:go_default_library",
            "@org_golang_google_grpc//grpclog:go_default_library",
            "@org_golang_google_grpc//status:go_default_library",
            "@grpc_ecosystem_grpc_gateway//runtime:go_default_library",
            "@grpc_ecosystem_grpc_gateway//utilities:go_default_library",
            "@com_github_golang_protobuf//descriptor:go_default_library",
            "@com_github_golang_protobuf//proto:go_default_library",
        ],
    )
