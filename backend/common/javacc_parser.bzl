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

"""Build rules for generating parser for the given grammar file with Javacc."""

def generate_javacc_parser(name, srcs, parser_class_name, extra_deps, extra_headers, extra_srcs, **kwds):
    """Builds a javacc parser in C++ from the given grammar definition.

    Args:
        name (string): Name of the javacc parser library.
        srcs (list): List of files that form jjt grammar definition.
        parser_class_name (string): Name of the parser class defined as part of
        grammar definition in .jjt file that is used by javacc to name
        intermediate ast and parser files.
        extra_deps (list): List of dependencies required by javacc parser.
        extra_headers (list): List of headers required by javacc parser.
        extra_srcs (list): List of cpp source files required by javacc parser.
        **kwds (args): Optional parameters to pass to build rules.
    """

    # Build complete grammar definition from srcs into *parser_cpp.jjt.
    schema_merged_tree = [parser_class_name.lower() + "_cpp.jjt"]
    native.genrule(
        name = "schema_grammar_merge",
        srcs = srcs,
        outs = schema_merged_tree,
        cmd = "cat $(SRCS) > $(OUTS)",
    )

    # Generate abstract syntax tree definition files and grammar file as
    # understood by javacc parser from grammar definition file created above.
    schema_ast_genfiles = [
        "Node.h",
        parser_class_name + "Tree.h",
        parser_class_name + "Tree.cc",
        parser_class_name + "TreeConstants.h",
        parser_class_name + "Visitor.h",
        "JJT" + parser_class_name + "State.h",
        "JJT" + parser_class_name + "State.cc",
    ]
    schema_grammar_file = parser_class_name + ".jj"
    native.genrule(
        name = "schema_jjtree",
        srcs = schema_merged_tree,
        outs = schema_ast_genfiles + [schema_grammar_file],
        cmd = (
            "$(location @local_jdk//:bin/java) -cp $(location @maven//:net_java_dev_javacc_javacc) jjtree  " +
            "-OUTPUT_LANGUAGE=c++ -OUTPUT_DIRECTORY=$(@D) " +
            "-OUTPUT_FILE=" + schema_grammar_file + " $(SRCS) "
        ),
        tools = ["@maven//:net_java_dev_javacc_javacc", "@local_jdk//:bin/jar", "@local_jdk//:bin/java"],
    )

    # Generate the lexer and parser files from the grammar file in .jj
    # format created above.
    schema_parser_genfiles = [
        parser_class_name + ".h",
        parser_class_name + ".cc",
        parser_class_name + "Constants.h",
        parser_class_name + "TokenManager.h",
        parser_class_name + "TokenManager.cc",
        "JavaCC.h",
        "CharStream.h",
        "CharStream.cc",
        "TokenManager.h",
        "TokenMgrError.h",
        "TokenMgrError.cc",
        "ParseException.h",
        "ParseException.cc",
        "Token.h",
        "Token.cc",
        "ErrorHandler.h",
    ]
    native.genrule(
        name = "parser_files",
        srcs = [schema_grammar_file],
        outs = schema_parser_genfiles,
        cmd = (
            "$(location @local_jdk//:bin/java) -cp $(location @maven//:net_java_dev_javacc_javacc) javacc  " +
            "-OUTPUT_LANGUAGE=c++ -OUTPUT_DIRECTORY=$(@D) $(SRCS) "
        ),
        tools = ["@maven//:net_java_dev_javacc_javacc", "@local_jdk//:bin/jar", "@local_jdk//:bin/java"],
    )

    # Finally use generated AST and Parser files to produce c++ parser.
    native.cc_library(
        name = name,
        srcs = schema_ast_genfiles + schema_parser_genfiles + extra_srcs,
        hdrs = [
            parser_class_name + ".h",
            parser_class_name + "TokenManager.h",
            parser_class_name + "Tree.h",
            parser_class_name + "TreeConstants.h",
            "CharStream.h",
            "ErrorHandler.h",
            "JavaCC.h",
            "Node.h",
            "Token.h",
        ] + extra_headers,
        copts = [
            "-funsigned-char",
        ],
        deps = [
            "@com_google_zetasql//zetasql/base",
            "@com_google_absl//absl/base:core_headers",
            "@com_google_absl//absl/status",
            "@com_google_absl//absl/strings",
        ] + extra_deps,
        **kwds
    )
