//
// PostgreSQL is released under the PostgreSQL License, a liberal Open Source
// license, similar to the BSD or MIT licenses.
//
// PostgreSQL Database Management System
// (formerly known as Postgres, then as Postgres95)
//
// Portions Copyright © 1996-2020, The PostgreSQL Global Development Group
//
// Portions Copyright © 1994, The Regents of the University of California
//
// Portions Copyright 2023 Google LLC
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
// EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
// FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN
// "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
// MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
//------------------------------------------------------------------------------

#include <algorithm>
#include <cstdio>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "absl/flags/parse.h"
#include "zetasql/base/file_util.h"
#include "zetasql/base/status.h"
#include "zetasql/base/logging.h"
#include "zetasql/base/path.h"
#include "google/rpc/code.pb.h"
#include "google/rpc/error_details.pb.h"
#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "third_party/spanner_pg/errors/errors.pb.h"
#include "google/protobuf/text_format.h"
#include "re2/re2.h"

ABSL_FLAG(std::string, google3_root, "",
          "Absolute path to the google3 directory in which output "
          "files should be generated.");

ABSL_FLAG(std::string, package, "", "Output directory relative to google3.");

ABSL_FLAG(std::string, namespace, "", "C++ namespace with :: for separator.");

ABSL_FLAG(std::string, file_basename, "error_catalog",
          "Basename, excluding extension, of the generated files.");

namespace {

using ::spangres::error::FileProto;
using ::spangres::error::GroupProto;
using ::spangres::error::MessageProto;
using ::spangres::error::ParamTypeProto;

const char kPGErrorCodePrefix[] = "ERRCODE_";
const char kLookupPGErrorFuncSignature[] =
    "std::function<\n"
    "    absl::Status(const postgres_translator::PostgresEreportException&)>\n"
    "LookupPGErrorFunc(int pg_error_code, std::string pg_message_id)";

// Writes the top of a C++ file: basic header
void WriteTop(FILE* f, const std::string& file) {
  fprintf(f, "// Auto-generated from %s.  DO NOT EDIT!\n", file.c_str());
}

void NewLine(FILE* f) {
  fprintf(f, "\n");
}

enum NSLoc {
  HEAD,
  FOOT
};

// Print the namespace header/footer for a file.
void WriteNamespace(FILE* f, const std::vector<std::string>& namespaze,
                    NSLoc loc) {
  if (loc == HEAD) {
    for (const std::string& s : namespaze) {
      fprintf(f, "namespace %s {\n", s.c_str());
    }
  } else {
    for (int i = namespaze.size() - 1; i >= 0; i--) {
      fprintf(f, "}  // namespace %s\n", namespaze[i].c_str());
    }
  }
}

// Summarized version of a list of ParamTypeProtos.
class ParamTypes {
 public:
  // Required type definitions will be copied from "file_proto".
  void SetTypeSource(const FileProto* file_proto) {
    type_source_ = file_proto;
  }

  // Assert that some message requires a definition for "type_name".
  // This must be done before calling any other method that looks up
  // metadata about a type.
  // Returns true if "type_name" exists and false otherwise.
  bool ABSL_MUST_USE_RESULT Require(const std::string& type_name) {
    if (types_.count(type_name) > 0) return true;

    bool found = false;
    for (const auto& type_proto : type_source_->type()) {
      if (type_proto.name() == type_name) {
        ABSL_CHECK(!found) << "Duplicate type declaration: " << type_name;
        types_[type_proto.name()] = type_proto;
        found = true;
      }
    }
    return found;
  }

  // Insert the names of all header files required by types in this list.
  void AddHeadersTo(std::set<std::string>* out) const {
    for (const auto& entry : types_) {
      for (const std::string& header : entry.second.header()) {
        out->insert(header);
      }
    }
  }

  // Given a parameter type name, return the C++ type signature to be
  // used when this type is passed as a function argument.
  std::string GetArgCppType(const std::string& type_name) const {
    const auto& type = Find(type_name);
    const char* arg_printer;
    switch (type.pass_by()) {
      case ParamTypeProto::REFERENCE:
        arg_printer = "const $0&";
        break;
      case ParamTypeProto::POINTER:
        arg_printer = "const $0*";
        break;
      case ParamTypeProto::VALUE:
        arg_printer = "$0";
        break;
    }
    return absl::Substitute(arg_printer, type.cpp_type());
  }

  // Return printer to be used for a given type name and field. See
  // ParamTypeProto for more information about printers.
  std::string GetPrinter(const std::string& type_name,
                         const std::string& field_name
  ) const {
    const auto& arg_type = Find(type_name);
    if (field_name.empty()) {
      ABSL_CHECK(arg_type.has_printer()) << type_name;
      return ChoosePrinter(type_name,
                           arg_type
      );
    } else {
      const ParamTypeProto::Field* found = nullptr;
      for (const auto& field : arg_type.field()) {
        if (field.name() == field_name) {
          ABSL_CHECK(found == nullptr);
          found = &field;
        }
      }
      ABSL_CHECK(found != nullptr) << type_name << " " << field_name;
      return ChoosePrinter(absl::StrCat(type_name, ".", field_name),
                           *found
      );
    }
  }

 private:
  const FileProto* type_source_;
  std::map<std::string, ParamTypeProto> types_;

  const ParamTypeProto& Find(const std::string& name_suffix) const {
    auto it = types_.find(name_suffix);
    ABSL_CHECK(it != types_.end()) << name_suffix;
    return it->second;
  }

  template <typename T>
  static std::string ChoosePrinter(const std::string& expr,
                                   const T& proto
  ) {
    std::string ret = proto.printer();
    return ret;
  }
};

// The parameter type is the part of the parameter name after the last
// underscore.
std::string ParamTypeFromName(absl::string_view param_name) {
  std::vector<std::string> words = absl::StrSplit(param_name, '_');
  ABSL_CHECK_GE(words.size(), 1);
  return words.back();
}

// SubstituteExpr is a parsed form of the format strings from
// error.MessageProto. It consists of a absl::Substitute format
// string, and information about the positional arguments to be used.
struct SubstituteExpr {
  // A absl::Substitute expression.
  std::string format;

  // The positional arguments to be passed to absl::Substitute.
  std::vector<std::string> args;

  // Generate code for this substitute call.
  std::string ToString() const {
    std::string ret = (args.empty() ? "" : "absl::Substitute(");
    absl::StrAppend(&ret, "\"", absl::CEscape(format), "\"");
    if (!args.empty()) {
      for (const auto& arg : args) {
        absl::StrAppend(&ret, ", ", arg);
      }
      absl::StrAppend(&ret, ")");
    }
    return ret;
  }

  // Build a SubstituteExpr from a format string. Any embedded
  // placeholders have their types added to "types", and any new
  // unique parameter names appended to "params".
  static SubstituteExpr From(ParamTypes* types, absl::string_view format,
                             std::vector<std::string>* params) {
    SubstituteExpr ret;
    static LazyRE2 kPrefixNameRE = {"([^$]*\\$)(\\$|[a-z0-9_]+|{[a-z0-9_.]+})"};
    absl::string_view prefix;
    std::string name;
    int next_number = 0;
    std::map<std::string, int> name_to_number;
    while (RE2::Consume(&format, *kPrefixNameRE, &prefix, &name)) {
      absl::StrAppend(&ret.format, prefix);
      if (name == "$") {
        // $$ turns into a literal $ character.
        absl::StrAppend(&ret.format, "$");
        continue;
      }
      static LazyRE2 kParamFieldRE = {"\\{([a-z0-9_]+)\\.([a-z0-9_]+)\\}"};
      std::string param, field;
      if (!RE2::FullMatch(name, *kParamFieldRE, &param, &field)) {
        // There is no field reference.
        field.clear();
        // Strip {} off.
        static LazyRE2 kParamRE = {"\\{([a-z0-9_]+)\\}"};
        if (!RE2::FullMatch(name, *kParamRE, &param)) {
          param = name;
        }
      }
      if (std::find(params->begin(), params->end(), param) == params->end()) {
        // First time this parameter has been seen. Append it to
        // "params", and ensure that its type name is populated in
        // "types".
        params->push_back(param);
        ABSL_CHECK(types->Require(ParamTypeFromName(param)))
            << "Param: " << param << " Format: " << format;
      }

      if (name_to_number.count(name) == 0) {
        // This placeholder hasn't been used in the substitution yet,
        // so assign it a number to be used in the absl::Substitute
        // format string, and append the corresponding argument to
        // "args".
        const int number = next_number++;
        name_to_number[name] = number;
        ret.args.push_back(absl::Substitute(
            types->GetPrinter(ParamTypeFromName(param),
                              field
                              ),
            param));
      }
      // Append the placeholder number to the format.
      absl::StrAppend(&ret.format, name_to_number[name]);
    }
    // Everything after the final placeholder.
    absl::StrAppend(&ret.format, format);
    return ret;
  }
};

// Convert an underscore_separated_string to TitleCase. E.g. USER_ID or user_id
// -> UserId
std::string UnderscoresToTitleCase(const std::string& input) {
  std::string result;
  for (const absl::string_view segment :
       absl::StrSplit(absl::AsciiStrToLower(input), '_')) {
    for (int i = 0; i < segment.length(); i++) {
      if (i == 0) {
        result.push_back(absl::ascii_toupper(segment.at(i)));
      } else {
        result.push_back(segment.at(i));
      }
    }
  }
  return result;
}

// Represents a single error message from the error file.
struct Message {
  // The code: "BadUsage", "PermissionDenied", etc.
  std::string code;

  // The symbolic name for the message, e.g., "NullWrittenToNotNullColumn".
  std::string name;

  // The namespace for this message derived from "code", e.g.,
  // "UNDEFINED_SCHEMA" => "undefined_schema".
  std::string namespaze;

  // The format string from the error file, for use in comments.
  std::string original_message;

  // The message ID (original string) for a PG error.
  std::string pg_message_id;

  // The unique parameter names needed in the error construction
  // function, in order of first appearance in the message.
  std::vector<std::string> param_names;

  // The absl::Substitute call needed to build the message.
  SubstituteExpr expr;

  // Build the function signature for this error message.
  std::string Signature(const ParamTypes& types) const {
    std::string ret = absl::StrCat("absl::Status ", name, "(");
    for (int i = 0; i < param_names.size(); i++) {
      absl::StrAppend(&ret, i > 0 ? ", " : "",
                      types.GetArgCppType(ParamTypeFromName(param_names[i])),
                      " ", param_names[i]);
    }
    absl::StrAppend(&ret, ")");
    return ret;
  }

  // Build the function signature for original PG errors.
  std::string SignatureForPGErrors() const {
    return absl::StrCat(
        "absl::Status ", name,
        "(const postgres_translator::PostgresEreportException& exc)");
  }

  // Build the function signature for this error message in C.
  std::string SignatureC(const ParamTypes& types) const {
    std::string ret = absl::StrCat(
        "void ", "ereport", UnderscoresToTitleCase(namespaze), name, "(");
    for (int i = 0; i < param_names.size(); i++) {
      absl::StrAppend(&ret, i > 0 ? ", " : "",
                      "const char*"
                      " ", param_names[i]);
    }
    absl::StrAppend(&ret, ")");
    return ret;
  }

  // Build a Message from a corresponding proto. Any types required by
  // the message will be added to "types".
  static Message From(ParamTypes* types, const GroupProto& group,
                      const MessageProto& msg) {
    Message ret;
    ret.code = group.code();
    ret.name = msg.name();
    ret.namespaze = absl::AsciiStrToLower(group.code());
    // Loop over the declared parameters first, as the order in the parameters
    // list may be different from the order they appear in the message.
    if (msg.has_parameters()) {
      for (absl::string_view param : absl::StrSplit(msg.parameters(), ',')) {
        ret.param_names.push_back(std::string(param));
        ABSL_CHECK(types->Require(ParamTypeFromName(param))) << "Param: " << param;
      }
    }
    ret.original_message = msg.format();
    if (msg.has_pg_message_id()) {
      ret.pg_message_id = msg.pg_message_id();
      // Set to a fixed message format.
      ret.original_message = "$message_string";
    }
    ret.expr = SubstituteExpr::From(types, ret.original_message,
                                    &ret.param_names);

    return ret;
  }
};

// Represents all the errors from an error catalog file we are generating.
struct ErrorFile {
  // All types used by "messages".
  ParamTypes types;

  // Messages to appear in this catalog.
  std::vector<Message> messages;

  // Build an ErrorFile from the corresponding proto.
  static ErrorFile From(const FileProto& file) {
    ErrorFile ret;
    ret.types.SetTypeSource(&file);
    for (const auto& group : file.group()) {
      for (const auto& msg_proto : group.message()) {
        ret.messages.push_back(Message::From(&ret.types, group, msg_proto));
      }
    }
    // Sort into contiguous namespaces.
    std::sort(ret.messages.begin(), ret.messages.end(),
              [](const Message& m1, const Message& m2) {
                return m1.namespaze < m2.namespaze;
              });

    return ret;
  }
};

// Emit "names" as #include lines.
void WriteIncludes(FILE* f, const std::set<std::string>& names) {
  for (const auto& name : names) {
    ABSL_CHECK(!name.empty());
    if (name[0] == '<' && name[name.size() - 1] == '>') {
      // Includes like <vector>, <unordered_set>, etc have no quotes.
      fprintf(f, "#include %s\n", absl::CEscape(name).c_str());
    } else {
      fprintf(f, "#include \"%s\"\n", absl::CEscape(name).c_str());
    }
  }
}

void WriteTypeNamespace(FILE* f, const std::string& prev_namespace,
                        const std::string& current_namespace) {
  ABSL_CHECK(!prev_namespace.empty() || !current_namespace.empty());
  if (prev_namespace != current_namespace) {
    if (!prev_namespace.empty()) {
      WriteNamespace(f, {prev_namespace}, FOOT);
    }
    if (!current_namespace.empty()) {
      NewLine(f);
      WriteNamespace(f, {current_namespace}, HEAD);
      NewLine(f);
    }
  }
}

void WriteHeader(FILE* f, const ErrorFile& file,
                 const std::vector<std::string>& namespaze,
                 const std::string& filename) {
  // Generate #ifndef header string
  std::string pragma_once = filename;
  absl::StrReplaceAll({{"/", "_"}}, &pragma_once);
  absl::StrReplaceAll({{".", "_"}}, &pragma_once);
  absl::StrReplaceAll({{"-generated", ""}}, &pragma_once);
  absl::AsciiStrToUpper(&pragma_once);
  pragma_once.append("__");

  fprintf(f, "#ifndef %s\n", pragma_once.c_str());
  fprintf(f, "#define %s\n", pragma_once.c_str());
  NewLine(f);

  std::set<std::string> includes_c = {};
  file.types.AddHeadersTo(&includes_c);
  WriteIncludes(f, includes_c);
  NewLine(f);

  fprintf(f, "#if defined(__cplusplus)\n");
  NewLine(f);

  std::set<std::string> includes_cc = {
      "absl/status/status.h",
      "third_party/spanner_pg/interface/ereport.h"
  };
  WriteIncludes(f, includes_cc);
  NewLine(f);

  WriteNamespace(f, namespaze, HEAD);
  NewLine(f);

  fprintf(f, "%s;\n", kLookupPGErrorFuncSignature);
  NewLine(f);

  // Declarations for the factory functions.
  std::string prev_namespace;
  for (const auto& msg : file.messages) {
    // Write error type namespace.
    WriteTypeNamespace(f, prev_namespace, msg.namespaze);
    prev_namespace = msg.namespaze;
    const std::string signature = msg.pg_message_id.empty()
                                      ? msg.Signature(file.types)
                                      : msg.SignatureForPGErrors();
    fprintf(f, "// %s: %s\n", msg.code.c_str(),
            absl::CEscape(msg.pg_message_id.empty() ? msg.original_message
                                                    : msg.pg_message_id)
                .c_str());
    fprintf(f, "%s;\n\n", signature.c_str());
  }

  // Close error type namespace.
  WriteTypeNamespace(f, prev_namespace, "" /* current_namespace */);
  WriteNamespace(f, namespaze, FOOT);
  NewLine(f);

  // Declarations for the factory functions for C.
  fprintf(f, "extern \"C\" {\n");
  fprintf(f, "#endif\n");
  NewLine(f);

  for (const auto& msg : file.messages) {
    // We do not need to generate ereport functions for errors that are intended
    // to replace original PG errors.
    if (!msg.pg_message_id.empty()) continue;
    const std::string comment = absl::CEscape(msg.original_message);
    fprintf(f, "// %s: %s\n", msg.code.c_str(), comment.c_str());
    fprintf(f, "%s;\n\n", msg.SignatureC(file.types).c_str());
  }

  fprintf(f, "#if defined(__cplusplus)\n");
  fprintf(f, "}\n");
  fprintf(f, "#endif\n");
  NewLine(f);

  fprintf(f, "#endif  // %s\n", pragma_once.c_str());
}

void WriteSource(FILE* f, const ErrorFile& file, const std::string& h_filename,
                 const std::vector<std::string>& namespaze) {
  WriteIncludes(f, {h_filename});
  NewLine(f);

  std::set<std::string> includes = {
      "google/rpc/code.pb.h",
      "google/rpc/error_details.pb.h",
      "absl/container/flat_hash_map.h",
      "absl/strings/substitute.h",
      "third_party/spanner_pg/errors/errors.h",
      "third_party/spanner_pg/errors/errors.pb.h",
      "third_party/spanner_pg/postgres_includes/all.h",
      "third_party/spanner_pg/interface/ereport.h",
  };
  WriteIncludes(f, includes);
  NewLine(f);

  WriteNamespace(f, namespaze, HEAD);
  fprintf(f, "namespace {\n");
  fprintf(f,
          "absl::flat_hash_map<std::pair<int, std::string>, "
          "std::function<absl::Status(const "
          "postgres_translator::PostgresEreportException&)>> "
          "pg_message_id_to_func_ "
          "= {");

  for (const auto& msg : file.messages) {
    if (msg.pg_message_id.empty()) continue;
    fprintf(f, "{{%s%s, \"%s\"}, spangres::%s::%s},", kPGErrorCodePrefix,
            absl::AsciiStrToUpper(msg.code).c_str(),
            absl::CEscape(msg.pg_message_id).c_str(), msg.namespaze.c_str(),
            msg.name.c_str());
  }
  fprintf(f, "};\n");
  fprintf(f, "}\n\n");

  // Create a lookup function to find a corresponding error constructor for a PG
  // error.
  fprintf(f, "%s {\n", kLookupPGErrorFuncSignature);
  fprintf(f,
          "auto it = pg_message_id_to_func_.find(std::make_pair(pg_error_code, "
          "pg_message_id));\n");
  fprintf(f, "if (it == pg_message_id_to_func_.end()) return nullptr;\n");
  fprintf(f, "return it->second;\n");
  fprintf(f, "}\n");

  std::string prev_namespace;
  for (const auto& msg : file.messages) {
    // Write the error type namespace.
    WriteTypeNamespace(f, prev_namespace, msg.namespaze);
    prev_namespace = msg.namespaze;

    std::string msg_expr, signature;
    // Function signature.
    if (msg.pg_message_id.empty()) {
      signature = msg.Signature(file.types);
      msg_expr = msg.expr.ToString();
    } else {
      signature = msg.SignatureForPGErrors();
      msg_expr = "exc.what()";
    }
    fprintf(f, "%s {\n", signature.c_str());
    // Generate internal error message.
    fprintf(f,
            "  absl::Status err = "
            "absl::Status(spangres::error::CanonicalCode(%s%s), %s);\n",
            kPGErrorCodePrefix, absl::AsciiStrToUpper(msg.code).c_str(),
            msg_expr.c_str());

    fprintf(f, "  return err;\n");
    fprintf(f, "}\n\n");
  }

  // Close the error type namespace.
  WriteTypeNamespace(f, prev_namespace, "" /* current_namespace */);
  WriteNamespace(f, namespaze, FOOT);
  NewLine(f);

  // Generates error constructors that are only used in the googler-authored C
  // code.
  for (const auto& msg : file.messages) {
    // We do not need to generate ereport functions for errors that are intended
    // to replace original PG errors.
    if (!msg.pg_message_id.empty()) continue;

    // Function signature.
    fprintf(f, "extern \"C\" %s {\n", msg.SignatureC(file.types).c_str());

    // Generate statements to throw exceptions.
    fprintf(f, "ErrorData error_data = {};\n");
    fprintf(f, "std::string message = %s;\n", msg.expr.ToString().c_str());
    fprintf(f, "error_data.message = message.data();\n");
    fprintf(f, "error_data.elevel = PgErrorLevel::PG_ERROR;\n");
    fprintf(f, "error_data.sqlerrcode = %s%s;\n",
            kPGErrorCodePrefix, absl::AsciiStrToUpper(msg.code).c_str());
    fprintf(f, "absl::Status status = spangres::%s::%s(%s);\n",
            msg.namespaze.c_str(), msg.name.c_str(),
            absl::StrJoin(msg.param_names, ",").c_str());
    fprintf(f,
            "throw postgres_translator::PostgresEreportException(&error_data, "
            "status);");
    fprintf(f, "}\n\n");
  }
  NewLine(f);
}

}  // namespace

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);

  if (argc < 2) {
    ABSL_LOG(FATAL) << "Not enough arguments!";
  }

  const std::string message_file = argv[5];

  FileProto file_proto;
  {
    std::string text;
    ABSL_CHECK_OK(zetasql::internal::GetContents(message_file,
                               &text
                               ));
    bool is_ok =
        google::protobuf::TextFormat::ParseFromString(text, &file_proto);
    if (!is_ok) ABSL_LOG(QFATAL) << "Error parsing file: " << message_file;
  }

  auto error_file = ErrorFile::From(file_proto);
  if (error_file.messages.empty()) {
    ABSL_LOG(QFATAL) << "No messages in the error catalog";
  }

  // Generate output files.
  std::string basename = zetasql_base::JoinPath(absl::GetFlag(FLAGS_package),
                                        absl::GetFlag(FLAGS_file_basename));
  std::string h_filename = absl::StrCat(basename, ".h");
  std::string cc_filename = absl::StrCat(basename, ".cc");

  std::vector<std::string> namespaze =
      absl::StrSplit(absl::GetFlag(FLAGS_namespace), absl::ByString("::"));
  std::string root = absl::StrCat(absl::GetFlag(FLAGS_google3_root), "/");
  std::string h_path = absl::StrCat(root, h_filename);
  {
    FILE* f = fopen(h_path.c_str(), "w");
    ABSL_CHECK(f != nullptr) << "Could not open file " << h_path;
    WriteTop(f, message_file);
    WriteHeader(f, error_file, namespaze, h_filename);
    ABSL_CHECK_EQ(fclose(f), 0) << "Could not close the file";
  }

  std::string cc_path = absl::StrCat(root, cc_filename);
  {
    FILE* f = fopen(cc_path.c_str(), "w");
    ABSL_CHECK(f != nullptr) << "Could not open file " << cc_path;
    WriteTop(f, message_file);
    WriteSource(f, error_file, h_filename, namespaze);
    ABSL_CHECK_EQ(fclose(f), 0) << "Could not close the file";
  }

  return 0;
}
