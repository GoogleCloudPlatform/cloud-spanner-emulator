//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//


#include "backend/schema/parser/ddl_parser.h"

#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "google/protobuf/descriptor.h"
#include "absl/algorithm/container.h"
#include "zetasql/base/no_destructor.h"
#include "absl/container/flat_hash_set.h"
#include "absl/flags/flag.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "backend/common/utils.h"
#include "backend/schema/ddl/operations.pb.h"
#include "backend/schema/parser/DDLParserTokenManager.h"
#include "backend/schema/parser/DDLParserTree.h"
#include "backend/schema/parser/DDLParserTreeConstants.h"
#include "backend/schema/parser/JavaCC.h"
#include "backend/schema/parser/ddl_char_stream.h"
#include "backend/schema/parser/ddl_includes.h"
#include "backend/schema/parser/ddl_token_validation_utils.h"
#include "common/constants.h"
#include "common/errors.h"
#include "common/feature_flags.h"
#include "common/limits.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace ddl {

typedef google::protobuf::RepeatedPtrField<SetOption> OptionList;

const char kCommitTimestampOptionName[] = "allow_commit_timestamp";
const char kPGCommitTimestampOptionName[] = "commit_timestamp";
const char kChangeStreamValueCaptureTypeOptionName[] = "value_capture_type";
const char kChangeStreamRetentionPeriodOptionName[] = "retention_period";
const char kChangeStreamExcludeInsertOptionName[] = "exclude_insert";
const char kChangeStreamExcludeDeleteOptionName[] = "exclude_delete";
const char kChangeStreamExcludeUpdateOptionName[] = "exclude_update";
const char kChangeStreamExcludeTtlDeletesOptionName[] = "exclude_ttl_deletes";
const zetasql_base::NoDestructor<absl::flat_hash_set<std::string>>
    kChangeStreamBooleanOptions{{kChangeStreamExcludeInsertOptionName,
                                 kChangeStreamExcludeDeleteOptionName,
                                 kChangeStreamExcludeUpdateOptionName,
                                 kChangeStreamExcludeTtlDeletesOptionName}};
const zetasql_base::NoDestructor<absl::flat_hash_set<std::string>>
    kChangeStreamStringOptions{{kChangeStreamValueCaptureTypeOptionName,
                                kChangeStreamRetentionPeriodOptionName}};
const char kSearchIndexOptionSortOrderShardingName[] = "sort_order_sharding";
const char kSearchIndexOptionsDisableAutomaticUidName[] =
    "disable_automatic_uid_column";
const char kModelColumnRequiredOptionName[] = "required";
const char kModelDefaultBatchSizeOptionName[] = "default_batch_size";
const char kModelEndpointOptionName[] = "endpoint";
const char kModelEndpointsOptionName[] = "endpoints";
const char kWitnessLocationOptionName[] = "witness_location";
const char kDefaultLeaderOptionName[] = "default_leader";
const char kVersionRetentionPeriodOptionName[] = "version_retention_period";
const char kDefaultSequenceKindOptionName[] = "default_sequence_kind";

typedef google::protobuf::RepeatedPtrField<SetOption> OptionList;
typedef google::protobuf::RepeatedPtrField<Grantee> Grantees;
typedef google::protobuf::RepeatedPtrField<Privilege> Privileges;

namespace {

bool IsPrint(absl::string_view str) {
  const char* strp = str.data();
  const char* end = strp + str.size();
  while (strp < end) {
    if (!absl::ascii_isprint(*strp++)) {
      return false;
    }
  }
  return true;
}

bool UnescapeStringLiteral(absl::string_view val, std::string* result,
                           std::string* error) {
  if (val.size() <= 2) {
    *error = absl::StrCat("Invalid string literal: ", val);
    return false;
  }
  ABSL_CHECK_EQ(val[0], val[val.size() - 1]);
  ZETASQL_VLOG(val[0] == '\'' || val[0] == '"');
  if (!absl::CUnescape(absl::ClippedSubstr(val, 1, val.size() - 2), result)) {
    *error = absl::StrCat("Cannot parse string literal: ", val);
    return false;
  }
  return true;
}

// Build a string representing a syntax error for the given token.
std::string SyntaxError(const Token* token, absl::string_view detail) {
  return absl::StrCat("Syntax error on line ", token->beginLine, ", column ",
                      token->beginColumn, ": ", detail);
}

// Returns the image of the token with special handling of EOF.
std::string GetTokenRepresentation(Token* token) {
  if (token->kind == _EOF) {
    // token->image is empty string which is not helpful.
    return "'EOF'";
  }

  if (token->kind == UNEXPECTED_CHARACTER) {
    // The next character is not any of the known kinds of whitespace, and not
    // something we expected in any production. If the character is non-ASCII,
    // we produce an error message suggesting one common source of such
    // characters. Whatever the case, we suppress further error messages.
    std::string token_str = token->toString();
    if (token_str[0] & 0x80) {
      return "a non-ASCII UTF-8 character. Did you perhaps copy the Spanner "
             "Cloud DDL statements from a word-processed document, including "
             "non-breaking spaces or smart quotes?";
    } else if (!IsPrint(token_str)) {
      return absl::StrCat("a non-printable ASCII character ('",
                          absl::CEscape(token_str), "').");
    }
    return absl::StrCat("an unknown character ('", token_str, "').");
  }

  if (token->kind == ILLEGAL_STRING_ESCAPE) {
    // Revalidate the token image to get an error message.
    std::string error_string;
    absl::Status status =
        ValidateStringLiteralImage(token->image,
                                   /*force=*/true, &error_string);
    if (status.ok()) {
      return "Internal error: revalidation of string failed";
    }
    return error_string;
  }

  if (token->kind == ILLEGAL_BYTES_ESCAPE) {
    // Revalidate the token image to get an error message.
    std::string error_string;
    absl::Status status =
        ValidateBytesLiteralImage(token->image, &error_string);
    if (status.ok()) {
      return "Internal error: revalidation of bytes failed";
    }
    return error_string;
  }

  if (token->kind == UNCLOSED_SQ3 || token->kind == UNCLOSED_DQ3) {
    return SyntaxError(token, "Encountered an unclosed triple quoted string.");
  }

  return absl::StrCat("'", token->image, "'");
}

// Note that the methods in this class have unusual names because we are
// implementing JavaCC's ErrorHandler interface, which uses these names.
class CloudDDLErrorHandler : public ErrorHandler {
 public:
  explicit CloudDDLErrorHandler(std::vector<std::string>* errors)
      : errors_(errors), ignore_further_errors_(false) {}
  ~CloudDDLErrorHandler() override = default;

  // Called when the parser encounters a different token when expecting to
  // consume a specific kind of token.
  // expected_kind - token kind that the parser was trying to consume.
  // expected_token - the image of the token - tokenImages[expected_kind].
  // actual - the actual token that the parser got instead.
  void handleUnexpectedToken(int expected_kind, const JJString& expected_token,
                             Token* actual, DDLParser* parser) override {
    if (ignore_further_errors_) return;

    // expected_kind is -1 when the next token is not expected, when choosing
    // the next rule based on next token. Every invocation of
    // handleUnexpectedToken with expeced_kind=-1 is followed by a call to
    // handleParserError. We process the error there.
    if (expected_kind == -1) {
      return;
    }

    // The parser would continue to through unexpected token at us but only the
    // first error is the cause.
    ignore_further_errors_ = true;

    errors_->push_back(
        absl::StrCat("Syntax error on line ", actual->beginLine, ", column ",
                     actual->beginColumn, ": Expecting '",
                     absl::AsciiStrToUpper(expected_token), "' but found ",
                     GetTokenRepresentation(actual)));
  }

  // Called when the parser cannot continue parsing.
  // last - the last token successfully parsed.
  // unexpected - the token at which the error occurs.
  // production - the production in which this error occurs.
  void handleParseError(Token* last, Token* unexpected,
                        const JJSimpleString& production,
                        DDLParser* parser) override {
    if (ignore_further_errors_) return;
    ignore_further_errors_ = true;

    errors_->push_back(absl::StrCat(
        "Syntax error on line ", unexpected->beginLine, ", column ",
        unexpected->beginColumn, ": Encountered ",
        GetTokenRepresentation(unexpected), " while parsing: ", production));
  }

  int getErrorCount() override { return errors_->size(); }

 private:
  // List of errors found during the parse.  Will be empty IFF
  // there were no problems parsing.
  std::vector<std::string>* errors_;
  bool ignore_further_errors_;
};

//////////////////////////////////////////////////////////////////////////
// Node and Child helper functions

// Return child node of "parent" by position.
SimpleNode* GetChildNode(const SimpleNode* parent, int pos) {
  ABSL_CHECK_LT(pos, parent->jjtGetNumChildren())
      << "[" << pos << "] vs " << parent->jjtGetNumChildren();
  return dynamic_cast<SimpleNode*>(parent->jjtGetChild(pos));
}

void CheckNode(const SimpleNode* node, int expected_type) {
  ABSL_CHECK_EQ(node->getId(), expected_type)
      << "Expected '" << jjtNodeName[expected_type] << "' but was '"
      << jjtNodeName[node->getId()] << "'";
}

SimpleNode* GetChildNode(const SimpleNode* parent, int pos, int expected_type) {
  SimpleNode* child = GetChildNode(parent, pos);
  CheckNode(child, expected_type);
  return child;
}

// Returns the first child node of the type or NULL if not present
SimpleNode* GetFirstChildNode(const SimpleNode* parent, int type) {
  for (int i = 0; i < parent->jjtGetNumChildren(); i++) {
    SimpleNode* child = GetChildNode(parent, i);
    if (child->getId() == type) {
      return child;
    }
  }
  return nullptr;
}

// Returns the text in ddl_text that was used to parse node.
absl::string_view ExtractTextForNode(const SimpleNode* node,
                                     absl::string_view ddl_text) {
  ABSL_DCHECK(node);
  int node_offset = node->absolute_begin_column();
  int node_length = node->absolute_end_column() - node_offset;
  return absl::ClippedSubstr(ddl_text, node_offset, node_length);
}

std::string GetQualifiedIdentifier(const SimpleNode* node) {
  std::string rv;
  for (int i = 0; i < node->jjtGetNumChildren(); ++i) {
    absl::StrAppend(&rv, i != 0 ? "." : "",
                    GetChildNode(node, i, JJTPART)->image());
  }
  return rv;
}

////////////////////////////////////////////////////////////////////////////
// Visit functions each take a const SimpleNode* representing the "root" AST
// node for the given structure, parse its contents, and put the results
// in a peer proto node passed in as second argument.

std::string CheckOptionKeyValNodeAndGetName(const SimpleNode* node) {
  CheckNode(node, JJTOPTION_KEY_VAL);
  const int num_children = node->jjtGetNumChildren();
  ABSL_CHECK_GE(num_children, 2);
  const SimpleNode* key = GetChildNode(node, 0, JJTKEY);
  return key->image();
}

void VisitColumnOptionKeyValNode(const SimpleNode* node, OptionList* options,
                                 std::vector<std::string>* errors) {
  std::string option_name = CheckOptionKeyValNodeAndGetName(node);

  // If this is an invalid option, return error. Later during schema
  // change, we will verify the valid option against the
  // column type.
  if (option_name != kCommitTimestampOptionName &&
      option_name != kModelColumnRequiredOptionName) {
    errors->push_back(absl::StrCat("Option: ", option_name, " is unknown."));
    return;
  }

  SetOption* option = options->Add();
  option->set_option_name(option_name);

  const SimpleNode* child = GetChildNode(node, 1);
  switch (child->getId()) {
    case JJTNULLL:
      option->set_null_value(true);
      break;
    case JJTBOOL_TRUE_VAL:
      option->set_bool_value(true);
      break;
    case JJTBOOL_FALSE_VAL:
      option->set_bool_value(false);
      break;
    default: {
      // handleUnexpectedToken() should have already caught this case
      // and added an error.
      errors->push_back(
          absl::StrCat("Unexpected value for option: ", option_name,
                       ". "
                       "Supported option values are true, false, and null."));
      break;
    }
  }
}

void VisitColumnOptionListNode(const SimpleNode* node, int option_list_offset,
                               OptionList* options,
                               std::vector<std::string>* errors) {
  CheckNode(node, JJTOPTIONS_CLAUSE);
  // The option_list node is suppressed (#void) so it is not
  // created. The children of this node are OPTION_KEY_VALs.
  for (int i = option_list_offset; i < node->jjtGetNumChildren(); ++i) {
    VisitColumnOptionKeyValNode(GetChildNode(node, i, JJTOPTION_KEY_VAL),
                                options, errors);
  }
}

void VisitCreateDatabaseNode(const SimpleNode* node, CreateDatabase* database,
                             std::vector<std::string>* errors) {
  CheckNode(node, JJTCREATE_DATABASE_STATEMENT);

  for (int i = 0; i < node->jjtGetNumChildren(); i++) {
    SimpleNode* child = GetChildNode(node, i);

    switch (child->getId()) {
      case JJTDB_NAME:
        database->set_db_name(child->image());
        break;
      default:
        ABSL_LOG(FATAL) << "Unknown node type: " << node->toString();
    }
  }
}

void VisitOnDeleteClause(const SimpleNode* node,
                         InterleaveClause::Action* on_delete_action) {
  CheckNode(node, JJTON_DELETE_CLAUSE);
  if (GetFirstChildNode(node, JJTNO_ACTION) != nullptr) {
    *on_delete_action = InterleaveClause::NO_ACTION;
  } else if (GetFirstChildNode(node, JJTCASCADE) != nullptr) {
    *on_delete_action = InterleaveClause::CASCADE;
  } else {
    ABSL_LOG(FATAL) << "ON DELETE does not specify a valid behavior: "
               << node->toString();
  }
}

void VisitInterleaveNode(const SimpleNode* node, InterleaveClause* interleave) {
  interleave->set_table_name(
      GetQualifiedIdentifier(GetChildNode(node, 0, JJTINTERLEAVE_IN)));

  // Default behavior is ON DELETE NO ACTION.
  InterleaveClause::Action on_delete_action = InterleaveClause::NO_ACTION;
  SimpleNode* on_delete_node = GetFirstChildNode(node, JJTON_DELETE_CLAUSE);
  if (on_delete_node != nullptr) {
    VisitOnDeleteClause(on_delete_node, &on_delete_action);
  }
  interleave->set_on_delete(on_delete_action);
}

void VisitTableInterleaveNode(const SimpleNode* node,
                              InterleaveClause* interleave) {
  CheckNode(node, JJTTABLE_INTERLEAVE_CLAUSE);
  VisitInterleaveNode(node, interleave);
}

void VisitIndexInterleaveNode(const SimpleNode* node,
                              std::string* interleave_in_table) {
  CheckNode(node, JJTINDEX_INTERLEAVE_CLAUSE);
  ABSL_CHECK_EQ(1, node->jjtGetNumChildren());
  *interleave_in_table =
      GetQualifiedIdentifier(GetChildNode(node, 0, JJTINTERLEAVE_IN));
}

void VisitIntervalExpressionNode(const SimpleNode* node, int64_t* days) {
  CheckNode(node, JJTINTERVAL_EXPRESSION);
  ABSL_CHECK_EQ(1, node->jjtGetNumChildren());

  *days = GetChildNode(node, 0)->image_as_int64();
}

void VisitRowDeletionPolicyExpressionNode(const SimpleNode* node,
                                          RowDeletionPolicy* policy,
                                          std::vector<std::string>* errors) {
  CheckNode(node, JJTROW_DELETION_POLICY_EXPRESSION);
  ABSL_CHECK_EQ(3, node->jjtGetNumChildren());

  SimpleNode* function = GetChildNode(node, 0, JJTROW_DELETION_POLICY_FUNCTION);
  if (!absl::EqualsIgnoreCase(function->image(), "OLDER_THAN")) {
    errors->push_back("Only OLDER_THAN is supported.");
    return;
  }

  SimpleNode* column = GetChildNode(node, 1, JJTROW_DELETION_POLICY_COLUMN);
  policy->set_column_name(column->image());

  SimpleNode* interval_expr = GetChildNode(node, 2, JJTINTERVAL_EXPRESSION);
  int64_t days;
  VisitIntervalExpressionNode(interval_expr, &days);
  policy->mutable_older_than()->set_count(days);
  policy->mutable_older_than()->set_unit(DDLTimeLengthProto::DAYS);
}

void VisitTableRowDeletionPolicyNode(const SimpleNode* node,
                                     RowDeletionPolicy* policy,
                                     std::vector<std::string>* errors) {
  CheckNode(node, JJTROW_DELETION_POLICY_CLAUSE);
  ABSL_CHECK_EQ(1, node->jjtGetNumChildren());
  VisitRowDeletionPolicyExpressionNode(GetChildNode(node, 0), policy, errors);
}

void SetSortOrder(const SimpleNode* key_part_node, KeyPartClause* key_part,
                  std::vector<std::string>* errors,
                  bool set_default_asc = false) {
  if (GetFirstChildNode(key_part_node, JJTDESC) != nullptr) {
    key_part->set_order(KeyPartClause::DESC);
  }

  if (set_default_asc && !key_part->has_order()) {
    if (GetFirstChildNode(key_part_node, JJTASC) != nullptr) {
      key_part->set_order(KeyPartClause::ASC);
    }
  }
}

// Visit a node that defines a key.
void VisitKeyNode(const SimpleNode* node,
                  google::protobuf::RepeatedPtrField<KeyPartClause>* key,
                  std::vector<std::string>* errors) {
  for (int i = 0; i < node->jjtGetNumChildren(); i++) {
    SimpleNode* child = GetChildNode(node, i, JJTKEY_PART);
    KeyPartClause* key_part = key->Add();
    key_part->set_key_name(GetChildNode(child, 0, JJTPATH)->image());
    SetSortOrder(child, key_part, errors);
  }
}

void VisitStoredColumnNode(const SimpleNode* node, StoredColumnDefinition* def,
                           std::vector<std::string>* errors) {
  CheckNode(node, JJTSTORED_COLUMN);
  const int num_children = node->jjtGetNumChildren();
  ABSL_CHECK_EQ(num_children, 1);
  def->set_name(GetChildNode(node, 0, JJTPATH)->image());
}

void VisitStoredColumnListNode(
    const SimpleNode* node,
    google::protobuf::RepeatedPtrField<StoredColumnDefinition>* stored_columns,
    std::vector<std::string>* errors) {
  CheckNode(node, JJTSTORED_COLUMN_LIST);
  ABSL_CHECK_GT(node->jjtGetNumChildren(), 0);
  for (int i = 0; i < node->jjtGetNumChildren(); i++) {
    SimpleNode* stored_column = GetChildNode(node, i, JJTSTORED_COLUMN);
    VisitStoredColumnNode(stored_column, stored_columns->Add(), errors);
  }
}

void VisitCreateIndexWhereClause(
    const SimpleNode* node, google::protobuf::RepeatedPtrField<std::string>* columns) {
  CheckNode(node, JJTCREATE_INDEX_WHERE_CLAUSE);
  ABSL_CHECK_GT(node->jjtGetNumChildren(), 0);  // Crash ok.
  for (int i = 0; i < node->jjtGetNumChildren(); i++) {
    SimpleNode* child = GetChildNode(node, i, JJTPATH);
    *columns->Add() = child->image();
  }
}

// All length requirements/restrictions will be enforced by the validator code.
void SetColumnLength(const SimpleNode* length_node, ColumnDefinition* column) {
  // If the length is MAX, then we leave off the length from ColumnDefinition.
  // The server will use whatever maximum length it is willing to allow, which
  // may be universe- or database-specific.
  if (length_node != nullptr &&
      !absl::EqualsIgnoreCase(length_node->image(), "MAX")) {
    column->set_length(length_node->image_as_int64());
  }
}

std::string JoinDottedPath(const SimpleNode* dotted_path) {
  CheckNode(dotted_path, JJTDOTTED_PATH);
  std::string rv;
  for (int i = 0; i < dotted_path->jjtGetNumChildren(); ++i) {
    absl::StrAppend(&rv, i != 0 ? "." : "",
                    GetChildNode(dotted_path, i, JJTPART)->image());
  }
  return rv;
}

void SetTypeDefinitionLength(const SimpleNode* length_node,
                             TypeDefinition* type_definition) {
  if (length_node != nullptr &&
      !absl::EqualsIgnoreCase(length_node->image(), "MAX")) {
    type_definition->set_length(length_node->image_as_int64());
  }
}

void VisitTypeDefinitionNode(const SimpleNode* type_node,
                             TypeDefinition* type_definition,
                             int recursion_depth,
                             std::vector<std::string>* errors) {
  if (recursion_depth > limits::kMaxComplexTypeNestingDepth) {
    errors->push_back(
        absl::StrCat("DDL parser exceeded complex type nesting limit of ",
                     limits::kMaxComplexTypeNestingDepth));
    return;
  }

  const std::string type_name = absl::AsciiStrToUpper(type_node->image());
  TypeDefinition::Type type;

  if (type_name == "PG") {
    std::string pg_type = absl::AsciiStrToUpper(
        GetQualifiedIdentifier(GetChildNode(type_node, 0, JJTPGTYPE)));
    if (pg_type == "NUMERIC") {
      type = TypeDefinition::PG_NUMERIC;
    } else if (pg_type == "JSONB") {
      type = TypeDefinition::PG_JSONB;
    } else {
      errors->push_back(absl::Substitute(
          "Syntax error on line $0 column $1: Encountered '$2' while parsing: "
          "column_type",
          type_node->begin_line(), type_node->begin_column(),
          absl::StrCat(type_name, ".", pg_type)));
      return;
    }
  } else if (type_name == "FLOAT32") {
    // FLOAT32 => FLOAT.
    type = TypeDefinition::FLOAT;
  } else if (type_name == "FLOAT64") {
    // FLOAT64 => DOUBLE.
    type = TypeDefinition::DOUBLE;
  } else if (type_name == "TOKENLIST") {
    type = TypeDefinition::TOKENLIST;
  } else if (!TypeDefinition::Type_Parse(type_name, &type)) {
    errors->push_back(absl::StrCat("Unrecognized type: ", type_name));
    return;
  }

  type_definition->set_type(type);
  if (type == TypeDefinition::ARRAY) {
    SimpleNode* column_subtype = GetChildNode(type_node, 0, JJTCOLUMN_TYPE);
    VisitTypeDefinitionNode(column_subtype,
                            type_definition->mutable_array_subtype(),
                            recursion_depth + 1, errors);
  } else if (type == TypeDefinition::STRUCT) {
    SimpleNode* struct_fields_node =
        GetFirstChildNode(type_node, JJTSTRUCT_FIELDS);
    // Initialization in case the struct is empty.
    type_definition->mutable_struct_descriptor();
    if (struct_fields_node != nullptr) {
      for (int i = 0; i < struct_fields_node->jjtGetNumChildren(); ++i) {
        SimpleNode* field_node = GetChildNode(struct_fields_node, i);
        TypeDefinition::StructDescriptor::Field field;
        SimpleNode* name_node = GetFirstChildNode(field_node, JJTNAME);
        if (name_node != nullptr) {
          field.set_name(name_node->image());
        }
        VisitTypeDefinitionNode(GetFirstChildNode(field_node, JJTCOLUMN_TYPE),
                                field.mutable_type(), recursion_depth + 1,
                                errors);
        type_definition->mutable_struct_descriptor()->mutable_field()->Add(
            std::move(field));
      }
    }
  }

  SimpleNode* vector_length = GetFirstChildNode(type_node, JJTVECTOR_LENGTH);
  if (vector_length != nullptr) {
    errors->push_back(
        absl::StrCat("'vector_length' is not supported in STRUCT of ARRAY."));
  }

  const SimpleNode* length_node = GetFirstChildNode(type_node, JJTLENGTH);
  SetTypeDefinitionLength(length_node, type_definition);
  if (type_definition->length() < 0) {
    errors->push_back(absl::StrCat("Invalid length for type: ", type_name,
                                   ", found: ", length_node->image()));
  }
}

void VisitColumnTypeNode(const SimpleNode* column_type,
                         ColumnDefinition* column, int recursion_depth,
                         std::vector<std::string>* errors) {
  if (recursion_depth > limits::kMaxComplexTypeNestingDepth) {
    errors->push_back(
        absl::StrCat("DDL parser exceeded complex type nesting limit of ",
                     limits::kMaxComplexTypeNestingDepth));
    return;
  }

  // Proto/enum type names don't have an entry in type registry so we handle
  // them first.
  if (GetFirstChildNode(column_type, JJTDOTTED_PATH) != nullptr) {
    column->set_type(ColumnDefinition::NONE);
    column->set_proto_type_name(
        JoinDottedPath(GetChildNode(column_type, 0, JJTDOTTED_PATH)));
    return;
  }

  std::string type_name = absl::AsciiStrToUpper(column_type->image());
  ColumnDefinition::Type type;

  if (type_name == "PG") {
    std::string pg_type = absl::AsciiStrToUpper(
        GetQualifiedIdentifier(GetChildNode(column_type, 0, JJTPGTYPE)));
    if (pg_type == "NUMERIC") {
      type = ColumnDefinition::PG_NUMERIC;
    } else if (pg_type == "JSONB") {
      type = ColumnDefinition::PG_JSONB;
    } else {
      errors->push_back(absl::Substitute(
          "Syntax error on line $0 column $1: Encountered '$2' while parsing: "
          "column_type",
          column_type->begin_line(), column_type->begin_column(),
          absl::StrCat(type_name, ".", pg_type)));
      return;
    }
  } else {
    if (type_name == "FLOAT64") {
      type = ColumnDefinition::DOUBLE;
    } else if (type_name == "FLOAT32") {
      // FLOAT32 => FLOAT.
      type = ColumnDefinition::FLOAT;
    } else if (type_name == "TOKENLIST") {
      type = ColumnDefinition::TOKENLIST;
    } else if (!ColumnDefinition::Type_Parse(type_name, &type)) {
      ABSL_LOG(FATAL) << "Unrecognized type: " << type_name;
    }
  }

  column->set_type(type);
  if (type == ColumnDefinition::ARRAY) {
    // Read the subtype.
    SimpleNode* column_subtype = GetChildNode(column_type, 0, JJTCOLUMN_TYPE);
    VisitColumnTypeNode(column_subtype, column->mutable_array_subtype(),
                        recursion_depth + 1, errors);
    SimpleNode* vector_length =
        GetFirstChildNode(column_type, JJTVECTOR_LENGTH);
    if (vector_length != nullptr) {
      if (vector_length->image_as_int64() < 0) {
        errors->push_back(
            absl::StrCat("Invalid length for column: ", column->column_name(),
                         ", found: ", vector_length->image()));
      }
      column->set_vector_length(vector_length->image_as_int64());
    }
  } else if (type == ColumnDefinition::STRUCT) {
    VisitTypeDefinitionNode(column_type, column->mutable_type_definition(),
                            recursion_depth, errors);
  }
  const SimpleNode* length_node = GetFirstChildNode(column_type, JJTLENGTH);
  SetColumnLength(length_node, column);
  if (column->length() < 0) {
    errors->push_back(
        absl::StrCat("Invalid length for column: ", column->column_name(),
                     ", found: ", length_node->image()));
  }
}

void VisitGenerationClauseNode(const SimpleNode* node, ColumnDefinition* column,
                               absl::string_view ddl_text) {
  CheckNode(node, JJTGENERATION_CLAUSE);
  for (int i = 0; i < node->jjtGetNumChildren(); ++i) {
    SimpleNode* child = GetChildNode(node, i);
    switch (child->getId()) {
      case JJTEXPRESSION: {
        column->mutable_generated_column()->set_expression(
            absl::StrCat("(", ExtractTextForNode(child, ddl_text), ")"));
        break;
      }
      case JJTSTORED: {
        column->mutable_generated_column()->set_stored(true);
        break;
      }
      default:
        ABSL_LOG(FATAL) << "Unexpected generated column info: " << child->toString();
    }
  }
}

template <typename T>
void VisitSequenceParamNode(const SimpleNode* node, T* definition,
                            std::vector<std::string>* errors) {
  // For SKIP RANGE, there will be two children.
  for (int i = 0; i < node->jjtGetNumChildren(); ++i) {
    SimpleNode* child = GetChildNode(node, i);
    switch (child->getId()) {
      case JJTBIT_REVERSED_POSITIVE: {
        if (definition->has_type()) {
          errors->push_back("The sequence kind is set more than once.");
          return;
        }
        definition->set_type(T::BIT_REVERSED_POSITIVE);
        break;
      }
      case JJTSTART_WITH_COUNTER: {
        ABSL_DCHECK_EQ(child->jjtGetNumChildren(), 1);
        if (definition->has_start_with_counter()) {
          errors->push_back("START WITH COUNTER is set more than once.");
          return;
        }
        definition->set_start_with_counter(
            GetChildNode(child, 0)->image_as_int64());
        break;
      }
      case JJTSKIP_RANGE_MIN: {
        if (definition->has_skip_range_min()) {
          errors->push_back("SKIP RANGE is set more than once.");
          return;
        }
        definition->set_skip_range_min(child->image_as_int64());
        break;
      }
      case JJTSKIP_RANGE_MAX: {
        if (definition->has_skip_range_max()) {
          errors->push_back("SKIP RANGE is set more than once.");
          return;
        }
        definition->set_skip_range_max(child->image_as_int64());
        break;
      }
      default:
        errors->push_back(absl::StrCat("Unexpected sequence options info: ",
                                       child->toString()));
        return;
    }
  }
}

template <typename T>
void VisitSequenceParamListNode(const SimpleNode* node, T* definition,
                                std::vector<std::string>* errors) {
  CheckNode(node, JJTSEQUENCE_PARAM_LIST);

  for (int i = 0; i < node->jjtGetNumChildren(); ++i) {
    VisitSequenceParamNode(GetChildNode(node, i, JJTSEQUENCE_PARAM), definition,
                           errors);
  }
}

void VisitIdentityColumnClauseNode(const SimpleNode* node,
                                   ColumnDefinition* column,
                                   std::vector<std::string>* errors) {
  CheckNode(node, JJTIDENTITY_COLUMN_CLAUSE);
  if (!EmulatorFeatureFlags::instance().flags().enable_identity_columns) {
    errors->push_back("Identity columns are not supported.");
    return;
  }

  if (node->jjtGetNumChildren() != 1) {
    // There is no () clause. Make the side effect of creating the submessage
    // and return. We let the sdl schema to validate if a sequence kind is not
    // specified.
    column->mutable_identity_column();
    return;
  }

  SimpleNode* child = GetChildNode(node, 0, JJTSEQUENCE_PARAM_LIST);
  VisitSequenceParamListNode(child, column->mutable_identity_column(), errors);
}

void VisitColumnDefaultClauseNode(const SimpleNode* node,
                                  ColumnDefinition* column,
                                  absl::string_view ddl_text) {
  CheckNode(node, JJTCOLUMN_DEFAULT_CLAUSE);

  ABSL_DCHECK_EQ(node->jjtGetNumChildren(), 1);

  SimpleNode* child = GetChildNode(node, 0, JJTCOLUMN_DEFAULT_EXPRESSION);

  column->mutable_column_default()->set_expression(
      absl::StrCat(ExtractTextForNode(child, ddl_text)));
}

void VisitColumnNode(const SimpleNode* node, ColumnDefinition* column,
                     absl::string_view ddl_text,
                     std::vector<std::string>* errors) {
  CheckNode(node, JJTCOLUMN_DEF);
  column->set_column_name(GetChildNode(node, 0, JJTNAME)->image());
  SimpleNode* column_type = GetChildNode(node, 1, JJTCOLUMN_TYPE);
  VisitColumnTypeNode(column_type, column, 0, errors);

  // Handle NOT NULL, and OPTIONS
  for (int i = 2; i < node->jjtGetNumChildren(); ++i) {
    SimpleNode* child = GetChildNode(node, i);
    switch (child->getId()) {
      case JJTHIDDEN:
        column->set_hidden(true);
        break;
      case JJTNOT_NULL:
        column->set_not_null(true);
        break;
      case JJTGENERATION_CLAUSE:
        VisitGenerationClauseNode(child, column, ddl_text);
        break;
      case JJTOPTIONS_CLAUSE:
        VisitColumnOptionListNode(child, 0 /* option_list_offset */,
                                  column->mutable_set_options(), errors);
        break;
      case JJTCOLUMN_DEFAULT_CLAUSE:
        VisitColumnDefaultClauseNode(child, column, ddl_text);
        break;
      case JJTIDENTITY_COLUMN_CLAUSE:
        VisitIdentityColumnClauseNode(child, column, errors);
        break;
      default:
        ABSL_LOG(FATAL) << "Unexpected column info: " << child->toString();
    }
  }
}

void VisitColumnNodeAlterAttrs(const SimpleNode* node,
                               const std::string& column_name,
                               ColumnDefinition* column,
                               absl::string_view ddl_text,
                               std::vector<std::string>* errors) {
  CheckNode(node, JJTCOLUMN_DEF_ALTER_ATTRS);
  column->set_column_name(column_name);
  SimpleNode* column_type = GetChildNode(node, 0, JJTCOLUMN_TYPE);
  VisitColumnTypeNode(column_type, column, 0, errors);

  // Handle NOT NULL.
  for (int i = 1; i < node->jjtGetNumChildren(); ++i) {
    SimpleNode* child = GetChildNode(node, i);
    switch (child->getId()) {
      case JJTNOT_NULL:
        column->set_not_null(true);
        break;
      case JJTGENERATION_CLAUSE:
        VisitGenerationClauseNode(child, column, ddl_text);
        break;
      case JJTCOLUMN_DEFAULT_CLAUSE:
        VisitColumnDefaultClauseNode(child, column, ddl_text);
        break;
      case JJTIDENTITY_COLUMN_CLAUSE:
        VisitIdentityColumnClauseNode(child, column, errors);
        break;
      default:
        ABSL_LOG(FATAL) << "Unexpected column info: " << child->toString();
    }
  }
}

void VisitColumnNodeAlter(const std::string& table_name,
                          const std::string& column_name,
                          const SimpleNode* node, absl::string_view ddl_text,
                          DDLStatement* statement,
                          std::vector<std::string>* errors) {
  CheckNode(node, JJTCOLUMN_DEF_ALTER);
  const SimpleNode* child = GetChildNode(node, 0);
  AlterTable* alter_table = statement->mutable_alter_table();
  if (child->getId() == JJTOPTIONS_CLAUSE) {
    // "ALTER COLUMN c SET OPTIONS (...)" does not contain the
    // column TYPE or NOT NULL attributes. Translate this into a
    // SetColumnOptions statement which does not take these
    // attributes as input, and the schema change code will keep
    // these attributes unchanged.
    SetColumnOptions* set_options = statement->mutable_set_column_options();
    SetColumnOptions::ColumnPath* path = set_options->add_column_path();
    path->set_table_name(table_name);
    path->set_column_name(column_name);
    VisitColumnOptionListNode(child, 0 /* option_list_offset */,
                              set_options->mutable_options(), errors);
    return;
  }
  if (child->getId() == JJTCOLUMN_DEF_ALTER_ATTRS) {
    // For "ALTER COLUMN c TYPE NOT NULL"
    alter_table->set_table_name(table_name);
    ColumnDefinition* column =
        alter_table->mutable_alter_column()->mutable_column();
    VisitColumnNodeAlterAttrs(child, column_name, column, ddl_text, errors);
    return;
  }

  alter_table->set_table_name(table_name);
  AlterTable::AlterColumn* alter_column = alter_table->mutable_alter_column();
  ColumnDefinition* column = alter_column->mutable_column();
  column->set_column_name(column_name);
  // `type` is required in ColumnDefinition, so set it to NONE here.
  column->set_type(ColumnDefinition::NONE);

  switch (child->getId()) {
    case JJTCOLUMN_DEFAULT_CLAUSE: {
      // "ALTER COLUMN c SET DEFAULT " does not contain the column TYPE or
      // NOT NULL attributes.
      alter_column->set_operation(AlterTable::AlterColumn::SET_DEFAULT);
      VisitColumnDefaultClauseNode(child, column, ddl_text);
      break;
    }
    case JJTDROP_COLUMN_DEFAULT: {
      // "ALTER COLUMN c DROP DEFAULT " does not contain the column TYPE or
      // NOT NULL attributes.
      alter_column->set_operation(AlterTable::AlterColumn::DROP_DEFAULT);
      column->clear_column_default();
      break;
    }
    case JJTSET_NOT_NULL: {
      errors->push_back(
          "ALTER COLUMN SET NOT NULL not supported without a column type");
      break;
    }
    case JJTDROP_NOT_NULL: {
      errors->push_back(
          "ALTER COLUMN DROP NOT NULL not supported without a column type");
      break;
    }
    case JJTRESTART_COUNTER: {
      if (!EmulatorFeatureFlags::instance().flags().enable_identity_columns) {
        errors->push_back("Identity columns are not supported.");
        return;
      }
      // The reason to use two separate fields is because we would like to have
      // the capability to support updating multiple properties in a single
      // statement. Also, if we supported RESTART COUNTER without WITH, we can
      // set this bool to true but not set the start_with_counter field.
      alter_column->set_operation(AlterTable::AlterColumn::ALTER_IDENTITY);
      alter_column->set_identity_alter_start_with_counter(true);
      column->mutable_identity_column()->set_start_with_counter(
          child->image_as_int64());
      break;
    }
    case JJTSKIP_RANGE_MIN: {
      if (!EmulatorFeatureFlags::instance().flags().enable_identity_columns) {
        errors->push_back("Identity columns are not supported.");
        return;
      }
      if (node->jjtGetNumChildren() != 2) {
        // It should theoretically never happen.
        errors->push_back(
            "skip_range_max is missing in ALTER IDENTITY SET SKIP RANGE.");
        return;
      }
      const SimpleNode* next_child = GetChildNode(node, 1);
      CheckNode(next_child, JJTSKIP_RANGE_MAX);

      alter_column->set_operation(AlterTable::AlterColumn::ALTER_IDENTITY);
      alter_column->set_identity_alter_skip_range(true);
      column->mutable_identity_column()->set_skip_range_min(
          child->image_as_int64());
      column->mutable_identity_column()->set_skip_range_max(
          next_child->image_as_int64());
      break;
    }
    case JJTNO_SKIP_RANGE: {
      if (!EmulatorFeatureFlags::instance().flags().enable_identity_columns) {
        errors->push_back("Identity columns are not supported.");
        return;
      }
      alter_column->set_operation(AlterTable::AlterColumn::ALTER_IDENTITY);
      alter_column->set_identity_alter_skip_range(true);
      column->mutable_identity_column()->clear_skip_range_min();
      column->mutable_identity_column()->clear_skip_range_max();
      break;
    }
    default:
      ABSL_LOG(FATAL) << "Unexpected alter column type: "
                 << GetChildNode(node, 1)->toString();
  }
}

void AddForeignKeyColumnNames(SimpleNode* child,
                              std::function<void(const std::string&)> add) {
  const SimpleNode* names = GetChildNode(child, 0, JJTIDENTIFIER_LIST);
  for (int i = 0; i < names->jjtGetNumChildren(); i++) {
    const SimpleNode* name = GetChildNode(names, i, JJTIDENTIFIER);
    add(name->image());
  }
}

ForeignKey::Action GetForeignKeyAction(const SimpleNode* node) {
  CheckNode(node, JJTON_DELETE);
  SimpleNode* action = GetChildNode(node, 0, JJTREFERENTIAL_ACTION);
  SimpleNode* child = GetChildNode(action, 0);
  switch (child->getId()) {
    case JJTNO_ACTION:
      return ForeignKey::NO_ACTION;
    case JJTCASCADE:
      return ForeignKey::CASCADE;
    default:
      ABSL_LOG(FATAL) << "Unexpected foreign key action: " << child->toString();
      return ForeignKey::ACTION_UNSPECIFIED;
  }
}

void VisitForeignKeyEnforcementNode(const SimpleNode* node,
                                    ForeignKey* foreign_key,
                                    std::vector<std::string>* errors) {
  CheckNode(node, JJTENFORCEMENT);
  if (!EmulatorFeatureFlags::instance().flags().enable_fk_enforcement_option) {
    errors->push_back("Foreign key enforcement is not supported.");
    return;
  }
  SimpleNode* child = GetChildNode(node, 0);
  switch (child->getId()) {
    case JJTENFORCED:
      foreign_key->set_enforced(true);
      return;
    case JJTNOT_ENFORCED:
      foreign_key->set_enforced(false);
      // Relies on the fact that JJTON_DELETE, if exists, is parsed before this.
      if (foreign_key->on_delete() != ForeignKey::ACTION_UNSPECIFIED &&
          foreign_key->on_delete() != ForeignKey::NO_ACTION) {
        errors->push_back(
            "ON DELETE actions are not supported for NOT ENFORCED foreign "
            "keys.");
      }
      return;
    default:
      // Should never happen since the parser rule only defines the two nodes
      // above.
      ABSL_LOG(FATAL) << "Unexpected foreign key enforcement: "
                 << child->toString();  // Crash OK
      return;
  }
}

void VisitForeignKeyNode(const SimpleNode* node, ForeignKey* foreign_key,
                         std::vector<std::string>* errors) {
  CheckNode(node, JJTFOREIGN_KEY);
  // Default enforcement is true, when not specified.
  foreign_key->set_enforced(true);
  for (int i = 0; i < node->jjtGetNumChildren(); i++) {
    SimpleNode* child = GetChildNode(node, i);
    switch (child->getId()) {
      case JJTCONSTRAINT_NAME:
        foreign_key->set_constraint_name(child->image());
        break;
      case JJTREFERENCING_COLUMNS:
        AddForeignKeyColumnNames(
            child, [&foreign_key](const std::string& name) {
              foreign_key->add_constrained_column_name(name);
            });
        break;
      case JJTREFERENCED_TABLE:
        foreign_key->set_referenced_table_name(GetQualifiedIdentifier(child));
        break;
      case JJTREFERENCED_COLUMNS:
        AddForeignKeyColumnNames(
            child, [&foreign_key](const std::string& name) {
              foreign_key->add_referenced_column_name(name);
            });
        break;
      case JJTON_DELETE:
        foreign_key->set_on_delete(GetForeignKeyAction(child));
        break;
      case JJTENFORCEMENT:
        VisitForeignKeyEnforcementNode(child, foreign_key, errors);
        break;
      default:
        // We can only get here if there is a bug in the grammar or parser.
        ABSL_LOG(FATAL) << "Unexpected foreign key attribute: " << child->toString();
    }
  }
}

void VisitCheckConstraintNode(const SimpleNode* node,
                              absl::string_view ddl_text,
                              CheckConstraint* check_constraint,
                              std::vector<std::string>* errors) {
  CheckNode(node, JJTCHECK_CONSTRAINT);
  check_constraint->set_enforced(true);
  for (int i = 0; i < node->jjtGetNumChildren(); i++) {
    SimpleNode* child = GetChildNode(node, i);
    switch (child->getId()) {
      case JJTCONSTRAINT_NAME: {
        check_constraint->set_name(child->image());
        continue;
      }
      case JJTCHECK_CONSTRAINT_EXPRESSION: {
        check_constraint->set_expression(
            absl::StrCat(ExtractTextForNode(child, ddl_text)));
        continue;
      }
      default: {
        ABSL_LOG(FATAL) << "Unexpected check constraint attribute: "
                   << child->toString();
      }
    }
  }
}

void VisitCreateTableNode(const SimpleNode* node, CreateTable* table,
                          absl::string_view ddl_text,
                          std::vector<std::string>* errors) {
  CheckNode(node, JJTCREATE_TABLE_STATEMENT);

  int offset = 0;
  // We may have an optional IF NOT EXISTS node before the name.
  if (GetChildNode(node, offset)->getId() == JJTIF_NOT_EXISTS) {
    table->set_existence_modifier(IF_NOT_EXISTS);
    offset++;
  }

  table->set_table_name(
      GetQualifiedIdentifier(GetChildNode(node, offset, JJTNAME)));
  offset++;

  for (int i = offset; i < node->jjtGetNumChildren(); i++) {
    SimpleNode* child = GetChildNode(node, i);
    switch (child->getId()) {
      case JJTCOLUMN_DEF:
        VisitColumnNode(child, table->add_column(), ddl_text, errors);
        break;
      case JJTFOREIGN_KEY:
        VisitForeignKeyNode(child, table->add_foreign_key(), errors);
        break;
      case JJTCHECK_CONSTRAINT:
        VisitCheckConstraintNode(child, ddl_text, table->add_check_constraint(),
                                 errors);
        break;
      case JJTPRIMARY_KEY:
        VisitKeyNode(child, table->mutable_primary_key(), errors);
        break;
      case JJTSYNONYM_CLAUSE:
        table->set_synonym(GetChildNode(child, 0)->image());
        break;
      case JJTTABLE_INTERLEAVE_CLAUSE:
        VisitTableInterleaveNode(child, table->mutable_interleave_clause());
        break;
      case JJTROW_DELETION_POLICY_CLAUSE:
        VisitTableRowDeletionPolicyNode(
            child, table->mutable_row_deletion_policy(), errors);
        break;
      default:
        ABSL_LOG(FATAL) << "Unexpected table info: " << child->toString();
    }
  }
}

void VisitCreateFunctionNode(const SimpleNode* node,
                             CreateFunction* create_function,
                             bool is_or_replace, absl::string_view ddl_text,
                             std::vector<std::string>* errors) {
  if (!EmulatorFeatureFlags::instance().flags().enable_user_defined_functions) {
    errors->push_back("User defined functions are not supported.");
    return;
  }
  CheckNode(node, JJTCREATE_FUNCTION_STATEMENT);

  const SimpleNode* name_node = GetFirstChildNode(node, JJTNAME);
  ABSL_DCHECK(name_node);

  bool prev_param_has_default = false;
  const SimpleNode* param_list_node =
      GetFirstChildNode(node, JJTFUNCTION_PARAMETER_LIST);
  if (param_list_node) {
    for (int i = 0; i < param_list_node->jjtGetNumChildren(); i++) {
      SimpleNode* function_param_node = GetChildNode(param_list_node, i);
      Function::Parameter* param = create_function->add_param();
      param->set_name(ExtractTextForNode(
          GetFirstChildNode(function_param_node, JJTNAME), ddl_text));
      param->set_param_typename(ExtractTextForNode(
          GetFirstChildNode(function_param_node, JJTFUNCTION_DATA_TYPE),
          ddl_text));
      SimpleNode* default_value_node =
          GetFirstChildNode(function_param_node, JJTPARAM_DEFAULT_EXPRESSION);
      if (default_value_node) {
        prev_param_has_default = true;
        param->set_default_value(
            absl::StrCat(ExtractTextForNode(default_value_node, ddl_text)));
      } else if (prev_param_has_default) {
        errors->push_back(
            "Function parameters must have default values if any previous "
            "parameter has a default value.");
        return;
      }
    }
  }

  create_function->set_function_name(GetQualifiedIdentifier(name_node));
  create_function->set_function_kind(Function::FUNCTION);
  create_function->set_language(Function::SQL);

  if (is_or_replace) {
    create_function->set_is_or_replace(true);
  }

  const SimpleNode* return_type_node = GetFirstChildNode(node, JJTRETURN_TYPE);
  if (return_type_node) {
    create_function->set_return_typename(ExtractTextForNode(
        GetFirstChildNode(return_type_node, JJTFUNCTION_DATA_TYPE), ddl_text));
  }

  const SimpleNode* security_node = GetFirstChildNode(node, JJTSQL_SECURITY);
  if (security_node) {
    create_function->set_sql_security(Function::INVOKER);
  }

  const SimpleNode* definition_node =
      GetFirstChildNode(node, JJTFUNCTION_DEFINITION);
  ABSL_DCHECK(definition_node);
  create_function->set_sql_body(
      std::string(ExtractTextForNode(definition_node, ddl_text)));
}

void VisitCreateViewNode(const SimpleNode* node, CreateFunction* function,
                         bool is_or_replace, absl::string_view ddl_text,
                         std::vector<std::string>* errors) {
  CheckNode(node, JJTCREATE_VIEW_STATEMENT);

  const SimpleNode* name = GetFirstChildNode(node, JJTNAME);
  ABSL_DCHECK(name);
  function->set_function_name(GetQualifiedIdentifier(name));

  function->set_language(Function::SQL);
  function->set_function_kind(Function::VIEW);

  if (is_or_replace) {
    function->set_is_or_replace(true);
  }

  if (GetFirstChildNode(node, JJTSQL_SECURITY)) {
    function->set_sql_security(Function::INVOKER);
  }

  const SimpleNode* view_definition =
      GetFirstChildNode(node, JJTVIEW_DEFINITION);
  function->set_sql_body(
      absl::StrCat((ExtractTextForNode(view_definition, ddl_text))));
}

void VisitCreateIndexNode(const SimpleNode* node, CreateIndex* index,
                          std::vector<std::string>* errors) {
  CheckNode(node, JJTCREATE_INDEX_STATEMENT);
  for (int i = 0; i < node->jjtGetNumChildren(); i++) {
    SimpleNode* child = GetChildNode(node, i);
    switch (child->getId()) {
      case JJTUNIQUE_INDEX:
        index->set_unique(true);
        break;
      case JJTNULL_FILTERED:
        index->set_null_filtered(true);
        break;
      case JJTNAME:
        index->set_index_name(GetQualifiedIdentifier(child));
        break;
      case JJTTABLE:
        index->set_index_base_name(GetQualifiedIdentifier(child));
        break;
      case JJTCOLUMNS:
        VisitKeyNode(child, index->mutable_key(), errors);
        break;
      case JJTSTORED_COLUMN_LIST:
        VisitStoredColumnListNode(
            child, index->mutable_stored_column_definition(), errors);
        break;
      case JJTINDEX_INTERLEAVE_CLAUSE:
        VisitIndexInterleaveNode(child, index->mutable_interleave_in_table());
        break;
      case JJTCREATE_INDEX_WHERE_CLAUSE:
        VisitCreateIndexWhereClause(child,
                                    index->mutable_null_filtered_column());
        break;
      case JJTIF_NOT_EXISTS:
        index->set_existence_modifier(IF_NOT_EXISTS);
        break;
      default:
        ABSL_LOG(FATAL) << "Unexpected index info: " << child->toString();
    }
  }
}

void VisitTokenKeyListNode(
    const SimpleNode* node,
    google::protobuf::RepeatedPtrField<TokenColumnDefinition>* token_columns,
    std::vector<std::string>* errors) {
  CheckNode(node, JJTTOKEN_KEY_LIST);
  ABSL_CHECK_GT(node->jjtGetNumChildren(), 0);

  for (int i = 0; i < node->jjtGetNumChildren(); i++) {
    SimpleNode* token_key = GetChildNode(node, i, JJTKEY_PART);
    KeyPartClause* key_part = token_columns->Add()->mutable_token_column();
    key_part->set_key_name(GetChildNode(token_key, 0, JJTPATH)->image());
    SetSortOrder(token_key, key_part, errors, /*set_default_asc=*/true);
  }
}

void VisitSearchIndexOptionKeyValNode(const SimpleNode* node,
                                      absl::string_view option_name,
                                      OptionList* options,
                                      std::vector<std::string>* errors) {
  if (option_name != kSearchIndexOptionSortOrderShardingName &&
      option_name != kSearchIndexOptionsDisableAutomaticUidName) {
    errors->push_back(absl::StrCat("Option: ", option_name, " is unknown."));
    return;
  }
  SetOption* option = options->Add();
  option->set_option_name(option_name);
  const SimpleNode* child = GetChildNode(node, 1);
  switch (child->getId()) {
    case JJTBOOL_TRUE_VAL:
      option->set_bool_value(true);
      break;
    case JJTBOOL_FALSE_VAL:
      option->set_bool_value(false);
      break;
    default: {
      errors->push_back(
          absl::StrCat("Unexpected value for option: ", option_name,
                       ". "
                       "Supported option values are true and false."));
      break;
    }
  }
}

void VisitSearchIndexOptionsClause(const SimpleNode* node, OptionList* options,
                                   std::vector<std::string>* errors) {
  CheckNode(node, JJTOPTIONS_CLAUSE);
  absl::flat_hash_set<std::string> options_names;
  for (int i = 0; i < node->jjtGetNumChildren(); ++i) {
    const auto* child = GetChildNode(node, i, JJTOPTION_KEY_VAL);
    std::string option_name = CheckOptionKeyValNodeAndGetName(child);
    if (options_names.contains(option_name)) {
      errors->push_back(absl::StrCat("Duplicate option: ", option_name));
      return;
    }
    options_names.insert(option_name);
    VisitSearchIndexOptionKeyValNode(child, option_name, options, errors);
  }
}

void VisitCreateSearchIndexNode(const SimpleNode* node,
                                CreateSearchIndex* index,
                                std::vector<std::string>* errors) {
  CheckNode(node, JJTCREATE_SEARCH_INDEX_STATEMENT);
  for (int i = 0; i < node->jjtGetNumChildren(); i++) {
    SimpleNode* child = GetChildNode(node, i);
    switch (child->getId()) {
      case JJTNAME:
        index->set_index_name(GetQualifiedIdentifier(child));
        break;
      case JJTTABLE:
        index->set_index_base_name(GetQualifiedIdentifier(child));
        break;
      case JJTTOKEN_KEY_LIST:
        VisitTokenKeyListNode(child, index->mutable_token_column_definition(),
                              errors);
        break;
      case JJTPARTITION_KEY:
        VisitKeyNode(child, index->mutable_partition_by(), errors);
        break;
      case JJTORDER_BY_KEY:
        VisitKeyNode(child, index->mutable_order_by(), errors);
        break;
      case JJTSTORED_COLUMN_LIST:
        VisitStoredColumnListNode(
            child, index->mutable_stored_column_definition(), errors);
        break;
      case JJTCREATE_INDEX_WHERE_CLAUSE:
        VisitCreateIndexWhereClause(child,
                                    index->mutable_null_filtered_column());
        break;
      case JJTINDEX_INTERLEAVE_CLAUSE:
        VisitIndexInterleaveNode(child, index->mutable_interleave_in_table());
        break;
      case JJTOPTIONS_CLAUSE:
        VisitSearchIndexOptionsClause(child, index->mutable_set_options(),
                                      errors);
        break;
      default:
        ABSL_LOG(FATAL) << "Unexpected search index info: " << child->toString();
    }
  }
}

void VisitChangeStreamExplicitColumns(
    const SimpleNode* node,
    ChangeStreamForClause::TrackedTables::Entry::TrackedColumns*
        tracked_columns,
    std::vector<std::string>* errors) {
  CheckNode(node, JJTEXPLICIT_COLUMNS);
  for (int i = 0; i < node->jjtGetNumChildren(); i++) {
    SimpleNode* child = GetChildNode(node, i);
    switch (child->getId()) {
      case JJTCOLUMN:
        tracked_columns->add_column_name(child->image());
        break;
      default:
        ABSL_LOG(FATAL) << "Unexpected change streams tracked column: "
                   << child->toString();
    }
  }
}

void VisitChangeStreamTrackedTablesEntry(
    const SimpleNode* node,
    ChangeStreamForClause::TrackedTables::Entry* table_entry,
    std::vector<std::string>* errors) {
  CheckNode(node, JJTCHANGE_STREAM_TRACKED_TABLES_ENTRY);
  table_entry->set_table_name(
      GetQualifiedIdentifier(GetChildNode(node, 0, JJTTABLE)));
  bool all_columns = true;
  for (int i = 1; i < node->jjtGetNumChildren(); i++) {
    SimpleNode* child = GetChildNode(node, i);
    switch (child->getId()) {
      case JJTEXPLICIT_COLUMNS: {
        all_columns = false;
        VisitChangeStreamExplicitColumns(
            child, table_entry->mutable_tracked_columns(), errors);
        break;
      }
      default:
        ABSL_LOG(FATAL) << "Unexpected change streams tracked tables entry: "
                   << child->toString();
    }
  }
  if (all_columns) {
    // `all_columns` is part of a oneof, so we only set it if it is true.
    table_entry->set_all_columns(true);
  }
}

void VisitChangeStreamTrackedTables(
    const SimpleNode* node,
    ChangeStreamForClause::TrackedTables* tracked_tables,
    std::vector<std::string>* errors) {
  CheckNode(node, JJTCHANGE_STREAM_TRACKED_TABLES);
  // The parser does not accept a FOR clause without anything following.
  ABSL_CHECK_GE(node->jjtGetNumChildren(), 1);
  google::protobuf::RepeatedPtrField<ChangeStreamForClause::TrackedTables::Entry>*
      table_entry = tracked_tables->mutable_table_entry();
  ChangeStreamForClause::TrackedTables::Entry* last = nullptr;
  for (int i = 0; i < node->jjtGetNumChildren(); i++) {
    SimpleNode* child = GetChildNode(node, i);
    switch (child->getId()) {
      case JJTCHANGE_STREAM_TRACKED_TABLES_ENTRY: {
        last = table_entry->Add();
        VisitChangeStreamTrackedTablesEntry(child, last, errors);
        break;
      }
      default:
        ABSL_LOG(FATAL) << "Unexpected change stream tracked tables: "
                   << child->toString();
    }
  }
}

void VisitChangeStreamForClause(const SimpleNode* node,
                                ChangeStreamForClause* for_clause,
                                std::vector<std::string>* errors) {
  CheckNode(node, JJTCHANGE_STREAM_FOR_CLAUSE);
  ABSL_CHECK_EQ(1, node->jjtGetNumChildren());
  SimpleNode* child = GetChildNode(node, 0);
  switch (child->getId()) {
    case JJTALL:
      for_clause->set_all(true);
      break;
    case JJTCHANGE_STREAM_TRACKED_TABLES:
      VisitChangeStreamTrackedTables(
          child, for_clause->mutable_tracked_tables(), errors);
      break;
    default:
      ABSL_LOG(FATAL) << "Unexpected change stream for clause: "
                 << child->toString();
  }
}

// Build a string representing a basic logical error for the given node.
// Errors reported with LogicalError should be things that can only be detected
// during parsing, and not later during canonicalization.  If an error can be
// detected during canonicalization, defer it to then, because we expect that
// some customers may want to generate the DDL statements themselves and so
// we'll have to check for it there anyway.
std::string LogicalError(const SimpleNode* node, const std::string& detail) {
  return absl::StrCat("Error on line ", node->begin_line(), ", column ",
                      node->begin_column(), ": ", detail);
}

// Handle witness_location for database options
void VisitWitnessLocationDatabaseOptionValNode(
    const SimpleNode* value_node, SetOption* option,
    std::vector<std::string>* errors) {
  if (value_node->getId() == JJTNULLL) {
    option->set_null_value(true);
    return;
  }
  if (value_node->getId() != JJTSTR_VAL ||
      !ValidateStringLiteralImage(value_node->image(), /*force=*/true, nullptr)
           .ok()) {
    errors->push_back(
        absl::StrCat("Unexpected value for option: witness_location."
                     " Supported values are non-empty strings only."));
    return;
  }

  std::string string_value;
  std::string error = "";
  if (!UnescapeStringLiteral(value_node->image(), &string_value, &error)) {
    errors->push_back(error);
    return;
  }
  option->set_string_value(string_value);
}

void VisitDefaultLeaderDatabaseOptionValNode(const SimpleNode* value_node,
                                             SetOption* option,
                                             std::vector<std::string>* errors) {
  ABSL_DCHECK_EQ(option->option_name(), "default_leader");
  if (value_node->getId() == JJTSTR_VAL &&
      ValidateStringLiteralImage(value_node->image(), /*force=*/true, nullptr)
          .ok()) {
    std::string string_value;
    std::string error = "";
    if (!UnescapeStringLiteral(value_node->image(), &string_value, &error)) {
      errors->push_back(error);
      return;
    }
    if (string_value.empty()) {
      errors->push_back(
          "Empty string is an invalid value for default_leader. If you'd like "
          "to clear a previously set value, use NULL.");
      return;
    }
    option->set_string_value(string_value);
  } else if (value_node->getId() == JJTNULLL) {
    option->set_null_value(true);
  } else {
    errors->push_back(
        absl::StrCat("Unexpected value for option: ", "default_leader",
                     ". Supported option values are strings and NULL."));
    return;
  }
}

void VisitDefaultSequenceKindDatabaseOptionValNode(
    const SimpleNode* value_node, SetOption* option,
    std::vector<std::string>* errors) {
  ABSL_DCHECK_EQ(option->option_name(), kDefaultSequenceKindOptionName);

  if (value_node->getId() == JJTSTR_VAL &&
      ValidateStringLiteralImage(value_node->image(), /*force=*/true, nullptr)
          .ok()) {
    std::string string_value;
    std::string error = "";
    if (!UnescapeStringLiteral(value_node->image(), &string_value, &error)) {
      errors->push_back(error);
      return;
    }
    option->set_string_value(string_value);
  } else if (value_node->getId() == JJTNULLL) {
    option->set_null_value(true);
  } else {
    errors->push_back(absl::StrCat(
        "Unexpected value for option: ", kDefaultSequenceKindOptionName,
        ". Supported option values are strings and NULL."));
    return;
  }
}

void VisitVersionRetentionPeriodDatabaseOptionValNode(
    const SimpleNode* value_node, SetOption* option,
    std::vector<std::string>* errors) {
  ABSL_DCHECK_EQ(option->option_name(), kVersionRetentionPeriodOptionName);

  if (value_node->getId() == JJTNULLL) {
    option->set_null_value(true);
    return;
  }
  if (value_node->getId() != JJTSTR_VAL ||
      !ValidateStringLiteralImage(value_node->image(), /*force=*/true, nullptr)
           .ok()) {
    errors->push_back(
        absl::StrCat("Unexpected value for option: version_retention_period."
                     " Supported values are non-empty strings only."));
    return;
  }

  std::string string_value;
  std::string error = "";
  if (!UnescapeStringLiteral(value_node->image(), &string_value, &error)) {
    errors->push_back(error);
    return;
  }
  option->set_string_value(string_value);
}

void VisitDatabaseOptionKeyValNode(const SimpleNode* node, OptionList* options,
                                   std::vector<std::string>* errors) {
  std::string option_name = CheckOptionKeyValNodeAndGetName(node);
  if (absl::c_find_if(*options, [&option_name](const SetOption& option) {
        return option.option_name() == option_name;
      }) != options->end()) {
    errors->push_back(absl::StrCat("Duplicate option: ", option_name));
    return;
  }

  const SimpleNode* value_node = GetChildNode(node, 1);

  if (option_name == kWitnessLocationOptionName) {
    SetOption* option = options->Add();
    option->set_option_name("witness_location");
    VisitWitnessLocationDatabaseOptionValNode(value_node, option, errors);
    if (option->has_null_value()) {
      errors->push_back("Option: witness_location is null.");
      return;
    }
  } else if (option_name == "witness_location_type") {
    SetOption* option = options->Add();
    option->set_option_name("spanner.internal.cloud_witness_location_type");
    VisitWitnessLocationDatabaseOptionValNode(value_node, option, errors);
  } else if (option_name == kDefaultLeaderOptionName) {
    SetOption* option = options->Add();
    option->set_option_name("default_leader");
    VisitDefaultLeaderDatabaseOptionValNode(value_node, option, errors);
    if (option->has_null_value()) {
      errors->push_back("Option: default_leader is null.");
      return;
    }
  } else if (option_name == kDefaultSequenceKindOptionName &&
             EmulatorFeatureFlags::instance().flags().enable_identity_columns) {
    SetOption* option = options->Add();
    option->set_option_name(kDefaultSequenceKindOptionName);
    VisitDefaultSequenceKindDatabaseOptionValNode(value_node, option, errors);
  } else if (option_name == kVersionRetentionPeriodOptionName) {
    SetOption* option = options->Add();
    option->set_option_name(kVersionRetentionPeriodOptionName);
    VisitVersionRetentionPeriodDatabaseOptionValNode(value_node, option,
                                                     errors);
  } else {
    errors->push_back(absl::StrCat("Option: ", option_name, " is unknown."));
  }
}

void VisitDatabaseOptionListNode(const SimpleNode* node, int option_list_offset,
                                 OptionList* options,
                                 std::vector<std::string>* errors) {
  CheckNode(node, JJTOPTIONS_CLAUSE);
  // The option_list node is suppressed (defined #void in .jjt) so it is not
  // created. The children of this node are OPTION_KEY_VALs.
  for (int i = option_list_offset; i < node->jjtGetNumChildren(); ++i) {
    VisitDatabaseOptionKeyValNode(GetChildNode(node, i, JJTOPTION_KEY_VAL),
                                  options, errors);
  }
}

void VisitAlterDatabaseNode(const SimpleNode* node, AlterDatabase* database,
                            std::vector<std::string>* errors) {
  CheckNode(node, JJTALTER_DATABASE_STATEMENT);
  // Alter Database DDL doesn't take db_name, setting it for the ability to
  // print the DDL statement in sdl_printer
  database->set_db_name(GetChildNode(node, 0, JJTDATABASE_NAME)->image());
  VisitDatabaseOptionListNode(
      GetChildNode(node, 1, JJTOPTIONS_CLAUSE), /*option_list_offset=*/0,
      database->mutable_set_options()->mutable_options(), errors);
}

void VisitStringOrNullOptionValNode(const SimpleNode* value_node,
                                    SetOption* option,
                                    std::vector<std::string>* errors) {
  if (value_node->getId() == JJTNULLL) {
    option->set_null_value(true);
    return;
  }
  if (value_node->getId() != JJTSTR_VAL ||
      !ValidateStringLiteralImage(value_node->image(), /*force=*/true, nullptr)
           .ok()) {
    errors->push_back(
        absl::StrCat("Unexpected value for option: ", option->option_name(),
                     ". Supported option values are strings and NULL."));
    return;
  }
  std::string string_value;
  std::string error = "";
  if (!UnescapeStringLiteral(value_node->image(), &string_value, &error)) {
    errors->push_back(error);
    return;
  }
  option->set_string_value(string_value);
}

void VisitStringArrayOrNullOptionValNode(const SimpleNode* value_node,
                                         SetOption* option,
                                         std::vector<std::string>* errors) {
  if (value_node->getId() == JJTNULLL) {
    option->set_null_value(true);
    return;
  }

  if (value_node->getId() != JJTSTR_VAL_LIST) {
    errors->push_back(
        absl::StrCat("Unexpected value for option: ", option->option_name(),
                     ". Supported option values are string array and NULL."));
    return;
  }

  auto* values = option->mutable_string_list_value();
  for (int i = 0; i < value_node->jjtGetNumChildren(); ++i) {
    const SimpleNode* value = GetChildNode(value_node, i);
    CheckNode(value, JJTSTR_VAL);
    std::string error = "";
    if (!UnescapeStringLiteral(value->image(), values->Add(), &error)) {
      errors->push_back(error);
      return;
    }
  }
}

void VisitInt64OrNullOptionValNode(const SimpleNode* value_node,
                                   SetOption* option,
                                   std::vector<std::string>* errors) {
  if (value_node->getId() == JJTINTEGER_VAL) {
    const int64_t value = value_node->image_as_int64();
    option->set_int64_value(value);
  } else if (value_node->getId() == JJTNULLL) {
    option->set_null_value(true);
  } else {
    errors->push_back(
        absl::StrCat("Unexpected value for option: ", option->option_name(),
                     ". Supported option values are integers and NULL."));
    return;
  }
}

void VisitBoolOrNullOptionValNode(const SimpleNode* value_node,
                                  SetOption* option,
                                  std::vector<std::string>* errors) {
  if (value_node->getId() == JJTBOOL_TRUE_VAL) {
    option->set_bool_value(true);
  } else if (value_node->getId() == JJTBOOL_FALSE_VAL) {
    option->set_bool_value(false);
  } else if (value_node->getId() == JJTNULLL) {
    option->set_null_value(true);
  } else {
    errors->push_back(
        absl::StrCat("Unexpected value for option: ", option->option_name(),
                     ". Supported option values are booleans and NULL."));
    return;
  }
}

void VisitChangeStreamOptionKeyValNode(const SimpleNode* node,
                                       OptionList* options,
                                       std::vector<std::string>* errors) {
  std::string name = CheckOptionKeyValNodeAndGetName(node);
  for (const SetOption& option : *options) {
    if (option.option_name() == name) {
      errors->push_back(absl::StrCat("Duplicate option: ", name));
      return;
    }
  }

  const SimpleNode* value_node = GetChildNode(node, 1);

  if (kChangeStreamStringOptions->contains(name)) {
    SetOption* option = options->Add();
    option->set_option_name(name);
    VisitStringOrNullOptionValNode(value_node, option, errors);
  } else if (kChangeStreamBooleanOptions->contains(name)) {
    SetOption* option = options->Add();
    option->set_option_name(name);
    VisitBoolOrNullOptionValNode(value_node, option, errors);
  } else {
    errors->push_back(absl::StrCat("Option: ", name, " is unknown."));
  }
}

void VisitChangeStreamOptionsClause(const SimpleNode* node,
                                    int option_list_offset, OptionList* options,
                                    std::vector<std::string>* errors) {
  CheckNode(node, JJTOPTIONS_CLAUSE);
  // The option_list node is suppressed (defined #void in .jjt) so it is not
  // created. The children of this node are OPTION_KEY_VALs.
  for (int i = option_list_offset; i < node->jjtGetNumChildren(); ++i) {
    VisitChangeStreamOptionKeyValNode(GetChildNode(node, i, JJTOPTION_KEY_VAL),
                                      options, errors);
  }
}

void VisitCreateChangeStreamNode(const SimpleNode* node,
                                 CreateChangeStream* change_stream,
                                 std::vector<std::string>* errors) {
  CheckNode(node, JJTCREATE_CHANGE_STREAM_STATEMENT);
  change_stream->set_change_stream_name(
      GetQualifiedIdentifier(GetChildNode(node, 0, JJTNAME)));
  for (int i = 1; i < node->jjtGetNumChildren(); i++) {
    const SimpleNode* child = GetChildNode(node, i);
    switch (child->getId()) {
      case JJTCHANGE_STREAM_FOR_CLAUSE:
        VisitChangeStreamForClause(child, change_stream->mutable_for_clause(),
                                   errors);
        break;
      case JJTOPTIONS_CLAUSE:
        VisitChangeStreamOptionsClause(child, /*option_list_offset=*/0,
                                       change_stream->mutable_set_options(),
                                       errors);
        break;
      default:
        ABSL_LOG(FATAL) << "Unexpected create change stream clause: "
                   << child->toString();
    }
  }
}

bool IsSupportedSequenceOption(const std::string& option_name) {
  if (option_name == kSequenceKindOptionName ||
      option_name == kSequenceSkipRangeMinOptionName ||
      option_name == kSequenceSkipRangeMaxOptionName ||
      option_name == kSequenceStartWithCounterOptionName) {
    return true;
  }
  return false;
}

void VisitSequenceOptionKeyValNode(const SimpleNode* node, OptionList* options,
                                   std::vector<std::string>* errors,
                                   bool* sequence_kind_visited) {
  std::string name = CheckOptionKeyValNodeAndGetName(node);
  if (absl::c_find_if(*options, [&name](const SetOption& option) {
        return option.option_name() == name;
      }) != options->end()) {
    errors->push_back(absl::StrCat("Duplicate option: ", name));
    return;
  }

  const SimpleNode* value_node = GetChildNode(node, 1);

  if (!IsSupportedSequenceOption(name)) {
    errors->push_back(absl::StrCat("Option: ", name, " is unknown."));
    return;
  }

  *sequence_kind_visited = false;
  SetOption* option = options->Add();
  option->set_option_name(name);
  if (name == kSequenceKindOptionName) {
    VisitStringOrNullOptionValNode(value_node, option, errors);
    *sequence_kind_visited = true;
    if (option->has_null_value()) {
      errors->push_back(
          "The only supported sequence kind is `bit_reversed_positive`");
      return;
    }
    if (option->has_string_value() &&
        option->string_value() != kSequenceKindBitReversedPositive) {
      errors->push_back(
          absl::StrCat("Unsupported sequence kind: ", option->string_value()));
    }
  } else {
    VisitInt64OrNullOptionValNode(value_node, option, errors);
  }
}

void VisitCreateSequenceNode(const SimpleNode* node, CreateSequence* sequence,
                             std::vector<std::string>* errors) {
  CheckNode(node, JJTCREATE_SEQUENCE_STATEMENT);
  int offset = 0;
  if (GetFirstChildNode(node, JJTIF_NOT_EXISTS) != nullptr) {
    sequence->set_existence_modifier(IF_NOT_EXISTS);
    ++offset;
  }

  sequence->set_sequence_name(
      GetQualifiedIdentifier(GetChildNode(node, offset++, JJTNAME)));

  while (offset < node->jjtGetNumChildren()) {
    const SimpleNode* child = GetChildNode(node, offset++);
    switch (child->getId()) {
      case JJTSEQUENCE_PARAM_LIST:
        if (!EmulatorFeatureFlags::instance().flags().enable_identity_columns) {
          errors->push_back(
              "Using SQL clauses to configure sequence options is not "
              "supported in CREATE SEQUENCE statements.");
          return;
        }
        VisitSequenceParamListNode(child, sequence, errors);
        break;
      case JJTOPTIONS_CLAUSE: {
        OptionList* options = sequence->mutable_set_options();
        bool sequence_kind_visited = false;
        bool has_valid_sequence_kind = false;

        for (int i = 0; i < child->jjtGetNumChildren(); ++i) {
          VisitSequenceOptionKeyValNode(
              GetChildNode(child, i, JJTOPTION_KEY_VAL), options, errors,
              &sequence_kind_visited);
          if (sequence_kind_visited) {
            has_valid_sequence_kind = sequence_kind_visited;
          }
        }

        if (!has_valid_sequence_kind &&
            !EmulatorFeatureFlags::instance().flags().enable_identity_columns) {
          errors->push_back(
              "CREATE SEQUENCE statements require option `sequence_kind` to "
              "be set");
          return;
        }
        break;
      }
      default:
        errors->push_back(absl::StrCat("Unexpected create sequence clause: ",
                                       child->toString()));
        return;
    }
  }
}

void VisitCreateSchemaNode(const SimpleNode* node, CreateSchema* schema,
                           std::vector<std::string>* errors) {
  CheckNode(node, JJTCREATE_SCHEMA_STATEMENT);
  int offset = 0;
  if (GetFirstChildNode(node, JJTIF_NOT_EXISTS) != nullptr) {
    schema->set_existence_modifier(CreateSchema::IF_NOT_EXISTS);
    ++offset;
  }

  schema->set_schema_name(
      GetQualifiedIdentifier(GetChildNode(node, offset++, JJTNAME)));

  // No options for CreateSchema yet.
}

void VisitAlterSequenceNode(const SimpleNode* node, AlterSequence* sequence,
                            std::vector<std::string>* errors) {
  CheckNode(node, JJTALTER_SEQUENCE_STATEMENT);
  int offset = 0;
  if (GetFirstChildNode(node, JJTIF_EXISTS) != nullptr) {
    sequence->set_existence_modifier(IF_EXISTS);
    ++offset;
  }

  sequence->set_sequence_name(
      GetQualifiedIdentifier(GetChildNode(node, offset++, JJTNAME)));

  const SimpleNode* child = GetChildNode(node, offset);
  switch (child->getId()) {
    case JJTOPTIONS_CLAUSE: {
      OptionList* options = sequence->mutable_set_options()->mutable_options();
      for (int i = 0; i < child->jjtGetNumChildren(); ++i) {
        bool sequence_kind_visited;  // Unused
        VisitSequenceOptionKeyValNode(GetChildNode(child, i, JJTOPTION_KEY_VAL),
                                      options, errors, &sequence_kind_visited);
      }
      break;
    }
    case JJTRESTART_COUNTER: {
      if (!EmulatorFeatureFlags::instance().flags().enable_identity_columns) {
        errors->push_back(
            "RESTART COUNTER WITH is not supported in ALTER SEQUENCE "
            "statements.");
        return;
      }
      sequence->mutable_set_start_with_counter()->set_counter_value(
          child->image_as_int64());
      break;
    }
    case JJTSKIP_RANGE_MIN: {
      if (!EmulatorFeatureFlags::instance().flags().enable_identity_columns) {
        errors->push_back(
            "SKIP RANGE is not supported in ALTER SEQUENCE statements.");
        return;
      }
      if (node->jjtGetNumChildren() != 3) {
        // It should theoretically never happen.
        errors->push_back(
            "skip_range_max is missing for SET SKIP RANGE in ALTER SEQUENCE "
            "statements.");
        return;
      }
      const SimpleNode* next_child = GetChildNode(node, offset + 1);
      CheckNode(next_child, JJTSKIP_RANGE_MAX);
      sequence->mutable_set_skip_range()->set_min_value(
          child->image_as_int64());
      sequence->mutable_set_skip_range()->set_max_value(
          next_child->image_as_int64());
      break;
    }
    case JJTNO_SKIP_RANGE: {
      if (!EmulatorFeatureFlags::instance().flags().enable_identity_columns) {
        errors->push_back(
            "NO SKIP RANGE is not supported in ALTER SEQUENCE statements.");
        return;
      }
      sequence->mutable_set_skip_range()->clear_min_value();
      sequence->mutable_set_skip_range()->clear_max_value();
      break;
    }
    default:
      errors->push_back(absl::StrCat("Unexpected alter sequence clause: ",
                                     child->toString()));
      return;
  }
}

void VisitAlterSchemaNode(const SimpleNode* node, AlterSchema* schema,
                          std::vector<std::string>* errors) {
  CheckNode(node, JJTALTER_SCHEMA_STATEMENT);
  int offset = 0;
  if (GetFirstChildNode(node, JJTIF_EXISTS) != nullptr) {
    schema->set_if_exists(true);
    ++offset;
  }

  schema->set_schema_name(
      GetQualifiedIdentifier(GetChildNode(node, offset++, JJTNAME)));

  // No options for AlterSchema yet.
}

void VisitAlterChangeStreamNode(const SimpleNode* node,
                                AlterChangeStream* change_stream,
                                std::vector<std::string>* errors) {
  CheckNode(node, JJTALTER_CHANGE_STREAM_STATEMENT);
  change_stream->set_change_stream_name(
      GetQualifiedIdentifier(GetChildNode(node, 0, JJTNAME)));
  const SimpleNode* child = GetChildNode(node, 1);
  switch (child->getId()) {
    case JJTCHANGE_STREAM_FOR_CLAUSE:
      VisitChangeStreamForClause(child, change_stream->mutable_set_for_clause(),
                                 errors);
      break;
    case JJTOPTIONS_CLAUSE:
      VisitChangeStreamOptionsClause(
          child, /*option_list_offset=*/0,
          change_stream->mutable_set_options()->mutable_options(), errors);
      break;
    case JJTDROP_FOR_ALL: {
      ChangeStreamForClause* drop_for_clause =
          change_stream->mutable_drop_for_clause();
      drop_for_clause->set_all(true);
      break;
    }
    default:
      ABSL_LOG(FATAL) << "Unexpected alter change stream clause: "
                 << child->toString();
  }
}

void VisitAlterProtoBundleNode(const SimpleNode* node,
                               AlterProtoBundle* alter_proto_bundle,
                               std::vector<std::string>* errors) {
  CheckNode(node, JJTALTER_PROTO_BUNDLE_STATEMENT);
  google::protobuf::RepeatedPtrField<ProtoType>* type_source_names = nullptr;
  bool delete_type = false;
  for (int i = 0; i < node->jjtGetNumChildren(); i++) {
    SimpleNode* child = GetChildNode(node, i);
    switch (child->getId()) {
      case JJTINSERT:
        type_source_names = alter_proto_bundle->mutable_insert_type();
        break;
      case JJTUPDATE:
        type_source_names = alter_proto_bundle->mutable_update_type();
        break;
      case JJTDELETE:
        delete_type = true;
        break;
      case JJTDOTTED_PATH: {
        const std::string type = JoinDottedPath(child);
        if (!delete_type) {
          type_source_names->Add()->set_source_name(type);
        } else {
          alter_proto_bundle->add_delete_type(type);
        }
        break;
      }
      default:
        ABSL_LOG(FATAL) << "Unexpected alter proto bundle type: "
                   << child->toString();
    }
  }
}

void VisitAlterTableNode(const SimpleNode* node, absl::string_view ddl_text,
                         DDLStatement* statement,
                         std::vector<std::string>* errors) {
  CheckNode(node, JJTALTER_TABLE_STATEMENT);
  const SimpleNode* child = GetChildNode(node, 1);
  std::string table_name =
      GetQualifiedIdentifier(GetFirstChildNode(node, JJTTABLE_NAME));
  if (child->getId() == JJTALTER_COLUMN) {
    // Depending on the ALTER COLUMN variant, we may generate either
    // an AlterTable or SetColumnOptions statement.
    std::string column_name = GetChildNode(node, 2, JJTNAME)->image();
    VisitColumnNodeAlter(table_name, column_name,
                         GetChildNode(node, 3, JJTCOLUMN_DEF_ALTER), ddl_text,
                         statement, errors);
  } else {
    // These will generate an AlterTable statement.
    AlterTable* alter_table = statement->mutable_alter_table();
    alter_table->set_table_name(table_name);
    switch (child->getId()) {
      case JJTRENAME_TO: {
        alter_table->mutable_rename_to()->set_name(
            GetQualifiedIdentifier(child));
        if (node->jjtGetNumChildren() > 2) {
          SimpleNode* synonym = GetChildNode(node, 2, JJTSYNONYM);
          alter_table->mutable_rename_to()->set_synonym(
              GetQualifiedIdentifier(synonym));
        }
        break;
      }
      case JJTADD_COLUMN: {
        int offset = 2;
        if (GetChildNode(node, offset)->getId() == JJTIF_NOT_EXISTS) {
          offset++;
          alter_table->mutable_add_column()->set_existence_modifier(
              IF_NOT_EXISTS);
        }
        VisitColumnNode(GetChildNode(node, offset, JJTCOLUMN_DEF),
                        alter_table->mutable_add_column()->mutable_column(),
                        ddl_text, errors);
      } break;
      case JJTDROP_COLUMN: {
        SimpleNode* column = GetChildNode(node, 2, JJTCOLUMN_NAME);
        alter_table->set_drop_column(column->image());
        break;
      }
      case JJTSET_ON_DELETE: {
        SimpleNode* on_delete_node = GetChildNode(node, 2, JJTON_DELETE_CLAUSE);
        InterleaveClause::Action on_delete_action;
        VisitOnDeleteClause(on_delete_node, &on_delete_action);
        alter_table->mutable_set_on_delete()->set_action(on_delete_action);
        break;
      }
      case JJTFOREIGN_KEY:
        VisitForeignKeyNode(
            child,
            alter_table->mutable_add_foreign_key()->mutable_foreign_key(),
            errors);
        break;
      case JJTCHECK_CONSTRAINT:
        VisitCheckConstraintNode(child, ddl_text,
                                 alter_table->mutable_add_check_constraint()
                                     ->mutable_check_constraint(),
                                 errors);
        break;
      case JJTDROP_CONSTRAINT: {
        SimpleNode* constraint_name = GetChildNode(node, 2, JJTCONSTRAINT_NAME);
        alter_table->mutable_drop_constraint()->set_name(
            constraint_name->image());
        break;
      }
      case JJTADD_ROW_DELETION_POLICY: {
        VisitTableRowDeletionPolicyNode(
            GetChildNode(child, 0, JJTROW_DELETION_POLICY_CLAUSE),
            alter_table->mutable_add_row_deletion_policy(), errors);
        break;
      }
      case JJTREPLACE_ROW_DELETION_POLICY: {
        VisitTableRowDeletionPolicyNode(
            GetChildNode(child, 0, JJTROW_DELETION_POLICY_CLAUSE),
            alter_table->mutable_alter_row_deletion_policy(), errors);
        break;
      }
      case JJTDROP_ROW_DELETION_POLICY: {
        alter_table->mutable_drop_row_deletion_policy();
        break;
      }
      case JJTADD_SYNONYM: {
        alter_table->mutable_add_synonym()->set_synonym(
            GetQualifiedIdentifier(child));
        break;
      }
      case JJTDROP_SYNONYM: {
        alter_table->mutable_drop_synonym()->set_synonym(
            GetQualifiedIdentifier(child));
        break;
      }
      default:
        ABSL_LOG(FATAL) << "Unexpected alter table type: "
                   << GetChildNode(node, 1)->toString();
    }
  }
}

void VisitAlterIndexNode(const SimpleNode* node, AlterIndex* alter_index,
                         std::vector<std::string>* errors) {
  CheckNode(node, JJTALTER_INDEX_STATEMENT);
  const SimpleNode* alter_type = GetChildNode(node, 1);
  alter_index->set_index_name(
      GetQualifiedIdentifier(GetFirstChildNode(node, JJTNAME)));
  switch (alter_type->getId()) {
    case JJTADD: {
      const SimpleNode* column_name = GetFirstChildNode(node, JJTCOLUMN_NAME);
      alter_index->mutable_add_stored_column()->set_column_name(
          column_name->image());
      break;
    }
    case JJTDROP: {
      const SimpleNode* column_name = GetFirstChildNode(node, JJTCOLUMN_NAME);
      alter_index->set_drop_stored_column(column_name->image());
      break;
    }
    default: {
      errors->push_back(LogicalError(
          alter_type, absl::StrCat("Unexpected value for alter index type: ",
                                   alter_type->image())));
      break;
    }
  }
}

void VisitPrivilegeNode(const SimpleNode* node, Privilege* privilege,
                        std::vector<std::string>* errors) {
  CheckNode(node, JJTPRIVILEGE);
  std::string privilege_name = node->image();
  if (absl::EqualsIgnoreCase("SELECT", privilege_name)) {
    privilege->set_type(Privilege::SELECT);
  } else if (absl::EqualsIgnoreCase("INSERT", privilege_name)) {
    privilege->set_type(Privilege::INSERT);
  } else if (absl::EqualsIgnoreCase("UPDATE", privilege_name)) {
    privilege->set_type(Privilege::UPDATE);
  } else if (absl::EqualsIgnoreCase("DELETE", privilege_name)) {
    privilege->set_type(Privilege::DELETE);
  } else if (absl::EqualsIgnoreCase("EXECUTE", privilege_name)) {
    privilege->set_type(Privilege::EXECUTE);
  } else if (absl::EqualsIgnoreCase("USAGE", privilege_name)) {
    privilege->set_type(Privilege::USAGE);
  } else {
    errors->push_back(
        absl::StrCat("Unexpected privilege type: ", node->image(), "."));
    return;
  }

  // TODO: Add support for column-level FGAC
  if (node->jjtGetNumChildren() != 0) {
    errors->push_back(
        "Emulator does not yet support column level access controls");
  }
}

void VisitPrivilegesNode(const SimpleNode* node, Privileges* privileges,
                         std::vector<std::string>* errors) {
  CheckNode(node, JJTPRIVILEGES);
  for (int i = 0; i < node->jjtGetNumChildren(); i++) {
    SimpleNode* privilege = GetChildNode(node, i);
    VisitPrivilegeNode(privilege, privileges->Add(), errors);
  }
}

void VisitPrivilegeTargetsNode(const SimpleNode* node, PrivilegeTarget* target,
                               std::vector<std::string>* errors) {
  CheckNode(node, JJTPRIVILEGE_TARGET);
  SimpleNode* child = GetChildNode(node, 0);
  CheckNode(child, JJTTARGET_TYPE);
  child = GetChildNode(child, 0);
  switch (child->getId()) {
    case JJTTABLE: {
      target->set_type(PrivilegeTarget::TABLE);
      break;
    }
    case JJTCHANGE_STREAM: {
      target->set_type(PrivilegeTarget::CHANGE_STREAM);
      break;
    }
    case JJTVIEW: {
      target->set_type(PrivilegeTarget::VIEW);
      break;
    }
    case JJTFUNCTION: {
      target->set_type(PrivilegeTarget::FUNCTION);
      break;
    }
    case JJTTABLE_FUNCTION: {
      target->set_type(PrivilegeTarget::TABLE_FUNCTION);
      break;
    }
    case JJTSEQUENCE: {
      target->set_type(PrivilegeTarget::SEQUENCE);
      break;
    }
    default: {
      errors->push_back(
          absl::StrCat("Unexpected privilege target: ", child->image()));
    }
  }

  SimpleNode* names = GetChildNode(node, 1);
  for (int i = 0; i < names->jjtGetNumChildren(); ++i) {
    SimpleNode* name = GetChildNode(names, i);
    target->add_name(GetQualifiedIdentifier(name));
  }
}

void VisitGranteesNode(const SimpleNode* node, Grantees* grantees,
                       std::vector<std::string>* errors) {
  for (int i = 0; i < node->jjtGetNumChildren(); i++) {
    SimpleNode* grantee_node = GetChildNode(node, i);
    Grantee* grantee = grantees->Add();
    grantee->set_name(grantee_node->image());
    grantee->set_type(Grantee::ROLE);
  }
}

void VisitGrantPrivilegeNode(const SimpleNode* node, GrantPrivilege* statement,
                             std::vector<std::string>* errors) {
  CheckNode(node, JJTGRANT_STATEMENT);
  VisitPrivilegesNode(GetChildNode(node, 0), statement->mutable_privilege(),
                      errors);
  VisitPrivilegeTargetsNode(GetChildNode(node, 1), statement->mutable_target(),
                            errors);
  SimpleNode* grantees_node = GetChildNode(node, 2);
  VisitGranteesNode(GetChildNode(grantees_node, 0),
                    statement->mutable_grantee(), errors);
}

void VisitGrantMembershipNode(const SimpleNode* node, GrantMembership* grant,
                              std::vector<std::string>* errors) {
  CheckNode(node, JJTGRANT_STATEMENT);
  SimpleNode* roles_node = GetChildNode(node, 0);
  VisitGranteesNode(GetChildNode(roles_node, 0), grant->mutable_role(), errors);
  SimpleNode* grantees_node = GetChildNode(node, 1);
  VisitGranteesNode(GetChildNode(grantees_node, 0), grant->mutable_grantee(),
                    errors);
}

void VisitRevokePrivilegeNode(const SimpleNode* node, RevokePrivilege* revoke,
                              std::vector<std::string>* errors) {
  CheckNode(node, JJTREVOKE_STATEMENT);
  VisitPrivilegesNode(GetChildNode(node, 0), revoke->mutable_privilege(),
                      errors);
  VisitPrivilegeTargetsNode(GetChildNode(node, 1), revoke->mutable_target(),
                            errors);
  SimpleNode* grantees_node = GetChildNode(node, 2);
  VisitGranteesNode(GetChildNode(grantees_node, 0), revoke->mutable_grantee(),
                    errors);
}

void VisitRevokeMembershipNode(const SimpleNode* node, RevokeMembership* revoke,
                               std::vector<std::string>* errors) {
  CheckNode(node, JJTREVOKE_STATEMENT);
  SimpleNode* roles_node = GetChildNode(node, 0);
  VisitGranteesNode(GetChildNode(roles_node, 0), revoke->mutable_role(),
                    errors);
  SimpleNode* grantees_node = GetChildNode(node, 1);
  VisitGranteesNode(GetChildNode(grantees_node, 0), revoke->mutable_grantee(),
                    errors);
}

void VisitModelOptionKeyValNode(const SimpleNode* node, OptionList* options,
                                std::vector<std::string>* errors) {
  std::string name = CheckOptionKeyValNodeAndGetName(node);

  // Options is an accumulator that contains already visited options. If the
  // current node contains option name that was already seen, it's a duplicate.
  if (absl::c_find_if(*options, [&name](const SetOption& option) {
        return option.option_name() == name;
      }) != options->end()) {
    errors->push_back(absl::StrCat("Duplicate option: ", name));
    return;
  }

  SetOption* option = options->Add();
  option->set_option_name(name);

  const SimpleNode* value_node = GetChildNode(node, 1);
  if (name == kModelDefaultBatchSizeOptionName) {
    VisitInt64OrNullOptionValNode(value_node, option, errors);
  } else if (name == kModelEndpointOptionName) {
    VisitStringOrNullOptionValNode(value_node, option, errors);
  } else if (name == kModelEndpointsOptionName) {
    VisitStringArrayOrNullOptionValNode(value_node, option, errors);
  } else {
    errors->push_back(absl::StrCat("Option: ", name, " is unknown."));
  }
}

void VisitModelOptionListNode(const SimpleNode* node, OptionList* options,
                              std::vector<std::string>* errors) {
  CheckNode(node, JJTOPTIONS_CLAUSE);
  // The option_list node is suppressed (defined #void in .jjt) so it is not
  // created. The children of this node are OPTION_KEY_VALs.
  for (int i = 0; i < node->jjtGetNumChildren(); ++i) {
    VisitModelOptionKeyValNode(GetChildNode(node, i, JJTOPTION_KEY_VAL),
                               options, errors);
  }
}

void VisitModelColumnList(const SimpleNode* node,
                          google::protobuf::RepeatedPtrField<ColumnDefinition>* columns,
                          absl::string_view ddl_text,
                          std::vector<std::string>* errors) {
  for (int i = 0; i < node->jjtGetNumChildren(); i++) {
    SimpleNode* child = GetChildNode(node, i);
    CheckNode(child, JJTCOLUMN_DEF);
    VisitColumnNode(child, columns->Add(), ddl_text, errors);
  }
}

void VisitCreateModelNode(const SimpleNode* node, CreateModel* create_model,
                          bool is_or_replace, absl::string_view ddl_text,
                          std::vector<std::string>* errors) {
  CheckNode(node, JJTCREATE_MODEL_STATEMENT);
  create_model->set_model_name(
      GetQualifiedIdentifier(GetFirstChildNode(node, JJTNAME)));
  if (GetFirstChildNode(node, JJTIF_NOT_EXISTS) != nullptr) {
    create_model->set_existence_modifier(IF_NOT_EXISTS);
    if (is_or_replace) {
      errors->push_back(
          absl::StrCat("CREATE MODEL statement cannot have both OR REPLACE and "
                       "IF NOT EXISTS set: ",
                       create_model->model_name()));
      return;
    }
  }
  if (is_or_replace) {
    create_model->set_existence_modifier(OR_REPLACE);
  }
  SimpleNode* input = GetFirstChildNode(node, JJTINPUT);
  if (input != nullptr) {
    VisitModelColumnList(input, create_model->mutable_input(), ddl_text,
                         errors);
  }

  SimpleNode* output = GetFirstChildNode(node, JJTOUTPUT);
  if (output != nullptr) {
    VisitModelColumnList(output, create_model->mutable_output(), ddl_text,
                         errors);
  }

  const SimpleNode* remote = GetFirstChildNode(node, JJTREMOTE);
  if (remote != nullptr) {
    create_model->set_remote(true);
  }

  const SimpleNode* options = GetFirstChildNode(node, JJTOPTIONS_CLAUSE);
  if (options != nullptr) {
    VisitModelOptionListNode(options, create_model->mutable_set_options(),
                             errors);
  }
}

void VisitAlterModelNode(const SimpleNode* node, AlterModel* alter_model,
                         std::vector<std::string>* errors) {
  CheckNode(node, JJTALTER_MODEL_STATEMENT);
  alter_model->set_model_name(
      GetQualifiedIdentifier(GetFirstChildNode(node, JJTNAME)));
  if (GetFirstChildNode(node, JJTIF_EXISTS) != nullptr) {
    alter_model->set_if_exists(true);
  }
  const SimpleNode* options = GetFirstChildNode(node, JJTOPTIONS_CLAUSE);
  if (options != nullptr) {
    VisitModelOptionListNode(
        options, alter_model->mutable_set_options()->mutable_options(), errors);
  }
}

void VisitRenameTableNode(const SimpleNode* node, RenameTable* rename_table,
                          std::vector<std::string>* errors) {
  CheckNode(node, JJTRENAME_STATEMENT);
  for (int i = 0; i < node->jjtGetNumChildren(); ++i) {
    SimpleNode* op = GetChildNode(node, i);
    CheckNode(op, JJTRENAME_OP);
    ABSL_DCHECK_EQ(op->jjtGetNumChildren(), 2);
    RenameTable::RenameOp* rename_op = rename_table->add_rename_op();
    rename_op->set_from_name(GetChildNode(op, 0)->image());
    rename_op->set_to_name(GetChildNode(op, 1)->image());
  }
}

void VisitCreateProtoBundleNode(const SimpleNode* node,
                                CreateProtoBundle* proto_bundle,
                                std::vector<std::string>* errors) {
  CheckNode(node, JJTCREATE_PROTO_BUNDLE_STATEMENT);
  for (int i = 0; i < node->jjtGetNumChildren(); i++) {
    proto_bundle->add_insert_type()->set_source_name(
        JoinDottedPath(GetChildNode(node, i, JJTDOTTED_PATH)));
  }
}

// End Visit functions
//////////////////////////////////////////////////////////////////////////

// Walk over AST to build up an un-validated DDLStatement.
void BuildCloudDDLStatement(const SimpleNode* root, absl::string_view ddl_text,
                            DDLStatement* statement,
                            std::vector<std::string>* errors) {
  CheckNode(root, JJTDDL_STATEMENT);

  const SimpleNode* stmt = GetChildNode(root, 0);
  switch (stmt->getId()) {
    case JJTCREATE_DATABASE_STATEMENT:
      VisitCreateDatabaseNode(stmt, statement->mutable_create_database(),
                              errors);
      break;
    case JJTCREATE_PROTO_BUNDLE_STATEMENT: {
      CreateProtoBundle* create_proto_bundle =
          statement->mutable_create_proto_bundle();
      VisitCreateProtoBundleNode(stmt, create_proto_bundle, errors);
      break;
    }
    case JJTCREATE_TABLE_STATEMENT:
      VisitCreateTableNode(stmt, statement->mutable_create_table(), ddl_text,
                           errors);
      break;
    case JJTCREATE_INDEX_STATEMENT:
      VisitCreateIndexNode(stmt, statement->mutable_create_index(), errors);
      break;
    case JJTALTER_INDEX_STATEMENT: {
      VisitAlterIndexNode(stmt, statement->mutable_alter_index(), errors);
      break;
    }
    case JJTGRANT_STATEMENT: {
      // For grant privilege statement, first child node represents privileges.
      if (GetChildNode(stmt, 0)->getId() == JJTPRIVILEGES) {
        VisitGrantPrivilegeNode(stmt, statement->mutable_grant_privilege(),
                                errors);
      } else if (GetChildNode(stmt, 0)->getId() == JJTGRANTEES) {
        // For grant membership statement, first child node represents grantees.
        VisitGrantMembershipNode(stmt, statement->mutable_grant_membership(),
                                 errors);
      } else {
        ABSL_LOG(FATAL) << "Unexpected statement: " << stmt->toString();
      }
      break;
    }
    case JJTREVOKE_STATEMENT: {
      // For revoke privilege statement, first child node represents privileges.
      if (GetChildNode(stmt, 0)->getId() == JJTPRIVILEGES) {
        VisitRevokePrivilegeNode(stmt, statement->mutable_revoke_privilege(),
                                 errors);
      } else if (GetChildNode(stmt, 0)->getId() == JJTGRANTEES) {
        // For revoke membership statement, first child node represents
        // grantees.
        VisitRevokeMembershipNode(stmt, statement->mutable_revoke_membership(),
                                  errors);
      } else {
        ABSL_LOG(FATAL) << "Unexpected statement: " << stmt->toString();
      }
      break;
    }
    case JJTRENAME_STATEMENT: {
      VisitRenameTableNode(stmt, statement->mutable_rename_table(), errors);
      break;
    }
    case JJTCREATE_SEARCH_INDEX_STATEMENT:
      VisitCreateSearchIndexNode(stmt, statement->mutable_create_search_index(),
                                 errors);
      break;
    case JJTCREATE_CHANGE_STREAM_STATEMENT:
      VisitCreateChangeStreamNode(
          stmt, statement->mutable_create_change_stream(), errors);
      break;
    case JJTCREATE_SEQUENCE_STATEMENT:
      VisitCreateSequenceNode(stmt, statement->mutable_create_sequence(),
                              errors);
      break;
    case JJTDROP_STATEMENT: {
      const SimpleNode* drop_stmt = GetChildNode(stmt, 0);
      const SimpleNode* name_node = GetFirstChildNode(stmt, JJTNAME);
      std::string name;
        name = GetQualifiedIdentifier(name_node);

      switch (drop_stmt->getId()) {
        case JJTTABLE:
          if (GetFirstChildNode(stmt, JJTIF_EXISTS) != nullptr) {
            statement->mutable_drop_table()->set_existence_modifier(IF_EXISTS);
          }
          statement->mutable_drop_table()->set_table_name(name);
          break;
        case JJTINDEX:
          if (GetFirstChildNode(stmt, JJTIF_EXISTS) != nullptr) {
            statement->mutable_drop_index()->set_existence_modifier(IF_EXISTS);
          }
          statement->mutable_drop_index()->set_index_name(name);
          break;
        case JJTSEARCH_INDEX:
          if (GetFirstChildNode(stmt, JJTIF_EXISTS) != nullptr) {
            statement->mutable_drop_index()->set_existence_modifier(IF_EXISTS);
          }
          statement->mutable_drop_index()->set_index_name(name);
          break;
        case JJTCHANGE_STREAM:
          statement->mutable_drop_change_stream()->set_change_stream_name(name);
          break;
        case JJTROLE:
          statement->mutable_drop_role()->set_role_name(name);
          break;
        case JJTVIEW:
          if (GetFirstChildNode(stmt, JJTIF_EXISTS) != nullptr) {
            statement->mutable_drop_function()->set_existence_modifier(
                IF_EXISTS);
          }
          statement->mutable_drop_function()->set_function_kind(Function::VIEW);
          statement->mutable_drop_function()->set_function_name(name);
          break;
        case JJTFUNCTION:
          if (!EmulatorFeatureFlags::instance()
                   .flags()
                   .enable_user_defined_functions) {
            errors->push_back("User defined functions are not supported.");
            return;
          }
          if (GetFirstChildNode(stmt, JJTIF_EXISTS) != nullptr) {
            statement->mutable_drop_function()->set_existence_modifier(
                IF_EXISTS);
          }
          statement->mutable_drop_function()->set_function_kind(
              Function::FUNCTION);
          statement->mutable_drop_function()->set_function_name(name);
          break;
        case JJTMODEL: {
          if (GetFirstChildNode(stmt, JJTIF_EXISTS) != nullptr) {
            statement->mutable_drop_model()->set_if_exists(true);
          }
          statement->mutable_drop_model()->set_model_name(name);
          break;
        }
        case JJTPROTO_BUNDLE:
          statement->mutable_drop_proto_bundle();  // No fields.
          break;
        case JJTSEQUENCE:
          if (GetFirstChildNode(stmt, JJTIF_EXISTS) != nullptr) {
            statement->mutable_drop_sequence()->set_existence_modifier(
                IF_EXISTS);
          }
          statement->mutable_drop_sequence()->set_sequence_name(name);
          break;
        case JJTSCHEMA:
          if (GetFirstChildNode(stmt, JJTIF_EXISTS) != nullptr) {
            statement->mutable_drop_schema()->set_if_exists(true);
          }
          statement->mutable_drop_schema()->set_schema_name(name);
          break;
        default:
          ABSL_LOG(FATAL) << "Unexpected object type: "
                     << GetChildNode(stmt, 0)->toString();
      }
    } break;
    case JJTALTER_PROTO_BUNDLE_STATEMENT: {
      AlterProtoBundle* alter_proto_bundle =
          statement->mutable_alter_proto_bundle();
      VisitAlterProtoBundleNode(stmt, alter_proto_bundle, errors);
      break;
    }
    case JJTALTER_DATABASE_STATEMENT:
      VisitAlterDatabaseNode(stmt, statement->mutable_alter_database(), errors);
      break;
    case JJTALTER_TABLE_STATEMENT:
      VisitAlterTableNode(stmt, ddl_text, statement, errors);
      break;
    case JJTALTER_MODEL_STATEMENT:
      VisitAlterModelNode(stmt, statement->mutable_alter_model(), errors);
      break;
    case JJTALTER_CHANGE_STREAM_STATEMENT:
      VisitAlterChangeStreamNode(stmt, statement->mutable_alter_change_stream(),
                                 errors);
      break;
    case JJTALTER_SEQUENCE_STATEMENT:
      VisitAlterSequenceNode(stmt, statement->mutable_alter_sequence(), errors);
      break;
    case JJTALTER_SCHEMA_STATEMENT:
      VisitAlterSchemaNode(stmt, statement->mutable_alter_schema(), errors);
      break;
    case JJTANALYZE_STATEMENT:
      CheckNode(stmt, JJTANALYZE_STATEMENT);
      statement->mutable_analyze();
      break;
    case JJTCREATE_OR_REPLACE_STATEMENT: {
      bool has_or_replace = (GetFirstChildNode(stmt, JJTOR_REPLACE) != nullptr);
      const SimpleNode* actual_stmt =
          GetChildNode(stmt, has_or_replace ? 1 : 0);
      switch (actual_stmt->getId()) {
        case JJTCREATE_VIEW_STATEMENT:
          VisitCreateViewNode(actual_stmt, statement->mutable_create_function(),
                              has_or_replace, ddl_text, errors);
          break;
        case JJTCREATE_MODEL_STATEMENT:
          VisitCreateModelNode(actual_stmt, statement->mutable_create_model(),
                               has_or_replace, ddl_text, errors);
          break;
        case JJTCREATE_SCHEMA_STATEMENT:
          VisitCreateSchemaNode(actual_stmt, statement->mutable_create_schema(),
                                errors);
          break;
        case JJTCREATE_FUNCTION_STATEMENT:
          VisitCreateFunctionNode(actual_stmt,
                                  statement->mutable_create_function(),
                                  has_or_replace, ddl_text, errors);
          break;
        default:
          ABSL_LOG(FATAL) << "Unexpected statement: " << stmt->toString();
      }
      break;
    }
    case JJTCREATE_ROLE_STATEMENT: {
      ABSL_CHECK_EQ(stmt->jjtGetNumChildren(), 1);

      auto* create_role = statement->mutable_create_role();
      SimpleNode* name = GetChildNode(stmt, 0);
      ABSL_CHECK_EQ(name->getId(), JJTNAME);
      create_role->set_role_name(name->image());
      break;
    }
    default:
      ABSL_LOG(FATAL) << "Unexpected statement: " << stmt->toString();
  }
}

absl::Status UnvalidatedParseCloudDDLStatement(absl::string_view ddl,
                                               DDLStatement* statement) {
  // Special case: JavaCC doesn't like parsing a completely empty string. Return
  // an error immediately instead.
  if (ddl.empty()) {
    return error::EmptyDDLStatement();
  }

  // Create the JavaCC generated parser.
  DDLCharStream char_stream(ddl);
  DDLParserTokenManager token_manager(&char_stream);
  DDLParser parser(&token_manager);

  std::vector<std::string> errors;
  // The parser owns the error handler and deletes it.
  parser.setErrorHandler(new CloudDDLErrorHandler(&errors));

  SimpleNode* node = parser.ParseDDL();
  if (node == nullptr) {
    // NULL means error from JavaCC. "errors" contains parse issues.
    if (errors.empty()) errors.push_back("Unknown error.");
    return error::DDLStatementWithErrors(ddl, errors);
  }

  statement->Clear();
  BuildCloudDDLStatement(node, ddl, statement, &errors);
  return error::DDLStatementWithErrors(ddl, errors);
}

}  // namespace

absl::Status ParseDDLStatement(absl::string_view ddl, DDLStatement* statement) {
  return UnvalidatedParseCloudDDLStatement(ddl, statement);
}

}  // namespace ddl
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
