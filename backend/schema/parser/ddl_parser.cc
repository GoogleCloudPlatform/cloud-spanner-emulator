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
#include "absl/log/log.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "backend/schema/ddl/operations.pb.h"
#include "backend/schema/parser/DDLParserTree.h"
#include "backend/schema/parser/DDLParserTreeConstants.h"
#include "backend/schema/parser/JavaCC.h"
#include "backend/schema/parser/ddl_includes.h"
#include "backend/schema/parser/ddl_token_validation_utils.h"
#include "common/errors.h"
#include "common/feature_flags.h"
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
const char kSearchIndexOptionSortOrderShardingName[] = "sort_order_sharding";
const char kSearchIndexOptionsDisableAutomaticUidName[] =
    "disable_automatic_uid_column";

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
  ZETASQL_CHECK_LT(pos, parent->jjtGetNumChildren())
      << "[" << pos << "] vs " << parent->jjtGetNumChildren();
  return dynamic_cast<SimpleNode*>(parent->jjtGetChild(pos));
}

void CheckNode(const SimpleNode* node, int expected_type) {
  ZETASQL_CHECK_EQ(node->getId(), expected_type)
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
  ZETASQL_DCHECK(node);
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
  ZETASQL_CHECK_GE(num_children, 2);
  const SimpleNode* key = GetChildNode(node, 0, JJTKEY);
  return key->image();
}

void VisitColumnOptionKeyValNode(const SimpleNode* node, OptionList* options,
                                 std::vector<std::string>* errors) {
  std::string option_name = CheckOptionKeyValNodeAndGetName(node);

  // If this is an invalid option, return error. Later during schema
  // change, we will verify the valid option against the
  // column type.
  if (option_name != kCommitTimestampOptionName) {
    errors->push_back(absl::StrCat("Option: ", option_name, " is unknown."));
    return;
  }

  SetOption* option = options->Add();
  option->set_option_name(kCommitTimestampOptionName);

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
        ZETASQL_LOG(ERROR) << "Unknown node type: " << node->toString();
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
    ZETASQL_LOG(ERROR) << "ON DELETE does not specify a valid behavior: "
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
  ZETASQL_CHECK_EQ(1, node->jjtGetNumChildren());
  *interleave_in_table =
      GetQualifiedIdentifier(GetChildNode(node, 0, JJTINTERLEAVE_IN));
}

void VisitIntervalExpressionNode(const SimpleNode* node, int64_t* days) {
  CheckNode(node, JJTINTERVAL_EXPRESSION);
  ZETASQL_CHECK_EQ(1, node->jjtGetNumChildren());

  *days = GetChildNode(node, 0)->image_as_int64();
}

void VisitRowDeletionPolicyExpressionNode(const SimpleNode* node,
                                          RowDeletionPolicy* policy,
                                          std::vector<std::string>* errors) {
  CheckNode(node, JJTROW_DELETION_POLICY_EXPRESSION);
  ZETASQL_CHECK_EQ(3, node->jjtGetNumChildren());

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
  ZETASQL_CHECK_EQ(1, node->jjtGetNumChildren());
  VisitRowDeletionPolicyExpressionNode(GetChildNode(node, 0), policy, errors);
}

void SetSortOrder(const SimpleNode* key_part_node, KeyPartClause* key_part,
                  std::vector<std::string>* errors) {
  if (GetFirstChildNode(key_part_node, JJTDESC) != nullptr) {
    key_part->set_order(KeyPartClause::DESC);
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
  ZETASQL_CHECK_EQ(num_children, 1);
  def->set_name(GetChildNode(node, 0, JJTPATH)->image());
}

void VisitStoredColumnListNode(
    const SimpleNode* node,
    google::protobuf::RepeatedPtrField<StoredColumnDefinition>* stored_columns,
    std::vector<std::string>* errors) {
  CheckNode(node, JJTSTORED_COLUMN_LIST);
  ZETASQL_CHECK_GT(node->jjtGetNumChildren(), 0);
  for (int i = 0; i < node->jjtGetNumChildren(); i++) {
    SimpleNode* stored_column = GetChildNode(node, i, JJTSTORED_COLUMN);
    VisitStoredColumnNode(stored_column, stored_columns->Add(), errors);
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

void VisitColumnTypeNode(const SimpleNode* column_type,
                         ColumnDefinition* column, int recursion_depth,
                         std::vector<std::string>* errors) {
  if (recursion_depth > 4) {
    errors->push_back("DDL parser exceeded max recursion stack depth");
    return;
  }

  std::string type_name = absl::AsciiStrToUpper(column_type->image());
  ColumnDefinition::Type type;

    if (type_name == "FLOAT64") {
      type = ColumnDefinition::DOUBLE;
    } else if (!ColumnDefinition::Type_Parse(type_name, &type)) {
      ZETASQL_LOG(ERROR) << "Unrecognized type: " << type_name;
    }

  column->set_type(type);
  if (type == ColumnDefinition::ARRAY) {
    // Read the subtype.
    SimpleNode* column_subtype = GetChildNode(column_type, 0, JJTCOLUMN_TYPE);
    VisitColumnTypeNode(column_subtype, column->mutable_array_subtype(),
                        recursion_depth + 1, errors);
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
        ZETASQL_LOG(ERROR) << "Unexpected generated column info: " << child->toString();
    }
  }
}

void VisitColumnDefaultClauseNode(const SimpleNode* node,
                                  ColumnDefinition* column,
                                  absl::string_view ddl_text) {
  CheckNode(node, JJTCOLUMN_DEFAULT_CLAUSE);

  ZETASQL_DCHECK_EQ(node->jjtGetNumChildren(), 1);

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
      default:
        ZETASQL_LOG(ERROR) << "Unexpected column info: " << child->toString();
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
      default:
        ZETASQL_LOG(ERROR) << "Unexpected column info: " << child->toString();
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
  switch (child->getId()) {
    case JJTOPTIONS_CLAUSE: {
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
      break;
    }
    case JJTCOLUMN_DEFAULT_CLAUSE: {
      // "ALTER COLUMN c SET DEFAULT " does not contain the column TYPE or
      // NOT NULL attributes.
      alter_table->set_table_name(table_name);
      AlterTable::AlterColumn* alter_column =
          alter_table->mutable_alter_column();
      ColumnDefinition* column = alter_column->mutable_column();
      alter_column->set_operation(AlterTable::AlterColumn::SET_DEFAULT);
      column->set_column_name(column_name);
      // `type` is required in ColumnDefinition, so set it to NONE here.
      column->set_type(ColumnDefinition::NONE);
      VisitColumnDefaultClauseNode(child, column, ddl_text);
      break;
    }
    case JJTDROP_COLUMN_DEFAULT: {
      // "ALTER COLUMN c DROP DEFAULT " does not contain the column TYPE or
      // NOT NULL attributes.
      alter_table->set_table_name(table_name);
      AlterTable::AlterColumn* alter_column =
          alter_table->mutable_alter_column();
      ColumnDefinition* column = alter_column->mutable_column();
      alter_column->set_operation(AlterTable::AlterColumn::DROP_DEFAULT);
      column->set_column_name(column_name);
      // `type` is required in ColumnDefinition, so set it to NONE here.
      column->set_type(ColumnDefinition::NONE);
      column->clear_column_default();
      break;
    }
    case JJTCOLUMN_DEF_ALTER_ATTRS: {
      // For "ALTER COLUMN c TYPE NOT NULL"
      alter_table->set_table_name(table_name);
      ColumnDefinition* column =
          alter_table->mutable_alter_column()->mutable_column();
      VisitColumnNodeAlterAttrs(child, column_name, column, ddl_text, errors);
      break;
    }
    default:
      ZETASQL_LOG(ERROR) << "Unexpected alter column type: "
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
      ZETASQL_LOG(ERROR) << "Unexpected foreign key action: " << child->toString();
      return ForeignKey::ACTION_UNSPECIFIED;
  }
}

void VisitForeignKeyNode(const SimpleNode* node, ForeignKey* foreign_key,
                         std::vector<std::string>* errors) {
  CheckNode(node, JJTFOREIGN_KEY);
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
      default:
        // We can only get here if there is a bug in the grammar or parser.
        ZETASQL_LOG(ERROR) << "Unexpected foreign key attribute: " << child->toString();
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
        ZETASQL_LOG(ERROR) << "Unexpected check constraint attribute: "
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
      case JJTTABLE_INTERLEAVE_CLAUSE:
        VisitTableInterleaveNode(child, table->mutable_interleave_clause());
        break;
      case JJTROW_DELETION_POLICY_CLAUSE:
        VisitTableRowDeletionPolicyNode(
            child, table->mutable_row_deletion_policy(), errors);
        break;
      default:
        ZETASQL_LOG(ERROR) << "Unexpected table info: " << child->toString();
    }
  }
}

void VisitCreateViewNode(const SimpleNode* node, CreateFunction* function,
                         bool is_or_replace, absl::string_view ddl_text,
                         std::vector<std::string>* errors) {
  CheckNode(node, JJTCREATE_VIEW_STATEMENT);

  const SimpleNode* name = GetFirstChildNode(node, JJTNAME);
  ZETASQL_DCHECK(name);
  function->set_function_name(GetQualifiedIdentifier(name));

  function->set_language(Function::SQL);
  function->set_function_kind(Function::VIEW);

  if (is_or_replace) {
    function->set_is_or_replace(true);
  }

  if (GetFirstChildNode(node, JJTSQL_SECURITY_INVOKER)) {
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
      case JJTIF_NOT_EXISTS:
        index->set_existence_modifier(IF_NOT_EXISTS);
        break;
      default:
        ZETASQL_LOG(ERROR) << "Unexpected index info: " << child->toString();
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
        ZETASQL_LOG(ERROR) << "Unexpected change streams tracked column: "
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
        ZETASQL_LOG(ERROR) << "Unexpected change streams tracked tables entry: "
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
  ZETASQL_CHECK_GE(node->jjtGetNumChildren(), 1);
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
        ZETASQL_LOG(ERROR) << "Unexpected change stream tracked tables: "
                   << child->toString();
    }
  }
}

void VisitChangeStreamForClause(const SimpleNode* node,
                                ChangeStreamForClause* for_clause,
                                std::vector<std::string>* errors) {
  CheckNode(node, JJTCHANGE_STREAM_FOR_CLAUSE);
  ZETASQL_CHECK_EQ(1, node->jjtGetNumChildren());
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
      ZETASQL_LOG(ERROR) << "Unexpected change stream for clause: "
                 << child->toString();
  }
}

bool UnescapeStringLiteral(absl::string_view val, std::string* result) {
  if (val.size() <= 2) {
    ZETASQL_LOG(ERROR) << "Invalid string literal: " << val;
  }
  ZETASQL_CHECK_EQ(val[0], val[val.size() - 1]);
  ZETASQL_CHECK(val[0] == '\'' || val[0] == '"');
  return absl::CUnescape(absl::ClippedSubstr(val, 1, val.size() - 2), result);
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
  if (!UnescapeStringLiteral(value_node->image(), &string_value)) {
    errors->push_back(LogicalError(
        value_node,
        absl::StrCat("Cannot parse string literal: ", value_node->image())));
    return;
  }
  option->set_string_value(string_value);
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

  if (name == kChangeStreamRetentionPeriodOptionName ||
      name == kChangeStreamValueCaptureTypeOptionName) {
    SetOption* option = options->Add();
    option->set_option_name(name);
    VisitStringOrNullOptionValNode(value_node, option, errors);
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
        ZETASQL_LOG(ERROR) << "Unexpected create change stream clause: "
                   << child->toString();
    }
  }
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
      ZETASQL_LOG(ERROR) << "Unexpected alter change stream clause: "
                 << child->toString();
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
      default:
        ZETASQL_LOG(ERROR) << "Unexpected alter table type: "
                   << GetChildNode(node, 1)->toString();
    }
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
    case JJTCREATE_TABLE_STATEMENT:
      VisitCreateTableNode(stmt, statement->mutable_create_table(), ddl_text,
                           errors);
      break;
    case JJTCREATE_INDEX_STATEMENT:
      VisitCreateIndexNode(stmt, statement->mutable_create_index(), errors);
      break;
    case JJTCREATE_CHANGE_STREAM_STATEMENT:
      VisitCreateChangeStreamNode(
          stmt, statement->mutable_create_change_stream(), errors);
      break;
    case JJTDROP_STATEMENT: {
      const SimpleNode* drop_stmt = GetChildNode(stmt, 0);
      std::string name =
          GetQualifiedIdentifier(GetFirstChildNode(stmt, JJTNAME));
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
        case JJTCHANGE_STREAM:
          statement->mutable_drop_change_stream()->set_change_stream_name(name);
          break;
        case JJTVIEW:
          statement->mutable_drop_function()->set_function_kind(Function::VIEW);
          statement->mutable_drop_function()->set_function_name(name);
          break;
        default:
          ZETASQL_LOG(ERROR) << "Unexpected object type: "
                     << GetChildNode(stmt, 0)->toString();
      }
    } break;
    case JJTALTER_TABLE_STATEMENT:
      VisitAlterTableNode(stmt, ddl_text, statement, errors);
      break;
    case JJTALTER_CHANGE_STREAM_STATEMENT:
      VisitAlterChangeStreamNode(stmt, statement->mutable_alter_change_stream(),
                                 errors);
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
        default:
          ZETASQL_LOG(ERROR) << "Unexpected statement: " << stmt->toString();
      }
      break;
    }
    default:
      ZETASQL_LOG(ERROR) << "Unexpected statement: " << stmt->toString();
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
