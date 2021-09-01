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

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "zetasql/base/logging.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/escaping.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "backend/schema/ddl/operations.pb.h"
#include "backend/schema/parser/DDLParserTree.h"
#include "backend/schema/parser/ddl_includes.h"
#include "backend/schema/parser/ddl_token_validation_utils.h"
#include "common/errors.h"
#include "common/feature_flags.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace ddl {

const char kCommitTimestampOptionName[] = "allow_commit_timestamp";

namespace {

// Determines whether the given string is printable.
bool IsPrintable(absl::string_view str) {
  const char* strp = str.data();
  const char* end = strp + str.size();
  // Check that each character in the string is printable.
  while (strp < end) {
    if (!absl::ascii_isprint(*strp++)) {
      return false;
    }
  }
  return true;
}

// Returns the image of the token with special handling of EOF.
static std::string GetTokenRepresentation(Token* token) {
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
    } else if (!IsPrintable(token_str)) {
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
    return absl::StrCat("Syntax error on line ", token->beginLine, ", column ",
                        token->beginColumn,
                        ": Encountered an unclosed triple quoted string.");
  }

  return absl::StrCat("'", token->image, "'");
}

class DDLErrorHandler : public ErrorHandler {
 public:
  explicit DDLErrorHandler(std::vector<std::string>* errors)
      : errors_(errors), ignore_further_errors_(false) {}
  ~DDLErrorHandler() override {}

  void handleUnexpectedToken(int expected_kind,
                             JAVACC_STRING_TYPE expected_token, Token* actual,
                             DDLParser* parser) override {
    if (ignore_further_errors_) {
      return;
    }
    // expected_kind is -1 when the next token is not expected, when choosing
    // the next rule based on next token. Every invocation of
    // handleUnexpectedToken with expeced_kind=-1 is followed by a call to
    // handleParserError. We process the error there.
    if (expected_kind == -1) {
      return;
    }

    // The parser would continue to throw unexpected token at us but only the
    // first error is the cause.
    ignore_further_errors_ = true;

    errors_->push_back(
        absl::StrCat("Syntax error on line ", actual->beginLine, ", column ",
                     actual->beginColumn, ": Expecting '",
                     absl::AsciiStrToUpper(expected_token), "' but found ",
                     GetTokenRepresentation(actual)));
  }

  void handleParseError(Token* last, Token* unexpected,
                        JAVACC_STRING_TYPE production,
                        DDLParser* parser) override {
    if (ignore_further_errors_) {
      return;
    }
    ignore_further_errors_ = true;

    errors_->push_back(absl::StrCat(
        "Syntax error on line ", unexpected->beginLine, ", column ",
        unexpected->beginColumn, ": Encountered ",
        GetTokenRepresentation(unexpected), " while parsing: ", production));
  }

  int getErrorCount() override { return errors_->size(); }

 private:
  std::vector<std::string>* errors_;
  bool ignore_further_errors_;
};

zetasql_base::StatusOr<std::unique_ptr<SimpleNode>> ParseDDLStatementToNode(
    absl::string_view statement) {
  // Empty DDL statements are not allowed in Cloud Spanner.
  if (statement.empty()) {
    return error::EmptyDDLStatement();
  }

  // Instantiate a new DDLParser with given statement and custom error handler.
  std::vector<std::string> errors;
  DDLErrorHandler error_handler(&errors);
  DDLParser parser(new DDLParserTokenManager(new DDLCharStream(statement)));
  parser.setErrorHandler(&error_handler);

  // Parse the input statement as a valid DDL statement into a node.
  std::unique_ptr<SimpleNode> node =
      absl::WrapUnique<SimpleNode>(parser.ParseDDL());
  if (node == nullptr) {
    if (errors.empty()) {
      errors.push_back("Unknown error while parsing ddl statement into node.");
    }
    return error::DDLStatementWithErrors(statement, errors);
  }
  return std::move(node);
}

absl::Status CheckNodeType(const SimpleNode* node, int expected_type) {
  if (node->getId() != expected_type) {
    return error::Internal(
        absl::StrCat("Expected '", jjtNodeName[expected_type], "', found '",
                     jjtNodeName[node->getId()], "'"));
  }
  return absl::OkStatus();
}

zetasql_base::StatusOr<const SimpleNode*> GetChildAtIndex(const SimpleNode* parent,
                                                  int pos) {
  if (pos >= parent->jjtGetNumChildren()) {
    return error::Internal(absl::StrCat("Out of bounds children pos [", pos,
                                        "], total number of children ",
                                        parent->jjtGetNumChildren()));
  }
  return dynamic_cast<SimpleNode*>(parent->jjtGetChild(pos));
}

zetasql_base::StatusOr<const SimpleNode*> GetChildAtIndexWithType(
    const SimpleNode* parent, int pos, int expected_type) {
  ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* child, GetChildAtIndex(parent, pos));
  ZETASQL_RETURN_IF_ERROR(CheckNodeType(child, expected_type));
  return child;
}

zetasql_base::StatusOr<const SimpleNode*> GetFirstChildWithType(
    const SimpleNode* parent, int type) {
  for (int i = 0; i < parent->jjtGetNumChildren(); i++) {
    ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* child, GetChildAtIndex(parent, i));
    if (child->getId() == type) {
      return child;
    }
  }
  return nullptr;
}

void SetColumnLength(const SimpleNode* length_node, ColumnDefinition* column,
                     std::vector<std::string>* errors) {
  if (length_node == nullptr) {
    return;
  }
  if (absl::AsciiStrToUpper(length_node->image()) == "MAX") {
    return;
  }

  int64_t length = length_node->image_as_int64();
  if (length < 0) {
    errors->push_back(
        absl::StrCat("Invalid length for column: ", column->column_name(),
                     ", found: ", length_node->image()));
    return;
  }
  column->add_constraints()->mutable_column_length()->set_max_length(length);
}

absl::Status VisitColumnTypeNode(const SimpleNode* column_type_node,
                                 ColumnDefinition* column_definition,
                                 ColumnType* column_type, int recursion_depth,
                                 std::vector<std::string>* errors) {
  if (recursion_depth > 4) {
    return error::Internal("DDL parser exceeded max recursion stack depth");
  }

  // Parse column type.
  std::string type_name = absl::AsciiStrToUpper(column_type_node->image());
  ColumnType::Type type;
  if (!ColumnType::Type_Parse(type_name, &type)) {
    return error::Internal(
        absl::StrCat("Unrecognized column type: ", type_name));
  }
  column_type->set_type(type);

  if (type == ColumnType::ARRAY) {
    // If column type is Array, recursively set column subtypes.
    ZETASQL_ASSIGN_OR_RETURN(
        const SimpleNode* column_subtype_node,
        GetChildAtIndexWithType(column_type_node, 0, JJTCOLUMN_TYPE));
    ZETASQL_RETURN_IF_ERROR(VisitColumnTypeNode(column_subtype_node, column_definition,
                                        column_type->mutable_array_subtype(),
                                        recursion_depth + 1, errors));
  } else {
    // Set column length constraints since this is a leaf column type node and
    // would contain length constraints if applicable.
    ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* length_node,
                     GetFirstChildWithType(column_type_node, JJTLENGTH));
    SetColumnLength(length_node, column_definition, errors);
  }
  return absl::OkStatus();
}

absl::string_view GetExpressionStr(const SimpleNode& expression_node,
                                   absl::string_view ddl_text) {
  int node_offset = expression_node.absolute_begin_column();
  int node_length = expression_node.absolute_end_column() - node_offset;
  return absl::ClippedSubstr(ddl_text, node_offset, node_length);
}

absl::Status VisitGenerationClauseNode(const SimpleNode* node,
                                       ColumnDefinition* column_definition,
                                       absl::string_view ddl_text) {
  ZETASQL_RETURN_IF_ERROR(CheckNodeType(node, JJTGENERATION_CLAUSE));
  if (!EmulatorFeatureFlags::instance()
           .flags()
           .enable_stored_generated_columns) {
    return error::GeneratedColumnsNotEnabled();
  }
  bool is_stored = false;
  for (int i = 0; i < node->jjtGetNumChildren(); ++i) {
    ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* child, GetChildAtIndex(node, i));
    switch (child->getId()) {
      case JJTEXPRESSION: {
        column_definition->mutable_properties()->set_expression(
            absl::StrCat("(", GetExpressionStr(*child, ddl_text), ")"));
        break;
      }
      case JJTSTORED: {
        is_stored = true;
        break;
      }
      default:
        return error::Internal(absl::StrCat(
            "Unexpected generated column info: ", child->toString()));
    }
  }
  if (!is_stored) {
    return error::NonStoredGeneratedColumnUnsupported(
        column_definition->column_name());
  }

  return absl::OkStatus();
}
zetasql_base::StatusOr<std::string> GetOptionName(const SimpleNode* node) {
  ZETASQL_RETURN_IF_ERROR(CheckNodeType(node, JJTOPTION_KEY_VAL));
  if (node->jjtGetNumChildren() < 2) {
    return error::Internal(absl::StrCat(
        "Expected at least 2 children for node OPTION_KEY_VAL, found ",
        node->jjtGetNumChildren(), " children."));
  }
  ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* key,
                   GetChildAtIndexWithType(node, 0, JJTKEY));
  return key->image();
}

absl::Status VisitColumnOptionKeyValNode(const SimpleNode* node,
                                         Options* options) {
  // Check that option set on column is kCommitTimestampOptionName. When
  // multiple commit timestamp options are set, last value is applied.
  ZETASQL_ASSIGN_OR_RETURN(std::string option_name, GetOptionName(node));
  if (option_name != kCommitTimestampOptionName) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        absl::StrCat("Option: ", option_name, " is unknown."));
  }
  Options::Option* option = options->add_option_val();
  option->set_name(kCommitTimestampOptionName);

  // Set the tri-state boolean value for kAllowCommitTimestamp option.
  ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* bool_node, GetChildAtIndex(node, 1));
  switch (bool_node->getId()) {
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
      return absl::Status(
          absl::StatusCode::kInvalidArgument,
          absl::StrCat("Unexpected value for option: ", option_name,
                       ", supported values are true, false, and null."));
    }
  }
  return absl::OkStatus();
}

absl::Status VisitColumnOptionListNode(const SimpleNode* node,
                                       Options* options) {
  ZETASQL_RETURN_IF_ERROR(CheckNodeType(node, JJTOPTIONS_CLAUSE));
  for (int i = 0; i < node->jjtGetNumChildren(); ++i) {
    ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* option_node,
                     GetChildAtIndexWithType(node, i, JJTOPTION_KEY_VAL));
    ZETASQL_RETURN_IF_ERROR(VisitColumnOptionKeyValNode(option_node, options));
  }
  return absl::OkStatus();
}

absl::Status VisitColumnNode(const SimpleNode* node,
                             ColumnDefinition* column_definition,
                             absl::string_view ddl_text,
                             std::vector<std::string>* errors) {
  ZETASQL_RETURN_IF_ERROR(CheckNodeType(node, JJTCOLUMN_DEF));

  // Set optional column constraints and properties.
  for (int i = 0; i < node->jjtGetNumChildren(); ++i) {
    ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* child, GetChildAtIndex(node, i));
    switch (child->getId()) {
      case JJTNAME:
        column_definition->set_column_name(child->image());
        break;
      case JJTCOLUMN_TYPE:
        ZETASQL_RETURN_IF_ERROR(VisitColumnTypeNode(
            child, column_definition,
            column_definition->mutable_properties()->mutable_column_type(), 0,
            errors));
        break;
      case JJTNOT_NULL:
        column_definition->add_constraints()->mutable_not_null()->set_nullable(
            false);
        break;
      case JJTGENERATION_CLAUSE:
        ZETASQL_RETURN_IF_ERROR(
            VisitGenerationClauseNode(child, column_definition, ddl_text));
        break;
      case JJTOPTIONS_CLAUSE:
        ZETASQL_RETURN_IF_ERROR(VisitColumnOptionListNode(
            child, column_definition->mutable_options()));
        break;
      default:
        return error::Internal(
            absl::StrCat("Unexpected column info: ", child->toString()));
    }
  }
  return absl::OkStatus();
}

absl::Status VisitKeyNode(const SimpleNode* node,
                          PrimaryKeyConstraint* primary_key_constraint) {
  for (int i = 0; i < node->jjtGetNumChildren(); i++) {
    // Add key column to the given primary key.
    ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* key_part_node,
                     GetChildAtIndexWithType(node, i, JJTKEY_PART));
    PrimaryKeyConstraint::KeyPart* key_part =
        primary_key_constraint->add_key_part();

    // Set columns names for given primary key.
    ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* key_column,
                     GetChildAtIndexWithType(key_part_node, 0, JJTPATH));
    key_part->set_key_column_name(key_column->image());

    // Set sort order for the key column.
    ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* sort_order_desc,
                     GetFirstChildWithType(key_part_node, JJTDESC));
    if (sort_order_desc != nullptr) {
      key_part->set_order(PrimaryKeyConstraint::DESC);
    }
  }
  return absl::OkStatus();
}

absl::Status VisitOnDeleteClause(const SimpleNode* node,
                                 OnDeleteAction* on_delete_action) {
  ZETASQL_RETURN_IF_ERROR(CheckNodeType(node, JJTON_DELETE_CLAUSE));

  for (int i = 0; i < node->jjtGetNumChildren(); i++) {
    ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* child, GetChildAtIndex(node, i));
    switch (child->getId()) {
      case JJTNO_ACTION:
        on_delete_action->set_action(OnDeleteAction::NO_ACTION);
        break;
      case JJTCASCADE:
        on_delete_action->set_action(OnDeleteAction::CASCADE);
        break;
      default:
        return error::Internal(absl::StrCat(
            "ON DELETE does not specify a valid behavior: ", node->toString()));
    }
  }
  return absl::OkStatus();
}

absl::Status VisitTableInterleaveNode(const SimpleNode* node,
                                      InterleaveConstraint* interleave) {
  // Set interleave relationships.
  ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* parent_table_node,
                   GetChildAtIndexWithType(node, 0, JJTINTERLEAVE_IN));
  interleave->set_parent(parent_table_node->image());
  interleave->set_type(InterleaveConstraint::IN_PARENT);

  // Set on delete behavior for interleaved table.
  OnDeleteAction on_delete_action = OnDeleteAction{};
  on_delete_action.set_action(OnDeleteAction::NO_ACTION);
  ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* on_delete_node,
                   GetFirstChildWithType(node, JJTON_DELETE_CLAUSE));
  if (on_delete_node != nullptr) {
    ZETASQL_RETURN_IF_ERROR(VisitOnDeleteClause(on_delete_node, &on_delete_action));
  }
  *interleave->mutable_on_delete() = on_delete_action;
  return absl::OkStatus();
}

absl::Status VisitStoredColumnNode(const SimpleNode* node,
                                   ColumnDefinition* def) {
  ZETASQL_RETURN_IF_ERROR(CheckNodeType(node, JJTSTORED_COLUMN));
  if (node->jjtGetNumChildren() != 1) {
    return error::Internal(absl::StrCat(
        "Expect exactly 1 child for STORED_COLUMN node: ", node->toString(),
        ", found: ", node->jjtGetNumChildren(), "."));
  }

  // Get name of stored column and set the stored column properties.
  ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* name_node,
                   GetChildAtIndexWithType(node, 0, JJTPATH));
  def->set_column_name(name_node->image());
  def->mutable_properties()->set_stored(name_node->image());
  return absl::OkStatus();
}

absl::Status VisitStoredColumnListNode(const SimpleNode* node,
                                       CreateIndex* index) {
  ZETASQL_RETURN_IF_ERROR(CheckNodeType(node, JJTSTORED_COLUMN_LIST));
  if (node->jjtGetNumChildren() == 0) {
    return error::Internal(
        absl::StrCat("Expect at least 1 child for STORED_COLUMN_LIST node: ",
                     node->toString()));
  }

  // Populate each of the stored columns in the index.
  for (int i = 0; i < node->jjtGetNumChildren(); i++) {
    ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* stored_column,
                     GetChildAtIndexWithType(node, i, JJTSTORED_COLUMN));
    ColumnDefinition* column_definition = index->add_columns();
    ZETASQL_RETURN_IF_ERROR(VisitStoredColumnNode(stored_column, column_definition));
  }
  return absl::OkStatus();
}

absl::Status VisitIndexInterleaveNode(const SimpleNode* node,
                                      CreateIndex* index) {
  ZETASQL_RETURN_IF_ERROR(CheckNodeType(node, JJTINDEX_INTERLEAVE_CLAUSE));
  if (node->jjtGetNumChildren() != 1) {
    return error::Internal(absl::StrCat(
        "Expect exactly 1 child corresponding to parent table name for "
        "INDEX_INTERLEAVE_CLAUSE node: ",
        node->toString(), ", found: ", node->jjtGetNumChildren(), "."));
  }

  // Set name of parent table in which index is interleaved.
  ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* parent_name_node,
                   GetChildAtIndexWithType(node, 0, JJTINTERLEAVE_IN));
  index->add_constraints()->mutable_interleave()->set_parent(
      parent_name_node->image());
  return absl::OkStatus();
}

absl::Status AddForeignKeyColumnNames(
    const SimpleNode* child, std::function<void(const std::string&)> add) {
  ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* names,
                   GetChildAtIndexWithType(child, 0, JJTIDENTIFIER_LIST));
  for (int i = 0; i < names->jjtGetNumChildren(); i++) {
    ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* name,
                     GetChildAtIndexWithType(names, i, JJTIDENTIFIER));
    add(name->image());
  }
  return absl::OkStatus();
}

absl::Status VisitForeignKeyNode(const SimpleNode* node,
                                 ForeignKeyConstraint* foreign_key) {
  ZETASQL_RETURN_IF_ERROR(CheckNodeType(node, JJTFOREIGN_KEY));
  for (int i = 0; i < node->jjtGetNumChildren(); ++i) {
    ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* child, GetChildAtIndex(node, i));
    switch (child->getId()) {
      case JJTCONSTRAINT_NAME:
        foreign_key->set_constraint_name(child->image());
        break;
      case JJTREFERENCING_COLUMNS:
        ZETASQL_RETURN_IF_ERROR(AddForeignKeyColumnNames(
            child, [&foreign_key](const std::string& name) {
              foreign_key->add_referencing_column_name(name);
            }));
        break;
      case JJTREFERENCED_TABLE:
        foreign_key->set_referenced_table_name(child->image());
        break;
      case JJTREFERENCED_COLUMNS:
        ZETASQL_RETURN_IF_ERROR(AddForeignKeyColumnNames(
            child, [&foreign_key](const std::string& name) {
              foreign_key->add_referenced_column_name(name);
            }));
        break;
      default:
        return error::Internal(
            absl::StrCat("Unexpected foreign key node: ", child->toString()));
    }
  }
  return absl::OkStatus();
}

absl::Status VisitCheckConstraintNode(const SimpleNode* node,
                                      absl::string_view ddl_text,
                                      CheckConstraint* check_constraint) {
  ZETASQL_RETURN_IF_ERROR(CheckNodeType(node, JJTCHECK_CONSTRAINT));
  if (!EmulatorFeatureFlags::instance().flags().enable_check_constraint) {
    return error::CheckConstraintNotEnabled();
  }

  for (int i = 0; i < node->jjtGetNumChildren(); i++) {
    ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* child, GetChildAtIndex(node, i));
    switch (child->getId()) {
      case JJTCONSTRAINT_NAME:
        check_constraint->set_constraint_name(child->image());
        break;
      case JJTCHECK_CONSTRAINT_EXPRESSION: {
        check_constraint->set_sql_expression(
            std::string(GetExpressionStr(*child, ddl_text)));  // NOLINT
        break;
      }
      default:
        return error::Internal(absl::StrCat(
            "Unexpected check constraint attribute: ", child->toString()));
    }
  }
  return absl::OkStatus();
}

absl::Status VisitCreateTableNode(const SimpleNode* node, CreateTable* table,
                                  absl::string_view ddl_text,
                                  std::vector<std::string>* errors) {
  ZETASQL_RETURN_IF_ERROR(CheckNodeType(node, JJTCREATE_TABLE_STATEMENT));

  // Parse children nodes to set corresponding properties of table.
  for (int i = 0; i < node->jjtGetNumChildren(); i++) {
    ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* child, GetChildAtIndex(node, i));
    switch (child->getId()) {
      case JJTNAME:
        table->set_table_name(child->image());
        break;
      case JJTCOLUMN_DEF:
        ZETASQL_RETURN_IF_ERROR(
            VisitColumnNode(child, table->add_columns(), ddl_text, errors));
        break;
      case JJTFOREIGN_KEY:
        ZETASQL_RETURN_IF_ERROR(VisitForeignKeyNode(
            child, table->add_constraints()->mutable_foreign_key()));
        break;
      case JJTCHECK_CONSTRAINT:
        ZETASQL_RETURN_IF_ERROR(VisitCheckConstraintNode(
            child, ddl_text, table->add_constraints()->mutable_check()));
        break;
      case JJTPRIMARY_KEY:
        ZETASQL_RETURN_IF_ERROR(VisitKeyNode(
            child, table->add_constraints()->mutable_primary_key()));
        break;
      case JJTTABLE_INTERLEAVE_CLAUSE:
        ZETASQL_RETURN_IF_ERROR(VisitTableInterleaveNode(
            child, table->add_constraints()->mutable_interleave()));
        break;
      default:
        return error::Internal(
            absl::StrCat("Unexpected table info: ", child->toString()));
    }
  }
  return absl::OkStatus();
}

absl::Status VisitCreateIndexNode(const SimpleNode* node, CreateIndex* index) {
  ZETASQL_RETURN_IF_ERROR(CheckNodeType(node, JJTCREATE_INDEX_STATEMENT));

  // Parse children nodes to set corresponding properties of index.
  for (int i = 0; i < node->jjtGetNumChildren(); i++) {
    ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* child, GetChildAtIndex(node, i));
    switch (child->getId()) {
      case JJTUNIQUE_INDEX:
        index->mutable_properties()->set_unique(true);
        break;
      case JJTNULL_FILTERED:
        index->mutable_properties()->set_null_filtered(true);
        break;
      case JJTNAME:
        index->set_index_name(child->image());
        break;
      case JJTTABLE:
        index->set_table_name(child->image());
        break;
      case JJTCOLUMNS:
        ZETASQL_RETURN_IF_ERROR(VisitKeyNode(
            child, index->add_constraints()->mutable_primary_key()));
        break;
      case JJTSTORED_COLUMN_LIST:
        ZETASQL_RETURN_IF_ERROR(VisitStoredColumnListNode(child, index));
        break;
      case JJTINDEX_INTERLEAVE_CLAUSE:
        ZETASQL_RETURN_IF_ERROR(VisitIndexInterleaveNode(child, index));
        break;
      default:
        return error::Internal(
            absl::StrCat("Unexpected index info: ", child->toString()));
    }
  }
  return absl::OkStatus();
}

absl::Status VisitDropStatementNode(const SimpleNode* node,
                                    DDLStatement* ddl_statement) {
  // Name of the object to drop.
  ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* name_node,
                   GetFirstChildWithType(node, JJTNAME));

  // Find type of object to drop, and set corresponding fields in ddl_statement.
  ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* child, GetChildAtIndex(node, 0));
  switch (child->getId()) {
    case JJTTABLE:
      ddl_statement->mutable_drop_table()->set_table_name(name_node->image());
      break;
    case JJTINDEX:
      ddl_statement->mutable_drop_index()->set_index_name(name_node->image());
      break;
    default:
      return error::Internal(absl::StrCat(
          "Unexpected object type for drop statement: ", child->toString()));
  }
  return absl::OkStatus();
}

absl::Status VisitAlterTableAddColumnNode(const SimpleNode* node,
                                          AlterTable* alter_table,
                                          absl::string_view ddl_text,
                                          std::vector<std::string>* errors) {
  ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* column_node,
                   GetChildAtIndexWithType(node, 2, JJTCOLUMN_DEF));
  AlterColumn* alter_column = alter_table->mutable_alter_column();
  alter_column->set_type(AlterColumn::ADD);
  return VisitColumnNode(column_node, alter_column->mutable_column(), ddl_text,
                         errors);
}

absl::Status VisitAlterTableDropColumnNode(const SimpleNode* node,
                                           AlterTable* alter_table) {
  ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* name_node,
                   GetChildAtIndexWithType(node, 2, JJTCOLUMN_NAME));
  AlterColumn* drop_column = alter_table->mutable_alter_column();
  drop_column->set_type(AlterColumn::DROP);
  drop_column->set_column_name(name_node->image());
  return absl::OkStatus();
}

absl::Status VisitAlterColumnAttrsNode(const SimpleNode* node,
                                       ColumnDefinition* column_definition,
                                       absl::string_view ddl_text,
                                       std::vector<std::string>* errors) {
  ZETASQL_RETURN_IF_ERROR(CheckNodeType(node, JJTCOLUMN_DEF_ALTER_ATTRS));

  // Set altered column attributes.
  for (int i = 0; i < node->jjtGetNumChildren(); ++i) {
    ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* child, GetChildAtIndex(node, i));
    switch (child->getId()) {
      case JJTCOLUMN_TYPE:
        ZETASQL_RETURN_IF_ERROR(VisitColumnTypeNode(
            child, column_definition,
            column_definition->mutable_properties()->mutable_column_type(), 0,
            errors));
        break;
      case JJTNOT_NULL:
        column_definition->add_constraints()->mutable_not_null()->set_nullable(
            false);
        break;
      case JJTGENERATION_CLAUSE:
        ZETASQL_RETURN_IF_ERROR(
            VisitGenerationClauseNode(child, column_definition, ddl_text));
        break;
      default:
        return error::Internal(
            absl::StrCat("Unexpected alter column info: ", child->toString()));
    }
  }
  return absl::OkStatus();
}

absl::Status VisitAlterColumnNode(const SimpleNode* node,
                                  AlterColumn* alter_column,
                                  absl::string_view ddl_text,
                                  std::vector<std::string>* errors) {
  ZETASQL_RETURN_IF_ERROR(CheckNodeType(node, JJTCOLUMN_DEF_ALTER));

  // Check the type of alter column and set corresponding properties.
  ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* child, GetChildAtIndex(node, 0));
  switch (child->getId()) {
    case JJTSET_OPTIONS_CLAUSE: {
      ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* column_option_list_node,
                       GetChildAtIndexWithType(node, 1, JJTOPTIONS_CLAUSE));
      ZETASQL_RETURN_IF_ERROR(VisitColumnOptionListNode(
          column_option_list_node,
          alter_column->mutable_column()->mutable_options()));
      break;
    }
    case JJTCOLUMN_DEF_ALTER_ATTRS:
      ZETASQL_RETURN_IF_ERROR(VisitAlterColumnAttrsNode(
          child, alter_column->mutable_column(), ddl_text, errors));
      break;
    default:
      return error::Internal(
          absl::StrCat("Unexpected alter column type: ", child->toString()));
  }
  return absl::OkStatus();
}

absl::Status VisitAlterTableAlterColumnNode(const SimpleNode* node,
                                            AlterTable* alter_table,
                                            absl::string_view ddl_text,
                                            std::vector<std::string>* errors) {
  // Set name of the column to be altered.
  ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* column_name_node,
                   GetChildAtIndexWithType(node, 2, JJTNAME));
  AlterColumn* alter_column = alter_table->mutable_alter_column();
  alter_column->set_column_name(column_name_node->image());

  // Set the same column name in the ColumnDefinition section of an AlterColumn
  // operation as the DDL currently doesn't support renaming columns in ALTER
  // COLUMN.
  alter_column->mutable_column()->set_column_name(column_name_node->image());
  alter_column->set_type(AlterColumn::ALTER);

  // Set altered column definition.
  ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* alter_column_node,
                   GetChildAtIndexWithType(node, 3, JJTCOLUMN_DEF_ALTER));
  return VisitAlterColumnNode(alter_column_node, alter_column, ddl_text,
                              errors);
}

absl::Status VisitAlterTableSetOnDelete(const SimpleNode* node,
                                        AlterTable* alter_table) {
  ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* on_delete_node,
                   GetChildAtIndexWithType(node, 2, JJTON_DELETE_CLAUSE));

  AlterConstraint* alter_constraint = alter_table->mutable_alter_constraint();
  alter_constraint->set_type(AlterConstraint::ALTER);

  return VisitOnDeleteClause(on_delete_node,
                             alter_constraint->mutable_constraint()
                                 ->mutable_interleave()
                                 ->mutable_on_delete());
}

absl::Status VisitAlterTableAddForeignKey(const SimpleNode* node,
                                          AlterTable* alter_table) {
  ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* foreign_key,
                   GetChildAtIndexWithType(node, 1, JJTFOREIGN_KEY));
  ZETASQL_RETURN_IF_ERROR(
      VisitForeignKeyNode(foreign_key, alter_table->mutable_alter_constraint()
                                           ->mutable_constraint()
                                           ->mutable_foreign_key()));
  const std::string& constraint_name = alter_table->alter_constraint()
                                           .constraint()
                                           .foreign_key()
                                           .constraint_name();
  if (!constraint_name.empty()) {
    alter_table->mutable_alter_constraint()->set_constraint_name(
        constraint_name);
  }
  alter_table->mutable_alter_constraint()->set_type(AlterConstraint::ADD);
  return absl::OkStatus();
}

absl::Status VisitAlterTableDropConstraint(const SimpleNode* node,
                                           AlterTable* alter_table) {
  ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* name,
                   GetChildAtIndexWithType(node, 2, JJTCONSTRAINT_NAME));
  alter_table->mutable_alter_constraint()->set_constraint_name(name->image());
  alter_table->mutable_alter_constraint()->set_type(AlterConstraint::DROP);
  return absl::OkStatus();
}

absl::Status VisitAlterTableAddCheckConstraint(const SimpleNode* node,
                                               AlterTable* alter_table,
                                               absl::string_view ddl_text) {
  ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* check_constraint,
                   GetChildAtIndexWithType(node, 1, JJTCHECK_CONSTRAINT));
  ZETASQL_RETURN_IF_ERROR(
      VisitCheckConstraintNode(check_constraint, ddl_text,
                               alter_table->mutable_alter_constraint()
                                   ->mutable_constraint()
                                   ->mutable_check()));
  const std::string& constraint_name =
      alter_table->alter_constraint().constraint().check().constraint_name();
  if (!constraint_name.empty()) {
    alter_table->mutable_alter_constraint()->set_constraint_name(
        constraint_name);
  }
  alter_table->mutable_alter_constraint()->set_type(AlterConstraint::ADD);
  return absl::OkStatus();
}

absl::Status VisitAlterTableNode(const SimpleNode* node,
                                 DDLStatement* statement,
                                 absl::string_view ddl_text,
                                 std::vector<std::string>* errors) {
  ZETASQL_RETURN_IF_ERROR(CheckNodeType(node, JJTALTER_TABLE_STATEMENT));

  // Name of the table to alter.
  ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* table_name_node,
                   GetFirstChildWithType(node, JJTTABLE_NAME));
  AlterTable* alter_table = statement->mutable_alter_table();
  alter_table->set_table_name(table_name_node->image());

  // Find the type of alter table and set corresponding alter values.
  ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* child, GetChildAtIndex(node, 1));
  switch (child->getId()) {
    case JJTADD_COLUMN:
      return VisitAlterTableAddColumnNode(node, alter_table, ddl_text, errors);
    case JJTDROP_COLUMN:
      return VisitAlterTableDropColumnNode(node, alter_table);
    case JJTALTER_COLUMN:
      return VisitAlterTableAlterColumnNode(node, alter_table, ddl_text,
                                            errors);
    case JJTSET_ON_DELETE:
      return VisitAlterTableSetOnDelete(node, alter_table);
    case JJTFOREIGN_KEY:
      return VisitAlterTableAddForeignKey(node, alter_table);
    case JJTCHECK_CONSTRAINT:
      return VisitAlterTableAddCheckConstraint(node, alter_table, ddl_text);
    case JJTDROP_CONSTRAINT:
      return VisitAlterTableDropConstraint(node, alter_table);
    default:
      return error::Internal(
          absl::StrCat("Unexpected alter table type: ", child->toString()));
  }
}

zetasql_base::StatusOr<DDLStatement> BuildDDLStatement(
    const SimpleNode* root, absl::string_view ddl_text,
    std::vector<std::string>* errors) {
  ZETASQL_RETURN_IF_ERROR(CheckNodeType(root, JJTDDL_STATEMENT));
  if (root->jjtGetNumChildren() != 1) {
    return error::Internal(
        absl::StrCat("Expected 1 child of type DDL_STATEMENT, found ",
                     root->jjtGetNumChildren(), " children."));
  }
  ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* child, GetChildAtIndex(root, 0));

  DDLStatement statement;
  switch (child->getId()) {
    case JJTCREATE_TABLE_STATEMENT:
      ZETASQL_RETURN_IF_ERROR(VisitCreateTableNode(
          child, statement.mutable_create_table(), ddl_text, errors));
      break;
    case JJTCREATE_INDEX_STATEMENT:
      ZETASQL_RETURN_IF_ERROR(
          VisitCreateIndexNode(child, statement.mutable_create_index()));
      break;
    case JJTDROP_STATEMENT: {
      ZETASQL_RETURN_IF_ERROR(VisitDropStatementNode(child, &statement));
      break;
    }
    case JJTALTER_TABLE_STATEMENT:
      ZETASQL_RETURN_IF_ERROR(VisitAlterTableNode(child, &statement, ddl_text, errors));
      break;
    default:
      return error::Internal(
          absl::StrCat("Unexpected statement: ", child->toString()));
  }
  return statement;
}

zetasql_base::StatusOr<CreateDatabase> VisitCreateDatabaseNode(const SimpleNode* node) {
  ZETASQL_RETURN_IF_ERROR(CheckNodeType(node, JJTCREATE_DATABASE_STATEMENT));

  CreateDatabase database;
  for (int i = 0; i < node->jjtGetNumChildren(); i++) {
    ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* child, GetChildAtIndex(node, i));
    switch (child->getId()) {
      case JJTDB_NAME:
        database.set_database_name(child->image());
        break;
      default:
        return error::Internal(
            absl::StrCat("Unknown node type: ", node->toString()));
    }
  }
  return database;
}

zetasql_base::StatusOr<CreateDatabase> BuildCreateDatabaseStatement(
    const SimpleNode* root) {
  ZETASQL_RETURN_IF_ERROR(CheckNodeType(root, JJTDDL_STATEMENT));

  // Root node for CreateDatabase should have one create_database child node.
  if (root->jjtGetNumChildren() != 1) {
    return error::Internal(absl::StrCat(
        "Expected 1 child of type CREATE_DATABASE_STATEMENT, found ",
        root->jjtGetNumChildren(), " children."));
  }
  ZETASQL_ASSIGN_OR_RETURN(const SimpleNode* create_database_node,
                   GetChildAtIndex(root, 0));
  return VisitCreateDatabaseNode(create_database_node);
}

}  // namespace

zetasql_base::StatusOr<CreateDatabase> ParseCreateDatabase(
    absl::string_view create_statement) {
  // Parse input statement into the root node.
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<SimpleNode> node,
                   ParseDDLStatementToNode(create_statement));

  // Try to read root node as a valid CreateDatabaseStatement.
  return BuildCreateDatabaseStatement(node.get());
}

zetasql_base::StatusOr<DDLStatement> ParseDDLStatement(absl::string_view input) {
  // Parse input statement into the root node.
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<SimpleNode> node,
                   ParseDDLStatementToNode(input));

  // Try to read root node as a valid DDLStatement.
  std::vector<std::string> errors;
  DDLStatement statement;
  ZETASQL_ASSIGN_OR_RETURN(statement, BuildDDLStatement(node.get(), input, &errors));
  if (!errors.empty()) {
    return error::DDLStatementWithErrors(input, errors);
  }
  return statement;
}

zetasql_base::StatusOr<Schema> ParseDDLStatements(
    absl::Span<const absl::string_view> statements) {
  Schema schema;
  for (absl::string_view statement : statements) {
    ZETASQL_ASSIGN_OR_RETURN(*schema.add_ddl_statements(),
                     ParseDDLStatement(statement));
  }
  return schema;
}

}  // namespace ddl
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
