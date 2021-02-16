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

options {
  STATIC = false;
  IGNORE_CASE = true;
  NODE_SCOPE_HOOK = true;
  VISITOR = true;
  NODE_INCLUDES = "backend/schema/parser/ddl_node_base.h";
  TOKEN_MANAGER_INCLUDES =
      "backend/schema/parser/ddl_token_validation.h";
  USER_CHAR_STREAM = true;
  NODE_EXTENDS = "NodeBase";
  NAMESPACE = "google::spanner::emulator::backend::ddl";
  TOKEN_INCLUDES = "backend/schema/parser/ddl_char_stream.h";
  TOKEN_EXTENDS = "TokenBase";
  COMMON_TOKEN_ACTION = true;
}
PARSER_BEGIN(DDLParser)
 private:
  void jjtreeOpenNodeScope(Node* node) {
    SimpleNode* sn = dynamic_cast<SimpleNode*>(node);
    Token* token = getToken(1);
    sn->SetLocationInfo(token);
    std::string im = token->image;
    if (token->kind == QUOTED_ID) {
      im = im.substr(1, im.length() - 2);
    }
    sn->set_image(im);
  }

  void jjtreeCloseNodeScope(Node* node) {
    SimpleNode* sn = dynamic_cast<SimpleNode*>(node);
    sn->UpdateLocationInfo(getToken(0));
  }

 public:
  SimpleNode* GetParseResult() {
    Node* result;
    if (errorHandler->getErrorCount() == 0) {
      ZETASQL_CHECK_NE(static_cast<Node*>(NULL), jjtree.peekNode());
      result = jjtree.popNode();
    } else {
      result = NULL;
    }
    while (jjtree.peekNode() != NULL) {
      delete jjtree.popNode();
    }

    return dynamic_cast<SimpleNode*>(result);
  }

  SimpleNode* ParseDDL() {
    ddl_statement();
    return GetParseResult();
  }

 private:
PARSER_END(DDLParser)

TOKEN_MGR_DECLS:
{
 public:
  void CommonTokenAction(Token * t) {
    const DDLCharStream* stream = dynamic_cast<DDLCharStream*>(input_stream);
    t->set_absolute_position(stream->token_begin(), t->image.size());
  }
}
