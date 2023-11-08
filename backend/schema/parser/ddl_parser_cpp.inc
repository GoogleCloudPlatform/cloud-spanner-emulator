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
  OUTPUT_LANGUAGE = "c++";
  STATIC = false;
  IGNORE_CASE = true;  // Case is preserved, but ignored during token matching
  NODE_SCOPE_HOOK = true;
  VISITOR = true;
  TOKEN_MANAGER_INCLUDES =
      "backend/schema/parser/ddl_token_validation.h";
  USER_CHAR_STREAM = true;
  NODE_EXTENDS = "NodeBase";
  NAMESPACE = "google::spanner::emulator::backend::ddl";
  TOKEN_INCLUDES = "backend/schema/parser/ddl_char_stream.h";
  TOKEN_EXTENDS = "TokenBase";
  COMMON_TOKEN_ACTION = true;
}

// The C++ code fragment found between PARSER_BEGIN($name) and PARSER_END
// get generated into the middle of a class called $name in a file called
// $(name).cc.  Options above such as "NAMESPACE", "NODE_INCLUDES" etc
// affect how the code is generated.
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
    Node* result = nullptr;
    if (errorHandler->getErrorCount() == 0) {
      result = jjtree.peekNode();
      ABSL_CHECK_NE(nullptr, result);
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
  void CommonTokenAction(Token * t) {
    const DDLCharStream* stream = dynamic_cast<DDLCharStream*>(input_stream);
    t->set_absolute_position(stream->token_begin(), t->image.size());
  }
}
