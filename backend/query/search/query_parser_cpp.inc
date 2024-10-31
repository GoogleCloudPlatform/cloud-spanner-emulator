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
  NODE_SCOPE_HOOK = true;
  VISITOR = true;
  NODE_INCLUDES =
    "backend/query/search/query_node_base.h";
  USER_CHAR_STREAM = true;
  NODE_EXTENDS = "QueryNodeBase";
  NAMESPACE = "google::spanner::emulator::backend::query::search";
  TOKEN_INCLUDES =
    "backend/query/search/query_char_stream.h";
  TOKEN_EXTENDS = "QueryTokenBase";
  COMMON_TOKEN_ACTION = true;
  DEBUG_PARSER = false;
}
PARSER_BEGIN(SearchQueryParser)
private:
void jjtreeOpenNodeScope(Node* node) {
  SimpleNode* sn = dynamic_cast<SimpleNode*>(node);
  Token* token = getToken(1);
  if (token != nullptr) {
    sn->SetLocationInfo(*token);
    sn->set_image(token->image);
  }
}

void jjtreeCloseNodeScope(Node* node) {
  SimpleNode* sn = dynamic_cast<SimpleNode*>(node);
  Token* token = getToken(0);
  if (token != nullptr) {
    sn->UpdateLocationInfo(*token);
  }
}

public:
SimpleNode* GetParseResult() {
  Node* result = nullptr;
  if (errorHandler->getErrorCount() == 0) {
    result = jjtree.popNode();
    ABSL_CHECK_NE(nullptr, result);
  }

  return dynamic_cast<SimpleNode*>(result);
}

SimpleNode* ParseSearchQuery() {
  search_query();
  return GetParseResult();
}

private:
PARSER_END(SearchQueryParser)

TOKEN_MGR_DECLS : {
  void CommonTokenAction(Token * t) {
    const SearchQueryCharStream* stream =
        dynamic_cast<SearchQueryCharStream*>(input_stream);
    t->set_absolute_position(stream->token_begin(), t->image.size());
  }
}
