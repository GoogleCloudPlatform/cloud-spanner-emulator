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

#include "backend/query/ann_functions_rewriter.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/function_signature.h"
#include "zetasql/public/json_value.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/status/status.h"
#include "common/errors.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

bool IsANNFunction(std::string function_name) {
  if (function_name == "approx_cosine_distance" ||
      function_name == "approx_dot_product" ||
      function_name == "approx_euclidean_distance") {
    return true;
  }
  return false;
}

absl::Status ANNFunctionsRewriter::VisitResolvedFunctionCall(
    const zetasql::ResolvedFunctionCall* node) {
  if (!IsANNFunction(node->function()->Name())) {
    return CopyVisitResolvedFunctionCall(node);
  }
  if (node->argument_list_size() < 3) {
    return error::ApproxDistanceFunctionOptionsRequired(
        node->function()->Name());
  }
  ZETASQL_RET_CHECK(node->argument_list_size() == 3);
  const zetasql::ResolvedExpr* argument_for_placeholder =
      node->argument_list().back().get();
  if (!argument_for_placeholder->Is<zetasql::ResolvedLiteral>()) {
    return error::ApproxDistanceFunctionOptionMustBeLiteral(
        node->function()->Name());
  }
  const zetasql::Value& placeholder_value =
      argument_for_placeholder->GetAs<zetasql::ResolvedLiteral>()->value();
  ZETASQL_RET_CHECK(placeholder_value.has_content());

  if (placeholder_value.type_kind() == zetasql::TYPE_JSON) {
    zetasql::JSONValueConstRef json_value = placeholder_value.json_value();
    if (!json_value.HasMember("num_leaves_to_search")) {
      return error::ApproxDistanceFunctionInvalidJsonOption(
          node->function()->Name());
    }
    zetasql::JSONValueConstRef leaves_json =
        json_value.GetMember("num_leaves_to_search");
    if (!leaves_json.IsUInt64()) {
      return error::ApproxDistanceFunctionInvalidJsonOption(
          node->function()->Name());
    }
  }
  std::vector<std::unique_ptr<zetasql::ResolvedExpr>> argument_list;
  zetasql::FunctionArgumentTypeList argument_types;
  for (int i = 0; i < node->signature().arguments().size() - 1; ++i) {
    ZETASQL_ASSIGN_OR_RETURN(argument_list.emplace_back(),
                     Copy(node->argument_list(i)));
    argument_types.push_back(node->signature().argument(i));
  }
  zetasql::FunctionSignature new_signature(
      node->signature().result_type(), argument_types,
      node->signature().context_id(), node->signature().options());
  std::unique_ptr<zetasql::ResolvedFunctionCall> new_node =
      zetasql::MakeResolvedFunctionCall(
          node->type(), node->function(), new_signature,
          std::move(argument_list), node->error_mode());
  ann_functions_.insert(new_node.get());
  PushNodeToStack(std::move(new_node));
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
