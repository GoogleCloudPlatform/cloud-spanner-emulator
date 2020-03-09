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

#include "backend/query/function_catalog.h"

#include <map>
#include <memory>
#include <string>
#include <utility>

#include "zetasql/public/builtin_function.h"
#include "zetasql/public/builtin_function_options.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/type.h"
#include "absl/container/flat_hash_set.h"
#include "absl/strings/ascii.h"
#include "common/constants.h"
#include "zetasql/base/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

constexpr char kCloudSpannerEmulatorFunctionCatalog[] =
    "CloudSpannerEmulatorCatalog";

zetasql_base::StatusOr<zetasql::Value> EvalPendingCommitTimestamp(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.empty());

  // Timestamp returned by this function is ignored later by query engine and is
  // replaced by kCommitTimestampIdentifier sentinel string as expected by cloud
  // spanner. Note that this function cannot return a string sentinel here since
  // googlesql evaluator expects a timestamp value for the corresponding column.
  return zetasql::Value::Timestamp(zetasql::types::TimestampMinBaseTime());
}

std::unique_ptr<zetasql::Function> PendingCommitTimestampFunction() {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      zetasql::FunctionEvaluator(EvalPendingCommitTimestamp));

  return absl::make_unique<zetasql::Function>(
      kPendingCommitTimestampFunctionName, kCloudSpannerEmulatorFunctionCatalog,
      zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          zetasql::types::TimestampType(), {}, nullptr}},
      function_options);
}

}  // namespace

FunctionCatalog::FunctionCatalog(zetasql::TypeFactory* type_factory) {
  std::map<std::string, std::unique_ptr<zetasql::Function>> function_map;
  zetasql::ZetaSQLBuiltinFunctionOptions options{};
  options.language_options.set_product_mode(zetasql::PRODUCT_EXTERNAL);
  zetasql::GetZetaSQLFunctions(type_factory, options, &function_map);
  // Move the data from the temporary function_map into functions_.
  for (auto iter = function_map.begin(); iter != function_map.end();
       iter = function_map.erase(iter)) {
    functions_[iter->first] = std::move(iter->second);
  }

  // Add pending commit timestamp function to the list of known functions.
  auto pending_commit_ts_func = PendingCommitTimestampFunction();
  functions_[pending_commit_ts_func->Name()] =
      std::move(pending_commit_ts_func);
}

void FunctionCatalog::GetFunction(const std::string& name,
                                  const zetasql::Function** output) const {
  auto function_iter = functions_.find(name);
  *output =
      function_iter == functions_.end() ? nullptr : function_iter->second.get();
}

void FunctionCatalog::GetFunctions(
    absl::flat_hash_set<const zetasql::Function*>* output) const {
  for (const auto& [name, function] : functions_) {
    output->insert(function.get());
  }
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
