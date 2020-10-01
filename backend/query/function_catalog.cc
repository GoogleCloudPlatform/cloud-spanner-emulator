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
#include "zetasql/public/types/type_factory.h"
#include "absl/container/flat_hash_set.h"
#include "zetasql/base/statusor.h"
#include "absl/strings/ascii.h"
#include "backend/query/analyzer_options.h"
#include "common/constants.h"
#include "absl/status/status.h"

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
  // Add the subset of ZetaSQL built-in functions supported by Cloud Spanner.
  AddZetaSQLBuiltInFunctions(type_factory);
  // Add Cloud Spanner specific functions.
  AddSpannerFunctions();
  // Add aliases for the functions.
  AddFunctionAliases();
}

void FunctionCatalog::AddZetaSQLBuiltInFunctions(
    zetasql::TypeFactory* type_factory) {
  // Get all the ZetaSQL built-in functions.
  std::map<std::string, std::unique_ptr<zetasql::Function>> function_map;
  zetasql::GetZetaSQLFunctions(type_factory, MakeGoogleSqlLanguageOptions(),
                                   &function_map);

  // Move the data from the temporary function_map into functions_, keeping only
  // the functions that are available in Cloud Spanner.
  for (auto iter = function_map.begin(); iter != function_map.end();
       iter = function_map.erase(iter)) {
    functions_[iter->first] = std::move(iter->second);
  }
}

void FunctionCatalog::AddSpannerFunctions() {
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

void FunctionCatalog::AddFunctionAliases() {
  std::vector<std::pair<std::string, std::unique_ptr<zetasql::Function>>>
      aliases;
  for (auto it = functions_.begin(); it != functions_.end(); ++it) {
    const zetasql::Function* original_function = it->second.get();
    if (!original_function->alias_name().empty()) {
      zetasql::FunctionOptions function_options =
          original_function->function_options();
      std::string alias_name = function_options.alias_name;
      function_options.set_alias_name("");
      auto alias_function = absl::make_unique<zetasql::Function>(
          original_function->Name(), original_function->GetGroup(),
          original_function->mode(), original_function->signatures(),
          function_options);
      aliases.emplace_back(
          std::make_pair(alias_name, std::move(alias_function)));
    }
  }

  for (auto& alias : aliases) {
    functions_.insert(std::move(alias));
  }
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
