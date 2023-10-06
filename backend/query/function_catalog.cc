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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/builtin_function.h"
#include "zetasql/public/builtin_function_options.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "backend/query/analyzer_options.h"
#include "common/constants.h"
#include "third_party/spanner_pg/catalog/emulator_functions.h"
#include "zetasql/base/ret_check.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {
using postgres_translator::GetSpannerPGFunctions;
using postgres_translator::SpannerPGFunctions;

absl::StatusOr<zetasql::Value> EvalPendingCommitTimestamp(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK(args.empty());

  // Timestamp returned by this function is ignored later by query engine and is
  // replaced by kCommitTimestampIdentifier sentinel string as expected by cloud
  // spanner. Note that this function cannot return a string sentinel here since
  // googlesql evaluator expects a timestamp value for the corresponding column.
  return zetasql::Value::Timestamp(zetasql::types::TimestampMinBaseTime());
}

std::unique_ptr<zetasql::Function> PendingCommitTimestampFunction(
    const std::string& catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      zetasql::FunctionEvaluator(EvalPendingCommitTimestamp));

  return std::make_unique<zetasql::Function>(
      kPendingCommitTimestampFunctionName, catalog_name,
      zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          zetasql::types::TimestampType(), {}, nullptr}},
      function_options);
}

}  // namespace

FunctionCatalog::FunctionCatalog(zetasql::TypeFactory* type_factory,
                                 const std::string& catalog_name)
    : catalog_name_(catalog_name) {
  // Add the subset of ZetaSQL built-in functions supported by Cloud Spanner.
  AddZetaSQLBuiltInFunctions(type_factory);
  // Add Cloud Spanner specific functions.
  AddSpannerFunctions();
  // Add aliases for the functions.
  AddFunctionAliases();
  AddSpannerPGFunctions(type_factory);
}

void FunctionCatalog::AddZetaSQLBuiltInFunctions(
    zetasql::TypeFactory* type_factory) {
  // Get all the ZetaSQL built-in functions.
  absl::flat_hash_map<std::string, std::unique_ptr<zetasql::Function>>
      function_map;
  absl::flat_hash_map<std::string, const zetasql::Type*> type_map_unused;
  absl::Status status = zetasql::GetBuiltinFunctionsAndTypes(
      zetasql::BuiltinFunctionOptions(MakeGoogleSqlLanguageOptions()),
      *type_factory, function_map, type_map_unused);
  // `status` can be an error when `BuiltinFunctionOptions` is misconfigured.
  // The call above only supplies a `LangaugeOptions` and is low risk. If that
  // configuration becomes more complex, then this `status` should probably be
  // propagated out, which requires changing `FunctionCatalog` to use a factory
  // function rather than a constructor that is doing work.
  ABSL_DCHECK_OK(status);

  // Move the data from the temporary function_map into functions_, keeping only
  // the functions that are available in Cloud Spanner.
  for (auto& [name, function] : function_map) {
    functions_.emplace(name, std::move(function));
  }
}

void FunctionCatalog::AddSpannerFunctions() {
  // Add pending commit timestamp function to the list of known functions.
  auto pending_commit_ts_func = PendingCommitTimestampFunction(catalog_name_);
  functions_[pending_commit_ts_func->Name()] =
      std::move(pending_commit_ts_func);
}

// Adds Spanner PG-specific functions to the list of known functions.
void FunctionCatalog::AddSpannerPGFunctions(
    zetasql::TypeFactory* type_factory) {
  SpannerPGFunctions spanner_pg_functions =
      GetSpannerPGFunctions(catalog_name_);

  for (auto& function : spanner_pg_functions) {
    functions_[function->Name()] = std::move(function);
  }
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
      auto alias_function = std::make_unique<zetasql::Function>(
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
