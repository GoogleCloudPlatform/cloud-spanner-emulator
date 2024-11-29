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
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "zetasql/public/builtin_function.h"
#include "zetasql/public/builtin_function_options.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "absl/types/span.h"
#include "backend/query/analyzer_options.h"
#include "backend/query/ml/ml_predict_row_function.h"
#include "backend/query/ml/ml_predict_table_valued_function.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"
#include "backend/query/search/search_function_catalog.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/sequence.h"
#include "common/bit_reverse.h"
#include "common/constants.h"
#include "common/errors.h"
#include "common/feature_flags.h"
#include "third_party/spanner_pg/catalog/emulator_functions.h"
#include "zetasql/base/ret_check.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {
using postgres_translator::GetSpannerPGFunctions;
using postgres_translator::GetSpannerPGTVFs;
using postgres_translator::SpannerPGFunctions;
using postgres_translator::SpannerPGTVFs;

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

absl::StatusOr<zetasql::Value> EvalBitReverse(
    absl::Span<const zetasql::Value> args) {
  if (!EmulatorFeatureFlags::instance()
           .flags()
           .enable_bit_reversed_positive_sequences) {
    return error::UnsupportedFunction(kBitReverseFunctionName);
  }
  ZETASQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return zetasql::Value::NullInt64();
  }
  ZETASQL_RET_CHECK(args[0].type()->IsInt64() && args[1].type()->IsBool());
  return zetasql::Value::Int64(
      BitReverse(args[0].int64_value(), args[1].bool_value()));
}

std::unique_ptr<zetasql::Function> BitReverseFunction(
    const std::string& catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(zetasql::FunctionEvaluator(EvalBitReverse));

  return std::make_unique<zetasql::Function>(
      kBitReverseFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          zetasql::types::Int64Type(),
          {zetasql::types::Int64Type(), zetasql::types::BoolType()},
          nullptr}},
      function_options);
}

std::unique_ptr<zetasql::Function> MlPredictRowFunction(
    const std::string& catalog_name) {
  auto pg_jsonb = postgres_translator::spangres::datatypes::GetPgJsonbType();
  auto gsql_string = zetasql::types::StringType();

  zetasql::FunctionArgumentTypeOptions model_endpoint_opt;
  model_endpoint_opt.set_argument_name(kMlPredictRowParamModelEndpoint,
                                       zetasql::kPositionalOrNamed);

  zetasql::FunctionArgumentTypeOptions arg_opt;
  arg_opt.set_argument_name(kMlPredictRowParamArgs,
                            zetasql::kPositionalOrNamed);

  return std::make_unique<zetasql::Function>(
      kMlPredictRowFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{
              pg_jsonb,
              {{gsql_string, model_endpoint_opt}, {pg_jsonb, arg_opt}},
              nullptr},
          zetasql::FunctionSignature{
              pg_jsonb,
              {{pg_jsonb, model_endpoint_opt}, {pg_jsonb, arg_opt}},
              nullptr}},
      zetasql::FunctionOptions().set_evaluator({EvalMlPredictRow}));
}

std::optional<std::tuple<std::string, std::string, std::string>>
ParseFullyQualifiedColumnPath(const std::string& qualified_column_path) {
  std::vector<std::string> parts = absl::StrSplit(qualified_column_path, '.');
  if (parts.size() == 2) {
    return std::make_tuple(/*schema_name=*/"", /*table_name=*/parts[0],
                           /*column_name=*/parts[1]);
  } else if (parts.size() == 3) {
    return std::make_tuple(/*schema_name=*/parts[0], /*table_name=*/parts[1],
                           /*column_name=*/parts[2]);
  }
  return std::nullopt;
}
}  // namespace

FunctionCatalog::FunctionCatalog(zetasql::TypeFactory* type_factory,
                                 const std::string& catalog_name,
                                 const backend::Schema* schema)
    : catalog_name_(catalog_name), latest_schema_(schema) {
  // Add the subset of ZetaSQL built-in functions supported by Cloud Spanner.
  AddZetaSQLBuiltInFunctions(type_factory);
  // Add Cloud Spanner specific functions.
  AddSpannerFunctions();
  // Add aliases for the functions.
  AddFunctionAliases();
  AddMlFunctions();
  AddSpannerPGFunctions(type_factory);
  AddSearchFunctions(type_factory);
}

void FunctionCatalog::AddZetaSQLBuiltInFunctions(
    zetasql::TypeFactory* type_factory) {
  // Get all the ZetaSQL built-in functions.
  absl::flat_hash_map<std::string, std::unique_ptr<zetasql::Function>>
      function_map;
  absl::flat_hash_map<std::string, const zetasql::Type*> type_map_unused;
  absl::Status status = zetasql::GetBuiltinFunctionsAndTypes(
      MakeGoogleSqlBuiltinFunctionOptions(), *type_factory, function_map,
      type_map_unused);
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

  auto bit_reverse_func = BitReverseFunction(catalog_name_);
  functions_[bit_reverse_func->Name()] = std::move(bit_reverse_func);

  auto get_internal_sequence_state_func =
      GetInternalSequenceStateFunction(catalog_name_);
  functions_[get_internal_sequence_state_func->Name()] =
      std::move(get_internal_sequence_state_func);

  auto get_table_column_identity_state_func =
      GetTableColumnIdentityStateFunction(catalog_name_);
  functions_[get_table_column_identity_state_func->Name()] =
      std::move(get_table_column_identity_state_func);

  auto get_next_sequence_value_func =
      GetNextSequenceValueFunction(catalog_name_);
  functions_[get_next_sequence_value_func->Name()] =
      std::move(get_next_sequence_value_func);

  auto ml_predict_row_func = MlPredictRowFunction(catalog_name_);
  functions_[ml_predict_row_func->Name()] = std::move(ml_predict_row_func);
}

void FunctionCatalog::AddMlFunctions() {
  {
    auto ml_predict =
        std::make_unique<MlPredictTableValuedFunction>(/*safe=*/false);

    table_valued_functions_.insert(
        {ml_predict->FullName(), std::move(ml_predict)});
  }

  {
    auto safe_ml_predict =
        std::make_unique<MlPredictTableValuedFunction>(/*safe=*/true);
    table_valued_functions_.insert(
        {safe_ml_predict->FullName(), std::move(safe_ml_predict)});
  }
}

void FunctionCatalog::AddSearchFunctions(zetasql::TypeFactory* type_factory) {
  auto search_functions =
      query::search::GetSearchFunctions(type_factory, catalog_name_);

  for (auto& [name, function] : search_functions) {
    functions_.emplace(name, std::move(function));
  }
}

// Adds Spanner PG-specific functions to the list of known functions.
void FunctionCatalog::AddSpannerPGFunctions(
    zetasql::TypeFactory* type_factory) {
  SpannerPGFunctions spanner_pg_functions =
      GetSpannerPGFunctions(catalog_name_);

  for (auto& function : spanner_pg_functions) {
    // If function exists, add extra signatures instead of overwriting.
    // Needed for JSONB.
    if (auto f = functions_.find(function->Name()); f != functions_.end()) {
      for (auto& sig : function->signatures()) {
        f->second->AddSignature(sig);
      }
    } else {
      functions_[function->Name()] = std::move(function);
    }
  }

  SpannerPGTVFs spanner_pg_tvfs = GetSpannerPGTVFs(catalog_name_);

  for (auto& tvf : spanner_pg_tvfs) {
    table_valued_functions_[tvf->FullName()] = std::move(tvf);
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

void FunctionCatalog::GetTableValuedFunction(
    const std::string& name,
    const zetasql::TableValuedFunction** output) const {
  auto i = table_valued_functions_.find(name);
  *output = i == table_valued_functions_.end() ? nullptr : i->second.get();
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

std::unique_ptr<zetasql::Function>
FunctionCatalog::GetInternalSequenceStateFunction(
    const std::string& catalog_name) {
  // Defines the function evaluator as a lambda, so it has access to the schema.
  auto evaluator = [&](absl::Span<const zetasql::Value> args)
      -> absl::StatusOr<zetasql::Value> {
    ZETASQL_RET_CHECK(args.size() == 1 && args[0].type()->IsString());

    if (!EmulatorFeatureFlags::instance()
             .flags()
             .enable_bit_reversed_positive_sequences) {
      return error::UnsupportedFunction(kGetInternalSequenceStateFunctionName);
    }

    if (latest_schema_ == nullptr) {
      return error::SequenceNeedsAccessToSchema();
    }

    std::string sequence_name;
    if (latest_schema_->dialect() ==
        database_api::DatabaseDialect::POSTGRESQL) {
      sequence_name = args[0].string_value();
    } else {
      // ZetaSQL algebrizer prepends a prefix to the sequence name.
      sequence_name =
          std::string(absl::StripPrefix(args[0].string_value(), "_sequence_"));
    }
    const backend::Sequence* sequence =
        latest_schema_->FindSequence(sequence_name);
    if (sequence == nullptr) {
      return error::SequenceNotFound(sequence_name);
    }
    return sequence->GetInternalSequenceState();
  };

  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(zetasql::FunctionEvaluator(evaluator));

  return std::make_unique<zetasql::Function>(
      kGetInternalSequenceStateFunctionName, catalog_name,
      zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{
              zetasql::types::Int64Type(),
              {zetasql::FunctionArgumentType::AnySequence()},
              nullptr},
          zetasql::FunctionSignature{zetasql::types::Int64Type(),
                                       {zetasql::types::StringType()},
                                       nullptr}},
      function_options);
}

std::unique_ptr<zetasql::Function>
FunctionCatalog::GetTableColumnIdentityStateFunction(
    const std::string& catalog_name) {
  // Defines the function evaluator as a lambda, so it has access to the schema.
  auto evaluator = [&](absl::Span<const zetasql::Value> args)
      -> absl::StatusOr<zetasql::Value> {
    ZETASQL_RET_CHECK(args.size() == 1 && args[0].type()->IsString());

    if (!EmulatorFeatureFlags::instance().flags().enable_identity_columns) {
      return error::UnsupportedFunction(
          kGetTableColumnIdentityStateFunctionName);
    }

    if (latest_schema_ == nullptr) {
      return error::SequenceNeedsAccessToSchema();
    }

    std::string column_path = args[0].string_value();
    auto parsed_column_path = ParseFullyQualifiedColumnPath(column_path);
    if (!parsed_column_path.has_value()) {
      return error::InvalidColumnIdentifierFormat(column_path);
    }
    auto [schema_name, table_name, column_name] = *parsed_column_path;
    std::string full_table_name =
        schema_name.empty() ? table_name
                            : absl::StrCat(schema_name, ".", table_name);
    const Table* table = latest_schema_->FindTable(full_table_name);
    if (table == nullptr) {
      return error::TableNotFoundInIdentityFunction(full_table_name);
    }
    const Column* column = table->FindColumn(column_name);
    if (column == nullptr || !column->is_identity_column()) {
      return error::ColumnNotFoundInIdentityFunction(full_table_name,
                                                     column_name);
    }
    ZETASQL_RET_CHECK(column->sequences_used().size() == 1);
    const Sequence* sequence =
        static_cast<const Sequence*>(column->sequences_used().at(0));
    return sequence->GetInternalSequenceState();
  };

  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(zetasql::FunctionEvaluator(evaluator));

  return std::make_unique<zetasql::Function>(
      kGetTableColumnIdentityStateFunctionName, catalog_name,
      zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{zetasql::types::Int64Type(),
                                       {zetasql::types::StringType()},
                                       nullptr}},
      function_options);
}

std::unique_ptr<zetasql::Function>
FunctionCatalog::GetNextSequenceValueFunction(const std::string& catalog_name) {
  // Defines the function evaluator as a lambda, so it has access to the schema.
  auto evaluator = [&](absl::Span<const zetasql::Value> args)
      -> absl::StatusOr<zetasql::Value> {
    ZETASQL_RET_CHECK(args.size() == 1 && args[0].type()->IsString());

    if (!EmulatorFeatureFlags::instance()
             .flags()
             .enable_bit_reversed_positive_sequences) {
      return error::UnsupportedFunction(kGetNextSequenceValueFunctionName);
    }

    if (latest_schema_ == nullptr) {
      return error::SequenceNeedsAccessToSchema();
    }

    // ZetaSQL algebrizer prepends a prefix to the sequence name.
    std::string sequence_name;
    if (latest_schema_->dialect() ==
        database_api::DatabaseDialect::POSTGRESQL) {
      sequence_name = args[0].string_value();
    } else {
      sequence_name =
          std::string(absl::StripPrefix(args[0].string_value(), "_sequence_"));
    }
    const backend::Sequence* sequence =
        latest_schema_->FindSequence(sequence_name);
    if (sequence == nullptr) {
      return error::SequenceNotFound(sequence_name);
    }
    return sequence->GetNextSequenceValue();
  };

  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(zetasql::FunctionEvaluator(evaluator));

  return std::make_unique<zetasql::Function>(
      kGetNextSequenceValueFunctionName, catalog_name,
      zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{
              zetasql::types::Int64Type(),
              {zetasql::FunctionArgumentType::AnySequence()},
              nullptr},
          zetasql::FunctionSignature{zetasql::types::Int64Type(),
                                       {zetasql::types::StringType()},
                                       nullptr}},
      function_options);
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
