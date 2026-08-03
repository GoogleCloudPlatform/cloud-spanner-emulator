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

#include "googlesql/public/builtin_function.h"
#include "googlesql/public/function.h"
#include "googlesql/public/function_signature.h"
#include "googlesql/public/functions/json.h"
#include "googlesql/public/json_value.h"
#include "googlesql/public/table_valued_function.h"
#include "googlesql/public/type.h"
#include "googlesql/public/types/timestamp_util.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/public/value.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "absl/strings/substitute.h"
#include "absl/types/span.h"
#include "backend/common/case.h"
#include "backend/query/analyzer_options.h"
#include "backend/query/graph/mock_graph_algo_table_valued_function.h"
#include "backend/query/ml/ml_predict_row_function.h"
#include "backend/query/ml/ml_predict_table_valued_function.h"
#include "backend/query/ml/model_evaluator.h"
#include "backend/query/search/search_function_catalog.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
#include "googlesql/base/ret_check.h"
#include "googlesql/base/status_macros.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/sequence.h"
#include "common/bit_reverse.h"
#include "common/constants.h"
#include "common/errors.h"
#include "common/feature_flags.h"
#include "common/pg_literals.h"
#include "third_party/spanner_pg/catalog/emulator_function_evaluators.h"
#include "third_party/spanner_pg/catalog/emulator_functions.h"
#include "third_party/spanner_pg/datatypes/extended/pg_numeric_type.h"
#include "third_party/spanner_pg/interface/datetime_evaluators.h"
#include "third_party/spanner_pg/interface/formatting_evaluators.h"
#include "third_party/spanner_pg/interface/pg_timezone.h"
#include "googlesql/base/ret_check.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {
using postgres_translator::GetSpannerPGFunctions;
using postgres_translator::GetSpannerPGTVFs;
using postgres_translator::SpannerPGFunctions;
using postgres_translator::SpannerPGTVFs;

using postgres_translator::function_evaluators::CleanupPostgresDateTimeCache;
using postgres_translator::function_evaluators::CleanupPostgresNumberCache;

const googlesql::Type* gsql_float = googlesql::types::FloatType();
const googlesql::Type* gsql_double = googlesql::types::DoubleType();
const googlesql::Type* gsql_int64 = googlesql::types::Int64Type();
const googlesql::Type* gsql_string = googlesql::types::StringType();
const googlesql::Type* gsql_timestamp = googlesql::types::TimestampType();
const googlesql::Type* gsql_interval = googlesql::types::IntervalType();
const googlesql::Type* gsql_uuid = googlesql::types::UuidType();

absl::StatusOr<googlesql::Value> EvalPendingCommitTimestamp(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.empty());

  // Timestamp returned by this function is ignored later by query engine and is
  // replaced by kCommitTimestampIdentifier sentinel string as expected by cloud
  // spanner. Note that this function cannot return a string sentinel here since
  // googlesql evaluator expects a timestamp value for the corresponding column.
  return googlesql::Value::Timestamp(googlesql::types::TimestampMinBaseTime());
}

std::unique_ptr<googlesql::Function> PendingCommitTimestampFunction(
    const std::string& catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(
      googlesql::FunctionEvaluator(EvalPendingCommitTimestamp));

  return std::make_unique<googlesql::Function>(
      kPendingCommitTimestampFunctionName, catalog_name,
      googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{googlesql::FunctionSignature{
          googlesql::types::TimestampType(), {}, nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalBitReverse(
    absl::Span<const googlesql::Value> args) {
  if (!EmulatorFeatureFlags::instance()
           .flags()
           .enable_bit_reversed_positive_sequences) {
    return error::UnsupportedFunction(kBitReverseFunctionName);
  }
  GOOGLESQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return googlesql::Value::NullInt64();
  }
  GOOGLESQL_RET_CHECK(args[0].type()->IsInt64() && args[1].type()->IsBool());
  return googlesql::Value::Int64(
      BitReverse(args[0].int64_value(), args[1].bool_value()));
}

std::unique_ptr<googlesql::Function> BitReverseFunction(
    const std::string& catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(googlesql::FunctionEvaluator(EvalBitReverse));
  function_options.set_arguments_are_coercible(false);
  function_options.set_supports_safe_error_mode(false);

  return std::make_unique<googlesql::Function>(
      kBitReverseFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{googlesql::FunctionSignature{
          googlesql::types::Int64Type(),
          {googlesql::types::Int64Type(), googlesql::types::BoolType()},
          nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalAiIf(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 1);
  if (args[0].is_null()) {
    return googlesql::Value::NullBool();
  }

  GOOGLESQL_RET_CHECK(args[0].type()->IsString());
  googlesql::Value prompt = googlesql::values::String(args[0].string_value());

  // VertexAI Schema::Type 4 is BOOLEAN.
  GOOGLESQL_ASSIGN_OR_RETURN(
      googlesql::JSONValue response_schema_json,
      googlesql::JSONValue::ParseJSONString(R"json({"type": 4})json"));
  googlesql::Value response_schema =
      googlesql::values::Json(std::move(response_schema_json));
  googlesql::Value content;

  GOOGLESQL_RETURN_IF_ERROR(ModelEvaluator::Predict(
      /*model=*/ModelEvaluator::GetDefaultLlmModel().get(),
      /*model_inputs=*/{{"prompt", &prompt}},
      /*model_params=*/{{"response_schema", &response_schema}},
      /*model_outputs=*/{{"content", &content}}));

  if (content.is_null() || !content.type()->IsString()) {
    return error::AiOperator_UnexpectedResponse(content.ToString());
  }

  GOOGLESQL_ASSIGN_OR_RETURN(
      googlesql::JSONValue response_json,
      googlesql::JSONValue::ParseJSONString(content.string_value()),
      _.With([](const absl::Status& s) {
        return error::AiOperator_UnexpectedResponse(s.ToString());
      }));

  GOOGLESQL_ASSIGN_OR_RETURN(
      bool result,
      googlesql::functions::ConvertJsonToBool(response_json.GetConstRef()),
      _.With([](const absl::Status& s) {
        return error::AiOperator_UnexpectedResponse(s.ToString());
      }));

  return googlesql::Value::Bool(result);
}

std::unique_ptr<googlesql::Function> AiIfFunction(
    const std::string& catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(googlesql::FunctionEvaluator(EvalAiIf));
  function_options.set_supports_safe_error_mode(true);

  return std::make_unique<googlesql::Function>(
      kAiIfFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          {googlesql::types::BoolType(),
           {{googlesql::types::StringType(),
             googlesql::FunctionArgumentTypeOptions().set_argument_name(
                 "prompt", googlesql::kPositionalOrNamed)}},
           nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalAiScore(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 1);
  if (args[0].is_null()) {
    return googlesql::Value::NullDouble();
  }

  GOOGLESQL_RET_CHECK(args[0].type()->IsString());
  googlesql::Value prompt = googlesql::values::String(args[0].string_value());

  // VertexAI Schema::Type 2 is NUMBER.
  GOOGLESQL_ASSIGN_OR_RETURN(
      googlesql::JSONValue response_schema_json,
      googlesql::JSONValue::ParseJSONString(R"json({"type": 2})json"));
  googlesql::Value response_schema =
      googlesql::values::Json(std::move(response_schema_json));
  googlesql::Value content;

  GOOGLESQL_RETURN_IF_ERROR(ModelEvaluator::Predict(
      /*model=*/ModelEvaluator::GetDefaultLlmModel().get(),
      /*model_inputs=*/{{"prompt", &prompt}},
      /*model_params=*/{{"response_schema", &response_schema}},
      /*model_outputs=*/{{"content", &content}}));

  if (content.is_null() || !content.type()->IsString()) {
    return error::AiOperator_UnexpectedResponse(content.ToString());
  }

  GOOGLESQL_ASSIGN_OR_RETURN(
      googlesql::JSONValue response_json,
      googlesql::JSONValue::ParseJSONString(content.string_value()),
      _.With([](const absl::Status& s) {
        return error::AiOperator_UnexpectedResponse(s.ToString());
      }));

  GOOGLESQL_ASSIGN_OR_RETURN(double result,
                   googlesql::functions::ConvertJsonToDouble(
                       response_json.GetConstRef(),
                       googlesql::functions::WideNumberMode::kRound,
                       googlesql::ProductMode::PRODUCT_EXTERNAL),
                   _.With([](const absl::Status& s) {
                     return error::AiOperator_UnexpectedResponse(s.ToString());
                   }));

  return googlesql::Value::Double(result);
}

std::unique_ptr<googlesql::Function> AiScoreFunction(
    const std::string& catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(googlesql::FunctionEvaluator(EvalAiScore));
  function_options.set_supports_safe_error_mode(true);

  return std::make_unique<googlesql::Function>(
      kAiScoreFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          {googlesql::types::DoubleType(),
           {{googlesql::types::StringType(),
             googlesql::FunctionArgumentTypeOptions().set_argument_name(
                 "prompt", googlesql::kPositionalOrNamed)}},
           nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalAiClassify(
    absl::Span<const googlesql::Value> args) {
  GOOGLESQL_RET_CHECK(args.size() == 2);
  if (args[0].is_null() || args[1].is_null()) {
    return googlesql::Value::NullString();
  }

  // Build the prompt.
  std::vector<std::string> categories;
  std::string formatted_categories;

  if (args[1].type()->IsArray()) {
    if (args[1].elements().empty()) {
      return error::AiClassify_Categories_EmptyArray();
    }

    bool first = true;
    for (const auto& category : args[1].elements()) {
      absl::StrAppend(&formatted_categories, first ? "" : ", ");
      first = false;

      if (category.is_null()) {
        return error::AiClassify_Categories_NullElement();
      }

      if (category.type()->IsString()) {
        if (category.string_value().empty()) {
          return error::AiClassify_Categories_EmptyLabel();
        }

        absl::StrAppend(&formatted_categories, category.string_value());
        categories.push_back(category.string_value());
      } else if (category.type()->IsStruct()) {
        GOOGLESQL_RET_CHECK_EQ(category.fields().size(), 2);
        const auto& label = category.field(0);
        const auto& description = category.field(1);
        GOOGLESQL_RET_CHECK(label.type()->IsString());
        GOOGLESQL_RET_CHECK(description.type()->IsString());

        if (label.is_null() || label.string_value().empty()) {
          return error::AiClassify_Categories_EmptyLabel();
        }

        if (description.is_null() || description.string_value().empty()) {
          return error::AiClassify_Categories_EmptyDescription();
        }

        absl::StrAppend(&formatted_categories, label.string_value(), " (",
                        description.string_value(), ")");
        categories.push_back(label.string_value());
      } else {
        GOOGLESQL_RET_CHECK_FAIL() << "Unsupported categories type: "
                         << category.type()->DebugString();
      }
    }
  } else if (args[1].type()->Equals(
                 postgres_translator::spangres::datatypes::GetPgJsonbType())) {
    GOOGLESQL_ASSIGN_OR_RETURN(
        absl::Cord categories_cord,
        postgres_translator::spangres::datatypes::GetPgJsonbNormalizedValue(
            args[1]));
    GOOGLESQL_ASSIGN_OR_RETURN(
        googlesql::JSONValue categories_json,
        googlesql::JSONValue::ParseJSONString(categories_cord.Flatten()));

    if (!categories_json.GetConstRef().IsArray()) {
      return error::AiClassify_Categories_NotArray();
    }

    if (categories_json.GetConstRef().GetArraySize() == 0) {
      return error::AiClassify_Categories_EmptyArray();
    }
    bool first = true;
    for (const auto& category :
         categories_json.GetConstRef().GetArrayElements()) {
      absl::StrAppend(&formatted_categories, first ? "" : ", ");
      first = false;

      if (!category.IsObject() || category.GetObjectSize() != 2) {
        return error::AiClassify_Categories_NotArrayOfObjects();
      }
      std::optional<googlesql::JSONValueConstRef> label =
          category.GetMemberIfExists("label");
      std::optional<googlesql::JSONValueConstRef> description =
          category.GetMemberIfExists("description");
      if (!label.has_value() || !label->IsString() ||
          !description.has_value() || !description->IsString()) {
        return error::AiClassify_Categories_InvalidObject();
      }
      if (label->GetString().empty()) {
        return error::AiClassify_Categories_EmptyLabel();
      }
      if (description->GetString().empty()) {
        return error::AiClassify_Categories_EmptyDescription();
      }

      absl::StrAppend(&formatted_categories, label->GetString(), " (",
                      description->GetString(), ")");
      categories.push_back(label->GetString());
    }

  } else {
    GOOGLESQL_RET_CHECK_FAIL() << "Unsupported categories type: "
                     << args[1].type()->DebugString();
  }

  GOOGLESQL_RET_CHECK(args[0].type()->IsString());
  googlesql::Value prompt = googlesql::values::String(absl::Substitute(
      "Categorize the statement into one of the following: $0. Only return "
      "the "
      "result without any explanation. <statement>$1</statement>",
      formatted_categories, args[0].string_value()));

  // VertexAI Schema::Type 1 is STRING.
  googlesql::JSONValue response_schema_json;
  response_schema_json.GetRef().SetToEmptyObject();
  response_schema_json.GetRef().GetMember("type").SetInt64(1);
  response_schema_json.GetRef().GetMember("enum").SetToEmptyArray();
  for (const std::string& category : categories) {
    googlesql::JSONValue category_json;
    category_json.GetRef().SetString(category);
    GOOGLESQL_RETURN_IF_ERROR(
        response_schema_json.GetRef().GetMember("enum").AppendArrayElement(
            std::move(category_json)));
  }

  googlesql::Value response_schema =
      googlesql::values::Json(std::move(response_schema_json));
  googlesql::Value content;

  GOOGLESQL_RETURN_IF_ERROR(ModelEvaluator::Predict(
      /*model=*/ModelEvaluator::GetDefaultLlmModel().get(),
      /*model_inputs=*/{{"prompt", &prompt}},
      /*model_params=*/{{"response_schema", &response_schema}},
      /*model_outputs=*/{{"content", &content}}));

  if (content.is_null() || !content.type()->IsString()) {
    return error::AiOperator_UnexpectedResponse(content.ToString());
  }

  GOOGLESQL_ASSIGN_OR_RETURN(
      googlesql::JSONValue response_json,
      googlesql::JSONValue::ParseJSONString(content.string_value()),
      _.With([](const absl::Status& s) {
        return error::AiOperator_UnexpectedResponse(s.ToString());
      }));

  GOOGLESQL_ASSIGN_OR_RETURN(
      std::string result,
      googlesql::functions::ConvertJsonToString(response_json.GetConstRef()),
      _.With([](const absl::Status& s) {
        return error::AiOperator_UnexpectedResponse(s.ToString());
      }));

  return googlesql::Value::String(result);
}

std::unique_ptr<googlesql::Function> AiClassifyFunction(
    const std::string& catalog_name, googlesql::TypeFactory* type_factory) {
  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(googlesql::FunctionEvaluator(EvalAiClassify));
  function_options.set_supports_safe_error_mode(true);

  const googlesql::StructType* categories_struct_type;
  ABSL_CHECK_OK(type_factory->MakeStructType(  // Crash OK
      {{"label", googlesql::types::StringType()},
       {"description", googlesql::types::StringType()}},
      &categories_struct_type));

  const googlesql::ArrayType* categories_array_type;
  ABSL_CHECK_OK(type_factory->MakeArrayType(categories_struct_type,  // Crash OK
                                       &categories_array_type));

  return std::make_unique<googlesql::Function>(
      kAiClassifyFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          {googlesql::types::StringType(),
           {{googlesql::types::StringType(),
             googlesql::FunctionArgumentTypeOptions().set_argument_name(
                 "prompt", googlesql::kPositionalOrNamed)},
            {googlesql::types::StringArrayType(),
             googlesql::FunctionArgumentTypeOptions().set_argument_name(
                 "categories", googlesql::kPositionalOrNamed)}},
           nullptr},
          {googlesql::types::StringType(),
           {{googlesql::types::StringType(),
             googlesql::FunctionArgumentTypeOptions().set_argument_name(
                 "prompt", googlesql::kPositionalOrNamed)},
            {categories_array_type,
             googlesql::FunctionArgumentTypeOptions().set_argument_name(
                 "categories", googlesql::kPositionalOrNamed)}},
           nullptr},
          {googlesql::types::StringType(),
           {{googlesql::types::StringType(),
             googlesql::FunctionArgumentTypeOptions().set_argument_name(
                 "prompt", googlesql::kPositionalOrNamed)},
            {postgres_translator::spangres::datatypes::GetPgJsonbType(),
             googlesql::FunctionArgumentTypeOptions().set_argument_name(
                 "categories", googlesql::kPositionalOrNamed)}},
           nullptr},
      },
      function_options);
}

std::unique_ptr<googlesql::Function> MlPredictRowFunction(
    const std::string& catalog_name) {
  auto pg_jsonb = postgres_translator::spangres::datatypes::GetPgJsonbType();
  auto gsql_string = googlesql::types::StringType();

  googlesql::FunctionArgumentTypeOptions model_endpoint_opt;
  model_endpoint_opt.set_argument_name(kMlPredictRowParamModelEndpoint,
                                       googlesql::kPositionalOrNamed);

  googlesql::FunctionArgumentTypeOptions arg_opt;
  arg_opt.set_argument_name(kMlPredictRowParamArgs,
                            googlesql::kPositionalOrNamed);

  return std::make_unique<googlesql::Function>(
      kMlPredictRowFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{
              pg_jsonb,
              {{gsql_string, model_endpoint_opt}, {pg_jsonb, arg_opt}},
              nullptr},
          googlesql::FunctionSignature{
              pg_jsonb,
              {{pg_jsonb, model_endpoint_opt}, {pg_jsonb, arg_opt}},
              nullptr}},
      googlesql::FunctionOptions().set_evaluator({EvalMlPredictRow}));
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

absl::StatusOr<googlesql::Value> EvalSecureContext(
    absl::Span<const googlesql::Value> args,
    const absl::flat_hash_map<std::string, google::protobuf::Value>&
        secure_context) {
  GOOGLESQL_RET_CHECK(args.size() == 1);
  if (args[0].is_null()) {
    return absl::InvalidArgumentError(
        "The argument to SECURE_CONTEXT() cannot be NULL.");
  }
  GOOGLESQL_RET_CHECK(args[0].type()->IsString());
  const std::string& key = args[0].string_value();
  absl::flat_hash_map<std::string, google::protobuf::Value>::const_iterator it =
      secure_context.find(key);
  if (it == secure_context.end()) {
    // If we didn't find an exact match, try a case-insensitive lookup.
    for (auto const& [k, v] : secure_context) {
      if (absl::EqualsIgnoreCase(k, key)) {
        it = secure_context.find(k);
        break;
      }
    }
  }
  if (it == secure_context.end()) {
    return absl::InvalidArgumentError(
        absl::StrCat("Missing secure parameter: ", key));
  }
  const google::protobuf::Value& val = it->second;
  if (val.has_string_value()) {
    return googlesql::Value::StringValue(val.string_value());
  }
  if (val.has_null_value()) {
    return googlesql::Value::NullString();
  }
  return absl::InvalidArgumentError(
      "Secure parameters must be string or null values.");
}

std::unique_ptr<googlesql::Function> SecureContextFunction(
    absl::string_view catalog_name, const googlesql::FunctionOptions& options) {
  return std::make_unique<googlesql::Function>(
      "SECURE_CONTEXT", catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{googlesql::types::StringType(),
                                       {googlesql::types::StringType()},
                                       nullptr}},
      options);
}
}  // namespace

FunctionCatalog::FunctionCatalog(googlesql::TypeFactory* type_factory,
                                 const std::string& catalog_name,
                                 const backend::Schema* schema)
    : catalog_name_(catalog_name), latest_schema_(schema) {
  // Add the subset of GoogleSQL built-in functions supported by Cloud Spanner.
  AddGoogleSQLBuiltInFunctions(type_factory);
  // Add Cloud Spanner specific functions.
  AddSpannerFunctions();
  AddGraphSafeToJsonSignatures();
  // Add aliases for the functions.
  AddFunctionAliases();
  AddMlFunctions(type_factory);
  AddSpannerPGFunctions();
  AddPGLambdaFunctions();
  AddSearchFunctions(type_factory);
  AddMockGraphAlgoFunctions(table_valued_functions_);
}

void FunctionCatalog::AddGoogleSQLBuiltInFunctions(
    googlesql::TypeFactory* type_factory) {
  // Get all the GoogleSQL built-in functions.
  absl::flat_hash_map<std::string, std::unique_ptr<googlesql::Function>>
      function_map;
  absl::flat_hash_map<std::string, const googlesql::Type*> type_map_unused;
  absl::Status status = googlesql::GetBuiltinFunctionsAndTypes(
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
}

void FunctionCatalog::AddGraphSafeToJsonSignatures() {
  auto it = functions_.find("safe_to_json");
  if (it != functions_.end()) {
    it->second->AddSignature(googlesql::FunctionSignature(
        googlesql::types::JsonType(),
        {googlesql::FunctionArgumentType(googlesql::ARG_KIND_EXPR_GRAPH_NODE)},
        nullptr));
    it->second->AddSignature(googlesql::FunctionSignature(
        googlesql::types::JsonType(),
        {googlesql::FunctionArgumentType(googlesql::ARG_KIND_EXPR_GRAPH_EDGE)},
        nullptr));
  }
}

void FunctionCatalog::AddMlFunctions(googlesql::TypeFactory* type_factory) {
  {
    auto ai_if = AiIfFunction(catalog_name_);
    functions_[ai_if->Name()] = std::move(ai_if);
  }

  {
    auto ai_score = AiScoreFunction(catalog_name_);
    functions_[ai_score->Name()] = std::move(ai_score);
  }

  {
    auto ai_classify = AiClassifyFunction(catalog_name_, type_factory);
    functions_[ai_classify->Name()] = std::move(ai_classify);
  }

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

  {
    auto ml_predict_row_func = MlPredictRowFunction(catalog_name_);
    functions_[ml_predict_row_func->Name()] = std::move(ml_predict_row_func);
  }
}

void FunctionCatalog::AddSearchFunctions(googlesql::TypeFactory* type_factory) {
  auto dialect = database_api::DatabaseDialect::GOOGLE_STANDARD_SQL;
  if (latest_schema_ != nullptr) {
    dialect = latest_schema_->dialect();
  }

  auto search_functions =
      query::search::GetSearchFunctions(type_factory, catalog_name_, dialect);

  for (auto& [name, function] : search_functions) {
    functions_.emplace(name, std::move(function));
  }
}

// Adds Spanner PG-specific functions to the list of known functions.
void FunctionCatalog::AddSpannerPGFunctions() {
  SpannerPGFunctions spanner_pg_functions =
      GetSpannerPGFunctions(catalog_name_);

  for (auto& function : spanner_pg_functions) {
    // If function exists, add extra signatures instead of overwriting.
    // Needed for JSONB.
    if (auto f = functions_.find(function->Name()); f != functions_.end()) {
      // Copy the existing options and add any evaluators if they exist.
      googlesql::FunctionOptions function_options =
          f->second->function_options().Copy();
      // Add function evaluators if they exist.
      if (function->GetFunctionEvaluatorFactory() != nullptr) {
        function_options.set_evaluator_factory(
            function->GetFunctionEvaluatorFactory());
      } else if (function->GetAggregateFunctionEvaluatorFactory() != nullptr) {
        function_options.set_aggregate_function_evaluator_factory(
            function->GetAggregateFunctionEvaluatorFactory());
      }
      auto new_function = std::make_unique<googlesql::Function>(
          f->second->Name(), f->second->GetGroup(), f->second->mode(),
          function->signatures(), function_options);
      for (auto& sig : f->second->signatures()) {
        new_function->AddSignature(sig);
      }
      f->second = std::move(new_function);
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
                                  const googlesql::Function** output) const {
  auto function_iter = functions_.find(name);
  *output =
      function_iter == functions_.end() ? nullptr : function_iter->second.get();
}

void FunctionCatalog::GetFunctions(
    absl::flat_hash_set<const googlesql::Function*>* output) const {
  for (const auto& [name, function] : functions_) {
    output->insert(function.get());
  }
}

void FunctionCatalog::GetTableValuedFunction(
    const std::string& name,
    const googlesql::TableValuedFunction** output) const {
  auto i = table_valued_functions_.find(name);
  *output = i == table_valued_functions_.end() ? nullptr : i->second.get();
}

void FunctionCatalog::AddFunctionAliases() {
  std::vector<std::pair<std::string, std::unique_ptr<googlesql::Function>>>
      aliases;
  for (auto it = functions_.begin(); it != functions_.end(); ++it) {
    const googlesql::Function* original_function = it->second.get();
    if (!original_function->alias_name().empty()) {
      googlesql::FunctionOptions function_options =
          original_function->function_options();
      std::string alias_name = function_options.alias_name;
      function_options.set_alias_name("");
      auto alias_function = std::make_unique<googlesql::Function>(
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

void FunctionCatalog::AddPGLambdaFunctions() {
  // These date/timestamp PG functions need to use a lambda to access the
  // default time zone in the latest schema, so they need to be defined here.
  auto to_char_function = GetPGToCharFunction(catalog_name_);
  functions_[to_char_function->Name()] = std::move(to_char_function);
  auto extract_function = GetPGExtractFunction(catalog_name_);
  functions_[extract_function->Name()] = std::move(extract_function);
  auto cast_to_timestamp_function = GetPGCastToTimestampFunction(catalog_name_);
  functions_[cast_to_timestamp_function->Name()] =
      std::move(cast_to_timestamp_function);
  auto cast_to_string_function = GetPGCastToStringFunction(catalog_name_);
  functions_[cast_to_string_function->Name()] =
      std::move(cast_to_string_function);
  auto date_trunc_function = GetPGDateTruncFunction(catalog_name_);
  functions_[date_trunc_function->Name()] = std::move(date_trunc_function);
}

std::unique_ptr<googlesql::Function> FunctionCatalog::GetPGToCharFunction(
    const std::string& catalog_name) {
  static const googlesql::Type* gsql_pg_numeric =
      postgres_translator::spangres::datatypes::GetPgNumericType();
  // Defines the function as a lambda, so it has access to the schema.
  auto initialize_pg_timezone = [&]() {
    std::string default_time_zone = latest_schema_ != nullptr
                                        ? latest_schema_->default_time_zone()
                                        : kDefaultTimeZone;
    absl::Status status = postgres_translator::interfaces::InitPGTimezone(
        default_time_zone.c_str());
    if (!status.ok()) {
      ABSL_LOG(ERROR) << "Failed to initialize PG timezone for to_char function: "
                 << status;
    }
  };
  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(postgres_translator::PGFunctionEvaluator(
      postgres_translator::EvalToChar, initialize_pg_timezone, [] {
        CleanupPostgresNumberCache();
        CleanupPostgresDateTimeCache();
      }));
  function_options.set_arguments_are_coercible(false);
  function_options.set_supports_safe_error_mode(false);
  return std::make_unique<googlesql::Function>(
      postgres_translator::kPGToCharFunctionName, catalog_name,
      googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{
              gsql_string, {gsql_int64, gsql_string}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{gsql_string,
                                       {gsql_timestamp, gsql_string},
                                       /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{gsql_string,
                                       {gsql_double, gsql_string},
                                       /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{gsql_string,
                                       {gsql_float, gsql_string},
                                       /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{gsql_string,
                                       {gsql_pg_numeric, gsql_string},
                                       /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{gsql_string,
                                       {gsql_interval, gsql_string},
                                       /*context_ptr=*/nullptr},
      },
      function_options);
}

std::unique_ptr<googlesql::Function> FunctionCatalog::GetPGExtractFunction(
    const std::string& catalog_name) {
  static const googlesql::Type* gsql_pg_numeric =
      postgres_translator::spangres::datatypes::GetPgNumericType();
  // Defines the function as a lambda, so it has access to the schema.
  auto initialize_pg_timezone = [&]() {
    std::string default_time_zone = latest_schema_ != nullptr
                                        ? latest_schema_->default_time_zone()
                                        : kDefaultTimeZone;
    absl::Status status = postgres_translator::interfaces::InitPGTimezone(
        default_time_zone.c_str());
    if (!status.ok()) {
      ABSL_LOG(ERROR) << "Failed to initialize PG timezone for extract function: "
                 << status;
    }
  };
  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(postgres_translator::PGFunctionEvaluator(
      postgres_translator::EvalExtract, initialize_pg_timezone,
      CleanupPostgresDateTimeCache));
  function_options.set_arguments_are_coercible(false);
  function_options.set_supports_safe_error_mode(false);

  return std::make_unique<googlesql::Function>(
      postgres_translator::kPGExtractFunctionName, catalog_name,
      googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{gsql_pg_numeric,
                                       {googlesql::types::StringType(),
                                        googlesql::types::TimestampType()},
                                       nullptr},
          googlesql::FunctionSignature{
              gsql_pg_numeric,
              {googlesql::types::StringType(), googlesql::types::DateType()},
              nullptr}},
      function_options);
}

std::unique_ptr<googlesql::Function>
FunctionCatalog::GetPGCastToTimestampFunction(const std::string& catalog_name) {
  // Defines the function as a lambda, so it has access to the schema.
  auto initialize_pg_timezone = [&]() {
    std::string default_time_zone = latest_schema_ != nullptr
                                        ? latest_schema_->default_time_zone()
                                        : kDefaultTimeZone;
    absl::Status status = postgres_translator::interfaces::InitPGTimezone(
        default_time_zone.c_str());
    if (!status.ok()) {
      ABSL_LOG(ERROR)
          << "Failed to initialize PG timezone for cast_to_timestamp function: "
          << status;
    }
  };
  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(postgres_translator::PGFunctionEvaluator(
      postgres_translator::EvalCastToTimestamp, initialize_pg_timezone,
      CleanupPostgresDateTimeCache));
  function_options.set_arguments_are_coercible(false);
  function_options.set_supports_safe_error_mode(false);

  return std::make_unique<googlesql::Function>(
      postgres_translator::kPGCastToTimestampFunctionName, catalog_name,
      googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{googlesql::types::TimestampType(),
                                       {googlesql::types::StringType()},
                                       nullptr}},
      function_options);
}

std::unique_ptr<googlesql::Function> FunctionCatalog::GetPGCastToStringFunction(
    const std::string& catalog_name) {
  static const googlesql::Type* gsql_pg_numeric =
      postgres_translator::spangres::datatypes::GetPgNumericType();
  // Defines the function as a lambda, so it has access to the schema.
  auto initialize_pg_timezone = [&]() {
    std::string default_time_zone = latest_schema_ != nullptr
                                        ? latest_schema_->default_time_zone()
                                        : kDefaultTimeZone;
    absl::Status status = postgres_translator::interfaces::InitPGTimezone(
        default_time_zone.c_str());
    if (!status.ok()) {
      ABSL_LOG(ERROR)
          << "Failed to initialize PG timezone for cast_to_string function: "
          << status;
    }
  };
  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(postgres_translator::PGFunctionEvaluator(
      postgres_translator::EvalCastToString, initialize_pg_timezone));
  function_options.set_arguments_are_coercible(false);
  function_options.set_supports_safe_error_mode(false);

  return std::make_unique<googlesql::Function>(
      postgres_translator::kPGCastToStringFunctionName, catalog_name,
      googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{
              gsql_string, {gsql_pg_numeric}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_string, {gsql_interval}, /*context_ptr=*/nullptr},
          googlesql::FunctionSignature{
              gsql_string, {gsql_uuid}, /*context_ptr=*/nullptr},
      },
      function_options);
}

std::unique_ptr<googlesql::Function> FunctionCatalog::GetPGDateTruncFunction(
    const std::string& catalog_name) {
  // Defines the function as a lambda, so it has access to the schema.
  auto initialize_pg_timezone = [&]() {
    std::string default_time_zone = latest_schema_ != nullptr
                                        ? latest_schema_->default_time_zone()
                                        : kDefaultTimeZone;
    absl::Status status = postgres_translator::interfaces::InitPGTimezone(
        default_time_zone.c_str());
    if (!status.ok()) {
      ABSL_LOG(ERROR) << "Failed to initialize PG timezone for date_trunc function: "
                 << status;
    }
  };
  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(postgres_translator::PGFunctionEvaluator(
      postgres_translator::EvalTimestamptzTrunc, initialize_pg_timezone,
      CleanupPostgresDateTimeCache));
  function_options.set_arguments_are_coercible(false);
  function_options.set_supports_safe_error_mode(false);

  return std::make_unique<googlesql::Function>(
      postgres_translator::kPGTimestamptzTruncFunctionName, catalog_name,
      googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{googlesql::types::TimestampType(),
                                       {googlesql::types::StringType(),
                                        googlesql::types::TimestampType()},
                                       nullptr},
          googlesql::FunctionSignature{googlesql::types::TimestampType(),
                                       {googlesql::types::StringType(),
                                        googlesql::types::TimestampType(),
                                        googlesql::types::StringType()},
                                       nullptr}},
      function_options);
}

std::unique_ptr<googlesql::Function>
FunctionCatalog::GetInternalSequenceStateFunction(
    const std::string& catalog_name) {
  // Defines the function evaluator as a lambda, so it has access to the schema.
  auto evaluator = [&](absl::Span<const googlesql::Value> args)
      -> absl::StatusOr<googlesql::Value> {
    GOOGLESQL_RET_CHECK(args.size() == 1 && args[0].type()->IsString());

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
      // GoogleSQL algebrizer prepends a prefix to the sequence name.
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

  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(googlesql::FunctionEvaluator(evaluator));
  function_options.set_arguments_are_coercible(false);
  function_options.set_supports_safe_error_mode(false);

  return std::make_unique<googlesql::Function>(
      kGetInternalSequenceStateFunctionName, catalog_name,
      googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{
              googlesql::types::Int64Type(),
              {googlesql::FunctionArgumentType::AnySequence()},
              nullptr},
          googlesql::FunctionSignature{googlesql::types::Int64Type(),
                                       {googlesql::types::StringType()},
                                       nullptr}},
      function_options);
}

std::unique_ptr<googlesql::Function>
FunctionCatalog::GetTableColumnIdentityStateFunction(
    const std::string& catalog_name) {
  // Defines the function evaluator as a lambda, so it has access to the schema.
  auto evaluator = [&](absl::Span<const googlesql::Value> args)
      -> absl::StatusOr<googlesql::Value> {
    GOOGLESQL_RET_CHECK(args.size() == 1 && args[0].type()->IsString());

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
    GOOGLESQL_RET_CHECK(column->sequences_used().size() == 1);
    const Sequence* sequence =
        static_cast<const Sequence*>(column->sequences_used().at(0));
    return sequence->GetInternalSequenceState();
  };

  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(googlesql::FunctionEvaluator(evaluator));
  function_options.set_arguments_are_coercible(false);
  function_options.set_supports_safe_error_mode(false);

  return std::make_unique<googlesql::Function>(
      kGetTableColumnIdentityStateFunctionName, catalog_name,
      googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{googlesql::types::Int64Type(),
                                       {googlesql::types::StringType()},
                                       nullptr}},
      function_options);
}

std::unique_ptr<googlesql::Function>
FunctionCatalog::GetNextSequenceValueFunction(const std::string& catalog_name) {
  // Defines the function evaluator as a lambda, so it has access to the schema.
  auto evaluator = [&](absl::Span<const googlesql::Value> args)
      -> absl::StatusOr<googlesql::Value> {
    GOOGLESQL_RET_CHECK(args.size() == 1 && args[0].type()->IsString());

    if (!EmulatorFeatureFlags::instance()
             .flags()
             .enable_bit_reversed_positive_sequences) {
      return error::UnsupportedFunction(kGetNextSequenceValueFunctionName);
    }

    if (latest_schema_ == nullptr) {
      return error::SequenceNeedsAccessToSchema();
    }

    std::string sequence_name;
    if (latest_schema_->dialect() ==
        database_api::DatabaseDialect::POSTGRESQL) {
      sequence_name =
          GetFullyQualifiedNameFromPgLiteral(args[0].string_value());
    } else {
      // GoogleSQL algebrizer prepends a prefix to the sequence name.
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

  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(googlesql::FunctionEvaluator(evaluator));
  function_options.set_arguments_are_coercible(false);
  function_options.set_supports_safe_error_mode(false);
  function_options.set_volatility(googlesql::FunctionEnums::VOLATILE);

  return std::make_unique<googlesql::Function>(
      kGetNextSequenceValueFunctionName, catalog_name,
      googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{
              googlesql::types::Int64Type(),
              {googlesql::FunctionArgumentType::AnySequence()},
              nullptr},
          googlesql::FunctionSignature{googlesql::types::Int64Type(),
                                       {googlesql::types::StringType()},
                                       nullptr}},
      function_options);
}

std::unique_ptr<googlesql::Function> CreateSecureContextFunction(
    absl::string_view catalog_name,
    const absl::flat_hash_map<std::string, google::protobuf::Value>&
        secure_context) {
  googlesql::FunctionOptions function_options;
  function_options.set_arguments_are_coercible(false);
  function_options.set_supports_safe_error_mode(false);
  function_options.set_evaluator(googlesql::FunctionEvaluator(
      [secure_context](absl::Span<const googlesql::Value> args)
          -> absl::StatusOr<googlesql::Value> {
        return EvalSecureContext(args, secure_context);
      }));
  return SecureContextFunction(catalog_name, function_options);
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
