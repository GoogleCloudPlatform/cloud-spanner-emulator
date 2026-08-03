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

#include "backend/query/ml/model_evaluator.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "googlesql/public/catalog.h"
#include "googlesql/public/json_value.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/public/value.h"
#include "absl/base/no_destructor.h"
#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "backend/common/case.h"
#include "backend/query/queryable_model.h"
#include "backend/query/remote_udf/remote_udf_evaluator.h"
#include "backend/schema/catalog/model.h"
#include "googlesql/base/ret_check.h"
#include "googlesql/base/status_macros.h"

namespace google::spanner::emulator::backend {

namespace {

// Default prediction implementation. Takes a fingerprint of all model input
// values, then fills out model output columns by casting the hash value to
// output column type.
absl::Status DefaultPredict(
    const googlesql::Model* model,
    const CaseInsensitiveStringMap<const googlesql::Value*>& model_inputs,
    const CaseInsensitiveStringMap<const googlesql::Value*>& model_params,
    const CaseInsensitiveStringMap<googlesql::Value*>& model_outputs) {
  std::vector<googlesql::Value> args;

  for (const auto& model_input : model_inputs) {
    args.push_back(*model_input.second);
  }
  GOOGLESQL_ASSIGN_OR_RETURN(uint64_t input_fingerprint,
                   RemoteUdfProtocol::Fingerprint(args));

  for (const auto& model_output : model_outputs) {
    const googlesql::Column* output_column =
        model->FindOutputByName(model_output.first);
    GOOGLESQL_RET_CHECK(output_column != nullptr);
    GOOGLESQL_ASSIGN_OR_RETURN(*model_output.second,
                     RemoteUdfProtocol::ToValue(input_fingerprint,
                                                output_column->GetType()));
  }

  // Process fixed response schema for LLM models if provided.
  auto rs_it = model_params.find("response_schema");
  auto content_it = model_outputs.find("content");
  if (rs_it != model_params.end() && content_it != model_outputs.end()) {
    if (!rs_it->second->type()->IsJsonType() || rs_it->second->is_null()) {
      return absl::FailedPreconditionError(
          "Response schema must be a non-null JSON value.");
    }

    googlesql::JSONValueConstRef response_schema = rs_it->second->json_value();
    if (!response_schema.IsObject() || !response_schema.HasMember("type") ||
        !response_schema.GetMember("type").IsInt64()) {
      return absl::FailedPreconditionError(absl::StrCat(
          "Invalid response schema: ", response_schema.ToString()));
    }

    googlesql::JSONValue content;
    if (response_schema.HasMember("enum")) {
      auto enum_ = response_schema.GetMember("enum");
      if (!enum_.IsArray() || enum_.GetArraySize() <= 0) {
        return absl::FailedPreconditionError(
            absl::StrCat("Invalid response schema: enum should be a "
                         "non-empty array. Got: ",
                         enum_.ToString()));
      }

      googlesql::JSONValueConstRef enum_value =
          enum_.GetArrayElement(input_fingerprint % enum_.GetArraySize());
      content.GetRef().Set(googlesql::JSONValue::CopyFrom(enum_value));
    } else {
      int64_t type = response_schema.GetMember("type").GetInt64();
      switch (type) {
        // STRING
        case 1: {
          content.GetRef().SetString(absl::StrCat(input_fingerprint));
          break;
        }
        // NUMBER
        case 2:
          // Use integer values to avoid imprecise floats in compliance tests.
          content.GetRef().SetInt64((input_fingerprint % 2000) - 1000);
          break;
        // BOOLEAN
        case 4: {
          GOOGLESQL_ASSIGN_OR_RETURN(auto bool_value, RemoteUdfProtocol::ToValue(
                                                input_fingerprint,
                                                googlesql::types::BoolType()));
          content.GetRef().SetBoolean(bool_value.bool_value());
          break;
        }
        default:
          return absl::FailedPreconditionError(
              absl::StrCat("Unsupported response schema type: ", type));
      }
    }
    // Structured output is JSON formatted string.
    *content_it->second =
        googlesql::Value::String(content.GetConstRef().ToString());
  }

  return absl::OkStatus();
}

// Remote prediction implementation. Converts model inputs and parameters to
// JSON, invokes the remote endpoint, and converts the JSON response to model
// output values.
absl::Status RemotePredict(
    const googlesql::Model* model,
    const CaseInsensitiveStringMap<const googlesql::Value*>& model_inputs,
    const CaseInsensitiveStringMap<const googlesql::Value*>& model_params,
    const CaseInsensitiveStringMap<googlesql::Value*>& model_outputs) {
  GOOGLESQL_RET_CHECK(model->Is<QueryableModel>());
  const QueryableModel* spanner_model = model->GetAs<QueryableModel>();

  googlesql::JSONValue instance;
  instance.GetRef().SetToEmptyObject();
  for (const auto& model_input : model_inputs) {
    GOOGLESQL_ASSIGN_OR_RETURN(googlesql::JSONValue json_value,
                     RemoteUdfProtocol::ToJson(*model_input.second));
    instance.GetRef().GetMember(model_input.first).Set(std::move(json_value));
  }

  googlesql::JSONValue parameters;
  parameters.GetRef().SetToEmptyObject();
  for (const auto& model_param : model_params) {
    GOOGLESQL_ASSIGN_OR_RETURN(googlesql::JSONValue json_value,
                     RemoteUdfProtocol::ToJson(*model_param.second));
    parameters.GetRef().GetMember(model_param.first).Set(std::move(json_value));
  }

  GOOGLESQL_ASSIGN_OR_RETURN(absl::string_view endpoint,
                   spanner_model->GetFirstEndpoint());

  GOOGLESQL_ASSIGN_OR_RETURN(googlesql::JSONValue response,
                   RemoteUdfEvaluator::EvaluateRemoteFunction(
                       endpoint, model != nullptr ? model->FullName() : "",
                       {googlesql::Value::Json(std::move(instance)),
                        googlesql::Value::Json(std::move(parameters))}));

  googlesql::JSONValueConstRef response_ref = response.GetConstRef();
  if (!response_ref.IsObject()) {
    return absl::FailedPreconditionError(
        absl::StrCat("Remote prediction should return a JSON object. Body: ",
                     response_ref.ToString(), ". Endpoint: ", endpoint,
                     ". Schema object name: ", model->FullName()));
  }

  // Convert JSON response to model output values.
  for (const auto& [output_name, model_output] : model_outputs) {
    const googlesql::Column* output_column =
        model->FindOutputByName(output_name);
    GOOGLESQL_RET_CHECK(output_column != nullptr);
    const QueryableModelColumn* queryable_output_column =
        output_column->GetAs<QueryableModelColumn>();
    GOOGLESQL_RET_CHECK(queryable_output_column != nullptr);

    // Skip response fields that do not match model output columns.
    if (!response_ref.HasMember(output_name)) {
      if (queryable_output_column->required()) {
        return absl::FailedPreconditionError(absl::StrCat(
            "Remote prediction is missing required output column: ",
            output_name));
      }

      *model_output =
          googlesql::Value::Null(queryable_output_column->GetType());
      continue;
    }

    GOOGLESQL_ASSIGN_OR_RETURN(*model_output, RemoteUdfProtocol::ToValue(
                                        response_ref.GetMember(output_name),
                                        queryable_output_column->GetType()));
  }
  return absl::OkStatus();
}

// Default prediction implementation for PG. Takes a fingerprint of all model
// input and produces a single "Outcome" boolean field.
absl::Status DefaultPgPredict(absl::string_view endpoint,
                              const googlesql::JSONValueConstRef& instance,
                              const googlesql::JSONValueConstRef& parameters,
                              googlesql::JSONValueRef prediction) {
  GOOGLESQL_ASSIGN_OR_RETURN(uint64_t input_hash,
                   RemoteUdfProtocol::Fingerprint(instance));
  prediction.SetToEmptyObject();
  prediction.GetMember("Outcome").SetBoolean(input_hash % 2 == 0);
  return absl::OkStatus();
}

}  // namespace

absl::Status ModelEvaluator::Predict(
    const googlesql::Model* model,
    const CaseInsensitiveStringMap<const googlesql::Value*>& model_inputs,
    const CaseInsensitiveStringMap<const googlesql::Value*>& model_params,
    const CaseInsensitiveStringMap<googlesql::Value*>& model_outputs) {
  GOOGLESQL_RET_CHECK_NE(model, nullptr);
  // Validate if all required inputs are provided.
  for (int i = 0; i < model->NumInputs(); ++i) {
    const QueryableModelColumn* input_column =
        model->GetInput(i)->GetAs<QueryableModelColumn>();
    GOOGLESQL_RET_CHECK(input_column != nullptr);

    if (input_column->required() &&
        !model_inputs.contains(input_column->Name())) {
      return absl::FailedPreconditionError(
          absl::StrCat("Missing input column: ", input_column->Name()));
    }
  }

  // Validate no extra inputs are provided.
  for (const auto& [input_name, _] : model_inputs) {
    if (model->FindInputByName(input_name) == nullptr) {
      return absl::FailedPreconditionError(
          absl::StrCat("Unexpected input column: ", input_name));
    }
  }

  if (!absl::GetFlag(FLAGS_remote_functions_host_port).empty()) {
    return RemotePredict(model, model_inputs, model_params, model_outputs);
  }

  // Custom model prediction logic can be added here.

  return DefaultPredict(model, model_inputs, model_params, model_outputs);
}

absl::Status ModelEvaluator::PgPredict(
    absl::string_view endpoint, const googlesql::JSONValueConstRef& instance,
    const googlesql::JSONValueConstRef& parameters,
    googlesql::JSONValueRef prediction) {
  if (!absl::GetFlag(FLAGS_remote_functions_host_port).empty()) {
    GOOGLESQL_ASSIGN_OR_RETURN(
        googlesql::JSONValue response,
        RemoteUdfEvaluator::EvaluateRemoteFunction(
            endpoint, /*schema_object_name=*/"",
            {googlesql::Value::Json(googlesql::JSONValue::CopyFrom(instance)),
             googlesql::Value::Json(
                 googlesql::JSONValue::CopyFrom(parameters))}));
    prediction.Set(std::move(response));
    return absl::OkStatus();
  }

  // Custom model prediction logic can be added here.

  return DefaultPgPredict(endpoint, instance, parameters, prediction);
}

std::unique_ptr<QueryableModel> ModelEvaluator::GetDefaultLlmModel() {
  static const absl::NoDestructor<std::unique_ptr<backend::Model>>
      kDefaultLlmModel(absl::WrapUnique(new backend::Model(
          "default_llm_model",
          /*is_remote=*/true,
          /*input=*/
          absl::Span<const backend::Model::ModelColumn>{
              {"prompt", googlesql::types::StringType()}},
          /*output=*/
          absl::Span<const backend::Model::ModelColumn>{
              {"content", googlesql::types::StringType()}},
          /*endpoint=*/
          R"(//aiplatform.googleapis.com/projects/tp/locations/tl/publishers/google/models/gemini-2.5-flash)",
          /*endpoints=*/{},
          /*default_batch_size=*/std::nullopt)));

  return std::make_unique<QueryableModel>(kDefaultLlmModel->get());
}

}  // namespace google::spanner::emulator::backend
