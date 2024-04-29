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

#include "backend/query/ml/ml_predict_row_function.h"

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/json_value.h"
#include "zetasql/public/value.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "backend/query/ml/model_evaluator.h"
#include "common/errors.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google::spanner::emulator::backend {
namespace {

using zetasql::JSONValue;
using zetasql::JSONValueConstRef;
using zetasql::JSONValueRef;
using postgres_translator::spangres::datatypes::CreatePgJsonbValue;
using postgres_translator::spangres::datatypes::GetPgJsonbNormalizedValue;
using postgres_translator::spangres::datatypes::GetPgJsonbType;

inline constexpr absl::string_view kModelEndpointOptionName = "endpoint";
inline constexpr absl::string_view kModelEndpointsOptionName = "endpoints";
inline constexpr absl::string_view kModelDefaultBatchOptionName =
    "default_batch_size";

inline constexpr absl::string_view kModelInstancesArgName = "instances";
inline constexpr absl::string_view kModelParametersArgName = "parameters";
inline constexpr absl::string_view kModelPredictionsResultName = "predictions";

inline constexpr int64_t kModelMaxBatchSize = 250;

// Holder of model endpoint options.
struct ModelEndpoint {
  // Creates ModelEndpoint instance from a string.
  static absl::StatusOr<ModelEndpoint> FromString(absl::string_view str);

  // Creates ModelEndpoint instance from JSONB.
  static absl::StatusOr<ModelEndpoint> FromJsonb(absl::string_view jsonb);

  // Helper to validate if options are valid.
  absl::Status Validate();

  // Returns the first endpoint.
  absl::string_view Endpoint();

  std::string endpoint;
  std::vector<std::string> endpoints;
  std::optional<int64_t> default_batch_size;
};

absl::StatusOr<ModelEndpoint> ModelEndpoint::FromString(absl::string_view str) {
  ModelEndpoint result;
  result.endpoint = str;
  return result;
}

absl::StatusOr<ModelEndpoint> ModelEndpoint::FromJsonb(absl::string_view json) {
  ModelEndpoint result;
  ZETASQL_ASSIGN_OR_RETURN(JSONValue model_endpoint_json,
                   JSONValue::ParseJSONString(json));
  JSONValueConstRef model_endpoint = model_endpoint_json.GetConstRef();

  if (!model_endpoint.IsObject()) {
    return error::MlPredictRow_Argument_NotObject(
        kMlPredictRowParamModelEndpoint);
  }
  for (const auto& [key, value] : model_endpoint.GetMembers()) {
    std::string key_lower = absl::AsciiStrToLower(key);
    if (key_lower == kModelEndpointOptionName) {
      if (!value.IsString()) {
        return error::MlPredictRow_Argument_UnexpectedValueType(
            kMlPredictRowParamModelEndpoint, key, "string");
      }
      result.endpoint = value.GetString();
    } else if (key_lower == kModelEndpointsOptionName) {
      if (!value.IsArray()) {
        return error::MlPredictRow_Argument_UnexpectedValueType(
            kMlPredictRowParamModelEndpoint, key, "array of strings");
      }
      for (const JSONValueConstRef& item : value.GetArrayElements()) {
        if (!item.IsString()) {
          return error::MlPredictRow_Argument_UnexpectedValueType(
              kMlPredictRowParamModelEndpoint, key, "array of strings");
        }
        result.endpoints.push_back(item.GetString());
      }
    } else if (key_lower == kModelDefaultBatchOptionName) {
      if (!value.IsInt64()) {
        return error::MlPredictRow_Argument_UnexpectedValueType(
            kMlPredictRowParamModelEndpoint, key, "integer");
      }
      result.default_batch_size = value.GetInt64();
    } else {
      return error::MlPredictRow_Argument_UnexpectedKey(
          kMlPredictRowParamModelEndpoint, key);
    }
  }
  return result;
}

absl::Status ModelEndpoint::Validate() {
  if (endpoint.empty() && endpoints.empty()) {
    return error::MlPredictRow_ModelEndpoint_NoEndpoints();
  }

  if (!endpoint.empty() && !endpoints.empty()) {
    return error::MlPredictRow_ModelEndpoint_EndpointsAmbiguous();
  }

  if (default_batch_size.has_value() &&
      (*default_batch_size <= 0 || *default_batch_size > kModelMaxBatchSize)) {
    return error::MlPredictRow_ModelEndpoint_InvalidBatchSize(
        *default_batch_size, 1, kModelMaxBatchSize);
  }

  return absl::OkStatus();
}

absl::string_view ModelEndpoint::Endpoint() {
  return endpoints.empty() ? endpoint : endpoints[0];
}

absl::Status ParseArgsJsonb(absl::string_view args_jsonb_string,
                            std::vector<JSONValue>* instances,
                            JSONValue* parameters) {
  ZETASQL_ASSIGN_OR_RETURN(JSONValue args_json,
                   JSONValue::ParseJSONString(args_jsonb_string));
  JSONValueConstRef args = args_json.GetConstRef();
  if (!args.IsObject()) {
    return error::MlPredictRow_Argument_NotObject(kMlPredictRowParamArgs);
  }
  for (const auto& [key, value] : args.GetMembers()) {
    std::string key_lower = absl::AsciiStrToLower(key);
    if (key_lower == kModelInstancesArgName) {
      if (!value.IsArray()) {
        return error::MlPredictRow_Argument_UnexpectedValueType(
            kMlPredictRowParamArgs, key, "array of objects");
      }
      for (const JSONValueConstRef& item : value.GetArrayElements()) {
        instances->push_back(JSONValue::CopyFrom(item));
      }
    } else if (key_lower == kModelParametersArgName) {
      *parameters = JSONValue::CopyFrom(value);
    } else {
      return error::MlPredictRow_Argument_UnexpectedKey(kMlPredictRowParamArgs,
                                                        key);
    }
  }

  return absl::OkStatus();
}

absl::StatusOr<zetasql::Value> PredictionsToJsonB(
    std::vector<JSONValue> predictions) {
  JSONValue result;
  result.GetRef().SetToEmptyObject();
  JSONValueRef predictions_json =
      result.GetRef().GetMember(kModelPredictionsResultName);
  predictions_json.SetToEmptyArray();
  for (JSONValue& prediction : predictions) {
    ZETASQL_RETURN_IF_ERROR(predictions_json.AppendArrayElement(
        JSONValue::MoveFrom(prediction.GetRef())));
  }
  return CreatePgJsonbValue(result.GetConstRef().ToString());
}

}  // namespace

absl::StatusOr<zetasql::Value> EvalMlPredictRow(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RET_CHECK_EQ(args.size(), 2);

  // Validate model_endpoint parameter and get first endpoint.
  ModelEndpoint model_endpoint;
  if (args[0].is_null()) {
    return error::MlPredictRow_Argument_Null(kMlPredictRowParamModelEndpoint);
  } else if (args[0].type()->IsString()) {
    ZETASQL_ASSIGN_OR_RETURN(model_endpoint,
                     ModelEndpoint::FromString(args[0].string_value()));
  } else if (args[0].type()->Equals(GetPgJsonbType())) {
    ZETASQL_ASSIGN_OR_RETURN(absl::Cord cord, GetPgJsonbNormalizedValue(args[0]));
    ZETASQL_ASSIGN_OR_RETURN(model_endpoint, ModelEndpoint::FromJsonb(cord.Flatten()));
  } else {
    ZETASQL_RET_CHECK_FAIL() << "Unexpected model_endpoint argument type "
                     << args[0].type()->TypeName(zetasql::PRODUCT_EXTERNAL,
                                                 /*use_external_float32=*/true);
  }
  ZETASQL_RETURN_IF_ERROR(model_endpoint.Validate());
  absl::string_view first_endpoint = model_endpoint.Endpoint();

  // Validate args parameter and extract instances.
  std::vector<JSONValue> instances;
  JSONValue parameters;
  if (args[1].is_null()) {
    return error::MlPredictRow_Argument_Null(kMlPredictRowParamArgs);
  } else if (args[1].type()->Equals(GetPgJsonbType())) {
    ZETASQL_ASSIGN_OR_RETURN(absl::Cord cord, GetPgJsonbNormalizedValue(args[1]));
    ZETASQL_RETURN_IF_ERROR(ParseArgsJsonb(cord.Flatten(), &instances, &parameters));
  } else {
    ZETASQL_RET_CHECK_FAIL() << "Unexpected args var type "
                     << args[1].type()->TypeName(zetasql::PRODUCT_EXTERNAL,
                                                 /*use_external_float32=*/true);
  }
  if (instances.empty()) {
    return error::MlPredictRow_Args_NoInstances();
  }

  // Run prediction on each instance.
  std::vector<JSONValue> predictions;
  for (const JSONValue& instance : instances) {
    ZETASQL_RETURN_IF_ERROR(ModelEvaluator::PgPredict(
        first_endpoint, instance.GetConstRef(), parameters.GetConstRef(),
        predictions.emplace_back().GetRef()));
  }

  // Pack all predictions into JSONB result.
  return PredictionsToJsonB(std::move(predictions));
}

}  // namespace google::spanner::emulator::backend
