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
#include <vector>

#include "googlesql/public/catalog.h"
#include "googlesql/public/json_value.h"
#include "googlesql/public/types/struct_type.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/value.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "backend/common/case.h"
#include "googlesql/base/ret_check.h"
#include "googlesql/base/status_macros.h"
#include "farmhash.h"

namespace google::spanner::emulator::backend {

absl::StatusOr<uint64_t> Fingerprint(const googlesql::Value& value) {
  if (value.is_null()) {
    return 0;
  }

  switch (value.type_kind()) {
    case googlesql::TYPE_INT32:
      // Integers are encoded as strings on the wire.
      return farmhash::Fingerprint64(absl::StrCat(value.int32_value()));
    case googlesql::TYPE_INT64:
      // Integers are encoded as strings on the wire.
      return farmhash::Fingerprint64(absl::StrCat(value.int64_value()));
    case googlesql::TYPE_UINT32:
      // Integers are encoded as strings on the wire.
      return farmhash::Fingerprint64(absl::StrCat(value.uint32_value()));
    case googlesql::TYPE_UINT64:
      // Integers are encoded as strings on the wire.
      return farmhash::Fingerprint64(absl::StrCat(value.uint64_value()));
    case googlesql::TYPE_BOOL:
      return farmhash::Fingerprint(value.bool_value());
    case googlesql::TYPE_FLOAT:
      return farmhash::Fingerprint(value.float_value());
    case googlesql::TYPE_DOUBLE:
      return farmhash::Fingerprint(value.double_value());
    case googlesql::TYPE_STRING:
      return farmhash::Fingerprint64(value.string_value());
    case googlesql::TYPE_BYTES:
      return farmhash::Fingerprint64(value.bytes_value());
    case googlesql::TYPE_ARRAY: {
      uint64_t result = 0;
      for (const googlesql::Value& element : value.elements()) {
        GOOGLESQL_ASSIGN_OR_RETURN(uint64_t element_hash, Fingerprint(element));
        result += element_hash;
      }
      return result;
    }
    case googlesql::TYPE_STRUCT: {
      uint64_t result = 0;
      for (const googlesql::Value& field : value.fields()) {
        GOOGLESQL_ASSIGN_OR_RETURN(uint64_t field_hash, Fingerprint(field));
        result += field_hash;
      }
      return result;
    }

    default:
      return absl::UnimplementedError(
          absl::StrCat("ML.PREDICT function does not support inputs of type: ",
                       value.type()->TypeName(googlesql::PRODUCT_EXTERNAL,
                                              /*use_external_float32=*/true)));
  }
}

absl::StatusOr<uint64_t> Fingerprint(const googlesql::JSONValueConstRef& json) {
  if (json.IsNumber()) {
    return farmhash::Fingerprint(json.GetDouble());
  } else if (json.IsString()) {
    return farmhash::Fingerprint64(json.GetString());
  } else if (json.IsBoolean()) {
    return farmhash::Fingerprint(json.GetBoolean());
  } else if (json.IsNull()) {
    return 0;
  } else if (json.IsArray()) {
    uint64_t result = 0;
    for (uint64_t i = 0; i < json.GetArraySize(); ++i) {
      GOOGLESQL_ASSIGN_OR_RETURN(uint64_t element_hash,
                       Fingerprint(json.GetArrayElement(i)));
      result += element_hash;
    }
    return result;
  } else if (json.IsObject()) {
    uint64_t result = 0;
    for (const auto& [key, value] : json.GetMembers()) {
      GOOGLESQL_ASSIGN_OR_RETURN(uint64_t field_hash, Fingerprint(value));
      result += field_hash;
    }
    return result;
  } else {
    GOOGLESQL_RET_CHECK_FAIL() << "Unexpected JSON value type";
  }
}

absl::StatusOr<googlesql::Value> ToValue(uint64_t fingerprint,
                                         const googlesql::Type* type) {
  switch (type->kind()) {
    case googlesql::TYPE_INT32:
      return googlesql::Value::Int32(fingerprint);
    case googlesql::TYPE_INT64:
      return googlesql::Value::Int64(fingerprint);
    case googlesql::TYPE_BOOL:
      return googlesql::Value::Bool(fingerprint % 2 == 0);
    case googlesql::TYPE_FLOAT:
      return googlesql::Value::Float(fingerprint);
    case googlesql::TYPE_DOUBLE:
      return googlesql::Value::Double(fingerprint);
    case googlesql::TYPE_STRING:
      return googlesql::Value::String(absl::StrCat(fingerprint));
    case googlesql::TYPE_BYTES:
      return googlesql::Value::Bytes(absl::StrCat(fingerprint));
    case googlesql::TYPE_ARRAY: {
      GOOGLESQL_ASSIGN_OR_RETURN(googlesql::Value element_value,
                       ToValue(fingerprint, type->AsArray()->element_type()));
      return googlesql::Value::MakeArray(type->AsArray(), {element_value});
    }
    case googlesql::TYPE_STRUCT: {
      std::vector<googlesql::Value> field_values;
      for (const googlesql::StructField& field : type->AsStruct()->fields()) {
        GOOGLESQL_ASSIGN_OR_RETURN(googlesql::Value field_value,
                         ToValue(fingerprint, field.type));
        field_values.push_back(field_value);
      }
      return googlesql::Value::MakeStruct(type->AsStruct(), field_values);
    }
    default:
      return absl::UnimplementedError(
          absl::StrCat("ML.PREDICT function does not support outputs of type: ",
                       type->TypeName(googlesql::PRODUCT_EXTERNAL,
                                      /*use_external_float32=*/true)));
  }
}

// Default prediction implementation. Takes a fingerprint of all model input
// values, then fills out model output columns by casting the hash value to
// output column type.
absl::Status ModelEvaluator::DefaultPredict(
    const googlesql::Model* model,
    const CaseInsensitiveStringMap<const ModelColumn>& model_inputs,
    CaseInsensitiveStringMap<ModelColumn>& model_outputs) {
  uint64_t input_hash = 0;
  for (const auto& model_input : model_inputs) {
    GOOGLESQL_ASSIGN_OR_RETURN(uint64_t column_hash,
                     Fingerprint(*model_input.second.value));
    input_hash += column_hash;
  }

  for (const auto& model_output : model_outputs) {
    GOOGLESQL_ASSIGN_OR_RETURN(
        *model_output.second.value,
        ToValue(input_hash, model_output.second.model_column->GetType()));
  }

  return absl::OkStatus();
}

absl::Status ModelEvaluator::Predict(
    const googlesql::Model* model,
    const CaseInsensitiveStringMap<const ModelColumn>& model_inputs,
    CaseInsensitiveStringMap<ModelColumn>& model_outputs) {
  // Custom model prediction logic can be added here.
  return DefaultPredict(model, model_inputs, model_outputs);
}

// Default prediction implementation for PG. Takes a fingerprint of all model
// input and produces a single "Outcome" boolean field.
absl::Status DefaultPgPredict(absl::string_view endpoint,
                              const googlesql::JSONValueConstRef& instance,
                              const googlesql::JSONValueConstRef& parameters,
                              googlesql::JSONValueRef prediction) {
  GOOGLESQL_ASSIGN_OR_RETURN(uint64_t input_hash, Fingerprint(instance));
  prediction.SetToEmptyObject();
  prediction.GetMember("Outcome").SetBoolean(input_hash % 2 == 0);
  return absl::OkStatus();
}

absl::Status ModelEvaluator::PgPredict(
    absl::string_view endpoint, const googlesql::JSONValueConstRef& instance,
    const googlesql::JSONValueConstRef& parameters,
    googlesql::JSONValueRef prediction) {
  // Custom model prediction logic can be added here.
  return DefaultPgPredict(endpoint, instance, parameters, prediction);
}

}  // namespace google::spanner::emulator::backend
