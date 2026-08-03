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

#include "backend/query/remote_udf/remote_udf_evaluator.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "googlesql/public/function.h"
#include "googlesql/public/functions/convert_string.h"
#include "googlesql/public/functions/date_time_util.h"
#include "googlesql/public/functions/json.h"
#include "googlesql/public/functions/json_format.h"
#include "googlesql/public/interval_value.h"
#include "googlesql/public/json_value.h"
#include "googlesql/public/numeric_value.h"
#include "googlesql/public/type.pb.h"
#include "googlesql/public/types/struct_type.h"
#include "googlesql/public/types/timestamp_util.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/uuid_value.h"
#include "googlesql/public/value.h"
#include "absl/base/const_init.h"
#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/flags/flag.h"
#include "absl/numeric/int128.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/escaping.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "httplib.h"
#include "googlesql/base/ret_check.h"
#include "googlesql/base/status_macros.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_numeric_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_oid_type.h"
#include "third_party/spanner_pg/datatypes/extended/spanner_extended_type.h"
#include "google/protobuf/descriptor.h"
#include "farmhash.h"

ABSL_FLAG(std::string, remote_functions_host_port, "",
          "Host and port for remote functions backend. E.g. localhost:8080");

namespace google::spanner::emulator::backend {

using postgres_translator::spangres::datatypes::SpannerExtendedType;
using TypeAnnotationCode = ::google::spanner::v1::TypeAnnotationCode;

// Share http clients between requests to avoid TCP connection overhead.
// httplib::Client is thread-safe, although requests will be serialized.
// If this becomes a problem, create connection pool.
absl::StatusOr<httplib::Client*> GetHttpClient(absl::string_view host_port) {
  static absl::NoDestructor<
      absl::flat_hash_map<std::string, std::unique_ptr<httplib::Client>>>
      shared_clients;

  static absl::Mutex mutex(absl::kConstInit);
  absl::MutexLock lock(mutex);

  if (auto client = shared_clients->find(host_port);
      client != shared_clients->end()) {
    return client->second.get();
  }

  if (!absl::StartsWith(host_port, "localhost:")) {
    return absl::FailedPreconditionError(
        "Remote functions can connect only to localhost ports.");
  }

  return shared_clients
      ->emplace(host_port,
                std::make_unique<httplib::Client>(std::string(host_port)))
      .first->second.get();
}

absl::StatusOr<googlesql::JSONValue> RemoteUdfEvaluator::EvaluateRemoteFunction(
    absl::string_view endpoint, absl::string_view schema_object_name,
    absl::Span<const googlesql::Value> args) {
  // Serialize the call to JSON.
  googlesql::JSONValue calls;
  googlesql::JSONValueRef calls_ref = calls.GetRef();
  calls_ref.SetToEmptyArray();

  googlesql::JSONValue call;
  googlesql::JSONValueRef call_ref = call.GetRef();
  call_ref.SetToEmptyArray();
  for (const auto& arg : args) {
    GOOGLESQL_ASSIGN_OR_RETURN(googlesql::JSONValue call_arg,
                     RemoteUdfProtocol::ToJson(arg));
    GOOGLESQL_RETURN_IF_ERROR(call_ref.AppendArrayElement(std::move(call_arg)));
  }
  GOOGLESQL_RETURN_IF_ERROR(calls_ref.AppendArrayElement(std::move(call)));

  // Build the request body.
  googlesql::JSONValue json_body;
  googlesql::JSONValueRef json_body_ref = json_body.GetRef();
  json_body_ref.SetToEmptyObject();
  json_body_ref.GetMember("_spanner_schema_object")
      .SetString(schema_object_name);
  json_body_ref.GetMember("_spanner_endpoint").SetString(endpoint);
  json_body_ref.GetMember("caller").SetString("");
  json_body_ref.GetMember("sessionUser").SetString("");
  json_body_ref.GetMember("userDefinedContext").SetToEmptyObject();
  json_body_ref.GetMember("requestId")
      .SetString("00000000-0000-0000-0000-000000000000");
  json_body_ref.GetMember("calls").Set(std::move(calls));

  GOOGLESQL_ASSIGN_OR_RETURN(
      httplib::Client * http,
      GetHttpClient(absl::GetFlag(FLAGS_remote_functions_host_port)));

  const std::string path = "/";
  const std::string content_type = "application/json";
  httplib::Result result =
      http->Post(path, json_body.GetConstRef().ToString(), content_type);
  if (!result) {
    return absl::FailedPreconditionError(absl::StrCat(
        "Remote function call failed. Error: ", result.error(), ". Endpoint: ",
        endpoint, ". Schema object name: ", schema_object_name));
  }
  if (result->status != 200) {
    return absl::FailedPreconditionError(absl::StrCat(
        "Remote function call failed. Status: ", result->status,
        ". Endpoint: ", endpoint, ". Schema object name: ", schema_object_name,
        ". Body: ", result->body));
  }

  GOOGLESQL_ASSIGN_OR_RETURN(googlesql::JSONValue json_response,
                   googlesql::JSONValue::ParseJSONString(result->body));
  googlesql::JSONValueConstRef json_response_ref = json_response.GetConstRef();
  if (!json_response_ref.IsObject()) {
    return absl::FailedPreconditionError(absl::StrCat(
        "Remote function call did not return a JSON object. Endpoint: ",
        endpoint, ". Schema object name: ", schema_object_name,
        ". Body: ", result->body));
  }

  if (json_response_ref.HasMember("errorMessage")) {
    return absl::FailedPreconditionError(
        json_response_ref.GetMember("errorMessage").GetString());
  }

  if (!json_response_ref.HasMember("replies") ||
      !json_response_ref.GetMember("replies").IsArray()) {
    return absl::FailedPreconditionError(absl::StrCat(
        "Remote function call did not return a replies array. Endpoint: ",
        endpoint, ". Schema object name: ", schema_object_name,
        ". Body: ", result->body));
  }

  if (json_response_ref.GetMember("replies").GetArraySize() != 1) {
    return absl::FailedPreconditionError(absl::StrCat(
        "Unexpected number of replies from remote function call. Endpoint: ",
        endpoint, ". Schema object name: ", schema_object_name,
        ". Body: ", result->body));
  }

  // Return the only reply.
  return googlesql::JSONValue::CopyFrom(
      json_response_ref.GetMember("replies").GetArrayElement(0));
}

absl::StatusOr<googlesql::Value>
RemoteUdfEvaluator::EvaluatePseudoRandomRemoteFunction(
    absl::Span<const googlesql::Value> args, const googlesql::Type* type) {
  GOOGLESQL_ASSIGN_OR_RETURN(uint64_t fingerprint, RemoteUdfProtocol::Fingerprint(args));
  return RemoteUdfProtocol::ToValue(fingerprint, type);
}

googlesql::FunctionEvaluator RemoteUdfEvaluator::BuildEvaluator(
    std::string endpoint, std::string schema_object_name,
    const googlesql::Type* return_type) {
  return googlesql::FunctionEvaluator(
      [endpoint, schema_object_name,
       return_type](absl::Span<const googlesql::Value> args)
          -> absl::StatusOr<googlesql::Value> {
        if (!absl::GetFlag(FLAGS_remote_functions_host_port).empty()) {
          GOOGLESQL_ASSIGN_OR_RETURN(
              googlesql::JSONValue response,
              EvaluateRemoteFunction(endpoint, schema_object_name, args));
          return RemoteUdfProtocol::ToValue(response.GetConstRef(),
                                            return_type);
        } else {
          return EvaluatePseudoRandomRemoteFunction(args, return_type);
        }
      });
}

absl::StatusOr<googlesql::Value> RemoteUdfProtocol::ToValue(
    uint64_t fingerprint, const googlesql::Type* type) {
  switch (type->kind()) {
    case googlesql::TYPE_INT32:
      return googlesql::Value::Int32(fingerprint);
    case googlesql::TYPE_INT64:
      return googlesql::Value::Int64(fingerprint);
    case googlesql::TYPE_UINT32:
      return googlesql::Value::Uint32(fingerprint);
    case googlesql::TYPE_UINT64:
      return googlesql::Value::Uint64(fingerprint);
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
    case googlesql::TYPE_PROTO:
      return googlesql::Value::Proto(type->AsProto(), absl::Cord());
    case googlesql::TYPE_DATE:
      return googlesql::Value::Date(
          googlesql::types::kDateMin +
          (fingerprint %
           (googlesql::types::kDateMax - googlesql::types::kDateMin)));
    case googlesql::TYPE_ENUM: {
      const google::protobuf::EnumDescriptor* enum_descriptor =
          type->AsEnum()->enum_descriptor();
      return googlesql::Value::Enum(
          type->AsEnum(),
          enum_descriptor->value(fingerprint % enum_descriptor->value_count())
              ->number());
    }
    case googlesql::TYPE_TIMESTAMP:
      return googlesql::Value::Timestamp(absl::FromUnixMicros(
          googlesql::types::kTimestampMin +
          (fingerprint % (googlesql::types::kTimestampMax -
                          googlesql::types::kTimestampMin))));
    case googlesql::TYPE_NUMERIC:
      return googlesql::values::Numeric(static_cast<int64_t>(fingerprint));
    case googlesql::TYPE_JSON: {
      googlesql::JSONValue json_value;
      json_value.GetRef().SetUInt64(fingerprint);
      return googlesql::Value::Json(std::move(json_value));
    }
    case googlesql::TYPE_EXTENDED: {
      switch (static_cast<const SpannerExtendedType*>(type)->code()) {
        case TypeAnnotationCode::PG_JSONB:
          return postgres_translator::spangres::datatypes::CreatePgJsonbValue(
              absl::StrCat(fingerprint));
        case TypeAnnotationCode::PG_NUMERIC:
          return postgres_translator::spangres::datatypes::CreatePgNumericValue(
              absl::StrCat(fingerprint));
        case TypeAnnotationCode::PG_OID:
          return postgres_translator::spangres::datatypes::CreatePgOidValue(
              static_cast<uint32_t>(fingerprint));
        default:
          break;
      }
      break;
    }
    case googlesql::TYPE_INTERVAL: {
      GOOGLESQL_ASSIGN_OR_RETURN(
          googlesql::IntervalValue interval_value,
          googlesql::IntervalValue::FromMicros(
              fingerprint % (googlesql::IntervalValue::kMaxMicros -
                             googlesql::IntervalValue::kMinMicros)));
      return googlesql::Value::Interval(std::move(interval_value));
    }
    case googlesql::TYPE_UUID: {
      return googlesql::Value::Uuid(googlesql::UuidValue::FromPackedInt(
          (__int128)fingerprint << 64 | fingerprint));
    }
    case googlesql::TYPE_ARRAY: {
      GOOGLESQL_ASSIGN_OR_RETURN(googlesql::Value element_value,
                       ToValue(fingerprint, type->AsArray()->element_type()));
      return googlesql::Value::MakeArray(type->AsArray(), {element_value});
    }
    case googlesql::TYPE_MAP: {
      GOOGLESQL_ASSIGN_OR_RETURN(googlesql::Value key_value,
                       ToValue(fingerprint, type->AsMap()->key_type()));
      GOOGLESQL_ASSIGN_OR_RETURN(googlesql::Value value,
                       ToValue(fingerprint, type->AsMap()->value_type()));
      return googlesql::Value::MakeMap(type->AsMap(), {{key_value, value}});
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
      break;
  }

  return absl::UnimplementedError(
      absl::StrCat("Remote function does not support outputs of type: ",
                   type->TypeName(googlesql::PRODUCT_EXTERNAL,
                                  /*use_external_float32=*/true)));
}

namespace {
template <typename T>
absl::StatusOr<T> ToIntegerValue(googlesql::JSONValueConstRef json) {
  if (json.IsString()) {
    T result;
    absl::Status error;
    if (!googlesql::functions::StringToNumeric<T>(json.GetString(), &result,
                                                  &error)) {
      return error;
    }
    return result;
  }

  if constexpr (std::is_same_v<T, int32_t>) {
    return googlesql::functions::ConvertJsonToInt32(json);
  } else if constexpr (std::is_same_v<T, int64_t>) {
    return googlesql::functions::ConvertJsonToInt64(json);
  } else if constexpr (std::is_same_v<T, uint32_t>) {
    return googlesql::functions::ConvertJsonToUint32(json);
  } else if constexpr (std::is_same_v<T, uint64_t>) {
    return googlesql::functions::ConvertJsonToUint64(json);
  } else {
    static_assert(false, "Unsupported integer type");
  }
}
}  // namespace

absl::StatusOr<googlesql::Value> RemoteUdfProtocol::ToValue(
    googlesql::JSONValueConstRef json, const googlesql::Type* type) {
  if (json.IsNull()) {
    return googlesql::Value::Null(type);
  }

  switch (type->kind()) {
    case googlesql::TYPE_INT32: {
      GOOGLESQL_ASSIGN_OR_RETURN(int32_t result, ToIntegerValue<int32_t>(json));
      return googlesql::Value::Int32(result);
    }
    case googlesql::TYPE_INT64: {
      GOOGLESQL_ASSIGN_OR_RETURN(int64_t result, ToIntegerValue<int64_t>(json));
      return googlesql::Value::Int64(result);
    }
    case googlesql::TYPE_UINT32: {
      GOOGLESQL_ASSIGN_OR_RETURN(uint32_t result, ToIntegerValue<uint32_t>(json));
      return googlesql::Value::Uint32(result);
    }
    case googlesql::TYPE_UINT64: {
      GOOGLESQL_ASSIGN_OR_RETURN(uint64_t result, ToIntegerValue<uint64_t>(json));
      return googlesql::Value::Uint64(result);
    }
    case googlesql::TYPE_BOOL:
      if (json.IsBoolean()) {
        return googlesql::Value::Bool(json.GetBoolean());
      }
      break;
    case googlesql::TYPE_FLOAT:
      if (json.IsDouble()) {
        return googlesql::Value::Float(json.GetDouble());
      }
      break;
    case googlesql::TYPE_DOUBLE:
      if (json.IsDouble()) {
        return googlesql::Value::Double(json.GetDouble());
      }
      break;
    case googlesql::TYPE_STRING:
      if (json.IsString()) {
        return googlesql::Value::String(json.GetString());
      }
      break;
    case googlesql::TYPE_BYTES:
      if (json.IsString()) {
        std::string decoded_bytes;
        if (!absl::Base64Unescape(json.GetString(), &decoded_bytes)) {
          return absl::InvalidArgumentError(
              absl::StrCat("Failed to decode bytes: ", json.GetString()));
        }

        return googlesql::Value::Bytes(decoded_bytes);
      }
      break;
    case googlesql::TYPE_PROTO:
      if (json.IsString()) {
        std::string decoded_bytes;
        if (!absl::Base64Unescape(json.GetString(), &decoded_bytes)) {
          return absl::InvalidArgumentError(
              absl::StrCat("Failed to decode bytes: ", json.GetString()));
        }
        return googlesql::Value::Proto(type->AsProto(),
                                       absl::Cord(std::move(decoded_bytes)));
      }
      break;
    case googlesql::TYPE_DATE:
      if (json.IsString()) {
        int32_t date;
        GOOGLESQL_RETURN_IF_ERROR(
            googlesql::functions::ConvertStringToDate(json.GetString(), &date));
        return googlesql::Value::Date(date);
      }
      break;
    case googlesql::TYPE_ENUM: {
      GOOGLESQL_ASSIGN_OR_RETURN(int64_t result, ToIntegerValue<int64_t>(json));
      return googlesql::Value::Enum(type->AsEnum(), result);
    }
    case googlesql::TYPE_TIMESTAMP:
      if (json.IsString()) {
        absl::Time time;
        GOOGLESQL_RETURN_IF_ERROR(googlesql::functions::ConvertStringToTimestamp(
            json.GetString(), absl::UTCTimeZone(),
            googlesql::functions::TimestampScale::kMicroseconds,
            /*allow_tz_in_str=*/true, &time));
        return googlesql::Value::Timestamp(time);
      }
      break;
    case googlesql::TYPE_NUMERIC:
      if (json.IsString()) {
        GOOGLESQL_ASSIGN_OR_RETURN(
            googlesql::NumericValue numeric_value,
            googlesql::NumericValue::FromStringStrict(json.GetString()));
        return googlesql::values::Numeric(numeric_value);
      }
      break;
    case googlesql::TYPE_JSON:
      return googlesql::Value::Json(googlesql::JSONValue::CopyFrom(json));

    case googlesql::TYPE_EXTENDED: {
      switch (static_cast<const SpannerExtendedType*>(type)->code()) {
        case TypeAnnotationCode::PG_JSONB:
          return postgres_translator::spangres::datatypes::CreatePgJsonbValue(
              json.ToString());
        case TypeAnnotationCode::PG_NUMERIC:
          if (json.IsString()) {
            return postgres_translator::spangres::datatypes::
                CreatePgNumericValue(json.GetString());
          }
          break;
        case TypeAnnotationCode::PG_OID: {
          GOOGLESQL_ASSIGN_OR_RETURN(uint32_t result, ToIntegerValue<uint32_t>(json));
          return postgres_translator::spangres::datatypes::CreatePgOidValue(
              result);
        }
        default:
          break;
      }
      break;
    }
    case googlesql::TYPE_INTERVAL: {
      if (json.IsString()) {
        GOOGLESQL_ASSIGN_OR_RETURN(googlesql::IntervalValue interval_value,
                         googlesql::IntervalValue::ParseFromISO8601(
                             json.GetString(), /*allow_nanos=*/true));
        return googlesql::Value::Interval(std::move(interval_value));
      }
      break;
    }
    case googlesql::TYPE_UUID: {
      if (json.IsString()) {
        GOOGLESQL_ASSIGN_OR_RETURN(googlesql::UuidValue uuid_value,
                         googlesql::UuidValue::FromString(json.GetString()));
        return googlesql::Value::Uuid(std::move(uuid_value));
      }
      break;
    }
    case googlesql::TYPE_ARRAY: {
      if (json.IsArray()) {
        std::vector<googlesql::Value> element_values;
        for (uint64_t i = 0; i < json.GetArraySize(); ++i) {
          GOOGLESQL_ASSIGN_OR_RETURN(googlesql::Value element_value,
                           ToValue(json.GetArrayElement(i),
                                   type->AsArray()->element_type()));
          element_values.push_back(std::move(element_value));
        }
        return googlesql::Value::MakeArray(type->AsArray(),
                                           std::move(element_values));
      }
      break;
    }
    case googlesql::TYPE_MAP: {
      if (json.IsArray()) {
        std::vector<std::pair<const googlesql::Value, const googlesql::Value>>
            entries;
        for (uint64_t i = 0; i < json.GetArraySize(); ++i) {
          googlesql::JSONValueConstRef entry = json.GetArrayElement(i);
          if (!entry.IsObject() || !entry.HasMember("key") ||
              !entry.HasMember("value") || entry.GetMembers().size() != 2) {
            return absl::InvalidArgumentError(
                absl::StrCat("Invalid map entry: ", entry.ToString(),
                             ". Expected an array of 2-element objects with "
                             "'key' and 'value' members."));
          };

          GOOGLESQL_ASSIGN_OR_RETURN(
              googlesql::Value key_value,
              ToValue(entry.GetMember("key"), type->AsMap()->key_type()));
          GOOGLESQL_ASSIGN_OR_RETURN(
              googlesql::Value value,
              ToValue(entry.GetMember("value"), type->AsMap()->value_type()));
          entries.push_back({key_value, value});
        }
        return googlesql::Value::MakeMap(type, entries);
      }
      break;
    }
    case googlesql::TYPE_STRUCT: {
      if (json.IsObject()) {
        std::vector<googlesql::Value> field_values;
        for (const googlesql::StructField& field : type->AsStruct()->fields()) {
          if (!json.HasMember(field.name)) {
            field_values.push_back(googlesql::Value::Null(field.type));
            continue;
          }
          GOOGLESQL_ASSIGN_OR_RETURN(googlesql::Value field_value,
                           ToValue(json.GetMember(field.name), field.type));
          field_values.push_back(std::move(field_value));
        }
        return googlesql::Value::MakeStruct(type->AsStruct(), field_values);
      }
      break;
    }
    default:
      break;
  }

  return absl::UnimplementedError(
      absl::StrCat("Cannot convert JSON value ", json.ToString(), " to type: ",
                   type->TypeName(googlesql::PRODUCT_EXTERNAL,
                                  /*use_external_float32=*/true)));
}

absl::StatusOr<googlesql::JSONValue> RemoteUdfProtocol::ToJson(
    const googlesql::Value& value) {
  googlesql::JSONValue json_value;
  googlesql::JSONValueRef json_ref = json_value.GetRef();

  if (value.is_null()) {
    json_ref.SetNull();
    return json_value;
  }

  switch (value.type_kind()) {
    case googlesql::TYPE_INT32:
      json_ref.SetInt64(value.int32_value());
      break;
    case googlesql::TYPE_INT64:
      json_ref.SetInt64(value.int64_value());
      break;
    case googlesql::TYPE_UINT32:
      json_ref.SetUInt64(value.uint32_value());
      break;
    case googlesql::TYPE_UINT64:
      json_ref.SetUInt64(value.uint64_value());
      break;
    case googlesql::TYPE_BOOL:
      json_ref.SetBoolean(value.bool_value());
      break;
    case googlesql::TYPE_FLOAT:
      json_ref.SetDouble(value.float_value());
      break;
    case googlesql::TYPE_DOUBLE:
      json_ref.SetDouble(value.double_value());
      break;
    case googlesql::TYPE_STRING:
      json_ref.SetString(value.string_value());
      break;
    case googlesql::TYPE_BYTES:
      json_ref.SetString(absl::Base64Escape(value.bytes_value()));
      break;
    case googlesql::TYPE_PROTO:
      json_ref.SetString(absl::Base64Escape(std::string(value.proto_value())));
      break;
    case googlesql::TYPE_DATE: {
      std::string date_string;
      GOOGLESQL_RETURN_IF_ERROR(
          googlesql::functions::JsonFromDate(value.date_value(), &date_string,
                                             /*quote_output_string=*/false));
      json_ref.SetString(std::move(date_string));
      break;
    }
    case googlesql::TYPE_ENUM: {
      json_ref.SetInt64(value.enum_value());
      break;
    }
    case googlesql::TYPE_TIMESTAMP: {
      std::string timestamp_string;
      GOOGLESQL_RETURN_IF_ERROR(googlesql::functions::JsonFromTimestamp(
          value.ToTime(), &timestamp_string,
          /*quote_output_string=*/false));
      json_ref.SetString(std::move(timestamp_string));
      break;
    }
    case googlesql::TYPE_NUMERIC:
      json_ref.SetString(value.numeric_value().ToString());
      break;
    case googlesql::TYPE_JSON:
      json_value = googlesql::JSONValue::CopyFrom(value.json_value());
      break;
    case googlesql::TYPE_EXTENDED: {
      switch (static_cast<const SpannerExtendedType*>(value.type())->code()) {
        case TypeAnnotationCode::PG_JSONB: {
          GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord jsonb_value,
                           postgres_translator::spangres::datatypes::
                               GetPgJsonbNormalizedValue(value));
          GOOGLESQL_ASSIGN_OR_RETURN(json_value, googlesql::JSONValue::ParseJSONString(
                                           absl::StrCat(jsonb_value)));
          break;
        }
        case TypeAnnotationCode::PG_NUMERIC: {
          GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord numeric_value,
                           postgres_translator::spangres::datatypes::
                               GetPgNumericNormalizedValue(value));
          json_ref.SetString(absl::StrCat(numeric_value));
          break;
        }
        case TypeAnnotationCode::PG_OID: {
          GOOGLESQL_ASSIGN_OR_RETURN(
              int64_t oid_value,
              postgres_translator::spangres::datatypes::GetPgOidValue(value));
          json_ref.SetUInt64(oid_value);
          break;
        }
        default:
          break;
      }
      break;
    }
    case googlesql::TYPE_INTERVAL: {
      json_ref.SetString(value.interval_value().ToISO8601());
      break;
    }
    case googlesql::TYPE_UUID: {
      GOOGLESQL_ASSIGN_OR_RETURN(googlesql::UuidValue uuid_value, value.uuid_value());
      json_ref.SetString(uuid_value.ToString());
      break;
    }
    case googlesql::TYPE_ARRAY: {
      json_ref.SetToEmptyArray();
      for (const googlesql::Value& element : value.elements()) {
        GOOGLESQL_ASSIGN_OR_RETURN(googlesql::JSONValue element_json, ToJson(element));
        GOOGLESQL_RETURN_IF_ERROR(json_ref.AppendArrayElement(std::move(element_json)));
      }
      break;
    }
    case googlesql::TYPE_MAP: {
      json_ref.SetToEmptyArray();
      for (const auto& [key, value] : value.map_entries()) {
        GOOGLESQL_ASSIGN_OR_RETURN(googlesql::JSONValue key_json, ToJson(key));
        GOOGLESQL_ASSIGN_OR_RETURN(googlesql::JSONValue value_json, ToJson(value));
        googlesql::JSONValue entry;
        entry.GetRef().SetToEmptyObject();
        entry.GetRef().GetMember("key").Set(std::move(key_json));
        entry.GetRef().GetMember("value").Set(std::move(value_json));
        GOOGLESQL_RETURN_IF_ERROR(json_ref.AppendArrayElement(std::move(entry)));
      }
      break;
    }
    case googlesql::TYPE_STRUCT: {
      json_ref.SetToEmptyObject();
      const googlesql::StructType* struct_type = value.type()->AsStruct();
      GOOGLESQL_RET_CHECK_NE(struct_type, nullptr);
      for (int i = 0; i < struct_type->num_fields(); ++i) {
        GOOGLESQL_ASSIGN_OR_RETURN(googlesql::JSONValue field_json,
                         ToJson(value.field(i)));
        json_ref.GetMember(struct_type->field(i).name)
            .Set(std::move(field_json));
      }
      break;
    }
    default:
      return absl::UnimplementedError(absl::StrCat(
          "Cannot convert value: ", value.DebugString(), " to JSON"));
  }

  return json_value;
}

absl::StatusOr<uint64_t> RemoteUdfProtocol::Fingerprint(
    absl::Span<const googlesql::Value> values) {
  uint64_t fingerprint = 0;
  for (const auto& value : values) {
    GOOGLESQL_ASSIGN_OR_RETURN(uint64_t value_fingerprint,
                     RemoteUdfProtocol::Fingerprint(value));
    fingerprint += value_fingerprint;
  }
  return fingerprint;
}

absl::StatusOr<uint64_t> RemoteUdfProtocol::Fingerprint(
    const googlesql::Value& value) {
  if (value.is_null()) {
    return 0;
  }

  switch (value.type_kind()) {
    case googlesql::TYPE_INT32:
      return farmhash::Fingerprint64(absl::StrCat(value.int32_value()));
    case googlesql::TYPE_INT64:
      return farmhash::Fingerprint64(absl::StrCat(value.int64_value()));
    case googlesql::TYPE_UINT32:
      return farmhash::Fingerprint64(absl::StrCat(value.uint32_value()));
    case googlesql::TYPE_UINT64:
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
    case googlesql::TYPE_PROTO:
      return farmhash::Fingerprint64(absl::StrCat(value.proto_value()));
    case googlesql::TYPE_DATE:
      return farmhash::Fingerprint(value.date_value());
    case googlesql::TYPE_ENUM:
      return farmhash::Fingerprint64(absl::StrCat(value.enum_value()));
    case googlesql::TYPE_TIMESTAMP:
      return farmhash::Fingerprint(value.ToUnixMicros());
    case googlesql::TYPE_NUMERIC:
      return farmhash::Fingerprint64(value.numeric_value().ToString());
    case googlesql::TYPE_JSON:
      return farmhash::Fingerprint64(value.json_value().ToString());
    case googlesql::TYPE_EXTENDED:
      switch (static_cast<const SpannerExtendedType*>(value.type())->code()) {
        case TypeAnnotationCode::PG_JSONB: {
          GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord jsonb_value,
                           postgres_translator::spangres::datatypes::
                               GetPgJsonbNormalizedValue(value));
          return farmhash::Fingerprint64(absl::StrCat(jsonb_value));
        }
        case TypeAnnotationCode::PG_NUMERIC: {
          GOOGLESQL_ASSIGN_OR_RETURN(absl::Cord numeric_value,
                           postgres_translator::spangres::datatypes::
                               GetPgNumericNormalizedValue(value));
          return farmhash::Fingerprint64(absl::StrCat(numeric_value));
        }
        case TypeAnnotationCode::PG_OID: {
          GOOGLESQL_ASSIGN_OR_RETURN(
              int64_t oid_value,
              postgres_translator::spangres::datatypes::GetPgOidValue(value));
          return farmhash::Fingerprint64(absl::StrCat(oid_value));
        }
        default:
          break;
      }
      break;
    case googlesql::TYPE_INTERVAL: {
      return farmhash::Fingerprint64(value.interval_value().ToString());
    }
    case googlesql::TYPE_UUID: {
      GOOGLESQL_ASSIGN_OR_RETURN(googlesql::UuidValue uuid_value, value.uuid_value());
      return farmhash::Fingerprint64(uuid_value.ToString());
    }
    case googlesql::TYPE_ARRAY: {
      uint64_t result = 0;
      for (const googlesql::Value& element : value.elements()) {
        GOOGLESQL_ASSIGN_OR_RETURN(uint64_t element_hash, Fingerprint(element));
        result += element_hash;
      }
      return result;
    }
    case googlesql::TYPE_MAP: {
      uint64_t result = 0;
      for (const auto& [key, value] : value.map_entries()) {
        GOOGLESQL_ASSIGN_OR_RETURN(uint64_t key_hash, Fingerprint(key));
        GOOGLESQL_ASSIGN_OR_RETURN(uint64_t value_hash, Fingerprint(value));
        result += farmhash::Fingerprint(value_hash + key_hash);
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
      break;
  }

  return absl::UnimplementedError(
      absl::StrCat("Remote function does not support inputs of type: ",
                   value.type()->TypeName(googlesql::PRODUCT_EXTERNAL,
                                          /*use_external_float32=*/true)));
}

absl::StatusOr<uint64_t> RemoteUdfProtocol::Fingerprint(
    const googlesql::JSONValueConstRef& json) {
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
    for (const auto& [unused_key, value] : json.GetMembers()) {
      GOOGLESQL_ASSIGN_OR_RETURN(uint64_t field_hash, Fingerprint(value));
      result += field_hash;
    }
    return result;
  } else {
    GOOGLESQL_RET_CHECK_FAIL() << "Unexpected JSON value type";
  }
}

}  // namespace google::spanner::emulator::backend
