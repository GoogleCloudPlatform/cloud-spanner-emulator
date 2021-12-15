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

#include "frontend/converters/values.h"

#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/base/statusor.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/strip.h"
#include "absl/time/time.h"
#include "common/constants.h"
#include "common/errors.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {

// Time format used by Cloud Spanner to encode timestamps.
constexpr char kRFC3339TimeFormatNoOffset[] = "%E4Y-%m-%dT%H:%M:%E*S";

}  // namespace

zetasql_base::StatusOr<zetasql::Value> ValueFromProto(
    const google::protobuf::Value& value_pb, const zetasql::Type* type) {
  if (value_pb.kind_case() == google::protobuf::Value::kNullValue) {
    return zetasql::values::Null(type);
  }

  switch (type->kind()) {
    case zetasql::TypeKind::TYPE_BOOL: {
      if (value_pb.kind_case() != google::protobuf::Value::kBoolValue) {
        return error::ValueProtoTypeMismatch(value_pb.DebugString(),
                                             type->DebugString());
      }
      return zetasql::values::Bool(value_pb.bool_value());
    }

    case zetasql::TypeKind::TYPE_INT64: {
      int64_t num = 0;
      if (value_pb.kind_case() != google::protobuf::Value::kStringValue) {
        return error::ValueProtoTypeMismatch(value_pb.DebugString(),
                                             type->DebugString());
      }
      if (!absl::SimpleAtoi(value_pb.string_value(), &num)) {
        return error::CouldNotParseStringAsInteger(value_pb.string_value());
      }
      return zetasql::values::Int64(num);
    }

    case zetasql::TypeKind::TYPE_DOUBLE: {
      double val = 0;
      if (value_pb.kind_case() == google::protobuf::Value::kStringValue) {
        if (value_pb.string_value() == "Infinity") {
          val = std::numeric_limits<double>::infinity();
        } else if (value_pb.string_value() == "-Infinity") {
          val = -std::numeric_limits<double>::infinity();
        } else if (value_pb.string_value() == "NaN") {
          val = std::numeric_limits<double>::quiet_NaN();
        } else {
          return error::CouldNotParseStringAsDouble(value_pb.string_value());
        }
      } else if (value_pb.kind_case() ==
                 google::protobuf::Value::kNumberValue) {
        val = value_pb.number_value();
      } else {
        return error::ValueProtoTypeMismatch(value_pb.DebugString(),
                                             type->DebugString());
      }
      return zetasql::values::Double(val);
    }

    case zetasql::TypeKind::TYPE_TIMESTAMP: {
      if (value_pb.kind_case() != google::protobuf::Value::kStringValue) {
        return error::ValueProtoTypeMismatch(value_pb.DebugString(),
                                             type->DebugString());
      }
      if (value_pb.string_value() == kCommitTimestampIdentifier) {
        return zetasql::values::String(value_pb.string_value());
      }
      absl::string_view time_str(value_pb.string_value());
      if (!absl::ConsumeSuffix(&time_str, "Z")) {
        return error::TimestampMustBeInUTCTimeZone(value_pb.string_value());
      }
      absl::Time time;
      std::string error;
      if (!absl::ParseTime(kRFC3339TimeFormatNoOffset, std::string(time_str),
                           &time, &error)) {
        return error::CouldNotParseStringAsTimestamp(value_pb.string_value(),
                                                     error);
      }
      if (!zetasql::functions::IsValidTime(time)) {
        return error::TimestampOutOfRange(
            absl::FormatTime(time, absl::UTCTimeZone()));
      }
      return zetasql::values::Timestamp(time);
    }

    case zetasql::TypeKind::TYPE_DATE: {
      if (value_pb.kind_case() != google::protobuf::Value::kStringValue) {
        return error::ValueProtoTypeMismatch(value_pb.DebugString(),
                                             type->DebugString());
      }
      absl::CivilDay date;
      if (!absl::ParseCivilTime(value_pb.string_value(), &date)) {
        return error::CouldNotParseStringAsDate(value_pb.string_value());
      }
      if (date.year() < 1 || date.year() > 9999) {
        return error::InvalidDate(value_pb.string_value());
      }
      absl::CivilDay epoch_date(1970, 1, 1);
      return zetasql::values::Date(static_cast<int32_t>(date - epoch_date));
    }

    case zetasql::TypeKind::TYPE_STRING: {
      if (value_pb.kind_case() != google::protobuf::Value::kStringValue) {
        return error::ValueProtoTypeMismatch(value_pb.DebugString(),
                                             type->DebugString());
      }
      return zetasql::values::String(value_pb.string_value());
    }

    case zetasql::TypeKind::TYPE_BYTES: {
      if (value_pb.kind_case() != google::protobuf::Value::kStringValue) {
        return error::ValueProtoTypeMismatch(value_pb.DebugString(),
                                             type->DebugString());
      }
      std::string bytes;
      if (!absl::Base64Unescape(value_pb.string_value(), &bytes)) {
        return error::CouldNotParseStringAsBytes(value_pb.string_value());
      }
      return zetasql::values::Bytes(bytes);
    }

    case zetasql::TypeKind::TYPE_NUMERIC: {
      if (value_pb.kind_case() != google::protobuf::Value::kStringValue) {
        return error::ValueProtoTypeMismatch(value_pb.DebugString(),
                                             type->DebugString());
      }
      auto status_or_numeric =
          zetasql::NumericValue::FromStringStrict(value_pb.string_value());
      if (!status_or_numeric.ok()) {
        return error::CouldNotParseStringAsNumeric(value_pb.string_value());
      }
      return zetasql::values::Numeric(status_or_numeric.value());
    }

    case zetasql::TypeKind::TYPE_JSON: {
      if (value_pb.kind_case() != google::protobuf::Value::kStringValue) {
        return error::ValueProtoTypeMismatch(value_pb.DebugString(),
                                             type->DebugString());
      }
      auto status_or_json =
          zetasql::JSONValue::ParseJSONString(value_pb.string_value());
      if (!status_or_json.ok()) {
        return error::CouldNotParseStringAsJson(value_pb.string_value());
      }
      return zetasql::values::Json(std::move(status_or_json.value()));
    }

    case zetasql::TypeKind::TYPE_ARRAY: {
      if (value_pb.kind_case() != google::protobuf::Value::kListValue) {
        return error::ValueProtoTypeMismatch(value_pb.DebugString(),
                                             type->DebugString());
      }
      std::vector<zetasql::Value> values(value_pb.list_value().values_size());
      for (int i = 0; i < value_pb.list_value().values_size(); ++i) {
        const google::protobuf::Value& element_pb =
            value_pb.list_value().values(i);
        ZETASQL_ASSIGN_OR_RETURN(
            values[i],
            ValueFromProto(element_pb, type->AsArray()->element_type()),
            _ << "\nWhen parsing array element #" << i << ": {"
              << element_pb.DebugString() << "} in " << value_pb.DebugString());
      }
      return zetasql::values::Array(type->AsArray(), values);
    }

    case zetasql::TypeKind::TYPE_STRUCT: {
      if (value_pb.kind_case() != google::protobuf::Value::kListValue) {
        return error::ValueProtoTypeMismatch(value_pb.DebugString(),
                                             type->DebugString());
      }
      std::vector<zetasql::Value> values(value_pb.list_value().values_size());
      for (int i = 0; i < value_pb.list_value().values_size(); ++i) {
        const google::protobuf::Value& field_pb =
            value_pb.list_value().values(i);
        ZETASQL_ASSIGN_OR_RETURN(
            values[i],
            ValueFromProto(field_pb, type->AsStruct()->field(i).type),
            _ << "\nWhen parsing struct element #" << i << ": {"
              << field_pb.DebugString() << "} in " << value_pb.DebugString());
      }
      return zetasql::values::Struct(type->AsStruct(), values);
    }

    default: {
      return error::Internal(absl::StrCat(
          "Cloud Spanner unsupported type ", type->DebugString(),
          " passed to ValueFromProto when parsing ", value_pb.DebugString()));
    }
  }
}

zetasql_base::StatusOr<google::protobuf::Value> ValueToProto(
    const zetasql::Value& value) {
  if (!value.is_valid()) {
    return error::Internal(
        "Uninitialized ZetaSQL value passed to ValueToProto");
  }

  google::protobuf::Value value_pb;
  if (value.is_null()) {
    value_pb.set_null_value(google::protobuf::NullValue());
    return value_pb;
  }

  switch (value.type_kind()) {
    case zetasql::TypeKind::TYPE_BOOL: {
      value_pb.set_bool_value(value.bool_value());
      break;
    }

    case zetasql::TypeKind::TYPE_INT64: {
      value_pb.set_string_value(absl::StrCat(value.int64_value()));
      break;
    }

    case zetasql::TypeKind::TYPE_DOUBLE: {
      double val = value.double_value();
      if (std::isfinite(val)) {
        value_pb.set_number_value(val);
      } else if (val == std::numeric_limits<double>::infinity()) {
        value_pb.set_string_value("Infinity");
      } else if (val == -std::numeric_limits<double>::infinity()) {
        value_pb.set_string_value("-Infinity");
      } else if (std::isnan(val)) {
        value_pb.set_string_value("NaN");
      } else {
        return error::Internal(absl::StrCat("Unsupported double value ",
                                            value.double_value(),
                                            " passed to ValueToProto"));
      }
      break;
    }

    case zetasql::TypeKind::TYPE_TIMESTAMP: {
      value_pb.set_string_value(
          absl::StrCat(absl::FormatTime(kRFC3339TimeFormatNoOffset,
                                        value.ToTime(), absl::UTCTimeZone()),
                       "Z"));
      break;
    }

    case zetasql::TypeKind::TYPE_DATE: {
      int32_t days_since_epoch = value.date_value();
      absl::CivilDay epoch_date(1970, 1, 1);
      absl::CivilDay date = epoch_date + days_since_epoch;
      if (date.year() > 9999 || date.year() < 1) {
        return error::Internal(absl::StrCat(
            "Unsupported date value ", value.DebugString(),
            " passed to ValueToProto. Year must be between 1 and 9999."));
      }
      absl::StrAppendFormat(value_pb.mutable_string_value(), "%04d-%02d-%02d",
                            date.year(), date.month(), date.day());
      break;
    }

    case zetasql::TypeKind::TYPE_STRING: {
      value_pb.set_string_value(value.string_value());
      break;
    }

    case zetasql::TypeKind::TYPE_NUMERIC: {
      value_pb.set_string_value(value.numeric_value().ToString());
      break;
    }

    case zetasql::TypeKind::TYPE_JSON: {
      value_pb.set_string_value(value.json_string());
      break;
    }

    case zetasql::TypeKind::TYPE_BYTES: {
      absl::Base64Escape(value.bytes_value(), value_pb.mutable_string_value());
      break;
    }

    case zetasql::TypeKind::TYPE_ARRAY: {
      google::protobuf::ListValue* list_value_pb =
          value_pb.mutable_list_value();
      for (int i = 0; i < value.num_elements(); ++i) {
        ZETASQL_ASSIGN_OR_RETURN(*list_value_pb->add_values(),
                         ValueToProto(value.element(i)),
                         _ << "\nWhen encoding array element #" << i << ": "
                           << value.element(i).DebugString() << " in "
                           << value.DebugString());
      }
      break;
    }

    case zetasql::TypeKind::TYPE_STRUCT: {
      google::protobuf::ListValue* list_value_pb =
          value_pb.mutable_list_value();
      for (int i = 0; i < value.num_fields(); ++i) {
        ZETASQL_ASSIGN_OR_RETURN(
            *list_value_pb->add_values(), ValueToProto(value.field(i)),
            _ << "\nWhen encoding struct element #" << i << ": "
              << value.field(i).DebugString() << " in " << value.DebugString());
      }
      break;
    }

    default: {
      return error::Internal(
          absl::StrCat("Cloud Spanner unsupported ZetaSQL value ",
                       value.DebugString(), " passed to ValueToProto"));
    }
  }

  return value_pb;
}

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
