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

#include <cmath>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "google/protobuf/struct.pb.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/interval_value.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/uuid_value.h"
#include "zetasql/public/value.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "absl/time/time.h"
#include "backend/query/search/tokenizer.h"
#include "common/constants.h"
#include "common/errors.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_numeric_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_oid_type.h"
#include "third_party/spanner_pg/datatypes/extended/spanner_extended_type.h"
#include "third_party/spanner_pg/interface/pg_arena.h"
#include "third_party/spanner_pg/interface/pg_arena_factory.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {

using backend::query::search::TokenListFromBytes;
using google::spanner::v1::TypeAnnotationCode;
using postgres_translator::spangres::datatypes::CreatePgJsonbValue;
using postgres_translator::spangres::datatypes::CreatePgNumericValue;
using postgres_translator::spangres::datatypes::CreatePgOidValue;
using postgres_translator::spangres::datatypes::GetPgJsonbNormalizedValue;
using postgres_translator::spangres::datatypes::GetPgNumericNormalizedValue;
using postgres_translator::spangres::datatypes::GetPgOidValue;
using postgres_translator::spangres::datatypes::SpannerExtendedType;

// Time format used by Cloud Spanner to encode timestamps.
constexpr char kRFC3339TimeFormatNoOffset[] = "%E4Y-%m-%dT%H:%M:%E*S";

// Create PG.JSONB value in a valid memory context which is required for calling
// PG code.
static absl::StatusOr<zetasql::Value> CreatePgJsonbValueWithMemoryContext(
    absl::string_view jsonb_string) {
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<postgres_translator::interfaces::PGArena> pg_arena,
      postgres_translator::interfaces::CreatePGArena(nullptr));
  return postgres_translator::spangres::datatypes::CreatePgJsonbValue(
      jsonb_string);
}

// Create PG.NUMERIC value in a valid memory context which is required for
// calling PG code.
static absl::StatusOr<zetasql::Value> CreatePgNumericValueWithMemoryContext(
    absl::string_view numeric_string) {
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<postgres_translator::interfaces::PGArena> pg_arena,
      postgres_translator::interfaces::CreatePGArena(nullptr));
  return postgres_translator::spangres::datatypes::CreatePgNumericValue(
      numeric_string);
}

static bool IsValidFloat(double value) {
  double float_lower_limit =
      static_cast<double>(std::numeric_limits<float>::lowest());
  double float_upper_limit =
      static_cast<double>(std::numeric_limits<float>::max());
  bool is_valid_finite_float = std::isfinite(value) &&
                               float_lower_limit <= value &&
                               value <= float_upper_limit;

  return is_valid_finite_float || std::isinf(value) || std::isnan(value);
}

}  // namespace

absl::StatusOr<zetasql::Value> ValueFromProto(
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

    case zetasql::TypeKind::TYPE_FLOAT: {
      double val = 0;
      if (value_pb.kind_case() == google::protobuf::Value::kStringValue) {
        if (value_pb.string_value() == "Infinity") {
          val = std::numeric_limits<float>::infinity();
        } else if (value_pb.string_value() == "-Infinity") {
          val = -std::numeric_limits<float>::infinity();
        } else if (value_pb.string_value() == "NaN") {
          val = std::numeric_limits<float>::quiet_NaN();
        } else {
          return error::CouldNotParseStringAsFloat(value_pb.string_value());
        }
      } else if (ABSL_PREDICT_TRUE(IsValidFloat(value_pb.number_value()))) {
        val = value_pb.number_value();
      } else {
        return error::ValueProtoTypeMismatch(value_pb.DebugString(),
                                             type->DebugString());
      }
      return zetasql::values::Float(val);
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

    case zetasql::TypeKind::TYPE_EXTENDED: {
      auto type_code = static_cast<const SpannerExtendedType*>(type)->code();
      switch (type_code) {
        case TypeAnnotationCode::PG_JSONB: {
          if (value_pb.kind_case() != google::protobuf::Value::kStringValue) {
            return error::ValueProtoTypeMismatch(value_pb.DebugString(),
                                                 type->DebugString());
          }
          auto pg_jsonb =
              CreatePgJsonbValueWithMemoryContext(value_pb.string_value());
          if (!pg_jsonb.ok()) {
            return error::CouldNotParseStringAsPgJsonb(value_pb.string_value());
          }
          return *pg_jsonb;
        }
        case TypeAnnotationCode::PG_NUMERIC: {
          if (value_pb.kind_case() != google::protobuf::Value::kStringValue) {
            return error::ValueProtoTypeMismatch(value_pb.DebugString(),
                                                 type->DebugString());
          }
          auto pg_numeric =
              CreatePgNumericValueWithMemoryContext(value_pb.string_value());
          if (!pg_numeric.ok()) {
            return error::CouldNotParseStringAsPgNumeric(
                value_pb.string_value());
          }
          return *pg_numeric;
        }
        case TypeAnnotationCode::PG_OID: {
          int64_t oid = 0;
          if (value_pb.kind_case() != google::protobuf::Value::kStringValue) {
            return error::ValueProtoTypeMismatch(value_pb.DebugString(),
                                                 type->DebugString());
          }
          if (!absl::SimpleAtoi(value_pb.string_value(), &oid)) {
            return error::CouldNotParseStringAsPgOid(value_pb.string_value());
          }
          return CreatePgOidValue(oid);
        }
        default:
          return error::Internal(absl::StrCat(
              "Cloud Spanner unsupported type ", type->DebugString(),
              " passed to ValueFromProto when parsing ",
              value_pb.DebugString()));
      }
      break;
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
      if (!absl::ParseTime(kRFC3339TimeFormatNoOffset, time_str, &time,
                           &error)) {
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

    case zetasql::TypeKind::TYPE_TOKENLIST: {
      if (value_pb.kind_case() != google::protobuf::Value::kStringValue) {
        return error::ValueProtoTypeMismatch(value_pb.DebugString(),
                                             type->DebugString());
      }
      std::string bytes;
      if (!absl::Base64Unescape(value_pb.string_value(), &bytes)) {
        return error::CouldNotParseStringAsBytes(value_pb.string_value());
      }
      return TokenListFromBytes(bytes);
    }

    case zetasql::TypeKind::TYPE_INTERVAL: {
      if (value_pb.kind_case() != google::protobuf::Value::kStringValue) {
        return error::ValueProtoTypeMismatch(value_pb.DebugString(),
                                             type->DebugString());
      }

      absl::StatusOr<zetasql::IntervalValue> interval_value =
          zetasql::IntervalValue::Parse(value_pb.string_value(),
                                          /*allow_nanos=*/true);
      if (!interval_value.ok()) {
        return error::CouldNotParseStringAsInterval(
            value_pb.string_value(), interval_value.status().message());
      }
      return zetasql::values::Interval(interval_value.value());
    }

    case zetasql::TypeKind::TYPE_UUID: {
      if (value_pb.kind_case() != google::protobuf::Value::kStringValue) {
        return error::ValueProtoTypeMismatch(value_pb.DebugString(),
                                             type->DebugString());
      }

      absl::StatusOr<zetasql::UuidValue> uuid_value =
          zetasql::UuidValue::FromString(value_pb.string_value());
      if (!uuid_value.ok()) {
        return error::CouldNotParseStringAsUuid(value_pb.string_value(),
                                                uuid_value.status().message());
      }
      return zetasql::values::Uuid(uuid_value.value());
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

    case zetasql::TypeKind::TYPE_PROTO: {
      if (value_pb.kind_case() != google::protobuf::Value::kStringValue) {
        return error::ValueProtoTypeMismatch(value_pb.DebugString(),
                                             type->DebugString());
      }
      std::string bytes;
      if (!absl::Base64Unescape(value_pb.string_value(), &bytes)) {
        return error::CouldNotParseStringAsBytes(value_pb.string_value());
      }
      return zetasql::values::Proto(type->AsProto(), absl::Cord(bytes));
    }
    case zetasql::TypeKind::TYPE_ENUM: {
      if (value_pb.kind_case() != google::protobuf::Value::kStringValue) {
        return error::ValueProtoTypeMismatch(value_pb.DebugString(),
                                             type->DebugString());
      }
      int num = 0;
      if (!absl::SimpleAtoi(value_pb.string_value(), &num)) {
        return error::CouldNotParseStringAsInteger(value_pb.string_value());
      }

      return zetasql::values::Enum(type->AsEnum(), num);
    }

    default: {
      return error::Internal(absl::StrCat(
          "Cloud Spanner unsupported type ", type->DebugString(),
          " passed to ValueFromProto when parsing ", value_pb.DebugString()));
    }
  }
}

absl::StatusOr<google::protobuf::Value> ValueToProto(
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

    case zetasql::TypeKind::TYPE_FLOAT: {
      float val = value.float_value();
      if (std::isfinite(val)) {
        value_pb.set_number_value(static_cast<double>(val));
      } else if (val == std::numeric_limits<float>::infinity()) {
        value_pb.set_string_value("Infinity");
      } else if (val == -std::numeric_limits<float>::infinity()) {
        value_pb.set_string_value("-Infinity");
      } else if (std::isnan(val)) {
        value_pb.set_string_value("NaN");
      } else {
        return error::Internal(absl::StrCat("Unsupported float value ",
                                            value.float_value(),
                                            " passed to ValueToProto"));
      }
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

    case zetasql::TypeKind::TYPE_EXTENDED: {
      auto type_code =
          static_cast<const SpannerExtendedType*>(value.type())->code();
      switch (type_code) {
        case TypeAnnotationCode::PG_JSONB: {
          value_pb.set_string_value(
              std::string(*GetPgJsonbNormalizedValue(value)));
          break;
        }
        case TypeAnnotationCode::PG_NUMERIC: {
          value_pb.set_string_value(
              std::string(*GetPgNumericNormalizedValue(value)));
          break;
        }
        case TypeAnnotationCode::PG_OID: {
          value_pb.set_string_value(absl::StrCat(*GetPgOidValue(value)));
          break;
        }
        default:
          return error::Internal(
              absl::StrCat("Cloud Spanner unsupported ZetaSQL value ",
                           value.DebugString(), " passed to ValueToProto"));
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

    case zetasql::TypeKind::TYPE_ENUM: {
      value_pb.set_string_value(std::to_string(value.enum_value()));
      break;
    }

    case zetasql::TypeKind::TYPE_PROTO: {
      std::string strvalue;
      absl::CopyCordToString(value.ToCord(), &strvalue);
      absl::Base64Escape(strvalue, value_pb.mutable_string_value());
      break;
    }

    case zetasql::TYPE_TOKENLIST: {
      absl::Base64Escape(value.tokenlist_value().GetBytes(),
                         value_pb.mutable_string_value());
      break;
    }

    case zetasql::TypeKind::TYPE_INTERVAL: {
      zetasql::IntervalValue interval_value = value.interval_value();
      value_pb.set_string_value(interval_value.ToISO8601());
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

    case zetasql::TypeKind::TYPE_UUID: {
      ZETASQL_ASSIGN_OR_RETURN(zetasql::UuidValue uuid_value, value.uuid_value());
      value_pb.set_string_value(uuid_value.ToString());
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
