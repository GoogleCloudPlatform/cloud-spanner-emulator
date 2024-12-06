//
// PostgreSQL is released under the PostgreSQL License, a liberal Open Source
// license, similar to the BSD or MIT licenses.
//
// PostgreSQL Database Management System
// (formerly known as Postgres, then as Postgres95)
//
// Portions Copyright © 1996-2020, The PostgreSQL Global Development Group
//
// Portions Copyright © 1994, The Regents of the University of California
//
// Portions Copyright 2023 Google LLC
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
// EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
// FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN
// "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
// MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
//------------------------------------------------------------------------------

#include "third_party/spanner_pg/catalog/type.h"

#include <algorithm>
#include <string>

#include "zetasql/base/logging.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/interval_value.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/types/extended_type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/uuid_value.h"
#include "zetasql/base/no_destructor.h"
#include "absl/hash/hash.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/numbers.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/types/span.h"
#include "third_party/spanner_pg/bootstrap_catalog/bootstrap_catalog.h"
#include "third_party/spanner_pg/catalog/pg_catalog_util.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "third_party/spanner_pg/util/datetime_conversion.h"
#include "third_party/spanner_pg/util/oid_to_string.h"
#include "third_party/spanner_pg/util/postgres.h"
#include "third_party/spanner_pg/util/uuid_conversion.h"
#include "re2/re2.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {

std::string PostgresTypeMapping::DefaultInitializeTypeName(
    const Oid oid) const {
  auto raw_name_or = PgBootstrapCatalog::Default()->GetTypeName(oid);
  ABSL_LOG_IF(FATAL, !raw_name_or.ok())
      << "Oid = " << PostgresTypeOid() << ", status = " << raw_name_or.status();
  return raw_name_or.value();
}

PostgresTypeMapping::PostgresTypeMapping(const zetasql::TypeFactory* factory,
                                         const Oid oid)
    : zetasql::ExtendedType(factory),
      postgres_type_oid_(oid),
      raw_type_name_(DefaultInitializeTypeName(oid)) {}

std::string PostgresTypeMapping::TypeName(zetasql::ProductMode mode) const {
  return AddPostgresPrefix(raw_type_name_);
}

absl::StatusOr<const char*> PostgresTypeMapping::PostgresExternalTypeName()
    const {
  return PgBootstrapCatalog::Default()->GetFormattedTypeName(
      postgres_type_oid_);
}

absl::string_view PostgresTypeMapping::raw_type_name() const {
  return raw_type_name_;
}

bool PostgresTypeMapping::IsSupportedType(
    const zetasql::LanguageOptions& language_options) const {
  return mapped_type() != nullptr;
}

absl::HashState PostgresTypeMapping::HashTypeParameter(
    absl::HashState state) const {
  ABSL_LOG(FATAL) << "Not Implemented";
  return state;
}

absl::Status
PostgresTypeMapping::SerializeToProtoAndDistinctFileDescriptorsImpl(
    const BuildFileDescriptorSetMapOptions& options,
    zetasql::TypeProto* type_proto,
    FileDescriptorSetMap* file_descriptor_set_map) const {
  ABSL_LOG(FATAL) << "Not Implemented";
  return absl::OkStatus();
}

int64_t PostgresTypeMapping::GetEstimatedOwnedMemoryBytesSize() const {
  return sizeof(*this);
}

absl::StatusOr<zetasql::Value> PostgresTypeMapping::MakeGsqlValue(
    const Const* pg_const) const {
  return absl::UnimplementedError(
      absl::StrCat("Unimplemented PostgreSQL data type: ",
                   OidToTypeString(pg_const->consttype)));
}

absl::StatusOr<zetasql::Value>
PostgresTypeMapping::MakeGsqlValueFromStringConst(
    const absl::string_view& string_const) const {
  return absl::UnimplementedError(
      absl::StrCat("Unknown data type"));
}

absl::StatusOr<Const*> PostgresTypeMapping::MakePgConst(
    const zetasql::Value& val) const {
  return absl::UnimplementedError(absl::StrCat(
      "Unimplemented ZetaSQL data type: ", val.type()->DebugString()));
}

bool PostgresTypeMapping::EqualsForSameKind(const Type* that,
                                            bool equivalent) const {
  return true;
}

void PostgresTypeMapping::DebugStringImpl(bool details,
                                          TypeOrStringVector* stack,
                                          std::string* debug_string) const {
  ABSL_LOG(FATAL) << "Not Implemented";
}

bool PostgresTypeMapping::ValueContentEquals(
    const zetasql::ValueContent& x, const zetasql::ValueContent& y,
    const zetasql::ValueEqualityCheckOptions& options) const {
  ABSL_LOG(FATAL) << "Not Implemented";
  return true;
}

bool PostgresTypeMapping::ValueContentLess(const zetasql::ValueContent& x,
                                           const zetasql::ValueContent& y,
                                           const Type* other_type) const {
  ABSL_LOG(FATAL) << "Not Implemented";
  return true;
}

absl::HashState PostgresTypeMapping::HashValueContent(
    const zetasql::ValueContent& value, absl::HashState state) const {
  ABSL_LOG(FATAL) << "Not Implemented";
  return state;
}

std::string PostgresTypeMapping::FormatValueContent(
    const zetasql::ValueContent& value,
    const FormatValueContentOptions& options) const {
  ABSL_LOG(FATAL) << "Not Implemented";
  return "";
}

absl::Status PostgresTypeMapping::SerializeValueContent(
    const zetasql::ValueContent& value,
    zetasql::ValueProto* value_proto) const {
  ABSL_LOG(FATAL) << "Not Implemented";
  return absl::OkStatus();
}

absl::Status PostgresTypeMapping::DeserializeValueContent(
    const zetasql::ValueProto& value_proto,
    zetasql::ValueContent* value) const {
  ABSL_LOG(FATAL) << "Not Implemented";
  return absl::OkStatus();
}

namespace {

static absl::StatusOr<absl::string_view> const_to_string(
    const Const* pg_const) {
  // Byte strings must not be treated as null-terminated. Get explicit
  // length from the Datum. We can't use the Const node's constlen because
  // it's not used (-1) for out-of-line types like these. Those types must
  // know their own lengths. VARSIZE_ANY_EXHDR is alignment insensitive
  // (_ANY) and gets length of they payload only (no header: _EXHDR).
  ZETASQL_ASSIGN_OR_RETURN(char* char_ptr,
                   CheckedPgTextDatumGetCString(pg_const->constvalue));
  absl::string_view string_value =
      absl::string_view(char_ptr, VARSIZE_ANY_EXHDR(pg_const->constvalue));
  return string_value;
}

}  // namespace

// Supported Scalar Types.
class PostgresBoolMapping : public PostgresTypeMapping {
 public:
  PostgresBoolMapping(const zetasql::TypeFactory* factory)
      : PostgresTypeMapping(factory, BOOLOID) {}

  const zetasql::Type* mapped_type() const override {
    return zetasql::types::BoolType();
  }

  absl::StatusOr<zetasql::Value> MakeGsqlValue(
      const Const* pg_const) const override {
    if (pg_const->constisnull) {
      return zetasql::Value::NullBool();
    }
    bool bool_value = DatumGetBool(pg_const->constvalue);
    return zetasql::Value::Bool(bool_value);
  }

  absl::StatusOr<zetasql::Value> MakeGsqlValueFromStringConst(
      const absl::string_view& string_const) const override {
    if (string_const == "null") {
      return zetasql::Value::NullBool();
    }
    ZETASQL_ASSIGN_OR_RETURN(Datum bool_const, CheckedOidFunctionCall1(
        F_BOOLIN, CStringGetDatum(string_const.data())));
    bool bool_value = DatumGetBool(bool_const);
    return zetasql::Value::Bool(bool_value);
  }

  absl::StatusOr<Const*> MakePgConst(
      const zetasql::Value& val) const override {
    bool bool_val = (!val.is_null() ? val.bool_value() : false);
    // Bool is special as its size is hardwired as 1 in postgres.
    ZETASQL_ASSIGN_OR_RETURN(Node* bool_const,
                     CheckedPgMakeBoolConst(bool_val, val.is_null()));
    return internal::PostgresCastNode(Const, bool_const);
  }
};

class PostgresInt8Mapping : public PostgresTypeMapping {
 public:
  PostgresInt8Mapping(const zetasql::TypeFactory* factory)
      : PostgresTypeMapping(factory, INT8OID) {}

  const zetasql::Type* mapped_type() const override {
    return zetasql::types::Int64Type();
  }

  absl::StatusOr<zetasql::Value> MakeGsqlValue(
      const Const* pg_const) const override {
    if (pg_const->constisnull) {
      return zetasql::Value::NullInt64();
    }
    int64_t int64_value = DatumGetInt64(pg_const->constvalue);
    return zetasql::Value::Int64(int64_value);
  }

  absl::StatusOr<zetasql::Value> MakeGsqlValueFromStringConst(
      const absl::string_view& string_const) const override {
    if (string_const == "null") {
      return zetasql::Value::NullInt64();
    }
    ZETASQL_ASSIGN_OR_RETURN(Datum int_const, CheckedOidFunctionCall1(
        F_INT8IN, CStringGetDatum(string_const.data())));
    int64_t int64_value = DatumGetInt64(int_const);
    return zetasql::Value::Int64(int64_value);
  }

  absl::StatusOr<Const*> MakePgConst(
      const zetasql::Value& val) const override {
    int64_t int64_val = (!val.is_null() ? val.int64_value() : 0);
    return internal::makeScalarConst(PostgresTypeOid(),
                                     Int64GetDatum(int64_val), val.is_null());
  }
};

class PostgresFloat8Mapping : public PostgresTypeMapping {
 public:
  PostgresFloat8Mapping(const zetasql::TypeFactory* factory)
      : PostgresTypeMapping(factory, FLOAT8OID) {}

  const zetasql::Type* mapped_type() const override {
    return zetasql::types::DoubleType();
  }

  absl::StatusOr<zetasql::Value> MakeGsqlValue(
      const Const* pg_const) const override {
    if (pg_const->constisnull) {
      return zetasql::Value::NullDouble();
    }
    double double_value = DatumGetFloat8(pg_const->constvalue);
    return zetasql::Value::Double(double_value);
  }

  absl::StatusOr<zetasql::Value> MakeGsqlValueFromStringConst(
      const absl::string_view& string_const) const override {
    if (string_const == "null") {
      return zetasql::Value::NullDouble();
    }
    ZETASQL_ASSIGN_OR_RETURN(Datum double_const, CheckedOidFunctionCall1(
        F_FLOAT8IN, CStringGetDatum(string_const.data())));
    double double_value = DatumGetFloat8(double_const);
    return zetasql::Value::Double(double_value);
  }


  absl::StatusOr<Const*> MakePgConst(
      const zetasql::Value& val) const override {
    double double_val = (!val.is_null() ? val.double_value() : 0.0);
    return internal::makeScalarConst(PostgresTypeOid(),
                                     Float8GetDatum(double_val), val.is_null());
  }
};

class PostgresFloat4Mapping : public PostgresTypeMapping {
 public:
  explicit PostgresFloat4Mapping(const zetasql::TypeFactory* factory)
      : PostgresTypeMapping(factory, FLOAT4OID) {}

  const zetasql::Type* mapped_type() const override {
    return zetasql::types::FloatType();
  }

  absl::StatusOr<zetasql::Value> MakeGsqlValue(
      const Const* pg_const) const override {
    if (pg_const->constisnull) {
      return zetasql::Value::NullFloat();
    }
    float4 float4_value = DatumGetFloat4(pg_const->constvalue);
    return zetasql::Value::Float(float4_value);
  }

  absl::StatusOr<zetasql::Value> MakeGsqlValueFromStringConst(
      const absl::string_view& string_const) const override {
    if (string_const == "null") {
      return zetasql::Value::NullFloat();
    }
    ZETASQL_ASSIGN_OR_RETURN(Datum float4_const, CheckedOidFunctionCall1(
        F_FLOAT4IN, CStringGetDatum(string_const.data())));
    float4 float4_value = DatumGetFloat4(float4_const);
    return zetasql::Value::Float(float4_value);
  }

  absl::StatusOr<Const*> MakePgConst(
      const zetasql::Value& val) const override {
    float4 float_val = (!val.is_null() ? val.float_value() : 0.0);
    return internal::makeScalarConst(PostgresTypeOid(),
                                     Float4GetDatum(float_val), val.is_null());
  }
};

class PostgresVarcharMapping : public PostgresTypeMapping {
 public:
  PostgresVarcharMapping(const zetasql::TypeFactory* factory)
      : PostgresTypeMapping(factory, VARCHAROID) {}

  const zetasql::Type* mapped_type() const override {
    return zetasql::types::StringType();
  }

  absl::StatusOr<zetasql::Value> MakeGsqlValue(
      const Const* pg_const) const override {
    if (pg_const->constisnull) {
      return zetasql::Value::NullString();
    }
    ZETASQL_ASSIGN_OR_RETURN(absl::string_view string_value, const_to_string(pg_const));
    return zetasql::Value::String(string_value);
  }

  absl::StatusOr<zetasql::Value> MakeGsqlValueFromStringConst(
      const absl::string_view& string_const) const override {
    if (string_const == "null") {
      return zetasql::Value::NullString();
    }
    if (RE2::FullMatch(string_const, R"(^'.*'$)")) {
      // Strip the leading and trailing single quotes.
      std::string stripped_string = (std::string) string_const.substr(
          1, string_const.size() - 2);
      ZETASQL_ASSIGN_OR_RETURN(Datum varchar_const,
                       CheckedOidFunctionCall3(
                           F_VARCHARIN,
                           CStringGetDatum(stripped_string.data()),
                           /*typelem=*/InvalidOid,  // Unused.
                           /*atttypmod=*/-1));
      ZETASQL_ASSIGN_OR_RETURN(absl::string_view varchar_value,
                       CheckedPgTextDatumGetCString(varchar_const));
      return zetasql::Value::String(varchar_value);
    }
    return absl::InvalidArgumentError(
        absl::StrCat("Invalid constant string: ", string_const));
  }

  absl::StatusOr<Const*> MakePgConst(
      const zetasql::Value& val) const override {
    std::string string_val = (!val.is_null() ? val.string_value() : "");
    return internal::makeStringConst(PostgresTypeOid(), string_val.c_str(),
                                     val.is_null());
  }
};

class PostgresTextMapping : public PostgresTypeMapping {
 public:
  PostgresTextMapping(const zetasql::TypeFactory* factory)
      : PostgresTypeMapping(factory, TEXTOID) {}

  const zetasql::Type* mapped_type() const override {
    return zetasql::types::StringType();
  }

  absl::StatusOr<zetasql::Value> MakeGsqlValue(
      const Const* pg_const) const override {
    if (pg_const->constisnull) {
      return zetasql::Value::NullString();
    }
    ZETASQL_ASSIGN_OR_RETURN(absl::string_view string_value, const_to_string(pg_const));
    return zetasql::Value::String(string_value);
  }

  absl::StatusOr<zetasql::Value> MakeGsqlValueFromStringConst(
      const absl::string_view& string_const) const override {
    if (string_const == "null") {
      return zetasql::Value::NullString();
    }
    if (RE2::FullMatch(string_const, R"(^'.*'$)")) {
      // Strip the leading and trailing single quotes.
      std::string stripped_string = (std::string) string_const.substr(
          1, string_const.size() - 2);
      ZETASQL_ASSIGN_OR_RETURN(Datum text_const, CheckedOidFunctionCall1(
          F_TEXTIN, CStringGetDatum(stripped_string.data())));
      ZETASQL_ASSIGN_OR_RETURN(absl::string_view text_value,
                       CheckedPgTextDatumGetCString(text_const));
      return zetasql::Value::String(text_value);
    }
    return absl::InvalidArgumentError(
        absl::StrCat("Invalid constant string: ", string_const));
  }

  absl::StatusOr<Const*> MakePgConst(
      const zetasql::Value& val) const override {
    std::string string_val = (!val.is_null() ? val.string_value() : "");
    return internal::makeStringConst(PostgresTypeOid(), string_val.c_str(),
                                     val.is_null());
  }
};

class PostgresBytesaMapping : public PostgresTypeMapping {
 public:
  PostgresBytesaMapping(const zetasql::TypeFactory* factory)
      : PostgresTypeMapping(factory, BYTEAOID) {}

  const zetasql::Type* mapped_type() const override {
    return zetasql::types::BytesType();
  }

  absl::StatusOr<zetasql::Value> MakeGsqlValue(
      const Const* pg_const) const override {
    if (pg_const->constisnull) {
      return zetasql::Value::NullBytes();
    }
    ZETASQL_ASSIGN_OR_RETURN(absl::string_view bytes_value, const_to_string(pg_const));
    return zetasql::Value::Bytes(bytes_value);
  }

  absl::StatusOr<zetasql::Value> MakeGsqlValueFromStringConst(
      const absl::string_view& string_const) const override {
    if (string_const == "null") {
      return zetasql::Value::NullBytes();
    }
    if (RE2::FullMatch(string_const, R"(^'.*'$)")) {
      // Strip the leading and trailing single quotes.
      std::string stripped_string = (std::string) string_const.substr(
          1, string_const.size() - 2);
      ZETASQL_ASSIGN_OR_RETURN(Datum bytea_const, CheckedOidFunctionCall1(
          F_BYTEAIN, CStringGetDatum(stripped_string.data())));
      ZETASQL_ASSIGN_OR_RETURN(absl::string_view bytea_value,
                       CheckedPgTextDatumGetCString(bytea_const));
      return zetasql::Value::Bytes(bytea_value);
    }
    return absl::InvalidArgumentError(
        absl::StrCat("Invalid constant string: ", string_const));
  }

  absl::StatusOr<Const*> MakePgConst(
      const zetasql::Value& val) const override {
    return internal::makeByteConst(val.is_null() ? "" : val.bytes_value(),
                                   val.is_null());
  }
};

// Represents PostgreSQL Timestamptz type, aka TIMESTAMP WITH TIME ZONE.
class PostgresTimestamptzMapping : public PostgresTypeMapping {
 public:
  PostgresTimestamptzMapping(const zetasql::TypeFactory* factory)
      : PostgresTypeMapping(factory, TIMESTAMPTZOID) {}

  const zetasql::Type* mapped_type() const override {
    return zetasql::types::TimestampType();
  }

  absl::StatusOr<zetasql::Value> MakeGsqlValue(
      const Const* pg_const) const override {
    if (pg_const->constisnull) {
      return zetasql::Value::NullTimestamp();
    }
    const TimestampTz timestamptz_value =
        DatumGetTimestampTz(pg_const->constvalue);
    absl::Time time_val = PgTimestamptzToAbslTime(timestamptz_value);
    if (!zetasql::functions::IsValidTime(time_val)) {
      return absl::InvalidArgumentError("Timestamp is out of supported range");
    }
    return zetasql::Value::Timestamp(time_val);
  }

  absl::StatusOr<zetasql::Value> MakeGsqlValueFromStringConst(
      const absl::string_view& string_const) const override {
    return absl::UnimplementedError(
        "Timestamptz default values are not supported");
  }

  absl::StatusOr<Const*> MakePgConst(
      const zetasql::Value& val) const override {
    TimestampTz timestamptz_val =
        (!val.is_null() ? AbslTimeToPgTimestamptz(val.ToTime()) : 0);
    return internal::makeScalarConst(
        PostgresTypeOid(), TimestampTzGetDatum(timestamptz_val), val.is_null());
  }
};

// Represents PostgreSQL Date type.
class PostgresDateMapping : public PostgresTypeMapping {
 public:
  PostgresDateMapping(const zetasql::TypeFactory* factory)
      : PostgresTypeMapping(factory, DATEOID) {}

  const zetasql::Type* mapped_type() const override {
    return zetasql::types::DateType();
  }

  absl::StatusOr<zetasql::Value> MakeGsqlValue(
      const Const* pg_const) const override {
    if (pg_const->constisnull) {
      return zetasql::Value::NullDate();
    }
    const DateADT dateadt_value =
        DatumGetDateADT(pg_const->constvalue);
    int32_t date_val =
        PgDateOffsetToGsqlDateOffset(static_cast<int32_t>(dateadt_value));
    if (!zetasql::functions::IsValidDate(date_val)) {
      return absl::InvalidArgumentError("Date is out of supported range");
    }
    return zetasql::Value::Date(date_val);
  }

  absl::StatusOr<zetasql::Value> MakeGsqlValueFromStringConst(
      const absl::string_view& string_const) const override {
    return absl::UnimplementedError(
        "Date default values are not supported");
  }

  absl::StatusOr<Const*> MakePgConst(
      const zetasql::Value& val) const override {
    int32_t date_val =
        (!val.is_null() ? GsqlDateOffsetToPgDateOffset(val.date_value()) : 0);
    return internal::makeScalarConst(
        PostgresTypeOid(), DateADTGetDatum(date_val), val.is_null());
  }
};

class PostgresIntervalMapping : public PostgresTypeMapping {
 public:
  explicit PostgresIntervalMapping(const zetasql::TypeFactory* factory)
      : PostgresTypeMapping(factory, INTERVALOID) {}

  const zetasql::Type* mapped_type() const override {
    return zetasql::types::IntervalType();
  }

  absl::StatusOr<zetasql::Value> MakeGsqlValue(
      const Const* pg_const) const override {
    if (pg_const->constisnull) {
      return zetasql::Value::NullInterval();
    }

    Interval* interval = DatumGetIntervalP(pg_const->constvalue);
    ZETASQL_ASSIGN_OR_RETURN(zetasql::IntervalValue interval_value,
                     zetasql::IntervalValue::FromMonthsDaysMicros(
                         interval->month, interval->day, interval->time));
    return zetasql::Value::Interval(interval_value);
  }

  absl::StatusOr<zetasql::Value> MakeGsqlValueFromStringConst(
      const absl::string_view& string_const) const override {
    return absl::UnimplementedError(
        "Interval default values are not supported");
  }

  absl::StatusOr<Const*> MakePgConst(
      const zetasql::Value& value) const override {
    zetasql::IntervalValue interval_value =
        (!value.is_null() ? value.interval_value()
                          : zetasql::IntervalValue());

    ZETASQL_ASSIGN_OR_RETURN(void* p, CheckedPgPalloc(sizeof(Interval)));
    Interval* interval = reinterpret_cast<Interval*>(p);
    interval->month = static_cast<int>(interval_value.get_months());
    interval->day = static_cast<int>(interval_value.get_days());
    interval->time = interval_value.get_micros();

    return CheckedPgMakeConst(
        /*consttype=*/INTERVALOID,
        /*consttypmod=*/-1,
        /*constcollid=*/InvalidOid,
        /*constlen=*/sizeof(Interval),
        /*constvalue=*/IntervalPGetDatum(interval),
        /*constisnull=*/value.is_null(),
        /*constbyval=*/false);
  }
};

absl::StatusOr<zetasql::Value> PostgresExtendedArrayMapping::MakeGsqlValue(
    const Const* pg_const) const {
  // Technically this means we support multi-dimensional NULL arrays, but PG
  // doesn't actually care about dimensionality declarations, so in practice
  // NULL::bigint[] means the same as NULL::bigint[][][][][].
  if (pg_const->constisnull) {
    return zetasql::Value::Null(mapped_type());
  }

  std::vector<zetasql::Value> array_element_values;
  ZETASQL_ASSIGN_OR_RETURN(ArrayType * array,
                   CheckedPgDatumGetArrayTypeP(pg_const->constvalue));
  // Verify array is not multi-dimensional. Empty arrays are 0-D and ok.
  if (array->ndim > 1) {
    return absl::InvalidArgumentError(
        "Multi-dimensional arrays are not supported");
  } else if (array->ndim == 1 && ARR_LBOUND(array)[0] != 1) {
    return absl::InvalidArgumentError(
        "Arrays with lower bounds other than 1 are not supported");
  }

  Oid element_oid = ARR_ELEMTYPE(array);
  ZETASQL_ASSIGN_OR_RETURN(ArrayIterator array_iterator,
                   CheckedPgArrayCreateIterator(array, /*slice_ndim=*/0,
                                                /*mstate=*/nullptr));
  Datum value;
  bool isnull;
  ZETASQL_ASSIGN_OR_RETURN(bool array_has_next,
                   CheckedPgArrayIterate(array_iterator, &value, &isnull));
  while (array_has_next) {
    // TODO: Refactor to avoid creating a Const here.
    // TODO: This assumes none of the MakeGsqlValue methods
    // want more than consttype, constvalue and constisnull populated. Enforce
    // (or remove the need for) this assumption.
    ZETASQL_ASSIGN_OR_RETURN(const Const* element_const,
                     internal::makeScalarConst(element_oid, value, isnull));
    ZETASQL_ASSIGN_OR_RETURN(zetasql::Value gsql_value,
                     element_type_->MakeGsqlValue(element_const));
    array_element_values.push_back(std::move(gsql_value));

    ZETASQL_ASSIGN_OR_RETURN(array_has_next,
                     CheckedPgArrayIterate(array_iterator, &value, &isnull));
  }

  return zetasql::Value::MakeArray(mapped_type()->AsArray(),
                                     array_element_values);
}

absl::StatusOr<zetasql::Value>
PostgresExtendedArrayMapping::MakeGsqlValueFromStringConst(
    const absl::string_view& string_const) const {
  if (string_const == "null") {
    return zetasql::Value::Null(mapped_type());
  }
  // Array constants are formatted as '{element, element}'
  if (RE2::FullMatch(string_const, R"(^'\{.*\}'$)")) {
    std::vector<zetasql::Value> array_element_values;
    // Strip the leading and trailing single quotes and braces.
    absl::string_view array_string = string_const.substr(
        2, string_const.size() - 4);
    if (!array_string.empty()) {
      for (absl::string_view element_string :
          absl::StrSplit(array_string, ',')) {
        std::string element_string_value = std::string(element_string);
        if (element_type_->mapped_type()->IsString() ||
            element_type_->mapped_type()->IsBytes()) {
          // Add quotes to the string elements.
          element_string_value = absl::StrCat("'", element_string, "'");
        }
        ZETASQL_ASSIGN_OR_RETURN(zetasql::Value gsql_value,
                        element_type_->MakeGsqlValueFromStringConst(
                            element_string_value));
        array_element_values.push_back(std::move(gsql_value));
      }
    }
    return zetasql::Value::MakeArray(mapped_type()->AsArray(),
                                       array_element_values);
  }
  return absl::InvalidArgumentError(
      absl::StrCat("Invalid array constant: ", string_const));
}

absl::StatusOr<Const*> PostgresExtendedArrayMapping::MakePgConst(
    const zetasql::Value& val) const {
  // Lookup the collation for the element type.
  ZETASQL_ASSIGN_OR_RETURN(const FormData_pg_type* pg_type,
                   PgBootstrapCatalog::Default()->GetType(
                       element_type_->PostgresTypeOid()));
  const Oid collation = pg_type->typcollation;

  // Two special cases to handle: NULL array and empty array. Note that if the
  // array is NULL, we cannot call any of the array-specific zetasql::Value
  // functions.
  if (val.is_null() || val.num_elements() == 0) {
    ArrayType* array_type = nullptr;
    if (!val.is_null()) {
      ZETASQL_ASSIGN_OR_RETURN(array_type, CheckedPgConstructEmptyArray(
                                       element_type_->PostgresTypeOid()));
    }
    return CheckedPgMakeConst(PostgresTypeOid(),
                              /*consttypmod=*/-1,
                              /*constcollid=*/collation,
                              /*constlen=*/-1,  // Pass-by-reference
                              /*constvalue=*/PointerGetDatum(array_type),
                              /*constisnull=*/val.is_null(),
                              /*constbyval=*/false);
  }

  // Create arrays of Datums and null flags from the googlesql value elements.
  // Here we leave nulls for null-valued elements. The array constructor will
  // pack them.
  std::vector<Datum> pg_datums;
  pg_datums.reserve(val.num_elements());
  // NB: vector<bool> is unhelpful here because we need bool* for the PG API.
  ZETASQL_ASSIGN_OR_RETURN(void* p, CheckedPgPalloc(val.num_elements() * sizeof(bool)));
  bool* nulls = reinterpret_cast<bool*>(p);
  for (int i = 0; i < val.num_elements(); ++i) {
    // TODO: Refactor to avoid creating a Const here.
    ZETASQL_ASSIGN_OR_RETURN(Const * pg_const,
                     element_type_->MakePgConst(val.element(i)));
    pg_datums.push_back(pg_const->constvalue);
    nulls[i] = pg_const->constisnull;
  }

  // Create PG Array from the Datums
  Oid element_typid = element_type_->PostgresTypeOid();
  int16_t elmlen;
  bool elmbyval;
  char elmalign;
  ZETASQL_RETURN_IF_ERROR(CheckedPgGetTyplenbyvalalign(element_typid, &elmlen,
                                               &elmbyval, &elmalign));
  // We need 1-element "arrays" for dimensions and lower bound indices.
  // NB: We're using the multi-dimensional constructor here because the 1-D
  // version doesn't handle NULLs.
  int dims = val.num_elements();
  int lower_bounds = 1;
  ZETASQL_ASSIGN_OR_RETURN(
      ArrayType * pg_array,
      CheckedPgConstructMdArray(pg_datums.data(), nulls, /*ndims=*/1, &dims,
                                &lower_bounds, element_typid, elmlen, elmbyval,
                                elmalign));

  return CheckedPgMakeConst(
      /*consttype=*/PostgresTypeOid(),
      /*consttypmod=*/-1,
      /*constcollid=*/collation,
      /*constlen=*/-1,  // Pass-by-reference
      /*constvalue=*/PointerGetDatum(pg_array),
      /*constisnull=*/false,
      /*constbyval=*/false);
}

// Unsupported Types.
class PostgresInt4Mapping : public PostgresTypeMapping {
 public:
  PostgresInt4Mapping(const zetasql::TypeFactory* factory)
      : PostgresTypeMapping(factory, INT4OID) {}
  ~PostgresInt4Mapping() {}

  const zetasql::Type* mapped_type() const override {
    // Not supported.
    return nullptr;
  }

  absl::StatusOr<zetasql::Value> MakeGsqlValue(
      const Const* pg_const) const override {
    if (pg_const->constisnull) {
      return zetasql::Value::NullInt32();
    }
    int32_t int32_value = DatumGetInt32(pg_const->constvalue);
    return zetasql::Value::Int32(int32_value);
  }

  absl::StatusOr<Const*> MakePgConst(
      const zetasql::Value& val) const override {
    int32_t int32_val = (!val.is_null() ? val.int32_value() : 0);
    return internal::makeScalarConst(PostgresTypeOid(),
                                     Int32GetDatum(int32_val), val.is_null());
  }
};

zetasql::TypeFactory* GetTypeFactory() {
  static zetasql::TypeFactory* s_type_factory =
      new zetasql::TypeFactory(zetasql::TypeFactoryOptions{
          .keep_alive_while_referenced_from_value = false});
  return s_type_factory;
}

namespace types {

// Supported Scalar Types.
const PostgresTypeMapping* PgBoolMapping() {
  static const zetasql_base::NoDestructor<PostgresBoolMapping> s_pg_bool_mapping(
      GetTypeFactory());
  return s_pg_bool_mapping.get();
}

const PostgresTypeMapping* PgInt8Mapping() {
  static const zetasql_base::NoDestructor<PostgresInt8Mapping> s_pg_int8_mapping(
      GetTypeFactory());
  return s_pg_int8_mapping.get();
}

const PostgresTypeMapping* PgFloat8Mapping() {
  static const zetasql_base::NoDestructor<PostgresFloat8Mapping> s_pg_float8_mapping(
      GetTypeFactory());
  return s_pg_float8_mapping.get();
}

const PostgresTypeMapping* PgFloat4Mapping() {
  static const zetasql_base::NoDestructor<PostgresFloat4Mapping> s_pg_float4_mapping(
      GetTypeFactory());
  return s_pg_float4_mapping.get();
}

const PostgresTypeMapping* PgVarcharMapping() {
  static const zetasql_base::NoDestructor<PostgresVarcharMapping> s_pg_varchar_mapping(
      GetTypeFactory());
  return s_pg_varchar_mapping.get();
}

const PostgresTypeMapping* PgTextMapping() {
  static const zetasql_base::NoDestructor<PostgresTextMapping> s_pg_text_mapping(
      GetTypeFactory());
  return s_pg_text_mapping.get();
}

const PostgresTypeMapping* PgByteaMapping() {
  static const zetasql_base::NoDestructor<PostgresBytesaMapping> s_pg_bytea_mapping(
      GetTypeFactory());
  return s_pg_bytea_mapping.get();
}

const PostgresTypeMapping* PgTimestamptzMapping() {
  static const zetasql_base::NoDestructor<PostgresTimestamptzMapping>
      s_pg_timestamptz_mapping(GetTypeFactory());
  return s_pg_timestamptz_mapping.get();
}

const PostgresTypeMapping* PgDateMapping() {
  static const zetasql_base::NoDestructor<PostgresDateMapping> s_pg_date_mapping(
      GetTypeFactory());
  return s_pg_date_mapping.get();
}

const PostgresTypeMapping* PgIntervalMapping() {
  static const zetasql_base::NoDestructor<PostgresIntervalMapping>
      s_pg_interval_mapping(GetTypeFactory());
  return s_pg_interval_mapping.get();
}

// Supported Array Types.
const PostgresTypeMapping* PgBoolArrayMapping() {
  static const zetasql_base::NoDestructor<PostgresExtendedArrayMapping>
      s_pg_bool_array_mapping(
          /*type_factory=*/GetTypeFactory(), /*array_type_oid=*/BOOLARRAYOID,
          /*element_type=*/types::PgBoolMapping(),
          /*mapped_type=*/zetasql::types::BoolArrayType(),
          /*requires_nan_handling=*/false);
  return s_pg_bool_array_mapping.get();
}

const PostgresTypeMapping* PgInt8ArrayMapping() {
  static const zetasql_base::NoDestructor<PostgresExtendedArrayMapping>
      s_pg_int8_array_mapping(
          /*type_factory=*/GetTypeFactory(), /*array_type_oid=*/INT8ARRAYOID,
          /*element_type=*/types::PgInt8Mapping(),
          /*mapped_type=*/zetasql::types::Int64ArrayType(),
          /*requires_nan_handling=*/false);
  return s_pg_int8_array_mapping.get();
}

const PostgresTypeMapping* PgFloat8ArrayMapping() {
  static const zetasql_base::NoDestructor<PostgresExtendedArrayMapping>
      s_pg_float8_array_mapping(
          /*type_factory=*/GetTypeFactory(), /*array_type_oid=*/FLOAT8ARRAYOID,
          /*element_type=*/types::PgFloat8Mapping(),
          /*mapped_type=*/zetasql::types::DoubleArrayType(),
          /*requires_nan_handling=*/true);
  return s_pg_float8_array_mapping.get();
}

const PostgresTypeMapping* PgFloat4ArrayMapping() {
  static const zetasql_base::NoDestructor<PostgresExtendedArrayMapping>
      s_pg_float4_array_mapping(
          /*type_factory=*/GetTypeFactory(), /*array_type_oid=*/FLOAT4ARRAYOID,
          /*element_type=*/types::PgFloat4Mapping(),
          /*mapped_type=*/zetasql::types::FloatArrayType(),
          /*requires_nan_handling=*/true);
  return s_pg_float4_array_mapping.get();
}

const PostgresTypeMapping* PgVarcharArrayMapping() {
  static const zetasql_base::NoDestructor<PostgresExtendedArrayMapping>
      s_pg_varchar_array_mapping(
          /*type_factory=*/GetTypeFactory(), /*array_type_oid=*/VARCHARARRAYOID,
          /*element_typmapping*/ types::PgTextMapping(),
          /*mapped_type=*/zetasql::types::StringArrayType(),
          /*requires_nan_handling=*/false);
  return s_pg_varchar_array_mapping.get();
}

const PostgresTypeMapping* PgTextArrayMapping() {
  static const zetasql_base::NoDestructor<PostgresExtendedArrayMapping>
      s_pg_text_array_mapping(
          /*type_factory=*/GetTypeFactory(), /*array_type_oid=*/TEXTARRAYOID,
          /*element_type=*/types::PgTextMapping(),
          /*mapped_type=*/zetasql::types::StringArrayType(),
          /*requires_nan_handling=*/false);
  return s_pg_text_array_mapping.get();
}

const PostgresTypeMapping* PgByteaArrayMapping() {
  static const zetasql_base::NoDestructor<PostgresExtendedArrayMapping>
      s_pg_bytea_array_mapping(
          /*type_factory=*/GetTypeFactory(), /*array_type_oid=*/BYTEAARRAYOID,
          /*element_type=*/types::PgByteaMapping(),
          /*mapped_type=*/zetasql::types::BytesArrayType(),
          /*requires_nan_handling=*/false);
  return s_pg_bytea_array_mapping.get();
}

// Represents PostgreSQL Timestamptz type, aka TIMESTAMP WITH TIME ZONE.
const PostgresTypeMapping* PgTimestamptzArrayMapping() {
  static const zetasql_base::NoDestructor<PostgresExtendedArrayMapping>
      s_pg_timestamptz_array_mapping(
          /*type_factory=*/GetTypeFactory(),
          /*array_type_oid=*/TIMESTAMPTZARRAYOID,
          /*element_type=*/types::PgTimestamptzMapping(),
          /*mapped_type=*/zetasql::types::TimestampArrayType(),
          /*requires_nan_handling=*/false);
  return s_pg_timestamptz_array_mapping.get();
}

const PostgresTypeMapping* PgDateArrayMapping() {
  static const zetasql_base::NoDestructor<PostgresExtendedArrayMapping>
      s_pg_date_array_mapping(
          /*type_factory=*/GetTypeFactory(), /*array_type_oid=*/DATEARRAYOID,
          /*element_type=*/types::PgDateMapping(),
          /*mapped_type=*/zetasql::types::DateArrayType(),
          /*requires_nan_handling=*/false);
  return s_pg_date_array_mapping.get();
}

const PostgresTypeMapping* PgIntervalArrayMapping() {
  static const zetasql_base::NoDestructor<PostgresExtendedArrayMapping>
      s_pg_interval_array_mapping(
          /*type_factory=*/GetTypeFactory(),
          /*array_type_oid=*/INTERVALARRAYOID,
          /*element_type=*/types::PgIntervalMapping(),
          /*mapped_type=*/zetasql::types::IntervalArrayType(),
          /*requires_nan_handling=*/false);
  return s_pg_interval_array_mapping.get();
}

}  // namespace types

}  // namespace postgres_translator
