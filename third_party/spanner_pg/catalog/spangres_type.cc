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

#include "third_party/spanner_pg/catalog/spangres_type.h"

#include <stdbool.h>

#include <cstdint>
#include <limits>
#include <string>

#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "zetasql/base/no_destructor.h"
#include "absl/flags/flag.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/bootstrap_catalog/bootstrap_catalog.h"
#include "third_party/spanner_pg/catalog/type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_numeric_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_oid_type.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "third_party/spanner_pg/util/postgres.h"
#include "re2/re2.h"

namespace postgres_translator {
namespace spangres {
namespace types {

class PostgresNumericMapping : public PostgresTypeMapping {
 public:
  PostgresNumericMapping(const zetasql::TypeFactory* factory)
      : PostgresTypeMapping(factory, NUMERICOID) {}

  const zetasql::Type* mapped_type() const override {
    return postgres_translator::spangres::datatypes::GetPgNumericType();
  }

  absl::StatusOr<zetasql::Value> MakeGsqlValue(
      const Const* pg_const) const override {
    ZETASQL_RET_CHECK_EQ(pg_const->consttypmod, -1)
        << "Typmod seen for numeric constant. Typmod: "
        << pg_const->consttypmod;

    if (pg_const->constisnull) {
      return zetasql::Value::Null(
          postgres_translator::spangres::datatypes::GetPgNumericType());
    }
    ZETASQL_ASSIGN_OR_RETURN(Datum numeric, CheckedOidFunctionCall1(
        F_NUMERIC_OUT, pg_const->constvalue));
    absl::string_view string_value = DatumGetCString(numeric);
    return postgres_translator::spangres::datatypes::CreatePgNumericValue(
        string_value);
  }

  absl::StatusOr<zetasql::Value> MakeGsqlValueFromStringConst(
      const absl::string_view& string_const) const override {
    return absl::UnimplementedError(
        "Numeric default values are not supported");
  }

  absl::StatusOr<Const*> MakePgConst(
      const zetasql::Value& val) const override {
    // Pg.Numeric is stored in memory as a readable string and is converted via
    // Postgres function into a PgConst.
    std::string readable_numeric;
    if (val.is_null()) {
      readable_numeric = "0";
    } else {
        ZETASQL_ASSIGN_OR_RETURN(absl::Cord normalized_numeric,
                         postgres_translator::spangres::datatypes::
                             GetPgNumericNormalizedValue(val));
        readable_numeric.reserve(normalized_numeric.size());
        absl::CopyCordToString(normalized_numeric, &readable_numeric);
    }

    ZETASQL_ASSIGN_OR_RETURN(
        Datum const_value,
        CheckedOidFunctionCall3(
            F_NUMERIC_IN, CStringGetDatum(readable_numeric.data()),
            ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1)));
    return internal::makeScalarConst(NUMERICOID, const_value, val.is_null());
  }
};

class PostgresJsonbMapping : public PostgresTypeMapping {
 public:
  PostgresJsonbMapping(const zetasql::TypeFactory* factory)
      : PostgresTypeMapping(factory, JSONBOID) {}

  const zetasql::Type* mapped_type() const override {
    return postgres_translator::spangres::datatypes::GetPgJsonbType();
  }

  absl::StatusOr<zetasql::Value> MakeGsqlValue(
      const Const* pg_const) const override {
    if (pg_const->constisnull) {
      return zetasql::Value::Null(
          postgres_translator::spangres::datatypes::GetPgJsonbType());
    }
    return MakeGsqlValueFromDatum(pg_const->constvalue);
  }

  absl::StatusOr<zetasql::Value> MakeGsqlValueFromStringConst(
      const absl::string_view& string_const) const override {
    if (string_const == "null") {
      return zetasql::Value::Null(
          postgres_translator::spangres::datatypes::GetPgJsonbType());
    }
    if (RE2::FullMatch(string_const, R"(^'.*'$)")) {
      // Strip the leading and trailing single quotes.
      std::string stripped_string = (std::string) string_const.substr(
          1, string_const.size() - 2);
      ZETASQL_ASSIGN_OR_RETURN(
          Datum const_value, CheckedOidFunctionCall1(
              F_JSONB_IN, CStringGetDatum(stripped_string.c_str())));
      return MakeGsqlValueFromDatum(const_value);
    }
    return absl::InvalidArgumentError(
        absl::StrCat("Invalid jsonb string: ", string_const));
  }

  absl::StatusOr<Const*> MakePgConst(
      const zetasql::Value& val) const override {
    Datum const_value = 0;
    if (!val.is_null()) {
      ZETASQL_ASSIGN_OR_RETURN(absl::Cord normalized_jsonb,
                           postgres_translator::spangres::datatypes::
                               GetPgJsonbNormalizedValue(val));
      std::string normalized_jsonb_str;
      normalized_jsonb_str.reserve(normalized_jsonb.size());
      absl::CopyCordToString(normalized_jsonb, &normalized_jsonb_str);
      Datum conval = CStringGetDatum(normalized_jsonb_str.c_str());
      ZETASQL_ASSIGN_OR_RETURN(const_value,
                       CheckedOidFunctionCall1(F_JSONB_IN, conval));
    }

    return CheckedPgMakeConst(
        /*consttype=*/PostgresTypeOid(),
        /*consttypmod=*/-1,
        /*constcollid=*/InvalidOid,
        /*constlen=*/-1,
        /*constvalue=*/const_value,
        /*constisnull=*/val.is_null(),
        /*constbyval=*/false);
  }

 private:
  absl::StatusOr<zetasql::Value> MakeGsqlValueFromDatum(Datum datum) const {
    ZETASQL_ASSIGN_OR_RETURN(Datum jsonb, CheckedOidFunctionCall1(F_JSONB_OUT, datum));
    return postgres_translator::spangres::datatypes::CreatePgJsonbValue(
        DatumGetCString(jsonb));
  }
};

class PostgresOidMapping : public PostgresTypeMapping {
 public:
  PostgresOidMapping(const zetasql::TypeFactory* factory)
      : PostgresTypeMapping(factory, OIDOID) {}

  const zetasql::Type* mapped_type() const override {
    return postgres_translator::spangres::datatypes::GetPgOidType();
  }

  absl::StatusOr<zetasql::Value> MakeGsqlValue(
      const Const* pg_const) const override {
    if (pg_const->constisnull) {
      return zetasql::Value::Null(
          postgres_translator::spangres::datatypes::GetPgOidType());
    }
    // oidout simply calls sprintf. Skipping in favor of accessing the const
    // value directly.
    return postgres_translator::spangres::datatypes::CreatePgOidValue(
        DatumGetObjectId(pg_const->constvalue));
  }

  absl::StatusOr<zetasql::Value> MakeGsqlValueFromStringConst(
      const absl::string_view& string_const) const override {
    return absl::UnimplementedError(
        "OID default values are not supported");
  }

  absl::StatusOr<Const*> MakePgConst(
      const zetasql::Value& val) const override {
    Datum const_value = 0;
    if (!val.is_null()) {
      ZETASQL_ASSIGN_OR_RETURN(
          uint64_t oid,
              postgres_translator::spangres::datatypes::GetPgOidValue(val));
      // PostgreSQL oid values are uint32_t.
      if (oid < std::numeric_limits<uint32_t>::min() ||
          oid > std::numeric_limits<uint32_t>::max()) {
        return absl::OutOfRangeError("oid value out of range");
      }
      std::string oid_str = absl::StrCat(oid);
      Datum conval = CStringGetDatum(oid_str.c_str());
      ZETASQL_ASSIGN_OR_RETURN(const_value, CheckedOidFunctionCall1(F_OIDIN, conval));
    }

    return CheckedPgMakeConst(
        /*consttype=*/PostgresTypeOid(),
        /*consttypmod=*/-1,
        /*constcollid=*/InvalidOid,
        /*constlen=*/4,
        /*constvalue=*/const_value,
        /*constisnull=*/val.is_null(),
        /*constbyval=*/true);
  }
};

const PostgresTypeMapping* PgNumericMapping() {
  static const zetasql_base::NoDestructor<PostgresNumericMapping> s_pg_numeric_mapping(
      GetTypeFactory());
  return s_pg_numeric_mapping.get();
}

const PostgresTypeMapping* PgNumericArrayMapping() {
  static const zetasql_base::NoDestructor<PostgresExtendedArrayMapping>
      s_pg_numeric_array_mapping(
          /*type_factory=*/GetTypeFactory(), /*array_type_oid=*/NUMERICARRAYOID,
          /*element_type=*/types::PgNumericMapping(), /*mapped_type=*/
          postgres_translator::spangres::datatypes::GetPgNumericArrayType(),
          /*requires_nan_handling=*/true);
  return s_pg_numeric_array_mapping.get();
}

const PostgresTypeMapping* PgJsonbMapping() {
  static const zetasql_base::NoDestructor<PostgresJsonbMapping> s_pg_jsonb_mapping(
      GetTypeFactory());
  return s_pg_jsonb_mapping.get();
}

const PostgresTypeMapping* PgJsonbArrayMapping() {
  static const zetasql_base::NoDestructor<PostgresExtendedArrayMapping>
      s_pg_jsonb_array_mapping(
          /*type_factory=*/GetTypeFactory(), /*array_type_oid=*/JSONBARRAYOID,
          /*element_type=*/types::PgJsonbMapping(), /*mapped_type=*/
          postgres_translator::spangres::datatypes::GetPgJsonbArrayType(),
          /*requires_nan_handling=*/false);
  return s_pg_jsonb_array_mapping.get();
}

const PostgresTypeMapping* PgOidMapping() {
  static const zetasql_base::NoDestructor<PostgresOidMapping> s_pg_oid_mapping(
      GetTypeFactory());
  return s_pg_oid_mapping.get();
}

const PostgresTypeMapping* PgOidArrayMapping() {
  static const zetasql_base::NoDestructor<PostgresExtendedArrayMapping>
      s_pg_oid_array_mapping(
          /*type_factory=*/GetTypeFactory(), /*array_type_oid=*/OIDARRAYOID,
          /*element_type=*/types::PgOidMapping(), /*mapped_type=*/
          postgres_translator::spangres::datatypes::GetPgOidArrayType(),
          /*requires_nan_handling=*/false);
  return s_pg_oid_array_mapping.get();
}

}  // namespace types
}  // namespace spangres
}  // namespace postgres_translator
