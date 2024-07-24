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

#include "third_party/spanner_pg/datatypes/extended/conversion_finder.h"

#include <string>
#include <vector>

#include "zetasql/public/cast.h"
#include "zetasql/public/function.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/base/no_destructor.h"
#include "absl/container/flat_hash_set.h"
#include "third_party/spanner_pg/datatypes/extended/conversion_functions.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_conversion_functions.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_numeric_conversion_functions.h"
#include "third_party/spanner_pg/datatypes/extended/pg_numeric_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_oid_conversion_functions.h"
#include "third_party/spanner_pg/datatypes/extended/pg_oid_type.h"

namespace postgres_translator::spangres {
namespace datatypes {

using ::zetasql::CastFunctionProperty;
using ::zetasql::CastFunctionType;
using ::zetasql::Catalog;

const zetasql::Type* gsql_bool = zetasql::types::BoolType();
const zetasql::Type* gsql_float = zetasql::types::FloatType();
const zetasql::Type* gsql_double = zetasql::types::DoubleType();
const zetasql::Type* gsql_int64 = zetasql::types::Int64Type();
const zetasql::Type* gsql_string = zetasql::types::StringType();

class ConversionInfo {
 public:
  ConversionInfo(CastFunctionProperty cast_property,
                 const zetasql::Function* cast_function)
      : cast_property_(cast_property), cast_function_(cast_function) {}

  bool ConversionOptionsMatch(
      const zetasql::Catalog::FindConversionOptions& options) const {
    switch (cast_property_.type) {
      case CastFunctionType::IMPLICIT:
        return true;
      case CastFunctionType::EXPLICIT_OR_LITERAL_OR_PARAMETER:
        return options.is_explicit() ||
               options.source_kind() ==
                   Catalog::ConversionSourceExpressionKind::kLiteral ||
               options.source_kind() ==
                   Catalog::ConversionSourceExpressionKind::kParameter;
      case CastFunctionType::EXPLICIT_OR_LITERAL:
        return options.is_explicit() ||
               options.source_kind() ==
                   Catalog::ConversionSourceExpressionKind::kLiteral;
      case CastFunctionType::EXPLICIT:
        return options.is_explicit();
      default:
        break;
    }
    ABSL_LOG(ERROR) << "Unexpected CastFunctionType: "
               << static_cast<int32_t>(cast_property_.type);
    return false;
  }

  const CastFunctionProperty& GetCastProperty() const { return cast_property_; }

  const zetasql::Function* GetCastFunction() const { return cast_function_; }

 private:
  const CastFunctionProperty cast_property_;
  const zetasql::Function* cast_function_;
};

using ConversionPair =
    std::pair<const zetasql::Type*, const zetasql::Type*>;
using ConversionMap = absl::flat_hash_map<ConversionPair, const ConversionInfo>;

static const ConversionMap& GetConversionMap() {
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();
  static const zetasql::Type* gsql_pg_jsonb =
      spangres::datatypes::GetPgJsonbType();
  static const zetasql::Type* gsql_pg_oid =
      spangres::datatypes::GetPgOidType();

  static const zetasql_base::NoDestructor<ConversionMap> kConversionMap(
      {// PG.NUMERIC -> PG.NUMERIC conversion (fixed-precision cast)
       {{gsql_pg_numeric, gsql_pg_numeric},
        {CastFunctionProperty(CastFunctionType::EXPLICIT, /*coercion_cost=*/0),
         GetGenericConversionFunction()}},
       // <TYPE> -> PG.NUMERIC conversions
       {{gsql_int64, gsql_pg_numeric},
        {CastFunctionProperty(CastFunctionType::EXPLICIT, /*coercion_cost=*/0),
         GetInt64ToPgNumericConversion()}},
       {{gsql_double, gsql_pg_numeric},
        {CastFunctionProperty(CastFunctionType::EXPLICIT, /*coercion_cost=*/0),
         GetDoubleToPgNumericConversion()}},
       {{gsql_float, gsql_pg_numeric},
        {CastFunctionProperty(CastFunctionType::EXPLICIT, /*coercion_cost=*/0),
         GetFloatToPgNumericConversion()}},
       {{gsql_string, gsql_pg_numeric},
        {CastFunctionProperty(CastFunctionType::EXPLICIT, /*coercion_cost=*/0),
         GetStringToPgNumericConversion()}},
       // PG.NUMERIC -> <TYPE> conversions
       {{gsql_pg_numeric, gsql_int64},
        {CastFunctionProperty(CastFunctionType::EXPLICIT, /*coercion_cost=*/0),
         GetPgNumericToInt64Conversion()}},
       {{gsql_pg_numeric, gsql_double},
        {CastFunctionProperty(CastFunctionType::EXPLICIT, /*coercion_cost=*/0),
         GetPgNumericToDoubleConversion()}},
       {{gsql_pg_numeric, gsql_float},
        {CastFunctionProperty(CastFunctionType::EXPLICIT, /*coercion_cost=*/0),
         GetPgNumericToFloatConversion()}},
       {{gsql_pg_numeric, gsql_string},
        {CastFunctionProperty(CastFunctionType::EXPLICIT, /*coercion_cost=*/0),
         GetPgNumericToStringConversion()}},
       // <TYPE> -> PG.JSONB
       {{gsql_string, gsql_pg_jsonb},
        {CastFunctionProperty(CastFunctionType::EXPLICIT, /*coercion_cost=*/0),
         GetStringToPgJsonbConversion()}},
       // PG.JSONB -> <TYPE>
       {{gsql_pg_jsonb, gsql_bool},
        {CastFunctionProperty(CastFunctionType::EXPLICIT, /*coercion_cost=*/0),
         GetPgJsonbToBoolConversion()}},
       {{gsql_pg_jsonb, gsql_double},
        {CastFunctionProperty(CastFunctionType::EXPLICIT, /*coercion_cost=*/0),
         GetPgJsonbToDoubleConversion()}},
       {{gsql_pg_jsonb, gsql_float},
        {CastFunctionProperty(CastFunctionType::EXPLICIT, /*coercion_cost=*/0),
         GetPgJsonbToFloatConversion()}},
       {{gsql_pg_jsonb, gsql_int64},
        {CastFunctionProperty(CastFunctionType::EXPLICIT, /*coercion_cost=*/0),
         GetPgJsonbToInt64Conversion()}},
       {{gsql_pg_jsonb, gsql_pg_numeric},
        {CastFunctionProperty(CastFunctionType::EXPLICIT, /*coercion_cost=*/0),
         GetPgJsonbToPgNumericConversion()}},
       {{gsql_pg_jsonb, gsql_string},
        {CastFunctionProperty(CastFunctionType::EXPLICIT, /*coercion_cost=*/0),
         GetPgJsonbToStringConversion()}},
       // <TYPE> -> PG.OID
       {{gsql_int64, gsql_pg_oid},
        {CastFunctionProperty(CastFunctionType::IMPLICIT, /*coercion_cost=*/0),
         GetInt64ToPgOidConversion()}},
       {{gsql_string, gsql_pg_oid},
        {CastFunctionProperty(CastFunctionType::EXPLICIT, /*coercion_cost=*/0),
         GetStringToPgOidConversion()}},
       // PG.OID -> <TYPE>
       {{gsql_pg_oid, gsql_int64},
        {CastFunctionProperty(
             CastFunctionType::EXPLICIT_OR_LITERAL_OR_PARAMETER,
             /*coercion_cost=*/0),
         GetPgOidToInt64Conversion()}},
       {{gsql_pg_oid, gsql_string},
        {CastFunctionProperty(CastFunctionType::EXPLICIT, /*coercion_cost=*/0),
         GetPgOidToStringConversion()}}});
  return *kConversionMap;
}

absl::StatusOr<zetasql::Conversion> FindExtendedTypeConversion(
    const zetasql::Type* from, const zetasql::Type* to,
    const zetasql::Catalog::FindConversionOptions& options) {
  const ConversionMap& conversion_map = GetConversionMap();
  auto conversion_info = conversion_map.find(ConversionPair(from, to));

  if (conversion_info == conversion_map.end() ||
      !conversion_info->second.ConversionOptionsMatch(options)) {
    return zetasql_base::NotFoundErrorBuilder()
           << (options.is_explicit() ? "Cast" : "Coercion") << " from type "
           << from->DebugString() << " to type " << to->DebugString()
           << " not found in catalog.";
  }

  return zetasql::Conversion::Create(
      from, to, conversion_info->second.GetCastFunction(),
      conversion_info->second.GetCastProperty());
}

}  // namespace datatypes
}  // namespace postgres_translator::spangres
