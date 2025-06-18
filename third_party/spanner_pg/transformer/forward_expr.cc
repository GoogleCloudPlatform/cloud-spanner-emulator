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

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/cast.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/input_argument_type.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/signature_match_result.h"
#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/simple_value.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/types/type_parameters.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/cleanup/cleanup.h"
#include "absl/container/flat_hash_map.h"
#include "absl/flags/flag.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "third_party/spanner_pg/bootstrap_catalog/bootstrap_catalog.h"
#include "third_party/spanner_pg/catalog/catalog_adapter.h"
#include "third_party/spanner_pg/catalog/function.h"
#include "third_party/spanner_pg/catalog/spangres_type.h"
#include "third_party/spanner_pg/catalog/type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_oid_type.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "third_party/spanner_pg/transformer/expr_transformer_helper.h"
#include "third_party/spanner_pg/transformer/forward_transformer.h"
#include "third_party/spanner_pg/transformer/transformer_helper.h"
#include "third_party/spanner_pg/util/nodetag_to_string.h"
#include "third_party/spanner_pg/util/oid_to_string.h"
#include "third_party/spanner_pg/util/pg_list_iterators.h"
#include "third_party/spanner_pg/util/postgres.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {

using ::postgres_translator::internal::PostgresCastNodeTemplate;
using ::postgres_translator::internal::PostgresCastToExpr;
using ::postgres_translator::internal::PostgresCastToNode;
using ::postgres_translator::internal::PostgresConstCastNodeTemplate;
using ::postgres_translator::internal::PostgresConstCastToExpr;
using ::postgres_translator::internal::PostgresConstCastToNode;

namespace {

struct PrecisionAndScale {
  int32_t precision;
  int32_t scale;
};

// Use to extract precision and scale for typmod associated with pg.numeric
PrecisionAndScale GetPrecisionAndScaleFromTypMod(int32_t typmod) {
  // PostgreSQL typmods are index-shifted by adding VARHDRSZ. Subtract
  // VARHDRSZ to get back the original typmod.
  return {.precision = ((typmod - VARHDRSZ) >> 16) & 0xffff,
          .scale = (typmod - VARHDRSZ) & 0xffff};
}

bool IsFixedPrecisionNumericCastSignature(
    const zetasql::FunctionSignature& signature) {
  return signature.arguments().size() == 3 &&
         signature.argument(0).type()->Equals(
             spangres::types::PgNumericMapping()->mapped_type()) &&
         signature.argument(1).type()->IsInt64() &&
         signature.argument(2).type()->IsInt64();
}

}  // namespace

absl::StatusOr<std::unique_ptr<zetasql::ResolvedLiteral>>
ForwardTransformer::BuildGsqlResolvedLiteral(Oid const_type, Datum const_value,
                                             bool const_is_null,
                                             CoercionForm cast_format) {
  // Make a copy of the const with the new type and value.
  Const* new_const;
  // Bool is special as its size is hardwired as 1 in postgres.
  if (const_type == BOOLOID) {
    Node* bool_const;
    ZETASQL_ASSIGN_OR_RETURN(bool_const, CheckedPgMakeBoolConst(
                                     DatumGetBool(const_value), const_is_null));
    new_const = PostgresCastNode(Const, bool_const);
  } else {
    ZETASQL_ASSIGN_OR_RETURN(new_const, internal::makeScalarConst(
                                    const_type, const_value, const_is_null));
  }

  return BuildGsqlResolvedLiteral(*new_const);
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedLiteral>>
ForwardTransformer::BuildGsqlResolvedLiteral(const Const& _const) {
  ZETASQL_ASSIGN_OR_RETURN(const zetasql::Type* type,
                   BuildGsqlType(_const.consttype));

  if (!type) {
    return absl::UnimplementedError(
        absl::StrCat("Unrecognized PostgreSQL data type: ",
                     OidToTypeString(_const.consttype)));
  }

  ZETASQL_ASSIGN_OR_RETURN(zetasql::Value value,
                   catalog_adapter_->GetEngineSystemCatalog()
                       ->GetType(_const.consttype)
                       ->MakeGsqlValue(&_const));

  std::unique_ptr<zetasql::ResolvedLiteral> literal =
      zetasql::MakeResolvedLiteral(type, value);
  if (_const.location != -1) {
    zetasql::ParseLocationRange parse_location_range;
    parse_location_range.set_start(
        zetasql::ParseLocationPoint::FromByteOffset(_const.location));

    absl::StatusOr<int> end_location =
        GetEndLocationForStartLocation(_const.location);
    if (end_location.ok()) {
      parse_location_range.set_end(
          zetasql::ParseLocationPoint::FromByteOffset(*end_location));
      literal->SetParseLocationRange(parse_location_range);
    } else {
      ABSL_LOG(ERROR) << "dropping token start location due to failure to look up "
                    "end location: "
                 << end_location.status();
    }
  }

  return literal;
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
ForwardTransformer::BuildGsqlResolvedExpr(
    const ArrayExpr& expr, ExprTransformerInfo* expr_transformer_info) {
  // Multi-dimensional arrays are not supported.
  if (expr.multidims) {
    return absl::InvalidArgumentError(
        "Multi-dimensional arrays are not supported");
  }

  // Determine whether we're building a ResolvedLiteral or a
  // ResolvedFunctionCall.
  bool all_constants = true;
  for (Node* node : StructList<Node*>(expr.elements)) {
    if (!IsA(node, Const)) {
      all_constants = false;
      break;
    }
  }

  if (all_constants) {
    // Build a ResolvedLiteral
    std::vector<zetasql::Value> array_element_values;
    array_element_values.reserve(list_length(expr.elements));
    for (Const* node : StructList<Const*>(expr.elements)) {
      // Shouldn't happen, but it's cheap to check and could save us a crash.
      ZETASQL_RET_CHECK_EQ(node->consttype, expr.element_typeid)
          << "Array constructor has mismatched types. Array has "
             "element_typeid: "
          << expr.element_typeid << " but found an element with typeid "
          << node->consttype;
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<zetasql::ResolvedLiteral> element,
                       BuildGsqlResolvedLiteral(*node));
      ZETASQL_RET_CHECK_EQ(element->node_kind(), zetasql::RESOLVED_LITERAL)
          << element->DebugString();
      array_element_values.emplace_back(element->value());
    }

    ZETASQL_ASSIGN_OR_RETURN(const zetasql::Type* type,
                     BuildGsqlType(expr.array_typeid));
    ZETASQL_RET_CHECK(type->IsArray());
    const zetasql::ArrayType* array_type = type->AsArray();
    ZETASQL_ASSIGN_OR_RETURN(
        zetasql::Value array_literal_value,
        zetasql::Value::MakeArray(array_type, array_element_values));
    std::unique_ptr<zetasql::ResolvedLiteral> literal =
        zetasql::MakeResolvedLiteral(array_literal_value);
    if (expr.location != -1) {
      zetasql::ParseLocationRange parse_location_range;
      parse_location_range.set_start(
          zetasql::ParseLocationPoint::FromByteOffset(expr.location));

      // ArrayExpr's location only includes the ARRAY keyword. Get the end
      // location from the last element. Note: this does not include the closing
      // ']' character so we'll add one to grab it.
      // In case there are no elements, default to just "ARRAY[]".
      int last_element_location = expr.location + 1;
      if (list_length(expr.elements) > 0) {
        last_element_location =
            PostgresCastNode(Const, llast(expr.elements))->location;
      }

      absl::StatusOr<int> end_location =
          GetEndLocationForStartLocation(last_element_location);
      if (end_location.ok()) {
        parse_location_range.set_end(
            zetasql::ParseLocationPoint::FromByteOffset((*end_location) + 1));
        literal->SetParseLocationRange(parse_location_range);
      } else {
        ABSL_LOG(ERROR) << "dropping token start location due to failure to look up "
                      "end location: "
                   << end_location.status();
      }
    }
    return literal;
  } else {
    // Build a $make_array ResolvedFunctionCall
    return BuildGsqlResolvedMakeArrayFunctionCall(expr, expr_transformer_info);
  }
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedParameter>>
ForwardTransformer::BuildGsqlResolvedParameter(const Param& pg_param) {
  if (pg_param.paramkind != PARAM_EXTERN) {
    return absl::InvalidArgumentError(
        absl::StrCat("Unsupported parameter type: ",
                     internal::ParamKindToString(pg_param.paramkind)));
  }

  bool is_undefined = pg_param.paramtype == UNKNOWNOID;
  ZETASQL_ASSIGN_OR_RETURN(const zetasql::Type* type,
                   BuildGsqlType(pg_param.paramtype));

  std::unique_ptr<zetasql::ResolvedParameter> gsql_param =
      zetasql::MakeResolvedParameter(
          type, absl::StrCat(kParameterPrefix, pg_param.paramid), 0,
          is_undefined);

  // Track the parameter types so that they can be returned separately
  // when transformation is completed.
  query_parameter_types_[gsql_param->name()] = type;

  return std::move(gsql_param);
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedLiteral>>
ForwardTransformer::BuildGsqlResolvedLiteralFromCast(
    Oid cast_function, const Const& input_literal, const Const* typmod,
    const Const* explicit_cast, Oid output_type, CoercionForm cast_format) {
  // Only unsupported literals should execute the PostgreSQL casting logic here.
  // All other types can execute the storage engine casting logic during
  // query execution.
  ZETASQL_RET_CHECK_EQ(catalog_adapter_->GetEngineSystemCatalog()->GetType(
                   input_literal.consttype),
               nullptr);

  // Requirement: the output type is supported.
  ZETASQL_RETURN_IF_ERROR(BuildGsqlType(output_type).status());

  // Execute the corresponding Postgres casting function.
  Datum new_constvalue;
  if (typmod == nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(
        new_constvalue,
        CheckedOidFunctionCall1(cast_function, input_literal.constvalue));
  } else if (explicit_cast == nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(
        new_constvalue,
        CheckedOidFunctionCall2(cast_function, input_literal.constvalue,
                                typmod->constvalue));
  } else {
    ZETASQL_ASSIGN_OR_RETURN(
        new_constvalue,
        CheckedOidFunctionCall3(cast_function, input_literal.constvalue,
                                typmod->constvalue, explicit_cast->constvalue));
  }

  return BuildGsqlResolvedLiteral(output_type, new_constvalue,
                                  input_literal.constisnull, cast_format);
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
ForwardTransformer::BuildGsqlCastExpression(
    Oid result_type, const Expr& input, int32_t typmod,
    ExprTransformerInfo* expr_transformer_info) {
  ZETASQL_ASSIGN_OR_RETURN(Oid source_type_oid,
                   CheckedPgExprType(PostgresConstCastToNode(&input)));

  ZETASQL_ASSIGN_OR_RETURN(const zetasql::Type* resolved_type,
                   BuildGsqlType(result_type));

  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<zetasql::ResolvedExpr> resolved_input,
                   BuildGsqlResolvedExpr(input, expr_transformer_info));

  const zetasql::Type* target_type = resolved_type;
  const zetasql::Type* source_type = resolved_input->type();

  // If this is a generic cast override, build a ResolvedFunctionCall instead.
  if (catalog_adapter_->GetEngineSystemCatalog()->HasCastOverrideFunction(
          source_type, target_type)) {
    ZETASQL_ASSIGN_OR_RETURN(FunctionAndSignature function_and_signature,
                     catalog_adapter_->GetEngineSystemCatalog()
                         ->GetCastOverrideFunctionAndSignature(
                             source_type, target_type,
                             catalog_adapter_->analyzer_options().language()));
    std::vector<std::unique_ptr<zetasql::ResolvedExpr>> argument_list;
    argument_list.push_back(std::move(resolved_input));
    return zetasql::MakeResolvedFunctionCall(
        target_type, function_and_signature.function(),
        function_and_signature.signature(), std::move(argument_list),
        zetasql::ResolvedFunctionCallBase::DEFAULT_ERROR_MODE);
  }

  // PG.NUMERIC cast overrides are treated separately.
    if (result_type == NUMERICOID || source_type_oid == NUMERICOID) {

    ZETASQL_ASSIGN_OR_RETURN(
        FunctionAndSignature function_and_signature,
        catalog_adapter_->GetEngineSystemCatalog()->GetPgNumericCastFunction(
            source_type, target_type,
            catalog_adapter_->analyzer_options().language()));
    ABSL_DCHECK_NE(function_and_signature.function(), nullptr);

    std::vector<std::unique_ptr<zetasql::ResolvedExpr>> argument_list;
    argument_list.push_back(std::move(resolved_input));
    if (IsFixedPrecisionNumericCastSignature(
            function_and_signature.signature())) {
      if (typmod == -1) {
        return absl::InvalidArgumentError(
            absl::StrCat("Fixed precision numeric cast requires valid typmod. "
                         "Provided typmod: ",
                         typmod));
      }
      PrecisionAndScale precision_and_scale =
          GetPrecisionAndScaleFromTypMod(typmod);
      argument_list.push_back(zetasql::MakeResolvedLiteral(
          zetasql::types::Int64Type(),
          zetasql::Value::Int64(precision_and_scale.precision)));
      argument_list.push_back(zetasql::MakeResolvedLiteral(
          zetasql::types::Int64Type(),
          zetasql::Value::Int64(precision_and_scale.scale)));
    } else if (typmod != -1) {
      return absl::UnimplementedError(
          "Casts with a typmod are generally not supported. The only "
          "supported cast with a typmod is varchar(<length>) and "
          "numeric(<precision>,<scale>).");
    }
    resolved_input = zetasql::MakeResolvedFunctionCall(
        target_type, function_and_signature.function(),
        function_and_signature.signature(), std::move(argument_list),
        zetasql::ResolvedFunctionCallBase::DEFAULT_ERROR_MODE);

    return resolved_input;
  }

  // If not pg.numeric, rely on ResolvedCast.
  ZETASQL_ASSIGN_OR_RETURN(bool is_cast_supported,
                   catalog_adapter_->GetEngineSystemCatalog()->IsValidCast(
                       source_type, target_type,
                       catalog_adapter_->analyzer_options().language()));
  if (!is_cast_supported) {
    return UnsupportedCastError(source_type_oid, result_type);
  }
  std::unique_ptr<zetasql::ResolvedCast> resolved_cast =
      zetasql::MakeResolvedCast(resolved_type, std::move(resolved_input),
                                  /*return_null_on_error=*/false);

  bool is_jsonb_source_or_result_in_emulator =
      (source_type_oid == JSONBOID || result_type == JSONBOID);
  bool is_oid_source_or_result_in_emulator =
      (source_type_oid == OIDOID || result_type == OIDOID);
  if (is_jsonb_source_or_result_in_emulator ||
      is_oid_source_or_result_in_emulator) {
    zetasql::ExtendedCompositeCastEvaluator extended_conversion_evaluator =
        zetasql::ExtendedCompositeCastEvaluator::Invalid();
    zetasql::SignatureMatchResult result;
    ABSL_CHECK(coercer_ != nullptr);
    ZETASQL_ASSIGN_OR_RETURN(
        bool cast_is_valid,
        coercer_->CoercesTo(zetasql::InputArgumentType(source_type),
                            target_type, /*is_explicit=*/true, &result,
                            &extended_conversion_evaluator));
    ZETASQL_RET_CHECK(cast_is_valid);

    if (extended_conversion_evaluator.is_valid()) {
      std::vector<std::unique_ptr<const zetasql::ResolvedExtendedCastElement>>
          conversion_list;
      for (const zetasql::ConversionEvaluator& evaluator :
           extended_conversion_evaluator.evaluators()) {
        conversion_list.push_back(MakeResolvedExtendedCastElement(
            evaluator.from_type(), evaluator.to_type(), evaluator.function()));
      }
      resolved_cast->set_extended_cast(
          MakeResolvedExtendedCast(std::move(conversion_list)));
    }
  }

  // Fixed-precision cast; requires additional type parameters
  if (result_type == NUMERICOID && source_type_oid == NUMERICOID) {
    if (typmod == -1) {
      return absl::InvalidArgumentError(
          absl::StrCat("Fixed precision numeric cast requires valid typmod. "
                       "Provided typmod: ",
                       typmod));
    }
    PrecisionAndScale precision_and_scale =
        GetPrecisionAndScaleFromTypMod(typmod);
    resolved_cast->set_type_parameters(
        zetasql::TypeParameters::MakeExtendedTypeParameters(
            zetasql::ExtendedTypeParameters(
                {zetasql::SimpleValue::Int64(precision_and_scale.precision),
                 zetasql::SimpleValue::Int64(precision_and_scale.scale)})));
  } else if (result_type == VARCHAROID && typmod != -1) {
    // PostgreSQL typmods are index-shifted by adding VARHDRSZ. Subtract
    // VARHDRSZ to get back the original typmod.
    int32_t substring_length = typmod - VARHDRSZ;
    return BuildGsqlSubstrFunctionCall(std::move(resolved_cast),
                                       substring_length);
  }
  return resolved_cast;
}

absl::StatusOr<bool> ForwardTransformer::IsCastFunction(
    const FuncExpr& func_expr) {
  auto pg_cast_or =
      PgBootstrapCatalog::Default()->GetCastByFunctionOid(func_expr.funcid);
  if (pg_cast_or.ok()) {
    ZETASQL_RET_CHECK_NE(pg_cast_or.value(), nullptr);
    return true;
  }
  ZETASQL_RET_CHECK(absl::IsNotFound(pg_cast_or.status())) << pg_cast_or.status();
  return false;
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
ForwardTransformer::BuildGsqlResolvedExpr(
    const FuncExpr& func_expr, ExprTransformerInfo* expr_transformer_info) {
  ZETASQL_ASSIGN_OR_RETURN(bool is_cast_function, IsCastFunction(func_expr));

  // PostgreSQL has casting functions but ZetaSQL does not. Only non-casting
  // functions should be transformed to a ResolvedFunctionCall.
  if (!is_cast_function) {
    return BuildGsqlResolvedFunctionCall(func_expr, expr_transformer_info);
  } else {
    // Validity check: The casting function has at most three arguments.
    // The first is the input value. The second, if it exists, is an int4 which
    // represents the typmod. The third, if it exists, is a boolean which
    // indicates if the cast is explicit.
    ZETASQL_RET_CHECK(list_length(func_expr.args) <= 3);
    Node* argument = PostgresCastToNode(linitial(func_expr.args));
    Const* typmod = nullptr;
    Const* explicit_cast = nullptr;
    if (list_length(func_expr.args) >= 2) {
      typmod = PostgresCastNode(Const, lsecond(func_expr.args));
      if (list_length(func_expr.args) == 3) {
        explicit_cast = PostgresCastNode(Const, lthird(func_expr.args));
      }
    }

    // First, get value of typmod. typmod is currently only supported for
    // VARCHAROID and NUMERICOID.
    int32_t typmod_value = -1;
    if (typmod != nullptr) {
      ZETASQL_RET_CHECK_EQ(typmod->consttype, INT4OID);
      typmod_value = DatumGetInt32(typmod->constvalue);
      if (func_expr.funcresulttype != VARCHAROID &&
          func_expr.funcresulttype != NUMERICOID) {
        return absl::UnimplementedError(
            "Casts with a typmod are generally not supported. The only "
            "supported casts with a typmod are varchar(<length>) and "
            "numeric(<precision>,<scale>).");
      }
    }
    // Construct a cast expression. The cast expression may be
    // a ResolvedCast or a ResolvedFunctionCall.
    // Note that this is different from the ZetaSQL
    // analyzer behavior, which attempts to execute all casts of literals
    // inside of the analyzer. We've chosen to build a cast expression here
    // instead of executing a PostgreSQL casting function so that all casts
    // (literals, table columns, computed columns, etc) are executed by the
    // storage engine instead of using the storage engine casting logic in
    // some scenarios and using PostgreSQL casting logic in other scenarios.
    return BuildGsqlCastExpression(func_expr.funcresulttype,
                                   *PostgresCastToExpr(argument), typmod_value,
                                   expr_transformer_info);
  }
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
ForwardTransformer::BuildGsqlResolvedExpr(
    const CoerceViaIO& coerce_via_io,
    ExprTransformerInfo* expr_transformer_info) {
  Expr* argument = coerce_via_io.arg;
  // Construct a cast expression.
  // Note that this is different from the ZetaSQL
  // analyzer behavior, which attempts to execute all casts of literals
  // inside of the analyzer. We've chosen to build a cast expression here
  // instead of executing a PostgreSQL casting function so that all casts
  // (literals, table columns, computed columns, etc) are executed by the
  // storage engine instead of using the storage engine casting logic in some
  // scenarios and using PostgreSQL casting logic in other scenarios.
  return BuildGsqlCastExpression(coerce_via_io.resulttype, *argument,
                                 /*typmod=*/-1, expr_transformer_info);
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
ForwardTransformer::BuildGsqlResolvedExpr(
    const RelabelType& relabel_type,
    ExprTransformerInfo* expr_transformer_info) {
  ZETASQL_ASSIGN_OR_RETURN(Oid source_type_oid,
                   CheckedPgExprType(PostgresCastToNode(relabel_type.arg)));
  ZETASQL_ASSIGN_OR_RETURN(const zetasql::Type* source_type,
                   BuildGsqlType(source_type_oid));
  ZETASQL_ASSIGN_OR_RETURN(const zetasql::Type* target_type,
                   BuildGsqlType(relabel_type.resulttype));

  // The only relabel type we currently support is varchar <-> text, which both
  // transform to string, so the relabel type is a no-op. All other casts that
  // require transformation are unsupported.
  if (!source_type->Equals(target_type)) {
    return UnsupportedCastError(source_type_oid, relabel_type.resulttype);
  }
  // The type cast is a no-op so we can just return the transformed input.
  return BuildGsqlResolvedExpr(*relabel_type.arg, expr_transformer_info);
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
ForwardTransformer::BuildGsqlResolvedExpr(
    const NamedArgExpr& named_arg,
    ExprTransformerInfo* expr_transformer_info) {
  return BuildGsqlResolvedExpr(*named_arg.arg, expr_transformer_info);
}

absl::Status ForwardTransformer::UnsupportedCastError(Oid source_type_oid,
                                                      Oid target_type_oid) {
  ZETASQL_ASSIGN_OR_RETURN(
      const char* source_type,
      PgBootstrapCatalog::Default()->GetFormattedTypeName(source_type_oid));
  ZETASQL_ASSIGN_OR_RETURN(
      const char* target_type,
      PgBootstrapCatalog::Default()->GetFormattedTypeName(target_type_oid));
  return absl::UnimplementedError(absl::StrFormat(
      "Cast from %s to %s is unsupported.", source_type, target_type));
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedDMLDefault>>
ForwardTransformer::BuildGsqlResolvedDMLDefault(
    const SetToDefault& set_to_default) {
  ZETASQL_ASSIGN_OR_RETURN(const zetasql::Type* type,
                   BuildGsqlType(set_to_default.typeId));
  return zetasql::MakeResolvedDMLDefault(type);
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
ForwardTransformer::BuildGsqlResolvedScalarExpr(
    const Expr& expr, const VarIndexScope* var_index_scope,
    const char* clause_name) {
  // Build an ExprTransformerInfo that disallows aggregation.
  ExprTransformerInfo expr_transformer_info =
      ExprTransformerInfo::ForScalarFunctions(var_index_scope, clause_name);
  return BuildGsqlResolvedExpr(expr, &expr_transformer_info);
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
ForwardTransformer::BuildGsqlResolvedExpr(
    const Expr& expr, ExprTransformerInfo* expr_transformer_info) {
  ++expression_recursion_tree_depth_;

  // We only want to keep track of an expression tree depth.
  // `expression_recursion_tree_depth_` will be decremented when
  // `decrement_expression_count` goes out of scope.
  auto decrement_expression_count =
      absl::MakeCleanup([this]() { --this->expression_recursion_tree_depth_; });

  if (expression_recursion_tree_depth_ >
      absl::GetFlag(FLAGS_spangres_expression_recursion_limit)) {
    return absl::InvalidArgumentError(
        absl::StrCat("SQL expression limit exceeded: ",
                     absl::GetFlag(FLAGS_spangres_expression_recursion_limit)));
  }

  if (expr_transformer_info->use_post_grouping_columns) {
    // We have already resolved aggregate function calls for SELECT list
    // aggregate expressions.  Second pass aggregate function transformer
    // should look up these resolved aggregate function calls in the map.
    ZETASQL_RET_CHECK(expr_transformer_info->transformer_info != nullptr);
    const zetasql::ResolvedComputedColumn* computed_aggregate_column =
        zetasql_base::FindPtrOrNull(
            expr_transformer_info->transformer_info->aggregate_expr_map(),
            PostgresConstCastToExpr(&expr));
    if (computed_aggregate_column != nullptr) {
      const zetasql::ResolvedColumn& column =
          computed_aggregate_column->column();
      return BuildGsqlResolvedColumnRef(column);
    }
    // If we do not find them in the map, then fall through
    // and resolve the aggregate function call normally.  This is required
    // when the aggregate function appears in the HAVING or ORDER BY.
  }

  switch (expr.type) {
    case T_Const: {
      return BuildGsqlResolvedLiteral(*PostgresConstCastNode(Const, &expr));
    }
    case T_NamedArgExpr: {
      return BuildGsqlResolvedExpr(*PostgresConstCastNode(NamedArgExpr, &expr),
                                   expr_transformer_info);
    }
    case T_Var: {
      // NOTE: Var might mean `ResolvedColumn` in some cases. But those cases
      // should specially handle Var and then call BuildGsqlResolvedExpr on
      // the other Expr types.
      return BuildGsqlResolvedColumnRef(
          *PostgresConstCastNode(Var, &expr),
          *expr_transformer_info->var_index_scope);
    }
    case T_Param: {
      Param param = *PostgresConstCastNode(Param, &expr);
      // If there are no argument names, then the param is a query parameter.
      if (create_func_arg_names_.empty()) {
        return BuildGsqlResolvedParameter(param);
      } else {
        // DDL statements with input arguments have parameter names.
        // NOTE: zetasql::Validator won't allow ArgumentRef nodes outside of
        // UDF/TVF type stmts
        std::string param_name = create_func_arg_names_[param.paramid - 1];
        ZETASQL_ASSIGN_OR_RETURN(const zetasql::Type* type,
                         BuildGsqlType(param.paramtype));
        create_func_arg_types_[param_name] = type;
        // NOTE: only scalar arguments are supported presently.
        auto column_ref = zetasql::MakeResolvedArgumentRef(
            type, param_name, zetasql::ResolvedArgumentDef::SCALAR);
        return column_ref;
      }
    }
    case T_OpExpr: {
      return BuildGsqlResolvedFunctionCall(
          *PostgresConstCastNode(OpExpr, &expr), expr_transformer_info);
    }
    case T_FuncExpr: {
      return BuildGsqlResolvedExpr(*PostgresConstCastNode(FuncExpr, &expr),
                                   expr_transformer_info);
    }
    case T_SubLink: {
      return BuildGsqlResolvedSubqueryExpr(
          *PostgresConstCastNode(SubLink, &expr), expr_transformer_info);
    }
    case T_CoerceViaIO: {
      return BuildGsqlResolvedExpr(*PostgresConstCastNode(CoerceViaIO, &expr),
                                   expr_transformer_info);
    }
    case T_RelabelType: {
      return BuildGsqlResolvedExpr(*PostgresConstCastNode(RelabelType, &expr),
                                   expr_transformer_info);
    }
    case T_CurrentOfExpr: {
      return absl::UnimplementedError(
          "UPDATE...CURRENT OF and DELETE...CURRENT OF statements are not "
          "supported.");
    }
    case T_SetToDefault: {
      return BuildGsqlResolvedDMLDefault(
          *PostgresConstCastNode(SetToDefault, &expr));
    }
    case T_Aggref: {
      return BuildGsqlResolvedAggregateFunctionCall(
          *PostgresConstCastNode(Aggref, &expr), expr_transformer_info);
    }
    case T_BoolExpr: {
      return BuildGsqlResolvedBoolFunctionCall(
          *PostgresConstCastNode(BoolExpr, &expr), expr_transformer_info);
    }
    case T_CoalesceExpr: {
      return BuildGsqlResolvedFunctionCall(
          expr.type, PostgresConstCastNode(CoalesceExpr, &expr)->args,
          expr_transformer_info);
    }
    case T_CaseExpr: {
      return BuildGsqlResolvedCaseFunctionCall(
          *PostgresConstCastNode(CaseExpr, &expr), expr_transformer_info);
    }
    case T_NullTest: {
      return BuildGsqlResolvedNullTestFunctionCall(
          *PostgresConstCastNode(NullTest, &expr), expr_transformer_info);
    }
    case T_MinMaxExpr: {
      return BuildGsqlResolvedGreatestLeastFunctionCall(
          *PostgresConstCastNode(MinMaxExpr, &expr), expr_transformer_info);
    }
    case T_BooleanTest: {
      return BuildGsqlResolvedBooleanTestExpr(
          *PostgresConstCastNode(BooleanTest, &expr), expr_transformer_info);
    }
    case T_NullIfExpr: {
      return BuildGsqlResolvedFunctionCall(
          expr.type, PostgresConstCastNode(NullIfExpr, &expr)->args,
          expr_transformer_info);
    }
    case T_ScalarArrayOpExpr: {
      return BuildGsqlResolvedScalarArrayFunctionCall(
          *PostgresConstCastNode(ScalarArrayOpExpr, &expr),
          expr_transformer_info);
    }
    case T_SQLValueFunction: {
      return BuildGsqlResolvedSQLValueFunctionCall(
          *PostgresConstCastNode(SQLValueFunction, &expr));
    }
    case T_ArrayExpr: {
      return BuildGsqlResolvedExpr(*PostgresConstCastNode(ArrayExpr, &expr),
                                   expr_transformer_info);
    }
    case T_SubscriptingRef: {
      return BuildGsqlArrayAccess(
          *PostgresConstCastNode(SubscriptingRef, &expr),
          expr_transformer_info);
    }
    case T_ArrayCoerceExpr: {
      return absl::UnimplementedError(
          "Array casting / coercion is not supported");
    }

    default:
      // This should really be an internal error default case with user-friendly
      // messages for unsupported cases we can identify (like T_CurrentOfExpr).
      return absl::InvalidArgumentError(
          absl::StrCat("Unsupported PostgreSQL expression type: ",
                       NodeTagToNodeString(expr.type)));
  }
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedSubqueryExpr>>
ForwardTransformer::BuildGsqlResolvedSubqueryExpr(
    const Query& pg_subquery, const Expr* in_testexpr, const List* hint_list,
    const zetasql::Type* output_type,
    zetasql::ResolvedSubqueryExpr::SubqueryType subquery_type,
    ExprTransformerInfo* expr_transformer_info) {
  std::unique_ptr<zetasql::ResolvedExpr> resolved_in_expr = nullptr;
  std::vector<std::unique_ptr<const zetasql::ResolvedOption>> resolved_hints;

  if (subquery_type != zetasql::ResolvedSubqueryExpr::IN) {
    // in_expr and hints are only used by IN subquery expressions.
    ZETASQL_RET_CHECK_EQ(in_testexpr, nullptr);
    ZETASQL_RET_CHECK_EQ(hint_list, nullptr);
  } else {
    ZETASQL_RET_CHECK_NE(in_testexpr, nullptr);
    ZETASQL_RET_CHECK(IsA(in_testexpr, OpExpr));
    const OpExpr* in_opexpr = PostgresConstCastNode(OpExpr, in_testexpr);

    // Extract and transform the in_expr, which is the left hand argument to the
    // operator.
    const Expr* in_expr = PostgresConstCastToExpr(linitial(in_opexpr->args));
    ZETASQL_ASSIGN_OR_RETURN(resolved_in_expr,
                     BuildGsqlResolvedExpr(*in_expr, expr_transformer_info));

    // Transform the hints, if there are any.
    if (hint_list != nullptr) {
      ZETASQL_ASSIGN_OR_RETURN(
          resolved_hints,
          BuildGsqlResolvedOptionList(*hint_list, &empty_var_index_scope_));
    }
  }

  // Transform the subquery and collect information about any correlated
  // columns in the subquery.
  CorrelatedColumnsSet correlated_columns_set;
  VarIndexScope subquery_scope(expr_transformer_info->var_index_scope,
                               &correlated_columns_set);
  // Output columns are not constructed for subqueries, so there is no
  // need to collect the output_name_list.
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<zetasql::ResolvedScan> subquery,
                   BuildGsqlResolvedScanForQueryExpression(
                       pg_subquery,
                       /*is_top_level_query=*/false, &subquery_scope,
                       kAnonymousExprSubquery, /*output_name_list=*/nullptr));
  ZETASQL_ASSIGN_OR_RETURN(
      std::vector<std::unique_ptr<const zetasql::ResolvedColumnRef>>
          parameters,
      BuildGsqlCorrelatedSubqueryParameters(correlated_columns_set));

  if (subquery_type == zetasql::ResolvedSubqueryExpr::IN) {
    // Determine if the in_expr and the IN subquery must be modified to handle
    // comparisons.
    //
    // For example, in Spanner comparisons between two DOUBLE expressions
    // require both expressions to be wrapped in pg.map_double_to_int in order
    // to correctly handled NaN comparison.
    //
    // If comparison handling is required, wrap the in_expr and the subquery
    // so that `x IN (select y FROM t)` becomes
    // `wrap_func(x) IN (select wrap_func(y) FROM (select y FROM t))`
    const OpExpr* in_opexpr = PostgresConstCastNode(OpExpr, in_testexpr);
    ZETASQL_ASSIGN_OR_RETURN(
        const FormData_pg_operator* op,
        PgBootstrapCatalog::Default()->GetOperator(in_opexpr->opno));
    bool needs_comparison_handling =
        catalog_adapter_->GetEngineSystemCatalog()
            ->GetMappedOidForComparisonFuncid(op->oprcode)
            .has_value();

    if (needs_comparison_handling) {
      // Handle the in_expr.
      ZETASQL_ASSIGN_OR_RETURN(
          resolved_in_expr,
          catalog_adapter_->GetEngineSystemCatalog()
              ->GetResolvedExprForComparison(
                  std::move(resolved_in_expr),
                  catalog_adapter_->analyzer_options().language()));

      // Handle the IN subquery.
      ZETASQL_ASSIGN_OR_RETURN(subquery,
                       AddGsqlProjectScanForInSubqueries(std::move(subquery)));
    }
  }

  // Build the ResolvedSubqueryExpr.
  auto subquery_expr = zetasql::MakeResolvedSubqueryExpr(
      output_type, subquery_type, std::move(parameters),
      std::move(resolved_in_expr),
      /*subquery=*/std::move(subquery));
  subquery_expr->set_hint_list(std::move(resolved_hints));

  // Every ResolvedOrderByScan is initially constructed with is_ordered set
  // to true. However, other than array-returning subqueries, other types of
  // subqueries should not preserve order, so we clear is_ordered on the top
  // level scan of other subquery types, even if they are ResolvedOrderByScan.
  if (subquery_type != zetasql::ResolvedSubqueryExpr::ARRAY) {
    const_cast<zetasql::ResolvedScan*>(subquery_expr->subquery())
      ->set_is_ordered(false);
  }
  return subquery_expr;
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
ForwardTransformer::BuildGsqlResolvedSubqueryExpr(
    const SubLink& pg_sublink, ExprTransformerInfo* expr_transformer_info) {
  switch (pg_sublink.subLinkType) {
    case EXISTS_SUBLINK: {
      return BuildGsqlResolvedSubqueryExpr(
          *PostgresConstCastNode(Query, pg_sublink.subselect),
          /*in_testexpr=*/nullptr, /*hint_list=*/nullptr,
          zetasql::types::BoolType(), zetasql::ResolvedSubqueryExpr::EXISTS,
          expr_transformer_info);
    }
    case EXPR_SUBLINK: {
      const Query* subquery =
          PostgresConstCastNode(Query, pg_sublink.subselect);
      ZETASQL_RET_CHECK_GT(list_length(subquery->targetList), 0)
          << "Expr Subquery should have exactly 1 output column.";
      const TargetEntry* entry =
          linitial_node(TargetEntry, subquery->targetList);
      // It can be assumed that resjunk columns come after non-junk columns.
      for (int i = 1; i < list_length(subquery->targetList); ++i) {
        const TargetEntry* target_entry = list_nth_node(
          TargetEntry, subquery->targetList, i);
        ZETASQL_RET_CHECK(target_entry->resjunk)
         << "Expr Subquery should have exactly 1 output column.";
      }
      ZETASQL_ASSIGN_OR_RETURN(Oid output_type_oid,
                       CheckedPgExprType(PostgresCastToNode(entry->expr)));
      ZETASQL_ASSIGN_OR_RETURN(const zetasql::Type* output_type,
                       BuildGsqlType(output_type_oid));
      return BuildGsqlResolvedSubqueryExpr(
          *subquery, /*in_testexpr=*/nullptr, /*hint_list=*/nullptr,
          output_type, zetasql::ResolvedSubqueryExpr::SCALAR,
          expr_transformer_info);
    }
    case ARRAY_SUBLINK: {
      // ARRAY(SELECT with single output column ...)
      const Query* subquery =
          PostgresConstCastNode(Query, pg_sublink.subselect);
      ZETASQL_RET_CHECK_GT(list_length(subquery->targetList), 0)
          << "Array subquery should have exactly 1 output column.";
      // It can be assumed that resjunk columns come after non-junk columns.
      for (int i = 1; i < list_length(subquery->targetList); ++i) {
        const TargetEntry* target_entry = list_nth_node(
          TargetEntry, subquery->targetList, i);
        if (!target_entry->resjunk) {
          return absl::InvalidArgumentError(
              "Array subquery should have exactly 1 output column.");
        }
      }
      const TargetEntry* target_entry = list_nth_node(
          TargetEntry, subquery->targetList, 0);
      ZETASQL_ASSIGN_OR_RETURN(Oid output_type_oid, CheckedPgExprType(
          PostgresCastToNode(target_entry->expr)));
      ZETASQL_ASSIGN_OR_RETURN(const zetasql::Type* output_type,
                      BuildGsqlType(output_type_oid));
      if (output_type->IsArray()) {
        return absl::InvalidArgumentError(
            "Cannot use array subquery with column of type " +
            OidToTypeString(output_type_oid) +
            " because nested arrays are not supported.");
      }
      const zetasql::ArrayType* array_type = nullptr;
      ZETASQL_RETURN_IF_ERROR(GetTypeFactory()->MakeArrayType(output_type,
                                                        &array_type));
      return BuildGsqlResolvedSubqueryExpr(
          *subquery, /*in_testexpr=*/nullptr, /*hint_list=*/nullptr,
          array_type, zetasql::ResolvedSubqueryExpr::ARRAY,
          expr_transformer_info);
    }
    // Error cases to cover the remaining enum types. We handle each
    // individually to provide helpful error messages.
    case ALL_SUBLINK: {
      // (lefthand) op ALL (SELECT ...)
      const Query* subquery =
          PostgresConstCastNode(Query, pg_sublink.subselect);

      if (list_length(subquery->targetList) > 1) {
        return absl::InvalidArgumentError(
            "ALL subqueries must only have one output column");
      }

      // The OpExpr has the operator type, the lefthand expression, and a
      // parameter placeholder with the type of the subquery column.
      const OpExpr* testexpr =
          PostgresConstCastNode(OpExpr, pg_sublink.testexpr);
      ZETASQL_ASSIGN_OR_RETURN(
          const FormData_pg_operator* op,
          PgBootstrapCatalog::Default()->GetOperator(testexpr->opno));
      const char* opname = NameStr(op->oprname);
      if (strcmp(opname, "<>") != 0) {
        return absl::InvalidArgumentError(
            "ALL subqueries with operators other than <>/!= are not "
            "supported");
      }

      // We've confirmed that this is a subquery expression with a single column
      // and a not-equals operator. These expressions can be transformed to
      // ZetaSQL's equivalent of `NOT (x IN (select y....))`
      ZETASQL_ASSIGN_OR_RETURN(const zetasql::Type* output_type,
                       BuildGsqlType(BOOLOID));
      ZETASQL_RET_CHECK(list_length(testexpr->args) > 0);
      ZETASQL_ASSIGN_OR_RETURN(
          std::unique_ptr<zetasql::ResolvedSubqueryExpr> in_subquery_expr,
          BuildGsqlResolvedSubqueryExpr(
              *subquery, PostgresConstCastToExpr(pg_sublink.testexpr),
              pg_sublink.joinHints, output_type,
              zetasql::ResolvedSubqueryExpr::IN, expr_transformer_info));

      // Wrap the IN subquery in a NOT(...) function call.
      return BuildGsqlResolvedNotFunctionCall(std::move(in_subquery_expr));
    }
    case ANY_SUBLINK: {
      // (lefthand) op ANY/SOME (subquery) OR (lefthand) IN (subquery)
      const Query* subquery =
          PostgresConstCastNode(Query, pg_sublink.subselect);

      if (list_length(subquery->targetList) > 1) {
        return absl::InvalidArgumentError(
            "ANY/SOME/IN subqueries must only have one output column");
      }

      // The OpExpr has the operator type, the lefthand expression, and a
      // parameter placeholder with the type of the subquery column.
      const OpExpr* testexpr =
          PostgresConstCastNode(OpExpr, pg_sublink.testexpr);
      ZETASQL_ASSIGN_OR_RETURN(
          const FormData_pg_operator* op,
          PgBootstrapCatalog::Default()->GetOperator(testexpr->opno));
      const char* opname = NameStr(op->oprname);
      if (strcmp(opname, "=") != 0) {
        return absl::InvalidArgumentError(
            "ANY/SOME subqueries with non-equality operators are not "
            "supported");
      }

      // We've confirmed that this is a subquery expression with a single column
      // and an equality operator. These expressions can be transformed to
      // ZetaSQL's equivalent of `x IN (select y....)`
      ZETASQL_ASSIGN_OR_RETURN(const zetasql::Type* output_type,
                       BuildGsqlType(BOOLOID));
      ZETASQL_RET_CHECK(list_length(testexpr->args) > 0);
      return BuildGsqlResolvedSubqueryExpr(
          *subquery, PostgresConstCastToExpr(pg_sublink.testexpr),
          pg_sublink.joinHints, output_type,
          zetasql::ResolvedSubqueryExpr::IN, expr_transformer_info);
    }
    case ROWCOMPARE_SUBLINK: {
      // (lefthand) op (SELECT ...)
      return absl::UnimplementedError(
          "Row Comparison Subquery expressions are not supported");
    }
    case MULTIEXPR_SUBLINK: {
      // = (SELECT with multiple targetlist items ...)
      return absl::UnimplementedError(
          "Multi-valued Subquery Assignment expressions are not supported");
    }
    case CTE_SUBLINK: {
      // WITH query (never actually part of an expression). Since these can't be
      // expressions, it's invalid (not unsupported) in this context.
      return absl::InvalidArgumentError("Invalid use of WITH clause");
    }
    default: {
      // We've covered all the sublink types from primnodes.h, so this is a real
      // internal error (memory corruption). SubLinkTypeToString probably only
      // gives us the (invalid) int value of the enum here, but try it anyway.
      return absl::InternalError(
          absl::StrCat("Encountered unknown SubLink type: ",
                       internal::SubLinkTypeToString(pg_sublink.subLinkType)));
    }
  }
}

absl::StatusOr<std::vector<std::unique_ptr<const zetasql::ResolvedColumnRef>>>
ForwardTransformer::BuildGsqlCorrelatedSubqueryParameters(
    const CorrelatedColumnsSet& correlated_columns_set) {
  std::vector<std::unique_ptr<const zetasql::ResolvedColumnRef>> parameters;
  for (const auto& item : correlated_columns_set) {
    const zetasql::ResolvedColumn& column = item.first;
    const bool is_already_correlated = item.second;
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<zetasql::ResolvedColumnRef> column_ref,
                     BuildGsqlResolvedColumnRef(column, is_already_correlated));
    parameters.push_back(std::move(column_ref));
  }
  return std::move(parameters);
}

absl::StatusOr<int> ForwardTransformer::GetEndLocationForStartLocation(
    int start_location) {
  const absl::flat_hash_map<int, int>& token_locations =
      catalog_adapter_->token_locations();
  auto it = token_locations.find(start_location);
  if (it == token_locations.end()) {
    return absl::InternalError(absl::StrCat(
        "did not find end location for token start location ", start_location));
  } else {
    return it->second;
  }
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedColumnRef>>
ForwardTransformer::BuildGsqlResolvedColumnRef(
    const Var& var, const VarIndexScope& var_index_scope) {
  if (var.varattno == 0) {
    return absl::InvalidArgumentError("Whole-row selection is unsupported");
  }

  bool is_correlated = var.varlevelsup > 0;
  CorrelatedColumnsSetList correlated_columns_sets;
  ZETASQL_ASSIGN_OR_RETURN(
      zetasql::ResolvedColumn column,
      GetResolvedColumn(var_index_scope, var.varno, var.varattno,
                        var.varlevelsup, &correlated_columns_sets));
  ZETASQL_RET_CHECK_EQ(is_correlated, !correlated_columns_sets.empty());
  return BuildGsqlResolvedColumnRefWithCorrelation(column,
                                                   correlated_columns_sets);
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedColumnRef>>
ForwardTransformer::BuildGsqlResolvedColumnRefWithCorrelation(
    const zetasql::ResolvedColumn& column,
    const CorrelatedColumnsSetList& correlated_columns_sets,
    zetasql::ResolvedStatement::ObjectAccess access_flags) {
  bool is_correlated = false;
  if (!correlated_columns_sets.empty()) {
    is_correlated = true;
    for (CorrelatedColumnsSet* column_set : correlated_columns_sets) {
      // If we are referencing a variable correlated through more than one
      // level of subquery, the sets are ordered so that the set for
      // the outermost query is last.
      const bool is_already_correlated =
          (column_set != correlated_columns_sets.back());
      if (!zetasql_base::InsertIfNotPresent(column_set, column, is_already_correlated)) {
        // is_already_correlated should always be computed consistently.
        ABSL_DCHECK_EQ((*column_set)[column], is_already_correlated);
      }
    }
  }
  return BuildGsqlResolvedColumnRef(column, is_correlated, access_flags);
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedColumnRef>>
ForwardTransformer::BuildGsqlResolvedColumnRef(
    const zetasql::ResolvedColumn& column, bool is_correlated,
    zetasql::ResolvedStatement::ObjectAccess access_flags) {
  RecordColumnAccess(column, access_flags);
  return zetasql::MakeResolvedColumnRef(column.type(), column, is_correlated);
}

absl::StatusOr<std::vector<std::unique_ptr<const zetasql::ResolvedOption>>>
ForwardTransformer::BuildGsqlResolvedOptionList(
    const List& pg_hint_list, const VarIndexScope* var_index_scope) {
  std::vector<std::unique_ptr<const zetasql::ResolvedOption>> gsql_hint_list;
  for (DefElem* hint : StructList<DefElem*>(&pg_hint_list)) {
    ZETASQL_RET_CHECK_NE(hint, nullptr);
    const std::string name(hint->defname);
    const std::string qualifier =
        hint->defnamespace == nullptr ? "" : hint->defnamespace;

    ZETASQL_RET_CHECK_NE(hint->arg, nullptr);
    ZETASQL_RET_CHECK(internal::IsExpr(*hint->arg));
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<zetasql::ResolvedExpr> resolved_expr,
                     BuildGsqlResolvedScalarExpr(*PostgresCastToExpr(hint->arg),
                                                 var_index_scope, "hint"));

    gsql_hint_list.push_back(zetasql::MakeResolvedOption(
        std::move(qualifier), std::move(name), std::move(resolved_expr)));
  }

  return std::move(gsql_hint_list);
}

zetasql::IdString ForwardTransformer::GetColumnAliasForTopLevelExpression(
    ExprTransformerInfo* expr_transformer_info, const Expr* ast_expr) {
  const zetasql::IdString alias = expr_transformer_info->column_alias;
  if (expr_transformer_info->top_level_ast_expr == ast_expr) {
    return alias;
  }
  return zetasql::IdString();
}

}  // namespace postgres_translator
