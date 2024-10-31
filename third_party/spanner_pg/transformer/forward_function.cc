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

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/input_argument_type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/bootstrap_catalog/bootstrap_catalog.h"
#include "third_party/spanner_pg/catalog/catalog_adapter.h"
#include "third_party/spanner_pg/catalog/engine_system_catalog.h"
#include "third_party/spanner_pg/catalog/function.h"
#include "third_party/spanner_pg/catalog/function_identifier.h"
#include "third_party/spanner_pg/catalog/type.h"
#include "third_party/spanner_pg/interface/bootstrap_catalog_data.pb.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "third_party/spanner_pg/transformer/expr_transformer_helper.h"
#include "third_party/spanner_pg/transformer/forward_transformer.h"
#include "third_party/spanner_pg/transformer/transformer_helper.h"
#include "third_party/spanner_pg/util/pg_list_iterators.h"
#include "third_party/spanner_pg/util/postgres.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {

using ::postgres_translator::internal::PostgresCastToExpr;
using ::postgres_translator::internal::PostgresConstCastToExpr;

absl::StatusOr<std::vector<std::unique_ptr<zetasql::ResolvedExpr>>>
ForwardTransformer::BuildGsqlFunctionArgumentList(
    List* args, ExprTransformerInfo* expr_transformer_info) {
  std::vector<std::unique_ptr<zetasql::ResolvedExpr>> argument_list;
  argument_list.reserve(list_length(args));
  for (Expr* arg : StructList<Expr*>(args)) {
    std::unique_ptr<zetasql::ResolvedExpr> arg_expr;
    if (arg->type == T_TargetEntry) {
      // This is an aggregate function argument. Build the argument from
      // the inner expression.
      TargetEntry* target_entry_arg =
          internal::PostgresCastNode(TargetEntry, arg);
      ZETASQL_ASSIGN_OR_RETURN(arg_expr,
                       BuildGsqlResolvedExpr(*target_entry_arg->expr,
                                             expr_transformer_info));
    } else if (arg->type == T_NamedArgExpr) {
      return absl::InvalidArgumentError(
          "NamedArgExpr is not supported in this context");
    } else {
      ZETASQL_ASSIGN_OR_RETURN(arg_expr,
                       BuildGsqlResolvedExpr(*arg, expr_transformer_info));
    }
    argument_list.push_back(std::move(arg_expr));
  }
  return argument_list;
}

absl::StatusOr<std::vector<std::unique_ptr<zetasql::ResolvedExpr>>>
ForwardTransformer::BuildGsqlFunctionArgumentList(
    const PgProcData& proc_data, List* args,
    ExprTransformerInfo* expr_transformer_info) {
  std::vector<std::unique_ptr<zetasql::ResolvedExpr>> argument_list;

  // Named arguments can be out of order but the NamedArgExpr will have the
  // correct position. We need to build a map of the position to the argument
  // so we can build the argument list in the correct order.
  absl::flat_hash_map<size_t, std::unique_ptr<zetasql::ResolvedExpr>>
      arg_index_to_expr;
  size_t positional_index = 0;
  for (Expr* arg : StructList<Expr*>(args)) {
    if (arg->type == T_TargetEntry) {
      // This is an aggregate function argument. Build the argument from
      // the inner expression.
      TargetEntry* target_entry_arg =
          internal::PostgresCastNode(TargetEntry, arg);
      ZETASQL_ASSIGN_OR_RETURN(arg_index_to_expr[positional_index++],
                       BuildGsqlResolvedExpr(*target_entry_arg->expr,
                                             expr_transformer_info));
    } else if (arg->type == T_NamedArgExpr) {
      NamedArgExpr* named_arg_expr =
          internal::PostgresCastNode(NamedArgExpr, arg);
      ZETASQL_ASSIGN_OR_RETURN(
          arg_index_to_expr[named_arg_expr->argnumber],
          BuildGsqlResolvedExpr(*named_arg_expr->arg, expr_transformer_info));
    } else {
      ZETASQL_ASSIGN_OR_RETURN(arg_index_to_expr[positional_index++],
                       BuildGsqlResolvedExpr(*arg, expr_transformer_info));
    }
  }

  std::vector<std::string> arg_defaults;
  if (proc_data.has_proargdefaults()) {
    arg_defaults = absl::StrSplit(proc_data.proargdefaults(), ", ");
  }

  // Builds the argument list. Default values will be inserted if the number of
  // arguments is less than the number of arguments in the function signature.
  int total_arguments = proc_data.pronargs() > list_length(args) ?
      proc_data.pronargs() : list_length(args);
  argument_list.reserve(total_arguments);
  for (int i = 0; i < total_arguments; ++i) {
    if (!arg_index_to_expr.contains(i)) {
      if (!proc_data.has_proargdefaults()) {
        return absl::InvalidArgumentError(
            absl::StrCat("Function argument ", i, " is missing"));
      }
      const int8_t default_index =
          i - (proc_data.pronargs() - proc_data.pronargdefaults());
      ZETASQL_RET_CHECK_GE(default_index, 0);
      ZETASQL_RET_CHECK_LT(default_index, arg_defaults.size());
      const std::string& default_value = arg_defaults[default_index];
      const Oid arg_type = proc_data.proargtypes(i);
      ZETASQL_ASSIGN_OR_RETURN(arg_index_to_expr[i],
                       BuildGsqlResolvedLiteralForDefaultArgument(
                           arg_type, default_value));
    }
    argument_list.push_back(std::move(arg_index_to_expr[i]));
  }
  return argument_list;
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedLiteral>>
ForwardTransformer::BuildGsqlResolvedLiteralForDefaultArgument(
    Oid arg_type, const std::string& default_value) {
  const PostgresTypeMapping* type_mapping =
      catalog_adapter_->GetEngineSystemCatalog()->GetType(arg_type);
  if (type_mapping == nullptr) {
    return absl::InvalidArgumentError(
        absl::StrCat(
            "Function argument type with OID ", arg_type, " is not supported"));
  }

  // The default value is expected to be in the format of "literal::type".
  std::vector<std::string> default_value_parts =
      absl::StrSplit(default_value, "::");
  ZETASQL_RET_CHECK_EQ(default_value_parts.size(), 2);
  const std::string& default_literal = default_value_parts.front();
  const std::string& expected_type_name = default_value_parts.back();

  ZETASQL_ASSIGN_OR_RETURN(auto type_name,
                   type_mapping->PostgresExternalTypeName());
  if (type_name != expected_type_name) {
    return absl::InvalidArgumentError(
        absl::StrCat(
            "Function argument type ", type_name, " does not match default ",
            "value type of ", expected_type_name));
  }
  ZETASQL_ASSIGN_OR_RETURN(zetasql::Value gsql_value,
                   type_mapping->MakeGsqlValueFromStringConst(default_literal));
  return zetasql::MakeResolvedLiteral(gsql_value);
}

std::vector<zetasql::InputArgumentType>
ForwardTransformer::GetInputArgumentTypes(
    const std::vector<std::unique_ptr<zetasql::ResolvedExpr>>& argument_list)
    const {
  std::vector<zetasql::InputArgumentType> input_argument_types;
  input_argument_types.reserve(argument_list.size());
  for (const std::unique_ptr<zetasql::ResolvedExpr>& argument :
       argument_list) {
    input_argument_types.emplace_back(argument->type());
  }
  return input_argument_types;
}

static std::unique_ptr<zetasql::ResolvedFunctionCall>
MakeResolvedFunctionCall(
    FunctionAndSignature function_and_signature,
    std::vector<std::unique_ptr<zetasql::ResolvedExpr>> argument_list) {
  zetasql::ResolvedFunctionCallBase::ErrorMode error_mode =
      zetasql::ResolvedFunctionCallBase::DEFAULT_ERROR_MODE;
  return zetasql::MakeResolvedFunctionCall(
      function_and_signature.signature().result_type().type(),
      function_and_signature.function(), function_and_signature.signature(),
      std::move(argument_list), error_mode);
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedFunctionCall>>
ForwardTransformer::BuildGsqlResolvedFunctionCall(
    Oid funcid, List* args, ExprTransformerInfo* expr_transformer_info) {
  // Sanity check: if this is a UDF/TVF, it's not supported in this context.
  // Without this check, we will assume it's a builtin, fail to find it, and
  // return an internal error.
  auto tvf = catalog_adapter().GetTVFFromOid(funcid);
  if (tvf.ok()) {
    return absl::InvalidArgumentError(
        absl::StrCat("Function call ", tvf.value()->Name(),
                     " is unsupported in this context"));
  }

  // Check if there is a custom error message for this function.
  ZETASQL_RETURN_IF_ERROR(
      catalog_adapter_->GetEngineSystemCatalog()->GetCustomErrorForProc(
          funcid));

  std::vector<std::unique_ptr<zetasql::ResolvedExpr>> argument_list;

  // Lookup if the input funcid is a comparison (=, <=, <, etc). If so, handle
  // by transforming the input argument with GetResolvedExpForComparison.
  std::optional<Oid> mapped_funcid =
      catalog_adapter_->GetEngineSystemCatalog()
          ->GetMappedOidForComparisonFuncid(funcid);
  if (mapped_funcid.has_value()) {
    funcid = mapped_funcid.value();
    ZETASQL_ASSIGN_OR_RETURN(
        argument_list,
        BuildGsqlFunctionArgumentList(args, expr_transformer_info));
    for (std::unique_ptr<zetasql::ResolvedExpr>& arg : argument_list) {
      ZETASQL_ASSIGN_OR_RETURN(
          arg, catalog_adapter_->GetEngineSystemCatalog()
                  ->GetResolvedExprForComparison(
                      std::move(arg),
                      catalog_adapter_->analyzer_options().language()));
    }
  } else {
    // Build argument list with named and default argument support.
    ZETASQL_ASSIGN_OR_RETURN(auto proc_proto,
                    PgBootstrapCatalog::Default()->GetProcProto(funcid));
    ZETASQL_ASSIGN_OR_RETURN(argument_list, BuildGsqlFunctionArgumentList(
          *proc_proto, args, expr_transformer_info));
  }

  // Look up the function and signature.
  std::vector<zetasql::InputArgumentType> input_argument_types =
      GetInputArgumentTypes(argument_list);
  ZETASQL_ASSIGN_OR_RETURN(
      FunctionAndSignature function_and_signature,
      catalog_adapter_->GetEngineSystemCatalog()->GetFunctionAndSignature(
          funcid, input_argument_types,
          catalog_adapter_->analyzer_options().language()));

  return MakeResolvedFunctionCall(function_and_signature,
                                  std::move(argument_list));
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedFunctionCall>>
ForwardTransformer::BuildGsqlResolvedFunctionCall(
    const FuncExpr& func, ExprTransformerInfo* expr_transformer_info) {
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<zetasql::ResolvedFunctionCall> function_call,
      BuildGsqlResolvedFunctionCall(func.funcid, func.args,
                                    expr_transformer_info));
  if (func.functionHints) {
    ZETASQL_ASSIGN_OR_RETURN(
        std::vector<std::unique_ptr<const zetasql::ResolvedOption>> hint_list,
        BuildGsqlResolvedOptionList(*func.functionHints, /*scope=*/nullptr));
    function_call->set_hint_list(std::move(hint_list));
  }
  return function_call;
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedFunctionCall>>
ForwardTransformer::BuildGsqlResolvedFunctionCall(
    const OpExpr& op, ExprTransformerInfo* expr_transformer_info) {
  return BuildGsqlResolvedFunctionCall(op.opfuncid, op.args,
                                       expr_transformer_info);
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedFunctionCall>>
ForwardTransformer::BuildGsqlSubstrFunctionCall(
    std::unique_ptr<zetasql::ResolvedExpr> value, int length) {
  // Get the PostgreSQL oid for substr.
  constexpr absl::string_view pg_catalog_name = "pg_catalog";
  constexpr absl::string_view function_name = "substr";
  std::vector<Oid> function_input_types = {TEXTOID, INT8OID, INT8OID};
  ZETASQL_ASSIGN_OR_RETURN(Oid substring_oid,
                   PgBootstrapCatalog::Default()->GetProcOid(
                       pg_catalog_name, function_name, function_input_types));

  // Build the argument list for the function call substr(value, 0, length);
  Const* position_const;
  ZETASQL_ASSIGN_OR_RETURN(position_const,
                   internal::makeScalarConst(INT8OID, Int64GetDatum(0),
                                             /*constisnull=*/false));
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<zetasql::ResolvedLiteral> position_literal,
                   BuildGsqlResolvedLiteral(*position_const));
  Const* length_const;
  ZETASQL_ASSIGN_OR_RETURN(length_const,
                   internal::makeScalarConst(INT8OID, Int64GetDatum(length),
                                             /*constisnull=*/false));
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<zetasql::ResolvedLiteral> length_literal,
                   BuildGsqlResolvedLiteral(*length_const));
  std::vector<std::unique_ptr<zetasql::ResolvedExpr>> argument_list;
  argument_list.reserve(3);
  argument_list.push_back(std::move(value));
  argument_list.push_back(std::move(position_literal));
  argument_list.push_back(std::move(length_literal));

  // Look up the function and signature.
  std::vector<zetasql::InputArgumentType> input_argument_types =
      GetInputArgumentTypes(argument_list);
  ZETASQL_ASSIGN_OR_RETURN(
      FunctionAndSignature function_and_signature,
      catalog_adapter_->GetEngineSystemCatalog()->GetFunctionAndSignature(
          substring_oid, input_argument_types,
          catalog_adapter_->analyzer_options().language()));

  return MakeResolvedFunctionCall(function_and_signature,
                                  std::move(argument_list));
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
ForwardTransformer::BuildGsqlResolvedAggregateFunctionCall(
    const Aggref& agg_function, ExprTransformerInfo* expr_transformer_info) {
  if (!expr_transformer_info->allows_aggregation) {
    return absl::InvalidArgumentError(
        absl::StrCat("Aggregate functions are not supported in ",
                     expr_transformer_info->clause_name));
  }

  ZETASQL_RETURN_IF_ERROR(CheckForUnsupportedFields(agg_function.aggorder,
                                            "Aggregate functions", "ORDER BY"));
  ZETASQL_RETURN_IF_ERROR(CheckForUnsupportedFields(
      agg_function.aggfilter, "Aggregate functions", "FILTER clauses"));

  // Construct a local ExprTransformerInfo whose var_index_scope is the
  // aggregate var_index_scope so that the aggregate function arguments will
  // be transformed against the correct scope.
  ExprTransformerInfo local_expr_transformer_info(
      expr_transformer_info, expr_transformer_info->aggregate_var_index_scope,
      expr_transformer_info->clause_name);
  // When resolving arguments of aggregation functions, we resolve
  // against pre-grouped versions of columns only.
  local_expr_transformer_info.use_post_grouping_columns = false;

  Oid funcid = agg_function.aggfnoid;

  // Transform the input arguments.
  ZETASQL_ASSIGN_OR_RETURN(
      std::vector<std::unique_ptr<zetasql::ResolvedExpr>> argument_list,
      BuildGsqlFunctionArgumentList(agg_function.args,
                                    &local_expr_transformer_info));

  // Look up the function and signature.
  std::vector<zetasql::InputArgumentType> input_argument_types =
      GetInputArgumentTypes(argument_list);
  ZETASQL_ASSIGN_OR_RETURN(
      FunctionAndSignature function_and_signature,
      catalog_adapter_->GetEngineSystemCatalog()->GetFunctionAndSignature(
          funcid, input_argument_types,
          catalog_adapter_->analyzer_options().language()));

  // Set aggregation to true.
  local_expr_transformer_info.has_aggregation = true;

  // Construct the aggregate function call.
  zetasql::ResolvedFunctionCallBase::ErrorMode error_mode =
      zetasql::ResolvedFunctionCallBase::DEFAULT_ERROR_MODE;
  zetasql::ResolvedNonScalarFunctionCallBase::NullHandlingModifier
      null_handling_modifier =
          zetasql::ResolvedNonScalarFunctionCallBase::DEFAULT_NULL_HANDLING;
  std::vector<std::unique_ptr<const zetasql::ResolvedOrderByItem>>
      order_by_item_list;
  auto resolved_function_call = zetasql::MakeResolvedAggregateFunctionCall(
      function_and_signature.signature().result_type().type(),
      function_and_signature.function(), function_and_signature.signature(),
      std::move(argument_list), error_mode,
      /*distinct=*/agg_function.aggdistinct != nullptr, null_handling_modifier,
      /*having_modifier=*/nullptr, std::move(order_by_item_list),
      /*limit=*/nullptr);
  if (agg_function.functionHints) {
    ZETASQL_ASSIGN_OR_RETURN(
        std::vector<std::unique_ptr<const zetasql::ResolvedOption>> hint_list,
        BuildGsqlResolvedOptionList(*agg_function.functionHints,
                                    /*scope=*/nullptr));
    resolved_function_call->set_hint_list(std::move(hint_list));
  }

  // Track the computed column in TransformerInfo.
  // Modeled after ZetaSQL's FinishResolvingAggregateFunction.
  // If this aggregate function call is the top level function call in
  // expr_transformer_info and it has an alias, then use that alias.
  // Otherwise create an internal alias for this expression.
  zetasql::IdString alias = GetColumnAliasForTopLevelExpression(
      expr_transformer_info, PostgresConstCastToExpr(&agg_function));
  if (alias.empty()) {
    alias = catalog_adapter_->analyzer_options().id_string_pool()->Make(
        absl::StrCat("$agg", expr_transformer_info->transformer_info
                                     ->aggregate_columns_to_compute()
                                     .size() +
                                 1));
  }
  return BuildGsqlAggregateColumnRef(alias, &agg_function,
                                     std::move(resolved_function_call),
                                     expr_transformer_info->transformer_info);
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedFunctionCall>>
ForwardTransformer::BuildGsqlResolvedFunctionCall(
    NodeTag expr_node_tag, List* args,
    ExprTransformerInfo* expr_transformer_info) {
  // Transform the input arguments.
  ZETASQL_ASSIGN_OR_RETURN(
      std::vector<std::unique_ptr<zetasql::ResolvedExpr>> argument_list,
      BuildGsqlFunctionArgumentList(args, expr_transformer_info));

  // Look up the function and signature.
  std::vector<zetasql::InputArgumentType> input_argument_types =
      GetInputArgumentTypes(argument_list);
  ZETASQL_ASSIGN_OR_RETURN(
      FunctionAndSignature function_and_signature,
      catalog_adapter_->GetEngineSystemCatalog()->GetFunctionAndSignature(
          PostgresExprIdentifier::Expr(expr_node_tag), input_argument_types,
          catalog_adapter_->analyzer_options().language()));

  return MakeResolvedFunctionCall(function_and_signature,
                                  std::move(argument_list));
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedFunctionCall>>
ForwardTransformer::BuildGsqlResolvedCaseFunctionCall(
    const CaseExpr& case_expr, ExprTransformerInfo* expr_transformer_info) {
  if (case_expr.arg != nullptr) {
    return BuildGsqlResolvedCaseWithValueFunctionCall(case_expr,
                                                      expr_transformer_info);
  } else {
    return BuildGsqlResolvedCaseNoValueFunctionCall(case_expr,
                                                    expr_transformer_info);
  }
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedFunctionCall>>
ForwardTransformer::BuildGsqlResolvedCaseNoValueFunctionCall(
    const CaseExpr& case_expr, ExprTransformerInfo* expr_transformer_info) {
  // Check that the CaseExpr arg is null.
  ZETASQL_RET_CHECK(case_expr.arg == nullptr);

  // Build the argument list here because each CaseWhen expr expands to two
  // ZetaSQL input arguments.
  std::vector<std::unique_ptr<zetasql::ResolvedExpr>> argument_list;

  // Transform the WHEN expression and then THEN expression.
  for (CaseWhen* case_when : StructList<CaseWhen*>(case_expr.args)) {
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<zetasql::ResolvedExpr> when_expr,
        BuildGsqlResolvedExpr(*case_when->expr, expr_transformer_info));
    argument_list.push_back(std::move(when_expr));
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<zetasql::ResolvedExpr> result_expr,
        BuildGsqlResolvedExpr(*case_when->result, expr_transformer_info));
    argument_list.push_back(std::move(result_expr));
  }
  // Add the ELSE clause as an input argument.
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<zetasql::ResolvedExpr> expr,
      BuildGsqlResolvedExpr(*case_expr.defresult, expr_transformer_info));
  argument_list.push_back(std::move(expr));

  // Look up the function and signature.
  std::vector<zetasql::InputArgumentType> input_argument_types =
      GetInputArgumentTypes(argument_list);
  ZETASQL_ASSIGN_OR_RETURN(
      FunctionAndSignature function_and_signature,
      catalog_adapter_->GetEngineSystemCatalog()->GetFunctionAndSignature(
          PostgresExprIdentifier::CaseExpr(/*case_has_testexpr=*/false),
          input_argument_types,
          catalog_adapter_->analyzer_options().language()));

  return MakeResolvedFunctionCall(function_and_signature,
                                  std::move(argument_list));
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedFunctionCall>>
ForwardTransformer::BuildGsqlResolvedCaseWithValueFunctionCall(
    const CaseExpr& case_expr, ExprTransformerInfo* expr_transformer_info) {
  // Check that the CaseExpr arg is not null.
  ZETASQL_RET_CHECK(case_expr.arg != nullptr);

  // Building an argument list.
  std::vector<std::unique_ptr<zetasql::ResolvedExpr>> argument_list;

  // First item in the argument list is the CASE expression arg.
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<zetasql::ResolvedExpr> arg_expr,
      BuildGsqlResolvedExpr(*case_expr.arg, expr_transformer_info));
  const zetasql::Type* value_type = arg_expr->type();
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<zetasql::ResolvedExpr> mapped_arg_expr,
      catalog_adapter_->GetEngineSystemCatalog()->GetResolvedExprForComparison(
          std::move(arg_expr),
          catalog_adapter_->analyzer_options().language()));
  argument_list.push_back(std::move(mapped_arg_expr));
  // The following arguments are pairs of (WHEN ... THEN) values
  for (CaseWhen* case_when : StructList<CaseWhen*>(case_expr.args)) {
    ZETASQL_RET_CHECK(IsA(case_when->expr, OpExpr));
    OpExpr* case_op_expr = internal::PostgresCastNode(OpExpr, case_when->expr);

    // PostgreSQL represents the WHEN clause as an OpExpr that is the equivalent
    // of `<value> = <when_arg>`. ZetaSQL includes <when_arg> in the function
    // parameter list, but does not repeat <value> and it does not have the '='
    // operator. Extract <when_arg> from the PostgreSQL OpExpr, transform it,
    // and add it to the argument list.
    Expr* when_arg = PostgresCastToExpr(lsecond(case_op_expr->args));

    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<zetasql::ResolvedExpr> expr_arg,
                     BuildGsqlResolvedExpr(*when_arg, expr_transformer_info));
    if (expr_arg->type() != value_type) {
      return absl::InvalidArgumentError(
          "CASE expression and WHEN clauses must have the same type");
    }
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<zetasql::ResolvedExpr> mapped_expr_arg,
                     catalog_adapter_->GetEngineSystemCatalog()
                         ->GetResolvedExprForComparison(
                             std::move(expr_arg),
                             catalog_adapter_->analyzer_options().language()));
    argument_list.push_back(std::move(mapped_expr_arg));

    // Transform the THEN value and add it to the argument list.
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<zetasql::ResolvedExpr> result_expr,
        BuildGsqlResolvedExpr(*case_when->result, expr_transformer_info));
    argument_list.push_back(std::move(result_expr));
  }
  // Add the ELSE clause as an input argument.
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<zetasql::ResolvedExpr> expr,
      BuildGsqlResolvedExpr(*case_expr.defresult, expr_transformer_info));
  argument_list.push_back(std::move(expr));

  // Look up the function and signature.
  std::vector<zetasql::InputArgumentType> input_argument_types =
      GetInputArgumentTypes(argument_list);
  ZETASQL_ASSIGN_OR_RETURN(
      FunctionAndSignature function_and_signature,
      catalog_adapter_->GetEngineSystemCatalog()->GetFunctionAndSignature(
          PostgresExprIdentifier::CaseExpr(/*case_has_testexpr=*/true),
          input_argument_types,
          catalog_adapter_->analyzer_options().language()));

  return MakeResolvedFunctionCall(function_and_signature,
                                  std::move(argument_list));
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedFunctionCall>>
ForwardTransformer::BuildGsqlResolvedBoolFunctionCall(
    const BoolExpr& bool_expr, ExprTransformerInfo* expr_transformer_info) {
  // Transform the input arguments.
  ZETASQL_ASSIGN_OR_RETURN(
      std::vector<std::unique_ptr<zetasql::ResolvedExpr>> argument_list,
      BuildGsqlFunctionArgumentList(bool_expr.args, expr_transformer_info));

  // Look up the function and signature.
  std::vector<zetasql::InputArgumentType> input_argument_types =
      GetInputArgumentTypes(argument_list);
  ZETASQL_ASSIGN_OR_RETURN(
      FunctionAndSignature function_and_signature,
      catalog_adapter_->GetEngineSystemCatalog()->GetFunctionAndSignature(
          PostgresExprIdentifier::BoolExpr(bool_expr.boolop),
          input_argument_types,
          catalog_adapter_->analyzer_options().language()));

  return MakeResolvedFunctionCall(function_and_signature,
                                  std::move(argument_list));
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedFunctionCall>>
ForwardTransformer::BuildGsqlResolvedNotFunctionCall(
    std::unique_ptr<zetasql::ResolvedExpr> arg) {
  std::vector<std::unique_ptr<zetasql::ResolvedExpr>> argument_list;
  argument_list.push_back(std::move(arg));

  // Look up the function and signature for NOT.
  std::vector<zetasql::InputArgumentType> input_argument_types =
      GetInputArgumentTypes(argument_list);
  ZETASQL_ASSIGN_OR_RETURN(
      FunctionAndSignature function_and_signature,
      catalog_adapter_->GetEngineSystemCatalog()->GetFunctionAndSignature(
          PostgresExprIdentifier::BoolExpr(NOT_EXPR), input_argument_types,
          catalog_adapter_->analyzer_options().language()));

  return MakeResolvedFunctionCall(function_and_signature,
                                  std::move(argument_list));
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedFunctionCall>>
ForwardTransformer::BuildGsqlResolvedGreatestLeastFunctionCall(
    const MinMaxExpr& min_max_expr,
    ExprTransformerInfo* expr_transformer_info) {
  // Transform the input arguments.
  ZETASQL_ASSIGN_OR_RETURN(
      std::vector<std::unique_ptr<zetasql::ResolvedExpr>> argument_list,
      BuildGsqlFunctionArgumentList(min_max_expr.args, expr_transformer_info));

  // Look up the function and signature.
  std::vector<zetasql::InputArgumentType> input_argument_types =
      GetInputArgumentTypes(argument_list);
  ZETASQL_ASSIGN_OR_RETURN(
      FunctionAndSignature function_and_signature,
      catalog_adapter_->GetEngineSystemCatalog()->GetFunctionAndSignature(
          PostgresExprIdentifier::MinMaxExpr(min_max_expr.op),
          input_argument_types,
          catalog_adapter_->analyzer_options().language()));

  return MakeResolvedFunctionCall(function_and_signature,
                                  std::move(argument_list));
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedFunctionCall>>
ForwardTransformer::BuildGsqlResolvedNullTestFunctionCall(
    const NullTest& null_test, ExprTransformerInfo* expr_transformer_info) {
  if (null_test.argisrow) {
    return absl::UnimplementedError(
        "Field-by-field full row null checks are not supported.");
  }

  // If IS_NOT_NULL(value), rewrite the query to NOT(IS_NULL(value)).
  if (null_test.nulltesttype == IS_NOT_NULL) {
    NullTest* is_null_test;
    ZETASQL_ASSIGN_OR_RETURN(is_null_test,
                     internal::makeNullTest(null_test.arg, IS_NULL, false, -1));
    List* input_args;
    ZETASQL_ASSIGN_OR_RETURN(input_args,
                     CheckedPgListMake1(PostgresCastToExpr(is_null_test)));
    Expr* bool_expr = makeBoolExpr(NOT_EXPR, input_args,
                                   /*location=*/-1);

    // BuildGsqlResolvedBoolFunction will call out to BuildGsqlResolvedExpr
    // for the 1 arg, which will increment the tree depth for the AST.
    // The downside of directly calling to BuildGsqlResolvedBoolFunctionCall
    // is that if later checks are added into BuildGsqlResolvedExpr that are
    // expected to run on every node, we will miss those checks here.
    return BuildGsqlResolvedBoolFunctionCall(
        *internal::PostgresCastNode(BoolExpr, bool_expr),
        expr_transformer_info);
  }

  // Transform the input argument.
  std::vector<std::unique_ptr<zetasql::ResolvedExpr>> argument_list;
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<zetasql::ResolvedExpr> expr,
      BuildGsqlResolvedExpr(*null_test.arg, expr_transformer_info));
  argument_list.push_back(std::move(expr));

  // Look up the function and signature.
  std::vector<zetasql::InputArgumentType> input_argument_types =
      GetInputArgumentTypes(argument_list);
  ZETASQL_ASSIGN_OR_RETURN(
      FunctionAndSignature function_and_signature,
      catalog_adapter_->GetEngineSystemCatalog()->GetFunctionAndSignature(
          PostgresExprIdentifier::NullTest(null_test.nulltesttype),
          input_argument_types,
          catalog_adapter_->analyzer_options().language()));

  return MakeResolvedFunctionCall(function_and_signature,
                                  std::move(argument_list));
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
ForwardTransformer::BuildGsqlResolvedBooleanTestExpr(
    const BooleanTest& boolean_test,
    ExprTransformerInfo* expr_transformer_info) {
  switch (boolean_test.booltesttype) {
    case IS_UNKNOWN:
    case IS_NOT_UNKNOWN:
      // Unknown is identical to NULL, so delegate to the null test handler for
      // these cases.
      {
        NullTest* null_test;
        ZETASQL_ASSIGN_OR_RETURN(
            null_test,
            internal::makeNullTest(
                boolean_test.arg,
                boolean_test.booltesttype == IS_UNKNOWN ? IS_NULL : IS_NOT_NULL,
                false, boolean_test.location));
        return BuildGsqlResolvedNullTestFunctionCall(*null_test,
                                                     expr_transformer_info);
      }

    case IS_NOT_TRUE:
    case IS_NOT_FALSE:
      // X IS NOT Y is the same as NOT (X IS Y) so rewrite the expression and
      // dispatch to the boolan expression handler. This'll make it back to this
      // function, but that's OK.
      {
        BooleanTest* subtest;
        ZETASQL_ASSIGN_OR_RETURN(
            subtest,
            internal::makeBooleanTest(
                boolean_test.arg,
                (boolean_test.booltesttype == IS_NOT_TRUE) ? IS_TRUE : IS_FALSE,
                boolean_test.location));

        // Build up the fake expression node
        List* input_args;
        ZETASQL_ASSIGN_OR_RETURN(input_args, CheckedPgListMake1(subtest));
        BoolExpr* bool_expr = internal::PostgresCastNode(
            BoolExpr,
            makeBoolExpr(NOT_EXPR, input_args, boolean_test.location));

        return BuildGsqlResolvedBoolFunctionCall(*bool_expr,
                                                 expr_transformer_info);
        break;
      }
    case IS_TRUE:
    case IS_FALSE: {
      // Evaluate the input argument.
      std::vector<std::unique_ptr<zetasql::ResolvedExpr>> argument_list;
      ZETASQL_ASSIGN_OR_RETURN(
          std::unique_ptr<zetasql::ResolvedExpr> expr,
          BuildGsqlResolvedExpr(*boolean_test.arg, expr_transformer_info));
      argument_list.push_back(std::move(expr));
      // Look up the function and signature.
      std::vector<zetasql::InputArgumentType> input_argument_types =
          GetInputArgumentTypes(argument_list);
      ZETASQL_ASSIGN_OR_RETURN(
          FunctionAndSignature function_and_signature,
          catalog_adapter_->GetEngineSystemCatalog()->GetFunctionAndSignature(
              PostgresExprIdentifier::BooleanTest(boolean_test.booltesttype),
              input_argument_types,
              catalog_adapter_->analyzer_options().language()));
      return MakeResolvedFunctionCall(function_and_signature,
                                      std::move(argument_list));
    }
  }

  return absl::UnimplementedError(
      absl::StrCat("BooleanTest not implemented for type ",
                   std::to_string(boolean_test.booltesttype)));
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedFunctionCall>>
ForwardTransformer::BuildGsqlResolvedScalarArrayFunctionCall(
    const ScalarArrayOpExpr& scalar_array,
    ExprTransformerInfo* expr_transformer_info) {
  // Get the form data for the opno then set the comparator_type variable.
  ZETASQL_ASSIGN_OR_RETURN(
      const FormData_pg_operator* opno_data,
      PgBootstrapCatalog::Default()->GetOperator(scalar_array.opno));

  std::string comparator_type = (NameStr(opno_data->oprname));
  bool useOr = scalar_array.useOr;

  if (useOr && comparator_type != "=") {
    return absl::UnimplementedError(
        absl::StrCat("ANY/SOME expression is not supported with ",
                     comparator_type, " operator."));
  }
  if (useOr == false && comparator_type != "<>") {
    return BuildGsqlAllFunctionCall(scalar_array, expr_transformer_info);
  }

  // IN functions always have a useOr value of true, while NOT IN always has a
  // useOr value of false. Check that this is the case.
  if (comparator_type == "=") {
    ZETASQL_RET_CHECK(useOr);
    return BuildGsqlInFunctionCall(scalar_array, expr_transformer_info);
  } else {
    // The else case should always be NOT IN
    ZETASQL_RET_CHECK(useOr == false);

    // Use the oprleft and oprright to make a new scalar array
    // (not_scalar_array) that has a useOr=true and the correct opno and
    // opfuncids for value types with a "=" comparator_type.
    ZETASQL_ASSIGN_OR_RETURN(
        const Oid not_opno,
        PgBootstrapCatalog::Default()->GetOperatorOidByOprLeftRight(
            "=", opno_data->oprleft, opno_data->oprright));

    ZETASQL_ASSIGN_OR_RETURN(const FormData_pg_operator* not_opno_data,
                     PgBootstrapCatalog::Default()->GetOperator(not_opno));

    ScalarArrayOpExpr* not_scalar_array;
    ZETASQL_ASSIGN_OR_RETURN(not_scalar_array,
                     internal::makeScalarArrayOpExpr(
                         /*args=*/scalar_array.args,
                         /*opno=*/not_opno,
                         /*opfuncid=*/not_opno_data->oprcode));
    // Wrap the not_scalar_array with a BoolExpr (Not Expr).
    List* bool_args;
    ZETASQL_ASSIGN_OR_RETURN(bool_args,
                     CheckedPgListMake1(PostgresCastToExpr(not_scalar_array)));
    Expr* bool_expr = makeBoolExpr(NOT_EXPR, bool_args,
                                   /*location=*/-1);

    return BuildGsqlResolvedBoolFunctionCall(
        *internal::PostgresCastNode(BoolExpr, bool_expr),
        expr_transformer_info);
  }
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedFunctionCall>>
ForwardTransformer::BuildGsqlInFunctionCall(
    const ScalarArrayOpExpr& scalar_array,
    ExprTransformerInfo* expr_transformer_info) {
  // ANY/SOME/IN always have a useOr value of true.
  ZETASQL_RET_CHECK(scalar_array.useOr == true);
  void* array_argument = lsecond(scalar_array.args);

  if (IsA(array_argument, ArrayCoerceExpr)) {
    // In some cases, PostgreSQL may supply an ArrayCoerceExpr as the array
    // argument to request runtime casting of the array elements to match the
    // first argument. This is not supported, but we special case it for a good
    // error message.
    return absl::InvalidArgumentError(
        "ANY, SOME, and IN expressions requiring array casting are not "
        "supported. Consider rewriting as an explicit JOIN");
  } else if (IsA(array_argument, Const)) {
    return absl::InvalidArgumentError(
        "ANY, SOME, and IN expressions with array literal arguments are not "
        "supported. Try using an array constructor (ARRAY[])");
  } else if (IsA(array_argument, SubLink)) {
    // UnimplementedError to match the equivalent error in
    // BuildGsqlResolvedSubqueryExpr
    return absl::UnimplementedError(
        "ANY, SOME, and IN expressions with a subquery on the right hand "
        "side are not supported");
  } else if (IsA(array_argument, FuncExpr) || IsA(array_argument, OpExpr)) {
    // OpExpr and FuncExpr are semantically equivalent, so we'll handle both of
    // them together.
    return absl::InvalidArgumentError(
        "ANY, SOME, and IN expressions with function or operator arguments are "
        "not supported");
  }

  // Build the argument list for the googlesql function. The argument list is
  // a vector that consists of the scalar expression and each expression from
  // the array.
  std::vector<std::unique_ptr<zetasql::ResolvedExpr>> argument_list;

  // Get the scalar expression and add it to the argument list.
  Expr* scalar_node = PostgresCastToExpr(linitial(scalar_array.args));
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<zetasql::ResolvedExpr> scalar_arg,
                   BuildGsqlResolvedExpr(*scalar_node, expr_transformer_info));
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<zetasql::ResolvedExpr> mapped_scalar_arg,
      catalog_adapter_->GetEngineSystemCatalog()->GetResolvedExprForComparison(
          std::move(scalar_arg),
          catalog_adapter_->analyzer_options().language()));
  argument_list.push_back(std::move(mapped_scalar_arg));

  ZETASQL_ASSIGN_OR_RETURN(bool array_op_arg_is_array,
                   AppendGsqlInFunctionCallArrayArg(
                       array_argument, expr_transformer_info, argument_list));
  ZETASQL_RET_CHECK(argument_list.size() > 1);

  // TODO: b/322384712 - Remove the following two if statements after
  // implementing PG NaN comparison for double precision[] and float4[] columns.
  if (array_op_arg_is_array &&
      argument_list.at(1)->type()->AsArray()->element_type()->IsFloat()) {
    return absl::InvalidArgumentError(
        "ANY/SOME expressions with float4[] column references are "
        "not supported.");
  }
  if (array_op_arg_is_array &&
      argument_list.at(1)->type()->AsArray()->element_type()->IsDouble()) {
    return absl::InvalidArgumentError(
        "ANY/SOME expressions with double precision[] column references are "
        "not supported.");
  }
  std::vector<zetasql::InputArgumentType> input_argument_types =
      GetInputArgumentTypes(argument_list);

  ZETASQL_ASSIGN_OR_RETURN(
      const FormData_pg_operator* opno_data,
      PgBootstrapCatalog::Default()->GetOperator(scalar_array.opno));
  std::string comparator_type = (NameStr(opno_data->oprname));
  ZETASQL_RET_CHECK(comparator_type == "=");
  ZETASQL_ASSIGN_OR_RETURN(
      FunctionAndSignature function_and_signature,
      catalog_adapter_->GetEngineSystemCatalog()->GetFunctionAndSignature(
          PostgresExprIdentifier::ScalarArrayOpExpr(
              array_op_arg_is_array, scalar_array.useOr, comparator_type),
          input_argument_types,
          catalog_adapter_->analyzer_options().language()));

  return MakeResolvedFunctionCall(function_and_signature,
                                  std::move(argument_list));
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedFunctionCall>>
ForwardTransformer::BuildGsqlAllFunctionCall(
    const ScalarArrayOpExpr& scalar_array,
    ExprTransformerInfo* expr_transformer_info) {
  // ALL always have a useOr value of false.
  ZETASQL_RET_CHECK(scalar_array.useOr == false);
  void* array_argument = lsecond(scalar_array.args);

  if (IsA(array_argument, ArrayCoerceExpr)) {
    // In some cases, PostgreSQL may supply an ArrayCoerceExpr as the array
    // argument to request runtime casting of the array elements to match the
    // first argument. This is not supported, but we special case it for a good
    // error message.
    return absl::InvalidArgumentError(
        "ALL expressions requiring array casting are not supported. Consider "
        "rewriting as an explicit JOIN");
  } else if (IsA(array_argument, Const)) {
    return absl::InvalidArgumentError(
        "ALL expressions with array literal arguments are not supported. "
        "Try using an array constructor (ARRAY[])");
  } else if (IsA(array_argument, SubLink)) {
    // UnimplementedError to match the equivalent error in
    // BuildGsqlResolvedSubqueryExpr
    return absl::UnimplementedError(
        "ALL expressions with a subquery on the right hand side are not "
        "supported");
  } else if (IsA(array_argument, FuncExpr) || IsA(array_argument, OpExpr)) {
    // OpExpr and FuncExpr are semantically equivalent, so we'll handle both of
    // them together.
    return absl::InvalidArgumentError(
        "ALL expressions with function or operator arguments are not "
        "supported");
  }

  // Build the argument list for the googlesql function. The argument list is
  // a vector that consists of the scalar expression and each expression from
  // the array.
  std::vector<std::unique_ptr<zetasql::ResolvedExpr>> argument_list;

  // Get the scalar expression and add it to the argument list.
  Expr* scalar_node = PostgresCastToExpr(linitial(scalar_array.args));
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<zetasql::ResolvedExpr> scalar_arg,
                   BuildGsqlResolvedExpr(*scalar_node, expr_transformer_info));
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<zetasql::ResolvedExpr> mapped_scalar_arg,
      catalog_adapter_->GetEngineSystemCatalog()->GetResolvedExprForComparison(
          std::move(scalar_arg),
          catalog_adapter_->analyzer_options().language()));
  argument_list.push_back(std::move(mapped_scalar_arg));

  ZETASQL_RETURN_IF_ERROR(AppendGsqlAllFunctionCallArrayArg(
      array_argument, expr_transformer_info, argument_list));
  ZETASQL_RET_CHECK(argument_list.size() == 2);

  if (argument_list.at(1)->type()->AsArray()->element_type()->IsFloat()) {
    return absl::InvalidArgumentError(
        "ALL expressions with float4[] column references are not supported.");
  }
  if (argument_list.at(1)->type()->AsArray()->element_type()->IsDouble()) {
    return absl::InvalidArgumentError(
        "ALL expressions with double precision[] column references are "
        "not supported.");
  }
  std::vector<zetasql::InputArgumentType> input_argument_types =
      GetInputArgumentTypes(argument_list);

  ZETASQL_ASSIGN_OR_RETURN(
      const FormData_pg_operator* opno_data,
      PgBootstrapCatalog::Default()->GetOperator(scalar_array.opno));
  std::string comparator_type = (NameStr(opno_data->oprname));
  ZETASQL_ASSIGN_OR_RETURN(
      FunctionAndSignature function_and_signature,
      catalog_adapter_->GetEngineSystemCatalog()->GetFunctionAndSignature(
          PostgresExprIdentifier::ScalarArrayOpExpr(
              /*array_op_arg_is_array=*/true, scalar_array.useOr,
              comparator_type),
          input_argument_types,
          catalog_adapter_->analyzer_options().language()));

  return MakeResolvedFunctionCall(function_and_signature,
                                  std::move(argument_list));
}

absl::Status ForwardTransformer::AppendGsqlAllFunctionCallArrayArg(
    void* array_argument, ExprTransformerInfo* expr_transformer_info,
    std::vector<std::unique_ptr<zetasql::ResolvedExpr>>& argument_list) {
  ZETASQL_RET_CHECK_EQ(argument_list.size(), 1)
      << "The scalar argument should be added to argument_list before the "
         "array arguments";
  if (IsA(array_argument, Var)) {
    // ALL expressions with column arguments.
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<zetasql::ResolvedExpr> column_ref,
                     BuildGsqlResolvedColumnRef(
                         *internal::PostgresConstCastNode(Var, array_argument),
                         *expr_transformer_info->var_index_scope));
    ZETASQL_RET_CHECK(column_ref->type()->IsArray());
    argument_list.push_back(std::move(column_ref));
  } else if (IsA(array_argument, Param)) {
    // ALL expressions with parameter arguments.
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<zetasql::ResolvedParameter> param_ref,
        BuildGsqlResolvedParameter(
            *internal::PostgresConstCastNode(Param, array_argument)));
    ZETASQL_RET_CHECK(param_ref->type()->IsArray());
    argument_list.push_back(std::move(param_ref));
  } else {
    ZETASQL_RET_CHECK(IsA(array_argument, ArrayExpr));
    ArrayExpr* array_node =
        internal::PostgresCastNode(ArrayExpr, array_argument);
    if (array_node->multidims) {
      return absl::InvalidArgumentError(
          "Multi-dimensional arrays are not supported.");
    }
    std::vector<std::unique_ptr<zetasql::ResolvedExpr>> array_elements;
    for (Expr* array_element : StructList<Expr*>(array_node->elements)) {
      ZETASQL_ASSIGN_OR_RETURN(
          std::unique_ptr<zetasql::ResolvedExpr> expr_arg,
          BuildGsqlResolvedExpr(*array_element, expr_transformer_info));
      ZETASQL_ASSIGN_OR_RETURN(
          std::unique_ptr<zetasql::ResolvedExpr> mapped_expr_arg,
          catalog_adapter_->GetEngineSystemCatalog()
              ->GetResolvedExprForComparison(
                  std::move(expr_arg),
                  catalog_adapter_->analyzer_options().language()));
      array_elements.push_back(std::move(mapped_expr_arg));
    }
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<zetasql::ResolvedExpr> array_arg,
                     BuildGsqlResolvedExpr(*array_node, expr_transformer_info));
    argument_list.push_back(std::move(array_arg));
  }
  return absl::OkStatus();
}

absl::StatusOr<bool> ForwardTransformer::AppendGsqlInFunctionCallArrayArg(
    void* array_argument, ExprTransformerInfo* expr_transformer_info,
    std::vector<std::unique_ptr<zetasql::ResolvedExpr>>& argument_list) {
  ZETASQL_RET_CHECK_EQ(argument_list.size(), 1)
      << "The scalar argument should be added to argument_list before the "
         "array arguments";
  bool appended_arg_is_array =
      IsA(array_argument, Var) || IsA(array_argument, Param);
  if (IsA(array_argument, Var)) {
    // ANY/SOME expressions with column arguments.
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<zetasql::ResolvedExpr> column_ref,
                     BuildGsqlResolvedColumnRef(
                         *internal::PostgresConstCastNode(Var, array_argument),
                         *expr_transformer_info->var_index_scope));
    ZETASQL_RET_CHECK(column_ref->type()->IsArray());
    argument_list.push_back(std::move(column_ref));
  } else if (IsA(array_argument, Param)) {
    // ANY/SOME expressions with parameter arguments.
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<zetasql::ResolvedParameter> param_ref,
        BuildGsqlResolvedParameter(
            *internal::PostgresConstCastNode(Param, array_argument)));
    ZETASQL_RET_CHECK(param_ref->type()->IsArray());
    argument_list.push_back(std::move(param_ref));
  } else {
    ZETASQL_RET_CHECK(IsA(array_argument, ArrayExpr));
    // Get each expression from the array and add them to the argument list.
    ArrayExpr* array_node =
        internal::PostgresCastNode(ArrayExpr, array_argument);
    if (array_node->multidims) {
      return absl::InvalidArgumentError(
          "Multi-dimensional arrays are not supported.");
    }
    for (Expr* array_element : StructList<Expr*>(array_node->elements)) {
      ZETASQL_ASSIGN_OR_RETURN(
          std::unique_ptr<zetasql::ResolvedExpr> expr_arg,
          BuildGsqlResolvedExpr(*array_element, expr_transformer_info));
      ZETASQL_ASSIGN_OR_RETURN(
          std::unique_ptr<zetasql::ResolvedExpr> mapped_expr_arg,
          catalog_adapter_->GetEngineSystemCatalog()
              ->GetResolvedExprForComparison(
                  std::move(expr_arg),
                  catalog_adapter_->analyzer_options().language()));
      argument_list.push_back(std::move(mapped_expr_arg));
    }
    // Handle empty array e.g., value =ANY(array[])
    if (list_length(array_node->elements) == 0) {
      ZETASQL_ASSIGN_OR_RETURN(
          std::unique_ptr<zetasql::ResolvedExpr> array_arg,
          BuildGsqlResolvedExpr(*array_node, expr_transformer_info));
      argument_list.push_back(std::move(array_arg));
      appended_arg_is_array = true;
    }
  }
  return appended_arg_is_array;
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
ForwardTransformer::BuildGsqlResolvedSQLValueFunctionCall(
    const SQLValueFunction& function) {
  // Look up the function. SQL Value Functions do not have any arguments.
  ZETASQL_ASSIGN_OR_RETURN(
      FunctionAndSignature function_and_signature,
      catalog_adapter_->GetEngineSystemCatalog()->GetFunctionAndSignature(
          PostgresExprIdentifier::SQLValueFunction(function.op),
          /*input_argument_types=*/{},
          catalog_adapter_->analyzer_options().language()));

  return MakeResolvedFunctionCall(function_and_signature,
                                  /*argument_list=*/{});
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
ForwardTransformer::BuildGsqlArrayAccess(
    const SubscriptingRef& subscripting_ref,
    ExprTransformerInfo* expr_transformer_info) {
  // Input validation--no slices, no assignment, 1-dimensional

  if (subscripting_ref.refassgnexpr != nullptr) {
    return absl::InvalidArgumentError(
        "Assignment to array elements is not supported");
  }
  ZETASQL_RET_CHECK_NE(subscripting_ref.refupperindexpr, nullptr);
  if (list_length(subscripting_ref.refupperindexpr) != 1) {
    return absl::InvalidArgumentError(
        "Multi-dimensional arrays are not supported");
  }

  if (subscripting_ref.reflowerindexpr != nullptr &&
      list_length(subscripting_ref.reflowerindexpr) > 0) {
    if (list_length(subscripting_ref.reflowerindexpr) != 1) {
      return absl::InvalidArgumentError(
          "Multi-dimensional arrays are not supported");
    }
    return BuildGsqlResolvedArraySliceFunctionCall(subscripting_ref,
                                                   expr_transformer_info);
  }
  return BuildGsqlResolvedSafeArrayAtOrdinalFunctionCall(subscripting_ref,
                                                         expr_transformer_info);
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
ForwardTransformer::BuildGsqlResolvedSafeArrayAtOrdinalFunctionCall(
    const SubscriptingRef& subscripting_ref,
    ExprTransformerInfo* expr_transformer_info) {
  // Should have verified the absence of lowerbound before calling this
  // function. If lowerbound exists, that means array slicing.
  ZETASQL_RET_CHECK(subscripting_ref.reflowerindexpr == nullptr ||
            list_length(subscripting_ref.reflowerindexpr) == 0);
  // Should have verified the presence of upperbound before calling this
  // function.
  ZETASQL_RET_CHECK_NE(subscripting_ref.refupperindexpr, nullptr);
  // Should have verified it's not a multi-dimensional array before calling this
  // function.
  ZETASQL_RET_CHECK_EQ(list_length(subscripting_ref.refupperindexpr), 1);
  // There should be an expression that evaluates to an array.
  ZETASQL_RET_CHECK_NE(subscripting_ref.refexpr, nullptr);

  // Transform the input arguments.
  ZETASQL_RET_CHECK_NE(subscripting_ref.refexpr, nullptr);
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<zetasql::ResolvedExpr> array_source,
      BuildGsqlResolvedExpr(*subscripting_ref.refexpr, expr_transformer_info));

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<zetasql::ResolvedExpr> index,
      BuildGsqlResolvedExpr(*internal::PostgresConstCastToExpr(
                                linitial(subscripting_ref.refupperindexpr)),
                            expr_transformer_info));

  std::vector<std::unique_ptr<zetasql::ResolvedExpr>> argument_list;
  argument_list.push_back(std::move(array_source));
  argument_list.push_back(std::move(index));

  // Look up the function and signature.
  std::vector<zetasql::InputArgumentType> input_argument_types =
      GetInputArgumentTypes(argument_list);
  ZETASQL_ASSIGN_OR_RETURN(
      FunctionAndSignature function_and_signature,
      catalog_adapter_->GetEngineSystemCatalog()->GetFunctionAndSignature(
          PostgresExprIdentifier::SubscriptingRef(
              /*is_array_slice=*/false),
          input_argument_types,
          catalog_adapter_->analyzer_options().language()));

  return MakeResolvedFunctionCall(function_and_signature,
                                  std::move(argument_list));
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
ForwardTransformer::BuildGsqlResolvedArraySliceFunctionCall(
    const SubscriptingRef& subscripting_ref,
    ExprTransformerInfo* expr_transformer_info) {
  // Should have verified the presence of lower and upperbound (which imply
  // slicing) before calling this function.
  ZETASQL_RET_CHECK_NE(subscripting_ref.reflowerindexpr, nullptr);
  ZETASQL_RET_CHECK_NE(subscripting_ref.refupperindexpr, nullptr);
  // Should have verified it's not a multi-dimensional array before calling this
  // function.
  ZETASQL_RET_CHECK_EQ(list_length(subscripting_ref.reflowerindexpr), 1);
  ZETASQL_RET_CHECK_EQ(list_length(subscripting_ref.refupperindexpr), 1);
  // There should be an expression that evaluates to an array.
  ZETASQL_RET_CHECK_NE(subscripting_ref.refexpr, nullptr);

  std::unique_ptr<zetasql::ResolvedExpr> lower_index;
  // If this List element is a nullptr it means no lowerbound was specified for
  // slicing. Set the lowerbound as 1 i.e.,the first array index. For example,
  // array_value[:3] would get transformed as array_value[1:3]
  if (linitial(subscripting_ref.reflowerindexpr) == nullptr) {
    lower_index = zetasql::MakeResolvedLiteral(zetasql::types::Int64Type(),
                                                 zetasql::Value::Int64(1));
  } else {
    ZETASQL_ASSIGN_OR_RETURN(
        lower_index,
        BuildGsqlResolvedExpr(*internal::PostgresConstCastToExpr(
                                  linitial(subscripting_ref.reflowerindexpr)),
                              expr_transformer_info));
  }

  // If this List element is a nullptr it means no upperbound was specified for
  // slicing. Set the upperbound as the return value of array_upper() i.e., the
  // last array index. For example, array_value[2:] would get transformed as
  // array_value[2: array_upper(array_value, 1)]
  std::unique_ptr<zetasql::ResolvedExpr> upper_index;
  if (linitial(subscripting_ref.refupperindexpr) == nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<zetasql::ResolvedExpr> upper_bound_array,
                     BuildGsqlResolvedExpr(*subscripting_ref.refexpr,
                                           expr_transformer_info));

    std::unique_ptr<zetasql::ResolvedExpr> one_literal =
        zetasql::MakeResolvedLiteral(zetasql::types::Int64Type(),
                                       zetasql::Value::Int64(1));

    std::vector<std::unique_ptr<zetasql::ResolvedExpr>> upper_bound_arg_list;
    upper_bound_arg_list.push_back(std::move(upper_bound_array));
    upper_bound_arg_list.push_back(std::move(one_literal));

    std::vector<zetasql::InputArgumentType> upper_bound_arg_types =
        GetInputArgumentTypes(upper_bound_arg_list);

    ZETASQL_ASSIGN_OR_RETURN(
        FunctionAndSignature array_upper_function_and_signature,
        catalog_adapter_->GetEngineSystemCatalog()->GetFunctionAndSignature(
            F_ARRAY_UPPER, upper_bound_arg_types,
            catalog_adapter_->analyzer_options().language()));
    upper_index = MakeResolvedFunctionCall(array_upper_function_and_signature,
                                           std::move(upper_bound_arg_list));
  } else {
    ZETASQL_ASSIGN_OR_RETURN(
        upper_index,
        BuildGsqlResolvedExpr(*internal::PostgresConstCastToExpr(
                                  linitial(subscripting_ref.refupperindexpr)),
                              expr_transformer_info));
  }

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<zetasql::ResolvedExpr> array_source,
      BuildGsqlResolvedExpr(*subscripting_ref.refexpr, expr_transformer_info));

  std::vector<std::unique_ptr<zetasql::ResolvedExpr>> slicing_arg_list;
  slicing_arg_list.push_back(std::move(array_source));
  slicing_arg_list.push_back(std::move(lower_index));
  slicing_arg_list.push_back(std::move(upper_index));

  // Look up the function and signature.
  std::vector<zetasql::InputArgumentType> slicing_arg_types =
      GetInputArgumentTypes(slicing_arg_list);
  ZETASQL_ASSIGN_OR_RETURN(
      FunctionAndSignature function_and_signature,
      catalog_adapter_->GetEngineSystemCatalog()->GetFunctionAndSignature(
          PostgresExprIdentifier::SubscriptingRef(
              /*is_array_slice=*/true),
          slicing_arg_types, catalog_adapter_->analyzer_options().language()));

  return MakeResolvedFunctionCall(function_and_signature,
                                  std::move(slicing_arg_list));
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
ForwardTransformer::BuildGsqlResolvedMakeArrayFunctionCall(
    const ArrayExpr& array_expr, ExprTransformerInfo* expr_transformer_info) {
  std::vector<std::unique_ptr<zetasql::ResolvedExpr>> argument_list;
  // Get array element type for error checking.
  ZETASQL_ASSIGN_OR_RETURN(const zetasql::Type* element_type,
                   BuildGsqlType(array_expr.element_typeid));
  for (Expr* expr : StructList<Expr*>(array_expr.elements)) {
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<zetasql::ResolvedExpr> resolved_expr,
                     BuildGsqlResolvedExpr(*expr, expr_transformer_info));
    // Shouldn't happen, but it's cheap to check and could save us a crash.
    ZETASQL_RET_CHECK(resolved_expr->type()->Equals(element_type))
        << "Array constructor has mismatched types. Array has element type: "
        << element_type->DebugString() << " but found an element with type "
        << resolved_expr->type()->DebugString();
    argument_list.push_back(std::move(resolved_expr));
  }

  // Look up the function and signature.
  std::vector<zetasql::InputArgumentType> input_argument_types =
      GetInputArgumentTypes(argument_list);
  ZETASQL_ASSIGN_OR_RETURN(
      FunctionAndSignature function_and_signature,
      catalog_adapter_->GetEngineSystemCatalog()->GetFunctionAndSignature(
          PostgresExprIdentifier::Expr(T_ArrayExpr), input_argument_types,
          catalog_adapter_->analyzer_options().language()));
  return MakeResolvedFunctionCall(function_and_signature,
                                  std::move(argument_list));
}

}  // namespace postgres_translator
