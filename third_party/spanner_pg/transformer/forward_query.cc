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

#include <string.h>

#include <climits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/analyzer/expr_matching_helpers.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/input_argument_type.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_enums.pb.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "absl/cleanup/cleanup.h"
#include "absl/container/flat_hash_map.h"
#include "absl/flags/flag.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/bootstrap_catalog/bootstrap_catalog.h"
#include "third_party/spanner_pg/catalog/catalog_adapter.h"
#include "third_party/spanner_pg/catalog/engine_system_catalog.h"
#include "third_party/spanner_pg/catalog/engine_user_catalog.h"
#include "third_party/spanner_pg/catalog/table_name.h"
#include "third_party/spanner_pg/catalog/udf_support.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "third_party/spanner_pg/transformer/expr_transformer_helper.h"
#include "third_party/spanner_pg/transformer/forward_transformer.h"
#include "third_party/spanner_pg/transformer/transformer_helper.h"
#include "third_party/spanner_pg/util/nodetag_to_string.h"
#include "third_party/spanner_pg/util/pg_list_iterators.h"
#include "third_party/spanner_pg/util/postgres.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

// TODO: Augment ParserOutput with the query string length data to
// fill in the statement end location in the case where PG puts an end location
// of 0 to mean "rest of string."  Once that's done, remove this flag.
ABSL_FLAG(bool, spangres_include_invalid_statement_parse_locations, true,
          "Include a start parse location for statements even when the end "
          "location is not known.  By convention, the end location will be set "
          "to 0 to indicate that it is the remainder of the query string.");

ABSL_FLAG(int64_t, spangres_expression_recursion_limit, 1'000,
          "The maximum depth of expression recursion tree in a SQL query. The "
          "transformer will return an error if it sees a tree depth higher "
          "than the limit.");

ABSL_FLAG(
    int64_t, spangres_set_operation_recursion_limit, 100,
    "The maximum depth of set operation recursion tree in a SQL query. The "
    "transformer will return an error if it sees a tree depth higher "
    "than the limit.");

ABSL_FLAG(bool, spangres_use_emulator_ordinality_transformer, false,
         "When true, array scans with array_offset_column will be wrapped in "
         "a ProjectScan that adds one to the offset column to convert "
         "zero-based offset to one-based ordinal values");

namespace postgres_translator {

using ::postgres_translator::internal::PostgresCastNodeTemplate;
using ::postgres_translator::internal::PostgresCastToExpr;
using ::postgres_translator::internal::PostgresCastToNode;
using ::postgres_translator::internal::PostgresConstCastNodeTemplate;
using ::postgres_translator::internal::PostgresConstCastToExpr;

namespace {

absl::Status CheckTypeSupportsGrouping(
    const Oid& group_by_column_pg_type_oid,
    const zetasql::Type* group_by_column_type,
    const zetasql::LanguageOptions& language_options) {
  if (!group_by_column_type->SupportsGrouping(language_options,
                                              /*type_description=*/nullptr)) {
    ZETASQL_ASSIGN_OR_RETURN(const char* pg_type_name,
                     PgBootstrapCatalog::Default()->GetFormattedTypeName(
                         group_by_column_pg_type_oid));
    return absl::UnimplementedError(
        absl::StrFormat("Queries containing a GROUP BY clause on column "
                        "with Type %s are not supported",
                        pg_type_name));
  }
  return absl::OkStatus();
}

absl::Status CheckTypeSupportsOrdering(
    const Oid& sort_item_pg_type_oid,
    const zetasql::Type* sort_item_gsql_type,
    const zetasql::LanguageOptions& language_options) {
  if (!sort_item_gsql_type->SupportsOrdering(language_options,
                                             /*type_description=*/nullptr)) {
    ZETASQL_ASSIGN_OR_RETURN(const char* pg_type_name,
                     PgBootstrapCatalog::Default()->GetFormattedTypeName(
                         sort_item_pg_type_oid));
    return absl::UnimplementedError(
        absl::StrFormat("Queries containing a ORDER BY clause on column "
                        "with Type %s are not supported",
                        pg_type_name));
  }
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<zetasql::ResolvedColumn> ForwardTransformer::GetResolvedColumn(
    const VarIndexScope& var_index_scope, Index varno, AttrNumber varattno,
    int var_levels_up, CorrelatedColumnsSetList* correlated_columns_sets) {
  VarIndex var_index{.varno = varno, .varattno = varattno};
  zetasql::ResolvedColumn result;
  ZETASQL_ASSIGN_OR_RETURN(bool var_index_found, var_index_scope.LookupVarIndex(
                                             var_index, var_levels_up, &result,
                                             correlated_columns_sets));
  if (!var_index_found) {
    ABSL_LOG(INFO) << absl::StrFormat(
        "Unable to look up var index for (varno, varattno, varlevelsup): (%d, "
        "%d, %d) in scope: \ncurrent_scope: %s",
        varno, varattno, var_levels_up, var_index_scope.DebugString());
    return absl::InvalidArgumentError(
        "Column reference is unsupported in this context");
  }
  return result;
}

absl::StatusOr<zetasql::ResolvedColumn>
ForwardTransformer::BuildNewGsqlResolvedColumn(
    absl::string_view table_name, absl::string_view column_name,
    const zetasql::Type* column_type) {
  ZETASQL_ASSIGN_OR_RETURN(int column_id, catalog_adapter_->AllocateColumnId());
  return zetasql::ResolvedColumn(
      column_id,
      /*table_name=*/
      catalog_adapter_->analyzer_options().id_string_pool()->Make(table_name),
      /*name=*/
      catalog_adapter_->analyzer_options().id_string_pool()->Make(column_name),
      /*type=*/column_type);
}

absl::StatusOr<const zetasql::Table*> ForwardTransformer::GetTableFromRTE(
    const RangeTblEntry& rte) {
  ZETASQL_RET_CHECK_EQ(rte.rtekind, RTE_RELATION);
  Oid table_oid = rte.relid;
  // PostgreSQL query tree doesn't contain the actual table name, just alias.
  // So look it up in the catalog adapter using the table oid.
  ZETASQL_ASSIGN_OR_RETURN(const TableName table_name,
                   catalog_adapter_->GetTableNameFromOid(table_oid));

  const zetasql::Table* table;
  ZETASQL_RETURN_IF_ERROR(catalog_adapter_->GetEngineUserCatalog()->FindTable(
      table_name.AsSpan(), &table));
  return table;
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedTableScan>>
ForwardTransformer::BuildGsqlResolvedTableScan(const RangeTblEntry& rte,
                                               Index rtindex,
                                               VarIndexScope* output_scope) {
  ZETASQL_ASSIGN_OR_RETURN(const zetasql::Table* table, GetTableFromRTE(rte));

  absl::string_view table_alias =
      (rte.alias != nullptr) ? absl::string_view(rte.alias->aliasname) : "";
  int numcolumns = table->NumColumns();
  ZETASQL_RET_CHECK_EQ(list_length(rte.eref->colnames), numcolumns);
  std::vector<zetasql::ResolvedColumn> column_list;
  column_list.reserve(numcolumns);
  std::vector<int> column_index_list(numcolumns);
  std::iota(column_index_list.begin(), column_index_list.end(), 0);

  for (int i = 0; i < numcolumns; ++i) {
    const zetasql::Column* column = table->GetColumn(i);

    // Table columns are always new (we only build TableScan once per table).
    // So we always build new ResolvedColumns and new column IDs for them.
    ZETASQL_ASSIGN_OR_RETURN(zetasql::ResolvedColumn resolved_column,
                     BuildNewGsqlResolvedColumn(table->Name(), column->Name(),
                                                column->GetType()));
    column_list.push_back(resolved_column);

    // Add each column to the var_index_scope.
    // Fail if the entry already exists.
    ZETASQL_RET_CHECK(output_scope->MapVarIndexToColumn(
        {.varno = rtindex, .varattno = i + 1}, resolved_column));
  }

  std::unique_ptr<zetasql::ResolvedTableScan> table_scan =
      zetasql::MakeResolvedTableScan(column_list, table,
                                       /*for_system_time_expr=*/nullptr,
                                       std::string(table_alias));
  table_scan->set_column_index_list(column_index_list);

  if (rte.tableHints) {
    ZETASQL_ASSIGN_OR_RETURN(
        std::vector<std::unique_ptr<const zetasql::ResolvedOption>> hint_list,
        BuildGsqlResolvedOptionList(*rte.tableHints, output_scope));
    table_scan->set_hint_list(std::move(hint_list));
  }

  return table_scan;
}

absl::Status ForwardTransformer::MapVarIndexToColumn(
    const zetasql::ResolvedScan& scan, Index rtindex,
    VarIndexScope* var_index_scope) {
  ZETASQL_RET_CHECK(var_index_scope != nullptr);
  for (int i = 0; i < scan.column_list_size(); ++i) {
    // var_index_scope may be overwritten as multiple scope inputs are combined.
    var_index_scope->MapVarIndexToColumn(
        VarIndex{.varno = rtindex, .varattno = i + 1}, scan.column_list(i),
        /*allow_override=*/true);
  }
  return absl::OkStatus();
}

absl::Status ForwardTransformer::MapVarIndexToColumnForJoin(
    const RangeTblEntry& rte, Index rtindex, VarIndexScope* var_index_scope) {
  ZETASQL_RET_CHECK_EQ(rte.rtekind, RTE_JOIN);
  int var_index = 1;

  // Iterate through joinaliasvars to retrieve the varno and varattrno from the
  // PG Vars inside. These are references to actual Vars of tables or
  // subqueries, or another Var references inside a child join. Either way, add
  // an entry to the map here, so the final output_column_list of QueryStmt is
  // built correctly.
  for (Node* join_alias_var : StructList<Node*>(rte.joinaliasvars)) {
    // We add the coalesce expr vars which come from full join with using at
    // their time of creation so continue here.
    if (join_alias_var->type == T_CoalesceExpr) {
      ++var_index;
      continue;
    }
    if (join_alias_var->type != T_Var) {
      return absl::UnimplementedError(
          absl::StrCat("Unsupported PostgreSQL node type: ",
                       NodeTagToNodeString(nodeTag(join_alias_var))));
    }
    Var* alias_var = PostgresCastNode(Var, join_alias_var);
    ZETASQL_ASSIGN_OR_RETURN(
        zetasql::ResolvedColumn column,
        GetResolvedColumn(*var_index_scope, alias_var->varno,
                          alias_var->varattno, alias_var->varlevelsup));

    // Fail if the entry already exists.
    ZETASQL_RET_CHECK(var_index_scope->MapVarIndexToColumn(
        {.varno = rtindex, .varattno = var_index}, column));
    ++var_index;
  }
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedScan>>
ForwardTransformer::BuildGsqlResolvedScanForTableExpression(
    const Node& node, const List& rtable, const VarIndexScope* external_scope,
    const VarIndexScope* local_scope, VarIndexScope* output_scope) {
  // Modeled after the ZetaSQL ResolveTableExpression function.
  if (IsA(&node, RangeTblRef)) {
    Index rtindex = PostgresConstCastNode(RangeTblRef, &node)->rtindex;
    RangeTblEntry* rte = rt_fetch(rtindex, &rtable);
    switch (rte->rtekind) {
      case RTE_RELATION: {
        return BuildGsqlResolvedTableScan(*rte, rtindex, output_scope);
      }
      case RTE_SUBQUERY: {
        if (rte->lateral) {
          return absl::UnimplementedError("Lateral subqueries are not "
                                          "supported");
        }
        ZETASQL_RET_CHECK_NE(rte->subquery, nullptr);
        ZETASQL_RET_CHECK_NE(rte->alias->aliasname, nullptr);

        // ZetaSQL does not create another scope here because it is only
        // concerned with whether a column is correlated or not.
        // Spangres needs to create a new scope for each subquery level because
        // the scope level must match varlevelsup.
        CorrelatedColumnsSet correlated_columns_set;
        VarIndexScope subquery_scope(external_scope, &correlated_columns_set);

        // Output columns are not constructed for subqueries, so there is no
        // need to collect the output_name_list.
        ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<zetasql::ResolvedScan> input_scan,
                         BuildGsqlResolvedScanForQueryExpression(
                             *rte->subquery, /*is_top_level_query=*/false,
                             &subquery_scope, rte->alias->aliasname,
                             /*output_name_list=*/nullptr));
        ZETASQL_RETURN_IF_ERROR(
            MapVarIndexToColumn(*input_scan, rtindex, output_scope));
        // Every ResolvedOrderByScan is initially constructed with is_ordered
        // set to true. However, a subquery never preserves order, so we clear
        // is_ordered on the top level scan of the subquery, even if it is a
        // ResolvedOrderByScan.
        input_scan->set_is_ordered(false);
        return input_scan;
      }
      // Error cases to cover the remaining enum variants. We handle each
      // individually to provide helpful error messages.
      case RTE_JOIN: {
        // Shouldn't happen because this is for RangeTblRef from clause items.
        return absl::InvalidArgumentError("Unsupported JOIN in FROM clause");
      }
      case RTE_FUNCTION: {
        // Hand off to a funciton-call helper.
        return BuildGsqlResolvedScanForFunctionCall(
            rte, rtindex, external_scope, output_scope);
      }
      case RTE_TABLEFUNC: {
        // NB: This is really just for XML functions.
        return absl::UnimplementedError(
            "Unsupported function call in FROM clause");
      }
      case RTE_VALUES: {
        return absl::UnimplementedError(
            "VALUES lists in FROM clause are not supported");
      }
      case RTE_CTE: {
        ZETASQL_ASSIGN_OR_RETURN(
            std::unique_ptr<zetasql::ResolvedWithRefScan> scan,
            BuildGsqlResolvedWithRefScan(rte->ctename, rtindex, output_scope));
        ZETASQL_RETURN_IF_ERROR(MapVarIndexToColumn(*scan, rtindex, output_scope));
        return scan;
      }
      default:
        return absl::InternalError(
            absl::StrCat("Unsupported RangeTblEntry type in the from list: ",
                         internal::RTEKindToString(rte->rtekind)));
    }
  } else if (IsA(&node, JoinExpr)) {
    JoinExpr* join_expr = PostgresCastNode(JoinExpr, (void*)&node);
    std::unique_ptr<zetasql::ResolvedScan> join_scan;
    if (list_length(join_expr->usingClause) > 0) {
      ZETASQL_ASSIGN_OR_RETURN(join_scan, BuildGsqlResolvedJoinScan(
                                      *join_expr, rtable, external_scope,
                                      local_scope, output_scope,
                                      /*has_using=*/true));
    } else {
      ZETASQL_ASSIGN_OR_RETURN(join_scan, BuildGsqlResolvedJoinScan(
                                      *join_expr, rtable, external_scope,
                                      local_scope, output_scope));
    }
    return join_scan;
  } else {
    return absl::UnimplementedError(absl::StrCat(
        "Node type ", NodeTagToNodeString(node.type), " is unsupported"));
  }
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedScan>>
ForwardTransformer::BuildGsqlResolvedScanForFunctionCall(
    const RangeTblEntry* rte, const Index rtindex,
    const VarIndexScope* external_scope, VarIndexScope* output_scope) {
  // Only a few FROM-clause function calls are supported. Determine what they
  // are and dispatch the appropriate ResolvedScan builder.
  ZETASQL_RET_CHECK_GE(list_length(rte->functions), 1);
  Node* expr_node = linitial_node(RangeTblFunction, rte->functions)->funcexpr;
  // We don't support queries that generate a RangeTblFunction which contains a
  // funcexpr that is not of type FuncExpr e.g.,
  // `select * from current_date` generates a RangeTblFunction with
  // funcexpr type `SQLVALUEFUNCTION` which is not supported. However,
  // `select current_date` which has a different tree structure is supported.
  // The implication is that only a few types of functions (e.g., UNNEST()) are
  // supported in the FROM clause.
  if (!IsA(expr_node, FuncExpr)) {
    return absl::InvalidArgumentError(
        "Unsupported function call in FROM clause");
  }
  FuncExpr* func_expr = PostgresCastNode(FuncExpr, expr_node);
  ZETASQL_RET_CHECK_NE(func_expr, nullptr);
  ZETASQL_ASSIGN_OR_RETURN(Oid array_unnest_proc_oid,
                   internal::GetArrayUnnestProcOid());
  // We only support the Array 'unnest' proc: unnest(anyarray)->anyelement, not
  // the other variants (for record or range). UNNEST becomes an ArrayScan in
  // ZetaSQL.
  if (func_expr->funcid == array_unnest_proc_oid) {
    absl::StatusOr<std::unique_ptr<zetasql::ResolvedArrayScan>> array_scan =
        BuildGsqlResolvedArrayScan(*rte, rtindex, external_scope, output_scope);

    if (array_scan.ok() &&
        array_scan.value()->array_offset_column() != nullptr) {
      // For `unnest with ordinality`, convert ZetaSQL's zero-based
      //  offset to a one-based ordinal column as expected by postgres.
      return ConvertZeroBasedOffsetToOneBasedOrdinal(
          std::move(array_scan.value()), rtindex, output_scope);
    }
    return array_scan;
  } else {
    // Anything else had better be a TVF. For now that's Change Streams, which
    // is user defined and assigned a temporary oid in CatalogAdapter, or
    // builtin TVFs.
    auto tvf_catalog_entry = catalog_adapter().GetTVFFromOid(func_expr->funcid);
    if (tvf_catalog_entry.ok()) {
      return BuildGsqlResolvedTVFScan(*rte, rtindex, external_scope,
                                      *tvf_catalog_entry, output_scope);
    }
    return absl::InvalidArgumentError(
        "Unsupported function call in FROM clause");
  }
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedProjectScan>>
ForwardTransformer::ConvertZeroBasedOffsetToOneBasedOrdinal(
    std::unique_ptr<zetasql::ResolvedArrayScan> array_scan,
    const Index& rtindex, VarIndexScope* output_scope) {
  ZETASQL_RET_CHECK(array_scan->array_offset_column() != nullptr);

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<zetasql::ResolvedColumnRef> ordinality_column_ref,
      BuildGsqlResolvedColumnRef(array_scan->array_offset_column()->column()));

  // Create a function call that adds one to the ordinality column.
  std::unique_ptr<zetasql::ResolvedExpr> one_literal =
      zetasql::MakeResolvedLiteral(zetasql::types::Int64Type(),
                                     zetasql::Value::Int64(1));

  std::vector<zetasql::InputArgumentType> input_argument_types;
  input_argument_types.emplace_back(ordinality_column_ref->type());
  input_argument_types.emplace_back(one_literal->type());

  std::vector<std::unique_ptr<zetasql::ResolvedExpr>> argument_list;
  argument_list.push_back(std::move(ordinality_column_ref));
  argument_list.push_back(std::move(one_literal));

  ZETASQL_ASSIGN_OR_RETURN(
      FunctionAndSignature function_and_signature,
      catalog_adapter_->GetEngineSystemCatalog()->GetFunctionAndSignature(
          F_INT8PL, input_argument_types,
          catalog_adapter_->analyzer_options().language()));

  std::unique_ptr<zetasql::ResolvedFunctionCall> function_call =
      zetasql::MakeResolvedFunctionCall(
          function_and_signature.signature().result_type().type(),
          function_and_signature.function(), function_and_signature.signature(),
          std::move(argument_list),
          zetasql::ResolvedFunctionCallBase::DEFAULT_ERROR_MODE);

  // Create a new ordinality column as a computed column with the function call
  // that adds one to the original ordinality column.
  ZETASQL_ASSIGN_OR_RETURN(
      zetasql::ResolvedColumn ordinality_column,
      BuildNewGsqlResolvedColumn(array_scan->column_list().at(1).table_name(),
                                 array_scan->column_list().at(1).name(),
                                 function_call->type()));
  std::unique_ptr<const zetasql::ResolvedComputedColumn>
      computed_ordinality_column = zetasql::MakeResolvedComputedColumn(
          ordinality_column, std::move(function_call));

  std::vector<std::unique_ptr<const zetasql::ResolvedComputedColumn>>
      computed_columns;
  computed_columns.push_back(std::move(computed_ordinality_column));

  // Create a project scan with the original unnest column and the
  // computed ordinality column.
  std::vector<zetasql::ResolvedColumn> project_scan_columns;
  project_scan_columns.reserve(array_scan->column_list().size());
  project_scan_columns.push_back(array_scan->column_list().at(0));
  project_scan_columns.push_back(computed_columns.at(0)->column());
  output_scope->MapVarIndexToColumn({.varno = rtindex, .varattno = 2},
                                    computed_columns.at(0)->column(),
                                    /*allow_override=*/true);
  return zetasql::MakeResolvedProjectScan(
      project_scan_columns, std::move(computed_columns), std::move(array_scan));
}

absl::Status ForwardTransformer::PrepareTVFInputArguments(
    const FuncExpr& func_expr,
    const zetasql::TableValuedFunction* tvf_catalog_entry,
    const VarIndexScope* external_scope,
    std::unique_ptr<zetasql::FunctionSignature>* result_signature,
    std::vector<std::unique_ptr<const zetasql::ResolvedFunctionArgument>>*
        resolved_tvf_args,
    std::vector<zetasql::TVFInputArgumentType>* tvf_input_arguments) {
  // The structure of this function is intentionally similar in structure to
  // ZetaSQL's Resolver::PrepareTVFInputArguments, though we support only a
  // subset of the behavior here. To preserve that structure for future
  // additions, we follow the same grouping of processing steps:
  //   - Transform and validate the input argument expressions.
  //     (ZetaSQL performs this as part of MatchTVFSignature, but we have
  //     already matched signatures in the PG Analyzer)
  //   - We omit type coercion because PG has already done this for us.
  //   - Prepare the TVF-specific input argument structures required by
  //     TableValuedFunction::Resolve() (computes the TVF's output signature).
  const int num_args = list_length(func_expr.args);
  ExprTransformerInfo expr_transformer_info =
      ExprTransformerInfo::ForScalarFunctions(external_scope, "FROM");
  resolved_tvf_args->reserve(num_args);
  for (Expr* arg : StructList<Expr*>(func_expr.args)) {
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<zetasql::ResolvedExpr> expr,
                     BuildGsqlResolvedExpr(*arg, &expr_transformer_info));
    resolved_tvf_args->push_back(zetasql::MakeResolvedTVFArgument(
        std::move(expr), /*scan=*/nullptr,
        /*model=*/nullptr, /*connection=*/nullptr,
        /*descriptor_arg=*/nullptr,
        /*argument_column_list=*/{}));
  }

  // Prepare the input argument list for calling TableValuedFunction::Resolve.
  // ZetaSQL supports a wide range of argument types, including full
  // relations. We only support scalar expressions.
  tvf_input_arguments->reserve(num_args);
  for (int i = 0; i < num_args; ++i) {
    const std::unique_ptr<const zetasql::ResolvedFunctionArgument>&
        resolved_arg = (*resolved_tvf_args)[i];
    ZETASQL_RET_CHECK_NE(resolved_arg->expr(), nullptr);
    if (resolved_arg->expr()->node_kind() == zetasql::RESOLVED_LITERAL) {
      const zetasql::Value& value =
          resolved_arg->expr()->GetAs<zetasql::ResolvedLiteral>()->value();
      tvf_input_arguments->push_back(
          zetasql::TVFInputArgumentType(zetasql::InputArgumentType(value)));
    } else {
      tvf_input_arguments->push_back(zetasql::TVFInputArgumentType(
          zetasql::InputArgumentType(resolved_arg->expr()->type())));
    }
    tvf_input_arguments->back().set_scalar_expr(resolved_arg->expr());
  }

  // The PG Analyzer has already matched the signature, so we won't do it again.
  // Just extract the only signature and return it.
  ZETASQL_RET_CHECK_EQ(tvf_catalog_entry->NumSignatures(), 1);
  const int signature_index = 0;
  const zetasql::FunctionSignature& signature =
      *tvf_catalog_entry->GetSignature(signature_index);
  // We're not going to resolve templated types here, just make sure there
  // aren't any.
  ZETASQL_RET_CHECK(signature.IsConcrete());
  result_signature->reset();
  *result_signature = std::make_unique<zetasql::FunctionSignature>(signature);

  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedWithRefScan>>
ForwardTransformer::BuildGsqlResolvedWithRefScan(absl::string_view with_alias,
                                                 Index rtindex,
                                                 VarIndexScope* output_scope) {
  // The ZetaSQL ResolvedWithRefScan is used whenever a WITH clause subquery
  // is referenced in the primary query or in another WITH clause subquery.
  //
  // The original subquery is represented as a ResolvedWithEntry and has the
  // original columns with the original column names.
  // The subquery reference that is represented by the ResolvedWithRefScan has
  // new columns with the column aliases.
  //
  // When the transformer was constructing the ResolvedWithEntry for this WITH
  // clause subquery, it also stored a mapping of the WITH clause alias to the
  // original columns and the column aliases. Now we look up the stored metadata
  // to construct the ResolvedWithRefScan.
  ZETASQL_ASSIGN_OR_RETURN(const NamedSubquery* named_subquery,
                   GetWithClauseMetadata(with_alias));

  // For each new column produced in the WithRefScan, we want to name it
  // using the WITH alias, not the original column name.  e.g. In
  //   WITH Q AS (SELECT Key K FROM KeyValue)
  //   SELECT * FROM Q;
  // we want to call the new column Q.K, not Q.Key.  Since the column_list
  // may not map 1:1 with select-list column names, we need to build a map.
  std::map<zetasql::ResolvedColumn, zetasql::IdString> with_column_to_alias;
  for (const NamedColumn& named_column : named_subquery->column_aliases) {
    zetasql_base::InsertIfNotPresent(&with_column_to_alias, named_column.column,
                            named_column.name);
  }

  // Make a new ResolvedColumn for each column from the WITH scan.
  // This is necessary so that if the WITH subquery is referenced twice,
  // we get distinct column names for each scan.
  std::vector<zetasql::ResolvedColumn> column_list;
  for (int i = 0; i < named_subquery->column_list.size(); ++i) {
    const zetasql::ResolvedColumn& column = named_subquery->column_list[i];

    // Get the alias for the column produced by the WITH reference,
    // using the first alias for that column in the WITH subquery.
    // Every column in the column_list should correspond to at least one column
    // in the WITH subquery's NameList.
    zetasql::IdString new_column_alias;
    const zetasql::IdString* found =
        zetasql_base::FindOrNull(with_column_to_alias, column);
    ZETASQL_RET_CHECK(found != nullptr) << column.DebugString();
    new_column_alias = *found;

    ZETASQL_ASSIGN_OR_RETURN(
        zetasql::ResolvedColumn new_column,
        BuildNewGsqlResolvedColumn(with_alias, new_column_alias.ToStringView(),
                                   named_subquery->column_list[i].type()));
    column_list.push_back(new_column);
    RecordColumnAccess(column_list.back());

    // Add each column to the var_index_scope.
    // Fail if the entry already exists.
    ZETASQL_RET_CHECK(output_scope->MapVarIndexToColumn(
        {.varno = rtindex, .varattno = i + 1}, new_column));
  }

  return zetasql::MakeResolvedWithRefScan(column_list,
                                            std::string(with_alias));
}

absl::StatusOr<GsqlJoinType> ForwardTransformer::BuildGsqlJoinType(
    PgJoinType pg_join_type) {
  switch (pg_join_type) {
    case JOIN_INNER: {
      return zetasql::ResolvedJoinScan::INNER;
    }
    case JOIN_LEFT: {
      return zetasql::ResolvedJoinScan::LEFT;
    }
    case JOIN_RIGHT: {
      return zetasql::ResolvedJoinScan::RIGHT;
    }
    case JOIN_FULL: {
      return zetasql::ResolvedJoinScan::FULL;
    }
    default: {
      return absl::UnimplementedError(
          absl::StrCat("Unsupported join type: ",
                       internal::PostgresJoinTypeToString(pg_join_type)));
    }
  }
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedScan>>
ForwardTransformer::BuildGsqlResolvedJoinScan(
    const JoinExpr& join_expr, const List& rtable,
    const VarIndexScope* external_scope, const VarIndexScope* local_scope,
    VarIndexScope* output_scope, bool has_using) {
  ZETASQL_ASSIGN_OR_RETURN(GsqlJoinType gsql_join_type,
                   BuildGsqlJoinType(join_expr.jointype));
  VarIndexScope output_scope_lhs;
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<zetasql::ResolvedScan> left_scan,
                   BuildGsqlResolvedScanForTableExpression(
                       *(join_expr.larg), rtable, external_scope, local_scope,
                       &output_scope_lhs));

  bool is_right_or_full_join =
      gsql_join_type == zetasql::ResolvedJoinScan::RIGHT ||
      gsql_join_type == zetasql::ResolvedJoinScan::FULL;

  // A clean scope only includes external names (and none of the names
  // introduced locally in the same FROM clause). There are two cases where we
  // need a "clean" scope on the right hand side of the JOIN:
  //
  // 1. The right hand side is a parenthesized join.
  // 2. The rhs is not a parenthesized join (e.g. table names, path
  // expressions, table subqueries) and the current JOIN type is RIGHT JOIN or
  // FULL JOIN.
  //
  // Otherwise, the lhs name scope is still needed because it is allowed to
  // have names correlated to the lhs of the JOIN.
  VarIndexScope scope_for_rhs(external_scope);
  if (!is_right_or_full_join) {
    scope_for_rhs.MergeFrom(output_scope_lhs);
  }

  bool rhs_is_unnest_expr = false;
  // Check whether RHS is unnest
  if (IsA(join_expr.rarg, RangeTblRef)) {
    Index rtindex = PostgresConstCastNode(RangeTblRef, join_expr.rarg)->rtindex;
    RangeTblEntry* rte = rt_fetch(rtindex, &rtable);
    if (rte->rtekind == RTE_FUNCTION) {
      ZETASQL_RET_CHECK_GE(list_length(rte->functions), 1);
      Node* expr_node =
          linitial_node(RangeTblFunction, rte->functions)->funcexpr;
      ZETASQL_RET_CHECK(IsA(expr_node, FuncExpr));
      FuncExpr* func_expr = PostgresCastNode(FuncExpr, expr_node);
      ZETASQL_RET_CHECK_NE(func_expr, nullptr);
      ZETASQL_ASSIGN_OR_RETURN(const Oid array_unnest_proc_oid,
                       internal::GetArrayUnnestProcOid());
      rhs_is_unnest_expr = func_expr->funcid == array_unnest_proc_oid;
    }
  }
  std::unique_ptr<zetasql::ResolvedScan> result;
  if (rhs_is_unnest_expr && !is_right_or_full_join) {
    bool is_left_outer = false;
    switch (gsql_join_type) {
      case zetasql::ResolvedJoinScan::INNER:
        break;
      case zetasql::ResolvedJoinScan::LEFT:
        is_left_outer = true;
        break;
      case zetasql::ResolvedJoinScan::RIGHT:
        return absl::UnimplementedError(
            "UNNEST is not supported with RIGHT JOIN.");
      case zetasql::ResolvedJoinScan::FULL:
        return absl::UnimplementedError(
            "UNNEST is not supported with FULL JOIN.");
    }
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<zetasql::ResolvedScan> right_scan,
                     BuildGsqlResolvedScanForTableExpression(
                         *(join_expr.rarg), rtable, &scope_for_rhs,
                         &scope_for_rhs, &scope_for_rhs));
    std::vector<zetasql::ResolvedColumn> output_columns =
        left_scan->column_list();
    output_columns.insert(output_columns.end(),
                          right_scan->column_list().begin(),
                          right_scan->column_list().end());

    // Build a join expression representing an ON or USING clause. Note that
    // both PostgreSQL and ZetaSQL represent these clauses the same way, so
    // the transformer can just take a PostgreSQL Expr and transform it to a
    // ZetaSQL ResolvedExpr.
    output_scope->MergeFrom(scope_for_rhs);
    VarIndexScope join_expr_scope(external_scope, *output_scope);
    std::unique_ptr<const zetasql::ResolvedExpr> gsql_join_expr;
    if (join_expr.quals != nullptr) {
      ZETASQL_ASSIGN_OR_RETURN(
          gsql_join_expr,
          BuildGsqlResolvedScalarExpr(*PostgresConstCastToExpr(join_expr.quals),
                                      &join_expr_scope, "JOIN clause"));
    }
    // A join expression can only be null if gsql_join_type is INNER.
    if (gsql_join_type != zetasql::ResolvedJoinScan::INNER) {
      ZETASQL_RET_CHECK(gsql_join_expr != nullptr);
    }

    if (right_scan->Is<zetasql::ResolvedArrayScan>()) {
      zetasql::ResolvedArrayScan* right_array_scan =
          right_scan->GetAs<zetasql::ResolvedArrayScan>();
      // Extract UNNEST WITH ORDINALITY column in the right scan if it exists.
      std::unique_ptr<const zetasql::ResolvedColumnHolder>
          right_scan_array_offset_col_holder = nullptr;
      result = zetasql::MakeResolvedArrayScan(
          /*column_list=*/output_columns,
          /*input_scan=*/std::move(left_scan),
          /*array_expr=*/right_array_scan->release_array_expr(),
          /*element_column=*/right_array_scan->element_column(),
          /*array_offset_column=*/std::move(right_scan_array_offset_col_holder),
          /*join_expr=*/std::move(gsql_join_expr),
          /*is_outer=*/is_left_outer);
    } else {
      // Although the RHS is an unnest expr, it may not be a ResolvedArrayScan
      // if it represents `unnest with ordinality` and was wrapped in a
      // ProjectScan. See `ConvertZeroBasedOffsetToOneBasedOrdinal()` for
      // details.
      result = MakeResolvedJoinScan(output_columns, gsql_join_type,
      std::move(left_scan), std::move(right_scan), std::move(gsql_join_expr));
    }
  } else {
    /// Now we're in the normal table-scan case.
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<zetasql::ResolvedScan> right_scan,
                     BuildGsqlResolvedScanForTableExpression(
                         *(join_expr.rarg), rtable, external_scope,
                         &scope_for_rhs, &scope_for_rhs));

    // Merge the lhs and rhs into output_scope.
    output_scope->MergeFrom(output_scope_lhs);
    output_scope->MergeFrom(scope_for_rhs);

    // The to-be-built JoinScan uses all columns from the left and right scans.
    std::vector<zetasql::ResolvedColumn> columns(
        left_scan->column_list().begin(), left_scan->column_list().end());
    columns.insert(columns.end(), right_scan->column_list().begin(),
                   right_scan->column_list().end());

    // Build a join expression representing an ON or USING clause. Note that
    // both PostgreSQL and ZetaSQL represent these clauses the same way, so
    // the transformer can just take a PostgreSQL Expr and transform it to a
    // ZetaSQL ResolvedExpr.
    VarIndexScope join_expr_scope(external_scope, *output_scope);
    std::unique_ptr<const zetasql::ResolvedExpr> gsql_join_expr;
    if (join_expr.quals != nullptr) {
      ZETASQL_ASSIGN_OR_RETURN(
          gsql_join_expr,
          BuildGsqlResolvedScalarExpr(*PostgresConstCastToExpr(join_expr.quals),
                                      &join_expr_scope, "JOIN clause"));
    }
    // A join expression can only be null if gsql_join_type is INNER.
    if (gsql_join_type != zetasql::ResolvedJoinScan::INNER) {
      ZETASQL_RET_CHECK(gsql_join_expr != nullptr);
    }
    result = MakeResolvedJoinScan(columns, gsql_join_type, std::move(left_scan),
                             std::move(right_scan), std::move(gsql_join_expr));
  }

  if (join_expr.joinHints != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(
        std::vector<std::unique_ptr<const zetasql::ResolvedOption>> hint_list,
        BuildGsqlResolvedOptionList(*join_expr.joinHints, external_scope));
    result->set_hint_list(std::move(hint_list));
  }

  // The VarIndex -> ResolvedColumn map is used when transforming PostgreSQL Var
  // expressions that reference RangeTblEntry objects. The Var expressions
  // typically reference a RangeTblEntry that represents an actual table or a
  // named subquery.
  //
  // However, `SELECT *` queries with a JoinExpr will also have Var expressions
  // that reference the RangeTblEntry representing the JoinExpr.
  //
  // Populate the map with the JoinExpr/JoinScan data now so that the Vars can
  // be successfully transformed later.
  RangeTblEntry* rte = rt_fetch(join_expr.rtindex, &rtable);
  ZETASQL_RETURN_IF_ERROR(
      MapVarIndexToColumnForJoin(*rte, join_expr.rtindex, output_scope));
  return result;
}

std::unique_ptr<zetasql::ResolvedJoinScan>
ForwardTransformer::BuildGsqlCommaJoinResolvedJoinScan(
    std::unique_ptr<zetasql::ResolvedScan> left_scan,
    std::unique_ptr<zetasql::ResolvedScan> right_scan) {
  std::vector<zetasql::ResolvedColumn> columns(
      left_scan->column_list().begin(), left_scan->column_list().end());
  columns.insert(columns.end(), right_scan->column_list().begin(),
                 right_scan->column_list().end());
  return MakeResolvedJoinScan(columns, zetasql::ResolvedJoinScan::INNER,
                              std::move(left_scan), std::move(right_scan),
                              /*join_expr=*/nullptr);
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedScan>>
ForwardTransformer::BuildGsqlResolvedScanForFromList(
    const List& fromlist, const List& rtable,
    const VarIndexScope* external_scope, VarIndexScope* output_scope) {
  std::unique_ptr<zetasql::ResolvedScan> current_tree;
  bool contains_explicit_join = false;

  // If there are more than one Node in the list, then build nested
  // ResolvedJoinScan objects, with the first node in the innermost JoinScan.
  // The result is a left-deep tree.
  for (Node* listnode : StructList<Node*>(&fromlist)) {
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<zetasql::ResolvedScan> current_scan,
        BuildGsqlResolvedScanForTableExpression(
            *listnode, rtable, external_scope, external_scope, output_scope));

    // Connect the newly built JoinScan to the current tree, forming a left-deep
    // join tree.
    if (current_tree != nullptr) {
      // See if we should combine this comma join into the unnest (ArrayScan) we
      // just created. ZetaSQL requires that we do this due to name visibility
      // such as in the following query:
      //   ... FROM <table>, UNNEST(<table>.array_col)
      // If we resolved this as a comma join between a TableScan and ArrayScan,
      // <table> is not visible to ArrayScan because they are on different
      // subtrees:
      //
      // JoinScan
      // - left input: TableScan(<table>)
      // - right input: ArrayScan(<table>.array_col) --ERROR: cannot see <table>
      //
      // Instead, we follow ZetaSQL's model and move the TableScan under the
      // ArrayScan node (having ArrayScan perform the join operation):
      //
      // ArrayScan
      // - array: UNNEST(<table>.array_col)
      // - input scan: TableScan(<table>)
      //
      // Note: there are many edge cases and caveats not covered in this
      // example, but our comma join case can ignore those.
      if (current_scan->Is<zetasql::ResolvedArrayScan>()) {
        zetasql::ResolvedArrayScan* array_scan =
            current_scan->GetAs<zetasql::ResolvedArrayScan>();

        std::vector<zetasql::ResolvedColumn> output_columns(
            current_tree->column_list().begin(),
            current_tree->column_list().end());
        output_columns.insert(output_columns.end(),
                              array_scan->column_list().begin(),
                              array_scan->column_list().end());
        array_scan->set_column_list(output_columns);
        array_scan->set_input_scan(std::move(current_tree));
      } else {
        if (current_scan->Is<zetasql::ResolvedJoinScan>()) {
          contains_explicit_join = true;
        }
        // Call the helper function, but don't add the resulted JoinScan to
        // var_index_to_resolved_column_maps_, since it represents a comma
        // join, which is never referred to by any TargetEntry object.
        current_scan = BuildGsqlCommaJoinResolvedJoinScan(
            std::move(current_tree), std::move(current_scan));
      }
    }
    current_tree = std::move(current_scan);
  }
  is_mixed_joins_query_ = contains_explicit_join && fromlist.length > 1;
  return current_tree;
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedFilterScan>>
ForwardTransformer::BuildGsqlResolvedFilterScan(
    const Node& where_clause, const VarIndexScope* from_scan_scope,
    std::unique_ptr<zetasql::ResolvedScan> current_scan) {
  if (internal::IsExpr(where_clause)) {
    // reinterpret_cast is safe here because we have already checked that
    // where_clause is an Expr. We also can't use PostgresCastToExpr here
    // because it doesn't work on const objects.
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<zetasql::ResolvedExpr> resolved_where,
        BuildGsqlResolvedScalarExpr(reinterpret_cast<const Expr&>(where_clause),
                                    from_scan_scope, "WHERE clause"));

    const std::vector<zetasql::ResolvedColumn>& tmp_column_list =
        current_scan->column_list();
    return MakeResolvedFilterScan(tmp_column_list, std::move(current_scan),
                                  std::move(resolved_where));
  } else {
    return absl::UnimplementedError(
        absl::StrCat("Node type ", NodeTagToNodeString(nodeTag(&where_clause)),
                     " is unsupported in WHERE clauses."));
  }
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedLimitOffsetScan>>
ForwardTransformer::BuildGsqlResolvedLimitOffsetScan(
    const Node* limit_clause, const Node* offset_clause,
    std::unique_ptr<const zetasql::ResolvedScan> current_scan) {
  std::unique_ptr<const zetasql::ResolvedExpr> limit_expr;
  std::unique_ptr<const zetasql::ResolvedExpr> offset_expr;
  Node* offset_node = const_cast<Node*>(offset_clause);
  Node* limit_node = const_cast<Node*>(limit_clause);

  ZETASQL_RET_CHECK(limit_node != nullptr || offset_node != nullptr);

  // If a limit clause is provided then we validate it and also validate an
  // offset clause if it is provided.
  if (limit_node != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ValidateLimitOffset(limit_node, "LIMIT"));
    if (offset_node != nullptr) {
      ZETASQL_RETURN_IF_ERROR(ValidateLimitOffset(offset_node, "OFFSET"));
    }
  } else {
    if (IsA(offset_node, Param)) {
      // If OFFSET is a parameter then the value cannot be computed by Spangres.
      // In this case, Spangres cannot compute a valid LIMIT so we return an
      // error saying that the user must provide a LIMIT.
      return absl::UnimplementedError(
          "Queries containing parameters in the OFFSET clause must contain a "
          "LIMIT clause.");
    } else {
      ZETASQL_RETURN_IF_ERROR(ValidateLimitOffset(offset_node, "OFFSET"));
      // If OFFSET is not a parameter then the only other valid node type is a
      // Const. We can use that Const integer value to compute a valid LIMIT for
      // the user such that the LIMIT value + OFFSET value = INT64MAX.
      ZETASQL_ASSIGN_OR_RETURN(limit_node, BuildLimitConst(offset_node));
    }
  }
  // After we validate and possibly assign the limit and offset nodes, we build
  // the actual expressions.
  if (offset_node != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(offset_expr, BuildGsqlResolvedScalarExpr(
                                      *PostgresCastToExpr(offset_node),
                                      &empty_var_index_scope_, "OFFSET"));
  }
  ZETASQL_ASSIGN_OR_RETURN(limit_expr, BuildGsqlResolvedScalarExpr(
                                   *PostgresCastToExpr(limit_node),
                                   &empty_var_index_scope_, "LIMIT"));
  const std::vector<zetasql::ResolvedColumn>& column_list =
      current_scan->column_list();
  return zetasql::MakeResolvedLimitOffsetScan(
      column_list, std::move(current_scan), std::move(limit_expr),
      std::move(offset_expr));
}

absl::StatusOr<Node*> ForwardTransformer::BuildLimitConst(
    const Node* offset_node) {
  ZETASQL_RET_CHECK(IsA(offset_node, Const));
  const Const* offset_const = PostgresConstCastNode(Const, offset_node);
  int64_t limit_value = LONG_MAX - DatumGetInt64(offset_const->constvalue);
  ZETASQL_ASSIGN_OR_RETURN(Const * max_const,
                   internal::makeScalarConst(INT8OID, limit_value, false));
  return PostgresCastToNode(max_const);
}

// The Spangres analyzer will never hand us a const which is not an int8_t or a
// const parameter which is not an int8_t as it type casts for other types
// resulting in either an error or a FunctionCall instead of a Const or
// Parameter. Therefore, we can call ZETASQL_RET_CHECK when validating the
// const/parameter types.
absl::Status ForwardTransformer::ValidateLimitOffset(
    const Node* limitoffset_node, absl::string_view clause) const {
  if (IsA(limitoffset_node, Const)) {
    const Const* limitoffset_const =
        PostgresConstCastNode(Const, limitoffset_node);
    ZETASQL_RET_CHECK_EQ(limitoffset_const->consttype, INT8OID);

    // Check if LIMIT/OFFSET is NULL.
    if (limitoffset_const->constisnull == true) {
      return absl::UnimplementedError(absl::StrCat(
          clause, " clauses that are NULL values are not supported."));
    }

    const int64_t limitoffset_val =
        DatumGetInt64(limitoffset_const->constvalue);

    // Check if LIMIT/OFFSET is negative.
    if (limitoffset_val < 0) {
      return absl::UnimplementedError(absl::StrCat(
          clause, " clauses that are negative values are not supported."));
    }

    return absl::OkStatus();
  }

  if (IsA(limitoffset_node, Param)) {
    const Param* limitoffset_param =
        PostgresConstCastNode(Param, limitoffset_node);
    ZETASQL_RET_CHECK_EQ(limitoffset_param->paramtype, INT8OID);
    return absl::OkStatus();
  }

  return absl::UnimplementedError(
      absl::StrCat(clause,
                   " clauses that are not integer literals or parameters are "
                   "not supported."));
}

std::unique_ptr<zetasql::ResolvedProjectScan>
ForwardTransformer::AddGsqlProjectScanForComputedColumns(
    std::unique_ptr<const zetasql::ResolvedScan> input_scan,
    std::vector<std::unique_ptr<const zetasql::ResolvedComputedColumn>>
        expr_list) {
  std::vector<zetasql::ResolvedColumn> project_scan_columns(
      input_scan->column_list());
  project_scan_columns.reserve(input_scan->column_list().size() +
                               expr_list.size());
  for (auto& computed_column : expr_list) {
    project_scan_columns.push_back(computed_column->column());
  }
  return zetasql::MakeResolvedProjectScan(
      project_scan_columns, std::move(expr_list), std::move(input_scan));
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedProjectScan>>
ForwardTransformer::AddGsqlProjectScanForInSubqueries(
    std::unique_ptr<const zetasql::ResolvedScan> subquery) {
  // As of now, this is only supported for single column IN subquery expressions
  // that require an additional query layer for comparisons.
  // The additional query layer will turn `select x FROM t` into
  // `select comparison_function(x) FROM (select x FROM t)`
  ZETASQL_RET_CHECK_EQ(subquery->column_list_size(), 1);
  const zetasql::ResolvedColumn subquery_column = subquery->column_list(0);

  // Create a computed column for the comparison_function expression.
  // First build the wrapped expression. Then allocate a column_id for the
  // new column and create a ResolvedColumn with the same alias as the subquery
  // column and the type from the wrapped expression.
  // Finally, combine the expression and column in a ResolvedComputedColumn.
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<zetasql::ResolvedColumnRef> column_ref,
                   BuildGsqlResolvedColumnRef(subquery_column));
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<zetasql::ResolvedExpr> comparison_expr,
      catalog_adapter_->GetEngineSystemCatalog()->GetResolvedExprForComparison(
          std::move(column_ref),
          catalog_adapter_->analyzer_options().language()));

  ZETASQL_ASSIGN_OR_RETURN(
      zetasql::ResolvedColumn column,
      BuildNewGsqlResolvedColumn(kAnonymousExprSubquery, subquery_column.name(),
                                 comparison_expr->type()));
  std::unique_ptr<const zetasql::ResolvedComputedColumn> computed_column =
      zetasql::MakeResolvedComputedColumn(column, std::move(comparison_expr));

  // Mark the new column as used in the query so that column pruning
  // doesn't remove it.
  RecordColumnAccess(column);

  // Create another query level with the comparison expression by wrapping
  // the subquery in a ProjectScan.
  std::vector<std::unique_ptr<const zetasql::ResolvedComputedColumn>>
      computed_columns;
  computed_columns.push_back(std::move(computed_column));
  return zetasql::MakeResolvedProjectScan(
      {column}, std::move(computed_columns), std::move(subquery));
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedScan>>
ForwardTransformer::BuildGsqlResolvedScanForFromClause(
    const Query& query, const VarIndexScope* external_scope,
    VarIndexScope* output_scope) {
  ZETASQL_RET_CHECK_NE(query.jointree, nullptr);
  if (query.jointree->fromlist != nullptr) {
    return BuildGsqlResolvedScanForFromList(
        *query.jointree->fromlist, *query.rtable, external_scope, output_scope);
  } else {
    // No-from-clause query has special rules about what else can exist.
    // These errors are modeled after the errors for no-from-clause queries in
    // the ZetaSQL Resolver::ResolveFromClauseAndCreateScan function.
    if (query.jointree->quals != nullptr) {
      return absl::UnimplementedError(
          "Queries containing a WHERE clause without a FROM clause are not "
          "supported");
    }

    if (query.distinctClause != nullptr) {
      return absl::UnimplementedError(
          "Queries containing a DISTINCT clause without a FROM clause are not "
          "supported");
    }

    if (query.groupClause != nullptr) {
      return absl::UnimplementedError(
          "Queries containing a GROUP BY clause without a FROM clause are not "
          "supported");
    }

    if (query.havingQual != nullptr) {
      return absl::UnimplementedError(
          "Queries containing a HAVING clause without a FROM clause are not "
          "supported");
    }

    if (query.windowClause != nullptr) {
      return absl::UnimplementedError(
          "Queries containing a WINDOW clause without a FROM clause are not "
          "supported");
    }

    if (query.sortClause != nullptr) {
      return absl::UnimplementedError(
          "Queries containing an ORDER BY clause without a FROM clause are not "
          "supported");
    }

    // SELECT with no FROM clause is a single-row SELECT.
    return zetasql::MakeResolvedSingleRowScan();
  }
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedScan>>
ForwardTransformer::BuildGsqlResolvedScanForQueryExpression(
    const Query& query, bool is_top_level_query, const VarIndexScope* scope,
    absl::string_view alias, std::vector<NamedColumn>* output_name_list) {
  if (query.setOperations == nullptr) {
    return BuildGsqlResolvedScanForSelect(query, is_top_level_query, scope,
                                          alias, output_name_list);
  } else {
    std::unique_ptr<zetasql::ResolvedScan> current_scan;
    VarIndexScope output_scope;
    ZETASQL_ASSIGN_OR_RETURN(current_scan, BuildGsqlResolvedSetOperationScan(
                                       query, scope, is_top_level_query,
                                       output_name_list, &output_scope));
    if (list_length(query.sortClause) > 0) {
      auto temporary_index_to_targetentry_map =
          TemporaryVectorElement(ressortgroupref_to_target_entry_maps_);
      VarIndexScope query_expression_scope(scope, output_scope);

      ZETASQL_RETURN_IF_ERROR(BuildSortGroupIndexToTargetEntryMap(query.targetList));
      ZETASQL_ASSIGN_OR_RETURN(current_scan,
                       BuildGsqlResolvedOrderByScanAfterSetOperation(
                           query.sortClause, &query_expression_scope,
                           std::move(current_scan)));
    }

    if (query.limitOffset != nullptr || query.limitCount != nullptr) {
      ZETASQL_ASSIGN_OR_RETURN(current_scan, BuildGsqlResolvedLimitOffsetScan(
                                         query.limitCount, query.limitOffset,
                                         std::move(current_scan)));
    }

    return current_scan;
  }
}

// ArrayScan is used by ZetaSQL to also handle joins where an input looks like
// an array (including repeated proto values).
absl::StatusOr<std::unique_ptr<zetasql::ResolvedArrayScan>>
ForwardTransformer::BuildGsqlResolvedArrayScan(
    const RangeTblEntry& rte, Index rtindex,
    const VarIndexScope* external_scope, VarIndexScope* output_scope) {
  // Caller has already verified that this is an UNNEST function call. We only
  // need to verify it's a supported kind of UNNEST.
  // Validate inputs: support for single function calls only.
  if (list_length(rte.functions) > 1) {
    // TODO: If we support WITH ORDINALITY but not multiple
    // unnest, suggest that customers join on the ordinality column to achieve
    // the same result.

    // PG's analyzer transforms UNNEST(a, b) the same as
    // ROWS FROM (UNNEST(a), UNNEST(b)), so we'll cover both in our error.
    return absl::InvalidArgumentError(
        "UNNEST of multiple arrays and ROWS FROM expressions are not "
        "supported");
  }
  FuncExpr* func_expr = PostgresCastNode(
      FuncExpr, linitial_node(RangeTblFunction, rte.functions)->funcexpr);
  if (list_length(func_expr->args) > 1) {
    return absl::InvalidArgumentError(
        "UNNEST of multiple arrays is not supported");
  }
  ZETASQL_RET_CHECK_GE(list_length(func_expr->args), 1);
  Expr* array_input_expr =
      internal::PostgresCastToExpr(linitial(func_expr->args));
  ZETASQL_RET_CHECK_NE(array_input_expr, nullptr);

  // If the UNNEST call depends on a Table prior to it in the comma join, we
  // need to combine the external_scope and output_scope so that the UNNEST
  // can access VarIndex's from the Table in the output_scope.
  // If there is no Table prior to the UNNEST call in the comma join,
  // output_scope will be empty, which is ok.
  VarIndexScope expr_scope(external_scope, *output_scope);
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<zetasql::ResolvedExpr> resolved_array_expr_argument,
      BuildGsqlResolvedScalarExpr(*array_input_expr, &expr_scope, "UNNEST"));
  // Sanity check: input must be an array. PG's analyzer should have already
  // checked this for us, but verify in case they support UNNEST for non-array
  // types someday.
  if (!resolved_array_expr_argument->type()->IsArray()) {
    return absl::InvalidArgumentError("Input to UNNEST must be an array");
  }

  // Ok, done validating input, build the array scan.

  // Build element_column and register in the scope
  ZETASQL_RET_CHECK_NE(rte.eref, nullptr);
  ZETASQL_RET_CHECK_GE(list_length(rte.eref->colnames), 1);
  Node* column_name =
      internal::PostgresCastToNode(list_nth(rte.eref->colnames, 0));
  ZETASQL_RET_CHECK(IsA(column_name, String));
  std::string column_name_str(strVal(column_name));
  ZETASQL_ASSIGN_OR_RETURN(
      const zetasql::ResolvedColumn array_element_column,
      BuildNewGsqlResolvedColumn(
          rte.eref->aliasname, column_name_str,
          resolved_array_expr_argument->type()->AsArray()->element_type()));
  // We register one output Var under the assumption that UNNEST always produces
  // exactly one output column from this RTE. This is true as long as we are
  // unnesting only a single array (checked above) and not supporting WITH
  // ORDINALITY/WITH OFFSET (checked below).
  output_scope->MapVarIndexToColumn({.varno = rtindex, .varattno = 1},
                                    array_element_column,
                                    /*allow_override=*/true);

  // Placeholder for explicit joins
  // Build output column_list. If there is an input scan (when array comes
  // from an explicit join), initialize from there. Else create a new one.
  // TODO: Handle explicit joins.
  std::unique_ptr<const zetasql::ResolvedScan> resolved_input_scan;
  zetasql::ResolvedColumnList output_column_list;
  if (resolved_input_scan != nullptr) {
    output_column_list = resolved_input_scan->column_list();
  }
  output_column_list.emplace_back(array_element_column);

  // Handle WITH ORDINALITY mapping to GSQL WITH OFFSET.
  std::unique_ptr<zetasql::ResolvedColumnHolder> array_offset_column;
  if (rte.funcordinality) {
    ZETASQL_RET_CHECK_NE(rte.eref, nullptr);
    ZETASQL_RET_CHECK_GE(list_length(rte.eref->colnames), 2);
    Node* ordinality_column_name =
        internal::PostgresCastToNode(list_nth(rte.eref->colnames, 1));
    ZETASQL_RET_CHECK(IsA(ordinality_column_name, String));
    std::string ordinality_column_name_str(strVal(ordinality_column_name));

    ZETASQL_ASSIGN_OR_RETURN(const zetasql::ResolvedColumn ordinality_column,
                     BuildNewGsqlResolvedColumn(rte.eref->aliasname,
                                                ordinality_column_name_str,
                                                zetasql::types::Int64Type()));
    output_scope->MapVarIndexToColumn({.varno = rtindex, .varattno = 2},
                                      ordinality_column,
                                      /*allow_override=*/true);
    output_column_list.emplace_back(ordinality_column);
    array_offset_column =
        zetasql::MakeResolvedColumnHolder(ordinality_column);
  }

  // Placeholder for join condition expressions. No plans to support this today.
  std::unique_ptr<const zetasql::ResolvedExpr> resolved_condition;

  // is_outer_scan only applies when joining an array (ZetaSQLism).
  const bool is_outer_scan = false;

  return MakeResolvedArrayScan(
      output_column_list, std::move(resolved_input_scan),
      std::move(resolved_array_expr_argument), array_element_column,
      std::move(array_offset_column), std::move(resolved_condition),
      is_outer_scan);
}

// Resolve the TVF into a TVFScan following ZetaSQL's ResolveTVF with a few
// limitations (these cases cause an internal error):
//   - Templated (recursive) functions are not supported
//   - Value tables are not supported for the output schema
//   - Pseudo columns are not supported in the output schema
//   - TVF Arguments are scalar only (never relation or model)
absl::StatusOr<std::unique_ptr<zetasql::ResolvedTVFScan>>
ForwardTransformer::BuildGsqlResolvedTVFScan(
    const RangeTblEntry& rte, Index rtindex,
    const VarIndexScope* external_scope,
    const zetasql::TableValuedFunction* tvf_catalog_entry,
    VarIndexScope* output_scope) {
  ZETASQL_RET_CHECK_NE(tvf_catalog_entry, nullptr);
  ZETASQL_RET_CHECK_EQ(tvf_catalog_entry->NumSignatures(), 1);

  // Caller already verified the structure here is correct.
  const FuncExpr* func_expr = PostgresConstCastNode(
      FuncExpr, linitial_node(RangeTblFunction, rte.functions)->funcexpr);
  ZETASQL_RET_CHECK_NE(func_expr, nullptr);

  // Transform the TVF arguments, match the signature, and generate the input
  // arguments for resolving the TVF's output signature.
  std::unique_ptr<zetasql::FunctionSignature> result_signature;
  std::vector<std::unique_ptr<const zetasql::ResolvedFunctionArgument>>
      resolved_tvf_args;
  std::vector<zetasql::TVFInputArgumentType> tvf_input_arguments;
  ZETASQL_RETURN_IF_ERROR(PrepareTVFInputArguments(
      *func_expr, tvf_catalog_entry, external_scope, &result_signature,
      &resolved_tvf_args, &tvf_input_arguments));

  std::shared_ptr<zetasql::TVFSignature> tvf_signature;
  // Note for non-Spanner engines: Spanner makes *very* limited use of TVF
  // complexity. The TVF this code was tested against had only a single, fixed
  // output signature and ignores most of the arguments to Resolve() below. They
  // are passed faithfully, but testing (and support) of more complex TVF
  // resolution rules is limited as a result.
  ZETASQL_RETURN_IF_ERROR(tvf_catalog_entry->Resolve(
      &catalog_adapter().analyzer_options(), tvf_input_arguments,
      *result_signature, catalog_adapter().GetEngineUserCatalog(),
      GetTypeFactory(), &tvf_signature));
  ZETASQL_RET_CHECK(!tvf_signature->result_schema().is_value_table());
  ZETASQL_RET_CHECK_NE(tvf_signature->result_schema().num_columns(), 0);

  std::vector<zetasql::ResolvedColumn> resolved_column_list;
  resolved_column_list.reserve(tvf_signature->result_schema().num_columns());
  for (int i = 0; i < tvf_signature->result_schema().num_columns(); ++i) {
    const zetasql::TVFRelation::Column& column =
        tvf_signature->result_schema().column(i);
    ZETASQL_RET_CHECK(!column.is_pseudo_column);
    ZETASQL_ASSIGN_OR_RETURN(
        zetasql::ResolvedColumn resolved_column,
        BuildNewGsqlResolvedColumn(
            tvf_catalog_entry->FullName(),
            !column.name.empty() ? column.name : absl::StrCat("$col", i),
            column.type));

    // Add each column to the var_index_scope.
    // Fail if the entry already exists.
    ZETASQL_RET_CHECK(output_scope->MapVarIndexToColumn(
        {.varno = rtindex, .varattno = i + 1}, resolved_column));

    resolved_column_list.push_back(std::move(resolved_column));
  }

  std::vector<int> column_index_list(
      tvf_signature->result_schema().columns().size());
  // Fill column_index_list with 0, 1, 2, ..., column_list.size()-1.
  std::iota(column_index_list.begin(), column_index_list.end(), 0);

  std::string alias = rte.alias == nullptr ? "" : rte.alias->aliasname;
  return zetasql::MakeResolvedTVFScan(
      resolved_column_list, tvf_catalog_entry, tvf_signature,
      std::move(resolved_tvf_args), column_index_list, alias);
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedScan>>
ForwardTransformer::BuildGsqlResolvedScanForSelect(
    const Query& query, bool is_top_level_query,
    const VarIndexScope* external_scope, absl::string_view alias,
    std::vector<NamedColumn>* output_name_list) {
  // Returns an unimplemented error if a query feature is not yet supported.
  // Query features that are partially supported are handled seperately.
  ZETASQL_RETURN_IF_ERROR(CheckForUnsupportedFeatures(query, is_top_level_query));

  // Build a new map for this level of Query.
  auto temporary_index_to_targetentry_map =
      TemporaryVectorElement(ressortgroupref_to_target_entry_maps_);

  VarIndexScope from_scan_output_scope;

  // Postgres Query corresponds to nested ResolvedScan in ZetaSQL AST.
  std::unique_ptr<zetasql::ResolvedScan> current_scan = nullptr;
  ZETASQL_ASSIGN_OR_RETURN(current_scan,
                   BuildGsqlResolvedScanForFromClause(query, external_scope,
                                                      &from_scan_output_scope));

  VarIndexScope from_scan_scope(external_scope, from_scan_output_scope);
  // The WHERE clause depends only on the FROM clause, so we resolve it before
  // looking at the SELECT-list or GROUP BY.
  if (query.jointree->quals != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(
        current_scan,
        BuildGsqlResolvedFilterScan(*(query.jointree->quals), &from_scan_scope,
                                    std::move(current_scan)));
  }

  auto transformer_info = std::make_unique<TransformerInfo>();
  transformer_info->set_has_group_by(list_length(query.groupClause) > 0);
  transformer_info->set_has_having(query.havingQual != nullptr);
  transformer_info->set_has_order_by(list_length(query.sortClause) > 0);
  transformer_info->set_has_select_distinct(list_length(query.distinctClause) >
                                            0);

  // Populate an internal map that keep track of sort group indexes and
  // PostgreSQL TargetEntry objects. The map is used to look up TargetEntry
  // from sort group indexes when we transform GROUP BY and ORDER BY items.
  if (transformer_info->has_order_by() || transformer_info->has_group_by()) {
    ZETASQL_RETURN_IF_ERROR(BuildSortGroupIndexToTargetEntryMap(query.targetList));
  }

  // Transform the TargetEntry expr for each select column in the target list.
  ZETASQL_RETURN_IF_ERROR(BuildGsqlSelectListResolvedExprsFirstPass(
      query.targetList, &from_scan_scope, transformer_info.get()));

  if (transformer_info->HasGroupByOrAggregation() &&
      transformer_info->HasHavingOrOrderBy()) {
    ZETASQL_RETURN_IF_ERROR(BuildGsqlSelectColumnsToPrecomputeBeforeAggregation(
        transformer_info.get()));
  }

  // Transform GROUP BY column exprs
  if (transformer_info->has_group_by()) {
    ZETASQL_RETURN_IF_ERROR(BuildGsqlGroupByList(query.groupClause, &from_scan_scope,
                                         transformer_info.get()));
  }

  if (!transformer_info->HasGroupByOrAggregation()) {
    // There is no GROUP BY, and no aggregation functions in the
    // SELECT list, so the initial resolution pass on the SELECT list is
    // final.  This will create ResolvedColumns for the SELECT columns, and
    // identify any columns necessary to precompute.
    ZETASQL_RETURN_IF_ERROR(FinalizeSelectListTransformState(
        alias, transformer_info.get(),
        transformer_info->select_list_transform_state()));
  }

  // Do a second pass on resolving SELECT list expressions. This second pass
  // accounts for dependencies between the SELECT list and GROUP BY clause.
  //
  // The from_clause_or_group_by_scope is derived from the FROM clause scope but
  // reflects what Vars are and are not available post-GROUP BY by invalidating
  // all column references that are not GROUPED BY and overriding all column
  // references that are GROUPED BY to the GROUPED versions of the columns.
  std::unique_ptr<const VarIndexScope> group_by_scope;
  const VarIndexScope* from_clause_or_group_by_scope = &from_scan_scope;
  if (transformer_info->HasGroupByOrAggregation()) {
    group_by_scope = from_scan_scope.CreatePostGroupByScope(
        transformer_info->group_by_map());
    from_clause_or_group_by_scope = group_by_scope.get();
  }

  std::vector<NamedColumn> final_project_name_list;
  ZETASQL_RETURN_IF_ERROR(BuildGsqlSelectListResolvedExprsSecondPass(
      query.targetList, alias, from_clause_or_group_by_scope,
      &final_project_name_list, transformer_info.get()));

  if (output_name_list != nullptr) {
    *output_name_list = final_project_name_list;
  }

  // Create two more scopes for HAVING and ORDER BY clauses.
  // The having_and_order_by_scope is for general HAVING and ORDER BY
  // transformation and the select_list_and_from_scan_scope is for
  // transforming aggregate functions in the HAVING and ORDER BY clauses.
  //
  // The having_and_order_by_scope is derived from the GROUP BY scope but
  // overrides column references to the SELECT list post-grouping versions of
  // the columns.
  // The select_list_and_from_scan_scope is derived from the
  // from_scan_scope but overrides column references to the SELECT list
  // pre-grouping versions of the columns.
  VarIndexMap having_and_order_by_override;
  VarIndexMap select_list_and_from_scan_override;
  for (const std::unique_ptr<SelectColumnTransformState>& select_column_state :
       transformer_info->select_list_transform_state()
           ->select_column_state_list()) {
    const TargetEntry* entry = select_column_state->target_entry;
    if (IsA(entry->expr, Var)) {
      Var* var = PostgresCastNode(Var, entry->expr);
      VarIndex var_index = {.varno = var->varno, .varattno = var->varattno};
      having_and_order_by_override.insert(
          {var_index, select_column_state->resolved_select_column});
      select_list_and_from_scan_override.insert(
          {var_index,
           select_column_state->resolved_pre_group_by_select_column});
    }
  }
  std::unique_ptr<VarIndexScope> having_and_order_by_scope =
      from_clause_or_group_by_scope->CopyVarIndexScopeWithOverridingMap(
          having_and_order_by_override);
  std::unique_ptr<VarIndexScope> select_list_and_from_scan_scope =
      from_scan_scope.CopyVarIndexScopeWithOverridingMap(
          select_list_and_from_scan_override);

  std::unique_ptr<const zetasql::ResolvedExpr> resolved_having_expr = nullptr;
  if (query.havingQual != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(
        resolved_having_expr,
        BuildGsqlHavingClause(*PostgresCastToExpr(query.havingQual),
                              having_and_order_by_scope.get(),
                              select_list_and_from_scan_scope.get(),
                              transformer_info.get()));
  }

  // The ZetaSQL analyzer typically does some additional computation for
  // aggregate functions, GROUP BY clauses, ORDER BY clauses, and Analytic
  // functions here.
  // If we have SELECT DISTINCT, we will transform the ORDER BY expressions
  // after transforming the DISTINCT since we must transform the ORDER BY
  // against post-DISTINCT versions of columns.
  if (transformer_info->has_order_by() &&
      !transformer_info->has_select_distinct()) {
    ZETASQL_RETURN_IF_ERROR(BuildGsqlOrderByList(
        query.sortClause, having_and_order_by_scope.get(),
        select_list_and_from_scan_scope.get(),
        /*is_post_distinct=*/false, transformer_info.get()));
  }

  // We are done with analysis and can now build the remaining scans.
  // The <current_scan> covers the FROM and WHERE clauses.  The remaining
  // scans are built on top of the <current_scan>.
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<zetasql::ResolvedScan> output_scan,
      BuildGsqlRemainingScansForSelect(
          query.sortClause, query.limitCount, query.limitOffset,
          having_and_order_by_scope.get(), std::move(resolved_having_expr),
          std::move(current_scan), transformer_info.get(), output_name_list));

  // Any columns produced in a SELECT list (for the final query or any subquery)
  // count as referenced and cannot be pruned.
  RecordColumnAccess(output_scan->column_list());

  return output_scan;
}

bool ForwardTransformer::IsSetReturningFunction(const Expr* expr) const {
  if (expr->type != T_FuncExpr) {
    return false;
  }
  return PostgresConstCastNode(FuncExpr, expr)->funcretset;
}

absl::Status ForwardTransformer::BuildGsqlSelectListResolvedExprsFirstPass(
    const List* targetList, const VarIndexScope* from_scan_scope,
    TransformerInfo* transformer_info) {
  for (TargetEntry* entry : StructList<TargetEntry*>(targetList)) {
    if (entry->resjunk) {
      // This is not a select list column.
      continue;
    }

    if (IsSetReturningFunction(entry->expr)) {
      // Set-returning functions are allowed in SELECT in PostgreSQL, but not in
      // ZetaSQL. We check this before looking up the function to give a
      // better error message when the function is valid in another context
      // (e.g. UNNEST).
      return absl::InvalidArgumentError(
          "Set-returning functions are not allowed in SELECT");
    }

    // Build an ExprTransformerInfo that allows aggregation and transform the
    // expr.
    zetasql::IdString column_name =
        catalog_adapter_->analyzer_options().id_string_pool()->Make(
            entry->resname);
    ExprTransformerInfo expr_transformer_info =
        ExprTransformerInfo::ForAggregation(from_scan_scope, transformer_info,
                                            entry->expr, column_name);
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const zetasql::ResolvedExpr> resolved_expr,
        BuildGsqlResolvedExpr(*entry->expr, &expr_transformer_info));

    // Track the select column in transformer_info.
    SelectColumnTransformState* select_column_state =
        transformer_info->select_list_transform_state()->AddSelectColumn(
            entry, column_name, expr_transformer_info.has_aggregation,
            std::move(resolved_expr));

    // Aggregate functions are pulled into the aggregate column list of the
    // AggregateScan and the select column is a ColumnRef pointing to the
    // aggregate function call.
    if (select_column_state->resolved_expr
            ->Is<zetasql::ResolvedAggregateFunctionCall>()) {
      ZETASQL_RET_CHECK_EQ(entry->expr->type, T_Aggref);
      ZETASQL_ASSIGN_OR_RETURN(
          select_column_state->resolved_expr,
          BuildGsqlAggregateColumnRef(
              select_column_state->alias, PostgresCastNode(Aggref, entry->expr),
              std::move(select_column_state->resolved_expr), transformer_info));
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedColumnRef>>
ForwardTransformer::BuildGsqlAggregateColumnRef(
    zetasql::IdString column_name, const Aggref* aggregate_func_expr,
    std::unique_ptr<const zetasql::ResolvedExpr> aggregate_expr,
    TransformerInfo* transformer_info) {
  ZETASQL_ASSIGN_OR_RETURN(int column_id, catalog_adapter_->AllocateColumnId());
  zetasql::IdString table_name =
      catalog_adapter_->analyzer_options().id_string_pool()->Make(
          kAnonymousAggregateQuery);
  zetasql::ResolvedColumn aggregate_column(column_id, table_name, column_name,
                                             aggregate_expr->annotated_type());

  transformer_info->AddAggregateComputedColumn(
      PostgresConstCastToExpr(aggregate_func_expr),
      zetasql::MakeResolvedComputedColumn(aggregate_column,
                                            std::move(aggregate_expr)));
  return BuildGsqlResolvedColumnRef(aggregate_column);
}

absl::Status
ForwardTransformer::BuildGsqlSelectColumnsToPrecomputeBeforeAggregation(
    TransformerInfo* transformer_info) {
  for (const std::unique_ptr<SelectColumnTransformState>& select_column_state :
       transformer_info->select_list_transform_state()
           ->select_column_state_list()) {
    // If the column has aggregate functions, then we do not compute this
    // before the AggregateScan.
    if (select_column_state->has_aggregation) {
      continue;
    }

    ZETASQL_RET_CHECK(select_column_state->resolved_expr != nullptr);
    zetasql::ResolvedColumn pre_group_by_column;
    if (select_column_state->resolved_expr
            ->Is<zetasql::ResolvedColumnRef>()) {
      // The expression already resolved to a column, so just use it.
      pre_group_by_column = select_column_state->resolved_expr
                                ->GetAs<zetasql::ResolvedColumnRef>()
                                ->column();
    } else {
      // The expression is not a simple column reference, it is a more
      // complicated expression that must be computed before aggregation
      // so that we can GROUP BY that computed column.
      ZETASQL_ASSIGN_OR_RETURN(int column_id, catalog_adapter_->AllocateColumnId());
      pre_group_by_column = zetasql::ResolvedColumn(
          column_id,
          catalog_adapter_->analyzer_options().id_string_pool()->Make(
              kPreGroupByPrefix),
          select_column_state->alias,
          select_column_state->resolved_expr->annotated_type());

      transformer_info->select_list_columns_to_compute_before_aggregation()
          ->push_back(zetasql::MakeResolvedComputedColumn(
              pre_group_by_column,
              std::move(select_column_state->resolved_expr)));
      // This column reference will be used when resolving the GROUP BY
      // expressions.
      ZETASQL_ASSIGN_OR_RETURN(select_column_state->resolved_expr,
                       BuildGsqlResolvedColumnRef(pre_group_by_column));
    }
    select_column_state->resolved_pre_group_by_select_column =
        pre_group_by_column;
  }

  return absl::OkStatus();
}

absl::Status ForwardTransformer::FinalizeSelectListTransformState(
    absl::string_view query_alias, TransformerInfo* transformer_info,
    SelectListTransformState* select_list_transform_state) {
  // For each select column, finalize its ResolvedColumn and if applicable,
  // its ResolvedComputedColumn.
  for (const std::unique_ptr<SelectColumnTransformState>& select_column_state :
       select_list_transform_state->select_column_state_list()) {
    if (select_column_state->resolved_expr
            ->Is<zetasql::ResolvedColumnRef>() &&
        !select_column_state->resolved_expr
             ->GetAs<zetasql::ResolvedColumnRef>()
             ->is_correlated()) {
      // The expression was already resolved to a column, just use it.
      const zetasql::ResolvedColumn& select_column =
          select_column_state->resolved_expr
              ->GetAs<zetasql::ResolvedColumnRef>()
              ->column();
      select_column_state->resolved_select_column = select_column;
    } else {
      // This is a computed column. Allocate a new column id and create its
      // ResolvedColumn.
      ZETASQL_ASSIGN_OR_RETURN(int column_id, catalog_adapter_->AllocateColumnId());
      zetasql::ResolvedColumn select_column(
          column_id,
          catalog_adapter_->analyzer_options().id_string_pool()->Make(
              query_alias),
          select_column_state->alias,
          select_column_state->resolved_expr->annotated_type());
      select_column_state->resolved_select_column = select_column;

      // Create the ResolvedComputedColumn using the Expr that was previously
      // transformed.
      std::unique_ptr<const zetasql::ResolvedComputedColumn>
          resolved_computed_column = zetasql::MakeResolvedComputedColumn(
              select_column, std::move(select_column_state->resolved_expr));
      select_column_state->resolved_computed_column =
          resolved_computed_column.get();
      transformer_info->select_list_columns_to_compute()->push_back(
          std::move(resolved_computed_column));
    }
  }
  return absl::OkStatus();
}

absl::Status ForwardTransformer::BuildGsqlSelectListResolvedExprsSecondPass(
    const List* targetList, absl::string_view query_alias,
    const VarIndexScope* group_by_scope,
    std::vector<NamedColumn>* final_project_name_list,
    TransformerInfo* transformer_info) {
  for (const std::unique_ptr<SelectColumnTransformState>& select_column_state :
       transformer_info->select_list_transform_state()
           ->select_column_state_list()) {
    ZETASQL_RETURN_IF_ERROR(BuildGsqlSelectColumnSecondPass(query_alias, group_by_scope,
                                                    select_column_state.get(),
                                                    transformer_info));
    // Some sanity checks.
    ZETASQL_RET_CHECK(select_column_state->GetType() != nullptr);
    ZETASQL_RET_CHECK(select_column_state->resolved_select_column.IsInitialized());

    // In the ZetaSQL analyzer, final_project_name_list is updated in
    // ResolveSelectColumnSecondPass, which is the basis for
    // ForwardTransformer::BuildGsqlSelectColumnSecondPass. However,
    // BuildGsqlSelectColumnSecondPass is structured a little differently from
    // ResolveSelectColumnSecondPass, making it easier to update
    // final_project_name_list here rather than in
    // BuildGsqlSelectColumnSecondPass.
    final_project_name_list->push_back(
        {select_column_state->alias,
         select_column_state->resolved_select_column});
  }
  return absl::OkStatus();
}

absl::Status ForwardTransformer::BuildGsqlSelectColumnSecondPass(
    absl::string_view query_alias, const VarIndexScope* group_by_scope,
    SelectColumnTransformState* select_column_state,
    TransformerInfo* transformer_info) {
  if (select_column_state->resolved_select_column.IsInitialized()) {
    // The column is already initialized, continue.
    return absl::OkStatus();
  }

  ExprTransformerInfo expr_transformer_info(
      group_by_scope, group_by_scope,
      /*allows_aggregation_in=*/true,
      transformer_info->HasGroupByOrAggregation(), "SELECT list",
      transformer_info, select_column_state->target_entry->expr,
      select_column_state->alias);

  // Try to get the corresponding GROUP BY column if it exists.
  // We need to compare the transformed expr instead of relying on the PG
  // ressortgroupref because of queries like
  // "select key + 1 as col1, key + 1 as col2 from keyvalue group by col1"
  // PostgreSQL creates a separate TargetEntry for each SELECT column and
  // only assigns a ressortgroupref to the first column so that the first
  // column is linked to the GROUP BY clause but the second column is not.
  // ZetaSQL links both columns to the GROUP BY clause.
  if (select_column_state->resolved_expr != nullptr) {
    bool found_group_by_expression = false;
    for (const std::unique_ptr<const zetasql::ResolvedComputedColumn>&
             resolved_computed_column :
         transformer_info->group_by_columns_to_compute()) {
      ZETASQL_ASSIGN_OR_RETURN(bool is_same_expr,
                       zetasql::IsSameExpressionForGroupBy(
                           select_column_state->resolved_expr.get(),
                           resolved_computed_column->expr()));
      if (is_same_expr) {
        // We matched this SELECT list expression to a GROUP BY
        // expression.
        // Update the select_column_state to point at the GROUP BY
        // computed column.
        select_column_state->resolved_select_column =
            resolved_computed_column->column();
        found_group_by_expression = true;
        break;
      }
    }
    if (found_group_by_expression) {
      return absl::OkStatus();
    }
  }

  // Re-transform the expr for this SELECT column so that we use the original
  // expression rather than the pre-aggregation expression, which may have been
  // wrapped in a ColumnRef in
  // `BuildGsqlSelectColumnsToPrecomputeBeforeAggregation`
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<const zetasql::ResolvedExpr> resolved_expr,
      BuildGsqlResolvedExpr(*select_column_state->target_entry->expr,
                            &expr_transformer_info));

  if (resolved_expr->Is<zetasql::ResolvedColumnRef>() &&
      !resolved_expr->GetAs<zetasql::ResolvedColumnRef>()->is_correlated()) {
    // The expression was already resolved to a column, just use it.
    const zetasql::ResolvedColumn& select_column =
        select_column_state->resolved_expr
            ->GetAs<zetasql::ResolvedColumnRef>()
            ->column();
    select_column_state->resolved_select_column = select_column;
    return absl::OkStatus();
  }

  // This is a computed column. Allocate a new column id and create its
  // ResolvedColumn.
  ZETASQL_ASSIGN_OR_RETURN(int column_id, catalog_adapter_->AllocateColumnId());
  zetasql::ResolvedColumn select_column(
      column_id,
      catalog_adapter_->analyzer_options().id_string_pool()->Make(query_alias),
      select_column_state->alias,
      select_column_state->resolved_expr->annotated_type());
  select_column_state->resolved_select_column = select_column;

  // Create the ResolvedComputedColumn using the Expr that was previously
  // transformed.
  std::unique_ptr<const zetasql::ResolvedComputedColumn>
      resolved_computed_column = zetasql::MakeResolvedComputedColumn(
          select_column, std::move(resolved_expr));
  transformer_info->select_list_columns_to_compute()->push_back(
      std::move(resolved_computed_column));

  return absl::OkStatus();
}

absl::Status ForwardTransformer::BuildGsqlGroupByList(
    List* groupClause, const VarIndexScope* from_clause_scope,
    TransformerInfo* transformer_info) {
  // For each GROUP BY item, transform the expr and build the GROUP BY column
  // and computed column as needed.
  for (SortGroupClause* group_item :
       StructList<SortGroupClause*>(groupClause)) {
    // Get the TargetEntry associated with this sort item.
    ZETASQL_ASSIGN_OR_RETURN(ForwardTransformer::TargetEntryIndex target_entry_index,
                     GetPgSortGroupTargetEntry(group_item->tleSortGroupRef));
    const TargetEntry* entry = target_entry_index.target_entry;
    ZETASQL_ASSIGN_OR_RETURN(Oid group_column_pg_type,
                     CheckedPgExprType(PostgresCastToNode(entry->expr)));

    std::unique_ptr<const zetasql::ResolvedExpr> resolved_expr;
    zetasql::ResolvedColumn group_by_column;
    if (target_entry_index.select_index != 0) {
      // This is a GROUP BY column and a SELECT column. Get the current state
      // of the column.
      SelectColumnTransformState* select_column_state =
          transformer_info->GetSelectColumnState(
              target_entry_index.select_index);
      ZETASQL_RETURN_IF_ERROR(CheckTypeSupportsGrouping(
          group_column_pg_type, select_column_state->resolved_expr->type(),
          catalog_adapter_->analyzer_options().language()));

      // Build the ResolvedColumn for the GROUP BY column.
      ZETASQL_ASSIGN_OR_RETURN(
          group_by_column,
          BuildGsqlGroupBySelectColumn(select_column_state, transformer_info));

      // Take ownership of the old SELECT expr, which will be overridden by
      // the new GROUP BY expr and used to build the computed column.
      resolved_expr = std::move(select_column_state->resolved_expr);
      // We are grouping by a SELECT column so we must update the
      // SelectColumnTransformState to reflect it is being grouped by.
      ZETASQL_RETURN_IF_ERROR(UpdateGroupBySelectColumnTransformState(
          group_by_column, select_column_state));
    } else {
      // This is not a SELECT column.
      ZETASQL_RET_CHECK(entry->resjunk);

      // Transform the expression.
      ZETASQL_ASSIGN_OR_RETURN(resolved_expr,
                       BuildGsqlResolvedScalarExpr(
                           *entry->expr, from_clause_scope, "GROUP BY"));

      ZETASQL_RETURN_IF_ERROR(CheckTypeSupportsGrouping(
          group_column_pg_type, resolved_expr->type(),
          catalog_adapter_->analyzer_options().language()));

      // Build the GROUP BY column. When ORDER BY + GROUP BY are supported in
      // the same query, resolved_expr may be overwritten.
      // If the expr is a Var, override the VarIndex -> ResolvedColumn mapping
      // to point to the group_by_column.
      ZETASQL_ASSIGN_OR_RETURN(
          group_by_column,
          BuildGsqlGroupByColumn(entry, resolved_expr->type(),
                                 from_clause_scope, transformer_info));
    }

    if (IsA(entry->expr, Var)) {
      // Track the GROUP BY VarIndex -> ResolvedColumn mapping so that it can be
      // used in future GROUP BY scopes
      Var* var = PostgresCastNode(Var, entry->expr);
      transformer_info->AddGroupByVarIndexColumn(var, group_by_column);
    }

    if (resolved_expr == nullptr) {
      return absl::InternalError("Unexpected resolved_expr = nullptr");
    }

    transformer_info->AddGroupByComputedColumnIfNeeded(
        group_by_column, std::move(resolved_expr));
  }
  return absl::OkStatus();
}

absl::StatusOr<zetasql::ResolvedColumn>
ForwardTransformer::BuildGsqlGroupBySelectColumn(
    const SelectColumnTransformState* select_column_state,
    TransformerInfo* transformer_info) {
  // When there are redundant GROUP BY select columns, the ZetaSQL
  // analyzer de-duplicates them by looking for an equivalent computed column
  // and referencing it instead of creating a new column.
  // The Spangres forward transformer keeps all the duplicate columns separate.
  return BuildNewGsqlResolvedColumn(
      /*table_name=*/kGroupByPrefix,
      /*column_name=*/select_column_state->alias.ToStringView(),
      select_column_state->resolved_expr->type());
}

absl::Status ForwardTransformer::UpdateGroupBySelectColumnTransformState(
    const zetasql::ResolvedColumn& group_by_column,
    SelectColumnTransformState* select_column_state) {
  ZETASQL_ASSIGN_OR_RETURN(select_column_state->resolved_expr,
                   BuildGsqlResolvedColumnRef(group_by_column));
  select_column_state->is_group_by_column = true;
  // Update the SelectColumnTransformState to reflect the grouped by version
  // of the column.
  select_column_state->resolved_select_column = group_by_column;
  return absl::OkStatus();
}

absl::StatusOr<zetasql::ResolvedColumn>
ForwardTransformer::BuildGsqlGroupByColumn(
    const TargetEntry* entry, const zetasql::Type* resolved_expr_type,
    const VarIndexScope* from_clause_scope, TransformerInfo* transformer_info) {
  // ZetaSQL tries to look up a SELECT column here with the same expr as the
  // GROUP BY expr because ZetaSQL stores SELECT columns and GROUP BY
  // columns separately in its non-resolved AST. Since PostgreSQL stores the
  // SELECT and GROUP BY columns in the same TargetEntry, they have already
  // been matched and handled in BuildGsqlGroupBySelectColumn. The GROUP BY
  // columns here are not SELECT columns.

  // Assign a table name and column name that match ZetaSQL
  // behavior.
  std::string column_name;
  if (entry->resname != nullptr) {
    column_name = entry->resname;
  } else if (IsA(entry->expr, Var)) {
    // A Var TargetEntry means this a simple table column. ZetaSQL uses
    // the column name for the ResolvedComputedColumn built below. So we are
    // doing the same here. A ResolvedColumn should already be built for it,
    // retrieve the column and get its name.
    Var* var = PostgresCastNode(Var, entry->expr);
    ZETASQL_ASSIGN_OR_RETURN(const zetasql::ResolvedColumn old_column,
                     GetResolvedColumn(*from_clause_scope, var->varno,
                                       var->varattno, var->varlevelsup));
    column_name = old_column.name();
  } else {
    // This TargetEntry represents an expression. Generate a column name.
    int computed_column_index =
        transformer_info->group_by_columns_to_compute().size() + 1;
    column_name = absl::StrCat(kGroupByColumn, computed_column_index);
  }

  ZETASQL_ASSIGN_OR_RETURN(
      zetasql::ResolvedColumn group_by_column,
      BuildNewGsqlResolvedColumn(
          /*table_name=*/kGroupByPrefix, column_name, resolved_expr_type));

  return group_by_column;
}

// Modeled after ZetaSQL's Resolver::ResolveHavingExpr.
absl::StatusOr<std::unique_ptr<const zetasql::ResolvedExpr>>
ForwardTransformer::BuildGsqlHavingClause(
    const Expr& having_expr, const VarIndexScope* having_and_order_by_scope,
    const VarIndexScope* select_list_and_from_scan_scope,
    TransformerInfo* transformer_info) {
  // ZetaSQL does a lot of validation here about the presence of GROUP BY or
  // aggregation clauses. The PostgreSQL analyzer should have already validated
  // the statement so we can just transform the expr.
  ExprTransformerInfo expr_transformer_info(
      having_and_order_by_scope, select_list_and_from_scan_scope,
      /*allows_aggregation_in=*/true,
      transformer_info->HasGroupByOrAggregation(), "HAVING clause",
      transformer_info);
  return BuildGsqlResolvedExpr(having_expr, &expr_transformer_info);
}

absl::Status ForwardTransformer::BuildGsqlOrderByList(
    const List* sortClause, const VarIndexScope* having_and_order_by_scope,
    const VarIndexScope* select_list_and_from_scan_scope, bool is_post_distinct,
    TransformerInfo* transformer_info) {
  ExprTransformerInfo expr_transformer_info(
      having_and_order_by_scope, select_list_and_from_scan_scope,
      /*allows_aggregation_in=*/!is_post_distinct,
      transformer_info->HasGroupByOrAggregation(), "ORDER BY clause",
      transformer_info);

  // Transform the expr, null order, and sort direction for each ORDER BY
  // item.
  std::vector<OrderByItemTransformInfo>* order_by_item_list =
      transformer_info->mutable_order_by_item_transform_info();
  for (SortGroupClause* sort_item : StructList<SortGroupClause*>(sortClause)) {
    ZETASQL_ASSIGN_OR_RETURN(OrderByItemTransformInfo order_by_item,
                     BuildGsqlOrderByItem(*sort_item, &expr_transformer_info));
    order_by_item_list->emplace_back(std::move(order_by_item));
  }

  // Finalize the sort order list -- each column reference will have a
  // ResolvedColumn and each computed column will have a
  // ResolvedComputedColumn. For now, only column references are supported.
  ZETASQL_RETURN_IF_ERROR(FinalizeOrderByTransformState(
      transformer_info->mutable_order_by_item_transform_info(),
      transformer_info->order_by_columns_to_compute()));
  return absl::OkStatus();
}

absl::StatusOr<OrderByItemTransformInfo>
ForwardTransformer::BuildGsqlOrderByItem(
    const SortGroupClause& sort_item,
    ExprTransformerInfo* expr_transformer_info) {
  // Get the TargetEntry associated with this sort item.
  ZETASQL_ASSIGN_OR_RETURN(ForwardTransformer::TargetEntryIndex target_entry_index,
                   GetPgSortGroupTargetEntry(sort_item.tleSortGroupRef));
  const TargetEntry* entry = target_entry_index.target_entry;

  if (nodeTag(entry->expr) == T_Const && entry->resjunk == true) {
    return absl::UnimplementedError(
        "Literals are not supported as ORDER BY items");
  }

  ZETASQL_ASSIGN_OR_RETURN(Oid sort_item_type,
                   CheckedPgExprType(PostgresCastToNode(entry->expr)));
  ZETASQL_ASSIGN_OR_RETURN(const zetasql::Type* sort_item_gsql_type,
                   BuildGsqlType(sort_item_type));
  ZETASQL_RETURN_IF_ERROR(CheckTypeSupportsOrdering(
      sort_item_type, sort_item_gsql_type,
      catalog_adapter_->analyzer_options().language()));

  // Transform the null order.
  zetasql::ResolvedOrderByItemEnums::NullOrderMode null_order =
      zetasql::ResolvedOrderByItemEnums::ORDER_UNSPECIFIED;
  if (sort_item.nulls_first) {
    null_order = zetasql::ResolvedOrderByItemEnums::NULLS_FIRST;
  } else {
    null_order = zetasql::ResolvedOrderByItemEnums::NULLS_LAST;
  }

  // Transform the sort direction, inspired by PostgreSQL get_rule_orderby()
  ZETASQL_ASSIGN_OR_RETURN(const TypeCacheEntry* type_entry,
                   CheckedPgLookupTypeCache(
                       sort_item_type, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR));
  bool is_descending;
  if (sort_item.sortop == type_entry->lt_opr) {
    is_descending = false;
  } else if (sort_item.sortop == type_entry->gt_opr) {
    is_descending = true;
  } else {
    return absl::UnimplementedError(
        "Only < and > are supported operators for ORDER BY items.");
  }

  if (target_entry_index.select_index != 0) {
    // This is a SELECT column. Track the SELECT index since all SELECT exprs
    // have already been transformed and can be reused.
    ZETASQL_RET_CHECK(!entry->resjunk);
    return OrderByItemTransformInfo(target_entry_index.select_index,
                                    is_descending, null_order);
  } else {
    // This is not a SELECT column. Transform the expr.
    ZETASQL_RET_CHECK(entry->resjunk);
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<zetasql::ResolvedExpr> resolved_expr,
        BuildGsqlResolvedExpr(*entry->expr, expr_transformer_info));

    return OrderByItemTransformInfo(std::move(resolved_expr), is_descending,
                                    null_order);
  }
}

absl::Status ForwardTransformer::FinalizeOrderByTransformState(
    std::vector<OrderByItemTransformInfo>* order_by_info,
    std::vector<std::unique_ptr<const zetasql::ResolvedComputedColumn>>*
        computed_columns) {
  for (int order_by_item_idx = 0; order_by_item_idx < order_by_info->size();
       ++order_by_item_idx) {
    OrderByItemTransformInfo& item_info = (*order_by_info)[order_by_item_idx];

    if (item_info.select_list_index > 0) {
      // This is a SELECT column that has already been fully transformed.
      continue;
    }

    if (item_info.order_expression->Is<zetasql::ResolvedColumnRef>() &&
        !item_info.order_expression->GetAs<zetasql::ResolvedColumnRef>()
             ->is_correlated()) {
      // The expression was already resolved to a column, just use it.
      // Based on the ZetaSQL analyzer, all columns will be wrapped in a
      // ResolvedColumnRef when the OrderByScan is created. Non-computed
      // columns are extracted from their ResolvedColumnRef here so that they
      // aren't double wrapped later on.
      item_info.order_column =
          item_info.order_expression->GetAs<zetasql::ResolvedColumnRef>()
              ->column();
    } else {
      // This is a computed column.
      // Assign a table name and column name that match ZetaSQL behavior.
      std::string order_by_column_name =
          absl::StrCat(kOrderByColumn, order_by_item_idx + 1);
      ZETASQL_ASSIGN_OR_RETURN(item_info.order_column,
                       BuildNewGsqlResolvedColumn(
                           /*table_name=*/kOrderByPrefix, order_by_column_name,
                           item_info.order_expression->type()));
      computed_columns->emplace_back(zetasql::MakeResolvedComputedColumn(
          item_info.order_column, std::move(item_info.order_expression)));
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedScan>>
ForwardTransformer::BuildGsqlRemainingScansForSelect(
    const List* sort_clause, const Node* limit_clause,
    const Node* offset_clause, const VarIndexScope* having_and_order_by_scope,
    std::unique_ptr<const zetasql::ResolvedExpr> resolved_having_expr,
    std::unique_ptr<zetasql::ResolvedScan> current_scan,
    TransformerInfo* transformer_info,
    std::vector<NamedColumn>* output_name_list) {
  if (!transformer_info->select_list_columns_to_compute_before_aggregation()
           ->empty()) {
    current_scan = AddGsqlProjectScanForComputedColumns(
        std::move(current_scan),
        transformer_info
            ->release_select_list_columns_to_compute_before_aggregation());
  }

  if (transformer_info->HasGroupByOrAggregation()) {
    // Create an AggregateScan.
    ZETASQL_ASSIGN_OR_RETURN(current_scan,
                     BuildGsqlResolvedAggregateScan(std::move(current_scan),
                                                    transformer_info));
  }

  if (resolved_having_expr != nullptr) {
    // ZetaSQL allows the HAVING clause to reference SELECT aliases, and
    // pre-computes the aliases here. PostgreSQL does not allow the HAVING
    // clause to reference SELECT aliases so we can skip that step and just
    // build the ResolvedFilterScan.
    const auto& tmp_column_list = current_scan->column_list();
    current_scan = zetasql::MakeResolvedFilterScan(
        tmp_column_list, std::move(current_scan),
        std::move(resolved_having_expr));
  }

  if (transformer_info->has_select_distinct()) {
    // If there are (aliased or non-aliased) select list columns to compute
    // then add a projection first.
    if (!transformer_info->select_list_columns_to_compute()->empty()) {
      current_scan = AddGsqlProjectScanForComputedColumns(
          std::move(current_scan),
          transformer_info->release_select_list_columns_to_compute());
    }

    // Note: The DISTINCT processing is very similar to the GROUP BY
    // processing.  The output of GROUP BY is used for transforming subsequent
    // clauses and expressions (e.g., the SELECT list), and the output of
    // DISTINCT is used for transforming the subsequent ORDER BY expressions.
    ZETASQL_ASSIGN_OR_RETURN(current_scan, BuildGsqlResolvedSelectDistinct(
                                       std::move(current_scan),
                                       transformer_info, output_name_list));
  }

  if (transformer_info->has_order_by()) {
    if (transformer_info->has_select_distinct()) {
      // Check expected state.  If DISTINCT is present, then we already
      // computed any necessary SELECT list columns before processing the
      // DISTINCT.
      ZETASQL_RET_CHECK(transformer_info->select_list_columns_to_compute()->empty());

      // If DISTINCT is present, then the ORDER BY expressions have *not*
      // been resolved yet.  Resolve the ORDER BY expressions to reference
      // the post-DISTINCT versions of columns.  Note that the DISTINCT
      // processing already updated <transformer_info> with the
      // mapping from pre-DISTINCT to post-DISTINCT versions of columns
      // and expressions, so we simply need to resolve the ORDER BY
      // expressions with the updated <transformer_info> and
      // post-distinct VarIndexScope.  Resolution of ORDER BY expressions
      // against the output of DISTINCT has the same characteristics
      // as post-GROUP BY expression resolution.  ORDER BY expressions
      // resolve successfully to columns and path expressions that were
      // output from DISTINCT.  As such, any column reference that is
      // not in the SELECT list is an error.

      // Create a new VarIndexScope for what comes out of the DISTINCT
      // AggregateScan.  It is derived from the <having_and_order_by_scope>,
      // and allows column references to resolve to the post-DISTINCT versions
      // of the columns.
      std::unique_ptr<const VarIndexScope> distinct_scope;
      distinct_scope = having_and_order_by_scope->CreatePostGroupByScope(
          transformer_info->group_by_map());

      // The second 'distinct_scope' VarIndexScope argument is only
      // used for resolving the arguments to aggregate functions, but when
      // DISTINCT is present then aggregate functions are not allowed in
      // ORDER BY so we will always get an error regardless of whether or
      // not the name is visible post-DISTINCT.
      ZETASQL_RETURN_IF_ERROR(BuildGsqlOrderByList(
          sort_clause, distinct_scope.get(), distinct_scope.get(),
          /*is_post_distinct=*/true, transformer_info));
    } else {
      // DISTINCT is *not* present so we have already transformed the ORDER BY
      // expressions in BuildGsqlResolvedScanForSelect() and do not transform
      // them here.
      // Also, the ORDER BY might have computed columns to compute so
      // add a wrapper ProjectScan to compute them if necessary.
      if (!transformer_info->select_list_columns_to_compute()->empty()) {
        current_scan = AddGsqlProjectScanForComputedColumns(
            std::move(current_scan),
            transformer_info->release_select_list_columns_to_compute());
      }
    }

    // If there are computed SELECT columns, add a ProjectScan for them.
    if (!transformer_info->select_list_columns_to_compute()->empty()) {
      current_scan = AddGsqlProjectScanForComputedColumns(
          std::move(current_scan),
          transformer_info->release_select_list_columns_to_compute());
    }

    // If there are computed ORDER BY columns, add a ProjectScan for them.
    if (!transformer_info->order_by_columns_to_compute()->empty()) {
      current_scan = AddGsqlProjectScanForComputedColumns(
          std::move(current_scan),
          transformer_info->release_order_by_columns_to_compute());
    }

    ZETASQL_ASSIGN_OR_RETURN(current_scan,
                     BuildGsqlResolvedOrderByScan(
                         transformer_info->select_list_transform_state()
                             ->resolved_column_list(),
                         std::move(current_scan), transformer_info));
  }

  if (!transformer_info->has_order_by()) {
    // ZetaSQL always adds the ProjectScan if there is no ORDER BY
    // clause and no DISTINCT clause.
    // Spangres adds the ProjectScan if there is no ORDER BY clause. This means
    // that queries with a DISTINCT clause but no ORDER BY clause will look
    // different in Spangres than ZetaSQL, which is ok. An extra ProjectScan
    // in Spangres does not affect the semantics of the query and it helps
    // simplify the reverse transformer logic.
    current_scan = zetasql::MakeResolvedProjectScan(
        transformer_info->select_list_transform_state()->resolved_column_list(),
        transformer_info->release_select_list_columns_to_compute(),
        std::move(current_scan));
  }

  if (limit_clause != nullptr || offset_clause != nullptr) {
    // If a LIMIT or OFFSET value is provided, create a LimitOffsetScan.
    ZETASQL_ASSIGN_OR_RETURN(current_scan,
                     BuildGsqlResolvedLimitOffsetScan(
                         limit_clause, offset_clause, std::move(current_scan)));
  }
  return current_scan;
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedAggregateScan>>
ForwardTransformer::BuildGsqlResolvedAggregateScan(
    std::unique_ptr<zetasql::ResolvedScan> current_scan,
    TransformerInfo* transformer_info) {
  zetasql::ResolvedColumnList column_list;
  for (const std::unique_ptr<const zetasql::ResolvedComputedColumn>&
           group_by_column : transformer_info->group_by_columns_to_compute()) {
    column_list.push_back(group_by_column->column());
  }
  for (const std::unique_ptr<const zetasql::ResolvedComputedColumn>&
           aggregate_column :
       transformer_info->aggregate_columns_to_compute()) {
    column_list.push_back(aggregate_column->column());
  }

  // Aggregate features that are not supported yet, but required to construct
  // a ResolvedAggregateScan.
  std::vector<std::unique_ptr<const zetasql::ResolvedGroupingSet>>
      grouping_set_list;
  std::vector<std::unique_ptr<const zetasql::ResolvedColumnRef>>
      rollup_column_list;

  return zetasql::MakeResolvedAggregateScan(
      column_list, std::move(current_scan),
      transformer_info->release_group_by_columns_to_compute(),
      transformer_info->release_aggregate_columns_to_compute(),
      std::move(grouping_set_list), std::move(rollup_column_list));
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedAggregateScan>>
ForwardTransformer::BuildGsqlResolvedSelectDistinct(
    std::unique_ptr<zetasql::ResolvedScan> current_scan,
    TransformerInfo* transformer_info,
    std::vector<NamedColumn>* output_name_list) {
  // For DISTINCT processing, we will build and maintain a mapping from
  // SELECT list expressions to post-DISTINCT versions of columns.  This
  // mapping will be used when transforming ORDER BY expression after the
  // DISTINCT.

  // DISTINCT processing will re-use the GROUP BY information, so clear out
  // any old GROUP BY information that may exist.  But first, we must
  // preserve the mapping from SELECT list expressions to post-GROUP BY
  // columns so that we can update it during DISTINCT processing to capture the
  // mapping from SELECT list expressions to post-DISTINCT columns instead.
  transformer_info->ClearGroupByInfo();

  std::vector<std::unique_ptr<SelectColumnTransformState>>&
      select_column_state_list = transformer_info->select_list_transform_state()
                                     ->select_column_state_list();

  std::vector<NamedColumn> name_list;

  for (int column_pos = 0; column_pos < select_column_state_list.size();
       ++column_pos) {
    const std::unique_ptr<SelectColumnTransformState>& select_column_state =
        select_column_state_list.at(column_pos);
    const zetasql::ResolvedColumn& column =
        select_column_state->resolved_select_column;

    if (!select_column_state->GetType()->SupportsGrouping(
            catalog_adapter_->analyzer_options().language())) {
      ZETASQL_ASSIGN_OR_RETURN(Oid column_pg_type_oid,
                       CheckedPgExprType(PostgresCastToNode(
                           select_column_state->target_entry->expr)));
      ZETASQL_ASSIGN_OR_RETURN(const char* pg_type_name,
                       PgBootstrapCatalog::Default()->GetFormattedTypeName(
                           column_pg_type_oid));
      return absl::UnimplementedError(absl::StrFormat(
          "Column %s of type %s cannot be used in SELECT DISTINCT",
          column.name(), pg_type_name));
    }

    const zetasql::ResolvedExpr* resolved_expr =
        select_column_state->resolved_expr.get();
    if (resolved_expr == nullptr) {
      ZETASQL_RET_CHECK_NE(select_column_state->resolved_computed_column, nullptr);
      resolved_expr = select_column_state->resolved_computed_column->expr();
    }
    ZETASQL_RET_CHECK_NE(resolved_expr, nullptr);

    // When a query has duplicate columns in the DISTINCT clause, the ZetaSQL
    // analyzer de-duplicates them in the ResolvedAggregateScan by referencing
    // the first occurrence instead of creating a new column each time.
    // The Spangres forward transformer keeps all the duplicate columns separate
    // and always creates a new DISTINCT column.
    ZETASQL_ASSIGN_OR_RETURN(int column_id, catalog_adapter_->AllocateColumnId());
    zetasql::ResolvedColumn distinct_column = zetasql::ResolvedColumn(
        column_id,
        catalog_adapter_->analyzer_options().id_string_pool()->Make(
            kDistinctId),
        column.name_id(), column.annotated_type());
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const zetasql::ResolvedExpr> column_ref,
                     BuildGsqlResolvedColumnRef(column));
    // Add a computed column for the new post-DISTINCT column.
    transformer_info->AddGroupByComputedColumnIfNeeded(distinct_column,
                                                       std::move(column_ref));

    if (output_name_list != nullptr) {
      name_list.push_back(
          {output_name_list->at(column_pos).name, distinct_column});
    }

    // Update the SelectListColumnState with the new post-DISTINCT
    // ResolvedColumn information.
    select_column_state->resolved_select_column = distinct_column;

    if (IsA(select_column_state->target_entry->expr, Var)) {
      // Store the mapping of pre-DISTINCT to post-DISTINCT column.
      Var* var = PostgresCastNode(Var, select_column_state->target_entry->expr);
      transformer_info->AddGroupByVarIndexColumn(var, distinct_column);
    }
  }

  if (output_name_list != nullptr) {
    *output_name_list = name_list;
  }

  // Set the query transformer context so subsequent ORDER BY expression
  // transformation will know it is in the context of post-DISTINCT processing.
  transformer_info->set_is_post_distinct(true);
  // TODO: Check all places where is_post_distinct  is called in
  // googlesql to ensure it makes sense for spangres.

  return BuildGsqlResolvedAggregateScan(std::move(current_scan),
                                        transformer_info);
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedOrderByScan>>
ForwardTransformer::BuildGsqlResolvedOrderByScan(
    zetasql::ResolvedColumnList output_column_list,
    std::unique_ptr<zetasql::ResolvedScan> current_scan,
    TransformerInfo* transformer_info) {
  // For each ORDER BY item, wrap it in a ColumnRef and build a
  // ResolvedOrderByItem.
  std::vector<std::unique_ptr<const zetasql::ResolvedOrderByItem>>
      order_by_item_list;
  for (const OrderByItemTransformInfo& order_by_item :
       transformer_info->order_by_item_transform_info()) {
    zetasql::ResolvedColumn order_by_column;
    if (order_by_item.select_list_index == 0) {
      // This is not a SELECT list column. Use the computed ORDER BY column.
      order_by_column = order_by_item.order_column;
    } else {
      // This is a SELECT list column. Switch the index back to 0-based and
      // look up the column.
      order_by_column = output_column_list[order_by_item.select_list_index - 1];
    }

    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const zetasql::ResolvedColumnRef> resolved_column_ref,
        BuildGsqlResolvedColumnRef(order_by_column));
    order_by_item_list.push_back(zetasql::MakeResolvedOrderByItem(
        std::move(resolved_column_ref), /*collation_name=*/nullptr,
        order_by_item.is_descending, order_by_item.null_order));
  }

  // Build the ResolvedOrderByScan.
  std::unique_ptr<zetasql::ResolvedOrderByScan> order_by_scan =
      zetasql::MakeResolvedOrderByScan(output_column_list,
                                         std::move(current_scan),
                                         std::move(order_by_item_list));
  order_by_scan->set_is_ordered(true);
  return RewriteResolvedOrderByScanIfNeeded(std::move(order_by_scan));
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedOrderByScan>>
ForwardTransformer::BuildGsqlResolvedOrderByScanAfterSetOperation(
    List* sortClause, const VarIndexScope* scope,
    std::unique_ptr<zetasql::ResolvedScan> current_scan) {
  static const char clause_name[] = "ORDER BY clause after set operation";
  auto transformer_info = std::make_unique<TransformerInfo>();
  auto expr_transformer_info = ExprTransformerInfo(
      /*var_index_scope_in=*/scope,
      /*aggregate_var_index_scope_in=*/scope,
      /*allows_aggregation_in=*/false,
      /*use_post_grouping_columns_in=*/false, clause_name,
      transformer_info.get());

  std::vector<OrderByItemTransformInfo>* order_by_item_list =
      transformer_info->mutable_order_by_item_transform_info();
  for (SortGroupClause* sort_item : StructList<SortGroupClause*>(sortClause)) {
    ZETASQL_ASSIGN_OR_RETURN(OrderByItemTransformInfo order_by_item,
                     BuildGsqlOrderByItem(*sort_item, &expr_transformer_info));
    order_by_item_list->emplace_back(std::move(order_by_item));
  }

  ZETASQL_RETURN_IF_ERROR(FinalizeOrderByTransformState(
      transformer_info->mutable_order_by_item_transform_info(),
      transformer_info->order_by_columns_to_compute()));

  // The output columns of the ORDER BY are the same as the output of the
  // original input.
  const auto column_list = current_scan->column_list();

  // In googlesql a project scan for computed columns would be added here. In
  // the transformer we skip this because it is not allowed in postgresql
  // and should be caught by the analyzer.

  ZETASQL_RET_CHECK(
      transformer_info->select_list_columns_to_compute_before_aggregation()
          ->empty());

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<zetasql::ResolvedOrderByScan> order_by_scan,
      BuildGsqlResolvedOrderByScan(column_list, std::move(current_scan),
                                   transformer_info.get()));
  return order_by_scan;
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedOrderByScan>>
ForwardTransformer::RewriteResolvedOrderByScanIfNeeded(
    std::unique_ptr<zetasql::ResolvedOrderByScan> order_by_scan) {
  if (!IsTransformationRequiredForOrderByScan(*order_by_scan)) {
    return order_by_scan;
  }

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<zetasql::ResolvedOrderByScan> transformed_order_by_scan,
      BuildGsqlResolvedOrderByScanWithTransformations(
          order_by_scan->column_list(), order_by_scan->release_input_scan(),
          order_by_scan->release_order_by_item_list()));

  transformed_order_by_scan->set_is_ordered(true);
  return transformed_order_by_scan;
}

bool ForwardTransformer::IsTransformationRequiredForOrderByScan(
    const zetasql::ResolvedOrderByScan& order_by_scan) {
  for (const std::unique_ptr<const zetasql::ResolvedOrderByItem>&
           order_by_item : order_by_scan.order_by_item_list()) {
    const zetasql::ResolvedColumnRef& column_ref =
        *(order_by_item->column_ref());
    if (catalog_adapter_->GetEngineSystemCatalog()
            ->IsTransformationRequiredForComparison(column_ref)) {
      return true;
    }
  }
  return false;
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedOrderByScan>>
ForwardTransformer::BuildGsqlResolvedOrderByScanWithTransformations(
    const std::vector<zetasql::ResolvedColumn>& column_list,
    std::unique_ptr<const zetasql::ResolvedScan> input_scan,
    std::vector<std::unique_ptr<const zetasql::ResolvedOrderByItem>>
        order_by_items) {
  std::vector<std::unique_ptr<const zetasql::ResolvedOrderByItem>>
      transformed_order_by_items;

  // Columns wrapping transformation over ORDER BY columns.
  std::vector<std::unique_ptr<const zetasql::ResolvedComputedColumn>>
      computed_columns;

  transformed_order_by_items.reserve(order_by_items.size());
  computed_columns.reserve(order_by_items.size());

  for (int idx = 0; idx < order_by_items.size(); ++idx) {
    std::unique_ptr<const zetasql::ResolvedOrderByItem> order_by_item =
        std::move(order_by_items[idx]);
    std::unique_ptr<const zetasql::ResolvedOrderByItem>
        transformed_order_by_item;
    if (catalog_adapter_->GetEngineSystemCatalog()
            ->IsTransformationRequiredForComparison(
                *order_by_item->column_ref())) {
      // Column requires transformation, get computed column and build
      // a column reference using it. Also add computed column to list of
      // computed columns.
      const zetasql::ResolvedColumn& order_by_column =
          order_by_item->column_ref()->column();

      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const zetasql::ResolvedComputedColumn>
                           computed_column,
                       BuildGsqlResolvedComputedColumnForOrderByClause(
                           order_by_column, idx + 1));

      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const zetasql::ResolvedColumnRef>
                           transformed_order_by_column_ref,
                       BuildGsqlResolvedColumnRef(computed_column->column()));

      computed_columns.push_back(std::move(computed_column));

      transformed_order_by_item = zetasql::MakeResolvedOrderByItem(
          std::move(transformed_order_by_column_ref),
          /*collation_name=*/nullptr, order_by_item->is_descending(),
          order_by_item->null_order());
    } else {
      // No transformation required.
      transformed_order_by_item = std::move(order_by_item);
    }

    transformed_order_by_items.push_back(std::move(transformed_order_by_item));
  }

  ZETASQL_RET_CHECK(!computed_columns.empty());

  // Build a project scan from computed columns.
  std::unique_ptr<const zetasql::ResolvedScan> project_scan =
      AddGsqlProjectScanForComputedColumns(std::move(input_scan),
                                           std::move(computed_columns));

  return zetasql::MakeResolvedOrderByScan(
      column_list, std::move(project_scan),
      std::move(transformed_order_by_items));
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedComputedColumn>>
ForwardTransformer::BuildGsqlResolvedComputedColumnForOrderByClause(
    const zetasql::ResolvedColumn& order_by_column, int column_index) {
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<zetasql::ResolvedColumnRef> resolved_column_ref,
      BuildGsqlResolvedColumnRef(order_by_column));

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<zetasql::ResolvedExpr> expr,
      catalog_adapter_->GetEngineSystemCatalog()->GetResolvedExprForComparison(
          std::move(resolved_column_ref),
          catalog_adapter_->analyzer_options().language()));

  // Assign a table name and column name that match ZetaSQL behavior.
  std::string order_by_column_name = absl::StrCat(kOrderByColumn, column_index);
  ZETASQL_ASSIGN_OR_RETURN(
      zetasql::ResolvedColumn computed_column,
      BuildNewGsqlResolvedColumn(
          /*table_name=*/kOrderByPrefix, order_by_column_name, expr->type()));

  return zetasql::MakeResolvedComputedColumn(computed_column,
                                               std::move(expr));
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedScan>>
ForwardTransformer::BuildGsqlResolvedSetOperationScan(
    const Query& query, const VarIndexScope* scope, bool is_top_level_query,
    std::vector<NamedColumn>* output_name_list, VarIndexScope* output_scope) {
  // Returns an unimplemented error if a query feature is not yet supported.
  // Query features that are partially supported are handled seperately.
  ZETASQL_RETURN_IF_ERROR(CheckForUnsupportedFeatures(query, is_top_level_query));

  ZETASQL_RET_CHECK(query.setOperations != nullptr);

  // Build a new map for this level of Query.
  auto temporary_index_to_targetentry_map =
      TemporaryVectorElement(ressortgroupref_to_target_entry_maps_);

  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<zetasql::ResolvedScan> scan,
                   BuildGsqlResolvedSetOperationScan(
                       *PostgresCastNode(SetOperationStmt, query.setOperations),
                       *query.rtable, scope, output_scope));
  if (output_name_list != nullptr) {
    // Build the name list. The names and columns come from the first input to
    // the set operation scan.
    output_name_list->reserve(scan->column_list_size());
    for (const zetasql::ResolvedColumn& column : scan->column_list()) {
      output_name_list->push_back({column.name_id(), column});
    }
  }
  return scan;
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedScan>>
ForwardTransformer::BuildGsqlResolvedSetOperationScan(
    const SetOperationStmt& set_operation, const List& rtable,
    const VarIndexScope* scope, VarIndexScope* output_scope) {
  ++set_operation_recursion_tree_depth_;

  // We only want to keep track of the set operation tree depth.
  // `set_operation_recursion_tree_depth_` will be decremented when
  // `decrement_set_operation_count` goes out of scope.
  auto decrement_set_operation_count = absl::MakeCleanup(
      [this]() { --this->set_operation_recursion_tree_depth_; });

  if (set_operation_recursion_tree_depth_ >
      absl::GetFlag(FLAGS_spangres_set_operation_recursion_limit)) {
    return absl::InvalidArgumentError(absl::StrCat(
        "SQL set operation recursion limit exceeded: ",
        absl::GetFlag(FLAGS_spangres_set_operation_recursion_limit)));
  }

  // Transform the operation type.
  ZETASQL_ASSIGN_OR_RETURN(
      zetasql::ResolvedSetOperationScan::SetOperationType op_type,
      BuildGsqlSetOperationType(set_operation.op, set_operation.all));
  zetasql::IdString query_alias =
      catalog_adapter_->analyzer_options().id_string_pool()->Make(
          BuildGsqlSetOperationString(op_type));

  // Transform the inputs.
  // If a query has multiple set operations, PostgreSQL nests them such that
  // each set operation only has two inputs. In ZetaSQL, if consecutive set
  // operations have the same operation type, they are combined in a single
  // set operation scan with unlimited inputs. Here, the Spangres transformer
  // maintains the PostgreSQL nested structure with two inputs instead of
  // expanding and combining nested set operations like ZetaSQL.
  ZETASQL_ASSIGN_OR_RETURN(ForwardTransformer::SetOperationInput left_input,
                   BuildGsqlSetOperationInput(*set_operation.larg, rtable,
                                              query_alias.ToStringView(), scope,
                                              /*query_index=*/1, output_scope));
  ZETASQL_ASSIGN_OR_RETURN(ForwardTransformer::SetOperationInput right_input,
                   BuildGsqlSetOperationInput(*set_operation.rarg, rtable,
                                              query_alias.ToStringView(), scope,
                                              /*query_index=*/2, output_scope));

  // Verify that the output column types are identical between the left and
  // right input. ZetaSQL requires identical column types in the resolved AST
  // and the ZetaSQL analyzer will attempt to cast the inputs to a common
  // type. PostgreSQL allows different types in the query tree.
  // For now, return an Unimplemented error if the types differ, instead of
  // attempting to perform casting in the Transformer.
  ZETASQL_RET_CHECK_EQ(left_input.node->output_column_list_size(),
               right_input.node->output_column_list_size());
  for (int i = 0; i < left_input.node->output_column_list_size(); ++i) {
    if (left_input.node->output_column_list(i).type() !=
        right_input.node->output_column_list(i).type()) {
      return absl::UnimplementedError(
          "Set operations with mismatched input types are not supported");
    }
  }

  // Build the column list.
  // The column list always has newly allocated columns ids, and the names are
  // based on the output column names of the larg.
  // Note that the left_input `output_column_names` accounts for aliases but
  // the left_input node's `output_column_list` does not. The final column list
  // combines the column types from the left_input `node` and the column names
  // from the left_input `output_column_names`.
  ZETASQL_RET_CHECK_EQ(left_input.node->output_column_list_size(),
               left_input.output_column_names.size());
  std::vector<zetasql::ResolvedColumn> final_column_list;
  for (int i = 0; i < left_input.node->output_column_list_size(); ++i) {
    ZETASQL_ASSIGN_OR_RETURN(int column_id, catalog_adapter_->AllocateColumnId());
    zetasql::IdString column_name =
        catalog_adapter_->analyzer_options().id_string_pool()->Make(
            left_input.output_column_names[i]);
    const zetasql::Type* column_type =
        left_input.node->output_column_list(i).type();
    zetasql::ResolvedColumn output_column(column_id, query_alias, column_name,
                                            column_type);
    RecordColumnAccess(output_column);
    final_column_list.push_back(output_column);
  }

  // Extract the ResolvedSetOperationItem from the SetOperationInput.
  std::vector<std::unique_ptr<zetasql::ResolvedSetOperationItem>>
      resolved_inputs;
  resolved_inputs.push_back(std::move(left_input.node));
  resolved_inputs.push_back(std::move(right_input.node));
  return zetasql::MakeResolvedSetOperationScan(final_column_list, op_type,
                                                 std::move(resolved_inputs));
}

absl::StatusOr<zetasql::ResolvedSetOperationScan::SetOperationType>
ForwardTransformer::BuildGsqlSetOperationType(SetOperation set_op, bool all) {
  switch (set_op) {
    case SETOP_UNION:
      return all ? zetasql::ResolvedSetOperationScan::UNION_ALL
                 : zetasql::ResolvedSetOperationScan::UNION_DISTINCT;
    case SETOP_INTERSECT:
      return all ? zetasql::ResolvedSetOperationScan::INTERSECT_ALL
                 : zetasql::ResolvedSetOperationScan::INTERSECT_DISTINCT;
    case SETOP_EXCEPT:
      return all ? zetasql::ResolvedSetOperationScan::EXCEPT_ALL
                 : zetasql::ResolvedSetOperationScan::EXCEPT_DISTINCT;
    case SETOP_NONE:
      return absl::InvalidArgumentError(
          "Set operation must have a valid operation type");
  }
}

std::string ForwardTransformer::BuildGsqlSetOperationString(
    zetasql::ResolvedSetOperationScan::SetOperationType set_op) const {
  switch (set_op) {
    case zetasql::ResolvedSetOperationScan::UNION_ALL:
      return "$union_all";
    case zetasql::ResolvedSetOperationScan::UNION_DISTINCT:
      return "$union_distinct";
    case zetasql::ResolvedSetOperationScan::INTERSECT_ALL:
      return "$intersect_all";
    case zetasql::ResolvedSetOperationScan::INTERSECT_DISTINCT:
      return "$intersect_distinct";
    case zetasql::ResolvedSetOperationScan::EXCEPT_ALL:
      return "$except_all";
    case zetasql::ResolvedSetOperationScan::EXCEPT_DISTINCT:
      return "$except_distinct";
  }
}

absl::StatusOr<ForwardTransformer::SetOperationInput>
ForwardTransformer::BuildGsqlSetOperationInput(
    const Node& input, const List& rtable, absl::string_view op_type_str,
    const VarIndexScope* scope, int query_index, VarIndexScope* output_scope) {
  ZETASQL_RET_CHECK(IsA(&input, RangeTblRef) || IsA(&input, SetOperationStmt));

  ForwardTransformer::SetOperationInput result;
  std::unique_ptr<const zetasql::ResolvedScan> resolved_scan;
  std::vector<zetasql::ResolvedColumn> columns;
  if (IsA(&input, SetOperationStmt)) {
    // This is a nested set operation.
    ZETASQL_ASSIGN_OR_RETURN(resolved_scan,
                     BuildGsqlResolvedSetOperationScan(
                         *PostgresConstCastNode(SetOperationStmt, &input),
                         rtable, scope, output_scope));
    // Nested set operations don't have aliases, so just use the column names
    // from the input scan.
    for (const zetasql::ResolvedColumn column :
         resolved_scan->column_list()) {
      result.output_column_names.push_back(column.name());
    }
    columns = resolved_scan->column_list();
  } else {
    Index rtindex = PostgresConstCastNode(RangeTblRef, &input)->rtindex;
    RangeTblEntry* rte = rt_fetch(rtindex, &rtable);
    ZETASQL_RET_CHECK(rte->rtekind == RTE_SUBQUERY);

    // Transform the subquery.
    CorrelatedColumnsSet correlated_columns_set;
    VarIndexScope subquery_scope(scope, &correlated_columns_set);
    std::string query_alias = absl::StrCat(op_type_str, query_index);
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<zetasql::ResolvedQueryStmt> resolved_statement,
        BuildPartialGsqlResolvedQueryStmt(*rte->subquery,
                                          /*is_top_level_query=*/false,
                                          &subquery_scope, query_alias));
    resolved_scan = resolved_statement->release_query();
    ZETASQL_RETURN_IF_ERROR(MapVarIndexToColumn(*resolved_scan, rtindex, output_scope));

    // Subqueries can have aliases, which are in the output_column_list but not
    // the input scan. Extract the column names and columns from the
    // output_column_list.
    for (const std::unique_ptr<const zetasql::ResolvedOutputColumn>&
             output_column : resolved_statement->output_column_list()) {
      result.output_column_names.push_back(output_column->name());
      columns.push_back(output_column->column());
    }
  }

  // Build a ResolvedSetOperationItem from the input.
  result.node = zetasql::MakeResolvedSetOperationItem(
      std::move(resolved_scan), columns);
  return result;
}

absl::Status ForwardTransformer::MapWithClauseAliasToMetadata(
    const std::string& with_alias,
    const std::vector<zetasql::ResolvedColumn>& column_list,
    const std::vector<NamedColumn>& with_column_aliases) {
  named_subquery_map_.insert(
      {with_alias, std::make_unique<NamedSubquery>(with_alias, column_list,
                                                   with_column_aliases)});
  return absl::OkStatus();
}

absl::StatusOr<const NamedSubquery*> ForwardTransformer::GetWithClauseMetadata(
    absl::string_view with_alias) {
  auto it = named_subquery_map_.find(with_alias);
  ZETASQL_RET_CHECK(it != named_subquery_map_.end());
  return it->second.get();
}

absl::StatusOr<std::vector<std::unique_ptr<const zetasql::ResolvedWithEntry>>>
ForwardTransformer::BuildGsqlWithEntriesIfPresent(const Query& query,
                                                  bool is_top_level_query) {
  std::vector<std::unique_ptr<const zetasql::ResolvedWithEntry>> with_entries;
  if (list_length(query.cteList) == 0) {
    return with_entries;
  }
  for (CommonTableExpr* cte : StructList<CommonTableExpr*>(query.cteList)) {
    if (cte->cterecursive) {
      return absl::UnimplementedError(
          "Recursive WITH expressions are not supported");
    }

    Query* ctequery = PostgresCastNode(Query, cte->ctequery);

    if (list_length(ctequery->cteList) > 0) {
      return absl::UnimplementedError(
          "Nested WITH expressions are not supported");
    }

    // Transform the WITH clause query.
    std::vector<NamedColumn> subquery_name_list;
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const zetasql::ResolvedScan> resolved_ctequery,
        BuildGsqlResolvedScanForQueryExpression(
            *ctequery, is_top_level_query, &empty_var_index_scope_,
            cte->ctename, &subquery_name_list));

    ZETASQL_RETURN_IF_ERROR(MapWithClauseAliasToMetadata(
        cte->ctename, resolved_ctequery->column_list(), subquery_name_list));

    // Build the ResolvedWithEntry for this WITH clause subquery and add it to
    // the with_entries list.
    std::unique_ptr<const zetasql::ResolvedWithEntry> resolved_with_entry =
        zetasql::MakeResolvedWithEntry(cte->ctename,
                                         std::move(resolved_ctequery));
    with_entries.push_back(std::move(resolved_with_entry));
  }
  return with_entries;
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedWithScan>>
ForwardTransformer::BuildGsqlResolvedWithScan(
    const Query& query,
    std::vector<std::unique_ptr<const zetasql::ResolvedWithEntry>>
        with_entries,
    std::unique_ptr<zetasql::ResolvedScan> input_scan) {
  // Recursive WITH statements are not supported. An error is thrown when
  // building the with_entries list if any recursive WITH statements are
  // detected.
  const auto& tmp_column_list = input_scan->column_list();
  return MakeResolvedWithScan(tmp_column_list, std::move(with_entries),
                              std::move(input_scan), /*recursive=*/false);
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedQueryStmt>>
ForwardTransformer::BuildPartialGsqlResolvedQueryStmt(
    const Query& query, bool is_top_level_query, const VarIndexScope* scope,
    const std::string& query_alias) {
  ZETASQL_ASSIGN_OR_RETURN(
      std::vector<std::unique_ptr<const zetasql::ResolvedWithEntry>>
          with_entries,
      BuildGsqlWithEntriesIfPresent(query, is_top_level_query));

  // Mimic the ZetaSQL behavior for anonymous table names.
  absl::string_view alias;
  if (!query_alias.empty()) {
    alias = query_alias;
  } else if (query.hasAggs) {
    alias = kAnonymousAggregateQuery;
  } else {
    alias = kAnonymousQuery;
  }

  std::vector<NamedColumn> name_list;
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<zetasql::ResolvedScan> resolved_scan,
                   BuildGsqlResolvedScanForQueryExpression(
                       query, is_top_level_query, scope, alias, &name_list));

  if (list_length(query.cteList) > 0) {
    ZETASQL_ASSIGN_OR_RETURN(resolved_scan,
                     BuildGsqlResolvedWithScan(query, std::move(with_entries),
                                               std::move(resolved_scan)));
  }

  std::vector<std::unique_ptr<const zetasql::ResolvedOutputColumn>>
      output_column_list;
  output_column_list.reserve(name_list.size());
  for (const NamedColumn& named_column : name_list) {
    output_column_list.push_back(zetasql::MakeResolvedOutputColumn(
        named_column.name.ToString(), named_column.column));
  }

  bool is_value_table =
      (output_column_list.size() == 1 && output_column_list[0]->name().empty());

  return zetasql::MakeResolvedQueryStmt(
      std::move(output_column_list), is_value_table, std::move(resolved_scan));
}

absl::Status ForwardTransformer::BuildSortGroupIndexToTargetEntryMap(
    const List* target_list) {
  ZETASQL_RET_CHECK(!ressortgroupref_to_target_entry_maps_.empty());

  absl::flat_hash_map<Index, TargetEntry*> sort_group_index_map;
  int select_column_count = 0;
  // Iterate through target_list, add an entry to the temporary map if the
  // TargetEntry has a non-zero ressortgroupref:
  for (TargetEntry* entry : StructList<TargetEntry*>(target_list)) {
    if (!entry->resjunk) {
      // This is a SELECT list TargetEntry.
      ++select_column_count;
    }
    if (entry->ressortgroupref != 0) {
      int select_index = entry->resjunk ? 0 : select_column_count;
      auto insert_result = ressortgroupref_to_target_entry_maps_.back().insert(
          {entry->ressortgroupref,
           ForwardTransformer::TargetEntryIndex{.select_index = select_index,
                                                .target_entry = entry}});
      // Make sure that the insert succeeds
      ZETASQL_RET_CHECK(insert_result.second);
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<ForwardTransformer::TargetEntryIndex>
ForwardTransformer::GetPgSortGroupTargetEntry(Index sort_group_index) {
  ZETASQL_RET_CHECK(!ressortgroupref_to_target_entry_maps_.empty());
  auto iter =
      ressortgroupref_to_target_entry_maps_.back().find(sort_group_index);
  if (iter == ressortgroupref_to_target_entry_maps_.back().end()) {
    return absl::InternalError(
        absl::StrCat("Cannot find a valid TargetEntry "
                     "for the sort group index ",
                     sort_group_index));
  }
  return iter->second;
}

void ForwardTransformer::RecordColumnAccess(
    const zetasql::ResolvedColumn& column,
    zetasql::ResolvedStatement::ObjectAccess access_flags) {
  zetasql::ResolvedStatement::ObjectAccess& access =
      referenced_column_access_[column];
  access = static_cast<zetasql::ResolvedStatement::ObjectAccess>(
      access_flags | access);
}

void ForwardTransformer::RecordColumnAccess(
    const std::vector<zetasql::ResolvedColumn>& columns,
    zetasql::ResolvedStatement::ObjectAccess access_flags) {
  for (const zetasql::ResolvedColumn& column : columns) {
    RecordColumnAccess(column, access_flags);
  }
}

absl::Status ForwardTransformer::PruneColumnLists(
    const zetasql::ResolvedNode* node) {
  std::vector<const zetasql::ResolvedNode*> scan_nodes;
  node->GetDescendantsSatisfying(&zetasql::ResolvedNode::IsScan, &scan_nodes);

  std::vector<zetasql::ResolvedColumn> pruned_column_list;
  std::vector<int> pruned_column_index_list;
  for (const zetasql::ResolvedNode* scan_node : scan_nodes) {
    const zetasql::ResolvedScan* scan =
        scan_node->GetAs<zetasql::ResolvedScan>();

    const std::vector<int>* column_index_list = nullptr;
    if (scan_node->node_kind() == zetasql::RESOLVED_TABLE_SCAN) {
      column_index_list =
          &scan->GetAs<zetasql::ResolvedTableScan>()->column_index_list();
    } else if (scan_node->node_kind() == zetasql::RESOLVED_TVFSCAN) {
      column_index_list =
          &scan->GetAs<zetasql::ResolvedTVFScan>()->column_index_list();
    }

    pruned_column_list.clear();
    pruned_column_index_list.clear();
    for (int i = 0; i < scan->column_list().size(); ++i) {
      const zetasql::ResolvedColumn& column = scan->column_list(i);
      if (referenced_column_access_.find(column) != referenced_column_access_.end()) {
        pruned_column_list.push_back(column);
        if (column_index_list != nullptr) {
          const int column_index = (*column_index_list)[i];
          pruned_column_index_list.push_back(column_index);
        }
      }
    }

    if (pruned_column_list.size() < scan->column_list_size()) {
      // The ZetaSQL analyzer handles pivot scans here, but they are not
      // supported in Spangres yet, so skip that step.

      // We use const_cast to mutate the column_list vector on Scan nodes.
      // This is only called right at the end, after we've done all resolving,
      // and before we transfer ownership to the caller.
      zetasql::ResolvedScan* mutable_scan =
          const_cast<zetasql::ResolvedScan*>(scan);
      mutable_scan->set_column_list(pruned_column_list);
      if (column_index_list != nullptr) {
        if (scan_node->node_kind() == zetasql::RESOLVED_TABLE_SCAN) {
          mutable_scan->GetAs<zetasql::ResolvedTableScan>()
              ->set_column_index_list(pruned_column_index_list);
        } else if (scan_node->node_kind() == zetasql::RESOLVED_TVFSCAN) {
          mutable_scan->GetAs<zetasql::ResolvedTVFScan>()
              ->set_column_index_list(pruned_column_index_list);
        }
      }
    }
  }
  return absl::OkStatus();
}

absl::Status ForwardTransformer::SetColumnAccessList(
    zetasql::ResolvedStatement* statement) {
  const zetasql::ResolvedTableScan* scan = nullptr;

  // Currently, we are only setting column access info on nodes that support it,
  // including Update, Insert, and Delete.
  std::vector<zetasql::ResolvedStatement::ObjectAccess>* mutable_access_list =
      nullptr;
  switch (statement->node_kind()) {
    case zetasql::RESOLVED_UPDATE_STMT: {
      auto update_stmt = static_cast<zetasql::ResolvedUpdateStmt*>(statement);
      scan = update_stmt->table_scan();
      mutable_access_list = update_stmt->mutable_column_access_list();
      break;
    }
    case zetasql::RESOLVED_INSERT_STMT: {
      auto insert_stmt = static_cast<zetasql::ResolvedInsertStmt*>(statement);
      scan = insert_stmt->table_scan();
      mutable_access_list = insert_stmt->mutable_column_access_list();
      break;
    }
    case zetasql::RESOLVED_DELETE_STMT: {
      auto delete_stmt = static_cast<zetasql::ResolvedDeleteStmt*>(statement);
      scan = delete_stmt->table_scan();
      mutable_access_list = delete_stmt->mutable_column_access_list();
      break;
    }
    default: {
      return absl::OkStatus();
    }
  }

  ZETASQL_RET_CHECK(scan != nullptr);
  ZETASQL_RET_CHECK(mutable_access_list != nullptr);
  // ZetaSQL records "implied access" for update and merge statements here,
  // but it doesn't seem to impact golden tests results, so it is skipped in
  // Spangres.

  std::vector<zetasql::ResolvedStatement::ObjectAccess> column_access_list;
  for (int i = 0; i < scan->column_list().size(); ++i) {
    const zetasql::ResolvedColumn& column = scan->column_list(i);
      if (referenced_column_access_.find(column) !=
            referenced_column_access_.end()) {
      column_access_list.push_back(referenced_column_access_.at(column));
    } else if (!catalog_adapter_->analyzer_options().prune_unused_columns()) {
      column_access_list.push_back(zetasql::ResolvedStatement::NONE);
    }
  }
  *mutable_access_list = column_access_list;
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedCallStmt>>
ForwardTransformer::BuildPartialGsqlResolvedCallStmt(const FuncExpr& func) {
  // Resolve the Procedure arguments.
  ExprTransformerInfo expr_transformer_info =
      ExprTransformerInfo::ForScalarFunctions(&empty_var_index_scope_,
                                              "standalone expression");
  ZETASQL_ASSIGN_OR_RETURN(
      std::vector<std::unique_ptr<zetasql::ResolvedExpr>> argument_list,
      BuildGsqlFunctionArgumentList(func.args, &expr_transformer_info));

  // Look up the procedure and signature.
  std::vector<zetasql::InputArgumentType> input_argument_types =
      GetInputArgumentTypes(argument_list);
  ZETASQL_ASSIGN_OR_RETURN(
      ProcedureAndSignature procedure_and_signature,
      catalog_adapter_->GetEngineSystemCatalog()->GetProcedureAndSignature(
          func.funcid, input_argument_types,
          catalog_adapter_->analyzer_options().language()));

  return zetasql::MakeResolvedCallStmt(procedure_and_signature.procedure(),
                                         procedure_and_signature.signature(),
                                         std::move(argument_list));
}

absl::Status ForwardTransformer::CheckForUnsupportedFields(
    const void* field, absl::string_view feature_name,
    absl::string_view field_name) const {
  if (field != nullptr) {
    return absl::UnimplementedError(absl::StrFormat(
        "%s with %s are not supported", feature_name, field_name));
  }
  return absl::OkStatus();
}

absl::Status ForwardTransformer::CheckForUnsupportedFeatures(
    const Query& query, bool is_top_level_query) const {
  std::string feature_name = "Statements";
  ZETASQL_RETURN_IF_ERROR(CheckForUnsupportedFields(query.groupingSets, feature_name,
                                            "GROUPING SET clauses"));
  ZETASQL_RETURN_IF_ERROR(CheckForUnsupportedFields(query.windowClause, feature_name,
                                            "WINDOW clauses"));
  ZETASQL_RETURN_IF_ERROR(CheckForUnsupportedFields(query.rowMarks, feature_name,
                                            "ROW MARK clauses"));
  ZETASQL_RETURN_IF_ERROR(CheckForUnsupportedFields(query.constraintDeps, feature_name,
                                            "constraint clauses"));
  ZETASQL_RETURN_IF_ERROR(CheckForUnsupportedFields(
      query.withCheckOptions, feature_name, "WITH CHECK options"));

  if (query.groupDistinct) {
    return absl::UnimplementedError(
        "Statements with GROUP BY DISTINCT clauses are not supported");
  }

  if (query.isReturn) {
    return absl::UnimplementedError("RETURN statements are not supported");
  }

  if (query.hasDistinctOn) {
    return absl::UnimplementedError(
        "Statements with DISTINCT ON clauses are not supported");
  }

  if (query.override != OVERRIDING_NOT_SET) {
    return absl::UnimplementedError(
        "Statements with OVERRIDING clauses are not supported");
  }

  if (query.commandType == CMD_SELECT) {
    // Disallow select lists with no output columns. This only applies to SELECT
    // statements--DDL need not have a target list.
    bool all_target_entries_are_junk = true;
    for (auto target : StructList<TargetEntry*>(query.targetList)) {
      if (!target->resjunk) {
        all_target_entries_are_junk = false;
        break;
      }
    }
    if (all_target_entries_are_junk) {
      return absl::InvalidArgumentError(
          "Statement must produce at least one output column");
    }

    // Certain features are disabled in subqueries.
    if (!is_top_level_query) {
      if (list_length(query.cteList) != 0) {
        return absl::UnimplementedError(
            "WITH expressions in subqueries are not supported");
      }
    }
  } else {
    if (list_length(query.cteList) != 0) {
      return absl::UnimplementedError(
          "WITH expressions are only supported for SELECT statements");
    }
  }

  // Make sure any CTEs are SELECT queries (no DML, etc.).
  for (const CommonTableExpr* ctexpr :
       StructList<CommonTableExpr*>(query.cteList)) {
    ZETASQL_RET_CHECK_NE(ctexpr, nullptr);
    ZETASQL_RET_CHECK_NE(ctexpr->ctequery, nullptr);
    ZETASQL_RET_CHECK(IsA(ctexpr->ctequery, Query));
    if (PostgresCastNode(Query, ctexpr->ctequery)->commandType != CMD_SELECT) {
      return absl::UnimplementedError(
          "WITH subexpressions must be SELECT statements");
    }
  }

  if (query.onConflict != nullptr) {
    if (query.onConflict->action == ONCONFLICT_NONE) {
      return CheckForUnsupportedFields(query.onConflict, feature_name,
                                       "ON CONFLICT clauses");
    }
  }

  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedStatement>>
ForwardTransformer::BuildGsqlResolvedStatement(const Query& query) {
  absl::StatusOr<char*> query_string_or_status = CheckedPgNodeToString(&query);
  if (query_string_or_status.ok()) {
    ZETASQL_VLOG(4) << "BuildGsqlResolvedStatement, query=\n"
            << pretty_format_node_dump(*query_string_or_status);
  }
  // Returns an unimplemented error if a query feature is not yet supported.
  // Query features that are partially supported are handled seperately.
  ZETASQL_RETURN_IF_ERROR(
      CheckForUnsupportedFeatures(query, /*is_top_level_query=*/true));

  std::unique_ptr<zetasql::ResolvedStatement> statement;
  switch (query.commandType) {
    // Query types
    case CMD_SELECT: {
      ZETASQL_ASSIGN_OR_RETURN(statement, BuildPartialGsqlResolvedQueryStmt(
                                      query, /*is_top_level_query=*/true,
                                      &empty_var_index_scope_,
                                      /*query_alias=*/""));
      break;
    }

    case CMD_UPDATE: {
      ZETASQL_ASSIGN_OR_RETURN(statement, BuildPartialGsqlResolvedUpdateStmt(query));
      break;
    }

    case CMD_INSERT: {
      ZETASQL_ASSIGN_OR_RETURN(statement, BuildPartialGsqlResolvedInsertStmt(query));
      break;
    }

    case CMD_DELETE: {
      ZETASQL_ASSIGN_OR_RETURN(statement, BuildPartialGsqlResolvedDeleteStmt(query));
      break;
    }

    case CMD_UTILITY: {
      ZETASQL_RET_CHECK(query.utilityStmt != NULL);
      switch (query.utilityStmt->type) {
        case T_CallStmt: {
          ZETASQL_ASSIGN_OR_RETURN(
              statement, BuildPartialGsqlResolvedCallStmt(
                             *PostgresConstCastNode(CallStmt, query.utilityStmt)
                                  ->funcexpr));
          break;
        }
        case T_CreatedbStmt:
        case T_CreateStmt:
        case T_CreateSeqStmt:
        case T_AlterSeqStmt:
        case T_AlterTableStmt:
        case T_DropStmt:
        case T_AlterDatabaseSetStmt:
        case T_IndexStmt:
        case T_CreateSchemaStmt:
        case T_AlterSpangresStatsStmt:
        case T_ViewStmt:
        case T_AlterOwnerStmt:
        case T_AlterStatsStmt:
        case T_AlterObjectSchemaStmt:
          return absl::UnimplementedError(
              "DDL statements cannot be issued as SELECT/DML statements");
        default:
          return absl::UnimplementedError(
              "Utility SQL statements are not supported");
      }
      break;
    }

    // Command types that are not queries
    case CMD_UNKNOWN:
      return absl::InternalError(absl::StrCat("Query type is unknown"));

    case CMD_NOTHING:
      return absl::InternalError(
          absl::StrCat("Placeholder query objects are not supported"));

    default:
      return absl::InternalError(
          absl::StrCat("Unknown command type: ", query.commandType));
  }

  if (query.stmt_location != -1) {
    zetasql::ParseLocationRange location_range;
    location_range.set_start(
        zetasql::ParseLocationPoint::FromByteOffset(query.stmt_location));
    if (query.stmt_len == 0) {
      if (absl::GetFlag(
              FLAGS_spangres_include_invalid_statement_parse_locations)) {
        // In PostgreSQL, stmt_len == 0 indicates rest of the query string.
        // We have no info about the query string length here, so we cannot
        // actually construct the range's end.
        // We follow PostgreSQL convention here: a range's end == 0 means rest
        // of the query string. Otherwise we set the end according to
        // stmt_len.
        location_range.set_end(
            zetasql::ParseLocationPoint::FromByteOffset(0));
        statement->SetParseLocationRange(location_range);
      } else {
        ABSL_LOG(ERROR) << "unknown length for query statement starting at "
                   << query.stmt_location << "; dropping start location";
      }
    } else {
      location_range.set_end(zetasql::ParseLocationPoint::FromByteOffset(
          query.stmt_location + query.stmt_len));
      statement->SetParseLocationRange(location_range);
    }
  }
  if (query.statementHints != nullptr) {
    VarIndexScope var_index_scope;
    ZETASQL_ASSIGN_OR_RETURN(
        std::vector<std::unique_ptr<const zetasql::ResolvedOption>> hint_list,
        BuildGsqlResolvedOptionList(*query.statementHints, &var_index_scope));
    statement->set_hint_list(std::move(hint_list));
  }

  if (catalog_adapter_->analyzer_options().prune_unused_columns()) {
    ZETASQL_RETURN_IF_ERROR(PruneColumnLists(statement.get()));
  }
  ZETASQL_RETURN_IF_ERROR(SetColumnAccessList(statement.get()));

  return statement;
}

}  // namespace postgres_translator
