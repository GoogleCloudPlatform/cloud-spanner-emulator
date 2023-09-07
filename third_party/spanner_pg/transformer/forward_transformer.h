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

#ifndef TRANSFORMER_FORWARD_TRANSFORMER_H_
#define TRANSFORMER_FORWARD_TRANSFORMER_H_

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/types/type.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "absl/container/flat_hash_map.h"
#include "absl/flags/declare.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "third_party/spanner_pg/catalog/catalog_adapter.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/transformer/expr_transformer_helper.h"
#include "third_party/spanner_pg/transformer/transformer_helper.h"

ABSL_DECLARE_FLAG(bool, spangres_include_invalid_statement_parse_locations);
ABSL_DECLARE_FLAG(int64_t, spangres_expression_recursion_limit);
ABSL_DECLARE_FLAG(int64_t, spangres_set_operation_recursion_limit);

namespace postgres_translator {

using ObjectAccess = ::zetasql::ResolvedStatement::ObjectAccess;
using ::postgres_translator::CatalogAdapter;
// A type of different ZetaSQL joins. This is used to distinguish ZetaSQL
// from PostgreSQL join types below.
using GsqlJoinType = ::zetasql::ResolvedJoinScan::JoinType;
// A type of different PostgreSQL joins.
using PgJoinType = JoinType;

// Lifetime of Postgres objects:
// For BuildPg transformations, the functions will create Postgres objects
// and return them as raw pointers. The lifetimes of these created objects are
// controlled by `MemoryContextManager`, which wraps the underlying Postgres
// memmory management: Ultimately it's Postgres that allocates and owns the
// objects at a lower layer.
//
// BuildPg and BuildGsql functions have a single output object and one or more
// input objects. The output object must be uninitialized when passed in.
// It will be constructed and fully populated before the function returns.
// The output should not be re-modified outside of the function.
// It is not a requirement that the inputs be fully exhausted when creating
// the output.
// In some cases, helper methods are required which partially populate an output
// object. Their names should be prefixed with BuildPartialPg or
// BuildPartialGsql.

class ForwardTransformer {
 public:
  struct TargetEntryIndex {
    // The select list index. 0 if the TargetEntry is not in the SELECT list.
    int select_index;
    const TargetEntry* target_entry;
  };

  // Modeled after the ZetaSQL SetOperationResolver::ResolvedInputResult.
  struct SetOperationInput {
    std::unique_ptr<zetasql::ResolvedSetOperationItem> node;
    std::vector<std::string> output_column_names;
  };

  ForwardTransformer(std::unique_ptr<CatalogAdapter> catalog_adapter)
      : catalog_adapter_(std::move(catalog_adapter)) {
    ABSL_CHECK(catalog_adapter_ != nullptr);
    ABSL_CHECK(catalog_adapter_->analyzer_options().id_string_pool() != nullptr);
  }
  ForwardTransformer(ForwardTransformer&&) = default;
  virtual ~ForwardTransformer() = default;

  std::unique_ptr<CatalogAdapter> ReleaseCatalogAdapter() {
    return absl::WrapUnique(catalog_adapter_.release());
  }

  CatalogAdapter& catalog_adapter() { return *catalog_adapter_; }

  std::map<std::string, const zetasql::Type*>& query_parameter_types() {
    return query_parameter_types_;
  }

  // Query =====================================================================

  absl::StatusOr<const zetasql::Table*> GetTableFromRTE(
      const RangeTblEntry& rte);

  // Builds a `ResolvedTableScan` from the `rte` by querying the catalog.
  // Requires that `rte.rtekind` is RTE_RELATION.
  // `rtindex` is used for MapVarIndexToColumn.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedTableScan>>
  BuildGsqlResolvedTableScan(const RangeTblEntry& rte, Index rtindex,
                             VarIndexScope* output_scope);

  // Builds a `ResolvedFilterScan` object. `where_clause` is taken from the
  // `quals` node in a PostgreSQL query's `jointree`. `current_scan` should
  // already be built by call sites.
  // Modeled after ZetaSQL's ResolveWhereClauseAndCreateScan function.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedFilterScan>>
  BuildGsqlResolvedFilterScan(
      const Node& where_clause, const VarIndexScope* from_scan_scope,
      std::unique_ptr<zetasql::ResolvedScan> current_scan);

  // Builds a `ResolvedLimitOffsetScan` object.`limit_clause` is taken from
  // query.limitCount, while `offset_clause` is taken from query.limitOffset.
  // The function is modeled after ZetaSQL's ResolveLimitOffsetScan. In
  // addition, this function adds logic that allows LIMIT/OFFSET to assume
  // PostgreSQL behavior such as allowing OFFSET to be used on its own and
  // allowing the usage of input types other than integers.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedLimitOffsetScan>>
  BuildGsqlResolvedLimitOffsetScan(
      const Node* limit_clause, const Node* offset_clause,
      std::unique_ptr<const zetasql::ResolvedScan> current_scan);

  // Builds a nested `ResolvedScan` object for the FROM clause of this query.
  // Returns a SingleRowScan if there is no FROM  clause.
  // Modeled after the ZetaSQL ResolveFromClauseAndCreateScan function.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedScan>>
  BuildGsqlResolvedScanForFromClause(const Query& query,
                                     const VarIndexScope* external_scope,
                                     VarIndexScope* output_scope);

  // Builds a nested `ResolvedScan` for this query or subquery.
  // Modeled after the ZetaSQL ResolveQueryExpression and
  // ResolveQueryAfterWith functions.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedScan>>
  BuildGsqlResolvedScanForQueryExpression(
      const Query& query, bool is_top_level_query, const VarIndexScope* scope,
      absl::string_view alias, std::vector<NamedColumn>* output_name_list);

  // Builds a `ResolvedArray` scan for UNNEST.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedScan>>
  BuildGsqlResolvedArrayScan(const RangeTblEntry& rte, Index rtindex,
                             const VarIndexScope* external_scope,
                             VarIndexScope* output_scope);

  // Builds a `ResolvedTVFScan` for a TVF in the FROM clause.
  // Currently this is just the Change Streams TVF.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedTVFScan>>
  BuildGsqlResolvedTVFScan(
      const RangeTblEntry& rte, Index rtindex,
      const zetasql::TableValuedFunction* tvf_catalog_entry,
      VarIndexScope* output_scope);

  // Builds a nested `ResolvedScan` object for this SELECT `query` or subquery.
  // Note that this is a LOSSY conversion!  In particular, only some of the
  // information on `query->targetList` can be stored in the resulted
  // `ResolvedScan`.
  // `alias` is used as the table name for computed columns.
  // Modeled after the ZetaSQL ResolveSelect function.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedScan>>
  BuildGsqlResolvedScanForSelect(const Query& query, bool is_top_level_query,
                                 const VarIndexScope* external_scope,
                                 absl::string_view alias,
                                 std::vector<NamedColumn>* output_name_list);

  // Note that this is a LOSSY conversion!  In particular, only some of the
  // information on `query->targetList` can be stored on a `ResolvedScan`.
  //
  // `query->targetList` can be stored on a ResolvedQueryStmt object.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedScan>>
  BuildGsqlResolvedScan(const Query& query);

  // Maps the WITH clause alias to the WITH clause metadata.
  absl::Status MapWithClauseAliasToMetadata(
      const std::string& with_alias,
      const std::vector<zetasql::ResolvedColumn>& column_list,
      const std::vector<NamedColumn>& with_column_aliases);

  // Get the metadata for a WITH clause subquery.
  absl::StatusOr<const NamedSubquery*> GetWithClauseMetadata(
      absl::string_view with_alias);

  absl::StatusOr<
      std::vector<std::unique_ptr<const zetasql::ResolvedWithEntry>>>
  BuildGsqlWithEntriesIfPresent(const Query& query, bool is_top_level_query);

  absl::StatusOr<std::unique_ptr<zetasql::ResolvedWithScan>>
  BuildGsqlResolvedWithScan(
      const Query& query,
      std::vector<std::unique_ptr<const zetasql::ResolvedWithEntry>>
          with_entries,
      std::unique_ptr<zetasql::ResolvedScan> input_scan);

  absl::StatusOr<std::unique_ptr<zetasql::ResolvedQueryStmt>>
  BuildPartialGsqlResolvedQueryStmt(const Query& query, bool is_top_level_query,
                                    const VarIndexScope* scope,
                                    const std::string& query_alias);

  absl::StatusOr<std::unique_ptr<zetasql::ResolvedStatement>>
  BuildGsqlResolvedStatement(const Query& query);

  // Returns an unimplemented error if a query feature is not yet supported.
  // Query features that are partially supported are handled seperately.
  absl::Status CheckForUnsupportedFeatures(const Query& query,
                                           bool is_top_level_query) const;

  // Returns an unimplemented error with the feature name if a field that is
  // not supported is populated (not nullptr).
  absl::Status CheckForUnsupportedFields(const void* field,
                                         absl::string_view feature_name) const;

  // Builds a list of ResolvedColumn objects from the argument list of
  // `function_call`.
  std::vector<zetasql::ResolvedColumn> BuildGsqlSelectedColumnList(
      const zetasql::ResolvedFunctionCall& function_call);

 private:
  // Copied from ZetaSQL's Resolver::RecordColumnAccess.
  void RecordColumnAccess(
      const zetasql::ResolvedColumn& column,
      zetasql::ResolvedStatement::ObjectAccess access_flags =
          zetasql::ResolvedStatement::READ);

  // Copied from ZetaSQL's Resolver::RecordColumnAccess.
  void RecordColumnAccess(
      const std::vector<zetasql::ResolvedColumn>& columns,
      zetasql::ResolvedStatement::ObjectAccess access_flags =
          zetasql::ResolvedStatement::READ);

  // Copied from ZetaSQL's Resolver::SetColumnAccessList.
  absl::Status SetColumnAccessList(zetasql::ResolvedStatement* statement);

  // Modeled off of ZetaSQL's Resolver::PruneColumnLists.
  absl::Status PruneColumnLists(const zetasql::ResolvedNode* node);

  // Helper function to instantiate a new `ResolvedColumn`. A new column ID will
  // be allocated and assigned to the returned `ResolvedColumn`.
  absl::StatusOr<zetasql::ResolvedColumn> BuildNewGsqlResolvedColumn(
      absl::string_view table_name, absl::string_view column_name,
      const zetasql::Type* column_type);

  // Given a Postgres Node, builds a corresponding ZetaSQL ResolvedScan
  // object. Modeled after the ZetaSQL ResolveTableExpression function.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedScan>>
  BuildGsqlResolvedScanForTableExpression(const Node& node, const List& rtable,
                                          const VarIndexScope* external_scope,
                                          const VarIndexScope* local_scope,
                                          VarIndexScope* output_scope);

  // Helper to BuildGsqlResolvedScanForTableExpression for validating and
  // dispatching the various RTE_FUNCTION cases. Currently supported are:
  // UNNEST, Change Stream TVF.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedScan>>
  BuildGsqlResolvedScanForFunctionCall(const RangeTblEntry* rte,
                                       const Index rtindex,
                                       const VarIndexScope* external_scope,
                                       VarIndexScope* output_scope);

  // Simplified version of the ZetaSQL analyzer function of the same name.
  // This version only supports scalar arguments, which eliminates the need for
  // the permissive TVFFunctionArg wrapper, which exists to support Scans, etc.
  // This version also skips signature matching since that was performed by the
  // Postgres analyzer.
  absl::Status PrepareTVFInputArguments(
      const FuncExpr& func_expr,
      const zetasql::TableValuedFunction* tvf_catalog_entry,
      VarIndexScope* output_scope,
      std::unique_ptr<zetasql::FunctionSignature>* result_signature,
      std::vector<std::unique_ptr<const zetasql::ResolvedFunctionArgument>>*
          resolved_tvf_args,
      std::vector<zetasql::TVFInputArgumentType>* tvf_input_arguments);

  absl::StatusOr<std::unique_ptr<zetasql::ResolvedWithRefScan>>
  BuildGsqlResolvedWithRefScan(absl::string_view with_alias, Index rtindex,
                               VarIndexScope* output_scope);

  // Returns a ZetaSQL join type corresponding to the input PostgreSQL
  // join type.
  absl::StatusOr<GsqlJoinType> BuildGsqlJoinType(PgJoinType pg_join_type);

  // Given a PostgreSQL JoinExpr object, builds a corresponding ZetaSQL
  // ResolvedScan object that's either a ResolvedJoinScan or ResolvedArrayScan.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedScan>>
  BuildGsqlResolvedJoinScan(const JoinExpr& join_expr, const List& rtable,
                            const VarIndexScope* external_scope,
                            const VarIndexScope* local_scope,
                            VarIndexScope* output_scope);

  // Builds a ZetaSQL ResolvedJoinScan object based on the ZetaSQL inputs.
  // The resulted ResolvedJoinScan has type INNER and does not have any join
  // expression.
  // Call sites don't have to build a ResolvedColumn list and call
  // MakeResolvedJoinScan() directly.
  std::unique_ptr<zetasql::ResolvedJoinScan>
  BuildGsqlCommaJoinResolvedJoinScan(
      std::unique_ptr<zetasql::ResolvedScan> left_scan,
      std::unique_ptr<zetasql::ResolvedScan> right_scan);

  // Builds ZetaSQL `ResolvedJoinScan` objects from items in `fromlist`.
  // Calls BuildGsqlResolvedScanForTableExpression on each item in the
  // `fromlist` and joins them in a left deep tree.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedScan>>
  BuildGsqlResolvedScanForFromList(const List& fromlist, const List& rtable,
                                   const VarIndexScope* external_scope,
                                   VarIndexScope* output_scope);

  // Build a ResolvedProjectScan from the inputs. Its column list is built
  // by concatenating the input_scan's column list and the ResolvedColumn
  // objects inside expr_list.
  // This function should only be used to add an intermediate (not top-level)
  // ProjectScan for computed columns. It should not be used to build the
  // top-level ProjectScan for a query or subquery.
  std::unique_ptr<zetasql::ResolvedProjectScan>
  AddGsqlProjectScanForComputedColumns(
      std::unique_ptr<const zetasql::ResolvedScan> input_scan,
      std::vector<std::unique_ptr<const zetasql::ResolvedComputedColumn>>
          expr_list);

  // Wrap the subquery in another ProjectScan. For each column in the subquery,
  // compute a comparison expression for that column as needed.
  // For example, convert `select x FROM t` into
  // `select comparison_function(x) FROM (select x FROM t)`.
  // For now, only supports single column subqueries whose SELECT column
  // requires a comparison expression.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedProjectScan>>
  AddGsqlProjectScanForInSubqueries(
      std::unique_ptr<const zetasql::ResolvedScan> subquery);

  // Transform the expr for each SELECT column.
  // Modeled off of ZetaSQL's ResolveSelectListExprsFirstPass.
  absl::Status BuildGsqlSelectListResolvedExprsFirstPass(
      const List* targetList, const VarIndexScope* from_scan_scope,
      TransformerInfo* transformer_info);

  // Add aggregate functions to the aggregate_computed_columns list.
  // In ZetaSQL, this happens when resolving an expression so that aggregate
  // functions that are nested inside of non-aggregate functions are supported.
  // However, Spangres does not support this yet because TransformerInfo is not
  // passed into BuildGsqlResolvedExpr.
  // TODO: add support for nested aggregate functions.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedColumnRef>>
  BuildGsqlAggregateColumnRef(
      zetasql::IdString column_name, const Aggref* aggregate_func_expr,
      std::unique_ptr<const zetasql::ResolvedExpr> aggregate_expr,
      TransformerInfo* transformer_info);

  // Transform select columns that must be pre-computed before aggregation.
  // Modeled off of ZetaSQL's
  // AnalyzeSelectColumnsToPrecomputeBeforeAggregation.
  absl::Status BuildGsqlSelectColumnsToPrecomputeBeforeAggregation(
      TransformerInfo* transformer_info);

  // Finalize the select column list -- each column reference will have a
  // ResolvedColumn and each computed column will have a ResolvedComputedColumn.
  // Modeled off of ZetaSQL's FinalizeSelectColumnStateList.
  absl::Status FinalizeSelectListTransformState(
      absl::string_view query_alias, TransformerInfo* transformer_info,
      SelectListTransformState* select_list_transform_state);

  // Re-transform the select column list so that GROUP BY column dependencies
  // in the SELECT list are accounted for.
  // Modeled off of ZetaSQL's ResolveSelectListExprsSecondPass.
  absl::Status BuildGsqlSelectListResolvedExprsSecondPass(
      const List* targetList, absl::string_view query_alias,
      const VarIndexScope* group_by_scope,
      std::vector<NamedColumn>* final_project_name_list,
      TransformerInfo* transformer_info);

  // Modeled off of ZetaSQL's ResolveSelectColumnSecondPass, but does not
  // update final_project_name_list -- that work is done by
  // BuildGsqlSelectListResolvedExprsSecondPass.
  absl::Status BuildGsqlSelectColumnSecondPass(
      absl::string_view query_alias, const VarIndexScope* group_by_scope,
      SelectColumnTransformState* select_column_state,
      TransformerInfo* transformer_info);

  // Transform the expr for each GROUP BY column.
  // Modeled off of ZetaSQL's ResolveGroupByExprs.
  absl::Status BuildGsqlGroupByList(List* groupClause,
                                    const VarIndexScope* from_clause_scope,
                                    TransformerInfo* transformer_info);

  // Transform a GROUP BY column that is also a SELECT column.
  // Modeled off of part of ZetaSQL's HandleGroupBySelectColumn, but does not
  // compute the ResolvedExpr or update the column state.
  absl::StatusOr<zetasql::ResolvedColumn> BuildGsqlGroupBySelectColumn(
      const SelectColumnTransformState* select_column_state,
      TransformerInfo* transformer_info);

  // Update the SelectColumnTransformState for a GROUP BY column that is also a
  // SELECT column.
  // Modeled off of part of ZetaSQL's HandleGroupBySelectColumn, but does not
  // compute the ResolvedColumn or ResolvedExpr.
  absl::Status UpdateGroupBySelectColumnTransformState(
      const zetasql::ResolvedColumn& group_by_column,
      SelectColumnTransformState* select_column_state);

  // Transform a GROUP BY column that is not a SELECT column.
  // May overwrite resolved_expr.
  // Modeled off of ZetaSQL's HandleGroupByExpression.
  absl::StatusOr<zetasql::ResolvedColumn> BuildGsqlGroupByColumn(
      const TargetEntry* entry, const zetasql::Type* resolved_expr_type,
      const VarIndexScope* from_clause_scope,
      TransformerInfo* transformer_info);

  absl::StatusOr<std::unique_ptr<const zetasql::ResolvedExpr>>
  BuildGsqlHavingClause(const Expr& having_expr,
                        const VarIndexScope* having_and_order_by_scope,
                        const VarIndexScope* select_list_and_from_scan_scope,
                        TransformerInfo* transformer_info);

  // Transform the ORDER BY list.
  // Modeled off of ZetaSQL's ResolveOrderByExprs.
  absl::Status BuildGsqlOrderByList(
      const List* sortClause, const VarIndexScope* having_and_order_by_scope,
      const VarIndexScope* select_list_and_from_scan_scope,
      bool is_post_distinct, TransformerInfo* transformer_info);

  // Transform the expr, null order, and sort direction for the ORDER BY
  // item.
  // Modeled off of ZetaSQL's ResolveOrderingExprs.
  absl::StatusOr<OrderByItemTransformInfo> BuildGsqlOrderByItem(
      const SortGroupClause& sort_item,
      ExprTransformerInfo* expr_transformer_info);

  // Finalize the sort order list -- each column reference will have a
  // ResolvedColumn and each computed column will have a ResolvedComputedColumn.
  // For now, only column references are supported.
  // Modeled off of ZetaSQL's AddColumnsForOrderByExprs.
  absl::Status FinalizeOrderByTransformState(
      std::vector<OrderByItemTransformInfo>* order_by_info,
      std::vector<std::unique_ptr<const zetasql::ResolvedComputedColumn>>*
          computed_columns);

  // Build the remaining scans for this query.
  // Modeled off of ZetaSQL's AddRemainingScansForSelect.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedScan>>
  BuildGsqlRemainingScansForSelect(
      const List* sort_clause, const Node* limit_clause,
      const Node* offset_clause, const VarIndexScope* having_and_order_by_scope,
      std::unique_ptr<const zetasql::ResolvedExpr> resolved_having_expr,
      std::unique_ptr<zetasql::ResolvedScan> current_scan,
      TransformerInfo* transformer_info,
      std::vector<NamedColumn>* output_name_list);

  // Build a ResolvedAggregateScan.
  // Modeled off of ZetaSQL's AddAggregateScan.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedAggregateScan>>
  BuildGsqlResolvedAggregateScan(
      std::unique_ptr<zetasql::ResolvedScan> current_scan,
      TransformerInfo* transformer_info);

  // Resolve the 'SELECT DISTINCT ...' part of the query.
  // Modeled off of ZetaSQL's ResolveSelectDistinct.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedAggregateScan>>
  BuildGsqlResolvedSelectDistinct(
      std::unique_ptr<zetasql::ResolvedScan> current_scan,
      TransformerInfo* transformer_info,
      std::vector<NamedColumn>* output_name_list);

  // Build a ResolvedOrderByScan.
  // Modeled off of ZetaSQL's MakeResolvedOrderByScan and ResolveOrderByItems.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedOrderByScan>>
  BuildGsqlResolvedOrderByScan(
      zetasql::ResolvedColumnList output_column_list,
      std::unique_ptr<zetasql::ResolvedScan> current_scan,
      TransformerInfo* transformer_info);

  // Build a ResolvedOrderByScan after a set operation.
  // Modeled off of ZetaSQL's ResolveOrderByAfterSetOperation
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedOrderByScan>>
  BuildGsqlResolvedOrderByScanAfterSetOperation(
      List* sortClause, const VarIndexScope* scope,
      std::unique_ptr<zetasql::ResolvedScan> current_scan);

  // Build a ResolvedSetOperationScan.
  // `is_top_level_query` is used to run CheckForUnsupportedFeatures since some
  // features are only disallowed in subqueries.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedScan>>
  BuildGsqlResolvedSetOperationScan(const Query& query,
                                    const VarIndexScope* scope,
                                    bool is_top_level_query,
                                    std::vector<NamedColumn>* output_name_list,
                                    VarIndexScope* output_scope);

  // Build a ResolvedSetOperationScan
  // Modeled off of ZetaSQL's SetOperationResolver::Resolve.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedScan>>
  BuildGsqlResolvedSetOperationScan(const SetOperationStmt& set_operation,
                                    const List& rtable,
                                    const VarIndexScope* scope,
                                    VarIndexScope* output_scope);

  // Transform a PostgreSQL set operation type to ZetaSQL.
  absl::StatusOr<zetasql::ResolvedSetOperationScan::SetOperationType>
  BuildGsqlSetOperationType(SetOperation set_op, bool all);

  // Get the set operation query alias.
  // Modeled off of ZetaSQL's SetOperationResolver constructor.
  std::string BuildGsqlSetOperationString(
      zetasql::ResolvedSetOperationScan::SetOperationType set_op) const;

  // Transform a PostgreSQL set operation input.
  // Modeled off of ZetaSQL's SetOperationResolver::ResolveInputQuery but
  // returns the Transformer::SetOperationInput instead of the
  // SetOperationResolver::ResolvedInputResult.
  absl::StatusOr<SetOperationInput> BuildGsqlSetOperationInput(
      const Node& input, const List& rtable, absl::string_view op_type_str,
      const VarIndexScope* scope, int query_index, VarIndexScope* output_scope);

  // Determines whether this expr (which may not even be a FuncExpr) is for a
  // set-returning function (SRF). This is used to provide better error messages
  // for query locations where PostgreSQL permits an SRF but ZetaSQL (and thus
  // Spangres) does not.
  bool IsSetReturningFunction(const Expr* expr) const;

 public:
  // DML =======================================================================
  absl::flat_hash_map<int, const zetasql::Column*> GetUnwritableColumns(
      const zetasql::Table* table);

  absl::StatusOr<std::unique_ptr<zetasql::ResolvedInsertStmt>>
  BuildPartialGsqlResolvedInsertStmt(const Query& query);

  absl::StatusOr<std::unique_ptr<zetasql::ResolvedUpdateStmt>>
  BuildPartialGsqlResolvedUpdateStmt(const Query& query);

  absl::StatusOr<std::unique_ptr<zetasql::ResolvedDMLDefault>>
  BuildGsqlResolvedDMLDefault(const SetToDefault& set_to_default);

  absl::StatusOr<std::unique_ptr<zetasql::ResolvedDeleteStmt>>
  BuildPartialGsqlResolvedDeleteStmt(const Query& query);

 private:
  // Builds a list of ZetaSQL ResolvedDMLValue from a PostgreSQL list of
  // Expr objects
  absl::StatusOr<
      std::vector<std::unique_ptr<const zetasql::ResolvedDMLValue>>>
  BuildGsqlResolvedDMLValueList(List* expr_list,
                                const VarIndexScope* var_index_scope);

  // Builds a ZetaSQL ResolvedDMLValue from a PostgreSQL Expr object.
  absl::StatusOr<std::unique_ptr<const zetasql::ResolvedDMLValue>>
  BuildGsqlResolvedDMLValue(Expr& expr, const VarIndexScope* var_index_scope,
                            const char* clause_name);

  // Builds a ZetaSQL ResolvedExpr for the WHERE clause of UPDATE and DELETE
  // statements, using the input PostgreSQL `pg_quals`. If the input is nullptr,
  // the function builds a ResolvedExpr representing the "WHERE true" clause.
  // The transformer needs to build such a clause during forward transformation
  // of UPDATE and DELETE statements because PostgreSQL doesn't require a WHERE
  // clause, but ZetaSQL does.
  absl::StatusOr<std::unique_ptr<const zetasql::ResolvedExpr>>
  BuildGsqlWhereClauseExprForDML(Node* pg_where_clause,
                                 const VarIndexScope* var_index_scope);

  // Builds a ZetaSQL ResolvedReturningClause for the DML statements, using
  // the input PostgreSQL query `returningList`. If the input is nullptr, the
  // function return a null value for ResolvedReturningClause. It's modeled
  // after the ZetaSQL analyzer's ResolveReturningClause function.
  absl::StatusOr<std::unique_ptr<const zetasql::ResolvedReturningClause>>
  BuildGsqlReturningClauseForDML(const List* pg_returning_list,
                                 absl::string_view table_alias,
                                 const VarIndexScope* target_table_scope);

  // Expression ================================================================
 public:
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedLiteral>>
  BuildGsqlResolvedLiteral(const Const& _const);

  // Builds a new PG Const from the const_type, const_value, and const_is_null,
  // and then transforms it into a ZetaSQL literal.
  // Only used when transforming casts from the PG Query*.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedLiteral>>
  BuildGsqlResolvedLiteral(Oid const_type, Datum const_value,
                           bool const_is_null, CoercionForm cast_format);

  // Transforms an ArrayExpr (array literal) to the appropriate ZetaSQL
  // ResolvedExpr depending on array contents:
  //   Const-only arrays transform to ResolvedLiterals like other Const types.
  //   Arrays that have non-const values (like vars or other exprs) transform to
  //   a make_array function call.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
  BuildGsqlResolvedExpr(const ArrayExpr& expr,
                        ExprTransformerInfo* expr_transformer_info);

  absl::StatusOr<std::unique_ptr<zetasql::ResolvedParameter>>
  BuildGsqlResolvedParameter(const Param& pg_param);

  // Transform a PostgreSQL SubLink.
  // Returns a ResolvedExpr instead of a ResolvedSubqueryExpr because
  // `x != ALL (select y)` is transformed into `NOT(x IN (select y))` and
  // NOT(...) is a ResolvedFunctionCall.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
  BuildGsqlResolvedSubqueryExpr(const SubLink& pg_sublink,
                                ExprTransformerInfo* expr_transformer_info);

  // in_testexpr is only populated for IN subquery expressions, nullptr
  // otherwise.
  // hint_list may be populated for IN subquery expressions. It is nullptr for
  // other expressions.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedSubqueryExpr>>
  BuildGsqlResolvedSubqueryExpr(
      const Query& pg_subquery, const Expr* in_testexpr, const List* hint_list,
      const zetasql::Type* output_type,
      zetasql::ResolvedSubqueryExpr::SubqueryType subquery_type,
      ExprTransformerInfo* expr_transformer_info);

  absl::StatusOr<
      std::vector<std::unique_ptr<const zetasql::ResolvedColumnRef>>>
  BuildGsqlCorrelatedSubqueryParameters(
      const CorrelatedColumnsSet& correlated_columns_set);

  // Transform an expr without support for aggregate functions.
  // Modeled after the ZetaSQL ResolveScalarExpr function.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
  BuildGsqlResolvedScalarExpr(const Expr& expr,
                              const VarIndexScope* var_index_scope,
                              const char* clause_name);

  absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
  BuildGsqlResolvedExpr(const Expr& expr,
                        ExprTransformerInfo* expr_transformer_info);

  absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
  BuildGsqlResolvedExpr(const FuncExpr& func_expr,
                        ExprTransformerInfo* expr_transformer_info);

  // Cast a literal with an unsupported type to a supported type using a
  // PostgreSQL casting function. The output type must be supported.
  // The typmod and explicit_cast are optional values.
  // This function should not be used to cast literals with supported types
  // since those can be cast during query execution.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedLiteral>>
  BuildGsqlResolvedLiteralFromCast(Oid cast_function,
                                   const Const& input_literal,
                                   const Const* typmod,
                                   const Const* explicit_cast, Oid output_type,
                                   CoercionForm cast_format);

  // Constructs either a ResolvedCast or a ResolvedFunctionCall that performs
  // the casting operation.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
  BuildGsqlCastExpression(Oid result_type, const Expr& input, int32_t typmod,
                          ExprTransformerInfo* expr_transformer_info);

  absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
  BuildGsqlResolvedExpr(const CoerceViaIO& coerce_via_io,
                        ExprTransformerInfo* expr_transformer_info);

  absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
  BuildGsqlResolvedExpr(const RelabelType& relabel_type,
                        ExprTransformerInfo* expr_transformer_info);

  absl::Status UnsupportedCastError(Oid source_type_oid, Oid target_type_oid);

  // Transforms a list of DefElem nodes containing query hints into a vector of
  // equivalent ResolvedOption nodes. This list can then be consumed by
  // ResolvedScan::set_hint_list() or similar.
  absl::StatusOr<std::vector<std::unique_ptr<const zetasql::ResolvedOption>>>
  BuildGsqlResolvedOptionList(const List& pg_hint_list,
                              const VarIndexScope* var_index_scope);

  zetasql::IdString GetColumnAliasForTopLevelExpression(
      ExprTransformerInfo* expr_transformer_info, const Expr* ast_expr);

  // Function ==================================================================
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedFunctionCall>>
  BuildGsqlResolvedFunctionCall(Oid funcid, List* args,
                                ExprTransformerInfo* expr_transformer_info);

  // Transform a PG function into a built in function.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedFunctionCall>>
  BuildGsqlResolvedFunctionCall(const FuncExpr& func,
                                ExprTransformerInfo* expr_transformer_info);

  // Transform a PG operator into a built in function.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedFunctionCall>>
  BuildGsqlResolvedFunctionCall(const OpExpr& op,
                                ExprTransformerInfo* expr_transformer_info);

  // Transform a PG aggregate function into a built in aggregate function.
  // If the aggregate function is a SELECT column and also appears in the HAVING
  // or ORDER BY clause, returns a column reference the second time this is
  // called.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
  BuildGsqlResolvedAggregateFunctionCall(
      const Aggref& agg_function, ExprTransformerInfo* expr_transformer_info);

  // Transform a PG expression into a built in function.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedFunctionCall>>
  BuildGsqlResolvedFunctionCall(NodeTag expr_node_tag, List* args,
                                ExprTransformerInfo* expr_transformer_info);

  // Build a substr function call with position = 0. Should only be used to
  // transform casts to varchar(<length>).
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedFunctionCall>>
  BuildGsqlSubstrFunctionCall(std::unique_ptr<zetasql::ResolvedExpr> value,
                              int length);

  // Transform a PG Case expression into a built in function.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedFunctionCall>>
  BuildGsqlResolvedCaseFunctionCall(const CaseExpr& case_expr,
                                    ExprTransformerInfo* expr_transformer_info);

  absl::StatusOr<std::unique_ptr<zetasql::ResolvedFunctionCall>>
  BuildGsqlResolvedCaseNoValueFunctionCall(
      const CaseExpr& case_expr, ExprTransformerInfo* expr_transformer_info);

  absl::StatusOr<std::unique_ptr<zetasql::ResolvedFunctionCall>>
  BuildGsqlResolvedCaseWithValueFunctionCall(
      const CaseExpr& case_expr, ExprTransformerInfo* expr_transformer_info);

  // Transform a PG boolean expression into a built in function.
  // Special cased because there isn't a single built in function for all
  // BoolExprs. The BoolExprType on the BoolExpr must be examined to find the
  // matching builtin function.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedFunctionCall>>
  BuildGsqlResolvedBoolFunctionCall(const BoolExpr& bool_expr,
                                    ExprTransformerInfo* expr_transformer_info);

  // Wrap a NOT function around the provided argument.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedFunctionCall>>
  BuildGsqlResolvedNotFunctionCall(
      std::unique_ptr<zetasql::ResolvedExpr> arg);

  // Transform a PG min max expression into a built in function.
  // Special cased because there isn't a single built in function for all
  // MixMaxExpr. The MinMaxOp in the MixMaxExpr must be examined to find the
  // matching builtin function.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedFunctionCall>>
  BuildGsqlResolvedGreatestLeastFunctionCall(
      const MinMaxExpr& min_max_expr,
      ExprTransformerInfo* expr_transformer_info);

  // Transform a PG NullTest expression into a builtin function.
  // Special cased because the NullTestType on the NulLTest must be examined to
  // find the matching builtin function.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedFunctionCall>>
  BuildGsqlResolvedNullTestFunctionCall(
      const NullTest& null_test, ExprTransformerInfo* expr_transformer_info);

  // Transform a PG BooleanTest expression into a ZetaSQL IS TRUE/IS FALSE
  // function call. This may be an expression -- GoogeSQL doesn't have "X IS NOT
  // TRUE" as an atomic function, so that's turned into the expression
  // "NOT (X IS TRUE)".
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
  BuildGsqlResolvedBooleanTestExpr(const BooleanTest& boolean_test,
                                   ExprTransformerInfo* expr_transformer_info);

  absl::StatusOr<std::vector<std::unique_ptr<zetasql::ResolvedExpr>>>
  BuildGsqlFunctionArgumentList(List* args,
                                ExprTransformerInfo* expr_transformer_info);

  // Transform a ScalarOpExpr into a built in function. Currently only supports
  // the IN  and NOT IN function. Postgres uses ScalarOpExpr for IN, NOT IN,
  // ANY/SAME and ALL.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedFunctionCall>>
  BuildGsqlResolvedScalarArrayFunctionCall(
      const ScalarArrayOpExpr& scalar_array,
      ExprTransformerInfo* expr_transformer_info);

  // Transform a SQLValueFunction into a built in function.
  // Special cased because the SQLValueFunctionOp on the SQLValueFunction must
  // be examined to find the matching builtin function.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
  BuildGsqlResolvedSQLValueFunctionCall(const SQLValueFunction& function);

  // Transform a SubscriptingRef into a function call for array element
  // accesses: array_value[4]. Supports only read-only accesses (SELECT), not
  // writable accesses (UPDATE).
  //
  // We use safe_array_at_ordinal to match two PostgreSQL behaviors: 1-based
  // indexing (ordinal), and NULL return values for out-of-bounds accesses
  // (safe).
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
  BuildGsqlResolvedSafeArrayAtOrdinalFunctionCall(
      const SubscriptingRef& subscripting_ref,
      ExprTransformerInfo* expr_transformer_info);

  // Transform an ArrayExpr into a $make_array function call after recursively
  // transforming the element expressions. This supports any kind of
  // otherwise-supported element expression. To match ZetaSQL's analyzer, this
  // should only be called when the elements are not all Const.
  // BuildGsqlResolvedExpr(const ArrayExpr& expr) makes this determination.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
  BuildGsqlResolvedMakeArrayFunctionCall(
      const ArrayExpr& array_expr, ExprTransformerInfo* expr_transformer_info);

  // Transform the postgres NOT IN operation into two GoogleSWL functions (NOT
  // function and IN fucntion). The NOT function wraps around the IN function.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedFunctionCall>>
  BuildGsqlInFunctionCall(const ScalarArrayOpExpr& scalar_array,
                          ExprTransformerInfo* expr_transformer_info);

  // Type ======================================================================
  // If the result PostgresTypeMapping has mapped_type, returns the
  // mapped_type. Otherwise returns a PostgresTypeMapping.
  absl::StatusOr<const zetasql::Type*> BuildGsqlType(const Oid pg_type_oid);

  // Other =====================================================================
  // Populates `ressortgroupref_to_target_entry_maps_` from the inputs.
  absl::Status BuildSortGroupIndexToTargetEntryMap(const List* target_list);

  // Returns a TargetEntryIndex keyed by the input `sort_group_index` in
  // `ressortgroupref_to_target_entry_maps_`. The TargetEntryIndex represents
  // a GROUP BY or an ORDER BY column with the SELECT list index where
  // applicable.
  absl::StatusOr<TargetEntryIndex> GetPgSortGroupTargetEntry(
      Index sort_group_index);

  // Rewrites the OrderByScan with transformations on ORDER BY columns if
  // needed. The transformations are applied on the ORDER BY columns to make
  // sure that they follow Postgres' comparison semantics.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedOrderByScan>>
  RewriteResolvedOrderByScanIfNeeded(
      std::unique_ptr<zetasql::ResolvedOrderByScan> order_by_scan);

  // Checks if any of the column in ORDER BY clause requires transformation for
  // comparison.
  bool IsTransformationRequiredForOrderByScan(
      const zetasql::ResolvedOrderByScan& order_by_scan);

  // Adds transformations on the `order_by_items` and build a
  // ResolvedOrderByScan.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedOrderByScan>>
  BuildGsqlResolvedOrderByScanWithTransformations(
      const std::vector<zetasql::ResolvedColumn>& column_list,
      std::unique_ptr<const zetasql::ResolvedScan> input_scan,
      std::vector<std::unique_ptr<const zetasql::ResolvedOrderByItem>>
          order_by_items);

  // Builds a computed column by wrapping `order_by_column`
  // with appropriate transformation.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedComputedColumn>>
  BuildGsqlResolvedComputedColumnForOrderByClause(
      const zetasql::ResolvedColumn& order_by_column, int column_index);

 private:
  absl::StatusOr<int> GetEndLocationForStartLocation(int start_location);

  // Returns if the `func_expr` is a cast function.
  absl::StatusOr<bool> IsCastFunction(const FuncExpr& func_expr);

  absl::StatusOr<std::unique_ptr<zetasql::ResolvedColumnRef>>
  BuildGsqlResolvedColumnRef(const Var& var,
                             const VarIndexScope& var_index_scope);

  absl::StatusOr<std::unique_ptr<zetasql::ResolvedColumnRef>>
  BuildGsqlResolvedColumnRefWithCorrelation(
      const zetasql::ResolvedColumn& column,
      const CorrelatedColumnsSetList& correlated_columns_sets,
      zetasql::ResolvedStatement::ObjectAccess access_flags =
          zetasql::ResolvedStatement::READ);

  absl::StatusOr<std::unique_ptr<zetasql::ResolvedColumnRef>>
  BuildGsqlResolvedColumnRef(
      const zetasql::ResolvedColumn& column, bool is_correlated = false,
      zetasql::ResolvedStatement::ObjectAccess access_flags =
          zetasql::ResolvedStatement::READ);

  // Maps `VarIndex` to corresponding `zetasql::ResolvedColumn` for the given
  // `scan`.
  //
  // When a ResolvedColumn transformed from a Var is built either because the
  // Transformer builds a TableScan (for a table) or a ProjectScan
  // (for a subquery), it should be added to the VarIndex -> ResolvedColumn map.
  // This function helps adding all ResolvedColumns of `scan` to the map.
  // `rtindex` is the index of `scan`'s corresponding PostgreSQL RangeTblEntry
  // object.
  //
  // This is used for the forward transformation.
  absl::Status MapVarIndexToColumn(const zetasql::ResolvedScan& scan,
                                   Index rtindex,
                                   VarIndexScope* var_index_scope);
  // Builds a synthetic limit node from a provided offset
  // if a limit is not provided.
  absl::StatusOr<Node*> BuildLimitConst(const Node* offset_node);

  // Checks that the LIMIT/OFFSET clause is valid.
  absl::Status ValidateLimitOffset(const Node* limitoffset_node,
                                   absl::string_view clause) const;

  // Maps `VarIndex` to corresponding `zetasql::ResolvedColumn` for the
  // given RangeTblEntry object representing a join. `rtindex` should be the
  // index of of `rte` in its containing rtable.
  //
  // Call sites should have already called MapVarIndexToColumn() for all
  // other non-join RangeTblEntry objects, so that their ResolvedColumns are
  // already indexed in the map.
  absl::Status MapVarIndexToColumnForJoin(const RangeTblEntry& rte,
                                          Index rtindex,
                                          VarIndexScope* var_index_scope);

  // This is used for the forward transformation.
  absl::StatusOr<zetasql::ResolvedColumn> GetResolvedColumn(
      const VarIndexScope& var_index_scope, Index varno, AttrNumber varattno,
      int var_levels_up,
      CorrelatedColumnsSetList* correlated_columns_sets = nullptr);

  std::unique_ptr<CatalogAdapter> catalog_adapter_;

  const VarIndexScope empty_var_index_scope_;

  // Query-level internal data structures --------------------------------------
  // The following data structures are used to keep track of the transformer's
  // internal states during forward/reverse transformation. They are defined
  // as vectors to accommodate multi-level queries. E.g. a top-level query
  // state is an element of the vector, a subquery has another element.

  // A map from PostgreSQL a sort group index to a TargetEntryIndex with the
  // TargetEntry pointer and SELECT list index where applicable.
  // This is used to replace query.sortClause and query.groupClause
  // when the forward transformer needs to look up a TargetEntry from a
  // sort group index.
  std::vector<absl::flat_hash_map<Index, TargetEntryIndex>>
      ressortgroupref_to_target_entry_maps_;

  // End of query-level internal data structures -------------------------------

  // Depth of the current PostgreSQL expression tree that the transformer is
  // translating. Used to prevent stack overflow when the tree grows too deep.
  int expression_recursion_tree_depth_ = 0;

  // Depth of the current PostgreSQL set operation tree that the transformer is
  // translating. Used to prevent stack overflow when the tree grows too deep.
  int set_operation_recursion_tree_depth_ = 0;

  // Track the columns of named subqueries, from WITH clauses.
  absl::flat_hash_map<std::string, std::unique_ptr<NamedSubquery>>
      named_subquery_map_;

  // Track the parameter names and types in the statement.
  std::map<std::string, const zetasql::Type*> query_parameter_types_;

  // Store how columns have actually been referenced in the query.
  // Once we transform the full query, this will be used to prune column_lists
  // of unreferenced columns. It is also used to populate column_access_list,
  // which indicates whether columns were read and/or written.
  std::map<zetasql::ResolvedColumn,
           zetasql::ResolvedStatement::ObjectAccess>
      referenced_column_access_;

  // Space for streamz flags
  // Flags if a from clause contains both implicit and explicit joins
  bool is_mixed_joins_query_ = false;
};

}  // namespace postgres_translator

#endif  // TRANSFORMER_FORWARD_TRANSFORMER_H_
