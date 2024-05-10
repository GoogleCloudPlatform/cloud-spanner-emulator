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

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/catalog.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/flags/flag.h"
#include "absl/meta/type_traits.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "third_party/spanner_pg/transformer/expr_transformer_helper.h"
#include "third_party/spanner_pg/transformer/forward_transformer.h"
#include "third_party/spanner_pg/util/nodetag_to_string.h"
#include "third_party/spanner_pg/util/pg_list_iterators.h"
#include "third_party/spanner_pg/util/postgres.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

constexpr absl::string_view kExcludedAlias = "excluded";

namespace postgres_translator {

using ::postgres_translator::internal::PostgresCastToExpr;

absl::flat_hash_map<int, const zetasql::Column*>
ForwardTransformer::GetUnwritableColumns(const zetasql::Table* table) {
  absl::flat_hash_map<int, const zetasql::Column*> unwritable_columns;
  for (int i = 0; i < table->NumColumns(); ++i) {
    const zetasql::Column* column = table->GetColumn(i);
    if (!column->IsWritableColumn()) {
      unwritable_columns.insert({i, column});
    }
  }
  return unwritable_columns;
}

absl::StatusOr<std::unique_ptr<const zetasql::ResolvedReturningClause>>
ForwardTransformer::BuildGsqlReturningClauseForDML(
    const List* pg_returning_list, absl::string_view table_alias,
    const VarIndexScope* target_table_scope) {
  if (pg_returning_list == nullptr) {
    return nullptr;
  }

  auto transformer_info = std::make_unique<TransformerInfo>();

  // Transform the TargetEntry expr for each returning column in target list.
  ZETASQL_RETURN_IF_ERROR(BuildGsqlSelectListResolvedExprsFirstPass(
      pg_returning_list, target_table_scope, transformer_info.get()));

  transformer_info->set_has_group_by(false);
  transformer_info->set_has_having(false);
  transformer_info->set_has_order_by(false);

  ZETASQL_RETURN_IF_ERROR(FinalizeSelectListTransformState(
      table_alias, transformer_info.get(),
      transformer_info->select_list_transform_state()));

  std::vector<NamedColumn> final_output_list;
  ZETASQL_RETURN_IF_ERROR(BuildGsqlSelectListResolvedExprsSecondPass(
      pg_returning_list, table_alias, target_table_scope, &final_output_list,
      transformer_info.get()));

  // ZetaSQL runs 'ResolveAdditionalExprsSecondPass' to resolve dot-star
  // expansions, which are already handled by the PostgreSQL analyzer, so we can
  // skip it here.
  ZETASQL_RET_CHECK(
      transformer_info->select_list_columns_to_compute_before_aggregation()
          ->empty());

  // Postgres does not support "WITH ACTION" in returning clause.
  std::unique_ptr<zetasql::ResolvedColumnHolder> action_column;

  std::vector<std::unique_ptr<const zetasql::ResolvedOutputColumn>>
      output_column_list;
  for (int index = 0; index < final_output_list.size(); ++index) {
    std::unique_ptr<zetasql::ResolvedOutputColumn> output_column =
        zetasql::MakeResolvedOutputColumn(
            final_output_list[index].name.ToString(),
            final_output_list[index].column);
    output_column_list.push_back(std::move(output_column));
  }

  std::vector<std::unique_ptr<const zetasql::ResolvedComputedColumn>>
      computed_columns =
          transformer_info->release_select_list_columns_to_compute();
  return zetasql::MakeResolvedReturningClause(std::move(output_column_list),
                                                std::move(action_column),
                                                std::move(computed_columns));
}

absl::Status ForwardTransformer::CheckForUnsupportedOnConflictClause(
    const Query& query, Index rte_index, const zetasql::Table& table,
    const std::vector<zetasql::ResolvedColumn>& insert_column_list,
    VarIndexScope* scope, bool is_ignore_mode) {
  ZETASQL_RET_CHECK_NE(query.onConflict, nullptr);
  OnConflictExpr* expr = query.onConflict;

  //
  // Check that the conflict target columns are primary key columns.
  //
  ZETASQL_ASSIGN_OR_RETURN(
      auto key_columns,
      catalog_adapter().GetEngineUserCatalog()->GetPrimaryKeyColumns(table));
  if (key_columns.empty()) {
    return absl::InvalidArgumentError(
        absl::StrCat("ON CONFLICT clauses for tables without primary "
                     "key columns are not supported"));
  }
  std::unordered_set<std::string> key_columns_in_conflict_target;
  for (InferenceElem* elem : StructList<InferenceElem*>(expr->arbiterElems)) {
    ZETASQL_RET_CHECK_EQ(nodeTag(elem->expr), NodeTag::T_Var);
    ZETASQL_ASSIGN_OR_RETURN(
        zetasql::ResolvedColumn column,
        GetResolvedColumn(
            *scope, 1,
            ((const Var*)internal::PostgresCastToNode(elem->expr))->varattno,
            /*var_levels_up=*/0));
    if (std::find(key_columns.begin(), key_columns.end(), column.name()) !=
        key_columns.end()) {
      key_columns_in_conflict_target.insert(column.name());
    } else {
      return absl::UnimplementedError(
          absl::StrCat("Column '", column.name(),
                       "' is not a key column. "
                       "Columns other than primary key columns are not "
                       "supported as ON CONFLICT targets"));
    }
  }
  if (key_columns_in_conflict_target.size() != key_columns.size()) {
    return absl::InvalidArgumentError(
        "ON CONFLICT target must list all key columns");
  }

  // Below validations are not relevant for OR_IGNORE mode.
  if (is_ignore_mode) {
    return absl::OkStatus();
  }

  // Get the table entry that exposes `excluded` alias. Build the scan that
  // allows further validation on the values in SET clause below.
  RangeTblEntry* rte_for_on_conflict = rt_fetch(rte_index, query.rtable);
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<zetasql::ResolvedTableScan> alias_table_scan,
      BuildGsqlResolvedTableScan(*rte_for_on_conflict, rte_index, scope));
  ZETASQL_RET_CHECK(!alias_table_scan->alias().empty() &&
            alias_table_scan->alias() == kExcludedAlias);

  //
  // Validate the set clause for supported use cases.
  //
  List* set_clauses = expr->onConflictSet;
  std::set<std::string> insert_column_names;
  for (const zetasql::ResolvedColumn& column : insert_column_list) {
    insert_column_names.insert(column.name());
  }
  for (TargetEntry* entry : StructList<TargetEntry*>(set_clauses)) {
    // Get the column in SET clause.
    ZETASQL_ASSIGN_OR_RETURN(zetasql::ResolvedColumn table_column,
                     GetResolvedColumn(*scope, rte_index, entry->resno,
                                       /*var_levels_up=*/0));
    // Get the value expression in SET clause.
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<zetasql::ResolvedExpr> resolved_expr,
        BuildGsqlResolvedScalarExpr(*entry->expr, scope, "ON CONFLICT"));

    // 1. Value expressions other than reference to same column in the
    // insert row is not supported.
    if (resolved_expr->node_kind() != zetasql::RESOLVED_COLUMN_REF) {
      return absl::UnimplementedError(
          absl::StrCat("Column '", table_column.name(),
                       "' must be set to the insert value in the statement "
                       "using excluded.",
                       table_column.name(),
                       " in the "
                       "ON CONFLICT DO UPDATE SET clause"));
    }

    const zetasql::ResolvedColumnRef* referenced_column =
        resolved_expr->GetAs<zetasql::ResolvedColumnRef>();
    if (referenced_column == nullptr ||
        referenced_column->column().name() != table_column.name()) {
      return absl::UnimplementedError(
          absl::StrCat("Column '", table_column.name(),
                       "' must be set to the insert value in the statement "
                       "using excluded.",
                       table_column.name(),
                       " in the "
                       "ON CONFLICT DO UPDATE SET clause"));
    }

    // 2. Setting columns not in the insert column list is not supported.
    if (insert_column_names.find(table_column.name()) ==
        insert_column_names.end()) {
      return absl::UnimplementedError(
          absl::StrCat("Column '", table_column.name(),
                       "' is not specified "
                       "in insert column list. ON CONFLICT DO "
                       "UPDATE SET clause can only include columns that are in "
                       "insert column list."));
    }
    insert_column_names.erase(table_column.name());
  }

  // 3. All columns in the insert list must be present in the SET clause.
  // In case of INSERT...DEFAULT VALUES, all writable columns must be specified
  // in the SET clause.
  if (!insert_column_names.empty()) {
    std::string missing_columns = std::accumulate(
        std::begin(insert_column_names), std::end(insert_column_names),
        std::string{}, [](const std::string& a, const std::string& b) {
          return a.empty() ? b : a + ',' + b;
        });
    return absl::UnimplementedError(absl::StrCat(
        "Column", insert_column_names.size() > 1 ? "(s) '" : " '",
        missing_columns, "' ", insert_column_names.size() > 1 ? "are " : "is ",
        "not specified in the SET clause. "
        "ON CONFLICT DO UPDATE statements "
        "must include all columns in the insert list"));
  }

  //
  // Return error if ON CONFLICT clause has WHERE clause.
  //
  if (query.onConflict->onConflictWhere != nullptr) {
    return absl::UnimplementedError(
        "WHERE clause in ON CONFLICT DO UPDATE is not supported");
  }
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedInsertStmt>>
ForwardTransformer::BuildPartialGsqlResolvedInsertStmt(const Query& query) {
  zetasql::ResolvedInsertStmt::InsertMode insert_mode =
      zetasql::ResolvedInsertStmt::OR_ERROR;
  if (query.onConflict != nullptr) {
    switch (query.onConflict->action) {
      case ONCONFLICT_NOTHING:
          insert_mode = zetasql::ResolvedInsertStmt::OR_IGNORE;
        break;
      case ONCONFLICT_UPDATE:
        insert_mode = zetasql::ResolvedInsertStmt::OR_UPDATE;
        break;
      case ONCONFLICT_NONE:
        return absl::UnimplementedError(
            "INSERT...ON CONFLICT statements are not supported.");
        break;
    }
  }

  // The first RangeTblEntry is always the INSERT target table.
  // If there is exactly one RangeTblEntry, the statement is a simple
  // INSERT...VALUES statement with a single row.
  // If there is a second RangeTblEntry with rtekind = RTE_VALUES, the
  // statement is an INSERT...VALUES statement with multiple rows.
  // If there is a second RangeTblEntry with rtekind = RTE_SUBQUERY, the
  // statement is an INSERT...SELECT statement.
  // If the statement has on conflict clause then there is an additional
  // RangeTblEntry for the `excluded` alias that allows access to rows
  // being inserted in the query.
  int rte_count = list_length(query.rtable);
  if (insert_mode == zetasql::ResolvedInsertStmt::OR_UPDATE) {
    ZETASQL_RET_CHECK(rte_count == 2 || rte_count == 3);
  } else {
    ZETASQL_RET_CHECK(rte_count == 1 || rte_count == 2);
  }

  // Get the target table, which is the first rte in the list.
  Index rtindex = 1;
  RangeTblEntry* rte = rt_fetch(rtindex, query.rtable);
  VarIndexScope target_table_scope;
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<zetasql::ResolvedTableScan> table_scan,
      BuildGsqlResolvedTableScan(*rte, rtindex, &target_table_scope));
  std::string table_alias = table_scan->alias().empty()
                                ? table_scan->table()->Name()
                                : table_scan->alias();

  ZETASQL_ASSIGN_OR_RETURN(const zetasql::Table* table, GetTableFromRTE(*rte));
  absl::flat_hash_map<int, const zetasql::Column*> unwritable_table_columns =
      GetUnwritableColumns(table);

  // insert_column_list is the list of table columns in the same order as the
  // the inserted row, but not necessarily the same order as the target table.
  std::vector<zetasql::ResolvedColumn> insert_column_list;
  absl::flat_hash_map<int, const zetasql::Column*>
      unwritable_insert_list_columns;
  if (list_length(query.targetList) == 0) {
    // This is an INSERT...DEFAULT VALUES statement. Use all writable columns
    // from the target table.

    // Use insertedCols to build the insert_column_list since no TargetEntry
    // objects are provided.
    ZETASQL_RET_CHECK_EQ(table_scan->column_list().size(),
                 table_scan->column_index_list().size());
    for (int i = 0; i < table_scan->column_list().size(); ++i) {
      // Skip over unwritable columns.
      if (unwritable_table_columns.find(i) != unwritable_table_columns.end()) {
        continue;
      }
      int column_index = table_scan->column_index_list()[i];
      int column_attnum = column_index - FirstLowInvalidHeapAttributeNumber + 1;
      ZETASQL_ASSIGN_OR_RETURN(bool inserted,
                       CheckedPgBmsIsMember(column_attnum, rte->insertedCols));
      if (inserted) {
        insert_column_list.push_back(table_scan->column_list()[i]);
      }
    }
  } else {
    // Use the columns from the target list.
    int i = 0;
    for (TargetEntry* entry : StructList<TargetEntry*>(query.targetList)) {
      // Adjust from 1-based to 0-based indexing.
      int column_index = entry->resno - 1;
      auto it = unwritable_table_columns.find(column_index);
      if (it == unwritable_table_columns.end()) {
        ZETASQL_ASSIGN_OR_RETURN(zetasql::ResolvedColumn column,
                         GetResolvedColumn(target_table_scope, rtindex,
                                           entry->resno, /*var_levels_up=*/0));
        insert_column_list.push_back(column);
      } else {
        // There is an unwritable column in the INSERT list.
        // If this is an INSERT...SELECT statement, return an error immediately
        // because INSERT...SELECT cannot generate DEFAULT values.
        if ((rte_count == 2 || rte_count == 4) &&
            rt_fetch(2, query.rtable)->rtekind == RTE_SUBQUERY) {
          return absl::InvalidArgumentError(
              absl::StrFormat("Cannot insert into column \"%s\", which is a "
                              "non-writable column",
                              it->second->Name()));
        }

        // Otherwise, this is a single row or multi row INSERT...VALUES
        // statement. If the provided value for this column is always DEFAULT,
        // the statement is valid and Spangres will simply omit the unwritable
        // column from the resolved AST.
        //
        // Skip adding unwritable columns to the insert list and also collect
        // their original index into the insert list so we can validate that
        // the specified values are DEFAULT and skip over them when building the
        // insert rows. Note that the insert list index may differ from the
        // table column index if the insert list does not include all of the
        // table columns or if the table columns are in a different order in
        // the insert list.
        unwritable_insert_list_columns[i] = it->second;
      }
      ++i;
    }
  }

  RecordColumnAccess(insert_column_list, zetasql::ResolvedStatement::WRITE);

  // insert_select_query, query_output_column_list, and query_parameter_list are
  // used for INSERT...SELECT statements.
  std::unique_ptr<const zetasql::ResolvedScan> insert_select_query = nullptr;
  zetasql::ResolvedColumnList query_output_column_list;
  std::vector<std::unique_ptr<const zetasql::ResolvedColumnRef>>
      query_parameter_list;

  // row_list is the list of inserted rows for INSERT...VALUES statements.
  std::vector<std::unique_ptr<const zetasql::ResolvedInsertRow>> row_list;

  if (list_length(query.targetList) == 0) {
    // This is an INSERT...DEFAULT VALUES statement. Create a fake row filled
    // with default values.
    std::vector<std::unique_ptr<const zetasql::ResolvedDMLValue>> value_list;
    value_list.reserve(insert_column_list.size());
    for (const zetasql::ResolvedColumn& column : insert_column_list) {
      value_list.push_back(zetasql::MakeResolvedDMLValue(
          zetasql::MakeResolvedDMLDefault(column.type())));
    }
    std::unique_ptr<const zetasql::ResolvedInsertRow> insert_row =
        zetasql::MakeResolvedInsertRow(std::move(value_list));
    row_list.push_back(std::move(insert_row));
  } else if (rte_count == 1 ||
             (rte_count == 2 &&
              insert_mode == zetasql::ResolvedInsertStmt::OR_UPDATE)) {
    // A single row INSERT...VALUES statement.
    // Collect the list of Expr objects from the TargetEntry list.
    // Use the list of Expr objects to construct a row.
    List* expr_list = nullptr;
    for (TargetEntry* entry : StructList<TargetEntry*>(query.targetList)) {
      ZETASQL_ASSIGN_OR_RETURN(expr_list, CheckedPgLappend(expr_list, entry->expr));
    }
    ZETASQL_ASSIGN_OR_RETURN(
        std::vector<std::unique_ptr<const zetasql::ResolvedDMLValue>>
            value_list,
        BuildGsqlResolvedDMLValueList(expr_list, unwritable_insert_list_columns,
                                      &target_table_scope));
    std::unique_ptr<const zetasql::ResolvedInsertRow> insert_row =
        zetasql::MakeResolvedInsertRow(std::move(value_list));
    row_list.push_back(std::move(insert_row));
  } else {
    ZETASQL_RET_CHECK(rte_count == 2 ||
              (rte_count == 3 &&
               insert_mode == zetasql::ResolvedInsertStmt::OR_UPDATE));
    RangeTblEntry* rte = rt_fetch(2, query.rtable);
    switch (rte->rtekind) {
      case RTE_VALUES: {
        // A multi row INSERT...VALUES statement.
        // Get all of the inserted rows from the RTE. Add them to the row_list.
        // values_list is a list of expression lists. The outer list is the set
        // of inserted rows and the inner lists are the values for each row.
        for (List* expr_list : StructList<List*>(rte->values_lists)) {
          ZETASQL_ASSIGN_OR_RETURN(
              std::vector<std::unique_ptr<const zetasql::ResolvedDMLValue>>
                  value_list,
              BuildGsqlResolvedDMLValueList(expr_list,
                                            unwritable_insert_list_columns,
                                            &target_table_scope));
          std::unique_ptr<const zetasql::ResolvedInsertRow> insert_row =
              zetasql::MakeResolvedInsertRow(std::move(value_list));
          row_list.push_back(std::move(insert_row));
        }
        break;
      }
      case RTE_SUBQUERY: {
        // An INSERT...SELECT statement.
        // Transform the SELECT query. The output columns from the scan are the
        // output columns for the subquery.
        // The subquery output columns do not have names, so there is no need
        // to collect the output_name_list.
        ZETASQL_RET_CHECK_NE(rte->subquery, nullptr);
        ZETASQL_RET_CHECK_NE(rte->alias->aliasname, nullptr);
        ZETASQL_ASSIGN_OR_RETURN(insert_select_query,
                         BuildGsqlResolvedScanForQueryExpression(
                             *rte->subquery, /*top_level_query=*/false,
                             &empty_var_index_scope_, rte->alias->aliasname,
                             /*output_name_list=*/nullptr));
        query_output_column_list = insert_select_query->column_list();
        break;
      }
      default:
        return absl::UnimplementedError(
            absl::StrCat("rtekind not supported in an INSERT statement: ",
                         internal::RTEKindToString(rte->rtekind)));
    }
  }

  if (insert_mode == zetasql::ResolvedInsertStmt::OR_UPDATE ||
      insert_mode == zetasql::ResolvedInsertStmt::OR_IGNORE) {
    if (
        query.returningList != nullptr) {
      return absl::UnimplementedError(
          "RETURNING with ON CONFLICT clause is not supported");
    }
    ZETASQL_RETURN_IF_ERROR(CheckForUnsupportedOnConflictClause(
        query, rte_count, *table_scan->table(), insert_column_list,
        &target_table_scope,
        (insert_mode == zetasql::ResolvedInsertStmt::OR_IGNORE)));
  }

  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const zetasql::ResolvedReturningClause>
                       returning_clause,
                   BuildGsqlReturningClauseForDML(
                       query.returningList, table_alias, &target_table_scope));

  // Construct the ResolvedInsertStmt.
  // insert_mode is OR_ERROR because we don't support ON CONFLICT.
  // assert_rows_modified is not a supported feature in PostgreSQL.
  // INSERT...RETURNING is not supported.
  // Construct a ResolvedInsertStmt.
  return zetasql::MakeResolvedInsertStmt(
      std::move(table_scan), insert_mode,
      /*assert_rows_modified=*/nullptr, std::move(returning_clause),
      insert_column_list, std::move(query_parameter_list),
      std::move(insert_select_query), query_output_column_list,
      std::move(row_list));
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedUpdateStmt>>
ForwardTransformer::BuildPartialGsqlResolvedUpdateStmt(const Query& query) {
  // The first RangeTblEntry is always the UPDATE target table.
  int rte_count = list_length(query.rtable);
  if (rte_count > 1) {
    return absl::UnimplementedError(
        "UPDATE...FROM statements are not supported.");
  }

  // Validity check.
  ZETASQL_RET_CHECK(list_length(query.jointree->fromlist) == 1);

  Index rtindex = 1;
  RangeTblEntry* rte = rt_fetch(rtindex, query.rtable);
  VarIndexScope target_table_scope;
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<zetasql::ResolvedTableScan> table_scan,
      BuildGsqlResolvedTableScan(*rte, rtindex, &target_table_scope));
  std::string table_alias = table_scan->alias().empty()
                                ? table_scan->table()->Name()
                                : table_scan->alias();

  // Build a ResolvedExpr for the WHERE clause.
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<const zetasql::ResolvedExpr> gsql_where_expr,
      BuildGsqlWhereClauseExprForDML(query.jointree->quals,
                                     &target_table_scope));

  ZETASQL_ASSIGN_OR_RETURN(const zetasql::Table* table, GetTableFromRTE(*rte));
  absl::flat_hash_map<int, const zetasql::Column*> unwritable_table_columns =
      GetUnwritableColumns(table);

  absl::flat_hash_set<int> updated_resnos;
  std::vector<std::unique_ptr<const zetasql::ResolvedUpdateItem>>
      update_item_list;
  for (TargetEntry* entry : StructList<TargetEntry*>(query.targetList)) {
    // Check for cases where the same column is set multiple times.
    // Note that this (and vanilla PG) will return an error even if the set
    // value is the same.
    if (updated_resnos.contains(entry->resno)) {
      return absl::InvalidArgumentError(
          absl::StrFormat("multiple assignments to same column \"%s\"",
                       entry->resname));
    }
    // Adjust from 1-based to 0-based indexing.
    int column_index = entry->resno - 1;
    auto it = unwritable_table_columns.find(column_index);
    if (it != unwritable_table_columns.end()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("Cannot UPDATE value on non-writable column \"%s\"",
                          it->second->Name()));
    }
    updated_resnos.insert(entry->resno);

    // Build a ResolvedUpdateItem for each TargetEntry and append it to
    // update_item_list.
    ZETASQL_ASSIGN_OR_RETURN(zetasql::ResolvedColumn column,
                     GetResolvedColumn(target_table_scope, rtindex,
                                       entry->resno, /*var_levels_up=*/0));

    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const zetasql::ResolvedDMLValue> dml_value,
        BuildGsqlResolvedDMLValue(*entry->expr, &target_table_scope,
                                  "UPDATE clause"));

    std::unique_ptr<zetasql::ResolvedUpdateItem> update_item =
        zetasql::MakeResolvedUpdateItem();
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<zetasql::ResolvedColumnRef> column_ref,
        BuildGsqlResolvedColumnRef(column,
                                   /*is_correlated=*/false,
                                   zetasql::ResolvedStatement::WRITE));
    update_item->set_target(std::move(column_ref));
    update_item->set_set_value(std::move(dml_value));

    update_item_list.push_back(std::move(update_item));
  }

  // Build a ResolvedReturningClause.
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const zetasql::ResolvedReturningClause>
                       returning_clause,
                   BuildGsqlReturningClauseForDML(
                       query.returningList, table_alias, &target_table_scope));

  // Construct a ResolvedUpdateStmt.
  return MakeResolvedUpdateStmt(
      std::move(table_scan), /*assert_rows_modified=*/nullptr,
      std::move(returning_clause), /*array_offset_column=*/nullptr,
      std::move(gsql_where_expr), std::move(update_item_list),
      /*from_scan=*/nullptr);
}

absl::StatusOr<std::unique_ptr<const zetasql::ResolvedExpr>>
ForwardTransformer::BuildGsqlWhereClauseExprForDML(
    Node* pg_where_clause, const VarIndexScope* var_index_scope) {
  if (pg_where_clause == nullptr) {
    // PostgreSQL allows an UPDATE/DELETE statements without a WHERE clause, in
    // which all rows of the target table will be updated. Meanwhile, ZetaSQL
    // requires presence of a WHERE clause. In this case, simply add a
    // "WHERE true" clause into the resolved AST produced by the transformer.
    return zetasql::MakeResolvedLiteral(zetasql::types::BoolType(),
                                          zetasql::Value::Bool(true));
  } else {
    if (internal::IsExpr(*pg_where_clause)) {
      return BuildGsqlResolvedScalarExpr(*PostgresCastToExpr(pg_where_clause),
                                         var_index_scope, "WHERE clause");
    } else {
      return absl::UnimplementedError(absl::StrCat(
          "Node type ", NodeTagToNodeString(nodeTag(&pg_where_clause)),
          " is unsupported in WHERE clauses."));
    }
  }
}

absl::StatusOr<std::unique_ptr<const zetasql::ResolvedDMLValue>>
ForwardTransformer::BuildGsqlResolvedDMLValue(
    Expr& expr, const VarIndexScope* var_index_scope, const char* clause_name) {
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<zetasql::ResolvedExpr> resolved_expr,
      BuildGsqlResolvedScalarExpr(expr, var_index_scope, clause_name));
  return zetasql::MakeResolvedDMLValue(std::move(resolved_expr));
}

// Build an INSERT row from the Expr list.
absl::StatusOr<std::vector<std::unique_ptr<const zetasql::ResolvedDMLValue>>>
ForwardTransformer::BuildGsqlResolvedDMLValueList(
    List* expr_list,
    const absl::flat_hash_map<int, const zetasql::Column*>&
        unwritable_insert_list_columns,
    const VarIndexScope* var_index_scope) {
  std::vector<std::unique_ptr<const zetasql::ResolvedDMLValue>> value_list;
  int i = 0;
  for (Expr* expr : StructList<Expr*>(expr_list)) {
    auto it = unwritable_insert_list_columns.find(i);
    if (it != unwritable_insert_list_columns.end()) {
      // This is an unwritable column. If the value is DEFAULT, just skip
      // over it. Otherwise, return an error.
      if (expr->type != T_SetToDefault) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "Cannot insert into column \"%s\", which is a non-writable column",
            it->second->Name()));
      }
    } else {
      ZETASQL_ASSIGN_OR_RETURN(
          std::unique_ptr<const zetasql::ResolvedDMLValue> dml_value,
          BuildGsqlResolvedDMLValue(*expr, var_index_scope, "INSERT VALUES"));
      value_list.push_back(std::move(dml_value));
    }
    ++i;
  }
  return value_list;
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedDeleteStmt>>
ForwardTransformer::BuildPartialGsqlResolvedDeleteStmt(const Query& query) {
  // The first RangeTblEntry is always the DELETE target table.
  // Any additional RangeTblEntry objects are for USING statements, which are
  // not supported.
  int rte_count = list_length(query.rtable);
  ZETASQL_RET_CHECK(rte_count >= 1);
  if (rte_count > 1) {
    return absl::UnimplementedError(
        "DELETE...USING statements are not supported.");
  }
  Index rtindex = 1;
  RangeTblEntry* rte = rt_fetch(rtindex, query.rtable);
  VarIndexScope target_table_scope;
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<zetasql::ResolvedTableScan> table_scan,
      BuildGsqlResolvedTableScan(*rte, rtindex, &target_table_scope));
  std::string table_alias = table_scan->alias().empty()
                                ? table_scan->table()->Name()
                                : table_scan->alias();

  // There should be exactly one range table ref which points to the DELETE
  // target table range table entry.
  ZETASQL_RET_CHECK(query.jointree != nullptr && query.jointree->fromlist != nullptr &&
            list_length(query.jointree->fromlist) == 1);


  // Build a ResolvedExpr for the WHERE clause
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const zetasql::ResolvedExpr> where_expr,
                   BuildGsqlWhereClauseExprForDML(query.jointree->quals,
                                                  &target_table_scope));

  // Build a ResolvedReturningClause.
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const zetasql::ResolvedReturningClause>
                       returning_clause,
                   BuildGsqlReturningClauseForDML(
                       query.returningList, table_alias, &target_table_scope));

  // Construct a ResolvedDeleteStmt.
  return zetasql::MakeResolvedDeleteStmt(
      std::move(table_scan), /*assert_rows_modified=*/nullptr,
      std::move(returning_clause), /*array_offset_column=*/nullptr,
      std::move(where_expr));
}

}  // namespace postgres_translator
